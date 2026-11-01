//! Source orchestration: connection handling and start-up decisions. Batching, `seq`
//! numbering and acknowledgement-driven checkpointing are the shared CDC core
//! (`sources::cdc`).

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use mysql_async::{Conn, Opts, OptsBuilder, Row, SslOpts, prelude::Queryable};
use serde_json::Value as Json;
use vector_lib::config::LogNamespace;

use super::{
    EventFormat, EventProcessingFailureHandlingMode, MysqlCdcConfig, RecordImages,
    SnapshotLockingMode, SnapshotMode, SnapshotStrategy, SslMode, UnplannedSnapshots,
    binlog::{self, StreamPosition},
    convert::ConvertOptions,
    filter::Filters,
    incremental,
    offset::{CheckpointStore, GtidSet, Offset, TableRef},
    schema::TableInfo,
    snapshot,
    table_schema::{self, Observed},
};
pub(super) use crate::sources::cdc::{
    control::{snapshot_completed, snapshot_held},
    emitter::{EventBuilder, EventMeta},
};
use crate::{
    SourceSender,
    config::SourceContext,
    internal_events::MysqlCdcDecodeError,
    shutdown::ShutdownSignal,
    sources::{
        Source,
        cdc::{
            checkpoint::{Checkpoints, OffsetPublisher, check_acknowledgements},
            lock::{WriterGuard, run_locked},
            recovery::{OffsetRecovery, Recovered},
            signal::{self, SignalsConfig},
        },
        source_with_error,
    },
};

/// `MySQL`'s system databases, never captured.
const SYSTEM_DATABASES: &[&str] = &["mysql", "sys", "performance_schema", "information_schema"];

/// Version reported in `source.version`.
pub const CONNECTOR_VERSION: &str = concat!("vector-mysql_cdc-", env!("CARGO_PKG_VERSION"));

/// Static settings shared by the snapshot and streaming phases.
#[allow(
    clippy::struct_excessive_bools,
    reason = "settings derived from independent configuration options"
)]
pub(super) struct Settings {
    pub opts: Opts,
    pub endpoint: String,
    pub server_id: u32,
    pub topic_prefix: String,
    pub filters: Filters,
    pub convert: ConvertOptions,
    pub include_query: bool,
    pub include_schema_changes: bool,
    pub heartbeat: Duration,
    pub connect_timeout: Duration,
    pub snapshot_mode: SnapshotMode,
    pub locking_mode: SnapshotLockingMode,
    pub failure_mode: EventProcessingFailureHandlingMode,
    pub snapshot_batch_size: usize,
    pub snapshot_threads: usize,
    pub snapshot_chunk_size: u64,
    pub event_format: EventFormat,
    pub record_images: RecordImages,
    pub include_table_schemas: bool,
    pub observe_output: bool,
    pub snapshot_end_markers: bool,
    pub snapshot_strategy: SnapshotStrategy,
    pub snapshot_new_tables: bool,
    pub unplanned_snapshots: UnplannedSnapshots,
    pub incremental_chunk_size: usize,
    /// Publish committed offsets at most this often (`publish_offsets`).
    pub publish_offsets: Option<Duration>,
    pub signals: Option<SignalsConfig>,
    /// Tables already reported as having no key.
    pub warned_no_key: std::sync::Mutex<HashSet<String>>,
}

impl Settings {
    /// Warns once per table that has neither a primary key nor `pk_override`: its
    /// events carry no key, so downstream merges cannot tell rows apart.
    pub fn warn_if_keyless(&self, info: &TableInfo) {
        if info.has_primary_key() {
            return;
        }
        let name = format!("{}.{}", info.database, info.table);
        let first = self
            .warned_no_key
            .lock()
            .is_ok_and(|mut warned| warned.insert(name.clone()));
        if first {
            warn!(
                message = "Table has no primary key; its change events carry no key, so a downstream \
                           merge cannot identify rows. Add a primary key or set `pk_override.<database>.<table>`.",
                table = %name,
                internal_log_rate_limit = false,
            );
        }
    }

    /// Applies `event_processing_failure_handling_mode` to a change that could not be converted.
    /// `Ok(())` means the change is skipped; `Err` stops the source.
    pub fn handle_failure(&self, error: String, database: &str, table: &str) -> Result<(), String> {
        match self.failure_mode {
            EventProcessingFailureHandlingMode::Fail => {
                let message =
                    format!("failed to convert a change in `{database}`.`{table}`: {error}");
                emit!(MysqlCdcDecodeError {
                    error,
                    database,
                    table,
                });
                Err(message)
            }
            EventProcessingFailureHandlingMode::Warn => {
                emit!(MysqlCdcDecodeError {
                    error,
                    database,
                    table,
                });
                Ok(())
            }
            EventProcessingFailureHandlingMode::Skip => {
                debug!(message = "Skipping change that could not be converted.", %error, database, table);
                Ok(())
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "validates the config and wires the source together"
)]
pub(super) fn build(config: &MysqlCdcConfig, cx: SourceContext) -> crate::Result<Source> {
    if config.server_id == 0 {
        return Err("`server_id` must be a non-zero, unique replica id".into());
    }
    if config.batch_size == 0 || config.snapshot_batch_size == Some(0) {
        return Err("`batch_size` and `snapshot_batch_size` must be greater than zero".into());
    }
    if config.snapshot_max_threads == 0 || config.snapshot_max_threads > 64 {
        return Err("`snapshot_max_threads` must be between 1 and 64".into());
    }
    if config.heartbeat_interval_secs == 0 {
        return Err("`heartbeat_interval_secs` must be greater than zero".into());
    }

    let filters = Filters::new(
        &config.database_include_list,
        &config.database_exclude_list,
        &config.table_include_list,
        &config.table_exclude_list,
        &config.column_exclude_list,
    )?
    .with_system_databases(SYSTEM_DATABASES)
    .excluding_online_schema_change_tables(config.exclude_online_schema_change_tables)
    .with_key_columns(key_columns(&config.pk_override)?);

    let data_dir = cx
        .globals
        .resolve_and_make_data_subdir(config.data_dir.as_ref(), cx.key.id())?;

    let settings = Settings {
        opts: build_opts(config),
        endpoint: format!("{}:{}", config.hostname, config.port),
        server_id: config.server_id,
        topic_prefix: config.topic_prefix.clone(),
        filters,
        convert: ConvertOptions {
            decimal: config.decimal_handling_mode,
            binary: config.binary_handling_mode,
            time_precision: config.time_precision_mode,
            json: config.json_handling_mode,
        },
        include_query: config.include_query,
        include_schema_changes: config.include_schema_changes,
        heartbeat: Duration::from_secs(config.heartbeat_interval_secs),
        connect_timeout: Duration::from_secs(config.connect_timeout_secs.max(1)),
        snapshot_mode: config.snapshot_mode,
        locking_mode: config.snapshot_locking_mode,
        failure_mode: config.event_processing_failure_handling_mode,
        snapshot_batch_size: config.snapshot_batch_size.unwrap_or(config.batch_size),
        snapshot_threads: config.snapshot_max_threads,
        snapshot_chunk_size: config.snapshot_chunk_size,
        event_format: config.event_format,
        record_images: config.record_images,
        include_table_schemas: config.include_table_schemas,
        observe_output: config.observe_output,
        snapshot_end_markers: config.snapshot_end_markers,
        snapshot_strategy: config.snapshot_strategy,
        snapshot_new_tables: config.snapshot_new_tables,
        unplanned_snapshots: config.unplanned_snapshots,
        incremental_chunk_size: config.incremental_snapshot_chunk_size.max(1),
        publish_offsets: config
            .publish_offsets
            .then(|| Duration::from_secs(config.offset_publish_interval_secs.max(1))),
        signals: config.signals.clone(),
        warned_no_key: std::sync::Mutex::default(),
    };

    let acknowledgements = cx.do_acknowledgements(config.acknowledgements);
    let log_namespace = cx.log_namespace(config.log_namespace);
    check_acknowledgements(
        MysqlCdcConfig::NAME,
        acknowledgements,
        config.publish_offsets,
    )?;

    let lock = config
        .single_writer_lock
        .as_ref()
        .map(|lock| lock.build(&cx.proxy))
        .transpose()?;
    if lock.is_none() {
        info!(
            message = "No `single_writer_lock` configured: make sure only one instance captures \
                       this server at a time (including during redeploys)."
        );
    }
    let recovery = config
        .offset_recovery
        .as_ref()
        .map(|recovery| recovery.build(lock.as_ref()))
        .transpose()?;
    if recovery.is_some() && !config.publish_offsets {
        return Err(
            "`offset_recovery` resumes from published offsets: enable `publish_offsets`".into(),
        );
    }

    let batch_size = config.batch_size;
    let shutdown = cx.shutdown;
    let out = cx.out;
    Ok(source_with_error(run_locked(
        lock,
        shutdown.clone(),
        move |guard, written| {
            run(
                settings,
                data_dir,
                recovery,
                out,
                shutdown,
                acknowledgements,
                log_namespace,
                batch_size,
                guard,
                written,
            )
        },
    )))
}

/// Flattens `pk_override` (`database` → `table` → `"col1, col2"`) into `database.table` → columns.
fn key_columns(
    overrides: &HashMap<String, HashMap<String, String>>,
) -> Result<HashMap<String, Vec<String>>, String> {
    let mut out = HashMap::new();
    for (database, tables) in overrides {
        for (table, columns) in tables {
            let columns: Vec<String> = columns
                .split(',')
                .map(|c| c.trim().trim_matches('`').to_owned())
                .filter(|c| !c.is_empty())
                .collect();
            if columns.is_empty() {
                return Err(format!("`pk_override.{database}.{table}` lists no columns"));
            }
            out.insert(format!("{database}.{table}"), columns);
        }
    }
    Ok(out)
}

fn build_opts(config: &MysqlCdcConfig) -> Opts {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(config.hostname.clone())
        .tcp_port(config.port)
        .user(Some(config.user.clone()))
        .pass(config.password.as_ref().map(|p| p.inner().to_owned()))
        .prefer_socket(false)
        .tcp_keepalive(Some(Duration::from_mins(1)));

    if config.ssl_mode != SslMode::Disabled {
        let mut ssl = SslOpts::default();
        if let Some(ca) = &config.ssl_ca_file {
            ssl = ssl.with_root_certs(vec![ca.clone().into()]);
        }
        ssl = match config.ssl_mode {
            SslMode::Required => ssl
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
            SslMode::VerifyCa => ssl.with_danger_skip_domain_validation(true),
            SslMode::VerifyIdentity | SslMode::Disabled => ssl,
        };
        builder = builder.ssl_opts(ssl);
    }

    builder.into()
}

/// Opens a new connection, bounded by the configured connect timeout.
pub(super) async fn connect(settings: &Settings) -> Result<Conn, String> {
    match tokio::time::timeout(settings.connect_timeout, Conn::new(settings.opts.clone())).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(error)) => Err(format!(
            "failed to connect to {}: {error}",
            settings.endpoint
        )),
        Err(_) => Err(format!(
            "timed out after {:?} connecting to {}",
            settings.connect_timeout, settings.endpoint
        )),
    }
}

/// Server facts relevant to change capture.
#[derive(Debug)]
pub(super) struct ServerInfo {
    pub server_uuid: String,
    pub gtid_mode_on: bool,
    pub version: String,
}

/// Reads and validates the server's binlog configuration.
pub(super) async fn inspect_server(conn: &mut Conn) -> Result<ServerInfo, String> {
    type Vars = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let vars: Option<Vars> = conn
        .query_first(
            "SELECT @@global.server_uuid, @@global.log_bin, @@global.binlog_format, \
             @@global.binlog_row_image, @@global.binlog_row_metadata, @@global.gtid_mode, @@version",
        )
        .await
        .map_err(|error| {
            format!(
                "failed to read server variables (MySQL 8.0 or newer is required): {error}"
            )
        })?;
    let (uuid, log_bin, format, row_image, row_metadata, gtid_mode, version) =
        vars.ok_or("failed to read server variables")?;

    let is = |v: &Option<String>, expected: &str| {
        v.as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case(expected))
    };

    if !(is(&log_bin, "1") || is(&log_bin, "ON")) {
        return Err("binary logging is disabled on the server (`log_bin` must be ON)".into());
    }
    if !is(&format, "ROW") {
        return Err(format!(
            "`binlog_format` must be ROW (current: {})",
            format.unwrap_or_default()
        ));
    }
    if !is(&row_metadata, "FULL") {
        return Err(format!(
            "`binlog_row_metadata` must be FULL (current: {}); run `SET PERSIST binlog_row_metadata = 'FULL'`",
            row_metadata.unwrap_or_default()
        ));
    }
    // Incomplete images would blank out real values downstream: the first change fails anyway.
    if !is(&row_image, "FULL") {
        return Err(format!(
            "`binlog_row_image` must be FULL (current: {}); run `SET PERSIST binlog_row_image = 'FULL'`",
            row_image.unwrap_or_default()
        ));
    }

    // Partial JSON updates cannot be turned into full `after` images.
    let row_value_options: Option<Option<String>> = conn
        .query_first("SELECT @@global.binlog_row_value_options")
        .await
        .unwrap_or(None);
    if let Some(Some(options)) = row_value_options
        && options.to_ascii_uppercase().contains("PARTIAL_JSON")
    {
        return Err(
            "`binlog_row_value_options=PARTIAL_JSON` is not supported; set it to an empty value"
                .into(),
        );
    }

    Ok(ServerInfo {
        server_uuid: uuid.unwrap_or_default(),
        gtid_mode_on: is(&gtid_mode, "ON"),
        version: version.unwrap_or_default(),
    })
}

/// The server's current binlog position.
#[derive(Debug)]
pub(super) struct BinlogPosition {
    pub file: String,
    pub pos: u64,
    pub gtid_set: String,
}

/// Reads the foreign keys of captured tables. Warns about cascading ones: `MySQL` applies
/// `ON DELETE/UPDATE CASCADE`, `SET NULL` and `SET DEFAULT` inside the storage engine and does
/// not write the resulting child-row changes to the binlog, so they never reach the stream.
async fn foreign_keys(conn: &mut Conn, filters: &Filters) -> Result<HashMap<String, Json>, String> {
    type FkRow = (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    );
    let rows: Vec<FkRow> = conn
        .query(
            "SELECT rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME, \
             rc.UNIQUE_CONSTRAINT_SCHEMA, rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE, \
             GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION SEPARATOR ','), \
             GROUP_CONCAT(k.REFERENCED_COLUMN_NAME ORDER BY k.ORDINAL_POSITION SEPARATOR ',') \
             FROM information_schema.REFERENTIAL_CONSTRAINTS rc \
             JOIN information_schema.KEY_COLUMN_USAGE k \
               ON k.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA AND k.TABLE_NAME = rc.TABLE_NAME \
              AND k.CONSTRAINT_NAME = rc.CONSTRAINT_NAME \
             GROUP BY rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME, \
               rc.UNIQUE_CONSTRAINT_SCHEMA, rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE",
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut by_table: HashMap<String, Vec<Json>> = HashMap::new();
    for (db, table, name, ref_db, ref_table, on_update, on_delete, cols, ref_cols) in rows {
        if !filters.table_included(&db, &table) {
            continue;
        }
        let acts = |rule: &str| rule != "RESTRICT" && rule != "NO ACTION";
        if acts(&on_update) || acts(&on_delete) {
            warn!(
                message = "Foreign key changes child rows inside MySQL; those changes are not in the \
                           binlog and will not be captured. Apply the rule downstream (see the \
                           table's schema document) or reconcile with a snapshot.",
                table = %format!("{db}.{table}"),
                constraint = %name,
                references = %format!("{ref_db}.{ref_table}"),
                on_update = %on_update,
                on_delete = %on_delete,
                internal_log_rate_limit = false,
            );
        }
        let split = |s: &str| s.split(',').map(str::to_owned).collect::<Vec<_>>();
        by_table
            .entry(format!("{db}.{table}"))
            .or_default()
            .push(serde_json::json!({
                "name": name,
                "columns": split(&cols),
                "referenced_db": ref_db,
                "referenced_table": ref_table,
                "referenced_columns": split(&ref_cols),
                "on_update": on_update,
                "on_delete": on_delete,
            }));
    }
    Ok(by_table
        .into_iter()
        .map(|(k, v)| (k, Json::Array(v)))
        .collect())
}

/// Reads the current binlog position (`SHOW BINARY LOG STATUS` on 8.2+, `SHOW MASTER STATUS`
/// before that).
pub(super) async fn current_position(conn: &mut Conn) -> Result<BinlogPosition, String> {
    let row: Option<Row> = match conn.query_first("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(_) => conn
            .query_first("SHOW MASTER STATUS")
            .await
            .map_err(|error| format!("failed to read binlog position: {error}"))?,
    };
    let row = row.ok_or("the server reported no binlog position; is binary logging enabled?")?;
    let file: Option<String> = row.get("File");
    let pos: Option<u64> = row.get("Position");
    let gtid_set: Option<Option<String>> = row.get("Executed_Gtid_Set");
    Ok(BinlogPosition {
        file: file.ok_or("missing `File` in binlog status")?,
        pos: pos.ok_or("missing `Position` in binlog status")?,
        gtid_set: gtid_set
            .flatten()
            .unwrap_or_default()
            .replace(['\n', '\r', ' '], ""),
    })
}

/// Whether the server still has the binlog needed to resume from `offset`.
async fn offset_available(
    conn: &mut Conn,
    offset: &Offset,
    use_gtid: bool,
) -> Result<bool, String> {
    if use_gtid {
        // Every purged transaction must already be contained in our executed set.
        let subset: Option<i64> = conn
            .exec_first(
                "SELECT GTID_SUBSET(@@global.gtid_purged, ?)",
                (offset.gtid_set.clone(),),
            )
            .await
            .map_err(|error| format!("failed to compare GTID sets: {error}"))?;
        return Ok(subset == Some(1));
    }

    let logs: Vec<Row> = conn
        .query("SHOW BINARY LOGS")
        .await
        .map_err(|error| format!("failed to list binary logs: {error}"))?;
    Ok(logs.iter().any(|row| {
        row.get::<String, _>(0)
            .is_some_and(|name| name == offset.file)
    }))
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the source's lifecycle: recover, snapshot, stream"
)]
async fn run(
    settings: Settings,
    data_dir: PathBuf,
    recovery: Option<OffsetRecovery>,
    out: SourceSender,
    mut shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    batch_size: usize,
    guard: WriterGuard,
    written: tokio::sync::oneshot::Sender<()>,
) -> Result<(), String> {
    let store = CheckpointStore::<Offset>::new(&data_dir);
    let mut stored = store.load().await.map_err(|error| {
        format!(
            "failed to load offsets from {}: {error}",
            store.path().display()
        )
    })?;

    // No stored offset: resume from the published one if the lake has it (`offset_recovery`).
    let mut lost_table_state = false;
    if stored.is_none()
        && let Some(recovery) = &recovery
    {
        let Some(recovered) = recovery
            .recover(&settings.topic_prefix, &mut shutdown)
            .await?
        else {
            return Ok(());
        };
        let offset = match recovered {
            Recovered::Empty => {
                info!(
                    message = "No stored offset and the connector's folder in the lake is empty; starting from scratch."
                );
                None
            }
            Recovered::Published(payload) => {
                if let Some(checkpoint) = payload.get("checkpoint") {
                    Some(
                        serde_json::from_value::<Offset>(checkpoint.clone()).map_err(|error| {
                            format!(
                                "the published offset's `checkpoint` is not a valid offset: {error}"
                            )
                        })?,
                    )
                } else {
                    // Published before offsets carried `checkpoint`: rebuild the snapshot state.
                    let Some(snapshotted) = recovery
                        .snapshotted_tables(&settings.topic_prefix, &mut shutdown)
                        .await?
                    else {
                        return Ok(());
                    };
                    lost_table_state = true;
                    Some(Offset::from_published(&payload, snapshotted)?)
                }
            }
        };
        if let Some(offset) = offset {
            info!(
                message = "No stored offset; resuming from the offset published in the lake.",
                offset = %offset,
                next_seq = offset.next_seq,
            );
            // Stored at once, so an operator can act on it (for example approve a re-snapshot)
            // if the source cannot go on from it.
            store.store(&offset).await.map_err(|error| {
                format!(
                    "failed to store the recovered offset in {}: {error}",
                    store.path().display()
                )
            })?;
            stored = Some(offset);
        }
    }

    // Connect for start-up inspection; retry while the database is unavailable.
    let mut conn = loop {
        match connect(&settings).await {
            Ok(conn) => break conn,
            Err(error) => {
                emit!(crate::internal_events::MysqlCdcConnectionError {
                    error,
                    endpoint: &settings.endpoint,
                });
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }
    };

    let server = inspect_server(&mut conn).await?;
    info!(
        message = "Connected to MySQL.",
        endpoint = %settings.endpoint,
        version = %server.version,
        server_uuid = %server.server_uuid,
        gtid_mode = server.gtid_mode_on,
    );

    let poisoned = Arc::new(AtomicBool::new(false));
    let builder = EventBuilder::new(
        MysqlCdcConfig::NAME,
        log_namespace,
        settings.event_format,
        settings.record_images,
    );
    let publisher = settings.publish_offsets.map(|interval| OffsetPublisher {
        out: out.clone(),
        builder,
        server_name: settings.topic_prefix.clone(),
        interval,
    });
    let checkpoints = Checkpoints::start(
        acknowledgements,
        store,
        Arc::clone(&poisoned),
        publisher,
        guard,
        written,
    );
    let mut emitter = Emitter::new(
        out,
        builder,
        acknowledgements,
        checkpoints.finalizer,
        batch_size,
        poisoned,
        settings.include_table_schemas,
    )
    .with_observe(settings.observe_output);

    // Continue numbering where the stored offset left off.
    emitter.set_next_seq(
        stored.as_ref().map_or(1, |offset| offset.next_seq),
        stored.as_ref().map_or(0, |offset| offset.seq_sub),
    );

    match foreign_keys(&mut conn, &settings.filters).await {
        Ok(fks) => emitter.set_foreign_keys(fks),
        Err(error) => warn!(message = "Could not read foreign keys.", %error),
    }

    // Decide where to start.
    let mut needs_snapshot = match &stored {
        None => settings.snapshot_mode != SnapshotMode::NoData,
        Some(offset) if !offset.snapshot_completed => {
            info!(message = "The previous snapshot did not complete; snapshotting again.");
            true
        }
        Some(_) => false,
    };

    let mut start_offset = stored.clone().filter(|o| o.snapshot_completed);

    if let Some(offset) = &start_offset {
        let use_gtid = server.gtid_mode_on && !offset.gtid_set.is_empty();
        if !use_gtid
            && let Some(uuid) = &offset.server_uuid
            && uuid != &server.server_uuid
        {
            return Err(format!(
                "the stored offset {offset} was taken on server {uuid}, but the source is now \
                 connected to {}; without GTIDs binlog positions are not portable between servers. \
                 Enable GTID mode or remove {} to start over",
                server.server_uuid,
                data_dir.join(super::offset::CHECKPOINT_FILENAME).display()
            ));
        }
        if !offset_available(&mut conn, offset, use_gtid).await? {
            if settings.snapshot_mode == SnapshotMode::WhenNeeded {
                if settings.unplanned_snapshots == UnplannedSnapshots::Hold
                    && !offset.resnapshot_approved
                {
                    let reason = format!(
                        "the stored binlog offset {offset} is no longer available on the server \
                         (the binlog was purged), so every captured table would be snapshotted \
                         again"
                    );
                    let Some(signals) = &settings.signals else {
                        return Err(format!(
                            "{reason}. `unplanned_snapshots` is `hold`: approve the re-snapshot \
                             by setting `\"resnapshot_approved\": true` in {} while the source \
                             is stopped, or set `unplanned_snapshots: allow`",
                            data_dir.join(super::offset::CHECKPOINT_FILENAME).display()
                        ));
                    };
                    if !signal::await_approval(signals, &reason, &mut shutdown).await? {
                        return emitter.close().await;
                    }
                }
                warn!(
                    message = "The stored binlog offset is no longer available on the server; re-snapshotting.",
                    offset = %offset,
                );
                needs_snapshot = true;
                start_offset = None;
            } else {
                return Err(format!(
                    "the stored binlog offset {offset} is no longer available on the server (the binlog \
                     was purged). Set `snapshot_mode: when_needed` to re-snapshot automatically"
                ));
            }
        }
    }

    // The tables the filters capture right now.
    let captured = incremental::captured_tables(&mut conn, &settings).await?;
    let incremental_initial =
        needs_snapshot && settings.snapshot_strategy == SnapshotStrategy::Incremental;
    if incremental_initial {
        if !server.gtid_mode_on {
            return Err("`snapshot_strategy: incremental` needs GTID mode (`gtid_mode=ON`)".into());
        }
        if settings.snapshot_mode == SnapshotMode::InitialOnly {
            return Err(
                "`snapshot_strategy: incremental` reads tables while streaming, so it \
                        cannot be combined with `snapshot_mode: initial_only`"
                    .into(),
            );
        }
    }

    if needs_snapshot && !incremental_initial {
        drop(conn);
        let completed = snapshot::run(&settings, &server, &mut emitter, shutdown.clone()).await?;
        let Some(offset) = completed else {
            // Shut down mid-snapshot; it will be redone on the next start.
            return emitter.close().await;
        };
        start_offset = Some(offset);

        if settings.snapshot_mode == SnapshotMode::InitialOnly {
            info!(
                message = "Snapshot complete; `snapshot_mode` is `initial_only`, so not streaming."
            );
            return emitter.close().await;
        }
    } else if needs_snapshot || start_offset.is_none() {
        // An incremental snapshot, or `no_data` on first start: stream from the current
        // position right away.
        let position = current_position(&mut conn).await?;
        let (snapshotted, progress) = if incremental_initial {
            info!(
                message = "Starting an incremental snapshot while streaming.",
                tables = captured.len(),
            );
            (
                Vec::new(),
                (!captured.is_empty()).then(|| incremental::new_progress(captured.clone())),
            )
        } else {
            // `no_data`: existing rows are deliberately not captured.
            (captured.clone(), None)
        };
        let offset = Offset::new(
            Some(server.server_uuid.clone()),
            position.file,
            position.pos,
            position.gtid_set,
            true,
        )
        .with_tables(Some(snapshotted), progress);
        info!(message = "Starting to stream from the current binlog position.", offset = %offset);
        emitter.commit(offset.clone());
        emitter.flush().await?;
        start_offset = Some(offset);
        drop(conn);
    } else {
        drop(conn);
    }

    let offset = start_offset.expect("start offset is set");
    let use_gtid = server.gtid_mode_on && !offset.gtid_set.is_empty();
    let gtid_set: GtidSet = offset
        .gtid_set
        .parse()
        .map_err(|error| format!("invalid GTID set in offset: {error}"))?;
    info!(message = "Streaming changes from the binlog.", offset = %offset, use_gtid);

    // Offsets written before table tracking existed: the tables captured now are covered.
    let snapshotted: Vec<TableRef> = offset
        .snapshotted_tables
        .clone()
        .unwrap_or_else(|| captured.clone());
    let mut progress = offset.incremental.clone();

    // Held snapshots: kept (for tables still captured) until an operator releases them, or
    // started now if unplanned snapshots are allowed. Tables whose snapshot state was lost in
    // recovery join them.
    let holding = settings.unplanned_snapshots == UnplannedSnapshots::Hold;
    let mut held: Vec<TableRef> = if holding {
        offset
            .held_tables
            .iter()
            .filter(|t| captured.contains(t))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    if lost_table_state && holding {
        let known: HashSet<&TableRef> = snapshotted
            .iter()
            .chain(progress.iter().flat_map(|p| p.tables.iter()))
            .collect();
        let unknown: Vec<TableRef> = captured
            .iter()
            .filter(|t| !known.contains(t) && !held.contains(t))
            .cloned()
            .collect();
        held.extend(unknown);
    }

    let covered: HashSet<&TableRef> = snapshotted
        .iter()
        .chain(progress.iter().flat_map(|p| p.tables.iter()))
        .chain(held.iter())
        .collect();
    let new_tables: Vec<TableRef> = captured
        .iter()
        .filter(|t| !covered.contains(t))
        .cloned()
        .collect();
    if !new_tables.is_empty() {
        let names = new_tables
            .iter()
            .map(|(db, table)| format!("{db}.{table}"))
            .collect::<Vec<_>>()
            .join(", ");
        if !settings.snapshot_new_tables {
            info!(
                message = "Some captured tables have never been snapshotted; only their changes from \
                           now on are captured. Set `snapshot_new_tables: true` to snapshot them.",
                tables = %names,
            );
        } else if !use_gtid {
            warn!(
                message = "Newly captured tables are not snapshotted: incremental snapshots need \
                           GTID mode.",
                tables = %names,
            );
        } else {
            info!(
                message = "Snapshotting newly captured tables incrementally.",
                tables = %names,
            );
            progress
                .get_or_insert_with(|| incremental::new_progress(Vec::new()))
                .tables
                .extend(new_tables);
        }
    }

    if !held.is_empty() {
        announce_held(
            &settings,
            &mut emitter,
            &offset,
            &snapshotted,
            progress.as_ref(),
            &held,
        )
        .await?;
    }

    let position = StreamPosition {
        file: offset.file,
        pos: offset.pos,
        gtid_set,
        use_gtid,
        snapshotted_tables: snapshotted,
        incremental: progress,
        held_tables: held,
        last_signal: offset.last_signal,
    };
    binlog::stream(&settings, &server, &mut emitter, position, shutdown).await?;
    emitter.close().await
}

/// Reports held snapshots: a warning, and a `snapshot_held` control event per table, committed
/// with the start offset (which records them in `held_tables`).
async fn announce_held(
    settings: &Settings,
    emitter: &mut Emitter,
    offset: &Offset,
    snapshotted: &[TableRef],
    progress: Option<&super::offset::IncrementalProgress>,
    held: &[TableRef],
) -> Result<(), String> {
    const REASON: &str = "the snapshot state of this table is unknown (the offset was recovered \
                          without it), and `unplanned_snapshots` is `hold`";
    let names = held
        .iter()
        .map(|(db, table)| format!("{db}.{table}"))
        .collect::<Vec<_>>()
        .join(", ");
    warn!(
        message = "Snapshots held: these captured tables may need a snapshot that nobody asked \
                   for. Their changes are streamed, but their existing rows are not read until \
                   they are removed from `held_tables` in the offset file: to snapshot them, or \
                   added to `snapshotted_tables` to mark them as complete.",
        tables = %names,
        internal_log_rate_limit = false,
    );
    for (database, table) in held {
        emitter.push_control(
            snapshot_held(&settings.topic_prefix, database, table, REASON),
            EventMeta {
                key: None,
                database,
                table: Some(table),
                op: "snapshot_held",
                timestamp: chrono::Utc::now(),
                schema_version: None,
            },
            true,
        );
    }
    emitter.commit(
        offset
            .clone()
            .with_tables(Some(snapshotted.to_vec()), progress.cloned())
            .with_held(held.to_vec()),
    );
    emitter.flush().await
}

/// `MySQL`'s emitter: the shared CDC emitter with binlog offsets.
pub(super) type Emitter = crate::sources::cdc::emitter::Emitter<Offset>;

/// Queues `MySQL` schema documents on the emitter.
pub(super) trait NoteTableSchema {
    /// Queues the schema document of `info` for the `table_schemas` output, the first time this
    /// process sees its schema version.
    fn note_table_schema(&mut self, info: &TableInfo, server_name: &str, observed: &Observed<'_>);
}

impl NoteTableSchema for Emitter {
    fn note_table_schema(&mut self, info: &TableInfo, server_name: &str, observed: &Observed<'_>) {
        self.note_schema_document(&info.database, &info.table, &info.schema_version, || {
            table_schema::document(
                &info.schema_core,
                &info.schema_version,
                server_name,
                observed,
            )
        });
    }
}
