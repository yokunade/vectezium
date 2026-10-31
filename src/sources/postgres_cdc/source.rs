//! Source orchestration: start-up decisions (slot, publication, snapshot or stream) and the row
//! building both phases share. Batching, `seq` numbering and acknowledgement-driven
//! checkpointing are the shared CDC core (`sources::cdc`).

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use chrono::{DateTime, Utc};
use pgrepl_tokio_postgres::Client;
use serde_json::{Map, Value as Json};
use vector_lib::config::LogNamespace;

use super::{
    EventFormat, EventProcessingFailureHandlingMode, PostgresCdcConfig, PublicationAutocreateMode,
    RecordImages, SnapshotMode, SnapshotStrategy, UnplannedSnapshots,
    catalog::{self, TableInfo, TypeRegistry, quote_ident},
    connection::{ConnectionSettings, error_text, simple_rows},
    convert::{self, ConvertOptions},
    envelope::{Op, SourceInfo},
    filter::Filters,
    offset::{CheckpointStore, IncrementalProgress, Lsn, Offset, TableRef},
    snapshot, stream, table_schema,
};
pub(super) use crate::sources::cdc::emitter::{EventBuilder, EventMeta};
use crate::{
    SourceSender,
    config::SourceContext,
    internal_events::{PostgresCdcConnectionError, PostgresCdcDecodeError},
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

/// Schemas that are never captured.
const SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema", "pg_toast"];

/// Version reported in `source.version`.
pub const CONNECTOR_VERSION: &str = concat!("vector-postgres_cdc-", env!("CARGO_PKG_VERSION"));

/// PostgreSQL's emitter: the shared CDC emitter with WAL offsets.
pub(super) type Emitter = crate::sources::cdc::emitter::Emitter<Offset>;

/// Static settings shared by the snapshot and streaming phases.
#[allow(
    clippy::struct_excessive_bools,
    reason = "settings derived from independent configuration options"
)]
pub(super) struct Settings {
    pub conn: ConnectionSettings,
    pub database: String,
    pub topic_prefix: String,
    pub slot_name: String,
    pub publication_name: String,
    pub autocreate: PublicationAutocreateMode,
    pub filters: Filters,
    pub convert: ConvertOptions,
    pub placeholder: String,
    pub include_schema_changes: bool,
    pub include_table_schemas: bool,
    pub observe_output: bool,
    pub snapshot_end_markers: bool,
    pub snapshot_mode: SnapshotMode,
    pub snapshot_strategy: SnapshotStrategy,
    pub snapshot_new_tables: bool,
    pub incremental_chunk_size: usize,
    pub unplanned_snapshots: UnplannedSnapshots,
    pub failure_mode: EventProcessingFailureHandlingMode,
    pub snapshot_batch_size: usize,
    pub snapshot_threads: usize,
    pub fetch_size: usize,
    pub event_format: EventFormat,
    pub record_images: RecordImages,
    pub publish_offsets: Option<Duration>,
    pub status_interval: Duration,
    pub signals: Option<SignalsConfig>,
    /// Tables already reported as having no key.
    pub warned_no_key: Mutex<HashSet<String>>,
}

impl Settings {
    /// Warns once per table that has neither a primary key nor `pk_override`.
    pub fn warn_if_keyless(&self, info: &TableInfo) {
        if !info.key.is_empty() {
            return;
        }
        let name = format!("{}.{}", info.schema, info.table);
        let first = self
            .warned_no_key
            .lock()
            .is_ok_and(|mut warned| warned.insert(name.clone()));
        if first {
            warn!(
                message = "Table has no primary key; its change events carry no key, so a downstream \
                           merge cannot identify rows. Add a primary key or set `pk_override.<schema>.<table>`.",
                table = %name,
                internal_log_rate_limit = false,
            );
        }
    }

    /// Applies `event_processing_failure_handling_mode` to a change that could not be converted.
    pub fn handle_failure(&self, error: String, schema: &str, table: &str) -> Result<(), String> {
        match self.failure_mode {
            EventProcessingFailureHandlingMode::Fail => {
                let message = format!("failed to convert a change in {schema}.{table}: {error}");
                emit!(PostgresCdcDecodeError {
                    error,
                    schema,
                    table
                });
                Err(message)
            }
            EventProcessingFailureHandlingMode::Warn => {
                emit!(PostgresCdcDecodeError {
                    error,
                    schema,
                    table
                });
                Ok(())
            }
            EventProcessingFailureHandlingMode::Skip => {
                debug!(message = "Skipping change that could not be converted.", %error, schema, table);
                Ok(())
            }
        }
    }

    /// The `source` block of a change in `info`.
    pub fn source_info<'a>(
        &'a self,
        info: &'a TableInfo,
        ts_us: i64,
        snapshot: super::envelope::SnapshotMarker,
        txid: Option<u32>,
        lsn: Option<Lsn>,
        last_commit_lsn: Option<Lsn>,
    ) -> SourceInfo<'a> {
        SourceInfo {
            connector_version: CONNECTOR_VERSION,
            name: &self.topic_prefix,
            ts_us,
            snapshot,
            database: &self.database,
            schema: &info.schema,
            table: &info.table,
            txid,
            lsn,
            last_commit_lsn,
        }
    }
}

/// Converts one row: `value(i)` gives column `i`'s text (`None` for SQL `NULL`). Excluded
/// columns are left out.
pub(super) fn row_map<'v>(
    settings: &Settings,
    info: &TableInfo,
    value: impl Fn(usize) -> Option<&'v str>,
) -> Result<Map<String, Json>, String> {
    let mut row = Map::with_capacity(info.columns.len());
    for (i, column) in info.columns.iter().enumerate() {
        if column.excluded {
            continue;
        }
        row.insert(
            column.name.clone(),
            convert::convert(column, value(i), &settings.convert)?,
        );
    }
    Ok(row)
}

/// The key of a row (its key columns), or `None` for a table without a key.
pub(super) fn row_key(info: &TableInfo, row: &Map<String, Json>) -> Option<Map<String, Json>> {
    if info.key.is_empty() {
        return None;
    }
    let mut key = Map::with_capacity(info.key.len());
    for &i in &info.key {
        let name = &info.columns[i].name;
        key.insert(name.clone(), row.get(name).cloned().unwrap_or(Json::Null));
    }
    Some(key)
}

/// Builds a row-change event.
#[allow(clippy::too_many_arguments)]
pub(super) fn row_event(
    builder: &EventBuilder,
    info: &TableInfo,
    op: Op,
    before: Option<Map<String, Json>>,
    after: Option<Map<String, Json>>,
    source: &SourceInfo<'_>,
    timestamp: DateTime<Utc>,
) -> crate::event::LogEvent {
    let key = match op {
        Op::Delete => before.as_ref().and_then(|row| row_key(info, row)),
        _ => after.as_ref().and_then(|row| row_key(info, row)),
    };
    builder.row_change(
        op,
        before,
        after,
        source,
        EventMeta {
            key,
            database: &info.schema,
            table: Some(&info.table),
            op: op.code(),
            timestamp,
            schema_version: Some(&info.schema_version),
        },
    )
}

/// Queues the schema document of `info`, the first time this process sees its version.
pub(super) fn note_table_schema(
    emitter: &mut Emitter,
    settings: &Settings,
    info: &TableInfo,
    origin: &'static str,
    lsn: Option<Lsn>,
) {
    emitter.note_schema_document(&info.schema, &info.table, &info.schema_version, || {
        table_schema::document(
            &info.schema_core,
            &info.schema_version,
            &settings.topic_prefix,
            &settings.database,
            origin,
            lsn,
        )
    });
}

#[allow(
    clippy::too_many_lines,
    reason = "validates the config and wires the source together"
)]
pub(super) fn build(config: &PostgresCdcConfig, cx: SourceContext) -> crate::Result<Source> {
    if config.database.is_empty() {
        return Err("`database` must name the database to capture".into());
    }
    if config.batch_size == 0 || config.snapshot_batch_size == Some(0) {
        return Err("`batch_size` and `snapshot_batch_size` must be greater than zero".into());
    }
    if config.snapshot_max_threads == 0 || config.snapshot_max_threads > 64 {
        return Err("`snapshot_max_threads` must be between 1 and 64".into());
    }
    let valid_name = |s: &str| {
        !s.is_empty()
            && s.len() <= 63
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    };
    if config.snapshot_strategy == SnapshotStrategy::Incremental
        && config.snapshot_mode == SnapshotMode::InitialOnly
    {
        return Err(
            "`snapshot_strategy: incremental` reads tables while streaming, so it cannot be \
                    combined with `snapshot_mode: initial_only`"
                .into(),
        );
    }
    if !valid_name(&config.slot_name) {
        return Err(
            "`slot_name` may only contain lower-case letters, digits and `_` (at most 63)".into(),
        );
    }

    let filters = Filters::new(
        &config.schema_include_list,
        &config.schema_exclude_list,
        &config.table_include_list,
        &config.table_exclude_list,
        &config.column_exclude_list,
    )
    .map_err(|e| e.replace("database_", "schema_"))?
    .with_system_databases(SYSTEM_SCHEMAS)
    .with_key_columns(key_columns(&config.pk_override)?);

    let data_dir = cx
        .globals
        .resolve_and_make_data_subdir(config.data_dir.as_ref(), cx.key.id())?;

    let settings = Settings {
        conn: ConnectionSettings::new(config)?,
        database: config.database.clone(),
        topic_prefix: config.topic_prefix.clone(),
        slot_name: config.slot_name.clone(),
        publication_name: config.publication_name.clone(),
        autocreate: config.publication_autocreate_mode,
        filters,
        convert: ConvertOptions {
            decimal: config.decimal_handling_mode,
            binary: config.binary_handling_mode,
            time_precision: config.time_precision_mode,
            interval: config.interval_handling_mode,
            json: config.json_handling_mode,
            money_fraction_digits: config.money_fraction_digits,
        },
        placeholder: config.unavailable_value_placeholder.clone(),
        include_schema_changes: config.include_schema_changes,
        include_table_schemas: config.include_table_schemas,
        observe_output: config.observe_output,
        snapshot_end_markers: config.snapshot_end_markers,
        snapshot_mode: config.snapshot_mode,
        snapshot_strategy: config.snapshot_strategy,
        snapshot_new_tables: config.snapshot_new_tables,
        incremental_chunk_size: config.incremental_snapshot_chunk_size.max(1),
        unplanned_snapshots: config.unplanned_snapshots,
        failure_mode: config.event_processing_failure_handling_mode,
        snapshot_batch_size: config.snapshot_batch_size.unwrap_or(config.batch_size),
        snapshot_threads: config.snapshot_max_threads,
        fetch_size: config.snapshot_fetch_size.max(1),
        event_format: config.event_format,
        record_images: config.record_images,
        publish_offsets: config
            .publish_offsets
            .then(|| Duration::from_secs(config.offset_publish_interval_secs.max(1))),
        status_interval: Duration::from_secs(config.status_update_interval_secs.max(1)),
        signals: config.signals.clone(),
        warned_no_key: Mutex::default(),
    };

    let acknowledgements = cx.do_acknowledgements(config.acknowledgements);
    let log_namespace = cx.log_namespace(config.log_namespace);
    check_acknowledgements(
        PostgresCdcConfig::NAME,
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
                       this database at a time (including during redeploys)."
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
                Arc::new(settings),
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

/// Flattens `pk_override` (`schema` → `table` → `"col1, col2"`) into `schema.table` → columns.
fn key_columns(
    overrides: &HashMap<String, HashMap<String, String>>,
) -> Result<HashMap<String, Vec<String>>, String> {
    let mut out = HashMap::new();
    for (schema, tables) in overrides {
        for (table, columns) in tables {
            let columns: Vec<String> = columns
                .split(',')
                .map(|c| c.trim().trim_matches('"').to_owned())
                .filter(|c| !c.is_empty())
                .collect();
            if columns.is_empty() {
                return Err(format!("`pk_override.{schema}.{table}` lists no columns"));
            }
            out.insert(format!("{schema}.{table}"), columns);
        }
    }
    Ok(out)
}

/// Connects, retrying every 5 seconds while the server is unavailable. `None` on shutdown.
pub(super) async fn connect_retrying(
    settings: &Settings,
    replication: bool,
    shutdown: &mut ShutdownSignal,
) -> Option<Client> {
    loop {
        match settings.conn.connect(replication).await {
            Ok(client) => return Some(client),
            Err(error) => {
                emit!(PostgresCdcConnectionError {
                    error,
                    endpoint: &settings.conn.endpoint,
                });
                tokio::select! {
                    _ = &mut *shutdown => return None,
                    () = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }
    }
}

/// Checks the server and returns its `system_identifier`.
async fn inspect_server(client: &Client, settings: &Settings) -> Result<String, String> {
    let row = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('wal_level'), \
                    current_setting('server_version')",
            &[],
        )
        .await
        .map_err(|error| format!("failed to read server settings: {}", error_text(&error)))?;
    let version_num: i32 = row.get(0);
    let wal_level: String = row.get(1);
    let version: String = row.get(2);
    if version_num < 130_000 {
        return Err(format!(
            "PostgreSQL 13 or newer is required (the server is {version})"
        ));
    }
    if wal_level != "logical" {
        return Err(format!(
            "`wal_level` must be `logical` (it is `{wal_level}`); set it and restart the server"
        ));
    }
    let repl = settings.conn.connect(true).await?;
    let ident = simple_rows(&repl, "IDENTIFY_SYSTEM")
        .await
        .map_err(|error| {
            format!(
                "IDENTIFY_SYSTEM failed (does the user have the REPLICATION attribute?): {}",
                error_text(&error)
            )
        })?;
    let system_id = ident
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .ok_or("IDENTIFY_SYSTEM returned no system identifier")?;
    info!(
        message = "Connected to PostgreSQL.",
        endpoint = %settings.conn.endpoint,
        version = %version,
        system_identifier = %system_id,
    );
    Ok(system_id)
}

/// Splits the tables a filtered publication is created for: those with a replica identity, and
/// those left out (`keyless`, where PostgreSQL would reject updates and deletes once published).
fn publishable(wanted: &[TableRef], keyless: &[TableRef]) -> (Vec<TableRef>, Vec<TableRef>) {
    wanted.iter().cloned().partition(|t| !keyless.contains(t))
}

/// `schema.table, …`, for messages.
fn names(tables: &[TableRef]) -> String {
    tables
        .iter()
        .map(|(s, t)| format!("{s}.{t}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Makes sure the publication exists (creating it per `publication_autocreate_mode`) and warns
/// about captured tables it does not include.
async fn ensure_publication(client: &Client, settings: &Settings) -> Result<(), String> {
    let name = &settings.publication_name;
    let exists = client
        .query_opt("SELECT 1 FROM pg_publication WHERE pubname = $1", &[name])
        .await
        .map_err(|error| format!("failed to read pg_publication: {}", error_text(&error)))?
        .is_some();
    let wanted = catalog::filtered_tables(client, &settings.filters).await?;
    if !exists {
        // Once a table is published, PostgreSQL rejects every UPDATE and DELETE on it unless it
        // has a replica identity: creating the publication must not break the application.
        let keyless = match settings.autocreate {
            PublicationAutocreateMode::Disabled => Vec::new(),
            _ => catalog::tables_without_identity(client).await?,
        };
        let (sql, tables) = match settings.autocreate {
            PublicationAutocreateMode::Disabled => {
                return Err(format!(
                    "publication `{name}` does not exist and `publication_autocreate_mode` is \
                     `disabled`: create it (CREATE PUBLICATION {name} FOR TABLE …)"
                ));
            }
            PublicationAutocreateMode::AllTables => {
                if !keyless.is_empty() {
                    return Err(format!(
                        "publication `{name}` does not exist, and creating it FOR ALL TABLES \
                         would make PostgreSQL reject every UPDATE and DELETE on tables without \
                         a replica identity: {}. Give them a primary key (or REPLICA IDENTITY \
                         USING INDEX / FULL), or use `publication_autocreate_mode: filtered`",
                        names(&keyless)
                    ));
                }
                (
                    format!("CREATE PUBLICATION {} FOR ALL TABLES", quote_ident(name)),
                    wanted.len(),
                )
            }
            PublicationAutocreateMode::Filtered => {
                let (published, left_out) = publishable(&wanted, &keyless);
                if !left_out.is_empty() {
                    warn!(
                        message = "Left tables without a replica identity out of the new publication: \
                                   once published, PostgreSQL would reject every UPDATE and DELETE \
                                   on them. Give them a primary key (or REPLICA IDENTITY USING INDEX / \
                                   FULL), then ALTER PUBLICATION … ADD TABLE.",
                        publication = %name,
                        tables = %names(&left_out),
                        internal_log_rate_limit = false,
                    );
                }
                if published.is_empty() {
                    return Err(format!(
                        "publication `{name}` does not exist and the include/exclude lists match \
                         no tables (with a replica identity) to create it for"
                    ));
                }
                let tables = published
                    .iter()
                    .map(|(s, t)| format!("{}.{}", quote_ident(s), quote_ident(t)))
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    format!(
                        "CREATE PUBLICATION {} FOR TABLE {tables}",
                        quote_ident(name)
                    ),
                    published.len(),
                )
            }
        };
        client.batch_execute(&sql).await.map_err(|error| {
            format!(
                "failed to create publication `{name}`: {}",
                error_text(&error)
            )
        })?;
        info!(message = "Created the publication.", publication = %name, tables);
        return Ok(());
    }
    let published: HashSet<TableRef> = catalog::captured_tables(client, name, &settings.filters)
        .await?
        .into_iter()
        .collect();
    let missing: Vec<String> = wanted
        .iter()
        .filter(|t| !published.contains(*t))
        .map(|(s, t)| format!("{s}.{t}"))
        .collect();
    if !missing.is_empty() {
        warn!(
            message = "Tables match the include lists but are not in the publication, so they are \
                       not captured. Add them with ALTER PUBLICATION … ADD TABLE.",
            publication = %name,
            tables = %missing.join(", "),
            internal_log_rate_limit = false,
        );
    }
    Ok(())
}

/// Warns about captured tables whose before images will be incomplete.
async fn check_replica_identity(client: &Client, settings: &Settings, captured: &[TableRef]) {
    let mut partial = Vec::new();
    for (schema, table) in captured {
        let identity = client
            .query_opt(
                "SELECT c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[schema, table],
            )
            .await
            .ok()
            .flatten()
            .map(|row| row.get::<_, String>(0));
        if identity.as_deref() != Some("f") {
            partial.push(format!("{schema}.{table}"));
        }
    }
    let wants_before = settings.event_format == EventFormat::Debezium
        || settings.record_images == RecordImages::BeforeAndAfter;
    if !partial.is_empty() && wants_before {
        warn!(
            message = "Tables without REPLICA IDENTITY FULL: their updates carry no before image, \
                       their deletes only the key, and unchanged large values the placeholder. \
                       Run ALTER TABLE … REPLICA IDENTITY FULL for complete images.",
            tables = %partial.join(", "),
            internal_log_rate_limit = false,
        );
    }
}

/// The state of the source's slot on the server.
#[derive(Debug, PartialEq)]
pub(super) enum SlotState {
    Missing,
    /// Invalidated (`wal_status = lost`, e.g. by `max_slot_wal_keep_size`).
    Lost,
    /// In use by another connection.
    Active(i32),
    Usable,
}

pub(super) async fn slot_state(client: &Client, settings: &Settings) -> Result<SlotState, String> {
    let row = client
        .query_opt(
            "SELECT coalesce(wal_status, ''), active_pid, plugin::text, database::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&settings.slot_name],
        )
        .await
        .map_err(|error| {
            format!(
                "failed to read pg_replication_slots: {}",
                error_text(&error)
            )
        })?;
    let Some(row) = row else {
        return Ok(SlotState::Missing);
    };
    let (wal_status, active, plugin, database): (
        String,
        Option<i32>,
        Option<String>,
        Option<String>,
    ) = (row.get(0), row.get(1), row.get(2), row.get(3));
    if plugin.as_deref() != Some("pgoutput")
        || database.as_deref() != Some(settings.database.as_str())
    {
        return Err(format!(
            "replication slot `{}` exists but is not a pgoutput slot of database `{}`; choose \
             another `slot_name`",
            settings.slot_name, settings.database
        ));
    }
    Ok(match (wal_status.as_str(), active) {
        ("lost", _) => SlotState::Lost,
        (_, Some(pid)) => SlotState::Active(pid),
        _ => SlotState::Usable,
    })
}

/// Drops the source's slot (to create it again with a fresh snapshot).
pub(super) async fn drop_slot(client: &Client, settings: &Settings) -> Result<(), String> {
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&settings.slot_name],
        )
        .await
        .map_err(|error| format!("failed to drop replication slot `{}`: {}", settings.slot_name, error_text(&error)))?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the source's lifecycle: recover, snapshot, stream"
)]
async fn run(
    settings: Arc<Settings>,
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
    let offset_file = store.path().display().to_string();
    let mut stored = store
        .load()
        .await
        .map_err(|error| format!("failed to load offsets from {offset_file}: {error}"))?;

    // No stored offset: resume from the published one if the lake has it (`offset_recovery`).
    if stored.is_none()
        && let Some(recovery) = &recovery
    {
        let Some(recovered) = recovery
            .recover(&settings.topic_prefix, &mut shutdown)
            .await?
        else {
            return Ok(());
        };
        if let Recovered::Published(payload) = recovered {
            let checkpoint = payload
                .get("checkpoint")
                .ok_or("the published offset has no `checkpoint`; cannot resume from it")?;
            let offset: Offset = serde_json::from_value(checkpoint.clone()).map_err(|error| {
                format!("the published offset's `checkpoint` is not a valid offset: {error}")
            })?;
            info!(
                message = "No stored offset; resuming from the offset published in the lake.",
                offset = %offset,
                next_seq = offset.next_seq,
            );
            store.store(&offset).await.map_err(|error| {
                format!("failed to store the recovered offset in {offset_file}: {error}")
            })?;
            stored = Some(offset);
        } else {
            info!(
                message = "No stored offset and the connector's folder in the lake is empty; starting from scratch."
            );
        }
    }

    let Some(query) = connect_retrying(&settings, false, &mut shutdown).await else {
        return Ok(());
    };
    let system_id = inspect_server(&query, &settings).await?;
    let mut registry = TypeRegistry::load(&query).await?;
    ensure_publication(&query, &settings).await?;
    let captured =
        catalog::captured_tables(&query, &settings.publication_name, &settings.filters).await?;
    if captured.is_empty() {
        warn!(
            message = "No captured tables: the publication streams no table the include lists match.",
            publication = %settings.publication_name,
        );
    }
    check_replica_identity(&query, &settings, &captured).await;

    let poisoned = Arc::new(AtomicBool::new(false));
    let builder = EventBuilder::new(
        PostgresCdcConfig::NAME,
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
    let (released_tx, released_rx) = tokio::sync::watch::channel(None);
    let checkpoints = Checkpoints::start_notifying(
        acknowledgements,
        store,
        Arc::clone(&poisoned),
        publisher,
        guard,
        written,
        Some(released_tx),
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
    emitter.set_next_seq(
        stored.as_ref().map_or(1, |offset| offset.next_seq),
        stored.as_ref().map_or(0, |offset| offset.seq_sub),
    );

    // Decide where to start.
    // A slot still held by the previous run's connection is released within moments; one held
    // for longer belongs to someone else.
    let mut slot = slot_state(&query, &settings).await?;
    for _ in 0..12 {
        if !matches!(slot, SlotState::Active(_)) {
            break;
        }
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
        slot = slot_state(&query, &settings).await?;
    }
    if let SlotState::Active(pid) = slot {
        return Err(format!(
            "replication slot `{}` is in use by another connection (pid {pid}); is another \
             instance capturing this database?",
            settings.slot_name
        ));
    }
    let resume = match &stored {
        Some(offset) if offset.snapshot_completed => {
            if let Some(id) = &offset.system_identifier
                && id != &system_id
            {
                return Err(format!(
                    "the stored offset {offset} belongs to the server with system identifier {id}, \
                     but the source is now connected to {system_id}; slots and LSNs do not move \
                     between servers. Remove {offset_file} to start over"
                ));
            }
            if offset.slot_name != settings.slot_name {
                return Err(format!(
                    "the stored offset {offset} is for slot `{}`, but `slot_name` is `{}`; \
                     remove {offset_file} to start over with the new slot",
                    offset.slot_name, settings.slot_name
                ));
            }
            if slot == SlotState::Usable {
                Some(offset.clone())
            } else {
                let why = if slot == SlotState::Lost {
                    "was invalidated (it fell more than `max_slot_wal_keep_size` behind)"
                } else {
                    "no longer exists"
                };
                if settings.snapshot_mode != SnapshotMode::WhenNeeded {
                    return Err(format!(
                        "replication slot `{}` {why}, so changes after {offset} are lost. Set \
                         `snapshot_mode: when_needed` to re-snapshot",
                        settings.slot_name
                    ));
                }
                if settings.unplanned_snapshots == UnplannedSnapshots::Hold
                    && !offset.resnapshot_approved
                {
                    let reason = format!(
                        "replication slot `{}` {why}, so every captured table would be \
                         snapshotted again",
                        settings.slot_name
                    );
                    let Some(signals) = &settings.signals else {
                        return Err(format!(
                            "{reason}. `unplanned_snapshots` is `hold`: approve the re-snapshot \
                             by setting `\"resnapshot_approved\": true` in {offset_file} while \
                             the source is stopped, or set `unplanned_snapshots: allow`"
                        ));
                    };
                    if !signal::await_approval(signals, &reason, &mut shutdown).await? {
                        return emitter.close().await;
                    }
                }
                warn!(message = "The replication slot was lost; re-snapshotting.", slot = %settings.slot_name, %why);
                None
            }
        }
        Some(_) => {
            info!(message = "The previous snapshot did not complete; snapshotting again.");
            None
        }
        None => None,
    };

    let start = if let Some(offset) = resume {
        offset
    } else {
        // Starting over: a leftover slot cannot give a snapshot, so it is replaced.
        if slot != SlotState::Missing {
            info!(message = "Dropping the replication slot to create it again with a snapshot.", slot = %settings.slot_name);
            drop_slot(&query, &settings).await?;
        }
        let snapshotting = stored.is_some() || settings.snapshot_mode != SnapshotMode::NoData;
        if snapshotting && settings.snapshot_strategy == SnapshotStrategy::Blocking {
            let completed = snapshot::run(
                Arc::clone(&settings),
                &query,
                &registry,
                &mut emitter,
                &captured,
                &system_id,
                shutdown.clone(),
            )
            .await?;
            let Some(offset) = completed else {
                // Shut down mid-snapshot; it is taken again on the next start.
                return emitter.close().await;
            };
            if settings.snapshot_mode == SnapshotMode::InitialOnly {
                info!(
                    message =
                        "Snapshot complete; `snapshot_mode` is `initial_only`, so not streaming."
                );
                return emitter.close().await;
            }
            offset
        } else {
            // An incremental snapshot (read while streaming), or `no_data` (existing rows are
            // deliberately not captured): stream from the slot's position right away.
            let repl = settings.conn.connect(true).await?;
            let created = simple_rows(
                &repl,
                &format!(
                    "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
                    quote_ident(&settings.slot_name)
                ),
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to create replication slot `{}`: {}",
                    settings.slot_name,
                    error_text(&error)
                )
            })?;
            let lsn: Lsn = created
                .first()
                .and_then(|row| row.get(1).cloned().flatten())
                .ok_or("CREATE_REPLICATION_SLOT returned no position")?
                .parse()?;
            let mut offset = Offset::new(lsn, &settings.slot_name, Some(system_id.clone()));
            offset.snapshot_completed = true;
            if snapshotting {
                info!(message = "Created the replication slot; snapshotting tables incrementally while streaming.", offset = %offset, tables = captured.len());
                offset.incremental =
                    (!captured.is_empty()).then(|| IncrementalProgress::new(captured.clone()));
            } else {
                info!(message = "Created the replication slot; streaming without a snapshot.", offset = %offset);
                offset.snapshotted_tables = captured.clone();
            }
            emitter.commit(offset.clone());
            emitter.flush().await?;
            offset
        }
    };

    // Captured tables that were never snapshotted: added to the publication or the include
    // lists since, or removed from `snapshotted_tables` by an operator (an ad-hoc snapshot).
    let mut start = start;
    let queued: HashSet<&TableRef> = start
        .snapshotted_tables
        .iter()
        .chain(start.incremental.iter().flat_map(|p| p.tables.iter()))
        .collect();
    let new_tables: Vec<TableRef> = captured
        .iter()
        .filter(|t| !queued.contains(t))
        .cloned()
        .collect();
    if !new_tables.is_empty() {
        let names = new_tables
            .iter()
            .map(|(s, t)| format!("{s}.{t}"))
            .collect::<Vec<_>>()
            .join(", ");
        if settings.snapshot_new_tables {
            info!(message = "Snapshotting captured tables that were never snapshotted, incrementally.", tables = %names);
            start
                .incremental
                .get_or_insert_with(|| IncrementalProgress::new(Vec::new()))
                .tables
                .extend(new_tables);
            emitter.commit(start.clone());
            emitter.flush().await?;
        } else {
            info!(
                message = "Some captured tables have never been snapshotted; only their changes from \
                           now on are captured. Set `snapshot_new_tables: true` to snapshot them.",
                tables = %names,
            );
        }
    }

    stream::stream(
        &settings,
        query,
        &mut registry,
        &mut emitter,
        start,
        &captured,
        released_rx,
        shutdown,
    )
    .await?;
    emitter.close().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_publication_leaves_out_tables_without_a_replica_identity() {
        let t = |name: &str| ("public".to_owned(), name.to_owned());
        let wanted = [t("customers"), t("events"), t("orders")];
        let keyless = [t("events"), t("audit")];
        let (published, left_out) = publishable(&wanted, &keyless);
        assert_eq!(published, [t("customers"), t("orders")]);
        assert_eq!(left_out, [t("events")]);
        assert_eq!(names(&left_out), "public.events");
    }
}
