//! Binlog streaming: registers as a replica, decodes row events into Debezium change events and
//! tracks transaction boundaries so offsets only ever point between transactions.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use mysql_async::{
    BinlogStreamRequest,
    binlog::{
        events::{Event, EventData, RowsEventData},
        row::BinlogRow,
    },
    prelude::Queryable,
};
use serde_json::{Map, Value as Json};

use super::{
    convert::convert_binlog_value,
    ddl::{self, DdlKind},
    envelope::{Op, SnapshotMarker, SourceInfo},
    incremental::{Incremental, StepError},
    offset::{GtidSet, IncrementalProgress, Offset, TableRef},
    schema::TableInfo,
    source::{
        CONNECTOR_VERSION, Emitter, EventMeta, NoteTableSchema, ServerInfo, Settings, connect,
    },
    table_schema::Observed,
};
use crate::{
    internal_events::MysqlCdcConnectionError,
    shutdown::ShutdownSignal,
    sources::cdc::signal::{self, SnapshotQueue, Spool},
};

/// How long the stream may be quiet before pending events are flushed.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Reconnect backoff bounds.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Server error: the requested binlog position can no longer be read (usually purged binlog).
const ER_MASTER_FATAL_ERROR_READING_BINLOG: u16 = 1236;

/// The committed streaming position: the next event to read starts a new transaction.
#[derive(Clone, Debug)]
pub(super) struct StreamPosition {
    pub file: String,
    pub pos: u64,
    pub gtid_set: GtidSet,
    pub use_gtid: bool,
    /// Tables whose existing rows are covered.
    pub snapshotted_tables: Vec<TableRef>,
    /// An incremental snapshot to continue (or start).
    pub incremental: Option<IncrementalProgress>,
    /// Tables whose (unplanned) snapshot is held: streamed, not snapshotted.
    pub held_tables: Vec<TableRef>,
    /// The last signal applied.
    pub last_signal: Option<String>,
}

enum StreamError {
    /// Transient (network, server restart, …): reconnect and resume.
    Retry(String),
    /// Unrecoverable: stop the source.
    Fatal(String),
}

impl From<mysql_async::Error> for StreamError {
    fn from(error: mysql_async::Error) -> Self {
        match &error {
            mysql_async::Error::Server(server)
                if server.code == ER_MASTER_FATAL_ERROR_READING_BINLOG =>
            {
                StreamError::Fatal(format!(
                    "the server cannot provide the binlog from the stored position ({error}). The \
                     binlog was probably purged; set `snapshot_mode: when_needed` to re-snapshot"
                ))
            }
            _ => StreamError::Retry(error.to_string()),
        }
    }
}

/// Streams the binlog until shutdown, reconnecting on transient errors.
pub(super) async fn stream(
    settings: &Settings,
    server: &ServerInfo,
    emitter: &mut Emitter,
    position: StreamPosition,
    mut shutdown: ShutdownSignal,
) -> Result<(), String> {
    let mut spool = settings
        .signals
        .as_ref()
        .map(|config| Spool::open(config, position.last_signal.as_deref()))
        .transpose()?;
    let mut handler = Handler::new(settings, server, position);
    let mut backoff = MIN_BACKOFF;

    loop {
        let started = tokio::time::Instant::now();
        match run_connection(
            settings,
            emitter,
            &mut handler,
            spool.as_mut(),
            &mut shutdown,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(StreamError::Fatal(error)) => return Err(error),
            Err(StreamError::Retry(error)) => {
                emit!(MysqlCdcConnectionError {
                    error,
                    endpoint: &settings.endpoint,
                });
                // Events of the interrupted transaction are re-read after reconnecting.
                emitter.discard_uncommitted();
                emitter.flush().await?;
                handler.reset_connection_state();

                if started.elapsed() > Duration::from_mins(1) {
                    backoff = MIN_BACKOFF;
                }
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn run_connection(
    settings: &Settings,
    emitter: &mut Emitter,
    handler: &mut Handler<'_>,
    mut spool: Option<&mut Spool>,
    shutdown: &mut ShutdownSignal,
) -> Result<(), StreamError> {
    let mut conn = connect(settings).await.map_err(StreamError::Retry)?;

    // Ask the server to send heartbeats on an idle stream (both names: MySQL 8.0.26 renamed it).
    let heartbeat_ns = settings.heartbeat.as_nanos();
    conn.query_drop(format!(
        "SET @master_heartbeat_period = {heartbeat_ns}, @source_heartbeat_period = {heartbeat_ns}"
    ))
    .await?;
    // The pipeline may apply backpressure for a while; don't let the server drop us.
    conn.query_drop("SET SESSION net_write_timeout = 3600, SESSION wait_timeout = 86400")
        .await?;

    let position = handler.position.clone();
    let stream = if position.use_gtid {
        let sids = position.gtid_set.to_sids().map_err(StreamError::Fatal)?;
        let request = BinlogStreamRequest::new(settings.server_id)
            .with_gtid()
            .with_gtid_set(sids);
        conn.get_binlog_stream(request).await?
    } else {
        let request = BinlogStreamRequest::new(settings.server_id)
            .with_filename(position.file.as_bytes())
            .with_pos(position.pos);
        conn.get_binlog_stream(request).await?
    };
    let mut stream = stream;
    debug!(message = "Binlog stream opened.", file = %position.file, pos = position.pos);

    let idle_limit = settings.heartbeat * 2 + Duration::from_secs(5);
    let mut last_event_at = tokio::time::Instant::now();

    loop {
        emitter.check_health().map_err(StreamError::Fatal)?;

        let next = tokio::select! {
            biased;
            _ = &mut *shutdown => None,
            next = tokio::time::timeout(POLL_INTERVAL, stream.next()) => Some(next),
        };
        let Some(next) = next else {
            info!(message = "Shutting down binlog stream.");
            emitter.discard_uncommitted();
            emitter.flush().await.map_err(StreamError::Fatal)?;
            _ = stream.close().await;
            return Ok(());
        };

        match next {
            Err(_elapsed) => {
                if last_event_at.elapsed() > idle_limit {
                    return Err(StreamError::Retry(format!(
                        "no binlog events or heartbeats received for {:?}",
                        last_event_at.elapsed()
                    )));
                }
            }
            Ok(None) => {
                return Err(StreamError::Retry(
                    "the server closed the binlog stream".into(),
                ));
            }
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(Some(Ok(event))) => {
                last_event_at = tokio::time::Instant::now();
                handler
                    .handle(&event, emitter)
                    .map_err(StreamError::Fatal)?;
            }
        }

        if handler.incremental.has_work() && handler.between_transactions() {
            let position = handler.position.gtid_set.clone();
            let changed = handler
                .incremental
                .step(settings, &position, emitter)
                .await
                .map_err(|error| match error {
                    StepError::Retry(error) => StreamError::Retry(error),
                    StepError::Fatal(error) => StreamError::Fatal(error),
                })?;
            if changed {
                handler.commit_progress(emitter);
            }
        }

        if let Some(spool) = spool.as_deref_mut()
            && handler.between_transactions()
        {
            let done = signal::apply_due(spool, handler);
            if !done.is_empty() {
                handler.last_signal = signal::last_signal(&done);
                handler.commit_progress(emitter);
                emitter.flush().await.map_err(StreamError::Fatal)?;
                for (received, outcome) in &done {
                    spool.finish(received, outcome);
                }
            }
        }

        // An open XA transaction's rows may still be dropped at its `XA_prepare` event, so they
        // are held back until then rather than flushed part-way.
        if handler.tx.xa.is_none() {
            emitter.maybe_flush().await.map_err(StreamError::Fatal)?;
        }
    }
}

/// State of the transaction currently being read.
#[derive(Debug, Default)]
struct Transaction {
    /// Inside a `BEGIN` … `COMMIT`/`XID` block.
    in_tx: bool,
    /// `(source id, gno)` from the GTID event, if GTIDs are on.
    gtid: Option<(String, u64)>,
    /// `uuid[:tag]:gno`, for `source.gtid`.
    gtid_text: Option<String>,
    /// Commit time on the originating server, in microseconds.
    commit_ts_us: Option<i64>,
    /// SQL of the current statement (`binlog_rows_query_log_events=ON`).
    query: Option<String>,
    /// Inside `XA START` … `XA PREPARE`: the XA transaction id, as the binlog spells it.
    xa: Option<String>,
    /// Captured changes of the XA transaction, `(database, table, key)`: they reach the
    /// incremental snapshot only if the transaction commits in one phase.
    xa_changes: Vec<(String, String, Option<Map<String, Json>>)>,
}

struct Handler<'a> {
    settings: &'a Settings,
    server_uuid: String,
    /// Committed position (between transactions).
    position: StreamPosition,
    /// Binlog file currently being read.
    file: String,
    /// End position of the last event that carried one.
    last_end_pos: u64,
    /// Start position of the last event that carried one.
    last_start_pos: u64,
    /// Table id → table description (`None` when the table is not captured).
    tables: HashMap<u64, Option<Arc<TableInfo>>>,
    tx: Transaction,
    incremental: Incremental,
    /// Tables whose snapshot is held; kept in every offset until an operator releases them.
    held: Vec<TableRef>,
    /// The last signal applied; kept in every offset.
    last_signal: Option<String>,
    /// A compressed transaction payload was read and none of its events have followed yet.
    payload_unread: bool,
}

impl<'a> Handler<'a> {
    fn new(settings: &'a Settings, server: &ServerInfo, mut position: StreamPosition) -> Self {
        let incremental = Incremental::new(
            std::mem::take(&mut position.snapshotted_tables),
            position.incremental.take(),
        );
        Self {
            incremental,
            held: std::mem::take(&mut position.held_tables),
            last_signal: position.last_signal.take(),
            settings,
            server_uuid: server.server_uuid.clone(),
            file: position.file.clone(),
            last_end_pos: position.pos,
            last_start_pos: position.pos,
            position,
            tables: HashMap::new(),
            tx: Transaction::default(),
            payload_unread: false,
        }
    }

    /// Forgets per-connection state; the server re-sends table maps after reconnecting.
    fn reset_connection_state(&mut self) {
        self.tables.clear();
        self.incremental.reset_pending();
        self.tx = Transaction::default();
        self.payload_unread = false;
        self.file = self.position.file.clone();
        self.last_end_pos = self.position.pos;
        self.last_start_pos = self.position.pos;
    }

    #[allow(clippy::too_many_lines, reason = "one arm per binlog event type")]
    fn handle(&mut self, event: &Event, emitter: &mut Emitter) -> Result<(), String> {
        let header = event.header();
        let log_pos = u64::from(header.log_pos());
        // mysql_async expands a compressed transaction payload into its events, but if the
        // payload cannot be parsed or decompressed it yields none, silently. An event with a
        // position right after a payload means its events never came.
        if self.payload_unread {
            self.payload_unread = false;
            if log_pos != 0 {
                self.settings.handle_failure(
                    format!(
                        "a compressed transaction payload in {} before position {log_pos} could \
                         not be decompressed; its changes are missing. Re-snapshot the affected \
                         tables",
                        self.file
                    ),
                    "",
                    "",
                )?;
            }
        }
        // Events inside a compressed transaction payload carry no position; they inherit the
        // payload event's.
        if log_pos != 0 {
            self.last_end_pos = log_pos;
            self.last_start_pos = log_pos.saturating_sub(u64::from(header.event_size()));
        }
        let header_ts_us = i64::from(header.timestamp()) * 1_000_000;

        let data = match event.read_data() {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(()),
            Err(error) => {
                return self.settings.handle_failure(
                    format!("undecodable binlog event: {error}"),
                    "",
                    "",
                );
            }
        };

        match data {
            EventData::RotateEvent(rotate) => {
                self.file = rotate.name().into_owned();
                self.last_end_pos = rotate.position();
                self.last_start_pos = rotate.position();
                if !self.tx.in_tx && self.tx.gtid.is_none() {
                    // Between transactions: the new file's start is a valid resume point.
                    self.position.file = self.file.clone();
                    self.position.pos = rotate.position();
                }
            }
            EventData::GtidEvent(gtid) => {
                let uuid = uuid::Uuid::from_bytes(gtid.sid()).hyphenated().to_string();
                let source_id = match gtid.tag() {
                    Some(tag) => format!("{uuid}:{}", tag.as_str()),
                    None => uuid,
                };
                self.tx = Transaction {
                    gtid_text: Some(format!("{source_id}:{}", gtid.gno())),
                    gtid: Some((source_id, gtid.gno())),
                    commit_ts_us: match gtid.original_commit_timestamp() {
                        0 => None,
                        ts => i64::try_from(ts).ok(),
                    },
                    ..Transaction::default()
                };
            }
            EventData::AnonymousGtidEvent(_) => {
                self.tx = Transaction::default();
            }
            EventData::QueryEvent(query) => {
                let sql = query.query();
                let statement = sql.trim();
                if statement.eq_ignore_ascii_case("BEGIN") {
                    // Transaction start; GTID info (if any) was set by the GTID event.
                    self.tx.in_tx = true;
                } else if statement.eq_ignore_ascii_case("COMMIT")
                    || statement.eq_ignore_ascii_case("ROLLBACK")
                {
                    // End of a transaction on a non-transactional engine (a ROLLBACK here still
                    // means the non-transactional row changes were applied).
                    self.commit(emitter);
                } else if let Some(xid) = strip_prefix_ignore_case(statement, "XA START ") {
                    // An XA transaction's rows follow, up to `XA END` and an `XA_prepare` event.
                    self.tx.in_tx = true;
                    self.tx.xa = Some(xid.trim().to_owned());
                } else if starts_with_ignore_case(statement, "XA COMMIT ")
                    || starts_with_ignore_case(statement, "XA ROLLBACK ")
                {
                    // The second phase of a prepared XA transaction: its own GTID, no rows.
                    self.commit(emitter);
                } else if starts_with_ignore_case(statement, "XA ") {
                    // `XA END`: the rows are over; the `XA_prepare` event ends the group.
                } else if self.tx.in_tx {
                    // `SAVEPOINT`, `ROLLBACK TO SAVEPOINT` and similar statements carry no row
                    // data. A write here was logged in statement format: its rows are not in
                    // the binlog.
                    let database = query.schema().into_owned();
                    self.statement_write(&database, statement)?;
                } else {
                    let database = query.schema().into_owned();
                    if ddl::dml_target(statement).is_some() {
                        self.statement_write(&database, statement)?;
                    } else {
                        self.schema_change(
                            &database,
                            statement,
                            header.server_id(),
                            header_ts_us,
                            emitter,
                        );
                    }
                    self.commit(emitter);
                }
            }
            EventData::IncidentEvent(incident) => {
                // The server knows events are missing (e.g. a failed binlog cache write); a MySQL
                // replica stops here too.
                self.settings.handle_failure(
                    format!(
                        "the binlog contains an incident event ({incident:?}); changes may be \
                         missing from the binlog. Re-snapshot the affected tables"
                    ),
                    "",
                    "",
                )?;
            }
            EventData::TableMapEvent(tme) => {
                let table_id = tme.table_id();
                let database = tme.database_name().into_owned();
                let table = tme.table_name().into_owned();
                if self.settings.filters.table_included(&database, &table) {
                    // Table maps repeat for every transaction; reuse the description (and its
                    // schema version) while the definition is unchanged.
                    let unchanged = self.tables.get(&table_id).is_some_and(|known| {
                        known
                            .as_ref()
                            .and_then(|info| info.table_map.as_ref())
                            .is_some_and(|known_tme| tme == *known_tme)
                    });
                    if !unchanged {
                        let info = TableInfo::from_table_map(
                            tme.into_owned(),
                            &self.settings.filters,
                            self.settings.convert,
                        )
                        .map_err(|error| {
                            format!("invalid table map for `{database}`.`{table}`: {error}")
                        })?;
                        self.settings.warn_if_keyless(&info);
                        self.tables.insert(table_id, Some(Arc::new(info)));
                    }
                } else {
                    self.tables.insert(table_id, None);
                }
            }
            EventData::RowsQueryEvent(rows_query) => {
                if self.settings.include_query {
                    self.tx.query = Some(rows_query.query().into_owned());
                }
            }
            EventData::RowsEvent(rows) => {
                self.rows(&rows, header.server_id(), header_ts_us, emitter)?;
            }
            EventData::XidEvent(_) => self.commit(emitter),
            EventData::XaPrepareLogEvent(data) => self.xa_prepare(&data, emitter)?,
            // Its events follow, expanded by mysql_async.
            EventData::TransactionPayloadEvent(_) => self.payload_unread = true,
            // Format description, heartbeats, previous-GTIDs and other bookkeeping events need
            // no action.
            _ => {}
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the three row-event kinds share their bookkeeping"
    )]
    fn rows(
        &mut self,
        rows: &RowsEventData<'_>,
        server_id: u32,
        header_ts_us: i64,
        emitter: &mut Emitter,
    ) -> Result<(), String> {
        let Some(Some(info)) = self.tables.get(&rows.table_id()).cloned() else {
            // Not captured (or a table map we never saw, which MySQL does not produce).
            return Ok(());
        };
        let Some(tme) = info.table_map.as_ref() else {
            return Ok(());
        };

        let op = match rows {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => Op::Create,
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => Op::Delete,
            RowsEventData::UpdateRowsEvent(_)
            | RowsEventData::UpdateRowsEventV1(_)
            | RowsEventData::PartialUpdateRowsEvent(_) => Op::Update,
        };
        let ts_us = self.tx.commit_ts_us.unwrap_or(header_ts_us);

        emitter.note_table_schema(
            &info,
            &self.settings.topic_prefix,
            &Observed {
                origin: "binlog",
                file: &self.file,
                pos: self.last_start_pos,
                gtid: self.tx.gtid_text.as_deref(),
            },
        );

        for (row_index, row) in rows.rows(tme).enumerate() {
            let images = row
                .map_err(|error| error.to_string())
                .and_then(|(before, after)| {
                    let before = before.map(|r| self.row_image(&info, &r)).transpose()?;
                    let after = after.map(|r| self.row_image(&info, &r)).transpose()?;
                    Ok((before, after))
                });
            let (before, after) = match images {
                Ok(images) => images,
                Err(error) => {
                    self.settings
                        .handle_failure(error, &info.database, &info.table)?;
                    continue;
                }
            };
            if self.tx.xa.is_some() {
                // Whether an XA transaction commits is known only at its `XA_prepare` event.
                for image in [&before, &after].into_iter().flatten() {
                    let key = image.1.clone();
                    self.tx
                        .xa_changes
                        .push((info.database.clone(), info.table.clone(), key));
                }
            } else if self.incremental.has_work() {
                let gtid = self.tx.gtid.clone();
                let gtid = gtid.as_ref().map(|(sid, gno)| (sid.as_str(), *gno));
                for image in [&before, &after].into_iter().flatten() {
                    self.incremental
                        .on_change(&info.database, &info.table, image.1.as_ref(), gtid);
                }
            }

            let source = SourceInfo {
                connector_version: CONNECTOR_VERSION,
                name: &self.settings.topic_prefix,
                ts_us,
                snapshot: SnapshotMarker::False,
                db: &info.database,
                table: &info.table,
                server_id,
                gtid: self.tx.gtid_text.as_deref(),
                file: &self.file,
                pos: self.last_start_pos,
                row: row_index,
                query: self.tx.query.as_deref(),
            };

            match (op, before, after) {
                (Op::Create, _, Some((after, key))) => {
                    emit(emitter, &info, &source, Op::Create, None, Some(after), key);
                }
                (Op::Delete, Some((before, key)), _) => {
                    emit(emitter, &info, &source, Op::Delete, Some(before), None, key);
                }
                (Op::Update, Some((before, before_key)), Some((after, after_key))) => {
                    if info.has_primary_key() && before_key != after_key {
                        // Like Debezium: a primary-key change becomes a delete plus a create.
                        emit(
                            emitter,
                            &info,
                            &source,
                            Op::Delete,
                            Some(before),
                            None,
                            before_key,
                        );
                        emit(
                            emitter,
                            &info,
                            &source,
                            Op::Create,
                            None,
                            Some(after),
                            after_key,
                        );
                    } else {
                        emit(
                            emitter,
                            &info,
                            &source,
                            Op::Update,
                            Some(before),
                            Some(after),
                            after_key,
                        );
                    }
                }
                (op, before, after) => {
                    warn!(
                        message = "Row event without the expected images; skipping.",
                        op = op.code(),
                        has_before = before.is_some(),
                        has_after = after.is_some(),
                        table = %format!("{}.{}", info.database, info.table),
                    );
                }
            }
        }
        Ok(())
    }

    /// Converts one row image into column values and the record key.
    fn row_image(
        &self,
        info: &TableInfo,
        row: &BinlogRow,
    ) -> Result<(Map<String, Json>, Option<Map<String, Json>>), String> {
        let mut values = Map::with_capacity(row.len());
        let mut key = Map::new();
        for (index, column) in row.columns_ref().iter().enumerate() {
            let name = column.name_str();
            let Some(desc) = info.column_index(&name).map(|i| &info.columns[i]) else {
                return Err(format!("column `{name}` is not in the table map"));
            };
            if desc.excluded {
                continue;
            }
            let Some(value) = row.as_ref(index) else {
                continue;
            };
            let json = convert_binlog_value(desc, value, self.settings.convert)
                .map_err(|error| error.to_string())?;
            if desc.primary_key {
                key.insert(desc.name.clone(), json.clone());
            }
            values.insert(desc.name.clone(), json);
        }
        // With `binlog_row_image` MINIMAL or NOBLOB (set by a session) images hold only some
        // columns; emitting them as rows would blank out real values downstream.
        let expected = info.columns.iter().filter(|c| !c.excluded).count();
        if values.len() < expected {
            return Err(format!(
                "incomplete row image ({} of {expected} columns): a session set \
                 `binlog_row_image` to MINIMAL or NOBLOB. Keep it FULL for captured tables, then \
                 re-snapshot the table",
                values.len()
            ));
        }
        Ok((values, (!key.is_empty()).then_some(key)))
    }

    /// Handles a write logged in statement format (its rows are not in the binlog): a failure
    /// unless the statement provably writes to a table that is not captured.
    fn statement_write(&self, default_db: &str, statement: &str) -> Result<(), String> {
        let Some(target) = ddl::dml_target(statement) else {
            return Ok(());
        };
        let (database, table) = match &target {
            Some(t) => (
                t.db.clone().unwrap_or_else(|| default_db.to_owned()),
                t.name.clone().unwrap_or_default(),
            ),
            None => (default_db.to_owned(), String::new()),
        };
        if target.is_some() && !self.settings.filters.table_included(&database, &table) {
            return Ok(());
        }
        let head: String = statement.chars().take(200).collect();
        self.settings.handle_failure(
            format!(
                "a write was logged in statement format, so its row changes are not in the binlog \
                 and cannot be captured (statement: {head}). A session changed `binlog_format`; keep \
                 it ROW for captured tables, then re-snapshot them"
            ),
            &database,
            &table,
        )
    }

    /// Emits a schema statement on the `schema_changes` output when it targets a captured
    /// database or table, and a truncate (`t`) change for each captured table it truncates.
    /// Statements that change no schema (accounts, privileges, `FLUSH`, `ANALYZE`, ...) are
    /// ignored.
    fn schema_change(
        &mut self,
        default_db: &str,
        statement: &str,
        server_id: u32,
        header_ts_us: i64,
        emitter: &mut Emitter,
    ) {
        let Some(ddl) = ddl::classify(statement) else {
            debug!(message = "Ignoring non-schema statement.", database = %default_db);
            return;
        };
        let filters = &self.settings.filters;
        let included: Vec<(String, Option<String>)> = ddl
            .targets(default_db)
            .into_iter()
            .filter(|(db, table)| {
                !db.is_empty()
                    && match table {
                        Some(table) => filters.table_included(db, table),
                        None => filters.database_included(db),
                    }
            })
            .collect();
        let Some((database, _)) = included.first().cloned() else {
            debug!(message = "Ignoring schema change outside the captured databases.");
            return;
        };
        debug!(message = "Schema change.", database = %database, ddl = %statement);
        self.track_tables(&ddl, default_db);
        if ddl.data_changed {
            warn!(
                message = "A partition operation removed or swapped rows without row events; the \
                           affected tables no longer match downstream. Re-snapshot them.",
                database = %database,
                ddl = %statement.chars().take(200).collect::<String>(),
                internal_log_rate_limit = false,
            );
        }
        let ts_us = self.tx.commit_ts_us.unwrap_or(header_ts_us);
        // Position of the statement, shared by its control events.
        let base = |kind: &str, db: &str| {
            let mut payload = Map::new();
            payload.insert("type".into(), Json::from(kind));
            payload.insert(
                "server_name".into(),
                Json::from(self.settings.topic_prefix.as_str()),
            );
            payload.insert("db".into(), Json::from(db));
            payload.insert(
                "gtid".into(),
                self.tx.gtid_text.as_deref().map_or(Json::Null, Json::from),
            );
            payload.insert("file".into(), Json::from(self.file.as_str()));
            payload.insert("pos".into(), Json::from(self.last_start_pos));
            payload.insert("server_id".into(), Json::from(server_id));
            payload.insert("commit_ts_ms".into(), Json::from(ts_us.div_euclid(1000)));
            payload
        };

        if ddl.kind == DdlKind::Truncate {
            // No row events exist for a TRUNCATE: consumers remove the table's rows with a lower
            // `seq` than this event.
            for (db, table) in &included {
                let table = table.as_deref().unwrap_or_default();
                let mut payload = base("truncate", db);
                payload.insert("table".into(), Json::from(table));
                emitter.push_control(
                    payload,
                    EventMeta {
                        key: None,
                        database: db,
                        table: Some(table),
                        op: "truncate",
                        timestamp: timestamp(ts_us),
                        schema_version: None,
                    },
                    false,
                );
            }
        }

        if !self.settings.include_schema_changes {
            return;
        }
        let mut payload = base("schema_change", &database);
        payload.insert("ddl".into(), Json::from(statement));
        payload.insert("kind".into(), Json::from(ddl.kind.as_str()));
        payload.insert("data_changed".into(), Json::from(ddl.data_changed));
        payload.insert(
            "objects".into(),
            Json::Array(
                included
                    .iter()
                    .map(|(db, table)| serde_json::json!({ "db": db, "table": table }))
                    .collect(),
            ),
        );
        emitter.push_control(
            payload,
            EventMeta {
                key: None,
                database: &database,
                table: None,
                op: "schema_change",
                timestamp: timestamp(ts_us),
                schema_version: None,
            },
            false,
        );
    }

    /// Ends the current transaction: records its GTID and the position after it.
    /// Ends an XA transaction's first group. `XA COMMIT … ONE PHASE` commits it here. `XA PREPARE`
    /// does not: the transaction commits or rolls back later, in a group of its own, and the rows
    /// are not kept until then, so a prepared transaction that changed captured tables is a
    /// failure (skipped, with its rows, unless `event_processing_failure_handling_mode: fail`).
    fn xa_prepare(&mut self, data: &[u8], emitter: &mut Emitter) -> Result<(), String> {
        // The event body starts with the `one_phase` flag.
        let one_phase = data.first().is_some_and(|flag| *flag != 0);
        let changes = std::mem::take(&mut self.tx.xa_changes);
        if one_phase {
            if self.incremental.has_work() {
                let gtid = self.tx.gtid.clone();
                let gtid = gtid.as_ref().map(|(sid, gno)| (sid.as_str(), *gno));
                for (database, table, key) in &changes {
                    self.incremental
                        .on_change(database, table, key.as_ref(), gtid);
                }
            }
        } else if let Some((database, table, _)) = changes.first() {
            let xid = self.tx.xa.as_deref().unwrap_or_default();
            self.settings.handle_failure(
                format!(
                    "XA transaction {xid} was prepared (two-phase XA), and two-phase XA \
                     transactions are not supported: whether its changes commit is only known \
                     later. Set `event_processing_failure_handling_mode` to `warn` to skip such \
                     transactions, then re-snapshot the tables they change"
                ),
                database,
                table,
            )?;
            emitter.discard_uncommitted();
        }
        self.commit(emitter);
        Ok(())
    }

    fn commit(&mut self, emitter: &mut Emitter) {
        if let Some((source_id, gno)) = self.tx.gtid.take() {
            self.position.gtid_set.add(&source_id, gno);
        }
        self.position.file = self.file.clone();
        self.position.pos = self.last_end_pos;
        emitter.commit(self.offset());
        self.tx = Transaction::default();
    }

    /// Keeps the set of snapshotted tables in step with DDL, and queues tables renamed into the
    /// captured set for an incremental snapshot.
    fn track_tables(&mut self, ddl: &ddl::Ddl, default_db: &str) {
        let targets = ddl.targets(default_db);
        let gtid = self.tx.gtid.clone();
        let gtid = gtid.as_ref().map(|(sid, gno)| (sid.as_str(), *gno));
        let filters = &self.settings.filters;
        match (ddl.verb, ddl.kind) {
            ("TRUNCATE", DdlKind::Truncate) => {
                for (db, table) in &targets {
                    if let Some(table) = table {
                        self.incremental.on_truncate(db, table, gtid);
                    }
                }
            }
            // A new table's rows all arrive through the binlog.
            ("CREATE", DdlKind::Table) => {
                for (db, table) in targets {
                    if let Some(table) = table
                        && filters.table_included(&db, &table)
                    {
                        let table = (db, table);
                        self.held.retain(|held| held != &table);
                        self.incremental.mark_snapshotted(table);
                    }
                }
            }
            ("DROP", DdlKind::Table) => {
                for (db, table) in targets {
                    if let Some(table) = table {
                        let table = (db, table);
                        self.held.retain(|held| held != &table);
                        self.incremental.forget(&table);
                    }
                }
            }
            ("RENAME", DdlKind::Table) => {
                let pairs: Vec<(TableRef, TableRef)> = targets
                    .chunks(2)
                    .filter_map(|pair| match pair {
                        [(db1, Some(t1)), (db2, Some(t2))] => {
                            Some(((db1.clone(), t1.clone()), (db2.clone(), t2.clone())))
                        }
                        _ => None,
                    })
                    .collect();
                // A name that already held a snapshotted table (an online schema-change swap)
                // keeps its history; anything else renamed in has rows the stream never saw.
                let known: Vec<bool> = pairs
                    .iter()
                    .map(|(_, to)| self.incremental.is_snapshotted(to))
                    .collect();
                for ((from, to), known) in pairs.into_iter().zip(known) {
                    self.incremental.forget(&from);
                    if !filters.table_included(&to.0, &to.1) {
                        continue;
                    }
                    let name = format!("{}.{}", to.0, to.1);
                    if known {
                        self.incremental.mark_snapshotted(to);
                    } else if !self.settings.snapshot_new_tables {
                        info!(
                            message = "A table was renamed into the captured set; its existing rows \
                                       are not snapshotted (`snapshot_new_tables` is false).",
                            table = %name,
                            internal_log_rate_limit = false,
                        );
                    } else if !self.position.use_gtid {
                        warn!(
                            message = "A table was renamed into the captured set, but incremental \
                                       snapshots need GTID mode; its existing rows are not captured.",
                            table = %name,
                            internal_log_rate_limit = false,
                        );
                    } else {
                        info!(
                            message = "A table was renamed into the captured set; snapshotting it \
                                       incrementally.",
                            table = %name,
                            internal_log_rate_limit = false,
                        );
                        self.incremental.queue(vec![to]);
                    }
                }
            }
            _ => {}
        }
    }

    /// The committed position with the table-snapshot state.
    fn offset(&self) -> Offset {
        Offset::new(
            Some(self.server_uuid.clone()),
            self.position.file.clone(),
            self.position.pos,
            self.position.gtid_set.to_string(),
            true,
        )
        .with_tables(
            Some(self.incremental.snapshotted_tables()),
            self.incremental.progress(),
        )
        .with_held(self.held.clone())
        .with_last_signal(self.last_signal.clone())
    }

    /// Records incremental-snapshot progress at the current (committed) position.
    fn commit_progress(&mut self, emitter: &mut Emitter) {
        emitter.commit(self.offset());
    }

    /// Not inside a transaction (incremental snapshot chunks are only emitted here).
    const fn between_transactions(&self) -> bool {
        !self.tx.in_tx && self.tx.gtid.is_none()
    }
}

impl SnapshotQueue for Handler<'_> {
    fn captured(&self, (db, table): &TableRef) -> bool {
        self.settings.filters.table_included(db, table)
    }

    fn unavailable(&self) -> Option<String> {
        (!self.position.use_gtid).then(|| "incremental snapshots need GTID mode".to_owned())
    }

    fn queued(&self, table: &TableRef) -> bool {
        self.incremental.is_queued(table)
    }

    fn queue(&mut self, tables: Vec<TableRef>) {
        self.held.retain(|held| !tables.contains(held));
        self.incremental.queue(tables);
    }

    fn stop(&mut self, tables: Option<&[TableRef]>) -> Vec<TableRef> {
        self.incremental.stop(tables)
    }

    fn held(&self, table: &TableRef) -> bool {
        self.held.contains(table)
    }

    fn dismiss(&mut self, tables: &[TableRef]) {
        self.held.retain(|held| !tables.contains(held));
        for table in tables {
            self.incremental.mark_snapshotted(table.clone());
        }
    }
}

fn emit(
    emitter: &mut Emitter,
    info: &TableInfo,
    source: &SourceInfo<'_>,
    op: Op,
    before: Option<Map<String, Json>>,
    after: Option<Map<String, Json>>,
    key: Option<Map<String, Json>>,
) {
    emitter.push_row_change(
        op,
        before,
        after,
        source,
        EventMeta {
            key,
            database: &info.database,
            table: Some(&info.table),
            op: op.code(),
            timestamp: timestamp(source.ts_us),
            schema_version: Some(&info.schema_version),
        },
    );
}

fn timestamp(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// `s` without `prefix` (an ASCII prefix, compared case-insensitively).
fn strip_prefix_ignore_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if starts_with_ignore_case(s, prefix) {
        s.get(prefix.len()..)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matching() {
        assert!(starts_with_ignore_case("xa start 'x'", "XA "));
        assert!(!starts_with_ignore_case("XAB", "XA "));
        assert!(!starts_with_ignore_case("X", "XA "));
        assert_eq!(
            strip_prefix_ignore_case("xa start X'7031',X'',1", "XA START "),
            Some("X'7031',X'',1")
        );
        assert_eq!(
            strip_prefix_ignore_case("XA END X'7031'", "XA START "),
            None
        );
        assert_eq!(strip_prefix_ignore_case("XA", "XA START "), None);
    }
}
