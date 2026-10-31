//! Streaming from the replication slot.
//!
//! pgoutput sends whole committed transactions (`Begin`, changes, `Commit`) in commit order, and
//! a `Relation` message before the first change of each table in a session (and again after its
//! columns change). Each commit hands the emitter an offset at the end of the commit record.
//!
//! The server keeps WAL for the slot until told it may forget it: status updates report as
//! flushed only offsets the checkpoint writer has released (stored, or published with
//! `publish_offsets`), so the slot never runs ahead of the lake. While no captured changes flow,
//! a keepalive's position becomes the offset (nothing is pending before it), so an idle source
//! keeps releasing WAL that other databases on the server produce.
//!
//! On a broken connection the uncommitted transaction is discarded and the source reconnects
//! from its last committed offset; the server replays from its last flushed position, and
//! transactions committed before the offset are skipped.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use pgrepl_tokio_postgres::Client;
use postgres_replication::protocol::{LogicalReplicationMessage as M, Tuple, TupleData};
use serde_json::{Map, Value as Json};
use tokio::{sync::watch, time::Instant};

use super::{
    catalog::{RelationColumn, TableInfo, TypeRegistry},
    connection::{Received, Replication, error_text, sqlstate},
    convert,
    envelope::{Op, SnapshotMarker},
    incremental::{self, Chunk, Window, key_text},
    offset::{IncrementalProgress, Lsn, Offset, TableRef},
    source::{
        Emitter, EventMeta, Settings, connect_retrying, note_table_schema, row_event, row_key,
    },
};
use crate::{
    internal_events::PostgresCdcConnectionError,
    shutdown::ShutdownSignal,
    sources::cdc::{
        control::snapshot_completed,
        signal::{self, SnapshotQueue, Spool},
    },
};

/// Microseconds between the Unix epoch and PostgreSQL's (2000-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// The transaction being read.
struct Txn {
    xid: u32,
    commit_lsn: Lsn,
    ts_us: i64,
    /// Committed before the offset: already processed.
    skip: bool,
}

/// How a session ended.
enum End {
    Shutdown,
    /// The connection broke: reconnect.
    Disconnected(String),
}

/// Streaming state that outlives one connection.
struct State<'a> {
    settings: &'a Settings,
    query: Client,
    registry: &'a mut TypeRegistry,
    offset: Offset,
    /// Per session: relation OID → table.
    tables: HashMap<u32, TableInfo>,
    /// The last definition seen of each table (for schema-change events across sessions).
    known: HashMap<(String, String), TableInfo>,
    last_commit_lsn: Option<Lsn>,
    txn: Option<Txn>,
    received: Lsn,
    released: Lsn,
    /// The incremental-snapshot chunk waiting for the stream to reach its high watermark.
    window: Option<Box<Window>>,
}

/// The keys a streamed change touched, for dropping them from an open window.
struct Touched {
    table: TableRef,
    xid: u32,
    keys: Vec<String>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "the streaming loop's inputs, each used throughout"
)]
pub(super) async fn stream(
    settings: &Settings,
    query: Client,
    registry: &mut TypeRegistry,
    emitter: &mut Emitter,
    start: Offset,
    captured: &[TableRef],
    mut released: watch::Receiver<Option<Offset>>,
    mut shutdown: ShutdownSignal,
) -> Result<(), String> {
    info!(message = "Streaming changes from the replication slot.", offset = %start);
    let mut spool = match &settings.signals {
        Some(config) => Some(Spool::open(config, start.last_signal.as_deref()).await?),
        None => None,
    };
    let known = known_tables(settings, &query, registry, captured).await?;
    let mut state = State {
        settings,
        query,
        registry,
        received: start.lsn,
        offset: start,
        tables: HashMap::new(),
        known,
        last_commit_lsn: None,
        txn: None,
        window: None,
        // Nothing is released until the checkpoint writer says so: the slot is already at or
        // before the last released offset, and must not pass it.
        released: Lsn(0),
    };
    loop {
        if !reconnect_query(&mut state, &mut shutdown).await {
            return Ok(());
        }
        let Some(client) = connect_retrying(settings, true, &mut shutdown).await else {
            return Ok(());
        };
        let mut replication = match Replication::start(
            client,
            &settings.slot_name,
            state.offset.lsn,
            &settings.publication_name,
        )
        .await
        {
            Ok(replication) => replication,
            Err(error) => {
                let code = sqlstate(&error);
                // 55000: the slot was invalidated; 42704: it does not exist.
                if matches!(code.as_deref(), Some("55000" | "42704")) {
                    return Err(format!(
                        "replication slot `{}` can no longer be streamed: {}. On the next start, \
                         `snapshot_mode: when_needed` re-snapshots",
                        settings.slot_name,
                        error_text(&error)
                    ));
                }
                // 55006: still active for the previous connection; it goes away shortly.
                emit!(PostgresCdcConnectionError {
                    error: format!("START_REPLICATION failed: {}", error_text(&error)),
                    endpoint: &settings.conn.endpoint,
                });
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(5)) => continue,
                }
            }
        };
        state.tables.clear();
        state.txn = None;
        // A chunk read for the previous connection is read again (from the stored progress).
        state.window = None;
        let end = match session(
            &mut state,
            &mut replication,
            emitter,
            &mut released,
            spool.as_mut(),
            &mut shutdown,
        )
        .await
        {
            Ok(end) => end,
            // A catalog query failed because its connection is gone: reconnect both.
            Err(error) if state.query.is_closed() => {
                End::Disconnected(format!("the catalog connection closed: {error}"))
            }
            Err(error) => return Err(error),
        };
        match end {
            End::Shutdown => return Ok(()),
            End::Disconnected(error) => {
                emitter.discard_uncommitted();
                state.txn = None;
                emit!(PostgresCdcConnectionError {
                    error,
                    endpoint: &settings.conn.endpoint,
                });
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
        }
    }
}

/// The definitions to compare the stream's `Relation` messages with: as the catalog has them now
/// (the snapshot's, right after a snapshot). The first change of a table after an `ALTER TABLE`
/// then shows up as a schema change, even if the table had no change before it.
async fn known_tables(
    settings: &Settings,
    query: &Client,
    registry: &TypeRegistry,
    captured: &[TableRef],
) -> Result<HashMap<TableRef, TableInfo>, String> {
    let mut known = HashMap::with_capacity(captured.len());
    for (schema, table) in captured {
        if let Some(info) = TableInfo::from_catalog(
            query,
            registry,
            &settings.filters,
            &settings.convert,
            schema,
            table,
        )
        .await?
        {
            known.insert((schema.clone(), table.clone()), info);
        }
    }
    Ok(known)
}

/// Reconnects the catalog connection if it closed: it goes down with the server too (a restart,
/// a failover). `false` if shutdown was requested first.
async fn reconnect_query(state: &mut State<'_>, shutdown: &mut ShutdownSignal) -> bool {
    if state.query.is_closed() {
        let Some(query) = connect_retrying(state.settings, false, shutdown).await else {
            return false;
        };
        state.query = query;
    }
    true
}

/// Reads one replication connection until it breaks or the source shuts down.
async fn session(
    state: &mut State<'_>,
    replication: &mut Replication,
    emitter: &mut Emitter,
    released: &mut watch::Receiver<Option<Offset>>,
    mut spool: Option<&mut Spool>,
    shutdown: &mut ShutdownSignal,
) -> Result<End, String> {
    let interval = state.settings.status_interval;
    let mut status = tokio::time::interval(interval);
    status.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut flush = tokio::time::interval(Duration::from_millis(100));
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // An idle server that hears from us sends nothing, so a status update after a quiet interval
    // asks it for a keepalive; no answer for much longer means the connection is gone (a
    // half-open TCP connection never errors on its own). Measured from the last message, not per
    // `select!` pass: the other branches wake the loop far more often.
    let silence = interval * 3 + Duration::from_mins(1);
    let mut last_message = Instant::now();
    if let Some(end) = open_window(state, replication).await? {
        return Ok(end);
    }
    loop {
        tokio::select! {
            _ = &mut *shutdown => return Ok(End::Shutdown),
            _ = status.tick() => {
                if last_message.elapsed() >= silence {
                    return Ok(End::Disconnected(format!("no message from the server for {silence:?}")));
                }
                let sent = if last_message.elapsed() >= interval {
                    replication.request_keepalive(state.received, state.released).await
                } else {
                    replication.status(state.received, state.released).await
                };
                if let Err(error) = sent {
                    return Ok(End::Disconnected(format!("failed to send a status update: {}", error_text(&error))));
                }
            }
            changed = released.changed() => {
                if changed.is_ok()
                    && let Some(offset) = released.borrow_and_update().as_ref()
                {
                    state.released = state.released.max(offset.lsn);
                }
            }
            _ = flush.tick() => {
                if let Some(spool) = spool.as_deref_mut()
                    && state.txn.is_none()
                    && apply_signals(state, spool, emitter).await?
                    && let Some(end) = open_window(state, replication).await?
                {
                    return Ok(end);
                }
                if let Some(end) = flush_alive(state, replication, emitter).await? {
                    return Ok(end);
                }
                emitter.check_health()?;
            }
            next = replication.next() => {
                let received = match next {
                    Err(error) => return Ok(End::Disconnected(error_text(&error))),
                    // The server ended the stream (for example `wal_sender_timeout`).
                    Ok(None) => return Ok(End::Disconnected("the server ended the replication stream".into())),
                    Ok(Some(received)) => received,
                };
                last_message = Instant::now();
                match received {
                    Received::KeepAlive { wal_end, reply } => {
                        state.received = state.received.max(wal_end);
                        // Nothing pending before `wal_end`: transactions not yet sent commit
                        // after it, so it is a safe place to resume.
                        if state.txn.is_none() && wal_end > state.offset.lsn {
                            state.offset = state.offset.at(wal_end);
                            emitter.commit(state.offset.clone());
                        }
                        if state.txn.is_none() {
                            close_window(state, emitter, wal_end);
                        }
                        if reply && let Err(error) = replication.status(state.received, state.released).await {
                            return Ok(End::Disconnected(format!("failed to send a status update: {}", error_text(&error))));
                        }
                        if let Some(end) = open_window(state, replication).await? {
                            return Ok(end);
                        }
                    }
                    Received::Message { message, wal_end } => {
                        state.received = state.received.max(wal_end);
                        handle(state, emitter, message).await?;
                        if let Some(end) = flush_alive(state, replication, emitter).await? {
                            return Ok(end);
                        }
                        if let Some(end) = open_window(state, replication).await? {
                            return Ok(end);
                        }
                    }
                }
            }
        }
    }
}

/// Sends what is due downstream, which waits while the pipeline applies backpressure, and keeps
/// sending status updates meanwhile: the server drops a connection that reports nothing for
/// `wal_sender_timeout`, and the source would then replay the same changes into the same
/// backpressure. The send always runs to completion; a failed status update ends the session
/// after it.
async fn flush_alive(
    state: &State<'_>,
    replication: &mut Replication,
    emitter: &mut Emitter,
) -> Result<Option<End>, String> {
    let interval = state.settings.status_interval;
    let mut status = tokio::time::interval_at(Instant::now() + interval, interval);
    let flush = emitter.maybe_flush();
    tokio::pin!(flush);
    let mut broken = None;
    loop {
        tokio::select! {
            flushed = &mut flush => return flushed.map(|()| broken),
            _ = status.tick(), if broken.is_none() => {
                if let Err(error) = replication.status(state.received, state.released).await {
                    broken = Some(End::Disconnected(format!(
                        "failed to send a status update: {}",
                        error_text(&error)
                    )));
                }
            }
        }
    }
}

/// Applies the signals that are due (between transactions), commits an offset recording them,
/// and moves them to `processed/`. Returns whether any were applied.
async fn apply_signals(
    state: &mut State<'_>,
    spool: &mut Spool,
    emitter: &mut Emitter,
) -> Result<bool, String> {
    let done = signal::apply_due(spool, state).await;
    if done.is_empty() {
        return Ok(false);
    }
    state.offset.last_signal = signal::last_signal(&done);
    emitter.commit(state.offset.clone());
    emitter.flush().await?;
    spool.finish(done).await;
    Ok(true)
}

impl SnapshotQueue for State<'_> {
    fn captured(&self, (schema, table): &TableRef) -> bool {
        self.settings.filters.table_included(schema, table)
    }

    fn queued(&self, table: &TableRef) -> bool {
        self.offset
            .incremental
            .as_ref()
            .is_some_and(|p| p.tables.contains(table))
    }

    fn queue(&mut self, tables: Vec<TableRef>) {
        let progress = self
            .offset
            .incremental
            .get_or_insert_with(|| IncrementalProgress::new(Vec::new()));
        for table in tables {
            if !progress.tables.contains(&table) {
                progress.tables.push(table);
            }
        }
    }

    fn stop(&mut self, tables: Option<&[TableRef]>) -> Vec<TableRef> {
        let Some(progress) = self.offset.incremental.as_mut() else {
            return Vec::new();
        };
        let first = progress.tables.first().cloned();
        let (stopped, kept): (Vec<TableRef>, Vec<TableRef>) = progress
            .tables
            .drain(..)
            .partition(|t| tables.is_none_or(|stop| stop.contains(t)));
        progress.tables = kept;
        if progress.tables.first() != first.as_ref() {
            // The table being read was stopped: the next one starts from its beginning.
            progress.last_key = None;
            progress.rows = 0;
            progress.started_seq = None;
            self.window = None;
        }
        if progress.tables.is_empty() {
            self.offset.incremental = None;
        }
        stopped
    }

    // PostgreSQL snapshots are never held: a lost slot stops the source until approved.
    fn held(&self, _table: &TableRef) -> bool {
        false
    }

    fn dismiss(&mut self, _tables: &[TableRef]) {}
}

const fn ts_us(pg_micros: i64) -> i64 {
    pg_micros + PG_EPOCH_OFFSET_US
}

fn timestamp(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

#[allow(clippy::too_many_lines, reason = "one arm per pgoutput message type")]
async fn handle(state: &mut State<'_>, emitter: &mut Emitter, message: M) -> Result<(), String> {
    match message {
        M::Begin(begin) => {
            let commit_lsn = Lsn(begin.final_lsn());
            // Every transaction before the window's high watermark has been streamed.
            close_window(state, emitter, commit_lsn);
            state.txn = Some(Txn {
                xid: begin.xid(),
                commit_lsn,
                ts_us: ts_us(begin.timestamp()),
                skip: commit_lsn < state.offset.lsn,
            });
        }
        M::Commit(commit) => {
            if let Some(txn) = state.txn.take()
                && !txn.skip
            {
                state.offset = state.offset.at(Lsn(commit.end_lsn()));
                emitter.commit(state.offset.clone());
                state.last_commit_lsn = Some(txn.commit_lsn);
            }
            close_window(state, emitter, Lsn(commit.end_lsn()));
        }
        M::Relation(relation) => {
            let schema = relation.namespace().map_err(|e| e.to_string())?.to_owned();
            let table = relation.name().map_err(|e| e.to_string())?.to_owned();
            let columns = relation
                .columns()
                .iter()
                .map(|c| {
                    Ok(RelationColumn {
                        name: c.name().map_err(|e| e.to_string())?.to_owned(),
                        type_oid: c.type_id().cast_unsigned(),
                        typmod: c.type_modifier(),
                        // Bit 1: part of the replica identity (all columns with FULL).
                        identity: c.flags() & 1 != 0,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            if columns.iter().any(|c| !state.registry.knows(c.type_oid)) {
                // A type created after start-up (an enum, a domain): read the types again.
                *state.registry = TypeRegistry::load(&state.query).await?;
            }
            let info = TableInfo::from_relation(
                &state.query,
                state.registry,
                &state.settings.filters,
                &state.settings.convert,
                &schema,
                &table,
                &columns,
            )
            .await?;
            let captured = state.settings.filters.table_included(&schema, &table);
            if captured {
                state.settings.warn_if_keyless(&info);
                state.settings.warn_if_key_outside_identity(&info);
                let key = (schema.clone(), table.clone());
                if let Some(previous) = state.known.get(&key)
                    && info.columns_differ(previous)
                {
                    schema_change(state, emitter, previous, &info);
                }
                state.known.insert(key, info.clone());
            }
            state.tables.insert(relation.rel_id(), info);
        }
        M::Insert(insert) => {
            let touched = row_change(
                state,
                emitter,
                insert.rel_id(),
                Op::Create,
                None,
                Some(insert.tuple()),
            )?;
            drop_touched(state, touched);
        }
        M::Update(update) => {
            let old = update.old_tuple().or(update.key_tuple());
            let full = update.old_tuple().is_some();
            let touched = row_change_update(
                state,
                emitter,
                update.rel_id(),
                old,
                full,
                update.new_tuple(),
            )?;
            drop_touched(state, touched);
        }
        M::Delete(delete) => {
            let old = delete.old_tuple().or(delete.key_tuple());
            let full = delete.old_tuple().is_some();
            let touched = row_change_delete(state, emitter, delete.rel_id(), old, full)?;
            drop_touched(state, touched);
        }
        M::Truncate(truncate) => {
            let Some(txn) = state.txn.as_ref().filter(|t| !t.skip) else {
                return Ok(());
            };
            let (xid, commit_lsn, ts) = (txn.xid, txn.commit_lsn, txn.ts_us);
            for rel in truncate.rel_ids() {
                let Some(info) = state.tables.get(rel) else {
                    continue;
                };
                if !state
                    .settings
                    .filters
                    .table_included(&info.schema, &info.table)
                {
                    continue;
                }
                // A chunk read before this truncate is stale as a whole.
                if let Some(window) = state.window.as_mut()
                    && window.table.0 == info.schema
                    && window.table.1 == info.table
                    && !window.snapshot.visible(xid)
                {
                    window.drop_all();
                }
                let mut payload =
                    control_base(state.settings, "truncate", &info.schema, xid, commit_lsn);
                payload.insert("table".into(), Json::from(info.table.as_str()));
                payload.insert("cascade".into(), Json::from(truncate.options() & 1 != 0));
                payload.insert(
                    "restart_identity".into(),
                    Json::from(truncate.options() & 2 != 0),
                );
                emitter.push_control(
                    payload,
                    EventMeta {
                        key: None,
                        database: &info.schema,
                        table: Some(&info.table),
                        op: "truncate",
                        timestamp: timestamp(ts),
                        schema_version: None,
                    },
                    false,
                );
            }
        }
        // Replication origins, logical decoding messages and type descriptions are not needed.
        _ => {}
    }
    Ok(())
}

fn control_base(
    settings: &Settings,
    kind: &str,
    schema: &str,
    xid: u32,
    lsn: Lsn,
) -> Map<String, Json> {
    let mut payload = Map::new();
    payload.insert("type".into(), Json::from(kind));
    payload.insert(
        "server_name".into(),
        Json::from(settings.topic_prefix.as_str()),
    );
    payload.insert("db".into(), Json::from(schema));
    payload.insert("database".into(), Json::from(settings.database.as_str()));
    payload.insert("txid".into(), Json::from(xid));
    payload.insert("lsn".into(), Json::from(lsn.to_string()));
    payload
}

/// A `schema_change` control event: PostgreSQL streams no DDL, so it describes what changed.
fn schema_change(state: &State<'_>, emitter: &mut Emitter, previous: &TableInfo, info: &TableInfo) {
    if !state.settings.include_schema_changes {
        return;
    }
    let Some(txn) = state.txn.as_ref() else {
        return;
    };
    let names = |t: &TableInfo| t.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    let (before, after) = (names(previous), names(info));
    let added: Vec<&String> = after.iter().filter(|c| !before.contains(c)).collect();
    let removed: Vec<&String> = before.iter().filter(|c| !after.contains(c)).collect();
    let retyped: Vec<&String> = info
        .columns
        .iter()
        .filter(|c| {
            previous
                .columns
                .iter()
                .any(|p| p.name == c.name && (p.type_oid != c.type_oid || p.typmod != c.typmod))
        })
        .map(|c| &c.name)
        .collect();
    let mut payload = control_base(
        state.settings,
        "schema_change",
        &info.schema,
        txn.xid,
        txn.commit_lsn,
    );
    payload.insert("table".into(), Json::from(info.table.as_str()));
    payload.insert("kind".into(), Json::from("table"));
    payload.insert(
        "previous_schema_version".into(),
        Json::from(previous.schema_version.as_str()),
    );
    payload.insert(
        "schema_version".into(),
        Json::from(info.schema_version.as_str()),
    );
    payload.insert("columns_added".into(), serde_json::json!(added));
    payload.insert("columns_removed".into(), serde_json::json!(removed));
    payload.insert("columns_retyped".into(), serde_json::json!(retyped));
    emitter.push_control(
        payload,
        EventMeta {
            key: None,
            database: &info.schema,
            table: Some(&info.table),
            op: "schema_change",
            timestamp: timestamp(txn.ts_us),
            schema_version: Some(&info.schema_version),
        },
        false,
    );
}

/// The text of a tuple's column `i` (`Ok(None)` for SQL NULL); an unchanged `TOASTed` value is
/// taken from `old` when it has it, else the placeholder.
fn column_text<'t>(
    tuple: &'t Tuple,
    old: Option<&'t Tuple>,
    i: usize,
    placeholder: &'t str,
) -> Result<Option<std::borrow::Cow<'t, str>>, String> {
    let text = |data: &'t TupleData| -> Result<Option<std::borrow::Cow<'t, str>>, String> {
        match data {
            TupleData::Null => Ok(None),
            TupleData::Text(bytes) => std::str::from_utf8(bytes)
                .map(|s| Some(std::borrow::Cow::Borrowed(s)))
                .map_err(|_| "a value is not valid UTF-8".to_owned()),
            TupleData::Binary(_) => Err("binary tuple data is not requested".into()),
            TupleData::UnchangedToast => Ok(Some(std::borrow::Cow::Borrowed(placeholder))),
        }
    };
    match tuple.tuple_data().get(i) {
        Some(TupleData::UnchangedToast) => match old.and_then(|o| o.tuple_data().get(i)) {
            Some(data @ (TupleData::Text(_) | TupleData::Null)) => text(data),
            _ => Ok(Some(std::borrow::Cow::Borrowed(placeholder))),
        },
        Some(data) => text(data),
        None => Ok(None),
    }
}

/// Converts a tuple to a row. `key_only` keeps only the replica-identity columns (an old-key
/// tuple carries no others). Unchanged `TOASTed` columns are written as the placeholder string,
/// bypassing conversion.
fn tuple_row(
    settings: &Settings,
    info: &TableInfo,
    tuple: &Tuple,
    old: Option<&Tuple>,
    key_only: bool,
) -> Result<Map<String, Json>, String> {
    let mut row = Map::with_capacity(info.columns.len());
    for (i, column) in info.columns.iter().enumerate() {
        if column.excluded || (key_only && !info.identity.contains(&i)) {
            continue;
        }
        let unchanged = matches!(tuple.tuple_data().get(i), Some(TupleData::UnchangedToast))
            && !matches!(
                old.and_then(|o| o.tuple_data().get(i)),
                Some(TupleData::Text(_) | TupleData::Null)
            );
        let value = if unchanged {
            Json::from(settings.placeholder.as_str())
        } else {
            let text = column_text(tuple, old, i, &settings.placeholder)?;
            convert::convert(column, text.as_deref(), &settings.convert)?
        };
        row.insert(column.name.clone(), value);
    }
    Ok(row)
}

/// The transaction and table for a change, or `None` if it is skipped or not captured.
fn target<'s>(state: &'s State<'_>, rel: u32) -> Result<Option<(&'s Txn, &'s TableInfo)>, String> {
    let Some(txn) = state.txn.as_ref() else {
        return Err("a change arrived outside a transaction".into());
    };
    if txn.skip {
        return Ok(None);
    }
    let info = state.tables.get(&rel).ok_or_else(|| {
        format!("a change arrived for relation {rel} before its Relation message")
    })?;
    if !state
        .settings
        .filters
        .table_included(&info.schema, &info.table)
    {
        return Ok(None);
    }
    Ok(Some((txn, info)))
}

fn push(
    state: &State<'_>,
    emitter: &mut Emitter,
    txn: &Txn,
    info: &TableInfo,
    op: Op,
    before: Option<Map<String, Json>>,
    after: Option<Map<String, Json>>,
) -> Touched {
    let keys = [before.as_ref(), after.as_ref()]
        .into_iter()
        .flatten()
        .filter_map(|row| row_key(info, row))
        .map(|key| key_text(&key))
        .collect();
    let touched = Touched {
        table: (info.schema.clone(), info.table.clone()),
        xid: txn.xid,
        keys,
    };
    note_table_schema(
        emitter,
        state.settings,
        info,
        "stream",
        Some(txn.commit_lsn),
    );
    let source = state.settings.source_info(
        info,
        txn.ts_us,
        SnapshotMarker::False,
        Some(txn.xid),
        Some(txn.commit_lsn),
        state.last_commit_lsn,
    );
    let event = row_event(
        &emitter.builder(),
        info,
        op,
        before,
        after,
        &source,
        timestamp(txn.ts_us),
    );
    emitter.push_prebuilt(event);
    touched
}

fn row_change(
    state: &State<'_>,
    emitter: &mut Emitter,
    rel: u32,
    op: Op,
    before: Option<&Tuple>,
    after: Option<&Tuple>,
) -> Result<Option<Touched>, String> {
    let Some((txn, info)) = target(state, rel)? else {
        return Ok(None);
    };
    let converted = (|| {
        let before = before
            .map(|t| tuple_row(state.settings, info, t, None, false))
            .transpose()?;
        let after = after
            .map(|t| tuple_row(state.settings, info, t, None, false))
            .transpose()?;
        Ok::<_, String>((before, after))
    })();
    match converted {
        Ok((before, after)) => Ok(Some(push(state, emitter, txn, info, op, before, after))),
        Err(error) => state
            .settings
            .handle_failure(error, &info.schema, &info.table)
            .map(|()| None),
    }
}

fn row_change_update(
    state: &State<'_>,
    emitter: &mut Emitter,
    rel: u32,
    old: Option<&Tuple>,
    full: bool,
    new: &Tuple,
) -> Result<Option<Touched>, String> {
    let Some((txn, info)) = target(state, rel)? else {
        return Ok(None);
    };
    let converted = (|| {
        let before = old
            .map(|t| tuple_row(state.settings, info, t, None, !full))
            .transpose()?;
        let after = tuple_row(state.settings, info, new, old.filter(|_| full), false)?;
        // The server sends the old key only when the identity changed (or always, with FULL).
        let key_changed = before
            .as_ref()
            .is_some_and(|before| row_key(info, before) != row_key(info, &after));
        if key_changed && !full {
            unkeyable(info)?;
        }
        Ok::<_, String>((before, after, key_changed))
    })();
    match converted {
        // Like Debezium: a key change becomes a delete of the old key plus a create of the new
        // one, so a merge on the event key removes the old row.
        Ok((before, after, true)) => {
            let mut touched = push(state, emitter, txn, info, Op::Delete, before, None);
            let created = push(state, emitter, txn, info, Op::Create, None, Some(after));
            touched.keys.extend(created.keys);
            Ok(Some(touched))
        }
        Ok((before, after, false)) => Ok(Some(push(
            state,
            emitter,
            txn,
            info,
            Op::Update,
            before,
            Some(after),
        ))),
        Err(error) => state
            .settings
            .handle_failure(error, &info.schema, &info.table)
            .map(|()| None),
    }
}

/// Fails when the key has columns an old-key tuple does not carry: the old key is unknown.
fn unkeyable(info: &TableInfo) -> Result<(), String> {
    let outside = info.key_outside_identity();
    if outside.is_empty() {
        return Ok(());
    }
    Err(format!(
        "the old row's key is unknown: key columns {} are not in the replica identity (set \
         REPLICA IDENTITY FULL, or make the identity cover the key)",
        outside.join(", ")
    ))
}

fn row_change_delete(
    state: &State<'_>,
    emitter: &mut Emitter,
    rel: u32,
    old: Option<&Tuple>,
    full: bool,
) -> Result<Option<Touched>, String> {
    let Some((txn, info)) = target(state, rel)? else {
        return Ok(None);
    };
    let Some(old) = old else {
        // REPLICA IDENTITY NOTHING: the server sends no old row at all.
        return state
            .settings
            .handle_failure(
                "a delete carried no old row (REPLICA IDENTITY NOTHING)".into(),
                &info.schema,
                &info.table,
            )
            .map(|()| None);
    };
    let converted = if full {
        tuple_row(state.settings, info, old, None, false)
    } else {
        unkeyable(info).and_then(|()| tuple_row(state.settings, info, old, None, true))
    };
    match converted {
        Ok(before) => Ok(Some(push(
            state,
            emitter,
            txn,
            info,
            Op::Delete,
            Some(before),
            None,
        ))),
        Err(error) => state
            .settings
            .handle_failure(error, &info.schema, &info.table)
            .map(|()| None),
    }
}

/// Drops the keys a change touched from the open window, if the change is newer than the
/// window's chunk (its transaction is not visible in the chunk's snapshot).
fn drop_touched(state: &mut State<'_>, touched: Option<Touched>) {
    if let (Some(window), Some(touched)) = (state.window.as_mut(), touched)
        && window.table == touched.table
        && !window.snapshot.visible(touched.xid)
    {
        for key in &touched.keys {
            window.drop_key(key);
        }
    }
}

/// Reads the next chunk of the incremental snapshot when none is open and no transaction is
/// being read, and asks the server for its position so the window can close soon even when
/// nothing is written. Returns `Some` if the connection broke.
async fn open_window(
    state: &mut State<'_>,
    replication: &mut Replication,
) -> Result<Option<End>, String> {
    if state.window.is_some() || state.txn.is_some() {
        return Ok(None);
    }
    loop {
        let Some(progress) = state.offset.incremental.as_ref() else {
            return Ok(None);
        };
        let Some(table) = progress.tables.first().cloned() else {
            state.offset.incremental = None;
            return Ok(None);
        };
        let last_key = progress.last_key.clone();
        let chunk = incremental::read_chunk(
            &state.query,
            state.settings,
            state.registry,
            &table,
            last_key.as_deref(),
            state.settings.incremental_chunk_size,
        )
        .await?;
        let name = format!("{}.{}", table.0, table.1);
        match chunk {
            Chunk::Window(window) => {
                state.window = Some(window);
                break;
            }
            Chunk::Gone => {
                warn!(message = "A table queued for an incremental snapshot no longer exists; skipping it.", table = %name);
            }
            Chunk::NoKey => {
                warn!(
                    message = "A table queued for an incremental snapshot has no primary key (or \
                               `pk_override`), so it cannot be read in chunks; skipping it.",
                    table = %name,
                    internal_log_rate_limit = false,
                );
            }
            Chunk::BadPosition => {
                warn!(
                    message = "The stored position of an incremental snapshot is not a valid key \
                               of the table; skipping it. Snapshot the table again.",
                    table = %name,
                    internal_log_rate_limit = false,
                );
            }
        }
        // Skipped: move on to the next table.
        if let Some(progress) = state.offset.incremental.as_mut() {
            progress.tables.remove(0);
            progress.last_key = None;
            progress.rows = 0;
            progress.started_seq = None;
        }
    }
    if let Err(error) = replication
        .request_keepalive(state.received, state.released)
        .await
    {
        return Ok(Some(End::Disconnected(format!(
            "failed to send a status update: {}",
            error_text(&error)
        ))));
    }
    Ok(None)
}

/// Emits the open window's remaining rows once the stream has reached its high watermark
/// (`position`: everything committed before it has been streamed), and records the progress.
fn close_window(state: &mut State<'_>, emitter: &mut Emitter, position: Lsn) {
    if state.window.as_ref().is_none_or(|w| position < w.high) {
        return;
    }
    let window = *state.window.take().expect("window is open");
    let Some(progress) = state.offset.incremental.as_mut() else {
        return;
    };
    let started_seq = *progress.started_seq.get_or_insert(emitter.next_seq());
    let snapshot_id = progress.snapshot_id.clone();
    let started_ms = progress.started_ms;
    let (table, info, high, complete) = (
        window.table.clone(),
        window.info.clone(),
        window.high,
        window.complete,
    );
    progress.last_key.clone_from(&window.last_key);

    note_table_schema(emitter, state.settings, &info, "snapshot", Some(high));
    let builder = emitter.builder();
    let now = Utc::now();
    let mut count = 0;
    for after in window.into_rows() {
        let source = state.settings.source_info(
            &info,
            now.timestamp_micros(),
            SnapshotMarker::Incremental,
            None,
            Some(high),
            None,
        );
        let mut event = row_event(&builder, &info, Op::Read, None, Some(after), &source, now);
        builder.set_snapshot_id(&mut event, &snapshot_id);
        emitter.push_snapshot_row(event);
        count += 1;
    }
    let progress = state.offset.incremental.as_mut().expect("checked above");
    progress.rows += count;
    if complete {
        let rows = progress.rows;
        if state.settings.snapshot_end_markers {
            emitter.push_control(
                snapshot_completed(
                    &state.settings.topic_prefix,
                    &table.0,
                    &table.1,
                    &snapshot_id,
                    started_seq,
                    started_ms,
                    rows,
                    Some(&info.schema_version),
                ),
                EventMeta {
                    key: None,
                    database: &table.0,
                    table: Some(&table.1),
                    op: "snapshot_completed",
                    timestamp: now,
                    schema_version: None,
                },
                true,
            );
        }
        info!(message = "Snapshotted table incrementally.", table = %format!("{}.{}", table.0, table.1), rows);
        progress.tables.retain(|t| t != &table);
        progress.last_key = None;
        progress.rows = 0;
        progress.started_seq = None;
        if !state.offset.snapshotted_tables.contains(&table) {
            state.offset.snapshotted_tables.push(table);
        }
        if state
            .offset
            .incremental
            .as_ref()
            .is_some_and(|p| p.tables.is_empty())
        {
            state.offset.incremental = None;
            info!(message = "Incremental snapshot complete.");
        }
    }
    // The rows and the progress are stored together.
    emitter.commit(state.offset.clone());
}
