//! Incremental (chunked) snapshots, read while the binlog streams.
//!
//! This is the read-only variant of the DBLog watermark algorithm (as in Debezium's read-only
//! incremental snapshots for MySQL). Each chunk of a table is read with one short query, in key
//! order, bracketed by two reads of `@@GLOBAL.gtid_executed`:
//!
//! * `low`: executed before the chunk was read;
//! * `high`: executed after it.
//!
//! The chunk's rows reflect the table at some instant between `low` and `high`. While the
//! stream catches up to `high`, any captured change to one of the chunk's keys from a
//! transaction *not* in `low` is newer than (or as new as) the chunk's copy, so that key is
//! dropped from the chunk; the change itself is emitted by the stream as usual. Once the
//! stream's position contains `high`, the remaining rows are emitted as snapshot reads: every
//! change older than them has already been emitted, and every newer change follows them.
//!
//! Locks are held for one chunk query at a time, so DDL never waits behind a long snapshot
//! transaction. Progress (the last key emitted) is stored with each offset, so a restart
//! resumes from the last delivered chunk.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use chrono::Utc;
use mysql_async::{Conn, prelude::Queryable};
use serde_json::{Map, Value as Json};

use super::{
    envelope::SnapshotMarker,
    offset::{GtidSet, IncrementalProgress, TableRef},
    schema::{ColumnDetails, TableInfo},
    snapshot::{build_read, column_details, convert_row, quote_ident, snapshot_connection},
    source::{Emitter, EventMeta, NoteTableSchema, Settings, snapshot_completed},
    table_schema::Observed,
};
use crate::event::LogEvent;

/// A new snapshot id (sortable, unique per snapshot).
pub(super) fn new_snapshot_id() -> String {
    Utc::now().format("%Y%m%dT%H%M%S%.6fZ").to_string()
}

/// Starts tracking a new incremental snapshot of `tables`.
pub(super) fn new_progress(tables: Vec<TableRef>) -> IncrementalProgress {
    IncrementalProgress {
        snapshot_id: new_snapshot_id(),
        started_ms: Utc::now().timestamp_millis(),
        started_seq: None,
        tables,
        last_key: None,
        rows: 0,
    }
}

/// The base tables the filters capture, in a stable order.
pub(super) async fn captured_tables(
    conn: &mut Conn,
    settings: &Settings,
) -> Result<Vec<TableRef>, String> {
    let all: Vec<(String, String)> = conn
        .query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .await
        .map_err(|error| format!("listing tables: {error}"))?;
    Ok(all
        .into_iter()
        .filter(|(db, table)| settings.filters.table_included(db, table))
        .collect())
}

/// The canonical form of a row key, for matching snapshot rows against streamed changes.
fn key_string(key: &Map<String, Json>) -> String {
    serde_json::to_string(key).unwrap_or_default()
}

/// A chunk that has been read and waits for the stream to reach its high watermark.
struct Chunk {
    table: TableRef,
    info: Arc<TableInfo>,
    low: GtidSet,
    high: GtidSet,
    /// `(key, event)` in key order.
    rows: Vec<(String, LogEvent)>,
    /// Keys changed by the stream since `low`: their chunk copies are stale.
    dropped: HashSet<String>,
    /// Truncated since `low`: every chunk row is stale.
    truncated: bool,
    /// Key literals of the last row read (the next chunk starts after it).
    last_key: Option<Vec<String>>,
    /// Fewer rows than requested: this was the table's last chunk.
    exhausted: bool,
}

pub(super) struct Incremental {
    progress: Option<IncrementalProgress>,
    snapshotted: BTreeSet<TableRef>,
    pending: Option<Chunk>,
    conn: Option<Conn>,
    /// Key columns per table, in index order.
    keys: HashMap<TableRef, Vec<String>>,
}

impl Incremental {
    pub fn new(snapshotted: Vec<TableRef>, progress: Option<IncrementalProgress>) -> Self {
        Self {
            progress: progress.filter(|p| !p.tables.is_empty()),
            snapshotted: snapshotted.into_iter().collect(),
            pending: None,
            conn: None,
            keys: HashMap::new(),
        }
    }

    pub fn snapshotted_tables(&self) -> Vec<TableRef> {
        self.snapshotted.iter().cloned().collect()
    }

    pub fn progress(&self) -> Option<IncrementalProgress> {
        self.progress.clone()
    }

    pub fn has_work(&self) -> bool {
        self.progress.as_ref().is_some_and(|p| !p.tables.is_empty())
    }

    pub fn is_snapshotted(&self, table: &TableRef) -> bool {
        self.snapshotted.contains(table)
    }

    /// Records that all of a table's rows are covered (e.g. it was created while streaming).
    pub fn mark_snapshotted(&mut self, table: TableRef) {
        self.snapshotted.insert(table);
    }

    /// Forgets a table (dropped or renamed away).
    pub fn forget(&mut self, table: &TableRef) {
        self.snapshotted.remove(table);
    }

    /// Queues tables for an incremental snapshot (after any already queued).
    pub fn queue(&mut self, tables: Vec<TableRef>) {
        let progress = self
            .progress
            .get_or_insert_with(|| new_progress(Vec::new()));
        for table in tables {
            if !progress.tables.contains(&table) {
                progress.tables.push(table);
            }
        }
    }

    /// Forgets the chunk being held (after a reconnect it is read again).
    pub fn reset_pending(&mut self) {
        self.pending = None;
    }

    /// A streamed change to `key` of a table; `gtid` is its transaction.
    pub fn on_change(
        &mut self,
        database: &str,
        table: &str,
        key: Option<&Map<String, Json>>,
        gtid: Option<(&str, u64)>,
    ) {
        let Some(chunk) = &mut self.pending else {
            return;
        };
        if chunk.table.0 != database || chunk.table.1 != table {
            return;
        }
        if gtid.is_some_and(|(sid, gno)| chunk.low.contains(sid, gno)) {
            return; // older than the chunk's read
        }
        if let Some(key) = key {
            chunk.dropped.insert(key_string(key));
        }
    }

    /// A streamed `TRUNCATE` of a table.
    pub fn on_truncate(&mut self, database: &str, table: &str, gtid: Option<(&str, u64)>) {
        if let Some(chunk) = &mut self.pending
            && chunk.table.0 == database
            && chunk.table.1 == table
            && !gtid.is_some_and(|(sid, gno)| chunk.low.contains(sid, gno))
        {
            chunk.truncated = true;
        }
    }

    /// Advances the snapshot: emits the held chunk once the stream has reached its high
    /// watermark, and reads the next one. Call only between transactions. Returns whether the
    /// progress changed (so the caller commits a new offset).
    pub async fn step(
        &mut self,
        settings: &Settings,
        position: &GtidSet,
        emitter: &mut Emitter,
    ) -> Result<bool, String> {
        let mut changed = false;
        // At most one emit and one read per call, so streaming is never starved.
        if self
            .pending
            .as_ref()
            .is_some_and(|chunk| position.contains_set(&chunk.high))
        {
            let chunk = self.pending.take().expect("checked above");
            self.emit(chunk, settings, emitter);
            changed = true;
        }
        if self.pending.is_none() && self.has_work() {
            changed |= self.read_chunk(settings, emitter).await?;
            if self
                .pending
                .as_ref()
                .is_some_and(|chunk| position.contains_set(&chunk.high))
            {
                let chunk = self.pending.take().expect("checked above");
                self.emit(chunk, settings, emitter);
                changed = true;
            }
        }
        Ok(changed)
    }

    fn emit(&mut self, chunk: Chunk, settings: &Settings, emitter: &mut Emitter) {
        let Some(progress) = self.progress.as_mut() else {
            return;
        };
        if progress.tables.first() != Some(&chunk.table) {
            return; // the table left the queue meanwhile
        }
        let builder = emitter.builder();
        emitter.note_table_schema(
            &chunk.info,
            &settings.topic_prefix,
            &Observed {
                origin: "incremental_snapshot",
                file: "",
                pos: 0,
                gtid: None,
            },
        );
        if !chunk.truncated {
            for (key, mut event) in chunk.rows {
                if chunk.dropped.contains(&key) {
                    continue;
                }
                builder.set_snapshot_marker(&mut event, SnapshotMarker::Incremental);
                builder.set_snapshot_id(&mut event, &progress.snapshot_id);
                emitter.push_snapshot_row(event);
                progress.rows += 1;
            }
        }
        if chunk.last_key.is_some() {
            progress.last_key = chunk.last_key;
        }
        if chunk.exhausted {
            let (database, table) = progress.tables.remove(0);
            if settings.snapshot_end_markers {
                let started_seq = progress.started_seq.unwrap_or_else(|| emitter.next_seq());
                emitter.push_control(
                    snapshot_completed(
                        &settings.topic_prefix,
                        &database,
                        &table,
                        &progress.snapshot_id,
                        started_seq,
                        progress.started_ms,
                        progress.rows,
                        Some(&chunk.info.schema_version),
                    ),
                    EventMeta {
                        key: None,
                        database: &database,
                        table: Some(&table),
                        op: "snapshot_completed",
                        timestamp: Utc::now(),
                        schema_version: Some(&chunk.info.schema_version),
                    },
                    true,
                );
            }
            info!(
                message = "Incremental snapshot: table done.",
                table = %format!("{database}.{table}"),
                rows = progress.rows,
                snapshot_id = %progress.snapshot_id,
                internal_log_rate_limit = false,
            );
            self.snapshotted.insert((database, table));
            progress.last_key = None;
            progress.rows = 0;
            progress.started_seq = None;
            if progress.tables.is_empty() {
                info!(
                    message = "Incremental snapshot completed.",
                    snapshot_id = %progress.snapshot_id,
                );
                self.progress = None;
                self.conn = None;
            }
        }
    }

    /// Drops the first queued table without snapshotting it.
    fn skip_table(&mut self, reason: &str) {
        if let Some(progress) = self.progress.as_mut()
            && !progress.tables.is_empty()
        {
            let (database, table) = progress.tables.remove(0);
            warn!(
                message = "Incremental snapshot: skipping table.",
                table = %format!("{database}.{table}"),
                reason,
                internal_log_rate_limit = false,
            );
            progress.last_key = None;
            progress.rows = 0;
            progress.started_seq = None;
            if progress.tables.is_empty() {
                self.progress = None;
            }
        }
    }

    /// Reads the next chunk of the first queued table into `pending`. Returns whether the queue
    /// changed (a table was skipped).
    async fn read_chunk(&mut self, settings: &Settings, emitter: &Emitter) -> Result<bool, String> {
        let Some(progress) = self.progress.as_ref() else {
            return Ok(false);
        };
        let table_ref = progress.tables[0].clone();
        let last_key = progress.last_key.clone();
        let (database, table) = (&table_ref.0, &table_ref.1);
        if !settings.filters.table_included(database, table) {
            self.skip_table("no longer captured");
            return Ok(true);
        }

        if self.conn.is_none() {
            self.conn = Some(snapshot_connection(settings).await?);
        }
        let conn = self.conn.as_mut().expect("connected above");
        let result = read(
            conn,
            settings,
            &table_ref,
            last_key,
            &mut self.keys,
            emitter,
        )
        .await;
        match result {
            Ok(Some(chunk)) => {
                // Changes from here on are newer than this table's snapshot.
                if let Some(progress) = self.progress.as_mut()
                    && progress.started_seq.is_none()
                {
                    progress.started_seq = Some(emitter.next_seq());
                }
                if progress_is_new(self.progress.as_ref()) {
                    info!(
                        message = "Incremental snapshot: reading table.",
                        table = %format!("{database}.{table}"),
                        snapshot_id = %self.progress.as_ref().map(|p| p.snapshot_id.as_str()).unwrap_or(""),
                        internal_log_rate_limit = false,
                    );
                }
                self.pending = Some(chunk);
                Ok(false)
            }
            Ok(None) => Ok(false),
            Err(ReadError::Skip(reason)) => {
                self.skip_table(&reason);
                Ok(true)
            }
            Err(ReadError::Fail(error)) => {
                self.conn = None;
                Err(error)
            }
        }
    }
}

fn progress_is_new(progress: Option<&IncrementalProgress>) -> bool {
    progress.is_some_and(|p| p.last_key.is_none() && p.rows == 0)
}

enum ReadError {
    /// The table cannot be snapshotted incrementally; skip it.
    Skip(String),
    /// A database error; retry later.
    Fail(String),
}

async fn gtid_executed(conn: &mut Conn) -> Result<GtidSet, ReadError> {
    let text: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .map_err(|error| ReadError::Fail(format!("reading gtid_executed: {error}")))?;
    text.unwrap_or_default()
        .parse()
        .map_err(|error: String| ReadError::Fail(error))
}

/// The table's key columns in index order: `pk_override`, else the primary key.
async fn key_columns(
    conn: &mut Conn,
    settings: &Settings,
    table: &TableRef,
    cache: &mut HashMap<TableRef, Vec<String>>,
) -> Result<Vec<String>, ReadError> {
    if let Some(keys) = cache.get(table) {
        return Ok(keys.clone());
    }
    let keys = match settings.filters.key_columns(&table.0, &table.1) {
        Some(keys) => keys.to_vec(),
        None => conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
                 ORDER BY ORDINAL_POSITION",
                (&table.0, &table.1),
            )
            .await
            .map_err(|error| ReadError::Fail(format!("reading the primary key: {error}")))?,
    };
    if keys.is_empty() {
        return Err(ReadError::Skip(
            "no primary key; set `pk_override` to snapshot it incrementally".into(),
        ));
    }
    cache.insert(table.clone(), keys.clone());
    Ok(keys)
}

/// Reads one chunk. `Ok(None)` when there is nothing to do.
async fn read(
    conn: &mut Conn,
    settings: &Settings,
    table_ref: &TableRef,
    last_key: Option<Vec<String>>,
    key_cache: &mut HashMap<TableRef, Vec<String>>,
    emitter: &Emitter,
) -> Result<Option<Chunk>, ReadError> {
    let (database, table) = (&table_ref.0, &table_ref.1);
    let details: HashMap<String, ColumnDetails> = column_details(conn, database, table)
        .await
        .map_err(ReadError::Fail)?;
    if details.is_empty() {
        return Err(ReadError::Skip("the table no longer exists".into()));
    }
    let keys = key_columns(conn, settings, table_ref, key_cache).await?;
    if let Some(missing) = keys.iter().find(|k| !details.contains_key(*k)) {
        key_cache.remove(table_ref);
        return Err(ReadError::Skip(format!(
            "key column `{missing}` does not exist"
        )));
    }

    let mut columns: Vec<(&String, &ColumnDetails)> = details.iter().collect();
    columns.sort_by_key(|(_, detail)| detail.position);
    let select_list = columns
        .iter()
        .map(|(name, _)| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let key_list = keys
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let mut query = format!(
        "SELECT {select_list} FROM {}.{}",
        quote_ident(database),
        quote_ident(table)
    );
    if let Some(last) = &last_key {
        if last.len() != keys.len() {
            return Err(ReadError::Skip(
                "the key changed since the snapshot started; snapshot it again".into(),
            ));
        }
        query.push_str(&format!(" WHERE ({key_list}) > ({})", last.join(", ")));
    }
    let limit = settings.incremental_chunk_size;
    query.push_str(&format!(" ORDER BY {key_list} LIMIT {limit}"));

    let low = gtid_executed(conn).await?;
    let mut result = conn
        .exec_iter(query, ())
        .await
        .map_err(|error| ReadError::Fail(format!("reading `{database}`.`{table}`: {error}")))?;
    let result_columns = result
        .columns()
        .ok_or_else(|| ReadError::Fail("no result-set metadata".into()))?;
    let info = Arc::new(TableInfo::from_result_columns(
        database,
        table,
        &result_columns,
        &details,
        &settings.filters,
        &settings.convert,
    ));
    let key_index: Vec<usize> = keys
        .iter()
        .filter_map(|k| {
            result_columns
                .iter()
                .position(|c| c.name_str() == k.as_str())
        })
        .collect();
    let rows: Vec<mysql_async::Row> = result
        .collect()
        .await
        .map_err(|error| ReadError::Fail(format!("reading `{database}`.`{table}`: {error}")))?;
    drop(result);
    let high = gtid_executed(conn).await?;

    let builder = emitter.builder();
    let exhausted = rows.len() < limit;
    let mut chunk_rows = Vec::with_capacity(rows.len());
    let mut last = None;
    for row in rows {
        let literals: Option<Vec<String>> = key_index
            .iter()
            .map(|&i| row.as_ref(i).map(|v| v.as_sql(false)))
            .collect();
        let Some(literals) = literals else {
            return Err(ReadError::Fail("a key value is missing from a row".into()));
        };
        last = Some(literals);
        match convert_row(&info, row, &settings.convert) {
            Ok((after, key)) => {
                let key_text = key.as_ref().map(key_string).unwrap_or_default();
                let event = build_read(&builder, &settings.topic_prefix, "", 0, &info, after, key);
                chunk_rows.push((key_text, event));
            }
            Err(error) => {
                settings
                    .handle_failure(error, database, table)
                    .map_err(ReadError::Fail)?;
            }
        }
    }
    Ok(Some(Chunk {
        table: table_ref.clone(),
        info,
        low,
        high,
        rows: chunk_rows,
        dropped: HashSet::new(),
        truncated: false,
        last_key: last,
        exhausted,
    }))
}
