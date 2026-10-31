//! Incremental (chunked) snapshots, read while the binlog streams.
//!
//! This is the read-only variant of the `DBLog` watermark algorithm (as in Debezium's read-only
//! incremental snapshots for `MySQL`). Each chunk of a table is read with one short query, in key
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
    fmt::Write as _,
    sync::Arc,
};

use chrono::Utc;
use mysql_async::{Conn, Value, prelude::Queryable};
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

    /// Whether the table is queued (the first one is being snapshotted).
    pub fn is_queued(&self, table: &TableRef) -> bool {
        self.progress
            .as_ref()
            .is_some_and(|p| p.tables.contains(table))
    }

    /// Drops tables (`None`: all) from the queue; returns the ones that were queued. The rows
    /// of a table already emitted stay emitted, and it gets no `snapshot_completed` event.
    pub fn stop(&mut self, tables: Option<&[TableRef]>) -> Vec<TableRef> {
        let Some(progress) = self.progress.as_mut() else {
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
            self.pending = None;
        }
        if progress.tables.is_empty() {
            self.progress = None;
            self.conn = None;
        }
        stopped
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
    ) -> Result<bool, StepError> {
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
    async fn read_chunk(
        &mut self,
        settings: &Settings,
        emitter: &Emitter,
    ) -> Result<bool, StepError> {
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
            self.conn = Some(
                snapshot_connection(settings)
                    .await
                    .map_err(StepError::Retry)?,
            );
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
                        snapshot_id = %self.progress.as_ref().map_or("", |p| p.snapshot_id.as_str()),
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
                Err(StepError::Retry(error))
            }
            Err(ReadError::Fatal(error)) => Err(StepError::Fatal(error)),
        }
    }
}

fn progress_is_new(progress: Option<&IncrementalProgress>) -> bool {
    progress.is_some_and(|p| p.last_key.is_none() && p.rows == 0)
}

/// The placeholder that binds a key column's position in `(keys) > (…)`, or why the column
/// cannot order chunks. The comparison must order exactly as `ORDER BY` does, or chunks skip
/// rows.
fn key_placeholder(name: &str, detail: &ColumnDetails) -> Result<String, String> {
    if detail.nullable {
        // `(k) > (NULL)` is NULL: the chunk after a NULL key comes back empty, ending the table.
        return Err(format!(
            "key column `{name}` is nullable, so the table cannot be read in key order; use \
             NOT NULL columns in `pk_override`"
        ));
    }
    match detail.data_type.as_str() {
        // Sorted by their index but compared with a string as text.
        "enum" | "set" => Err(format!(
            "key column `{name}` is an {}, which sorts by its index but compares as text, so \
             chunks could skip rows",
            detail.data_type.to_uppercase()
        )),
        // A string compared with a DECIMAL is compared as a double: exact only as a DECIMAL.
        "decimal" | "numeric" => Ok(format!(
            "CAST(? AS DECIMAL({}, {}))",
            detail.precision.unwrap_or(65),
            detail.scale.unwrap_or(0)
        )),
        _ => Ok("?".into()),
    }
}

enum ReadError {
    /// The table cannot be snapshotted incrementally; skip it.
    Skip(String),
    /// A database error; retry later.
    Fail(String),
    /// A row could not be converted and `event_processing_failure_handling_mode` is `fail`.
    Fatal(String),
}

/// Why a step of the incremental snapshot failed.
pub enum StepError {
    /// A database error: reconnect and retry.
    Retry(String),
    /// Stop the source.
    Fatal(String),
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
#[allow(
    clippy::too_many_lines,
    reason = "query, watermark check and emit kept together, in order"
)]
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
    let placeholders = keys
        .iter()
        .map(|k| key_placeholder(k, &details[k]))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ReadError::Skip)?
        .join(", ");

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
    let mut params = Vec::new();
    if let Some(last) = &last_key {
        if last.len() != keys.len() {
            return Err(ReadError::Skip(
                "the key changed since the snapshot started; snapshot it again".into(),
            ));
        }
        // The position comes from the stored or recovered offset: bind it, never splice it in.
        params = last
            .iter()
            .map(|literal| parse_key_literal(literal))
            .collect::<Option<Vec<Value>>>()
            .ok_or_else(|| {
                ReadError::Skip(
                    "the stored chunk position is not a valid key; snapshot it again".into(),
                )
            })?;
        write!(query, " WHERE ({key_list}) > ({placeholders})").expect("writing to a String");
    }
    let limit = settings.incremental_chunk_size;
    write!(query, " ORDER BY {key_list} LIMIT {limit}").expect("writing to a String");

    let low = gtid_executed(conn).await?;
    let mut result = conn
        .exec_iter(query, params)
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
        settings.convert,
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
        match convert_row(&info, row, settings.convert) {
            Ok((after, key)) => {
                let key_text = key.as_ref().map(key_string).unwrap_or_default();
                let event = build_read(&builder, &settings.topic_prefix, "", 0, &info, after, key);
                chunk_rows.push((key_text, event));
            }
            Err(error) => {
                // In `fail` mode the same row fails on every retry: stop instead.
                settings
                    .handle_failure(error, database, table)
                    .map_err(ReadError::Fatal)?;
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

/// Parses a stored chunk position, written by `Value::as_sql(false)`, back into a value to bind.
/// Only the forms `as_sql` writes are accepted: `NULL`, a number, `0x` + hex bytes, or a quoted
/// string with backslash escapes (which also covers dates and times). Anything else is `None`.
fn parse_key_literal(literal: &str) -> Option<Value> {
    if literal == "NULL" {
        return Some(Value::NULL);
    }
    if let Some(quoted) = literal.strip_prefix('\'') {
        return unescape_quoted(quoted.strip_suffix('\'')?).map(|s| Value::Bytes(s.into_bytes()));
    }
    if let Some(hex_digits) = literal.strip_prefix("0x") {
        return hex::decode(hex_digits).ok().map(Value::Bytes);
    }
    if let Ok(i) = literal.parse::<i64>() {
        return Some(Value::Int(i));
    }
    if let Ok(u) = literal.parse::<u64>() {
        return Some(Value::UInt(u));
    }
    // Floats, as `{}` prints them; `parse` alone would also take `inf` and `NaN`.
    if literal
        .bytes()
        .all(|b| b.is_ascii_digit() || b"+-.e".contains(&b))
    {
        return literal.parse::<f64>().ok().map(Value::Double);
    }
    None
}

/// The inverse of `as_sql`'s string escaping: rejects an unescaped quote or an unknown escape.
fn unescape_quoted(text: &str) -> Option<String> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.push(match chars.next()? {
                '0' => '\0',
                'n' => '\n',
                'r' => '\r',
                'Z' => '\x1a',
                c @ ('\\' | '\'' | '"') => c,
                _ => return None,
            }),
            '\'' => return None,
            c => out.push(c),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(table: &str) -> TableRef {
        ("shop".into(), table.into())
    }

    #[test]
    fn key_columns_bind_so_chunks_order_like_order_by() {
        let column = |data_type: &str, nullable: bool| ColumnDetails {
            position: 1,
            data_type: data_type.into(),
            nullable,
            unsigned: false,
            precision: Some(12),
            scale: Some(2),
            fsp: 0,
            bit_len: 0,
            primary_key: false,
            values: None,
        };
        assert_eq!(
            key_placeholder("id", &column("bigint", false)).unwrap(),
            "?"
        );
        assert_eq!(
            key_placeholder("price", &column("decimal", false)).unwrap(),
            "CAST(? AS DECIMAL(12, 2))"
        );
        assert!(
            key_placeholder("tenant", &column("int", true))
                .unwrap_err()
                .contains("nullable")
        );
        assert!(
            key_placeholder("status", &column("enum", false))
                .unwrap_err()
                .contains("ENUM")
        );
        assert!(key_placeholder("tags", &column("set", false)).is_err());
    }

    #[test]
    fn stopping_the_table_being_read_resets_the_position() {
        let mut incremental = Incremental::new(Vec::new(), None);
        incremental.queue(vec![t("a"), t("b"), t("c")]);
        let progress = incremental.progress.as_mut().unwrap();
        progress.last_key = Some(vec!["5".into()]);
        progress.rows = 5;

        // A queued table: the one being read keeps its position.
        assert_eq!(incremental.stop(Some(&[t("b")])), vec![t("b")]);
        assert_eq!(incremental.progress().unwrap().rows, 5);
        assert!(!incremental.is_queued(&t("b")));

        // The one being read: the next starts from its beginning.
        assert_eq!(incremental.stop(Some(&[t("a")])), vec![t("a")]);
        let progress = incremental.progress().unwrap();
        assert_eq!(progress.tables, vec![t("c")]);
        assert_eq!(progress.last_key, None);
        assert_eq!(progress.rows, 0);

        assert_eq!(incremental.stop(Some(&[t("x")])), Vec::<TableRef>::new());
        assert_eq!(incremental.stop(None), vec![t("c")]);
        assert!(incremental.progress().is_none());
        assert!(!incremental.has_work());
    }

    #[test]
    fn key_literals_round_trip() {
        let values = [
            Value::NULL,
            Value::Int(-42),
            Value::Int(i64::MAX),
            Value::UInt(u64::MAX),
            Value::Double(2.5),
            Value::Double(-1e-7),
            Value::Bytes(b"plain".to_vec()),
            Value::Bytes("it's a \"quote\", a \\ and \0\n\r\x1a".as_bytes().to_vec()),
            Value::Bytes("ünïcödé".as_bytes().to_vec()),
            Value::Bytes(vec![0xff, 0x00, 0x80]),
        ];
        for value in values {
            let literal = value.as_sql(false);
            assert_eq!(parse_key_literal(&literal), Some(value), "{literal}");
        }
        // Dates and times come back as strings, which MySQL compares as it did the literal.
        let date = Value::Date(2026, 10, 31, 12, 30, 0, 250);
        assert_eq!(
            parse_key_literal(&date.as_sql(false)),
            Some(Value::Bytes(b"2026-10-31 12:30:00.000250".to_vec()))
        );
    }

    #[test]
    fn key_literals_reject_injection() {
        for literal in [
            "1) OR (1=1",
            "1; DROP TABLE t",
            "'a' OR '1'='1'",
            "'unterminated",
            "'a'b'",
            "'\\x'",
            "0xZZ",
            "inf",
            "NaN",
            "SLEEP(10)",
            "",
        ] {
            assert_eq!(parse_key_literal(literal), None, "{literal}");
        }
    }
}
