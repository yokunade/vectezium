//! Consistent initial snapshot, modeled on Debezium's MySQL snapshot with
//! `snapshot.locking.mode=minimal`, optionally read by several connections in parallel:
//!
//! 1. `FLUSH TABLES WITH READ LOCK` (blocks writes for a moment);
//! 2. on each of the `snapshot_max_threads` connections, `START TRANSACTION WITH CONSISTENT
//!    SNAPSHOT` at `REPEATABLE READ` — all started while writes are blocked, so they all see the
//!    database at the same instant;
//! 3. read the binlog position / executed GTID set and the list of tables;
//! 4. `UNLOCK TABLES` — writes resume, but the transactions keep seeing the data exactly as of
//!    the recorded binlog position;
//! 5. split the work into units — whole tables, or primary-key ranges of about
//!    `snapshot_chunk_size` rows for tables with a single-column integer primary key — and read
//!    them on the worker connections, emitting `op: "r"` events;
//! 6. commit, and stream the binlog from the recorded position.
//!
//! The snapshot's offset is only stored once every snapshot event has been delivered. If the
//! source stops before that, the snapshot is taken again on the next start.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use crate::event::LogEvent;
use chrono::Utc;
use mysql_async::{Conn, Row, prelude::Queryable};
use serde_json::{Map, Value as Json};
use tokio::sync::mpsc;

use super::{
    SnapshotLockingMode,
    convert::{ConvertOptions, Origin, convert_value},
    envelope::{Op, SnapshotMarker, SourceInfo},
    filter::Filters,
    offset::Offset,
    schema::{ColumnDetails, TableInfo, parse_bit_len},
    source::{
        BinlogPosition, CONNECTOR_VERSION, Emitter, EventBuilder, EventMeta, NoteTableSchema,
        ServerInfo, Settings, connect, current_position, snapshot_completed,
    },
    table_schema::{self, Observed},
};
use crate::shutdown::ShutdownSignal;

/// Log progress every this many rows.
const PROGRESS_INTERVAL: u64 = 1_000_000;

/// Rows a worker sends to the emitter in one message.
const ROWS_PER_MESSAGE: usize = 512;

/// Upper bound on the number of chunks a single table is split into.
const MAX_CHUNKS_PER_TABLE: u64 = 100_000;

/// Integer types whose primary key can be split into ranges.
const CHUNKABLE_TYPES: &[&str] = &[
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
];

/// Quotes a MySQL identifier.
pub fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

type Image = Map<String, Json>;

/// A table to snapshot.
struct TablePlan {
    database: String,
    table: String,
    details: Arc<HashMap<String, ColumnDetails>>,
    /// The single-column integer primary key, if the table can be chunked.
    chunk_key: Option<String>,
}

/// One query's worth of work.
#[derive(Clone, Debug)]
struct Unit {
    table: usize,
    /// `[start, end)` primary-key range; `end: None` means unbounded.
    range: Option<(i64, Option<i64>)>,
}

enum Message {
    /// A worker started reading a unit of `table`.
    Started {
        table: usize,
        info: Arc<TableInfo>,
    },
    /// Fully built `op: "r"` events (marker `true`; first/last markers are set by the emitter).
    Rows {
        table: usize,
        events: Vec<LogEvent>,
    },
    /// A row could not be converted.
    BadRow {
        table: usize,
        error: String,
    },
    UnitDone {
        table: usize,
    },
    Failed(String),
}

/// Splits `[min, max]` into about `rows / chunk_size` ranges. The last range is unbounded.
fn plan_chunks(
    min: i64,
    max: i64,
    estimated_rows: u64,
    chunk_size: u64,
) -> Vec<(i64, Option<i64>)> {
    let span = (i128::from(max) - i128::from(min) + 1).max(1) as u128;
    let rows = if estimated_rows == 0 {
        span as u64
    } else {
        estimated_rows
    };
    let chunks = rows
        .div_ceil(chunk_size.max(1))
        .clamp(1, MAX_CHUNKS_PER_TABLE);
    if chunks <= 1 {
        return vec![(min, None)];
    }
    let width = span.div_ceil(u128::from(chunks)).max(1);
    let mut ranges = Vec::with_capacity(chunks as usize);
    let mut start = i128::from(min);
    loop {
        let end = start + width as i128;
        if end > i128::from(max) {
            ranges.push((start as i64, None));
            break;
        }
        ranges.push((start as i64, Some(end as i64)));
        start = end;
    }
    ranges
}

/// Opens a connection configured for snapshot reads.
pub(super) async fn snapshot_connection(settings: &Settings) -> Result<Conn, String> {
    let mut conn = connect(settings).await?;
    // UTF-8 results; UTC so TIMESTAMP values need no conversion; long timeouts because reading
    // may pause for a long time while the pipeline applies backpressure.
    for statement in [
        "SET NAMES utf8mb4",
        "SET SESSION time_zone = '+00:00', SESSION net_write_timeout = 3600, \
         SESSION net_read_timeout = 3600, SESSION wait_timeout = 86400",
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    ] {
        conn.query_drop(statement)
            .await
            .map_err(|error| format!("snapshot: configuring session: {error}"))?;
    }
    Ok(conn)
}

/// Runs the snapshot. Returns the offset to stream from, or `None` if shut down part-way.
pub(super) async fn run(
    settings: &Settings,
    server: &ServerInfo,
    emitter: &mut Emitter,
    mut shutdown: ShutdownSignal,
) -> Result<Option<Offset>, String> {
    let threads = settings.snapshot_threads.max(1);
    let mut conns = Vec::with_capacity(threads);
    for _ in 0..threads {
        conns.push(snapshot_connection(settings).await?);
    }

    let locked = match settings.locking_mode {
        SnapshotLockingMode::Minimal => {
            info!(message = "Snapshot: acquiring global read lock.");
            conns[0]
                .query_drop("FLUSH TABLES WITH READ LOCK")
                .await
                .map_err(|error| {
                    format!(
                        "snapshot: `FLUSH TABLES WITH READ LOCK` failed ({error}). Grant the RELOAD \
                         privilege, or set `snapshot_locking_mode: none` if writes can be paused \
                         while the snapshot starts"
                    )
                })?;
            true
        }
        SnapshotLockingMode::None => {
            if threads > 1 {
                warn!(
                    message = "`snapshot_locking_mode: none` with several snapshot threads: the \
                               threads' views are only identical if no writes happen while the \
                               snapshot starts."
                );
            }
            false
        }
    };

    let prepared = prepare(&mut conns, settings, locked).await;
    if locked {
        // Releases the global read lock without ending the consistent-snapshot transactions.
        conns[0]
            .query_drop("UNLOCK TABLES")
            .await
            .map_err(|error| format!("snapshot: releasing global read lock: {error}"))?;
        info!(message = "Snapshot: global read lock released.");
    }
    let (position, tables) = prepared?;

    // Plan the work inside the snapshot transaction of the first connection.
    let mut plans = Vec::with_capacity(tables.len());
    let mut units = VecDeque::new();
    for (database, table) in tables {
        let details = column_details(&mut conns[0], &database, &table).await?;
        let pk: Vec<(&String, &ColumnDetails)> =
            details.iter().filter(|(_, d)| d.primary_key).collect();
        let chunk_key = (settings.snapshot_chunk_size > 0
            && pk.len() == 1
            && CHUNKABLE_TYPES.contains(&pk[0].1.data_type.to_ascii_lowercase().as_str()))
        .then(|| pk[0].0.clone());

        let index = plans.len();
        let ranges = match &chunk_key {
            Some(key) => {
                chunk_ranges(
                    &mut conns[0],
                    &database,
                    &table,
                    key,
                    settings.snapshot_chunk_size,
                )
                .await?
            }
            None => None,
        };
        match ranges {
            Some(ranges) => units.extend(ranges.into_iter().map(|range| Unit {
                table: index,
                range: Some(range),
            })),
            None => units.push_back(Unit {
                table: index,
                range: None,
            }),
        }
        plans.push(TablePlan {
            database,
            table,
            details: Arc::new(details),
            chunk_key,
        });
    }

    // Identifies this snapshot's rows, so consumers can tell which keys a snapshot contained.
    let started = Utc::now();
    let started_ms = started.timestamp_millis();
    // Every event of this snapshot, and every change streamed after it, gets a seq from here on.
    let started_seq = emitter.next_seq();
    let snapshot_id: Arc<str> = Arc::from(started.format("%Y%m%dT%H%M%S%.6fZ").to_string());
    info!(
        message = "Snapshot started.",
        snapshot_id = %snapshot_id,
        tables = plans.len(),
        units = units.len(),
        threads,
        binlog_file = %position.file,
        binlog_pos = position.pos,
        gtid_set = %position.gtid_set,
    );
    if plans.is_empty() {
        warn!(message = "Snapshot: no tables match the configured filters.");
    }

    let mut units_left: Vec<usize> = vec![0; plans.len()];
    for unit in &units {
        units_left[unit.table] += 1;
    }
    let total_units = units.len();
    let plans = Arc::new(plans);
    let queue = Arc::new(Mutex::new(units));
    let (tx, mut rx) = mpsc::channel::<Message>(threads * 4);

    for conn in conns {
        let worker = Worker {
            conn,
            plans: Arc::clone(&plans),
            queue: Arc::clone(&queue),
            tx: tx.clone(),
            filters: settings.filters.clone(),
            convert: settings.convert,
            builder: emitter.builder(),
            server_name: settings.topic_prefix.clone(),
            file: position.file.clone(),
            pos: position.pos,
            snapshot_id: Arc::clone(&snapshot_id),
        };
        crate::spawn_in_current_span(worker.run());
    }
    drop(tx);

    // Emit rows as they arrive. One row per table is held back so the first and last rows of
    // each table (and of the whole snapshot) can be marked like Debezium does.
    let streaming_batch_size = emitter.set_batch_size(settings.snapshot_batch_size);
    let builder = emitter.builder();
    let mut held: Vec<Option<LogEvent>> = (0..plans.len()).map(|_| None).collect();
    let mut seen = vec![false; plans.len()];
    let mut schema_noted = vec![false; plans.len()];
    let mut versions: Vec<Option<String>> = vec![None; plans.len()];
    let mut table_rows: Vec<u64> = vec![0; plans.len()];
    let mut first_overall = true;
    let mut tables_left = units_left.iter().filter(|n| **n > 0).count();
    let mut units_done = 0;
    let mut total_rows: u64 = 0;
    let observed = Observed {
        origin: "snapshot",
        file: &position.file,
        pos: position.pos,
        gtid: None,
    };

    loop {
        let message = tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!(message = "Snapshot interrupted by shutdown; it will restart on the next start.");
                return Ok(None);
            }
            message = rx.recv() => message,
        };
        let Some(message) = message else { break };
        match message {
            Message::Started { table, info } => {
                if !schema_noted[table] {
                    schema_noted[table] = true;
                    versions[table] = Some(info.schema_version.clone());
                    settings.warn_if_keyless(&info);
                    emitter.note_table_schema(&info, &settings.topic_prefix, &observed);
                }
            }
            Message::Rows { table, events } => {
                table_rows[table] += events.len() as u64;
                for mut event in events {
                    if first_overall {
                        builder.set_snapshot_marker(&mut event, SnapshotMarker::First);
                    } else if !seen[table] {
                        builder
                            .set_snapshot_marker(&mut event, SnapshotMarker::FirstInDataCollection);
                    }
                    first_overall = false;
                    seen[table] = true;
                    if let Some(previous) = held[table].replace(event) {
                        emitter.push_snapshot_row(previous);
                    }
                    total_rows += 1;
                    if total_rows % PROGRESS_INTERVAL == 0 {
                        info!(
                            message = "Snapshot progress.",
                            rows = total_rows,
                            units_done,
                            units = total_units,
                        );
                    }
                }
                emitter.maybe_flush().await?;
                emitter.check_health()?;
            }
            Message::BadRow { table, error } => {
                let plan = &plans[table];
                settings.handle_failure(error, &plan.database, &plan.table)?;
            }
            Message::UnitDone { table } => {
                units_done += 1;
                units_left[table] -= 1;
                if units_left[table] == 0 {
                    tables_left -= 1;
                    if let Some(mut event) = held[table].take() {
                        let marker = if tables_left == 0 {
                            SnapshotMarker::Last
                        } else {
                            SnapshotMarker::LastInDataCollection
                        };
                        builder.set_snapshot_marker(&mut event, marker);
                        emitter.push_snapshot_row(event);
                    }
                    if settings.snapshot_end_markers {
                        let plan = &plans[table];
                        emitter.push_control(
                            snapshot_completed(
                                &settings.topic_prefix,
                                &plan.database,
                                &plan.table,
                                &snapshot_id,
                                started_seq,
                                started_ms,
                                table_rows[table],
                                versions[table].as_deref(),
                            ),
                            EventMeta {
                                key: None,
                                database: &plan.database,
                                table: Some(&plan.table),
                                op: "snapshot_completed",
                                timestamp: Utc::now(),
                                schema_version: versions[table].as_deref(),
                            },
                            true,
                        );
                    }
                    emitter.maybe_flush().await?;
                    debug!(
                        message = "Snapshot: table done.",
                        table = %format!("{}.{}", plans[table].database, plans[table].table),
                    );
                }
            }
            Message::Failed(error) => return Err(error),
        }
    }

    if units_done != total_units {
        return Err(format!(
            "snapshot: workers stopped after {units_done} of {total_units} units"
        ));
    }

    let offset = Offset::new(
        Some(server.server_uuid.clone()),
        position.file,
        position.pos,
        position.gtid_set,
        true,
    )
    .with_tables(
        Some(
            plans
                .iter()
                .map(|p| (p.database.clone(), p.table.clone()))
                .collect(),
        ),
        None,
    );
    emitter.commit(offset.clone());
    emitter.flush().await?;
    emitter.set_batch_size(streaming_batch_size);
    info!(message = "Snapshot completed.", snapshot_id = %snapshot_id, rows = total_rows, offset = %offset);
    Ok(Some(offset))
}

/// Starts the consistent-snapshot transactions and reads position and table list.
///
/// With the global read lock held, no writes happen in between, so the order does not matter.
/// Without it (`snapshot_locking_mode: none`), the position is read *before* the transactions
/// start: a write that lands in between is then both in the snapshot and streamed again
/// (a harmless duplicate) instead of being in neither (a lost change).
async fn prepare(
    conns: &mut [Conn],
    settings: &Settings,
    locked: bool,
) -> Result<(BinlogPosition, Vec<(String, String)>), String> {
    let early_position = if locked {
        None
    } else {
        Some(current_position(&mut conns[0]).await?)
    };
    for conn in conns.iter_mut() {
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .map_err(|error| format!("snapshot: starting transaction: {error}"))?;
    }
    let conn = &mut conns[0];
    let position = match early_position {
        Some(position) => position,
        None => current_position(conn).await?,
    };

    let all: Vec<(String, String)> = conn
        .query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .await
        .map_err(|error| format!("snapshot: listing tables: {error}"))?;
    let tables = all
        .into_iter()
        .filter(|(db, table)| settings.filters.table_included(db, table))
        .collect();
    Ok((position, tables))
}

/// Primary-key ranges for a chunkable table, or `None` to read it with a single query.
async fn chunk_ranges(
    conn: &mut Conn,
    database: &str,
    table: &str,
    key: &str,
    chunk_size: u64,
) -> Result<Option<Vec<(i64, Option<i64>)>>, String> {
    let bounds: Result<Option<(Option<i64>, Option<i64>)>, _> = conn
        .query_first(format!(
            "SELECT MIN({k}), MAX({k}) FROM {}.{}",
            quote_ident(database),
            quote_ident(table),
            k = quote_ident(key),
        ))
        .await;
    let (min, max) = match bounds {
        Ok(Some((Some(min), Some(max)))) => (min, max),
        // Empty table, or keys outside the i64 range (BIGINT UNSIGNED): no chunking.
        Ok(_) | Err(_) => return Ok(None),
    };
    let estimated: Option<Option<u64>> = conn
        .exec_first(
            "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (database, table),
        )
        .await
        .map_err(|error| format!("snapshot: estimating rows of `{database}`.`{table}`: {error}"))?;
    let ranges = plan_chunks(min, max, estimated.flatten().unwrap_or(0), chunk_size);
    Ok((ranges.len() > 1).then_some(ranges))
}

/// Reads units from the shared queue on its own connection until the queue is empty.
struct Worker {
    conn: Conn,
    plans: Arc<Vec<TablePlan>>,
    queue: Arc<Mutex<VecDeque<Unit>>>,
    tx: mpsc::Sender<Message>,
    filters: Filters,
    convert: ConvertOptions,
    builder: EventBuilder,
    server_name: String,
    file: String,
    pos: u64,
    snapshot_id: Arc<str>,
}

impl Worker {
    async fn run(mut self) {
        if let Err(error) = self.work().await {
            _ = self.tx.send(Message::Failed(error)).await;
        }
        _ = self.conn.query_drop("COMMIT").await;
    }

    async fn work(&mut self) -> Result<(), String> {
        loop {
            let unit = self.queue.lock().expect("snapshot queue lock").pop_front();
            let Some(unit) = unit else { return Ok(()) };
            let plan = &self.plans[unit.table];
            // Name every column: `SELECT *` leaves out invisible columns (including generated
            // invisible primary keys), which the binlog does include.
            let mut columns: Vec<(&String, &ColumnDetails)> = plan.details.iter().collect();
            columns.sort_by_key(|(_, detail)| detail.position);
            let select_list = if columns.is_empty() {
                "*".to_owned()
            } else {
                columns
                    .iter()
                    .map(|(name, _)| quote_ident(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let mut query = format!(
                "SELECT {select_list} FROM {}.{}",
                quote_ident(&plan.database),
                quote_ident(&plan.table)
            );
            if let (Some((start, end)), Some(key)) = (unit.range, &plan.chunk_key) {
                let key = quote_ident(key);
                query.push_str(&format!(" WHERE {key} >= {start}"));
                if let Some(end) = end {
                    query.push_str(&format!(" AND {key} < {end}"));
                }
            }
            let context = || format!("snapshot: reading `{}`.`{}`", plan.database, plan.table);

            let mut result = self
                .conn
                // The binary protocol returns typed values (dates, times) that the converter needs.
                .exec_iter(query, ())
                .await
                .map_err(|error| format!("{}: {error}", context()))?;
            let columns = result
                .columns()
                .ok_or_else(|| format!("{}: no result-set metadata", context()))?;
            let info = Arc::new(TableInfo::from_result_columns(
                &plan.database,
                &plan.table,
                &columns,
                &plan.details,
                &self.filters,
                &self.convert,
            ));
            if self
                .tx
                .send(Message::Started {
                    table: unit.table,
                    info: Arc::clone(&info),
                })
                .await
                .is_err()
            {
                return Ok(()); // the snapshot was stopped
            }

            let mut rows = Vec::with_capacity(ROWS_PER_MESSAGE);
            while let Some(row) = result
                .next()
                .await
                .map_err(|error| format!("{}: {error}", context()))?
            {
                match convert_row(&info, row, &self.convert) {
                    Ok((after, key)) => {
                        let mut event = build_read(
                            &self.builder,
                            &self.server_name,
                            &self.file,
                            self.pos,
                            &info,
                            after,
                            key,
                        );
                        self.builder.set_snapshot_id(&mut event, &self.snapshot_id);
                        rows.push(event);
                    }
                    Err(error) => {
                        if self
                            .tx
                            .send(Message::BadRow {
                                table: unit.table,
                                error,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                if rows.len() == ROWS_PER_MESSAGE {
                    let batch = std::mem::replace(&mut rows, Vec::with_capacity(ROWS_PER_MESSAGE));
                    if self
                        .tx
                        .send(Message::Rows {
                            table: unit.table,
                            events: batch,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            drop(result);
            if !rows.is_empty()
                && self
                    .tx
                    .send(Message::Rows {
                        table: unit.table,
                        events: rows,
                    })
                    .await
                    .is_err()
            {
                return Ok(());
            }
            if self
                .tx
                .send(Message::UnitDone { table: unit.table })
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
}

/// Reads the column facts needed for conversion and schema documents from `information_schema`.
pub(super) async fn column_details(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<HashMap<String, ColumnDetails>, String> {
    type Row9 = (
        String,
        String,
        String,
        String,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<String>,
        u64,
    );
    let rows: Vec<Row9> = conn
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, \
             NUMERIC_SCALE, DATETIME_PRECISION, COLUMN_KEY, ORDINAL_POSITION \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (database, table),
        )
        .await
        .map_err(|error| format!("snapshot: reading columns of `{database}`.`{table}`: {error}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(name, column_type, data_type, is_nullable, precision, scale, fsp, key, position)| {
                let column_type_lower = column_type.to_ascii_lowercase();
                let kind = data_type.to_ascii_lowercase();
                let details = ColumnDetails {
                    position: position as u32,
                    nullable: is_nullable.eq_ignore_ascii_case("YES"),
                    unsigned: column_type_lower.contains("unsigned"),
                    precision: (kind == "decimal" || kind == "numeric")
                        .then(|| precision.map(|p| p as u32))
                        .flatten(),
                    scale: (kind == "decimal" || kind == "numeric")
                        .then(|| scale.map(|s| s as u32))
                        .flatten(),
                    fsp: fsp.map_or(0, |p| p.min(6) as u8),
                    bit_len: parse_bit_len(&column_type_lower).unwrap_or(0),
                    primary_key: key.as_deref() == Some("PRI"),
                    values: table_schema::parse_value_list(&column_type).map(Arc::new),
                    data_type,
                };
                (name, details)
            },
        )
        .collect())
}

/// Converts a snapshot row into the `after` image and the record key.
pub(super) fn convert_row(
    info: &TableInfo,
    row: Row,
    convert: &ConvertOptions,
) -> Result<(Image, Option<Image>), String> {
    let values = row.unwrap();
    let mut after = Map::with_capacity(values.len());
    let mut key = Map::new();
    for (column, value) in info.columns.iter().zip(values.iter()) {
        if column.excluded {
            continue;
        }
        let json = convert_value(column, value, Origin::Snapshot, convert)
            .map_err(|error| error.to_string())?;
        if column.primary_key {
            key.insert(column.name.clone(), json.clone());
        }
        after.insert(column.name.clone(), json);
    }
    Ok((after, (!key.is_empty()).then_some(key)))
}

/// Builds an `op: "r"` event with snapshot marker `true`.
pub(super) fn build_read(
    builder: &EventBuilder,
    server_name: &str,
    file: &str,
    pos: u64,
    info: &TableInfo,
    after: Image,
    key: Option<Image>,
) -> LogEvent {
    let now = Utc::now();
    let source = SourceInfo {
        connector_version: CONNECTOR_VERSION,
        name: server_name,
        ts_us: now.timestamp_micros(),
        snapshot: SnapshotMarker::True,
        db: &info.database,
        table: &info.table,
        server_id: 0,
        gtid: None,
        file,
        pos,
        row: 0,
        query: None,
    };
    builder.row_change(
        Op::Read,
        None,
        Some(after),
        &source,
        EventMeta {
            key,
            database: &info.database,
            table: Some(&info.table),
            op: Op::Read.code(),
            timestamp: now,
            schema_version: Some(&info.schema_version),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_planning() {
        // Dense keys: about rows / chunk_size ranges, contiguous, last one unbounded.
        let ranges = plan_chunks(1, 1000, 1000, 250);
        assert_eq!(
            ranges,
            vec![
                (1, Some(251)),
                (251, Some(501)),
                (501, Some(751)),
                (751, None)
            ]
        );

        // Small tables are read with one query.
        assert_eq!(plan_chunks(1, 100, 100, 250), vec![(1, None)]);

        // Unknown row count falls back to the key span.
        assert_eq!(plan_chunks(1, 1000, 0, 500).len(), 2);

        // Sparse keys are split by estimated rows, not by span.
        let sparse = plan_chunks(0, 1_000_000_000_000, 1000, 250);
        assert_eq!(sparse.len(), 4);
        assert_eq!(sparse.last().unwrap().1, None);

        // Extreme bounds do not overflow, and the chunk count is capped.
        let wide = plan_chunks(i64::MIN, i64::MAX, u64::MAX, 1);
        assert_eq!(wide.len() as u64, MAX_CHUNKS_PER_TABLE);
        assert_eq!(wide[0].0, i64::MIN);

        // Ranges always cover every key exactly once.
        let ranges = plan_chunks(-50, 49, 100, 7);
        let mut covered = 0;
        for k in -50..=49_i64 {
            let hits = ranges
                .iter()
                .filter(|(s, e)| k >= *s && e.is_none_or(|e| k < e))
                .count();
            assert_eq!(hits, 1, "key {k}");
            covered += 1;
        }
        assert_eq!(covered, 100);
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("orders"), "`orders`");
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
    }
}
