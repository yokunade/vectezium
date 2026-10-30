//! The initial snapshot, read from the replication slot's exported snapshot:
//!
//! 1. `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT` creates the slot and exports a snapshot of the
//!    database exactly at the slot's consistent point;
//! 2. each of the `snapshot_max_threads` connections imports it (`SET TRANSACTION SNAPSHOT`)
//!    while the exporting connection is still idle, so all of them see the same instant;
//! 3. tables are read through server-side cursors (`snapshot_fetch_size` rows per round trip),
//!    in parallel, as `op: r` events;
//! 4. streaming then starts at the consistent point: every change after the snapshot comes from
//!    the slot, and none before it. No locks are taken.
//!
//! The snapshot's offset is only stored once every snapshot event has been delivered. If the
//! source stops before that, the slot is replaced and the snapshot taken again.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use chrono::Utc;
use pgrepl_tokio_postgres::{Client, SimpleQueryMessage};
use tokio::sync::mpsc;

use super::{
    catalog::{TableInfo, TypeRegistry, quote_ident},
    connection::{error_text, simple_rows},
    envelope::{Op, SnapshotMarker},
    offset::{Lsn, Offset, TableRef},
    source::{Emitter, EventBuilder, EventMeta, Settings, note_table_schema, row_event, row_map},
};
use crate::{event::LogEvent, shutdown::ShutdownSignal, sources::cdc::control::snapshot_completed};

/// What a reader sends the emitting task.
enum Read {
    Row(usize, LogEvent),
    Done(usize, u64),
    Failed(String),
}

/// A new snapshot id (sortable, unique per snapshot).
fn new_snapshot_id() -> String {
    Utc::now().format("%Y%m%dT%H%M%S%.6fZ").to_string()
}

/// Takes the snapshot. Returns the offset to stream from, or `None` if the source was shut down.
pub(super) async fn run(
    settings: Arc<Settings>,
    query: &Client,
    registry: &TypeRegistry,
    emitter: &mut Emitter,
    captured: &[TableRef],
    system_id: &str,
    mut shutdown: ShutdownSignal,
) -> Result<Option<Offset>, String> {
    // 1. The slot and its exported snapshot.
    let repl = settings.conn.connect(true).await?;
    let created = simple_rows(
        &repl,
        &format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
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
    let row = created.first().ok_or("CREATE_REPLICATION_SLOT returned nothing")?;
    let lsn: Lsn = row
        .get(1)
        .cloned()
        .flatten()
        .ok_or("CREATE_REPLICATION_SLOT returned no position")?
        .parse()?;
    let snapshot_name = row
        .get(2)
        .cloned()
        .flatten()
        .ok_or("CREATE_REPLICATION_SLOT exported no snapshot")?;

    // The tables as the catalog describes them now.
    let mut tables = Vec::with_capacity(captured.len());
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
            settings.warn_if_keyless(&info);
            tables.push(Arc::new(info));
        }
    }

    // 2. Every reader imports the snapshot before the exporting connection does anything else.
    let threads = settings.snapshot_threads.min(tables.len()).max(1);
    let mut readers = Vec::with_capacity(threads);
    for _ in 0..threads {
        let client = settings.conn.connect(false).await?;
        client
            .batch_execute(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'",
                snapshot_name.replace('\'', "''")
            ))
            .await
            .map_err(|error| format!("failed to import the slot's snapshot: {}", error_text(&error)))?;
        readers.push(client);
    }
    drop(repl);
    info!(
        message = "Created the replication slot; snapshotting tables.",
        slot = %settings.slot_name,
        lsn = %lsn,
        tables = tables.len(),
        threads,
    );

    let snapshot_id = new_snapshot_id();
    let started_seq = emitter.next_seq();
    let started_ms = Utc::now().timestamp_millis();
    let previous_batch = emitter.set_batch_size(settings.snapshot_batch_size);
    for info in &tables {
        note_table_schema(emitter, &settings, info, "snapshot", Some(lsn));
    }

    // 3. Readers take tables from a shared queue.
    let queue = Arc::new(Mutex::new((0..tables.len()).collect::<VecDeque<_>>()));
    let (tx, mut rx) = mpsc::channel::<Read>(4096);
    let builder = emitter.builder();
    let mut tasks = Vec::with_capacity(threads);
    for client in readers {
        let (queue, tx, tables, settings) = (
            Arc::clone(&queue),
            tx.clone(),
            tables.clone(),
            Arc::clone(&settings),
        );
        let snapshot_id = snapshot_id.clone();
        tasks.push(tokio::spawn(async move {
            let result = read_tables(&client, &queue, &tx, &tables, &settings, builder, lsn, &snapshot_id).await;
            if let Err(error) = result {
                _ = tx.send(Read::Failed(error)).await;
            }
            _ = client.batch_execute("COMMIT").await;
        }));
    }
    drop(tx);

    // The emitting side: one row of each table is held back, so the last row of a table (and of
    // the whole snapshot) can be marked as such.
    let mut held: HashMap<usize, LogEvent> = HashMap::new();
    let mut first = true;
    let mut done_tables = 0;
    let mut rows_total = 0u64;
    let mark = |event: &mut LogEvent, marker: SnapshotMarker| builder.set_snapshot_marker(event, marker);
    loop {
        let read = tokio::select! {
            _ = &mut shutdown => {
                for task in &tasks {
                    task.abort();
                }
                info!(message = "Shut down during the snapshot; it will be taken again on the next start.");
                return Ok(None);
            }
            read = rx.recv() => read,
        };
        match read {
            None => break,
            Some(Read::Failed(error)) => {
                for task in &tasks {
                    task.abort();
                }
                return Err(format!("snapshot failed: {error}"));
            }
            Some(Read::Row(table, event)) => {
                if let Some(mut previous) = held.insert(table, event) {
                    if first {
                        mark(&mut previous, SnapshotMarker::First);
                        first = false;
                    }
                    emitter.push_snapshot_row(previous);
                }
            }
            Some(Read::Done(table, rows)) => {
                done_tables += 1;
                rows_total += rows;
                if let Some(mut last) = held.remove(&table) {
                    let marker = if done_tables == tables.len() {
                        SnapshotMarker::Last
                    } else if first {
                        SnapshotMarker::First
                    } else {
                        SnapshotMarker::LastInDataCollection
                    };
                    first = false;
                    mark(&mut last, marker);
                    emitter.push_snapshot_row(last);
                }
                let info = &tables[table];
                if settings.snapshot_end_markers {
                    emitter.push_control(
                        snapshot_completed(
                            &settings.topic_prefix,
                            &info.schema,
                            &info.table,
                            &snapshot_id,
                            started_seq,
                            started_ms,
                            rows,
                            Some(&info.schema_version),
                        ),
                        EventMeta {
                            key: None,
                            database: &info.schema,
                            table: Some(&info.table),
                            op: "snapshot_completed",
                            timestamp: Utc::now(),
                            schema_version: None,
                        },
                        true,
                    );
                }
                info!(message = "Snapshotted table.", table = %format!("{}.{}", info.schema, info.table), rows);
            }
        }
        emitter.maybe_flush().await?;
        emitter.check_health()?;
    }

    // 4. Done: stream from the consistent point.
    let mut offset = Offset::new(lsn, &settings.slot_name, Some(system_id.to_owned()));
    offset.snapshot_completed = true;
    offset.snapshotted_tables = tables.iter().map(|t| (t.schema.clone(), t.table.clone())).collect();
    emitter.commit(offset.clone());
    emitter.flush().await?;
    emitter.set_batch_size(previous_batch);
    info!(message = "Snapshot complete.", tables = tables.len(), rows = rows_total, lsn = %lsn);
    Ok(Some(offset))
}

/// One reader: takes tables from `queue` and sends their rows.
#[allow(clippy::too_many_arguments)]
async fn read_tables(
    client: &Client,
    queue: &Mutex<VecDeque<usize>>,
    tx: &mpsc::Sender<Read>,
    tables: &[Arc<TableInfo>],
    settings: &Settings,
    builder: EventBuilder,
    lsn: Lsn,
    snapshot_id: &str,
) -> Result<(), String> {
    loop {
        let Some(index) = queue.lock().ok().and_then(|mut q| q.pop_front()) else {
            return Ok(());
        };
        let info = &tables[index];
        let name = format!("{}.{}", quote_ident(&info.schema), quote_ident(&info.table));
        let columns = info
            .columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        client
            .batch_execute(&format!("DECLARE vtz_snapshot NO SCROLL CURSOR FOR SELECT {columns} FROM {name}"))
            .await
            .map_err(|error| format!("failed to read {name}: {}", error_text(&error)))?;
        let fetch = format!("FETCH {} FROM vtz_snapshot", settings.fetch_size);
        let now = Utc::now();
        let mut rows = 0u64;
        loop {
            let messages = client
                .simple_query(&fetch)
                .await
                .map_err(|error| format!("failed to read {name}: {}", error_text(&error)))?;
            let mut fetched = 0;
            for message in messages {
                let SimpleQueryMessage::Row(row) = message else {
                    continue;
                };
                fetched += 1;
                let after = match row_map(settings, info, |i| row.get(i)) {
                    Ok(after) => after,
                    Err(error) => {
                        settings.handle_failure(error, &info.schema, &info.table)?;
                        continue;
                    }
                };
                let source = settings.source_info(
                    info,
                    now.timestamp_micros(),
                    SnapshotMarker::True,
                    None,
                    Some(lsn),
                    None,
                );
                let mut event = row_event(&builder, info, Op::Read, None, Some(after), &source, now);
                builder.set_snapshot_id(&mut event, snapshot_id);
                rows += 1;
                if tx.send(Read::Row(index, event)).await.is_err() {
                    return Ok(());
                }
            }
            if fetched < settings.fetch_size {
                break;
            }
        }
        client
            .batch_execute("CLOSE vtz_snapshot")
            .await
            .map_err(|error| format!("failed to read {name}: {}", error_text(&error)))?;
        if tx.send(Read::Done(index, rows)).await.is_err() {
            return Ok(());
        }
    }
}
