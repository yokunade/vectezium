# Vectezium CDC sources: `mysql_cdc` and `postgres_cdc`

Two Vector sources that do what Debezium Server's MySQL and PostgreSQL connectors do (a
consistent snapshot, then streaming changes from the MySQL binlog or a PostgreSQL logical
replication slot, as Debezium-format change events) inside a single Vector binary. Everything
Vector can do with events (VRL transforms, routing, disk buffers, the `azure_blob` sink for ADLS
Gen2, S3, Kafka, …) applies directly.

Both connectors share one core (`src/sources/cdc/`), so most of this document applies to both:
event formats, `seq`, control events, published offsets, snapshots, signals, the single-writer
lock and offset recovery. Where they differ, the section says so. The connector-specific parts
are [MySQL](#mysql) and [PostgreSQL](#postgresql).

> **Status: tested locally and in CI.** Built on Vector v0.60.0. Unit tests pass in CI, and so
> do the integration tests against PostgreSQL 13, 17 and 18 and MySQL 8.0 and 8.4, with
> Azurite. Not yet tested against Azure Database for
> MySQL or PostgreSQL or real ADLS, or for long runs.

## Build

Vectezium is a fork of Vector v0.60.0 (`vectezium` branch of
[yokunade/Vectezium](https://github.com/yokunade/Vectezium)).

```bash
git clone -b vectezium https://github.com/yokunade/Vectezium.git && cd Vectezium

# Small binary with just what the pipeline needs (add transforms-remap etc. for VRL):
cargo build --release --locked --no-default-features \
  --features "sources-mysql_cdc,sources-postgres_cdc,sinks-azure_blob,sinks-file"

# Unit tests for the CDC code, as CI runs them:
cargo test --locked --lib --no-default-features \
  --features "sources-mysql_cdc,sources-postgres_cdc,sinks-azure_blob" -- \
  sources::cdc sources::mysql_cdc sources::postgres_cdc sinks::azure_blob
```

Either source can be built alone (`sources-mysql_cdc` or `sources-postgres_cdc`). Building Vector
needs Rust (the version in `rust-toolchain.toml`), `protoc`, `clang` and `cmake`; see
`docs/DEVELOPING.md`. CI (`.github/workflows/vectezium.yml`) also runs `cargo fmt --check` and
clippy with `-D warnings`.

The integration tests run each source in the test process against a real PostgreSQL or MySQL,
and Azurite, from `tests/integration/cdc/compose.yaml` (`PG_VERSION` and `MYSQL_VERSION` pick
the versions, default 17 and 8.4). Each test uses its own database (and slot, or replica server
ID), so they run in parallel; recreate the servers (`down -v`) between local runs, as every run
leaves its databases and slots behind. CI runs them on PostgreSQL 13, 17 and 18 and MySQL 8.0
and 8.4 (`.github/workflows/vectezium-integration.yml`).

```bash
docker compose -f tests/integration/cdc/compose.yaml up -d --wait
cargo test --locked --lib --no-default-features --features postgres_cdc-integration-tests --   sources::postgres_cdc::integration_tests
cargo test --locked --lib --no-default-features --features mysql_cdc-integration-tests --   sources::mysql_cdc::integration_tests
```

The `azure_blob` integration tests, including the ones for the options Vectezium adds, run
against Azurite:

```bash
AZURITE_ADDRESS=localhost cargo test --locked --lib --no-default-features \
  --features azure-blob-integration-tests -- sinks::azure_blob::integration_tests
```

## Test a release binary end to end (Docker, MySQL)

The integration tests above cover the source itself. This script runs a release binary with a
whole pipeline, and also checks the blobs the `azure_blob` sink writes.

```bash
cd contrib/mysql_cdc
./run-e2e.sh ../../target/release/vector
```

This starts MySQL 8.4 (configured for CDC) and Azurite (a local Blob/ADLS emulator), runs
`vector-e2e.yaml`, and checks the snapshot, type conversions, inserts/updates/deletes, primary-key
changes, rolled-back transactions, DDL, filters, restart/resume, the single-writer lock, and the gzip NDJSON blobs written
to Azurite. Requires Docker and `jq`.

## Load test (Docker, MySQL)

```bash
cd contrib/mysql_cdc
./run-load.sh ../../target/release/vector 500 300 5   # changes/s, seconds, tables
```

Seeds the tables (snapshot input), generates a steady mix of inserts, updates and deletes (some
in multi-row transactions), hard-kills Vector with SIGKILL halfway through and restarts it, then
checks against MySQL that every change arrived and that replaying the captured changes reproduces
each table exactly. It reports duplicates, capture lag (commit to capture) percentiles,
throughput, and Vector's memory and CPU use (`out/load-report.json`). Requires Docker, `jq` and
`python3`.

## Parallel snapshots

`snapshot_max_threads` (default `1`) reads tables on that many connections at once, and the
result is identical to a single-threaded snapshot:

* **MySQL:** every reader starts its consistent-snapshot transaction while the global read lock
  is held, so they all see the database at the same instant. Tables with a single-column integer
  primary key are split into primary-key ranges of about `snapshot_chunk_size` rows (default
  1,000,000; `0` disables chunking), so one large table is also spread across the threads.
* **PostgreSQL:** every reader imports the snapshot the replication slot exported when it was
  created, so the snapshot ends exactly where streaming starts. Each table is read through a
  server-side cursor, `snapshot_fetch_size` rows (default 10,000) per round trip; tables are not
  split.

Event building happens on the reader threads; Debezium's first/last snapshot markers are
preserved. Each reader is a long-running read transaction; on MySQL consider snapshotting from a
replica, and on PostgreSQL note that it holds back `VACUUM` while it runs (or use
`snapshot_strategy: incremental`).

### Snapshot benchmark (MySQL)

```bash
./run-snapshot-bench.sh ../../target/release/vector 100000000 5 8   # 5 x 100M rows, 8 threads
```

Seeds the tables on the server (`INSERT … SELECT`, binlog off for the seed), takes one snapshot
(`snapshot_mode: initial_only`) into local gzip NDJSON files, and checks every table's row count,
`SUM(id)` and `SUM(val)` against MySQL plus the snapshot markers. It checks free disk first and
refuses to start if the run will not fit (about 560 bytes per row: ~490 in InnoDB, ~70 of
gzip output).

## MySQL

### Server requirements

MySQL 8.0 or newer with:

```ini
log_bin              = mysql-bin
binlog_format        = ROW
binlog_row_image     = FULL
binlog_row_metadata  = FULL     # required: row events carry column names and ENUM/SET values
gtid_mode            = ON       # recommended: resume works across failovers
enforce_gtid_consistency = ON
```

User privileges: `SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT`.
The source checks these settings at start-up and fails with a clear message if one is wrong.

### How it maps to Debezium

| Debezium MySQL connector                  | `mysql_cdc`                                            |
|-------------------------------------------|--------------------------------------------------------|
| `database.hostname/port/user/password`    | `hostname`, `port`, `user`, `password`                 |
| `database.server.id`                      | `server_id`                                            |
| `topic.prefix`                            | `topic_prefix` (→ `source.name`)                       |
| `database/table.include/exclude.list`     | `database_include_list`, `table_include_list`, …       |
| `column.exclude.list`                     | `column_exclude_list`                                  |
| `snapshot.mode` initial / initial_only / no_data / when_needed | `snapshot_mode` (same values)     |
| `snapshot.locking.mode` minimal / none    | `snapshot_locking_mode`                                |
| `decimal.handling.mode`                   | `decimal_handling_mode` (`precise` default, `string`, `double`) |
| `binary.handling.mode`                    | `binary_handling_mode` (`base64`, `base64_url_safe`, `hex`) |
| `time.precision.mode`                     | `time_precision_mode` (`adaptive_time_microseconds`, `connect`) |
| `include.schema.changes`                  | `include_schema_changes` → `control` output            |
| `include.query`                           | `include_query`                                        |
| `event.processing.failure.handling.mode`  | `event_processing_failure_handling_mode`               |
| `heartbeat` / connection keepalive        | `heartbeat_interval_secs`                              |
| `ExtractNewRecordState` SMT (flattening)  | `event_format: record`, `record_images`                |
| `max.batch.size` (snapshot vs streaming)  | `batch_size`, `snapshot_batch_size`                    |
| Offset store (file / Kafka)               | `offsets.json` in Vector's `data_dir`, written atomically |
| Kafka message key                         | `%mysql_cdc.key` event metadata (primary-key columns)  |

Each event body is the Debezium payload as the JSON converter emits it with
`schemas.enable=false`: `before`, `after`, `source` (`version`, `connector`, `name`, `ts_ms`,
`snapshot`, `db`, `table`, `server_id`, `gtid`, `file`, `pos`, `row`, `thread`, `query`, `seq`, …),
`transaction`, `op` (`r`/`c`/`u`/`d`), `ts_ms`, `ts_us`, `ts_ns`. With Vector's default (legacy)
log namespace, Vector also adds `source_type` and `timestamp` (the commit time) to each event.
Value representations follow Debezium's defaults; see the table in `mysql_cdc/convert.rs`.

## PostgreSQL

### Server requirements

PostgreSQL 13 or newer with `wal_level = logical` (a restart is needed to change it), and room for
one more replication slot and WAL sender (`max_replication_slots`, `max_wal_senders`). Set
`max_slot_wal_keep_size` so a stopped source can't fill the disk with WAL; a slot that falls
further behind is invalidated, and the source then re-snapshots (`snapshot_mode: when_needed`) or
stops.

The user needs the `REPLICATION` attribute and `SELECT` on the captured tables. To let the source
create its publication (`publication_autocreate_mode`, below) it also needs `CREATE` on the
database and ownership of the tables (`filtered`), or superuser (`all_tables`). The source checks
the version, `wal_level` and the replication attribute at start-up and fails with a clear message
if one is wrong.

### Slot and publication

One source captures one database through one logical replication slot (`slot_name`, default
`vectezium`, using `pgoutput`) and one publication (`publication_name`, default `vectezium`).

* **The slot** is created on the first start. It holds WAL until the source has stored (or
  published and delivered) an offset past it, so offsets never get ahead of the lake. Give every
  source on a database its own `slot_name`: the source only drops and re-creates an existing slot
  (to snapshot again) when its own stored offset names that slot, or the slot was invalidated.
  Finding a slot it has no offset for, it stops with an error instead of taking it over. If a
  snapshot fails, the slot it created is dropped again.
* **The publication** decides which tables are streamed; the include and exclude lists then
  filter further. `publication_autocreate_mode`:

  | Mode | |
  |---|---|
  | `filtered` (default) | Create it `FOR TABLE` the captured tables if it does not exist. Tables without a replica identity are left out, with a warning, because PostgreSQL rejects every `UPDATE` and `DELETE` on a published table that has none. |
  | `all_tables` | Create it `FOR ALL TABLES`. Refused while any table lacks a replica identity. Tables created later are published too. |
  | `disabled` | The publication must exist; the source never creates or changes it. |

  Publications the source creates use `publish_via_partition_root = true`, so a partitioned
  table's changes arrive as the table's, not its partitions'. For a publication you create
  yourself, set it too; the source warns about captured partitioned tables otherwise. Tables
  that match the include lists but are not in the publication are logged at start.
* **Replica identity:** without `REPLICA IDENTITY FULL`, an update carries no before image, a
  delete carries only the key, and a large (TOASTed) value the update did not change arrives as
  `unavailable_value_placeholder` (default `__debezium_unavailable_value`, as in Debezium). The
  source warns about such tables when the event format includes before images.
* **Generated columns** are streamed only when the publication publishes them (PostgreSQL 18,
  `publish_generated_columns = stored`); the snapshot includes them exactly when the stream
  does, so both have the same columns.

`status_update_interval_secs` (default 10) is how often the source reports its position to the
server; keep it well below `wal_sender_timeout`.

### How it maps to Debezium

| Debezium PostgreSQL connector             | `postgres_cdc`                                         |
|-------------------------------------------|--------------------------------------------------------|
| `database.hostname/port/user/password/dbname` | `hostname`, `port`, `user`, `password`, `database` |
| `topic.prefix`                            | `topic_prefix` (→ `source.name`)                       |
| `slot.name`, `publication.name`           | `slot_name`, `publication_name`                        |
| `publication.autocreate.mode`             | `publication_autocreate_mode` (`filtered` default, `all_tables`, `disabled`) |
| `schema/table.include/exclude.list`       | `schema_include_list`, `table_include_list`, …         |
| `column.exclude.list`                     | `column_exclude_list`                                  |
| `message.key.columns`                     | `pk_override`                                          |
| `snapshot.mode` initial / initial_only / no_data / when_needed | `snapshot_mode` (same values)     |
| `decimal.handling.mode`                   | `decimal_handling_mode` (`precise` default, `string`, `double`; also `MONEY`) |
| `binary.handling.mode`                    | `binary_handling_mode` (`base64`, `base64_url_safe`, `hex`) |
| `time.precision.mode`                     | `time_precision_mode` (`adaptive_time_microseconds`, `connect`) |
| `interval.handling.mode`                  | `interval_handling_mode` (`numeric`, `string`)         |
| `money.fraction.digits`                   | `money_fraction_digits`                                |
| `unavailable.value.placeholder`           | `unavailable_value_placeholder`                        |
| `status.update.interval.ms`               | `status_update_interval_secs`                          |
| `database.sslmode`, `database.sslrootcert` | `ssl_mode` (`verify_full` default), `ssl_ca_file`     |
| `event.processing.failure.handling.mode`  | `event_processing_failure_handling_mode`               |
| `ExtractNewRecordState` SMT (flattening)  | `event_format: record`, `record_images`                |
| Offset store (file / Kafka)               | `offsets.json` in Vector's `data_dir`, written atomically |
| Kafka message key                         | `%postgres_cdc.key` event metadata (key columns)       |

The `source` block is Debezium's for PostgreSQL (`connector: "postgresql"`, `db`, `schema`,
`table`, `txId`, `lsn`, `sequence`, …, plus `seq`); with `event_format: record`, `metadata` has
`database`, `db` (the schema), `table`, `txid` and `lsn`. Value representations follow Debezium's
defaults; see the table in `postgres_cdc/convert.rs`.

## Event formats

`event_format: debezium` (default) emits Debezium's payload, as described in each connector's
section.

`event_format: record` emits one object per change with the row in `record` and everything
about the change in `metadata` (a MySQL example; the position fields after `captured_ts_ms` are
each connector's, see [PostgreSQL](#how-it-maps-to-debezium-1)):

```json
{
  "metadata": {
    "op": "u", "seq": "00000000000000009001.0000000000", "deleted": false, "snapshot": "false", "server_name": "prod-mysql",
    "db": "inventory", "table": "customers", "key": {"id": 1},
    "commit_ts_ms": 1792923843026, "commit_ts_us": 1792923843026123, "captured_ts_ms": 1792923843101,
    "server_id": 1, "gtid": "3e11fa47-…:42", "file": "mysql-bin.000003", "pos": 1543, "row": 0,
    "query": null
  },
  "record": {
    "before": {"id": 1, "name": "Alice"},
    "after": {"id": 1, "name": "Alicia"}
  }
}
```

What `record` holds depends on `record_images`:

| `record_images` | `record` | Insert, snapshot read | Update | Delete (`metadata.deleted: true`) |
|---|---|---|---|---|
| `after` (default) | the row | the new row | the row after the change | the deleted row |
| `before_and_after` | `{"before": …, "after": …}` (as above) | `before: null` | both images | `before`: the deleted row; `after`: its key only |

* A delete's `after` holds the key columns of the deleted row: the primary key, or the
  `pk_override` columns (the same fields as `metadata.key`). A table without a key gets `null`.
  So every event carries the row's key in `record.after`: merge downstream on the key fields of
  `record.after` and treat `metadata.op = "d"` as a delete. The deleted row itself is in
  `record.before`.
* Vector adds `source_type` and `timestamp` to every event; drop them in the sink with
  `encoding.except_fields: ["source_type", "timestamp"]` to keep rows to `metadata` + `record`.
* Field names inside `record` come out in alphabetical order (Vector's event model); the column
  order is in the table's schema document.

## Ordering, control events and published offsets

### `seq`

Every event has **`seq`** (`metadata.seq`, or `source.seq` with `event_format: debezium`): one
master sequence per source, as a fixed-width string that sorts correctly as text:

```
00000000000000009001.0000000000    change 9001
00000000000000009001.0000000042    42nd snapshot row emitted after it
00000000000000009002.0000000000    the next change
```

* The **first part** (20 digits) counts events from the change stream (the binlog or the
  WAL): changes, `truncate` and
  `schema_change`. Each advances it by one, and it is stored with the offset, so events replayed
  after a crash get the **same** `seq`: a repeated change is identical, `seq` included.
* The **second part** (10 digits) positions snapshot-derived events (`r` rows and
  `snapshot_completed`) after the latest streamed event, without advancing the counter, because
  when a snapshot chunk is emitted differs between runs. So, unlike a change, a snapshot read
  does **not** keep its `seq` across a crash (see [Duplicates](#duplicates)).
* Apply events in `seq` order (`AUTO CDC … SEQUENCE BY seq`); no other field is needed. For a
  number, `CAST(seq AS DECIMAL(30,10))` is exact.

### Duplicates

Delivery is at-least-once: after a crash, events since the last stored offset arrive again.
Drop the repeats by kind of event:

| Events | A repeat | Deduplicate on |
|---|---|---|
| Changes (`c`, `u`, `d`), `truncate`, `schema_change` | Is identical, `seq` included | `seq` |
| Snapshot reads (`r`) | May have a different `seq`, and its `seq` may belong to a different row in each delivery | `(seq, snapshot_id, key)`, or keep both |
| `snapshot_completed` | May have a different `seq` | `(snapshot_id, db, table)` |

**Never deduplicate snapshot reads on `seq` alone.** A snapshot read again after a crash numbers
its rows afresh: a blocking snapshot's parallel readers emit rows in a different order each run,
and a resumed incremental snapshot (same `snapshot_id`) may emit a chunk at a different point in
the stream, with different rows. One `seq` can then name one row in the first delivery and
another in the second, and dropping the second as a repeat loses a real row. With the key
included the rows stay apart. A snapshot read delivered twice is harmless when applied with
`SEQUENCE BY seq`: each copy is consistent with the changes around it, and the later `seq` wins.

### Row events are CRUD only

| `op` | Meaning | `record` / `after` |
|---|---|---|
| `r` | Snapshot read. Carries `snapshot_id` (in `metadata`, or `source` for `debezium`). | the row |
| `c`, `u`, `d` | Insert, update, delete. | the row (for `d`: the deleted row) |

### Control events (`control` output)

Events about a table or the connector, each with `seq` (in the same numbering as row events):

| `type` | When | Fields |
|---|---|---|
| `schema_change` | MySQL: DDL on captured objects. PostgreSQL: a captured table's columns changed (it streams no DDL). Both need `include_schema_changes`. | `db`, `kind`; MySQL: `ddl`, `objects`, `data_changed`, `gtid`, `file`, `pos`; PostgreSQL: `table`, `columns_added`, `columns_removed`, `columns_retyped`, `txid`, `lsn` |
| `truncate` | `TRUNCATE TABLE` on a captured table (it has no row events) | `db`, `table`; MySQL: `gtid`, `file`, `pos`; PostgreSQL: `cascade`, `restart_identity`, `txid`, `lsn` |
| `snapshot_completed` | The snapshot of a table is complete (`snapshot_end_markers`) | `db`, `table`, `snapshot_id`, `started_seq`, `rows` |
| `snapshot_held` | At start, for each table whose unplanned snapshot is held (`unplanned_snapshots: hold`) | `db`, `table`, `reason` |

### Published offsets (`offsets` output, `publish_offsets: true`)

After each offset save, which happens only once every event up to a transaction boundary has
been delivered, the source publishes it (at most every `offset_publish_interval_secs`):

```json
{"connector": "prod-mysql", "seq": "00000000000000009100.0000000000", "change_seq": 9100,
 "gtid_set": "3e11fa47-…:1-5002",
 "file": "mysql-bin.000412", "pos": 88231, "snapshot_in_progress": false,
 "committed_at": "2026-10-26T09:40:12.418Z", "checkpoint": {…}}
```

The position fields are the connector's: `gtid_set`, `file` and `pos` for MySQL, `lsn` and
`slot_name` for PostgreSQL. PostgreSQL also publishes when its position moves without a new
`seq` (the slot advancing past transactions on tables it does not capture), so the slot can
release WAL.

Every event with `seq` ≤ the published `seq` is delivered, and every transaction it belongs to
is complete, across all tables. Published offsets are not acknowledged, so a slow offsets sink
never holds back checkpoints. Requires end-to-end acknowledgements.

`checkpoint` is the whole stored offset (what `offsets.json` holds: position, `seq` counters,
snapshotted tables, incremental-snapshot progress, held tables), so a source that lost its data
directory can resume from it (`offset_recovery`, below).

The source keeps one published offset in flight at a time: the next one is published only after
the last was delivered or failed. To keep the latest one in a single blob (`latest.json`), use
an `azure_blob` sink with `batch.max_events: 1`, `blob_overwrite: true` and
`blob_overwrite_order_by: ["seq", "committed_at"]`, so a request that times out and lands late
can never replace a newer offset (see [`azure_blob` options](#azure_blob-sink-options)).

### Unacknowledged copies (`observe` output, `observe_output: true`)

Acknowledgements wait for every sink an event goes to, so a sink that is only there to watch
(a live view, a debug file) can stall or stop capture: if it is slow, offsets wait; if it fails,
the source stops. Route such sinks from `observe` instead. It carries a copy of every event of
the default, `control` and `table_schemas` outputs, sent after the originals and without
acknowledgement, so whatever happens to it never affects offsets or the other outputs:

```yaml
sinks:
  live:
    type: file
    inputs: ["mysql.observe", "mysql.offsets"]   # offsets are never acknowledged either
    path: /var/log/vectezium/live.ndjson
    encoding: { codec: json }
    buffer: { type: memory, max_size: 33554432, when_full: drop_newest }
```

Give these sinks a buffer that drops when full, as above: the copies are not acknowledged, but
a blocking buffer would still make the source wait to hand them over.

### Applying it downstream (for example Databricks)

* **Per table:** a CRUD merge keyed on `metadata.key`, `SEQUENCE BY seq`, with `d` as delete
  (Databricks `AUTO CDC`). If you deduplicate before it, use the keys in
  [Duplicates](#duplicates), not `seq` alone.
* **`truncate`:** delete the table's rows with a lower `seq` than the event (or feed it to
  `apply_as_truncates` for SCD type 1).
* **`snapshot_completed`:** once you hold `rows` rows of the table with its `snapshot_id`, delete
  every key the snapshot did not contain whose last change has `seq < started_seq`. This removes
  rows deleted while the change stream was unavailable (a re-snapshot after a purged binlog or
  a lost slot produces no `d` events for them). `started_seq` is exact; no clocks are involved.
* **Whole transactions:** apply (or show) only rows with `seq ≤` the latest published offset. With
  an SCD type 2 target sequenced by `seq`, an "as of commit" view
  (`__START_AT <= c.seq AND (__END_AT IS NULL OR __END_AT > c.seq)`) is consistent across tables.

## Snapshot strategies and new tables

| Setting | Default | |
|---|---|---|
| `snapshot_strategy` | `blocking` | How the initial snapshot (and a re-snapshot after a purged binlog or a lost slot) reads tables. |
| `snapshot_new_tables` | `false` | Snapshot tables that become captured later. |
| `incremental_snapshot_chunk_size` | `4096` | Rows per incremental chunk. |
| `pk_override.<db or schema>.<table>` | — | Key columns, e.g. `"call_ref, site"`. |

**`blocking`** reads every table in one consistent snapshot before streaming starts (see
*Parallel snapshots*). Its long transactions hold table locks (MySQL metadata locks,
PostgreSQL `ACCESS SHARE`): an `ALTER TABLE` on a table being read waits until the snapshot
ends, and writes queue behind that `ALTER`.

**`incremental`** starts streaming immediately and reads tables in key order, in chunks of
`incremental_snapshot_chunk_size` rows, between streamed transactions, using the read-only form
of the DBLog watermark algorithm that Debezium also uses. A streamed change to one of the
chunk's keys, from a transaction the chunk could not see, replaces the chunk's copy, and the
chunk is only emitted once the stream has passed every transaction it could see:

* **MySQL:** each chunk is one short query bracketed by two reads of `@@GLOBAL.gtid_executed`.
  Needs GTID mode.
* **PostgreSQL:** each chunk is one short `REPEATABLE READ` transaction that also reads
  `pg_current_snapshot()` (which transactions it sees) and then `pg_current_wal_insert_lsn()`
  (how far the stream must get). Locks last one chunk; a snapshot row never
overwrites a newer change. Progress (the last key delivered) is stored with the offset, so a
restart resumes from the last delivered chunk. Rows have `metadata.snapshot: "incremental"`,
and each table ends with a `snapshot_completed` control event. Needs a primary key (or
`pk_override`) on every table; tables without one are skipped with a warning. In tests, 1M rows
took about a minute on MySQL with 400 writes/s running, and an `ALTER TABLE` during the
snapshot took 0.1 s.

**`snapshot_new_tables: true`** snapshots, incrementally, tables that become captured after the
initial snapshot: tables added to the include lists (or, on PostgreSQL, to the publication),
found at start-up, and on MySQL tables renamed into a captured database while streaming. Tables
created while streaming need no snapshot (all their rows come from the stream), and on MySQL an
online schema-change swap (gh-ost, pt-osc) keeps the table's history. With `false`, such tables
only get changes from the moment they are captured, and the log lists them at every start.

The offset file records which tables have been snapshotted. Offsets written by earlier versions
do not have this; on the first start with this version, the tables captured at that moment are
treated as snapshotted.

### Unplanned snapshots (`unplanned_snapshots`)

Planned snapshots always run: the first start, tables added to the include lists
(`snapshot_new_tables`), and tables an operator queues (an `execute-snapshot` signal, or removing
them from `snapshotted_tables` in the offset file while the source is stopped). Two other situations would
snapshot tables nobody asked for; with `unplanned_snapshots: hold` (the default) they wait for an
operator instead:

| Situation | `hold` | `allow` |
|---|---|---|
| The change stream can't resume from the stored offset (`snapshot_mode: when_needed`): MySQL's binlog was purged past it, or PostgreSQL's slot was invalidated, dropped, or has already released the WAL after it. Every table must be read again | The source refuses to start until `"resnapshot_approved": true` is set in the offset file (it is used once) | Re-snapshot every table |
| An offset was recovered without its table-snapshot state (see *Offset recovery*) | The tables are streamed but not snapshotted: listed in `held_tables` of the offset, logged, and announced with a `snapshot_held` control event at every start | Snapshot them as new tables |

To release a held table, send an `execute-snapshot` signal for it (it is snapshotted) or a
`dismiss-held` signal (it is treated as complete); see *Signals*. Without `signals`, stop the
source and either remove it from `held_tables` (it is then snapshotted as a new table) or move it
to `snapshotted_tables`. With `allow`, held tables are released on the next start.

With `signals` configured, a source that needs approval for a full re-snapshot waits for an
`approve-resnapshot` signal instead of refusing to start (it holds the lock meanwhile, and
writes `waiting.json` in the signal folder).

### Signals (`signals`)

Operator requests a running source takes from files, without a restart: Debezium's file signal
channel, one file per signal. `postgres_cdc` takes the same signals.

```yaml
signals:
  folder: /var/lib/vectezium/signals/mysql   # pending/ and processed/ are created in it
  poll_interval_secs: 2
```

Write a file to `<folder>/pending/` (write it under a name starting with `.` or ending in
`.tmp`, then rename it, so it is never read half-written). Signals are applied in the order
they were written (by modification time, which a rename keeps; then by name), whatever they are
called:

```json
{"id": "8f0c…", "type": "execute-snapshot",
 "data": {"data-collections": ["shop.orders", "shop.customers"], "type": "incremental"}}
```

| `type` | `data` | Effect |
|---|---|---|
| `execute-snapshot` | `data-collections`; `type` (optional) must be `incremental` | Queue the tables for an incremental snapshot (after any already queued). Releases them if held. MySQL needs GTID mode. |
| `stop-snapshot` | `data-collections` (optional) | Drop the tables (without `data-collections`: every table) from the queue. Rows already emitted stay; the table gets no `snapshot_completed`. |
| `dismiss-held` | `data-collections` | Treat held tables as snapshotted, without reading them (Vectezium only). |
| `approve-resnapshot` | — | Approve the full re-snapshot the source is waiting for at start-up (Vectezium only). |

`data` may also be a JSON string, as in Debezium's signal table. Tables must be captured (by the
include and exclude lists). To capture a new table, add it to the include list and redeploy:
with `snapshot_new_tables: true` it is snapshotted on start.

Within `poll_interval_secs` the source applies each signal between transactions, commits an
offset recording it (`last_signal`), and moves the file to `<folder>/processed/`, named
`<seq>-<name>` with a 12-digit sequence number in the order signals were applied, with its
result added:

```json
"result": {"status": "applied", "message": "queued shop.orders", "ts_ms": 1767225600000}
```

A rejected signal (unknown type, a table that is not captured, invalid JSON) is moved there too,
with `"status": "rejected"` and the reason; every signal is also logged. The latest 200 processed
files are kept. If the source stops before the offset recording a signal is stored, it applies
the signals in `processed/` numbered after its stored `last_signal` again at the next start; applying a
signal twice does the same as applying it once. (An approval is not applied again: it is only
good for the start it was given to.)

## Schema changes and other statements (MySQL)

With `include_schema_changes: true`, the `control` output carries `schema_change` events for DDL
on tables, indexes, views, procedures/functions, triggers, events and databases, plus `TRUNCATE`.
Each has the statement (`ddl`), a `kind` (`table`, `index`, `view`, `routine`, `trigger`,
`event`, `database`, `truncate`), the captured `objects` it touches, and `db` set to the database
the statement *targets*. A statement is emitted only when it touches a captured database (and, for
table-level statements, a captured table), whatever the session's default database was, so
`USE other; ALTER TABLE appdb.t …` is emitted and `USE appdb; CREATE TABLE other.t …` is not.

Statements that change no schema are ignored: accounts and privileges (`CREATE/ALTER/DROP USER`,
`GRANT`, `REVOKE`, roles; their binlog form contains password hashes), `FLUSH`, `ANALYZE`,
`OPTIMIZE`, `REPAIR`, tablespaces, servers, plugins and so on.

Two situations would otherwise lose data silently, so they are failures (subject to
`event_processing_failure_handling_mode`):

* A **write logged in statement format** to a captured table (a session ran
  `SET SESSION binlog_format = 'STATEMENT'`): its rows are not in the binlog. Writes to tables
  that are not captured are ignored.
* An **incident event**: the server recorded that events are missing from the binlog.

Other MySQL behaviours to know about:

* **Foreign-key cascades are not in the binlog.** MySQL applies `ON DELETE/UPDATE CASCADE`,
  `SET NULL` and `SET DEFAULT` inside InnoDB and writes no row events for the child rows. At
  start-up the source warns about every such key on a captured table and lists each table's
  foreign keys (columns, referenced table, `on_update`, `on_delete`) in `foreign_keys` of its
  schema document, so the rules can be applied downstream. They are read at start-up and are
  not part of `schema_version`.
* **Partition operations** (`ALTER TABLE … TRUNCATE/DROP/EXCHANGE PARTITION`) remove or swap rows
  without row events. They are logged as a warning and flagged `data_changed: true` on
  the `schema_change` event; re-snapshot the table.
* **Incomplete row images:** the source refuses to start unless the server's
  `binlog_row_image` is `FULL`. A session that sets it to `MINIMAL` or `NOBLOB` anyway produces
  incomplete images, which are failures, like statement-format writes: emitting them would blank
  out columns downstream.
* **`ALTER TABLE … RENAME [TO|AS]`** is classified as a rename, like `RENAME TABLE`, so the
  table's history follows it.
* **Tables without a primary key** produce events without a key; the source warns once per
  table. `pk_override` sets the key (or replaces the primary key), like Debezium's
  `message.key.columns`:

  ```yaml
  pk_override:
    appdb:
      calls: "call_ref, site"    # pk_override.appdb.calls = call_ref, site
  ```
* **Invisible columns**, including generated invisible primary keys (`my_row_id`), are captured
  from both the snapshot and the binlog.
* **Online schema changes**: shadow tables of gh-ost (`_t_gho`, `_t_ghc`, `_t_del`) and
  pt-online-schema-change (`_t_new`, `_t_old`) are skipped
  (`exclude_online_schema_change_tables`, default `true`); the final `RENAME` swap is captured
  and the table continues under its new schema version.

`JSON` columns are compact JSON text (`json_handling_mode: string`, the default), identical
whether a row came from the snapshot or the binlog; `json_handling_mode: object` embeds the
parsed document instead.

## Schema changes and other behaviours (PostgreSQL)

PostgreSQL's logical replication streams no DDL. Before the first change to a table after its
definition changed, pgoutput sends the table's new description, and with
`include_schema_changes: true` the source emits a `schema_change` event describing the difference:
`columns_added`, `columns_removed` and `columns_retyped`. `TRUNCATE` on a captured table is
always emitted as a `truncate` event, with `cascade` and `restart_identity`. Other statements,
and logical decoding messages, are not captured.

* **Transactions** are decoded at commit (pgoutput protocol 1): a large transaction arrives in
  one piece once it commits, and one that rolls back is never sent.
* **Keys outside the replica identity:** a change to a key column that the replica identity
  does not cover (with `pk_override`, or `REPLICA IDENTITY USING INDEX` on another index) can't
  be matched downstream. Such deletes and key changes are failures, subject to
  `event_processing_failure_handling_mode`; set `REPLICA IDENTITY FULL` on those tables.
* **Primary-key updates** are emitted as a delete of the old key followed by a create of the new
  key, as for MySQL.
* **Partitioned tables** are captured as the partitioned table when the publication has
  `publish_via_partition_root = true` (see *Slot and publication*).
* `JSON` and `JSONB` columns follow `json_handling_mode` (text by default), and `INTERVAL`
  follows `interval_handling_mode`.

## Table schemas

With `include_table_schemas: true` the source emits a schema document on the `table_schemas`
output for every captured table, once per distinct table definition (and again after a restart),
and `event_format: record` adds `metadata.schema_version` to every row:

```json
{
  "schema_version": "338df4226391e36c", "server_name": "prod-mysql", "format_version": 1,
  "db": "inventory", "table": "customers", "primary_key": ["id"],
  "encoding": {"decimal_handling_mode": "string", "binary_handling_mode": "base64",
               "time_precision_mode": "adaptive_time_microseconds"},
  "columns": [
    {"name": "id", "position": 1, "mysql_type": "int", "nullable": false, "primary_key": true,
     "unsigned": false, "json_type": "integer", "logical_type": "int32"},
    {"name": "balance", "position": 4, "mysql_type": "decimal", "precision": 12, "scale": 2,
     "json_type": "string", "logical_type": "decimal", "representation": "string", …},
    {"name": "created", "mysql_type": "datetime", "fsp": 6, "logical_type": "timestamp_micros_local", …},
    {"name": "status", "mysql_type": "enum", "values": ["new", "active", "closed"], "logical_type": "enum", …},
    {"name": "avatar", "mysql_type": "varbinary", "logical_type": "bytes", "encoding": "base64", …}
  ],
  "first_seen": {"origin": "snapshot", "file": "mysql-bin.000003", "pos": 198, "gtid": null, "captured_ts_ms": …}
}
```

That is a MySQL document. A PostgreSQL one has `postgres_type` in place of `mysql_type`, the
schema in `db`, and its own position in `first_seen`; the rest is the same.

* `schema_version` is a hash of the column descriptions and the value-encoding settings, so it
  changes on `ALTER TABLE` (or when an encoding option changes) and nowhere else. A snapshot and
  the binlog produce the same version for the same table definition.
* `logical_type` says how values are encoded in the JSON, from a fixed vocabulary (`int32`,
  `int64`, `float32`, `float64`, `decimal`, `date_days`, `time_micros`, `timestamp_micros_local`,
  `timestamp_iso8601_utc`, `bytes`, `enum`, `json`, `geometry`, …). Each maps one-to-one onto a
  Parquet/Arrow type (see the table in `table_schema.rs`), which is what a future Parquet output
  will be driven by.
* `first_seen` and `server_name` are informational and not part of the version hash.
* Schema documents do not take part in checkpointing (they are re-emitted on every start), so a
  slowly batching schema sink never delays offset saving. Events on the `control` output do take
  part, so give that sink a short `batch.timeout_secs`.
* Writing each document to `…/_schemas/<db>/<table>/<schema_version>.json` with
  `blob_overwrite: true` (see the example config) makes re-emitted documents rewrite their own
  file instead of piling up copies.

## `azure_blob` sink options

Vectezium adds three options to Vector's `azure_blob` sink.

**`blob_extension`**: By default the extension is
derived from `compression` (`.log`, `.log.gz`, …); `blob_extension: json.gz` (or `ndjson`, …)
overrides it. It only changes the name, not the encoding or compression.

**`blob_overwrite`** (default `false`): normally the sink never replaces an existing block blob
and fails with `409 Conflict`; with `blob_overwrite: true` it does, which the schema sink in the
example configs uses so re-emitted schema documents rewrite their own file.

**`blob_overwrite_order_by`**: a list of JSON fields. Each upload first reads the existing blob
and skips the write (successfully) if that blob orders after the new one by those fields,
compared in turn; otherwise it writes only if the blob hasn't changed since it was read (an ETag
condition), comparing again if it has. Numbers compare as numbers, strings as text, and a
missing field orders first. Use `["seq", "committed_at"]` for a published-offset blob, so a
late request never replaces a newer offset. Needs `blob_overwrite: true`, `blob_type: block`,
`compression: none` and one JSON object per blob (`batch.max_events: 1`).

## Batching and file sizes

* The source sends events downstream in batches of `batch_size` (default 2048) or after 250 ms,
  and uses `snapshot_batch_size` (defaults to `batch_size`) during a blocking snapshot.
* Blob size and frequency are set on the `azure_blob` sink (`batch.max_bytes`,
  `batch.max_events`, `batch.timeout_secs`). The example config writes every table's events,
  snapshot reads included, to one folder per table (`<db>/<table>/date=…/`); files may land in
  any order, and `seq` puts the events back in order.

## Delivery guarantees

* Offsets always point at **transaction boundaries** (MySQL: after `XID`/`COMMIT`, or after a
  DDL; PostgreSQL: after a transaction's `Commit`).
* With `acknowledgements` enabled, an offset is written only after **every event up to it** was
  acknowledged by the sinks. A crash replays from the last acknowledged transaction:
  **at-least-once**, like Debezium. Replayed changes carry the same `seq` as their first
  delivery; snapshot reads may not. See [Duplicates](#duplicates) for what to deduplicate on.
* With `publish_offsets`, the published offset marks the delivered, transaction-complete prefix.
* A source that stops cleanly waits (up to 10 s) for its last acknowledged offset to be
  stored, so the next start resumes from it.
* **PostgreSQL:** the slot is told it may release WAL only up to an offset that is stored (with
  `publish_offsets`: published and delivered), so a published offset is never behind what the
  slot still holds. At start, an offset older than the slot's position is treated as lost
  rather than silently skipped ahead (see *Unplanned snapshots*).
* If a sink permanently rejects a batch, the source stops advancing offsets and shuts down so the
  data is replayed after a restart rather than skipped.
* Primary-key updates are emitted as a delete of the old key followed by a create of the new key,
  as Debezium does.
* With a `disk` buffer on the sink, acknowledgement happens once events are durably in the buffer.

### One writer at a time (`single_writer_lock`)

Two instances capturing the same server, with the same offsets (an overlapping redeploy, a
platform maintenance move, a mistaken second copy), would race on `offsets.json` and on the
published offsets. With `single_writer_lock`, an instance holds a lease on an Azure blob while it
runs:

```yaml
    single_writer_lock:
      account_name: mystorage                  # or connection_string / blob_endpoint, as for azure_blob
      auth:
        azure_credential_kind: managed_identity
      container_name: raw                      # must exist
      blob_name: cdc/prod-mysql/_lock          # created empty if missing; one per captured server
      lease_duration_secs: 30                  # 15–60
```

* The lease is taken **before** the stored offset is read. A second instance logs *Another
  instance holds the single-writer lock* and waits, without connecting to the database.
* The holder renews every third of the lease. On shutdown it waits (up to 10 s) for its last
  acknowledged offset to be stored, then releases the lease, so the next instance starts at
  once and resumes from that offset. A crashed holder blocks a successor until its lease
  expires.
* If the holder cannot renew (storage unreachable, or the lease was broken), it stops writing
  and publishing offsets a third of the lease before it could expire, and the source stops with
  an error. Events it already sent may arrive twice (at-least-once, same `seq`); offsets never
  go to two writers.
* Without the option, Vectezium logs that nothing prevents a second instance.

### Offset recovery (`offset_recovery`)

The stored offset lives in the data directory. If it is lost (a new container without the old
volume), starting over would snapshot every table again and number `seq` from 1 into a folder
that already holds those numbers. With `offset_recovery`, a source that finds no stored offset
reads the lake (after taking the `single_writer_lock`, with its storage settings):

```yaml
    offset_recovery:
      latest_blob: cdc/prod-mysql/_offsets/latest.json   # what the `offsets` output overwrites
      folder: cdc/prod-mysql/                            # the connector's folder
      control_prefix: cdc/prod-mysql/_control/           # optional; see below
```

| The lake has | The source |
|---|---|
| A published offset with `checkpoint` | Stores it as its offset and resumes from it: same position, same `seq` numbering, same table-snapshot state. Events after it are read again: changes with the `seq` they had, snapshot reads possibly not (see [Duplicates](#duplicates)). |
| A published offset without `checkpoint` (published by an earlier version) | Rebuilds the offset from its position and `seq`, and the snapshotted tables from the `snapshot_completed` events under `control_prefix` (required in this case). Other captured tables follow `unplanned_snapshots`. |
| Nothing under `folder` but the lock blob | Starts from scratch (first start). |
| No published offset, but other blobs under `folder` | Stops with an error listing some of them: restore the data directory or the published offset, or use a new folder. |
| A published offset of another connector, or one it cannot parse | Stops with an error. |

A failed read (network, permissions, timeout) is retried every 5 seconds; it is never taken for
"nothing there". Needs `single_writer_lock` and `publish_offsets`.

## Metrics and logs

Standard Vector component metrics apply. Because each event's timestamp is the commit time,
`source_lag_time_seconds` measures **replication lag**. Connection errors, decode errors,
checkpoint write errors and delivery failures increment `component_errors_total`.

## Not supported (yet)

**MySQL:**

* MariaDB (different GTID format and binlog events).
* Debezium's signal table and Kafka signal channels (signals are read from files: see
  *Signals*), and blocking ad-hoc snapshots (`execute-snapshot` with `type: blocking`).
* Two-phase XA transactions (`XA PREPARE`, then `XA COMMIT` or `XA ROLLBACK`): whether their
  changes commit is only known later, in a separate binlog group. One that changes a captured
  table stops the source; with `event_processing_failure_handling_mode: warn` or `skip` its
  changes are left out instead (re-snapshot the tables afterwards). One-phase XA
  (`XA COMMIT … ONE PHASE`) is captured like any other transaction.
* Partial JSON updates (`binlog_row_value_options=PARTIAL_JSON` is rejected).
* `snapshot_locking_mode: minimal` needs `FLUSH TABLES WITH READ LOCK`; on managed services that
  forbid it, use `none` and start the snapshot during a quiet period.
* Incremental snapshots read one table at a time.
* Character sets other than UTF-8 and latin1 (decoded as cp1252, as MySQL does) are decoded
  as UTF-8 (lossy).

**PostgreSQL:**

* Streaming transactions before they commit (pgoutput protocol 2 and later), and two-phase
  transactions.
* Logical decoding messages (`pg_logical_emit_message`).
* More than one database per source (a slot belongs to one database).
* Generated columns, unless the publication publishes them (PostgreSQL 18): pgoutput does not
  send them, so they are left out of snapshots too.

**Both:**

* Tombstone events and Debezium's Kafka-specific headers are not produced.
* Vector component reference docs (`website/cue/...`) are not generated yet.

## Files

The CDC core shared by every connector (`src/sources/cdc/`):

| Path                                   | Purpose                                                        |
|----------------------------------------|----------------------------------------------------------------|
| `src/sources/cdc/mod.rs`               | Shared settings and types (event format, record images, …)     |
| `src/sources/cdc/emitter.rs`           | Batching, `seq` numbering, hand-over of offsets                |
| `src/sources/cdc/checkpoint.rs`        | Durable offsets, acknowledgement-driven checkpoints, published offsets |
| `src/sources/cdc/seq.rs`               | The master sequence (`seq`)                                    |
| `src/sources/cdc/envelope.rs`          | `record` and Debezium event envelopes                          |
| `src/sources/cdc/control.rs`           | Shared control-event payloads (`snapshot_completed`)           |
| `src/sources/cdc/filter.rs`            | Database, table and column include/exclude lists               |
| `src/sources/cdc/lock.rs`              | Single-writer lock (Azure blob lease)                          |
| `src/sources/cdc/recovery.rs`          | Offset recovery from the lake                                  |
| `src/sources/cdc/signal.rs`            | Signals (the file signal channel)                              |
| `src/internal_events/cdc.rs`           | Internal events of the shared core                             |

The MySQL connector (`src/sources/mysql_cdc/`):

| Path                                   | Purpose                                                        |
|----------------------------------------|----------------------------------------------------------------|
| `src/sources/mysql_cdc/mod.rs`         | Configuration and component registration                       |
| `src/sources/mysql_cdc/source.rs`      | Connection handling and start-up decisions                     |
| `src/sources/mysql_cdc/binlog.rs`      | Binlog streaming, transaction boundaries, reconnects           |
| `src/sources/mysql_cdc/snapshot.rs`    | Blocking (consistent, optionally parallel) initial snapshot    |
| `src/sources/mysql_cdc/incremental.rs` | Incremental (chunked) snapshots while streaming                |
| `src/sources/mysql_cdc/ddl.rs`         | Classification of statements in the binlog (DDL and others)    |
| `src/sources/mysql_cdc/schema.rs`      | Column metadata from table maps and result sets                |
| `src/sources/mysql_cdc/table_schema.rs`| Table schema documents and schema versions                     |
| `src/sources/mysql_cdc/convert.rs`     | MySQL value → JSON conversion                                  |
| `src/sources/mysql_cdc/envelope.rs`    | The MySQL `source` block of events                             |
| `src/sources/mysql_cdc/offset.rs`      | Binlog offsets and GTID sets                                   |
| `src/internal_events/mysql_cdc.rs`     | MySQL error events and metrics                                 |

The PostgreSQL connector (`src/sources/postgres_cdc/`):

| Path                                      | Purpose                                                     |
|-------------------------------------------|-------------------------------------------------------------|
| `src/sources/postgres_cdc/mod.rs`         | Configuration and component registration                    |
| `src/sources/postgres_cdc/source.rs`      | Server checks, slot and publication, start-up decisions     |
| `src/sources/postgres_cdc/connection.rs`  | Connections, TLS, the replication stream                    |
| `src/sources/postgres_cdc/stream.rs`      | pgoutput streaming, transactions, status updates, reconnects |
| `src/sources/postgres_cdc/snapshot.rs`    | Blocking initial snapshot in the slot's exported snapshot   |
| `src/sources/postgres_cdc/incremental.rs` | Incremental (chunked) snapshots while streaming             |
| `src/sources/postgres_cdc/catalog.rs`     | Tables, columns, types and publications from the catalog    |
| `src/sources/postgres_cdc/table_schema.rs`| Table schema documents and schema versions                  |
| `src/sources/postgres_cdc/convert.rs`     | PostgreSQL value → JSON conversion                          |
| `src/sources/postgres_cdc/envelope.rs`    | The PostgreSQL `source` block of events                     |
| `src/sources/postgres_cdc/offset.rs`      | LSNs and stored offsets                                     |
| `src/internal_events/postgres_cdc.rs`     | PostgreSQL error events and metrics                         |

Also changed: `src/sinks/azure_blob/` (`blob_extension`, `blob_overwrite`,
`blob_overwrite_order_by`), `Cargo.toml` and `Cargo.lock` (the `mysql_async` and Materialize
`rust-postgres` dependencies, and the `sources-mysql_cdc` and `sources-postgres_cdc` features),
`LICENSE-3rdparty.csv`, and `.github/workflows/vectezium.yml` (CI). `contrib/mysql_cdc/` holds
this README, the Docker test environment, the MySQL test and benchmark scripts and the example
configs.
