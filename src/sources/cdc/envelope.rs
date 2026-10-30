//! Change-event envelopes shared by every CDC connector.
//!
//! Two shapes (`event_format`):
//!
//! * **Debezium** — the Debezium change-event payload (JSON converter, `schemas.enable=false`):
//!   `before`, `after`, `source`, `transaction`, `op`, `ts_ms`, `ts_us`, `ts_ns`. The `source`
//!   block starts with the fields every Debezium connector has, then the connector's own
//!   position fields ([`SourceBlock::debezium_position`]).
//! * **Record** — `metadata` (the change, where it came from, `seq`, `schema_version`) and
//!   `record` (the row), one table row per change.

use chrono::Utc;
use serde_json::{Map, Value as Json, json};

use super::RecordImages;

/// Debezium operation codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Op {
    Create,
    Update,
    Delete,
    Read,
}

impl Op {
    pub const fn code(self) -> &'static str {
        match self {
            Op::Create => "c",
            Op::Update => "u",
            Op::Delete => "d",
            Op::Read => "r",
        }
    }
}

/// The `source.snapshot` marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotMarker {
    False,
    True,
    First,
    FirstInDataCollection,
    LastInDataCollection,
    /// A row of an incremental snapshot.
    Incremental,
    Last,
}

impl SnapshotMarker {
    pub const fn as_str(self) -> &'static str {
        match self {
            SnapshotMarker::False => "false",
            SnapshotMarker::True => "true",
            SnapshotMarker::First => "first",
            SnapshotMarker::FirstInDataCollection => "first_in_data_collection",
            SnapshotMarker::LastInDataCollection => "last_in_data_collection",
            SnapshotMarker::Last => "last",
            SnapshotMarker::Incremental => "incremental",
        }
    }
}

/// Where one change came from. Each connector implements this for its own position type
/// (binlog file/position/GTID for MySQL, an LSN for PostgreSQL or SQL Server, …).
pub trait SourceBlock {
    /// Debezium's `source.connector`, e.g. `mysql`.
    fn connector(&self) -> &'static str;
    fn connector_version(&self) -> &str;
    /// The logical server name (`topic_prefix`).
    fn server_name(&self) -> &str;
    /// Commit time of the change in microseconds since the epoch.
    fn ts_us(&self) -> i64;
    fn snapshot(&self) -> SnapshotMarker;
    /// The database (or schema) of the table.
    fn db(&self) -> &str;
    fn table(&self) -> &str;
    /// Debezium's `source.sequence` (null unless the connector defines one).
    fn sequence(&self) -> Json {
        Json::Null
    }
    /// Appends the connector's position fields to Debezium's `source` block (after `table`).
    fn debezium_position(&self, source: &mut Map<String, Json>);
    /// Appends the connector's position fields to `metadata` in the record format (after
    /// `captured_ts_ms`).
    fn record_position(&self, metadata: &mut Map<String, Json>);
}

fn source_json<S: SourceBlock + ?Sized>(source: &S) -> Json {
    let ts_us = source.ts_us();
    let mut block = Map::with_capacity(18);
    block.insert("version".into(), json!(source.connector_version()));
    block.insert("connector".into(), json!(source.connector()));
    block.insert("name".into(), json!(source.server_name()));
    block.insert("ts_ms".into(), json!(ts_us.div_euclid(1000)));
    block.insert("snapshot".into(), json!(source.snapshot().as_str()));
    block.insert("db".into(), json!(source.db()));
    block.insert("sequence".into(), source.sequence());
    block.insert("ts_us".into(), json!(ts_us));
    block.insert("ts_ns".into(), json!(ts_us.saturating_mul(1000)));
    block.insert("table".into(), json!(source.table()));
    source.debezium_position(&mut block);
    Json::Object(block)
}

/// Builds the Debezium change-event payload.
pub fn change_event<S: SourceBlock + ?Sized>(
    op: Op,
    before: Option<Map<String, Json>>,
    after: Option<Map<String, Json>>,
    source: &S,
) -> Map<String, Json> {
    let now_us = Utc::now().timestamp_micros();
    let mut event = Map::with_capacity(8);
    event.insert("before".into(), before.map_or(Json::Null, Json::Object));
    event.insert("after".into(), after.map_or(Json::Null, Json::Object));
    event.insert("source".into(), source_json(source));
    event.insert("transaction".into(), Json::Null);
    event.insert("op".into(), json!(op.code()));
    event.insert("ts_ms".into(), json!(now_us.div_euclid(1000)));
    event.insert("ts_us".into(), json!(now_us));
    event.insert("ts_ns".into(), json!(now_us.saturating_mul(1000)));
    event
}

/// Builds a `metadata` + `record` change event (`event_format: record`).
///
/// With [`RecordImages::After`], `record` is the latest known state of the row: the after
/// image, or for deletes the deleted row. With [`RecordImages::BeforeAndAfter`], `record`
/// holds both images: `{"before": …, "after": …}`, where `before` is `null` for inserts and
/// snapshot reads, and `after` holds only the key columns for deletes (`null` without a key).
pub fn record_event<S: SourceBlock + ?Sized>(
    op: Op,
    before: Option<Map<String, Json>>,
    after: Option<Map<String, Json>>,
    source: &S,
    key: Option<&Map<String, Json>>,
    schema_version: Option<&str>,
    images: RecordImages,
) -> Map<String, Json> {
    let deleted = op == Op::Delete;
    let ts_us = source.ts_us();
    let mut metadata = Map::with_capacity(17);
    metadata.insert("op".into(), json!(op.code()));
    metadata.insert("deleted".into(), json!(deleted));
    metadata.insert("snapshot".into(), json!(source.snapshot().as_str()));
    metadata.insert("server_name".into(), json!(source.server_name()));
    metadata.insert("db".into(), json!(source.db()));
    metadata.insert("table".into(), json!(source.table()));
    metadata.insert(
        "key".into(),
        key.map_or(Json::Null, |k| Json::Object(k.clone())),
    );
    metadata.insert("commit_ts_ms".into(), json!(ts_us.div_euclid(1000)));
    metadata.insert("commit_ts_us".into(), json!(ts_us));
    metadata.insert(
        "captured_ts_ms".into(),
        json!(Utc::now().timestamp_millis()),
    );
    source.record_position(&mut metadata);
    metadata.insert(
        "schema_version".into(),
        schema_version.map_or(Json::Null, |v| json!(v)),
    );

    let mut event = Map::with_capacity(3);
    event.insert("metadata".into(), Json::Object(metadata));
    match images {
        RecordImages::After => {
            let record = if deleted { before } else { after };
            event.insert("record".into(), record.map_or(Json::Null, Json::Object));
        }
        RecordImages::BeforeAndAfter => {
            // A delete has no after image; its `after` holds the key of the deleted row (the
            // primary key, or the `pk_override` columns), so a merge keyed on `record.after`
            // finds the row to delete. Tables without a key get `null`.
            let after = if deleted { key.cloned() } else { after };
            let mut record = Map::with_capacity(2);
            record.insert("before".into(), before.map_or(Json::Null, Json::Object));
            record.insert("after".into(), after.map_or(Json::Null, Json::Object));
            event.insert("record".into(), Json::Object(record));
        }
    }
    event
}
