//! Payloads of control events (the `control` output) that every connector shares.
//!
//! `schema_change` and `truncate` payloads are connector-specific (they describe the statement
//! the connector saw) but follow the same shape: `type`, `server_name`, `db`, `table`, plus the
//! emitter's `seq`.

use chrono::Utc;
use serde_json::{Map, Value as Json};

use super::seq::master_seq;

/// The `snapshot_completed` control event: the snapshot of a table is complete, so keys it did
/// not contain and that have not changed since `started_seq` no longer exist.
#[allow(clippy::too_many_arguments)]
pub fn snapshot_completed(
    server_name: &str,
    database: &str,
    table: &str,
    snapshot_id: &str,
    started_seq: u64,
    started_ms: i64,
    rows: u64,
    schema_version: Option<&str>,
) -> Map<String, Json> {
    let mut payload = Map::new();
    payload.insert("type".into(), Json::from("snapshot_completed"));
    payload.insert("server_name".into(), Json::from(server_name));
    payload.insert("db".into(), Json::from(database));
    payload.insert("table".into(), Json::from(table));
    payload.insert("snapshot_id".into(), Json::from(snapshot_id));
    payload.insert("started_seq".into(), Json::from(master_seq(started_seq, 0)));
    payload.insert("started_ms".into(), Json::from(started_ms));
    payload.insert("rows".into(), Json::from(rows));
    payload.insert(
        "schema_version".into(),
        schema_version.map_or(Json::Null, Json::from),
    );
    payload.insert("ts_ms".into(), Json::from(Utc::now().timestamp_millis()));
    payload
}

/// The `snapshot_held` control event: the table needs a snapshot nobody asked for, and
/// `unplanned_snapshots` is `hold`, so its changes are streamed but its existing rows are not
/// read until an operator queues it. Emitted at every start while the table is held.
#[must_use]
pub fn snapshot_held(
    server_name: &str,
    database: &str,
    table: &str,
    reason: &str,
) -> Map<String, Json> {
    let mut payload = Map::new();
    payload.insert("type".into(), Json::from("snapshot_held"));
    payload.insert("server_name".into(), Json::from(server_name));
    payload.insert("db".into(), Json::from(database));
    payload.insert("table".into(), Json::from(table));
    payload.insert("reason".into(), Json::from(reason));
    payload.insert("ts_ms".into(), Json::from(Utc::now().timestamp_millis()));
    payload
}
