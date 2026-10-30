//! The MySQL `source` block of change events (binlog position, GTID, …). The envelopes
//! themselves are shared with every connector (`cdc::envelope`).
//!
//! ```json
//! "source": {
//!   "version": "…", "connector": "mysql", "name": "dbserver1", "ts_ms": 1700000000000,
//!   "snapshot": "false", "db": "inventory", "sequence": null, "ts_us": …, "ts_ns": …,
//!   "table": "customers", "server_id": 223344, "gtid": "…:42", "file": "mysql-bin.000003",
//!   "pos": 1543, "row": 0, "thread": null, "query": null
//! }
//! ```

use serde_json::{Map, Value as Json, json};

#[cfg(test)]
use super::RecordImages;
pub use crate::sources::cdc::envelope::{Op, SnapshotMarker, SourceBlock};
#[cfg(test)]
use crate::sources::cdc::envelope::{change_event, record_event};

/// Values for the `source` block of one MySQL change event.
#[derive(Clone, Debug)]
pub struct SourceInfo<'a> {
    pub connector_version: &'a str,
    pub name: &'a str,
    /// Commit time of the change in microseconds since the epoch.
    pub ts_us: i64,
    pub snapshot: SnapshotMarker,
    pub db: &'a str,
    pub table: &'a str,
    pub server_id: u32,
    pub gtid: Option<&'a str>,
    pub file: &'a str,
    pub pos: u64,
    pub row: usize,
    pub query: Option<&'a str>,
}

impl SourceBlock for SourceInfo<'_> {
    fn connector(&self) -> &'static str {
        "mysql"
    }

    fn connector_version(&self) -> &str {
        self.connector_version
    }

    fn server_name(&self) -> &str {
        self.name
    }

    fn ts_us(&self) -> i64 {
        self.ts_us
    }

    fn snapshot(&self) -> SnapshotMarker {
        self.snapshot
    }

    fn db(&self) -> &str {
        self.db
    }

    fn table(&self) -> &str {
        self.table
    }

    fn debezium_position(&self, source: &mut Map<String, Json>) {
        source.insert("server_id".into(), json!(self.server_id));
        source.insert("gtid".into(), self.gtid.map_or(Json::Null, |g| json!(g)));
        source.insert("file".into(), json!(self.file));
        source.insert("pos".into(), json!(self.pos));
        source.insert("row".into(), json!(self.row));
        source.insert("thread".into(), Json::Null);
        source.insert("query".into(), self.query.map_or(Json::Null, |q| json!(q)));
    }

    fn record_position(&self, metadata: &mut Map<String, Json>) {
        metadata.insert("server_id".into(), json!(self.server_id));
        metadata.insert("gtid".into(), self.gtid.map_or(Json::Null, |g| json!(g)));
        metadata.insert("file".into(), json!(self.file));
        metadata.insert("pos".into(), json!(self.pos));
        metadata.insert("row".into(), json!(self.row));
        metadata.insert("query".into(), self.query.map_or(Json::Null, |q| json!(q)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_shape() {
        let source = SourceInfo {
            connector_version: "test",
            name: "srv",
            ts_us: 1_700_000_000_123_456,
            snapshot: SnapshotMarker::False,
            db: "app",
            table: "t",
            server_id: 1,
            gtid: Some("uuid:5"),
            file: "binlog.000001",
            pos: 100,
            row: 0,
            query: None,
        };
        let mut after = Map::new();
        after.insert("id".into(), json!(1));
        let event = change_event(Op::Create, None, Some(after), &source);

        let keys: Vec<&str> = event.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "before",
                "after",
                "source",
                "transaction",
                "op",
                "ts_ms",
                "ts_us",
                "ts_ns"
            ]
        );
        assert_eq!(event["op"], json!("c"));
        assert_eq!(event["before"], Json::Null);
        assert_eq!(event["source"]["ts_ms"], json!(1_700_000_000_123_i64));
        assert_eq!(event["source"]["gtid"], json!("uuid:5"));
        assert_eq!(event["source"]["connector"], json!("mysql"));
        // Debezium's MySQL `source` block, field for field and in order.
        let source_keys: Vec<&str> = event["source"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            source_keys,
            [
                "version",
                "connector",
                "name",
                "ts_ms",
                "snapshot",
                "db",
                "sequence",
                "ts_us",
                "ts_ns",
                "table",
                "server_id",
                "gtid",
                "file",
                "pos",
                "row",
                "thread",
                "query"
            ]
        );
    }

    fn sample_source() -> SourceInfo<'static> {
        SourceInfo {
            connector_version: "test",
            name: "srv",
            ts_us: 1_700_000_000_123_456,
            snapshot: SnapshotMarker::False,
            db: "app",
            table: "t",
            server_id: 1,
            gtid: Some("uuid:5"),
            file: "binlog.000001",
            pos: 100,
            row: 0,
            query: None,
        }
    }

    fn row(name: &str) -> Map<String, Json> {
        let mut row = Map::new();
        row.insert("id".into(), json!(1));
        row.insert("name".into(), json!(name));
        row
    }

    #[test]
    fn record_format_after_only() {
        let source = sample_source();
        let mut key = Map::new();
        key.insert("id".into(), json!(1));

        let update = record_event(
            Op::Update,
            Some(row("old")),
            Some(row("new")),
            &source,
            Some(&key),
            Some("v1"),
            RecordImages::After,
        );
        let keys: Vec<&str> = update.keys().map(String::as_str).collect();
        assert_eq!(keys, ["metadata", "record"]);
        let metadata_keys: Vec<&str> = update["metadata"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            metadata_keys,
            [
                "op",
                "deleted",
                "snapshot",
                "server_name",
                "db",
                "table",
                "key",
                "commit_ts_ms",
                "commit_ts_us",
                "captured_ts_ms",
                "server_id",
                "gtid",
                "file",
                "pos",
                "row",
                "query",
                "schema_version"
            ]
        );
        assert_eq!(update["record"]["name"], json!("new"));
        assert_eq!(update["metadata"]["op"], json!("u"));
        assert_eq!(update["metadata"]["deleted"], json!(false));
        assert_eq!(update["metadata"]["key"], json!({"id": 1}));
        assert_eq!(update["metadata"]["schema_version"], json!("v1"));
        assert_eq!(
            update["metadata"]["commit_ts_ms"],
            json!(1_700_000_000_123_i64)
        );

        let delete = record_event(
            Op::Delete,
            Some(row("gone")),
            None,
            &source,
            Some(&key),
            Some("v1"),
            RecordImages::After,
        );
        assert_eq!(delete["record"]["name"], json!("gone"));
        assert_eq!(delete["metadata"]["deleted"], json!(true));
    }

    #[test]
    fn record_format_before_and_after() {
        let source = sample_source();
        let update = record_event(
            Op::Update,
            Some(row("old")),
            Some(row("new")),
            &source,
            None,
            Some("v1"),
            RecordImages::BeforeAndAfter,
        );
        let keys: Vec<&str> = update.keys().map(String::as_str).collect();
        assert_eq!(keys, ["metadata", "record"]);
        let record_keys: Vec<&str> = update["record"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(record_keys, ["before", "after"]);
        assert_eq!(update["record"]["before"]["name"], json!("old"));
        assert_eq!(update["record"]["after"]["name"], json!("new"));
        assert_eq!(update["metadata"]["key"], Json::Null);

        let insert = record_event(
            Op::Create,
            None,
            Some(row("x")),
            &source,
            None,
            Some("v1"),
            RecordImages::BeforeAndAfter,
        );
        assert_eq!(insert["record"]["before"], Json::Null);
        assert_eq!(insert["record"]["after"]["name"], json!("x"));

        // A delete's `after` is the key of the deleted row: whatever the key columns are (the
        // primary key, or `pk_override`, possibly several columns), and nothing else.
        let mut key = Map::new();
        key.insert("tenant".into(), json!("acme"));
        key.insert("id".into(), json!(1));
        let mut deleted = row("gone");
        deleted.insert("tenant".into(), json!("acme"));
        let delete = record_event(
            Op::Delete,
            Some(deleted),
            None,
            &source,
            Some(&key),
            Some("v1"),
            RecordImages::BeforeAndAfter,
        );
        assert_eq!(delete["record"]["before"]["name"], json!("gone"));
        assert_eq!(
            delete["record"]["after"],
            json!({"tenant": "acme", "id": 1})
        );
        assert_eq!(delete["record"]["after"], delete["metadata"]["key"]);
        assert_eq!(delete["metadata"]["deleted"], json!(true));

        // A table without a key: nothing to put in `after`.
        let keyless = record_event(
            Op::Delete,
            Some(row("gone")),
            None,
            &source,
            None,
            Some("v1"),
            RecordImages::BeforeAndAfter,
        );
        assert_eq!(keyless["record"]["after"], Json::Null);
    }
}
