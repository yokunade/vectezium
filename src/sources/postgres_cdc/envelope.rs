//! The PostgreSQL `source` block of change events. The envelopes themselves are shared with every
//! connector (`cdc::envelope`).
//!
//! ```json
//! "source": {
//!   "version": "…", "connector": "postgresql", "name": "dbserver1", "ts_ms": 1700000000000,
//!   "snapshot": "false", "db": "inventory", "sequence": "[\"24023119\",\"24023128\"]",
//!   "ts_us": …, "ts_ns": …, "table": "customers", "schema": "public", "txId": 555,
//!   "lsn": 24023128, "xmin": null
//! }
//! ```
//!
//! As in Debezium, `source.db` is the database and `source.schema` the schema. In the record
//! format `metadata.db` is the schema (the level the include lists and the lake layout use, as
//! the database is for `MySQL`), with the database in `metadata.database`.

use serde_json::{Map, Value as Json, json};

use super::offset::Lsn;
pub use crate::sources::cdc::envelope::{Op, SnapshotMarker, SourceBlock};

/// Values for the `source` block of one PostgreSQL change event.
#[derive(Clone, Debug)]
pub struct SourceInfo<'a> {
    pub connector_version: &'a str,
    pub name: &'a str,
    /// Commit time of the change in microseconds since the epoch.
    pub ts_us: i64,
    pub snapshot: SnapshotMarker,
    pub database: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub txid: Option<u32>,
    /// The commit LSN of the change's transaction (the snapshot's position for snapshot reads).
    pub lsn: Option<Lsn>,
    /// The commit LSN of the transaction before it, Debezium's `lastCommitLsn`.
    pub last_commit_lsn: Option<Lsn>,
}

impl SourceBlock for SourceInfo<'_> {
    fn connector(&self) -> &'static str {
        "postgresql"
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
        self.schema
    }

    fn table(&self) -> &str {
        self.table
    }

    /// Debezium's `["<lastCommitLsn>", "<lsn>"]`, as a JSON string.
    fn sequence(&self) -> Json {
        let text = |lsn: Option<Lsn>| lsn.map(|l| l.0.to_string());
        Json::from(json!([text(self.last_commit_lsn), text(self.lsn)]).to_string())
    }

    fn debezium_position(&self, source: &mut Map<String, Json>) {
        // `db` is the database in Debezium's PostgreSQL connector (replaced in place, so the
        // field order stays Debezium's).
        source.insert("db".into(), json!(self.database));
        source.insert("schema".into(), json!(self.schema));
        source.insert("txId".into(), self.txid.map_or(Json::Null, |t| json!(t)));
        source.insert("lsn".into(), self.lsn.map_or(Json::Null, |l| json!(l.0)));
        source.insert("xmin".into(), Json::Null);
    }

    fn record_position(&self, metadata: &mut Map<String, Json>) {
        metadata.insert("database".into(), json!(self.database));
        metadata.insert("txid".into(), self.txid.map_or(Json::Null, |t| json!(t)));
        metadata.insert(
            "lsn".into(),
            self.lsn.map_or(Json::Null, |l| json!(l.to_string())),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::cdc::{
        RecordImages,
        envelope::{change_event, record_event},
    };

    fn source() -> SourceInfo<'static> {
        SourceInfo {
            connector_version: "test",
            name: "srv",
            ts_us: 1_700_000_000_123_456,
            snapshot: SnapshotMarker::False,
            database: "shop",
            schema: "public",
            table: "orders",
            txid: Some(555),
            lsn: Some(Lsn(24_023_128)),
            last_commit_lsn: Some(Lsn(24_023_119)),
        }
    }

    #[test]
    fn debezium_source_block() {
        let mut after = Map::new();
        after.insert("id".into(), json!(1));
        let event = change_event(Op::Create, None, Some(after), &source());
        let block = event["source"].as_object().unwrap();
        let keys: Vec<&str> = block.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
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
                "schema",
                "txId",
                "lsn",
                "xmin"
            ]
        );
        assert_eq!(block["connector"], json!("postgresql"));
        assert_eq!(block["db"], json!("shop"));
        assert_eq!(block["schema"], json!("public"));
        assert_eq!(block["sequence"], json!("[\"24023119\",\"24023128\"]"));
        assert_eq!(block["lsn"], json!(24_023_128));
    }

    #[test]
    fn record_metadata_uses_schema_as_db() {
        let event = record_event(
            Op::Update,
            None,
            Some(Map::new()),
            &source(),
            None,
            Some("v1"),
            RecordImages::After,
        );
        let metadata = event["metadata"].as_object().unwrap();
        assert_eq!(metadata["db"], json!("public"));
        assert_eq!(metadata["database"], json!("shop"));
        assert_eq!(metadata["lsn"], json!("0/16E9058"));
        assert_eq!(metadata["txid"], json!(555));
    }
}
