//! Offsets: a WAL position (LSN) at a transaction boundary. Storage, checkpointing and
//! publishing are shared by every connector (`cdc::checkpoint`); [`Offset`] is PostgreSQL's
//! [`CdcOffset`].
//!
//! `lsn` is the end of the last fully processed commit record, or (while no captured changes
//! flow) a position the server reported with nothing pending before it. Every transaction that
//! commits at or after it is still to come; every one before it was processed. Resuming
//! therefore skips transactions whose commit LSN is before `lsn`.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value as Json};

use crate::sources::cdc::checkpoint::CdcOffset;
pub use crate::sources::cdc::checkpoint::CheckpointStore;

/// Current on-disk checkpoint format version.
const CHECKPOINT_VERSION: u32 = 1;

/// A captured table: `(schema, table)`.
pub type TableRef = (String, String);

/// A WAL position, printed the way PostgreSQL prints it (`16/B374D848`).
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .trim()
            .split_once('/')
            .ok_or_else(|| format!("invalid LSN `{s}`"))?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| format!("invalid LSN `{s}`"))?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| format!("invalid LSN `{s}`"))?;
        if hi > 0xFFFF_FFFF || lo > 0xFFFF_FFFF {
            return Err(format!("invalid LSN `{s}`"));
        }
        Ok(Self((hi << 32) | lo))
    }
}

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A resumable position in the WAL.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Offset {
    /// Checkpoint format version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Where to resume: transactions committing before it were processed.
    pub lsn: Lsn,
    /// The slot the position belongs to.
    pub slot_name: String,
    /// The server's `system_identifier`: a slot (and its LSNs) only exists on one cluster.
    #[serde(default)]
    pub system_identifier: Option<String>,
    /// Whether the initial snapshot completed. When `false`, the snapshot is taken again.
    pub snapshot_completed: bool,
    /// The tables the completed snapshot covered (or that were captured when the source started
    /// with `no_data`).
    #[serde(default)]
    pub snapshotted_tables: Vec<TableRef>,
    /// The counter of the next change event (see `cdc::seq`).
    #[serde(default = "default_next_seq")]
    pub next_seq: u64,
    /// The position of the last snapshot event after `next_seq - 1`.
    #[serde(default)]
    pub seq_sub: u64,
    /// Set by an operator (in the offset file, while the source is stopped) to allow the
    /// re-snapshot after the slot was lost, when `unplanned_snapshots` is `hold`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resnapshot_approved: bool,
    /// The incremental snapshot in progress, if any (same shape as `mysql_cdc`'s, so the same
    /// tools can queue tables).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<IncrementalProgress>,
    /// The last signal applied (its file name, see `signals`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_signal: Option<String>,
}

/// An incremental snapshot: tables read in key order, in chunks, while changes stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IncrementalProgress {
    pub snapshot_id: String,
    /// When the snapshot started (ms since the epoch); informational.
    pub started_ms: i64,
    /// The `seq` when the first table in `tables` started being read: changes with a `seq` at
    /// or above it are newer than the snapshot of that table.
    #[serde(default)]
    pub started_seq: Option<u64>,
    /// Tables still to snapshot; the first is in progress.
    pub tables: Vec<TableRef>,
    /// SQL literals of the key of the last row delivered from the first table.
    #[serde(default)]
    pub last_key: Option<Vec<String>>,
    /// Rows delivered so far from the first table.
    #[serde(default)]
    pub rows: u64,
}

impl IncrementalProgress {
    pub fn new(tables: Vec<TableRef>) -> Self {
        Self {
            snapshot_id: chrono::Utc::now().format("%Y%m%dT%H%M%S%.6fZ").to_string(),
            started_ms: chrono::Utc::now().timestamp_millis(),
            started_seq: None,
            tables,
            last_key: None,
            rows: 0,
        }
    }
}

const fn default_version() -> u32 {
    CHECKPOINT_VERSION
}

const fn default_next_seq() -> u64 {
    1
}

impl Offset {
    pub fn new(lsn: Lsn, slot_name: &str, system_identifier: Option<String>) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            lsn,
            slot_name: slot_name.to_owned(),
            system_identifier,
            snapshot_completed: false,
            snapshotted_tables: Vec::new(),
            next_seq: 1,
            seq_sub: 0,
            resnapshot_approved: false,
            incremental: None,
            last_signal: None,
        }
    }

    /// The same offset at another position.
    #[must_use]
    pub fn at(&self, lsn: Lsn) -> Self {
        let mut offset = self.clone();
        offset.lsn = lsn;
        offset.resnapshot_approved = false;
        offset
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "slot {} lsn {}", self.slot_name, self.lsn)
    }
}

impl CdcOffset for Offset {
    fn seq(&self) -> (u64, u64) {
        (self.next_seq, self.seq_sub)
    }

    fn set_seq(&mut self, next_seq: u64, seq_sub: u64) {
        self.next_seq = next_seq;
        self.seq_sub = seq_sub;
    }

    fn snapshot_in_progress(&self) -> bool {
        !self.snapshot_completed || self.incremental.is_some()
    }

    fn publish_position(&self, payload: &mut Map<String, Json>) {
        payload.insert("lsn".into(), Json::from(self.lsn.to_string()));
        payload.insert("slot_name".into(), Json::from(self.slot_name.as_str()));
    }

    fn advances_without_seq(&self, published: &Self) -> bool {
        self.lsn > published.lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_text_roundtrip() {
        let lsn: Lsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn.0, 0x16_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(Lsn(0).to_string(), "0/0");
        assert!("16B374D848".parse::<Lsn>().is_err());
        assert!("1FFFFFFFF/0".parse::<Lsn>().is_err());
        assert!(Lsn(5) < "0/6".parse().unwrap());
    }

    #[test]
    fn offset_json_roundtrip() {
        let mut offset = Offset::new(Lsn(0x1_0000_0010), "lake", Some("7300".into()));
        offset.snapshot_completed = true;
        offset.snapshotted_tables = vec![("public".into(), "orders".into())];
        offset.set_seq(42, 3);
        let json = serde_json::to_value(&offset).unwrap();
        assert_eq!(json["lsn"], Json::from("1/10"));
        assert!(json.get("resnapshot_approved").is_none());
        let back: Offset = serde_json::from_value(json).unwrap();
        assert_eq!(back, offset);
    }

    #[test]
    fn approval_is_read_and_cleared_on_move() {
        let offset: Offset = serde_json::from_value(serde_json::json!({
            "lsn": "0/10", "slot_name": "s", "snapshot_completed": true, "resnapshot_approved": true
        }))
        .unwrap();
        assert!(offset.resnapshot_approved);
        assert_eq!(offset.next_seq, 1);
        assert!(!offset.at(Lsn(0x20)).resnapshot_approved);
    }

    #[test]
    fn idle_offsets_advance_without_seq() {
        let offset = Offset::new(Lsn(10), "s", None);
        assert!(offset.at(Lsn(11)).advances_without_seq(&offset));
        assert!(!offset.advances_without_seq(&offset));
    }
}
