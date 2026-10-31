//! Binlog offsets and GTID-set bookkeeping. Storage, checkpointing and publishing are shared
//! by every connector (`cdc::checkpoint`); [`Offset`] is `MySQL`'s [`CdcOffset`].
//!
//! An [`Offset`] always points at a transaction boundary: the binlog position *after* the last
//! fully processed transaction, plus the GTID set executed up to and including that transaction.
//! Restarting from an offset therefore never resumes in the middle of a transaction.

use std::{collections::BTreeMap, fmt, str::FromStr};

use mysql_async::Sid;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

use crate::sources::cdc::checkpoint::CdcOffset;
pub use crate::sources::cdc::checkpoint::{CHECKPOINT_FILENAME, CheckpointStore};

/// Current on-disk checkpoint format version.
const CHECKPOINT_VERSION: u32 = 1;

/// A resumable position in the `MySQL` binlog.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Offset {
    /// Checkpoint format version.
    #[serde(default = "default_version")]
    pub version: u32,

    /// `@@server_uuid` of the server the offset was taken from.
    #[serde(default)]
    pub server_uuid: Option<String>,

    /// The binlog file name.
    pub file: String,

    /// Position inside `file` right after the last processed transaction.
    pub pos: u64,

    /// Executed GTID set including the last processed transaction (empty when GTIDs are off).
    #[serde(default)]
    pub gtid_set: String,

    /// Whether the initial snapshot has fully completed and been delivered.
    #[serde(default)]
    pub snapshot_completed: bool,

    /// The `seq` the next event will get. Every event with a lower `seq` is delivered, and
    /// events replayed after a restart get the same numbers again.
    #[serde(default)]
    pub next_seq: u64,

    /// The snapshot position within `next_seq - 1` reached at this offset (see `master_seq`).
    #[serde(default)]
    pub seq_sub: u64,

    /// Captured tables whose existing rows have been snapshotted (or that were created while
    /// streaming, so all their rows came from the binlog), as `[database, table]`. Absent in
    /// offsets written before incremental snapshots existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshotted_tables: Option<Vec<TableRef>>,

    /// An incremental snapshot in progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<IncrementalProgress>,

    /// Captured tables that need a snapshot nobody asked for (`unplanned_snapshots: hold`), as
    /// `[database, table]`. They are streamed but not snapshotted until an operator queues them
    /// (or marks them as snapshotted).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held_tables: Vec<TableRef>,

    /// Set by an operator to approve a full re-snapshot after the binlog was purged
    /// (`unplanned_snapshots: hold`). Used once: offsets written afterwards do not carry it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resnapshot_approved: bool,

    /// The last signal applied (its file name, see `signals`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_signal: Option<String>,
}

/// `(database, table)`.
pub type TableRef = (String, String);

/// Progress of an incremental (chunked) snapshot, stored with the offset so it resumes after a
/// restart from the last delivered chunk.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

const fn default_version() -> u32 {
    CHECKPOINT_VERSION
}

impl Offset {
    pub const fn new(
        server_uuid: Option<String>,
        file: String,
        pos: u64,
        gtid_set: String,
        snapshot_completed: bool,
    ) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            server_uuid,
            file,
            pos,
            gtid_set,
            snapshot_completed,
            next_seq: 0,
            seq_sub: 0,
            snapshotted_tables: None,
            incremental: None,
            held_tables: Vec::new(),
            resnapshot_approved: false,
            last_signal: None,
        }
    }

    /// Attaches the table-snapshot state.
    #[must_use]
    pub fn with_tables(
        mut self,
        snapshotted_tables: Option<Vec<TableRef>>,
        incremental: Option<IncrementalProgress>,
    ) -> Self {
        self.snapshotted_tables = snapshotted_tables;
        self.incremental = incremental;
        self
    }

    /// Attaches the tables whose snapshot is held.
    #[must_use]
    pub fn with_held(mut self, held_tables: Vec<TableRef>) -> Self {
        self.held_tables = held_tables;
        self
    }

    /// Attaches the last applied signal.
    #[must_use]
    pub fn with_last_signal(mut self, last_signal: Option<String>) -> Self {
        self.last_signal = last_signal;
        self
    }

    /// Rebuilds an offset from a published offset that predates the `checkpoint` field (it has
    /// the position and `seq`, but not the table-snapshot state). `snapshotted` are the tables
    /// with a `snapshot_completed` event in the lake.
    pub fn from_published(
        payload: &Map<String, Json>,
        snapshotted: Vec<TableRef>,
    ) -> Result<Self, String> {
        let text = |key: &str| {
            payload
                .get(key)
                .and_then(Json::as_str)
                .ok_or_else(|| format!("the published offset has no `{key}`"))
        };
        let change_seq = payload
            .get("change_seq")
            .and_then(Json::as_u64)
            .ok_or("the published offset has no `change_seq`")?;
        let seq_sub = text("seq")?
            .split_once('.')
            .and_then(|(_, sub)| sub.parse::<u64>().ok())
            .ok_or("the published offset has an invalid `seq`")?;
        let pos = payload
            .get("pos")
            .and_then(Json::as_u64)
            .ok_or("the published offset has no `pos`")?;
        let mut offset = Self::new(
            None,
            text("file")?.to_owned(),
            pos,
            text("gtid_set")?.to_owned(),
            true,
        )
        .with_tables(Some(snapshotted), None);
        offset.next_seq = change_seq + 1;
        offset.seq_sub = seq_sub;
        Ok(offset)
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
        payload.insert("gtid_set".into(), Json::from(self.gtid_set.as_str()));
        payload.insert("file".into(), Json::from(self.file.as_str()));
        payload.insert("pos".into(), Json::from(self.pos));
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)?;
        if !self.gtid_set.is_empty() {
            write!(f, " (gtid_set: {})", self.gtid_set)?;
        }
        Ok(())
    }
}

/// An executed GTID set, e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:7,…`.
///
/// Intervals are stored inclusive and kept merged so the textual form stays compact.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GtidSet {
    /// Keyed by `uuid` (lowercase) or `uuid:tag` for tagged GTIDs (`MySQL` 8.4+).
    sets: BTreeMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Adds a single transaction (`source_id` is `uuid` or `uuid:tag`).
    pub fn add(&mut self, source_id: &str, gno: u64) {
        let intervals = self.sets.entry(source_id.to_ascii_lowercase()).or_default();
        insert_interval(intervals, gno, gno);
    }

    /// Whether the set contains a transaction.
    pub fn contains(&self, source_id: &str, gno: u64) -> bool {
        self.sets
            .get(&source_id.to_ascii_lowercase())
            .is_some_and(|intervals| intervals.iter().any(|&(s, e)| s <= gno && gno <= e))
    }

    /// Whether every transaction of `other` is in this set.
    pub fn contains_set(&self, other: &GtidSet) -> bool {
        other.sets.iter().all(|(key, theirs)| {
            let ours = self.sets.get(key).map(Vec::as_slice).unwrap_or_default();
            theirs
                .iter()
                .all(|&(s, e)| ours.iter().any(|&(os, oe)| os <= s && e <= oe))
        })
    }

    /// Converts the set into the `Sid` list used by `COM_BINLOG_DUMP_GTID`.
    pub fn to_sids(&self) -> Result<Vec<Sid<'static>>, String> {
        self.to_string()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                Sid::from_str(s).map_err(|error| format!("invalid GTID set entry {s:?}: {error}"))
            })
            .collect()
    }
}

fn insert_interval(intervals: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    intervals.push((start, end));
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
    for &(s, e) in intervals.iter() {
        match merged.last_mut() {
            Some(last) if s <= last.1.saturating_add(1) => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    *intervals = merged;
}

impl FromStr for GtidSet {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut set = GtidSet::default();
        for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let mut parts = entry.split(':');
            let uuid = parts
                .next()
                .filter(|u| u.len() == 36)
                .ok_or_else(|| format!("invalid GTID set entry: {entry:?}"))?
                .to_ascii_lowercase();
            let mut key = uuid.clone();
            for part in parts {
                let first = part.chars().next().unwrap_or('0');
                if first.is_ascii_alphabetic() || first == '_' {
                    // A tag applies to the intervals that follow it.
                    key = format!("{uuid}:{part}");
                    continue;
                }
                let (start, end) = if let Some((a, b)) = part.split_once('-') {
                    (parse_gno(a, entry)?, parse_gno(b, entry)?)
                } else {
                    let n = parse_gno(part, entry)?;
                    (n, n)
                };
                if start == 0 || end < start {
                    return Err(format!("invalid GTID interval {part:?} in {entry:?}"));
                }
                insert_interval(set.sets.entry(key.clone()).or_default(), start, end);
            }
        }
        Ok(set)
    }
}

fn parse_gno(s: &str, entry: &str) -> Result<u64, String> {
    s.trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid GTID number {s:?} in {entry:?}"))
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (key, intervals) in &self.sets {
            if intervals.is_empty() {
                continue;
            }
            if !first {
                f.write_str(",")?;
            }
            first = false;
            f.write_str(key)?;
            for &(s, e) in intervals {
                if s == e {
                    write!(f, ":{s}")?;
                } else {
                    write!(f, ":{s}-{e}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const U2: &str = "4e11fa47-71ca-11e1-9e33-c80aa9429563";

    #[test]
    fn gtid_set_roundtrip_and_merge() {
        let input = format!("{U2}:1-3,\n{U1}:1-5:7:9-10");
        let mut set: GtidSet = input.parse().unwrap();
        assert_eq!(set.to_string(), format!("{U1}:1-5:7:9-10,{U2}:1-3"));

        set.add(U1, 6);
        set.add(U1, 8);
        set.add(&U2.to_uppercase(), 4);
        assert_eq!(set.to_string(), format!("{U1}:1-10,{U2}:1-4"));
    }

    #[test]
    fn gtid_set_containment() {
        let big: GtidSet = format!("{U1}:1-10:15,{U2}:1-3").parse().unwrap();
        assert!(big.contains(U1, 7));
        assert!(big.contains(&U1.to_uppercase(), 15));
        assert!(!big.contains(U1, 12));
        assert!(!big.contains(U2, 4));
        let small: GtidSet = format!("{U1}:2-5:15").parse().unwrap();
        assert!(big.contains_set(&small));
        assert!(!small.contains_set(&big));
        let other: GtidSet = format!("{U1}:9-11").parse().unwrap();
        assert!(!big.contains_set(&other));
        assert!(big.contains_set(&GtidSet::default()));
    }

    #[test]
    fn offsets_without_table_state_still_load() {
        let old = r#"{"version":1,"file":"mysql-bin.000001","pos":4,"gtid_set":"","snapshot_completed":true}"#;
        let offset: Offset = serde_json::from_str(old).unwrap();
        assert_eq!(offset.snapshotted_tables, None);
        assert_eq!(offset.incremental, None);
    }

    #[test]
    fn the_last_signal_is_kept_in_the_file() {
        let offset = Offset::new(None, "mysql-bin.000001".into(), 4, String::new(), true);
        assert!(
            serde_json::to_value(&offset)
                .unwrap()
                .get("last_signal")
                .is_none()
        );
        let signalled = offset.with_last_signal(Some("0001-a.json".into()));
        let json = serde_json::to_string(&signalled).unwrap();
        let loaded: Offset = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.last_signal.as_deref(), Some("0001-a.json"));
    }

    #[test]
    fn held_tables_and_approval_are_optional_in_the_file() {
        let offset = Offset::new(None, "mysql-bin.000001".into(), 4, String::new(), true);
        let json = serde_json::to_value(&offset).unwrap();
        assert!(json.get("held_tables").is_none());
        assert!(json.get("resnapshot_approved").is_none());

        let held = offset.with_held(vec![("shop".into(), "orders".into())]);
        let json = serde_json::to_value(&held).unwrap();
        assert_eq!(json["held_tables"], serde_json::json!([["shop", "orders"]]));

        let approved: Offset = serde_json::from_str(
            r#"{"file":"mysql-bin.000001","pos":4,"resnapshot_approved":true}"#,
        )
        .unwrap();
        assert!(approved.resnapshot_approved);
        assert!(approved.held_tables.is_empty());
    }

    #[test]
    fn offset_from_a_published_offset_without_checkpoint() {
        let payload: Map<String, Json> = serde_json::from_value(serde_json::json!({
            "connector": "shop",
            "seq": "00000000000000005020.0000013236",
            "change_seq": 5020,
            "gtid_set": format!("{U1}:1-27"),
            "file": "mysql-bin.000003",
            "pos": 4,
            "snapshot_in_progress": false,
        }))
        .unwrap();
        let offset =
            Offset::from_published(&payload, vec![("shop".into(), "orders".into())]).unwrap();
        assert_eq!((offset.next_seq, offset.seq_sub), (5021, 13236));
        assert_eq!((offset.file.as_str(), offset.pos), ("mysql-bin.000003", 4));
        assert_eq!(offset.gtid_set, format!("{U1}:1-27"));
        assert!(offset.snapshot_completed);
        assert_eq!(
            offset.snapshotted_tables,
            Some(vec![("shop".into(), "orders".into())])
        );

        let mut broken = payload;
        broken.remove("change_seq");
        assert!(Offset::from_published(&broken, Vec::new()).is_err());
    }

    #[test]
    fn gtid_set_tagged() {
        let set: GtidSet = format!("{U1}:1-3:tag_a:1-2").parse().unwrap();
        assert_eq!(set.to_string(), format!("{U1}:1-3,{U1}:tag_a:1-2"));
    }

    #[test]
    fn gtid_set_empty() {
        let set: GtidSet = "".parse().unwrap();
        assert!(set.is_empty());
        assert_eq!(set.to_string(), "");
        assert!(set.to_sids().unwrap().is_empty());
    }

    #[test]
    fn gtid_set_rejects_garbage() {
        assert!("nope:1-2".parse::<GtidSet>().is_err());
        assert!(format!("{U1}:5-2").parse::<GtidSet>().is_err());
        assert!(format!("{U1}:0").parse::<GtidSet>().is_err());
    }

    #[test]
    fn gtid_set_to_sids() {
        let set: GtidSet = format!("{U1}:1-5:7").parse().unwrap();
        let sids = set.to_sids().unwrap();
        assert_eq!(sids.len(), 1);
        assert_eq!(sids[0].intervals().len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::<Offset>::new(dir.path());
        assert_eq!(store.load().await.unwrap(), None);

        let offset = Offset::new(
            Some("abc".into()),
            "binlog.000002".into(),
            1234,
            format!("{U1}:1-9"),
            true,
        );
        store.store(&offset).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(offset.clone()));

        let next = Offset {
            pos: 5678,
            ..offset
        };
        store.store(&next).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(next));
    }
}
