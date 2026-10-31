//! Signals: operator requests a running source picks up from a folder, without a restart.
//!
//! This is Debezium's file signal channel, one file per signal. An operator (or a UI) writes
//! `<folder>/pending/<name>.json` — under another name first, then renamed, so it appears whole —
//! holding a Debezium signal:
//!
//! ```json
//! {"id": "d5c1…", "type": "execute-snapshot",
//!  "data": {"data-collections": ["shop.orders"], "type": "incremental"}}
//! ```
//!
//! | `type` | `data` | |
//! |---|---|---|
//! | `execute-snapshot` | `data-collections`, `type: incremental` | Snapshot the tables incrementally, while streaming. Also releases a held snapshot. |
//! | `stop-snapshot` | `data-collections` (optional) | Drop the tables (without `data-collections`: all) from the snapshot queue. |
//! | `dismiss-held` | `data-collections` | Mark held tables as snapshotted, without reading them (Vectezium). |
//! | `approve-resnapshot` | | Approve the full re-snapshot a source with `unplanned_snapshots: hold` is waiting for (Vectezium). |
//!
//! The source reads `pending/` in name order every `poll_interval_secs`, applies each signal,
//! commits an offset that records it (`last_signal`), and moves the file to `processed/` with its
//! result added (`"result": {"status": "applied" | "rejected", "message": …, "ts_ms": …}`). If
//! the source stops before that offset is stored, it applies the signals in `processed/` after
//! its stored `last_signal` again when it starts: applying a signal twice has the same effect
//! as applying it once.

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::Utc;
use serde_json::{Map, Value as Json};
use tokio::time::Instant;
use vector_lib::configurable::configurable_component;

use crate::shutdown::ShutdownSignal;

/// `(database or schema, table)`.
pub type TableRef = (String, String);

/// How many processed signals are kept.
const KEEP_PROCESSED: usize = 200;

/// The file a source waiting for an approval writes in the signal folder, so a UI can show it.
pub const WAITING_FILE: &str = "waiting.json";

/// Take operator requests (ad-hoc snapshots, held snapshots, approvals) from files in a folder
/// while running: Debezium's file signal channel, one file per signal.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SignalsConfig {
    /// The folder. Signals are written to `pending/` in it and moved to `processed/` once
    /// applied; both are created if missing.
    #[configurable(metadata(docs::examples = "/var/lib/vectezium/signals/mysql"))]
    pub folder: PathBuf,

    /// How often `pending/` is read.
    #[serde(default = "default_poll_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub poll_interval_secs: u64,
}

const fn default_poll_interval_secs() -> u64 {
    2
}

/// An operator request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Signal {
    ExecuteSnapshot(Vec<TableRef>),
    /// `None`: every queued table.
    StopSnapshot(Option<Vec<TableRef>>),
    DismissHeld(Vec<TableRef>),
    ApproveResnapshot,
}

/// A signal file read from the folder.
#[derive(Debug)]
pub struct Received {
    /// The file name: signals are applied in name order.
    pub name: String,
    /// The file's contents, kept in the processed file.
    pub body: Json,
    /// The signal, or why it cannot be applied.
    pub signal: Result<Signal, String>,
    /// Read from `processed/`: applied before, but maybe not stored with an offset.
    pub replay: bool,
}

impl Received {
    /// The signal's `id`, for logs.
    pub fn id(&self) -> &str {
        self.body.get("id").and_then(Json::as_str).unwrap_or("")
    }
}

/// What applying a signal did.
#[derive(Debug, Eq, PartialEq)]
pub enum Outcome {
    Applied(String),
    Rejected(String),
}

/// The tables a source can queue for a snapshot, and its held tables.
pub trait SnapshotQueue {
    /// Whether the table is captured (it may be snapshotted).
    fn captured(&self, table: &TableRef) -> bool;
    /// Why snapshots cannot be taken now, if they cannot.
    fn unavailable(&self) -> Option<String> {
        None
    }
    /// Whether the table is queued (or being snapshotted).
    fn queued(&self, table: &TableRef) -> bool;
    /// Queues tables for an incremental snapshot, releasing them if they are held.
    fn queue(&mut self, tables: Vec<TableRef>);
    /// Drops tables (`None`: all) from the queue; returns the ones that were queued.
    fn stop(&mut self, tables: Option<&[TableRef]>) -> Vec<TableRef>;
    /// Whether the table's snapshot is held.
    fn held(&self, table: &TableRef) -> bool;
    /// Marks held tables as snapshotted.
    fn dismiss(&mut self, tables: &[TableRef]);
}

/// Applies a signal to a running source.
pub fn apply(queue: &mut impl SnapshotQueue, signal: &Signal) -> Outcome {
    match signal {
        Signal::ExecuteSnapshot(tables) => {
            if let Some(why) = queue.unavailable() {
                return Outcome::Rejected(why);
            }
            if let Some(table) = tables.iter().find(|t| !queue.captured(t)) {
                return Outcome::Rejected(format!("{} is not captured", name(table)));
            }
            let (queued, new): (Vec<&TableRef>, Vec<&TableRef>) =
                tables.iter().partition(|t| queue.queued(t));
            let released: Vec<&TableRef> = new.iter().copied().filter(|t| queue.held(t)).collect();
            let mut parts = Vec::new();
            if !new.is_empty() {
                parts.push(format!("queued {}", names(&new)));
            }
            if !released.is_empty() {
                parts.push(format!(
                    "released the held snapshot of {}",
                    names(&released)
                ));
            }
            if !queued.is_empty() {
                parts.push(format!("already queued: {}", names(&queued)));
            }
            queue.queue(new.into_iter().cloned().collect());
            Outcome::Applied(parts.join("; "))
        }
        Signal::StopSnapshot(tables) => {
            let stopped = queue.stop(tables.as_deref());
            if stopped.is_empty() {
                Outcome::Applied("nothing to stop".into())
            } else {
                Outcome::Applied(format!(
                    "stopped {}",
                    names(&stopped.iter().collect::<Vec<_>>())
                ))
            }
        }
        Signal::DismissHeld(tables) => {
            let (held, not_held): (Vec<&TableRef>, Vec<&TableRef>) =
                tables.iter().partition(|t| queue.held(t));
            let mut parts = Vec::new();
            if !held.is_empty() {
                parts.push(format!("marked {} as snapshotted", names(&held)));
            }
            if !not_held.is_empty() {
                parts.push(format!("not held: {}", names(&not_held)));
            }
            queue.dismiss(&held.into_iter().cloned().collect::<Vec<_>>());
            Outcome::Applied(parts.join("; "))
        }
        Signal::ApproveResnapshot => {
            Outcome::Rejected("no re-snapshot is waiting for approval".into())
        }
    }
}

fn name((db, table): &TableRef) -> String {
    format!("{db}.{table}")
}

fn names(tables: &[&TableRef]) -> String {
    tables
        .iter()
        .map(|t| name(t))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Parses a signal file's contents.
///
/// # Errors
///
/// Fails on anything but a known signal with valid `data`.
pub fn parse(body: &Json) -> Result<Signal, String> {
    let kind = body
        .get("type")
        .and_then(Json::as_str)
        .ok_or("the signal has no `type`")?;
    // Debezium's signal table holds `data` as a JSON string.
    let parsed;
    let data = match body.get("data") {
        Some(Json::String(text)) => {
            parsed = serde_json::from_str::<Json>(text)
                .map_err(|error| format!("`data` is not valid JSON: {error}"))?;
            &parsed
        }
        Some(other) => other,
        None => &Json::Null,
    };
    let collections = || -> Result<Option<Vec<TableRef>>, String> {
        let Some(list) = data.get("data-collections") else {
            return Ok(None);
        };
        let list = list
            .as_array()
            .ok_or("`data-collections` must be a list of `<db>.<table>` names")?;
        list.iter()
            .map(|item| {
                item.as_str()
                    .and_then(|s| s.split_once('.'))
                    .filter(|(db, table)| !db.is_empty() && !table.is_empty())
                    .map(|(db, table)| (db.to_owned(), table.to_owned()))
                    .ok_or_else(|| format!("`{item}` is not a `<db>.<table>` name"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    };
    let required = || -> Result<Vec<TableRef>, String> {
        match collections()? {
            Some(tables) if !tables.is_empty() => Ok(tables),
            _ => Err(format!("`{kind}` needs `data-collections`")),
        }
    };
    match kind {
        "execute-snapshot" => {
            match data.get("type").and_then(Json::as_str) {
                Some(t) if !t.eq_ignore_ascii_case("incremental") => {
                    return Err(format!(
                        "snapshot type `{t}` is not supported: only `incremental`"
                    ));
                }
                _ => {}
            }
            Ok(Signal::ExecuteSnapshot(required()?))
        }
        "stop-snapshot" => Ok(Signal::StopSnapshot(
            collections()?.filter(|tables| !tables.is_empty()),
        )),
        "dismiss-held" => Ok(Signal::DismissHeld(required()?)),
        "approve-resnapshot" => Ok(Signal::ApproveResnapshot),
        other => Err(format!("unknown signal type `{other}`")),
    }
}

/// The signal folder of a running source.
pub struct Spool {
    pending: PathBuf,
    processed: PathBuf,
    waiting: PathBuf,
    interval: Duration,
    next_poll: Instant,
    /// Processed signals to apply again before reading `pending/`.
    replay: Vec<Received>,
}

impl Spool {
    /// Opens (creating it if missing) the folder of `config`. Signals in `processed/` after
    /// `last_signal` (the stored offset's) are applied again; with no `last_signal`, none are.
    ///
    /// # Errors
    ///
    /// Fails if the folder cannot be created.
    pub fn open(config: &SignalsConfig, last_signal: Option<&str>) -> Result<Self, String> {
        let pending = config.folder.join("pending");
        let processed = config.folder.join("processed");
        for dir in [&pending, &processed] {
            fs::create_dir_all(dir).map_err(|error| {
                format!("creating the signal folder {}: {error}", dir.display())
            })?;
        }
        let replay = last_signal.map_or_else(Vec::new, |last| {
            read_dir_sorted(&processed)
                .into_iter()
                .filter(|(name, _)| name.as_str() > last)
                .filter_map(|(name, path)| {
                    let body: Json = serde_json::from_slice(&fs::read(&path).ok()?).ok()?;
                    let applied =
                        body.pointer("/result/status").and_then(Json::as_str) == Some("applied");
                    let signal = parse(&body);
                    // An approval is only good for the start it was given to.
                    let replayable = applied
                        && signal
                            .as_ref()
                            .is_ok_and(|s| *s != Signal::ApproveResnapshot);
                    replayable.then_some(Received {
                        name,
                        body,
                        signal,
                        replay: true,
                    })
                })
                .collect()
        });
        Ok(Self {
            pending,
            processed,
            waiting: config.folder.join(WAITING_FILE),
            interval: Duration::from_secs(config.poll_interval_secs.max(1)),
            next_poll: Instant::now(),
            replay,
        })
    }

    /// The signals to apply now: replayed ones first, then `pending/` once per poll interval.
    pub fn poll(&mut self) -> Vec<Received> {
        let mut out = std::mem::take(&mut self.replay);
        if Instant::now() >= self.next_poll {
            self.next_poll = Instant::now() + self.interval;
            out.extend(self.read_pending(|_| true));
        }
        out
    }

    /// Waits for an `approve-resnapshot` signal, leaving other signals pending, and writes the
    /// waiting file (with `reason`) meanwhile. Returns `None` if `stop` completes first.
    pub async fn wait_for_approval(
        &mut self,
        reason: &str,
        stop: impl Future<Output = ()>,
    ) -> Option<Received> {
        let mut waiting = Map::new();
        waiting.insert("waiting_for".into(), Json::from("approve-resnapshot"));
        waiting.insert("reason".into(), Json::from(reason));
        waiting.insert("since_ms".into(), Json::from(Utc::now().timestamp_millis()));
        if let Err(error) = write_atomic(&self.waiting, &Json::Object(waiting)) {
            warn!(message = "Could not write the signal folder's waiting file.", %error);
        }
        tokio::pin!(stop);
        let approval = loop {
            if let Some(approval) = self
                .read_pending(|s| matches!(s, Ok(Signal::ApproveResnapshot)))
                .into_iter()
                .next()
            {
                break Some(approval);
            }
            tokio::select! {
                () = &mut stop => break None,
                () = tokio::time::sleep(self.interval) => {}
            }
        };
        _ = fs::remove_file(&self.waiting);
        approval
    }

    fn read_pending(&self, wanted: impl Fn(&Result<Signal, String>) -> bool) -> Vec<Received> {
        read_dir_sorted(&self.pending)
            .into_iter()
            .filter(|(name, _)| {
                Path::new(name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            })
            .filter_map(|(name, path)| {
                let (body, signal) = match fs::read(&path) {
                    Ok(bytes) => match serde_json::from_slice::<Json>(&bytes) {
                        Ok(body) => {
                            let signal = parse(&body);
                            (body, signal)
                        }
                        Err(error) => (Json::Null, Err(format!("not valid JSON: {error}"))),
                    },
                    // Removed meanwhile: skip it.
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
                    Err(error) => (Json::Null, Err(format!("cannot be read: {error}"))),
                };
                wanted(&signal).then_some(Received {
                    name,
                    body,
                    signal,
                    replay: false,
                })
            })
            .collect()
    }

    /// Moves a signal to `processed/` with its result. Call once the offset recording it has
    /// been committed.
    pub fn finish(&self, received: &Received, outcome: &Outcome) {
        if received.replay {
            return;
        }
        let (status, message) = match outcome {
            Outcome::Applied(message) => ("applied", message),
            Outcome::Rejected(message) => ("rejected", message),
        };
        let mut body = match &received.body {
            Json::Object(map) => map.clone(),
            _ => Map::new(),
        };
        let mut result = Map::new();
        result.insert("status".into(), Json::from(status));
        result.insert("message".into(), Json::from(message.as_str()));
        result.insert("ts_ms".into(), Json::from(Utc::now().timestamp_millis()));
        body.insert("result".into(), Json::Object(result));
        if let Err(error) = write_atomic(&self.processed.join(&received.name), &Json::Object(body))
        {
            warn!(message = "Could not write a processed signal.", signal = %received.name, %error);
        }
        if let Err(error) = fs::remove_file(self.pending.join(&received.name))
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(message = "Could not remove a processed signal.", signal = %received.name, %error);
        }
        self.prune();
    }

    fn prune(&self) {
        let files = read_dir_sorted(&self.processed);
        let excess = files.len().saturating_sub(KEEP_PROCESSED);
        for (_, path) in files.into_iter().take(excess) {
            _ = fs::remove_file(path);
        }
    }
}

/// Waits at start-up for an operator to approve a full re-snapshot (`reason` says why one is
/// needed). Returns `false` if the source is shut down first.
///
/// # Errors
///
/// Fails if the signal folder cannot be created.
pub async fn await_approval(
    config: &SignalsConfig,
    reason: &str,
    shutdown: &mut ShutdownSignal,
) -> Result<bool, String> {
    let mut spool = Spool::open(config, None)?;
    warn!(
        message = "Waiting for an operator to approve a full re-snapshot (`unplanned_snapshots` is \
                   `hold`): write an `approve-resnapshot` signal to the signal folder's `pending/`.",
        %reason,
        folder = %config.folder.display(),
        internal_log_rate_limit = false,
    );
    let stop = async {
        _ = (&mut *shutdown).await;
    };
    let Some(approval) = spool.wait_for_approval(reason, stop).await else {
        return Ok(false);
    };
    let outcome = Outcome::Applied("approved the full re-snapshot".into());
    log(&approval, &outcome);
    spool.finish(&approval, &outcome);
    Ok(true)
}

/// Applies the signals that are due to a running source and logs each outcome. Returns them
/// with their outcomes, or nothing if none were due: the caller then commits an offset (which
/// records the last one, see [`last_signal`]) and passes them to [`Spool::finish`].
pub fn apply_due(spool: &mut Spool, queue: &mut impl SnapshotQueue) -> Vec<(Received, Outcome)> {
    spool
        .poll()
        .into_iter()
        .map(|received| {
            let outcome = match &received.signal {
                Ok(signal) => apply(queue, signal),
                Err(error) => Outcome::Rejected(error.clone()),
            };
            log(&received, &outcome);
            (received, outcome)
        })
        .collect()
}

/// The name to store as the offset's `last_signal` after applying `done`.
#[must_use]
pub fn last_signal(done: &[(Received, Outcome)]) -> Option<String> {
    done.last().map(|(received, _)| received.name.clone())
}

/// Logs a signal's outcome.
pub fn log(received: &Received, outcome: &Outcome) {
    match outcome {
        Outcome::Applied(message) => info!(
            message = "Signal applied.",
            signal = %received.name,
            id = %received.id(),
            result = %message,
            replay = received.replay,
            internal_log_rate_limit = false,
        ),
        Outcome::Rejected(message) => warn!(
            message = "Signal rejected.",
            signal = %received.name,
            id = %received.id(),
            reason = %message,
            internal_log_rate_limit = false,
        ),
    }
}

/// The regular files of a folder (not dot-files or `.tmp` files), sorted by name.
fn read_dir_sorted(dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<(String, PathBuf)> = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_file()))
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let tmp = Path::new(&name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"));
            (!name.starts_with('.') && !tmp).then(|| (name, entry.path()))
        })
        .collect();
    files.sort();
    files
}

fn write_atomic(path: &Path, value: &Json) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    fs::rename(tmp, path)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn t(db: &str, table: &str) -> TableRef {
        (db.into(), table.into())
    }

    #[derive(Default)]
    struct Queue {
        queued: Vec<TableRef>,
        held: Vec<TableRef>,
        done: Vec<TableRef>,
    }

    impl SnapshotQueue for Queue {
        fn captured(&self, table: &TableRef) -> bool {
            table.0 == "shop"
        }
        fn queued(&self, table: &TableRef) -> bool {
            self.queued.contains(table)
        }
        fn queue(&mut self, tables: Vec<TableRef>) {
            self.held.retain(|h| !tables.contains(h));
            self.queued.extend(tables);
        }
        fn stop(&mut self, tables: Option<&[TableRef]>) -> Vec<TableRef> {
            let (stopped, kept) = self
                .queued
                .drain(..)
                .partition(|q| tables.is_none_or(|t| t.contains(q)));
            self.queued = kept;
            stopped
        }
        fn held(&self, table: &TableRef) -> bool {
            self.held.contains(table)
        }
        fn dismiss(&mut self, tables: &[TableRef]) {
            self.held.retain(|h| !tables.contains(h));
            self.done.extend(tables.iter().cloned());
        }
    }

    #[test]
    fn parses_debezium_signals() {
        let snapshot = json!({"id": "1", "type": "execute-snapshot",
            "data": {"data-collections": ["shop.orders", "shop.items"], "type": "INCREMENTAL"}});
        assert_eq!(
            parse(&snapshot),
            Ok(Signal::ExecuteSnapshot(vec![
                t("shop", "orders"),
                t("shop", "items")
            ]))
        );
        // Debezium's signal table carries `data` as a string.
        let as_text = json!({"id": "2", "type": "stop-snapshot",
            "data": "{\"data-collections\": [\"shop.orders\"]}"});
        assert_eq!(
            parse(&as_text),
            Ok(Signal::StopSnapshot(Some(vec![t("shop", "orders")])))
        );
        assert_eq!(
            parse(&json!({"type": "stop-snapshot"})),
            Ok(Signal::StopSnapshot(None))
        );
        assert_eq!(
            parse(&json!({"type": "approve-resnapshot"})),
            Ok(Signal::ApproveResnapshot)
        );
    }

    #[test]
    fn rejects_bad_signals() {
        for (body, error) in [
            (json!({}), "no `type`"),
            (json!({"type": "log"}), "unknown signal type"),
            (
                json!({"type": "execute-snapshot"}),
                "needs `data-collections`",
            ),
            (
                json!({"type": "execute-snapshot", "data": {"data-collections": ["orders"]}}),
                "not a `<db>.<table>` name",
            ),
            (
                json!({"type": "execute-snapshot",
                    "data": {"data-collections": ["shop.orders"], "type": "blocking"}}),
                "only `incremental`",
            ),
            (json!({"type": "dismiss-held", "data": {}}), "needs"),
        ] {
            let parsed = parse(&body);
            assert!(
                parsed.as_ref().is_err_and(|e| e.contains(error)),
                "{body}: {parsed:?}"
            );
        }
    }

    #[test]
    fn applies_signals() {
        let mut queue = Queue {
            held: vec![t("shop", "orders")],
            ..Queue::default()
        };
        let both = Signal::ExecuteSnapshot(vec![t("shop", "orders"), t("shop", "items")]);
        assert_eq!(
            apply(&mut queue, &both),
            Outcome::Applied(
                "queued shop.orders, shop.items; released the held snapshot of shop.orders".into()
            )
        );
        assert!(queue.held.is_empty());
        // Applying it again changes nothing.
        assert_eq!(
            apply(&mut queue, &both),
            Outcome::Applied("already queued: shop.orders, shop.items".into())
        );
        assert_eq!(queue.queued.len(), 2);

        assert!(matches!(
            apply(&mut queue, &Signal::ExecuteSnapshot(vec![t("other", "x")])),
            Outcome::Rejected(reason) if reason.contains("other.x is not captured")
        ));
        assert_eq!(
            apply(
                &mut queue,
                &Signal::StopSnapshot(Some(vec![t("shop", "items")]))
            ),
            Outcome::Applied("stopped shop.items".into())
        );
        assert_eq!(
            apply(&mut queue, &Signal::StopSnapshot(None)),
            Outcome::Applied("stopped shop.orders".into())
        );

        queue.held = vec![t("shop", "customers")];
        assert_eq!(
            apply(
                &mut queue,
                &Signal::DismissHeld(vec![t("shop", "customers"), t("shop", "orders")])
            ),
            Outcome::Applied("marked shop.customers as snapshotted; not held: shop.orders".into())
        );
        assert_eq!(queue.done, vec![t("shop", "customers")]);
        assert!(matches!(
            apply(&mut queue, &Signal::ApproveResnapshot),
            Outcome::Rejected(_)
        ));
    }

    fn config(dir: &Path) -> SignalsConfig {
        SignalsConfig {
            folder: dir.to_path_buf(),
            poll_interval_secs: 1,
        }
    }

    fn write(dir: &Path, name: &str, body: &str) {
        fs::write(dir.join("pending").join(name), body).unwrap();
    }

    #[tokio::test]
    async fn reads_pending_in_order_and_moves_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).unwrap();
        write(dir.path(), "002.json", r#"{"type": "stop-snapshot"}"#);
        write(
            dir.path(),
            "001.json",
            r#"{"id": "a", "type": "approve-resnapshot"}"#,
        );
        write(dir.path(), "003.json.tmp", r#"{"type": "stop-snapshot"}"#);
        write(dir.path(), "004.json", "not json");
        let received = spool.poll();
        let names: Vec<&str> = received.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["001.json", "002.json", "004.json"]);
        assert_eq!(received[0].id(), "a");
        assert!(received[2].signal.is_err());
        // Not again until the next poll.
        assert!(spool.poll().is_empty());

        spool.finish(&received[1], &Outcome::Applied("nothing to stop".into()));
        assert!(!dir.path().join("pending/002.json").exists());
        let processed: Json =
            serde_json::from_slice(&fs::read(dir.path().join("processed/002.json")).unwrap())
                .unwrap();
        assert_eq!(processed["type"], "stop-snapshot");
        assert_eq!(processed["result"]["status"], "applied");
        assert_eq!(processed["result"]["message"], "nothing to stop");
    }

    #[tokio::test]
    async fn replays_applied_signals_after_the_stored_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).unwrap();
        for name in ["001.json", "002.json", "003.json", "004.json"] {
            write(
                dir.path(),
                name,
                r#"{"type": "execute-snapshot", "data": {"data-collections": ["shop.orders"]}}"#,
            );
        }
        write(dir.path(), "005.json", r#"{"type": "approve-resnapshot"}"#);
        let received = spool.poll();
        for (i, received) in received.iter().enumerate() {
            let outcome = if i == 2 {
                Outcome::Rejected(String::new())
            } else {
                Outcome::Applied(String::new())
            };
            spool.finish(received, &outcome);
        }

        // The stored offset recorded 001: 002 and 004 are applied again, not the rejected 003
        // or the approval.
        let mut reopened = Spool::open(&config(dir.path()), Some("001.json")).unwrap();
        let replayed = reopened.poll();
        let names: Vec<&str> = replayed.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["002.json", "004.json"]);
        assert!(replayed.iter().all(|r| r.replay));
        // Without a stored `last_signal`, nothing is.
        assert!(
            Spool::open(&config(dir.path()), None)
                .unwrap()
                .poll()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn applies_due_signals_and_reports_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).unwrap();
        write(
            dir.path(),
            "001.json",
            r#"{"type": "execute-snapshot", "data": {"data-collections": ["shop.orders"]}}"#,
        );
        write(dir.path(), "002.json", r#"{"type": "nope"}"#);
        let mut queue = Queue::default();
        let done = apply_due(&mut spool, &mut queue);
        assert_eq!(queue.queued, vec![t("shop", "orders")]);
        assert!(matches!(done[1].1, Outcome::Rejected(ref r) if r.contains("unknown")));
        assert_eq!(last_signal(&done).as_deref(), Some("002.json"));
    }

    #[tokio::test]
    async fn waits_for_an_approval() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).unwrap();
        write(dir.path(), "001.json", r#"{"type": "stop-snapshot"}"#);
        // Stopped before an approval arrives.
        let none = spool
            .wait_for_approval("purged", async {
                tokio::time::sleep(Duration::from_millis(50)).await;
            })
            .await;
        assert!(none.is_none());
        assert!(!dir.path().join(WAITING_FILE).exists());

        write(dir.path(), "002.json", r#"{"type": "approve-resnapshot"}"#);
        let approval = spool
            .wait_for_approval("purged", std::future::pending())
            .await
            .unwrap();
        assert_eq!(approval.name, "002.json");
        // Other signals stay pending.
        assert!(dir.path().join("pending/001.json").exists());
    }
}
