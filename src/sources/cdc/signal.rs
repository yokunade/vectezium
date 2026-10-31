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
//! The source reads `pending/` every `poll_interval_secs` and applies the signals in the order
//! they arrived (by modification time, then name). It commits an offset that records the last
//! one (`last_signal`) and moves each file to `processed/<seq>-<name>`, numbered in the order
//! applied, with its result added (`"result": {"status": "applied" | "rejected", "message": …,
//! "ts_ms": …}`). If the source stops before that offset is stored, it applies the signals in
//! `processed/` numbered after its stored `last_signal` again when it starts: applying a signal
//! twice has the same effect as applying it once.

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
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
    /// The file name.
    pub name: String,
    /// The file name in `processed/`, numbered in the order signals are applied.
    pub key: String,
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
///
/// Its file operations run on the blocking thread pool, off the capture loop.
pub struct Spool {
    dirs: Arc<Dirs>,
    interval: Duration,
    next_poll: Instant,
    /// The sequence number of the next signal read from `pending/`.
    next_seq: u64,
    /// Processed signals to apply again before reading `pending/`.
    replay: Vec<Received>,
}

struct Dirs {
    pending: PathBuf,
    processed: PathBuf,
    waiting: PathBuf,
}

impl Spool {
    /// Opens (creating it if missing) the folder of `config`. Signals in `processed/` applied
    /// after `last_signal` (the stored offset's) are applied again; with no `last_signal`, none
    /// are.
    ///
    /// # Errors
    ///
    /// Fails if the folder cannot be created.
    pub async fn open(config: &SignalsConfig, last_signal: Option<&str>) -> Result<Self, String> {
        let dirs = Arc::new(Dirs {
            pending: config.folder.join("pending"),
            processed: config.folder.join("processed"),
            waiting: config.folder.join(WAITING_FILE),
        });
        // A `last_signal` stored before processed signals were numbered matches none of them.
        let after = last_signal.map(|last| sequence(last).unwrap_or(u64::MAX));
        let (next_seq, replay) = blocking({
            let dirs = Arc::clone(&dirs);
            move || dirs.open(after)
        })
        .await?;
        Ok(Self {
            dirs,
            interval: Duration::from_secs(config.poll_interval_secs.max(1)),
            next_poll: Instant::now(),
            next_seq,
            replay,
        })
    }

    /// The signals to apply now: replayed ones first, then `pending/` once per poll interval.
    pub async fn poll(&mut self) -> Vec<Received> {
        let mut out = std::mem::take(&mut self.replay);
        if Instant::now() >= self.next_poll {
            self.next_poll = Instant::now() + self.interval;
            out.extend(self.read_pending(|_| true).await);
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
        let dirs = Arc::clone(&self.dirs);
        let written = blocking(move || write_atomic(&dirs.waiting, &Json::Object(waiting))).await;
        if let Err(error) = written {
            warn!(message = "Could not write the signal folder's waiting file.", %error);
        }
        tokio::pin!(stop);
        let approval = loop {
            if let Some(approval) = self
                .read_pending(|s| matches!(s, Ok(Signal::ApproveResnapshot)))
                .await
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
        let dirs = Arc::clone(&self.dirs);
        blocking(move || _ = fs::remove_file(&dirs.waiting)).await;
        approval
    }

    /// Reads the signals in `pending/` that are `wanted`, in the order they arrived, and numbers
    /// them.
    async fn read_pending(
        &mut self,
        wanted: impl Fn(&Result<Signal, String>) -> bool + Send + 'static,
    ) -> Vec<Received> {
        let dirs = Arc::clone(&self.dirs);
        let mut received = blocking(move || dirs.read_pending(wanted)).await;
        for received in &mut received {
            received.key = format!("{:012}-{}", self.next_seq, received.name);
            self.next_seq += 1;
        }
        received
    }

    /// Moves applied signals to `processed/` with their results. Call once the offset recording
    /// them has been committed.
    pub async fn finish(&self, done: Vec<(Received, Outcome)>) {
        let dirs = Arc::clone(&self.dirs);
        blocking(move || {
            for (received, outcome) in &done {
                dirs.finish(received, outcome);
            }
            dirs.prune();
        })
        .await;
    }
}

impl Dirs {
    fn open(&self, after: Option<u64>) -> Result<(u64, Vec<Received>), String> {
        for dir in [&self.pending, &self.processed] {
            fs::create_dir_all(dir).map_err(|error| {
                format!("creating the signal folder {}: {error}", dir.display())
            })?;
        }
        let processed = self.processed();
        let next_seq = processed
            .last()
            .and_then(|(seq, _, _)| *seq)
            .map_or(0, |seq| seq + 1);
        let replay = after.map_or_else(Vec::new, |after| {
            processed
                .into_iter()
                .filter(|(seq, _, _)| seq.is_some_and(|seq| seq > after))
                .filter_map(|(_, key, path)| {
                    let body: Json = serde_json::from_slice(&fs::read(&path).ok()?).ok()?;
                    let applied =
                        body.pointer("/result/status").and_then(Json::as_str) == Some("applied");
                    let signal = parse(&body);
                    // An approval is only good for the start it was given to.
                    let replayable = applied
                        && signal
                            .as_ref()
                            .is_ok_and(|s| *s != Signal::ApproveResnapshot);
                    replayable.then(|| Received {
                        name: key.clone(),
                        key,
                        body,
                        signal,
                        replay: true,
                    })
                })
                .collect()
        });
        Ok((next_seq, replay))
    }

    /// The processed signals in the order they were applied (unnumbered ones, processed before
    /// signals were numbered, first), with their sequence numbers.
    fn processed(&self) -> Vec<(Option<u64>, String, PathBuf)> {
        let mut files: Vec<_> = files(&self.processed)
            .into_iter()
            .map(|(name, path, _)| (sequence(&name), name, path))
            .collect();
        files.sort();
        files
    }

    fn read_pending(&self, wanted: impl Fn(&Result<Signal, String>) -> bool) -> Vec<Received> {
        let mut files: Vec<_> = files(&self.pending)
            .into_iter()
            .filter(|(name, _, _)| {
                Path::new(name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            })
            .collect();
        // In the order they arrived, whatever they are called: a signal written elsewhere and
        // renamed into `pending/` keeps the time it was written.
        files.sort_by(|(a, _, a_time), (b, _, b_time)| a_time.cmp(b_time).then_with(|| a.cmp(b)));
        files
            .into_iter()
            .filter_map(|(name, path, _)| {
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
                    key: String::new(),
                    body,
                    signal,
                    replay: false,
                })
            })
            .collect()
    }

    fn finish(&self, received: &Received, outcome: &Outcome) {
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
        if let Err(error) = write_atomic(&self.processed.join(&received.key), &Json::Object(body)) {
            warn!(message = "Could not write a processed signal.", signal = %received.name, %error);
        }
        if let Err(error) = fs::remove_file(self.pending.join(&received.name))
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(message = "Could not remove a processed signal.", signal = %received.name, %error);
        }
    }

    fn prune(&self) {
        let files = self.processed();
        let excess = files.len().saturating_sub(KEEP_PROCESSED);
        for (_, _, path) in files.into_iter().take(excess) {
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
    let mut spool = Spool::open(config, None).await?;
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
    spool.finish(vec![(approval, outcome)]).await;
    Ok(true)
}

/// Applies the signals that are due to a running source and logs each outcome. Returns them
/// with their outcomes, or nothing if none were due: the caller then commits an offset (which
/// records the last one, see [`last_signal`]) and passes them to [`Spool::finish`].
pub async fn apply_due(
    spool: &mut Spool,
    queue: &mut impl SnapshotQueue,
) -> Vec<(Received, Outcome)> {
    spool
        .poll()
        .await
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
    done.last().map(|(received, _)| received.key.clone())
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

/// The sequence number of a processed signal's file name (`<seq>-<name>`).
fn sequence(key: &str) -> Option<u64> {
    let (seq, _) = key.split_once('-')?;
    if seq.len() == 12 {
        seq.parse().ok()
    } else {
        None
    }
}

/// Runs file operations on the blocking thread pool.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("signal folder task panicked")
}

/// The regular files of a folder (not dot-files or `.tmp` files), with their modification
/// times.
fn files(dir: &Path) -> Vec<(String, PathBuf, Option<SystemTime>)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let metadata = entry.metadata().ok().filter(fs::Metadata::is_file)?;
            let name = entry.file_name().into_string().ok()?;
            let tmp = Path::new(&name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"));
            (!name.starts_with('.') && !tmp).then(|| (name, entry.path(), metadata.modified().ok()))
        })
        .collect()
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

    /// Writes a pending signal, written `at` seconds after the epoch.
    fn write(dir: &Path, name: &str, body: &str, at: u64) {
        let path = dir.join("pending").join(name);
        fs::write(&path, body).unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(at))
            .unwrap();
    }

    const SNAPSHOT: &str =
        r#"{"type": "execute-snapshot", "data": {"data-collections": ["shop.orders"]}}"#;

    fn names(received: &[Received]) -> Vec<&str> {
        received.iter().map(|r| r.name.as_str()).collect()
    }

    #[tokio::test]
    async fn reads_pending_in_arrival_order_and_moves_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).await.unwrap();
        write(dir.path(), "b.json", r#"{"type": "stop-snapshot"}"#, 1);
        write(
            dir.path(),
            "a.json",
            r#"{"id": "a", "type": "approve-resnapshot"}"#,
            2,
        );
        write(dir.path(), "c.json.tmp", r#"{"type": "stop-snapshot"}"#, 3);
        write(dir.path(), "d.json", "not json", 2);
        let received = spool.poll().await;
        // By the time written, then by name: not by name alone.
        assert_eq!(names(&received), ["b.json", "a.json", "d.json"]);
        assert_eq!(received[1].id(), "a");
        assert!(received[2].signal.is_err());
        // Not again until the next poll.
        assert!(spool.poll().await.is_empty());

        let first = received.into_iter().next().unwrap();
        spool
            .finish(vec![(first, Outcome::Applied("nothing to stop".into()))])
            .await;
        assert!(!dir.path().join("pending/b.json").exists());
        let processed: Json = serde_json::from_slice(
            &fs::read(dir.path().join("processed/000000000000-b.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(processed["type"], "stop-snapshot");
        assert_eq!(processed["result"]["status"], "applied");
        assert_eq!(processed["result"]["message"], "nothing to stop");
    }

    #[tokio::test]
    async fn replays_applied_signals_after_the_stored_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).await.unwrap();
        // Applied in this order, although their names sort the other way.
        for (at, name) in (0..).zip(["5.json", "4.json", "3.json", "2.json"]) {
            write(dir.path(), name, SNAPSHOT, at);
        }
        write(dir.path(), "1.json", r#"{"type": "approve-resnapshot"}"#, 9);
        let received = spool.poll().await;
        let stored = received[0].key.clone();
        let done = received
            .into_iter()
            .enumerate()
            .map(|(i, received)| {
                let outcome = if i == 2 {
                    Outcome::Rejected(String::new())
                } else {
                    Outcome::Applied(String::new())
                };
                (received, outcome)
            })
            .collect();
        spool.finish(done).await;

        // The stored offset recorded the first: the second and fourth are applied again, not
        // the rejected third or the approval.
        // A new signal is read after them, numbered after the processed ones.
        write(dir.path(), "0.json", SNAPSHOT, 10);
        let mut reopened = Spool::open(&config(dir.path()), Some(&stored))
            .await
            .unwrap();
        let received = reopened.poll().await;
        assert_eq!(
            names(&received),
            ["000000000001-4.json", "000000000003-2.json", "0.json"]
        );
        assert_eq!(
            received.iter().map(|r| r.replay).collect::<Vec<_>>(),
            [true, true, false]
        );
        assert_eq!(received[2].key, "000000000005-0.json");

        // Without a stored `last_signal`, nothing is replayed; nor with one stored before
        // processed signals were numbered.
        for last in [None, Some("5.json")] {
            let mut spool = Spool::open(&config(dir.path()), last).await.unwrap();
            assert!(spool.poll().await.iter().all(|r| !r.replay));
        }
    }

    #[tokio::test]
    async fn keeps_the_latest_processed_signals() {
        let dir = tempfile::tempdir().unwrap();
        let processed = dir.path().join("processed");
        fs::create_dir_all(&processed).unwrap();
        // From before signals were numbered: pruned first.
        fs::write(processed.join("zzz.json"), "{}").unwrap();
        for seq in 0..KEEP_PROCESSED {
            fs::write(processed.join(format!("{seq:012}-a.json")), "{}").unwrap();
        }
        let mut spool = Spool::open(&config(dir.path()), None).await.unwrap();
        write(dir.path(), "new.json", SNAPSHOT, 1);
        let received = spool.poll().await;
        spool
            .finish(
                received
                    .into_iter()
                    .map(|r| (r, Outcome::Applied(String::new())))
                    .collect(),
            )
            .await;
        let left = files(&processed);
        assert_eq!(left.len(), KEEP_PROCESSED);
        assert!(!processed.join("zzz.json").exists());
        assert!(!processed.join("000000000000-a.json").exists());
        assert!(
            processed
                .join(format!("{KEEP_PROCESSED:012}-new.json"))
                .exists()
        );
    }

    #[tokio::test]
    async fn applies_due_signals_and_reports_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).await.unwrap();
        write(dir.path(), "001.json", SNAPSHOT, 1);
        write(dir.path(), "002.json", r#"{"type": "nope"}"#, 2);
        let mut queue = Queue::default();
        let done = apply_due(&mut spool, &mut queue).await;
        assert_eq!(queue.queued, vec![t("shop", "orders")]);
        assert!(matches!(done[1].1, Outcome::Rejected(ref r) if r.contains("unknown")));
        assert_eq!(last_signal(&done).as_deref(), Some("000000000001-002.json"));
    }

    #[tokio::test]
    async fn waits_for_an_approval() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(&config(dir.path()), None).await.unwrap();
        write(dir.path(), "001.json", r#"{"type": "stop-snapshot"}"#, 1);
        // Stopped before an approval arrives.
        let none = spool
            .wait_for_approval("purged", async {
                tokio::time::sleep(Duration::from_millis(50)).await;
            })
            .await;
        assert!(none.is_none());
        assert!(!dir.path().join(WAITING_FILE).exists());

        write(
            dir.path(),
            "002.json",
            r#"{"type": "approve-resnapshot"}"#,
            2,
        );
        let approval = spool
            .wait_for_approval("purged", std::future::pending())
            .await
            .unwrap();
        assert_eq!(approval.name, "002.json");
        // Other signals stay pending.
        assert!(dir.path().join("pending/001.json").exists());
    }
}
