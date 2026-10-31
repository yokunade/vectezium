//! Durable offsets, acknowledgement-driven checkpointing and published offsets.
//!
//! A connector hands the [`Emitter`](super::emitter::Emitter) an offset at every transaction
//! boundary. With end-to-end acknowledgements the offset is stored only once every event before
//! it has been delivered ([`Checkpoints`]); the [`checkpoint_writer`] coalesces offsets, writes
//! them crash-safely ([`CheckpointStore`]) and, with `publish_offsets`, publishes them on the
//! `offsets` output.

use std::{
    fmt,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use futures::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value as Json};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use vector_lib::finalizer::OrderedFinalizer;

use super::{
    OFFSETS_OUTPUT,
    emitter::{EventBuilder, EventMeta},
    lock::WriterGuard,
    seq::master_seq,
};
use crate::{
    SourceSender,
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver},
    internal_events::{CdcCheckpointWriteError, CdcDeliveryFailed},
};

/// The name of the checkpoint file inside the source's data directory.
pub const CHECKPOINT_FILENAME: &str = "offsets.json";

/// How often the checkpoint file is rewritten while offsets keep advancing.
const CHECKPOINT_WRITE_INTERVAL: Duration = Duration::from_secs(1);

/// How long offset writes may keep failing before the source stops.
const CHECKPOINT_FAILURE_LIMIT: Duration = Duration::from_mins(5);

/// A connector's resumable position. It always points at a transaction boundary, and carries
/// the `seq` counters so replays after a restart number events exactly as before.
pub trait CdcOffset:
    Clone + fmt::Debug + fmt::Display + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// `(next_seq, seq_sub)`: the counter of the next change event, and the position of the
    /// last snapshot event after it.
    fn seq(&self) -> (u64, u64);

    /// Records the `seq` counters (called by the emitter when the offset is committed).
    fn set_seq(&mut self, next_seq: u64, seq_sub: u64);

    /// Whether a snapshot (initial or incremental) is still running at this offset.
    fn snapshot_in_progress(&self) -> bool;

    /// Adds the connector's position fields to a published offset (for example `gtid_set`,
    /// `file`, `pos` for `MySQL`; `lsn` for PostgreSQL).
    fn publish_position(&self, payload: &mut Map<String, Json>);

    /// Whether this offset should be published although its `seq` equals that of `published`:
    /// its log position moved on while no captured changes flowed (a connector whose server
    /// keeps log until offsets are published, such as a PostgreSQL slot with `publish_offsets`,
    /// needs this so an idle source keeps releasing log), or a signal changed its snapshot state
    /// (so the published offset shows a held table released or a snapshot stopped).
    fn advances_without_seq(&self, _published: &Self) -> bool {
        false
    }
}

/// Durable, crash-safe storage of the latest acknowledged offset.
pub struct CheckpointStore<O> {
    path: PathBuf,
    tmp_path: PathBuf,
    _offset: PhantomData<fn() -> O>,
}

impl<O: CdcOffset> CheckpointStore<O> {
    #[must_use]
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join(CHECKPOINT_FILENAME),
            tmp_path: data_dir.join(format!("{CHECKPOINT_FILENAME}.tmp")),
            _offset: PhantomData,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads the stored offset, if any.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be read, or with `InvalidData` if it does not hold an offset.
    pub async fn load(&self) -> std::io::Result<Option<O>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => Ok(None),
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("corrupt checkpoint file {}: {error}", self.path.display()),
                )
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Atomically replaces the stored offset (write temp file, fsync, rename, fsync directory).
    ///
    /// # Errors
    ///
    /// Fails if any step of the write, rename or sync fails; the previous offset then stays
    /// in place.
    pub async fn store(&self, offset: &O) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec_pretty(offset)?;
        bytes.push(b'\n');

        let mut file = tokio::fs::File::create(&self.tmp_path).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);

        tokio::fs::rename(&self.tmp_path, &self.path).await?;

        #[cfg(unix)]
        if let Some(dir) = self.path.parent() {
            // Persist the rename itself.
            let dir = tokio::fs::File::open(dir).await?;
            dir.sync_all().await?;
        }
        Ok(())
    }
}

/// Checks the delivery settings every connector shares; warns when offsets can run ahead of
/// delivered data.
///
/// # Errors
///
/// Fails if `publish_offsets` is set without end-to-end acknowledgements.
pub fn check_acknowledgements(
    source_type: &str,
    acknowledgements: bool,
    publish_offsets: bool,
) -> crate::Result<()> {
    if publish_offsets && !acknowledgements {
        return Err(
            "`publish_offsets` needs end-to-end acknowledgements: without them offsets are \
             stored before events are delivered, so they cannot mark committed data. \
             Enable `acknowledgements`"
                .into(),
        );
    }
    if !acknowledgements {
        warn!(
            message = "End-to-end acknowledgements are disabled for this CDC source; offsets are \
                       stored as soon as events are handed to the pipeline, so changes buffered \
                       in memory can be lost on a crash. Enable `acknowledgements` for at-least-once delivery.",
            source_type,
        );
    }
    Ok(())
}

/// Where acknowledged offsets go.
pub enum Finalizer<O: CdcOffset> {
    /// Acknowledgements disabled: offsets are stored once events are handed to the pipeline.
    Sync(mpsc::UnboundedSender<O>),
    /// Acknowledgements enabled: offsets are stored once their batch is delivered.
    Async(OrderedFinalizer<Option<O>>),
}

/// The checkpointing tasks of one source run.
pub struct Checkpoints<O: CdcOffset> {
    pub finalizer: Finalizer<O>,
}

impl<O: CdcOffset> Checkpoints<O> {
    /// Starts the checkpoint writer (and, with acknowledgements, the task that releases offsets
    /// in order as their batches are delivered). `written` fires after the last offset write.
    pub fn start(
        acknowledgements: bool,
        store: CheckpointStore<O>,
        poisoned: Arc<AtomicBool>,
        publisher: Option<OffsetPublisher>,
        guard: WriterGuard,
        written: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self::start_notifying(
            acknowledgements,
            store,
            poisoned,
            publisher,
            guard,
            written,
            None,
        )
    }

    /// Like [`Self::start`], and also reports on `stored` every offset the source may forget
    /// the log before: each stored offset, or with `publish_offsets` each published one once its
    /// delivery is acknowledged (a source that lost its data directory resumes from the published
    /// offset). A connector whose
    /// server keeps its log for it (a PostgreSQL replication slot) lets the server discard log
    /// only up to these offsets.
    pub fn start_notifying(
        acknowledgements: bool,
        store: CheckpointStore<O>,
        poisoned: Arc<AtomicBool>,
        publisher: Option<OffsetPublisher>,
        guard: WriterGuard,
        written: tokio::sync::oneshot::Sender<()>,
        stored: Option<tokio::sync::watch::Sender<Option<O>>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<O>();
        let failing = Arc::clone(&poisoned);
        crate::spawn_in_current_span(async move {
            checkpoint_writer(rx, store, publisher, guard, stored, failing).await;
            _ = written.send(());
        });

        let finalizer = if acknowledgements {
            let (finalizer, mut acks) = OrderedFinalizer::<Option<O>>::new(None);
            crate::spawn_in_current_span(async move {
                let mut failed = false;
                while let Some((status, offset)) = acks.next().await {
                    if failed {
                        continue;
                    }
                    if status != BatchStatus::Delivered {
                        failed = true;
                        poisoned.store(true, Ordering::SeqCst);
                        emit!(CdcDeliveryFailed {
                            status: format!("{status:?}"),
                        });
                        continue;
                    }
                    if let Some(offset) = offset {
                        _ = tx.send(offset);
                    }
                }
            });
            Finalizer::Async(finalizer)
        } else {
            Finalizer::Sync(tx)
        };
        Self { finalizer }
    }
}

/// Publishes stored offsets on the `offsets` output (`publish_offsets`).
pub struct OffsetPublisher {
    pub out: SourceSender,
    pub builder: EventBuilder,
    pub server_name: String,
    pub interval: Duration,
}

impl OffsetPublisher {
    /// The published offset: every event with `seq` up to `seq` is delivered, and every
    /// transaction it belongs to is complete.
    pub fn payload<O: CdcOffset>(server_name: &str, offset: &O) -> Map<String, Json> {
        let (next_seq, seq_sub) = offset.seq();
        let counter = next_seq.saturating_sub(1);
        let mut payload = Map::new();
        payload.insert("connector".into(), Json::from(server_name));
        payload.insert("seq".into(), Json::from(master_seq(counter, seq_sub)));
        // The plain change counter, for monitoring.
        payload.insert("change_seq".into(), Json::from(counter));
        offset.publish_position(&mut payload);
        payload.insert(
            "snapshot_in_progress".into(),
            Json::from(offset.snapshot_in_progress()),
        );
        payload.insert(
            "committed_at".into(),
            Json::from(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        // The whole stored offset, so a source that lost its data directory can resume from
        // the published offset (`offset_recovery`) with its table-snapshot state.
        if let Ok(checkpoint) = serde_json::to_value(offset) {
            payload.insert("checkpoint".into(), checkpoint);
        }
        payload
    }

    /// Sends the offset event, and returns the receiver of its delivery status (`None` if the
    /// output is closed). Checkpoints never wait for it: a slow offsets sink must not hold back
    /// checkpoints, and a lost event is superseded by the next one.
    async fn publish<O: CdcOffset>(&mut self, offset: &O) -> Option<BatchStatusReceiver> {
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let event = self
            .builder
            .plain(
                Self::payload(&self.server_name, offset),
                EventMeta {
                    key: None,
                    database: "",
                    table: None,
                    op: "offset",
                    timestamp: Utc::now(),
                    schema_version: None,
                },
            )
            .with_batch_notifier(&batch);
        drop(batch);
        if self
            .out
            .send_batch_named(OFFSETS_OUTPUT, vec![event])
            .await
            .is_err()
        {
            debug!(message = "The offsets output is closed; offset not published.");
            return None;
        }
        Some(receiver)
    }
}

/// Waits for a published offset's delivery and reports it on `acked`: the offset itself once
/// delivered, `None` otherwise.
fn await_delivery<O: CdcOffset>(
    receiver: BatchStatusReceiver,
    offset: O,
    acked: mpsc::UnboundedSender<Option<O>>,
) {
    crate::spawn_in_current_span(async move {
        let status = receiver.await;
        let delivered = status == BatchStatus::Delivered;
        if !delivered {
            warn!(
                message = "A published offset was not delivered; the log before it is kept until a \
                           later one is.",
                status = ?status,
            );
        }
        _ = acked.send(delivered.then_some(offset));
    });
}

/// Coalesces offsets and writes the newest one at most once per interval; publishes stored
/// offsets (at most once per publish interval) when `publish_offsets` is on. Writes and
/// publishes nothing while `guard` says this instance may not write.
#[allow(
    clippy::similar_names,
    reason = "`publisher` publishes offsets; `published` is the last offset it published"
)]
async fn checkpoint_writer<O: CdcOffset>(
    mut rx: mpsc::UnboundedReceiver<O>,
    store: CheckpointStore<O>,
    mut publisher: Option<OffsetPublisher>,
    guard: WriterGuard,
    stored_tx: Option<tokio::sync::watch::Sender<Option<O>>>,
    poisoned: Arc<AtomicBool>,
) {
    let notify = |offset: &O| {
        if let Some(tx) = &stored_tx {
            tx.send_replace(Some(offset.clone()));
        }
    };
    // Since when writes have failed without a success in between.
    let mut failing_since: Option<tokio::time::Instant> = None;
    let mut latest: Option<O> = None;
    let mut stored: Option<O> = None;
    let mut published_seq = (0, 0);
    let mut published: Option<O> = None;
    let mut last_published: Option<Instant> = None;
    // A published offset counts (for `stored_tx`) only once delivered: a source that lost its
    // data directory resumes from the offset in the lake, so the server must keep its log until
    // the lake has that offset. One publication is in flight at a time.
    let (acked_tx, mut acked_rx) = mpsc::unbounded_channel::<Option<O>>();
    let mut in_flight = false;
    let mut tick = tokio::time::interval(CHECKPOINT_WRITE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            message = rx.recv() => match message {
                Some(offset) => latest = Some(offset),
                None => break,
            },
            Some(acked) = acked_rx.recv() => {
                in_flight = false;
                if let Some(offset) = acked {
                    notify(&offset);
                }
            }
            _ = tick.tick() => {
                if !guard.may_write() {
                    // Not (or no longer certainly) the single writer: store and publish nothing.
                    continue;
                }
                if let Some(offset) = latest.take() {
                    if write_offset(&store, &offset).await {
                        failing_since = None;
                        if publisher.is_none() {
                            notify(&offset);
                        }
                        stored = Some(offset);
                    } else {
                        // Retry on the next tick unless a newer offset arrives first.
                        latest = Some(offset);
                        let since =
                            *failing_since.get_or_insert_with(tokio::time::Instant::now);
                        if since.elapsed() >= CHECKPOINT_FAILURE_LIMIT
                            && !poisoned.swap(true, Ordering::SeqCst)
                        {
                            // Nothing advances while offsets cannot be stored (a PostgreSQL slot
                            // keeps all its WAL): stop, so the failure is seen.
                            error!(
                                message = "Offsets could not be stored for too long; stopping the source.",
                                path = %store.path().display(),
                                failing_for = ?since.elapsed(),
                            );
                        }
                    }
                }
                if let Some(publisher) = publisher.as_mut()
                    && !in_flight
                    && let Some(offset) = stored.as_ref()
                    && (offset.seq() > published_seq
                        || published.as_ref().is_some_and(|p| offset.advances_without_seq(p)))
                    && last_published.is_none_or(|t| t.elapsed() >= publisher.interval)
                {
                    if let Some(receiver) = publisher.publish(offset).await {
                        in_flight = true;
                        await_delivery(receiver, offset.clone(), acked_tx.clone());
                    }
                    published_seq = offset.seq();
                    published = Some(offset.clone());
                    last_published = Some(Instant::now());
                }
            }
        }
    }
    if !guard.may_write() {
        return;
    }
    if let Some(offset) = latest.take()
        && write_offset(&store, &offset).await
    {
        if publisher.is_none() {
            notify(&offset);
        }
        stored = Some(offset);
    }
    if let Some(publisher) = publisher.as_mut()
        && let Some(offset) = stored.as_ref()
        && (offset.seq() > published_seq
            || published
                .as_ref()
                .is_some_and(|p| offset.advances_without_seq(p)))
    {
        // The source is stopping: nothing is released after this, so the delivery is not awaited.
        _ = publisher.publish(offset).await;
    }
}

async fn write_offset<O: CdcOffset>(store: &CheckpointStore<O>, offset: &O) -> bool {
    match store.store(offset).await {
        Ok(()) => {
            debug!(message = "Stored offset.", offset = %offset);
            true
        }
        Err(error) => {
            emit!(CdcCheckpointWriteError {
                error,
                path: &store.path().display().to_string(),
            });
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    struct TestOffset {
        lsn: u64,
        next_seq: u64,
        seq_sub: u64,
        snapshot: bool,
    }

    impl fmt::Display for TestOffset {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "lsn {}", self.lsn)
        }
    }

    impl CdcOffset for TestOffset {
        fn seq(&self) -> (u64, u64) {
            (self.next_seq, self.seq_sub)
        }
        fn set_seq(&mut self, next_seq: u64, seq_sub: u64) {
            self.next_seq = next_seq;
            self.seq_sub = seq_sub;
        }
        fn snapshot_in_progress(&self) -> bool {
            self.snapshot
        }
        fn publish_position(&self, payload: &mut Map<String, Json>) {
            payload.insert("lsn".into(), Json::from(self.lsn));
        }
    }

    #[tokio::test]
    async fn store_roundtrip_for_any_offset_type() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::<TestOffset>::new(dir.path());
        assert_eq!(store.load().await.unwrap(), None);
        let offset = TestOffset {
            lsn: 42,
            next_seq: 7,
            seq_sub: 0,
            snapshot: false,
        };
        store.store(&offset).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(offset));
    }

    /// Publishes one offset, delivered downstream with `status`; returns what was released.
    async fn released_when(status: vector_lib::event::EventStatus) -> Option<TestOffset> {
        use vector_lib::config::LogNamespace;

        use crate::sources::cdc::{EventFormat, RecordImages};

        let dir = tempfile::tempdir().unwrap();
        let (mut out, _changes) = SourceSender::new_test();
        let mut offsets = out.add_outputs(status, OFFSETS_OUTPUT.to_owned());
        let publisher = OffsetPublisher {
            out,
            builder: EventBuilder::new(
                "test",
                LogNamespace::Legacy,
                EventFormat::default(),
                RecordImages::default(),
            ),
            server_name: "test".into(),
            interval: Duration::ZERO,
        };
        let (release, released) = tokio::sync::watch::channel(None);
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = tokio::spawn(checkpoint_writer(
            rx,
            CheckpointStore::<TestOffset>::new(dir.path()),
            Some(publisher),
            WriterGuard::unlocked(),
            Some(release),
            Arc::new(AtomicBool::new(false)),
        ));
        tx.send(TestOffset {
            lsn: 7,
            next_seq: 2,
            seq_sub: 0,
            snapshot: false,
        })
        .unwrap();
        // Stored and published; the offsets "sink" finalizes it with `status`.
        drop(offsets.next().await.unwrap());
        tokio::time::sleep(CHECKPOINT_WRITE_INTERVAL * 2).await;
        drop(tx);
        writer.await.unwrap();
        released.borrow().clone()
    }

    #[tokio::test]
    async fn a_published_offset_releases_the_log_only_once_delivered() {
        use vector_lib::event::EventStatus;

        assert_eq!(released_when(EventStatus::Errored).await, None);
        assert_eq!(
            released_when(EventStatus::Delivered).await.map(|o| o.lsn),
            Some(7)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn offsets_that_cannot_be_stored_stop_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let poisoned = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = tokio::spawn(checkpoint_writer(
            rx,
            CheckpointStore::<TestOffset>::new(&dir.path().join("missing")),
            None,
            WriterGuard::unlocked(),
            None,
            Arc::clone(&poisoned),
        ));
        tx.send(TestOffset::default()).unwrap();
        // Just short of the limit.
        tokio::time::sleep(Duration::from_secs(295)).await;
        assert!(
            !poisoned.load(Ordering::SeqCst),
            "a passing failure is retried"
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(
            poisoned.load(Ordering::SeqCst),
            "a lasting failure stops the source"
        );
        drop(tx);
        writer.await.unwrap();
    }

    #[test]
    fn published_offset_shape() {
        let offset = TestOffset {
            lsn: 99,
            next_seq: 9101,
            seq_sub: 3,
            snapshot: true,
        };
        let payload = OffsetPublisher::payload("orders", &offset);
        let keys: Vec<&str> = payload.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "connector",
                "seq",
                "change_seq",
                "lsn",
                "snapshot_in_progress",
                "committed_at",
                "checkpoint"
            ]
        );
        assert_eq!(
            payload["seq"],
            Json::from("00000000000000009100.0000000003")
        );
        assert_eq!(payload["change_seq"], Json::from(9100));
        assert_eq!(payload["snapshot_in_progress"], Json::from(true));
        let checkpoint: TestOffset = serde_json::from_value(payload["checkpoint"].clone()).unwrap();
        assert_eq!(checkpoint, offset);
    }
}
