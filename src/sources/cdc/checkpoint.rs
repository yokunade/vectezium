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
    event::BatchStatus,
    internal_events::{CdcCheckpointWriteError, CdcDeliveryFailed},
};

/// The name of the checkpoint file inside the source's data directory.
pub const CHECKPOINT_FILENAME: &str = "offsets.json";

/// How often the checkpoint file is rewritten while offsets keep advancing.
const CHECKPOINT_WRITE_INTERVAL: Duration = Duration::from_secs(1);

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
    /// `file`, `pos` for MySQL; `lsn` for PostgreSQL).
    fn publish_position(&self, payload: &mut Map<String, Json>);

    /// Whether this offset should be published although its `seq` equals that of `published`:
    /// its log position moved on while no captured changes flowed. A connector whose server
    /// keeps log until offsets are published (a PostgreSQL slot with `publish_offsets`) needs
    /// this so an idle source keeps releasing log.
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
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join(CHECKPOINT_FILENAME),
            tmp_path: data_dir.join(format!("{CHECKPOINT_FILENAME}.tmp")),
            _offset: PhantomData,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads the stored offset, if any.
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
        Self::start_notifying(acknowledgements, store, poisoned, publisher, guard, written, None)
    }

    /// Like [`Self::start`], and also reports on `stored` every offset the source may forget
    /// the log before: each stored offset, or with `publish_offsets` each published one (a
    /// source that lost its data directory resumes from the published offset). A connector whose
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
        crate::spawn_in_current_span(async move {
            checkpoint_writer(rx, store, publisher, guard, stored).await;
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

    /// Sends the offset event. Not acknowledged: a slow offsets sink must never hold back
    /// checkpoints, and a lost event is superseded by the next one.
    async fn publish<O: CdcOffset>(&mut self, offset: &O) {
        let event = self.builder.plain(
            Self::payload(&self.server_name, offset),
            EventMeta {
                key: None,
                database: "",
                table: None,
                op: "offset",
                timestamp: Utc::now(),
                schema_version: None,
            },
        );
        if self
            .out
            .send_batch_named(OFFSETS_OUTPUT, vec![event])
            .await
            .is_err()
        {
            debug!(message = "The offsets output is closed; offset not published.");
        }
    }
}

/// Coalesces offsets and writes the newest one at most once per interval; publishes stored
/// offsets (at most once per publish interval) when `publish_offsets` is on. Writes and
/// publishes nothing while `guard` says this instance may not write.
async fn checkpoint_writer<O: CdcOffset>(
    mut rx: mpsc::UnboundedReceiver<O>,
    store: CheckpointStore<O>,
    mut publisher: Option<OffsetPublisher>,
    guard: WriterGuard,
    stored_tx: Option<tokio::sync::watch::Sender<Option<O>>>,
) {
    let notify = |offset: &O| {
        if let Some(tx) = &stored_tx {
            tx.send_replace(Some(offset.clone()));
        }
    };
    let mut latest: Option<O> = None;
    let mut stored: Option<O> = None;
    let mut published_seq = (0, 0);
    let mut published: Option<O> = None;
    let mut last_published: Option<Instant> = None;
    let mut tick = tokio::time::interval(CHECKPOINT_WRITE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            message = rx.recv() => match message {
                Some(offset) => latest = Some(offset),
                None => break,
            },
            _ = tick.tick() => {
                if !guard.may_write() {
                    // Not (or no longer certainly) the single writer: store and publish nothing.
                    continue;
                }
                if let Some(offset) = latest.take() {
                    if write_offset(&store, &offset).await {
                        if publisher.is_none() {
                            notify(&offset);
                        }
                        stored = Some(offset);
                    } else {
                        // Retry on the next tick unless a newer offset arrives first.
                        latest = Some(offset);
                    }
                }
                if let Some(publisher) = publisher.as_mut()
                    && let Some(offset) = stored.as_ref()
                    && (offset.seq() > published_seq
                        || published.as_ref().is_some_and(|p| offset.advances_without_seq(p)))
                    && last_published.is_none_or(|t| t.elapsed() >= publisher.interval)
                {
                    publisher.publish(offset).await;
                    notify(offset);
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
            || published.as_ref().is_some_and(|p| offset.advances_without_seq(p)))
    {
        publisher.publish(offset).await;
        notify(offset);
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
