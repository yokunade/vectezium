//! Batching of change events, `seq` numbering and hand-over of offsets for checkpointing.
//!
//! A connector reads its change stream, builds row events with the [`EventBuilder`] and queues
//! them on the [`Emitter`]; at every transaction boundary it calls [`Emitter::commit`] with its
//! offset. The emitter numbers events (`seq`), sends them on the default, `control` and
//! `table_schemas` outputs, and passes each offset to the finalizer only with the batch that
//! holds the last event of its transaction, so an offset is stored only once everything before
//! it was delivered.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde_json::{Map, Value as Json};
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    config::{LegacyKey, LogNamespace},
    internal_event::{CountByteSize, EventsReceived, InternalEventHandle as _, Registered},
    lookup::{OwnedValuePath, event_path, path},
};
use vrl::value::Value;

use super::{
    CONTROL_OUTPUT, EventFormat, OBSERVE_OUTPUT, RecordImages, TABLE_SCHEMAS_OUTPUT,
    checkpoint::{CdcOffset, Finalizer},
    envelope::{Op, SnapshotMarker, SourceBlock, change_event, record_event},
    seq::master_seq,
};
use crate::{
    SourceSender,
    config::log_schema,
    event::{BatchNotifier, BatchStatusReceiver, Finalizable, LogEvent},
    internal_events::StreamClosedError,
};

/// The longest an event waits in the pending batch before it is sent downstream.
const MAX_BATCH_LATENCY: Duration = Duration::from_millis(250);

/// How often an offset is enqueued while no captured changes flow (for example, when only
/// filtered tables are written to), so the stored position keeps up with the log.
const IDLE_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);

/// Batches change events, sends them downstream and hands offsets to the finalizer.
///
/// Offsets are only ever attached to the batch containing the last event of the transaction
/// they follow, so an offset becomes durable only when everything before it was delivered.
pub struct Emitter<O: CdcOffset> {
    out: SourceSender,
    builder: EventBuilder,
    acknowledgements: bool,
    finalizer: Finalizer<O>,
    batch_size: usize,
    events: Vec<LogEvent>,
    control_events: Vec<LogEvent>,
    table_schema_events: Vec<LogEvent>,
    committed_events: usize,
    committed_control_events: usize,
    committed_table_schema_events: usize,
    /// The `seq` of the next change event (from the transaction log); and its value at the last
    /// commit (restored when an incomplete transaction is discarded, so replays get the same
    /// numbers). Only change events advance it, so it is identical on every replay.
    next_seq: u64,
    committed_seq: u64,
    /// Snapshot rows and snapshot control events take the current `seq` and the next `seq_sub`,
    /// placing them between change events without consuming the counter (their timing differs
    /// between runs).
    seq_sub: u64,
    committed_sub: u64,
    include_table_schemas: bool,
    /// Schema versions already emitted by this process.
    emitted_schema_versions: HashSet<String>,
    /// `<database>.<table>` → foreign keys (as read at start-up), for schema documents.
    foreign_keys: HashMap<String, Json>,
    batch: Option<BatchNotifier>,
    receiver: Option<BatchStatusReceiver>,
    pending_offset: Option<O>,
    first_pending_at: Option<Instant>,
    last_offset_enqueued_at: Instant,
    events_received: Registered<EventsReceived>,
    poisoned: Arc<AtomicBool>,
    /// Also send every event, without acknowledgement, on the `observe` output.
    observe: bool,
}

/// Builds log events from change payloads. Cheap to copy, so parallel snapshot readers can
/// build events on their own tasks.
#[derive(Clone, Copy, Debug)]
pub struct EventBuilder {
    /// The source type (`mysql_cdc`, …) for Vector's source metadata.
    source_type: &'static str,
    log_namespace: LogNamespace,
    event_format: EventFormat,
    record_images: RecordImages,
}

impl EventBuilder {
    #[must_use]
    pub const fn new(
        source_type: &'static str,
        log_namespace: LogNamespace,
        event_format: EventFormat,
        record_images: RecordImages,
    ) -> Self {
        Self {
            source_type,
            log_namespace,
            event_format,
            record_images,
        }
    }

    /// Builds a row-change event in the configured `event_format`.
    pub fn row_change<S: SourceBlock + ?Sized>(
        &self,
        op: Op,
        before: Option<Map<String, Json>>,
        after: Option<Map<String, Json>>,
        source: &S,
        meta: EventMeta<'_>,
    ) -> LogEvent {
        let payload = match self.event_format {
            EventFormat::Debezium => change_event(op, before, after, source),
            EventFormat::Record => record_event(
                op,
                before,
                after,
                source,
                meta.key.as_ref(),
                meta.schema_version,
                self.record_images,
            ),
        };
        self.build(payload, meta)
    }

    /// Sets the snapshot marker of an already built row-change event.
    pub fn set_snapshot_marker(&self, log: &mut LogEvent, marker: SnapshotMarker) {
        let path = match self.event_format {
            EventFormat::Debezium => event_path!("source", "snapshot"),
            EventFormat::Record => event_path!("metadata", "snapshot"),
        };
        log.insert(path, marker.as_str());
    }

    /// Sets the `snapshot_id` of a snapshot event (`metadata.snapshot_id`, or
    /// `source.snapshot_id` in the Debezium format).
    pub fn set_snapshot_id(&self, log: &mut LogEvent, snapshot_id: &str) {
        let path = match self.event_format {
            EventFormat::Debezium => event_path!("source", "snapshot_id"),
            EventFormat::Record => event_path!("metadata", "snapshot_id"),
        };
        log.insert(path, snapshot_id.to_owned());
    }

    /// Sets the `seq` of a row event (`metadata.seq`, or `source.seq` in the Debezium format).
    pub fn set_seq(&self, log: &mut LogEvent, seq: u64, seq_sub: u64) {
        let path = match self.event_format {
            EventFormat::Debezium => event_path!("source", "seq"),
            EventFormat::Record => event_path!("metadata", "seq"),
        };
        log.insert(path, master_seq(seq, seq_sub));
    }

    /// Builds an event from a plain payload (control and offset events).
    #[must_use]
    pub fn plain(&self, payload: Map<String, Json>, meta: EventMeta<'_>) -> LogEvent {
        self.build(payload, meta)
    }

    fn build(&self, payload: Map<String, Json>, meta: EventMeta<'_>) -> LogEvent {
        let mut log = LogEvent::try_from(Json::Object(payload))
            .expect("change event payload is a JSON object");

        let ns = self.log_namespace;
        ns.insert_standard_vector_source_metadata(&mut log, self.source_type, Utc::now());
        ns.insert_source_metadata(
            self.source_type,
            &mut log,
            None::<LegacyKey<&OwnedValuePath>>,
            path!("key"),
            meta.key
                .map_or(Value::Null, |k| Value::from(Json::Object(k))),
        );
        ns.insert_source_metadata(
            self.source_type,
            &mut log,
            None::<LegacyKey<&OwnedValuePath>>,
            path!("database"),
            meta.database.to_owned(),
        );
        ns.insert_source_metadata(
            self.source_type,
            &mut log,
            None::<LegacyKey<&OwnedValuePath>>,
            path!("table"),
            meta.table
                .map_or(Value::Null, |t| Value::from(t.to_owned())),
        );
        ns.insert_source_metadata(
            self.source_type,
            &mut log,
            None::<LegacyKey<&OwnedValuePath>>,
            path!("op"),
            meta.op.to_owned(),
        );
        // Using the commit time as the event timestamp makes Vector's
        // `source_lag_time_seconds` histogram report replication lag.
        ns.insert_source_metadata(
            self.source_type,
            &mut log,
            log_schema().timestamp_key().map(LegacyKey::Overwrite),
            path!("timestamp"),
            meta.timestamp,
        );
        log
    }
}

/// Metadata attached to every emitted change event.
pub struct EventMeta<'a> {
    pub key: Option<Map<String, Json>>,
    pub database: &'a str,
    pub table: Option<&'a str>,
    pub op: &'a str,
    /// Commit time of the change.
    pub timestamp: DateTime<Utc>,
    pub schema_version: Option<&'a str>,
}

impl<O: CdcOffset> Emitter<O> {
    pub fn new(
        out: SourceSender,
        builder: EventBuilder,
        acknowledgements: bool,
        finalizer: Finalizer<O>,
        batch_size: usize,
        poisoned: Arc<AtomicBool>,
        include_table_schemas: bool,
    ) -> Self {
        Self {
            builder,
            include_table_schemas,
            table_schema_events: Vec::new(),
            committed_table_schema_events: 0,
            emitted_schema_versions: HashSet::new(),
            foreign_keys: HashMap::new(),
            out,
            acknowledgements,
            finalizer,
            batch_size,
            events: Vec::with_capacity(batch_size),
            control_events: Vec::new(),
            committed_events: 0,
            committed_control_events: 0,
            next_seq: 1,
            committed_seq: 1,
            seq_sub: 0,
            committed_sub: 0,
            batch: None,
            receiver: None,
            pending_offset: None,
            first_pending_at: None,
            last_offset_enqueued_at: Instant::now(),
            events_received: register!(EventsReceived),
            poisoned,
            observe: false,
        }
    }

    /// Also sends an unacknowledged copy of every event on the `observe` output
    /// (`observe_output`).
    #[must_use]
    pub const fn with_observe(mut self, observe: bool) -> Self {
        self.observe = observe;
        self
    }

    /// Sets the foreign keys included in schema documents.
    pub fn set_foreign_keys(&mut self, foreign_keys: HashMap<String, Json>) {
        self.foreign_keys = foreign_keys;
    }

    /// Changes the maximum batch size (used while the snapshot runs); returns the previous one.
    pub fn set_batch_size(&mut self, batch_size: usize) -> usize {
        std::mem::replace(&mut self.batch_size, batch_size.max(1))
    }

    /// Fails once a downstream delivery failure has been reported.
    ///
    /// # Errors
    ///
    /// Fails after a batch was rejected downstream.
    pub fn check_health(&self) -> Result<(), String> {
        if self.poisoned.load(Ordering::SeqCst) {
            Err(
                "a batch of change events was rejected downstream; stopping so the changes are \
                 replayed from the last delivered offset after a restart"
                    .into(),
            )
        } else {
            Ok(())
        }
    }

    fn make_event(&mut self, payload: Map<String, Json>, meta: EventMeta<'_>) -> LogEvent {
        self.make_event_with(payload, meta, true)
    }

    /// Builds an event; with `acknowledge`, it joins the current batch so the batch's offset is
    /// only stored once the event is delivered.
    fn make_event_with(
        &mut self,
        payload: Map<String, Json>,
        meta: EventMeta<'_>,
        acknowledge: bool,
    ) -> LogEvent {
        let log = self.builder.build(payload, meta);
        if acknowledge {
            self.attach(log)
        } else {
            self.touch();
            log
        }
    }

    fn touch(&mut self) {
        if self.first_pending_at.is_none() {
            self.first_pending_at = Some(Instant::now());
        }
    }

    /// Adds `log` to the current acknowledgement batch.
    fn attach(&mut self, log: LogEvent) -> LogEvent {
        if self.acknowledgements && self.batch.is_none() {
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            self.batch = Some(batch);
            self.receiver = Some(receiver);
        }
        self.touch();
        log.with_batch_notifier_option(&self.batch)
    }

    /// The event builder, for building events on other tasks (parallel snapshot readers).
    pub const fn builder(&self) -> EventBuilder {
        self.builder
    }

    /// Starts numbering events at `next_seq` (from the stored offset).
    pub fn set_next_seq(&mut self, next_seq: u64, seq_sub: u64) {
        self.next_seq = next_seq.max(1);
        self.committed_seq = self.next_seq;
        self.seq_sub = seq_sub;
        self.committed_sub = seq_sub;
    }

    /// The `seq` the next event will get.
    pub const fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The `(seq, seq_sub)` of the next change event.
    const fn take_seq(&mut self) -> (u64, u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.seq_sub = 0;
        (seq, 0)
    }

    /// The `(seq, seq_sub)` of the next snapshot-derived event: after the last change event.
    const fn take_snapshot_seq(&mut self) -> (u64, u64) {
        self.seq_sub += 1;
        (self.next_seq - 1, self.seq_sub)
    }

    /// Queues a row change (from the transaction log) built with [`Self::builder`], giving it
    /// the next `seq`.
    pub fn push_prebuilt(&mut self, mut log: LogEvent) {
        let (seq, sub) = self.take_seq();
        self.builder.set_seq(&mut log, seq, sub);
        let log = self.attach(log);
        self.events.push(log);
    }

    /// Queues a snapshot row (`op: r`), placed after the last change event (`seq_sub`).
    pub fn push_snapshot_row(&mut self, mut log: LogEvent) {
        let (seq, sub) = self.take_snapshot_seq();
        self.builder.set_seq(&mut log, seq, sub);
        let log = self.attach(log);
        self.events.push(log);
    }

    /// Queues a row change in the configured `event_format`.
    pub fn push_row_change<S: SourceBlock + ?Sized>(
        &mut self,
        op: Op,
        before: Option<Map<String, Json>>,
        after: Option<Map<String, Json>>,
        source: &S,
        meta: EventMeta<'_>,
    ) {
        let log = self.builder.row_change(op, before, after, source, meta);
        self.push_prebuilt(log);
    }

    /// Queues a connector event (`schema_change`, `truncate`, `snapshot_completed`) for the
    /// `control` output, giving it the next `seq`. Acknowledged like row events, so an offset
    /// is only stored once the control events before it are delivered.
    ///
    /// `from_snapshot` events (`snapshot_completed`) are numbered like snapshot rows.
    pub fn push_control(
        &mut self,
        mut payload: Map<String, Json>,
        meta: EventMeta<'_>,
        from_snapshot: bool,
    ) {
        let (seq, sub) = if from_snapshot {
            self.take_snapshot_seq()
        } else {
            self.take_seq()
        };
        payload.insert("seq".into(), Json::from(master_seq(seq, sub)));
        let event = self.make_event(payload, meta);
        self.control_events.push(event);
    }

    /// Queues a table's schema document for the `table_schemas` output, the first time this
    /// process sees its schema version. `document` builds it (only called when it is new).
    pub fn note_schema_document(
        &mut self,
        database: &str,
        table: &str,
        schema_version: &str,
        document: impl FnOnce() -> Map<String, Json>,
    ) {
        if !self.include_table_schemas || self.emitted_schema_versions.contains(schema_version) {
            return;
        }
        self.emitted_schema_versions
            .insert(schema_version.to_owned());
        let mut doc = document();
        // Not part of the schema version: read once at start-up, not from the change stream.
        if let Some(fks) = self.foreign_keys.get(&format!("{database}.{table}")) {
            doc.insert("foreign_keys".into(), fks.clone());
        }
        // Schema documents are re-emitted after every start, so they do not need to hold back
        // checkpoints: a slowly batching schema sink must not delay offsets for all tables.
        let event = self.make_event_with(
            doc,
            EventMeta {
                key: None,
                database,
                table: Some(table),
                op: "schema",
                timestamp: Utc::now(),
                schema_version: Some(schema_version),
            },
            false,
        );
        self.table_schema_events.push(event);
    }

    /// Marks everything queued so far as belonging to transactions that end at `offset`.
    pub fn commit(&mut self, mut offset: O) {
        offset.set_seq(self.next_seq, self.seq_sub);
        self.committed_sub = self.seq_sub;
        self.committed_seq = self.next_seq;
        self.pending_offset = Some(offset);
        self.committed_events = self.events.len();
        self.committed_control_events = self.control_events.len();
        self.committed_table_schema_events = self.table_schema_events.len();
    }

    /// Drops queued events of a transaction that did not complete (e.g. the connection broke).
    pub fn discard_uncommitted(&mut self) {
        self.events.truncate(self.committed_events);
        self.control_events.truncate(self.committed_control_events);
        // The transaction is read again from its start, so its events get the same numbers.
        self.next_seq = self.committed_seq;
        self.seq_sub = self.committed_sub;
        if self.table_schema_events.len() > self.committed_table_schema_events {
            // A discarded schema document must be emitted again; forgetting all versions is
            // simplest and only causes harmless repeats.
            self.table_schema_events
                .truncate(self.committed_table_schema_events);
            self.emitted_schema_versions.clear();
        }
        if self.pending_len() == 0 {
            self.first_pending_at = None;
        }
    }

    const fn pending_len(&self) -> usize {
        self.events.len() + self.control_events.len() + self.table_schema_events.len()
    }

    /// Flushes when the batch is full or has waited long enough, and enqueues an offset now and
    /// then while nothing is captured.
    ///
    /// # Errors
    ///
    /// Fails if the events cannot be sent downstream (see [`Self::flush`]).
    pub async fn maybe_flush(&mut self) -> Result<(), String> {
        let full = self.pending_len() >= self.batch_size;
        let stale = self
            .first_pending_at
            .is_some_and(|t| t.elapsed() >= MAX_BATCH_LATENCY);
        let idle_offset = self.pending_len() == 0
            && self.pending_offset.is_some()
            && self.last_offset_enqueued_at.elapsed() >= IDLE_CHECKPOINT_INTERVAL;
        if full || stale || idle_offset {
            self.flush().await?;
        }
        Ok(())
    }

    /// Sends all queued events downstream and hands the pending offset to the finalizer.
    ///
    /// # Errors
    ///
    /// Fails if the downstream channel is closed (the topology is shutting down).
    pub async fn flush(&mut self) -> Result<(), String> {
        let offset = self.pending_offset.take();

        if self.pending_len() == 0 {
            if let Some(offset) = offset {
                self.enqueue_offset(offset, None);
            }
            return Ok(());
        }

        // Release our handle so the status resolves once all events are finalized.
        drop(self.batch.take());
        let receiver = self.receiver.take();

        let count = self.pending_len();
        let byte_size = self.events.estimated_json_encoded_size_of()
            + self.control_events.estimated_json_encoded_size_of()
            + self.table_schema_events.estimated_json_encoded_size_of();
        self.events_received.emit(CountByteSize(count, byte_size));

        // Copies for `observe`, taken before the originals are sent. Without finalizers they take
        // no part in acknowledgement: whatever happens to them downstream (slow, failed,
        // dropped) never holds back or fails a checkpoint.
        let observed: Vec<LogEvent> = if self.observe {
            self.table_schema_events
                .iter()
                .chain(&self.events)
                .chain(&self.control_events)
                .map(unacknowledged_copy)
                .collect()
        } else {
            Vec::new()
        };

        // Schema documents go first so they tend to land before the rows that reference them.
        if !self.table_schema_events.is_empty() {
            let events = std::mem::take(&mut self.table_schema_events);
            if self
                .out
                .send_batch_named(TABLE_SCHEMAS_OUTPUT, events)
                .await
                .is_err()
            {
                emit!(StreamClosedError { count });
                return Err("the downstream pipeline closed".into());
            }
        }
        if !self.events.is_empty() {
            let events = std::mem::replace(&mut self.events, Vec::with_capacity(self.batch_size));
            if self.out.send_batch(events).await.is_err() {
                emit!(StreamClosedError { count });
                return Err("the downstream pipeline closed".into());
            }
        }
        if !self.control_events.is_empty() {
            let events = std::mem::take(&mut self.control_events);
            if self
                .out
                .send_batch_named(CONTROL_OUTPUT, events)
                .await
                .is_err()
            {
                emit!(StreamClosedError { count });
                return Err("the downstream pipeline closed".into());
            }
        }
        // Sent after the tracked outputs. Sinks on `observe` should drop events when full
        // (`buffer.when_full: drop_newest`) so a slow viewer cannot delay the next batch.
        if !observed.is_empty()
            && self
                .out
                .send_batch_named(OBSERVE_OUTPUT, observed)
                .await
                .is_err()
        {
            debug!(message = "The observe output is closed; copies not sent.");
        }

        self.committed_events = 0;
        self.committed_control_events = 0;
        self.committed_table_schema_events = 0;
        self.first_pending_at = None;

        match offset {
            Some(offset) => self.enqueue_offset(offset, receiver),
            None => {
                if let (Finalizer::Async(finalizer), Some(receiver)) = (&self.finalizer, receiver) {
                    // Track the batch anyway so a delivery failure stops checkpointing.
                    finalizer.add(None, receiver);
                }
            }
        }
        Ok(())
    }

    fn enqueue_offset(&mut self, offset: O, receiver: Option<BatchStatusReceiver>) {
        self.last_offset_enqueued_at = Instant::now();
        match &self.finalizer {
            Finalizer::Sync(tx) => {
                _ = tx.send(offset);
            }
            Finalizer::Async(finalizer) => {
                let receiver = receiver.unwrap_or_else(|| {
                    // An offset with no events of its own: an empty batch resolves as
                    // delivered, in order behind every batch enqueued before it.
                    let (batch, receiver) = BatchNotifier::new_with_receiver();
                    drop(batch);
                    receiver
                });
                finalizer.add(Some(offset), receiver);
            }
        }
    }

    /// Flushes committed events and releases the finalizer so pending acknowledgements drain.
    ///
    /// # Errors
    ///
    /// Fails if the last events cannot be sent downstream (see [`Self::flush`]).
    pub async fn close(mut self) -> Result<(), String> {
        self.discard_uncommitted();
        self.flush().await
    }
}

/// A copy of `event` that is not part of its acknowledgement batch.
fn unacknowledged_copy(event: &LogEvent) -> LogEvent {
    let mut copy = event.clone();
    // Releases the copy's shares of the finalizers; the original still holds them.
    drop(copy.take_finalizers());
    copy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{BatchStatus, EventStatus};

    #[tokio::test]
    async fn observed_copies_take_no_part_in_acknowledgement() {
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let original = LogEvent::from("change").with_batch_notifier(&batch);
        drop(batch);

        // Whatever happens to the copy downstream does not reach the batch.
        let copy = unacknowledged_copy(&original);
        copy.metadata().update_status(EventStatus::Errored);
        drop(copy);

        original.metadata().update_status(EventStatus::Delivered);
        drop(original);
        assert_eq!(receiver.await, BatchStatus::Delivered);
    }
}
