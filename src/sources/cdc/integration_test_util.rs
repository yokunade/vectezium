//! Shared harness of the CDC sources' integration tests (`postgres_cdc` and `mysql_cdc`, against
//! the servers in `tests/integration/cdc/compose.yaml`): runs a source in the test, reads its
//! outputs, and sets up the Azurite blobs of the single-writer lock and offset recovery.

#![allow(dead_code, reason = "each connector's tests use part of the harness")]

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use azure_core::http::{RequestContent, StatusCode};
use futures::{Stream, StreamExt, stream};
use serde_json::Value as Json;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use vector_lib::event::EventContainer;

use super::{
    CONTROL_OUTPUT, OFFSETS_OUTPUT, TABLE_SCHEMAS_OUTPUT,
    lock::{LockClient, SingleWriterLockConfig},
};
use crate::{
    SourceSender,
    config::{ComponentKey, ProxyConfig, SourceConfig, SourceContext},
    event::{Event, EventStatus},
    shutdown::SourceShutdownCoordinator,
    test_util::{temp_dir, trace_init},
};

/// How long to wait for anything the source should do.
pub(crate) const WAIT: Duration = Duration::from_mins(1);

/// The container the lock and recovery tests use.
const CONTAINER: &str = "cdc-it";

pub(crate) type Events = Pin<Box<dyn Stream<Item = Event> + Send>>;

pub(crate) fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

pub(crate) fn new_dir() -> PathBuf {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).expect("failed to create a temporary directory");
    dir
}

pub(crate) fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// A running source. Row events and published offsets are acknowledged as the test reads them,
/// so a stream the test does not read is a stalled sink. Control events take part in
/// checkpoints too, so they are always read, into `control`; so are table schemas.
pub(crate) struct Running {
    pub(crate) events: Events,
    pub(crate) control: mpsc::UnboundedReceiver<Json>,
    pub(crate) table_schemas: mpsc::UnboundedReceiver<Json>,
    pub(crate) offsets: Option<Events>,
    shutdown: SourceShutdownCoordinator,
    /// Whether the source ended without an error.
    task: JoinHandle<bool>,
}

fn named_output(out: &mut SourceSender, name: &str) -> Events {
    Box::pin(
        out.add_outputs(EventStatus::Delivered, name.to_owned())
            .flat_map(|item| stream::iter(item.events.into_events())),
    )
}

/// An output read in the background into a channel.
fn drained_output(out: &mut SourceSender, name: &str) -> mpsc::UnboundedReceiver<Json> {
    let mut events = named_output(out, name);
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            _ = tx.send(to_json(&event));
        }
    });
    rx
}

pub(crate) fn to_json(event: &Event) -> Json {
    serde_json::to_value(event.as_log()).expect("an event that serializes")
}

/// Builds and runs a source whose config enables `publish_offsets` and `include_table_schemas`
/// as given.
pub(crate) async fn start_source(
    config: &impl SourceConfig,
    publish_offsets: bool,
    table_schemas: bool,
) -> Running {
    trace_init();
    let (mut out, events) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let control = drained_output(&mut out, CONTROL_OUTPUT);
    let table_schemas = if table_schemas {
        drained_output(&mut out, TABLE_SCHEMAS_OUTPUT)
    } else {
        mpsc::unbounded_channel().1
    };
    let offsets = publish_offsets.then(|| named_output(&mut out, OFFSETS_OUTPUT));
    let (mut cx, shutdown) = SourceContext::new_shutdown(&ComponentKey::from("cdc"), out);
    cx.acknowledgements = true;
    let source = config.build(cx).await.expect("failed to build the source");
    Running {
        events: Box::pin(events),
        control,
        table_schemas,
        offsets,
        shutdown,
        task: tokio::spawn(async move { source.await.is_ok() }),
    }
}

pub(crate) async fn next(events: &mut Events, what: &str) -> Json {
    let event = timeout(WAIT, events.next())
        .await
        .unwrap_or_else(|_| panic!("no {what} within {WAIT:?}"))
        .unwrap_or_else(|| panic!("the {what} stream ended"));
    to_json(&event)
}

impl Running {
    pub(crate) async fn rows(&mut self, n: usize) -> Vec<Json> {
        let mut rows = Vec::with_capacity(n);
        for _ in 0..n {
            rows.push(next(&mut self.events, "row event").await);
        }
        rows
    }

    /// The next `control` event of type `kind`, skipping others.
    pub(crate) async fn control_event(&mut self, kind: &str) -> Json {
        loop {
            let event = timeout(WAIT, self.control.recv())
                .await
                .unwrap_or_else(|_| panic!("no {kind} event within {WAIT:?}"))
                .expect("the control stream ended");
            if event["type"] == kind {
                return event;
            }
        }
    }

    pub(crate) async fn table_schema(&mut self) -> Json {
        timeout(WAIT, self.table_schemas.recv())
            .await
            .unwrap_or_else(|_| panic!("no table schema within {WAIT:?}"))
            .expect("the table schema stream ended")
    }

    pub(crate) async fn no_rows_for(&mut self, period: Duration) {
        if let Ok(Some(event)) = timeout(period, self.events.next()).await {
            panic!("unexpected row event: {event:?}");
        }
    }

    /// Shuts the source down cleanly; whether it ended without an error.
    pub(crate) async fn stop(self) -> bool {
        self.shutdown
            .shutdown_all(Some(Instant::now() + Duration::from_secs(30)))
            .await;
        timeout(WAIT, self.task)
            .await
            .expect("the source did not stop")
            .expect("the source panicked")
    }

    /// Waits for the source to end on its own; whether it ended without an error.
    pub(crate) async fn exit(self) -> bool {
        timeout(WAIT, self.task)
            .await
            .expect("the source kept running")
            .expect("the source panicked")
    }

    /// Ends the source as a crash would: nothing more is stored.
    pub(crate) fn kill(self) {
        self.task.abort();
    }
}

pub(crate) fn op(event: &Json) -> &str {
    event["metadata"]["op"].as_str().expect("metadata.op")
}

pub(crate) fn seq(event: &Json) -> &str {
    event["metadata"]["seq"].as_str().expect("metadata.seq")
}

pub(crate) fn after(event: &Json) -> &Json {
    &event["record"]["after"]
}

pub(crate) fn before(event: &Json) -> &Json {
    &event["record"]["before"]
}

pub(crate) fn id(event: &Json) -> i64 {
    after(event)["id"].as_i64().expect("record.after.id")
}

pub(crate) fn ids(events: &[Json]) -> Vec<i64> {
    let mut ids: Vec<i64> = events.iter().map(id).collect();
    ids.sort_unstable();
    ids
}

pub(crate) fn columns(event: &Json) -> BTreeSet<String> {
    after(event)
        .as_object()
        .expect("record.after")
        .keys()
        .cloned()
        .collect()
}

/// Writes a signal to `<folder>/pending/`, under another name first so it appears whole.
pub(crate) fn write_signal(folder: &Path, name: &str, signal: &Json) {
    let pending = folder.join("pending");
    std::fs::create_dir_all(&pending).unwrap();
    let tmp = pending.join(format!(".{name}.tmp"));
    std::fs::write(&tmp, signal.to_string()).unwrap();
    std::fs::rename(tmp, pending.join(format!("{name}.json"))).unwrap();
}

/// The signal with this `id` in `<folder>/processed/`.
pub(crate) fn processed_signal(folder: &Path, id: &str) -> Json {
    std::fs::read_dir(folder.join("processed"))
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .map(|text| serde_json::from_str::<Json>(&text).unwrap())
        .find(|signal| signal["id"] == id)
        .expect("the signal is not in processed/")
}

pub(crate) async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + WAIT;
    while !condition().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(250)).await;
    }
}

fn azurite_connection_string() -> String {
    let address = env("AZURITE_ADDRESS", "localhost");
    format!(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{address}:10000/devstoreaccount1;"
    )
}

/// The single-writer lock on `<prefix>/_lock`, as YAML fields.
fn lock_fields(prefix: &str) -> String {
    format!(
        "connection_string: \"{}\"\ncontainer_name: {CONTAINER}\nblob_name: {prefix}/_lock\n\
         lease_duration_secs: 15\n",
        azurite_connection_string(),
    )
}

fn lock(prefix: &str) -> LockClient {
    let config: SingleWriterLockConfig = serde_yaml::from_str(&lock_fields(prefix)).unwrap();
    config.build(&ProxyConfig::default()).unwrap()
}

/// `single_writer_lock` on `<prefix>/_lock` for a source's config, once the container exists.
pub(crate) async fn lock_yaml(prefix: &str) -> String {
    if let Err(error) = lock(prefix).container().create(None).await {
        assert_eq!(
            error.http_status(),
            Some(StatusCode::Conflict),
            "failed to create the container: {error}"
        );
    }
    let fields = lock_fields(prefix).trim_end().replace('\n', "\n  ");
    format!("single_writer_lock:\n  {fields}\n")
}

/// `offset_recovery` from `<prefix>/_offsets/latest.json` (needs `lock_yaml(prefix)`).
pub(crate) fn recovery_yaml(prefix: &str) -> String {
    format!(
        "publish_offsets: true\noffset_recovery:\n  latest_blob: {prefix}/_offsets/latest.json\n  \
         folder: {prefix}/\n"
    )
}

/// Uploads `offset` as `<prefix>/_offsets/latest.json`, as the `azure_blob` sink of the example
/// configs would.
pub(crate) async fn publish_latest(prefix: &str, offset: &Json) {
    let mut offset = offset.clone();
    let object = offset.as_object_mut().unwrap();
    object.remove("source_type");
    object.remove("timestamp");
    lock(prefix)
        .container()
        .blob_client(&format!("{prefix}/_offsets/latest.json"))
        .upload(
            RequestContent::from(format!("{offset}\n").into_bytes()),
            None,
        )
        .await
        .expect("failed to upload latest.json");
}

/// Reads published offsets until one has reached `seq`, and returns it.
pub(crate) async fn latest_offset(offsets: &mut Events, seq: &str) -> Json {
    let mut latest = Json::Null;
    while latest.get("seq").and_then(Json::as_str) < Some(seq) {
        latest = next(offsets, "published offset").await;
    }
    latest
}

/// The last offset still queued on `offsets`, or `latest`.
pub(crate) async fn drain_offsets(offsets: &mut Events, mut latest: Json) -> Json {
    while let Ok(Some(event)) = timeout(Duration::from_secs(2), offsets.next()).await {
        latest = to_json(&event);
    }
    latest
}
