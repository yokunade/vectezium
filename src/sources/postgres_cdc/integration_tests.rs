//! Integration tests against a real PostgreSQL server, plus Azurite for the single-writer lock
//! and offset recovery (`tests/integration/cdc/compose.yaml`). Each test creates its own
//! database, slot and publication, so the tests run in parallel against one server.
//!
//! A test that names a pull request fails on the code before that pull request's fix.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use azure_core::http::{RequestContent, StatusCode};
use futures::{Stream, StreamExt, stream};
use pgrepl_tokio_postgres::{Client, NoTls};
use serde_json::{Value as Json, json};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use vector_lib::event::EventContainer;

use super::PostgresCdcConfig;
use crate::{
    SourceSender,
    config::{ComponentKey, ProxyConfig, SourceConfig, SourceContext},
    event::{Event, EventStatus},
    shutdown::SourceShutdownCoordinator,
    sources::cdc::{
        CONTROL_OUTPUT, OFFSETS_OUTPUT,
        lock::{LockClient, SingleWriterLockConfig},
    },
    test_util::{random_string, temp_dir, trace_init},
};

/// How long to wait for anything the source should do.
const WAIT: Duration = Duration::from_mins(1);

/// The container the lock and recovery tests use.
const CONTAINER: &str = "cdc-it";

type Events = Pin<Box<dyn Stream<Item = Event> + Send>>;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn pg_host() -> String {
    env("PG_CDC_HOST", "localhost")
}

fn pg_port() -> String {
    env("PG_CDC_PORT", "5432")
}

fn azurite_connection_string() -> String {
    let address = env("AZURITE_ADDRESS", "localhost");
    format!(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{address}:10000/devstoreaccount1;"
    )
}

async fn connect(database: &str) -> Client {
    let config = format!(
        "host={} port={} user=postgres password=postgres dbname={database}",
        pg_host(),
        pg_port()
    );
    let (client, connection) = pgrepl_tokio_postgres::connect(&config, NoTls)
        .await
        .expect("failed to connect to PostgreSQL");
    tokio::spawn(connection);
    client
}

/// A database of its own, with a data directory for the source's offsets.
struct Db {
    name: String,
    client: Client,
    data_dir: PathBuf,
}

impl Db {
    async fn new(setup: &str) -> Self {
        let name = format!("cdc_{}", random_string(12).to_lowercase());
        connect("postgres")
            .await
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("CREATE DATABASE failed");
        let client = connect(&name).await;
        client
            .batch_execute(setup)
            .await
            .unwrap_or_else(|error| panic!("setup failed: {error}"));
        let data_dir = new_dir();
        Self {
            name,
            client,
            data_dir,
        }
    }

    async fn exec(&self, sql: &str) {
        self.client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    /// The first column of the first row, as text.
    async fn value(&self, sql: &str) -> Option<String> {
        self.client
            .query_opt(&format!("SELECT ({sql})::text"), &[])
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .and_then(|row| row.get(0))
    }

    async fn server_version(&self) -> u32 {
        self.value("current_setting('server_version_num')")
            .await
            .and_then(|v| v.parse().ok())
            .expect("server_version_num")
    }

    /// The PID of the connection streaming this database's slot, if one is.
    async fn slot_pid(&self) -> Option<String> {
        self.value(&format!(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{}'",
            self.name
        ))
        .await
    }

    /// Waits for a connection to hold the slot (from the slot's creation on, so possibly still
    /// during a blocking snapshot), and returns its PID.
    async fn streaming_pid(&self) -> String {
        wait_until("the slot to stream", || async {
            self.slot_pid().await.is_some()
        })
        .await;
        self.slot_pid().await.expect("the slot stopped streaming")
    }

    async fn confirmed_flush_lsn(&self) -> String {
        self.value(&format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.name
        ))
        .await
        .expect("the slot has no confirmed_flush_lsn")
    }

    /// Waits for the slot's `confirmed_flush_lsn` to move past `lsn`.
    async fn slot_advanced_past(&self, lsn: &str) {
        let sql = format!(
            "SELECT confirmed_flush_lsn > '{lsn}'::pg_lsn FROM pg_replication_slots \
             WHERE slot_name = '{}'",
            self.name
        );
        wait_until("the slot to advance", || async {
            self.value(&sql).await.as_deref() == Some("true")
        })
        .await;
    }

    fn config(&self, extra: &str) -> PostgresCdcConfig {
        self.config_in(&self.data_dir, extra)
    }

    /// The source's config: record events with both images, and a status update every second
    /// (the test server's `wal_sender_timeout` is 5 s). `extra` is appended YAML.
    fn config_in(&self, data_dir: &Path, extra: &str) -> PostgresCdcConfig {
        let yaml = format!(
            "hostname: {host}\nport: {port}\nuser: postgres\npassword: postgres\n\
             database: {db}\nslot_name: {db}\npublication_name: {db}\ntopic_prefix: it\n\
             ssl_mode: disable\nevent_format: record\nrecord_images: before_and_after\n\
             status_update_interval_secs: 1\noffset_publish_interval_secs: 1\n\
             data_dir: {dir}\n{extra}",
            host = pg_host(),
            port = pg_port(),
            db = self.name,
            dir = data_dir.display(),
        );
        serde_yaml::from_str(&yaml).unwrap_or_else(|error| panic!("{error}:\n{yaml}"))
    }

    /// The single-writer lock on this database's lock blob, as YAML fields.
    fn lock_fields(&self) -> String {
        format!(
            "connection_string: \"{}\"\ncontainer_name: {CONTAINER}\nblob_name: {}/_lock\n\
             lease_duration_secs: 15\n",
            azurite_connection_string(),
            self.name
        )
    }

    fn lock(&self) -> LockClient {
        let config: SingleWriterLockConfig = serde_yaml::from_str(&self.lock_fields()).unwrap();
        config.build(&ProxyConfig::default()).unwrap()
    }

    /// `single_writer_lock` for the source's config, once the container exists.
    async fn lock_yaml(&self) -> String {
        if let Err(error) = self.lock().container().create(None).await {
            assert_eq!(
                error.http_status(),
                Some(StatusCode::Conflict),
                "failed to create the container: {error}"
            );
        }
        let fields = self.lock_fields().trim_end().replace('\n', "\n  ");
        format!("single_writer_lock:\n  {fields}\n")
    }
}

fn new_dir() -> PathBuf {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).expect("failed to create a temporary directory");
    dir
}

fn copy_dir(from: &Path, to: &Path) {
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
/// checkpoints too, so they are always read, into `control`.
struct Running {
    events: Events,
    control: mpsc::UnboundedReceiver<Json>,
    offsets: Option<Events>,
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

fn to_json(event: &Event) -> Json {
    serde_json::to_value(event.as_log()).expect("an event that serializes")
}

async fn start(config: &PostgresCdcConfig) -> Running {
    trace_init();
    let (mut out, events) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let mut control_events = named_output(&mut out, CONTROL_OUTPUT);
    let (control_tx, control) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = control_events.next().await {
            _ = control_tx.send(to_json(&event));
        }
    });
    let offsets = config
        .publish_offsets
        .then(|| named_output(&mut out, OFFSETS_OUTPUT));
    let (mut cx, shutdown) = SourceContext::new_shutdown(&ComponentKey::from("postgres"), out);
    cx.acknowledgements = true;
    let source = config.build(cx).await.expect("failed to build the source");
    Running {
        events: Box::pin(events),
        control,
        offsets,
        shutdown,
        task: tokio::spawn(async move { source.await.is_ok() }),
    }
}

async fn next(events: &mut Events, what: &str) -> Json {
    let event = timeout(WAIT, events.next())
        .await
        .unwrap_or_else(|_| panic!("no {what} within {WAIT:?}"))
        .unwrap_or_else(|| panic!("the {what} stream ended"));
    to_json(&event)
}

impl Running {
    async fn rows(&mut self, n: usize) -> Vec<Json> {
        let mut rows = Vec::with_capacity(n);
        for _ in 0..n {
            rows.push(next(&mut self.events, "row event").await);
        }
        rows
    }

    /// The next `control` event of type `kind`, skipping others.
    async fn control_event(&mut self, kind: &str) -> Json {
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

    async fn no_rows_for(&mut self, period: Duration) {
        if let Ok(Some(event)) = timeout(period, self.events.next()).await {
            panic!("unexpected row event: {event:?}");
        }
    }

    /// Shuts the source down cleanly; whether it ended without an error.
    async fn stop(self) -> bool {
        self.shutdown
            .shutdown_all(Some(Instant::now() + Duration::from_secs(30)))
            .await;
        timeout(WAIT, self.task)
            .await
            .expect("the source did not stop")
            .expect("the source panicked")
    }

    /// Waits for the source to end on its own; whether it ended without an error.
    async fn exit(self) -> bool {
        timeout(WAIT, self.task)
            .await
            .expect("the source kept running")
            .expect("the source panicked")
    }

    /// Ends the source as a crash would: nothing more is stored.
    fn kill(self) {
        self.task.abort();
    }
}

fn op(event: &Json) -> &str {
    event["metadata"]["op"].as_str().expect("metadata.op")
}

fn seq(event: &Json) -> &str {
    event["metadata"]["seq"].as_str().expect("metadata.seq")
}

fn after(event: &Json) -> &Json {
    &event["record"]["after"]
}

fn before(event: &Json) -> &Json {
    &event["record"]["before"]
}

fn id(event: &Json) -> i64 {
    after(event)["id"].as_i64().expect("record.after.id")
}

fn ids(events: &[Json]) -> Vec<i64> {
    let mut ids: Vec<i64> = events.iter().map(id).collect();
    ids.sort_unstable();
    ids
}

fn columns(event: &Json) -> BTreeSet<String> {
    after(event)
        .as_object()
        .expect("record.after")
        .keys()
        .cloned()
        .collect()
}

/// Writes a signal to `<folder>/pending/`, under another name first so it appears whole.
fn write_signal(folder: &Path, name: &str, signal: &Json) {
    let pending = folder.join("pending");
    std::fs::create_dir_all(&pending).unwrap();
    let tmp = pending.join(format!(".{name}.tmp"));
    std::fs::write(&tmp, signal.to_string()).unwrap();
    std::fs::rename(tmp, pending.join(format!("{name}.json"))).unwrap();
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
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

const TABLE: &str =
    "CREATE TABLE t (id int PRIMARY KEY, v text); ALTER TABLE t REPLICA IDENTITY FULL;";

/// A blocking snapshot, then changes in commit order with rising `seq`. A rolled-back
/// transaction never appears; `snapshot_completed` and `truncate` arrive on `control`.
#[tokio::test]
async fn snapshots_then_streams_changes() {
    let db = Db::new(&format!(
        "{TABLE} INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    ))
    .await;
    let mut source = start(&db.config("")).await;

    let snapshot = source.rows(3).await;
    assert!(snapshot.iter().all(|e| op(e) == "r"), "{snapshot:?}");
    assert_eq!(ids(&snapshot), [1, 2, 3]);
    let completed = source.control_event("snapshot_completed").await;
    assert_eq!(completed["table"], "t");
    assert_eq!(completed["rows"], json!(3));

    // One statement per call: in a multi-statement query, `BEGIN` would take in the statements
    // before it, and `ROLLBACK` undo them.
    for sql in [
        "INSERT INTO t VALUES (4, 'd')",
        "UPDATE t SET v = 'B' WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
        "BEGIN; INSERT INTO t VALUES (99, 'x'); ROLLBACK",
        "INSERT INTO t VALUES (5, 'e')",
    ] {
        db.exec(sql).await;
    }
    let changes = source.rows(4).await;
    let got: Vec<(&str, i64)> = changes.iter().map(|e| (op(e), id(e))).collect();
    assert_eq!(got, [("c", 4), ("u", 2), ("d", 3), ("c", 5)]);
    assert_eq!(before(&changes[1])["v"], "b");
    assert_eq!(after(&changes[1])["v"], "B");
    assert_eq!(before(&changes[2])["v"], "c");

    let seqs: Vec<&str> = snapshot.iter().chain(&changes).map(seq).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");

    db.exec("TRUNCATE t").await;
    let truncate = source.control_event("truncate").await;
    assert_eq!(truncate["table"], "t");
    source.no_rows_for(Duration::from_secs(2)).await;
    assert!(source.stop().await);
}

/// A clean restart resumes after the last delivered change and numbers on from it (a stop
/// waits for the last offset write, also without `single_writer_lock`). After a crash, changes
/// are delivered again with the `seq` they had.
#[tokio::test]
async fn resumes_after_a_restart_and_a_crash() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let config = db.config("");

    let mut source = start(&config).await;
    source.rows(1).await;
    db.exec("INSERT INTO t VALUES (2, 'b'); INSERT INTO t VALUES (3, 'c');")
        .await;
    let first = source.rows(2).await;
    assert!(source.stop().await);

    db.exec("INSERT INTO t VALUES (4, 'd');").await;
    let mut source = start(&config).await;
    let resumed = source.rows(1).await;
    assert_eq!((op(&resumed[0]), id(&resumed[0])), ("c", 4));
    assert!(seq(&resumed[0]) > seq(&first[1]));
    source.no_rows_for(Duration::from_secs(2)).await;

    db.exec("INSERT INTO t VALUES (5, 'e'); INSERT INTO t VALUES (6, 'f');")
        .await;
    let delivered: HashMap<i64, String> = source
        .rows(2)
        .await
        .iter()
        .map(|e| (id(e), seq(e).to_owned()))
        .collect();
    source.kill();

    let mut source = start(&config).await;
    db.exec("INSERT INTO t VALUES (7, 'g');").await;
    loop {
        let event = source.rows(1).await.remove(0);
        assert_eq!(op(&event), "c");
        if let Some(first_seq) = delivered.get(&id(&event)) {
            assert_eq!(seq(&event), first_seq, "a repeated change changed its seq");
        }
        if id(&event) == 7 {
            break;
        }
    }
    assert!(source.stop().await);
}

/// #7: a primary-key change is a delete of the old key and a create of the new one, so a merge
/// on the key does not keep the old row.
#[tokio::test]
async fn primary_key_change_is_a_delete_and_a_create() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let mut source = start(&db.config("")).await;
    source.rows(1).await;

    db.exec("UPDATE t SET id = 10 WHERE id = 1").await;
    let changes = source.rows(2).await;
    assert_eq!((op(&changes[0]), id(&changes[0])), ("d", 1));
    assert_eq!((op(&changes[1]), id(&changes[1])), ("c", 10));
    assert!(source.stop().await);
}

/// #7: a delete whose key has columns outside the replica identity can't be matched
/// downstream. With the default `fail` mode the source stops rather than emit a null key.
#[tokio::test]
async fn key_outside_the_replica_identity_fails() {
    let db = Db::new(
        "CREATE TABLE t (id int PRIMARY KEY, region text NOT NULL); \
         INSERT INTO t VALUES (1, 'eu');",
    )
    .await;
    let mut source = start(&db.config("pk_override:\n  public:\n    t: \"id, region\"\n")).await;
    source.rows(1).await;

    db.exec("DELETE FROM t WHERE id = 1").await;
    let mut deletes = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_secs(5), source.events.next()).await {
        deletes.push(to_json(&event));
    }
    assert!(deletes.is_empty(), "emitted {deletes:?}");
    assert!(!source.exit().await);
}

/// An idle connection stays up past the source's silence limit (63 s here): the source asks a
/// quiet server for a keepalive (#7). This server sends something often enough that the code
/// before #7 passes too, so this checks the behaviour without guarding the fix.
#[tokio::test]
async fn idle_connection_stays_up() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let mut source = start(&db.config("")).await;
    source.rows(1).await;
    let pid = db.streaming_pid().await;

    sleep(Duration::from_secs(75)).await;
    assert_eq!(db.slot_pid().await, Some(pid), "the source reconnected");
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    assert_eq!(id(&source.rows(1).await[0]), 2);
    assert!(source.stop().await);
}

/// #7: while the pipeline applies backpressure the source keeps reporting its position, so a
/// sink stalled for longer than `wal_sender_timeout` (5 s) does not get the connection dropped,
/// and nothing is delivered twice.
#[tokio::test]
async fn stalled_sink_keeps_the_connection() {
    let db = Db::new(TABLE).await;
    let mut source = start(&db.config("batch_size: 1\n")).await;
    wait_until("the slot to stream", || async {
        db.slot_pid().await.is_some()
    })
    .await;
    let pid = db.slot_pid().await;

    db.exec("INSERT INTO t SELECT g, 'x' FROM generate_series(1, 1000) g")
        .await;
    sleep(Duration::from_secs(15)).await;
    assert_eq!(db.slot_pid().await, pid, "the connection was dropped");

    let rows = source.rows(1000).await;
    assert_eq!(ids(&rows), (1..=1000).collect::<Vec<_>>());
    source.no_rows_for(Duration::from_secs(3)).await;
    assert!(source.stop().await);
}

/// #8: with `publish_offsets`, the slot frees WAL only once a published offset is delivered.
/// Here the offsets output is not read at first, so `confirmed_flush_lsn` must not move.
#[tokio::test]
async fn slot_waits_for_published_offsets() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let mut source = start(&db.config("publish_offsets: true\n")).await;
    source.rows(1).await;
    wait_until("the slot to stream", || async {
        db.slot_pid().await.is_some()
    })
    .await;
    let confirmed = db.confirmed_flush_lsn().await;

    for i in 2..5 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 'x')")).await;
    }
    source.rows(3).await;
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        db.confirmed_flush_lsn().await,
        confirmed,
        "WAL released early"
    );

    let mut offsets = source.offsets.take().unwrap();
    let reader = tokio::spawn(async move {
        while let Some(event) = offsets.next().await {
            drop(event);
        }
    });
    db.slot_advanced_past(&confirmed).await;
    assert!(source.stop().await);
    reader.abort();
}

/// #8: an offset older than the slot's position (an old copy of the data directory)
/// means changes are lost, so the source must not stream from the slot's later position. It
/// waits for an `approve-resnapshot` signal, then snapshots every table again.
#[tokio::test]
async fn offset_behind_the_slot_waits_for_approval() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let signals = new_dir();
    let config = db.config(&format!(
        "snapshot_mode: when_needed\nsignals:\n  folder: {}\n  poll_interval_secs: 1\n",
        signals.display()
    ));

    let mut source = start(&config).await;
    source.rows(1).await;
    assert!(source.stop().await);
    let old = new_dir();
    copy_dir(&db.data_dir, &old);
    let confirmed = db.confirmed_flush_lsn().await;

    let mut source = start(&config).await;
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    source.rows(1).await;
    db.slot_advanced_past(&confirmed).await;
    assert!(source.stop().await);

    std::fs::remove_dir_all(&db.data_dir).unwrap();
    copy_dir(&old, &db.data_dir);
    let mut source = start(&config).await;
    wait_until("the source to wait for approval", || async {
        signals.join("waiting.json").exists()
    })
    .await;
    source.no_rows_for(Duration::from_secs(3)).await;

    write_signal(
        &signals,
        "approve",
        &json!({"id": "approve-1", "type": "approve-resnapshot"}),
    );
    let rows = source.rows(2).await;
    assert!(rows.iter().all(|e| op(e) == "r"), "{rows:?}");
    assert_eq!(ids(&rows), [1, 2]);
    assert!(source.stop().await);
}

/// #9: an auto-created publication leaves out tables without a replica identity, so the
/// application's updates on them keep working.
#[tokio::test]
async fn autocreated_publication_leaves_keyless_tables_out() {
    let db = Db::new(
        "CREATE TABLE keyed (id int PRIMARY KEY); CREATE TABLE keyless (v int); \
         INSERT INTO keyless VALUES (1);",
    )
    .await;
    let mut source = start(&db.config("")).await;
    wait_until("the publication", || async {
        db.value(&format!(
            "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
            db.name
        ))
        .await
        .is_some()
    })
    .await;
    let published = db
        .value(&format!(
            "SELECT string_agg(tablename, ',') FROM pg_publication_tables WHERE pubname = '{}'",
            db.name
        ))
        .await;
    assert_eq!(published.as_deref(), Some("keyed"));
    db.exec("UPDATE keyless SET v = 2").await;

    source.control_event("snapshot_completed").await;
    db.exec("INSERT INTO keyed VALUES (1)").await;
    assert_eq!(op(&source.rows(1).await[0]), "c");
    assert!(source.stop().await);
}

/// #9: creating a `FOR ALL TABLES` publication is refused while a table has no replica
/// identity.
#[tokio::test]
async fn all_tables_publication_is_refused_with_a_keyless_table() {
    let db =
        Db::new("CREATE TABLE keyed (id int PRIMARY KEY); CREATE TABLE keyless (v int);").await;
    let source = start(&db.config("publication_autocreate_mode: all_tables\n")).await;
    assert!(!source.exit().await);
    let exists = db
        .value(&format!(
            "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
            db.name
        ))
        .await;
    assert_eq!(exists, None);
    db.exec("UPDATE keyless SET v = 2").await;
}

/// #13: snapshots and the stream have the same columns: generated columns are in both only
/// when the publication publishes them (PostgreSQL 18), and in neither otherwise.
#[tokio::test]
async fn generated_columns_match_in_snapshot_and_stream() {
    let db = Db::new(
        "CREATE TABLE t (id int PRIMARY KEY, g int GENERATED ALWAYS AS (id * 2) STORED); \
         ALTER TABLE t REPLICA IDENTITY FULL; INSERT INTO t (id) VALUES (1);",
    )
    .await;
    let publishes = db.server_version().await >= 180_000;
    if publishes {
        db.exec(&format!(
            "CREATE PUBLICATION {} FOR TABLE t WITH (publish_generated_columns = stored)",
            db.name
        ))
        .await;
    }
    let mut source = start(&db.config("")).await;
    let snapshot = source.rows(1).await;
    db.exec("INSERT INTO t (id) VALUES (2)").await;
    let change = source.rows(1).await;

    assert_eq!(columns(&snapshot[0]), columns(&change[0]));
    assert_eq!(columns(&change[0]).contains("g"), publishes);
    if publishes {
        assert_eq!(after(&change[0])["g"], json!(4));
    }
    assert!(source.stop().await);
}

/// #13: a partitioned table is captured as itself, in the snapshot and the stream, and not
/// also as its partitions.
#[tokio::test]
async fn partitioned_table_is_captured_as_itself() {
    let db = Db::new(
        "CREATE TABLE p (id int, region text, PRIMARY KEY (id, region)) PARTITION BY LIST (region); \
         CREATE TABLE p_eu PARTITION OF p FOR VALUES IN ('eu'); \
         CREATE TABLE p_us PARTITION OF p FOR VALUES IN ('us'); \
         INSERT INTO p VALUES (1, 'eu'), (2, 'us');",
    )
    .await;
    let mut source = start(&db.config("")).await;
    let snapshot = source.rows(2).await;
    assert_eq!(ids(&snapshot), [1, 2]);
    source.no_rows_for(Duration::from_secs(3)).await;

    db.exec("INSERT INTO p VALUES (3, 'us')").await;
    let change = source.rows(1).await;
    assert_eq!(id(&change[0]), 3);
    for event in snapshot.iter().chain(&change) {
        assert_eq!(event["metadata"]["table"], "p", "{event}");
    }
    assert!(source.stop().await);
}

/// #13: a source that finds a slot it has no offset for (another source's) stops, instead of
/// dropping the slot and taking it over.
#[tokio::test]
async fn second_source_cannot_take_over_a_slot() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let mut first = start(&db.config("")).await;
    first.rows(1).await;
    assert!(first.stop().await);

    let second = start(&db.config_in(&new_dir(), "")).await;
    assert!(!second.exit().await);

    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    let mut first = start(&db.config("")).await;
    let change = first.rows(1).await;
    assert_eq!((op(&change[0]), id(&change[0])), ("c", 2));
    assert!(first.stop().await);
}

/// The incremental snapshot's watermarks: with writes running throughout, applying the events
/// by key in `seq` order reproduces the table exactly.
#[tokio::test]
async fn incremental_snapshot_during_writes_reproduces_the_table() {
    let db = Db::new(
        "CREATE TABLE t (id int PRIMARY KEY, v int); ALTER TABLE t REPLICA IDENTITY FULL; \
         INSERT INTO t SELECT g, 0 FROM generate_series(1, 5000) g;",
    )
    .await;
    let mut source =
        start(&db.config("snapshot_strategy: incremental\nincremental_snapshot_chunk_size: 250\n"))
            .await;

    let writer = {
        let name = db.name.clone();
        tokio::spawn(async move {
            let client = connect(&name).await;
            for i in 0..300_i32 {
                let target = (i * 37) % 5000 + 1;
                let sql = match i % 3 {
                    0 => format!("UPDATE t SET v = v + 1 WHERE id = {target}"),
                    1 => format!("DELETE FROM t WHERE id = {target}"),
                    _ => format!("INSERT INTO t VALUES ({}, 1)", 5000 + i),
                };
                client.batch_execute(&sql).await.unwrap();
                sleep(Duration::from_millis(10)).await;
            }
        })
    };

    // Per key: the latest `seq` and the row it left (`None`: deleted).
    let mut lake: HashMap<i64, (String, Option<i64>)> = HashMap::new();
    let mut apply = |event: &Json| {
        let row = (op(event) != "d").then(|| after(event)["v"].as_i64().unwrap());
        let entry = lake
            .entry(id(event))
            .or_insert_with(|| (String::new(), None));
        if seq(event) > entry.0.as_str() {
            *entry = (seq(event).to_owned(), row);
        }
    };
    let mut completed = false;
    let mut writer = Some(writer);
    loop {
        tokio::select! {
            event = next(&mut source.events, "row event") => apply(&event),
            Some(event) = source.control.recv() => {
                completed |= event["type"] == "snapshot_completed";
            }
            () = sleep(Duration::from_secs(3)), if completed && writer.is_none() => break,
            result = async { writer.as_mut().unwrap().await }, if writer.is_some() => {
                result.unwrap();
                writer = None;
            }
        }
    }

    let expected: BTreeMap<i64, i64> = db
        .client
        .query("SELECT id::bigint, v::bigint FROM t", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let replayed: BTreeMap<i64, i64> = lake
        .into_iter()
        .filter_map(|(id, (_, row))| row.map(|v| (id, v)))
        .collect();
    assert_eq!(replayed, expected);
    assert!(source.stop().await);
}

/// An `execute-snapshot` signal snapshots a table while the source runs, and the signal is
/// moved to `processed/` with its result.
#[tokio::test]
async fn signal_snapshots_a_table_while_running() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a'), (2, 'b');")).await;
    let signals = new_dir();
    let mut source = start(&db.config(&format!(
        "signals:\n  folder: {}\n  poll_interval_secs: 1\n",
        signals.display()
    )))
    .await;
    source.rows(2).await;

    write_signal(
        &signals,
        "snapshot-t",
        &json!({"id": "s1", "type": "execute-snapshot",
                "data": {"data-collections": ["public.t"], "type": "incremental"}}),
    );
    let rows = source.rows(2).await;
    assert!(rows.iter().all(|e| op(e) == "r"), "{rows:?}");
    assert!(
        rows.iter()
            .all(|e| e["metadata"]["snapshot"] == "incremental")
    );
    assert_eq!(ids(&rows), [1, 2]);

    let processed = signals.join("processed");
    let result = std::fs::read_dir(&processed)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .map(|text| serde_json::from_str::<Json>(&text).unwrap())
        .find(|signal| signal["id"] == "s1")
        .expect("the signal is not in processed/");
    assert_eq!(result["result"]["status"], "applied");
    assert!(source.stop().await);
}

/// Before images and large values: with `REPLICA IDENTITY FULL` an update has the whole old row
/// and every value; without it, no before image, and an unchanged large (`TOASTed`) value is
/// the placeholder.
#[tokio::test]
async fn replica_identity_decides_the_images() {
    let big = "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300))";
    let db = Db::new(&format!(
        "CREATE TABLE full_t (id int PRIMARY KEY, big text, n int); \
         ALTER TABLE full_t REPLICA IDENTITY FULL; \
         CREATE TABLE default_t (id int PRIMARY KEY, big text, n int); \
         INSERT INTO full_t VALUES (1, {big}, 0); INSERT INTO default_t VALUES (1, {big}, 0);"
    ))
    .await;
    let mut source = start(&db.config("")).await;
    source.rows(2).await;

    db.exec("UPDATE full_t SET n = 1; UPDATE default_t SET n = 1;")
        .await;
    let changes = source.rows(2).await;
    let (full, default) = (&changes[0], &changes[1]);
    assert_eq!(full["metadata"]["table"], "full_t");
    assert_eq!(before(full)["n"], json!(0));
    assert_eq!(after(full)["big"].as_str().map(str::len), Some(9600));
    assert_eq!(default["metadata"]["table"], "default_t");
    assert!(before(default).is_null(), "{default}");
    assert_eq!(after(default)["big"], "__debezium_unavailable_value");
    assert_eq!(after(default)["n"], json!(1));
    assert!(source.stop().await);
}

/// Uploads `offset` as the published latest offset, as the `azure_blob` sink of the example
/// configs would.
async fn publish_latest(db: &Db, offset: &Json) {
    let container = db.lock().container();
    let mut offset = offset.clone();
    let object = offset.as_object_mut().unwrap();
    object.remove("source_type");
    object.remove("timestamp");
    container
        .blob_client(&format!("{}/_offsets/latest.json", db.name))
        .upload(
            RequestContent::from(format!("{offset}\n").into_bytes()),
            None,
        )
        .await
        .expect("failed to upload latest.json");
}

/// A source that lost its data directory resumes from the published offset: the same
/// position, `seq` numbering and table-snapshot state, so nothing is snapshotted again.
#[tokio::test]
async fn recovers_from_the_published_offset() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let config = db.config(&format!(
        "publish_offsets: true\n{}offset_recovery:\n  latest_blob: {db}/_offsets/latest.json\n  \
         folder: {db}/\n",
        db.lock_yaml().await,
        db = db.name
    ));

    let mut source = start(&config).await;
    source.rows(1).await;
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    let change = source.rows(1).await;
    let mut offsets = source.offsets.take().unwrap();
    let mut latest = Json::Null;
    while latest.get("seq").and_then(Json::as_str) < Some(seq(&change[0])) {
        latest = next(&mut offsets, "published offset").await;
    }
    assert!(source.stop().await);
    while let Ok(Some(event)) = timeout(Duration::from_secs(2), offsets.next()).await {
        latest = to_json(&event);
    }
    publish_latest(&db, &latest).await;

    std::fs::remove_dir_all(&db.data_dir).unwrap();
    std::fs::create_dir_all(&db.data_dir).unwrap();
    db.exec("INSERT INTO t VALUES (3, 'c')").await;
    let mut source = start(&config).await;
    let resumed = source.rows(1).await;
    assert_eq!((op(&resumed[0]), id(&resumed[0])), ("c", 3));
    assert!(seq(&resumed[0]) > seq(&change[0]));
    source.no_rows_for(Duration::from_secs(3)).await;
    assert!(source.stop().await);
}

/// A second instance waits for the single-writer lock without streaming, and takes over from
/// the stored offset once the first one stops.
#[tokio::test]
async fn second_instance_waits_for_the_lock() {
    let db = Db::new(&format!("{TABLE} INSERT INTO t VALUES (1, 'a');")).await;
    let config = db.config(&db.lock_yaml().await);

    let mut first = start(&config).await;
    first.rows(1).await;
    let pid = db.streaming_pid().await;
    let mut second = start(&config).await;
    second.no_rows_for(Duration::from_secs(5)).await;
    assert_eq!(db.slot_pid().await, Some(pid));

    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    first.rows(1).await;
    assert!(first.stop().await);
    db.exec("INSERT INTO t VALUES (3, 'c')").await;
    let change = second.rows(1).await;
    assert_eq!((op(&change[0]), id(&change[0])), ("c", 3));
    assert!(second.stop().await);
}
