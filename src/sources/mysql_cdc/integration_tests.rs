//! Integration tests against a real `MySQL` server, plus Azurite for the single-writer lock and
//! offset recovery (`tests/integration/cdc/compose.yaml`), run the same way as `postgres_cdc`'s.
//! Each test creates its own database and registers with its own replica server ID, so the
//! tests run in parallel against one server.
//!
//! A test that names a pull request fails on the code before that pull request's fix.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use futures::StreamExt;
use mysql_async::{Conn, OptsBuilder, prelude::Queryable};
use rand::RngExt;
use serde_json::{Value as Json, json};
use tokio::{
    sync::{Mutex, RwLock, RwLockReadGuard},
    time::{sleep, timeout},
};

use super::{EventFormat, MysqlCdcConfig};
use crate::{sources::cdc::integration_test_util::*, test_util::random_string};

/// Taken for writing by the one test that changes a global server setting, and for reading by
/// every other test, so no test runs while the setting is changed.
static SERVER_SETTINGS: RwLock<()> = RwLock::const_new(());

fn mysql_host() -> String {
    env("MYSQL_CDC_HOST", "localhost")
}

fn mysql_port() -> u16 {
    env("MYSQL_CDC_PORT", "3306")
        .parse()
        .expect("MYSQL_CDC_PORT")
}

async fn connect(database: Option<&str>) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname(mysql_host())
        .tcp_port(mysql_port())
        .user(Some("root"))
        .pass(Some("root"))
        .db_name(database)
        .prefer_socket(false);
    Conn::new(opts).await.expect("failed to connect to MySQL")
}

/// A database of its own, with a data directory for the source's offsets.
struct Db {
    name: String,
    conn: Mutex<Conn>,
    data_dir: PathBuf,
    /// Keeps global server settings unchanged while the test runs.
    _settings: Option<RwLockReadGuard<'static, ()>>,
}

impl Db {
    async fn new(setup: &str) -> Self {
        let settings = SERVER_SETTINGS.read().await;
        Self::create(setup, Some(settings)).await
    }

    async fn create(setup: &str, settings: Option<RwLockReadGuard<'static, ()>>) -> Self {
        let name = format!("cdc_{}", random_string(12).to_lowercase());
        connect(None)
            .await
            .query_drop(format!("CREATE DATABASE {name}"))
            .await
            .expect("CREATE DATABASE failed");
        let db = Self {
            conn: Mutex::new(connect(Some(&name)).await),
            name,
            data_dir: new_dir(),
            _settings: settings,
        };
        db.exec(setup).await;
        db
    }

    async fn exec(&self, sql: &str) {
        self.conn
            .lock()
            .await
            .query_drop(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    fn config(&self, extra: &str) -> MysqlCdcConfig {
        self.config_in(&self.data_dir, extra)
    }

    /// The source's config: this database only, record events with both images, a heartbeat
    /// every second, and a replica server ID of its own. `extra` is appended YAML.
    fn config_in(&self, data_dir: &Path, extra: &str) -> MysqlCdcConfig {
        let server_id: u32 = rand::rng().random_range(10_000..2_000_000_000);
        let yaml = format!(
            "hostname: {host}\nport: {port}\nuser: root\npassword: root\nserver_id: {server_id}\n\
             topic_prefix: it\ndatabase_include_list: [\"{db}\"]\nssl_mode: disabled\n\
             event_format: record\nrecord_images: before_and_after\n\
             heartbeat_interval_secs: 1\noffset_publish_interval_secs: 1\n\
             data_dir: {dir}\n{extra}",
            host = mysql_host(),
            port = mysql_port(),
            db = self.name,
            dir = data_dir.display(),
        );
        serde_yaml::from_str(&yaml).unwrap_or_else(|error| panic!("{error}:\n{yaml}"))
    }
}

async fn start(config: &MysqlCdcConfig) -> Running {
    start_source(config, config.publish_offsets, config.include_table_schemas).await
}

fn table(event: &Json) -> &str {
    event["metadata"]["table"].as_str().expect("metadata.table")
}

const TABLE: &str = "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))";

/// The value conversions of a snapshot, in Debezium's format: the snapshot markers, an excluded
/// table and column, and one value of most `MySQL` types.
#[tokio::test]
async fn snapshot_converts_types() {
    let db = Db::new(
        "CREATE TABLE customers (
           id INT PRIMARY KEY, balance DECIMAL(12,2), ratio FLOAT, big BIGINT UNSIGNED,
           active BIT(1), born DATE, wake_up TIME(3), created DATETIME(6),
           updated TIMESTAMP(3) NULL, status ENUM('new','active','closed'), tags SET('a','b','c'),
           profile JSON, avatar VARBINARY(16), latin VARCHAR(20) CHARACTER SET latin1,
           location POINT SRID 4326, ssn CHAR(11));
         INSERT INTO customers VALUES
           (1, 1234.50, 0.1, 18446744073709551615, b'1', '1990-05-17', '07:30:00.250',
            '2024-01-01 12:00:00.123456', '2024-01-01 12:00:00.500', 'active', 'a,c',
            '{\"tier\": \"gold\"}', X'DEADBEEF', 'café', ST_SRID(POINT(1, 2), 4326), '123-45-6789'),
           (2, -0.01, NULL, 42, b'0', NULL, '-12:00:00', NULL, NULL, 'new', '', NULL, NULL,
            NULL, NULL, NULL),
           (3, 0, 1.5, 0, b'1', '2000-02-29', NULL, NULL, NULL, 'closed', 'b', '[]', X'00',
            'naïve', NULL, NULL);
         CREATE TABLE audit_log (id INT PRIMARY KEY);
         INSERT INTO audit_log VALUES (1);",
    )
    .await;
    let mut config = db.config(&format!(
        "decimal_handling_mode: string\n\
         table_exclude_list: [\"{db}\\\\.audit_log\"]\n\
         column_exclude_list: [\"{db}\\\\.customers\\\\.ssn\"]\n",
        db = db.name
    ));
    config.event_format = EventFormat::Debezium;
    let mut source = start(&config).await;

    let rows = source.rows(3).await;
    let markers: Vec<&str> = rows
        .iter()
        .map(|e| e["source"]["snapshot"].as_str().unwrap())
        .collect();
    assert_eq!(markers, ["first", "true", "last"]);
    assert!(
        rows.iter()
            .all(|e| e["op"] == "r" && e["source"]["table"] == "customers")
    );
    let by_id: HashMap<i64, &Json> = rows
        .iter()
        .map(|e| (e["after"]["id"].as_i64().unwrap(), &e["after"]))
        .collect();
    let alice = by_id[&1];
    assert!(alice.get("ssn").is_none(), "{alice}");
    assert_eq!(alice["balance"], "1234.50");
    assert_eq!(alice["ratio"], json!(0.1));
    assert_eq!(
        alice["big"],
        json!(-1),
        "BIGINT UNSIGNED wraps, as in Debezium"
    );
    assert_eq!(alice["active"], json!(true));
    assert_eq!(alice["born"], json!(7441));
    assert_eq!(alice["wake_up"], json!(27_000_250_000_i64));
    assert_eq!(alice["created"], json!(1_704_110_400_123_456_i64));
    assert_eq!(alice["updated"], "2024-01-01T12:00:00.500Z");
    assert_eq!(alice["status"], "active");
    assert_eq!(alice["tags"], "a,c");
    let profile: Json = serde_json::from_str(alice["profile"].as_str().unwrap()).unwrap();
    assert_eq!(profile["tier"], "gold");
    assert_eq!(alice["avatar"], "3q2+7w==");
    assert_eq!(alice["latin"], "café");
    assert_eq!(alice["location"]["srid"], json!(4326));
    assert_eq!(by_id[&2]["wake_up"], json!(-43_200_000_000_i64));
    assert_eq!(by_id[&3]["latin"], "naïve");
    assert!(source.stop().await);
}

/// Changes in commit order: insert, update with both images, a key change as a delete and a
/// create, delete, a rolled-back transaction left out, one GTID per transaction, the statement
/// (`include_query`), DDL on `control`, a new column after it, and an excluded table ignored.
#[tokio::test]
async fn streams_changes() {
    let db = Db::new(&format!(
        "{TABLE}; INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
         CREATE TABLE audit_log (id INT PRIMARY KEY);"
    ))
    .await;
    let mut source = start(&db.config(&format!(
        "include_query: true\ninclude_schema_changes: true\n\
         table_exclude_list: [\"{}\\\\.audit_log\"]\n",
        db.name
    )))
    .await;
    source.rows(3).await;
    source.control_event("snapshot_completed").await;

    for sql in [
        "INSERT INTO t VALUES (4, 'Dan')",
        "UPDATE t SET v = 'Alicia' WHERE id = 1",
        "UPDATE t SET id = 20 WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
        "BEGIN; INSERT INTO t VALUES (99, 'x'); ROLLBACK",
        "INSERT INTO audit_log VALUES (1)",
        "BEGIN; INSERT INTO t VALUES (6, 'f'); INSERT INTO t VALUES (7, 'g'); COMMIT",
    ] {
        db.exec(sql).await;
    }
    let changes = source.rows(7).await;
    let got: Vec<(&str, i64)> = changes.iter().map(|e| (op(e), id(e))).collect();
    assert_eq!(
        got,
        [
            ("c", 4),
            ("u", 1),
            ("d", 2),
            ("c", 20),
            ("d", 3),
            ("c", 6),
            ("c", 7)
        ]
    );
    assert_eq!(before(&changes[1])["v"], "Alice");
    assert_eq!(after(&changes[1])["v"], "Alicia");
    let insert = &changes[0]["metadata"];
    assert!(
        insert["gtid"].is_string() && insert["pos"].as_u64() > Some(0),
        "{insert}"
    );
    assert!(
        insert["query"]
            .as_str()
            .is_some_and(|q| q.contains("INSERT")),
        "{insert}"
    );
    assert_eq!(
        changes[5]["metadata"]["gtid"], changes[6]["metadata"]["gtid"],
        "one transaction, one GTID"
    );
    assert!(changes.iter().all(|e| table(e) == "t"));

    db.exec("ALTER TABLE t ADD COLUMN nickname VARCHAR(20)")
        .await;
    let ddl = source.control_event("schema_change").await;
    assert!(
        ddl["ddl"].as_str().unwrap().contains("ADD COLUMN nickname"),
        "{ddl}"
    );
    db.exec("INSERT INTO t VALUES (8, 'Gina', 'gg')").await;
    let added = source.rows(1).await;
    assert_eq!(after(&added[0])["nickname"], "gg");

    db.exec("TRUNCATE t").await;
    assert_eq!(source.control_event("truncate").await["table"], "t");
    source.no_rows_for(Duration::from_secs(2)).await;
    assert!(source.stop().await);
}

/// A clean restart resumes after the last delivered change and numbers on from it, without a
/// new snapshot. After a crash, changes are delivered again with the `seq` they had.
#[tokio::test]
async fn resumes_after_a_restart_and_a_crash() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let config = db.config("");

    let mut source = start(&config).await;
    source.rows(1).await;
    db.exec("INSERT INTO t VALUES (2, 'b'); INSERT INTO t VALUES (3, 'c')")
        .await;
    let first = source.rows(2).await;
    assert!(source.stop().await);

    db.exec("INSERT INTO t VALUES (4, 'd')").await;
    let mut source = start(&config).await;
    let resumed = source.rows(1).await;
    assert_eq!((op(&resumed[0]), id(&resumed[0])), ("c", 4));
    assert!(seq(&resumed[0]) > seq(&first[1]));
    source.no_rows_for(Duration::from_secs(2)).await;

    db.exec("INSERT INTO t VALUES (5, 'e'); INSERT INTO t VALUES (6, 'f')")
        .await;
    let delivered: HashMap<i64, String> = source
        .rows(2)
        .await
        .iter()
        .map(|e| (id(e), seq(e).to_owned()))
        .collect();
    source.kill();

    let mut source = start(&config).await;
    db.exec("INSERT INTO t VALUES (7, 'g')").await;
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

/// Table schema documents: one per table definition, the same version from the snapshot and
/// the binlog, a new version after `ALTER TABLE`, and every row tagged with its version.
#[tokio::test]
async fn table_schemas_follow_the_definition() {
    let db = Db::new(
        "CREATE TABLE c (id INT PRIMARY KEY, email VARCHAR(200), balance DECIMAL(12,2),
           created DATETIME(6), updated TIMESTAMP(3) NULL, big BIGINT UNSIGNED,
           status ENUM('new','active','closed'), avatar VARBINARY(16), ssn CHAR(11));
         INSERT INTO c (id) VALUES (1)",
    )
    .await;
    let mut source = start(&db.config(&format!(
        "include_table_schemas: true\ncolumn_exclude_list: [\"{}\\\\.c\\\\.ssn\"]\n",
        db.name
    )))
    .await;
    let snapshot = source.rows(1).await;
    let schema = source.table_schema().await;
    let version = schema["schema_version"].as_str().unwrap().to_owned();
    assert_eq!(snapshot[0]["metadata"]["schema_version"], version.as_str());
    assert_eq!(schema["primary_key"], json!(["id"]));
    let column = |name: &str| -> Json {
        schema["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .cloned()
            .unwrap_or(Json::Null)
    };
    assert_eq!(
        (
            column("balance")["precision"].clone(),
            column("balance")["scale"].clone()
        ),
        (json!(12), json!(2))
    );
    assert_eq!(column("balance")["logical_type"], "decimal");
    assert_eq!(column("created")["logical_type"], "timestamp_micros_local");
    assert_eq!(column("updated")["logical_type"], "timestamp_iso8601_utc");
    assert_eq!(column("big")["unsigned"], json!(true));
    assert_eq!(
        column("status")["values"],
        json!(["new", "active", "closed"])
    );
    assert_eq!(column("avatar")["logical_type"], "bytes");
    assert_eq!(
        (
            column("id")["nullable"].clone(),
            column("email")["nullable"].clone()
        ),
        (json!(false), json!(true))
    );
    assert!(column("ssn").is_null());

    db.exec("UPDATE c SET email = 'a@example.com' WHERE id = 1")
        .await;
    let update = source.rows(1).await;
    assert_eq!(update[0]["metadata"]["schema_version"], version.as_str());

    db.exec("ALTER TABLE c ADD COLUMN nickname VARCHAR(20)")
        .await;
    db.exec("INSERT INTO c (id, nickname) VALUES (2, 'n')")
        .await;
    let insert = source.rows(1).await;
    let new_version = insert[0]["metadata"]["schema_version"].as_str().unwrap();
    assert_ne!(new_version, version);
    loop {
        let document = source.table_schema().await;
        if document["schema_version"] == new_version {
            break;
        }
    }
    assert!(source.stop().await);
}

/// #4: a two-phase XA transaction's rows are not known to commit when they are read, so one
/// that changes a captured table stops the source instead of emitting its rows (which a later
/// `XA ROLLBACK` would make phantom rows).
#[tokio::test]
async fn prepared_xa_transaction_stops_the_source() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let mut source = start(&db.config("")).await;
    source.rows(1).await;
    source.control_event("snapshot_completed").await;

    for sql in [
        "XA START 'x1'",
        "INSERT INTO t VALUES (99, 'phantom')",
        "XA END 'x1'",
        "XA PREPARE 'x1'",
        "XA ROLLBACK 'x1'",
    ] {
        db.exec(sql).await;
    }
    let mut emitted = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_secs(5), source.events.next()).await {
        emitted.push(to_json(&event));
    }
    assert!(emitted.is_empty(), "emitted {emitted:?}");
    assert!(!source.exit().await);
}

/// A write logged as a statement has no rows in the binlog, so it stops the source rather than
/// being lost silently.
#[tokio::test]
async fn statement_format_write_stops_the_source() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let mut source = start(&db.config("")).await;
    source.rows(1).await;
    source.control_event("snapshot_completed").await;

    db.exec("SET SESSION binlog_format = 'STATEMENT'").await;
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    assert!(!source.exit().await);
}

/// An incremental snapshot reads every row of a table whose DECIMAL keys collide as doubles
/// (2^53 and up). #10 binds the chunk position as a DECIMAL for this, but `MySQL` 8.0 and 8.4
/// already compare a DECIMAL column with a string exactly, so the code before #10 passes too:
/// this checks the behaviour without guarding a fix.
#[tokio::test]
async fn incremental_snapshot_reads_every_decimal_key() {
    let db = Db::new(
        "CREATE TABLE d (k DECIMAL(20,0) PRIMARY KEY, v INT);
         INSERT INTO d WITH RECURSIVE s (n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 599)
           SELECT 9007199254740992 + n, n FROM s",
    )
    .await;
    let mut source = start(&db.config(
        "snapshot_strategy: incremental\nincremental_snapshot_chunk_size: 100\n\
         decimal_handling_mode: string\n",
    ))
    .await;
    source.control_event("snapshot_completed").await;
    let mut keys = HashSet::new();
    while let Ok(Some(event)) = timeout(Duration::from_secs(3), source.events.next()).await {
        let event = to_json(&event);
        assert_eq!(op(&event), "r");
        assert!(keys.insert(after(&event)["k"].as_str().unwrap().to_owned()));
    }
    assert_eq!(keys.len(), 600);
    assert!(source.stop().await);
}

/// #10: ENUM keys sort by index but compare as text, and a nullable `pk_override` column
/// compares as NULL, so chunking by them would end a table early. Such tables are skipped
/// instead: a table is snapshotted whole or not at all.
#[tokio::test]
async fn incremental_snapshot_never_reads_part_of_a_table() {
    let db = Db::new(
        "CREATE TABLE e (k ENUM('z', 'a', 'm') PRIMARY KEY);
         INSERT INTO e VALUES ('z'), ('a'), ('m');
         CREATE TABLE n (id INT NULL, v INT, UNIQUE KEY (id));
         INSERT INTO n VALUES (NULL, 1), (1, 2), (2, 3);
         CREATE TABLE ok (id INT PRIMARY KEY);
         INSERT INTO ok VALUES (1), (2), (3);",
    )
    .await;
    let mut source = start(&db.config(&format!(
        "snapshot_strategy: incremental\nincremental_snapshot_chunk_size: 1\n\
         pk_override:\n  {}:\n    n: id\n",
        db.name
    )))
    .await;
    let mut rows: BTreeMap<String, usize> = BTreeMap::new();
    loop {
        tokio::select! {
            Some(event) = source.events.next() => {
                *rows.entry(table(&to_json(&event)).to_owned()).or_default() += 1;
            }
            Some(event) = source.control.recv() => {
                if event["type"] == "snapshot_completed" && event["table"] == "ok" {
                    break;
                }
            }
            () = sleep(WAIT) => panic!("table ok was not snapshotted"),
        }
    }
    while let Ok(Some(event)) = timeout(Duration::from_secs(3), source.events.next()).await {
        *rows.entry(table(&to_json(&event)).to_owned()).or_default() += 1;
    }
    assert_eq!(rows.get("ok"), Some(&3));
    for name in ["e", "n"] {
        let count = rows.get(name).copied().unwrap_or(0);
        assert!(count == 0 || count == 3, "{count} of 3 rows of {name}");
    }
    assert!(source.stop().await);
}

/// The incremental snapshot's watermarks: with writes running throughout, applying the events
/// by key in `seq` order reproduces the table exactly.
#[tokio::test]
async fn incremental_snapshot_during_writes_reproduces_the_table() {
    let db = Db::new(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         INSERT INTO t WITH RECURSIVE s (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 999)
           SELECT n, 0 FROM s",
    )
    .await;
    let mut source =
        start(&db.config("snapshot_strategy: incremental\nincremental_snapshot_chunk_size: 50\n"))
            .await;

    let writer = {
        let name = db.name.clone();
        tokio::spawn(async move {
            let mut conn = connect(Some(&name)).await;
            for i in 0..300_i32 {
                let target = (i * 37) % 1000 + 1;
                let sql = match i % 3 {
                    0 => format!("UPDATE t SET v = v + 1 WHERE id = {target}"),
                    1 => format!("DELETE FROM t WHERE id = {target}"),
                    _ => format!("INSERT INTO t VALUES ({}, 1)", 1000 + i),
                };
                conn.query_drop(sql).await.unwrap();
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
        .conn
        .lock()
        .await
        .query::<(i64, i64), _>("SELECT id, v FROM t")
        .await
        .unwrap()
        .into_iter()
        .collect();
    let replayed: BTreeMap<i64, i64> = lake
        .into_iter()
        .filter_map(|(id, (_, row))| row.map(|v| (id, v)))
        .collect();
    assert_eq!(replayed, expected);
    assert!(source.stop().await);
}

/// #14: `latin1` is cp1252, so the euro sign, curly quotes and dashes come out the same from
/// the binlog as from the snapshot.
#[tokio::test]
async fn latin1_text_matches_in_snapshot_and_binlog() {
    let text = "€ “quoted” – ‰ café";
    let db = Db::new(&format!(
        "CREATE TABLE l (id INT PRIMARY KEY, s VARCHAR(40) CHARACTER SET latin1);
         INSERT INTO l VALUES (1, '{text}')"
    ))
    .await;
    let mut source = start(&db.config("")).await;
    let snapshot = source.rows(1).await;
    db.exec(&format!("INSERT INTO l VALUES (2, '{text}')"))
        .await;
    let streamed = source.rows(1).await;
    assert_eq!(after(&snapshot[0])["s"], text);
    assert_eq!(after(&streamed[0])["s"], text);
    assert!(source.stop().await);
}

/// #14: zero dates are the same from the snapshot and the binlog, as in Debezium: null in a
/// nullable column, the epoch in a NOT NULL one.
#[tokio::test]
async fn zero_dates_match_in_snapshot_and_binlog() {
    let zero_row = |id: i32| {
        format!(
            "INSERT INTO z VALUES ({id}, '0000-00-00', '0000-00-00', '0000-00-00 00:00:00', \
             '0000-00-00 00:00:00', '0000-00-00 00:00:00', '0000-00-00 00:00:00')"
        )
    };
    let db = Db::new(&format!(
        "SET SESSION sql_mode = '';
         CREATE TABLE z (id INT PRIMARY KEY, d DATE NOT NULL, dn DATE NULL,
           dt DATETIME NOT NULL, dtn DATETIME NULL,
           ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, tsn TIMESTAMP NULL);
         {}",
        zero_row(1)
    ))
    .await;
    let mut source = start(&db.config("")).await;
    let snapshot = source.rows(1).await;
    db.exec(&zero_row(2)).await;
    let streamed = source.rows(1).await;

    let values = |event: &Json| {
        let mut row = after(event).as_object().unwrap().clone();
        row.remove("id");
        row
    };
    assert_eq!(values(&snapshot[0]), values(&streamed[0]));
    let row = values(&streamed[0]);
    assert_eq!(row["d"], json!(0));
    assert_eq!(row["dt"], json!(0));
    assert!(
        row["dn"].is_null() && row["dtn"].is_null() && row["tsn"].is_null(),
        "{row:?}"
    );
    assert!(source.stop().await);
}

/// #14: `ALTER TABLE … RENAME TO` a captured database is a rename, so with
/// `snapshot_new_tables` the table is snapshotted, as after `RENAME TABLE`.
#[tokio::test]
async fn table_renamed_in_by_alter_table_is_snapshotted() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let other = format!("{}_x", db.name);
    db.exec(&format!(
        "CREATE DATABASE {other}; CREATE TABLE {other}.r (id INT PRIMARY KEY);
         INSERT INTO {other}.r VALUES (1), (2)"
    ))
    .await;
    let mut source = start(&db.config("snapshot_new_tables: true\n")).await;
    source.rows(1).await;
    source.control_event("snapshot_completed").await;

    db.exec(&format!("ALTER TABLE {other}.r RENAME TO {}.r", db.name))
        .await;
    let rows = source.rows(2).await;
    assert!(
        rows.iter().all(|e| op(e) == "r" && table(e) == "r"),
        "{rows:?}"
    );
    assert_eq!(ids(&rows), [1, 2]);
    assert!(source.stop().await);
}

/// #14: a `binlog_row_image` other than FULL is refused at start-up (rows would have missing
/// columns), instead of failing on the first change.
#[tokio::test]
async fn row_image_other_than_full_is_refused() {
    let _settings = SERVER_SETTINGS.write().await;
    let mut admin = connect(None).await;
    admin
        .query_drop("SET GLOBAL binlog_row_image = 'MINIMAL'")
        .await
        .unwrap();
    let result = async {
        let db = Db::create(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')"), None).await;
        let source = start(&db.config("")).await;
        timeout(Duration::from_secs(30), source.exit()).await
    };
    let ended = result.await;
    admin
        .query_drop("SET GLOBAL binlog_row_image = 'FULL'")
        .await
        .unwrap();
    assert_eq!(
        ended.ok(),
        Some(false),
        "the source did not refuse to start"
    );
}

/// An `execute-snapshot` signal snapshots a table while the source runs, and the signal is
/// moved to `processed/` with its result.
#[tokio::test]
async fn signal_snapshots_a_table_while_running() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a'), (2, 'b')")).await;
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
                "data": {"data-collections": [format!("{}.t", db.name)], "type": "incremental"}}),
    );
    let rows = source.rows(2).await;
    assert!(rows.iter().all(|e| op(e) == "r"), "{rows:?}");
    assert!(
        rows.iter()
            .all(|e| e["metadata"]["snapshot"] == "incremental")
    );
    assert_eq!(ids(&rows), [1, 2]);
    assert_eq!(
        processed_signal(&signals, "s1")["result"]["status"],
        "applied"
    );
    assert!(source.stop().await);
}

/// A source that lost its data directory resumes from the published offset: the same
/// position, `seq` numbering and table-snapshot state, so nothing is snapshotted again.
#[tokio::test]
async fn recovers_from_the_published_offset() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let config = db.config(&format!(
        "{}{}",
        lock_yaml(&db.name).await,
        recovery_yaml(&db.name)
    ));

    let mut source = start(&config).await;
    source.rows(1).await;
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    let change = source.rows(1).await;
    let mut offsets = source.offsets.take().unwrap();
    let latest = latest_offset(&mut offsets, seq(&change[0])).await;
    assert!(source.stop().await);
    let latest = drain_offsets(&mut offsets, latest).await;
    publish_latest(&db.name, &latest).await;

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

/// #11: a second instance waits for the single-writer lock without streaming. When the holder
/// is dropped without a clean stop (a forced shutdown, a reload), its lock is released, so the
/// second instance takes over from the stored offset; before, the holder's renewer kept the
/// lease forever.
#[tokio::test]
async fn dropped_lock_holder_hands_over() {
    let db = Db::new(&format!("{TABLE}; INSERT INTO t VALUES (1, 'a')")).await;
    let config = db.config(&lock_yaml(&db.name).await);

    let mut first = start(&config).await;
    first.rows(1).await;
    db.exec("INSERT INTO t VALUES (2, 'b')").await;
    first.rows(1).await;
    let mut second = start(&config).await;
    second.no_rows_for(Duration::from_secs(5)).await;

    first.kill();
    db.exec("INSERT INTO t VALUES (3, 'c')").await;
    loop {
        let event = second.rows(1).await.remove(0);
        assert_ne!(op(&event), "r", "the second instance snapshotted again");
        if id(&event) == 3 {
            break;
        }
    }
    assert!(second.stop().await);
}
