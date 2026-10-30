//! `mysql_cdc` source: Debezium-compatible change data capture from the MySQL binlog.
//!
//! The source registers with MySQL as a replica, optionally takes a consistent initial snapshot,
//! and then streams row-based binlog events. Each row change is emitted as a log event whose body
//! is a Debezium change-event payload (`before`, `after`, `source`, `op`, `ts_ms`, …).
//!
//! Offsets are only persisted once the events up to that offset have been delivered by the
//! downstream sinks (when end-to-end acknowledgements are enabled), giving at-least-once
//! delivery across restarts. Offsets always point at transaction boundaries.
//!
//! Server requirements: MySQL 8.0+ with `log_bin=ON`, `binlog_format=ROW`,
//! `binlog_row_image=FULL` and `binlog_row_metadata=FULL`. GTID mode is recommended.

use std::{collections::HashMap, path::PathBuf};

use vector_lib::{
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    lookup::owned_value_path,
    schema::Definition,
    sensitive_string::SensitiveString,
};
use vrl::value::{Kind, kind::Collection};

use crate::config::{
    GenerateConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
    log_schema,
};

mod binlog;
mod convert;
mod ddl;
mod envelope;
mod incremental;
mod offset;
mod schema;
mod snapshot;
mod source;
mod table_schema;

// The shared CDC core: outputs, envelopes, emitter, checkpoints, filters and the lock.
use crate::sources::cdc::filter;
pub use crate::sources::cdc::{
    CONTROL_OUTPUT, EventFormat, EventProcessingFailureHandlingMode, OBSERVE_OUTPUT,
    OFFSETS_OUTPUT, RecordImages, TABLE_SCHEMAS_OUTPUT, lock::SingleWriterLockConfig,
    recovery::OffsetRecoveryConfig,
};

/// How the source bootstraps when it starts.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// Take a consistent snapshot of all captured tables when no stored offset exists (or the
    /// previous snapshot did not complete), then stream changes from the binlog.
    #[default]
    Initial,

    /// Take the snapshot as with `initial`, then stop without streaming the binlog.
    InitialOnly,

    /// Never snapshot table data; start streaming from the current binlog position when no
    /// stored offset exists.
    NoData,

    /// Like `initial`, but also re-snapshot when the stored offset is no longer available on
    /// the server (for example, because the binlog was purged).
    WhenNeeded,
}

/// How the initial snapshot (and a re-snapshot after a purged binlog) reads tables.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    /// One consistent read of every table before streaming starts (`snapshot_locking_mode`,
    /// `snapshot_max_threads`, `snapshot_chunk_size` apply). Long transactions hold metadata
    /// locks, so DDL on a table being read waits, and so do writes queued behind that DDL.
    #[default]
    Blocking,

    /// Stream from the current position right away and read tables in key order, in chunks of
    /// `incremental_snapshot_chunk_size` rows, each a short query interleaved with streaming.
    /// Locks are held for one chunk at a time. Needs GTID mode and a primary key (or
    /// `pk_override`) on every table.
    Incremental,
}

/// What the source does when it would have to snapshot tables that nobody asked for.
///
/// Planned snapshots always run: the first start (with no data in the lake), tables added to the
/// include lists (`snapshot_new_tables`) and tables an operator queues. Unplanned ones are a
/// re-snapshot of every table after the binlog was purged (`snapshot_mode: when_needed`), and
/// tables whose snapshot state was lost (an offset recovered from a published offset that
/// predates `checkpoint`).
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UnplannedSnapshots {
    /// Do not start them. After a purged binlog the source refuses to start until the
    /// re-snapshot is approved (`resnapshot_approved: true` in the offset file); tables with
    /// a lost snapshot state are streamed but not snapshotted (`held_tables` in the offset, a
    /// `snapshot_held` event on the `control` output) until queued or marked as snapshotted.
    #[default]
    Hold,

    /// Start them automatically.
    Allow,
}

/// How tables are locked while the snapshot position is established.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotLockingMode {
    /// Hold a global read lock (`FLUSH TABLES WITH READ LOCK`) only while the binlog position
    /// and table list are read, then release it before rows are read. Requires the `RELOAD`
    /// privilege.
    #[default]
    Minimal,

    /// Take no locks. The snapshot is only guaranteed to be consistent with the recorded binlog
    /// position if no writes happen while the snapshot starts.
    None,
}

/// How `DECIMAL` / `NUMERIC` values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DecimalHandlingMode {
    /// The unscaled value as big-endian two's-complement bytes (Debezium's default). The scale is
    /// not included, so consumers need it from elsewhere.
    #[default]
    Precise,

    /// The exact decimal as a string, such as `"1234.50"`. Recommended for data lakes.
    String,

    /// A JSON number. May lose precision.
    Double,
}

/// How binary values (`BINARY`, `VARBINARY`, `BLOB`, `BIT(n)`, precise decimals) are encoded.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BinaryHandlingMode {
    /// Standard base64 (what Debezium's JSON converter emits for bytes).
    #[default]
    Base64,

    /// URL-safe base64.
    Base64UrlSafe,

    /// Lowercase hexadecimal.
    Hex,
}

/// How `JSON` column values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JsonHandlingMode {
    /// A string holding the document as compact JSON text, the same from the snapshot and the
    /// binlog (Debezium's convention). Suits lakes with varying document shapes: parse it
    /// downstream, for example into a Databricks `VARIANT`.
    #[default]
    String,

    /// The parsed document, embedded in the event.
    Object,
}

/// How temporal values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TimePrecisionMode {
    /// `TIME` and `DATETIME(4-6)` use microseconds; `DATETIME(0-3)` uses milliseconds.
    #[default]
    AdaptiveTimeMicroseconds,

    /// Milliseconds for `TIME` and `DATETIME` regardless of their precision.
    Connect,
}

/// TLS mode for connections to MySQL.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SslMode {
    /// Do not use TLS.
    Disabled,

    /// Require TLS but do not verify the server certificate.
    Required,

    /// Require TLS and verify the server certificate against the CA, without checking the host name.
    VerifyCa,

    /// Require TLS, verify the server certificate and its host name.
    #[default]
    VerifyIdentity,
}

/// Configuration for the `mysql_cdc` source.
#[configurable_component(source(
    "mysql_cdc",
    "Capture row-level changes from MySQL through the binlog as Debezium-compatible events."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct MysqlCdcConfig {
    /// The MySQL server host name or IP address.
    #[serde(default = "default_hostname")]
    #[configurable(metadata(docs::examples = "mysql.internal"))]
    pub hostname: String,

    /// The MySQL server port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// The user to connect as.
    ///
    /// The user needs `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE` and
    /// `REPLICATION CLIENT` privileges.
    #[configurable(metadata(docs::examples = "vector_cdc"))]
    pub user: String,

    /// The password of the user.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "${MYSQL_PASSWORD}"))]
    pub password: Option<SensitiveString>,

    /// The replica server ID this source registers with. Must be unique among all replicas
    /// (and other CDC clients) of the server.
    #[configurable(metadata(docs::examples = 184054))]
    pub server_id: u32,

    /// Logical name of the captured server, emitted as `source.name` (Debezium's `topic.prefix`).
    #[serde(default = "default_topic_prefix")]
    #[configurable(metadata(docs::examples = "dbserver1"))]
    pub topic_prefix: String,

    /// Regular expressions matching database names to capture. Mutually exclusive with
    /// `database_exclude_list`.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "inventory"))]
    pub database_include_list: Vec<String>,

    /// Regular expressions matching database names to skip.
    #[serde(default)]
    pub database_exclude_list: Vec<String>,

    /// Regular expressions matching `<database>.<table>` to capture. Mutually exclusive with
    /// `table_exclude_list`.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "inventory\\.orders"))]
    pub table_include_list: Vec<String>,

    /// Regular expressions matching `<database>.<table>` to skip.
    #[serde(default)]
    pub table_exclude_list: Vec<String>,

    /// Regular expressions matching `<database>.<table>.<column>` to omit from change events.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "inventory\\.customers\\.ssn"))]
    pub column_exclude_list: Vec<String>,

    /// Skip the shadow tables of online schema-change tools: gh-ost (`_<table>_gho`,
    /// `_<table>_ghc`, `_<table>_del`) and pt-online-schema-change (`_<table>_new`,
    /// `_<table>_old`). Their copy traffic duplicates the real table; the final `RENAME`
    /// swap is still captured.
    #[serde(default = "crate::serde::default_true")]
    pub exclude_online_schema_change_tables: bool,

    /// Key columns per table, replacing the primary key (or giving a table without one a key):
    /// `pk_override.<database>.<table>: "col1, col2"`. Like Debezium's `message.key.columns`.
    ///
    /// The key is used for `metadata.key`, primary-key change detection, and incremental
    /// snapshot chunking (the columns should then be unique and indexed).
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "Tables of a database and their key columns."
    ))]
    pub pk_override: HashMap<String, HashMap<String, String>>,

    /// How the source bootstraps when it starts.
    #[serde(default)]
    pub snapshot_mode: SnapshotMode,

    /// How tables are locked while the snapshot position is established.
    #[serde(default)]
    pub snapshot_locking_mode: SnapshotLockingMode,

    /// How the initial snapshot reads tables: `blocking` (one consistent read before streaming)
    /// or `incremental` (chunked reads while streaming; minimal locking).
    #[serde(default)]
    pub snapshot_strategy: SnapshotStrategy,

    /// Snapshot the existing rows of tables that become captured after the initial snapshot:
    /// tables added to the include lists (found at start-up) and tables renamed into a
    /// captured database while streaming. They are read incrementally, in chunks, while
    /// streaming continues. Tables created while streaming need no snapshot. Needs GTID mode.
    ///
    /// When `false`, such tables only get changes from the moment they are captured.
    #[serde(default)]
    pub snapshot_new_tables: bool,

    /// What to do when tables would have to be snapshotted without anyone asking for it (after
    /// a purged binlog, or when their snapshot state was lost): `hold` (default) or `allow`.
    #[serde(default)]
    pub unplanned_snapshots: UnplannedSnapshots,

    /// Rows per chunk of an incremental snapshot. Each chunk is read with one short query and
    /// held in memory until the binlog has caught up with it.
    #[serde(default = "default_incremental_snapshot_chunk_size")]
    #[configurable(metadata(docs::type_unit = "rows"))]
    pub incremental_snapshot_chunk_size: usize,

    /// How `DECIMAL` values are represented.
    #[serde(default)]
    pub decimal_handling_mode: DecimalHandlingMode,

    /// How binary values are encoded.
    #[serde(default)]
    pub binary_handling_mode: BinaryHandlingMode,

    /// How temporal values are represented.
    #[serde(default)]
    pub time_precision_mode: TimePrecisionMode,

    /// The shape of emitted row-change events.
    #[serde(default)]
    pub event_format: EventFormat,

    /// How `JSON` column values are represented.
    #[serde(default)]
    pub json_handling_mode: JsonHandlingMode,

    /// When the snapshot of a table completes, emit a `snapshot_completed` event on the
    /// `control` output with the snapshot's `snapshot_id`, `started_seq` and row count.
    ///
    /// Every snapshot row carries the same `snapshot_id`, so a consumer can delete keys of the
    /// table that the snapshot did not contain and that have not changed since `started_seq`
    /// (for example rows deleted while the binlog was purged).
    #[serde(default = "crate::serde::default_true")]
    pub snapshot_end_markers: bool,

    /// Which row images `event_format: record` includes. Ignored for `debezium`, which always
    /// includes both.
    #[serde(default)]
    pub record_images: RecordImages,

    /// What to do when a row change cannot be converted.
    #[serde(default)]
    pub event_processing_failure_handling_mode: EventProcessingFailureHandlingMode,

    /// Emit a schema document for every captured table on the `table_schemas` output, once per
    /// distinct table definition (and again after each restart).
    ///
    /// Each document has a `schema_version`, which `event_format: record` also puts in every
    /// row's `metadata.schema_version`.
    #[serde(default)]
    pub include_table_schemas: bool,

    /// Emit DDL statements as `schema_change` events on the `control` output. (`truncate` and
    /// `snapshot_completed` events are always emitted there.)
    #[serde(default)]
    pub include_schema_changes: bool,

    /// Publish each committed offset on the `offsets` output, once everything up to it has been
    /// delivered: every event with `seq` up to the published `seq` is downstream, and every
    /// transaction it belongs to is complete. Route the output to a sink (for example an
    /// append-only folder and a `latest.json`) so consumers can read only committed data.
    /// Requires end-to-end acknowledgements.
    #[serde(default)]
    pub publish_offsets: bool,

    /// Also send an unacknowledged copy of every event (row changes, `control` and
    /// `table_schemas`) on the `observe` output, for viewers such as a live feed. Sinks on it
    /// take no part in acknowledgement: if they are slow, fail or drop events, offsets and the
    /// other outputs are unaffected. Give them a buffer that drops when full
    /// (`buffer.when_full: drop_newest`) so a slow viewer cannot delay the source.
    #[serde(default)]
    pub observe_output: bool,

    /// The minimum time between two published offsets.
    #[serde(default = "default_offset_publish_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub offset_publish_interval_secs: u64,

    /// Hold a lease on an Azure blob while running, so only one instance captures this server
    /// at a time. A second instance (an overlapping redeploy, a platform move) waits until the
    /// first stops or its lease expires, instead of racing on the stored and published offsets.
    #[serde(default)]
    pub single_writer_lock: Option<SingleWriterLockConfig>,

    /// When there is no stored offset (for example the data directory was lost), resume from
    /// the last published offset in the lake instead of starting over. Starts from scratch only
    /// if the connector's folder is empty; stops with an error if it holds data it cannot
    /// account for. Needs `single_writer_lock` (it reads with the lock's storage settings) and
    /// `publish_offsets`.
    #[serde(default)]
    pub offset_recovery: Option<OffsetRecoveryConfig>,

    /// Populate `source.query` with the SQL statement that caused each row change.
    ///
    /// Requires `binlog_rows_query_log_events=ON` on the server.
    #[serde(default)]
    pub include_query: bool,

    /// The maximum number of change events sent downstream in one batch.
    #[serde(default = "default_batch_size")]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub batch_size: usize,

    /// The maximum number of events sent downstream in one batch while the initial snapshot
    /// runs. Snapshots move large volumes of rows, so bigger batches reduce overhead.
    ///
    /// Defaults to `batch_size`. To also write bigger files during the snapshot, route events
    /// with `.op == "r"` to a separate sink with larger batch settings (see the example config).
    #[serde(default)]
    #[configurable(metadata(docs::type_unit = "events"))]
    #[configurable(metadata(docs::examples = 20000))]
    pub snapshot_batch_size: Option<usize>,

    /// The number of connections that read tables in parallel during the initial snapshot.
    ///
    /// All connections share one consistent snapshot, started while the global read lock is
    /// held, so the result is identical to a single-threaded snapshot. Each connection is a
    /// long-running read transaction on the server.
    #[serde(default = "default_snapshot_max_threads")]
    #[configurable(metadata(docs::examples = 8))]
    pub snapshot_max_threads: usize,

    /// Tables with a single-column integer primary key are read in chunks of about this many
    /// rows (by primary-key range), so large tables are split across snapshot threads.
    ///
    /// Set to `0` to read every table with a single query.
    #[serde(default = "default_snapshot_chunk_size")]
    #[configurable(metadata(docs::type_unit = "rows"))]
    pub snapshot_chunk_size: u64,

    /// The interval at which the server sends heartbeats on an idle replication connection.
    ///
    /// The connection is considered dead and re-established after two missed heartbeats.
    #[serde(default = "default_heartbeat_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub heartbeat_interval_secs: u64,

    /// The timeout for establishing a connection.
    #[serde(default = "default_connect_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub connect_timeout_secs: u64,

    /// TLS mode for connections to MySQL.
    #[serde(default)]
    pub ssl_mode: SslMode,

    /// Path to a PEM file with the CA certificate(s) used to verify the server.
    ///
    /// When not set, the system trust store is used.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "/etc/ssl/certs/mysql-ca.pem"))]
    pub ssl_ca_file: Option<PathBuf>,

    /// The directory used to persist binlog offsets.
    ///
    /// By default, the [global `data_dir` option][global_data_dir] is used.
    ///
    /// [global_data_dir]: https://vector.dev/docs/reference/configuration/global-options/#data_dir
    #[serde(default)]
    #[configurable(metadata(docs::examples = "/var/lib/vector"))]
    #[configurable(metadata(docs::human_name = "Data Directory"))]
    pub data_dir: Option<PathBuf>,

    #[serde(default, deserialize_with = "crate::serde::bool_or_struct")]
    pub acknowledgements: SourceAcknowledgementsConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub log_namespace: Option<bool>,
}

fn default_hostname() -> String {
    "localhost".to_owned()
}

const fn default_port() -> u16 {
    3306
}

fn default_topic_prefix() -> String {
    "mysql".to_owned()
}

const fn default_batch_size() -> usize {
    2048
}

const fn default_snapshot_max_threads() -> usize {
    1
}

const fn default_offset_publish_interval_secs() -> u64 {
    5
}

const fn default_incremental_snapshot_chunk_size() -> usize {
    4096
}

const fn default_snapshot_chunk_size() -> u64 {
    1_000_000
}

const fn default_heartbeat_interval_secs() -> u64 {
    10
}

const fn default_connect_timeout_secs() -> u64 {
    30
}

impl GenerateConfig for MysqlCdcConfig {
    fn generate_config() -> serde_json::Value {
        serde_yaml::from_str(indoc::indoc! {
            r#"
            hostname: localhost
            port: 3306
            user: vector_cdc
            password: "${MYSQL_PASSWORD}"
            server_id: 184054
            topic_prefix: dbserver1
            database_include_list:
              - inventory
            "#,
        })
        .unwrap()
    }
}

impl MysqlCdcConfig {
    fn schema_definition(log_namespace: LogNamespace) -> Definition {
        Definition::new_with_default_metadata(Kind::object(Collection::any()), [log_namespace])
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("key"),
                Kind::object(Collection::any()).or_null(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("database"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("table"),
                Kind::bytes().or_null(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("op"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                log_schema()
                    .timestamp_key()
                    .cloned()
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "mysql_cdc")]
impl SourceConfig for MysqlCdcConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        source::build(self, cx).await
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        crate::sources::cdc::outputs(
            Self::schema_definition,
            global_log_namespace.merge(self.log_namespace),
            self.include_table_schemas,
            self.publish_offsets,
            self.observe_output,
        )
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MysqlCdcConfig>();
    }

    #[test]
    fn parses_full_config() {
        let config: MysqlCdcConfig = serde_yaml::from_str(indoc::indoc! {r#"
            hostname: db
            user: cdc
            password: secret
            server_id: 42
            table_include_list: ["app\\.orders"]
            snapshot_mode: when_needed
            snapshot_batch_size: 20000
            snapshot_max_threads: 8
            snapshot_chunk_size: 250000
            event_format: record
            record_images: before_and_after
            decimal_handling_mode: string
            ssl_mode: required
            acknowledgements: true
        "#})
        .unwrap();
        assert_eq!(config.port, 3306);
        assert_eq!(config.snapshot_mode, SnapshotMode::WhenNeeded);
        assert_eq!(config.snapshot_batch_size, Some(20000));
        assert_eq!(config.snapshot_max_threads, 8);
        assert_eq!(config.snapshot_chunk_size, 250_000);
        assert_eq!(config.event_format, EventFormat::Record);
        assert_eq!(config.record_images, RecordImages::BeforeAndAfter);
        assert_eq!(config.decimal_handling_mode, DecimalHandlingMode::String);
        assert_eq!(config.ssl_mode, SslMode::Required);
        assert!(config.acknowledgements.enabled());
        assert_eq!(config.unplanned_snapshots, UnplannedSnapshots::Hold);
        assert!(config.offset_recovery.is_none());
    }

    #[test]
    fn parses_recovery_settings() {
        let config: MysqlCdcConfig = serde_yaml::from_str(indoc::indoc! {r"
            user: cdc
            server_id: 42
            unplanned_snapshots: allow
            offset_recovery:
              latest_blob: cdc/shop/_offsets/latest.json
              folder: cdc/shop/
              control_prefix: cdc/shop/_control/
        "})
        .unwrap();
        assert_eq!(config.unplanned_snapshots, UnplannedSnapshots::Allow);
        let recovery = config.offset_recovery.unwrap();
        assert_eq!(recovery.latest_blob, "cdc/shop/_offsets/latest.json");
        assert_eq!(
            recovery.control_prefix.as_deref(),
            Some("cdc/shop/_control/")
        );
        // It reads the lake through the lock's client.
        assert!(
            recovery
                .build(None)
                .is_err_and(|error| error.contains("single_writer_lock"))
        );
    }

    #[test]
    fn outputs_include_control_and_optional_ports() {
        let mut config: MysqlCdcConfig = serde_yaml::from_str("user: u\nserver_id: 1").unwrap();
        let outputs = config.outputs(LogNamespace::Legacy);
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[1].port.as_deref(), Some(CONTROL_OUTPUT));
        config.include_table_schemas = true;
        config.publish_offsets = true;
        let outputs = config.outputs(LogNamespace::Legacy);
        assert_eq!(outputs.len(), 4);
        assert_eq!(outputs[1].port.as_deref(), Some(TABLE_SCHEMAS_OUTPUT));
        assert_eq!(outputs[2].port.as_deref(), Some(CONTROL_OUTPUT));
        assert_eq!(outputs[3].port.as_deref(), Some(OFFSETS_OUTPUT));
        config.observe_output = true;
        let outputs = config.outputs(LogNamespace::Legacy);
        assert_eq!(outputs.len(), 5);
        assert_eq!(outputs[4].port.as_deref(), Some(OBSERVE_OUTPUT));
    }
}
