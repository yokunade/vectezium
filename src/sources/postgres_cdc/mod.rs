//! `postgres_cdc` source: Debezium-compatible change data capture from PostgreSQL logical
//! replication (`pgoutput`).
//!
//! The source reads a logical replication slot through a publication. On its first start it
//! creates the slot and snapshots the captured tables: `blocking`, in the slot's exported
//! snapshot before streaming (so the snapshot and the stream meet exactly at the slot's
//! position), or `incremental`, in chunks while streaming (Debezium's read-only incremental
//! snapshot, see `incremental.rs`). Then it streams committed transactions. Each row change is emitted as a Debezium change event (or a `record` event), in
//! the same lake layout as `mysql_cdc`: both are built on the shared CDC core (`sources::cdc`).
//!
//! Offsets point at transaction boundaries (the end of a commit record). They are stored once
//! everything before them was delivered, and only then (or once published, with
//! `publish_offsets`) does the source let the server discard WAL up to them, so the slot never
//! runs ahead of the lake.
//!
//! Server requirements: PostgreSQL 13+ with `wal_level=logical`; a user with the `REPLICATION`
//! attribute and `SELECT` on the captured tables. `REPLICA IDENTITY FULL` gives full before
//! images; without it updates carry no before image and deletes only the key.
//!
//! Option names follow Debezium's PostgreSQL connector where one exists.

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

mod catalog;
mod connection;
mod convert;
mod envelope;
mod incremental;
#[cfg(all(test, feature = "postgres_cdc-integration-tests"))]
mod integration_tests;
mod offset;
mod snapshot;
mod source;
mod stream;
mod table_schema;

use crate::sources::cdc::filter;
pub use crate::sources::cdc::{
    CONTROL_OUTPUT, EventFormat, EventProcessingFailureHandlingMode, OBSERVE_OUTPUT,
    OFFSETS_OUTPUT, RecordImages, TABLE_SCHEMAS_OUTPUT, lock::SingleWriterLockConfig,
    recovery::OffsetRecoveryConfig, signal::SignalsConfig,
};

/// How the source bootstraps when it starts.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// Snapshot all captured tables when no stored offset exists (or the previous snapshot did
    /// not complete), then stream changes.
    #[default]
    Initial,

    /// Take the snapshot as with `initial`, then stop without streaming.
    InitialOnly,

    /// Never snapshot table data; stream from the slot's position when no stored offset exists.
    NoData,

    /// Like `initial`, but also re-snapshot when the stored offset can no longer be streamed
    /// (the slot was invalidated by `max_slot_wal_keep_size`, or dropped).
    WhenNeeded,
}

/// How the initial snapshot (and a re-snapshot after the slot was lost) reads tables.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    /// Read every table in the slot's exported snapshot before streaming starts
    /// (`snapshot_max_threads` readers). One long read transaction per reader.
    #[default]
    Blocking,

    /// Stream from the slot's position right away and read tables in key order, in chunks of
    /// `incremental_snapshot_chunk_size` rows, each one short transaction interleaved with
    /// streaming (Debezium's read-only incremental snapshot, watermarked with
    /// `pg_current_snapshot()`). Needs a primary key (or `pk_override`) on every table.
    Incremental,
}

/// What the source does when it would have to snapshot tables that nobody asked for (after the
/// slot was lost).
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UnplannedSnapshots {
    /// Refuse to start until the re-snapshot is approved (`resnapshot_approved: true` in the
    /// offset file).
    #[default]
    Hold,

    /// Start it automatically.
    Allow,
}

/// Whether the source creates its publication.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAutocreateMode {
    /// The publication must exist; the source never creates or changes it.
    Disabled,

    /// Create the publication for the tables the include/exclude lists capture, if it does not
    /// exist. Needs ownership of those tables (or superuser). Created with
    /// `publish_via_partition_root = true`, so a partitioned table's changes are captured as
    /// the table's.
    ///
    /// Once a table is published, PostgreSQL rejects every `UPDATE` and `DELETE` on it unless it
    /// has a replica identity (a primary key, `REPLICA IDENTITY USING INDEX` or `FULL`). Tables
    /// without one are left out of the publication, with a warning, so production writes keep
    /// working.
    #[default]
    Filtered,

    /// Create the publication `FOR ALL TABLES` (with `publish_via_partition_root = true`), if it
    /// does not exist. Needs superuser.
    ///
    /// Refused while any table in the database lacks a replica identity: PostgreSQL would reject
    /// every `UPDATE` and `DELETE` on it. Tables created later are published too, so give each
    /// new table a primary key (or another replica identity) before writing to it.
    AllTables,
}

/// How `NUMERIC`, `DECIMAL` and `MONEY` values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DecimalHandlingMode {
    /// The unscaled value as big-endian two's-complement bytes (Debezium's default). Columns
    /// without a declared scale give `{"scale": …, "value": …}`. `NaN` and infinities give null.
    #[default]
    Precise,

    /// The exact decimal as a string, such as `"1234.50"` (`"NaN"`, `"Infinity"` as such).
    /// Recommended for data lakes.
    String,

    /// A JSON number. May lose precision.
    Double,
}

/// How binary values (`BYTEA`, `BIT(n)`, precise decimals) are encoded.
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

/// How `JSON` and `JSONB` values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JsonHandlingMode {
    /// The document as text, as PostgreSQL prints it (Debezium's convention).
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
    /// `TIME` uses microseconds; `TIMESTAMP(0-3)` milliseconds and `TIMESTAMP(4-6)`
    /// microseconds since the epoch.
    #[default]
    AdaptiveTimeMicroseconds,

    /// Milliseconds for `TIME` and `TIMESTAMP` regardless of their precision.
    Connect,
}

/// How `INTERVAL` values are represented.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IntervalHandlingMode {
    /// Microseconds, counting a month as 30.4375 days (Debezium's `numeric`).
    #[default]
    Numeric,

    /// An ISO 8601 duration such as `"P1Y2M3DT4H5M6.78S"`.
    String,
}

/// TLS mode for connections to PostgreSQL.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SslMode {
    /// Do not use TLS.
    Disable,

    /// Require TLS but do not verify the server certificate.
    Require,

    /// Require TLS and verify the server certificate against the CA, without checking the host
    /// name.
    VerifyCa,

    /// Require TLS, verify the server certificate and its host name.
    #[default]
    VerifyFull,
}

/// Configuration for the `postgres_cdc` source.
#[configurable_component(source(
    "postgres_cdc",
    "Capture row-level changes from PostgreSQL through logical replication as Debezium-compatible events."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "configuration options, each an independent switch"
)]
pub struct PostgresCdcConfig {
    /// The PostgreSQL server host name or IP address.
    #[serde(default = "default_hostname")]
    #[configurable(metadata(docs::examples = "postgres.internal"))]
    pub hostname: String,

    /// The PostgreSQL server port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// The user to connect as. It needs the `REPLICATION` attribute and `SELECT` on the captured
    /// tables (and ownership of them for `publication_autocreate_mode: filtered`).
    #[configurable(metadata(docs::examples = "vectezium"))]
    pub user: String,

    /// The password of the user.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "${POSTGRES_PASSWORD}"))]
    pub password: Option<SensitiveString>,

    /// The database to capture. One source captures one database (a slot belongs to one).
    #[configurable(metadata(docs::examples = "inventory"))]
    pub database: String,

    /// Logical name of the captured server, emitted as `source.name` (Debezium's `topic.prefix`).
    #[serde(default = "default_topic_prefix")]
    #[configurable(metadata(docs::examples = "dbserver1"))]
    pub topic_prefix: String,

    /// The logical replication slot. Created (with `pgoutput`) on the first start. Unique per
    /// source: two sources must never share a slot.
    ///
    /// The source only drops and re-creates an existing slot (to snapshot again) when its own
    /// stored offset names the slot, or the slot was invalidated. Finding the slot without such
    /// an offset (another source's slot, or this source's offsets lost without
    /// `offset_recovery`), it stops with an error instead.
    #[serde(default = "default_slot_name")]
    pub slot_name: String,

    /// The publication the slot streams. See `publication_autocreate_mode`.
    #[serde(default = "default_publication_name")]
    pub publication_name: String,

    /// Whether the source creates its publication.
    #[serde(default)]
    pub publication_autocreate_mode: PublicationAutocreateMode,

    /// Regular expressions matching schema names to capture. Mutually exclusive with
    /// `schema_exclude_list`.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "public"))]
    pub schema_include_list: Vec<String>,

    /// Regular expressions matching schema names to skip.
    #[serde(default)]
    pub schema_exclude_list: Vec<String>,

    /// Regular expressions matching `<schema>.<table>` to capture. Mutually exclusive with
    /// `table_exclude_list`.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "public\\.orders"))]
    pub table_include_list: Vec<String>,

    /// Regular expressions matching `<schema>.<table>` to skip.
    #[serde(default)]
    pub table_exclude_list: Vec<String>,

    /// Regular expressions matching `<schema>.<table>.<column>` to omit from change events.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "public\\.customers\\.ssn"))]
    pub column_exclude_list: Vec<String>,

    /// Key columns per table, replacing the primary key (or giving a table without one a key):
    /// `pk_override.<schema>.<table>: "col1, col2"`. Like Debezium's `message.key.columns`.
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "Tables of a schema and their key columns."
    ))]
    pub pk_override: HashMap<String, HashMap<String, String>>,

    /// How the source bootstraps when it starts.
    #[serde(default)]
    pub snapshot_mode: SnapshotMode,

    /// What to do when tables would have to be snapshotted without anyone asking for it (after
    /// the slot was lost): `hold` (default) or `allow`.
    #[serde(default)]
    pub unplanned_snapshots: UnplannedSnapshots,

    /// How the initial snapshot reads tables: `blocking` (in the slot's exported snapshot,
    /// before streaming) or `incremental` (in chunks, while streaming).
    #[serde(default)]
    pub snapshot_strategy: SnapshotStrategy,

    /// Snapshot the existing rows of captured tables that were never snapshotted: tables added
    /// to the publication or the include lists later, and tables removed from
    /// `snapshotted_tables` in the offset file (an ad-hoc snapshot). Found at start-up and read
    /// incrementally while streaming. When `false`, such tables only get changes from the moment
    /// they are captured.
    #[serde(default)]
    pub snapshot_new_tables: bool,

    /// Rows per chunk of an incremental snapshot. Each chunk is read with one short query and
    /// held in memory until the stream has caught up with it.
    #[serde(default = "default_incremental_snapshot_chunk_size")]
    #[configurable(metadata(docs::type_unit = "rows"))]
    pub incremental_snapshot_chunk_size: usize,

    /// The number of connections that read tables in parallel during the initial snapshot. All
    /// of them read the slot's exported snapshot, so the result is the same as with one.
    #[serde(default = "default_snapshot_max_threads")]
    pub snapshot_max_threads: usize,

    /// Rows fetched per round trip while snapshotting a table (a server-side cursor), which
    /// bounds the memory a snapshot reader holds.
    #[serde(default = "default_snapshot_fetch_size")]
    #[configurable(metadata(docs::type_unit = "rows"))]
    pub snapshot_fetch_size: usize,

    /// How `NUMERIC`, `DECIMAL` and `MONEY` values are represented.
    #[serde(default)]
    pub decimal_handling_mode: DecimalHandlingMode,

    /// How binary values are encoded.
    #[serde(default)]
    pub binary_handling_mode: BinaryHandlingMode,

    /// How temporal values are represented.
    #[serde(default)]
    pub time_precision_mode: TimePrecisionMode,

    /// How `INTERVAL` values are represented.
    #[serde(default)]
    pub interval_handling_mode: IntervalHandlingMode,

    /// How `JSON` and `JSONB` values are represented.
    #[serde(default)]
    pub json_handling_mode: JsonHandlingMode,

    /// Digits after the decimal point of `MONEY` values (match the server's `lc_monetary`).
    #[serde(default = "default_money_fraction_digits")]
    pub money_fraction_digits: u8,

    /// The value of a large (`TOASTed`) column an update did not change, when the old row does
    /// not have it either (`REPLICA IDENTITY` other than `FULL`). Debezium's
    /// `unavailable.value.placeholder`.
    #[serde(default = "default_unavailable_value_placeholder")]
    pub unavailable_value_placeholder: String,

    /// The shape of emitted row-change events.
    #[serde(default)]
    pub event_format: EventFormat,

    /// Which row images `event_format: record` includes. Ignored for `debezium`.
    #[serde(default)]
    pub record_images: RecordImages,

    /// When the snapshot of a table completes, emit a `snapshot_completed` event on the
    /// `control` output with the snapshot's `snapshot_id`, `started_seq` and row count.
    #[serde(default = "crate::serde::default_true")]
    pub snapshot_end_markers: bool,

    /// What to do when a row change cannot be converted.
    #[serde(default)]
    pub event_processing_failure_handling_mode: EventProcessingFailureHandlingMode,

    /// Emit a schema document for every captured table on the `table_schemas` output, once per
    /// distinct table definition (and again after each restart).
    #[serde(default)]
    pub include_table_schemas: bool,

    /// Emit a `schema_change` event on the `control` output when a captured table's columns
    /// change. PostgreSQL does not stream DDL, so the event describes the change (columns added,
    /// removed, retyped) rather than the statement. (`truncate` and `snapshot_completed` events
    /// are always emitted there.)
    #[serde(default)]
    pub include_schema_changes: bool,

    /// Publish each committed offset on the `offsets` output, once everything up to it has been
    /// delivered. Requires end-to-end acknowledgements.
    #[serde(default)]
    pub publish_offsets: bool,

    /// Also send an unacknowledged copy of every event on the `observe` output, for viewers such
    /// as a live feed (see `mysql_cdc`).
    #[serde(default)]
    pub observe_output: bool,

    /// The minimum time between two published offsets.
    #[serde(default = "default_offset_publish_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub offset_publish_interval_secs: u64,

    /// Hold a lease on an Azure blob while running, so only one instance captures this database
    /// at a time.
    #[serde(default)]
    pub single_writer_lock: Option<SingleWriterLockConfig>,

    /// When there is no stored offset, resume from the last published offset in the lake.
    /// Needs `single_writer_lock` and `publish_offsets`.
    #[serde(default)]
    pub offset_recovery: Option<OffsetRecoveryConfig>,

    /// Take operator requests from files in a folder while running (Debezium's file signal
    /// channel): ad-hoc incremental snapshots (`execute-snapshot`, `stop-snapshot`) and approving
    /// a full re-snapshot (see `mysql_cdc`). Without it, those need the offset file edited while
    /// the source is stopped.
    #[serde(default)]
    pub signals: Option<SignalsConfig>,

    /// The maximum number of change events sent downstream in one batch.
    #[serde(default = "default_batch_size")]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub batch_size: usize,

    /// The maximum number of events sent downstream in one batch while the initial snapshot
    /// runs. Defaults to `batch_size`.
    #[serde(default)]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub snapshot_batch_size: Option<usize>,

    /// How often the source reports its position to the server (and answers keepalives at the
    /// latest). Keep it well below the server's `wal_sender_timeout`.
    #[serde(default = "default_status_update_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub status_update_interval_secs: u64,

    /// The timeout for establishing a connection.
    #[serde(default = "default_connect_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub connect_timeout_secs: u64,

    /// TLS mode for connections to PostgreSQL.
    #[serde(default)]
    pub ssl_mode: SslMode,

    /// Path to a PEM file with the CA certificate(s) used to verify the server. When not set,
    /// the system trust store is used.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "/etc/ssl/certs/postgres-ca.pem"))]
    pub ssl_ca_file: Option<PathBuf>,

    /// The directory used to persist offsets. By default, the global `data_dir` option is used.
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
    5432
}

fn default_topic_prefix() -> String {
    "postgres".to_owned()
}

fn default_slot_name() -> String {
    "vectezium".to_owned()
}

fn default_publication_name() -> String {
    "vectezium".to_owned()
}

fn default_unavailable_value_placeholder() -> String {
    "__debezium_unavailable_value".to_owned()
}

const fn default_money_fraction_digits() -> u8 {
    2
}

const fn default_batch_size() -> usize {
    2048
}

const fn default_snapshot_max_threads() -> usize {
    1
}

const fn default_incremental_snapshot_chunk_size() -> usize {
    4096
}

const fn default_snapshot_fetch_size() -> usize {
    10_000
}

const fn default_offset_publish_interval_secs() -> u64 {
    5
}

const fn default_status_update_interval_secs() -> u64 {
    10
}

const fn default_connect_timeout_secs() -> u64 {
    30
}

impl GenerateConfig for PostgresCdcConfig {
    fn generate_config() -> serde_json::Value {
        serde_yaml::from_str(indoc::indoc! {
            r#"
            hostname: localhost
            port: 5432
            user: vectezium
            password: "${POSTGRES_PASSWORD}"
            database: inventory
            topic_prefix: dbserver1
            schema_include_list:
              - public
            "#,
        })
        .unwrap()
    }
}

impl PostgresCdcConfig {
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
#[typetag::serde(name = "postgres_cdc")]
impl SourceConfig for PostgresCdcConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        source::build(self, cx)
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
        crate::test_util::test_generate_config::<PostgresCdcConfig>();
    }

    #[test]
    fn parses_full_config() {
        let config: PostgresCdcConfig = serde_yaml::from_str(indoc::indoc! {r#"
            hostname: db
            user: cdc
            password: secret
            database: shop
            slot_name: lake
            table_include_list: ["public\\.orders"]
            snapshot_mode: when_needed
            snapshot_max_threads: 4
            event_format: record
            record_images: before_and_after
            decimal_handling_mode: string
            interval_handling_mode: string
            ssl_mode: require
            acknowledgements: true
        "#})
        .unwrap();
        assert_eq!(config.port, 5432);
        assert_eq!(config.slot_name, "lake");
        assert_eq!(config.publication_name, "vectezium");
        assert_eq!(
            config.publication_autocreate_mode,
            PublicationAutocreateMode::Filtered
        );
        assert_eq!(config.snapshot_mode, SnapshotMode::WhenNeeded);
        assert_eq!(config.decimal_handling_mode, DecimalHandlingMode::String);
        assert_eq!(config.interval_handling_mode, IntervalHandlingMode::String);
        assert_eq!(config.ssl_mode, SslMode::Require);
        assert_eq!(
            config.unavailable_value_placeholder,
            "__debezium_unavailable_value"
        );
        assert!(config.acknowledgements.enabled());
    }

    #[test]
    fn database_is_required() {
        assert!(serde_yaml::from_str::<PostgresCdcConfig>("user: u").is_err());
    }

    #[test]
    fn outputs_include_control_and_optional_ports() {
        let mut config: PostgresCdcConfig = serde_yaml::from_str("user: u\ndatabase: d").unwrap();
        assert_eq!(config.outputs(LogNamespace::Legacy).len(), 2);
        config.include_table_schemas = true;
        config.publish_offsets = true;
        config.observe_output = true;
        let outputs = config.outputs(LogNamespace::Legacy);
        assert_eq!(outputs.len(), 5);
        assert_eq!(outputs[1].port.as_deref(), Some(TABLE_SCHEMAS_OUTPUT));
        assert_eq!(outputs[3].port.as_deref(), Some(OFFSETS_OUTPUT));
    }
}
