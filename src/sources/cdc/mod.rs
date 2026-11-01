//! The change-data-capture core shared by Vectezium's CDC connectors (`mysql_cdc`, and later
//! PostgreSQL, SQL Server, …).
//!
//! Everything that defines *what lands downstream* lives here, so every connector produces the
//! same storage layout and the same guarantees:
//!
//! | Module | |
//! |---|---|
//! | [`emitter`] | Batching, the master `seq`, the default / `control` / `table_schemas` outputs, and handing offsets over only with the batch that ends their transaction |
//! | [`checkpoint`] | The [`CdcOffset`](checkpoint::CdcOffset) trait, crash-safe offset storage, acknowledgement-driven checkpointing, published offsets (`offsets` output) |
//! | [`envelope`] | Debezium and record envelopes; a connector supplies its `source` block ([`SourceBlock`](envelope::SourceBlock)) |
//! | [`control`] | Shared control events (`snapshot_completed`, `snapshot_held`) |
//! | [`filter`] | Include/exclude lists, key-column overrides |
//! | [`lock`] | The single-writer lock (an Azure blob lease) and [`run_locked`](lock::run_locked) |
//! | [`recovery`] | Resuming from the published offset when the stored one is lost (`offset_recovery`) |
//! | [`seq`] | The master sequence format |
//! | [`signal`] | Operator requests (ad-hoc snapshots, held snapshots, approvals) taken from a folder while running |
//!
//! # Adding a connector
//!
//! A connector (`src/sources/<db>_cdc/`) provides:
//!
//! 1. **An offset type** implementing [`CdcOffset`](checkpoint::CdcOffset): its log position
//!    (binlog file/position and GTID set, an LSN, …) at a transaction boundary, plus the `seq`
//!    counters. It is what `offsets.json` holds and what published offsets show.
//! 2. **A `source` block** implementing [`SourceBlock`](envelope::SourceBlock) for its events.
//! 3. **Reading**: connect, decide where to start (stored offset, or snapshot), snapshot tables
//!    (`op: r` via [`Emitter::push_snapshot_row`](emitter::Emitter::push_snapshot_row), then
//!    [`control::snapshot_completed`]), stream changes (`push_row_change` / `push_prebuilt`), emit
//!    `truncate` and `schema_change` control events (`push_control`), and call
//!    [`Emitter::commit`](emitter::Emitter::commit) with its offset at every transaction
//!    boundary and [`Emitter::discard_uncommitted`](emitter::Emitter::discard_uncommitted) when
//!    a transaction is cut off.
//! 4. **Value conversion and schema documents** for its types, mapped onto the shared logical
//!    types so the output looks the same whatever the database.
//!
//! The connector's `build` wires it together: [`checkpoint::check_acknowledgements`],
//! [`lock::run_locked`], [`checkpoint::Checkpoints::start`] with a
//! [`checkpoint::CheckpointStore`], and an [`emitter::Emitter`] seeded with the stored `seq`.

pub mod checkpoint;
pub mod control;
pub mod emitter;
pub mod envelope;
pub mod filter;
#[cfg(all(
    test,
    any(
        feature = "mysql_cdc-integration-tests",
        feature = "postgres_cdc-integration-tests"
    )
))]
pub(crate) mod integration_test_util;
pub mod lock;
pub mod recovery;
pub mod seq;
pub mod signal;

use vector_lib::{config::LogNamespace, configurable::configurable_component, schema::Definition};

use crate::config::{DataType, SourceOutput};

/// The named output that carries connector events: `schema_change`, `truncate` and
/// `snapshot_completed`. Table streams on the default output carry only row changes.
pub const CONTROL_OUTPUT: &str = "control";

/// The named output that carries published offsets (`publish_offsets`).
pub const OFFSETS_OUTPUT: &str = "offsets";

/// The named output that carries table schema documents.
pub const TABLE_SCHEMAS_OUTPUT: &str = "table_schemas";

/// The named output that carries an unacknowledged copy of every event of the default,
/// `table_schemas` and `control` outputs (`observe_output`), for viewers such as a live feed.
pub const OBSERVE_OUTPUT: &str = "observe";

/// The outputs of a CDC source: row changes (default), `table_schemas` (optional), `control`,
/// `offsets` (optional) and `observe` (optional).
pub fn outputs(
    definition: impl Fn(LogNamespace) -> Definition,
    log_namespace: LogNamespace,
    include_table_schemas: bool,
    publish_offsets: bool,
    observe_output: bool,
) -> Vec<SourceOutput> {
    let output = || SourceOutput::new_maybe_logs(DataType::Log, definition(log_namespace));
    let mut outputs = vec![output()];
    if include_table_schemas {
        outputs.push(output().with_port(TABLE_SCHEMAS_OUTPUT));
    }
    outputs.push(output().with_port(CONTROL_OUTPUT));
    if publish_offsets {
        outputs.push(output().with_port(OFFSETS_OUTPUT));
    }
    if observe_output {
        outputs.push(output().with_port(OBSERVE_OUTPUT));
    }
    outputs
}

/// The shape of emitted row-change events.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventFormat {
    /// Debezium's change-event payload: `before`, `after`, `source`, `op`, `ts_ms`, ….
    #[default]
    Debezium,

    /// A `metadata` object describing the change and a `record` object holding the row, for
    /// example `{"metadata": {"op": "u", "db": "app", "table": "orders", …}, "record": {"id": 1, …}}`.
    /// Suited to data lakes, where each change becomes one row of a table.
    Record,
}

/// Which row images `event_format: record` includes.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RecordImages {
    /// `record` holds the row after the change. For deletes it holds the deleted row, and
    /// `metadata.deleted` is `true`.
    #[default]
    After,

    /// `record` holds both row images: `{"before": …, "after": …}`. `before` is the row before
    /// the change (`null` for inserts and snapshot reads); `after` is the row after it, or for a
    /// delete the key of the deleted row (its primary key or `pk_override` columns; `null` for
    /// a table without a key).
    BeforeAndAfter,
}

/// What to do when a row change cannot be converted.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventProcessingFailureHandlingMode {
    /// Stop the source. Restarting resumes from the last delivered offset, so the failing change
    /// is retried rather than lost.
    #[default]
    Fail,

    /// Log an error and skip the change.
    Warn,

    /// Skip the change, logging only at debug level.
    Skip,
}
