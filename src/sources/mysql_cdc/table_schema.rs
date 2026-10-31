//! Table schema documents and schema versions.
//!
//! For every captured table the source can emit a self-contained schema document on the
//! `table_schemas` output, once per distinct table definition. Each row change carries the
//! matching `metadata.schema_version`, so consumers can always find the exact schema a row was
//! written with, even across `ALTER TABLE`.
//!
//! A document describes each column twice:
//!
//! * as `MySQL` sees it — `mysql_type`, `nullable`, `primary_key`, `unsigned`, `precision`,
//!   `scale`, `fsp`, `bit_length`, `values` (ENUM/SET);
//! * as the source *encodes* it in JSON — `json_type` plus a `logical_type` from a small, fixed
//!   vocabulary (see [`LogicalType`]). The logical types map one-to-one onto Parquet / Arrow
//!   types, so a columnar writer can be driven from these documents.
//!
//! The schema version is a hash over the column descriptions *and* the value-encoding settings
//! (`decimal_handling_mode`, `binary_handling_mode`, `time_precision_mode`), so changing either
//! produces a new version. Snapshot reads (from `information_schema`) and binlog events (from
//! table maps) are normalized to the same description, so both yield the same version for the
//! same table definition.

use chrono::Utc;
use serde_json::{Map, Value as Json, json};

use super::{
    BinaryHandlingMode, DecimalHandlingMode, TimePrecisionMode,
    convert::ConvertOptions,
    schema::{ColKind, ColumnDesc},
};

/// Version of the schema document layout itself; bump on incompatible layout changes.
pub const DOCUMENT_FORMAT_VERSION: u32 = 1;

/// How a column's values are represented in the emitted JSON.
///
/// | logical type             | JSON value                         | Parquet / Arrow                      |
/// |--------------------------|------------------------------------|--------------------------------------|
/// | `int32` / `int64`        | integer                            | INT32 / INT64                        |
/// | `float32` / `float64`    | number                             | FLOAT / DOUBLE                       |
/// | `decimal`                | see `representation`               | DECIMAL(precision, scale)            |
/// | `date_days`              | days since 1970-01-01              | DATE                                 |
/// | `time_millis` / `_micros`| time of day (may be negative/>24h) | TIME(MILLIS/MICROS) or INT64         |
/// | `timestamp_millis_local` | ms since epoch, no time zone       | TIMESTAMP(MILLIS, isAdjustedToUTC=false) |
/// | `timestamp_micros_local` | µs since epoch, no time zone       | TIMESTAMP(MICROS, isAdjustedToUTC=false) |
/// | `timestamp_iso8601_utc`  | `"2024-01-01T12:00:00.123Z"`       | TIMESTAMP(MICROS, isAdjustedToUTC=true) |
/// | `year`                   | integer                            | INT32                                |
/// | `boolean`                | boolean                            | BOOLEAN                              |
/// | `bits_le`                | encoded bytes, little endian       | BINARY                               |
/// | `string`                 | string                             | STRING                               |
/// | `bytes`                  | encoded bytes (see `encoding`)     | BINARY                               |
/// | `enum` / `set`           | string (SET: comma separated)      | STRING (ENUM logical type for enum)  |
/// | `json`                   | compact JSON text (or the document with `json_handling_mode: object`) | STRING (JSON logical type) |
/// | `geometry`               | `{"wkb": bytes, "srid": int}`      | STRUCT<wkb: BINARY, srid: INT32>     |
/// | `vector_float32`         | array of numbers                   | LIST<FLOAT>                          |
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalType {
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    DateDays,
    TimeMillis,
    TimeMicros,
    TimestampMillisLocal,
    TimestampMicrosLocal,
    TimestampIso8601Utc,
    Year,
    Boolean,
    BitsLe,
    String,
    Bytes,
    Enum,
    Set,
    Json,
    Geometry,
    VectorFloat32,
    Unknown,
}

impl LogicalType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Decimal => "decimal",
            Self::DateDays => "date_days",
            Self::TimeMillis => "time_millis",
            Self::TimeMicros => "time_micros",
            Self::TimestampMillisLocal => "timestamp_millis_local",
            Self::TimestampMicrosLocal => "timestamp_micros_local",
            Self::TimestampIso8601Utc => "timestamp_iso8601_utc",
            Self::Year => "year",
            Self::Boolean => "boolean",
            Self::BitsLe => "bits_le",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Enum => "enum",
            Self::Set => "set",
            Self::Json => "json",
            Self::Geometry => "geometry",
            Self::VectorFloat32 => "vector_float32",
            Self::Unknown => "unknown",
        }
    }

    /// The JSON type of encoded values.
    pub const fn json_type(self, decimal: DecimalHandlingMode) -> &'static str {
        match self {
            Self::Int32
            | Self::Int64
            | Self::DateDays
            | Self::TimeMillis
            | Self::TimeMicros
            | Self::TimestampMillisLocal
            | Self::TimestampMicrosLocal
            | Self::Year => "integer",
            Self::Float32 | Self::Float64 => "number",
            Self::Decimal => match decimal {
                DecimalHandlingMode::Double => "number",
                DecimalHandlingMode::String | DecimalHandlingMode::Precise => "string",
            },
            Self::Boolean => "boolean",
            Self::Geometry => "object",
            Self::VectorFloat32 => "array",
            Self::TimestampIso8601Utc
            | Self::BitsLe
            | Self::String
            | Self::Bytes
            | Self::Enum
            | Self::Set
            | Self::Json
            | Self::Unknown => "string",
        }
    }
}

/// The logical type the converter produces for `column` (mirrors `convert.rs`).
pub fn logical_type(column: &ColumnDesc, convert: ConvertOptions) -> LogicalType {
    match column.kind {
        ColKind::Int if column.unsigned => LogicalType::Int64,
        ColKind::TinyInt | ColKind::SmallInt | ColKind::MediumInt | ColKind::Int => {
            LogicalType::Int32
        }
        ColKind::BigInt => LogicalType::Int64,
        ColKind::Float => LogicalType::Float32,
        ColKind::Double => LogicalType::Float64,
        ColKind::Decimal => match convert.decimal {
            DecimalHandlingMode::Double => LogicalType::Float64,
            DecimalHandlingMode::String | DecimalHandlingMode::Precise => LogicalType::Decimal,
        },
        ColKind::Date => LogicalType::DateDays,
        ColKind::Time => match convert.time_precision {
            TimePrecisionMode::AdaptiveTimeMicroseconds => LogicalType::TimeMicros,
            TimePrecisionMode::Connect => LogicalType::TimeMillis,
        },
        ColKind::DateTime => {
            if convert.time_precision == TimePrecisionMode::AdaptiveTimeMicroseconds
                && column.fsp > 3
            {
                LogicalType::TimestampMicrosLocal
            } else {
                LogicalType::TimestampMillisLocal
            }
        }
        ColKind::Timestamp => LogicalType::TimestampIso8601Utc,
        ColKind::Year => LogicalType::Year,
        ColKind::Bit if column.bit_len == 1 => LogicalType::Boolean,
        ColKind::Bit => LogicalType::BitsLe,
        ColKind::String if column.is_binary() => LogicalType::Bytes,
        ColKind::String => LogicalType::String,
        ColKind::Enum => LogicalType::Enum,
        ColKind::Set => LogicalType::Set,
        ColKind::Json => LogicalType::Json,
        ColKind::Geometry => LogicalType::Geometry,
        ColKind::Vector => LogicalType::VectorFloat32,
        ColKind::Unknown => LogicalType::Unknown,
    }
}

const fn binary_encoding(mode: BinaryHandlingMode) -> &'static str {
    match mode {
        BinaryHandlingMode::Base64 => "base64",
        BinaryHandlingMode::Base64UrlSafe => "base64_url_safe",
        BinaryHandlingMode::Hex => "hex",
    }
}

fn mode_str<T: serde::Serialize>(mode: T) -> Json {
    serde_json::to_value(mode).unwrap_or(Json::Null)
}

/// Describes one column. Only facts that both `information_schema` and binlog table maps
/// provide are included, so snapshot and streaming produce identical descriptions.
fn column_json(column: &ColumnDesc, convert: ConvertOptions) -> Json {
    let logical = logical_type(column, convert);
    let mut c = Map::with_capacity(16);
    c.insert("name".into(), json!(column.name));
    c.insert("position".into(), json!(column.position));
    c.insert("mysql_type".into(), json!(column.mysql_type));
    c.insert("nullable".into(), json!(column.nullable));
    c.insert("primary_key".into(), json!(column.primary_key));
    if column.reports_unsigned() {
        c.insert("unsigned".into(), json!(column.unsigned));
    }
    if column.kind == ColKind::Decimal {
        c.insert("precision".into(), json!(column.precision));
        c.insert("scale".into(), json!(column.scale));
    }
    if matches!(
        column.kind,
        ColKind::Time | ColKind::DateTime | ColKind::Timestamp
    ) {
        c.insert("fsp".into(), json!(column.fsp));
    }
    if column.kind == ColKind::Bit {
        c.insert("bit_length".into(), json!(column.bit_len));
    }
    let values = match column.kind {
        ColKind::Enum => column.enum_values.as_deref(),
        ColKind::Set => column.set_values.as_deref(),
        _ => None,
    };
    if let Some(values) = values {
        c.insert("values".into(), json!(values));
    }

    c.insert(
        "json_type".into(),
        json!(logical.json_type(convert.decimal)),
    );
    c.insert("logical_type".into(), json!(logical.as_str()));
    match logical {
        LogicalType::Decimal => {
            let representation = match convert.decimal {
                DecimalHandlingMode::String => "string",
                DecimalHandlingMode::Precise => "unscaled_bytes",
                DecimalHandlingMode::Double => "float64",
            };
            c.insert("representation".into(), json!(representation));
            if convert.decimal == DecimalHandlingMode::Precise {
                c.insert("encoding".into(), json!(binary_encoding(convert.binary)));
            }
        }
        LogicalType::Bytes | LogicalType::BitsLe | LogicalType::Geometry => {
            c.insert("encoding".into(), json!(binary_encoding(convert.binary)));
        }
        _ => {}
    }
    Json::Object(c)
}

/// The hashed part of a schema document.
pub fn core(
    database: &str,
    table: &str,
    columns: &[ColumnDesc],
    convert: ConvertOptions,
) -> Map<String, Json> {
    let included: Vec<&ColumnDesc> = columns.iter().filter(|c| !c.excluded).collect();
    let primary_key: Vec<&str> = included
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let mut encoding = Map::with_capacity(3);
    encoding.insert("decimal_handling_mode".into(), mode_str(convert.decimal));
    encoding.insert("binary_handling_mode".into(), mode_str(convert.binary));
    encoding.insert(
        "time_precision_mode".into(),
        mode_str(convert.time_precision),
    );
    // Only recorded when not the default, so existing schema versions keep their hash.
    if convert.json != super::JsonHandlingMode::String {
        encoding.insert("json_handling_mode".into(), mode_str(convert.json));
    }

    let mut core = Map::with_capacity(6);
    core.insert("format_version".into(), json!(DOCUMENT_FORMAT_VERSION));
    core.insert("db".into(), json!(database));
    core.insert("table".into(), json!(table));
    core.insert("primary_key".into(), json!(primary_key));
    core.insert("encoding".into(), Json::Object(encoding));
    core.insert(
        "columns".into(),
        Json::Array(included.iter().map(|c| column_json(c, convert)).collect()),
    );
    core
}

/// A short, stable identifier of a schema core (16 hex digits).
pub fn version(core: &Map<String, Json>) -> String {
    let bytes = serde_json::to_vec(core).expect("schema core serializes");
    format!("{:016x}", seahash::hash(&bytes))
}

/// Where a schema version was first observed by this process.
pub struct Observed<'a> {
    /// `snapshot` or `binlog`.
    pub origin: &'static str,
    pub file: &'a str,
    pub pos: u64,
    pub gtid: Option<&'a str>,
}

/// The full schema document for the `table_schemas` output.
pub fn document(
    core: &Map<String, Json>,
    version: &str,
    server_name: &str,
    observed: &Observed<'_>,
) -> Map<String, Json> {
    let mut doc = Map::with_capacity(core.len() + 3);
    doc.insert("schema_version".into(), json!(version));
    doc.insert("server_name".into(), json!(server_name));
    for (k, v) in core {
        doc.insert(k.clone(), v.clone());
    }
    let mut seen = Map::with_capacity(5);
    seen.insert("origin".into(), json!(observed.origin));
    seen.insert("file".into(), json!(observed.file));
    seen.insert("pos".into(), json!(observed.pos));
    seen.insert(
        "gtid".into(),
        observed.gtid.map_or(Json::Null, |g| json!(g)),
    );
    seen.insert(
        "captured_ts_ms".into(),
        json!(Utc::now().timestamp_millis()),
    );
    doc.insert("first_seen".into(), Json::Object(seen));
    doc
}

/// Normalized `MySQL` type name from a binlog column type (real type) and character set.
pub const fn mysql_type_from_binlog(
    kind: ColKind,
    column_type: mysql_async::consts::ColumnType,
    binary: bool,
) -> &'static str {
    use mysql_async::consts::ColumnType::{
        MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR,
    };
    match kind {
        ColKind::TinyInt => "tinyint",
        ColKind::SmallInt => "smallint",
        ColKind::MediumInt => "mediumint",
        ColKind::Int => "int",
        ColKind::BigInt => "bigint",
        ColKind::Float => "float",
        ColKind::Double => "double",
        ColKind::Decimal => "decimal",
        ColKind::Date => "date",
        ColKind::Time => "time",
        ColKind::DateTime => "datetime",
        ColKind::Timestamp => "timestamp",
        ColKind::Year => "year",
        ColKind::Bit => "bit",
        ColKind::Enum => "enum",
        ColKind::Set => "set",
        ColKind::Json => "json",
        ColKind::Geometry => "geometry",
        ColKind::Vector => "vector",
        ColKind::String => match (column_type, binary) {
            (MYSQL_TYPE_STRING, false) => "char",
            (MYSQL_TYPE_STRING, true) => "binary",
            (MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING, false) => "varchar",
            (MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING, true) => "varbinary",
            (_, false) => "text",
            (_, true) => "blob",
        },
        ColKind::Unknown => "unknown",
    }
}

/// Normalized `MySQL` type name from `information_schema.COLUMNS.DATA_TYPE`.
pub fn mysql_type_from_information_schema(data_type: &str) -> &'static str {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" => "tinyint",
        "smallint" => "smallint",
        "mediumint" => "mediumint",
        "int" | "integer" => "int",
        "bigint" => "bigint",
        "float" => "float",
        "double" | "real" => "double",
        "decimal" | "numeric" => "decimal",
        "date" => "date",
        "time" => "time",
        "datetime" => "datetime",
        "timestamp" => "timestamp",
        "year" => "year",
        "bit" => "bit",
        "char" => "char",
        "varchar" => "varchar",
        "binary" => "binary",
        "varbinary" => "varbinary",
        "tinytext" | "text" | "mediumtext" | "longtext" => "text",
        "tinyblob" | "blob" | "mediumblob" | "longblob" => "blob",
        "enum" => "enum",
        "set" => "set",
        "json" => "json",
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => "geometry",
        "vector" => "vector",
        _ => "unknown",
    }
}

/// Parses the values of `enum('a','b')` / `set('x','y')` from `COLUMN_TYPE`.
pub fn parse_value_list(column_type: &str) -> Option<Vec<String>> {
    let (prefix, list) = column_type.split_once('(')?;
    let prefix = prefix.trim().to_ascii_lowercase();
    if prefix != "enum" && prefix != "set" {
        return None;
    }
    let mut values = Vec::new();
    let mut chars = list.chars().peekable();
    loop {
        match chars.next()? {
            '\'' => {
                let mut value = String::new();
                loop {
                    match chars.next()? {
                        '\'' if chars.peek() == Some(&'\'') => {
                            chars.next();
                            value.push('\'');
                        }
                        '\'' => break,
                        '\\' => value.push(chars.next()?),
                        c => value.push(c),
                    }
                }
                values.push(value);
            }
            ')' => return Some(values),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn col(name: &str, kind: ColKind, mysql_type: &'static str) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            position: 1,
            kind,
            mysql_type,
            nullable: true,
            unsigned: false,
            precision: None,
            scale: None,
            charset: 255,
            fsp: 0,
            bit_len: 0,
            enum_values: None,
            set_values: None,
            primary_key: false,
            excluded: false,
        }
    }

    fn opts() -> ConvertOptions {
        ConvertOptions {
            decimal: DecimalHandlingMode::String,
            binary: BinaryHandlingMode::Base64,
            time_precision: TimePrecisionMode::AdaptiveTimeMicroseconds,
            json: super::super::JsonHandlingMode::String,
        }
    }

    #[test]
    fn value_lists() {
        assert_eq!(
            parse_value_list("enum('a','b''c','d,e')"),
            Some(vec!["a".to_string(), "b'c".to_string(), "d,e".to_string()])
        );
        assert_eq!(
            parse_value_list("set('x','y')"),
            Some(vec!["x".to_string(), "y".to_string()])
        );
        assert_eq!(parse_value_list("varchar(10)"), None);
    }

    #[test]
    fn type_normalization_agrees() {
        use mysql_async::consts::ColumnType::*;
        assert_eq!(mysql_type_from_information_schema("MEDIUMTEXT"), "text");
        assert_eq!(
            mysql_type_from_binlog(ColKind::String, MYSQL_TYPE_BLOB, false),
            "text"
        );
        assert_eq!(mysql_type_from_information_schema("varbinary"), "varbinary");
        assert_eq!(
            mysql_type_from_binlog(ColKind::String, MYSQL_TYPE_VARCHAR, true),
            "varbinary"
        );
        assert_eq!(mysql_type_from_information_schema("point"), "geometry");
    }

    #[test]
    fn version_is_stable_and_sensitive() {
        let mut id = col("id", ColKind::Int, "int");
        id.primary_key = true;
        id.nullable = false;
        let mut created = col("created", ColKind::DateTime, "datetime");
        created.position = 2;
        created.fsp = 6;
        let mut status = col("status", ColKind::Enum, "enum");
        status.position = 3;
        status.enum_values = Some(Arc::from(vec!["new".into(), "done".into()]));
        let columns = vec![id, created, status];

        let core1 = core("app", "orders", &columns, opts());
        let v1 = version(&core1);
        assert_eq!(v1.len(), 16);
        assert_eq!(v1, version(&core("app", "orders", &columns, opts())));

        assert_eq!(core1["primary_key"], json!(["id"]));
        assert_eq!(
            core1["columns"][1]["logical_type"],
            json!("timestamp_micros_local")
        );
        assert_eq!(core1["columns"][2]["values"], json!(["new", "done"]));
        assert_eq!(core1["columns"][0]["json_type"], json!("integer"));

        // A different encoding setting is a different version.
        let mut other = opts();
        other.decimal = DecimalHandlingMode::Double;
        let mut with_decimal = columns.clone();
        let mut amount = col("amount", ColKind::Decimal, "decimal");
        amount.position = 4;
        amount.precision = Some(10);
        amount.scale = Some(2);
        with_decimal.push(amount);
        let v_string = version(&core("app", "orders", &with_decimal, opts()));
        let v_double = version(&core("app", "orders", &with_decimal, other));
        assert_ne!(v_string, v_double);
        assert_ne!(v1, v_string);

        // Excluded columns are not part of the schema.
        let mut excluded = columns;
        excluded[2].excluded = true;
        let core_ex = core("app", "orders", &excluded, opts());
        assert_eq!(core_ex["columns"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn document_adds_version_and_observation() {
        let columns = vec![col("id", ColKind::BigInt, "bigint")];
        let core = core("app", "t", &columns, opts());
        let v = version(&core);
        let observed = Observed {
            origin: "binlog",
            file: "mysql-bin.000001",
            pos: 4,
            gtid: None,
        };
        let doc = document(&core, &v, "srv", &observed);
        assert_eq!(doc["schema_version"], json!(v));
        assert_eq!(doc["first_seen"]["origin"], json!("binlog"));
        assert_eq!(doc["columns"][0]["logical_type"], json!("int64"));
    }
}
