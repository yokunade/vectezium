//! Table schema documents and schema versions, in the same layout as `mysql_cdc`'s: each column
//! as PostgreSQL sees it (`postgres_type`, `nullable`, `primary_key`, `precision`, `scale`, …)
//! and as the source encodes it (`json_type` and a `logical_type` from the shared vocabulary).
//!
//! The schema version hashes the column descriptions and the value-encoding settings. Snapshot
//! (catalog) and stream (`Relation` messages plus the catalog) describe a table the same way, so
//! both give the same version for the same definition.

use chrono::Utc;
use serde_json::{Map, Value as Json, json};

use super::{
    BinaryHandlingMode, DecimalHandlingMode, IntervalHandlingMode, JsonHandlingMode,
    TimePrecisionMode,
    catalog::{Column, Kind},
    convert::ConvertOptions,
    offset::Lsn,
};

/// Version of the schema document layout itself.
pub const DOCUMENT_FORMAT_VERSION: u32 = 1;

/// The logical type the converter produces (mirrors `convert.rs`); the names are shared with
/// `mysql_cdc` where the representation is the same.
fn logical_type(kind: &Kind, column: &Column, o: &ConvertOptions) -> &'static str {
    match kind {
        Kind::Bool => "boolean",
        Kind::Int16 | Kind::Int32 => "int32",
        Kind::Int64 | Kind::Oid => "int64",
        Kind::Float32 => "float32",
        Kind::Float64 => "float64",
        Kind::Numeric | Kind::Money => match o.decimal {
            DecimalHandlingMode::Double => "float64",
            _ if column.numeric_precision_scale().is_none() && kind == &Kind::Numeric
                && o.decimal == DecimalHandlingMode::Precise => "variable_scale_decimal",
            _ => "decimal",
        },
        Kind::Bytea => "bytes",
        Kind::Date => "date_days",
        Kind::Time => match o.time_precision {
            TimePrecisionMode::AdaptiveTimeMicroseconds => "time_micros",
            TimePrecisionMode::Connect => "time_millis",
        },
        Kind::TimeTz => "time_iso8601_utc",
        Kind::Timestamp => {
            if o.time_precision == TimePrecisionMode::AdaptiveTimeMicroseconds
                && column.time_precision() > 3
            {
                "timestamp_micros_local"
            } else {
                "timestamp_millis_local"
            }
        }
        Kind::TimestampTz => "timestamp_iso8601_utc",
        Kind::Interval => match o.interval {
            IntervalHandlingMode::Numeric => "duration_micros",
            IntervalHandlingMode::String => "duration_iso8601",
        },
        Kind::Uuid => "uuid",
        Kind::Json | Kind::Jsonb => "json",
        Kind::Bit if column.bit_length() == Some(1) => "boolean",
        Kind::Bit | Kind::VarBit => "bits_le",
        Kind::Point => "point",
        Kind::Enum => "enum",
        Kind::Array(_) => "array",
        Kind::Text => "string",
    }
}

fn json_type(logical: &str, o: &ConvertOptions) -> &'static str {
    match logical {
        "int32" | "int64" | "date_days" | "time_micros" | "time_millis"
        | "timestamp_micros_local" | "timestamp_millis_local" | "duration_micros" => "integer",
        "float32" | "float64" => "number",
        "boolean" => "boolean",
        "array" => "array",
        "point" | "variable_scale_decimal" => "object",
        "json" if o.json == JsonHandlingMode::Object => "any",
        _ => "string",
    }
}

fn binary_encoding(mode: BinaryHandlingMode) -> &'static str {
    match mode {
        BinaryHandlingMode::Base64 => "base64",
        BinaryHandlingMode::Base64UrlSafe => "base64_url_safe",
        BinaryHandlingMode::Hex => "hex",
    }
}

fn mode_str<T: serde::Serialize>(mode: T) -> Json {
    serde_json::to_value(mode).unwrap_or(Json::Null)
}

fn column_json(column: &Column, o: &ConvertOptions) -> Json {
    let logical = logical_type(&column.ty.kind, column, o);
    let mut c = Map::with_capacity(12);
    c.insert("name".into(), json!(column.name));
    c.insert("position".into(), json!(column.position));
    c.insert("postgres_type".into(), json!(column.ty.name));
    c.insert("nullable".into(), json!(column.nullable));
    c.insert("primary_key".into(), json!(column.primary_key.is_some()));
    if let Some((precision, scale)) = column.numeric_precision_scale() {
        c.insert("precision".into(), json!(precision));
        c.insert("scale".into(), json!(scale));
    }
    if column.ty.kind == Kind::Money {
        c.insert("scale".into(), json!(o.money_fraction_digits));
    }
    if matches!(column.ty.kind, Kind::Time | Kind::TimeTz | Kind::Timestamp | Kind::TimestampTz) {
        c.insert("fsp".into(), json!(column.time_precision()));
    }
    if let Some(bits) = column.bit_length() {
        c.insert("bit_length".into(), json!(bits));
    }
    c.insert("json_type".into(), json!(json_type(logical, o)));
    c.insert("logical_type".into(), json!(logical));
    if let Kind::Array(element) = &column.ty.kind {
        let element = logical_type(element, column, o);
        c.insert(
            "items".into(),
            json!({"logical_type": element, "json_type": json_type(element, o)}),
        );
    }
    match logical {
        "decimal" => {
            let representation = match o.decimal {
                DecimalHandlingMode::String => "string",
                DecimalHandlingMode::Precise => "unscaled_bytes",
                DecimalHandlingMode::Double => "float64",
            };
            c.insert("representation".into(), json!(representation));
            if o.decimal == DecimalHandlingMode::Precise {
                c.insert("encoding".into(), json!(binary_encoding(o.binary)));
            }
        }
        "variable_scale_decimal" | "bytes" | "bits_le" => {
            c.insert("encoding".into(), json!(binary_encoding(o.binary)));
        }
        _ => {}
    }
    Json::Object(c)
}

/// The hashed part of a schema document.
pub fn core(
    schema: &str,
    table: &str,
    columns: &[Column],
    key: &[usize],
    o: &ConvertOptions,
) -> Map<String, Json> {
    let included: Vec<&Column> = columns.iter().filter(|c| !c.excluded).collect();
    let primary_key: Vec<&str> = key
        .iter()
        .filter_map(|&i| columns.get(i))
        .filter(|c| !c.excluded)
        .map(|c| c.name.as_str())
        .collect();

    let mut encoding = Map::with_capacity(5);
    encoding.insert("decimal_handling_mode".into(), mode_str(o.decimal));
    encoding.insert("binary_handling_mode".into(), mode_str(o.binary));
    encoding.insert("time_precision_mode".into(), mode_str(o.time_precision));
    encoding.insert("interval_handling_mode".into(), mode_str(o.interval));
    encoding.insert("json_handling_mode".into(), mode_str(o.json));

    let mut core = Map::with_capacity(6);
    core.insert("format_version".into(), json!(DOCUMENT_FORMAT_VERSION));
    core.insert("db".into(), json!(schema));
    core.insert("table".into(), json!(table));
    core.insert("primary_key".into(), json!(primary_key));
    core.insert("encoding".into(), Json::Object(encoding));
    core.insert(
        "columns".into(),
        Json::Array(included.iter().map(|c| column_json(c, o)).collect()),
    );
    core
}

/// A short, stable identifier of a schema core (16 hex digits).
pub fn version(core: &Map<String, Json>) -> String {
    let bytes = serde_json::to_vec(core).expect("schema core serializes");
    format!("{:016x}", seahash::hash(&bytes))
}

/// The full schema document for the `table_schemas` output.
pub fn document(
    core: &Map<String, Json>,
    version: &str,
    server_name: &str,
    database: &str,
    origin: &'static str,
    lsn: Option<Lsn>,
) -> Map<String, Json> {
    let mut doc = Map::with_capacity(core.len() + 4);
    doc.insert("schema_version".into(), json!(version));
    doc.insert("server_name".into(), json!(server_name));
    doc.insert("database".into(), json!(database));
    for (k, v) in core {
        doc.insert(k.clone(), v.clone());
    }
    doc.insert(
        "first_seen".into(),
        json!({
            "origin": origin,
            "lsn": lsn.map(|l| l.to_string()),
            "captured_ts_ms": Utc::now().timestamp_millis(),
        }),
    );
    doc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::postgres_cdc::catalog::TypeInfo;

    fn options() -> ConvertOptions {
        ConvertOptions {
            decimal: DecimalHandlingMode::String,
            binary: BinaryHandlingMode::Base64,
            time_precision: TimePrecisionMode::AdaptiveTimeMicroseconds,
            interval: IntervalHandlingMode::Numeric,
            json: JsonHandlingMode::String,
            money_fraction_digits: 2,
        }
    }

    fn column(name: &str, kind: Kind, typmod: i32, pk: Option<usize>) -> Column {
        Column {
            name: name.into(),
            position: 1,
            type_oid: 0,
            typmod,
            ty: TypeInfo { name: "x".into(), kind },
            nullable: pk.is_none(),
            primary_key: pk,
            excluded: false,
        }
    }

    #[test]
    fn versions_follow_definitions_and_encodings() {
        let columns = vec![
            column("id", Kind::Int32, -1, Some(1)),
            column("total", Kind::Numeric, ((10 << 16) | 2) + 4, None),
        ];
        let a = core("public", "orders", &columns, &[0], &options());
        assert_eq!(a["primary_key"], json!(["id"]));
        assert_eq!(a["columns"][1]["scale"], json!(2));
        assert_eq!(a["columns"][1]["logical_type"], json!("decimal"));
        let v = version(&a);
        assert_eq!(v.len(), 16);
        assert_eq!(v, version(&core("public", "orders", &columns, &[0], &options())));
        let mut other = options();
        other.decimal = DecimalHandlingMode::Precise;
        assert_ne!(v, version(&core("public", "orders", &columns, &[0], &other)));
    }

    #[test]
    fn arrays_describe_their_items() {
        let columns = vec![column("tags", Kind::Array(Box::new(Kind::Text)), -1, None)];
        let c = core("public", "t", &columns, &[], &options());
        assert_eq!(c["columns"][0]["json_type"], json!("array"));
        assert_eq!(c["columns"][0]["items"]["logical_type"], json!("string"));
    }
}
