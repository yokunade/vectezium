//! Conversion of `MySQL` column values into Debezium's JSON representation.
//!
//! The mapping follows the Debezium `MySQL` connector with the JSON converter and
//! `schemas.enable=false`:
//!
//! | `MySQL` type                    | Output                                                       |
//! |-------------------------------|--------------------------------------------------------------|
//! | integer types                 | number (`BIGINT UNSIGNED` > 2^63-1 wraps, as in Debezium)    |
//! | `FLOAT` / `DOUBLE`            | number                                                       |
//! | `DECIMAL`                     | `precise`: base64 of the unscaled two's-complement integer; `string`; `double` |
//! | `DATE`                        | days since the epoch                                         |
//! | `TIME`                        | microseconds (`adaptive_time_microseconds`) or milliseconds (`connect`) |
//! | `DATETIME(0-3)`               | milliseconds since the epoch                                 |
//! | `DATETIME(4-6)`               | microseconds since the epoch (`connect`: milliseconds)       |
//! | `TIMESTAMP`                   | ISO-8601 string in UTC, e.g. `2024-05-01T10:15:30.123Z`      |
//! | `YEAR`                        | number                                                       |
//! | `BIT(1)`                      | boolean                                                      |
//! | `BIT(n>1)`                    | little-endian bytes                                          |
//! | `CHAR`/`VARCHAR`/`TEXT`       | string                                                       |
//! | `BINARY`/`VARBINARY`/`BLOB`   | bytes                                                        |
//! | `ENUM` / `SET`                | string (`SET` values comma separated)                        |
//! | `JSON`                        | string containing the JSON document                          |
//! | `GEOMETRY`                    | `{"wkb": <bytes>, "srid": <number>}`                         |
//! | `VECTOR`                      | array of numbers                                             |
//!
//! Bytes are encoded according to `binary_handling_mode` (base64 by default, like Debezium's
//! JSON converter).

use base64::prelude::{BASE64_STANDARD, BASE64_URL_SAFE, Engine as _};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use mysql_async::{Value, binlog::value::BinlogValue};
use serde_json::{Map, Number, Value as Json, json};

use super::{
    BinaryHandlingMode, DecimalHandlingMode, JsonHandlingMode, TimePrecisionMode,
    schema::{ColKind, ColumnDesc},
};

/// Where a value came from; the two protocols encode a few types differently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Origin {
    Binlog,
    Snapshot,
}

#[derive(Clone, Copy, Debug)]
pub struct ConvertOptions {
    pub decimal: DecimalHandlingMode,
    pub binary: BinaryHandlingMode,
    pub time_precision: TimePrecisionMode,
    pub json: JsonHandlingMode,
}

#[derive(Debug)]
pub struct ConvertError(pub String);

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(column: &ColumnDesc, msg: impl std::fmt::Display) -> Result<T, ConvertError> {
    Err(ConvertError(format!(
        "column `{}` ({:?}): {msg}",
        column.name, column.kind
    )))
}

/// Represents a `JSON` column value per `json_handling_mode`.
fn json_document(doc: Json, opts: ConvertOptions) -> Json {
    match opts.json {
        JsonHandlingMode::String => Json::String(doc.to_string()),
        JsonHandlingMode::Object => doc,
    }
}

/// Converts a value decoded from a binlog row image.
pub fn convert_binlog_value(
    column: &ColumnDesc,
    value: &BinlogValue<'_>,
    opts: ConvertOptions,
) -> Result<Json, ConvertError> {
    match value {
        BinlogValue::Value(v) => convert_value(column, v, Origin::Binlog, opts),
        BinlogValue::Jsonb(jsonb) => match serde_json::Value::try_from(jsonb.clone()) {
            Ok(doc) => Ok(json_document(doc, opts)),
            Err(error) => err(column, format!("cannot convert JSONB value: {error}")),
        },
        BinlogValue::JsonDiff(_) => err(
            column,
            "partial JSON update received; set `binlog_row_value_options` to an empty value on the server",
        ),
    }
}

/// Converts a plain `MySQL` value (snapshot rows, or the non-JSON part of binlog rows).
#[allow(clippy::too_many_lines, reason = "one arm per MySQL column type")]
pub fn convert_value(
    column: &ColumnDesc,
    value: &Value,
    origin: Origin,
    opts: ConvertOptions,
) -> Result<Json, ConvertError> {
    if matches!(value, Value::NULL) {
        return Ok(Json::Null);
    }

    match column.kind {
        ColKind::TinyInt
        | ColKind::SmallInt
        | ColKind::MediumInt
        | ColKind::Int
        | ColKind::BigInt => integer(column, value),
        ColKind::Year => match value {
            Value::Int(i) => Ok(json!(i)),
            Value::UInt(u) => Ok(json!(u)),
            Value::Bytes(b) => parse_str(column, b)?
                .trim()
                .parse::<i64>()
                .map(|y| json!(y))
                .or_else(|_| err(column, "invalid YEAR value")),
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Float => match value {
            // Go through the shortest textual form so 0.1f32 becomes 0.1 rather than
            // 0.10000000149011612.
            Value::Float(f) => float(
                column,
                f.to_string().parse::<f64>().unwrap_or(f64::from(*f)),
            ),
            Value::Double(d) => float(column, *d),
            Value::Bytes(b) => float(column, parse_f64(column, b)?),
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Double => match value {
            Value::Double(d) => float(column, *d),
            Value::Float(f) => float(column, f64::from(*f)),
            Value::Bytes(b) => float(column, parse_f64(column, b)?),
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Decimal => {
            let text = match value {
                Value::Bytes(b) => parse_str(column, b)?.trim().to_owned(),
                Value::Int(i) => i.to_string(),
                Value::UInt(u) => u.to_string(),
                Value::Double(d) => d.to_string(),
                other => return err(column, format!("unexpected value {other:?}")),
            };
            decimal(column, &text, opts)
        }
        ColKind::Date => match value {
            Value::Date(y, m, d, ..) => {
                Ok(date_days(*y, *m, *d).map_or(Json::Null, |days| json!(days)))
            }
            Value::Bytes(b) => {
                let s = parse_str(column, b)?;
                Ok(NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .ok()
                    .map_or(Json::Null, |d| json!(days_since_epoch(d))))
            }
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Time => match value {
            Value::Time(negative, days, h, m, s, us) => {
                let total_us = ((i64::from(*days) * 24 + i64::from(*h)) * 3600
                    + i64::from(*m) * 60
                    + i64::from(*s))
                    * 1_000_000
                    + i64::from(*us);
                let total_us = if *negative { -total_us } else { total_us };
                Ok(match opts.time_precision {
                    TimePrecisionMode::AdaptiveTimeMicroseconds => json!(total_us),
                    TimePrecisionMode::Connect => json!(total_us.div_euclid(1000)),
                })
            }
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::DateTime => match value {
            Value::Date(y, mo, d, h, mi, s, us) => {
                let Some(dt) = naive_datetime(*y, *mo, *d, *h, *mi, *s, *us) else {
                    // Zero or otherwise invalid dates are emitted as null, like Debezium.
                    return Ok(Json::Null);
                };
                let micros = dt.and_utc().timestamp_micros();
                let use_micros = opts.time_precision == TimePrecisionMode::AdaptiveTimeMicroseconds
                    && column.fsp > 3;
                Ok(if use_micros {
                    json!(micros)
                } else {
                    json!(micros.div_euclid(1000))
                })
            }
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Timestamp => timestamp(column, value, origin),
        ColKind::Bit => {
            let Value::Bytes(bytes) = value else {
                return err(column, format!("unexpected value {value:?}"));
            };
            if column.bit_len == 1 {
                Ok(Json::Bool(bytes.iter().any(|b| *b != 0)))
            } else {
                // MySQL sends big-endian bytes; Debezium emits little-endian.
                let mut le: Vec<u8> = bytes.iter().rev().copied().collect();
                let expected = (column.bit_len as usize).div_ceil(8).max(1);
                le.resize(expected.max(le.len()), 0);
                Ok(Json::String(encode_bytes(&le, opts.binary)))
            }
        }
        ColKind::String => match value {
            Value::Bytes(bytes) if column.is_binary() => {
                Ok(Json::String(encode_bytes(bytes, opts.binary)))
            }
            Value::Bytes(bytes) => Ok(Json::String(decode_text(bytes, column.charset))),
            Value::Int(i) => Ok(Json::String(i.to_string())),
            Value::UInt(u) => Ok(Json::String(u.to_string())),
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Enum => match (origin, value) {
            (_, Value::Bytes(bytes)) => Ok(Json::String(decode_text(bytes, column.charset))),
            (_, Value::Int(index)) => Ok(Json::String(enum_value(column, *index))),
            (_, other) => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Set => match (origin, value) {
            (Origin::Binlog, Value::Bytes(bitmap)) => Ok(Json::String(set_value(column, bitmap))),
            (Origin::Snapshot, Value::Bytes(bytes)) => {
                Ok(Json::String(decode_text(bytes, column.charset)))
            }
            (_, other) => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Json => match value {
            // MySQL's text form (`{"a": 1}`) is re-serialized so snapshot rows match binlog rows.
            Value::Bytes(bytes) => Ok(match serde_json::from_slice::<Json>(bytes) {
                Ok(doc) => json_document(doc, opts),
                Err(_) => Json::String(String::from_utf8_lossy(bytes).into_owned()),
            }),
            other => err(column, format!("unexpected value {other:?}")),
        },
        ColKind::Geometry => match value {
            Value::Bytes(bytes) if bytes.len() >= 4 => {
                let srid = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let mut geometry = Map::new();
                geometry.insert(
                    "wkb".into(),
                    Json::String(encode_bytes(&bytes[4..], opts.binary)),
                );
                geometry.insert("srid".into(), json!(srid));
                Ok(Json::Object(geometry))
            }
            other => err(column, format!("unexpected geometry value {other:?}")),
        },
        ColKind::Vector => match value {
            Value::Bytes(bytes) if bytes.len() % 4 == 0 => Ok(Json::Array(
                bytes
                    .chunks_exact(4)
                    .map(|c| {
                        let f = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
                        Number::from_f64(f.to_string().parse().unwrap_or(f64::from(f)))
                            .map_or(Json::Null, Json::Number)
                    })
                    .collect(),
            )),
            other => err(column, format!("unexpected vector value {other:?}")),
        },
        ColKind::Unknown => match value {
            Value::Bytes(bytes) => Ok(Json::String(encode_bytes(bytes, opts.binary))),
            Value::Int(i) => Ok(json!(i)),
            Value::UInt(u) => Ok(json!(u)),
            Value::Float(f) => float(column, f64::from(*f)),
            Value::Double(d) => float(column, *d),
            other => err(column, format!("unsupported value {other:?}")),
        },
    }
}

fn integer(column: &ColumnDesc, value: &Value) -> Result<Json, ConvertError> {
    match value {
        Value::Int(i) => Ok(json!(i)),
        // Like Debezium's default `bigint.unsigned.handling.mode=long`, `BIGINT UNSIGNED`
        // values above i64::MAX wrap around (Vector event values are signed 64-bit integers).
        Value::UInt(u) => Ok(json!(u.cast_signed())),
        Value::Bytes(b) => {
            let s = parse_str(column, b)?;
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                Ok(json!(i))
            } else if let Ok(u) = s.parse::<u64>() {
                Ok(json!(u))
            } else {
                err(column, format!("invalid integer {s:?}"))
            }
        }
        other => err(column, format!("unexpected value {other:?}")),
    }
}

fn float(column: &ColumnDesc, f: f64) -> Result<Json, ConvertError> {
    Number::from_f64(f)
        .map(Json::Number)
        .ok_or_else(|| ConvertError(format!("column `{}`: non-finite float", column.name)))
}

fn parse_str<'a>(column: &ColumnDesc, bytes: &'a [u8]) -> Result<&'a str, ConvertError> {
    std::str::from_utf8(bytes).or_else(|_| err(column, "value is not valid UTF-8"))
}

fn parse_f64(column: &ColumnDesc, bytes: &[u8]) -> Result<f64, ConvertError> {
    parse_str(column, bytes)?
        .trim()
        .parse()
        .or_else(|_| err(column, "invalid floating point value"))
}

fn decimal(column: &ColumnDesc, text: &str, opts: ConvertOptions) -> Result<Json, ConvertError> {
    match opts.decimal {
        DecimalHandlingMode::String => Ok(Json::String(text.to_owned())),
        DecimalHandlingMode::Double => text
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Json::Number)
            .ok_or_else(|| {
                ConvertError(format!(
                    "column `{}`: invalid decimal {text:?}",
                    column.name
                ))
            }),
        DecimalHandlingMode::Precise => decimal_unscaled_bytes(text)
            .map(|bytes| Json::String(encode_bytes(&bytes, opts.binary)))
            .ok_or_else(|| {
                ConvertError(format!(
                    "column `{}`: invalid decimal {text:?}",
                    column.name
                ))
            }),
    }
}

/// Returns the unscaled value of a decimal string as a minimal big-endian two's-complement byte
/// array (the format of Java's `BigInteger.toByteArray()`, used by Kafka Connect's `Decimal`).
pub fn decimal_unscaled_bytes(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if digits.is_empty() || digits.chars().filter(|c| *c == '.').count() > 1 {
        return None;
    }
    let mut decimal_digits: Vec<u8> = Vec::with_capacity(digits.len());
    for c in digits.chars() {
        match c {
            '0'..='9' => decimal_digits.push(c as u8 - b'0'),
            '.' => {}
            _ => return None,
        }
    }

    // Convert base-10 digits to base-256 magnitude (big-endian) by repeated division.
    let mut magnitude: Vec<u8> = Vec::new();
    let mut number = decimal_digits;
    while number.iter().any(|d| *d != 0) {
        let mut remainder: u32 = 0;
        let mut quotient = Vec::with_capacity(number.len());
        for d in &number {
            let acc = remainder * 10 + u32::from(*d);
            let q = acc / 256;
            remainder = acc % 256;
            if !quotient.is_empty() || q != 0 {
                // remainder < 256, so acc < 2570 and q <= 10.
                quotient.push(u8::try_from(q).expect("quotient below 256"));
            }
        }
        magnitude.push(u8::try_from(remainder).expect("remainder below 256"));
        number = quotient;
    }
    magnitude.reverse();

    if magnitude.is_empty() {
        return Some(vec![0]);
    }

    if !negative {
        if magnitude[0] & 0x80 != 0 {
            magnitude.insert(0, 0);
        }
        return Some(magnitude);
    }

    // Two's complement of the magnitude.
    let mut bytes: Vec<u8> = magnitude.iter().map(|b| !b).collect();
    for byte in bytes.iter_mut().rev() {
        let (sum, overflow) = byte.overflowing_add(1);
        *byte = sum;
        if !overflow {
            break;
        }
    }
    if bytes[0] & 0x80 == 0 {
        bytes.insert(0, 0xFF);
    }
    // Drop redundant sign bytes.
    while bytes.len() > 1 && bytes[0] == 0xFF && bytes[1] & 0x80 != 0 {
        bytes.remove(0);
    }
    Some(bytes)
}

fn days_since_epoch(date: NaiveDate) -> i64 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    (date - epoch).num_days()
}

fn date_days(y: u16, m: u8, d: u8) -> Option<i64> {
    NaiveDate::from_ymd_opt(i32::from(y), u32::from(m), u32::from(d)).map(days_since_epoch)
}

#[allow(clippy::too_many_arguments)]
fn naive_datetime(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> Option<NaiveDateTime> {
    let date = NaiveDate::from_ymd_opt(i32::from(y), u32::from(mo), u32::from(d))?;
    let time = NaiveTime::from_hms_micro_opt(u32::from(h), u32::from(mi), u32::from(s), us)?;
    Some(NaiveDateTime::new(date, time))
}

fn timestamp(column: &ColumnDesc, value: &Value, origin: Origin) -> Result<Json, ConvertError> {
    let micros: i64 = match (origin, value) {
        // Binlog TIMESTAMP2 values are "<epoch seconds>[.<microseconds>]".
        (Origin::Binlog, Value::Bytes(bytes)) => {
            let s = parse_str(column, bytes)?;
            let (secs, frac) = s.split_once('.').unwrap_or((s, "0"));
            let secs: i64 = secs.parse().or_else(|_| err(column, "invalid timestamp"))?;
            let frac = format!("{frac:0<6}");
            let usecs: i64 = frac
                .get(..6)
                .and_then(|digits| digits.parse().ok())
                .map_or_else(|| err(column, "invalid timestamp"), Ok)?;
            secs * 1_000_000 + usecs
        }
        (Origin::Binlog, Value::Int(secs)) => secs * 1_000_000,
        // Snapshot reads use a `+00:00` session time zone, so the value is already UTC.
        (_, Value::Date(y, mo, d, h, mi, s, us)) => {
            match naive_datetime(*y, *mo, *d, *h, *mi, *s, *us) {
                Some(dt) => dt.and_utc().timestamp_micros(),
                None => return Ok(Json::Null),
            }
        }
        (_, other) => return err(column, format!("unexpected value {other:?}")),
    };
    Ok(Json::String(format_zoned_timestamp(micros, column.fsp)))
}

/// Formats epoch microseconds as ISO-8601 UTC with exactly `fsp` fractional digits.
pub fn format_zoned_timestamp(micros: i64, fsp: u8) -> String {
    let Some(dt) = DateTime::from_timestamp_micros(micros) else {
        return micros.to_string();
    };
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let fsp = usize::from(fsp.min(6));
    if fsp == 0 {
        format!("{base}Z")
    } else {
        let mut frac = format!("{:06}", micros.rem_euclid(1_000_000));
        frac.truncate(fsp);
        format!("{base}.{frac}Z")
    }
}

fn enum_value(column: &ColumnDesc, index: i64) -> String {
    if index <= 0 {
        // Index 0 is MySQL's "invalid value" empty string.
        return String::new();
    }
    column
        .enum_values
        .as_ref()
        .zip(usize::try_from(index - 1).ok())
        .and_then(|(values, i)| values.get(i))
        .cloned()
        .unwrap_or_else(|| index.to_string())
}

fn set_value(column: &ColumnDesc, bitmap: &[u8]) -> String {
    let Some(values) = column.set_values.as_ref() else {
        return encode_bytes(bitmap, BinaryHandlingMode::Base64);
    };
    let mut selected = Vec::new();
    for (i, value) in values.iter().enumerate() {
        let byte = bitmap.get(i / 8).copied().unwrap_or(0);
        if byte & (1 << (i % 8)) != 0 {
            selected.push(value.as_str());
        }
    }
    selected.join(",")
}

/// `MySQL` character-set ids that use the latin1 repertoire.
const LATIN1_CHARSETS: &[u16] = &[5, 8, 15, 31, 47, 48, 49, 94];

pub fn decode_text(bytes: &[u8], charset: u16) -> String {
    if LATIN1_CHARSETS.contains(&charset) {
        bytes.iter().map(|b| char::from(*b)).collect()
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

pub fn encode_bytes(bytes: &[u8], mode: BinaryHandlingMode) -> String {
    match mode {
        BinaryHandlingMode::Base64 => BASE64_STANDARD.encode(bytes),
        BinaryHandlingMode::Base64UrlSafe => BASE64_URL_SAFE.encode(bytes),
        BinaryHandlingMode::Hex => hex::encode(bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn col(kind: ColKind) -> ColumnDesc {
        ColumnDesc {
            name: "c".into(),
            position: 1,
            kind,
            mysql_type: "test",
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
            json: JsonHandlingMode::String,
        }
    }

    #[test]
    fn unscaled_decimal_bytes_match_java_biginteger() {
        assert_eq!(decimal_unscaled_bytes("0"), Some(vec![0]));
        assert_eq!(decimal_unscaled_bytes("0.00"), Some(vec![0]));
        assert_eq!(decimal_unscaled_bytes("1"), Some(vec![1]));
        assert_eq!(decimal_unscaled_bytes("1.28"), Some(vec![0x00, 0x80]));
        assert_eq!(decimal_unscaled_bytes("-1"), Some(vec![0xFF]));
        assert_eq!(decimal_unscaled_bytes("-1.28"), Some(vec![0x80]));
        assert_eq!(decimal_unscaled_bytes("-129"), Some(vec![0xFF, 0x7F]));
        assert_eq!(decimal_unscaled_bytes("-256"), Some(vec![0xFF, 0x00]));
        assert_eq!(
            decimal_unscaled_bytes("65536"),
            Some(vec![0x01, 0x00, 0x00])
        );
        assert_eq!(decimal_unscaled_bytes("12a"), None);
    }

    #[test]
    fn temporal_values() {
        let o = opts();
        assert_eq!(
            convert_value(
                &col(ColKind::Date),
                &Value::Date(1970, 1, 2, 0, 0, 0, 0),
                Origin::Binlog,
                o
            )
            .unwrap(),
            json!(1)
        );
        assert_eq!(
            convert_value(
                &col(ColKind::Date),
                &Value::Date(0, 0, 0, 0, 0, 0, 0),
                Origin::Binlog,
                o
            )
            .unwrap(),
            Json::Null
        );

        let mut dt = col(ColKind::DateTime);
        let v = Value::Date(2024, 1, 1, 0, 0, 1, 500_000);
        assert_eq!(
            convert_value(&dt, &v, Origin::Binlog, o).unwrap(),
            json!(1_704_067_201_500_i64)
        );
        dt.fsp = 6;
        assert_eq!(
            convert_value(&dt, &v, Origin::Binlog, o).unwrap(),
            json!(1_704_067_201_500_000_i64)
        );

        let t = Value::Time(true, 1, 2, 0, 0, 5);
        assert_eq!(
            convert_value(&col(ColKind::Time), &t, Origin::Binlog, o).unwrap(),
            json!(-(26_i64 * 3600 * 1_000_000 + 5))
        );

        let mut ts = col(ColKind::Timestamp);
        ts.fsp = 3;
        assert_eq!(
            convert_value(
                &ts,
                &Value::Bytes(b"1704067200.123456".to_vec()),
                Origin::Binlog,
                o
            )
            .unwrap(),
            json!("2024-01-01T00:00:00.123Z")
        );
        ts.fsp = 0;
        assert_eq!(
            convert_value(
                &ts,
                &Value::Date(2024, 1, 1, 0, 0, 0, 0),
                Origin::Snapshot,
                o
            )
            .unwrap(),
            json!("2024-01-01T00:00:00Z")
        );
    }

    #[test]
    fn bit_enum_set_values() {
        let o = opts();
        let mut bit = col(ColKind::Bit);
        bit.bit_len = 1;
        assert_eq!(
            convert_value(&bit, &Value::Bytes(vec![1]), Origin::Binlog, o).unwrap(),
            json!(true)
        );
        bit.bit_len = 16;
        assert_eq!(
            convert_value(&bit, &Value::Bytes(vec![0x01, 0x02]), Origin::Binlog, o).unwrap(),
            json!(BASE64_STANDARD.encode([0x02, 0x01]))
        );

        let mut e = col(ColKind::Enum);
        e.enum_values = Some(Arc::from(vec!["a".into(), "b".into()]));
        assert_eq!(
            convert_value(&e, &Value::Int(2), Origin::Binlog, o).unwrap(),
            json!("b")
        );
        assert_eq!(
            convert_value(&e, &Value::Bytes(b"a".to_vec()), Origin::Snapshot, o).unwrap(),
            json!("a")
        );

        let mut s = col(ColKind::Set);
        s.set_values = Some(Arc::from(vec!["x".into(), "y".into(), "z".into()]));
        assert_eq!(
            convert_value(&s, &Value::Bytes(vec![0b101]), Origin::Binlog, o).unwrap(),
            json!("x,z")
        );
    }

    #[test]
    fn strings_and_binary() {
        let o = opts();
        assert_eq!(
            convert_value(
                &col(ColKind::String),
                &Value::Bytes("héllo".as_bytes().to_vec()),
                Origin::Binlog,
                o
            )
            .unwrap(),
            json!("héllo")
        );
        let mut latin1 = col(ColKind::String);
        latin1.charset = 8;
        assert_eq!(
            convert_value(&latin1, &Value::Bytes(vec![0x68, 0xE9]), Origin::Binlog, o).unwrap(),
            json!("hé")
        );
        let mut bin = col(ColKind::String);
        bin.charset = 63;
        assert_eq!(
            convert_value(&bin, &Value::Bytes(vec![0xDE, 0xAD]), Origin::Binlog, o).unwrap(),
            json!("3q0=")
        );
    }

    #[test]
    fn numbers_and_decimals() {
        let mut o = opts();
        assert_eq!(
            convert_value(
                &col(ColKind::BigInt),
                &Value::UInt(u64::MAX),
                Origin::Binlog,
                o
            )
            .unwrap(),
            json!(-1)
        );
        assert_eq!(
            convert_value(&col(ColKind::Float), &Value::Float(0.1), Origin::Binlog, o).unwrap(),
            json!(0.1)
        );
        let dec = Value::Bytes(b"-12.50".to_vec());
        assert_eq!(
            convert_value(&col(ColKind::Decimal), &dec, Origin::Binlog, o).unwrap(),
            json!("-12.50")
        );
        o.decimal = DecimalHandlingMode::Double;
        assert_eq!(
            convert_value(&col(ColKind::Decimal), &dec, Origin::Binlog, o).unwrap(),
            json!(-12.5)
        );
        o.decimal = DecimalHandlingMode::Precise;
        // -1250 => 0xFB1E
        assert_eq!(
            convert_value(&col(ColKind::Decimal), &dec, Origin::Binlog, o).unwrap(),
            json!(BASE64_STANDARD.encode([0xFB, 0x1E]))
        );
    }

    #[test]
    fn geometry_value() {
        let o = opts();
        let mut bytes = 4326_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[1, 2, 3]);
        assert_eq!(
            convert_value(
                &col(ColKind::Geometry),
                &Value::Bytes(bytes),
                Origin::Binlog,
                o
            )
            .unwrap(),
            json!({"wkb": BASE64_STANDARD.encode([1, 2, 3]), "srid": 4326})
        );
    }
}
