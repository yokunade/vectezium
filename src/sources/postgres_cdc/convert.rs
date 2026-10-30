//! Conversion of PostgreSQL column values (text format) into Debezium's JSON representation.
//!
//! Both the stream (pgoutput's text tuples) and the snapshot (text query results) print values
//! with the type's output function, under the session settings the source sets on every
//! connection (`TimeZone=UTC`, `DateStyle=ISO`, `IntervalStyle=iso_8601`,
//! `extra_float_digits=3`, `bytea_output=hex`), so both convert identically.
//!
//! The mapping follows Debezium's PostgreSQL connector with the JSON converter:
//!
//! | PostgreSQL type               | Output                                                       |
//! |-------------------------------|--------------------------------------------------------------|
//! | `smallint`/`integer`/`bigint` | number                                                       |
//! | `real`/`double precision`     | number (`"NaN"`, `"Infinity"`, `"-Infinity"` as strings)     |
//! | `numeric`/`money`             | `decimal_handling_mode`: `precise` base64 unscaled bytes (`{"scale", "value"}` without a declared scale); `string`; `double` |
//! | `boolean`                     | boolean                                                      |
//! | `date`                        | days since the epoch                                         |
//! | `time`                        | microseconds (`connect`: milliseconds)                        |
//! | `timetz`                      | string in UTC, e.g. `"10:15:30.123456Z"`                     |
//! | `timestamp(0-3)` / `(4-6)`    | milliseconds / microseconds since the epoch (`connect`: milliseconds) |
//! | `timestamptz`                 | ISO-8601 string in UTC, e.g. `"2024-05-01T10:15:30.123456Z"` |
//! | `interval`                    | microseconds (a month is 30.4375 days), or an ISO-8601 duration |
//! | `bytea`                       | bytes (`binary_handling_mode`)                               |
//! | `bit(1)` / `bit(n)`, `varbit` | boolean / bytes, little endian                               |
//! | `json`/`jsonb`                | string (or the parsed document with `json_handling_mode: object`) |
//! | `point`                       | `{"x": …, "y": …}`                                           |
//! | arrays                        | JSON arrays of the converted elements                         |
//! | `uuid`, text types, enums, `inet`, ranges, … | string                                        |
//!
//! `infinity` / `-infinity` dates and timestamps map to the largest / smallest values of their
//! representation (`"infinity"` / `"-infinity"` for strings).

use base64::Engine as _;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::{Map, Value as Json, json};

use super::{
    BinaryHandlingMode, DecimalHandlingMode, IntervalHandlingMode, JsonHandlingMode,
    TimePrecisionMode,
    catalog::{Column, Kind},
};

/// The settings that decide how values are represented (part of the schema version).
#[derive(Clone, Debug)]
pub struct ConvertOptions {
    pub decimal: DecimalHandlingMode,
    pub binary: BinaryHandlingMode,
    pub time_precision: TimePrecisionMode,
    pub interval: IntervalHandlingMode,
    pub json: JsonHandlingMode,
    pub money_fraction_digits: u8,
}

/// Days in a month for `interval_handling_mode: numeric` (Debezium's 365.25 / 12).
const DAYS_PER_MONTH: f64 = 30.4375;
const MICROS_PER_DAY: i64 = 86_400_000_000;

const EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

/// Converts one column value (its text, or `None` for SQL `NULL`).
pub fn convert(column: &Column, text: Option<&str>, options: &ConvertOptions) -> Result<Json, String> {
    match text {
        None => Ok(Json::Null),
        Some(text) => convert_kind(&column.ty.kind, column, text, options)
            .map_err(|error| format!("column `{}` ({}): {error}", column.name, column.ty.name)),
    }
}

fn convert_kind(kind: &Kind, column: &Column, text: &str, o: &ConvertOptions) -> Result<Json, String> {
    Ok(match kind {
        Kind::Bool => Json::Bool(match text {
            "t" | "true" => true,
            "f" | "false" => false,
            _ => return Err(format!("not a boolean: `{text}`")),
        }),
        Kind::Int16 | Kind::Int32 | Kind::Int64 => {
            json!(text.parse::<i64>().map_err(|_| format!("not an integer: `{text}`"))?)
        }
        Kind::Oid => json!(text.parse::<u64>().map_err(|_| format!("not an oid: `{text}`"))?),
        Kind::Float32 | Kind::Float64 => float(text)?,
        Kind::Numeric => {
            let scale = column.numeric_precision_scale().map(|(_, s)| s);
            decimal(text, scale, o)?
        }
        Kind::Money => money(text, o)?,
        Kind::Bytea => Json::from(encode_bytes(&bytea(text)?, o.binary)),
        Kind::Date => json!(date_days(text)?),
        Kind::Time => {
            let micros = time_micros(text)?;
            match o.time_precision {
                TimePrecisionMode::AdaptiveTimeMicroseconds => json!(micros),
                TimePrecisionMode::Connect => json!(micros.div_euclid(1000)),
            }
        }
        Kind::TimeTz => Json::from(timetz_utc(text)?),
        Kind::Timestamp => {
            let millis = o.time_precision == TimePrecisionMode::Connect || column.time_precision() <= 3;
            json!(timestamp(text, millis)?)
        }
        Kind::TimestampTz => Json::from(timestamptz_iso(text)?),
        Kind::Interval => match o.interval {
            IntervalHandlingMode::String => Json::from(text),
            IntervalHandlingMode::Numeric => json!(interval_micros(text)?),
        },
        Kind::Json | Kind::Jsonb => match o.json {
            JsonHandlingMode::String => Json::from(text),
            JsonHandlingMode::Object => {
                serde_json::from_str(text).map_err(|error| format!("invalid JSON: {error}"))?
            }
        },
        Kind::Bit | Kind::VarBit => bits(text, column, o)?,
        Kind::Point => point(text)?,
        Kind::Array(element) => array(text, element, column, o)?,
        Kind::Uuid | Kind::Enum | Kind::Text => Json::from(text),
    })
}

fn float(text: &str) -> Result<Json, String> {
    match text {
        "NaN" | "Infinity" | "-Infinity" => Ok(Json::from(text)),
        _ => {
            let value: f64 = text.parse().map_err(|_| format!("not a number: `{text}`"))?;
            Ok(json!(value))
        }
    }
}

/// A decimal in the configured representation. `scale` is the column's declared scale.
fn decimal(text: &str, scale: Option<i32>, o: &ConvertOptions) -> Result<Json, String> {
    let special = matches!(text, "NaN" | "Infinity" | "-Infinity");
    Ok(match o.decimal {
        DecimalHandlingMode::String => Json::from(text),
        DecimalHandlingMode::Double => {
            if special {
                Json::from(text)
            } else {
                json!(text.parse::<f64>().map_err(|_| format!("not a decimal: `{text}`"))?)
            }
        }
        // Not representable as an unscaled integer (Debezium gives null too).
        DecimalHandlingMode::Precise if special => Json::Null,
        DecimalHandlingMode::Precise => {
            let (unscaled, text_scale) = unscaled(text)?;
            match scale {
                // A declared scale: the value always has exactly that many digits.
                Some(scale) if scale >= 0 => {
                    let unscaled = rescale(&unscaled, text_scale, scale as usize);
                    Json::from(encode_bytes(&twos_complement(&unscaled), o.binary))
                }
                // Debezium's VariableScaleDecimal.
                _ => json!({
                    "scale": text_scale,
                    "value": encode_bytes(&twos_complement(&unscaled), o.binary),
                }),
            }
        }
    })
}

/// `"-12.340"` → (`"-12340"`, 3).
fn unscaled(text: &str) -> Result<(String, usize), String> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("not a decimal: `{text}`"));
    }
    let mut out = String::with_capacity(digits.len() + 1);
    if negative {
        out.push('-');
    }
    out.push_str(int);
    out.push_str(frac);
    Ok((out, frac.len()))
}

/// Adjusts an unscaled value from `from` to `to` fractional digits (only ever widens in practice;
/// narrowing truncates).
fn rescale(unscaled: &str, from: usize, to: usize) -> String {
    if to >= from {
        format!("{unscaled}{}", "0".repeat(to - from))
    } else {
        unscaled[..unscaled.len() - (from - to)].to_owned()
    }
}

/// The big-endian two's-complement bytes of a decimal integer of any size (Java's
/// `BigInteger.toByteArray`, which Debezium uses).
fn twos_complement(value: &str) -> Vec<u8> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    // Magnitude in base 256, little endian, by repeated division of the decimal digits.
    let mut decimal: Vec<u8> = digits.bytes().map(|b| b - b'0').collect();
    let mut magnitude = Vec::new();
    while decimal.iter().any(|&d| d != 0) {
        let mut remainder: u32 = 0;
        let mut quotient = Vec::with_capacity(decimal.len());
        for &d in &decimal {
            let acc = remainder * 10 + u32::from(d);
            quotient.push((acc / 256) as u8);
            remainder = acc % 256;
        }
        magnitude.push(remainder as u8);
        let first = quotient.iter().position(|&d| d != 0).unwrap_or(quotient.len());
        decimal = quotient.split_off(first);
    }
    if magnitude.is_empty() {
        return vec![0];
    }
    if negative {
        // Two's complement: invert and add one.
        let mut carry = true;
        for byte in &mut magnitude {
            *byte = !*byte;
            if carry {
                let (sum, overflow) = byte.overflowing_add(1);
                *byte = sum;
                carry = overflow;
            }
        }
        if magnitude.last().is_some_and(|&b| b & 0x80 == 0) {
            magnitude.push(0xff);
        }
    } else if magnitude.last().is_some_and(|&b| b & 0x80 != 0) {
        magnitude.push(0);
    }
    magnitude.reverse();
    magnitude
}

/// `money` prints with the server's `lc_monetary` (for example `-$1,234.56`): keep the sign,
/// digits and decimal point, then treat it as a decimal with `money_fraction_digits`.
fn money(text: &str, o: &ConvertOptions) -> Result<Json, String> {
    let negative = text.contains('-') || (text.starts_with('(') && text.ends_with(')'));
    let digits: String = text.chars().filter(|c| c.is_ascii_digit() || *c == '.').collect();
    if digits.is_empty() {
        return Err(format!("not a money value: `{text}`"));
    }
    let value = if negative { format!("-{digits}") } else { digits };
    decimal(&value, Some(i32::from(o.money_fraction_digits)), o)
}

/// `\x0a0b` (hex output) to bytes.
fn bytea(text: &str) -> Result<Vec<u8>, String> {
    let hex_text = text
        .strip_prefix("\\x")
        .ok_or_else(|| "bytea is not in hex format (bytea_output=hex)".to_owned())?;
    hex::decode(hex_text).map_err(|error| format!("invalid bytea: {error}"))
}

pub fn encode_bytes(bytes: &[u8], mode: BinaryHandlingMode) -> String {
    match mode {
        BinaryHandlingMode::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
        BinaryHandlingMode::Base64UrlSafe => base64::engine::general_purpose::URL_SAFE.encode(bytes),
        BinaryHandlingMode::Hex => hex::encode(bytes),
    }
}

/// Splits a trailing ` BC` off a date or timestamp.
fn era(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (text, false),
    }
}

/// A date (`YYYY-MM-DD`, `YYYY` may be longer, ` BC` for years before 1).
fn naive_date(text: &str) -> Result<NaiveDate, String> {
    let (text, bc) = era(text);
    let mut parts = text.splitn(3, '-');
    let (Some(y), Some(m), Some(d)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(format!("not a date: `{text}`"));
    };
    let parse = |s: &str| s.parse::<i32>().map_err(|_| format!("not a date: `{text}`"));
    let mut year = parse(y)?;
    if bc {
        // 1 BC is year 0 (proleptic Gregorian).
        year = 1 - year;
    }
    NaiveDate::from_ymd_opt(year, parse(m)? as u32, parse(d)? as u32)
        .ok_or_else(|| format!("not a date: `{text}`"))
}

fn date_days(text: &str) -> Result<i64, String> {
    match text {
        "infinity" => Ok(i64::from(i32::MAX)),
        "-infinity" => Ok(i64::from(i32::MIN)),
        _ => Ok((naive_date(text)? - EPOCH).num_days()),
    }
}

/// `HH:MM:SS[.ffffff]` to microseconds since midnight (`24:00:00` is a valid time).
fn time_micros(text: &str) -> Result<i64, String> {
    let bad = || format!("not a time: `{text}`");
    let (hms, frac) = text.split_once('.').unwrap_or((text, ""));
    let mut parts = hms.split(':');
    let (Some(h), Some(m), Some(s)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(bad());
    };
    let n = |s: &str| s.parse::<i64>().map_err(|_| bad());
    let micros_frac = if frac.is_empty() {
        0
    } else {
        let digits: String = frac.chars().chain(std::iter::repeat('0')).take(6).collect();
        n(&digits)?
    };
    Ok(((n(h)? * 60 + n(m)?) * 60 + n(s)?) * 1_000_000 + micros_frac)
}

/// Splits a UTC offset (`+05`, `-03:30`, `+05:30:15`) off the end of a time or timestamp;
/// returns the rest and the offset in seconds east of UTC.
fn split_offset(text: &str) -> Result<(&str, i64), String> {
    let at = text
        .rfind(['+', '-'])
        .filter(|&i| i > 0 && text[..i].contains(':'))
        .ok_or_else(|| format!("no time zone offset in `{text}`"))?;
    let (rest, offset) = text.split_at(at);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let mut seconds = 0;
    for (i, part) in offset[1..].split(':').enumerate() {
        let v: i64 = part.parse().map_err(|_| format!("bad offset in `{text}`"))?;
        seconds += v * [3600, 60, 1].get(i).copied().unwrap_or(0);
    }
    Ok((rest, sign * seconds))
}

/// `timetz` as Debezium's ZonedTime: the time of day in UTC with a `Z`.
fn timetz_utc(text: &str) -> Result<String, String> {
    let (time, offset) = split_offset(text)?;
    let micros = (time_micros(time)? - offset * 1_000_000).rem_euclid(MICROS_PER_DAY);
    let t = NaiveTime::from_num_seconds_from_midnight_opt(
        (micros / 1_000_000) as u32,
        ((micros % 1_000_000) * 1000) as u32,
    )
    .ok_or_else(|| format!("not a time: `{text}`"))?;
    Ok(format!("{}Z", format_time(&t)))
}

/// `HH:MM:SS` plus as many fractional digits as the value has (up to 6).
fn format_time(t: &NaiveTime) -> String {
    let micros = t.nanosecond() / 1000;
    let base = t.format("%H:%M:%S").to_string();
    if micros == 0 {
        base
    } else {
        let frac = format!("{micros:06}");
        format!("{base}.{}", frac.trim_end_matches('0'))
    }
}

fn naive_timestamp(text: &str) -> Result<NaiveDateTime, String> {
    let (body, bc) = era(text);
    let (date, time) = body
        .split_once(' ')
        .ok_or_else(|| format!("not a timestamp: `{text}`"))?;
    let date = naive_date(&if bc { format!("{date} BC") } else { date.to_owned() })?;
    let micros = time_micros(time)?;
    Ok(date.and_hms_opt(0, 0, 0).expect("midnight")
        + chrono::Duration::microseconds(micros))
}

/// `timestamp` as milliseconds or microseconds since the epoch.
fn timestamp(text: &str, millis: bool) -> Result<i64, String> {
    let micros = match text {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => naive_timestamp(text)?.and_utc().timestamp_micros(),
    };
    Ok(if millis { micros.div_euclid(1000) } else { micros })
}

/// `timestamptz` (printed in UTC) as Debezium's ZonedTimestamp.
fn timestamptz_iso(text: &str) -> Result<String, String> {
    if text == "infinity" || text == "-infinity" {
        return Ok(text.to_owned());
    }
    let (body, bc) = era(text);
    let (local, offset) = split_offset(body)?;
    let local = naive_timestamp(&if bc { format!("{local} BC") } else { local.to_owned() })?;
    let utc = local - chrono::Duration::seconds(offset);
    let date = utc.date().format("%Y-%m-%d");
    Ok(format!("{date}T{}Z", format_time(&utc.time())))
}

/// An ISO-8601 interval (`IntervalStyle=iso_8601`, e.g. `P1Y2M3DT4H5M6.5S`, `P-1D`, `PT-0.5S`)
/// as microseconds.
fn interval_micros(text: &str) -> Result<i64, String> {
    let bad = || format!("not an ISO 8601 interval: `{text}`");
    let body = text.strip_prefix('P').ok_or_else(bad)?;
    let (date, time) = body.split_once('T').unwrap_or((body, ""));
    let mut total = 0.0_f64;
    for (part, units) in [(date, "YMWD"), (time, "HMS")] {
        let mut number = String::new();
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' || c == '-' || c == '+' {
                number.push(c);
                continue;
            }
            if !units.contains(c) {
                return Err(bad());
            }
            let v: f64 = number.parse().map_err(|_| bad())?;
            number.clear();
            let day = MICROS_PER_DAY as f64;
            total += v * match (units, c) {
                ("YMWD", 'Y') => 12.0 * DAYS_PER_MONTH * day,
                ("YMWD", 'M') => DAYS_PER_MONTH * day,
                ("YMWD", 'W') => 7.0 * day,
                ("YMWD", 'D') => day,
                ("HMS", 'H') => 3_600_000_000.0,
                ("HMS", 'M') => 60_000_000.0,
                _ => 1_000_000.0,
            };
        }
        if !number.is_empty() {
            return Err(bad());
        }
    }
    Ok(total.round() as i64)
}

/// `bit(1)` as a boolean; longer bit strings as Debezium's `Bits`: bytes, least significant
/// bit first (the rightmost character is bit 0).
fn bits(text: &str, column: &Column, o: &ConvertOptions) -> Result<Json, String> {
    if !text.bytes().all(|b| b == b'0' || b == b'1') {
        return Err(format!("not a bit string: `{text}`"));
    }
    if column.ty.kind == Kind::Bit && column.bit_length() == Some(1) {
        return Ok(Json::Bool(text == "1"));
    }
    let mut bytes = vec![0u8; text.len().div_ceil(8)];
    for (i, b) in text.bytes().rev().enumerate() {
        if b == b'1' {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    Ok(Json::from(encode_bytes(&bytes, o.binary)))
}

/// `(x,y)`.
fn point(text: &str) -> Result<Json, String> {
    let inner = text
        .strip_prefix('(')
        .and_then(|t| t.strip_suffix(')'))
        .ok_or_else(|| format!("not a point: `{text}`"))?;
    let (x, y) = inner.split_once(',').ok_or_else(|| format!("not a point: `{text}`"))?;
    let n = |s: &str| s.trim().parse::<f64>().map_err(|_| format!("not a point: `{text}`"));
    let mut map = Map::with_capacity(2);
    map.insert("x".into(), json!(n(x)?));
    map.insert("y".into(), json!(n(y)?));
    Ok(Json::Object(map))
}

/// One parsed array element: its text, or `None` for an unquoted `NULL`.
#[derive(Debug, PartialEq)]
enum Element {
    Value(Option<String>),
    Nested(Vec<Element>),
}

/// An array literal (`{1,2,NULL}`, `{"a b","c\"d"}`, `{{1,2},{3,4}}`, optionally prefixed with
/// dimensions like `[0:1]=`) converted element by element.
fn array(text: &str, element: &Kind, column: &Column, o: &ConvertOptions) -> Result<Json, String> {
    let body = match text.find("={") {
        Some(i) if text.starts_with('[') => &text[i + 1..],
        _ => text,
    };
    let mut chars = body.chars().peekable();
    let parsed = parse_array(&mut chars).ok_or_else(|| format!("not an array: `{text}`"))?;
    to_json(&parsed, element, column, o)
}

fn to_json(elements: &[Element], kind: &Kind, column: &Column, o: &ConvertOptions) -> Result<Json, String> {
    elements
        .iter()
        .map(|e| match e {
            Element::Value(None) => Ok(Json::Null),
            Element::Value(Some(text)) => convert_kind(kind, column, text, o),
            Element::Nested(inner) => to_json(inner, kind, column, o),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Json::Array)
}

fn parse_array(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Vec<Element>> {
    if chars.next()? != '{' {
        return None;
    }
    let mut out = Vec::new();
    if chars.peek() == Some(&'}') {
        chars.next();
        return Some(out);
    }
    loop {
        match chars.peek()? {
            '{' => out.push(Element::Nested(parse_array(chars)?)),
            '"' => {
                chars.next();
                let mut value = String::new();
                loop {
                    match chars.next()? {
                        '\\' => value.push(chars.next()?),
                        '"' => break,
                        c => value.push(c),
                    }
                }
                out.push(Element::Value(Some(value)));
            }
            _ => {
                let mut value = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ',' || c == '}' {
                        break;
                    }
                    value.push(c);
                    chars.next();
                }
                let value = value.trim();
                out.push(Element::Value(
                    (!value.eq_ignore_ascii_case("NULL")).then(|| value.to_owned()),
                ));
            }
        }
        match chars.next()? {
            ',' => continue,
            '}' => return Some(out),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::postgres_cdc::catalog::TypeInfo;

    fn options() -> ConvertOptions {
        ConvertOptions {
            decimal: DecimalHandlingMode::Precise,
            binary: BinaryHandlingMode::Base64,
            time_precision: TimePrecisionMode::AdaptiveTimeMicroseconds,
            interval: IntervalHandlingMode::Numeric,
            json: JsonHandlingMode::String,
            money_fraction_digits: 2,
        }
    }

    fn col(kind: Kind, typmod: i32) -> Column {
        Column {
            name: "c".into(),
            position: 1,
            type_oid: 0,
            typmod,
            ty: TypeInfo { name: "t".into(), kind },
            nullable: true,
            primary_key: None,
            excluded: false,
        }
    }

    fn conv(kind: Kind, typmod: i32, text: &str) -> Json {
        convert(&col(kind, typmod), Some(text), &options()).unwrap()
    }

    #[test]
    fn scalars() {
        assert_eq!(conv(Kind::Bool, -1, "t"), json!(true));
        assert_eq!(conv(Kind::Int64, -1, "-9223372036854775808"), json!(i64::MIN));
        assert_eq!(conv(Kind::Float64, -1, "1.5"), json!(1.5));
        assert_eq!(conv(Kind::Float64, -1, "NaN"), json!("NaN"));
        assert_eq!(conv(Kind::Uuid, -1, "a0ee-1"), json!("a0ee-1"));
        assert_eq!(convert(&col(Kind::Int32, -1), None, &options()).unwrap(), Json::Null);
        assert!(convert(&col(Kind::Int32, -1), Some("x"), &options()).is_err());
    }

    #[test]
    fn decimals_match_java_biginteger() {
        // numeric(12,2): 1.5 is unscaled 150 = 0x0096.
        let typmod = ((12 << 16) | 2) + 4;
        assert_eq!(conv(Kind::Numeric, typmod, "1.50"), json!("AJY="));
        assert_eq!(twos_complement("150"), vec![0x00, 0x96]);
        assert_eq!(twos_complement("-150"), vec![0xff, 0x6a]);
        assert_eq!(twos_complement("-128"), vec![0x80]);
        assert_eq!(twos_complement("0"), vec![0]);
        assert_eq!(twos_complement("65536"), vec![0x01, 0x00, 0x00]);
        // Undeclared scale: Debezium's VariableScaleDecimal.
        assert_eq!(
            conv(Kind::Numeric, -1, "-12.340"),
            json!({"scale": 3, "value": base64::engine::general_purpose::STANDARD.encode(twos_complement("-12340"))})
        );
        assert_eq!(conv(Kind::Numeric, -1, "NaN"), Json::Null);
        let mut o = options();
        o.decimal = DecimalHandlingMode::String;
        assert_eq!(convert(&col(Kind::Numeric, -1), Some("NaN"), &o).unwrap(), json!("NaN"));
        assert_eq!(convert(&col(Kind::Money, -1), Some("-$1,234.56"), &o).unwrap(), json!("-1234.56"));
    }

    #[test]
    fn dates_and_times() {
        assert_eq!(conv(Kind::Date, -1, "1970-01-02"), json!(1));
        assert_eq!(conv(Kind::Date, -1, "1969-12-31"), json!(-1));
        assert_eq!(conv(Kind::Date, -1, "infinity"), json!(i32::MAX));
        assert_eq!(conv(Kind::Date, -1, "0001-01-01 BC"), json!(-719_528));
        assert_eq!(conv(Kind::Time, -1, "10:15:30.5"), json!(36_930_500_000_i64));
        assert_eq!(conv(Kind::Time, -1, "24:00:00"), json!(86_400_000_000_i64));
        assert_eq!(conv(Kind::TimeTz, -1, "10:15:30.123+02"), json!("08:15:30.123Z"));
        assert_eq!(conv(Kind::TimeTz, -1, "01:00:00-03:30"), json!("04:30:00Z"));
        // timestamp(3) → millis, timestamp → micros.
        assert_eq!(conv(Kind::Timestamp, 3, "1970-01-01 00:00:01.5"), json!(1500));
        assert_eq!(conv(Kind::Timestamp, -1, "1970-01-01 00:00:01.5"), json!(1_500_000));
        assert_eq!(conv(Kind::Timestamp, -1, "infinity"), json!(i64::MAX));
        assert_eq!(
            conv(Kind::TimestampTz, -1, "2024-05-01 10:15:30.123456+00"),
            json!("2024-05-01T10:15:30.123456Z")
        );
        assert_eq!(
            conv(Kind::TimestampTz, -1, "2024-05-01 00:15:30+02"),
            json!("2024-04-30T22:15:30Z")
        );
        assert_eq!(conv(Kind::TimestampTz, -1, "-infinity"), json!("-infinity"));
    }

    #[test]
    fn intervals() {
        assert_eq!(conv(Kind::Interval, -1, "PT1.5S"), json!(1_500_000));
        assert_eq!(conv(Kind::Interval, -1, "P1D"), json!(86_400_000_000_i64));
        assert_eq!(conv(Kind::Interval, -1, "P-1DT-1H"), json!(-90_000_000_000_i64));
        assert_eq!(conv(Kind::Interval, -1, "P1M"), json!(2_629_800_000_000_i64));
        let mut o = options();
        o.interval = IntervalHandlingMode::String;
        assert_eq!(convert(&col(Kind::Interval, -1), Some("P1Y2M"), &o).unwrap(), json!("P1Y2M"));
    }

    #[test]
    fn bytes_bits_points() {
        assert_eq!(conv(Kind::Bytea, -1, "\\x0a0b"), json!("Cgs="));
        assert_eq!(conv(Kind::Bit, 1, "1"), json!(true));
        // 0b1000000001 (10 bits): bit 0 and bit 9 → bytes [0x01, 0x02].
        assert_eq!(conv(Kind::VarBit, -1, "1000000001"), json!("AQI="));
        assert_eq!(conv(Kind::Point, -1, "(1.5,-2)"), json!({"x": 1.5, "y": -2.0}));
    }

    #[test]
    fn arrays() {
        let ints = Kind::Array(Box::new(Kind::Int32));
        assert_eq!(conv(ints.clone(), -1, "{1,2,NULL}"), json!([1, 2, null]));
        assert_eq!(conv(ints.clone(), -1, "{{1,2},{3,4}}"), json!([[1, 2], [3, 4]]));
        assert_eq!(conv(ints.clone(), -1, "[0:1]={5,6}"), json!([5, 6]));
        assert_eq!(conv(ints, -1, "{}"), json!([]));
        let texts = Kind::Array(Box::new(Kind::Text));
        assert_eq!(
            conv(texts, -1, r#"{a,"b c","d\"e","NULL",NULL}"#),
            json!(["a", "b c", "d\"e", "NULL", null])
        );
        assert!(convert(&col(Kind::Array(Box::new(Kind::Int32)), -1), Some("{1,2"), &options()).is_err());
    }

    #[test]
    fn json_modes() {
        assert_eq!(conv(Kind::Jsonb, -1, r#"{"a": 1}"#), json!(r#"{"a": 1}"#));
        let mut o = options();
        o.json = JsonHandlingMode::Object;
        assert_eq!(convert(&col(Kind::Jsonb, -1), Some(r#"{"a": 1}"#), &o).unwrap(), json!({"a": 1}));
    }
}
