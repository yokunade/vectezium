//! Incremental (chunked) snapshots, read while the slot streams: the read-only watermark
//! algorithm of Debezium's PostgreSQL connector (`DBLog`), with a transaction snapshot as the
//! watermark.
//!
//! Each chunk of a table is read in one short `REPEATABLE READ` transaction, in key order:
//!
//! * `S = pg_current_snapshot()`: the transaction's snapshot, which the chunk's rows reflect;
//! * `H = pg_current_wal_insert_lsn()`, read after `S`: every transaction visible in `S` has
//!   its commit record before `H`.
//!
//! The chunk is held (a "window") while the stream catches up to `H`. A captured change to one
//! of the chunk's keys from a transaction *not* visible in `S` is newer than the chunk's copy,
//! so that key is dropped from the chunk (the change itself is emitted as usual). Once every
//! transaction committed before `H` has been streamed, the remaining rows are emitted as
//! snapshot reads: every change older than them has been emitted, and every newer change
//! follows them. Windows only open between transactions, so no transaction is half-streamed
//! when one opens.
//!
//! Locks are only held for one chunk query at a time. Progress (the last key delivered) is
//! stored with each offset, so a restart resumes from the last delivered chunk.

use std::{collections::HashMap, str::FromStr};

use pgrepl_tokio_postgres::{Client, SimpleQueryMessage};
use serde_json::{Map, Value as Json};

use super::{
    catalog::{TableInfo, TypeRegistry, quote_ident},
    connection::error_text,
    offset::{Lsn, TableRef},
    source::{Settings, row_key, row_map},
};

/// A transaction snapshot (`pg_current_snapshot()`): `xmin:xmax:xip,…`, 64-bit transaction ids.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgSnapshot {
    xmin: u64,
    xmax: u64,
    xip: Vec<u64>,
}

impl FromStr for PgSnapshot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || format!("invalid snapshot `{s}`");
        let mut parts = s.split(':');
        let (Some(xmin), Some(xmax), Some(xip)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(bad());
        };
        let xip = xip
            .split(',')
            .filter(|x| !x.is_empty())
            .map(|x| x.parse().map_err(|_| bad()))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            xmin: xmin.parse().map_err(|_| bad())?,
            xmax: xmax.parse().map_err(|_| bad())?,
            xip,
        })
    }
}

impl PgSnapshot {
    /// Whether the (committed) transaction `xid` is visible in this snapshot: it committed
    /// before the snapshot was taken.
    pub fn visible(&self, xid: u32) -> bool {
        let full = widen(xid, self.xmax);
        full < self.xmin || (full < self.xmax && !self.xip.contains(&full))
    }
}

/// The 64-bit id of a 32-bit `xid` near `reference` (pgoutput sends 32-bit ids; snapshots have
/// 64-bit ones). Ids within 2^31 of the reference are unambiguous.
fn widen(xid: u32, reference: u64) -> u64 {
    const EPOCH: u64 = 1 << 32;
    const HALF: u64 = 1 << 31;
    let full = (reference & !(EPOCH - 1)) | u64::from(xid);
    if full > reference.saturating_add(HALF) {
        full.saturating_sub(EPOCH)
    } else if full.saturating_add(HALF) < reference {
        full + EPOCH
    } else {
        full
    }
}

/// One chunk, held until the stream reaches `high`.
pub struct Window {
    pub table: TableRef,
    pub info: TableInfo,
    pub snapshot: PgSnapshot,
    pub high: Lsn,
    /// Rows in key order; a dropped row is `None`.
    rows: Vec<Option<Map<String, Json>>>,
    /// Key (as JSON text) → index into `rows`.
    index: HashMap<String, usize>,
    /// SQL literals of the last key read (where the next chunk starts).
    pub last_key: Option<Vec<String>>,
    /// Fewer rows than a full chunk: the table is done once this window closes.
    pub complete: bool,
}

impl Window {
    /// Drops `key` (a change newer than the chunk touched it).
    pub fn drop_key(&mut self, key: &str) {
        if let Some(&i) = self.index.get(key) {
            self.rows[i] = None;
        }
    }

    /// Drops every row (the table was truncated after the chunk's snapshot).
    pub fn drop_all(&mut self) {
        self.rows.iter_mut().for_each(|r| *r = None);
    }

    /// The rows still to emit.
    pub fn into_rows(self) -> impl Iterator<Item = Map<String, Json>> {
        self.rows.into_iter().flatten()
    }
}

/// The key of a row as text, for matching chunk rows with streamed changes.
pub fn key_text(key: &Map<String, Json>) -> String {
    Json::Object(key.clone()).to_string()
}

/// What reading the next chunk of a table gave.
pub enum Chunk {
    Window(Box<Window>),
    /// The table no longer exists.
    Gone,
    /// The table has no key to read it in order by.
    NoKey,
    /// The stored position is not a key of the table (it changed, or the offset was altered).
    BadPosition,
}

/// Reads the next chunk of `table` after `last_key`.
#[allow(
    clippy::too_many_lines,
    reason = "builds and runs one chunk query as the watermark algorithm needs"
)]
pub async fn read_chunk(
    client: &Client,
    settings: &Settings,
    registry: &TypeRegistry,
    table: &TableRef,
    last_key: Option<&[String]>,
    chunk_size: usize,
) -> Result<Chunk, String> {
    let (schema, name) = table;
    let Some(info) = TableInfo::from_catalog(
        client,
        registry,
        &settings.filters,
        &settings.convert,
        schema,
        name,
    )
    .await?
    else {
        return Ok(Chunk::Gone);
    };
    if info.key.is_empty() {
        return Ok(Chunk::NoKey);
    }
    let qualified = format!("{}.{}", quote_ident(schema), quote_ident(name));
    let columns = info
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let key_columns = info
        .key
        .iter()
        .map(|&i| quote_ident(&info.columns[i].name))
        .collect::<Vec<_>>()
        .join(", ");
    // The position comes from the stored or recovered offset: parse it and quote it again, never
    // splice it in as it is. Untyped literals take the key columns' types in the row comparison.
    let after = match last_key {
        None => String::new(),
        Some(values) => {
            let literals = values
                .iter()
                .map(|value| parse_text_literal(value).map(|text| quote_literal(&text)))
                .collect::<Option<Vec<_>>>();
            match literals {
                Some(literals) if literals.len() == info.key.len() => {
                    format!(" WHERE ({key_columns}) > ({})", literals.join(", "))
                }
                _ => return Ok(Chunk::BadPosition),
            }
        }
    };
    let select = format!(
        "SELECT {columns} FROM {qualified}{after} ORDER BY {key_columns} LIMIT {chunk_size}"
    );

    let fail = |error: pgrepl_tokio_postgres::Error| {
        format!(
            "failed to read a chunk of {qualified}: {}",
            error_text(&error)
        )
    };
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(fail)?;
    let result = async {
        // The first statement fixes the transaction's snapshot, so it is the one the chunk
        // reads; the insert position, read after it, is past every commit the snapshot sees.
        let marks = client
            .query_one(
                "SELECT pg_current_snapshot()::text, pg_current_wal_insert_lsn()::text",
                &[],
            )
            .await
            .map_err(fail)?;
        let snapshot: PgSnapshot = marks.get::<_, String>(0).parse()?;
        let high: Lsn = marks.get::<_, String>(1).parse()?;
        let messages = client.simple_query(&select).await.map_err(fail)?;
        Ok::<_, String>((snapshot, high, messages))
    }
    .await;
    _ = client.batch_execute("COMMIT").await;
    let (snapshot, high, messages) = result?;

    let mut rows = Vec::new();
    let mut index = HashMap::new();
    let mut last = None;
    let mut fetched = 0;
    for message in messages {
        let SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        fetched += 1;
        last = Some(
            info.key
                .iter()
                .map(|&i| {
                    let text = row.get(i).unwrap_or_default();
                    format!("'{}'", text.replace('\'', "''"))
                })
                .collect::<Vec<_>>(),
        );
        let after = match row_map(settings, &info, |i| row.get(i)) {
            Ok(after) => after,
            Err(error) => {
                settings.handle_failure(error, schema, name)?;
                continue;
            }
        };
        if let Some(key) = row_key(&info, &after) {
            index.insert(key_text(&key), rows.len());
        }
        rows.push(Some(after));
    }
    let read = last.is_some();
    Ok(Chunk::Window(Box::new(Window {
        table: table.clone(),
        info,
        snapshot,
        high,
        rows,
        index,
        // A chunk with no rows leaves the position where it was.
        last_key: if read {
            last
        } else {
            last_key.map(<[String]>::to_vec)
        },
        complete: fetched < chunk_size,
    })))
}

/// Parses a stored chunk position: a key value's text in single quotes, with quotes doubled (as
/// `read_chunk` writes it). Anything else is `None`.
fn parse_text_literal(literal: &str) -> Option<String> {
    let inner = literal.strip_prefix('\'')?.strip_suffix('\'')?;
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\'' && chars.next()? != '\'' {
            return None;
        }
        out.push(c);
    }
    Some(out)
}

/// Quotes `text` as an escape string constant (`E'…'`), which reads the same whatever
/// `standard_conforming_strings` is set to.
fn quote_literal(text: &str) -> String {
    format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_positions_are_requoted() {
        for text in ["42", "it's", "back\\slash", "ünïcödé", "", "x') OR (1=1"] {
            let stored = format!("'{}'", text.replace('\'', "''"));
            assert_eq!(parse_text_literal(&stored).as_deref(), Some(text));
        }
        assert_eq!(quote_literal("a'b\\c"), "E'a''b\\\\c'");
        for stored in ["42", "'a'b'", "'unterminated", "'a') OR (1=1", "E'a'", ""] {
            assert_eq!(parse_text_literal(stored), None, "{stored}");
        }
    }

    #[test]
    fn parses_snapshots() {
        let s: PgSnapshot = "740:745:740,742".parse().unwrap();
        assert_eq!(
            s,
            PgSnapshot {
                xmin: 740,
                xmax: 745,
                xip: vec![740, 742]
            }
        );
        let s: PgSnapshot = "750:750:".parse().unwrap();
        assert!(s.xip.is_empty());
        assert!("750:750".parse::<PgSnapshot>().is_err());
    }

    #[test]
    fn visibility() {
        let s: PgSnapshot = "740:745:740,742".parse().unwrap();
        assert!(s.visible(739)); // committed before every running transaction
        assert!(!s.visible(740)); // running when the snapshot was taken
        assert!(s.visible(741)); // committed, between running ones
        assert!(!s.visible(742));
        assert!(!s.visible(745)); // started after the snapshot
        assert!(!s.visible(900));
    }

    #[test]
    #[allow(
        clippy::decimal_bitwise_operands,
        reason = "epochs and xids read best in decimal"
    )]
    fn widening_across_epochs() {
        let reference = (3_u64 << 32) | 10;
        assert_eq!(widen(5, reference), (3 << 32) | 5);
        assert_eq!(widen(20, reference), (3 << 32) | 20);
        // Just before the epoch boundary: the previous epoch.
        assert_eq!(
            widen(u32::MAX - 5, reference),
            (2 << 32) | u64::from(u32::MAX - 5)
        );
        let s = PgSnapshot {
            xmin: (3 << 32) | 2,
            xmax: (3 << 32) | 12,
            xip: vec![],
        };
        assert!(s.visible(u32::MAX - 5));
        assert!(!s.visible(12));
    }
}
