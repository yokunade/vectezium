//! Column descriptions used to convert MySQL values into Debezium's representation.
//!
//! Descriptions come from two places:
//!
//! * binlog `TABLE_MAP` events, which with `binlog_row_metadata=FULL` carry column names,
//!   real types, type metadata (fractional-second precision, `BIT` length, …), and the string
//!   values of `ENUM` / `SET` columns *as of the moment the row was written*. Because every row
//!   event is self-describing, no separate schema-history store is needed to decode old events
//!   correctly after an `ALTER TABLE`;
//! * result-set metadata of the snapshot `SELECT`s.

use std::{collections::HashMap, io, sync::Arc};

use serde_json::{Map, Value as Json};

use mysql_async::{
    Column,
    binlog::events::{OptionalMetaExtractor, OptionalMetadataField, TableMapEvent},
    consts::{ColumnFlags, ColumnType},
};

use super::{
    convert::{ConvertOptions, decode_text},
    filter::Filters,
    table_schema,
};

/// MySQL's `binary` character set; columns using it hold raw bytes.
pub const BINARY_CHARSET: u16 = 63;

/// MySQL's `utf8mb4_0900_ai_ci` collation id.
pub const UTF8MB4_CHARSET: u16 = 255;

/// Normalized column kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColKind {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
    Bit,
    /// `CHAR`, `VARCHAR`, `TEXT`, `BINARY`, `VARBINARY`, `BLOB` (binary-ness from the charset).
    String,
    Enum,
    Set,
    Json,
    Geometry,
    Vector,
    Unknown,
}

impl ColKind {
    pub fn from_column_type(column_type: ColumnType) -> Self {
        use ColumnType::*;
        match column_type {
            MYSQL_TYPE_TINY => Self::TinyInt,
            MYSQL_TYPE_SHORT => Self::SmallInt,
            MYSQL_TYPE_INT24 => Self::MediumInt,
            MYSQL_TYPE_LONG => Self::Int,
            MYSQL_TYPE_LONGLONG => Self::BigInt,
            MYSQL_TYPE_FLOAT => Self::Float,
            MYSQL_TYPE_DOUBLE => Self::Double,
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => Self::Decimal,
            MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Self::Date,
            MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => Self::Time,
            MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => Self::DateTime,
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => Self::Timestamp,
            MYSQL_TYPE_YEAR => Self::Year,
            MYSQL_TYPE_BIT => Self::Bit,
            MYSQL_TYPE_VARCHAR
            | MYSQL_TYPE_VAR_STRING
            | MYSQL_TYPE_STRING
            | MYSQL_TYPE_BLOB
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB => Self::String,
            MYSQL_TYPE_ENUM => Self::Enum,
            MYSQL_TYPE_SET => Self::Set,
            MYSQL_TYPE_JSON => Self::Json,
            MYSQL_TYPE_GEOMETRY => Self::Geometry,
            MYSQL_TYPE_VECTOR => Self::Vector,
            _ => Self::Unknown,
        }
    }
}

/// Everything needed to convert one column's values.
#[derive(Clone, Debug)]
pub struct ColumnDesc {
    pub name: String,
    /// 1-based ordinal position in the table.
    pub position: u32,
    pub kind: ColKind,
    /// Normalized MySQL type name (`int`, `varchar`, `text`, `blob`, `datetime`, …).
    pub mysql_type: &'static str,
    pub nullable: bool,
    /// Only meaningful for integer, decimal and floating-point columns.
    pub unsigned: bool,
    /// `DECIMAL(precision, scale)`.
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    /// Character set id (`63` = binary). Only meaningful for [`ColKind::String`].
    pub charset: u16,
    /// Fractional-second precision for temporal types.
    pub fsp: u8,
    /// Declared length of `BIT(n)` columns.
    pub bit_len: u32,
    /// Allowed values of `ENUM` columns (index 1 is the first value).
    pub enum_values: Option<Arc<Vec<String>>>,
    /// Allowed values of `SET` columns (bit 0 is the first value).
    pub set_values: Option<Arc<Vec<String>>>,
    pub primary_key: bool,
    /// Excluded through `column_exclude_list`.
    pub excluded: bool,
}

impl ColumnDesc {
    pub fn is_binary(&self) -> bool {
        self.charset == BINARY_CHARSET
    }

    /// Whether `unsigned` is a meaningful attribute of this column's type.
    pub fn reports_unsigned(&self) -> bool {
        matches!(
            self.kind,
            ColKind::TinyInt
                | ColKind::SmallInt
                | ColKind::MediumInt
                | ColKind::Int
                | ColKind::BigInt
                | ColKind::Decimal
                | ColKind::Float
                | ColKind::Double
        )
    }
}

/// A captured table as described by the most recent `TABLE_MAP` event (or snapshot result set).
#[derive(Debug)]
pub struct TableInfo {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnDesc>,
    index_by_name: HashMap<String, usize>,
    /// The raw table map, needed to decode row images (`None` for snapshot tables).
    pub table_map: Option<TableMapEvent<'static>>,
    /// The hashed part of this table's schema document.
    pub schema_core: Arc<Map<String, Json>>,
    /// Identifier of this table definition; see [`table_schema`].
    pub schema_version: String,
}

impl TableInfo {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.index_by_name.get(name).copied()
    }

    pub fn has_primary_key(&self) -> bool {
        self.columns.iter().any(|c| c.primary_key)
    }

    fn new(
        database: String,
        table: String,
        columns: Vec<ColumnDesc>,
        table_map: Option<TableMapEvent<'static>>,
        convert: &ConvertOptions,
    ) -> Self {
        let index_by_name = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        let schema_core = table_schema::core(&database, &table, &columns, convert);
        let schema_version = table_schema::version(&schema_core);
        Self {
            database,
            table,
            columns,
            index_by_name,
            table_map,
            schema_core: Arc::new(schema_core),
            schema_version,
        }
    }

    /// Builds the description from a binlog `TABLE_MAP` event.
    ///
    /// Fails if the event lacks column names, i.e. the server is not running with
    /// `binlog_row_metadata=FULL`.
    pub fn from_table_map(
        tme: TableMapEvent<'static>,
        filters: &Filters,
        convert: &ConvertOptions,
    ) -> io::Result<Self> {
        let database = tme.database_name().into_owned();
        let table = tme.table_name().into_owned();
        let num_columns = tme.columns_count() as usize;

        let mut names: Vec<String> = Vec::new();
        let mut enum_lists: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut set_lists: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut primary_key: Vec<usize> = Vec::new();

        for field in tme.iter_optional_meta() {
            match field? {
                OptionalMetadataField::ColumnName(column_names) => {
                    for name in column_names.iter_names() {
                        names.push(name?.name().into_owned());
                    }
                }
                OptionalMetadataField::EnumStrValue(values) => {
                    for column in values.iter_values() {
                        let column = column?;
                        enum_lists.push(
                            column
                                .values()
                                .iter()
                                .map(|v| v.value_raw().to_vec())
                                .collect(),
                        );
                    }
                }
                OptionalMetadataField::SetStrValue(values) => {
                    for column in values.iter_values() {
                        let column = column?;
                        set_lists.push(
                            column
                                .values()
                                .iter()
                                .map(|v| v.value_raw().to_vec())
                                .collect(),
                        );
                    }
                }
                OptionalMetadataField::SimplePrimaryKey(keys) => {
                    for key in keys.iter_indexes() {
                        primary_key.push(key? as usize);
                    }
                }
                OptionalMetadataField::PrimaryKeyWithPrefix(keys) => {
                    for key in keys.iter_keys() {
                        primary_key.push(key?.column_index() as usize);
                    }
                }
                _ => {}
            }
        }

        if names.len() != num_columns {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "table map for `{database}`.`{table}` has no column names; the server must run \
                     with `binlog_row_metadata=FULL`"
                ),
            ));
        }

        let columns = {
            // Charsets cover character and ENUM/SET columns, each in column order — the same walk mysql_common does when decoding rows.
            let extractor = OptionalMetaExtractor::new(tme.iter_optional_meta())?;
            let mut charsets = extractor.iter_charset();
            let mut signedness = extractor.iter_signedness();
            let nullable_bits = tme.null_bitmask();
            let mut enum_set_charsets = extractor.iter_enum_and_set_charset();
            let mut enum_lists = enum_lists.into_iter();
            let mut set_lists = set_lists.into_iter();
            let mut columns = Vec::with_capacity(num_columns);

            for (i, name) in names.into_iter().enumerate() {
                let column_type = tme
                    .get_column_type(i)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing column type")
                    })?;
                let meta = tme.get_column_metadata(i).unwrap_or(&[]);
                let kind = ColKind::from_column_type(column_type);

                let (fsp, bit_len) = match column_type {
                    ColumnType::MYSQL_TYPE_TIME2
                    | ColumnType::MYSQL_TYPE_DATETIME2
                    | ColumnType::MYSQL_TYPE_TIMESTAMP2 => (meta.first().copied().unwrap_or(0), 0),
                    ColumnType::MYSQL_TYPE_BIT => {
                        let bits = u32::from(meta.first().copied().unwrap_or(0));
                        let bytes = u32::from(meta.get(1).copied().unwrap_or(0));
                        (0, bytes * 8 + bits)
                    }
                    _ => (0, 0),
                };
                let charset = if column_type.is_character_type() {
                    charsets.next().transpose()?.unwrap_or_default()
                } else if column_type.is_enum_or_set_type() {
                    enum_set_charsets.next().transpose()?.unwrap_or_default()
                } else {
                    0
                };

                let decode_values = |raw: Vec<Vec<u8>>| -> Arc<Vec<String>> {
                    Arc::new(raw.iter().map(|v| decode_text(v, charset)).collect())
                };
                let enum_values = (kind == ColKind::Enum)
                    .then(|| enum_lists.next().map(decode_values))
                    .flatten();
                let set_values = (kind == ColKind::Set)
                    .then(|| set_lists.next().map(decode_values))
                    .flatten();

                // Signedness is listed for every numeric column, in order.
                let unsigned = column_type.is_numeric_type() && signedness.next().unwrap_or(false);
                let nullable = nullable_bits.get(i).as_deref().copied().unwrap_or(true);
                let (precision, scale) = if kind == ColKind::Decimal {
                    (
                        meta.first().map(|p| u32::from(*p)),
                        meta.get(1).map(|s| u32::from(*s)),
                    )
                } else {
                    (None, None)
                };
                let binary = charset == BINARY_CHARSET;

                columns.push(ColumnDesc {
                    excluded: filters.column_excluded(&database, &table, &name),
                    name,
                    position: i as u32 + 1,
                    kind,
                    mysql_type: table_schema::mysql_type_from_binlog(kind, column_type, binary),
                    nullable,
                    unsigned,
                    precision,
                    scale,
                    charset,
                    fsp,
                    bit_len,
                    enum_values,
                    set_values,
                    primary_key: primary_key.contains(&i),
                });
            }
            columns
        };

        let mut columns = columns;
        apply_key_columns(&mut columns, filters, &database, &table);
        Ok(Self::new(database, table, columns, Some(tme), convert))
    }

    /// Builds the description from snapshot result-set metadata, enriched with
    /// `information_schema` details (enum/set values, primary key, bit length).
    pub fn from_result_columns(
        database: &str,
        table: &str,
        result_columns: &[Column],
        details: &HashMap<String, ColumnDetails>,
        filters: &Filters,
        convert: &ConvertOptions,
    ) -> Self {
        let columns = result_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let name = column.name_str().into_owned();
                let detail = details.get(&name);
                let flags = column.flags();
                let mut kind = ColKind::from_column_type(column.column_type());
                // The binary protocol reports ENUM and SET as strings with a flag.
                if flags.contains(ColumnFlags::ENUM_FLAG) {
                    kind = ColKind::Enum;
                } else if flags.contains(ColumnFlags::SET_FLAG) {
                    kind = ColKind::Set;
                }
                let mysql_type = detail.map_or("unknown", |d| {
                    table_schema::mysql_type_from_information_schema(&d.data_type)
                });
                ColumnDesc {
                    excluded: filters.column_excluded(database, table, &name),
                    position: detail.map_or(index as u32 + 1, |d| d.position),
                    kind,
                    mysql_type,
                    nullable: detail.is_none_or(|d| d.nullable),
                    unsigned: detail
                        .map_or(flags.contains(ColumnFlags::UNSIGNED_FLAG), |d| d.unsigned),
                    precision: detail.and_then(|d| d.precision),
                    scale: detail.and_then(|d| d.scale),
                    // The snapshot session uses `SET NAMES utf8mb4`, so the server has already
                    // converted every text value to UTF-8; only binary columns keep raw bytes.
                    charset: if column.character_set() == BINARY_CHARSET {
                        BINARY_CHARSET
                    } else {
                        UTF8MB4_CHARSET
                    },
                    fsp: detail.map_or_else(|| column.decimals(), |d| d.fsp),
                    bit_len: detail.map_or(column.column_length(), |d| d.bit_len),
                    enum_values: (kind == ColKind::Enum)
                        .then(|| detail.and_then(|d| d.values.clone()))
                        .flatten(),
                    set_values: (kind == ColKind::Set)
                        .then(|| detail.and_then(|d| d.values.clone()))
                        .flatten(),
                    primary_key: detail.is_some_and(|d| d.primary_key)
                        || flags.contains(ColumnFlags::PRI_KEY_FLAG),
                    name,
                }
            })
            .collect::<Vec<_>>();
        let mut columns = columns;
        apply_key_columns(&mut columns, filters, database, table);
        Self::new(
            database.to_owned(),
            table.to_owned(),
            columns,
            None,
            convert,
        )
    }
}

/// Replaces the primary key with the configured `pk_override`, if any.
fn apply_key_columns(columns: &mut [ColumnDesc], filters: &Filters, database: &str, table: &str) {
    if let Some(keys) = filters.key_columns(database, table) {
        for column in columns {
            column.primary_key = keys.iter().any(|k| k == &column.name);
        }
    }
}

/// Per-column details read from `information_schema.COLUMNS` during the snapshot.
#[derive(Clone, Debug, Default)]
pub struct ColumnDetails {
    pub position: u32,
    /// `DATA_TYPE`, e.g. `varchar`.
    pub data_type: String,
    pub nullable: bool,
    pub unsigned: bool,
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    pub fsp: u8,
    pub bit_len: u32,
    pub primary_key: bool,
    /// ENUM / SET values parsed from `COLUMN_TYPE`.
    pub values: Option<Arc<Vec<String>>>,
}

/// Parses `bit(N)` from `information_schema.COLUMNS.COLUMN_TYPE`.
pub fn parse_bit_len(column_type: &str) -> Option<u32> {
    let rest = column_type.trim().strip_prefix("bit(")?;
    rest.split(')').next()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_len_parsing() {
        assert_eq!(parse_bit_len("bit(1)"), Some(1));
        assert_eq!(parse_bit_len("bit(64)"), Some(64));
        assert_eq!(parse_bit_len("int(11)"), None);
    }

    #[test]
    fn kind_mapping() {
        assert_eq!(
            ColKind::from_column_type(ColumnType::MYSQL_TYPE_DATETIME2),
            ColKind::DateTime
        );
        assert_eq!(
            ColKind::from_column_type(ColumnType::MYSQL_TYPE_BLOB),
            ColKind::String
        );
    }
}
