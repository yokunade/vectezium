//! What the source knows about types and tables, read from the system catalogs.
//!
//! Row values arrive as text (pgoutput's text format, and the snapshot's text results), so a
//! column is converted by its type: [`TypeRegistry`] resolves type OIDs (including domains,
//! enums and arrays of user-defined types) to a [`Kind`]. [`TableInfo`] describes a captured
//! table: its columns in the order the server sends them, its key, and its schema version.

use std::collections::HashMap;

use pgrepl_tokio_postgres::Client;

use super::{convert::ConvertOptions, filter::Filters, table_schema};

/// How a column's text values are converted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind {
    Bool,
    Int16,
    Int32,
    Int64,
    /// `oid`, `xid`, …: unsigned 32-bit.
    Oid,
    Float32,
    Float64,
    Numeric,
    Money,
    Bytea,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
    Bit,
    VarBit,
    Point,
    Enum,
    /// An array of the element kind.
    Array(Box<Kind>),
    /// Everything else (text, varchar, inet, ranges, xml, …): the text as printed.
    Text,
}

/// One resolved type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeInfo {
    /// The type's name as `format_type` would print it without modifiers (`int4`, `_text`,
    /// `my_enum`), schema-qualified outside `pg_catalog`.
    pub name: String,
    pub kind: Kind,
}

/// A row of `pg_type`.
#[derive(Clone, Debug)]
struct PgType {
    name: String,
    schema: String,
    typtype: char,
    elem: u32,
    basetype: u32,
}

/// Type OIDs to kinds. Built-in types are known; user-defined ones (enums, domains, arrays of
/// them) are read from `pg_type`, and read again when an unknown OID shows up.
#[derive(Debug, Default)]
pub struct TypeRegistry {
    types: HashMap<u32, PgType>,
}

impl TypeRegistry {
    /// Reads every type of the database.
    pub async fn load(client: &Client) -> Result<Self, String> {
        let rows = client
            .query(
                "SELECT t.oid, t.typname, n.nspname, t.typtype::text, t.typelem, t.typbasetype \
                 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace",
                &[],
            )
            .await
            .map_err(|error| format!("failed to read pg_type: {error}"))?;
        let mut types = HashMap::with_capacity(rows.len());
        for row in rows {
            let oid: u32 = row.get(0);
            let typtype: String = row.get(3);
            types.insert(
                oid,
                PgType {
                    name: row.get(1),
                    schema: row.get(2),
                    typtype: typtype.chars().next().unwrap_or('b'),
                    elem: row.get(4),
                    basetype: row.get(5),
                },
            );
        }
        Ok(Self { types })
    }

    pub fn knows(&self, oid: u32) -> bool {
        self.types.contains_key(&oid) || builtin(oid).is_some()
    }

    /// The type of `oid`; unknown OIDs convert as text.
    pub fn resolve(&self, oid: u32) -> TypeInfo {
        self.resolve_depth(oid, 0)
    }

    fn resolve_depth(&self, oid: u32, depth: u8) -> TypeInfo {
        let name = self.name(oid);
        if depth > 8 {
            return TypeInfo {
                name,
                kind: Kind::Text,
            };
        }
        if let Some(kind) = builtin(oid) {
            return TypeInfo { name, kind };
        }
        let Some(t) = self.types.get(&oid) else {
            return TypeInfo {
                name,
                kind: Kind::Text,
            };
        };
        let kind = match t.typtype {
            // A domain converts like its base type.
            'd' => self.resolve_depth(t.basetype, depth + 1).kind,
            'e' => Kind::Enum,
            // An array type is a base type whose element is set and whose name starts with `_`.
            'b' if t.elem != 0 && t.name.starts_with('_') => {
                Kind::Array(Box::new(self.resolve_depth(t.elem, depth + 1).kind))
            }
            _ => Kind::Text,
        };
        TypeInfo { name, kind }
    }

    fn name(&self, oid: u32) -> String {
        match self.types.get(&oid) {
            Some(t) if t.schema == "pg_catalog" => t.name.clone(),
            Some(t) => format!("{}.{}", t.schema, t.name),
            None => builtin_name(oid).map_or_else(|| format!("oid:{oid}"), str::to_owned),
        }
    }
}

/// Built-in type OIDs (stable across versions, from `pg_type.dat`).
fn builtin(oid: u32) -> Option<Kind> {
    Some(match oid {
        16 => Kind::Bool,
        21 => Kind::Int16,
        23 => Kind::Int32,
        20 => Kind::Int64,
        26 | 28 | 5069 => Kind::Oid,
        700 => Kind::Float32,
        701 => Kind::Float64,
        1700 => Kind::Numeric,
        790 => Kind::Money,
        17 => Kind::Bytea,
        1082 => Kind::Date,
        1083 => Kind::Time,
        1266 => Kind::TimeTz,
        1114 => Kind::Timestamp,
        1184 => Kind::TimestampTz,
        1186 => Kind::Interval,
        2950 => Kind::Uuid,
        114 => Kind::Json,
        3802 => Kind::Jsonb,
        1560 => Kind::Bit,
        1562 => Kind::VarBit,
        600 => Kind::Point,
        // Arrays of built-in types.
        1000 => Kind::Array(Box::new(Kind::Bool)),
        1005 => Kind::Array(Box::new(Kind::Int16)),
        1007 => Kind::Array(Box::new(Kind::Int32)),
        1016 => Kind::Array(Box::new(Kind::Int64)),
        1021 => Kind::Array(Box::new(Kind::Float32)),
        1022 => Kind::Array(Box::new(Kind::Float64)),
        1231 => Kind::Array(Box::new(Kind::Numeric)),
        1001 => Kind::Array(Box::new(Kind::Bytea)),
        1182 => Kind::Array(Box::new(Kind::Date)),
        1115 => Kind::Array(Box::new(Kind::Timestamp)),
        1185 => Kind::Array(Box::new(Kind::TimestampTz)),
        2951 => Kind::Array(Box::new(Kind::Uuid)),
        199 => Kind::Array(Box::new(Kind::Json)),
        3807 => Kind::Array(Box::new(Kind::Jsonb)),
        1009 | 1015 | 1014 | 1002 | 1003 | 1041 | 651 | 1040 => Kind::Array(Box::new(Kind::Text)),
        _ => return None,
    })
}

const fn builtin_name(oid: u32) -> Option<&'static str> {
    Some(match oid {
        16 => "bool",
        21 => "int2",
        23 => "int4",
        20 => "int8",
        26 => "oid",
        700 => "float4",
        701 => "float8",
        1700 => "numeric",
        790 => "money",
        17 => "bytea",
        25 => "text",
        1043 => "varchar",
        1042 => "bpchar",
        1082 => "date",
        1083 => "time",
        1266 => "timetz",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        2950 => "uuid",
        114 => "json",
        3802 => "jsonb",
        _ => return None,
    })
}

/// One column of a captured table.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    /// 1-based position among the columns the server sends.
    pub position: usize,
    pub type_oid: u32,
    pub typmod: i32,
    pub ty: TypeInfo,
    pub nullable: bool,
    /// Position in the primary key (1-based), if part of it.
    pub primary_key: Option<usize>,
    /// Omitted from events (`column_exclude_list`).
    pub excluded: bool,
}

impl Column {
    /// `(precision, scale)` of `numeric(p, s)`.
    pub fn numeric_precision_scale(&self) -> Option<(i32, i32)> {
        (self.ty.kind == Kind::Numeric && self.typmod >= 4).then(|| {
            let t = self.typmod - 4;
            // The scale is an 11-bit signed number since PostgreSQL 15.
            let scale = ((t & 0x7ff) ^ 0x400) - 0x400;
            ((t >> 16) & 0xffff, scale)
        })
    }

    /// Fractional-second digits of `time(p)`, `timestamp(p)`, … (6 when not declared).
    pub fn time_precision(&self) -> i32 {
        if self.typmod >= 0 {
            self.typmod.min(6)
        } else {
            6
        }
    }

    /// The declared length of `bit(n)` / `varbit(n)`.
    pub fn bit_length(&self) -> Option<i32> {
        (matches!(self.ty.kind, Kind::Bit | Kind::VarBit) && self.typmod > 0).then_some(self.typmod)
    }
}

/// A captured table as the source currently understands it.
#[derive(Clone, Debug)]
pub struct TableInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<Column>,
    /// Indexes into `columns` of the key (the primary key, or `pk_override`).
    pub key: Vec<usize>,
    /// Indexes into `columns` of the replica identity: the columns an old-key tuple carries.
    pub identity: Vec<usize>,
    /// `pg_class.relreplident`: `d` (default), `f` (full), `i` (index), `n` (nothing).
    pub replica_identity: char,
    pub schema_core: serde_json::Map<String, serde_json::Value>,
    pub schema_version: String,
}

/// What the catalog says about one column.
struct CatalogColumn {
    name: String,
    type_oid: u32,
    typmod: i32,
    not_null: bool,
    pk_position: Option<usize>,
}

/// The catalog's view of a table: OID, replica identity and columns (in `attnum` order).
async fn read_table(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<(u32, char, Vec<CatalogColumn>)>, String> {
    let rows = client
        .query(
            "SELECT c.oid, c.relreplident::text, a.attname, a.atttypid, a.atttypmod, a.attnotnull, \
                    array_position(i.indkey::int2[], a.attnum) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
             LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
             WHERE n.nspname = $1 AND c.relname = $2 \
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await
        .map_err(|error| format!("failed to read the columns of {schema}.{table}: {error}"))?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let relid: u32 = first.get(0);
    let identity: String = first.get(1);
    let columns = rows
        .iter()
        .map(|row| CatalogColumn {
            name: row.get(2),
            type_oid: row.get(3),
            typmod: row.get(4),
            not_null: row.get(5),
            pk_position: row
                .get::<_, Option<i32>>(6)
                .and_then(|p| usize::try_from(p).ok()),
        })
        .collect();
    Ok(Some((
        relid,
        identity.chars().next().unwrap_or('d'),
        columns,
    )))
}

/// A column as a replication `Relation` message describes it.
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    /// Part of the replica identity (the message's key flag): an old-key tuple carries it.
    pub identity: bool,
}

impl TableInfo {
    /// Reads a table from the catalog (for the snapshot). `None` if it does not exist.
    pub async fn from_catalog(
        client: &Client,
        registry: &TypeRegistry,
        filters: &Filters,
        convert: &ConvertOptions,
        schema: &str,
        table: &str,
    ) -> Result<Option<Self>, String> {
        let Some((_, identity, catalog)) = read_table(client, schema, table).await? else {
            return Ok(None);
        };
        let columns = catalog
            .into_iter()
            .map(|c| RelationColumn {
                name: c.name.clone(),
                type_oid: c.type_oid,
                typmod: c.typmod,
                // As the server flags it, except for an identity index (the stream's `Relation`
                // message says which columns that has).
                identity: identity == 'f' || (identity == 'd' && c.pk_position.is_some()),
            })
            .collect::<Vec<_>>();
        Self::from_relation(client, registry, filters, convert, schema, table, &columns)
            .await
            .map(|mut info| {
                info.replica_identity = identity;
                Some(info)
            })
    }

    /// Builds a table from a `Relation` message: the message decides the columns (it describes
    /// the tuples that follow); the catalog adds what the message does not carry (nullability,
    /// the primary key, the replica identity).
    #[allow(clippy::too_many_arguments)]
    pub async fn from_relation(
        client: &Client,
        registry: &TypeRegistry,
        filters: &Filters,
        convert: &ConvertOptions,
        schema: &str,
        table: &str,
        relation: &[RelationColumn],
    ) -> Result<Self, String> {
        let catalog = read_table(client, schema, table).await?;
        let (identity, by_name): (char, HashMap<String, CatalogColumn>) = match catalog {
            Some((_, identity, columns)) => (
                identity,
                columns.into_iter().map(|c| (c.name.clone(), c)).collect(),
            ),
            // Dropped since: describe it from the message alone.
            None => ('d', HashMap::new()),
        };
        let columns: Vec<Column> = relation
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let known = by_name.get(&c.name);
                Column {
                    name: c.name.clone(),
                    position: i + 1,
                    type_oid: c.type_oid,
                    typmod: c.typmod,
                    ty: registry.resolve(c.type_oid),
                    nullable: known.is_none_or(|k| !k.not_null),
                    primary_key: known.and_then(|k| k.pk_position),
                    excluded: filters.column_excluded(schema, table, &c.name),
                }
            })
            .collect();
        let key: Vec<usize> = if let Some(names) = filters.key_columns(schema, table) {
            names
                .iter()
                .filter_map(|n| columns.iter().position(|c| &c.name == n))
                .collect()
        } else {
            let mut pk: Vec<(usize, usize)> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| c.primary_key.map(|p| (p, i)))
                .collect();
            pk.sort_unstable();
            pk.into_iter().map(|(_, i)| i).collect()
        };
        let identity_columns = relation
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.identity.then_some(i))
            .collect();
        let schema_core = table_schema::core(schema, table, &columns, &key, convert);
        let schema_version = table_schema::version(&schema_core);
        Ok(Self {
            schema: schema.to_owned(),
            table: table.to_owned(),
            columns,
            key,
            identity: identity_columns,
            replica_identity: identity,
            schema_core,
            schema_version,
        })
    }

    /// Whether the columns (names and types) differ from `other`'s.
    pub fn columns_differ(&self, other: &Self) -> bool {
        self.columns.len() != other.columns.len()
            || self
                .columns
                .iter()
                .zip(&other.columns)
                .any(|(a, b)| a.name != b.name || a.type_oid != b.type_oid || a.typmod != b.typmod)
    }

    /// The key columns an old-key tuple does not carry (they are outside the replica identity):
    /// a delete, or a key change, cannot give their old values.
    pub fn key_outside_identity(&self) -> Vec<&str> {
        self.key
            .iter()
            .filter(|i| !self.identity.contains(i))
            .map(|&i| self.columns[i].name.as_str())
            .collect()
    }
}

/// The tables the publication streams that the filters capture, sorted.
pub async fn captured_tables(
    client: &Client,
    publication: &str,
    filters: &Filters,
) -> Result<Vec<(String, String)>, String> {
    let rows = client
        .query(
            "SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = $1 \
             ORDER BY 1, 2",
            &[&publication],
        )
        .await
        .map_err(|error| format!("failed to read the tables of publication `{publication}`: {error}"))?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .filter(|(schema, table)| filters.table_included(schema, table))
        .collect())
}

/// Ordinary and partitioned tables the filters capture (for creating the publication).
pub async fn filtered_tables(
    client: &Client,
    filters: &Filters,
) -> Result<Vec<(String, String)>, String> {
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
               AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .map_err(|error| format!("failed to list tables: {error}"))?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .filter(|(schema, table)| filters.table_included(schema, table))
        .collect())
}

/// Quotes an identifier for SQL.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(kind: Kind, typmod: i32) -> Column {
        Column {
            name: "c".into(),
            position: 1,
            type_oid: 0,
            typmod,
            ty: TypeInfo {
                name: "t".into(),
                kind,
            },
            nullable: true,
            primary_key: None,
            excluded: false,
        }
    }

    #[test]
    fn numeric_typmod() {
        // numeric(12, 2)
        let c = column(Kind::Numeric, ((12 << 16) | 2) + 4);
        assert_eq!(c.numeric_precision_scale(), Some((12, 2)));
        // numeric(5, -2) (PostgreSQL 15+)
        let c = column(Kind::Numeric, ((5 << 16) | (0x7ff & -2_i32)) + 4);
        assert_eq!(c.numeric_precision_scale(), Some((5, -2)));
        assert_eq!(column(Kind::Numeric, -1).numeric_precision_scale(), None);
    }

    #[test]
    fn builtin_arrays_and_names() {
        let registry = TypeRegistry::default();
        assert_eq!(
            registry.resolve(1007).kind,
            Kind::Array(Box::new(Kind::Int32))
        );
        assert_eq!(registry.resolve(25).kind, Kind::Text);
        assert_eq!(registry.resolve(1184).name, "timestamptz");
        assert_eq!(registry.resolve(99999).name, "oid:99999");
    }

    #[test]
    fn domains_and_enums() {
        let mut registry = TypeRegistry::default();
        let t = |name: &str, typtype, elem, basetype| PgType {
            name: name.into(),
            schema: "public".into(),
            typtype,
            elem,
            basetype,
        };
        registry.types.insert(50001, t("mood", 'e', 0, 0));
        registry.types.insert(50002, t("_mood", 'b', 50001, 0));
        registry.types.insert(50003, t("positive", 'd', 0, 1700));
        assert_eq!(registry.resolve(50001).kind, Kind::Enum);
        assert_eq!(registry.resolve(50001).name, "public.mood");
        assert_eq!(
            registry.resolve(50002).kind,
            Kind::Array(Box::new(Kind::Enum))
        );
        assert_eq!(registry.resolve(50003).kind, Kind::Numeric);
    }

    #[test]
    fn key_columns_outside_the_replica_identity() {
        let named = |name: &str| Column {
            name: name.into(),
            ..column(Kind::Int32, -1)
        };
        let info = |key: Vec<usize>, identity: Vec<usize>| TableInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![named("id"), named("tenant"), named("code")],
            key,
            identity,
            replica_identity: 'i',
            schema_core: serde_json::Map::new(),
            schema_version: String::new(),
        };
        // The primary key is the identity.
        assert!(info(vec![0], vec![0]).key_outside_identity().is_empty());
        // `pk_override` wider than the identity.
        assert_eq!(
            info(vec![0, 1], vec![0]).key_outside_identity(),
            vec!["tenant"]
        );
        // REPLICA IDENTITY USING INDEX on another unique column.
        assert_eq!(info(vec![0], vec![2]).key_outside_identity(), vec!["id"]);
        // No key: nothing to carry.
        assert!(info(vec![], vec![]).key_outside_identity().is_empty());
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("orders"), "\"orders\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }
}
