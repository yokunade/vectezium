//! Database, table and column filters, following Debezium's include/exclude list semantics.
//!
//! * `database_include_list` / `database_exclude_list` match the database name.
//! * `table_include_list` / `table_exclude_list` match `<database>.<table>`.
//! * `column_exclude_list` matches `<database>.<table>.<column>`.
//!
//! Every entry is a regular expression that must match the *whole* identifier. Include and
//! exclude lists of the same kind are mutually exclusive. The connector's system databases
//! ([`Filters::with_system_databases`]) are always excluded.
//!
//! "Database" is whatever the connector's first name level is (a `MySQL` database, a PostgreSQL or
//! SQL Server schema).

use std::{collections::HashMap, sync::Arc};

use regex::RegexSet;

#[derive(Clone, Debug, Default)]
pub struct Filters {
    database_include: Option<RegexSet>,
    database_exclude: Option<RegexSet>,
    table_include: Option<RegexSet>,
    table_exclude: Option<RegexSet>,
    column_exclude: Option<RegexSet>,
    /// Skip online schema-change shadow tables.
    exclude_osc_tables: bool,
    /// `<database>.<table>` → key columns replacing the primary key.
    key_columns: Arc<HashMap<String, Vec<String>>>,
    /// Databases whose changes are never captured.
    system_databases: &'static [&'static str],
}

/// Whether `table` is a shadow table of gh-ost (`_t_gho`, `_t_ghc`, `_t_del`) or
/// pt-online-schema-change (`_t_new`, `_t_old`).
#[must_use]
pub fn is_online_schema_change_table(table: &str) -> bool {
    let Some(rest) = table.strip_prefix('_') else {
        return false;
    };
    ["_gho", "_ghc", "_del", "_new", "_old"]
        .iter()
        .any(|suffix| rest.len() > suffix.len() && rest.ends_with(suffix))
}

fn build(patterns: &[String], what: &str) -> Result<Option<RegexSet>, String> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let anchored: Vec<String> = patterns.iter().map(|p| format!("^(?:{p})$")).collect();
    RegexSet::new(&anchored)
        .map(Some)
        .map_err(|error| format!("invalid regular expression in `{what}`: {error}"))
}

impl Filters {
    /// Builds the filters from the include and exclude lists (regular expressions, matched in
    /// full).
    ///
    /// # Errors
    ///
    /// Fails if both an include and an exclude list are set for databases or tables, or if a
    /// pattern is not a valid regular expression.
    pub fn new(
        database_include: &[String],
        database_exclude: &[String],
        table_include: &[String],
        table_exclude: &[String],
        column_exclude: &[String],
    ) -> Result<Self, String> {
        if !database_include.is_empty() && !database_exclude.is_empty() {
            return Err(
                "`database_include_list` and `database_exclude_list` cannot both be set".into(),
            );
        }
        if !table_include.is_empty() && !table_exclude.is_empty() {
            return Err("`table_include_list` and `table_exclude_list` cannot both be set".into());
        }
        Ok(Self {
            database_include: build(database_include, "database_include_list")?,
            database_exclude: build(database_exclude, "database_exclude_list")?,
            table_include: build(table_include, "table_include_list")?,
            table_exclude: build(table_exclude, "table_exclude_list")?,
            column_exclude: build(column_exclude, "column_exclude_list")?,
            exclude_osc_tables: false,
            key_columns: Arc::default(),
            system_databases: &[],
        })
    }

    /// Databases that are never captured, whatever the lists say (compared case-insensitively).
    #[must_use]
    pub const fn with_system_databases(mut self, databases: &'static [&'static str]) -> Self {
        self.system_databases = databases;
        self
    }

    /// Skips online schema-change shadow tables (see [`is_online_schema_change_table`]).
    #[must_use]
    pub const fn excluding_online_schema_change_tables(mut self, exclude: bool) -> Self {
        self.exclude_osc_tables = exclude;
        self
    }

    /// Sets key columns for tables (`<database>.<table>` → columns).
    #[must_use]
    pub fn with_key_columns(mut self, key_columns: HashMap<String, Vec<String>>) -> Self {
        self.key_columns = Arc::new(key_columns);
        self
    }

    /// The configured key columns of a table, if any.
    pub fn key_columns(&self, database: &str, table: &str) -> Option<&[String]> {
        self.key_columns
            .get(&format!("{database}.{table}"))
            .map(Vec::as_slice)
    }

    #[must_use]
    pub fn database_included(&self, database: &str) -> bool {
        if self
            .system_databases
            .iter()
            .any(|sys| sys.eq_ignore_ascii_case(database))
        {
            return false;
        }
        if let Some(include) = &self.database_include {
            return include.is_match(database);
        }
        if let Some(exclude) = &self.database_exclude {
            return !exclude.is_match(database);
        }
        true
    }

    #[must_use]
    pub fn table_included(&self, database: &str, table: &str) -> bool {
        if !self.database_included(database) {
            return false;
        }
        if self.exclude_osc_tables && is_online_schema_change_table(table) {
            return false;
        }
        let fqn = format!("{database}.{table}");
        if let Some(include) = &self.table_include {
            return include.is_match(&fqn);
        }
        if let Some(exclude) = &self.table_exclude {
            return !exclude.is_match(&fqn);
        }
        true
    }

    #[must_use]
    pub fn column_excluded(&self, database: &str, table: &str, column: &str) -> bool {
        self.column_exclude
            .as_ref()
            .is_some_and(|set| set.is_match(&format!("{database}.{table}.{column}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn system_databases_always_excluded() {
        let f = Filters::new(&s(&[".*"]), &[], &[], &[], &[])
            .unwrap()
            .with_system_databases(&["mysql", "sys"]);
        assert!(!f.database_included("mysql"));
        assert!(!f.database_included("MySQL"));
        assert!(!f.table_included("sys", "x"));
        assert!(f.database_included("app"));
    }

    #[test]
    fn include_lists_are_anchored() {
        let f = Filters::new(&s(&["app"]), &[], &s(&["app\\.orders.*"]), &[], &[]).unwrap();
        assert!(f.database_included("app"));
        assert!(!f.database_included("app2"));
        assert!(f.table_included("app", "orders"));
        assert!(f.table_included("app", "orders_archive"));
        assert!(!f.table_included("app", "customers"));
    }

    #[test]
    fn exclude_lists() {
        let f = Filters::new(&[], &s(&["tmp_.*"]), &[], &s(&["app\\.audit"]), &[]).unwrap();
        assert!(!f.database_included("tmp_x"));
        assert!(!f.table_included("app", "audit"));
        assert!(f.table_included("app", "audit_log"));
    }

    #[test]
    fn column_exclusion() {
        let f = Filters::new(&[], &[], &[], &[], &s(&["app\\.users\\.(password|ssn)"])).unwrap();
        assert!(f.column_excluded("app", "users", "password"));
        assert!(!f.column_excluded("app", "users", "email"));
    }

    #[test]
    fn online_schema_change_tables() {
        for t in [
            "_orders_gho",
            "_orders_ghc",
            "_orders_del",
            "_orders_new",
            "_orders_old",
        ] {
            assert!(is_online_schema_change_table(t), "{t}");
        }
        for t in ["orders", "orders_new", "_gho", "_new", "_orders"] {
            assert!(!is_online_schema_change_table(t), "{t}");
        }
        let f = Filters::new(&[], &[], &[], &[], &[])
            .unwrap()
            .excluding_online_schema_change_tables(true);
        assert!(!f.table_included("app", "_orders_gho"));
        assert!(f.table_included("app", "orders"));
        let f = Filters::new(&[], &[], &[], &[], &[]).unwrap();
        assert!(f.table_included("app", "_orders_gho"));
    }

    #[test]
    fn conflicting_lists_rejected() {
        assert!(Filters::new(&s(&["a"]), &s(&["b"]), &[], &[], &[]).is_err());
        assert!(Filters::new(&[], &[], &s(&["a.b"]), &s(&["a.c"]), &[]).is_err());
        assert!(Filters::new(&s(&["("]), &[], &[], &[], &[]).is_err());
    }
}
