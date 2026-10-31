//! Classification of statements logged as binlog query events.
//!
//! Outside a transaction, a query event is DDL, an account/administration statement, or (rarely)
//! a statement-format write. Only schema DDL is of interest downstream, and it must be filtered by
//! the database it *targets*, which is not always the session's default database
//! (`USE other; ALTER TABLE appdb.t ...`). This module reads just enough of each statement to tell
//! what kind of object it changes and which objects; it is not a SQL parser and errs on the side
//! of ignoring statements it does not recognise.

/// What a schema statement changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlKind {
    Table,
    Truncate,
    Index,
    View,
    Routine,
    Trigger,
    Event,
    Database,
}

impl DdlKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Truncate => "truncate",
            Self::Index => "index",
            Self::View => "view",
            Self::Routine => "routine",
            Self::Trigger => "trigger",
            Self::Event => "event",
            Self::Database => "database",
        }
    }

    /// Whether the statement's objects are tables, so table filters apply.
    pub const fn is_table_level(self) -> bool {
        matches!(
            self,
            Self::Table | Self::Truncate | Self::Index | Self::Trigger
        )
    }
}

/// An object named by a statement: `db` is `None` when the name is unqualified (the session's
/// default database applies) and `name` is `None` for statements on a database itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectName {
    pub db: Option<String>,
    pub name: Option<String>,
}

/// A recognised schema statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ddl {
    /// `CREATE`, `ALTER`, `DROP`, `RENAME` or `TRUNCATE`.
    pub verb: &'static str,
    pub kind: DdlKind,
    pub objects: Vec<ObjectName>,
    /// The statement removes or swaps rows without logging row events
    /// (`ALTER TABLE ... TRUNCATE/DROP/EXCHANGE PARTITION`).
    pub data_changed: bool,
}

impl Ddl {
    /// The `(database, table)` pairs this statement targets, resolving unqualified names against
    /// the session's default database. `table` is `None` for non-table objects.
    pub fn targets(&self, default_db: &str) -> Vec<(String, Option<String>)> {
        self.objects
            .iter()
            .map(|o| {
                let db = o.db.clone().unwrap_or_else(|| default_db.to_owned());
                let table = if self.kind.is_table_level() {
                    o.name.clone()
                } else {
                    None
                };
                (db, table)
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    /// An unquoted word: upper-cased for matching, and as written (identifiers can be
    /// case-sensitive).
    Word(String, String),
    /// A quoted identifier (backticks or double quotes), unescaped.
    Quoted(String),
    /// A string literal.
    Str,
    Punct(char),
}

fn tokenize(sql: &str, limit: usize) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() && tokens.len() < limit {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            // `/*!50001 ... */` (executable comment): keep its content; other comments: skip.
            if chars.get(i + 2) == Some(&'!') {
                i += 3;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            } else {
                i += 2;
                while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                    i += 1;
                }
                i += 2;
            }
        } else if c == '*' && chars.get(i + 1) == Some(&'/') {
            i += 2; // end of an executable comment
        } else if c == '#' || (c == '-' && chars.get(i + 1) == Some(&'-')) {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '`' || c == '"' {
            let quote = c;
            let mut s = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == quote {
                    if chars.get(i + 1) == Some(&quote) {
                        s.push(quote);
                        i += 2;
                        continue;
                    }
                    break;
                }
                s.push(chars[i]);
                i += 1;
            }
            i += 1;
            tokens.push(Token::Quoted(s));
        } else if c == '\'' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' {
                    i += 2;
                    continue;
                }
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            tokens.push(Token::Str);
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect();
            tokens.push(Token::Word(raw.to_ascii_uppercase(), raw));
        } else {
            tokens.push(Token::Punct(c));
            i += 1;
        }
    }
    tokens
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(sql: &str) -> Self {
        // Only the head of a statement matters; bound the work for huge statements.
        Self {
            tokens: tokenize(sql, 512),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn word(&self) -> Option<&str> {
        match self.peek() {
            Some(Token::Word(w, _)) => Some(w),
            _ => None,
        }
    }

    fn eat(&mut self, word: &str) -> bool {
        if self.word() == Some(word) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_punct(&mut self, c: char) -> bool {
        if self.peek() == Some(&Token::Punct(c)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Skips `= value` (for `ALGORITHM = MERGE` and similar).
    fn skip_assignment(&mut self) {
        self.eat_punct('=');
        self.pos += 1;
    }

    /// Skips `= user` in `DEFINER = user` (`'a'@'b'`, `` `a`@`b` ``, `a@b`, `CURRENT_USER[()]`).
    fn skip_definer(&mut self) {
        self.eat_punct('=');
        self.pos += 1;
        if self.eat_punct('(') {
            self.eat_punct(')');
        }
        if self.eat_punct('@') {
            self.pos += 1;
        }
    }

    fn skip_if_exists(&mut self) {
        let start = self.pos;
        if self.eat("IF") {
            self.eat("NOT");
            if !self.eat("EXISTS") {
                self.pos = start;
            }
        }
    }

    fn ident(&mut self) -> Option<String> {
        let value = match self.peek()? {
            Token::Quoted(s) => s.clone(),
            Token::Word(_, raw) => raw.clone(),
            _ => return None,
        };
        self.pos += 1;
        Some(value)
    }

    /// `name` or `db.name`.
    fn object_name(&mut self) -> Option<ObjectName> {
        let first = self.ident()?;
        if self.eat_punct('.') {
            let second = self.ident()?;
            Some(ObjectName {
                db: Some(first),
                name: Some(second),
            })
        } else {
            Some(ObjectName {
                db: None,
                name: Some(first),
            })
        }
    }

    /// `name [, name ...]`.
    fn name_list(&mut self) -> Vec<ObjectName> {
        let mut names = Vec::new();
        while let Some(name) = self.object_name() {
            names.push(name);
            if !self.eat_punct(',') {
                break;
            }
        }
        names
    }

    /// Whether the rest of an `ALTER TABLE` truncates, drops or exchanges partitions.
    fn partition_data_change(&self) -> bool {
        self.tokens[self.pos..].windows(2).any(|pair| {
            matches!(
                pair,
                [Token::Word(verb, _), Token::Word(what, _)]
                    if matches!(verb.as_str(), "TRUNCATE" | "DROP" | "EXCHANGE")
                        && (what == "PARTITION" || what == "SUBPARTITION")
            )
        })
    }

    /// Advances to the next top-level `ON` keyword and reads the table name after it.
    fn on_table(&mut self) -> Option<ObjectName> {
        while self.pos < self.tokens.len() {
            if self.eat("ON") {
                return self.object_name();
            }
            self.pos += 1;
        }
        None
    }
}

/// Classifies a statement. Returns `None` for anything that is not schema DDL on tables, views,
/// routines, triggers, events or databases: account and privilege statements, `FLUSH`,
/// `ANALYZE`/`OPTIMIZE`/`REPAIR TABLE`, tablespaces, servers, plugins, and so on.
#[allow(clippy::too_many_lines, reason = "one branch per DDL statement form")]
pub fn classify(sql: &str) -> Option<Ddl> {
    let mut p = Parser::new(sql);
    let verb = p.word()?.to_owned();
    p.pos += 1;
    match verb.as_str() {
        "CREATE" | "ALTER" | "DROP" => {
            // Modifiers that can precede the object kind.
            loop {
                match p.word() {
                    Some("OR") => {
                        p.pos += 1;
                        p.eat("REPLACE");
                    }
                    Some(
                        "TEMPORARY" | "UNIQUE" | "FULLTEXT" | "SPATIAL" | "ONLINE" | "OFFLINE"
                        | "IGNORE" | "AGGREGATE",
                    ) => p.pos += 1,
                    Some("ALGORITHM") => {
                        p.pos += 1;
                        p.skip_assignment();
                    }
                    Some("DEFINER") => {
                        p.pos += 1;
                        p.skip_definer();
                    }
                    Some("SQL") => {
                        p.pos += 1;
                        p.eat("SECURITY");
                        p.pos += 1;
                    }
                    _ => break,
                }
            }
            let kind_word = p.word()?.to_owned();
            p.pos += 1;
            let kind = match kind_word.as_str() {
                "TABLE" => DdlKind::Table,
                "INDEX" => DdlKind::Index,
                "VIEW" => DdlKind::View,
                "PROCEDURE" | "FUNCTION" => DdlKind::Routine,
                "TRIGGER" => DdlKind::Trigger,
                "EVENT" => DdlKind::Event,
                "DATABASE" | "SCHEMA" => DdlKind::Database,
                _ => return None,
            };
            p.skip_if_exists();
            let objects = match kind {
                // `ALTER DATABASE [name] CHARACTER SET ...`: without a name it applies to the
                // default database. Option keywords cannot be unquoted database names.
                DdlKind::Database
                    if matches!(
                        p.word(),
                        Some(
                            "CHARACTER" | "CHARSET" | "DEFAULT" | "COLLATE" | "ENCRYPTION" | "READ"
                        )
                    ) =>
                {
                    vec![ObjectName {
                        db: None,
                        name: None,
                    }]
                }
                DdlKind::Database => match p.ident() {
                    Some(db) => vec![ObjectName {
                        db: Some(db),
                        name: None,
                    }],
                    // `ALTER DATABASE CHARACTER SET ...` applies to the default database.
                    None => vec![ObjectName {
                        db: None,
                        name: None,
                    }],
                },
                DdlKind::Index => {
                    p.ident()?; // the index name
                    vec![p.on_table()?]
                }
                DdlKind::Trigger => {
                    let trigger = p.object_name()?;
                    if verb == "CREATE" {
                        let mut table = p.on_table()?;
                        // A trigger lives in its table's database.
                        if table.db.is_none() {
                            table.db = trigger.db;
                        }
                        vec![table]
                    } else {
                        // `DROP TRIGGER` names no table: filtered by database only.
                        vec![ObjectName {
                            db: trigger.db,
                            name: None,
                        }]
                    }
                }
                DdlKind::Table | DdlKind::View if verb == "DROP" => p.name_list(),
                _ => vec![p.object_name()?],
            };
            let data_changed =
                verb == "ALTER" && kind == DdlKind::Table && p.partition_data_change();
            let verb = match verb.as_str() {
                "CREATE" => "CREATE",
                "ALTER" => "ALTER",
                _ => "DROP",
            };
            (!objects.is_empty()).then_some(Ddl {
                verb,
                kind,
                objects,
                data_changed,
            })
        }
        "RENAME" => {
            if !(p.eat("TABLE") || p.eat("TABLES")) {
                return None; // RENAME USER
            }
            let mut objects = Vec::new();
            loop {
                objects.push(p.object_name()?);
                if !p.eat("TO") {
                    return None;
                }
                objects.push(p.object_name()?);
                if !p.eat_punct(',') {
                    break;
                }
            }
            Some(Ddl {
                verb: "RENAME",
                kind: DdlKind::Table,
                objects,
                data_changed: false,
            })
        }
        "TRUNCATE" => {
            p.eat("TABLE");
            Some(Ddl {
                verb: "TRUNCATE",
                kind: DdlKind::Truncate,
                objects: vec![p.object_name()?],
                data_changed: false,
            })
        }
        _ => None,
    }
}

/// For a data-changing statement (`INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `LOAD DATA`), returns
/// the table it writes to when that can be read from the statement (`Some(None)` when it cannot,
/// e.g. multi-table forms). Returns `None` for anything else.
///
/// With `binlog_format=ROW` such statements only appear in the binlog when a session switched
/// itself to statement format; their rows are then not in the binlog at all.
#[allow(
    clippy::option_option,
    reason = "not a data-changing statement, versus one whose target cannot be read"
)]
pub fn dml_target(sql: &str) -> Option<Option<ObjectName>> {
    let mut p = Parser::new(sql);
    let verb = p.word()?.to_owned();
    p.pos += 1;
    let skip_modifiers = |p: &mut Parser| {
        while matches!(
            p.word(),
            Some("LOW_PRIORITY" | "DELAYED" | "HIGH_PRIORITY" | "IGNORE" | "QUICK")
        ) {
            p.pos += 1;
        }
    };
    let target = match verb.as_str() {
        "INSERT" | "REPLACE" => {
            skip_modifiers(&mut p);
            p.eat("INTO");
            p.object_name()
        }
        "UPDATE" => {
            skip_modifiers(&mut p);
            let name = p.object_name();
            // `UPDATE a, b SET ...` and `UPDATE a JOIN b ...` write to more than one table.
            if p.eat("SET") { name } else { None }
        }
        "DELETE" => {
            skip_modifiers(&mut p);
            if p.eat("FROM") {
                let name = p.object_name();
                if matches!(
                    p.word(),
                    None | Some("WHERE" | "ORDER" | "LIMIT" | "PARTITION" | "AS")
                ) && p.peek() != Some(&Token::Punct(','))
                {
                    name
                } else {
                    None
                }
            } else {
                None // `DELETE a FROM a JOIN b ...`
            }
        }
        "LOAD" => {
            while p.pos < p.tokens.len() && !p.eat("TABLE") {
                p.pos += 1;
            }
            p.object_name()
        }
        _ => return None,
    };
    Some(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dml_targets() {
        let t = |db: Option<&str>, n: &str| {
            Some(Some(ObjectName {
                db: db.map(str::to_owned),
                name: Some(n.to_owned()),
            }))
        };
        assert_eq!(
            dml_target("INSERT INTO appdb.t VALUES (1)"),
            t(Some("appdb"), "t")
        );
        assert_eq!(dml_target("insert ignore t (a) values (1)"), t(None, "t"));
        assert_eq!(dml_target("REPLACE INTO `t` VALUES (1)"), t(None, "t"));
        assert_eq!(dml_target("UPDATE LOW_PRIORITY t SET a = 1"), t(None, "t"));
        assert_eq!(
            dml_target("DELETE FROM x.t WHERE id = 1"),
            t(Some("x"), "t")
        );
        assert_eq!(
            dml_target("LOAD DATA INFILE '/tmp/f' INTO TABLE t"),
            t(None, "t")
        );
        assert_eq!(
            dml_target("UPDATE a JOIN b ON a.id = b.id SET a.x = 1"),
            Some(None)
        );
        assert_eq!(
            dml_target("DELETE a FROM a JOIN b ON a.id = b.id"),
            Some(None)
        );
        assert_eq!(dml_target("SAVEPOINT s1"), None);
        assert_eq!(dml_target("ALTER TABLE t ADD c INT"), None);
    }

    fn on(db: Option<&str>, name: Option<&str>) -> ObjectName {
        ObjectName {
            db: db.map(str::to_owned),
            name: name.map(str::to_owned),
        }
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "compared with `kinds`, which returns an Option"
    )]
    fn ddl(kind: DdlKind, objects: Vec<ObjectName>) -> Option<(DdlKind, Vec<ObjectName>)> {
        Some((kind, objects))
    }

    /// `classify` without the verb and flags, for comparing kinds and objects.
    fn kinds(sql: &str) -> Option<(DdlKind, Vec<ObjectName>)> {
        classify(sql).map(|d| (d.kind, d.objects))
    }

    #[test]
    fn verbs() {
        assert_eq!(classify("CREATE TABLE t (a INT)").unwrap().verb, "CREATE");
        assert_eq!(classify("drop table t").unwrap().verb, "DROP");
        assert_eq!(classify("ALTER TABLE t ADD c INT").unwrap().verb, "ALTER");
        assert_eq!(classify("RENAME TABLE a TO b").unwrap().verb, "RENAME");
        assert_eq!(classify("TRUNCATE t").unwrap().verb, "TRUNCATE");
    }

    #[test]
    fn partition_data_changes() {
        for sql in [
            "ALTER TABLE pt TRUNCATE PARTITION p0",
            "ALTER TABLE app.pt DROP PARTITION p1, p2",
            "alter table pt exchange partition p2 with table pt_x",
        ] {
            assert!(classify(sql).unwrap().data_changed, "{sql}");
        }
        for sql in [
            "ALTER TABLE pt ADD PARTITION (PARTITION p9 VALUES LESS THAN (900))",
            "ALTER TABLE pt REORGANIZE PARTITION p0 INTO (PARTITION a VALUES LESS THAN (5))",
            "ALTER TABLE pt DROP COLUMN partition_key",
        ] {
            assert!(!classify(sql).unwrap().data_changed, "{sql}");
        }
    }

    #[test]
    fn tables() {
        assert_eq!(
            kinds("ALTER TABLE appdb.orders ADD COLUMN x INT"),
            ddl(DdlKind::Table, vec![on(Some("appdb"), Some("orders"))])
        );
        assert_eq!(
            kinds("create table if not exists `My``Db`.`T 1` (id int)"),
            ddl(DdlKind::Table, vec![on(Some("My`Db"), Some("T 1"))])
        );
        assert_eq!(
            kinds("CREATE TEMPORARY TABLE t (id INT)"),
            ddl(DdlKind::Table, vec![on(None, Some("t"))])
        );
        assert_eq!(
            kinds("DROP TABLE IF EXISTS `a`, other.b /* generated by server */"),
            ddl(
                DdlKind::Table,
                vec![on(None, Some("a")), on(Some("other"), Some("b"))]
            )
        );
        assert_eq!(
            kinds("RENAME TABLE a TO b, x.c TO y.c"),
            ddl(
                DdlKind::Table,
                vec![
                    on(None, Some("a")),
                    on(None, Some("b")),
                    on(Some("x"), Some("c")),
                    on(Some("y"), Some("c"))
                ]
            )
        );
        assert_eq!(
            kinds("TRUNCATE TABLE appdb.orders"),
            ddl(DdlKind::Truncate, vec![on(Some("appdb"), Some("orders"))])
        );
        assert_eq!(
            kinds("truncate orders"),
            ddl(DdlKind::Truncate, vec![on(None, Some("orders"))])
        );
        assert_eq!(
            kinds("/* app */ ALTER ONLINE TABLE Orders ENGINE=InnoDB"),
            ddl(DdlKind::Table, vec![on(None, Some("Orders"))])
        );
    }

    #[test]
    fn indexes_and_triggers() {
        assert_eq!(
            kinds("CREATE UNIQUE INDEX ix ON appdb.orders (a)"),
            ddl(DdlKind::Index, vec![on(Some("appdb"), Some("orders"))])
        );
        assert_eq!(
            kinds("DROP INDEX ix ON orders"),
            ddl(DdlKind::Index, vec![on(None, Some("orders"))])
        );
        assert_eq!(
            kinds(
                "CREATE DEFINER=`root`@`localhost` TRIGGER appdb.trg BEFORE INSERT ON orders \
                 FOR EACH ROW SET NEW.a = 1"
            ),
            ddl(DdlKind::Trigger, vec![on(Some("appdb"), Some("orders"))])
        );
        assert_eq!(
            kinds("DROP TRIGGER IF EXISTS appdb.trg"),
            ddl(DdlKind::Trigger, vec![on(Some("appdb"), None)])
        );
    }

    #[test]
    fn views_routines_events_databases() {
        assert_eq!(
            kinds(
                "CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `v` AS \
                 SELECT 1"
            ),
            ddl(DdlKind::View, vec![on(None, Some("v"))])
        );
        assert_eq!(
            kinds("/*!50001 CREATE OR REPLACE VIEW other.v AS SELECT 1 */"),
            ddl(DdlKind::View, vec![on(Some("other"), Some("v"))])
        );
        assert_eq!(
            kinds("CREATE DEFINER=CURRENT_USER() PROCEDURE appdb.p() BEGIN SELECT 1; END"),
            ddl(DdlKind::Routine, vec![on(Some("appdb"), Some("p"))])
        );
        assert_eq!(
            kinds("DROP FUNCTION IF EXISTS f"),
            ddl(DdlKind::Routine, vec![on(None, Some("f"))])
        );
        assert_eq!(
            kinds("ALTER EVENT appdb.e DISABLE"),
            ddl(DdlKind::Event, vec![on(Some("appdb"), Some("e"))])
        );
        assert_eq!(
            kinds("CREATE DATABASE IF NOT EXISTS appdb"),
            ddl(DdlKind::Database, vec![on(Some("appdb"), None)])
        );
        assert_eq!(
            kinds("ALTER DATABASE CHARACTER SET utf8mb4"),
            ddl(DdlKind::Database, vec![on(None, None)])
        );
    }

    #[test]
    fn ignored_statements() {
        for sql in [
            "CREATE USER 'app'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$xyz'",
            "ALTER USER 'app'@'%' IDENTIFIED BY 'secret'",
            "DROP USER 'app'@'%'",
            "RENAME USER a TO b",
            "GRANT SELECT ON appdb.* TO 'app'@'%'",
            "REVOKE ALL ON *.* FROM 'app'@'%'",
            "SET PASSWORD FOR 'app'@'%' = 'x'",
            "CREATE ROLE r",
            "FLUSH PRIVILEGES",
            "ANALYZE TABLE appdb.orders",
            "OPTIMIZE TABLE appdb.orders",
            "REPAIR TABLE appdb.orders",
            "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd'",
            "CREATE SERVER s FOREIGN DATA WRAPPER mysql OPTIONS (USER 'x')",
            "INSTALL PLUGIN p SONAME 'p.so'",
            "CREATE RESOURCE GROUP g TYPE = USER",
            "XA START 'x'",
            "SAVEPOINT s1",
            "INSERT INTO t VALUES (1)",
            "",
        ] {
            assert_eq!(kinds(sql), None, "{sql}");
        }
    }

    #[test]
    fn targets_resolve_the_default_database() {
        let d = classify("DROP TABLE a, other.b").unwrap();
        assert_eq!(
            d.targets("appdb"),
            vec![
                ("appdb".to_owned(), Some("a".to_owned())),
                ("other".to_owned(), Some("b".to_owned()))
            ]
        );
        let v = classify("CREATE VIEW v AS SELECT 1").unwrap();
        assert_eq!(v.targets("appdb"), vec![("appdb".to_owned(), None)]);
    }
}
