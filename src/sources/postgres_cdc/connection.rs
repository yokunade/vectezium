//! Connections: ordinary ones (catalog, snapshot) and replication ones, and a thin wrapper
//! around the replication stream. Only this file talks to the replication fork's stream API, so
//! swapping the fork (or replacing it) stays local.

use std::{pin::Pin, str::FromStr, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use pgrepl_openssl::MakeTlsConnector;
use pgrepl_tokio_postgres::{
    Client, Config, SimpleQueryMessage,
    config::{ReplicationMode, SslMode as PgSslMode},
    types::PgLsn,
};
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};

use super::{PostgresCdcConfig, SslMode, offset::Lsn};

/// Session settings that make every value print the same way in the stream and the snapshot.
const SESSION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=iso_8601 \
                               -c extra_float_digits=3 -c bytea_output=hex";

/// How to reach the server.
#[derive(Clone)]
pub struct ConnectionSettings {
    config: Config,
    tls: MakeTlsConnector,
    pub endpoint: String,
    pub connect_timeout: Duration,
}

impl ConnectionSettings {
    pub fn new(config: &PostgresCdcConfig) -> Result<Self, String> {
        let mut pg = Config::from_str("").map_err(|error| error.to_string())?;
        pg.host(&config.hostname)
            .port(config.port)
            .user(&config.user)
            .dbname(&config.database)
            .application_name("vectezium")
            .options(SESSION_OPTIONS)
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs.max(1)))
            .keepalives(true)
            .keepalives_idle(Duration::from_mins(1));
        if let Some(password) = &config.password {
            pg.password(password.inner());
        }
        pg.ssl_mode(match config.ssl_mode {
            SslMode::Disable => PgSslMode::Disable,
            _ => PgSslMode::Require,
        });

        let mut builder = SslConnector::builder(SslMethod::tls())
            .map_err(|error| format!("failed to set up TLS: {error}"))?;
        if let Some(ca) = &config.ssl_ca_file {
            builder.set_ca_file(ca).map_err(|error| {
                format!("failed to load `ssl_ca_file` {}: {error}", ca.display())
            })?;
        }
        if config.ssl_mode == SslMode::Require {
            builder.set_verify(SslVerifyMode::NONE);
        }
        let mut tls = MakeTlsConnector::new(builder.build());
        if matches!(config.ssl_mode, SslMode::Require | SslMode::VerifyCa) {
            tls.set_callback(|connect, _| {
                connect.set_verify_hostname(false);
                Ok(())
            });
        }
        Ok(Self {
            config: pg,
            tls,
            endpoint: format!("{}:{}/{}", config.hostname, config.port, config.database),
            connect_timeout: Duration::from_secs(config.connect_timeout_secs.max(1)),
        })
    }

    /// Opens a connection (a replication one with `replication`), its I/O driven on a task.
    pub async fn connect(&self, replication: bool) -> Result<Client, String> {
        let mut config = self.config.clone();
        if replication {
            config.replication_mode(ReplicationMode::Logical);
        }
        let connecting = config.connect(self.tls.clone());
        let (client, connection) =
            match tokio::time::timeout(self.connect_timeout, connecting).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(error)) => {
                    return Err(format!(
                        "failed to connect to {}: {}",
                        self.endpoint,
                        error_text(&error)
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "timed out after {:?} connecting to {}",
                        self.connect_timeout, self.endpoint
                    ));
                }
            };
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                debug!(message = "PostgreSQL connection ended.", %error);
            }
        });
        Ok(client)
    }
}

/// An error's SQLSTATE and message, or its text.
pub fn error_text(error: &pgrepl_tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{} (SQLSTATE {})", db.message(), db.code().code()),
        None => error.to_string(),
    }
}

/// The SQLSTATE of an error, if the server sent one.
pub fn sqlstate(error: &pgrepl_tokio_postgres::Error) -> Option<String> {
    error.code().map(|c| c.code().to_owned())
}

/// Runs a statement in the simple-query protocol (the only one replication connections speak)
/// and returns the text of every row.
pub async fn simple_rows(
    client: &Client,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, pgrepl_tokio_postgres::Error> {
    let mut rows = Vec::new();
    for message in client.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = message {
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            );
        }
    }
    Ok(rows)
}

/// What the server sent on the replication stream.
pub enum Received {
    /// A pgoutput message; `wal_end` is the server's WAL position when it was sent.
    Message {
        message: LogicalReplicationMessage,
        wal_end: Lsn,
    },
    /// A keepalive; `reply` asks for a status update right away.
    KeepAlive { wal_end: Lsn, reply: bool },
}

/// A started logical replication stream.
pub struct Replication {
    // Keeps the connection open for the stream's lifetime.
    _client: Client,
    stream: Pin<Box<LogicalReplicationStream>>,
}

/// `pg_current_wal_lsn`-style microseconds since 2000-01-01, the protocol's clock.
fn pg_now() -> i64 {
    chrono::Utc::now().timestamp_micros() - 946_684_800_000_000
}

impl Replication {
    /// `START_REPLICATION` on `slot` from `lsn`, streaming `publication` (pgoutput protocol 1,
    /// whole transactions only).
    pub async fn start(
        client: Client,
        slot: &str,
        lsn: Lsn,
        publication: &str,
    ) -> Result<Self, pgrepl_tokio_postgres::Error> {
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {lsn} (proto_version '1', publication_names '{}')",
            quote_ident_simple(slot),
            publication.replace('\'', "''"),
        );
        let duplex = client.copy_both_simple::<Bytes>(&query).await?;
        Ok(Self {
            _client: client,
            stream: Box::pin(LogicalReplicationStream::new(duplex)),
        })
    }

    /// The next message. `Ok(None)` means the server ended the stream (for example on
    /// `wal_sender_timeout`): a disconnect, never a clean end.
    pub async fn next(&mut self) -> Result<Option<Received>, pgrepl_tokio_postgres::Error> {
        match self.stream.next().await {
            None => Ok(None),
            Some(Err(error)) => Err(error),
            Some(Ok(ReplicationMessage::XLogData(body))) => {
                let wal_end = Lsn(body.wal_end());
                Ok(Some(Received::Message {
                    message: body.into_data(),
                    wal_end,
                }))
            }
            Some(Ok(ReplicationMessage::PrimaryKeepAlive(k))) => Ok(Some(Received::KeepAlive {
                wal_end: Lsn(k.wal_end()),
                reply: k.reply() == 1,
            })),
            Some(Ok(_)) => Ok(Some(Received::KeepAlive {
                wal_end: Lsn(0),
                reply: false,
            })),
        }
    }

    /// Reports what was received (`written`) and what the server may forget (`flushed`).
    pub async fn status(
        &mut self,
        written: Lsn,
        flushed: Lsn,
    ) -> Result<(), pgrepl_tokio_postgres::Error> {
        let (w, f) = (PgLsn::from(written.0), PgLsn::from(flushed.0));
        self.stream
            .as_mut()
            .standby_status_update(w, f, f, pg_now(), 0)
            .await
    }

    /// A status update that asks the server to answer with a keepalive right away (its current
    /// position), so a source waiting for the stream to reach a point need not wait for the next
    /// regular keepalive when nothing is being written.
    pub async fn request_keepalive(
        &mut self,
        written: Lsn,
        flushed: Lsn,
    ) -> Result<(), pgrepl_tokio_postgres::Error> {
        let (w, f) = (PgLsn::from(written.0), PgLsn::from(flushed.0));
        self.stream
            .as_mut()
            .standby_status_update(w, f, f, pg_now(), 1)
            .await
    }
}

/// Slot names are `[a-z0-9_]`, so quoting is only a guard.
fn quote_ident_simple(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
