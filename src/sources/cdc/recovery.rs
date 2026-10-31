//! Offset recovery from the lake (`offset_recovery`).
//!
//! A source keeps its offset in its data directory. If that directory is lost (for example a
//! new container without the old volume), starting over would snapshot every table again and
//! number `seq` from 1 into a folder that already holds events with those numbers. Every
//! published offset carries the whole stored offset as `checkpoint`, so with `offset_recovery`
//! a source without a stored offset resumes from the offset it last published.
//!
//! It starts from scratch only when the connector's folder holds nothing but the lock blob.
//! Anything else it cannot account for stops it with an error, and a failed read is retried:
//! it is never taken for "nothing there".

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use azure_core::http::StatusCode;
use azure_storage_blob::{BlobContainerClient, models::BlobContainerClientListBlobsOptions};
use futures::StreamExt;
use serde_json::{Map, Value as Json};
use vector_common::decompression::CappedDecoder;
use vector_lib::configurable::configurable_component;

use super::lock::LockClient;
use crate::shutdown::ShutdownSignal;

const READ_RETRY: Duration = Duration::from_secs(5);

/// How many unexpected blob names an error message lists.
const LISTED_BLOBS: usize = 5;

/// Resume from the published offset when the stored offset is missing.
///
/// Reads the connector's folder through the `single_writer_lock` client (same account and
/// container), after the lock is taken.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct OffsetRecoveryConfig {
    /// The blob the `offsets` output overwrites with each published offset.
    #[configurable(metadata(docs::examples = "cdc/azure-mysql/_offsets/latest.json"))]
    pub latest_blob: String,

    /// The connector's folder. With no stored and no published offset, the source starts from
    /// scratch only if this folder holds nothing but the lock blob.
    #[configurable(metadata(docs::examples = "cdc/azure-mysql/"))]
    pub folder: String,

    /// Where the `control` output writes. Only needed to recover from a published offset
    /// written before published offsets carried `checkpoint`: the snapshotted tables are then
    /// rebuilt from the `snapshot_completed` events found here.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "cdc/azure-mysql/_control/"))]
    pub control_prefix: Option<String>,
}

impl OffsetRecoveryConfig {
    /// Checks the settings against the lock, whose client reads the lake (no I/O).
    ///
    /// # Errors
    ///
    /// Fails without `single_writer_lock`, or if `latest_blob` or `folder` is empty.
    pub fn build(&self, lock: Option<&LockClient>) -> Result<OffsetRecovery, String> {
        let lock = lock.ok_or(
            "`offset_recovery` needs `single_writer_lock`: the folder must not change while it is \
             read, and the lock's storage settings are used to read it",
        )?;
        let latest_blob = self.latest_blob.trim_start_matches('/').to_owned();
        if latest_blob.is_empty() {
            return Err("`offset_recovery.latest_blob` is empty".into());
        }
        let folder = folder_prefix(&self.folder)
            .ok_or("`offset_recovery.folder` is empty: it must be the connector's own folder")?;
        Ok(OffsetRecovery {
            container: lock.container(),
            ignored: vec![lock.blob_name().to_owned(), latest_blob.clone()],
            latest_blob,
            folder,
            control_prefix: self.control_prefix.as_deref().and_then(folder_prefix),
        })
    }
}

/// `"cdc/x"` → `"cdc/x/"`; `None` for an empty folder (the whole container).
fn folder_prefix(folder: &str) -> Option<String> {
    let folder = folder.trim_matches('/');
    (!folder.is_empty()).then(|| format!("{folder}/"))
}

/// What the lake says about where to start.
#[derive(Debug, PartialEq)]
pub enum Recovered {
    /// The connector's folder is empty: start from scratch.
    Empty,
    /// The last published offset.
    Published(Map<String, Json>),
}

enum Failure {
    /// The lake could not be read: try again.
    Retry(String),
    /// The lake was read, and the source must not start.
    Fatal(String),
}

/// A configured recovery: reads the connector's folder.
pub struct OffsetRecovery {
    container: Arc<BlobContainerClient>,
    latest_blob: String,
    folder: String,
    control_prefix: Option<String>,
    /// Blobs that may exist in an otherwise empty folder: the lock (and the published offset,
    /// which is read first anyway).
    ignored: Vec<String>,
}

impl OffsetRecovery {
    /// Reads the published offset, or checks that the folder is empty. Read errors are retried
    /// until the lake answers; `None` means shutdown was requested first.
    ///
    /// # Errors
    ///
    /// Fails if the published offset cannot be parsed, or if the folder holds data but no
    /// published offset.
    pub async fn recover(
        &self,
        connector: &str,
        shutdown: &mut ShutdownSignal,
    ) -> Result<Option<Recovered>, String> {
        loop {
            match self.read_lake(connector).await {
                Ok(recovered) => return Ok(Some(recovered)),
                Err(Failure::Fatal(error)) => return Err(error),
                Err(Failure::Retry(error)) => warn!(
                    message = "Could not read the lake to recover the offset; retrying.",
                    %error,
                ),
            }
            tokio::select! {
                _ = &mut *shutdown => return Ok(None),
                () = tokio::time::sleep(READ_RETRY) => {}
            }
        }
    }

    async fn read_lake(&self, connector: &str) -> Result<Recovered, Failure> {
        if let Some(bytes) = self
            .download(&self.latest_blob)
            .await
            .map_err(Failure::Retry)?
        {
            return parse_published(&bytes, connector)
                .map(Recovered::Published)
                .map_err(|error| Failure::Fatal(format!("{}: {error}", self.latest_blob)));
        }
        let names = self.list(&self.folder).await.map_err(Failure::Retry)?;
        let unexpected = unexpected_blobs(&names, &self.ignored);
        if unexpected.is_empty() {
            return Ok(Recovered::Empty);
        }
        Err(Failure::Fatal(format!(
            "there is no stored offset and no published offset ({}), but the connector's folder \
             {} is not empty ({} blob(s), for example {}). Starting from scratch would snapshot \
             every table again and reuse `seq` numbers already in the lake. Restore the data \
             directory, restore the published offset, or point the connector at a new folder",
            self.latest_blob,
            self.folder,
            unexpected.len(),
            unexpected
                .iter()
                .take(LISTED_BLOBS)
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
        )))
    }

    /// The tables with a `snapshot_completed` event of `connector` under `control_prefix`.
    /// `None` means shutdown was requested first.
    ///
    /// # Errors
    ///
    /// Fails if `control_prefix` is not set. Read errors are logged and retried.
    pub async fn snapshotted_tables(
        &self,
        connector: &str,
        shutdown: &mut ShutdownSignal,
    ) -> Result<Option<Vec<(String, String)>>, String> {
        let Some(prefix) = &self.control_prefix else {
            return Err(format!(
                "the published offset {} was written before published offsets carried \
                 `checkpoint`, so it does not say which tables are snapshotted. Set \
                 `offset_recovery.control_prefix` to rebuild that from the `snapshot_completed` \
                 events of the `control` output",
                self.latest_blob
            ));
        };
        loop {
            match self.scan_control(prefix, connector).await {
                Ok(tables) => return Ok(Some(tables)),
                Err(error) => warn!(
                    message = "Could not read control events to recover the snapshotted tables; retrying.",
                    %error,
                ),
            }
            tokio::select! {
                _ = &mut *shutdown => return Ok(None),
                () = tokio::time::sleep(READ_RETRY) => {}
            }
        }
    }

    async fn scan_control(
        &self,
        prefix: &str,
        connector: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let mut tables = BTreeSet::new();
        for name in self.list(prefix).await? {
            let Some(bytes) = self.download(&name).await? else {
                continue;
            };
            // Gzipped files are stored as written (by magic number: the name is the sink's choice).
            let text = if bytes.starts_with(&[0x1f, 0x8b]) {
                let decompressed = CappedDecoder::gzip(bytes.as_slice())
                    .decompress()
                    .map_err(|error| format!("failed to decompress {name}: {error}"))?;
                String::from_utf8_lossy(&decompressed).into_owned()
            } else {
                String::from_utf8_lossy(&bytes).into_owned()
            };
            tables.extend(completed_snapshots(&text, connector));
        }
        Ok(tables.into_iter().collect())
    }

    /// The blob's content; `None` if it does not exist.
    async fn download(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        match self.container.blob_client(name).download(None).await {
            Ok(response) => response
                .body
                .collect()
                .await
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| format!("failed to read {name}: {error}")),
            Err(error) if error.http_status() == Some(StatusCode::NotFound) => Ok(None),
            Err(error) => Err(format!("failed to read {name}: {error}")),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let options = BlobContainerClientListBlobsOptions {
            prefix: Some(prefix.to_owned()),
            ..Default::default()
        };
        let mut pager = self
            .container
            .list_blobs(Some(options))
            .map_err(|error| format!("failed to list {prefix}: {error}"))?;
        let mut names = Vec::new();
        while let Some(item) = pager.next().await {
            let item = item.map_err(|error| format!("failed to list {prefix}: {error}"))?;
            if let Some(name) = item.name {
                names.push(name);
            }
        }
        Ok(names)
    }
}

/// Parses the published offset blob (newline-delimited JSON; the last line wins).
fn parse_published(bytes: &[u8], connector: &str) -> Result<Map<String, Json>, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "not UTF-8 text".to_owned())?;
    let line = text
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or("the published offset is empty")?;
    let Json::Object(payload) = serde_json::from_str::<Json>(line)
        .map_err(|error| format!("the published offset is not valid JSON: {error}"))?
    else {
        return Err("the published offset is not a JSON object".into());
    };
    match payload.get("connector").and_then(Json::as_str) {
        Some(name) if name == connector => Ok(payload),
        Some(name) => Err(format!(
            "the published offset belongs to connector `{name}`, not `{connector}`"
        )),
        None => Err("the published offset has no `connector`".into()),
    }
}

/// Blobs other than the ignored ones (folder placeholders such as `x/` excluded).
fn unexpected_blobs(names: &[String], ignored: &[String]) -> Vec<String> {
    names
        .iter()
        .filter(|name| !name.ends_with('/') && !ignored.contains(name))
        .cloned()
        .collect()
}

/// `(db, table)` of the `snapshot_completed` events of `connector` in NDJSON text.
fn completed_snapshots(text: &str, connector: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| serde_json::from_str::<Json>(line).ok())
        .filter(|event| {
            event.get("type").and_then(Json::as_str) == Some("snapshot_completed")
                && event
                    .get("server_name")
                    .and_then(Json::as_str)
                    .is_none_or(|name| name == connector)
        })
        .filter_map(|event| {
            Some((
                event.get("db")?.as_str()?.to_owned(),
                event.get("table")?.as_str()?.to_owned(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_prefix_normalises() {
        assert_eq!(folder_prefix("cdc/shop"), Some("cdc/shop/".into()));
        assert_eq!(folder_prefix("/cdc/shop/"), Some("cdc/shop/".into()));
        assert_eq!(folder_prefix("/"), None);
    }

    #[test]
    fn published_offset_must_be_ours() {
        let line = br#"{"connector":"shop","seq":"00000000000000000007.0000000000"}"#;
        assert_eq!(
            parse_published(line, "shop").unwrap()["seq"],
            "00000000000000000007.0000000000"
        );
        assert!(
            parse_published(line, "orders")
                .unwrap_err()
                .contains("`shop`")
        );
        assert!(parse_published(b"\n\n", "shop").is_err());
        assert!(parse_published(b"[1]", "shop").is_err());
        assert!(parse_published(br#"{"seq":"1"}"#, "shop").is_err());
        // Several lines: the last one is the newest.
        let two = b"{\"connector\":\"shop\",\"change_seq\":1}\n{\"connector\":\"shop\",\"change_seq\":2}\n";
        assert_eq!(parse_published(two, "shop").unwrap()["change_seq"], 2);
    }

    #[test]
    fn only_the_lock_counts_as_empty() {
        let ignored = vec![
            "cdc/shop/_lock".to_owned(),
            "cdc/shop/_offsets/latest.json".to_owned(),
        ];
        let names = |n: &[&str]| n.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert!(unexpected_blobs(&names(&["cdc/shop/_lock"]), &ignored).is_empty());
        assert!(unexpected_blobs(&names(&["cdc/shop/", "cdc/shop/_lock"]), &ignored).is_empty());
        assert_eq!(
            unexpected_blobs(
                &names(&[
                    "cdc/shop/_lock",
                    "cdc/shop/_offsets/history/date=2026-10-29/a.json"
                ]),
                &ignored
            ),
            names(&["cdc/shop/_offsets/history/date=2026-10-29/a.json"])
        );
    }

    #[test]
    fn finds_completed_snapshots() {
        let text = [
            r#"{"type":"snapshot_completed","server_name":"shop","db":"shop","table":"orders"}"#,
            r#"{"type":"truncate","server_name":"shop","db":"shop","table":"items"}"#,
            r#"{"type":"snapshot_completed","server_name":"other","db":"shop","table":"x"}"#,
            "not json",
            r#"{"type":"snapshot_completed","db":"shop","table":"customers"}"#,
        ]
        .join("\n");
        assert_eq!(
            completed_snapshots(&text, "shop"),
            vec![
                ("shop".to_owned(), "orders".to_owned()),
                ("shop".to_owned(), "customers".to_owned())
            ]
        );
    }
}
