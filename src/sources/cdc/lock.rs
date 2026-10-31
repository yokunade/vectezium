//! Single-writer lock: a lease on an Azure blob, held while the source runs.
//!
//! Two instances capturing the same server (an overlapping redeploy, a platform move) would
//! race on the stored offset and on the published offsets. With `single_writer_lock`
//! configured, an instance takes the lease before it reads its stored offset, renews it while
//! it runs, and releases it after its last offset write; a second instance waits until then
//! (or until the lease expires, if the holder crashed).
//!
//! The holder stops writing offsets as soon as it cannot be sure it still holds the lease
//! (`safety_margin` before the lease could expire), and the source stops with an error.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use azure_core::http::{RequestContent, StatusCode, Url};
use azure_storage_blob::{
    BlobClient, BlobContainerClient,
    models::{BlobClientAcquireLeaseOptions, BlockBlobClientUploadOptions},
};
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::{
    config::ProxyConfig,
    shutdown::ShutdownSignal,
    sinks::{
        azure_blob::config::build_client,
        azure_common::{
            config::{AzureAuthentication, AzureBlobTlsConfig},
            connection_string::ParsedConnectionString,
        },
    },
};

const ACQUIRE_RETRY: Duration = Duration::from_secs(5);
const MIN_LEASE_SECS: u64 = 15;
const MAX_LEASE_SECS: u64 = 60;

/// A lease on an Azure blob that makes this source the only writer for its offsets.
///
/// Configure it on every instance that could capture the same server, with the same blob.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SingleWriterLockConfig {
    /// Azure authentication, as for the `azure_blob` sink (for example a managed identity).
    #[serde(default)]
    pub auth: Option<AzureAuthentication>,

    /// The storage account connection string (account key or SAS).
    #[configurable(metadata(docs::examples = "AccountName=mystorage"))]
    pub connection_string: Option<SensitiveString>,

    /// The storage account name; requires `auth`.
    #[configurable(metadata(docs::examples = "mystorage"))]
    pub account_name: Option<String>,

    /// The blob endpoint; requires `auth`.
    #[configurable(metadata(docs::examples = "https://mystorage.blob.core.windows.net/"))]
    pub blob_endpoint: Option<String>,

    /// The container holding the lock blob. It must exist.
    #[configurable(metadata(docs::examples = "raw"))]
    pub container_name: String,

    /// The lock blob, created empty if missing. Use one per captured server.
    #[configurable(metadata(docs::examples = "cdc/azure-mysql/_lock"))]
    pub blob_name: String,

    /// The lease duration (15 to 60 seconds). A crashed holder blocks a successor for at most
    /// this long; the lease is renewed every third of it.
    #[serde(default = "default_lease_duration_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub lease_duration_secs: u64,

    /// TLS options for the storage endpoint.
    pub tls: Option<AzureBlobTlsConfig>,
}

const fn default_lease_duration_secs() -> u64 {
    30
}

impl SingleWriterLockConfig {
    fn connection_string(&self) -> Result<String, String> {
        match (
            &self.connection_string,
            &self.account_name,
            &self.blob_endpoint,
        ) {
            (Some(connection_string), None, None) => Ok(connection_string.inner().to_owned()),
            (None, Some(account), None) if self.auth.is_some() => {
                Ok(format!("AccountName={account}"))
            }
            (None, None, Some(endpoint)) if self.auth.is_some() => {
                Ok(format!("BlobEndpoint={}/", endpoint.trim_end_matches('/')))
            }
            (None, Some(_), None) | (None, None, Some(_)) => Err(
                "`single_writer_lock.auth` is required with `account_name` or `blob_endpoint`"
                    .into(),
            ),
            _ => Err(
                "`single_writer_lock` needs exactly one of `connection_string`, \
                      `account_name` or `blob_endpoint`"
                    .into(),
            ),
        }
    }

    /// Checks the settings and builds the client (no I/O).
    ///
    /// # Errors
    ///
    /// Fails if the lease duration is out of range, the blob name is empty, or the storage
    /// settings do not name exactly one way to reach the account.
    pub fn build(&self, proxy: &ProxyConfig) -> Result<LockClient, String> {
        if !(MIN_LEASE_SECS..=MAX_LEASE_SECS).contains(&self.lease_duration_secs) {
            return Err(format!(
                "`single_writer_lock.lease_duration_secs` must be between {MIN_LEASE_SECS} and \
                 {MAX_LEASE_SECS}"
            ));
        }
        if self.blob_name.trim_matches('/').is_empty() {
            return Err("`single_writer_lock.blob_name` is empty".into());
        }
        let parsed = ParsedConnectionString::parse(&self.connection_string()?)
            .map_err(|error| format!("invalid `single_writer_lock` connection string: {error}"))?;
        let url = parsed
            .container_url(&self.container_name)
            .map_err(|error| format!("invalid `single_writer_lock` container: {error}"))?;
        let url = Url::parse(&url).map_err(|error| format!("invalid container URL: {error}"))?;
        let container = build_client(self.auth.as_ref(), &parsed, url, proxy, self.tls.as_ref())
            .map_err(|error| format!("failed to build the `single_writer_lock` client: {error}"))?;
        let blob_name = self.blob_name.trim_start_matches('/').to_owned();
        Ok(LockClient {
            blob: Arc::new(container.blob_client(&blob_name)),
            container,
            blob_name: blob_name.clone(),
            name: format!("{}/{blob_name}", self.container_name),
            duration: Duration::from_secs(self.lease_duration_secs),
        })
    }
}

/// A configured, not yet acquired lock.
pub struct LockClient {
    blob: Arc<BlobClient>,
    container: Arc<BlobContainerClient>,
    blob_name: String,
    name: String,
    duration: Duration,
}

impl LockClient {
    /// The container holding the lock blob, for reading the connector's other blobs
    /// (`offset_recovery`).
    #[must_use]
    pub fn container(&self) -> Arc<BlobContainerClient> {
        Arc::clone(&self.container)
    }

    /// The lock blob's name inside the container.
    #[must_use]
    pub fn blob_name(&self) -> &str {
        &self.blob_name
    }
}

/// Whether this instance may still write: the lease is held and cannot have expired yet.
#[derive(Clone)]
pub struct WriterGuard {
    state: Arc<LeaseState>,
}

struct LeaseState {
    /// Set when the lease is lost, released, or may have expired.
    fenced: AtomicBool,
    /// When the lease could expire, minus the safety margin (latest successful renewal).
    valid_until: std::sync::Mutex<Instant>,
}

impl WriterGuard {
    /// A guard that always allows writes (no lock configured).
    #[must_use]
    pub fn unlocked() -> Self {
        Self {
            state: Arc::new(LeaseState {
                fenced: AtomicBool::new(false),
                valid_until: std::sync::Mutex::new(Instant::now() + Duration::from_hours(87600)),
            }),
        }
    }

    /// # Panics
    ///
    /// Panics if the lease mutex was poisoned by a thread that panicked while holding it.
    #[must_use]
    pub fn may_write(&self) -> bool {
        !self.state.fenced.load(Ordering::SeqCst)
            && Instant::now() < *self.state.valid_until.lock().expect("lock poisoned")
    }

    fn fence(&self) {
        self.state.fenced.store(true, Ordering::SeqCst);
    }

    fn extend(&self, until: Instant) {
        *self.state.valid_until.lock().expect("lock poisoned") = until;
    }
}

/// A held lease, renewed in the background until released or lost.
pub struct HeldLock {
    blob: Arc<BlobClient>,
    name: String,
    lease_id: String,
    guard: WriterGuard,
    renewer: tokio::task::JoinHandle<()>,
}

fn safety_margin(duration: Duration) -> Duration {
    duration / 3
}

impl LockClient {
    /// Waits for the lease. Returns `None` if shutdown is requested first.
    ///
    /// # Errors
    ///
    /// Does not fail today: storage errors are logged and retried until the lease is taken
    /// or shutdown is requested.
    pub async fn acquire(self, mut shutdown: ShutdownSignal) -> Result<Option<HeldLock>, String> {
        let lease_id = uuid::Uuid::new_v4().to_string();
        let seconds = i32::try_from(self.duration.as_secs()).unwrap_or(60);
        let mut announced = false;
        loop {
            self.ensure_blob().await;
            let requested = Instant::now();
            let options = BlobClientAcquireLeaseOptions {
                proposed_lease_id: Some(lease_id.clone()),
                ..Default::default()
            };
            match self.blob.acquire_lease(seconds, Some(options)).await {
                Ok(_) => {
                    info!(
                        message = "Acquired the single-writer lock.",
                        lock = %self.name,
                        lease_id = %lease_id,
                    );
                    let guard = WriterGuard {
                        state: Arc::new(LeaseState {
                            fenced: AtomicBool::new(false),
                            valid_until: std::sync::Mutex::new(
                                (requested + self.duration)
                                    .checked_sub(safety_margin(self.duration))
                                    // Cannot underflow (the margin is a third of the duration);
                                    // if it did, treat the lease as already lapsed.
                                    .unwrap_or(requested),
                            ),
                        }),
                    };
                    let renewer = tokio::spawn(renew(
                        Arc::clone(&self.blob),
                        self.name.clone(),
                        lease_id.clone(),
                        self.duration,
                        guard.clone(),
                    ));
                    return Ok(Some(HeldLock {
                        blob: self.blob,
                        name: self.name,
                        lease_id,
                        guard,
                        renewer,
                    }));
                }
                Err(error) if error.http_status() == Some(StatusCode::Conflict) => {
                    if !announced {
                        warn!(
                            message = "Another instance holds the single-writer lock; waiting for \
                                       it to stop (or for its lease to expire).",
                            lock = %self.name,
                        );
                        announced = true;
                    }
                }
                Err(error) => {
                    warn!(
                        message = "Could not acquire the single-writer lock; retrying.",
                        lock = %self.name,
                        %error,
                    );
                }
            }
            tokio::select! {
                _ = &mut shutdown => return Ok(None),
                () = tokio::time::sleep(ACQUIRE_RETRY) => {}
            }
        }
    }

    /// Creates the lock blob if it does not exist (errors surface on the lease request).
    async fn ensure_blob(&self) {
        let options = BlockBlobClientUploadOptions::default().if_not_exists();
        if let Err(error) = self
            .blob
            .upload(RequestContent::from(Vec::<u8>::new()), Some(options))
            .await
            && !matches!(
                error.http_status(),
                Some(StatusCode::Conflict | StatusCode::PreconditionFailed)
            )
        {
            debug!(message = "Could not create the lock blob.", lock = %self.name, %error);
        }
    }
}

async fn renew(
    blob: Arc<BlobClient>,
    name: String,
    lease_id: String,
    duration: Duration,
    guard: WriterGuard,
) {
    let interval = duration / 3;
    let margin = safety_margin(duration);
    let mut next = Instant::now() + interval;
    loop {
        tokio::time::sleep_until(next.into()).await;
        if guard.state.fenced.load(Ordering::SeqCst) {
            return;
        }
        let requested = Instant::now();
        let result = tokio::time::timeout(interval, blob.renew_lease(lease_id.clone(), None)).await;
        match result {
            Ok(Ok(_)) => {
                guard.extend((requested + duration).checked_sub(margin).unwrap());
                next = requested + interval;
                continue;
            }
            Ok(Err(error))
                if matches!(
                    error.http_status(),
                    Some(StatusCode::Conflict | StatusCode::PreconditionFailed)
                ) =>
            {
                guard.fence();
                error!(
                    message = "Lost the single-writer lock (another instance holds it); \
                               stopping without writing further offsets.",
                    lock = %name,
                    %error,
                );
                return;
            }
            Ok(Err(error)) => {
                warn!(message = "Could not renew the single-writer lock; retrying.", lock = %name, %error);
            }
            Err(_) => {
                warn!(message = "Renewing the single-writer lock timed out; retrying.", lock = %name);
            }
        }
        if !guard.may_write() {
            guard.fence();
            error!(
                message = "Could not renew the single-writer lock before it could expire; \
                           stopping without writing further offsets.",
                lock = %name,
            );
            return;
        }
        next = Instant::now() + Duration::from_secs(2);
    }
}

impl HeldLock {
    #[must_use]
    pub fn guard(&self) -> WriterGuard {
        self.guard.clone()
    }

    /// Resolves once this instance may no longer write.
    pub async fn lost(&self) {
        while self.guard.may_write() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Stops writes, then releases the lease so a successor can start at once.
    pub async fn release(self) {
        let held = self.guard.may_write();
        self.guard.fence();
        self.renewer.abort();
        if !held {
            return;
        }
        match self.blob.release_lease(self.lease_id, None).await {
            Ok(_) => info!(message = "Released the single-writer lock.", lock = %self.name),
            Err(error) => warn!(
                message = "Could not release the single-writer lock; it expires on its own.",
                lock = %self.name,
                %error,
            ),
        }
    }
}

/// How long a stopping source waits for its last offset write before releasing the lock.
const LOCK_RELEASE_WAIT: Duration = Duration::from_secs(10);

/// Runs a source under the single-writer lock (if configured): takes the lock before `run`
/// starts (so before the stored offset is read), stops `run` if the lock is lost, and releases
/// the lock once the last offset is written (`run` fires the `oneshot` sender after its last
/// write; see `Checkpoints::start`).
///
/// # Errors
///
/// Returns the error of `run`, or fails if the lock is lost while `run` is running.
pub async fn run_locked<F, Fut>(
    lock: Option<LockClient>,
    shutdown: ShutdownSignal,
    run: F,
) -> Result<(), String>
where
    F: FnOnce(WriterGuard, tokio::sync::oneshot::Sender<()>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let Some(lock) = lock else {
        let (done, _) = tokio::sync::oneshot::channel();
        return run(WriterGuard::unlocked(), done).await;
    };
    let Some(held) = lock.acquire(shutdown).await? else {
        return Ok(());
    };
    let (done, written) = tokio::sync::oneshot::channel();
    let result = tokio::select! {
        result = run(held.guard(), done) => result,
        () = held.lost() => Err("lost the single-writer lock; another instance may be capturing \
                                 this server".to_owned()),
    };
    // Let the last acknowledged offset be stored, then release (which also fences writes).
    _ = tokio::time::timeout(LOCK_RELEASE_WAIT, written).await;
    held.release().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(duration: u64) -> SingleWriterLockConfig {
        SingleWriterLockConfig {
            auth: None,
            connection_string: Some(
                "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
                    .to_owned()
                    .into(),
            ),
            account_name: None,
            blob_endpoint: None,
            container_name: "raw".into(),
            blob_name: "cdc/x/_lock".into(),
            lease_duration_secs: duration,
            tls: None,
        }
    }

    #[test]
    fn validates_settings() {
        let proxy = ProxyConfig::default();
        assert!(config(30).build(&proxy).is_ok());
        assert!(config(10).build(&proxy).is_err());
        assert!(config(61).build(&proxy).is_err());
        let mut no_auth = config(30);
        no_auth.connection_string = None;
        no_auth.account_name = Some("lake".into());
        assert!(no_auth.build(&proxy).is_err());
        let mut empty = config(30);
        empty.blob_name = "/".into();
        assert!(empty.build(&proxy).is_err());
    }

    #[test]
    fn guard_fences_on_expiry_and_release() {
        let guard = WriterGuard::unlocked();
        assert!(guard.may_write());
        guard.extend(
            Instant::now()
                .checked_sub(Duration::from_millis(1))
                .unwrap(),
        );
        assert!(!guard.may_write(), "expired lease must stop writes");
        guard.extend(Instant::now() + Duration::from_secs(10));
        assert!(guard.may_write());
        guard.fence();
        assert!(!guard.may_write(), "fenced guard must stop writes");
    }
}
