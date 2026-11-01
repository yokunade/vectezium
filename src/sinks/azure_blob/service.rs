use std::{
    cmp::Ordering,
    result::Result as StdResult,
    sync::Arc,
    task::{Context, Poll},
};

use azure_core::{
    error::ErrorKind,
    http::{RequestContent, StatusCode},
};
use azure_storage_blob::{
    AppendBlobClient, BlobClient, BlobContainerClient,
    models::{AppendBlobClientCreateOptions, BlockBlobClientUploadOptions, StorageErrorCode},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use serde_json::Value as Json;
use tower::Service;
use tracing::Instrument;

use crate::sinks::azure_blob::config::{AzureBlobRequest, AzureBlobResponse, AzureBlobType};

#[derive(Clone)]
pub struct AzureBlobService {
    // Using the new azure_storage_blob container client.
    client: Arc<BlobContainerClient>,
}

impl AzureBlobService {
    #[must_use]
    pub const fn new(client: Arc<BlobContainerClient>) -> AzureBlobService {
        AzureBlobService { client }
    }
}

impl Service<AzureBlobRequest> for AzureBlobService {
    type Response = AzureBlobResponse;
    type Error = Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    // Emission of an internal event in case of errors is handled upstream by the caller.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<StdResult<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // Emission of internal events for errors and dropped events is handled upstream by the caller.
    fn call(&mut self, request: AzureBlobRequest) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            let blob_client = this
                .client
                .blob_client(request.metadata.partition_key.as_str());
            let byte_size = request.blob_data.len();

            let order_by = request.overwrite_order_by.clone();
            let result = match (request.blob_type, order_by.as_deref()) {
                (AzureBlobType::Block, Some(order_by)) => {
                    upload_unless_older(
                        &blob_client,
                        order_by,
                        request.blob_data,
                        request.content_type,
                        request.content_encoding,
                        request.tags,
                        request.blob_metadata,
                    )
                    .await
                }
                (AzureBlobType::Block, None) => {
                    upload_block_blob(
                        &blob_client,
                        request.overwrite,
                        request.blob_data,
                        request.content_type,
                        request.content_encoding,
                        request.tags,
                        request.blob_metadata,
                    )
                    .await
                }
                (AzureBlobType::Append, _) => {
                    append_blob(
                        &blob_client.append_blob_client(),
                        request.blob_data,
                        request.content_type,
                        request.content_encoding,
                        request.tags,
                        request.blob_metadata,
                    )
                    .await
                }
            };

            result.map(|()| AzureBlobResponse {
                events_byte_size: request
                    .request_metadata
                    .into_events_estimated_json_encoded_byte_size(),
                byte_size,
            })
        })
    }
}

pub(super) async fn upload_block_blob(
    blob_client: &BlobClient,
    overwrite: bool,
    data: Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
    tags: Option<String>,
    blob_metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let upload_options = BlockBlobClientUploadOptions {
        blob_content_type: Some(content_type.to_string()),
        blob_content_encoding: content_encoding.map(str::to_string),
        blob_tags_string: tags,
        metadata: blob_metadata,
        ..Default::default()
    };
    // Unless overwriting is enabled, never replace an existing blob.
    let upload_options = if overwrite {
        upload_options
    } else {
        upload_options.if_not_exists()
    };

    match blob_client
        .upload(RequestContent::from(data.to_vec()), Some(upload_options))
        .instrument(info_span!("request").or_current())
        .await
    {
        Ok(_) => Ok(()),
        // An earlier attempt of this request may have landed although it timed out (a retry, by
        // the sink or by the SDK): the blob then already holds exactly these bytes.
        Err(error)
            if !overwrite
                && storage_error_code(&error).as_deref()
                    == Some(StorageErrorCode::BlobAlreadyExists.as_ref())
                && already_written(blob_client, &data).await =>
        {
            debug!(message = "The blob was already written by an earlier attempt of this request.");
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// How often [`upload_unless_older`] reads the blob again after another writer changed it.
const CONDITIONAL_ATTEMPTS: usize = 5;

/// Uploads `data` (a JSON object) unless the existing blob orders after it by the `order_by`
/// fields. The upload is conditional on the `ETag` of the blob that was compared (or on there
/// being none), so a concurrent write in between makes it compare again instead of replacing a
/// newer blob.
pub(super) async fn upload_unless_older(
    blob_client: &BlobClient,
    order_by: &[String],
    data: Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
    tags: Option<String>,
    blob_metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let new = order_key(&data, order_by).ok_or_else(|| {
        format!(
            "`blob_overwrite_order_by`: the blob is not one JSON object (set `batch.max_events: 1` \
             and a `json` codec) with the fields {order_by:?}"
        )
    })?;
    for _ in 0..CONDITIONAL_ATTEMPTS {
        let mut options = BlockBlobClientUploadOptions {
            blob_content_type: Some(content_type.to_string()),
            blob_content_encoding: content_encoding.map(str::to_string),
            blob_tags_string: tags.clone(),
            metadata: blob_metadata.clone(),
            ..Default::default()
        };
        match blob_client.download(None).await {
            Ok(existing) => {
                let etag = existing
                    .properties
                    .etag
                    .clone()
                    .ok_or("the existing blob has no ETag")?;
                let bytes = existing.body.collect().await?;
                // An existing blob that is not a JSON object with the fields is replaced.
                if let Some(old) = order_key(&bytes, order_by)
                    && compare(&new, &old) == Ordering::Less
                {
                    debug!(message = "Not replacing a blob that orders after this one.");
                    return Ok(());
                }
                options.if_match = Some(etag);
            }
            Err(error) if error.http_status() == Some(StatusCode::NotFound) => {
                options = options.if_not_exists();
            }
            Err(error) => return Err(error.into()),
        }
        match blob_client
            .upload(RequestContent::from(data.to_vec()), Some(options))
            .instrument(info_span!("request").or_current())
            .await
        {
            Ok(_) => return Ok(()),
            // Written meanwhile by another request: compare again.
            Err(error)
                if matches!(
                    error.http_status(),
                    Some(StatusCode::PreconditionFailed | StatusCode::Conflict)
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(
        format!("the blob kept changing while it was compared ({CONDITIONAL_ATTEMPTS} attempts)")
            .into(),
    )
}

/// The values of a JSON object's `fields`, for ordering; `None` if `bytes` is not one JSON
/// object.
fn order_key(bytes: &[u8], fields: &[String]) -> Option<Vec<Json>> {
    let Json::Object(object) = serde_json::from_slice::<Json>(bytes).ok()? else {
        return None;
    };
    Some(
        fields
            .iter()
            .map(|field| object.get(field).cloned().unwrap_or(Json::Null))
            .collect(),
    )
}

/// Compares order keys field by field: a missing field (or null) first, then numbers (as
/// numbers), then strings (as text); other values are equal to each other.
fn compare(a: &[Json], b: &[Json]) -> Ordering {
    const fn rank(value: &Json) -> u8 {
        match value {
            Json::Null => 0,
            Json::Number(_) => 1,
            Json::String(_) => 2,
            _ => 3,
        }
    }
    a.iter()
        .zip(b)
        .map(|(a, b)| match (a, b) {
            (Json::Number(a), Json::Number(b)) => a
                .as_f64()
                .partial_cmp(&b.as_f64())
                .unwrap_or(Ordering::Equal),
            (Json::String(a), Json::String(b)) => a.cmp(b),
            _ => rank(a).cmp(&rank(b)),
        })
        .find(|ordering| ordering.is_ne())
        .unwrap_or(Ordering::Equal)
}

/// Whether the existing blob holds exactly `data`. A blob with other content (a genuine name
/// collision) keeps the upload failing.
async fn already_written(blob_client: &BlobClient, data: &Bytes) -> bool {
    match blob_client.download(None).await {
        Ok(response) => response
            .body
            .collect()
            .await
            .is_ok_and(|existing| existing == *data),
        Err(_) => false,
    }
}

// Extracts the Azure storage error code string from an azure_core::Error, if present.
fn storage_error_code(e: &azure_core::Error) -> Option<String> {
    match e.kind() {
        ErrorKind::HttpResponse { error_code, .. } => error_code.clone(),
        _ => None,
    }
}

// Appends `data` to an existing append blob, creating the blob first if it doesn't exist.
// Uses the EAFP pattern: attempt append, create on 404, retry once.
// A 409 Conflict on create is swallowed — it means a concurrent writer created the blob first.
//
// Tags and metadata are applied at blob creation time only. Azure's `Set Blob Metadata` /
// `Set Blob Tags` are separate REST calls that operate on the whole blob (not per-append), and
// applying them from Vector on every append would race with concurrent creators and Azurite
// integration testing — so this sink treats them as one-time initialization data.
async fn append_blob(
    append_client: &AppendBlobClient,
    data: Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
    tags: Option<String>,
    blob_metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data_len = data.len() as u64;

    match append_client
        .append_block(RequestContent::from(data.to_vec()), data_len, None)
        .instrument(info_span!("request").or_current())
        .await
    {
        Ok(_) => return Ok(()),
        Err(e) => {
            let error_code = storage_error_code(&e);

            if e.http_status() == Some(StatusCode::NotFound) {
                if error_code.as_deref() == Some(StorageErrorCode::ContainerNotFound.as_ref()) {
                    // Container doesn't exist — this is a misconfiguration, not a missing blob.
                    // Propagate immediately; creating the blob would also fail.
                    warn!(
                        message = "Azure container not found when appending to blob. \
                        Verify the container exists and that `container_name` is correct.",
                    );
                    return Err(e.into());
                }
                // BlobNotFound (or unrecognised 404) — first write, fall through to create.
            } else {
                if error_code.as_deref() == Some(StorageErrorCode::BlockCountExceedsLimit.as_ref())
                {
                    warn!(
                        message = "Azure append blob has reached the 50,000-block limit. \
                        No further data can be appended to this blob until `blob_time_format` \
                        rolls over to a new name. Set a finer-grained `blob_time_format` (the \
                        default rotates hourly) so blobs rotate before the cap is reached, \
                        or delete the full blob manually.",
                    );
                }
                return Err(e.into());
            }
        }
    }

    let create_opts = AppendBlobClientCreateOptions {
        blob_content_type: Some(content_type.to_string()),
        blob_content_encoding: content_encoding.map(str::to_string),
        blob_tags_string: tags,
        metadata: blob_metadata,
        ..Default::default()
    }
    .if_not_exists();

    match append_client
        .create(Some(create_opts))
        .instrument(info_span!("request").or_current())
        .await
    {
        Ok(_) => {}
        Err(e) if e.http_status() == Some(StatusCode::Conflict) => {} // race: already created
        Err(e) => return Err(e.into()),
    }

    append_client
        .append_block(RequestContent::from(data.to_vec()), data_len, None)
        .instrument(info_span!("request").or_current())
        .await
        .map(|_| ())
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn key(value: &Json) -> Vec<Json> {
        order_key(value.to_string().as_bytes(), &["seq".into(), "at".into()]).unwrap()
    }

    #[test]
    fn orders_blobs_by_their_fields() {
        let older = key(&json!({"seq": "0002.0000", "at": "2026-01-01T00:00:01.000Z"}));
        let later_same_seq = key(&json!({"seq": "0002.0000", "at": "2026-01-01T00:00:02.000Z"}));
        let newer = key(&json!({"seq": "0010.0000", "at": "2026-01-01T00:00:00.000Z"}));
        assert_eq!(compare(&older, &later_same_seq), Ordering::Less);
        assert_eq!(compare(&newer, &later_same_seq), Ordering::Greater);
        assert_eq!(compare(&older, &older), Ordering::Equal);
        // Numbers compare as numbers, not as text.
        assert_eq!(
            compare(&key(&json!({"seq": 9})), &key(&json!({"seq": 10}))),
            Ordering::Less
        );
        // A missing field orders first.
        assert_eq!(
            compare(&key(&json!({"at": "x"})), &key(&json!({"seq": 1}))),
            Ordering::Less
        );
        // One JSON object per blob (a trailing newline is fine), nothing else.
        assert!(order_key(b"{\"seq\": 1}\n", &["seq".into()]).is_some());
        assert!(order_key(b"{\"seq\": 1}\n{\"seq\": 2}\n", &["seq".into()]).is_none());
        assert!(order_key(b"[1]", &["seq".into()]).is_none());
    }
}
