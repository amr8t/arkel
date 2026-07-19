use crate::index::{
    ArkelRaftConfig, ArkelStateMachine, IndexNodeRequest, IndexNodeResponse, ObjectMetadata,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, put},
};
use openraft::raft::ClientWriteResponse;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

pub struct BatchCollector {
    entries: Mutex<
        Vec<(
            IndexNodeRequest,
            tokio::sync::oneshot::Sender<IndexNodeResponse>,
        )>,
    >,
    notify: Notify,
    max_entries: usize,
    max_delay: Duration,
}

impl BatchCollector {
    pub fn new(max_entries: usize, max_delay_ms: u64) -> Self {
        Self {
            entries: Mutex::new(Vec::with_capacity(max_entries)),
            notify: Notify::new(),
            max_entries,
            max_delay: Duration::from_millis(max_delay_ms),
        }
    }

    pub async fn enqueue(
        &self,
        req: IndexNodeRequest,
    ) -> Result<IndexNodeResponse, tokio::sync::oneshot::error::RecvError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut entries = self.entries.lock().await;
            entries.push((req, tx));
            if entries.len() >= self.max_entries {
                self.notify.notify_one();
            }
        }
        self.notify.notify_one();
        rx.await
    }

    pub async fn take_batch(
        &self,
    ) -> Vec<(
        IndexNodeRequest,
        tokio::sync::oneshot::Sender<IndexNodeResponse>,
    )> {
        let mut entries = self.entries.lock().await;
        if !entries.is_empty() {
            return std::mem::take(&mut *entries);
        }
        drop(entries);

        let deadline = tokio::time::Instant::now() + self.max_delay;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let mut entries = self.entries.lock().await;
                return std::mem::take(&mut *entries);
            }
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {
                    let mut entries = self.entries.lock().await;
                    return std::mem::take(&mut *entries);
                }
                _ = self.notify.notified() => {
                    let mut entries = self.entries.lock().await;
                    if entries.len() >= self.max_entries {
                        return std::mem::take(&mut *entries);
                    }
                }
            }
        }
    }

    pub async fn flush_loop(
        self: Arc<Self>,
        raft: openraft::Raft<ArkelRaftConfig, ArkelStateMachine>,
    ) {
        loop {
            let batch = self.take_batch().await;
            if batch.is_empty() {
                continue;
            }

            let requests: Vec<IndexNodeRequest> =
                batch.iter().map(|(req, _)| req.clone()).collect();
            let senders: Vec<_> = batch.into_iter().map(|(_, tx)| tx).collect();

            let cmd = IndexNodeRequest::Batch(requests);
            match raft.client_write(cmd).await {
                Ok(ClientWriteResponse { data, .. }) => match data.batch_responses {
                    Some(responses) => {
                        for (tx, resp) in senders.into_iter().zip(responses.into_iter()) {
                            let _ = tx.send(resp);
                        }
                    }
                    None => {
                        for tx in senders {
                            let _ = tx.send(IndexNodeResponse::ok());
                        }
                    }
                },
                Err(e) => {
                    let err = IndexNodeResponse::err(&format!("raft batch error: {:?}", e));
                    for tx in senders {
                        let _ = tx.send(err.clone());
                    }
                }
            }
        }
    }
}

pub struct AppState {
    pub raft: openraft::Raft<ArkelRaftConfig, ArkelStateMachine>,
    pub state_machine: ArkelStateMachine,
    pub batch_collector: Arc<BatchCollector>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(list_buckets))
        .route("/:bucket", put(create_bucket).get(list_objects))
        .route("/:bucket/:key", put(put_object).get(get_object))
}

pub async fn serve_index(listener: tokio::net::TcpListener, app: Router) {
    tracing::info!(
        "Index HTTP API listening on {}",
        listener
            .local_addr()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
    );

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Index HTTP API server error: {}", e);
    }
}

async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let cmd = IndexNodeRequest::CreateBucket {
        name: bucket,
        created_at: now,
    };
    match state.batch_collector.enqueue(cmd).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            let resp = IndexNodeResponse::err(&format!("batch enqueue error: {}", e));
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

async fn list_buckets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.state_machine.list_buckets().await {
        Ok(names) => Json(names).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// S3-style object listing result. This JSON shape maps directly to the
/// `ListBucketResult` XML fields so we can flip to XML+query params later
/// without changing the wire semantics.
#[derive(Serialize)]
pub struct ListObjectsResponse {
    pub bucket: String,
    pub prefix: String,
    pub max_keys: usize,
    pub truncated: bool,
    pub contents: Vec<ListedObject>,
}

#[derive(Serialize)]
pub struct ListedObject {
    pub key: String,
    pub last_modified: u64,
    pub etag: String,
    pub size: u64,
}

async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let meta = ObjectMetadata {
        bucket: bucket.clone(),
        key: key.clone(),
        blob_hash: "test-hash".to_string(),
        etag: "test-etag".to_string(),
        size: body.len() as u64,
        content_type,
        version: None,
        storage_nodes: Vec::new(),
        created_at: now,
        updated_at: now,
    };

    let cmd = IndexNodeRequest::PutObject(meta);
    match state.batch_collector.enqueue(cmd).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            let resp = IndexNodeResponse::err(&format!("batch enqueue error: {}", e));
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.state_machine.get_object(&bucket, &key).await {
        Ok(Some(meta)) => Json(meta).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(())).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Read the requested prefix / pagination params now so the API contract
    // is already in place for S3 ListObjects compatibility.
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .or_else(|| params.get("max_keys"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    match state.state_machine.list_object_metadata(&bucket).await {
        Ok(metas) => {
            let mut contents: Vec<_> = metas
                .into_iter()
                .filter(|m| m.key.starts_with(&prefix))
                .map(|m| ListedObject {
                    key: m.key,
                    last_modified: m.updated_at,
                    etag: m.etag,
                    size: m.size,
                })
                .collect();

            // Simple key-based ordering; S3 compatible.
            contents.sort_by(|a, b| a.key.cmp(&b.key));

            let truncated = contents.len() > max_keys;
            contents.truncate(max_keys);

            Json(ListObjectsResponse {
                bucket,
                prefix,
                max_keys,
                truncated,
                contents,
            })
            .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
