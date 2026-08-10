use crate::index::{ArkelRaftConfig, ArkelStateMachine, IndexNodeRequest, IndexNodeResponse};
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post, put},
};
use openraft::raft::ClientWriteResponse;
use serde::{Deserialize, Serialize};
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
        .route(
            "/manifest/:bucket/:key",
            put(commit_manifest).get(read_manifest),
        )
        .route("/register", post(register_node))
        .route("/nodes", get(list_nodes))
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

async fn register_node(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RegisterNodePayload>,
) -> impl IntoResponse {
    let cmd = IndexNodeRequest::RegisterNode {
        node_id: payload.node_id,
        capacity_bytes: payload.capacity_bytes,
        addr: payload.addr,
        relay_url: payload.relay_url,
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

#[derive(Deserialize)]
pub struct CommitManifestPayload {
    pub object_hash: Vec<u8>,
    pub manifest_bytes: Vec<u8>,
    pub signature: Vec<u8>,
}

async fn commit_manifest(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Json(payload): Json<CommitManifestPayload>,
) -> impl IntoResponse {
    let cmd = IndexNodeRequest::CommitManifest {
        bucket,
        key,
        object_hash: payload.object_hash,
        manifest_bytes: payload.manifest_bytes,
        signature: payload.signature,
    };
    match state.batch_collector.enqueue(cmd).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            let resp = IndexNodeResponse::err(&format!("batch enqueue error: {}", e));
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct RegisterNodePayload {
    pub node_id: Vec<u8>,
    pub capacity_bytes: u64,
    pub addr: String,
    pub relay_url: Option<String>,
}

async fn read_manifest(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.state_machine.read_manifest(&bucket, &key).await {
        Ok(Some((manifest_bytes, signature))) => Json(serde_json::json!({
            "manifest_bytes": manifest_bytes,
            "signature": signature,
        }))
        .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({}))).into_response(),
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

    match state.state_machine.list_objects(&bucket).await {
        Ok(keys) => {
            let mut contents: Vec<_> = keys
                .into_iter()
                .filter(|k| k.starts_with(&prefix))
                .map(|k| ListedObject {
                    key: k,
                    last_modified: 0,
                    etag: String::new(),
                    size: 0,
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

#[derive(Serialize)]
pub struct RegisterNode {
    pub node_id: String,
    pub addr: String,
    pub relay_url: Option<String>,
}

async fn list_nodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.state_machine.get_healthy_nodes(64).await {
        Ok(nodes) => {
            let nodes: Vec<RegisterNode> = nodes
                .into_iter()
                .map(|(node_id, addr, relay_url)| RegisterNode {
                    node_id: hex::encode(node_id),
                    addr,
                    relay_url,
                })
                .collect();
            Json(nodes).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
