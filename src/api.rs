use crate::index::{ArkelRaftConfig, ArkelStore, IndexCommand, IndexResponse, ObjectMetadata};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, put},
    Router,
};
use openraft::raft::ClientWriteResponse;
use std::sync::Arc;

pub struct AppState {
    pub raft: openraft::Raft<ArkelRaftConfig>,
    pub store: ArkelStore,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(list_buckets))
        .route("/:bucket", put(create_bucket))
        .route("/:bucket/:key", put(put_object).get(get_object))
        .with_state(state)
}

pub async fn serve_index(
    listener: tokio::net::TcpListener,
    raft: openraft::Raft<ArkelRaftConfig>,
    store: ArkelStore,
) {
    let state = Arc::new(AppState { raft, store });
    let app = router(state);

    tracing::info!("Index HTTP API listening on {}", listener.local_addr().unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 0))));

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Index HTTP API server error: {}", e);
    }
}

async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let cmd = IndexCommand::CreateBucket { name: bucket };
    match state.raft.client_write(cmd).await {
        Ok(ClientWriteResponse { data, .. }) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            tracing::error!("client_write(CreateBucket) failed: {:?}", e);
            let resp = IndexResponse {
                success: false,
                error: Some(format!("{:?}", e)),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

async fn list_buckets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sm = state.store.state_machine.read().await;
    let buckets: Vec<String> = sm.buckets.keys().cloned().collect();
    Json(buckets)
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

    let cmd = IndexCommand::PutObject(meta);
    match state.raft.client_write(cmd).await {
        Ok(ClientWriteResponse { data, .. }) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            tracing::error!("client_write(PutObject) failed: {:?}", e);
            let resp = IndexResponse {
                success: false,
                error: Some(format!("{:?}", e)),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let sm = state.store.state_machine.read().await;
    match sm.objects.get(&(bucket.clone(), key.clone())) {
        Some(meta) => Json(meta).into_response(),
        None => (StatusCode::NOT_FOUND, Json(())).into_response(),
    }
}
