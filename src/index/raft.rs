use super::{ArkelRaftConfig, NodeId};
use axum::{
    Router,
    extract::State,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use openraft::{
    OptionalSend, Snapshot,
    errors::{NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable},
    network::RPCOption,
    network::RaftNetworkFactory,
    network::v2::RaftNetworkV2,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf},
};
use openraft_rt::WatchReceiver;
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;

use super::api::AppState;
use crate::ArkelIndexNode;

pub struct ArkelRaftNetworkFactory {
    client: reqwest::Client,
}

impl ArkelRaftNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl RaftNetworkFactory<ArkelRaftConfig> for ArkelRaftNetworkFactory {
    type Network = ArkelRaftNetwork;

    async fn new_client(&mut self, _target: NodeId, node: &ArkelIndexNode) -> Self::Network {
        let rpc_url = &node.params.rpc_url;
        ArkelRaftNetwork::new(self.client.clone(), rpc_url.clone())
    }
}

/// HTTP-based Raft RPC client.
pub struct ArkelRaftNetwork {
    client: reqwest::Client,
    peer_url: String,
}

impl ArkelRaftNetwork {
    pub fn new(client: reqwest::Client, peer_url: String) -> Self {
        Self { client, peer_url }
    }

    async fn request<Req, Resp>(
        &mut self,
        path: &str,
        req: Req,
    ) -> Result<Resp, RPCError<ArkelRaftConfig>>
    where
        Req: Serialize,
        Result<Resp, RaftError<ArkelRaftConfig>>: DeserializeOwned,
    {
        let url = format!("{}/{}", self.peer_url, path);

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    RPCError::Unreachable(Unreachable::new(&e))
                } else {
                    RPCError::Network(NetworkError::new(&e))
                }
            })?
            .error_for_status()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, RaftError<ArkelRaftConfig>> =
            resp.json().await.map_err(|e| NetworkError::new(&e))?;

        res.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

impl RaftNetworkV2<ArkelRaftConfig> for ArkelRaftNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<ArkelRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<ArkelRaftConfig>, RPCError<ArkelRaftConfig>> {
        self.request("raft/append-entries", req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<ArkelRaftConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ArkelRaftConfig>, RPCError<ArkelRaftConfig>> {
        self.request("raft/vote", req).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ArkelRaftConfig>,
        snapshot: SnapshotOf<ArkelRaftConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<ArkelRaftConfig>, StreamingError<ArkelRaftConfig>> {
        let req = (vote, snapshot.meta, snapshot.snapshot.into_inner());
        tokio::pin!(cancel);

        tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            res = self.request("raft/snapshot", req) => Ok(res?),
        }
    }
}

/// Shared application state used by Raft RPC handlers.
type RaftState = Arc<AppState>;

/// Axum router exposing the Raft RPC endpoints used between index nodes.
pub fn raft_router(state: RaftState) -> Router<RaftState> {
    Router::new()
        .route("/raft/append-entries", post(raft_append_entries))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        .route("/raft/metrics", get(raft_metrics))
        .with_state(state)
}

async fn raft_metrics(State(state): State<RaftState>) -> impl IntoResponse {
    let metrics = state.raft.metrics().borrow_watched().clone();
    Json(serde_json::json!({
        "id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_leader": metrics.current_leader,
    }))
}

async fn raft_append_entries(
    State(state): State<RaftState>,
    Json(req): Json<AppendEntriesRequest<ArkelRaftConfig>>,
) -> impl IntoResponse {
    Json(state.raft.append_entries(req).await)
}

async fn raft_vote(
    State(state): State<RaftState>,
    Json(req): Json<VoteRequest<ArkelRaftConfig>>,
) -> impl IntoResponse {
    Json(state.raft.vote(req).await)
}

async fn raft_snapshot(
    State(state): State<RaftState>,
    Json((vote, meta, data)): Json<(
        VoteOf<ArkelRaftConfig>,
        SnapshotMetaOf<ArkelRaftConfig>,
        Vec<u8>,
    )>,
) -> impl IntoResponse {
    let snapshot = Snapshot {
        meta,
        snapshot: Cursor::new(data),
    };
    let res: Result<SnapshotResponse<ArkelRaftConfig>, RaftError<ArkelRaftConfig>> = state
        .raft
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(RaftError::Fatal);
    Json(res)
}
