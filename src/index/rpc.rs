use super::{ArkelRaftConfig, NodeId};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
    Router,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use crate::api::AppState;

/// Shared application state used by Raft RPC handlers.
type RaftState = Arc<AppState>;

/// HTTP-based Raft RPC client.
pub struct ArkelRaftConnection {
    client: reqwest::Client,
    peer_url: String,
}

impl ArkelRaftConnection {
    pub fn new(client: reqwest::Client, peer_url: String) -> Self {
        Self { client, peer_url }
    }

    async fn post_rpc<Req, Resp>(&self, path: &str, req: &Req) -> RpcResult<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.client
            .post(format!("{}/{}", self.peer_url, path))
            .json(req)
            .send()
            .await
            .map_err(rpc_err)?
            .json()
            .await
            .map_err(rpc_err)
    }
}

type RpcResult<T> = Result<
    T,
    openraft::error::RPCError<
        NodeId,
        openraft::impls::BasicNode,
        openraft::error::RaftError<NodeId>,
    >,
>;

fn rpc_err<E: std::fmt::Display>(
    e: E,
) -> openraft::error::RPCError<NodeId, openraft::impls::BasicNode, openraft::error::RaftError<NodeId>>
{
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(
        &std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    ))
}

fn peer_url_from_addr(addr: &str) -> String {
    let host = addr.split_once('@').map(|(_, h)| h).unwrap_or(addr);
    format!("http://{host}")
}

impl openraft::RaftNetwork<ArkelRaftConfig> for ArkelRaftConnection {
    async fn append_entries(
        &mut self,
        req: openraft::raft::AppendEntriesRequest<ArkelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        self.post_rpc("raft/append-entries", &req).await
    }

    async fn install_snapshot(
        &mut self,
        _req: openraft::raft::InstallSnapshotRequest<ArkelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Snapshot engines remain inactive under baseline layout",
        );
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&io_err),
        ))
    }

    async fn vote(
        &mut self,
        req: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::impls::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        self.post_rpc("raft/vote", &req).await
    }
}

pub struct ArkelRaftNetwork {
    client: reqwest::Client,
}

impl ArkelRaftNetwork {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl openraft::RaftNetworkFactory<ArkelRaftConfig> for ArkelRaftNetwork {
    type Network = ArkelRaftConnection;

    async fn new_client(
        &mut self,
        _target: NodeId,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        ArkelRaftConnection::new(self.client.clone(), peer_url_from_addr(&node.addr))
    }
}

/// Axum router exposing the Raft RPC endpoints used between index nodes.
pub fn raft_router(state: RaftState) -> Router<RaftState> {
    Router::new()
        .route("/raft/append-entries", post(raft_append_entries))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        .with_state(state)
}

async fn raft_append_entries(
    State(state): State<RaftState>,
    Json(req): Json<openraft::raft::AppendEntriesRequest<ArkelRaftConfig>>,
) -> impl IntoResponse {
    match state.raft.append_entries(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => {
            tracing::error!("append_entries failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn raft_vote(
    State(state): State<RaftState>,
    Json(req): Json<openraft::raft::VoteRequest<NodeId>>,
) -> impl IntoResponse {
    match state.raft.vote(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => {
            tracing::error!("vote failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn raft_snapshot(
    State(state): State<RaftState>,
    Json(req): Json<openraft::raft::InstallSnapshotRequest<ArkelRaftConfig>>,
) -> impl IntoResponse {
    match state.raft.install_snapshot(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => {
            tracing::error!("install_snapshot failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
