use serde::{Deserialize, Serialize};
use std::io::Cursor;

pub type NodeId = u64;

/// Metadata record for an object stored in the global catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub blob_hash: String,
    pub etag: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub version: Option<String>,
    pub storage_nodes: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// System modifications committed to the Raft consensus log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IndexCommand {
    PutObject(ObjectMetadata),
    DeleteObject { bucket: String, key: String },
    CreateBucket { name: String },
    DeleteBucket { name: String },
}

/// The response confirmation sent back to the control API.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexResponse {
    pub success: bool,
    pub error: Option<String>,
}

// Boilerplate mapping Arkel's types to OpenRaft v0.9
openraft::declare_raft_types!(
    pub ArkelRaftConfig:
        D = IndexCommand,
        R = IndexResponse,
        NodeId = NodeId,
        Node = openraft::impls::BasicNode
);

#[derive(Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries(openraft::raft::AppendEntriesRequest<ArkelRaftConfig>),
    Vote(openraft::raft::VoteRequest<NodeId>),
}
