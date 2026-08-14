use serde::{Deserialize, Serialize};
use std::fmt;

pub type NodeId = u64;

/// System modifications committed to the Raft consensus log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IndexNodeRequest {
    CreateBucket {
        name: String,
        #[serde(default)]
        created_at: u64,
        owner: Vec<u8>,
    },
    DeleteBucket {
        name: String,
        caller: Vec<u8>,
    },
    CommitManifest {
        bucket: String,
        key: String,
        object_hash: Vec<u8>,
        manifest_bytes: Vec<u8>,
        caller: Vec<u8>,
    },
    DeleteManifest {
        bucket: String,
        key: String,
        caller: Vec<u8>,
    },
    SetRepairOperator {
        caller: Vec<u8>,
    },
    RepairManifest {
        bucket: String,
        key: String,
        object_hash: Vec<u8>,
        manifest_bytes: Vec<u8>,
        caller: Vec<u8>,
    },
    Batch(Vec<IndexNodeRequest>),
    RegisterNode {
        node_id: Vec<u8>,
        capacity_bytes: u64,
        addr: String,
        relay_url: Option<String>,
    },
    MarkNodesOffline {
        node_ids: Vec<Vec<u8>>,
    },
}

impl fmt::Display for IndexNodeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexNodeRequest::CreateBucket {
                name, created_at, ..
            } => {
                write!(f, "CreateBucket({}, {})", name, created_at)
            }
            IndexNodeRequest::DeleteBucket { name, .. } => write!(f, "DeleteBucket({})", name),
            IndexNodeRequest::CommitManifest { object_hash, .. } => {
                write!(f, "CommitManifest({})", hex::encode(object_hash))
            }
            IndexNodeRequest::DeleteManifest { bucket, key, .. } => {
                write!(f, "DeleteManifest({}, {})", bucket, key)
            }
            IndexNodeRequest::SetRepairOperator { .. } => {
                write!(f, "SetRepairOperator")
            }
            IndexNodeRequest::RepairManifest { object_hash, .. } => {
                write!(f, "RepairManifest({})", hex::encode(object_hash))
            }
            IndexNodeRequest::Batch(entries) => write!(f, "Batch({} entries)", entries.len()),
            IndexNodeRequest::RegisterNode { node_id, .. } => {
                write!(f, "RegisterNode({})", hex::encode(node_id))
            }
            IndexNodeRequest::MarkNodesOffline { node_ids } => {
                let ids: Vec<String> = node_ids
                    .iter()
                    .map(|id| hex::encode(id).chars().take(16).collect())
                    .collect();
                write!(f, "MarkNodesOffline({})", ids.join(", "))
            }
        }
    }
}

/// The response confirmation sent back to the control API.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexNodeResponse {
    pub success: bool,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_responses: Option<Vec<IndexNodeResponse>>,
}

impl IndexNodeResponse {
    pub fn ok() -> Self {
        Self {
            success: true,
            error: None,
            batch_responses: None,
        }
    }

    pub fn err(s: &str) -> Self {
        Self {
            success: false,
            error: Some(s.to_string()),
            batch_responses: None,
        }
    }

    pub fn batch(responses: Vec<IndexNodeResponse>) -> Self {
        Self {
            success: true,
            error: None,
            batch_responses: Some(responses),
        }
    }
}

openraft::declare_raft_types!(
    pub ArkelRaftConfig:
        D = IndexNodeRequest,
        R = IndexNodeResponse,
        NodeId = u64,
        Node = crate::ArkelIndexNode,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub node_id: Vec<u8>,
    pub capacity_bytes: u64,
    pub addr: String,
    pub relay_url: Option<String>,
    pub last_seen: u64, // Raft log index of last heartbeat
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Online,
    Offline,
}
