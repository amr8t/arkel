use serde::{Deserialize, Serialize};
use std::fmt;

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

impl fmt::Display for ObjectMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectMetadata {{ bucket: {}, key: {}, size: {}, etag: {} }}",
            self.bucket, self.key, self.size, self.etag
        )
    }
}

/// System modifications committed to the Raft consensus log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IndexNodeRequest {
    PutObject(ObjectMetadata),
    DeleteObject { bucket: String, key: String },
    CreateBucket {
        name: String,
        #[serde(default)]
        created_at: u64,
    },
    DeleteBucket { name: String },
    Batch(Vec<IndexNodeRequest>),
}

impl fmt::Display for IndexNodeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexNodeRequest::PutObject(meta) => write!(f, "PutObject({})", meta),
            IndexNodeRequest::DeleteObject { bucket, key } => {
                write!(f, "DeleteObject({}/{})", bucket, key)
            }
            IndexNodeRequest::CreateBucket { name, created_at } => {
                write!(f, "CreateBucket({}, {})", name, created_at)
            }
            IndexNodeRequest::DeleteBucket { name } => write!(f, "DeleteBucket({})", name),
            IndexNodeRequest::Batch(entries) => write!(f, "Batch({} entries)", entries.len()),
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
