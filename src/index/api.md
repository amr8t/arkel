With index node doubling as router, it handles everything:
User-facing API on Index Node
# Object operations
PUT    /:bucket/:key        # index picks storage node, forwards data, registers metadata
GET    /:bucket/:key        # index looks up hash+location, fetches from storage, streams to user
HEAD   /:bucket/:key        # index checks metadata exists
DELETE /:bucket/:key        # index removes from storage node, then removes metadata via raft

# Bucket operations  
PUT    /:bucket             # create bucket
DELETE /:bucket             # delete bucket and all objects
GET    /:bucket             # list objects in bucket (with ?prefix=&limit=&continuation-token=)
GET    /                    # list all buckets

# Health
GET    /health
GET    /ready


Internal API (storage node to index node, not user facing)
POST   /internal/blobs             # storage node registers completed upload
DELETE /internal/blobs/:hash       # storage node confirms deletion
GET    /internal/healthz           # index checks if storage node is alive

The flow for each operation:
PUT (two-phase — hard to retrofit, built in from day one):
User → Index → picks least loaded storage node
             → forwards stream to storage node
             → storage node writes bytes, computes hash + etag
             → storage node confirms { hash, etag, size }
             → only now: index writes metadata via raft
             → raft commits across all index nodes
             → 200 OK to user with ETag header
GET:
User → Index → looks up metadata locally (no raft needed)
             → fetches blob from storage node by hash
             → streams to user
DELETE:
User → Index → looks up metadata
             → tells storage node to delete hash
             → raft write to remove metadata
             → 200 OK to user


Goes through Raft consensus:
PUT    /:bucket/:key        # PutObject metadata (bucket, key, hash, size, storage_nodes)
DELETE /:bucket/:key        # DeleteObject metadata
PUT    /:bucket             # CreateBucket
DELETE /:bucket             # DeleteBucket

Does NOT go through consensus:
GET    /:bucket/:key        # read local state machine
GET    /:bucket             # read local state machine
GET    /                    # read local state machine
HEAD   /:bucket/:key        # read local state machine
GET    /health              # local
GET    /ready               # local

Object Metadata
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub blob_hash: String,
    pub etag: String,              // md5/blake3 of content, required by S3 clients
    pub size: u64,
    pub content_type: Option<String>,
    pub version: Option<String>,   // unused now, free to add versioning later
    pub storage_nodes: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}


The problem with index-as-router at scale:
Index nodes are precious — they're running Raft consensus which is CPU and network intensive. Proxying large blob uploads/downloads through them means:

A 10GB upload ties up an index node for the duration
Blob transfer bandwidth competes with Raft heartbeats
Index nodes become the bottleneck for everything

What you probably want long term:
User
  ↓
Gateway (stateless, just your api.rs, no raft)
  ↓                    ↓
Index Nodes          Storage Nodes
(raft, metadata)     (blob bytes)
Gateway asks index for metadata, talks directly to storage for bytes.
But for now index-as-router is fine because:

You have one cluster, small scale
Simplest thing that works
Easy to extract gateway later — it's just moving api.rs to its own binary


===================
