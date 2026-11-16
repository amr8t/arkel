pub mod encrypt;
pub mod erasure;
pub mod pool;

use anyhow::Result;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use erasure::ErasureConfig;
pub use pool::{ClientConfig, StorageTarget};

/// One row of an index object listing (mirrors `index::api::ListedObject`).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ListedObject {
    pub key: String,
    pub last_modified: u64,
    pub etag: String,
    pub size: u64,
}

/// Open a blob store with periodic GC enabled.
///
/// `Client::new` uses `FsStore::load`, whose options leave `gc: None` — no sweep
/// ever runs, so a long-lived process (the S3 gateway) accumulates every shard it
/// ever hosted. Shards are only protected by temp tags until each storage node
/// acks its pull, so plain interval GC is safe as long as `gc_interval` is
/// comfortably longer than the slowest put. `Duration::ZERO` disables GC.
pub async fn open_store_with_gc(
    dir: &Path,
    gc_interval: Duration,
) -> Result<iroh_blobs::api::Store> {
    let mut options = iroh_blobs::store::fs::options::Options::new(dir);
    options.gc = (!gc_interval.is_zero()).then(|| iroh_blobs::store::GcConfig {
        interval: gc_interval,
        add_protected: None,
    });
    Ok(
        iroh_blobs::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), options)
            .await?
            .into(),
    )
}

/// The arkel client library: bundles the shared data-plane config with an
/// iroh runtime (ephemeral endpoint + blob store). This is the embeddable,
/// programmatic interface (CLI and future gateway/adapters build on it).
pub struct Client {
    pub cfg: ClientConfig,
    pub endpoint: iroh::Endpoint,
    pub store: iroh_blobs::api::Store,
    pub router: iroh::protocol::Router,
}

impl Client {
    /// Build the iroh runtime: endpoint + blob store + blobs router (so
    /// storage nodes can pull shards from us, pull-based put). Reused by the
    /// M6 gateway, which runs the same runtime for its proxy path.
    pub async fn new(cfg: ClientConfig, store_dir: PathBuf) -> Result<Self> {
        let store: iroh_blobs::api::Store = iroh_blobs::store::fs::FsStore::load(&store_dir)
            .await?
            .into();
        Self::from_store(cfg, store).await
    }

    /// Build only the runtime around an already-opened blob store (lets the S3
    /// gateway use a store with GC enabled while the CLI keeps `Client::new`).
    pub async fn from_store(cfg: ClientConfig, store: iroh_blobs::api::Store) -> Result<Self> {
        let secret_key = cfg.secret_key.clone();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::endpoint::RelayMode::Default)
            .secret_key(secret_key)
            .bind()
            .await?;
        // Serve blobs locally so storage nodes can pull shards from us (pull-based put).
        let blobs = iroh_blobs::BlobsProtocol::new(&store, None);
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();
        Ok(Self {
            cfg,
            endpoint,
            store,
            router,
        })
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        targets: &[StorageTarget],
    ) -> Result<String> {
        pool::put(
            &self.cfg,
            targets,
            &self.endpoint,
            &self.store,
            bucket,
            key,
            data,
        )
        .await
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        targets: &[StorageTarget],
    ) -> Result<Vec<u8>> {
        pool::get(&self.cfg, targets, &self.endpoint, &self.store, bucket, key).await
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        pool::delete(&self.cfg, bucket, key).await
    }

    /// Bucket names owned by this identity (signed `GET /`).
    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        // Route "/" signs the bare path but the URL must not double the slash
        // (`index_get` already appends one), so sign "/" and request "".
        let body = crate::index::remote::index_read_signed_url(
            &self.cfg.http,
            &self.cfg.index_addrs,
            "/",
            "",
            &self.cfg.secret_key,
        )
        .await?;
        Ok(serde_json::from_value(body)?)
    }

    /// All objects in `bucket` whose key starts with `prefix`, with metadata.
    ///
    /// Deliberately asks for everything (`max-keys` is uncapped by the index) so
    /// the gateway can answer listings with `IsTruncated=false` and no
    /// continuation-token bookkeeping — a truncated page that a client cannot
    /// continue makes it conclude objects are missing and re-upload them.
    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<ListedObject>> {
        let url = format!(
            "{bucket}?prefix={}&max-keys={}",
            encode_query_value(prefix),
            1_000_000
        );
        let body = crate::index::remote::index_read_signed_url(
            &self.cfg.http,
            &self.cfg.index_addrs,
            bucket,
            &url,
            &self.cfg.secret_key,
        )
        .await?;
        Ok(serde_json::from_value(body["contents"].clone())?)
    }

    /// Object size, ETag and creation time without fetching shards (manifest
    /// read only).
    pub async fn stat_object(&self, bucket: &str, key: &str) -> Result<(u64, String, u64)> {
        let body = crate::index::remote::index_read_signed(
            &self.cfg.http,
            &self.cfg.index_addrs,
            &format!("manifest/{bucket}/{key}"),
            &self.cfg.secret_key,
        )
        .await?;
        let manifest_bytes: Vec<u8> = serde_json::from_value(body["manifest_bytes"].clone())?;
        let manifest = crate::manifest::deserialize_manifest(&manifest_bytes)?;
        Ok((
            manifest.original_size,
            crate::manifest::etag_from_hash(&manifest.object_hash),
            body["created_at"].as_u64().unwrap_or(0),
        ))
    }

    pub async fn repair_object(
        &self,
        bucket: &str,
        key: &str,
        object_hash: [u8; 32],
        ciphertext_size: u64,
        original_size: u64,
        shards: Vec<Vec<u8>>,
        target: ErasureConfig,
    ) -> Result<String> {
        pool::repair_object(
            &self.cfg,
            &self.endpoint,
            &self.store,
            bucket,
            key,
            object_hash,
            ciphertext_size,
            original_size,
            shards,
            target,
        )
        .await
    }
}

/// Percent-encode a query parameter value (RFC 3986 unreserved set only).
fn encode_query_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
