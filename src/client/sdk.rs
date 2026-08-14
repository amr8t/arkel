use anyhow::Result;
use std::path::PathBuf;

use crate::dataplane::{self, DataPlaneConfig, ErasureConfig, StorageTarget};

/// Client-facing alias for the shared data-plane config.
pub use crate::dataplane::DataPlaneConfig as ClientConfig;

/// Bundles the shared config with the CLI's runtime (ephemeral endpoint + store).
pub struct Client {
    pub cfg: DataPlaneConfig,
    pub endpoint: iroh::Endpoint,
    pub store: iroh_blobs::api::Store,
    pub router: iroh::protocol::Router,
}

impl Client {
    pub async fn new(cfg: DataPlaneConfig, store_dir: PathBuf) -> Result<Self> {
        let secret_key = cfg.secret_key.clone();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::endpoint::RelayMode::Default)
            .secret_key(secret_key)
            .bind()
            .await?;
        let store: iroh_blobs::api::Store = iroh_blobs::store::fs::FsStore::load(&store_dir)
            .await?
            .into();
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
        dataplane::put(
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
        dataplane::get(&self.cfg, targets, &self.endpoint, &self.store, bucket, key).await
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        dataplane::delete(&self.cfg, bucket, key).await
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
        dataplane::repair_object(
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
