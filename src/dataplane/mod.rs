//! Shared data-plane pipeline (EC + encrypt + reconstruct).
//!
//! This is the single implementation of the object pipeline. It is used by the
//! client SDK (`client::sdk`, which does EC + encrypt locally and talks
//! iroh-blobs QUIC) and will be reused by the M6 S3-proxy gateway (which runs
//! the same functions server-side for tools that can't speak iroh-blobs).
//! Keeping the pipeline here means both callers share one implementation and
//! the M6 split is trivial — no reimplementation, no divergence.

use anyhow::{Result, bail};
use blake3::Hash;
use iroh::{PublicKey, SecretKey};
use std::net::SocketAddr;
pub mod erasure;

pub use erasure::{ErasureConfig, decode, encode};

use crate::client::encrypt::{decrypt_shard, derive_key, encrypt_shard};
use crate::client::manifest::{
    Manifest, ShardPlacement, bytes_to_hash, deserialize_manifest, etag_from_hash,
    serialize_manifest, sign_manifest, verify_manifest,
};
use crate::index::client::{index_read, index_write};
use crate::storage::blob::{get_blob, put_blob};

pub struct PreparedUpload {
    pub object_hash: blake3::Hash,
    pub encrypted_shards: Vec<Vec<u8>>,
    pub shard_hashes: Vec<blake3::Hash>,
    pub erasure_config: ErasureConfig,
}
pub fn prepare_upload(
    data: &[u8],
    ec_config: ErasureConfig,
    master_key: &[u8; 32],
) -> Result<PreparedUpload> {
    let object_hash = blake3::hash(data);
    let derived_key = derive_key(master_key, &object_hash);

    let plain_shards = encode(data, &ec_config)?;
    let mut encrypted_shards = Vec::with_capacity(plain_shards.len());
    let mut shard_hashes = Vec::with_capacity(plain_shards.len());

    for shard in plain_shards {
        let encrypted = encrypt_shard(&shard, &derived_key);
        let hash = blake3::hash(&encrypted);
        encrypted_shards.push(encrypted);
        shard_hashes.push(hash);
    }

    Ok(PreparedUpload {
        object_hash,
        encrypted_shards,
        shard_hashes,
        erasure_config: ec_config,
    })
}

pub fn reconstruct_object(
    encrypted_shards: &[Option<&[u8]>],
    ec_config: ErasureConfig,
    master_key: &[u8; 32],
    expected_object_hash: &Hash,
    original_len: usize,
) -> Result<Vec<u8>> {
    let derived_key = derive_key(master_key, expected_object_hash);

    let mut decrypted = Vec::with_capacity(encrypted_shards.len());
    for shard in encrypted_shards {
        if let Some(enc) = shard {
            let plain = decrypt_shard(enc, &derived_key)?;
            decrypted.push(Some(plain));
        } else {
            decrypted.push(None);
        }
    }
    let refs: Vec<Option<&[u8]>> = decrypted.iter().map(|opt| opt.as_deref()).collect();
    let recovered = decode(&refs, &ec_config, original_len)?;

    let computed = blake3::hash(&recovered);
    if computed != *expected_object_hash {
        bail!("integrity check failed: BLAKE3 mismatch");
    }

    Ok(recovered)
}

/// A storage node the client can push/pull shards to/from.
pub struct StorageTarget {
    pub node_id: PublicKey,
    pub addr: SocketAddr,
}

/// Shared config for data-plane put/get operations.
///
/// Both the smart client (CLI) and the future M6 S3-proxy gateway build their
/// own instance and pass it with their own endpoint/store runtime.
pub struct DataPlaneConfig {
    pub index_addrs: Vec<String>,
    pub secret_key: SecretKey,
    pub ec_config: ErasureConfig,
    pub http: reqwest::Client,
}

/// Upload an object: EC + encrypt, distribute shards over iroh-blobs QUIC,
/// build + sign a Manifest, and commit it via Raft. Returns the ETag.
pub async fn put(
    cfg: &DataPlaneConfig,
    targets: &[StorageTarget],
    endpoint: &iroh::Endpoint,
    store: &iroh_blobs::api::Store,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Result<String> {
    if targets.is_empty() {
        bail!("no storage targets to distribute shards to");
    }
    let prepared = prepare_upload(data, cfg.ec_config, &cfg.secret_key.to_bytes())?;

    let mut shards = Vec::with_capacity(prepared.encrypted_shards.len());
    for (i, shard) in prepared.encrypted_shards.iter().enumerate() {
        let target = &targets[i % targets.len()];
        let blob_hash = put_blob(store, endpoint, shard, target.node_id, target.addr).await?;
        shards.push(ShardPlacement {
            shard_index: i as u8,
            node_id: target.node_id,
            blob_hash: *blob_hash.as_bytes(),
        });
    }

    let manifest = Manifest {
        bucket: bucket.into(),
        key: key.into(),
        object_hash: *prepared.object_hash.as_bytes(),
        original_size: data.len() as u64,
        k: prepared.erasure_config.k as u8,
        m: prepared.erasure_config.m as u8,
        shards,
    };
    let manifest_bytes = serialize_manifest(&manifest)?;
    let signature = sign_manifest(&manifest_bytes, &cfg.secret_key)?;
    index_write(
        &cfg.http,
        &cfg.index_addrs,
        &format!("manifest/{bucket}/{key}"),
        &serde_json::json!({
            "object_hash": prepared.object_hash.as_bytes().to_vec(),
            "manifest_bytes": manifest_bytes,
            "signature": signature.to_bytes().to_vec(),
        }),
    )
    .await?;
    Ok(etag_from_hash(prepared.object_hash.as_bytes()))
}

/// Download an object: read the manifest, verify the signature, fetch k shards
/// over iroh-blobs QUIC, and reconstruct (decrypt + EC decode + BLAKE3 verify).
///
/// Placement comes from the manifest itself — no target list needed. The
/// downloader dials storage nodes by `node_id` via the endpoint (relay-backed).
pub async fn get(
    cfg: &DataPlaneConfig,
    endpoint: &iroh::Endpoint,
    store: &iroh_blobs::api::Store,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>> {
    let body =
        index_read(&cfg.http, &cfg.index_addrs, &format!("manifest/{bucket}/{key}")).await?;
    let manifest_bytes: Vec<u8> = serde_json::from_value(body["manifest_bytes"].clone())?;
    let sig_bytes: Vec<u8> = serde_json::from_value(body["signature"].clone())?;
    let signature = iroh::Signature::from_bytes(sig_bytes.as_slice().try_into()?);
    verify_manifest(&manifest_bytes, &signature, &cfg.secret_key.public())?;

    let manifest = deserialize_manifest(&manifest_bytes)?;

    let total = manifest.k as usize + manifest.m as usize;
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
    for placement in manifest.shards.iter().take(manifest.k as usize) {
        let shard = get_blob(store, endpoint, iroh_blobs::Hash::from(placement.blob_hash), placement.node_id)
            .await?;
        shards[placement.shard_index as usize] = Some(shard);
    }

    let refs: Vec<Option<&[u8]>> = shards.iter().map(|o| o.as_deref()).collect();
    let master = cfg.secret_key.to_bytes();
    reconstruct_object(
        &refs,
        cfg.ec_config,
        &master,
        &bytes_to_hash(manifest.object_hash),
        manifest.original_size as usize,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    
    #[test]
    fn test_prepare_reconstruct_roundtrip() -> Result<()> {
        let ec_config = ErasureConfig { k: 4, m: 2 };
        let master = [0u8; 32];
        let data = b"Hello world";

        let prepared = prepare_upload(data, ec_config, &master)?;

        // Simulate losing shards 2 and 5 (indices 2 and 5).
        let mut available = vec![None; prepared.encrypted_shards.len()];
        for (i, shard) in prepared.encrypted_shards.iter().enumerate() {
            if i != 2 && i != 5 {
                available[i] = Some(shard.as_slice());
            }
        }

        let object_hash = blake3::hash(data);
        let recovered =
            reconstruct_object(&available, ec_config, &master, &object_hash, data.len())?;
        assert_eq!(recovered, data);
        Ok(())
    }
}
