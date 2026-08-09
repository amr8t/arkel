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
use rand::seq::SliceRandom;
pub mod erasure;

pub use erasure::{ErasureConfig, decode, encode};

use crate::client::encrypt::{decrypt_shard, derive_key, encrypt_shard};
use crate::client::manifest::{
    Manifest, ShardPlacement, bytes_to_hash, deserialize_manifest, etag_from_hash,
    serialize_manifest, sign_manifest, verify_manifest,
};
use crate::index::client::{index_put, index_read, list_healthy_nodes};
use crate::storage::blob::get_blob;

pub struct PreparedUpload {
    pub object_hash: blake3::Hash,
    pub ciphertext_size: usize,
    pub shards: Vec<Vec<u8>>,
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

    let ciphertext = encrypt_shard(data, &derived_key);
    let ciphertext_size = ciphertext.len();
    let shards = encode(&ciphertext, &ec_config)?;
    let shard_hashes: Vec<_> = shards.iter().map(|s| blake3::hash(s)).collect();

    Ok(PreparedUpload {
        object_hash,
        ciphertext_size,
        shards,
        shard_hashes,
        erasure_config: ec_config,
    })
}

pub fn reconstruct_object(
    shards: &[Option<&[u8]>],
    ec_config: ErasureConfig,
    master_key: &[u8; 32],
    expected_object_hash: &Hash,
    ciphertext_len: usize,
) -> Result<Vec<u8>> {
    let derived_key = derive_key(master_key, expected_object_hash);

    // Shards are ciphertext slices; decode back to the full ciphertext, then
    // decrypt once. Trim to the manifest-recorded ciphertext size.
    let recovered = decode(shards, &ec_config, ciphertext_len)?;
    let plaintext = decrypt_shard(&recovered, &derived_key)?;

    let computed = blake3::hash(&plaintext);
    if computed != *expected_object_hash {
        bail!("integrity check failed: BLAKE3 mismatch");
    }

    Ok(plaintext)
}

/// A storage node the client can push/pull shards to/from.
#[derive(Clone)]
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

/// Upload an object: EC + encrypt, host the shards locally and have storage
/// nodes pull them (iroh-blobs push is unreliable), build + sign a Manifest, and
/// commit it via Raft. Returns the ETag.
pub async fn put(
    cfg: &DataPlaneConfig,
    targets: &[StorageTarget],
    endpoint: &iroh::Endpoint,
    store: &iroh_blobs::api::Store,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Result<String> {
     let (ec, targets) = if targets.is_empty() {
        assign_shards(cfg).await?
    } else {
        (cfg.ec_config, targets.to_vec())
    };
    if targets.is_empty() {
        bail!("no storage targets to distribute shards to");
    }
    let prepared = prepare_upload(data, ec, &cfg.secret_key.to_bytes())?;

    // 1. Host each encrypted shard locally (as a provider) and keep the tags
    //    alive until the storage nodes have pulled them.
    let mut shard_hashes = Vec::with_capacity(prepared.shards.len());
    let mut temp_tags = Vec::new();
    for shard in &prepared.shards {
        let tag = store.blobs().add_bytes(shard.to_vec()).temp_tag().await?;
        shard_hashes.push(tag.hash());
        temp_tags.push(tag);
    }

    // 2. Tell each storage node to pull its assigned shard(s) from us, awaiting
    //    an ack per shard.
    for (i, blob_hash) in shard_hashes.iter().enumerate() {
        let target = &targets[i % targets.len()];
        pull_shard(endpoint, target, *blob_hash).await?;
    }

    // 3. Build the manifest exactly as before (placement = round-robin).
    let shards: Vec<ShardPlacement> = shard_hashes
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let target = &targets[i % targets.len()];
            ShardPlacement {
                shard_index: i as u8,
                node_id: target.node_id,
                blob_hash: *h.as_bytes(),
            }
        })
        .collect();

    let manifest = Manifest {
        bucket: bucket.into(),
        key: key.into(),
        object_hash: *prepared.object_hash.as_bytes(),
        original_size: data.len() as u64,
        ciphertext_size: prepared.ciphertext_size as u64,
        k: prepared.erasure_config.k as u8,
        m: prepared.erasure_config.m as u8,
        shards,
    };
    let manifest_bytes = serialize_manifest(&manifest)?;
    let signature = sign_manifest(&manifest_bytes, &cfg.secret_key)?;
    index_put(
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

/// Ask a storage node to download a shard from us over the `arkel-pull` protocol.
async fn pull_shard(
    endpoint: &iroh::Endpoint,
    target: &StorageTarget,
    blob_hash: iroh_blobs::Hash,
) -> Result<()> {
    use crate::storage::PULL_ALPN;

    let ea = iroh::EndpointAddr::from_parts(target.node_id, [iroh::TransportAddr::Ip(target.addr)]);
    let conn = endpoint.connect(ea, PULL_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(blob_hash.as_bytes()).await?;
    let mut ack = [0u8; 1];
    recv.read_exact(&mut ack).await?;
    if ack[0] != 0 {
        bail!("storage node rejected shard pull");
    }
    Ok(())
}

/// Download an object: read the manifest, verify the signature, fetch k shards
/// over iroh-blobs QUIC, and reconstruct (decrypt + EC decode + BLAKE3 verify).
///
/// Placement comes from the manifest itself. The downloader dials storage nodes
/// by `node_id`, so `targets` is used to teach the endpoint the peer addresses
/// first (no discovery is configured).
pub async fn get(
    cfg: &DataPlaneConfig,
    targets: &[StorageTarget],
    endpoint: &iroh::Endpoint,
    store: &iroh_blobs::api::Store,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>> {

    let targets: Vec<StorageTarget> = if targets.is_empty() {
        list_healthy_nodes(&cfg.http, &cfg.index_addrs)
            .await?
            .into_iter()
            .map(|n| StorageTarget { node_id: n.node_id, addr: n.addr })
            .collect()
    } else {
        targets.to_vec()
    };

    for t in targets {
        let ea = iroh::EndpointAddr::from_parts(t.node_id, [iroh::TransportAddr::Ip(t.addr)]);
        endpoint.connect(ea, iroh_blobs::ALPN).await?;
    }

    let body = index_read(
        &cfg.http,
        &cfg.index_addrs,
        &format!("manifest/{bucket}/{key}"),
    )
    .await?;
    let manifest_bytes: Vec<u8> = serde_json::from_value(body["manifest_bytes"].clone())?;
    let sig_bytes: Vec<u8> = serde_json::from_value(body["signature"].clone())?;
    let signature = iroh::Signature::from_bytes(sig_bytes.as_slice().try_into()?);
    verify_manifest(&manifest_bytes, &signature, &cfg.secret_key.public())?;

    let manifest = deserialize_manifest(&manifest_bytes)?;

    let total = manifest.k as usize + manifest.m as usize;
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
    for placement in manifest.shards.iter().take(manifest.k as usize) {
        let shard = get_blob(
            store,
            endpoint,
            iroh_blobs::Hash::from(placement.blob_hash),
            placement.node_id,
        )
        .await?;
        shards[placement.shard_index as usize] = Some(shard);
    }

    let refs: Vec<Option<&[u8]>> = shards.iter().map(|o| o.as_deref()).collect();
    let master = cfg.secret_key.to_bytes();
    // Reconstruction is driven by the manifest's recorded k/m, not the current
    // config target — objects may have been written at a degraded redundancy.
    let ec = ErasureConfig {
        k: manifest.k as usize,
        m: manifest.m as usize,
    };
    reconstruct_object(
        &refs,
        ec,
        &master,
        &bytes_to_hash(manifest.object_hash),
        manifest.ciphertext_size as usize,
    )
}

pub async fn assign_shards(cfg: &DataPlaneConfig) -> Result<(ErasureConfig, Vec<StorageTarget>)> {
    let pool = list_healthy_nodes(&cfg.http, &cfg.index_addrs).await?;
     if pool.is_empty() {
        bail!("no healthy storage nodes");
    }
    let target = cfg.ec_config;
    let total = target.total_shards().min(pool.len());   // best effort
     let (k, m) = if total >= target.total_shards() {
        (target.k, target.m)
    } else {
        let k = target.k.min(pool.len().saturating_sub(1).max(1));
        (k, total - k)
    };
    let ec = ErasureConfig { k, m };
    let mut chosen: Vec<_> = pool.iter().collect();
    chosen.shuffle(&mut rand::thread_rng());
    let targets = chosen.into_iter()
        .take(total)
        .map(|n| StorageTarget { node_id: n.node_id, addr: n.addr })
        .collect();
    Ok((ec, targets))
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
        let mut available = vec![None; prepared.shards.len()];
        for (i, shard) in prepared.shards.iter().enumerate() {
            if i != 2 && i != 5 {
                available[i] = Some(shard.as_slice());
            }
        }

        let object_hash = blake3::hash(data);
        let recovered = reconstruct_object(
            &available,
            ec_config,
            &master,
            &object_hash,
            prepared.ciphertext_size,
        )?;
        assert_eq!(recovered, data);
        Ok(())
    }
}
