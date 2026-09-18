//! Standalone repair operator: scans the index for objects below target k/m
//! and heals them (decode survivors -> re-encode -> redistribute -> re-commit).
//! Also migrates healthy objects recorded at an older/lower scheme up to the
//! effective target as the pool grows.
//!
//! Encryption-agnostic — works on ciphertext shards, no keys needed. Run
//! hourly via cron; idempotent and rate-limited. Health-aware detection:
//! shards on nodes marked Offline are treated as missing without a fetch.

use anyhow::{Context, Result};
use iroh::PublicKey;
use std::collections::HashSet;
use std::time::Duration;

use crate::client::Client as ArkelClient;
use crate::client::ErasureConfig;
use crate::client::pool::reencode;
use crate::index::remote;
use crate::manifest::deserialize_manifest;
use crate::storage::blob::get_blob;

/// Run one repair pass.
///
/// `register = true` registers this identity as the repair operator (one-time)
/// and exits. Otherwise it scans and repairs up to `rate_limit` objects.
pub async fn run(client: &ArkelClient, register: bool, rate_limit: usize) -> Result<()> {
    let cfg = &client.cfg;
    let http = &cfg.http;
    let index_addrs = &cfg.index_addrs;

    if register {
        remote::set_repair_operator(http, index_addrs, &cfg.secret_key).await?;
        tracing::info!("registered repair operator");
        return Ok(());
    }

    // Health map + teach the endpoint the node addresses so downloads work.
    let nodes = remote::list_all_nodes(http, index_addrs).await?;
    let mut offline: HashSet<PublicKey> = HashSet::new();
    for n in &nodes {
        if n.offline {
            offline.insert(n.node_id);
        } else {
            let ea =
                iroh::EndpointAddr::from_parts(n.node_id, vec![iroh::TransportAddr::Ip(n.addr)]);
            let _ = client.endpoint.connect(ea, iroh_blobs::ALPN).await;
        }
    }

    let manifests = remote::list_all_manifests(http, index_addrs, &cfg.secret_key).await?;
    let mut fixed = 0usize;

    for (bucket, key, mb) in manifests {
        if fixed >= rate_limit {
            break;
        }
        let manifest = deserialize_manifest(&mb)?;
        let total = manifest.k as usize + manifest.m as usize;
        let from = ErasureConfig {
            k: manifest.k as usize,
            m: manifest.m as usize,
        };

        // Health-aware availability: skip offline nodes, fetch the rest.
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut avail = 0usize;
        for placement in &manifest.shards {
            if offline.contains(&placement.node_id) {
                continue;
            }
            let hash = iroh_blobs::Hash::from(placement.blob_hash);
            match tokio::time::timeout(
                Duration::from_secs(15),
                get_blob(&client.store, &client.endpoint, hash, placement.node_id),
            )
            .await
            {
                Ok(Ok(shard)) => {
                    slots[placement.shard_index as usize] = Some(shard);
                    avail += 1;
                }
                _ => tracing::debug!("shard {} unavailable", placement.shard_index),
            }
        }

        // Effective target: assign_shards' degraded config, so the manifest k/m
        // always matches the actual shard count.
        let (effective, _) = crate::client::pool::assign_shards(cfg).await?;

        if avail < manifest.k as usize {
            tracing::warn!(
                "skip {bucket}/{key}: only {avail} of {total} shards available (need {})",
                manifest.k
            );
            continue; // unrecoverable until a node returns
        }
        // Concentration: an object whose shards are crammed onto fewer distinct
        // nodes than the scheme implies (e.g. explicit --storage-addrs with a
        // small pool) is not durable the way its k/m promises. Redistribute.
        let distinct = manifest
            .shards
            .iter()
            .map(|p| p.node_id)
            .collect::<HashSet<_>>()
            .len();
        let concentrated = distinct < total;
        if avail >= total && from == effective && !concentrated {
            continue; // healthy, at the effective target scheme, and well-spread
        }

        // Re-encode survivors to the effective target scheme and redistribute.
        let refs: Vec<Option<&[u8]>> = slots.iter().map(|o| o.as_deref()).collect();
        let new_shards = reencode(&refs, from, manifest.ciphertext_size as usize, effective)
            .context("re-encode failed")?;
        client
            .repair_object(
                &bucket,
                &key,
                manifest.object_hash,
                manifest.ciphertext_size,
                manifest.original_size,
                new_shards,
                effective,
            )
            .await?;
        fixed += 1;
        tracing::info!(
            "repaired {bucket}/{key} ({avail}/{total} shards on {distinct} nodes -> {effective:?})"
        );
    }

    tracing::info!("repair pass complete: fixed {fixed} object(s)");
    Ok(())
}
