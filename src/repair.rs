//! Standalone repair operator: scans the index for objects below target k/m
//! and heals them (decode survivors -> re-encode -> redistribute -> re-commit).
//!
//! Encryption-agnostic — works on ciphertext shards, no keys needed. Run
//! hourly via cron; idempotent and rate-limited. Health-aware detection:
//! shards on nodes marked Offline are treated as missing without a fetch.

use anyhow::{Context, Result};
use iroh::PublicKey;
use std::collections::HashSet;
use std::time::Duration;

use crate::client::Client as ArkelClient;
use crate::dataplane::{ErasureConfig, reencode};
use crate::index::client;
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
        client::set_repair_operator(http, index_addrs, &cfg.secret_key).await?;
        tracing::info!("registered repair operator");
        return Ok(());
    }

    // Health map + teach the endpoint the node addresses so downloads work.
    let nodes = client::list_all_nodes(http, index_addrs).await?;
    let mut offline: HashSet<PublicKey> = HashSet::new();
    for n in &nodes {
        if n.offline {
            offline.insert(n.node_id);
        } else {
            let mut addrs = vec![iroh::TransportAddr::Ip(n.addr)];
            if let Some(url) = &n.relay_url {
                addrs.push(iroh::TransportAddr::Relay(url.parse()?));
            }
            let ea = iroh::EndpointAddr::from_parts(n.node_id, addrs);
            let _ = client.endpoint.connect(ea, iroh_blobs::ALPN).await;
        }
    }

    let manifests = client::list_all_manifests(http, index_addrs).await?;
    let mut fixed = 0usize;

    for (bucket, key, mb) in manifests {
        if fixed >= rate_limit {
            break;
        }
        let m = crate::client::manifest::deserialize_manifest(&mb)?;
        let total = m.k as usize + m.m as usize;

        // Health-aware availability: skip offline nodes, fetch the rest.
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut avail = 0usize;
        for placement in &m.shards {
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

        if avail >= total || avail < m.k as usize {
            continue; // healthy, or unrecoverable until a node returns
        }

        // Re-encode survivors to the effective target scheme and redistribute.
        // Use assign_shards' effective config so the manifest k/m matches the
        // actual shard count (same discipline as put()).
        let (effective, _) = crate::dataplane::assign_shards(cfg).await?;
        let refs: Vec<Option<&[u8]>> = slots.iter().map(|o| o.as_deref()).collect();
        let from = ErasureConfig {
            k: m.k as usize,
            m: m.m as usize,
        };
        let new_shards = reencode(&refs, from, m.ciphertext_size as usize, effective)
            .context("re-encode failed")?;
        client
            .repair_object(
                &bucket,
                &key,
                m.object_hash,
                m.ciphertext_size,
                m.original_size,
                new_shards,
                effective,
            )
            .await?;
        fixed += 1;
        tracing::info!("repaired {bucket}/{key} ({avail}/{total} shards -> {effective:?})");
    }

    tracing::info!("repair pass complete: fixed {fixed} object(s)");
    Ok(())
}
