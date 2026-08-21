//! Index-side audit: prove storage nodes hold the shards manifests say they
//! hold. Content-addressed fetch means serving a shard == holding it; failures
//! mark the node Offline and repair regenerates the shards elsewhere.

use std::collections::HashMap;
use std::time::Duration;

use crate::index::{ArkelStateMachine, IndexNodeRequest};
use crate::storage::blob::get_blob;

const SAMPLE: usize = 32;
const FAIL_THRESHOLD: u32 = 2;

pub async fn run(
    raft: openraft::Raft<crate::index::ArkelRaftConfig, ArkelStateMachine>,
    sm: ArkelStateMachine,
    endpoint: iroh::Endpoint,
    store: iroh_blobs::api::Store,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        std::env::var("ARKEL_AUDIT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3600),
    ));
    loop {
        interval.tick().await;
        if !raft.is_leader() {
            continue;
        }
        let stats = sm.list_all_node_stats().await.unwrap_or_default();

        // Teach the endpoint each node's addresses (no discovery configured).
        let mut connects = Vec::new();
        for (_, ns) in &stats {
            let Ok(pk) = iroh::PublicKey::from_bytes(
                ns.node_id.as_slice().try_into().unwrap_or(&[0u8; 32]),
            ) else {
                continue;
            };
            let Ok(sa) = ns.addr.parse::<std::net::SocketAddr>() else {
                continue;
            };
            let mut addrs = vec![iroh::TransportAddr::Ip(sa)];
            if let Some(url) = &ns.relay_url
                && let Ok(u) = url.parse::<iroh::RelayUrl>()
            {
                addrs.push(iroh::TransportAddr::Relay(u));
            }
            let ea = iroh::EndpointAddr::from_parts(pk, addrs);
            let ep = endpoint.clone();
            connects.push(async move {
                tokio::time::timeout(
                    Duration::from_secs(3),
                    ep.connect(ea, iroh_blobs::ALPN),
                )
                .await
            });
        }
        let _ = futures_util::future::join_all(connects).await;

        // Sample placements and fetch each shard; count failures per node.
        let samples = match sm.sample_placements(SAMPLE).await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut fails: HashMap<Vec<u8>, u32> = HashMap::new();
        for (node_id, blob_hash) in samples {
            let Ok(node) = iroh::PublicKey::from_bytes(
                node_id.as_slice().try_into().unwrap_or(&[0u8; 32]),
            ) else {
                continue;
            };
            let hash = iroh_blobs::Hash::from(blob_hash);
            match tokio::time::timeout(
                Duration::from_secs(15),
                get_blob(&store, &endpoint, hash, node),
            )
            .await
            {
                Ok(Ok(_)) => {}
                _ => *fails.entry(node_id).or_default() += 1,
            }
        }
        let doomed: Vec<Vec<u8>> = fails
            .into_iter()
            .filter(|(_, n)| *n >= FAIL_THRESHOLD)
            .map(|(id, _)| id)
            .collect();
        if !doomed.is_empty() {
            raft.client_write(IndexNodeRequest::MarkNodesOffline { node_ids: doomed })
                .await
                .ok();
            tracing::info!("audit marked shard-dropping nodes offline");
        }
    }
}
