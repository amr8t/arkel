use crate::identity::NodeIdentity;
use crate::index::remote::index_write;
use anyhow::Result;
use iroh::PublicKey;
use std::net::SocketAddr;
use std::time::Duration;

/// Registers this storage node with the index cluster.
///
/// Intentionally unauthenticated server-side for beta: the index ignores the
/// request signature on /register, so a node can impersonate another identity.
/// Shard GC (/shards/gc-candidates) is likewise unsigned. Revisit both with
/// node attestation.
pub struct NodeRegistrar {
    node_id: PublicKey,
    secret_key: iroh::SecretKey,
    capacity_bytes: u64,
    addr: SocketAddr,
    index_addrs: Vec<String>,
    relay_url: Option<String>,
    http: reqwest::Client,
}

impl NodeRegistrar {
    pub fn new(
        identity: &NodeIdentity,
        capacity_bytes: u64,
        addr: SocketAddr,
        index_addrs: Vec<String>,
        relay_url: Option<String>,
    ) -> Self {
        Self {
            node_id: identity.node_id(),
            secret_key: identity.secret_key().clone(),
            capacity_bytes,
            addr,
            index_addrs,
            relay_url,
            http: reqwest::Client::new(),
        }
    }

    pub async fn register(&self) -> Result<()> {
        let payload = serde_json::json!({
            "node_id": self.node_id.as_bytes().to_vec(),
            "capacity_bytes": self.capacity_bytes,
            "addr": self.addr.to_string(),
            "relay_url": self.relay_url,
             "registered_at": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });
        index_write(
            &self.http,
            &self.index_addrs,
            "register",
            &payload,
            &self.secret_key,
        )
        .await?;
        tracing::info!("Registered with index cluster");
        Ok(())
    }

    pub async fn run(self) {
        // 15s cadence: short enough that a live node's `last_seen`
        // always advances between the index's 20s health polls, so the
        // advance-based offline detector never trips on a healthy node.
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await; // first tick is immediate at startup registration, then every 15s
            if let Err(e) = self.register().await {
                tracing::warn!("registration/heartbeat failed: {e}, will retry");
            }
        }
    }
}
