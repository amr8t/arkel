use crate::identity::NodeIdentity;
use crate::index::client::index_write;
use anyhow::Result;
use iroh::PublicKey;
use std::net::SocketAddr;
use std::time::Duration;

pub struct NodeRegistrar {
    node_id: PublicKey,
    capacity_bytes: u64,
    addr: SocketAddr,
    index_addrs: Vec<String>,
    http: reqwest::Client,
}

impl NodeRegistrar {
    pub fn new(
        identity: &NodeIdentity,
        capacity_bytes: u64,
        addr: SocketAddr,
        index_addrs: Vec<String>,
    ) -> Self {
        Self {
            node_id: identity.node_id(),
            capacity_bytes,
            addr,
            index_addrs,
            http: reqwest::Client::new(),
        }
    }

    pub async fn register(&self) -> Result<()> {
        let payload = serde_json::json!({
            "node_id": self.node_id.as_bytes().to_vec(),
            "capacity_bytes": self.capacity_bytes,
            "addr": self.addr.to_string(),
        });
        index_write(&self.http, &self.index_addrs, "register", &payload).await?;
        tracing::info!("Registered with index cluster");
        Ok(())
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await; // first tick is immediate (= startup registration), then every 30s
            if let Err(e) = self.register().await {
                tracing::warn!("registration/heartbeat failed: {e}, will retry");
            }
        }
    }
}
