use anyhow::{Context, Result};
use openraft;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

pub mod identity;
pub mod index;

pub struct BootstrapConfig {
    pub peer_addresses: Vec<String>,
}

pub enum NodeMode {
    /// Index Node: Cluster consensus and metadata tracker
    Index {
        node_id: u64,
        bootstrap: BootstrapConfig,
        http_addr: String,
    },
    /// Storage Node: Uses the standard Iroh Blobs Protocol engine
    Storage {
        blobs: iroh_blobs::BlobsProtocol,
        private_relay_url: Option<String>,
    },
}

pub struct Arkel {
    pub data_dir: PathBuf,
    pub identity: identity::NodeIdentity,
}

impl Arkel {
    pub async fn init(data_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&data_dir)
            .await
            .context("Failed to create root directory data track")?;

        let identity =
            identity::NodeIdentity::load_or_create(&data_dir.join("identity.key")).await?;
        Ok(Self { data_dir, identity })
    }

    pub async fn run(self, mode: NodeMode) -> Result<()> {
        // 1. Initialize the baseline endpoint builder topology
        let mut builder = match &mode {
            NodeMode::Index { http_addr, .. } => {
                // Index nodes: Minimal preset (just crypto provider), no DNS lookup, no relay.
                // Bind directly to the specified address so that peers can connect via the
                // advertised address (pubkey@<ip>:<port>).
                let addr: std::net::SocketAddr = http_addr.parse()
                    .context("Invalid http_addr format")?;
                iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .alpns(vec![b"arkel-raft".to_vec()])
                    .clear_ip_transports()
                    .bind_addr(addr)
                    .context("Failed to configure iroh bind address")?
            }
            NodeMode::Storage { .. } => {
                iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            }
        };
        builder = builder.secret_key(self.identity.secret_key().clone());
        if let NodeMode::Storage {
            private_relay_url, ..
        } = &mode
        {
            if let Some(url_str) = private_relay_url {
                let relay_map = iroh::RelayMap::try_from_iter([url_str.as_str()])
                    .context("Invalid private relay URL format supplied")?;
                builder = builder.relay_mode(iroh::endpoint::RelayMode::Custom(relay_map));
            } else {
                builder = builder.relay_mode(iroh::endpoint::RelayMode::Disabled);
            }
        }

        // 3. Bind the socket
        let endpoint = builder.bind().await?;
        let active_node_id = endpoint.id();
        assert_eq!(active_node_id, self.identity.node_id());
        tracing::info!("Arkel Protocol Socket Online: {active_node_id}");

        // 4. Run runtime engine event loops
        match mode {
            NodeMode::Index {
                node_id,
                bootstrap,
                http_addr,
            } => {
                tracing::info!("Running Index Engine {node_id}. Spinning up OpenRaft v0.9...");

                let store = crate::index::ArkelStore::new();
                let (log_store, state_machine) = openraft::storage::Adaptor::new(store);

                let config = std::sync::Arc::new(openraft::Config {
                    heartbeat_interval: 1000,
                    election_timeout_min: 4000,
                    election_timeout_max: 8000,
                    ..Default::default()
                });

                let network = crate::index::ArkelRaftNetwork::new(endpoint.clone());
                let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine)
                    .await
                    .context("Failed to spin up core Raft engine")?;

                // --- Live ALPN Wire RPC Handler ---
                let raft_handler = raft.clone();
                let endpoint_incoming = endpoint.clone();

                tokio::spawn(async move {
                    while let Some(incoming) = endpoint_incoming.accept().await {
                        let connecting = match incoming.accept() {
                            Ok(c) => c,
                            Err(_) => continue,
                        };

                        let raft = raft_handler.clone();
                        tokio::spawn(async move {
                            let connection = match connecting.await {
                                Ok(c) => c,
                                Err(e) => {
                                    tracing::debug!("accept connection failed: {}", e);
                                    return;
                                }
                            };

                            // Accept incoming Raft RPC streams
                            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                                let raft = raft.clone();
                                tokio::spawn(async move {
                                    match recv.read_to_end(1024 * 1024).await {
                                        Ok(buffer) => {
                                            if let Ok(msg) =
                                                serde_json::from_slice::<crate::index::RaftMessage>(
                                                    &buffer,
                                                )
                                            {
                                                match msg {
                                                    crate::index::RaftMessage::AppendEntries(
                                                        req,
                                                    ) => match raft.append_entries(req).await {
                                                        Ok(resp) => {
                                                            let out =
                                                                serde_json::to_vec(&resp).unwrap();
                                                            send.write_all(&out).await.ok();
                                                            send.finish().ok();
                                                        }
                                                        Err(e) => {
                                                            tracing::error!(
                                                                "append_entries failed: {:?}",
                                                                e
                                                            );
                                                            send.finish().ok();
                                                        }
                                                    },
                                                    crate::index::RaftMessage::Vote(req) => {
                                                        match raft.vote(req).await {
                                                            Ok(resp) => {
                                                                let out = serde_json::to_vec(&resp)
                                                                    .unwrap();
                                                                send.write_all(&out).await.ok();
                                                                send.finish().ok();
                                                            }
                                                            Err(e) => {
                                                                tracing::error!(
                                                                    "vote failed: {:?}",
                                                                    e
                                                                );
                                                                send.finish().ok();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::debug!("recv.read_to_end failed: {}", e);
                                        }
                                    }
                                });
                            }
                        });
                    }
                });

                // --- Cluster Membership Logic ---
                let my_full_addr = format!("{}@{}", self.identity.node_id(), http_addr);
                let mut initial_members = std::collections::BTreeMap::new();

                for peer in &bootstrap.peer_addresses {
                    let peer_id = self.derive_node_id(peer);
                    initial_members.insert(peer_id, openraft::impls::BasicNode::new(peer.clone()));
                }

                if !initial_members.contains_key(&node_id) {
                    initial_members.insert(node_id, openraft::impls::BasicNode::new(my_full_addr));
                }

                let min_node_id = initial_members.keys().next().copied().unwrap_or(node_id);

                if node_id == min_node_id {
                    tracing::info!(
                        "Node ID is lowest in configuration matrix. Triggering bootstrap..."
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    if let Err(e) = raft.initialize(initial_members).await {
                        tracing::warn!("Consensus initialization bypassed: {:?}", e);
                    } else {
                        tracing::info!("Consensus cluster bootstrap complete.");
                    }
                } else {
                    tracing::info!("Passive tracking node active. Awaiting election updates...");
                }

                tokio::signal::ctrl_c().await?;
                raft.shutdown().await.ok();
            }
            NodeMode::Storage { blobs, .. } => {
                tracing::info!("Running Storage Engine. Accepting binary data streams... ");

                let router = iroh::protocol::Router::builder(endpoint)
                    .accept(iroh_blobs::ALPN, blobs)
                    .spawn();

                tokio::signal::ctrl_c().await?;

                tracing::info!("Shutting down storage router...");
                router.shutdown().await?;
            }
        }

        Ok(())
    }

    pub fn derive_node_id(&self, addr: &str) -> u64 {
        // Extract the pubkey hex from `<pubkey>@<ip>:<port>` and derive the node ID
        // the same way main.rs does: first 8 little-endian bytes of the pubkey.
        let id_str = addr.split_once('@').map(|(s, _)| s).unwrap_or(addr);
        if let Ok(pk) = id_str.parse::<iroh::PublicKey>() {
            let bytes = pk.as_bytes();
            return u64::from_le_bytes(bytes[..8].try_into().expect("pubkey >= 8 bytes"));
        }
        // Fallback: hash the whole string (should not happen for well-formed addresses)
        let mut hasher = DefaultHasher::new();
        addr.hash(&mut hasher);
        hasher.finish()
    }
}
