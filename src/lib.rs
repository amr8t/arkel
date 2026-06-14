use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;

pub mod api;
pub mod identity;
pub mod index;

/// Index-node cluster seed addresses.
#[derive(Debug, Default)]
pub struct BootstrapConfig {
    pub peer_addresses: Vec<String>,
}

/// Runtime mode for this Arkel node.
pub enum NodeMode {
    /// Index Node: Cluster consensus and metadata tracker.
    /// Uses a bare Iroh endpoint bound directly to `http_addr`.
    Index {
        bootstrap: BootstrapConfig,
        http_addr: SocketAddr,
    },
    /// Storage Node: Uses the standard Iroh Blobs Protocol engine.
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
        let mut builder = match &mode {
            NodeMode::Index { http_addr, .. } => build_index_endpoint(*http_addr)?,
            NodeMode::Storage { .. } => build_storage_endpoint(),
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

        let endpoint = builder.bind().await?;
        let active_node_id = endpoint.id();
        assert_eq!(active_node_id, self.identity.node_id());
        tracing::info!("Arkel Protocol Socket Online: {active_node_id}");

        match mode {
            NodeMode::Index {
                bootstrap,
                http_addr,
            } => {
                run_index_node(self, endpoint, bootstrap, http_addr).await;
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
}

/// Index node: builds a bare QUIC endpoint bound directly to `addr`.
fn build_index_endpoint(addr: SocketAddr) -> Result<iroh::endpoint::Builder> {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .alpns(vec![b"arkel-raft".to_vec()])
        .clear_ip_transports()
        .bind_addr(addr)
        .context("Failed to configure index iroh bind address")
}

/// Storage node: builds an endpoint with N0 defaults.
fn build_storage_endpoint() -> iroh::endpoint::Builder {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0)
}

async fn run_index_node(
    arkel: Arkel,
    endpoint: iroh::Endpoint,
    bootstrap: BootstrapConfig,
    http_addr: SocketAddr,
) {
    let node_id = arkel.identity.raft_node_id();
    tracing::info!("Running Index Engine {node_id}. Spinning up OpenRaft v0.9...");

    let store = crate::index::ArkelStore::new();
    let store_for_api = store.clone();
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
        .context("Failed to spin up core Raft engine")
        .expect("Raft engine startup failed");

    // --- Index HTTP API server ---
    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .expect("Failed to bind HTTP API listener");
    tokio::spawn(crate::api::serve_index(
        listener,
        raft.clone(),
        store_for_api,
    ));

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

                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let raft = raft.clone();
                    tokio::spawn(async move {
                        match recv.read_to_end(1024 * 1024).await {
                            Ok(buffer) => {
                                if let Ok(msg) =
                                    serde_json::from_slice::<crate::index::RaftMessage>(&buffer)
                                {
                                    match msg {
                                        crate::index::RaftMessage::AppendEntries(req) => {
                                            match raft.append_entries(req).await {
                                                Ok(resp) => {
                                                    let out = serde_json::to_vec(&resp).unwrap();
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
                                            }
                                        }
                                        crate::index::RaftMessage::Vote(req) => {
                                            match raft.vote(req).await {
                                                Ok(resp) => {
                                                    let out = serde_json::to_vec(&resp).unwrap();
                                                    send.write_all(&out).await.ok();
                                                    send.finish().ok();
                                                }
                                                Err(e) => {
                                                    tracing::error!("vote failed: {:?}", e);
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
    let my_full_addr = arkel.identity.raft_full_addr(http_addr);
    let mut initial_members = std::collections::BTreeMap::new();

    for peer in &bootstrap.peer_addresses {
        let peer_id = crate::identity::raft_node_id_from_addr(peer);
        initial_members.insert(peer_id, openraft::impls::BasicNode::new(peer.clone()));
    }

    initial_members
        .entry(node_id)
        .or_insert_with(|| openraft::impls::BasicNode::new(my_full_addr));

    let min_node_id = initial_members.keys().next().copied().unwrap_or(node_id);

    if node_id == min_node_id {
        tracing::info!("Node ID is lowest in configuration matrix. Triggering bootstrap...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        if let Err(e) = raft.initialize(initial_members).await {
            tracing::warn!("Consensus initialization bypassed: {:?}", e);
        } else {
            tracing::info!("Consensus cluster bootstrap complete.");
        }
    } else {
        tracing::info!("Passive tracking node active. Awaiting election updates...");
    }

    tokio::signal::ctrl_c().await.ok();
    raft.shutdown().await.ok();
}
