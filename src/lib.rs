use anyhow::{Context, Result};
use openraft::Raft;
use openraft_rt::WatchReceiver;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::storage::{DiskStore, NodeRegistrar, ShardStore};

pub mod api;
pub mod client;
pub mod dataplane;
pub mod identity;
pub mod index;
pub mod storage;

/// Index-node cluster seed addresses.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub peer_addresses: Vec<String>,
}

/// Runtime mode for this Arkel node.
pub enum NodeMode {
    /// Index Node: Cluster consensus and metadata tracker.
    /// Uses a bare Iroh endpoint bound directly to `http_addr`. The endpoint is
    /// kept alive for future gateway<->storage use; Raft traffic now runs over
    /// HTTP on `http_addr`.
    Index {
        bootstrap: BootstrapConfig,
        http_addr: SocketAddr,
    },
    /// Storage Node: Uses the standard Iroh Blobs Protocol engine.
    Storage {
        base_dir: PathBuf,
        blobs: iroh_blobs::BlobsProtocol,
        store: iroh_blobs::api::Store,
        private_relay_url: Option<String>,
        index_addrs: Vec<String>,
        addr: SocketAddr,
        advertise_addr: Option<SocketAddr>,
    },
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RaftParams {
    pub rpc_url: String,
    pub bind_addr: String,
    pub http_addr: std::net::SocketAddr,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArkelNodeType {
    Index(RaftParams),
    Storage,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArkelIndexNode {
    pub id: u64,
    pub params: RaftParams,
    pub bootstrap: BootstrapConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArkelNode {
    pub id: u64,
    pub node_type: ArkelNodeType,
}

impl ArkelNode {
    pub fn from_local(pubkey: &iroh::PublicKey, mode: &crate::NodeMode) -> Self {
        let pk_bytes = pubkey.as_bytes();
        let id = u64::from_le_bytes(pk_bytes[..8].try_into().expect("valid pubkey"));

        let node_type = match mode {
            crate::NodeMode::Index { http_addr, .. } => {
                let host = http_addr.to_string();
                ArkelNodeType::Index(RaftParams {
                    rpc_url: format!("http://{host}"),
                    bind_addr: format!("{}@{}", pubkey, host),
                    http_addr: *http_addr,
                })
            }
            crate::NodeMode::Storage { .. } => ArkelNodeType::Storage,
        };

        Self { id, node_type }
    }
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
        let my_node = ArkelNode::from_local(&self.identity.node_id(), &mode);
        match my_node.node_type {
            ArkelNodeType::Index(params) => {
                let (bootstrap, http_addr) = match mode {
                    NodeMode::Index {
                        bootstrap,
                        http_addr,
                    } => (bootstrap, http_addr),
                    _ => unreachable!(),
                };

                let endpoint = build_index_endpoint(http_addr)?
                    .secret_key(self.identity.secret_key().clone())
                    .relay_mode(iroh::endpoint::RelayMode::Disabled)
                    .bind()
                    .await?;

                tracing::info!("Arkel Index Protocol Socket Online: {}", endpoint.id());

                let my_index_node = ArkelIndexNode {
                    id: my_node.id,
                    params,
                    bootstrap,
                };

                let _ = run_index_node(self, endpoint, my_index_node).await;
            }

            ArkelNodeType::Storage => {
                let (base_dir, blobs, store, _private_relay_url, index_addrs, addr, advertise_addr) =
                    match mode {
                        NodeMode::Storage {
                        base_dir,
                        blobs,
                        store,
                        private_relay_url,
                        index_addrs,
                        addr,
                        advertise_addr,
                    } => (
                        base_dir,
                        blobs,
                        store,
                        private_relay_url,
                        index_addrs,
                        addr,
                        advertise_addr,
                    ),
                    _ => unreachable!(),
                };

                let shard_dir = base_dir.join("shards");
                tokio::fs::create_dir_all(&shard_dir).await?;
                let disk_store = Arc::new(DiskStore::new(shard_dir));

                let endpoint = build_storage_endpoint(addr)?
                    .secret_key(self.identity.secret_key().clone())
                    .bind()
                    .await
                    .context("Failed to bind storage endpoint")?;

                let relay_url = loop {
                    if let Some(u) = endpoint.addr().relay_urls().next() {
                        break Some(u.to_string());
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                };

                tracing::info!(
                    "Storage Engine Online. ID: {}. endpointId: {}. relay_url: {}. Data: {:?}",
                    self.identity.raft_node_id(),
                    endpoint.id(),
                    relay_url.as_deref().unwrap_or_default(),
                    disk_store.data_dir()
                );

                let registrar = NodeRegistrar::new(
                    &self.identity,
                    1_000_000_000_000, // 1 TB default capacity
                    advertise_addr.unwrap_or(addr),
                    index_addrs,
                    relay_url,
                );
                tokio::spawn(registrar.run());

                let router = iroh::protocol::Router::builder(endpoint.clone())
                    .accept(iroh_blobs::ALPN, blobs)
                    .accept(
                        crate::storage::PULL_ALPN,
                        crate::storage::PullHandler::new(&store, &endpoint),
                    )
                    .spawn();

                tokio::signal::ctrl_c().await?;
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
fn build_storage_endpoint(addr: SocketAddr) -> Result<iroh::endpoint::Builder> {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .alpns(vec![b"arkel-blobs".to_vec()])
        .relay_mode(iroh::endpoint::RelayMode::Default)
        .bind_addr(addr)
        .context("Failed to configure storage iroh bind address")
}
async fn run_index_node(
    arkel: Arkel,
    endpoint: iroh::Endpoint,
    index_node: ArkelIndexNode,
) -> anyhow::Result<()> {
    let node_id = arkel.identity.raft_node_id();
    tracing::info!("Running Index Engine {node_id}. Spinning up OpenRaft v0.10...");

    let db_path = arkel.data_dir.join("manifests.db"); // define it
    let log_path = arkel.data_dir.join("raft_log.db");

    let log_store = index::ArkelLogStore::open(log_path).await?;
    let state_machine = index::ArkelStateMachine::open(db_path).await?;
    let state_machine_for_api = state_machine.clone();

    let config = std::sync::Arc::new(
        openraft::Config {
            heartbeat_interval: 10,
            election_timeout_min: 150,
            election_timeout_max: 300,
            max_payload_entries: 4096,
            allow_log_reversion: std::env::var("ARKEL_ALLOW_LOG_REVERSION")
                .is_ok()
                .then_some(true),
            ..Default::default()
        }
        .validate()
        .expect("Invalid Raft config"),
    );

    let network = index::ArkelRaftNetworkFactory::new();
    let raft = Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .context("Failed to spin up core Raft engine")
        .expect("Raft engine startup failed");

    let batch_collector = std::sync::Arc::new(crate::api::BatchCollector::new(
        std::env::var("ARKEL_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64),
        std::env::var("ARKEL_BATCH_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
    ));
    {
        let raft_for_flush = raft.clone();
        let collector_for_flush = batch_collector.clone();
        tokio::spawn(async move { collector_for_flush.flush_loop(raft_for_flush).await });
    }

    let state = std::sync::Arc::new(crate::api::AppState {
        raft: raft.clone(),
        state_machine: state_machine_for_api.clone(),
        batch_collector: batch_collector.clone(),
    });

    let listener = tokio::net::TcpListener::bind(index_node.params.http_addr)
        .await
        .expect("Failed to bind HTTP API listener");
    let app = crate::api::router()
        .merge(index::raft::raft_router(state.clone()))
        .with_state(state);
    tokio::spawn(crate::api::serve_index(listener, app));

    // Node Health checker
    let raft_health = raft.clone();
    let sm_health = state_machine_for_api.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if !raft_health.is_leader() {
                continue;
            }
            let current = raft_health
                .metrics()
                .borrow_watched()
                .last_log_index
                .unwrap_or(0);
            let stale: Vec<Vec<u8>> = sm_health
                .list_node_lags()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, last)| current.saturating_sub(*last) > 50) // LAG_THRESHOLD
                .map(|(id, _)| id)
                .collect();
            if !stale.is_empty() {
                raft_health
                    .client_write(crate::index::IndexNodeRequest::MarkNodesOffline {
                        node_ids: stale,
                    })
                    .await
                    .ok();
                tracing::info!("marked stale storage nodes offline");
            }
        }
    });

    // Keep the Iroh endpoint alive for future gateway<->storage use.
    let _endpoint = endpoint;

    initialize_raft_cluster(raft, index_node).await;
    Ok(())
}

async fn initialize_raft_cluster(
    raft: Raft<index::ArkelRaftConfig, index::ArkelStateMachine>,
    index_node: ArkelIndexNode,
) {
    let mut initial_members = std::collections::BTreeMap::new();

    // 1. Map out all peer configuration entries from the bootstrap addresses
    for peer in &index_node.bootstrap.peer_addresses {
        let peer_id = identity::raft_node_id_from_addr(peer);
        let host = peer
            .split_once('@')
            .map(|(_, h)| h)
            .unwrap_or(peer.as_str());

        let http_addr = host
            .parse::<std::net::SocketAddr>()
            .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());

        let raft_peer = ArkelIndexNode {
            id: peer_id,
            params: RaftParams {
                rpc_url: format!("http://{host}"),
                bind_addr: peer.clone(),
                http_addr,
            },
            bootstrap: BootstrapConfig::default(),
        };
        initial_members.insert(peer_id, raft_peer);
    }

    // 2. Insert node configuration directly using its internal ID
    let my_id = index_node.id;
    initial_members.insert(my_id, index_node);

    // 3. Bootstrap coordination: pick the lowest node ID dynamically from the map matrix
    let min_node_id = initial_members.keys().next().copied().unwrap_or(my_id);

    if my_id == min_node_id {
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
