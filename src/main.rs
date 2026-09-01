use anyhow::{Context, Result};
use arkel::{
    Arkel, BootstrapConfig, NodeMode,
    cli::{AccountCmd, Cli, ClientCmd, Commands, PaymentCmd, erasure_config, parse_targets},
    client::{Client as ArkelClient, ClientConfig},
    dataplane::ErasureConfig,
    identity::NodeIdentity,
};
use clap::Parser;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

async fn run_account(identity: &NodeIdentity, cmd: AccountCmd) -> Result<()> {
    match cmd {
        AccountCmd::Quota {
            account,
            index_addrs,
        } => {
            let account = account.unwrap_or_else(|| hex::encode(identity.node_id().as_bytes()));
            let http = reqwest::Client::new();
            let (total, used) = arkel::index::client::account_quota(
                &http,
                &index_addrs,
                &account,
                identity.secret_key(),
            )
            .await?;
            println!("{account}: {used} / {total} bytes used");
            Ok(())
        }
    }
}

/// Payment-operator tooling: register this identity as the payment operator,
/// or credit quota to an account (the payment service in CLI form).
async fn run_payment(identity: &NodeIdentity, cmd: PaymentCmd) -> Result<()> {
    let http = reqwest::Client::new();
    match cmd {
        PaymentCmd::Register { index_addrs } => {
            arkel::index::client::set_payment_operator(&http, &index_addrs, identity.secret_key())
                .await?;
            println!("registered payment operator: {}", identity.node_id());
        }
        PaymentCmd::Credit {
            account,
            bytes,
            source,
            ref_id,
            index_addrs,
        } => {
            arkel::index::client::credit_quota(
                &http,
                &index_addrs,
                &account,
                bytes,
                &source,
                &ref_id,
                identity.secret_key(),
            )
            .await?;
            println!("credited {bytes} bytes to {account} (ref {ref_id})");
        }
        PaymentCmd::SetDefaultQuota { bytes, index_addrs } => {
            let total = arkel::config::parse_size(&bytes)?;
            arkel::index::client::set_default_quota(
                &http,
                &index_addrs,
                total,
                identity.secret_key(),
            )
            .await?;
            println!("set default quota to {bytes} ({total} bytes)");
        }
    }
    Ok(())
}

async fn run_client(base_dir: PathBuf, identity: &NodeIdentity, cmd: ClientCmd) -> Result<()> {
    let store_dir = base_dir.join("blobs");

    let client;
    let result: Result<()> = match cmd {
        ClientCmd::Put {
            file,
            bucket,
            key,
            index_addrs,
            storage_addrs,
        } => {
            let key = key.unwrap_or_else(|| {
                file.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "object".to_string())
            });
            let cfg = ClientConfig {
                index_addrs,
                secret_key: identity.secret_key().clone(),
                ec_config: ErasureConfig { k: 8, m: 6 },
                http: reqwest::Client::new(),
            };
            client = ArkelClient::new(cfg, store_dir).await?;
            (async {
                let data = tokio::fs::read(&file).await?;
                let etag = client
                    .put_object(&bucket, &key, &data, &parse_targets(&storage_addrs)?)
                    .await?;
                println!("{etag}");
                Ok(())
            })
            .await
        }
        ClientCmd::Get {
            bucket,
            key,
            index_addrs,
            storage_addrs,
            output,
        } => {
            let cfg = ClientConfig {
                index_addrs,
                secret_key: identity.secret_key().clone(),
                ec_config: ErasureConfig { k: 8, m: 6 },
                http: reqwest::Client::new(),
            };
            client = ArkelClient::new(cfg, store_dir).await?;
            (async {
                let data = client
                    .get_object(&bucket, &key, &parse_targets(&storage_addrs)?)
                    .await?;
                match output {
                    Some(path) => tokio::fs::write(path, data).await?,
                    None => std::io::stdout().write_all(&data)?,
                }
                Ok(())
            })
            .await
        }
        ClientCmd::Rm {
            bucket,
            key,
            index_addrs,
        } => {
            let cfg = ClientConfig {
                index_addrs,
                secret_key: identity.secret_key().clone(),
                ec_config: ErasureConfig { k: 8, m: 6 },
                http: reqwest::Client::new(),
            };
            client = ArkelClient::new(cfg, store_dir).await?;
            (async {
                client.delete_object(&bucket, &key).await?;
                println!("deleted {bucket}/{key}");
                Ok(())
            })
            .await
        }
    };

    // Always close the endpoint, even on error, so in-flight shard pushes
    // finish cleanly and no "Endpoint dropped" warning is logged.
    client.endpoint.close().await;
    result
}

async fn run_repair(
    base_dir: PathBuf,
    identity: &NodeIdentity,
    index_addrs: Vec<String>,
    register: bool,
    ec_config: ErasureConfig,
    rate_limit: usize,
) -> Result<()> {
    let store_dir = base_dir.join("blobs");
    let cfg = ClientConfig {
        index_addrs,
        secret_key: identity.secret_key().clone(),
        ec_config,
        http: reqwest::Client::new(),
    };
    let client = ArkelClient::new(cfg, store_dir).await?;
    let result = arkel::repair::run(&client, register, rate_limit).await;
    client.endpoint.close().await;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let node_cfg = match &cli.config {
        Some(p) => arkel::config::NodeConfig::load(p)?,
        None => Default::default(),
    };
    let cfg_index = node_cfg.index_nodes.clone().unwrap_or_default();
    let cfg_storage = node_cfg.storage.clone().unwrap_or_default();
    let cli_data_dir = cli.data_dir.clone();

    match cli.command {
        Commands::Client { cmd } => {
            let base = cli_data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./.arkel_client_data"));
            let arkel = Arkel::init(base).await?;
            return run_client(arkel.data_dir.clone(), &arkel.identity, cmd).await;
        }
        Commands::Account { cmd } => {
            let base = cli_data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./.arkel_account_data"));
            let arkel = Arkel::init(base).await?;
            return run_account(&arkel.identity, cmd).await;
        }
        Commands::Payment { cmd } => {
            let base = cli_data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./.arkel_payment_data"));
            let arkel = Arkel::init(base).await?;
            return run_payment(&arkel.identity, cmd).await;
        }
        Commands::Repair {
            index_addrs,
            register,
            k,
            m,
            rate_limit,
        } => {
            let base = cli_data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./.arkel_repair_data"));
            let arkel = Arkel::init(base.clone()).await?;
            return run_repair(
                base,
                &arkel.identity,
                index_addrs,
                register,
                erasure_config(k, m),
                rate_limit,
            )
            .await;
        }
        Commands::Index {
            http_addr,
            peer_addresses,
        } => {
            let http_addr =
                arkel::config::resolve_addr(http_addr, cfg_index.http_addr, "127.0.0.1:8001")?;
            let peers = peer_addresses.or(cfg_index.peers).unwrap_or_default();
            let base = cli_data_dir
                .clone()
                .or(cfg_index.data_dir.clone().map(PathBuf::from))
                .unwrap_or_else(|| {
                    PathBuf::from(format!("./.arkel_index_{}_data", http_addr.port()))
                });
            let arkel = Arkel::init(base).await?;
            let node_id = arkel.identity.raft_node_id();
            let my_full_addr = arkel.identity.raft_full_addr(http_addr);

            // Filter peers to avoid self-referential network loops
            let filtered_peers: Vec<String> = peers
                .into_iter()
                .filter(|addr| addr != &my_full_addr)
                .collect();

            tracing::info!("==================================================");
            tracing::info!("Arkel Index Node Initialized");
            tracing::info!("Local Node ID : {}", node_id);
            tracing::info!("Full Address  : {}", my_full_addr);
            for peer in &filtered_peers {
                tracing::info!("Remote Peer   : {}", peer);
            }
            tracing::info!("==================================================");

            return arkel
                .run(NodeMode::Index {
                    bootstrap: BootstrapConfig {
                        peer_addresses: filtered_peers,
                    },
                    http_addr,
                })
                .await;
        }
        Commands::Storage {
            private_relay_url,
            index_addrs,
            addr,
            advertise_addr,
            gc_interval_secs,
            capacity,
        } => {
            let addr = arkel::config::resolve_addr(addr, cfg_storage.addr, "127.0.0.1:9001")?;
            let advertise_addr = advertise_addr.or_else(|| {
                cfg_storage
                    .advertise_addr
                    .as_deref()
                    .and_then(|s| s.parse::<SocketAddr>().ok())
            });
            let index_addrs = index_addrs.or(cfg_storage.index_addrs).unwrap_or_else(|| {
                arkel::cli::DEFAULT_INDEX_ADDRS
                    .split(',')
                    .map(String::from)
                    .collect()
            });
            let gc_interval_secs = gc_interval_secs
                .or(cfg_storage.gc_interval_secs)
                .unwrap_or(3600);
            let capacity = capacity
                .or_else(|| {
                    cfg_storage
                        .capacity
                        .as_deref()
                        .and_then(|s| arkel::config::parse_size(s).ok())
                })
                .unwrap_or(1_000_000_000_000); // 1 TB default
            let base = cli_data_dir
                .clone()
                .or(cfg_storage.data_dir.clone().map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("./.arkel_storage_data"));
            let arkel = Arkel::init(base.clone()).await?;
            let storage_key = arkel.identity.secret_key().clone();

            let blob_dir = base.join("blobs");
            let store: iroh_blobs::api::Store = {
                use iroh_blobs::store::{GcConfig, ProtectCb, ProtectOutcome};
                use std::collections::HashSet;

                let mut options = iroh_blobs::store::fs::options::Options::new(&blob_dir);
                let store_cell: Arc<tokio::sync::OnceCell<iroh_blobs::api::Store>> =
                    Arc::new(tokio::sync::OnceCell::new());
                let cell = store_cell.clone();
                let idx_addrs = index_addrs.clone();
                let http = reqwest::Client::new();
                let storage_key = storage_key.clone();
                // The GC callback runs on iroh-blobs' internal runtime, which has
                // IO disabled — so all network/file IO must be spawned onto the
                // main runtime and awaited here.
                let main_handle = tokio::runtime::Handle::current();
                let cb: ProtectCb = Arc::new(move |live: &mut HashSet<iroh_blobs::Hash>| {
                    let cell = cell.clone();
                    let idx_addrs = idx_addrs.clone();
                    let http = http.clone();
                    let storage_key = storage_key.clone();
                    let main_handle = main_handle.clone();
                    Box::pin(async move {
                        let store = match cell.get() {
                            Some(s) => s.clone(),
                            None => return ProtectOutcome::Abort,
                        };
                        let all = match main_handle
                            .spawn(async move { store.blobs().list().hashes().await })
                            .await
                        {
                            Ok(Ok(h)) => h,
                            Ok(Err(e)) => {
                                tracing::warn!("GC: list blobs failed: {e}");
                                return ProtectOutcome::Abort;
                            }
                            Err(e) => {
                                tracing::warn!("GC: list blobs task failed: {e}");
                                return ProtectOutcome::Abort;
                            }
                        };
                        if all.is_empty() {
                            return ProtectOutcome::Continue;
                        }
                        let hashes32: Vec<[u8; 32]> = all.iter().map(|h| *h.as_bytes()).collect();
                        let candidates = match main_handle
                            .spawn({
                                let http = http.clone();
                                let idx_addrs = idx_addrs.clone();
                                let storage_key = storage_key.clone();
                                async move {
                                    arkel::index::client::gc_candidates(
                                        &http,
                                        &idx_addrs,
                                        &hashes32,
                                        &storage_key,
                                    )
                                    .await
                                }
                            })
                            .await
                        {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                tracing::warn!("GC: candidate query failed: {e}");
                                return ProtectOutcome::Abort;
                            }
                            Err(e) => {
                                tracing::warn!("GC: candidate query task failed: {e}");
                                return ProtectOutcome::Abort;
                            }
                        };
                        let doomed: HashSet<[u8; 32]> = candidates
                            .iter()
                            .filter_map(|c| <[u8; 32]>::try_from(c.as_slice()).ok())
                            .collect();
                        for h in &all {
                            if !doomed.contains(h.as_bytes()) {
                                live.insert(*h);
                            }
                        }
                        tracing::info!(
                            "GC: protecting {} blobs, {} unreferenced",
                            live.len(),
                            doomed.len()
                        );
                        ProtectOutcome::Continue
                    })
                });
                options.gc = Some(GcConfig {
                    interval: std::time::Duration::from_secs(gc_interval_secs),
                    add_protected: Some(cb),
                });
                let store: iroh_blobs::api::Store = iroh_blobs::store::fs::FsStore::load_with_opts(
                    blob_dir.join("blobs.db"),
                    options,
                )
                .await?
                .into();
                let _ = store_cell.set(store.clone());
                store
            };
            let blobs = iroh_blobs::BlobsProtocol::new(&store, None);

            let shard_dir = base.join("shards");
            tokio::fs::create_dir_all(&shard_dir)
                .await
                .context("Failed to create shard directory")?;

            return arkel
                .run(NodeMode::Storage {
                    base_dir: base,
                    blobs,
                    store,
                    private_relay_url,
                    index_addrs,
                    addr,
                    advertise_addr,
                    capacity_bytes: capacity,
                })
                .await;
        }
    }
}
