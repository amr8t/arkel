use anyhow::{Context, Result};
use arkel::{
    Arkel, BootstrapConfig, NodeMode,
    client::sdk::{Client as ArkelClient, ClientConfig},
    dataplane::{ErasureConfig, StorageTarget},
    identity::NodeIdentity,
};
use clap::{Parser, Subcommand};
use iroh::PublicKey;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

const DEFAULT_INDEX_ADDRS: &str =
    "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003";

#[derive(Parser, Debug)]
#[command(
    name = "arkel",
    version,
    about = "Arkel Distributed Blob Index & Storage Node"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Dedicated data directory for this specific node's cryptographic identities and storage state
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Boot up as a metadata consensus node managing the global catalog ring
    Index {
        /// The network socket address this node will bind its HTTP interface to
        #[arg(long, default_value = "127.0.0.1:8001")]
        http_addr: SocketAddr,

        /// Seed nodes to cluster with if initializing or expanding the topology
        #[arg(long, value_delimiter = ',')]
        peer_addresses: Vec<String>,
    },
    /// Boot up as a high-throughput raw block storage endpoint
    Storage {
        /// Optional private Iroh relay architecture URL override
        #[arg(long)]
        private_relay_url: Option<String>,

        /// Index node HTTP URLs to register against (comma-separated; the
        /// registrar discovers the current Raft leader among them)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,

        /// Address the iroh QUIC endpoint binds to
        #[arg(long, default_value = "127.0.0.1:9001")]
        addr: SocketAddr,

        /// Address advertised for registration (defaults to --addr). 
        #[arg(long)]
        advertise_addr: Option<SocketAddr>,
    },
    /// Upload/download objects as an iroh-native client
    Client {
        #[command(subcommand)]
        cmd: ClientCmd,
    },
}

#[derive(Subcommand, Debug)]
enum ClientCmd {
    /// Upload a file (EC + encrypt locally, shards to storage nodes, manifest via Raft)
    Put {
        /// Path to the file to upload
        file: PathBuf,
        #[arg(long, default_value = "default")]
        bucket: String,
        /// Object key; defaults to the file name
        #[arg(long)]
        key: Option<String>,
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
        /// Storage nodes to distribute shards to, as pubkey@ip:port (comma-separated)
        #[arg(long, value_delimiter = ',')]
        storage_addrs: Vec<String>,
    },
    /// Download an object and write it to a file (or stdout)
    Get {
        bucket: String,
        key: String,
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
        /// Storage nodes that may hold shards, as pubkey@ip:port (comma-separated)
        #[arg(long, value_delimiter = ',')]
        storage_addrs: Vec<String>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

fn parse_targets(addrs: &[String]) -> Result<Vec<StorageTarget>> {
    addrs
        .iter()
        .map(|s| {
            let (node_id, addr) = s
                .split_once('@')
                .context("storage-addrs must be <pubkey>@<ip:port>")?;
            Ok(StorageTarget {
                node_id: node_id.parse::<PublicKey>()?,
                addr: addr.parse::<SocketAddr>()?,
                relay_url: None,
            })
        })
        .collect()
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
                ec_config: ErasureConfig { k: 4, m: 2 },
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
                ec_config: ErasureConfig { k: 4, m: 2 },
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
    };

    // Always close the endpoint, even on error, so in-flight shard pushes
    // finish cleanly and no "Endpoint dropped" warning is logged.
    client.endpoint.close().await;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let base_dir = cli.data_dir.unwrap_or_else(|| {
        let suffix = match &cli.command {
            Commands::Index { http_addr, .. } => format!("index_{}", http_addr.port()),
            Commands::Storage { .. } => "storage".to_string(),
            Commands::Client { .. } => "client".to_string(),
        };
        PathBuf::from(format!("./.arkel_{suffix}_data"))
    });

    let arkel = Arkel::init(base_dir.clone()).await?;
    let node_id = arkel.identity.raft_node_id();

    let mode = match cli.command {
        Commands::Client { cmd } => return run_client(base_dir, &arkel.identity, cmd).await,
        Commands::Index {
            http_addr,
            peer_addresses,
        } => {
            let my_full_addr = arkel.identity.raft_full_addr(http_addr);

            // Filter peers to avoid self-referential network loops
            let filtered_peers: Vec<String> = peer_addresses
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

            NodeMode::Index {
                bootstrap: BootstrapConfig {
                    peer_addresses: filtered_peers,
                },
                http_addr,
            }
        }
        Commands::Storage {
            private_relay_url,
            index_addrs,
            addr,
            advertise_addr,
        } => {
            let blob_dir = base_dir.join("blobs");
            let store: iroh_blobs::api::Store = iroh_blobs::store::fs::FsStore::load(&blob_dir)
                .await?
                .into();
            let blobs = iroh_blobs::BlobsProtocol::new(&store, None);

            let shard_dir = base_dir.join("shards");
            tokio::fs::create_dir_all(&shard_dir)
                .await
                .context("Failed to create shard directory")?;

            NodeMode::Storage {
                base_dir,
                blobs,
                store,
                private_relay_url,
                index_addrs,
                addr,
                advertise_addr,
            }
        }
    };

    arkel.run(mode).await
}
