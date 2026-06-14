use anyhow::Result;
use arkel::{Arkel, BootstrapConfig, NodeMode};
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

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
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let base_dir = cli.data_dir.unwrap_or_else(|| {
        let suffix = match &cli.command {
            Commands::Index { http_addr, .. } => format!("index_{}", http_addr.port()),
            Commands::Storage { .. } => "storage".to_string(),
        };
        PathBuf::from(format!("./.arkel_{suffix}_data"))
    });

    let arkel = Arkel::init(base_dir.clone()).await?;

    let mode = match cli.command {
        Commands::Index {
            http_addr,
            peer_addresses,
        } => {
            // 1. Derive identity and NodeID
            // Using the public key to generate the unique identifier for OpenRaft
            let pub_key = arkel.identity.secret_key().public();
            let node_id_bytes = pub_key.as_bytes();

            // OpenRaft requires a u64 for its internal node identification
            let node_id = u64::from_le_bytes(
                node_id_bytes[..8]
                    .try_into()
                    .expect("PublicKey must be at least 8 bytes"),
            );

            // 2. Format the local address as required by Iroh's cryptographic handshake
            // Format: <iroh_public_key>@<ip:port>
            let my_full_addr = format!("{}@{}", pub_key, http_addr);

            // 3. Filter peers to avoid self-referential network loops
            // We compare against the fully qualified address string
            let filtered_peers: Vec<String> = peer_addresses
                .into_iter()
                .filter(|addr| addr != &my_full_addr)
                .collect();

            // 4. Log startup information for debugging connectivity
            tracing::info!("==================================================");
            tracing::info!("Arkel Index Node Initialized");
            tracing::info!("Local Node ID : {}", node_id);
            tracing::info!("Full Address  : {}", my_full_addr);
            for peer in &filtered_peers {
                tracing::info!("Remote Peer   : {}", peer);
            }
            tracing::info!("==================================================");

            // 5. Construct the Index mode for the Arkel engine
            NodeMode::Index {
                node_id,
                bootstrap: BootstrapConfig {
                    peer_addresses: filtered_peers,
                },
                http_addr: http_addr.to_string(),
            }
        }
        Commands::Storage { private_relay_url } => {
            let blob_dir = base_dir.join("blobs");
            let db = iroh_blobs::store::fs::FsStore::load(&blob_dir).await?;
            let blobs = iroh_blobs::BlobsProtocol::new(&db, None);

            NodeMode::Storage {
                blobs,
                private_relay_url,
            }
        }
    };

    arkel.run(mode).await
}
