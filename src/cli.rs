//! CLI surface kept in the lib so doc generators can build markdown from the real definitions without re-parsing.

use anyhow::Context;
use clap::{Parser, Subcommand};
use iroh::PublicKey;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::dataplane::StorageTarget;

pub const DEFAULT_INDEX_ADDRS: &str =
    "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003";

#[derive(Parser, Debug)]
#[command(
    name = "arkel",
    version,
    about = "Arkel Distributed Blob Index & Storage Node"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Dedicated data directory for this specific node's cryptographic identities and storage state
    #[arg(long, global = true)]
    pub data_dir: Option<PathBuf>,

    /// Node config file (arkel-node.toml); command-line flags override it
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Boot up as a metadata consensus node managing the global catalog ring
    Index {
        /// The network socket address this node will bind its HTTP interface to
        #[arg(long)]
        http_addr: Option<SocketAddr>,

        /// Seed nodes to cluster with if initializing or expanding the topology
        #[arg(long, value_delimiter = ',')]
        peer_addresses: Option<Vec<String>>,
    },
    /// Boot up as a high-throughput raw block storage endpoint
    Storage {
        /// Optional private Iroh relay architecture URL override
        #[arg(long)]
        private_relay_url: Option<String>,

        /// Index node HTTP URLs to register against (comma-separated; the
        /// registrar discovers the current Raft leader among them)
        #[arg(long, value_delimiter = ',')]
        index_addrs: Option<Vec<String>>,

        /// Address the iroh QUIC endpoint binds to
        #[arg(long)]
        addr: Option<SocketAddr>,

        /// Address advertised for registration (defaults to --addr).
        #[arg(long)]
        advertise_addr: Option<SocketAddr>,

        /// How often to scan and delete unreferenced shards (seconds).
        #[arg(long)]
        gc_interval_secs: Option<u64>,
    },
    /// Upload/download objects as an iroh-native client
    Client {
        #[command(subcommand)]
        cmd: ClientCmd,
    },
    /// Account & quota tools
    Account {
        #[command(subcommand)]
        cmd: AccountCmd,
    },
    /// Payment-operator tooling (register the quota credit key; first-wins)
    Payment {
        #[command(subcommand)]
        cmd: PaymentCmd,
    },
    /// Heal objects below target k/m (standalone, idempotent; run by cron)
    Repair {
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
        /// Register this identity as the repair operator (one-time), then exit
        #[arg(long)]
        register: bool,
        /// Re-encode target data shards (default 8)
        #[arg(long, default_value_t = 8)]
        k: u8,
        /// Re-encode target parity shards (default 6)
        #[arg(long, default_value_t = 6)]
        m: u8,
        /// Max objects fixed per run (rate limit)
        #[arg(long, default_value_t = 600)]
        rate_limit: usize,
    },
}

#[derive(Subcommand, Debug)]
pub enum ClientCmd {
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
    /// Delete an object (removes its manifest via Raft; shards freed by GC)
    Rm {
        bucket: String,
        key: String,
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum AccountCmd {
    /// Show quota and usage for an account (default: this identity)
    Quota {
        /// Account (hex iroh pubkey); defaults to this node's identity
        #[arg(long)]
        account: Option<String>,
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum PaymentCmd {
    /// Register this identity as the payment operator (one-time, first-wins)
    Register {
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
    },
    /// Credit quota to an account (idempotent on --ref-id)
    Credit {
        /// Account (hex iroh pubkey) to credit
        #[arg(long)]
        account: String,
        /// Bytes of quota to add
        #[arg(long)]
        bytes: u64,
        /// Credit source label (e.g. 'payment')
        #[arg(long, default_value = "payment")]
        source: String,
        /// Idempotency reference (e.g. Stripe checkout id); replay is a no-op
        #[arg(long)]
        ref_id: String,
        /// Index node HTTP URLs (comma-separated)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_INDEX_ADDRS)]
        index_addrs: Vec<String>,
    },
}

pub fn parse_targets(addrs: &[String]) -> anyhow::Result<Vec<StorageTarget>> {
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

pub fn erasure_config(k: u8, m: u8) -> crate::dataplane::ErasureConfig {
    crate::dataplane::ErasureConfig {
        k: k as usize,
        m: m as usize,
    }
}
