//! Optional node config file (`--config arkel-node.toml`).
//!
//! Command-line flags override file values. Only `index` and `storage` read it.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Debug, Default, Clone, Deserialize)]
pub struct NodeConfig {
    pub index_nodes: Option<IndexConfig>,
    pub storage: Option<StorageConfig>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct IndexConfig {
    pub http_addr: Option<String>,
    pub data_dir: Option<String>,
    pub peers: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct StorageConfig {
    pub addr: Option<String>,
    pub advertise_addr: Option<String>,
    pub data_dir: Option<String>,
    pub index_addrs: Option<Vec<String>>,
    /// Physical allocation this node commits to the network, e.g. "1TB"
    /// (same syntax as --capacity).
    pub capacity: Option<String>,
    pub gc_interval_secs: Option<u64>,
}

impl NodeConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        toml::from_str(&text).context("failed to parse config")
    }
}

/// Parse `"1TB" | "500GB" | "1G" | "1048576"` into bytes.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_ascii_uppercase();
    let (num, mult) = if let Some(suffix) = ["KB", "MB", "GB", "TB"]
        .into_iter()
        .find(|sfx| s.ends_with(sfx))
    {
        let m = match suffix {
            "KB" => 1024u64,
            "MB" => 1024 * 1024,
            "GB" => 1024 * 1024 * 1024,
            "TB" => 1024 * 1024 * 1024 * 1024,
            _ => unreachable!(),
        };
        (&s[..s.len() - 2], m)
    } else {
        match s.chars().last() {
            Some('K') => (&s[..s.len() - 1], 1024u64),
            Some('M') => (&s[..s.len() - 1], 1024 * 1024),
            Some('G') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
            Some('T') => (&s[..s.len() - 1], 1024 * 1024 * 1024 * 1024),
            _ => (s.as_str(), 1),
        }
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("bad size: {s}"))?;
    n.checked_mul(mult)
        .ok_or_else(|| anyhow::anyhow!("size overflow: {s}"))
}

/// Resolve a CLI `Option` against a config value, then a built-in default.
/// CLI flag wins; then config file; then `default`.
pub fn resolve<T: Clone>(cli: Option<T>, config: Option<T>, default: T) -> T {
    cli.or(config).unwrap_or(default)
}

/// Resolve an addr flag (string in config) into a `SocketAddr`.
pub fn resolve_addr(
    cli: Option<SocketAddr>,
    config: Option<String>,
    default: &str,
) -> Result<SocketAddr> {
    Ok(cli
        .or_else(|| config.as_deref().map(|s| s.parse().ok()).flatten())
        .unwrap_or_else(|| default.parse().expect("valid default addr")))
}
