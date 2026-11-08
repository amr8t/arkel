use anyhow::Result;

use crate::identity::NodeIdentity;
use crate::index::remote;

/// Quota tooling: query the quota for an identity (default: this identity).
pub async fn run(
    identity: &NodeIdentity,
    index_addrs: Vec<String>,
    query: Option<String>,
) -> Result<()> {
    let query = query.unwrap_or_else(|| hex::encode(identity.node_id().as_bytes()));
    let http = reqwest::Client::new();
    let (total, used) = remote::quota(&http, &index_addrs, &query, identity.secret_key()).await?;
    println!("{query}: {used} / {total} bytes used");
    Ok(())
}
