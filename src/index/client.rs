use anyhow::{Context, Result, bail};

/// Discover the current Raft leader's base URL among the given index node URLs.
///
/// Reads `/raft/metrics` on each node until it finds the one whose `id`
/// matches `current_leader`. No discovery/DHT — just a static candidate list.
pub async fn find_leader(http: &reqwest::Client, index_addrs: &[String]) -> Result<String> {
    for url in index_addrs {
        let resp: serde_json::Value = http
            .get(format!("{url}/raft/metrics"))
            .send()
            .await?
            .json()
            .await?;
        let id = resp["id"].as_u64();
        let leader = resp["current_leader"].as_u64();
        if let (Some(id), Some(leader)) = (id, leader) {
            if id == leader {
                return Ok(url.clone());
            }
        }
    }
    bail!("no index leader found")
}

/// Issue a write to the index cluster, transparently surviving leader changes.
///
/// OpenRaft followers reject `client_write` with `ForwardToLeader`, so this
/// discovers the leader, sends `route` with `method`, and on rejection
/// re-discovers and retries (up to 3 attempts). The 30s heartbeat/retry loop
/// of callers remains the long-term guarantee; this handles the window.
async fn index_send(
    http: &reqwest::Client,
    index_addrs: &[String],
    method: reqwest::Method,
    route: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value> {
    for _ in 0..3 {
        let leader = find_leader(http, index_addrs).await?;
        let resp = http
            .request(method.clone(), format!("{leader}/{route}"))
            .json(payload)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        let body: serde_json::Value = serde_json::from_str(&text).with_context(|| {
            format!(
                "bad response body from {leader}/{route}: status {status}: {:?}",
                &text[..text.len().min(200)]
            )
        })?;
        if body.get("success").and_then(|s| s.as_bool()) == Some(true) {
            return Ok(body);
        }
        tracing::warn!("index write rejected by {leader}; re-discovering leader");
    }
    bail!("index write failed after retries")
}

/// POST a write to the index cluster (used by `POST /register`).
pub async fn index_write(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value> {
    index_send(http, index_addrs, reqwest::Method::POST, route, payload).await
}

/// PUT a write to the index cluster (used by manifest commit, `PUT /manifest/...`).
pub async fn index_put(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value> {
    index_send(http, index_addrs, reqwest::Method::PUT, route, payload).await
}

pub async fn index_read(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
) -> Result<serde_json::Value> {
    let leader = find_leader(http, index_addrs).await?;

    let body: serde_json::Value = http
        .get(format!("{leader}/{route}"))
        .send()
        .await?
        .json()
        .await?;

    Ok(body)
}
