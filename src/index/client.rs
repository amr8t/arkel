use anyhow::{Context, Result, bail};
use std::net::SocketAddr;

/// Discover the current Raft leader's base URL among the given index node URLs.
///
/// Reads `/raft/metrics` on each node until it finds the one whose `id`
/// matches `current_leader`. No discovery/DHT — just a static candidate list.
pub async fn find_leader(http: &reqwest::Client, index_addrs: &[String]) -> Result<String> {
    for url in index_addrs {
        let resp = match http.get(format!("{url}/raft/metrics")).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("index node {url} unreachable: {e}");
                continue; // a dead node must not block leader discovery
            }
        };
        let resp: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("index node {url} bad metrics response: {e}");
                continue;
            }
        };
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

/// Compute the Arkel auth headers (Authorization + X-Arkel-Time) for a request.
fn auth_headers(
    secret_key: &iroh::SecretKey,
    method: &reqwest::Method,
    path: &str,
    body: &[u8],
) -> (String, String) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let payload = format!(
        "{} {} {} {now}",
        method.as_str(),
        path,
        hex::encode(blake3::hash(body).as_bytes())
    );
    let sig = secret_key.sign(payload.as_bytes());
    (
        format!(
            "Arkel {}:{}",
            hex::encode(secret_key.public().as_bytes()),
            hex::encode(sig.to_bytes())
        ),
        now.to_string(),
    )
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
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    for _ in 0..3 {
        let leader = find_leader(http, index_addrs).await?;
        let body = serde_json::to_vec(payload)?;
        let (auth, time) = auth_headers(secret_key, &method, route, &body);
        let resp = http
            .request(method.clone(), format!("{leader}/{route}"))
            .header("authorization", auth)
            .header("x-arkel-time", time)
            .header("content-type", "application/json")
            .body(body)
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
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_send(
        http,
        index_addrs,
        reqwest::Method::POST,
        route,
        payload,
        secret_key,
    )
    .await
}

/// PUT a write to the index cluster (used by manifest commit, `PUT /manifest/...`).
pub async fn index_put(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    payload: &serde_json::Value,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_send(
        http,
        index_addrs,
        reqwest::Method::PUT,
        route,
        payload,
        secret_key,
    )
    .await
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

pub async fn index_delete(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    payload: &serde_json::Value,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_send(
        http,
        index_addrs,
        reqwest::Method::DELETE,
        route,
        payload,
        secret_key,
    )
    .await
}

pub struct HealthyNode {
    pub node_id: iroh::PublicKey,
    pub addr: SocketAddr,
    pub relay_url: Option<String>,
}
pub async fn list_healthy_nodes(
    http: &reqwest::Client,
    index_addrs: &[String],
) -> Result<Vec<HealthyNode>> {
    let body = index_read(http, index_addrs, "nodes").await?;
    let arr = body.as_array().context("GET /nodes expected an array")?;
    arr.iter()
        .map(|n| {
            let node_id = n["node_id"].as_str().context("missing node_id")?;
            let addr = n["addr"].as_str().context("missing addr")?;
            let relay_url = n["relay_url"].as_str().and_then(|s| s.parse().ok());
            Ok(HealthyNode {
                node_id: node_id.parse()?,
                addr: addr.parse()?,
                relay_url,
            })
        })
        .collect()
}

/// Ask the index which of the given shard blob hashes are referenced by no
/// live manifest (refs == 0) — the storage GC candidates. Returns the
/// unreferenced subset as hex strings.
pub async fn gc_candidates(
    http: &reqwest::Client,
    index_addrs: &[String],
    hashes: &[[u8; 32]],
) -> Result<Vec<Vec<u8>>> {
    let query: Vec<String> = hashes.iter().map(hex::encode).collect();
    let body = index_read(
        http,
        index_addrs,
        &format!("shards/gc-candidates?hashes={}", query.join(",")),
    )
    .await?;
    let arr = body
        .as_array()
        .context("GET /shards/gc-candidates expected an array")?;
    Ok(arr
        .iter()
        .filter_map(|v| v.as_str().and_then(|s| hex::decode(s).ok()))
        .collect())
}

/// All (bucket, key, manifest_bytes) — the repair scan source.
pub async fn list_all_manifests(
    http: &reqwest::Client,
    index_addrs: &[String],
) -> Result<Vec<(String, String, Vec<u8>)>> {
    let body = index_read(http, index_addrs, "manifests").await?;
    let arr = body
        .as_array()
        .context("GET /manifests expected an array")?;
    arr.iter()
        .map(|v| {
            Ok((
                v[0].as_str().context("bad bucket")?.to_string(),
                v[1].as_str().context("bad key")?.to_string(),
                serde_json::from_value(v[2].clone()).context("bad manifest_bytes")?,
            ))
        })
        .collect()
}

/// Signed POST to the repair route (re-commits a repaired manifest).
pub async fn repair_commit(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    payload: &serde_json::Value,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_send(
        http,
        index_addrs,
        reqwest::Method::POST,
        route,
        payload,
        secret_key,
    )
    .await
}

/// One-time registration of this identity as the repair operator.
pub async fn set_repair_operator(
    http: &reqwest::Client,
    index_addrs: &[String],
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_write(
        http,
        index_addrs,
        "repair-operator",
        &serde_json::json!({}),
        secret_key,
    )
    .await
}

pub struct NodeStatusInfo {
    pub node_id: iroh::PublicKey,
    pub addr: SocketAddr,
    pub relay_url: Option<String>,
    pub offline: bool,
}
/// All registered nodes with their Online/Offline status (repair health map).
pub async fn list_all_nodes(
    http: &reqwest::Client,
    index_addrs: &[String],
) -> Result<Vec<NodeStatusInfo>> {
    let body = index_read(http, index_addrs, "nodes/all").await?;
    let arr = body
        .as_array()
        .context("GET /nodes/all expected an array")?;
    arr.iter()
        .map(|n| {
            Ok(NodeStatusInfo {
                node_id: n["node_id"].as_str().context("missing node_id")?.parse()?,
                addr: n["addr"].as_str().context("missing addr")?.parse()?,
                relay_url: n["relay_url"].as_str().and_then(|s| s.parse().ok()),
                offline: n["status"].as_str() == Some("Offline"),
            })
        })
        .collect()
}

pub async fn account_quota(
    http: &reqwest::Client,
    index_addrs: &[String],
    account: &str,
) -> Result<(u64, u64)> {
    let body = index_read(
        http,
        index_addrs,
        &format!("account/quota?account={account}"),
    )
    .await?;
    Ok((
        body["total_bytes"].as_u64().unwrap_or(0),
        body["used_bytes"].as_u64().unwrap_or(0),
    ))
}

/// One-time registration of this identity as the payment operator
pub async fn set_payment_operator(
    http: &reqwest::Client,
    index_addrs: &[String],
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_write(
        http,
        index_addrs,
        "payment-operator",
        &serde_json::json!({}),
        secret_key,
    )
    .await
}

/// Signed credit of `bytes` quota to an account (idempotent on `ref_id`).
pub async fn credit_quota(
    http: &reqwest::Client,
    index_addrs: &[String],
    account_id: &str,
    bytes: u64,
    source: &str,
    ref_id: &str,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_send(
        http,
        index_addrs,
        reqwest::Method::POST,
        "account/quota",
        &serde_json::json!({
            "account_id": account_id,
            "bytes": bytes,
            "source": source,
            "ref_id": ref_id,
        }),
        secret_key,
    )
    .await
}

/// Payment operator sets the cluster-wide default quota (bytes) for accounts
/// with no quota row yet. Idempotent upsert — can be re-issued to change it.
pub async fn set_default_quota(
    http: &reqwest::Client,
    index_addrs: &[String],
    total_bytes: u64,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_write(
        http,
        index_addrs,
        "quota/default",
        &serde_json::json!({ "total_bytes": total_bytes }),
        secret_key,
    )
    .await
}
