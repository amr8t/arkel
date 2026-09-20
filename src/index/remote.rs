use anyhow::{Context, Result, bail};
use std::net::SocketAddr;

/// Expand an index base URL into concrete candidate URLs to probe.
async fn candidate_urls(url: &str) -> Vec<String> {
    let parsed = match reqwest::Url::parse(url) {
        Ok(u) => u,
        Err(_) => return vec![url.to_string()],
    };
    let host = match parsed.host_str() {
        Some(h) => h.to_string(),
        None => return vec![url.to_string()],
    };
    if host.parse::<std::net::IpAddr>().is_ok() {
        return vec![url.to_string()];
    }
    let port = parsed.port_or_known_default().unwrap_or(80);
    match tokio::net::lookup_host((host.as_str(), port)).await {
        Ok(addrs) => {
            let mut out: Vec<String> = addrs
                .map(|a| format!("{}://{}", parsed.scheme(), a))
                .collect();
            out.sort();
            out.dedup();
            if out.is_empty() {
                vec![url.to_string()]
            } else {
                out
            }
        }
        Err(e) => {
            tracing::warn!("index host {host} did not resolve: {e}");
            vec![url.to_string()]
        }
    }
}

/// Discover the current Raft leader's base URL among the given index node URLs.
///
/// Reads `/raft/metrics` on each candidate until it finds the one whose `id`
/// matches `current_leader`. Entries may be individual nodes or a DNS name that
/// resolves to several nodes. No discovery/DHT — just a static candidate list.
pub async fn find_leader(http: &reqwest::Client, index_addrs: &[String]) -> Result<String> {
    for url in index_addrs {
        for base in candidate_urls(url).await {
            let resp = match http.get(format!("{base}/raft/metrics")).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("index node {base} unreachable: {e}");
                    continue; // a dead node must not block leader discovery
                }
            };
            let resp: serde_json::Value = match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("index node {base} bad metrics response: {e}");
                    continue;
                }
            };
            let id = resp["id"].as_u64();
            let leader = resp["current_leader"].as_u64();
            if let (Some(id), Some(leader)) = (id, leader) {
                if id == leader {
                    return Ok(base);
                }
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

/// Percent-encode each path segment (RFC 3986), preserving `/` separators so
/// object keys may contain slashes.
fn percent_encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            let mut out = String::with_capacity(segment.len());
            for b in segment.bytes() {
                match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                        out.push(b as char)
                    }
                    _ => out.push_str(&format!("%{b:02X}")),
                }
            }
            out
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Encode the path portion of a route, leaving any `?query` untouched.
fn percent_encode_route(route: &str) -> String {
    match route.split_once('?') {
        Some((path, query)) => format!("{}?{query}", percent_encode_path(path)),
        None => percent_encode_path(route),
    }
}

/// Parse a response body as JSON, turning non-2xx or empty bodies into a clear
/// error instead of serde's "EOF while parsing a value".
async fn json_response(resp: reqwest::Response, context: &str) -> Result<serde_json::Value> {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail = text.trim();
        if detail.is_empty() {
            bail!("{context}: HTTP {status}");
        }
        bail!("{context}: HTTP {status}: {detail}");
    }
    serde_json::from_str(&text).with_context(|| {
        format!(
            "{context}: invalid JSON response: {:?}",
            &text[..text.len().min(200)]
        )
    })
}

fn retryable_index_error(error: &str) -> bool {
    error.contains("ForwardToLeader") || error.contains("raft batch error")
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
    let url_route = percent_encode_route(route);
    let mut last_error = None;
    for _ in 0..3 {
        let leader = find_leader(http, index_addrs).await?;
        let body = serde_json::to_vec(payload)?;
        let (auth, time) = auth_headers(secret_key, &method, route, &body);
        let resp = http
            .request(method.clone(), format!("{leader}/{url_route}"))
            .header("authorization", auth)
            .header("x-arkel-time", time)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("{method} {route}: HTTP {status}: {}", text.trim());
        }
        let value: serde_json::Value = serde_json::from_str(&text).with_context(|| {
            format!(
                "{method} {route}: invalid JSON response: {:?}",
                &text[..text.len().min(200)]
            )
        })?;
        if value.get("success").and_then(|s| s.as_bool()) == Some(true) {
            return Ok(value);
        }
        let error = value
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        if !retryable_index_error(error) {
            bail!("{method} {route} rejected: {error}");
        }
        tracing::warn!("index write rejected by {leader} ({error}); re-discovering leader");
        last_error = Some(error.to_string());
    }
    bail!(
        "{method} {route} failed after retries: {}",
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )
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

/// Issue a read GET to the index cluster, transparently surviving leader changes.
///
/// Mirrors `index_send` for writes: on a `503 Service Unavailable` (a node that
/// lost its leader lease or stepped down between discovery and request) or a
/// transport error, re-discovers the leader and retries (up to 3 attempts).
/// Other non-2xx statuses (401/403/404) are authoritative and returned as-is.
///
/// When `signed` is `Some((secret_key, sign_path))`, the request carries the
/// Arkel auth headers signing `sign_path` (which may differ from `url` when the
/// URL carries a query string).
async fn index_get(
    http: &reqwest::Client,
    index_addrs: &[String],
    url: &str,
    signed: Option<(&iroh::SecretKey, &str)>,
) -> Result<reqwest::Response> {
    for _ in 0..3 {
        let leader = find_leader(http, index_addrs).await?;
        let mut req = http.get(format!("{leader}/{}", percent_encode_route(url)));
        if let Some((secret_key, sign_path)) = signed {
            let (auth, time) = auth_headers(secret_key, &reqwest::Method::GET, sign_path, &[]);
            req = req
                .header("authorization", auth)
                .header("x-arkel-time", time);
        }
        match req.send().await {
            Ok(resp) if resp.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                return Ok(resp);
            }
            Ok(resp) => {
                tracing::warn!(
                    "index read rejected by {leader} ({}); re-discovering leader",
                    resp.status()
                );
            }
            Err(e) => {
                tracing::warn!("index read error from {leader}: {e}; re-discovering leader");
            }
        }
    }
    bail!("index read failed after retries")
}

pub async fn index_read(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
) -> Result<serde_json::Value> {
    let resp = index_get(http, index_addrs, route, None).await?;
    json_response(resp, &format!("GET {route}")).await
}

/// Signed GET for privacy-gated metadata reads (manifest, listings). The
/// signature covers the empty body, matching the server's `verify_request`.
pub async fn index_read_signed(
    http: &reqwest::Client,
    index_addrs: &[String],
    route: &str,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    index_read_signed_url(http, index_addrs, route, route, secret_key).await
}

/// Signed GET where the signing path differs from the URL — query strings are
/// NOT part of the signature, so routes carrying `?query=` sign the bare path.
async fn index_read_signed_url(
    http: &reqwest::Client,
    index_addrs: &[String],
    sign_path: &str,
    url: &str,
    secret_key: &iroh::SecretKey,
) -> Result<serde_json::Value> {
    let resp = index_get(http, index_addrs, url, Some((secret_key, sign_path))).await?;
    json_response(resp, &format!("GET {url}")).await
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
    pub capacity_bytes: u64,
    pub occupied_bytes: u64,
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
            Ok(HealthyNode {
                node_id: node_id.parse()?,
                addr: addr.parse()?,
                capacity_bytes: n["capacity_bytes"].as_u64().unwrap_or(0),
                occupied_bytes: n["occupied_bytes"].as_u64().unwrap_or(0),
            })
        })
        .collect()
}

/// Ask the index which of the given shard blob hashes `node` may delete —
/// Storj-style reconciliation scoped to the node's own placements + refs.
/// Signed by the storage node's identity.
pub async fn gc_candidates(
    http: &reqwest::Client,
    index_addrs: &[String],
    hashes: &[[u8; 32]],
    secret_key: &iroh::SecretKey,
) -> Result<Vec<Vec<u8>>> {
    let query: Vec<String> = hashes.iter().map(hex::encode).collect();
    let body = index_read_signed_url(
        http,
        index_addrs,
        "shards/gc-candidates",
        &format!("shards/gc-candidates?hashes={}", query.join(",")),
        secret_key,
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
/// All (bucket, key, manifest_bytes) — the repair scan source. Repair-operator
/// only; signed by the repair identity.
pub async fn list_all_manifests(
    http: &reqwest::Client,
    index_addrs: &[String],
    secret_key: &iroh::SecretKey,
) -> Result<Vec<(String, String, Vec<u8>)>> {
    let body = index_read_signed(http, index_addrs, "manifests", secret_key).await?;
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
                offline: n["status"].as_str() == Some("Offline"),
            })
        })
        .collect()
}

pub async fn account_quota(
    http: &reqwest::Client,
    index_addrs: &[String],
    account: &str,
    secret_key: &iroh::SecretKey,
) -> Result<(u64, u64)> {
    let body = index_read_signed_url(
        http,
        index_addrs,
        "account/quota",
        &format!("account/quota?account={account}"),
        secret_key,
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

#[cfg(test)]
mod tests {
    use super::candidate_urls;

    #[tokio::test]
    async fn candidate_urls_keeps_ip_literal() {
        assert_eq!(
            candidate_urls("http://10.0.0.2:8001").await,
            vec!["http://10.0.0.2:8001".to_string()]
        );
    }

    #[tokio::test]
    async fn candidate_urls_keeps_unparseable() {
        assert_eq!(
            candidate_urls("not a url").await,
            vec!["not a url".to_string()]
        );
    }

    #[tokio::test]
    async fn candidate_urls_expands_hostname() {
        // localhost resolves to at least 127.0.0.1 on the test host.
        let urls = candidate_urls("http://localhost:8001").await;
        assert!(
            urls.iter().any(|u| u == "http://127.0.0.1:8001"),
            "expected a 127.0.0.1 candidate, got {urls:?}"
        );
    }
}
