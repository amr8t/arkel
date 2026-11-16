//! Minimal S3-compatible HTTP gateway in front of [`crate::client::Client`].
//!
//! Deliberately small: only the operations backup tools (Duplicati, restic,
//! rclone) need, flat listings, and no multipart / range / SigV4. Auth here is
//! network topology (bind to loopback or a private container network), not code
//! — clients can use dummy non-blank creds.

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::client::Client;

struct S3State {
    client: Client,
}

const XML_HEAD: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
const S3NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const LOCATION: &str = "us-east-1";

/// Bucket subresources we answer with a stub instead of treating as a listing.
const SUBRESOURCES: &[&str] = &[
    "acl",
    "versioning",
    "policy",
    "tagging",
    "cors",
    "lifecycle",
    "replication",
    "encryption",
    "object-lock",
    "uploads",
];

/// Serve until the process is stopped.
pub async fn serve(client: Client, listen: SocketAddr, max_object_size: usize) -> Result<()> {
    let app = Router::new()
        .route("/", get(list_buckets))
        // Bucket routes, plus the `/bucket/` form some clients (restic) send.
        .route(
            "/:bucket",
            get(list_objects)
                .put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket),
        )
        .route(
            "/:bucket/",
            get(list_objects)
                .put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket),
        )
        .route(
            "/:bucket/*key",
            get(get_object)
                .put(put_object)
                .head(head_object)
                .delete(delete_object),
        )
        // Bounds memory: the data plane buffers the whole object (~4x peak RSS).
        .layer(DefaultBodyLimit::max(max_object_size))
        .layer(middleware::from_fn(log_request))
        .with_state(Arc::new(S3State { client }));

    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(
        "S3 gateway listening on http://{listen} (no SigV4; max object {max_object_size} bytes)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn log_request(req: Request, next: Next) -> Response {
    let (method, path) = (req.method().clone(), req.uri().path().to_string());
    if std::env::var("ARKEL_S3_LOG_HEADERS").is_ok() {
        let hdrs: Vec<String> = req
            .headers()
            .iter()
            .map(|(k, v)| format!("{k}={}", v.to_str().unwrap_or("<bin>")))
            .collect();
        tracing::info!("s3 {method} {path} headers: {}", hdrs.join("; "));
    }
    let resp = next.run(req).await;
    let status = resp.status();
    if std::env::var("ARKEL_S3_LOG_HEADERS").is_ok() {
        let hdrs: Vec<String> = resp
            .headers()
            .iter()
            .map(|(k, v)| format!("{k}={}", v.to_str().unwrap_or("<bin>")))
            .collect();
        tracing::info!("s3 {method} {path} -> {status} resp: {}", hdrs.join("; "));
    }
    // Failures at INFO (visible by default), successes at DEBUG (volume).
    if status.is_client_error() || status.is_server_error() {
        tracing::info!("s3 {method} {path} -> {status}");
    } else {
        tracing::debug!("s3 {method} {path} -> {status}");
    }
    resp
}

// ---------------------------------------------------------------- handlers

async fn list_buckets(State(st): State<Arc<S3State>>) -> Response {
    match st.client.list_buckets().await {
        Ok(names) => {
            let mut body = format!(
                "{XML_HEAD}<ListAllMyBucketsResult xmlns=\"{S3NS}\"><Owner><ID>arkel</ID><DisplayName>arkel</DisplayName></Owner><Buckets>"
            );
            for name in names {
                body.push_str(&format!(
                    "<Bucket><Name>{}</Name><CreationDate>1970-01-01T00:00:00.000Z</CreationDate></Bucket>",
                    esc(&name)
                ));
            }
            body.push_str("</Buckets></ListAllMyBucketsResult>");
            xml(StatusCode::OK, body)
        }
        Err(e) => map_err(e),
    }
}

/// Buckets are created implicitly by the index on first put, so this is a stub.
async fn create_bucket() -> Response {
    xml(StatusCode::OK, String::new())
}

/// Always "exists": buckets materialise on first put, and a 404 here makes
/// clients refuse to upload rather than create-and-continue.
async fn head_bucket() -> Response {
    StatusCode::OK.into_response()
}

async fn delete_bucket() -> Response {
    s3_err(
        StatusCode::NOT_IMPLEMENTED,
        "NotImplemented",
        "bucket deletion is not supported",
    )
}

async fn list_objects(
    State(st): State<Arc<S3State>>,
    Path(bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    // Bucket subresources: location is consulted by minio-go/aws-sdk on connect.
    if q.contains_key("location") {
        return xml(
            StatusCode::OK,
            format!(
                "{XML_HEAD}<LocationConstraint xmlns=\"{S3NS}\">{LOCATION}</LocationConstraint>"
            ),
        );
    }
    if q.keys().any(|k| SUBRESOURCES.contains(&k.as_str())) {
        return xml(StatusCode::OK, String::new());
    }

    let prefix = q.get("prefix").cloned().unwrap_or_default();
    // max-keys / marker / delimiter are deliberately ignored: the whole matching
    // set is returned with IsTruncated=false. Truncating without a working
    // continuation token makes clients re-upload objects they think are missing.
    match st.client.list_objects(&bucket, &prefix).await {
        Ok(objects) => {
            let mut body = format!(
                "{XML_HEAD}<ListBucketResult xmlns=\"{S3NS}\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
                esc(&bucket),
                esc(&prefix)
            );
            for o in objects {
                body.push_str(&format!(
                    "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    esc(&o.key),
                    rfc3339(o.last_modified),
                    esc(&o.etag),
                    o.size
                ));
            }
            body.push_str("</ListBucketResult>");
            xml(StatusCode::OK, body)
        }
        // Missing bucket (or one owned by another identity) reads as empty.
        Err(e) if forbidden(&e) => xml(
            StatusCode::OK,
            format!(
                "{XML_HEAD}<ListBucketResult xmlns=\"{S3NS}\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>",
                esc(&bucket),
                esc(&prefix)
            ),
        ),
        Err(e) => map_err(e),
    }
}

async fn put_object(
    State(st): State<Arc<S3State>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // SDK-based clients (minio-go/restic, aws-sdk) frame the body as
    // `aws-chunked` with per-chunk signatures. That is transport encoding, not
    // auth: decode it here or the chunk headers get stored as object content.
    let data = match unframe_body(&headers, &body) {
        Ok(data) => data,
        Err(e) => return s3_err(StatusCode::BAD_REQUEST, "InvalidRequest", &e),
    };
    match st.client.put_object(&bucket, &key, &data, &[]).await {
        Ok(_) => {
            let mut out = HeaderMap::new();
            // No ETag: see the note on get_object.
            // S3 echoes the checksum the client supplied; SDKs (Duplicati's
            // AWS SDK) treat a missing echo as a corrupt upload.
            if let Some(sum) = headers.get("x-amz-checksum-sha256") {
                out.insert(
                    header::HeaderName::from_static("x-amz-checksum-sha256"),
                    sum.clone(),
                );
            }
            (StatusCode::OK, out).into_response()
        }
        Err(e) => map_err(e),
    }
}

/// No `ETag` is served, on any method. S3 defines it as the object's MD5, and
/// clients (Duplicati's AWS SDK) validate downloads against it; but arkel does
/// not store an MD5 — computing one per request would only prove that the
/// response matches itself. Integrity is already checked harder in the read
/// path (`reconstruct_object` verifies BLAKE3 and the AEAD tags). So the header
/// is omitted rather than faked, and clients fall back to size and mtime.
async fn get_object(
    State(st): State<Arc<S3State>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    match st.client.get_object(&bucket, &key, &[]).await {
        Ok(data) => {
            let modified = http_date_now();
            // restic reads pack subtrees with Range requests (offset+length), so
            // range support is required, not an optimisation.
            let Some((start, end)) = headers
                .get(header::RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|spec| slice_range(spec, data.len()))
            else {
                let mut out = HeaderMap::new();
                out.insert(
                    header::CONTENT_TYPE,
                    "application/octet-stream".parse().unwrap(),
                );
                out.insert(header::LAST_MODIFIED, modified.parse().unwrap());
                // `x-amz-checksum-mode: ENABLED` asks for the object checksum;
                // the AWS SDK validates the body against it. Only computed when
                // asked, since it is a full pass over the object.
                if checksum_requested(&headers) {
                    let sum = sha256_b64(&data);
                    out.insert(
                        header::HeaderName::from_static("x-amz-checksum-sha256"),
                        sum.parse().unwrap(),
                    );
                }
                return (StatusCode::OK, out, data).into_response();
            };
            let slice = data[start..end].to_vec();
            let mut out = HeaderMap::new();
            out.insert(
                header::CONTENT_TYPE,
                "application/octet-stream".parse().unwrap(),
            );
            out.insert(header::LAST_MODIFIED, modified.parse().unwrap());
            out.insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end - 1, data.len())
                    .parse()
                    .unwrap(),
            );
            (StatusCode::PARTIAL_CONTENT, out, slice).into_response()
        }
        Err(e) => map_obj_err(e),
    }
}

fn checksum_requested(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("ENABLED"))
}

/// base64(sha256(data)), the S3 `x-amz-checksum-sha256` value.
fn sha256_b64(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    b64(&Sha256::digest(data))
}

fn b64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Parse `Range: bytes=a-b` / `bytes=a-` / `bytes=-n` into a half-open range.
fn slice_range(spec: &str, len: usize) -> Option<(usize, usize)> {
    let spec = spec.trim().strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.trim().is_empty() {
        let n: usize = b.trim().parse().ok()?;
        (len.saturating_sub(n), len)
    } else {
        let start: usize = a.trim().parse().ok()?;
        let end = if b.trim().is_empty() {
            len
        } else {
            b.trim().parse::<usize>().ok()?.saturating_add(1).min(len)
        };
        (start, end)
    };
    (start < end && start < len).then_some((start, end))
}

async fn head_object(
    State(st): State<Arc<S3State>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    // Manifest read only — never fetches shards.
    match st.client.stat_object(&bucket, &key).await {
        // restic's Stat parses Last-Modified (RFC 1123) and retries forever if
        // it is absent, so this header is required, not decorative.
        Ok((size, _etag, created_at)) => (
            StatusCode::OK,
            [
                (header::CONTENT_LENGTH, size.to_string()),
                (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                (header::LAST_MODIFIED, http_date(created_at)),
            ],
        )
            .into_response(),
        Err(e) => map_obj_err(e),
    }
}

async fn delete_object(
    State(st): State<Arc<S3State>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    match st.client.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_obj_err(e),
    }
}

/// Strip `aws-chunked` framing (`<hex-size>;chunk-signature=..\\r\\n<data>\\r\\n`)
/// when the request uses it; chunk signatures are ignored along with all other
/// authentication.
fn unframe_body(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, String> {
    let chunked = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("aws-chunked"));
    if !chunked && !headers.contains_key("x-amz-decoded-content-length") {
        return Ok(body.to_vec());
    }
    let mut out = Vec::with_capacity(body.len());
    let mut i = 0;
    loop {
        let line_end = body[i..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| p + i)
            .ok_or("truncated aws-chunked header")?;
        let header_line =
            std::str::from_utf8(&body[i..line_end]).map_err(|_| "non-utf8 chunk header")?;
        let size_hex = header_line
            .split(';')
            .next()
            .ok_or("bad aws-chunked header")?
            .trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_| "bad chunk size")?;
        i = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        let end = i + size;
        if end > body.len() {
            return Err("truncated aws-chunked body".to_string());
        }
        out.extend_from_slice(&body[i..end]);
        i = (end + 2).min(body.len()); // skip the chunk's trailing CRLF
    }
}

// ---------------------------------------------------------------- rendering

fn xml(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn s3_err(status: StatusCode, code: &str, message: &str) -> Response {
    xml(
        status,
        format!(
            "{XML_HEAD}<Error><Code>{}</Code><Message>{}</Message></Error>",
            esc(code),
            esc(message)
        ),
    )
}

/// Map index/data-plane errors onto S3 status codes that clients understand.
fn map_err(e: anyhow::Error) -> Response {
    map_kind(e, false)
}

/// Object-level mapping: the index reports "bucket not owned / absent" as a 403,
/// but S3 clients must see 404 for a missing key (a 403 makes them abort with
/// "Access Denied" instead of treating the object as absent).
fn map_obj_err(e: anyhow::Error) -> Response {
    map_kind(e, true)
}

fn map_kind(e: anyhow::Error, object: bool) -> Response {
    let m = e.to_string();
    if m.contains("507") {
        s3_err(StatusCode::INSUFFICIENT_STORAGE, "InsufficientStorage", &m)
    } else if forbidden(&e) {
        if object {
            s3_err(StatusCode::NOT_FOUND, "NoSuchKey", &m)
        } else {
            s3_err(StatusCode::FORBIDDEN, "AccessDenied", &m)
        }
    } else if m.contains("404") || m.contains("not found") {
        s3_err(StatusCode::NOT_FOUND, "NoSuchKey", &m)
    } else {
        s3_err(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &m)
    }
}

fn forbidden(e: &anyhow::Error) -> bool {
    let m = e.to_string();
    m.contains("403") || m.contains("forbidden")
}

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

fn http_date_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    http_date(secs)
}

/// Unix seconds → RFC 1123 (HTTP-date), e.g. `Tue, 14 Nov 2023 22:13:20 GMT`.
fn http_date(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let (y, m, d, h, mi, s) = civil(secs);
    // 1970-01-01 was a Thursday; DAYS is rotated so index 0 = Thursday.
    let day = DAYS[(secs / 86_400 % 7) as usize];
    format!(
        "{day}, {d:02} {} {y:04} {h:02}:{mi:02}:{s:02} GMT",
        MONTHS[(m - 1) as usize]
    )
}

/// Unix seconds → RFC 3339 (UTC), without pulling in a date crate.
fn rfc3339(secs: u64) -> String {
    let (y, m, d, h, mi, s) = civil(secs);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.000Z")
}

/// Unix seconds → (year, month, day, hour, minute, second) UTC.
fn civil(secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, h, mi, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_epoch_and_known_date() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(1_700_000_000), "2023-11-14T22:13:20.000Z");
        assert_eq!(rfc3339(1_709_164_800), "2024-02-29T00:00:00.000Z");
    }

    #[test]
    fn http_date_is_rfc1123() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(http_date(1_700_000_000), "Tue, 14 Nov 2023 22:13:20 GMT");
    }

    #[test]
    fn unframes_aws_chunked_body() {
        let mut body = Vec::new();
        for chunk in [b"hello ".as_slice(), b"arkel".as_slice()] {
            body.extend_from_slice(
                format!("{:x};chunk-signature=deadbeef\r\n", chunk.len()).as_bytes(),
            );
            body.extend_from_slice(chunk);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"0;chunk-signature=deadbeef\r\n\r\n");
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-decoded-content-length", "11".parse().unwrap());
        assert_eq!(unframe_body(&headers, &body).unwrap(), b"hello arkel");
        // Plain bodies pass through untouched.
        assert_eq!(unframe_body(&HeaderMap::new(), b"plain").unwrap(), b"plain");
    }

    #[test]
    fn base64_matches_reference() {
        assert_eq!(b64(b""), "");
        assert_eq!(b64(b"f"), "Zg==");
        assert_eq!(b64(b"fo"), "Zm8=");
        assert_eq!(b64(b"foo"), "Zm9v");
        assert_eq!(b64(b"hello arkel"), "aGVsbG8gYXJrZWw=");
        // SHA256("abc") base64, matching `echo -n abc | sha256sum -b | base64`
        assert_eq!(
            sha256_b64(b"abc"),
            "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="
        );
    }

    #[test]
    fn range_slicing() {
        assert_eq!(slice_range("bytes=0-9", 100), Some((0, 10)));
        assert_eq!(slice_range("bytes=90-", 100), Some((90, 100)));
        assert_eq!(slice_range("bytes=-10", 100), Some((90, 100)));
        assert_eq!(slice_range("bytes=95-500", 100), Some((95, 100)));
        assert_eq!(slice_range("bytes=100-", 100), None);
        assert_eq!(slice_range("garbage", 100), None);
    }

    #[test]
    fn xml_escapes() {
        assert_eq!(esc("a&b<c>\"d\""), "a&amp;b&lt;c&gt;&quot;d&quot;");
    }
}
