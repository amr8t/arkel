pub fn verify_request(
    method: &str,
    path: &str,
    body: &[u8],
    headers: &axum::http::HeaderMap,
) -> Result<iroh::PublicKey, String> {
    let time = headers.get("x-arkel-time").and_then(|v| v.to_str().ok())
        .ok_or("missing x-arkel-time")?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_secs();
    let t: u64 = time.parse().map_err(|_| "bad x-arkel-time")?;
    if now.abs_diff(t) > 60 { return Err("x-arkel-time outside skew window".to_string()); }
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok())
        .ok_or("missing authorization")?;
    let rest = auth.strip_prefix("Arkel ").ok_or("bad auth scheme")?;
    let (pk_hex, sig_hex) = rest.split_once(':').ok_or("bad auth format")?;
    let pk_bytes = hex::decode(pk_hex).map_err(|_| "bad pubkey")?;
    let sig_bytes = hex::decode(sig_hex).map_err(|_| "bad signature")?;
    let pubkey = iroh::PublicKey::from_bytes(
        pk_bytes.as_slice().try_into().map_err(|_| "bad pubkey len")?,
    )
    .map_err(|_| "bad pubkey")?;
    let sig = iroh::Signature::from_bytes(sig_bytes.as_slice().try_into()
        .map_err(|_| "bad sig len")?);
    let payload = format!("{method} {path} {} {t}", hex::encode(blake3::hash(body).as_bytes()));
    pubkey.verify(payload.as_bytes(), &sig)
        .map(|_| pubkey)
        .map_err(|_| "signature verification failed".to_string())
}