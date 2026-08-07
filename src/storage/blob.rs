use anyhow::{Context, Result};
use iroh::PublicKey;
use std::net::SocketAddr;

/// Upload data to a remote storage node. Adds to local store, pushes via iroh-blobs QUIC.
/// Returns the content hash for use in the Manifest.
pub async fn put_blob(
    store: &iroh_blobs::api::Store,
    endpoint: &iroh::Endpoint,
    data: &[u8],
    node_id: PublicKey,
    addr: SocketAddr,
) -> Result<iroh_blobs::Hash> {
    let temp_tag = store
        .blobs()
        .add_bytes(data.to_vec())
        .temp_tag()
        .await
        .context("Failed to add blob to local store")?;
    let hash = temp_tag.hash();
    drop(temp_tag);

    let endpoint_addr = iroh::EndpointAddr::from_parts(node_id, [iroh::TransportAddr::Ip(addr)]);
    let conn = endpoint
        .connect(endpoint_addr, iroh_blobs::ALPN)
        .await
        .context("Failed to connect to storage node")?;

    let request = iroh_blobs::protocol::GetRequest::blob(hash);
    store
        .remote()
        .execute_push(conn, request.into())
        .complete()
        .await
        .context("Failed to push blob to storage node")?;

    Ok(hash)
}

/// Download a blob from a remote node by hash.
pub async fn get_blob(
    store: &iroh_blobs::api::Store,
    endpoint: &iroh::Endpoint,
    hash: iroh_blobs::Hash,
    node_id: PublicKey,
) -> Result<Vec<u8>> {
    let downloader = store.downloader(endpoint);
    downloader
        .download(hash, vec![node_id])
        .await
        .context("Failed to download blob from remote node")?;

    let bytes = store
        .blobs()
        .get_bytes(hash)
        .await
        .context("Failed to read downloaded blob")?;

    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let db = iroh_blobs::store::fs::FsStore::load(dir.path())
            .await
            .unwrap();
        let store: iroh_blobs::api::Store = db.into();

        let tag = store.blobs().add_bytes(b"test data".to_vec()).temp_tag().await.unwrap();
        let hash = tag.hash();
        drop(tag);

        let read = store.blobs().get_bytes(hash).await.unwrap();
        assert_eq!(read.as_ref(), b"test data");
    }
}