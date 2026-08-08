use anyhow::{Context, Result};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::io::AsyncWriteExt;

/// ALPN for the shard-pull control protocol.
///
/// iroh-blobs' `execute_push` is experimental and truncates large blobs, so a
/// "put" is done the reliable way: the client hosts each shard and tells the
/// storage node to download it (pull). This is the control channel for that.
pub const PULL_ALPN: &[u8] = b"arkel-pull";

/// Protocol handler for `PULL_ALPN` connections.
///
/// Each incoming connection is from a client offering shards. For every
/// bidirectional stream it reads a 32-byte blob hash, downloads that blob from
/// the client (the peer of this connection), then writes a 1-byte ack.
#[derive(Debug)]
pub struct PullHandler {
    store: iroh_blobs::api::Store,
    endpoint: iroh::Endpoint,
}

impl PullHandler {
    pub fn new(store: &iroh_blobs::api::Store, endpoint: &iroh::Endpoint) -> Self {
        Self {
            store: store.clone(),
            endpoint: endpoint.clone(),
        }
    }
}

impl ProtocolHandler for PullHandler {
    fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send {
        let store = self.store.clone();
        let endpoint = self.endpoint.clone();
        async move {
            let client = connection.remote_id();
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => break, // connection closed
                };
                let store = store.clone();
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_pull(&store, &endpoint, client, &mut send, &mut recv).await
                    {
                        tracing::warn!("shard pull failed: {e}");
                    }
                });
            }
            Ok(())
        }
    }
}

async fn handle_pull(
    store: &iroh_blobs::api::Store,
    endpoint: &iroh::Endpoint,
    client: iroh::EndpointId,
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<()> {
    let mut hash_bytes = [0u8; 32];
    recv.read_exact(&mut hash_bytes)
        .await
        .context("read shard hash")?;
    let hash = iroh_blobs::Hash::from(hash_bytes);

    let downloader = store.downloader(endpoint);
    downloader
        .download(hash, vec![client])
        .await
        .context("storage node failed to pull shard from client")?;

    send.write_all(&[0u8]).await.context("write pull ack")?;
    send.flush().await.ok();
    Ok(())
}
