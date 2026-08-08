use anyhow::Result;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<()> {
    let pid = std::process::id();

    // ---- provider: hosts a 2MB blob ----
    let pdir = std::env::temp_dir().join(format!("arkel-dl-prov-{pid}"));
    let pstore: iroh_blobs::api::Store =
        iroh_blobs::store::fs::FsStore::load(&pdir).await?.into();
    let pendpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await?;
    let protos = iroh_blobs::BlobsProtocol::new(&pstore, None);
    let _router = iroh::protocol::Router::builder(pendpoint.clone())
        .accept(iroh_blobs::ALPN, protos)
        .spawn();

    let data: Vec<u8> = (0..2_097_152u32).map(|i| (i % 251) as u8).collect();
    let tag = pstore.blobs().add_bytes(data.clone()).temp_tag().await?;
    let hash = tag.hash();
    drop(tag);

    let provider_id = pendpoint.id();
    let direct: Vec<SocketAddr> = pendpoint.bound_sockets();
    let paddr = *direct
        .first()
        .expect("provider should have a bound socket");
    println!("provider={provider_id} addr={paddr} hash={hash} blob_bytes={}", data.len());

    // ---- downloader: fetches the 2MB blob over QUIC ----
    let ddir = std::env::temp_dir().join(format!("arkel-dl-dl-{pid}"));
    let dstore: iroh_blobs::api::Store =
        iroh_blobs::store::fs::FsStore::load(&ddir).await?.into();
    let dendpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await?;

    // teach the downloader endpoint the provider's address (as dataplane::get does)
    let ea = iroh::EndpointAddr::from_parts(provider_id, [iroh::TransportAddr::Ip(paddr)]);
    dendpoint.connect(ea, iroh_blobs::ALPN).await?;

    let downloader = dstore.downloader(&dendpoint);
    println!("downloading...");
    downloader.download(hash, vec![provider_id]).await?;
    let got = dstore.blobs().get_bytes(hash).await?;
    assert_eq!(got.len(), data.len(), "size mismatch");
    assert_eq!(got.as_ref(), data.as_slice(), "content mismatch");
    println!("✓ download of 2MB blob OK ({} bytes)", got.len());

    dendpoint.close().await;
    pendpoint.close().await;
    Ok(())
}
