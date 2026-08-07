use anyhow::{Context, Result};
use iroh::SignatureError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub bucket: String,
    pub key: String,
    pub object_hash: [u8; 32],
    pub original_size: u64,
    pub k: u8,
    pub m: u8,
    pub shards: Vec<ShardPlacement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardPlacement {
    pub shard_index: u8,
    pub node_id: iroh::PublicKey,
    pub blob_hash: [u8; 32],
}

pub fn serialize_manifest(manifest: &Manifest) -> Result<Vec<u8>> {
    let bytes = serde_cbor::to_vec(manifest).context("Failed to serialize manifest to CBOR")?;
    Ok(bytes)
}

pub fn deserialize_manifest(bytes: &[u8]) -> Result<Manifest> {
    let manifest: Manifest =
        serde_cbor::from_slice(bytes).context("Failed to deserialize manifest from CBOR")?;
    Ok(manifest)
}

pub fn sign_manifest(
    manifest_bytes: &[u8],
    secret_key: &iroh::SecretKey,
) -> Result<iroh::Signature> {
    let signature = secret_key.sign(manifest_bytes);
    Ok(signature)
}

pub fn verify_manifest(
    manifest_bytes: &[u8],
    signature: &iroh::Signature,
    public_key: &iroh::PublicKey,
) -> Result<(), SignatureError> {
    public_key.verify(manifest_bytes, signature)
}

pub fn etag_from_hash(hash: &[u8; 32]) -> String {
    hex::encode(hash)
}

pub fn etag_from_data(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(hash.as_bytes())
}

pub fn bytes_to_hash(bytes: [u8; 32]) -> blake3::Hash {
    blake3::Hash::from(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{PublicKey, SecretKey};

    #[test]
    fn test_serialize_deserialize() -> Result<()> {
        let manifest = Manifest {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            object_hash: *blake3::hash(b"hello").as_bytes(),
            original_size: 1024,
            k: 4,
            m: 2,
            shards: vec![
                ShardPlacement {
                    shard_index: 0,
                    node_id: SecretKey::generate().public(),
                    blob_hash: [0u8; 32],
                },
                ShardPlacement {
                    shard_index: 1,
                    node_id: SecretKey::generate().public(),
                    blob_hash: [1u8; 32],
                },
            ],
        };

        let bytes = serialize_manifest(&manifest)?;
        let deserialized = deserialize_manifest(&bytes)?;
        assert_eq!(manifest, deserialized);
        Ok(())
    }
}
