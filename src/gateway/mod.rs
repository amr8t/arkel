use anyhow::{Result, bail};
use blake3::Hash;
pub mod erasure;

pub use erasure::{ErasureConfig, decode, encode};

use crate::client::encrypt::{decrypt_shard, derive_key, encrypt_shard};

pub struct PreparedUpload {
    pub encrypted_shards: Vec<Vec<u8>>,
    pub shard_hashes: Vec<blake3::Hash>,
    pub erasure_config: ErasureConfig,
}
pub fn prepare_upload(
    data: &[u8],
    ec_config: ErasureConfig,
    master_key: &[u8; 32],
) -> Result<PreparedUpload> {
    let object_hash = blake3::hash(data);
    let derived_key = derive_key(master_key, &object_hash);

    let plain_shards = encode(data, &ec_config)?;
    let mut encrypted_shards = Vec::with_capacity(plain_shards.len());
    let mut shard_hashes = Vec::with_capacity(plain_shards.len());

    for shard in plain_shards {
        let encrypted = encrypt_shard(&shard, &derived_key);
        let hash = blake3::hash(&encrypted);
        encrypted_shards.push(encrypted);
        shard_hashes.push(hash);
    }

    Ok(PreparedUpload {
        encrypted_shards,
        shard_hashes,
        erasure_config: ec_config,
    })
}

pub fn reconstruct_object(
    encrypted_shards: &[Option<&[u8]>],
    ec_config: ErasureConfig,
    master_key: &[u8; 32],
    expected_object_hash: &Hash,
    original_len: usize,
) -> Result<Vec<u8>> {
    let derived_key = derive_key(master_key, expected_object_hash);

    let mut decrypted = Vec::with_capacity(encrypted_shards.len());
    for shard in encrypted_shards {
        if let Some(enc) = shard {
            let plain = decrypt_shard(enc, &derived_key)?;
            decrypted.push(Some(plain));
        } else {
            decrypted.push(None);
        }
    }
    let refs: Vec<Option<&[u8]>> = decrypted.iter().map(|opt| opt.as_deref()).collect();
    let recovered = decode(&refs, &ec_config, original_len)?;

    let computed = blake3::hash(&recovered);
    if computed != *expected_object_hash {
        bail!("integrity check failed: BLAKE3 mismatch");
    }

    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    
    #[test]
    fn test_prepare_reconstruct_roundtrip() -> Result<()> {
        let ec_config = ErasureConfig { k: 4, m: 2 };
        let master = [0u8; 32];
        let data = b"Hello world";

        let prepared = prepare_upload(data, ec_config, &master)?;

        // Simulate losing shards 2 and 5 (indices 2 and 5).
        let mut available = vec![None; prepared.encrypted_shards.len()];
        for (i, shard) in prepared.encrypted_shards.iter().enumerate() {
            if i != 2 && i != 5 {
                available[i] = Some(shard.as_slice());
            }
        }

        let object_hash = blake3::hash(data);
        let recovered =
            reconstruct_object(&available, ec_config, &master, &object_hash, data.len())?;
        assert_eq!(recovered, data);
        Ok(())
    }
}
