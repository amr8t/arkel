use anyhow::{Result, bail};
use reed_solomon_erasure::galois_8::ReedSolomon;

#[derive(Debug, Clone, Copy)]
pub struct ErasureConfig {
    pub k: usize, // number of data shards
    pub m: usize, // number of parity shards
}

impl ErasureConfig {
    pub fn total_shards(&self) -> usize {
        self.k + self.m
    }
}

pub fn encode(data: &[u8], config: &ErasureConfig) -> Result<Vec<Vec<u8>>> {
    let total = config.total_shards();
    let shard_size = (data.len() + config.k - 1) / config.k;

    let mut all_shards = Vec::with_capacity(total);
    for i in 0..config.k {
        let start = i * shard_size;
        let mut shard = vec![0u8; shard_size];
        if start < data.len() {
            let end = (start + shard_size).min(data.len());
            shard[..end - start].copy_from_slice(&data[start..end]);
        }
        all_shards.push(shard);
    }

    for _ in 0..config.m {
        all_shards.push(vec![0u8; shard_size]);
    }

    let rs = ReedSolomon::new(config.k, config.m)?;
    let mut refs = Vec::with_capacity(all_shards.len());
    for shard in &mut all_shards {
        refs.push(shard.as_mut_slice());
    }
    rs.encode(&mut refs)?;

    Ok(all_shards)
}

pub fn decode(
    shards: &[Option<&[u8]>],
    config: &ErasureConfig,
    original_len: usize,
) -> Result<Vec<u8>> {
    let total = config.k + config.m;
    if shards.len() != total {
        bail!("Expected {} shards, got {}", total, shards.len());
    }

    let present = shards.iter().filter(|s| s.is_some()).count();
    if present < config.k {
        bail!("Need at least {} shards, have {}", config.k, present);
    }

    // 1. Build a vector of Option<Vec<u8>> – missing = None
    let mut shard_vecs: Vec<Option<Vec<u8>>> = shards
        .iter()
        .map(|opt| opt.map(|slice| slice.to_vec()))
        .collect();

    // 2. Reconstruct missing shards (they will be filled in)
    let rs = ReedSolomon::new(config.k, config.m)?;
    rs.reconstruct(&mut shard_vecs)?;

    // 3. All shards should now be Some; concatenate the first k
    let mut result = Vec::new();
    for i in 0..config.k {
        let shard = shard_vecs[i]
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Shard {} still missing after reconstruction", i))?;
        result.extend_from_slice(shard);
    }

    // 4. Trim padding
    result.truncate(original_len);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_encode_decode_roundtrip() -> anyhow::Result<()> {
        let config = ErasureConfig { k: 4, m: 2 };
        let data = b"Hello world";

        let shards = encode(data, &config)?;
        assert_eq!(shards.len(), config.k + config.m);

        let mut available = vec![None; shards.len()];
        for (i, shard) in shards.iter().enumerate() {
            if i != 2 && i != 3 {
                available[i] = Some(shard.as_slice());
            }
        }

        let recovered = decode(&available, &config, data.len())?;
        assert_eq!(recovered, data);
        Ok(())
    }

    #[test]
    fn test_decode_insufficient_shards() -> anyhow::Result<()> {
        let config = ErasureConfig { k: 4, m: 2 };
        let data = b"Hello world";

        let shards = encode(data, &config)?;
        assert_eq!(shards.len(), config.k + config.m);

        let mut available = vec![None; shards.len()];
        for (i, shard) in shards.iter().enumerate() {
            if i != 2 && i != 3 && i != 4 {
                available[i] = Some(shard.as_slice());
            }
        }

        let result = decode(&available, &config, data.len());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Need at least 4"));
        Ok(())
    }
}
