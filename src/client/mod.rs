pub mod encrypt;
pub mod manifest;

// TODO: add these once encrypt.rs is implemented:
// pub use encrypt::{decrypt_shard, derive_key, encrypt_shard};

// TODO: uncomment once encrypt.rs AND gateway/erasure are available
// use crate::gateway::ErasureConfig;
//
// pub struct PreparedUpload {
//     pub object_hash: blake3::Hash,
//     pub encrypted_shards: Vec<Vec<u8>>,
//     pub ec_config: ErasureConfig,
//     pub original_len: usize,
// }
//
// pub fn prepare_upload(data: &[u8], ec_config: ErasureConfig, master_key: &[u8; 32]) -> Result<PreparedUpload> { ... }
// pub fn reconstruct_object(shards: &[Option<Vec<u8>>], ec_config: &ErasureConfig, master_key: &[u8; 32], object_hash: &blake3::Hash, original_len: usize) -> Result<Vec<u8>> { ... }
