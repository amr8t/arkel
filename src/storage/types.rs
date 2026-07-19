use anyhow::Result;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardId(pub blake3::Hash);

#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    async fn put_shard(&self, shard_id: &ShardId, data: &[u8]) -> Result<()>;
    async fn get_shard(&self, shard_id: &ShardId) -> Result<Vec<u8>>;
    async fn shard_exists(&self, shard_id: &ShardId) -> Result<bool>;
    async fn delete_shard(&self, shard_id: &ShardId) -> Result<()>;
    async fn total_bytes(&self) -> Result<u64>;
    fn data_dir(&self) -> &Path;
}
