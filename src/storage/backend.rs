use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::storage::types::{ShardId, ShardStore};

pub struct DiskStore {
    dir: PathBuf,
}

impl DiskStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn shard_path(&self, shard_id: &ShardId) -> PathBuf {
        self.dir.join(format!("{}.shard", shard_id.0.to_hex()))
    }

    fn temp_path(&self, shard_id: &ShardId) -> PathBuf {
        self.dir.join(format!("{}.tmp", shard_id.0.to_hex()))
    }
}

impl ShardId {
    fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

#[async_trait::async_trait]
impl ShardStore for DiskStore {
    async fn put_shard(&self, shard_id: &ShardId, data: &[u8]) -> Result<()> {
        let temp = self.temp_path(shard_id);
        let final_path = self.shard_path(shard_id);

        tokio::fs::write(&temp, data)
            .await
            .context("Failed to write shard temp file")?;

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .await
            .context("Failed to open temp file for fsync")?;
        file.sync_all().await.context("Failed to fsync shard")?;

        tokio::fs::rename(&temp, &final_path)
            .await
            .context("Failed to rename shard from temp to final")?;

        tracing::debug!("Shard {} persisted to disk", shard_id.to_hex());
        Ok(())
    }

    async fn get_shard(&self, shard_id: &ShardId) -> Result<Vec<u8>> {
        let path = self.shard_path(shard_id);
        tokio::fs::read(&path)
            .await
            .with_context(|| format!("Failed to read shard {}", shard_id.to_hex()))
    }

    async fn shard_exists(&self, shard_id: &ShardId) -> Result<bool> {
        let path = self.shard_path(shard_id);
        tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("Failed to check shard existence {}", shard_id.to_hex()))
    }

    async fn delete_shard(&self, shard_id: &ShardId) -> Result<()> {
        let path = self.shard_path(shard_id);
        tokio::fs::remove_file(&path)
            .await
            .with_context(|| format!("Failed to delete shard {}", shard_id.to_hex()))
    }

    // TODO: efficiency, use a counter
    async fn total_bytes(&self) -> Result<u64> {
        let mut total = 0u64;
        let mut entries = tokio::fs::read_dir(&self.dir)
            .await
            .context("Failed to read data directory")?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_file() {
                total += meta.len();
            }
        }
        Ok(total)
    }

    fn data_dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_put_get_delete_shard() {
        let dir = tempfile::tempdir().unwrap();
        let storage = DiskStore::new(dir.path().to_path_buf());

        let data = b"hello";
        let hash = blake3::hash(data);
        let shard_id = ShardId(hash);

        assert!(!storage.shard_exists(&shard_id).await.unwrap());

        storage.put_shard(&shard_id, data).await.unwrap();
        assert!(storage.shard_exists(&shard_id).await.unwrap());

        let read = storage.get_shard(&shard_id).await.unwrap();
        assert_eq!(read, data);

        let total = storage.total_bytes().await.unwrap();
        assert_eq!(total, data.len() as u64);

        storage.delete_shard(&shard_id).await.unwrap();
        assert!(!storage.shard_exists(&shard_id).await.unwrap());
        assert_eq!(storage.total_bytes().await.unwrap(), 0);
    }
}
