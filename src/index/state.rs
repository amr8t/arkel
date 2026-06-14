use super::{ArkelRaftConfig, IndexCommand, IndexResponse, NodeId, ObjectMetadata};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory state machine tracking buckets and objects.
#[derive(Debug, Default, Clone)]
pub struct ArkelStateMachine {
    pub buckets: HashMap<String, u64>,                    // bucket -> created_at
    pub objects: HashMap<(String, String), ObjectMetadata>, // (bucket, key) -> metadata
    pub last_applied_log: Option<openraft::LogId<NodeId>>,
    pub last_membership: openraft::StoredMembership<NodeId, openraft::impls::BasicNode>,
}

impl IndexResponse {
    pub fn ok() -> Self {
        Self {
            success: true,
            error: None,
        }
    }

    pub fn err(s: &str) -> Self {
        Self {
            success: false,
            error: Some(s.to_string()),
        }
    }
}

impl ArkelStateMachine {
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            objects: HashMap::new(),
            last_applied_log: None,
            last_membership: openraft::StoredMembership::default(),
        }
    }

    pub fn apply_command(&mut self, cmd: IndexCommand) -> IndexResponse {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        match cmd {
            IndexCommand::CreateBucket { name } => {
                self.buckets.entry(name.clone()).or_insert(now);
                IndexResponse::ok()
            }
            IndexCommand::DeleteBucket { name } => {
                if self.buckets.remove(&name).is_some() {
                    // Also remove all objects in the bucket
                    self.objects.retain(|(b, _), _| b != &name);
                    IndexResponse::ok()
                } else {
                    IndexResponse::err(&format!("Bucket '{}' not found", name))
                }
            }
            IndexCommand::PutObject(meta) => {
                // Implicitly create bucket if it doesn't exist
                self.buckets.entry(meta.bucket.clone()).or_insert(now);
                self.objects
                    .insert((meta.bucket.clone(), meta.key.clone()), meta);
                IndexResponse::ok()
            }
            IndexCommand::DeleteObject { bucket, key } => {
                if self
                    .objects
                    .remove(&(bucket.clone(), key.clone()))
                    .is_some()
                {
                    IndexResponse::ok()
                } else {
                    IndexResponse::err(&format!("Object '{}/{}' not found", bucket, key))
                }
            }
        }
    }
}

/// Core in-memory storage engine implementing the v0.9 openraft::RaftStorage trait.
#[derive(Debug, Default, Clone)]
pub struct ArkelStore {
    pub log_store: Arc<RwLock<HashMap<u64, openraft::Entry<ArkelRaftConfig>>>>,
    pub vote: Arc<RwLock<Option<openraft::Vote<NodeId>>>>,
    pub state_machine: Arc<RwLock<ArkelStateMachine>>,
    pub last_purged_log_id: Arc<RwLock<Option<openraft::LogId<NodeId>>>>,
}

impl ArkelStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl openraft::storage::RaftSnapshotBuilder<ArkelRaftConfig> for ArkelStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<ArkelRaftConfig>, openraft::StorageError<NodeId>> {
        Ok(openraft::Snapshot {
            meta: openraft::SnapshotMeta::default(),
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl openraft::storage::RaftLogReader<ArkelRaftConfig> for ArkelStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<ArkelRaftConfig>>, openraft::StorageError<NodeId>> {
        let log = self.log_store.read().await;
        let mut entries = Vec::new();
        for (_, entry) in log.iter() {
            if range.contains(&entry.log_id.index) {
                entries.push(entry.clone());
            }
        }
        entries.sort_by_key(|e| e.log_id.index);
        Ok(entries)
    }
}

impl openraft::RaftStorage<ArkelRaftConfig> for ArkelStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<NodeId>,
    ) -> Result<(), openraft::StorageError<NodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<NodeId>>, openraft::StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), openraft::StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<ArkelRaftConfig>> + Send,
    {
        let mut log = self.log_store.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: openraft::LogId<NodeId>,
    ) -> Result<(), openraft::StorageError<NodeId>> {
        let mut log = self.log_store.write().await;
        log.retain(|&index, _| index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: openraft::LogId<NodeId>,
    ) -> Result<(), openraft::StorageError<NodeId>> {
        let mut log = self.log_store.write().await;
        log.retain(|&index, _| index > log_id.index);
        // Track the purge point
        let mut purged = self.last_purged_log_id.write().await;
        *purged = Some(log_id);
        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<ArkelRaftConfig>, openraft::StorageError<NodeId>> {
        let log = self.log_store.read().await;
        let purged = self.last_purged_log_id.read().await;
        let last = log
            .values()
            .max_by_key(|e| e.log_id.index)
            .map(|e| e.log_id);
        Ok(openraft::storage::LogState {
            last_purged_log_id: *purged,
            last_log_id: last,
        })
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<NodeId>>,
            openraft::StoredMembership<NodeId, openraft::impls::BasicNode>,
        ),
        openraft::StorageError<NodeId>,
    > {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<ArkelRaftConfig>],
    ) -> Result<Vec<IndexResponse>, openraft::StorageError<NodeId>> {
        let mut sm = self.state_machine.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match &entry.payload {
                openraft::EntryPayload::Normal(cmd) => {
                    responses.push(sm.apply_command(cmd.clone()));
                }
                openraft::EntryPayload::Membership(mem) => {
                    sm.last_membership =
                        openraft::StoredMembership::new(Some(entry.log_id), mem.clone());
                    responses.push(IndexResponse::ok());
                }
                _ => responses.push(IndexResponse::ok()),
            }
        }
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &openraft::SnapshotMeta<NodeId, openraft::impls::BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<NodeId>> {
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<ArkelRaftConfig>>, openraft::StorageError<NodeId>> {
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
