use super::{ArkelRaftConfig, IndexNodeRequest, IndexNodeResponse};
use crate::index::types::NodeStats;
use crate::index::types::NodeStatus;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use futures_util::TryStreamExt;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::entry::RaftEntry;
use openraft::storage::{
    ApplyResponder, EntryResponder, IOFlushed, RaftLogReader, RaftLogStorage, RaftStateMachine,
};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

fn to_io_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

fn cbor_to_io<T: Serialize>(v: &T) -> Result<Vec<u8>, io::Error> {
    serde_cbor::to_vec(v).map_err(to_io_err)
}

fn cbor_from_io<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, io::Error> {
    serde_cbor::from_slice(bytes).map_err(to_io_err)
}

fn open_conn(path: &Path) -> Result<Connection, io::Error> {
    let conn = Connection::open(path).map_err(to_io_err)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA temp_store=MEMORY;
         PRAGMA mmap_size=134217728;",
    )
    .map_err(to_io_err)?;
    Ok(conn)
}

fn set_busy_timeout(conn: &Connection, ms: i32) -> Result<(), io::Error> {
    conn.pragma_update(None, "busy_timeout", ms)
        .map_err(to_io_err)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StateMachineSnapshotData {
    // Vec, not HashMap: row order is irrelevant for correctness, and this
    // is exactly what a SQL dump produces.
    buckets: Vec<(String, u64, Vec<u8>)>,
    manifests: Vec<(Vec<u8>, String, String, Vec<u8>, Vec<u8>)>, // (object_hash, bucket, key, manifest, signature)
    shard_refs: Vec<(Vec<u8>, i64)>,                              // (blob_hash, refs)
}

#[derive(Debug)]
struct StateMachineInner {
    conn: Connection,
    last_applied_log: Option<LogIdOf<ArkelRaftConfig>>,
    last_membership: StoredMembershipOf<ArkelRaftConfig>,
    snapshot_index: u64,
    current_snapshot: Option<(SnapshotMetaOf<ArkelRaftConfig>, Vec<u8>)>,
    node_registry: HashMap<Vec<u8>, NodeStats>,
}

const SM_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS buckets (
        name TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        owner BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS manifests (
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        object_hash BLOB NOT NULL,
        manifest BLOB NOT NULL,
        signature BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (bucket, key)
    );
    CREATE TABLE IF NOT EXISTS sm_meta (
        k TEXT PRIMARY KEY,
        v BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS shard_refs (
        blob_hash BLOB PRIMARY KEY,
        refs INTEGER NOT NULL
    );
";

impl StateMachineInner {
    fn open(path: &Path) -> Result<Self, io::Error> {
        let conn = open_conn(path)?;
        set_busy_timeout(&conn, 5000)?;
        conn.execute_batch(SM_SCHEMA).map_err(to_io_err)?;

        // Restore cached meta from disk (survives process restart).
        let last_applied_log: Option<LogIdOf<ArkelRaftConfig>> = conn
            .query_row(
                "SELECT v FROM sm_meta WHERE k = 'last_applied_log'",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(to_io_err)?
            .map(|bytes| cbor_from_io(&bytes))
            .transpose()?;

        let last_membership: StoredMembershipOf<ArkelRaftConfig> = conn
            .query_row(
                "SELECT v FROM sm_meta WHERE k = 'last_membership'",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(to_io_err)?
            .map(|bytes| cbor_from_io(&bytes))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            conn,
            last_applied_log,
            last_membership,
            snapshot_index: 0,
            current_snapshot: None,
            node_registry: HashMap::new(),
        })
    }

    fn bucket_owner(tx: &rusqlite::Transaction<'_>, bucket: &str) -> Result<Option<Vec<u8>>, io::Error> {
        tx.query_row(
        "SELECT owner FROM buckets WHERE name=?1",
        params![bucket],
        |r| r.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(to_io_err)
    }

    /// Apply one command inside an already-open transaction. No fsync here —
    /// the caller commits once for the whole batch.
    ///
    /// IMPORTANT: every bit of application state produced here must be
    /// deterministic from the command payload, because all three Raft nodes
    /// replay the exact same log entry. Do NOT call `SystemTime::now()` here;
    /// use timestamps carried inside the replicated command instead.
    fn apply_command(
        tx: &rusqlite::Transaction<'_>,
        cmd: IndexNodeRequest,
        log_index: u64,
        node_registry: &mut HashMap<Vec<u8>, NodeStats>,
    ) -> Result<IndexNodeResponse, io::Error> {
        let result = match cmd {
            IndexNodeRequest::CommitManifest {
                bucket,
                key,
                object_hash,
                manifest_bytes,
                caller,
            } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                let owner = Self::bucket_owner(tx, &bucket)?;
                if let Some(o) = &owner {
                    if *o != caller {
                        return Ok(IndexNodeResponse::err("forbidden"));
                    }
                }

                // Decrement refs of the overwritten manifest (INSERT OR REPLACE
                // drops the old row), so re-puts don't leak refcounts.
                if let Some(existing) = tx
                    .query_row(
                        "SELECT manifest FROM manifests WHERE bucket=?1 AND key=?2",
                        params![bucket, key],
                        |r| r.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(to_io_err)?
                {
                    for s in crate::client::manifest::deserialize_manifest(&existing)
                        .map_err(to_io_err)?
                        .shards
                    {
                        tx.execute(
                            "UPDATE shard_refs SET refs = refs - 1 WHERE blob_hash=?1 AND refs > 0",
                            params![s.blob_hash.to_vec()],
                        )
                        .map_err(to_io_err)?;
                    }
                }
                // Increment refs of the new manifest's shards.
                for s in crate::client::manifest::deserialize_manifest(&manifest_bytes)
                    .map_err(to_io_err)?
                    .shards
                {
                    tx.execute(
                        "INSERT INTO shard_refs (blob_hash, refs) VALUES (?1, 1)
                ON CONFLICT(blob_hash) DO UPDATE SET refs = refs + 1",
                        params![s.blob_hash.to_vec()],
                    )
                    .map_err(to_io_err)?;
                }

                tx.execute(
                    "INSERT OR IGNORE INTO buckets (name, created_at, owner) VALUES (?1, ?2, ?3)",
                    params![bucket, now, caller],
                )
                .map_err(to_io_err)?;
                tx.execute(
                    "INSERT OR REPLACE INTO manifests (object_hash, bucket, key, manifest, signature, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![object_hash, bucket, key, manifest_bytes, Vec::<u8>::new(), now],
                )
                .map_err(to_io_err)?;

                IndexNodeResponse::ok()
            }
            IndexNodeRequest::CreateBucket { name, created_at, owner } => {
                let existing = Self::bucket_owner(tx, &name)?;
                match existing {
                    Some(o) if o == owner => IndexNodeResponse::ok(),
                    Some(_) => IndexNodeResponse::err("bucket owned by another key"),
                    None => {
                        tx.execute(
                            "INSERT INTO buckets (name, created_at, owner) VALUES (?1, ?2, ?3)",
                            params![name, created_at as i64, owner],
                        )
                        .map_err(to_io_err)?;
                        IndexNodeResponse::ok()
                    }
                }
            }
            IndexNodeRequest::DeleteBucket { name, caller } => {
                if Self::bucket_owner(tx, &name)?.as_deref() != Some(caller.as_slice()) {
                    return Ok(IndexNodeResponse::err("forbidden"));
                }
                let rows = tx
                    .execute("DELETE FROM buckets WHERE name = ?1", params![name])
                    .map_err(to_io_err)?;
                if rows > 0 {
                    tx.execute("DELETE FROM manifests WHERE bucket = ?1", params![name])
                        .map_err(to_io_err)?;
                    IndexNodeResponse::ok()
                } else {
                    IndexNodeResponse::err(&format!("Bucket '{}' not found", name))
                }
            }

            IndexNodeRequest::Batch(entries) => {
                let responses: Vec<IndexNodeResponse> = entries
                    .into_iter()
                    .map(|entry| Self::apply_command(tx, entry, log_index, node_registry))
                    .collect::<Result<Vec<_>, _>>()?;
                IndexNodeResponse::batch(responses)
            }

            IndexNodeRequest::DeleteManifest { bucket, key, caller } => {
                if Self::bucket_owner(tx, &bucket)?.as_deref() != Some(caller.as_slice()) {
                    return Ok(IndexNodeResponse::err("forbidden"));
                }
                if let Some(mb) = tx
                    .query_row(
                        "SELECT manifest FROM manifests WHERE bucket=?1 AND key=?2",
                        params![bucket, key],
                        |r| r.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(to_io_err)?
                {
                    for s in crate::client::manifest::deserialize_manifest(&mb)
                        .map_err(to_io_err)?
                        .shards
                    {
                        tx.execute(
                            "UPDATE shard_refs SET refs = refs - 1 WHERE blob_hash=?1 AND refs > 0",
                            params![s.blob_hash.to_vec()],
                        )
                        .map_err(to_io_err)?;
                    }
                }
                let rows = tx
                    .execute(
                        "DELETE FROM manifests WHERE bucket=?1 AND key=?2",
                        params![bucket, key],
                    )
                    .map_err(to_io_err)?;
                if rows > 0 {
                    IndexNodeResponse::ok()
                } else {
                    IndexNodeResponse::err("object not found")
                }
            }

            IndexNodeRequest::RegisterNode {
                node_id,
                capacity_bytes,
                addr,
                relay_url,
            } => {
                node_registry.insert(
                    node_id.clone(),
                    NodeStats {
                        node_id,
                        capacity_bytes,
                        addr,
                        relay_url,
                        last_seen: log_index,
                        status: NodeStatus::Online,
                    },
                );
                IndexNodeResponse::ok()
            }

            IndexNodeRequest::MarkNodesOffline { node_ids } => {
                for id in node_ids {
                    if let Some(ns) = node_registry.get_mut(&id) {
                        ns.status = NodeStatus::Offline
                    }
                }
                IndexNodeResponse::ok()
            }
        };
        Ok(result)
    }
}

/// Logical snapshot of the state machine at a single point in time.
/// Returned by [`ArkelStateMachine::snapshot`] so reads do not block
/// writers while the Raft engine wants a snapshot builder.
#[derive(Clone)]
pub struct ArkelStateMachineSnapshot {
    buckets: Vec<(String, u64, Vec<u8>)>,
    manifests: Vec<(Vec<u8>, String, String, Vec<u8>, Vec<u8>)>,
    shard_refs: Vec<(Vec<u8>, i64)>,
    last_applied_log: Option<LogIdOf<ArkelRaftConfig>>,
    last_membership: StoredMembershipOf<ArkelRaftConfig>,
    snapshot_index: u64,
}

/// Cloneable, thread-safe handle — identical role to the in-memory version.
#[derive(Clone)]
pub struct ArkelStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
}

impl ArkelStateMachine {
    /// Replaces the old `new()`. Fallible + async because opening a real
    /// file and running schema migration can fail.
    pub async fn open(db_path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let path = db_path.as_ref().to_path_buf();
        let sm = tokio::task::spawn_blocking(move || StateMachineInner::open(&path))
            .await
            .map_err(to_io_err)??;
        Ok(Self {
            inner: Arc::new(Mutex::new(sm)),
        })
    }

    /// Capture a consistent logical snapshot of the current state under the
    /// mutex. Used by [`RaftSnapshotBuilder`] so the expensive serialization
    /// work can happen outside the write lock.
    pub async fn snapshot(&self) -> Result<ArkelStateMachineSnapshot, io::Error> {
        let sm = self.inner.lock().await;

        let mut buckets_stmt = sm
            .conn
            .prepare_cached("SELECT name, created_at, owner FROM buckets")
            .map_err(to_io_err)?;
        let buckets = buckets_stmt
            .query_map([], |r| {
               Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, Vec<u8>>(2)?))
            })
            .map_err(to_io_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_io_err)?;

        let mut stmt = sm
            .conn
            .prepare_cached("SELECT object_hash, bucket, key, manifest, signature FROM manifests")
            .map_err(to_io_err)?;
        let manifests = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })
            .map_err(to_io_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_io_err)?;

        Ok(ArkelStateMachineSnapshot {
            buckets,
            manifests,
            shard_refs: sm
                .conn
                .prepare_cached("SELECT blob_hash, refs FROM shard_refs")
                .map_err(to_io_err)?
                .query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)))
                .map_err(to_io_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(to_io_err)?,
            last_applied_log: sm.last_applied_log.clone(),
            last_membership: sm.last_membership.clone(),
            snapshot_index: sm.snapshot_index,
        })
    }

    /// Read API: list bucket names.
    pub async fn list_buckets(&self) -> Result<Vec<String>, io::Error> {
        let sm = self.inner.lock().await;
        let mut stmt = sm
            .conn
            .prepare_cached("SELECT name, owner FROM buckets")
            .map_err(to_io_err)?;
        let names = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(to_io_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_io_err)?;
        Ok(names)
    }

    /// Read API: get a single object metadata record.
    pub async fn read_manifest(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, io::Error> {
        let sm = self.inner.lock().await;
        let mut stmt = sm
            .conn
            .prepare_cached(
                "SELECT manifest, signature FROM manifests WHERE bucket = ?1 AND key = ?2",
            )
            .map_err(to_io_err)?;
        let result = stmt
            .query_row(params![bucket, key], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .optional()
            .map_err(to_io_err)?;
        Ok(result)
    }

    /// Read API: list object keys in a bucket.
    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, io::Error> {
        let sm = self.inner.lock().await;
        let mut stmt = sm
            .conn
            .prepare_cached("SELECT key FROM manifests WHERE bucket = ?1 ORDER BY key")
            .map_err(to_io_err)?;
        let keys = stmt
            .query_map(params![bucket], |r| r.get::<_, String>(0))
            .map_err(to_io_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_io_err)?;
        Ok(keys)
    }

    /// Which of the given shard blob hashes are referenced by no live
    /// manifest (refs == 0)? Used by the storage-node GC loop.
    pub async fn gc_candidates(&self, hashes: &[[u8; 32]]) -> Result<Vec<Vec<u8>>, io::Error> {
        let sm = self.inner.lock().await;
        let mut out = Vec::new();
        for h in hashes {
            let refs: i64 = sm
                .conn
                .query_row(
                    "SELECT refs FROM shard_refs WHERE blob_hash=?1",
                    params![h.to_vec()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(to_io_err)?
                .unwrap_or(0);
            if refs == 0 {
                out.push(h.to_vec());
            }
        }
        Ok(out)
    }

    pub async fn get_healthy_nodes(
        &self,
        count: usize,
    ) -> Result<Vec<(Vec<u8>, String, Option<String>)>, io::Error> {
        let sm = self.inner.lock().await;
        let nodes: Vec<_> = sm
            .node_registry
            .iter()
            .filter(|(_, ns)| ns.status == NodeStatus::Online)
            .take(count)
            .map(|(id, ns)| (id.clone(), ns.addr.clone(), ns.relay_url.clone()))
            .collect();
        Ok(nodes)
    }

    pub async fn list_node_lags(&self) -> Result<Vec<(Vec<u8>, u64)>, io::Error> {
        let sm = self.inner.lock().await;
        Ok(sm
            .node_registry
            .iter()
            .map(|(id, ns)| (id.clone(), ns.last_seen))
            .collect())
    }
}

impl RaftStateMachine<ArkelRaftConfig> for ArkelStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<ArkelRaftConfig>>,
            StoredMembershipOf<ArkelRaftConfig>,
        ),
        io::Error,
    > {
        let sm = self.inner.lock().await;
        Ok((sm.last_applied_log.clone(), sm.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<Item = Result<EntryResponder<ArkelRaftConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        let mut sm = self.inner.lock().await;
        let mut entries = entries;

        // 1. Drain the stream FIRST. No transaction is open yet, so holding
        //    things across .await here is fine — this is the only part of
        //    the function that awaits anything.
        let mut batch: Vec<(
            EntryOf<ArkelRaftConfig>,
            Option<ApplyResponder<ArkelRaftConfig>>,
        )> = Vec::new();
        while let Some(item) = entries.try_next().await? {
            batch.push(item);
        }

        if batch.is_empty() {
            return Ok(());
        }

        // 2. Run the whole batch through ONE transaction, fully
        //    synchronously — no .await anywhere inside this block, so the
        //    non-Send Transaction never crosses an await point.
        let mut last_log = None;
        let mut last_membership: Option<StoredMembershipOf<ArkelRaftConfig>> = None;
        let mut pending: Vec<(ApplyResponder<ArkelRaftConfig>, IndexNodeResponse)> = Vec::new();

        {
            let mut node_registry = std::mem::take(&mut sm.node_registry);
            let tx = sm.conn.transaction().map_err(to_io_err)?;

            for (entry, responder) in batch {
                last_log = Some(entry.log_id().clone());

                let response = match &entry.payload {
                    EntryPayload::Blank => IndexNodeResponse::ok(),
                    EntryPayload::Normal(cmd) => StateMachineInner::apply_command(
                        &tx,
                        cmd.clone(),
                        entry.index(),
                        &mut node_registry,
                    )?,
                    EntryPayload::Membership(mem) => {
                        last_membership = Some(StoredMembershipOf::<ArkelRaftConfig>::new(
                            Some(entry.log_id().clone()),
                            mem.clone(),
                        ));
                        IndexNodeResponse::ok()
                    }
                };

                // Defer sending until after commit: don't tell the caller
                // "done" before the data is durably on disk.
                if let Some(responder) = responder {
                    pending.push((responder, response));
                }
            }

            // Persist meta in the SAME transaction as the data it
            // describes — they must never be allowed to diverge across a
            // crash.
            if let Some(ref l) = last_log {
                let bytes = cbor_to_io(l)?;
                tx.execute(
                    "INSERT OR REPLACE INTO sm_meta (k, v) VALUES ('last_applied_log', ?1)",
                    params![bytes],
                )
                .map_err(to_io_err)?;
            }
            if let Some(ref m) = last_membership {
                let bytes = cbor_to_io(m)?;
                tx.execute(
                    "INSERT OR REPLACE INTO sm_meta (k, v) VALUES ('last_membership', ?1)",
                    params![bytes],
                )
                .map_err(to_io_err)?;
            }

            tx.commit().map_err(to_io_err)?; // <-- single fsync for the whole batch
            sm.node_registry = node_registry;
        } // tx dropped here, before any further .await

        if let Some(l) = last_log {
            sm.last_applied_log = Some(l);
        }
        if let Some(m) = last_membership {
            sm.last_membership = m;
        }

        // Only now that data is durable, notify callers.
        for (responder, response) in pending {
            let _ = responder.send(response);
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    /// Logical reconstruction, not a raw file copy: wipe tables, re-insert
    /// rows from the snapshot's row dump. Safe to run on a node whose disk
    /// was wiped entirely (the home-server-rejoin case).
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ArkelRaftConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let snap_data: StateMachineSnapshotData = cbor_from_io(snapshot.get_ref())?;

        let mut sm = self.inner.lock().await;
        let tx = sm.conn.transaction().map_err(to_io_err)?;

        tx.execute("DELETE FROM buckets", []).map_err(to_io_err)?;
        tx.execute("DELETE FROM manifests", []).map_err(to_io_err)?;
        tx.execute("DELETE FROM shard_refs", []).map_err(to_io_err)?;

        {
            let mut bucket_stmt = tx
                .prepare_cached("INSERT INTO buckets (name, created_at, owner) VALUES (?1, ?2, ?3)")
                .map_err(to_io_err)?;
            for (name, created_at, owner) in &snap_data.buckets {
                bucket_stmt
                    .execute(params![name, *created_at as i64, owner])
                    .map_err(to_io_err)?;
            }
        }
        {
            let mut stmt = tx
                .prepare_cached("INSERT INTO manifests (object_hash, bucket, key, manifest, signature, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
                .map_err(to_io_err)?;
            for (object_hash, bucket, key, manifest, signature) in &snap_data.manifests {
                stmt.execute(rusqlite::params![
                    object_hash,
                    bucket,
                    key,
                    manifest,
                    signature,
                    0i64
                ])
                .map_err(to_io_err)?;
            }
        }
        {
            let mut stmt = tx
                .prepare_cached("INSERT INTO shard_refs (blob_hash, refs) VALUES (?1, ?2)")
                .map_err(to_io_err)?;
            for (blob_hash, refs) in &snap_data.shard_refs {
                stmt.execute(params![blob_hash, refs]).map_err(to_io_err)?;
            }
        }

        let log_bytes = meta.last_log_id.as_ref().map(cbor_to_io).transpose()?;
        if let Some(bytes) = log_bytes {
            tx.execute(
                "INSERT OR REPLACE INTO sm_meta (k, v) VALUES ('last_applied_log', ?1)",
                params![bytes],
            )
            .map_err(to_io_err)?;
        }
        let membership_bytes = cbor_to_io(&meta.last_membership)?;
        tx.execute(
            "INSERT OR REPLACE INTO sm_meta (k, v) VALUES ('last_membership', ?1)",
            params![membership_bytes],
        )
        .map_err(to_io_err)?;

        tx.commit().map_err(to_io_err)?;

        sm.last_applied_log = meta.last_log_id.clone();
        sm.last_membership = meta.last_membership.clone();
        sm.current_snapshot = Some((meta.clone(), snapshot.into_inner()));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<ArkelRaftConfig>>, io::Error> {
        let sm = self.inner.lock().await;
        match &sm.current_snapshot {
            Some((meta, data)) => Ok(Some(SnapshotOf::<ArkelRaftConfig> {
                meta: meta.clone(),
                snapshot: Cursor::new(data.clone()),
            })),
            None => Ok(None),
        }
    }
}

impl RaftSnapshotBuilder<ArkelRaftConfig> for ArkelStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<ArkelRaftConfig>, io::Error> {
        // Capture the logical view under the lock and then do the heavy
        // serialization / metadata construction outside the critical section.
        let snapshot = self.snapshot().await?;

        let snap_data = StateMachineSnapshotData {
            buckets: snapshot.buckets,
            manifests: snapshot.manifests,
            shard_refs: snapshot.shard_refs,
        };
        let data = cbor_to_io(&snap_data)?;

        let mut sm = self.inner.lock().await;
        let idx = snapshot.snapshot_index + 1;
        sm.snapshot_index = idx;
        let snapshot_id = match &snapshot.last_applied_log {
            Some(last) => format!("{}-{}-{}", last.committed_leader_id(), last.index(), idx),
            None => format!("--{}", idx),
        };

        let meta = SnapshotMetaOf::<ArkelRaftConfig> {
            last_log_id: snapshot.last_applied_log,
            last_membership: snapshot.last_membership,
            snapshot_id,
        };

        let snapshot = SnapshotOf::<ArkelRaftConfig> {
            meta: meta.clone(),
            snapshot: Cursor::new(data.clone()),
        };

        sm.current_snapshot = Some((meta, data));

        Ok(snapshot)
    }
}

// ---------------------------------------------------------------------------
// Raft log storage — same restructuring: BTreeMap<u64, Entry> -> rows.
// Separate Connection (and typically separate file) from the state
// machine: log writes and state-machine applies are different lock
// domains in OpenRaft already, no reason to contend on one SQLite writer.
// ---------------------------------------------------------------------------

const LOG_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS raft_log (
        log_index INTEGER PRIMARY KEY,
        entry BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS raft_state (
        k TEXT PRIMARY KEY,
        v BLOB NOT NULL
    );
";

struct LogStoreInner {
    conn: Connection,
}

#[derive(Clone)]
pub struct ArkelLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl ArkelLogStore {
    /// Replaces the old `new()`.
    pub async fn open(db_path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let path = db_path.as_ref().to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, io::Error> {
            let conn = open_conn(&path)?;
            set_busy_timeout(&conn, 5000)?;
            conn.execute_batch(LOG_SCHEMA).map_err(to_io_err)?;
            Ok(conn)
        })
        .await
        .map_err(to_io_err)??;

        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner { conn })),
        })
    }

    fn read_state_row<T: for<'de> Deserialize<'de>>(
        conn: &rusqlite::Connection,
        key: &str,
    ) -> Result<Option<T>, io::Error> {
        let blob: Option<Vec<u8>> = conn
            .query_row("SELECT v FROM raft_state WHERE k = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(to_io_err)?;
        blob.map(|b| cbor_from_io(&b)).transpose()
    }

    fn with_conn_blocking<F, T>(&self, f: F) -> tokio::task::JoinHandle<Result<T, io::Error>>
    where
        F: FnOnce(&Connection) -> Result<T, io::Error> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = inner.blocking_lock();
            f(&guard.conn)
        })
    }

    async fn run_blocking<F, T>(&self, f: F) -> Result<T, io::Error>
    where
        F: FnOnce(&Connection) -> Result<T, io::Error> + Send + 'static,
        T: Send + 'static,
    {
        self.with_conn_blocking(f).await.map_err(to_io_err)?
    }

    async fn run_tx_blocking<F>(&self, f: F) -> Result<(), io::Error>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<(), io::Error> + Send + 'static,
    {
        self.run_blocking(move |conn| {
            let tx = conn.unchecked_transaction().map_err(to_io_err)?;
            f(&tx)?;
            tx.commit().map_err(to_io_err)
        })
        .await
    }
}

impl RaftLogReader<ArkelRaftConfig> for ArkelLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<ArkelRaftConfig>>, io::Error> {
        let (start, end_inclusive) = {
            let start = match range.start_bound() {
                std::ops::Bound::Included(&i) => i as i64,
                std::ops::Bound::Excluded(&i) => i as i64 + 1,
                std::ops::Bound::Unbounded => i64::MIN,
            };
            let end_inclusive = match range.end_bound() {
                std::ops::Bound::Included(&i) => i as i64,
                std::ops::Bound::Excluded(&i) => i as i64 - 1,
                std::ops::Bound::Unbounded => i64::MAX,
            };
            (start, end_inclusive)
        };
        self.run_blocking(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT entry FROM raft_log WHERE log_index >= ?1 AND log_index <= ?2 ORDER BY log_index",
                )
                .map_err(to_io_err)?;
            let rows = stmt
                .query_map(params![start, end_inclusive], |r| r.get::<_, Vec<u8>>(0))
                .map_err(to_io_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(to_io_err)?;

            rows.iter().map(|bytes| cbor_from_io(bytes)).collect()
        })
        .await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<ArkelRaftConfig>>, io::Error> {
        self.run_blocking(|conn| Self::read_state_row(conn, "vote"))
            .await
    }
}

impl RaftLogStorage<ArkelRaftConfig> for ArkelLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<openraft::LogState<ArkelRaftConfig>, io::Error> {
        self.run_blocking(|conn| {
            let last_entry_bytes: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT entry FROM raft_log ORDER BY log_index DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(to_io_err)?;
            let last_entry: Option<EntryOf<ArkelRaftConfig>> = last_entry_bytes
                .map(|b| cbor_from_io::<EntryOf<ArkelRaftConfig>>(&b))
                .transpose()?;
            let last_purged: Option<LogIdOf<ArkelRaftConfig>> =
                Self::read_state_row(conn, "last_purged_log_id")?;

            let last = match last_entry {
                None => last_purged.clone(),
                Some(e) => Some(e.log_id()),
            };

            Ok(openraft::LogState {
                last_purged_log_id: last_purged,
                last_log_id: last,
            })
        })
        .await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<ArkelRaftConfig>>,
    ) -> Result<(), io::Error> {
        self.run_blocking(move |conn| match committed {
            Some(c) => {
                let bytes = cbor_to_io(&c)?;
                conn.execute(
                    "INSERT OR REPLACE INTO raft_state (k, v) VALUES ('committed', ?1)",
                    params![bytes],
                )
                .map_err(to_io_err)?;
                Ok(())
            }
            None => {
                conn.execute("DELETE FROM raft_state WHERE k = 'committed'", [])
                    .map_err(to_io_err)?;
                Ok(())
            }
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<ArkelRaftConfig>>, io::Error> {
        self.run_blocking(|conn| Self::read_state_row(conn, "committed"))
            .await
    }

    async fn save_vote(&mut self, vote: &VoteOf<ArkelRaftConfig>) -> Result<(), io::Error> {
        let vote = vote.clone();
        self.run_blocking(move |conn| {
            let bytes = cbor_to_io(&vote)?;
            conn.execute(
                "INSERT OR REPLACE INTO raft_state (k, v) VALUES ('vote', ?1)",
                params![bytes],
            )
            .map_err(to_io_err)?;
            Ok(())
        })
        .await
    }

    /// Batched like apply(): every entry in this call goes into ONE
    /// transaction, ONE fsync on commit. The IOFlushed callback only fires
    /// after that fsync actually happened.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ArkelRaftConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<ArkelRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.run_tx_blocking(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO raft_log (log_index, entry) VALUES (?1, ?2)",
                )
                .map_err(to_io_err)?;
            for entry in entries {
                let index = entry.index() as i64;
                let bytes = cbor_to_io(&entry)?;
                stmt.execute(params![index, bytes]).map_err(to_io_err)?;
            }
            Ok(())
        })
        .await?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<ArkelRaftConfig>>,
    ) -> Result<(), io::Error> {
        self.run_blocking(move |conn| {
            let start: i64 = match last_log_id {
                Some(log_id) => log_id.index() as i64 + 1,
                None => 0,
            };
            conn.execute("DELETE FROM raft_log WHERE log_index >= ?1", params![start])
                .map_err(to_io_err)?;
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<ArkelRaftConfig>) -> Result<(), io::Error> {
        self.run_tx_blocking(move |tx| {
            let current_purged: Option<LogIdOf<ArkelRaftConfig>> =
                Self::read_state_row(tx, "last_purged_log_id")?;
            assert!(current_purged.as_ref() <= Some(&log_id));

            tx.execute(
                "DELETE FROM raft_log WHERE log_index <= ?1",
                params![log_id.index() as i64],
            )
            .map_err(to_io_err)?;
            let bytes = cbor_to_io(&log_id)?;
            tx.execute(
                "INSERT OR REPLACE INTO raft_state (k, v) VALUES ('last_purged_log_id', ?1)",
                params![bytes],
            )
            .map_err(to_io_err)?;
            Ok(())
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
