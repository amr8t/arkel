# Arkel Milestones

## Phase 1 — MVP: S3-Compatible Decentralized Storage (No Payments)

**Timeline:** Weeks 1–13  
**Goal:** A working, publicly launchable S3-compatible storage network. Index nodes form a Raft cluster and expose a standard object API. Storage nodes persist erasure-coded, encrypted shards. Identity is cryptographic from day one via `iroh::SecretKey`/`iroh::NodeId`. Discovery uses a curated static list of index nodes — **no DHT, no payments, no on-chain logic**.

**Systems engineering focus:** Raft consensus correctness, disk correctness (`fsync`, atomic writes), erasure coding, encryption, manifest-driven routing, and S3 compatibility.

---

### M1 — Identity, Index Consensus & Basic API (Weeks 1–4)

**Gate:** Three index nodes form a Raft cluster and replicate a metadata write end-to-end. A `curl` PUT against the leader returns the same object metadata from every node.

**wk 1: Node identity & direct iroh endpoint**
- ✅ Every node generates an `iroh::SecretKey` on first boot. `secret_key.public()` returns the node's `iroh::NodeId` directly — no hex encoding, no `ed25519-dalek`.
- ✅ Persist secret key to disk (`0600` permissions). Load on restart.
- ✅ `Endpoint::bind()` with `.secret_key(identity.secret_key.clone())`. Assert `identity.node_id() == endpoint.node_id()` on startup.
- ✅ **No DHT, no N0 relay/discovery.** Index-node endpoints are bare QUIC sockets bound directly to a known `SocketAddr`. Storage-node endpoints use `iroh::endpoint::presets::N0` but may be disabled.
- ✅ **You wrote:** ~50 lines.

**wk 1–2: Index-node Raft cluster + HTTP API skeleton**
- ✅ `axum` HTTP server bound to the index node's advertised address.
- ✅ Routes: `PUT /:bucket`, `PUT /:bucket/:key`, `GET /:bucket/:key`, `GET /` (list buckets).
- ✅ Writes (`PUT`) are committed through OpenRaft's `client_write`; reads hit the local state machine directly.
- ✅ CLI: `arkel index --http-addr <addr> --peer-addresses <pubkey@ip:port,...>`.
- ✅ Integration test: start 3 nodes with shared membership, write a bucket+object through the leader, read identical metadata from all 3 nodes.
- ✅ **You wrote:** ~300 lines.

**wk 3–4: Persistent Raft log & snapshot plumbing**
- Persist OpenRaft log, vote, and state machine to SQLite/sled so index nodes survive restarts without losing committed state. Currently in-memory (`HashMap`); needs durability.
- Wire snapshot builder to write/read a real snapshot rather than the current empty cursor stub.
- **You write:** ~180 lines.

---

### M2 — Storage Data Plane: Shards, Erasure Coding & Encryption (Weeks 5–9)

**Gate:** Upload an object, split it into encrypted shards, exchange a 1 GB blob between two storage nodes via `iroh-blobs`, reconstruct and decrypt locally, and verify the BLAKE3 hash.

**wk 5: Storage node skeleton + disk persistence + core trait boundaries**
- `arkel storage --data-dir <path> [--private-relay-url <url>]` CLI.
- Define `StorageBackend`, `MetadataStore`, `ManifestStore`, `IdentityProvider` traits now that the storage module exists to implement them against. `IdentityProvider` uses `iroh::NodeId` throughout — no `&str`, no encoding at trait boundaries.
- `StorageBackend` impl: write shard to temp file → `fsync` → `rename()` (atomic). Serve on request.
- Flat files + optional `mmap` for reads. Disk correctness matters here.
- **You write:** ~200 lines (traits ~80, storage impl ~120).

**wk 6: Raw blob transfer via `iroh-blobs`**
- Two storage nodes can exchange a 1 GB blob with BLAKE3 integrity, dialing by `NodeId` + known address.
- Storage nodes register with index nodes: "I am `NodeId` X, capacity Y GB, addr Z."
- **You write:** ~150 lines. `iroh-blobs`: zero custom code.

**wk 7: Erasure coding**
- Split object into `k` data shards + `m` parity shards using `reed-solomon-erasure`. Do not write RS yourself.
- Configurable `k`/`m` params and shard naming.
- **You write:** ~120 lines.

**wk 8–9: Encryption + M2 gate**
- Encrypt each shard with `chacha20poly1305` before it leaves the client. Key derived via HKDF over master secret + object ID (~35 lines). Nodes never see plaintext.
- End-to-end test: upload → erasure code → encrypt → distribute shards to 2 local storage nodes → fetch `k` shards sequentially via `(NodeId, BlobHash)` using `iroh-blobs` → reconstruct → decrypt → verify BLAKE3. Sequential fetch is fine here; parallelism comes in wk 12 with the real gateway.
- **You write:** ~150 lines.

---

### M3 — Manifests, S3 Gateway & Public Launch (Weeks 10–13)

**Gate:** `rclone sync` passes against your gateway. `aws-cli`, `rclone`, and `s3cmd` all work. Public nodes running.

**wk 10: `ObjectManifest` + new Raft command**
- Define `ObjectManifest` struct: replaces the raw `data` field in `StoredObject` with structured fields — `object_id`, shard BLAKE3 hashes, `k`/`m` params, `Vec<(NodeId, BlobHash)>` per shard, encryption metadata. CBOR-serialized.
- Client signs the serialized manifest with `iroh::SecretKey`; `iroh::Signature` stored inline. Manifest BLAKE3 hash = ETag.
- Add `CommitManifest { object_hash, manifest }` alongside the existing `Write` command in `ArkelRequest`. State machine stores it the same way; `GetManifest { object_hash }` is the new read path. Raft replication and durability are free — no new plumbing needed.
- Bootstrap index node list (`NodeId` + IP:port) added to existing CLI config in `arkel-node.toml`.
- **You write:** ~120 lines — `ObjectManifest` struct + `CommitManifest`/`GetManifest` command variants + state machine arms.

**wk 11: Storage node registry + health tracking**
- Storage nodes register on startup: "I am `NodeId` X, capacity Y GB, addr Z." Stored as a Raft-replicated `NodeRegistry` entry.
- `HashMap<NodeId, NodeStats>` on the gateway: last-seen timestamp, reported capacity, online/offline. Updated on registration heartbeat (every 30s). Gateway skips offline nodes when routing shard writes and reads.
- Reconnect loop: storage nodes re-register on index node reconnect.
- **You write:** ~120 lines — registration heartbeat + NodeStats tracker + reconnect loop.

**wk 12: S3 API surface + disk hardening**
- `axum` HTTP gateway. AWS SigV4 via `aws-sigv4` crate — do not write SigV4 yourself. Access key derived deterministically from `iroh::SecretKey`.
- `PUT` object: HTTP body → stream to erasure coder → encrypt shards → pick N nodes via health tracker → distribute via `iroh-blobs` → sign `ObjectManifest` → commit via Raft → return ETag (manifest hash).
- `GET`, `HEAD`, `DELETE`, `ListObjects`.
- Disk quota: track bytes per shard, refuse when over capacity. Background GC loop: delete shards for tombstoned objects. `fsync` on all writes.
- **You write:** ~280 lines.

**wk 13: Public deployment + docs**
- Deploy 3–5 index nodes and storage nodes to VPS/bare metal. Publish `NodeId` + IP:port as default `arkel-node.toml` config.
- Run `aws-cli`, `rclone`, `s3cmd` against your gateway; fix anything that breaks.
- README: how to run a node, how to configure index nodes, S3 endpoint URL.
- **You write:** deployment scripts + docs.

*Deferred to Phase 2 week 14: multipart upload, Rust client SDK.*

---

## Phase 2 — Quota System: Pay or Contribute Storage (Weeks 14–19)  

**Goal:** Turn the storage network into a self-sustaining marketplace without cryptocurrency or blockchain. Two paths to quota: pay with fiat or contribute storage capacity and receive 60–70% of your contribution as usable quota. Simple, auditable, no on-chain logic.

**Design philosophy:** Quota ledger lives in SQLite on index nodes, replicated via Raft. Fiat payments handled by a standard payment processor (Stripe). Storage contributions verified by the existing node health tracking.

---

### M4 — Polish & Quota Ledger (Weeks 14–16)

**Gate:** A node operator can register contributed storage and receive quota. A client can see their available quota enforced on PUT.

**wk 14: Multipart upload + thin SDK**
- `InitiateMultipartUpload` → `UploadPart` (each part independently erasure coded + manifested) → `CompleteMultipartUpload` (merge part manifests → final `ObjectManifest` → commit via Raft).
- Publish Rust client crate: thin S3 API wrapper + `ManifestClient` for direct manifest-based downloads via `iroh-blobs`.
- **You write:** ~200 lines + SDK.

**wk 15: Quota ledger on index nodes**
- New Raft commands: `AllocateQuota { node_id, bytes }`, `ConsumeQuota { client_id, bytes }`, `ReleaseQuota { client_id, bytes }`.
- SQLite on index nodes: `quota(account_id TEXT PK, total_bytes INTEGER, used_bytes INTEGER, source TEXT)`. Replicated via Raft like object metadata.
- Gateway enforces quota on `PUT`: check available bytes before distributing shards → `507 Insufficient Storage` if over limit.
- **You write:** ~200 lines — new Raft commands + SQLite schema + gateway enforcement.

**wk 16: Storage contribution path**
- Node operators declare contributed capacity on registration: `arkel storage --contribute 500GB`.
- Index nodes track contributed capacity per `NodeId`. When a storage node has been online for ≥24h, index nodes grant the operator account `contributed_bytes * 0.65` as quota (configurable 60–70%).
- Quota credited automatically via a background Raft proposal run by the leader every hour.
- Storage node can withdraw contribution (graceful drain: shard re-replication runs before quota is revoked).
- **You write:** ~180 lines — contribution registration + eligibility checker + drain logic.

---

### M5 — Fiat Payments, Hardening & Launch (Weeks 17–19)

**Gate:** Paying customers and contributing operators both work end-to-end. Abuse cases handled gracefully.

**wk 17: Fiat payment path**
- Stripe Checkout integration: client hits `POST /account/topup?bytes=10GB` → redirect to Stripe hosted page → webhook on success → leader proposes `AllocateQuota` via Raft.
- Idempotency: Stripe event ID stored in SQLite to prevent double-credit.
- Pricing table in `arkel-node.toml` (e.g., `$0.02/GB/month`). Quota expires after paid period; GC tombstones objects of expired accounts after a grace window.
- CLI: `arkel account quota`, `arkel account topup`.
- **You write:** ~220 lines — Stripe webhook handler + expiry GC + CLI commands.

**wk 18: Abuse prevention + operator dashboard**
- Cap contribution credit: a single node may not contribute more than `max_contribution_per_node` (config, e.g. 10 TB) to prevent Sybil quota farming.
- Integrity gate: quota is only credited while the node is online and healthy per `NodeStats`. Failed nodes have their contribution quota frozen (not revoked) until they recover.
- Operator dashboard (lightweight TUI or static HTML): contributed capacity, credited quota, node health, account balance — reads index node HTTP API + local SQLite.
- **You write:** ~180 lines.

**wk 19: Public launch**
- Update docs: "How to contribute storage," "How to purchase quota," "Pricing."
- Seed the network with 3 contributing nodes with pre-granted quota to bootstrap storage availability.
- Run full end-to-end smoke test: contribute storage → receive quota → upload via S3 gateway → verify via `rclone`.
- **You write:** deployment scripts + docs + smoke test.

---

## Principles

1. **Stay lean** — Phase 1 targets launch readiness in ~13 weeks by one person, with no payments complexity.
2. **iroh-native identity** — `iroh::SecretKey`, `iroh::NodeId`, and `iroh::Signature` everywhere. No `ed25519-dalek`, no `hex`, no string-encoded peer IDs at any boundary. One key, one identity — asserted on startup.
3. **No DHT** — Discovery is a static, curated list of stable index nodes in default config. Simple, auditable, zero extra infrastructure.
4. **Index nodes hold ObjectManifests** — lightweight (~1 KB each), SQLite-backed, the single stable touchpoint. Storage nodes hold shards; index nodes hold routing.
5. **iroh-blobs drives downloads** — `ObjectManifest` stores `(NodeId, BlobHash)` per shard. Downloads are manifest-driven: fetch manifest → hand `(NodeId, BlobHash)` pairs to `iroh-blobs`. BLAKE3 integrity and resumability free.
6. **Identity everywhere** — node IDs, manifests, integrity checks, and quota grants are all cryptographically bound to `iroh::SecretKey`/`iroh::NodeId` from day one.
7. **Systems engineering depth** — fsync, partial writes, erasure coding, encryption, disk quota. Make them correct.
8. **S3 compatibility is the moat** — `aws-cli`, `rclone`, and `s3cmd` must work out of the box.
9. **Phase 1 runs without payments** — it is a working, public, free storage network. Quota enforcement and contribution tracking are Phase 2.
10. **Two paths to quota** — pay with fiat (Stripe) or contribute storage capacity and receive 60–70% of it as usable quota. No cryptocurrency, no blockchain, no custom tokens.
