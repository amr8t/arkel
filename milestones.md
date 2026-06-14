# Arkel Milestones

## Phase 1 — MVP: Decentralized S3-Compatible Storage (Weeks 1–14)

**Goal:** A working S3-compatible storage network. Nodes are discovered via a curated set of stable index nodes (no DHT), store erasure-coded + encrypted shards, and expose a standard S3 API. Identity is built in from day one — every node and every object has a cryptographic identity backed by `iroh::SecretKey`/`iroh::NodeId` natively.

**Systems engineering focus:** disk correctness (fsync, partial writes), erasure coding, encryption, manifest-driven downloads, and HTTP gateway design.

---

### M1 — Core Data Plane (Weeks 1–5)

**Gate:** End-to-end round trip — upload → erasure code → encrypt → distribute shards → fetch manifest → reconstruct via `iroh-blobs` → decrypt → verify hash. Do not proceed until this is solid.

**wk 1: Node identity & iroh endpoint**
- Every node generates an `iroh::SecretKey` on first boot. `secret_key.public()` returns the node's `iroh::NodeId` directly — no hex encoding, no `ed25519-dalek`.
- `Endpoint::bind()` with `.secret_key(identity.secret_key.clone())`. Assert `identity.node_id() == endpoint.node_id()` on startup — this is the correctness proof.
- Persist secret key to disk (0600 permissions). Load on restart.
- **No DHT, no `discovery_n0()`.** Endpoint dials purely by `NodeId` + known address.
- **You write:** ~50 lines.

**wk 1: Core trait boundaries**
- Define `StorageBackend`, `MetadataStore`, `AuditProtocol`, `IdentityProvider` traits.
- `IdentityProvider` uses `iroh::NodeId` throughout — no `&str`, no encoding at trait boundaries. `iroh::Signature` is the signature type everywhere.
- No implementations yet — just interfaces. This protects every milestone that follows.
- **You write:** ~80 lines.

**wk 2: Disk persistence + raw blob transfer**
- `StorageBackend` impl: write shard to temp file → `fsync` → `rename()` (atomic). Serve on request.
- Flat files + optional `mmap` for reads. Disk correctness matters here.
- Verify two nodes can exchange a 1 GB blob via `iroh-blobs` with BLAKE3 integrity, dialing by `NodeId` + known address.
- **You write:** ~150 lines. `iroh-blobs`: zero custom code.

**wk 3: Erasure coding + encryption**
- Split object into `k` data shards + `m` parity shards using `reed-solomon-erasure`. Do not write RS yourself.
- Encrypt each shard with `chacha20poly1305` before it leaves the client. Key derived via HKDF over master secret + object ID. Nodes never see plaintext.
- Your work: `k`/`m` params, shard naming, key derivation scheme.
- **You write:** ~150 lines.

**wk 4: ObjectManifest + static index-node registry**
- Signed CBOR document: `object_id`, `bucket`, `key`, shard BLAKE3 hashes, `k`/`m` params, `(iroh::NodeId, iroh-blobs hash)` per shard, encryption metadata. Client signs with `iroh::SecretKey`; signature is `iroh::Signature`. Manifest hash = canonical object reference.
- Hardcoded bootstrap set of 20–30 stable index nodes (`NodeId` + IP:port) in `arkel-node.toml`. Index nodes hold manifests only (~1 KB each), not shards.
- Each index node persists manifests to SQLite (`rusqlite`): `manifests(object_hash TEXT PK, manifest BLOB, received_at INTEGER)`.
- On upload, gateway fans out signed manifest to all online index nodes over custom ALPN `arkel/manifest/1`.
- **You write:** ~200 lines — manifest schema, ALPN handler, fanout, SQLite persistence.

**wk 5: End-to-end integration test (M1 gate)**
- Upload → erasure code → encrypt → distribute shards to 2 local storage nodes → push manifest to 1 local index node → fetch manifest → use `(NodeId, BlobHash)` from manifest to pull `k` shards in parallel via `iroh-blobs` → reconstruct → decrypt → verify hash.
- Verify manifest signature. Verify `iroh::NodeId` matches on shard retrieval.
- **You write:** integration test harness.

---

### M2 — Audit, Repair & Network Health (Weeks 6–9)

**Gate:** Automated repair — kill a storage node mid-test, verify the system detects the failure, reconstructs the missing shard, uploads it to a replacement node, and updates the manifest on all index nodes. All audit messages signed with `iroh::Signature`.

**wk 6: Challenge-response audit protocol + index-node connectivity**
- Custom ALPN `arkel/audit/1`: client sends "return bytes at offsets `[x, y, z]` of `shard_id`" signed by `iroh::SecretKey`. Node returns bytes + BLAKE3 proof signed by its own `iroh::SecretKey`.
- Client verifies without downloading the whole shard.
- On startup, dial all index nodes by `NodeId` + known address. Maintain persistent connections; reconnect on drop. Storage nodes register with index nodes: "I am `NodeId` X, capacity Y GB, addr Z."
- **You write:** ~200 lines — audit ALPN + reconnect loop + registration.

**wk 7: Node health tracking + manifest distribution**
- `HashMap<NodeId, NodeStats>` tracking audit success rate, uptime, latency — updated after every audit. Gateway uses this to pick storage nodes for new uploads.
- `ManifestStore::push(manifest)` fans out to all connected index nodes; any client fetches by `object_hash` trying index nodes in order.
- **You write:** ~180 lines.

**wk 8–9: Automated repair + periodic audit runner**
- If a node fails an audit or goes offline: reconstruct missing shard from `k` survivors, re-encode parity, upload to a healthy replacement node, re-sign updated `ObjectManifest`, push to all index nodes.
- Background tokio task: randomly challenge 1 shard per object per hour. Log failures; repair pipeline handles recovery.
- **You write:** ~230 lines — repair pipeline + `tokio::time::interval` audit loop.

---

### M3 — S3 Gateway + Hardening + Launch (Weeks 10–14)

**Gate:** `rclone sync` passes against your gateway. Disk quota enforced. `aws-cli`, `rclone`, and `s3cmd` all work. Public nodes running.

**wk 10: HTTP skeleton + PUT object**
- `axum` for HTTP. AWS SigV4 via `aws-sigv4` crate — do not write SigV4 yourself. Access key derived deterministically from `iroh::SecretKey`.
- PUT: receive HTTP body → stream to erasure coder → encrypt shards → pick N nodes via health tracker → distribute via `iroh-blobs` → sign `ObjectManifest` → push to index nodes → return ETag (manifest hash).
- **You write:** ~250 lines — axum skeleton + SigV4 wiring + upload pipeline.

**wk 11: GET, HEAD, DELETE, ListObjects**
- GET: fetch manifest from index node → pull `k` shards in parallel via `(NodeId, BlobHash)` from manifest using `iroh-blobs` → decode → decrypt → stream. No custom download logic — `iroh-blobs` owns chunking, verification, resumability.
- HEAD: metadata from manifest only. DELETE: tombstone on index nodes; shards GC'd lazily.
- ListObjects: SQLite `SELECT ORDER BY key` on index node. `aws-cli ls` must work.
- **You write:** ~230 lines.

**wk 12: Multipart upload + CLI**
- `InitiateMultipartUpload` → `UploadPart` (each part independently erasure coded + manifested) → `CompleteMultipartUpload` (merge part manifests → final `ObjectManifest` → push to index nodes).
- `clap` CLI: `arkel-node start --storage-path /data --capacity 100GB --config arkel-node.toml`. Daemonize, PID file, structured logging via `tracing`.
- **You write:** ~250 lines.

**wk 13: Disk hardening + S3 compat smoke tests**
- Disk quota: track bytes per shard, refuse when over capacity. Background GC loop: delete shards for tombstoned objects. `fsync` on all writes.
- Run `aws-cli`, `rclone`, `s3cmd` against your gateway. Fix anything that breaks. This is the M3 gate.
- **You write:** ~120 lines — GC tokio task + quota enforcement.

**wk 14: Deploy public nodes + docs + SDK**
- Deploy 3–5 index nodes and storage nodes to VPS/bare metal. Publish `NodeId` + IP:port as default `arkel-node.toml` config. No DHT needed — these are the bootstrap touchpoints.
- README: how to run a node, how to configure index nodes, S3 endpoint URL.
- Publish Rust client crate: thin S3 API wrapper + `ManifestClient` for direct manifest-based downloads via `iroh-blobs`.
- **You write:** deployment scripts + docs + thin SDK.

---

## Phase 2 — Payments, Payouts & Economic Security on Base L2 (Weeks 15–19)

**Goal:** Turn the storage network into a functional marketplace on Base. Clients prepay for storage. Node operators earn USDC (or ETH) for storing data and passing audits. No custom tokens.

**Design philosophy:** One simple Solidity contract, minimal on-chain state, off-chain accounting with on-chain settlement.

---

### M4 — Base Smart Contract + Deposits + Claims (Weeks 15–17)

**Gate:** A storage node can stake, a client can deposit, an upload locks funds, and a node can claim payout for a completed audit period — all on Base Sepolia.

**wk 15: ArkelVault contract + Rust client**
- `deposit()` — USDC/ETH credited to `mapping(address => uint256) balances`.
- `stake()` — node operators lock collateral. `mapping(address => uint256) stakes`.
- `lockFunds(client, amount, objectHash)` — gateway reserves payment on upload.
- `slash(node, amount)` — oracle callable on audit failure. Slashed funds go to insurance pool.
- `alloy` bindings in Rust. Gateway watches deposit events, maintains local balance cache. Node CLI: `--base-rpc`, `--private-key` flags.
- **You write:** ~270 lines Solidity (+ Foundry tests) + Rust bindings.

**wk 16: Gateway payment gating + signed receipts**
- On PUT/multipart init: call `lockFunds`. Insufficient balance → HTTP `402`. Lock amount = `size * price_per_gb_month * months`. Store in local SQLite ledger keyed by `objectHash`.
- Gateway oracle signs daily receipts: "Node X stored object Y for period Z, passed N audits." Signed with `iroh::SecretKey` → `iroh::Signature`. Nodes collect locally.
- **You write:** ~160 lines.

**wk 17: On-chain claims + automated payout loop**
- Node calls `claimPayout(receipt, signature)` on ArkelVault. Contract verifies signature, checks receipt not already claimed, releases funds.
- Background tokio task: aggregate receipts every 24h, batch-submit to Base. Gateway oracle publishes audit results for slashing.
- **You write:** ~200 lines Solidity + Rust payout loop.

---

### M5 — Staking, Slashing & Launch (Weeks 18–19)

**Gate:** Funded beta live on Base mainnet — nodes staked, clients depositing, payouts flowing.

**wk 18: Staking gate + slash logic + operator dashboard**
- Gateway refuses shard allocation to nodes with `stake < minStake` (e.g., 50 USDC). Node CLI: `arkel-node stake --amount 50`.
- Oracle calls `slash()` after >3 audit failures in 24h.
- Operator dashboard (lightweight TUI or static HTML): stake, earnings, claimable balance, audit history — reads Base contract events + local SQLite.
- **You write:** ~160 lines.

**wk 19: Mainnet launch**
- Deploy ArkelVault to Base mainnet (after Sepolia validation).
- Update docs: "How to run a paid node," "Pricing," "How to withdraw."
- Seed 3 nodes with stake, invite beta users to deposit small amounts.
- **You write:** deployment scripts + docs.

---

## Principles

1. **Stay lean** — Phase 1 targets completion in ~14 weeks by one person.
2. **iroh-native identity** — `iroh::SecretKey`, `iroh::NodeId`, and `iroh::Signature` everywhere. No `ed25519-dalek`, no `hex`, no string-encoded peer IDs at any boundary. One key, one identity — asserted on startup.
3. **No DHT** — Discovery is a static, curated list of 20–30 stable index nodes in default config. Simple, auditable, zero extra infrastructure.
4. **Index nodes hold ObjectManifests** — lightweight (~1 KB each), SQLite-backed, the single stable touchpoint. Storage nodes hold shards; index nodes hold routing.
5. **iroh-blobs drives downloads** — `ObjectManifest` stores `(NodeId, BlobHash)` per shard. Downloads are manifest-driven: fetch manifest → hand `(NodeId, BlobHash)` pairs to `iroh-blobs`. BLAKE3 integrity and resumability free.
6. **Identity everywhere** — node IDs, manifests, audits, receipts, and payments are all cryptographically bound to `iroh::SecretKey`/`iroh::NodeId` from day one.
7. **Systems engineering depth** — fsync, partial writes, erasure coding, encryption, disk quota. Make them correct.
8. **S3 compatibility is the moat** — `aws-cli`, `rclone`, and `s3cmd` must work out of the box.
9. **Base L2 for payments** — USDC/ETH, simple escrow, off-chain receipts with on-chain settlement. No custom token, no payment channels.
