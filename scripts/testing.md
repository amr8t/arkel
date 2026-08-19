# Arkel testing

## Cluster lifecycle (`scripts/run_nodes.py`)

Start a fresh cluster (3 index nodes 8001-8003 + storage nodes 9001-9014):

    python scripts/run_nodes.py start --fresh

Start `N` storage nodes (default 3, max 14 — the max supports a full (8,6)
erasure placement of 14 shards):

    python scripts/run_nodes.py start-storage --count 14

Per-node index lifecycle (state is wiped but preserve the identity):

    python scripts/run_nodes.py kill --port 8001    # stop one node
    python scripts/run_nodes.py wipe --port 8001    # wipe its state, keep identity
    python scripts/run_nodes.py start --port 8001   # rejoin using saved identity + peers
    python scripts/run_nodes.py wipe --all          # wipe all index state
    python scripts/run_nodes.py clean               # stop everything + delete data/logs

Demo data — the index API is **signed** (access control), so use the CLI, not curl:

    cargo build
    ./target/debug/arkel client put hello.txt --bucket demo          # key defaults to filename
    ./target/debug/arkel client get demo hello.txt
    ./target/debug/arkel client rm demo hello.txt

## Performance (`run --performance`)

Steady-state (~2k writes/s) + 10x burst against the Raft leader:

    python scripts/run_nodes.py run --performance
    # tune: --perf-duration, --perf-concurrency, --perf-burst, --perf-target-rate

Network emulation (tc-netem on loopback, requires sudo):

    python scripts/run_nodes.py run --performance --perf-latency-ms 10 --perf-jitter-ms 3


## Smoke scenarios (`run_nodes.py smoke <name>`)

    basic    put→get roundtrip
    delete   rm + shard GC
    access   two-identity authz (B can't rm/overwrite A's object)
    parity   kill a storage node, EC recovers
    quorum   kill an index node, Raft 2/3
    relay    NAT'd node's shards pulled via relay
    repair   kill a node → repair re-encodes/redistributes
    full_ec  full (8,6) over 14 nodes; survive losing all 6 parity nodes + repair
    concentration  explicit 3-node put (14 shards crammed) → repair spreads to all 14
    account  quota credit/debit, 507 over limit, release, idempotency
    perf     throughput benchmark

Run all:

    for s in basic delete access parity quorum relay repair account; do \
      python scripts/run_nodes.py smoke $s; done

Count-parameterized scenarios take `--nodes` (default 3); `full_ec` defaults
to 14:

    python scripts/run_nodes.py smoke full_ec
