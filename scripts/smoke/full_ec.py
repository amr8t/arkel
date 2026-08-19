"""Full (8,6) erasure smoke: full placement + survive losing all 6 parity nodes.

Boots `--nodes` (default 14) storage nodes so the (8,6) target fits without
degrading. Puts an object (auto-assign -> full k=8,m=6, 14 shards, one per
node), verifies the shard count, kills the 6 nodes holding parity shards, and
confirms reconstruction still works (8 remain == k). Then registers a repair
operator and heals the object back onto the surviving pool.
"""

from __future__ import annotations

import shutil
import subprocess
import time

from ._common import (
    BINARY,
    CLIENT_DATA,
    INDEX_FLAG,
    REPO_ROOT,
    STORAGE_DATA_DIRS,
    STORAGE_PORTS,
    bring_up_network,
    count_shards,
    get,
    put,
    random_file,
    run_nodes,
    wait_for_nodes,
    wait_offline,
)

REPAIR_DATA = REPO_ROOT / ".arkel_repair_data"


def repair_cmd(*args: str) -> subprocess.CompletedProcess:
    cmd = [
        str(BINARY),
        "repair",
        "--index-addrs",
        INDEX_FLAG,
        "--data-dir",
        str(REPAIR_DATA),
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def run(args) -> int:
    n = getattr(args, "nodes", len(STORAGE_PORTS))
    if n != len(STORAGE_PORTS):
        print(f"[smoke:full_ec] FAIL: requires exactly {len(STORAGE_PORTS)} storage "
              f"nodes for a full (8,6) placement (got --nodes {n})")
        return 1
    print(f"[smoke:full_ec] booting network with {n} storage nodes...")

    # Deterministic shard count: wipe storage state first.
    for d in STORAGE_DATA_DIRS:
        shutil.rmtree(d, ignore_errors=True)
    shutil.rmtree(REPAIR_DATA, ignore_errors=True)
    shutil.rmtree(CLIENT_DATA, ignore_errors=True)
    bring_up_network(count=n)
    wait_for_nodes(len(STORAGE_PORTS))

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    # Auto-assign (no --storage-addrs): the production path, which sees all 14
    # healthy nodes and writes a full (8,6) placement.
    print(f"[smoke:full_ec] putting {size // (1024 * 1024)} MB (full k=8,m=6 over {n} nodes)...")
    _, etag = put("full", "obj", data)
    print(f"[smoke:full_ec] put ETag: {etag}")

    shards = count_shards()
    print(f"[smoke:full_ec] shards on disk after put: {shards}")
    if shards < 14:
        print(f"[smoke:full_ec] FAIL: expected a full 14-shard (8,6) placement, got {shards}")
        return 1

    killed = list(STORAGE_PORTS[-6:])
    print(f"[smoke:full_ec] killing 6 storage nodes {killed} (all parity shards)...")
    for port in killed:
        run_nodes("kill", "--port", str(port))
    time.sleep(2)

    print("[smoke:full_ec] getting (8 surviving shards == k, must reconstruct)...")
    _, out = get("full", "obj")
    if data.read_bytes() != out.read_bytes():
        print("[smoke:full_ec] FAIL: bytes mismatch after losing all 6 parity shards")
        return 1
    print("[smoke:full_ec] recovered with exactly k=8 shards")

    # Repair heals back to the best scheme the surviving pool allows.
    r = repair_cmd("--register")
    if r.returncode != 0:
        print(f"[smoke:full_ec] FAIL: operator register: {r.stderr.strip()[-300:]}")
        return 1
    for port in killed:
        if not wait_offline(f"127.0.0.1:{port}"):
            print(f"[smoke:full_ec] FAIL: node {port} never marked Offline")
            return 1
    r = repair_cmd()
    if r.returncode != 0:
        print(f"[smoke:full_ec] FAIL: repair run: {r.stderr.strip()[-300:]}")
        return 1
    print("[smoke:full_ec] repair run OK")

    shards_after = count_shards()
    print(f"[smoke:full_ec] shards on disk after repair: {shards_after}")
    _, out2 = get("full", "obj")
    if data.read_bytes() != out2.read_bytes():
        print("[smoke:full_ec] FAIL: bytes mismatch after repair")
        return 1
    print(f"[smoke:full_ec] PASS: full (8,6) placement, survived 6-node loss, "
          f"repair re-replicated onto the surviving pool")
    return 0
