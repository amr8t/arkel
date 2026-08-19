"""Concentration smoke: an over-concentrated object gets redistributed by repair.

Puts with explicit --storage-addrs pointing at only 3 of the 14 nodes — the
explicit-target path uses the full (8,6) config, so all 14 shards get crammed
onto 3 nodes (5/5/4). The manifest records k=8,m=6 so repair's old scheme-only
predicate would skip it as "healthy". Verifies repair detects the concentration
(distinct nodes < total) and re-spreads to one shard per node.
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
    get,
    put,
    random_file,
    storage_addrs,
    wait_for_nodes,
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


def dirs_with_shards() -> int:
    """How many storage data dirs currently hold at least one shard blob."""
    n = 0
    for d in STORAGE_DATA_DIRS:
        bd = d / "blobs" / "data"
        if bd.is_dir() and any(
            e.is_file() and e.name.endswith(".data") for e in bd.iterdir()
        ):
            n += 1
    return n


def run(args) -> int:
    print("[smoke:concentration] booting 14 storage nodes...")
    for d in STORAGE_DATA_DIRS:
        shutil.rmtree(d, ignore_errors=True)
    shutil.rmtree(REPAIR_DATA, ignore_errors=True)
    shutil.rmtree(CLIENT_DATA, ignore_errors=True)
    bring_up_network(count=len(STORAGE_PORTS))
    wait_for_nodes(len(STORAGE_PORTS))

    # Explicit targets = only the first 3 nodes; the full (8,6) config crams
    # 14 shards onto them.
    small_flag = storage_addrs(3)
    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    print("[smoke:concentration] putting with explicit 3-node targets (14 shards, concentrated)...")
    put("con", "obj", data, storage_flag=small_flag)

    before = dirs_with_shards()
    print(f"[smoke:concentration] storage dirs with shards after put: {before}")
    if before != 3:
        print(f"[smoke:concentration] FAIL: expected shards on exactly 3 nodes, got {before}")
        return 1

    r = repair_cmd("--register")
    if r.returncode != 0:
        print(f"[smoke:concentration] FAIL: operator register: {r.stderr.strip()[-300:]}")
        return 1

    r = repair_cmd()
    if r.returncode != 0:
        print(f"[smoke:concentration] FAIL: repair run: {r.stderr.strip()[-300:]}")
        return 1
    print("[smoke:concentration] repair run OK")

    after = dirs_with_shards()
    print(f"[smoke:concentration] storage dirs with shards after repair: {after}")
    if after < len(STORAGE_PORTS):
        print(f"[smoke:concentration] FAIL: expected redistribution across all "
              f"{len(STORAGE_PORTS)} nodes, got {after}")
        return 1

    _, out = get("con", "obj")
    if data.read_bytes() != out.read_bytes():
        print("[smoke:concentration] FAIL: bytes mismatch after redistribution")
        return 1
    print(f"[smoke:concentration] PASS: concentrated 3-node placement redistributed "
          f"to {after} nodes, bytes verified")
    return 0
