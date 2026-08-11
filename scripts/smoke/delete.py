"""Delete + shard GC smoke: put -> rm -> get 404 -> storage nodes GC the shards.

Drives the real `arkel client` CLI over real processes. Verifies the whole
point-2 chain: DeleteManifest via Raft, refs drop to 0, and the storage-node
GC loop actually removes the unreferenced shards from disk.
"""

from __future__ import annotations

import shutil
import time

from ._common import (
    STORAGE_DATA_DIRS,
    bring_up_network,
    count_shards,
    get,
    put,
    random_file,
    restart_storage,
    rm,
    REPO_ROOT,
)


def run(args) -> int:
    print("[smoke:delete] booting 3 index nodes + 3 storage nodes...")

    # Deterministic shard count: wipe storage state first.
    for d in STORAGE_DATA_DIRS:
        shutil.rmtree(d, ignore_errors=True)
    bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    print(f"[smoke:delete] putting {size // (1024 * 1024)} MB...")
    etag = put("smoke", "data.bin", data)
    print(f"[smoke:delete] put ETag: {etag}")

    shards_before = count_shards()
    print(f"[smoke:delete] shards on disk after put: {shards_before}")
    if shards_before == 0:
        print("[smoke:delete] FAIL: no shards stored after put")
        return 1

    print("[smoke:delete] deleting object...")
    rm("smoke", "data.bin")

    print("[smoke:delete] get after rm must fail (404)...")
    try:
        get("smoke", "data.bin")
        print("[smoke:delete] FAIL: get succeeded after rm")
        return 1
    except RuntimeError as e:
        print(f"[smoke:delete] get failed as expected: {str(e)[-80:]}")

    print("[smoke:delete] restarting storage nodes with 5s GC interval...")
    restart_storage(gc_interval_secs=5)
    time.sleep(8)  # a few GC ticks

    shards_after = count_shards()
    print(f"[smoke:delete] shards on disk after GC: {shards_after}")
    if shards_after == 0:
        print(
            f"[smoke:delete] PASS: put={shards_before} shards -> rm -> "
            f"GC removed them all ({shards_after})"
        )
        return 0
    print(f"[smoke:delete] FAIL: {shards_after} shards remain after GC")
    return 1
