"""Parity smoke: kill one storage node, verify EC fault tolerance.

Puts with explicit --storage-addrs so the object uses full k=4,m=2 (6 shards
across 3 nodes, 2 per node). Killing storage node 9001 loses 2 shards; the
remaining 4 >= k, so reconstruction must still succeed — get skips failed
shards and uses the parity slack (read-fault-tolerant).
"""

from __future__ import annotations

import time

from ._common import (
    bring_up_network,
    get,
    put,
    random_file,
    run_nodes,
)


def run(args) -> int:
    print("[smoke:parity] booting network...")
    flag = bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    print(f"[smoke:parity] putting {size // (1024 * 1024)} MB (full k=4,m=2 over 3 nodes)...")
    _, etag = put("parity", "obj", data, storage_flag=flag)
    print(f"[smoke:parity] put ETag: {etag}")

    print("[smoke:parity] killing storage node 9001 (2 of 6 shards lost)...")
    run_nodes("kill", "--port", "9001")
    time.sleep(2)

    print("[smoke:parity] getting (parity must cover the 2 lost shards)...")
    _, out = get("parity", "obj", storage_flag=flag)

    if data.read_bytes() == out.read_bytes():
        print(f"[smoke:parity] PASS: {size // (1024 * 1024)} MB recovered after killing 1 of 3 "
              f"storage nodes (m=2 parity covered the loss)")
        return 0
    print("[smoke:parity] FAIL: bytes mismatch after node loss")
    return 1
