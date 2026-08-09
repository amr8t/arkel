"""Quorum smoke: kill one index node, verify Raft quorum still serves reads/writes.

A 3-node Raft cluster tolerates 1 failure. Killing index node 8001 leaves 2 of 3
nodes; committed metadata is still readable and new writes still replicate to the
surviving quorum.
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

BUCKET = "quorum"


def run(args) -> int:
    print("[smoke:quorum] booting network...")
    flag = bring_up_network()

    data = random_file(64 * 1024)
    print("[smoke:quorum] writing 3 objects...")
    for i in range(3):
        put(BUCKET, f"obj-{i}", data, storage_flag=flag)
    print("[smoke:quorum] 3 objects committed.")

    print("[smoke:quorum] killing index node 8001...")
    run_nodes("kill", "--port", "8001")
    time.sleep(2)

    print("[smoke:quorum] writing a 4th object (quorum must still replicate)...")
    put(BUCKET, "obj-3", data, storage_flag=flag)

    print("[smoke:quorum] reading object 1 and the new object...")
    _, out1 = get(BUCKET, "obj-1", storage_flag=flag)
    _, out3 = get(BUCKET, "obj-3", storage_flag=flag)

    if out1.read_bytes() == data.read_bytes() and out3.read_bytes() == data.read_bytes():
        print("[smoke:quorum] PASS: reads + writes work after killing 1 of 3 index nodes (Raft 2/3)")
        return 0
    print("[smoke:quorum] FAIL: data inconsistent after index node loss")
    return 1
