"""Relay smoke: one storage node advertises an unreachable addr, forcing the
client to dial it via the relay. Verifies the relay-fallback path end-to-end
(home-NAT scenario: identity + relay dialing when direct is unreachable).
"""

from __future__ import annotations

import subprocess
import time

from ._common import (
    BINARY,
    REPO_ROOT,
    get,
    put,
    random_file,
    run_nodes,
    storage_addrs,
    wait_for_nodes,
)

INDEX_FLAG = "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"
UNREACHABLE = "192.0.2.1:9001"  # TEST-NET, unroutable


def _spawn_storage(port: str, extra: list[str]) -> None:
    """Spawn a storage node; logs go to storage{index}.log (9001 -> storage1.log)."""
    index = int(port) - 9000
    log = open(REPO_ROOT / "logs" / f"storage{index}.log", "wb")
    cmd = [str(BINARY), "storage", "--addr", f"127.0.0.1:{port}", *extra,
           "--index-addrs", INDEX_FLAG, "--data-dir",
           str(REPO_ROOT / f".arkel_storage_{port}_data")]
    subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                     stdin=subprocess.DEVNULL, start_new_session=True)


def run(args) -> int:
    print(f"[smoke:relay] booting index + storage (9001 advertises unreachable {UNREACHABLE})...")
    run_nodes("kill", "--all")
    time.sleep(1)
    run_nodes("start", "--fresh")
    _spawn_storage("9001", ["--advertise-addr", UNREACHABLE])
    _spawn_storage("9002", [])
    _spawn_storage("9003", [])
    storage_addrs()  # logs ready (relay URLs captured)
    wait_for_nodes(3)  # registered in the index registry

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    print(f"[smoke:relay] putting {size // (1024 * 1024)} MB "
          "(shards round-robin incl. the unreachable node)...")
    _, etag = put("relay", "obj", data)
    print(f"[smoke:relay] put ETag: {etag}")

    print("[smoke:relay] getting (shards from the unreachable node must come via relay)...")
    _, out = get("relay", "obj")
    if out.read_bytes() == data.read_bytes():
        print("[smoke:relay] PASS: unreachable node's shards pulled via relay (BLAKE3 verified)")
        return 0
    print("[smoke:relay] FAIL: bytes mismatch")
    return 1
