"""Repair smoke: kill a storage node, heal the object back to target k/m.

Flow:
  bring up network
  put object (auto-assigned across 3 storage nodes)
  register repair operator (fresh identity)
  kill storage node 9001 (loses its shard(s))
  wait until 9001 is marked Offline in /nodes/all
  run `arkel repair`
  assert repair fixed the object (log line) and get still returns matching bytes
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import time

from ._common import (
    BINARY,
    INDEX_FLAG,
    REPO_ROOT,
    bring_up_network,
    get,
    put,
    random_file,
    SCRIPTS,
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
    print("[smoke:repair] booting 3 index nodes + 3 storage nodes...")

    shutil.rmtree(REPAIR_DATA, ignore_errors=True)
    shutil.rmtree(REPO_ROOT / ".arkel_client_data", ignore_errors=True)
    bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    put("smoke", "data.bin", data)
    print("[smoke:repair] put OK")

    r = repair_cmd("--register")
    if r.returncode != 0:
        print(f"[smoke:repair] FAIL: operator register: {r.stderr.strip()[-300:]}")
        return 1
    print("[smoke:repair] repair operator registered")

    print("[smoke:repair] killing storage node 9001...")
    subprocess.run(
        [sys.executable, str(SCRIPTS / "run_nodes.py"), "kill", "--port", "9001"],
        check=True,
    )
    if not wait_offline("127.0.0.1:9001"):
        print("[smoke:repair] FAIL: node 9001 never marked Offline")
        return 1
    print("[smoke:repair] node 9001 Offline")

    r = repair_cmd()
    if r.returncode != 0:
        print(f"[smoke:repair] FAIL: repair run: {r.stderr.strip()[-300:]}")
        return 1
    repaired = "repaired smoke/data.bin" in (r.stdout + r.stderr)
    print(f"[smoke:repair] repair run OK (fixed object: {repaired})")
    if not repaired:
        print("[smoke:repair] FAIL: repair did not fix the object")
        return 1

    _, out = get("smoke", "data.bin")
    if data.read_bytes() == out.read_bytes():
        print("[smoke:repair] PASS: object survived node death and was repaired")
        return 0
    print("[smoke:repair] FAIL: bytes mismatch")
    return 1
