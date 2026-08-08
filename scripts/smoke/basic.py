"""Basic data-plane smoke: boot index + storage, put -> get -> verify bytes match.

Drives the real `arkel client` CLI over real processes. The GET runs with a
wiped client cache so shards must come from the storage nodes over the network
(not the client's own local store). Uses `run_nodes.py start/start-storage`.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS = REPO_ROOT / "scripts"
BINARY = REPO_ROOT / "target" / "debug" / "arkel"
CLIENT_DATA = REPO_ROOT / ".arkel_client_data"
INDEX_FLAG = ",".join(f"http://127.0.0.1:{p}" for p in (8001, 8002, 8003))


def _storage_addrs() -> str:
    parts: list[str] = []
    for i, port in enumerate((9001, 9002, 9003), start=1):
        log = (REPO_ROOT / "logs" / f"storage{i}.log").read_text(errors="replace")
        for line in log.splitlines():
            if "endpointId:" in line:
                pubkey = line.split("endpointId:", 1)[1].split(".")[0].strip()
                parts.append(f"{pubkey}@127.0.0.1:{port}")
                break
    if len(parts) != 3:
        raise RuntimeError(f"expected 3 storage pubkeys, got {len(parts)}")
    return ",".join(parts)


def run(args) -> int:
    print("[smoke:basic] booting 3 index nodes + 3 storage nodes...")
    for cmd in (["start", "--fresh"], ["start-storage"]):
        subprocess.run([sys.executable, str(SCRIPTS / "run_nodes.py"), *cmd], check=True)
    time.sleep(3)
    storage_flag = _storage_addrs()
    index_flag = INDEX_FLAG

    size = max(1, int(args.mb * 1024 * 1024))
    tmp = Path(tempfile.mkdtemp(prefix="arkel-smoke-"))
    data, out = tmp / "data.bin", tmp / "out.bin"
    data.write_bytes(os.urandom(size))

    if CLIENT_DATA.exists():
        shutil.rmtree(CLIENT_DATA)

    print(f"[smoke:basic] putting {size // (1024 * 1024)} MB...")
    put = subprocess.run(
        [str(BINARY), "client", "put", str(data),
         "--bucket", "smoke", "--key", "data.bin",
         "--index-addrs", index_flag, "--storage-addrs", storage_flag],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if put.returncode != 0:
        print(f"[smoke:basic] FAIL: put failed: {put.stderr.strip()[-400:]}")
        return 1
    etag = put.stdout.strip().splitlines()[-1]
    print(f"[smoke:basic] put ETag: {etag}")

    # Wipe the client blob cache so GET must come from storage over the network.
    shutil.rmtree(CLIENT_DATA / "blobs", ignore_errors=True)

    print("[smoke:basic] getting (network, cache wiped)...")
    get = subprocess.run(
        [str(BINARY), "client", "get", "smoke", "data.bin",
         "--index-addrs", index_flag, "--storage-addrs", storage_flag,
         "--output", str(out)],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if get.returncode != 0:
        print(f"[smoke:basic] FAIL: get failed: {get.stderr.strip()[-400:]}")
        return 1

    if data.read_bytes() == out.read_bytes():
        print(f"[smoke:basic] PASS: {size // (1024 * 1024)} MB roundtrip verified "
              "(EC + encrypt + pull-based shards + Raft manifest + reconstruct, BLAKE3)")
        return 0
    print("[smoke:basic] FAIL: bytes mismatch")
    return 1
