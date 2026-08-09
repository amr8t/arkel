"""Shared helpers for smoke scenarios: bring up the standard local network and
drive `arkel client` put/get, reporting timings."""

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
STORAGE_PORTS = (9001, 9002, 9003)


def run_nodes(*cmd: str) -> None:
    subprocess.run([sys.executable, str(SCRIPTS / "run_nodes.py"), *cmd], check=True)


def bring_up_network() -> str:
    """Boot index + storage nodes, return the storage-addrs flag value.

    Self-cleans first: stale nodes from prior runs hold ports and write to
    deleted inodes, leaving empty logs. kill --all avoids the stacking.
    """
    run_nodes("kill", "--all")
    time.sleep(1)
    run_nodes("start", "--fresh")
    run_nodes("start-storage")
    time.sleep(3)
    return storage_addrs()


def storage_addrs(timeout: float = 20.0) -> str:
    """Poll the storage logs until all 3 endpointIds are present (startup latency)."""
    deadline = time.monotonic() + timeout
    while True:
        parts: list[str] = []
        for i, port in enumerate(STORAGE_PORTS, start=1):
            log = (REPO_ROOT / "logs" / f"storage{i}.log").read_text(errors="replace")
            for line in log.splitlines():
                if "endpointId:" in line:
                    pubkey = line.split("endpointId:", 1)[1].split(".")[0].strip()
                    parts.append(f"{pubkey}@127.0.0.1:{port}")
                    break
        if len(parts) == 3:
            return ",".join(parts)
        if time.monotonic() > deadline:
            raise RuntimeError(f"expected 3 storage pubkeys, got {len(parts)}")
        time.sleep(0.5)


def random_file(size_bytes: int) -> Path:
    p = Path(tempfile.mkdtemp(prefix="arkel-smoke-data-")) / "data.bin"
    p.write_bytes(os.urandom(size_bytes))
    return p


def _client(args: list[str], cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(BINARY), "client", *args], cwd=cwd, capture_output=True, text=True
    )


def put(bucket: str, key: str, data: Path, storage_flag: str = "") -> tuple[float, str]:
    """Upload `data`; returns (elapsed_seconds, etag).

    Wipes only the blob cache, NOT the identity — the client's key must persist
    so manifest signatures verify on get (it's the signing + decryption key).
    """
    shutil.rmtree(CLIENT_DATA / "blobs", ignore_errors=True)
    args = ["put", str(data), "--bucket", bucket, "--key", key, "--index-addrs", INDEX_FLAG]
    if storage_flag:
        args += ["--storage-addrs", storage_flag]
    start = time.perf_counter()
    r = _client(args)
    elapsed = time.perf_counter() - start
    if r.returncode != 0:
        raise RuntimeError(f"put failed: {r.stderr.strip()[-400:]}")
    return elapsed, r.stdout.strip().splitlines()[-1]


def get(bucket: str, key: str, storage_flag: str = "") -> tuple[float, Path]:
    """Download (network-only: wipe client blobs first); returns (elapsed_seconds, out_path)."""
    shutil.rmtree(CLIENT_DATA / "blobs", ignore_errors=True)
    out = Path(tempfile.mkdtemp(prefix="arkel-smoke-out-")) / "out.bin"
    args = ["get", bucket, key, "--index-addrs", INDEX_FLAG, "--output", str(out)]
    if storage_flag:
        args += ["--storage-addrs", storage_flag]
    start = time.perf_counter()
    r = _client(args)
    elapsed = time.perf_counter() - start
    if r.returncode != 0:
        raise RuntimeError(f"get failed: {r.stderr.strip()[-400:]}")
    return elapsed, out
