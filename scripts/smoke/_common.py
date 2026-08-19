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
STORAGE_PORTS = tuple(range(9001, 9015))
STORAGE_DATA_DIRS = [
    REPO_ROOT / f".arkel_storage_{port}_data" for port in STORAGE_PORTS
]


def run_nodes(*cmd: str) -> None:
    subprocess.run([sys.executable, str(SCRIPTS / "run_nodes.py"), *cmd], check=True)


def bring_up_network(count: int = 3) -> str:
    """Boot index + storage nodes, return the storage-addrs flag value.

    Self-cleans first: stale nodes from prior runs hold ports and write to
    deleted inodes, leaving empty logs. kill --all avoids the stacking.
    """
    run_nodes("kill", "--all")
    time.sleep(1)
    run_nodes("start", "--fresh")
    run_nodes("start-storage", "--count", str(count))
    time.sleep(3)
    return storage_addrs(count)


def storage_addrs(count: int = 3, timeout: float = 20.0) -> str:
    """Poll the storage logs until `count` endpointIds are present (startup latency)."""
    deadline = time.monotonic() + timeout
    while True:
        parts: list[str] = []
        for i, port in enumerate(STORAGE_PORTS, start=1):
            if len(parts) >= count:
                break
            log = (REPO_ROOT / "logs" / f"storage{i}.log").read_text(errors="replace")
            for line in log.splitlines():
                if "endpointId:" in line:
                    pubkey = line.split("endpointId:", 1)[1].split(".")[0].strip()
                    parts.append(f"{pubkey}@127.0.0.1:{port}")
                    break
        if len(parts) >= count:
            return ",".join(parts[:count])
        if time.monotonic() > deadline:
            raise RuntimeError(f"expected {count} storage pubkeys, got {len(parts)}")
        time.sleep(0.5)


def random_file(size_bytes: int) -> Path:
    p = Path(tempfile.mkdtemp(prefix="arkel-smoke-data-")) / "data.bin"
    p.write_bytes(os.urandom(size_bytes))
    return p


def wait_for_nodes(count: int = 3, timeout: float = 25.0) -> None:
    """Poll /nodes until `count` storage nodes are registered (registration lags
    the startup log line)."""
    import json as _json
    import urllib.request as _urllib

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with _urllib.urlopen("http://127.0.0.1:8001/nodes") as r:
                if len(_json.load(r)) >= count:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"expected {count} registered storage nodes")


def wait_offline(addr: str, timeout: float = 150.0) -> bool:
    """Wait until `addr` shows Offline in /nodes/all.

    Accelerates the health check by re-registering a live node each iteration,
    which advances the Raft log (the offline detector is log-lag based).
    """
    import json as _json
    import urllib.parse as up
    import urllib.request as _urllib

    def live_node():
        with _urllib.urlopen("http://127.0.0.1:8001/nodes/all", timeout=3) as r:
            nodes = _json.load(r)
        return next((n for n in nodes if n.get("addr") != addr), None)

    def register(n):
        payload = _json.dumps(
            {
                "node_id": n["node_id"],
                "capacity_bytes": 1_000_000_000_000,
                "addr": n["addr"],
                "relay_url": n.get("relay_url"),
            }
        ).encode()
        req = _urllib.request.Request(
            "http://127.0.0.1:8001/register",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        _urllib.urlopen(req, timeout=3)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with _urllib.urlopen("http://127.0.0.1:8001/nodes/all", timeout=3) as r:
                nodes = _json.load(r)
            if any(n.get("addr") == addr and n.get("status") == "Offline" for n in nodes):
                return True
            node = live_node()
            if node is not None:
                register(node)  # advance the log so lag-based detection fires
        except Exception:
            pass
        time.sleep(2.0)
    return False


def _client(
    args: list[str], cwd: Path = REPO_ROOT, data_dir: Path | None = None
) -> subprocess.CompletedProcess:
    cmd = [str(BINARY), "client"]
    if data_dir is not None:
        cmd += ["--data-dir", str(data_dir)]
    cmd += args
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def put(
    bucket: str,
    key: str,
    data: Path,
    storage_flag: str = "",
    data_dir: Path | None = None,
) -> tuple[float, str]:
    """Upload `data`; returns (elapsed_seconds, etag).

    Wipes only the blob cache, NOT the identity — the client's key must persist
    so manifest decryption works on get. `data_dir` selects a client identity
    (for two-identity access-control smokes).
    """
    base = data_dir or CLIENT_DATA
    shutil.rmtree(base / "blobs", ignore_errors=True)
    args = ["put", str(data), "--bucket", bucket, "--key", key, "--index-addrs", INDEX_FLAG]
    if storage_flag:
        args += ["--storage-addrs", storage_flag]
    start = time.perf_counter()
    r = _client(args, data_dir=data_dir)
    elapsed = time.perf_counter() - start
    if r.returncode != 0:
        raise RuntimeError(f"put failed: {r.stderr.strip()[-400:]}")
    return elapsed, r.stdout.strip().splitlines()[-1]


def get(
    bucket: str,
    key: str,
    storage_flag: str = "",
    data_dir: Path | None = None,
) -> tuple[float, Path]:
    """Download (network-only: wipe client blobs first); returns (elapsed_seconds, out_path)."""
    base = data_dir or CLIENT_DATA
    shutil.rmtree(base / "blobs", ignore_errors=True)
    out = Path(tempfile.mkdtemp(prefix="arkel-smoke-out-")) / "out.bin"
    args = ["get", bucket, key, "--index-addrs", INDEX_FLAG, "--output", str(out)]
    if storage_flag:
        args += ["--storage-addrs", storage_flag]
    start = time.perf_counter()
    r = _client(args, data_dir=data_dir)
    elapsed = time.perf_counter() - start
    if r.returncode != 0:
        raise RuntimeError(f"get failed: {r.stderr.strip()[-400:]}")
    return elapsed, out


def rm(bucket: str, key: str, data_dir: Path | None = None) -> None:
    """Delete an object via `arkel client rm`."""
    r = _client(["rm", bucket, key, "--index-addrs", INDEX_FLAG], data_dir=data_dir)
    if r.returncode != 0:
        raise RuntimeError(f"rm failed: {r.stderr.strip()[-400:]}")


def count_shards() -> int:
    """Total shard blob `.data` files across all three storage nodes' FsStores.

    Pulled shards land in the iroh-blobs store as `blobs/data/<hex>.data`.
    """
    total = 0
    for d in STORAGE_DATA_DIRS:
        blob_data = d / "blobs" / "data"
        if blob_data.is_dir():
            total += sum(
                1
                for e in blob_data.iterdir()
                if e.is_file() and e.name.endswith(".data")
            )
    return total


def restart_storage(gc_interval_secs: int = 5) -> None:
    """Restart the storage nodes with a short GC sweep interval (for smoke)."""
    for port in STORAGE_PORTS:
        run_nodes("kill", "--port", str(port))
    time.sleep(0.5)
    run_nodes("start-storage", "--gc-interval-secs", str(gc_interval_secs))
    time.sleep(2)
