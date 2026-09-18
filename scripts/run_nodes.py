#!/usr/bin/env python3
"""
Arkel 3-Node Index Cluster Runner.

Mirrors scripts/run_nodes.sh but in Python, with a --performance mode that
pushes the Raft store to find 2k+ sustained writes/sec and 10x bursts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable

import aiohttp


REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"
DATA_DIR = REPO_ROOT

NODE_ADDRS = [
    "127.0.0.1:8001",
    "127.0.0.1:8002",
    "127.0.0.1:8003",
]

STORAGE_ADDRS = [
    f"127.0.0.1:{port}" for port in range(9001, 9015)
]

# Storage ports that actually map into STORAGE_ADDRS (9001-9014).
STORAGE_PORTS = {int(a.rsplit(":", 1)[-1]) for a in STORAGE_ADDRS}

# Index node URLs storage nodes register against (the registrar discovers the
# current Raft leader among these).
INDEX_ADDRS = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
]

# Number of synthetic buckets to spread perf-test writes across.
# Mostly cosmetic: SQLite serializes writes anyway, but sharding avoids
# concentrating the tiny bucket-existence check on a single row.
BUCKET_SHARD_COUNT = 64

# M2+ object write is the manifest commit route. The index API does not
# validate manifest content, so a stub payload stresses the Raft write path
# without involving the data plane.
STUB_MANIFEST = json.dumps(
    {
        "object_hash": [1] * 32,
        "manifest_bytes": [2, 2, 2],
        "signature": [3] * 64,
    }
)
STUB_HEADERS = {"Content-Type": "application/json"}


class NetworkEmulator:
    """Simulate latency and jitter between local nodes using tc-netem on loopback.

    Uses a prio qdisc with u32 filters to apply netem *only* to traffic
    on the Arkel node ports (8001–8003). All other loopback traffic is
    unaffected. Requires root (sudo) since it manipulates kernel tc qdiscs.
    """

    _NODE_PORTS = (8001, 8002, 8003)

    @staticmethod
    def add(delay_ms: int, jitter_ms: int = 0, loss_pct: float = 0.0) -> None:
        """Add port-scoped netem delay on loopback for Arkel node ports."""
        NetworkEmulator.remove()

        # Root prio qdisc: band 0 = normal, band 1 = netem-delayed
        subprocess.run(
            ["sudo", "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio",
             "bands", "2", "priomap", "1", "1", "1", "1", "1", "1", "1", "1",
             "1", "1", "1", "1", "1", "1", "1", "1"],
            check=True, capture_output=True, text=True,
        )

        # Band 0 (default for non-matching traffic): normal pfifo
        subprocess.run(
            ["sudo", "tc", "qdisc", "add", "dev", "lo", "parent", "1:1",
             "handle", "10:", "pfifo_fast"],
            check=True, capture_output=True, text=True,
        )

        # Band 1: netem with delay
        netem_args = ["delay", f"{delay_ms}ms", f"{jitter_ms}ms", "distribution", "normal"]
        if loss_pct > 0:
            netem_args.extend(["loss", f"{loss_pct}%"])
        subprocess.run(
            ["sudo", "tc", "qdisc", "add", "dev", "lo", "parent", "1:2",
             "handle", "20:", "netem", *netem_args],
            check=True, capture_output=True, text=True,
        )

        # u32 filters: match traffic to/from each node port → band 1
        for port in NetworkEmulator._NODE_PORTS:
            for match in ("dport", "sport"):
                subprocess.run(
                    ["sudo", "tc", "filter", "add", "dev", "lo", "protocol", "ip",
                     "parent", "1:0", "prio", "1", "u32",
                     "match", "ip", match, str(port), "0xffff", "flowid", "1:2"],
                    check=True, capture_output=True, text=True,
                )

        print(
            f"[netem] Added {delay_ms}ms delay ±{jitter_ms}ms jitter "
            f"on ports {list(NetworkEmulator._NODE_PORTS)} "
            f"(all other loopback traffic unaffected)"
            f"{' (+ ' + str(loss_pct) + '% loss)' if loss_pct > 0 else ''}"
        )

    @staticmethod
    def remove() -> None:
        """Remove the netem qdisc and all filters from loopback."""
        r = subprocess.run(
            ["sudo", "tc", "qdisc", "del", "dev", "lo", "root"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            print("[netem] Removed loopback latency emulation")

    @staticmethod
    def show() -> str:
        r = subprocess.run(
            ["tc", "qdisc", "show", "dev", "lo"],
            capture_output=True, text=True,
        )
        return r.stdout.strip()


class Node:
    def __init__(self, port: int, addr: str) -> None:
        self.port = port
        self.addr = addr
        self.full_addr: str | None = None
        self.pubkey: str | None = None
        self.process: subprocess.Popen | None = None
        self.log_path = LOG_DIR / f"node{port - 8000}.log"
        self.data_dir = DATA_DIR / f".arkel_index_{port}_data"

    def __repr__(self) -> str:
        return f"Node(port={self.port})"


class StorageNode:
    def __init__(self, port: int, addr: str) -> None:
        self.port = port
        self.addr = addr
        self.process: subprocess.Popen | None = None
        self.log_path = LOG_DIR / f"storage{port - 9000}.log"
        self.data_dir = DATA_DIR / f".arkel_storage_{port}_data"

    def __repr__(self) -> str:
        return f"StorageNode(port={self.port})"


def env() -> dict[str, str]:
    e = os.environ.copy()
    e.setdefault("RUST_LOG", "info")
    # Loopback smokes run fast: aggressive Raft timings (production defaults are
    # cross-region-safe: 250/1000/2000 ms). Override via the environment.
    e.setdefault("ARKEL_HEARTBEAT_INTERVAL_MS", "10")
    e.setdefault("ARKEL_ELECTION_TIMEOUT_MIN_MS", "150")
    e.setdefault("ARKEL_ELECTION_TIMEOUT_MAX_MS", "300")
    return e


def arkel_bin() -> Path:
    return REPO_ROOT / "target" / "debug" / "arkel"


def build_binary() -> Path:
    print("[build] Building arkel binary...")
    subprocess.run(
        ["cargo", "build", "--quiet"],
        cwd=REPO_ROOT,
        check=True,
    )
    return arkel_bin()


def kill_arkel_by_addr(addrs: Iterable[str]) -> None:
    for addr in addrs:
        subprocess.run(
            ["pkill", "-f", f"arkel index --http-addr {addr}"],
            capture_output=True,
        )


def kill_arkel_storage_by_addr(addrs: Iterable[str]) -> None:
    for addr in addrs:
        # Storage cmdline is `storage --index-addrs <urls> --addr <addr>`, so
        # --addr is never contiguous after `storage` — match with `.*` in between.
        subprocess.run(
            ["pkill", "-f", f"arkel storage.*--addr {addr}"],
            capture_output=True,
        )


def wait_for_log(log: Path, pattern: str, timeout: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if log.exists():
            with open(log, "r", errors="replace") as f:
                if pattern in f.read():
                    return True
        time.sleep(0.2)
    return False


def extract_pubkey(log: Path) -> str | None:
    if not log.exists():
        return None
    m = re.search(r"Full Address\s+:\s+([0-9a-fA-F]+)@", log.read_text(errors="replace"))
    return m.group(1) if m else None


def cleanup_data_dirs(nodes: list[Node], keep_identity: bool = True) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for node in nodes:
        if node.data_dir.exists() and keep_identity:
            # Delete every file except identity.key.
            for entry in node.data_dir.iterdir():
                if entry.name == "identity.key":
                    continue
                if entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    entry.unlink(missing_ok=True)
        else:
            shutil.rmtree(node.data_dir, ignore_errors=True)


def start_identity_phase(binary: Path, nodes: list[Node]) -> list[Node]:
    print("[identity] Generating node identities...")
    for node in nodes:
        node.log_path.unlink(missing_ok=True)
        cmd = [str(binary), "index", "--http-addr", node.addr]
        with open(node.log_path, "w") as logfile:
            node.process = subprocess.Popen(
                cmd,
                stdout=logfile,
                stderr=subprocess.STDOUT,
                env=env(),
            )

    for node in nodes:
        if not wait_for_log(node.log_path, "Full Address", timeout=10.0):
            print(f"ERROR: Node {node.port} did not print its identity in time")
            for n in nodes:
                if n.process:
                    n.process.kill()
            sys.exit(1)

    # Give stragglers a moment.
    time.sleep(1.0)

    for node in nodes:
        node.pubkey = extract_pubkey(node.log_path)
        node.full_addr = f"{node.pubkey}@{node.addr}"

    pubkey_map = LOG_DIR / "pubkey_map.txt"
    pubkey_map.write_text("\n".join(n.full_addr for n in nodes if n.full_addr is not None) + "\n")

    print("[identity] Extracting pubkeys...")
    for node in nodes:
        if node.pubkey is None:
            print(f"ERROR: Failed to extract pubkey for node {node.port}")
            for n in nodes:
                if n.process:
                    n.process.kill()
            sys.exit(1)
        short = node.pubkey[:16]
        print(f"        Node on port {node.port}: {short}... @{node.addr}")

    print("[identity] Killing temporary instances...")
    for node in nodes:
        if node.process:
            node.process.kill()
            node.process.wait()
            node.process = None
    kill_arkel_by_addr(n.addr for n in nodes)
    time.sleep(0.5)

    return nodes


def start_cluster(binary: Path, nodes: list[Node]) -> None:
    peers = ",".join(n.full_addr for n in nodes if n.full_addr is not None)
    print("\n[cluster] Starting 3-node cluster with shared membership...")
    for node in nodes:
        node.log_path.unlink(missing_ok=True)
        cmd = [
            str(binary),
            "index",
            "--http-addr",
            node.addr,
            "--peer-addresses",
            peers,
        ]
        with open(node.log_path, "w") as logfile:
            node.process = subprocess.Popen(
                cmd,
                stdout=logfile,
                stderr=subprocess.STDOUT,
                env=env(),
            )
        time.sleep(0.5)


def stop_cluster(nodes: list[Node]) -> None:
    for node in nodes:
        if node.process:
            node.process.terminate()
    for node in nodes:
        if node.process:
            try:
                node.process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                node.process.kill()
    kill_arkel_by_addr(n.addr for n in nodes)


def wait_for_leader(nodes: list[Node], timeout: float = 30.0) -> int | None:
    print("[wait] Waiting for leader election...")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders: set[int] = set()
        for node in nodes:
            try:
                metrics = json.loads(
                    subprocess.run(
                        ["curl", "-sS", f"http://127.0.0.1:{node.port}/raft/metrics"],
                        capture_output=True,
                        text=True,
                        timeout=1.0,
                    ).stdout
                )
                if metrics.get("current_leader") == metrics.get("id"):
                    leaders.add(node.port)
            except Exception:
                pass
        if len(leaders) == 1:
            return leaders.pop()
        time.sleep(0.5)
    return None


def basic_consistency_test(leader_port: int, nodes: list[Node]) -> bool:
    print(f"[test] Writing bucket and object through leader (port {leader_port})...")
    base = f"http://127.0.0.1:{leader_port}"
    subprocess.run(
        ["curl", "-sS", "-X", "PUT", f"{base}/photos"],
        check=False,
        capture_output=True,
    )
    # M2+ object write is the manifest commit route. The index API does not
    # validate manifest content, so a stub payload stresses the Raft write path.
    subprocess.run(
        ["curl", "-sS", "-X", "PUT", f"{base}/manifest/photos/avatar.png",
         "-H", "content-type: application/json", "-d", STUB_MANIFEST],
        check=False,
        capture_output=True,
    )

    time.sleep(1.5)

    print("[test] Read-back from all 3 nodes (expecting identical metadata)...")
    objects: list[str] = []
    for node in nodes:
        url = f"http://127.0.0.1:{node.port}/manifest/photos/avatar.png"
        body = subprocess.run(
            ["curl", "-sS", url], capture_output=True, text=True, timeout=5.0
        ).stdout
        objects.append(body)
        print(f"--- Node {node.port - 8000} (port {node.port}) ---\n{body}\n")

    print("[test] List buckets from all 3 nodes...")
    buckets: list[str] = []
    for node in nodes:
        url = f"http://127.0.0.1:{node.port}/"
        body = subprocess.run(
            ["curl", "-sS", url], capture_output=True, text=True, timeout=5.0
        ).stdout
        buckets.append(body)
        print(f"--- Node {node.port - 8000} (port {node.port}) ---\n{body}\n")

    print("[verify] Consistency check...")
    if len(set(objects)) != 1:
        print("  OBJECT METADATA: INCONSISTENT")
        for node, obj in zip(nodes, objects):
            print(f"    node{node.port}: {obj}")
        return False
    print("  OBJECT METADATA: CONSISTENT across all 3 nodes")

    if len(set(buckets)) != 1:
        print("  BUCKET LIST: INCONSISTENT")
        for node, buck in zip(nodes, buckets):
            print(f"    node{node.port}: {buck}")
        return False
    print("  BUCKET LIST: CONSISTENT across all 3 nodes")
    return True


async def performance_benchmark(
    leader_port: int,
    *,
    duration_s: float = 10.0,
    concurrency: int = 256,
    burst_multiplier: int = 10,
    burst_duration_s: float = 2.0,
    target_rate: int = 2200,
    burst_target_rate: int | None = None,
    latency_ms: int = 0,
    jitter_ms: int = 0,
) -> None:
    """
    Pump concurrent PUTs through the leader and measure throughput.

    We deliberately use tiny objects here to stress the *Raft metadata path*:
    the objective is to validate the SQLite/Raft state-machine throughput,
    not the network bandwidth.

    Phases:
      1. Optional: network emulation (tc-netem delay + jitter on loopback)
      2. Steady-state: rate-limited at target_rate writes/sec
      3. Burst: saturated with concurrency * burst_multiplier clients
      4. Optional: ceiling finder (binary search for max sustainable rate)
    """
    base = f"http://127.0.0.1:{leader_port}"
    netem_active = latency_ms > 0

    if netem_active:
        NetworkEmulator.add(delay_ms=latency_ms, jitter_ms=jitter_ms)
        print(f"[perf] Network emulation active — "
              f"all loopback RPC traffic delayed by {latency_ms}ms ±{jitter_ms}ms")

    try:
        print("\n[perf] Warming up bucket...")
        await _single_put(base, "perf", "warmup")

        print("\n[perf] Running steady-state throughput test...")
        steady_latencies = await _run_rate_limited_phase(
            base,
            "steady",
            duration_s=duration_s,
            concurrency=concurrency,
            target_rate=target_rate,
        )
        _report_phase("steady-state", steady_latencies, duration_s, target=target_rate)

        print("\n[perf] Running burst throughput test...")
        burst_latencies = await _run_saturated_phase(
            base,
            "burst",
            duration_s=burst_duration_s,
            concurrency=concurrency * burst_multiplier,
        )
        _report_phase("burst", burst_latencies, burst_duration_s, multiplier=burst_multiplier, target=burst_target_rate)

    finally:
        if netem_active:
            NetworkEmulator.remove()


def _bucket_for(seq: int) -> str:
    # Spread traffic across sharded perf buckets.
    return f"perf_{seq % BUCKET_SHARD_COUNT:02d}"


def _key_for(seq: int) -> str:
    return f"obj_{seq:08x}"


async def _single_put(base: str, bucket: str, key: str) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.put(
            f"{base}/manifest/{bucket}/{key}", data=STUB_MANIFEST, headers=STUB_HEADERS
        ) as resp:
            await resp.read()
            if not resp.ok:
                raise RuntimeError(f"warmup PUT failed: {resp.status}")


async def _run_rate_limited_phase(
    base: str,
    prefix: str,
    *,
    duration_s: float,
    concurrency: int,
    target_rate: int,
    seq_offset: int = 0,
) -> list[float]:
    """Run concurrent PUTs for `duration_s` at a fixed target request rate."""
    done_event = asyncio.Event()
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=concurrency * 4)
    results: list[float] = []
    errors: list[str] = []
    counter = seq_offset

    async def worker() -> None:
        async with aiohttp.ClientSession() as session:
            while not done_event.is_set() or not queue.empty():
                try:
                    seq = await asyncio.wait_for(queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                bucket = _bucket_for(seq)
                key = _key_for(seq)
                url = f"{base}/manifest/{bucket}/{key}"
                start = time.perf_counter()
                try:
                    async with session.put(url, data=STUB_MANIFEST, headers=STUB_HEADERS) as resp:
                        await resp.read()
                        if not resp.ok:
                            errors.append(f"{resp.status} on {url}")
                except Exception as e:
                    errors.append(str(e))
                finally:
                    elapsed = time.perf_counter() - start
                    results.append(elapsed)
                    queue.task_done()

    async def feeder() -> None:
        nonlocal counter
        interval = 1.0 / target_rate
        next_time = time.perf_counter()
        while not done_event.is_set():
            if queue.qsize() < concurrency * 2:
                await queue.put(counter)
                counter += 1
                next_time += interval
                sleep_for = next_time - time.perf_counter()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                else:
                    # Backpressure is keeping the queue full; yield briefly.
                    await asyncio.sleep(0)
            else:
                await asyncio.sleep(0.001)

    feeder_task = asyncio.create_task(feeder())
    workers = [
        asyncio.create_task(worker()) for _ in range(concurrency)
    ]

    await asyncio.sleep(duration_s)
    done_event.set()
    await queue.join()
    feeder_task.cancel()
    try:
        await feeder_task
    except asyncio.CancelledError:
        pass
    await asyncio.gather(*workers, return_exceptions=True)

    if errors:
        print(f"[perf] {len(errors)} errors in {prefix} phase (showing 5): {errors[:5]}")
    return results


async def _run_saturated_phase(
    base: str,
    prefix: str,
    *,
    duration_s: float,
    concurrency: int,
    seq_offset: int = 0,
) -> list[float]:
    """Run concurrent PUTs for `duration_s` with no rate limit; saturate the leader."""
    done_event = asyncio.Event()
    results: list[float] = []
    errors: list[str] = []
    counter = seq_offset
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal counter
        async with aiohttp.ClientSession() as session:
            while not done_event.is_set():
                async with lock:
                    seq = counter
                    counter += 1
                bucket = _bucket_for(seq)
                key = _key_for(seq)
                url = f"{base}/manifest/{bucket}/{key}"
                start = time.perf_counter()
                try:
                    async with session.put(url, data=STUB_MANIFEST, headers=STUB_HEADERS) as resp:
                        await resp.read()
                        if not resp.ok:
                            errors.append(f"{resp.status} on {url}")
                except Exception as e:
                    errors.append(str(e))
                finally:
                    elapsed = time.perf_counter() - start
                    results.append(elapsed)

    workers = [
        asyncio.create_task(worker()) for _ in range(concurrency)
    ]

    await asyncio.sleep(duration_s)
    done_event.set()
    await asyncio.gather(*workers, return_exceptions=True)

    if errors:
        print(f"[perf] {len(errors)} errors in {prefix} phase (showing 5): {errors[:5]}")
    return results


def _report_phase(
    label: str,
    latencies: list[float],
    duration_s: float,
    multiplier: int | None = None,
    target: int | None = None,
) -> None:
    if not latencies:
        print(f"[perf] {label}: no successful requests")
        return

    throughput = len(latencies) / duration_s
    lat_ms = [l * 1000 for l in latencies]
    p50 = statistics.median(lat_ms)
    p99 = sorted(lat_ms)[int(len(lat_ms) * 0.99)] if len(lat_ms) > 1 else p50
    extra = f" (x{multiplier} burst)" if multiplier else ""
    print(
        f"[perf] {label}{extra}: {throughput:,.1f} writes/sec | "
        f"{len(latencies)} writes | p50={p50:,.2f}ms p99={p99:,.2f}ms"
    )
    if target is not None:
        pct = throughput / target * 100
        if pct >= 99.5:
            print(f"[perf] ✅ {label} meets {target:,} writes/sec target ({pct:.1f}%)")
        else:
            print(f"[perf] ❌ {label} below {target:,} writes/sec target ({pct:.1f}%)")


def cmd_run(args: argparse.Namespace) -> int:
    print("== Arkel 3-Node Cluster Runner (Python) ==")
    print(f"Logs: {LOG_DIR}/\n")

    binary = build_binary()
    nodes = [Node(port, addr) for port, addr in zip((8001, 8002, 8003), NODE_ADDRS)]

    # Make sure no stale nodes are listening.
    kill_arkel_by_addr(n.addr for n in nodes)
    time.sleep(0.5)

    # Fresh data dirs, keep identities.
    print("[clean] Removing old data dirs and logs...")
    cleanup_data_dirs(nodes, keep_identity=False)

    try:
        nodes = start_identity_phase(binary, nodes)
        cleanup_data_dirs(nodes, keep_identity=True)
        start_cluster(binary, nodes)

        leader_port = wait_for_leader(nodes, timeout=30.0)
        if leader_port is None:
            print("ERROR: No leader elected")
            return 1

        print(f"        Leader detected on port: {leader_port}\n")
        if not basic_consistency_test(leader_port, nodes):
            return 1

        if args.performance:
            try:
                asyncio.run(
                    performance_benchmark(
                        leader_port,
                        duration_s=args.perf_duration,
                        concurrency=args.perf_concurrency,
                        burst_multiplier=args.perf_burst,
                        burst_duration_s=args.perf_burst_duration,
                        target_rate=args.perf_target_rate,
                        latency_ms=args.perf_latency_ms,
                        jitter_ms=args.perf_jitter_ms,
                        burst_target_rate=args.perf_burst_target_rate,
                    )
                )
            except Exception as e:
                print(f"[perf] Benchmark failed: {e}")
                return 1

        print("\n[done] Raft consensus is working end-to-end.")
        if not args.performance:
            print("Press Ctrl+C to stop all nodes...")
            signal.pause()
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 0
    finally:
        print("\nStopping nodes...")
        stop_cluster(nodes)
        print("Done.")


def load_peer_addresses() -> str | None:
    path = LOG_DIR / "pubkey_map.txt"
    if not path.exists():
        return None
    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    return ",".join(lines)


def cmd_start(args: argparse.Namespace) -> int:
    """Start the 3-node cluster (or one node) and return (nodes stay up)."""
    print("== Arkel 3-Node Cluster Manager: start ==")
    binary = build_binary()
    nodes = [Node(port, addr) for port, addr in zip((8001, 8002, 8003), NODE_ADDRS)]

    if args.port:
        # Single-node rejoin: don't touch the other nodes.
        node = next((n for n in nodes if n.port == args.port), None)
        if node is None:
            print(f"ERROR: Unknown port {args.port}")
            return 1
        if not node.data_dir.exists() or not (node.data_dir / "identity.key").exists():
            print(
                f"ERROR: Node {args.port} has no identity. "
                "Use 'wipe' without --full, or start a fresh cluster with 'start --fresh'."
            )
            return 1

        peers = load_peer_addresses()
        if peers is None:
            print("ERROR: No pubkey_map.txt found; cannot determine peers for rejoin.")
            return 1

        kill_arkel_by_addr([node.addr])
        time.sleep(0.5)

        node.log_path.unlink(missing_ok=True)
        cmd = [str(binary), "index", "--http-addr", node.addr, "--peer-addresses", peers]
        with open(node.log_path, "w") as logfile:
            node.process = subprocess.Popen(
                cmd,
                stdout=logfile,
                stderr=subprocess.STDOUT,
                env=env(),
            )
        print(f"[start] Rejoining node on port {args.port} with peers: {peers}")
        return 0

    # Full cluster start.
    kill_arkel_by_addr(n.addr for n in nodes)
    time.sleep(0.5)

    if args.fresh:
        print("[clean] Fresh start requested: removing old data dirs...")
        cleanup_data_dirs(nodes, keep_identity=False)
        nodes = start_identity_phase(binary, nodes)
        cleanup_data_dirs(nodes, keep_identity=True)
        start_cluster(binary, nodes)

        leader_port = wait_for_leader(nodes, timeout=30.0)
        if leader_port is None:
            print("ERROR: No leader elected")
            return 1
        print(f"[start] Leader elected on port {leader_port}.")
        print("[start] Nodes are running in the background.")
        return 0

    # Normal restart: reuse existing identities and persisted Raft state.
    peers = load_peer_addresses()
    if peers is None:
        print("ERROR: No logs/pubkey_map.txt found; cannot restart with existing state.")
        print("        Use 'start --fresh' to create a new cluster.")
        return 1

    missing_identity = [n.port for n in nodes if not (n.data_dir / "identity.key").exists()]
    if missing_identity:
        print(
            f"ERROR: identity.key missing for port(s) {missing_identity}. "
            "Use 'start --fresh' to recreate identities."
        )
        return 1

    # Populate full_addr/pubkey from the saved map.
    for line in (LOG_DIR / "pubkey_map.txt").read_text().splitlines():
        line = line.strip()
        if not line or "@" not in line:
            continue
        pubkey, addr = line.split("@", 1)
        port = int(addr.rsplit(":", 1)[-1])
        for node in nodes:
            if node.port == port:
                node.pubkey = pubkey
                node.full_addr = line
                break

    print("[start] Reusing existing identities and persisted state.")
    start_cluster(binary, nodes)

    leader_port = wait_for_leader(nodes, timeout=30.0)
    if leader_port is None:
        print("ERROR: No leader elected")
        return 1

    print(f"[start] Leader elected on port {leader_port}.")
    print("[start] Nodes are running in the background.")
    print("        Use 'kill' or 'clean' to stop/clean them later.")
    return 0


def cmd_kill(args: argparse.Namespace) -> int:
    print("== Arkel 3-Node Cluster Manager: kill ==")
    nodes = [Node(port, addr) for port, addr in zip((8001, 8002, 8003), NODE_ADDRS)]
    if args.all:
        print("[kill] Stopping all nodes...")
        stop_cluster(nodes)
        kill_arkel_storage_by_addr(STORAGE_ADDRS)
    elif args.port:
        node = next((n for n in nodes if n.port == args.port), None)
        if node is None:
            # Storage node port?
            if args.port in STORAGE_PORTS:
                addr = STORAGE_ADDRS[args.port - 9001]
                kill_arkel_storage_by_addr([addr])
                time.sleep(0.5)
                print(f"[kill] Stopping storage node on port {args.port}...")
                return 0
            print(f"ERROR: Unknown port {args.port}")
            return 1
        if node.process:
            node.process.terminate()
            try:
                node.process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                node.process.kill()
        kill_arkel_by_addr([node.addr])
        time.sleep(0.5)
        print(f"[kill] Node on port {args.port} stopped.")
    else:
        print("ERROR: specify --port PORT or --all")
        return 1
    return 0


def cmd_wipe(args: argparse.Namespace) -> int:
    print("== Arkel 3-Node Cluster Manager: wipe ==")
    nodes = [Node(port, addr) for port, addr in zip((8001, 8002, 8003), NODE_ADDRS)]
    if args.port:
        print(f"[wipe] Wiping data for node on port {args.port}...")
        node = next((n for n in nodes if n.port == args.port), None)
        if node is None:
            print(f"ERROR: Unknown port {args.port}")
            return 1
        # Make sure it is not running first.
        kill_arkel_by_addr([node.addr])
        time.sleep(0.5)
        if node.data_dir.exists():
            if args.full:
                shutil.rmtree(node.data_dir, ignore_errors=True)
                print(f"[wipe] Removed {node.data_dir} (including identity)")
            else:
                cleanup_data_dirs([node], keep_identity=True)
                print(f"[wipe] Wiped state from {node.data_dir}; identity.key preserved")
    elif args.all:
        print("[wipe] Wiping data for all nodes...")
        for node in nodes:
            kill_arkel_by_addr([node.addr])
        time.sleep(0.5)
        for node in nodes:
            if node.data_dir.exists():
                if args.full:
                    shutil.rmtree(node.data_dir, ignore_errors=True)
                    print(f"[wipe] Removed {node.data_dir} (including identity)")
                else:
                    cleanup_data_dirs([node], keep_identity=True)
                    print(f"[wipe] Wiped state from {node.data_dir}; identity.key preserved")
    else:
        print("ERROR: specify --port PORT or --all")
        return 1
    return 0


def cmd_clean(args: argparse.Namespace) -> int:
    print("== Arkel 3-Node Cluster Manager: clean ==")
    nodes = [Node(port, addr) for port, addr in zip((8001, 8002, 8003), NODE_ADDRS)]
    print("[clean] Stopping any running nodes...")
    stop_cluster(nodes)
    kill_arkel_storage_by_addr(STORAGE_ADDRS)
    print("[clean] Removing data dirs and logs...")
    for node in nodes:
        if node.data_dir.exists():
            shutil.rmtree(node.data_dir, ignore_errors=True)
            print(f"[clean] Removed {node.data_dir}")
    for addr in STORAGE_ADDRS:
        port = int(addr.rsplit(":", 1)[-1])
        data_dir = DATA_DIR / f".arkel_storage_{port}_data"
        if data_dir.exists():
            shutil.rmtree(data_dir, ignore_errors=True)
            print(f"[clean] Removed {data_dir}")
    if LOG_DIR.exists():
        shutil.rmtree(LOG_DIR, ignore_errors=True)
        print(f"[clean] Removed {LOG_DIR}")
    return 0


def cmd_start_storage(args: argparse.Namespace) -> int:
    print("== Arkel Storage Node Manager: start-storage ==")
    binary = build_binary()
    index_addrs = ",".join(args.index_addrs) if args.index_addrs else ",".join(INDEX_ADDRS)
    storage_nodes = [
        StorageNode(int(addr.rsplit(":", 1)[-1]), addr)
        for addr in STORAGE_ADDRS[: args.count]
    ]

    kill_arkel_storage_by_addr(n.addr for n in storage_nodes)
    time.sleep(0.5)

    for node in storage_nodes:
        node.log_path.unlink(missing_ok=True)
        cmd = [
            str(binary),
            "storage",
            "--index-addrs",
            index_addrs,
            "--addr",
            node.addr,
            "--data-dir",
            str(node.data_dir),
            "--gc-interval-secs",
            str(args.gc_interval_secs),
        ]
        if args.capacity:
            cmd += ["--capacity", args.capacity]
        with open(node.log_path, "w") as logfile:
            node.process = subprocess.Popen(
                cmd,
                stdout=logfile,
                stderr=subprocess.STDOUT,
                env=env(),
            )
        time.sleep(0.3)

    print(f"[start-storage] {len(storage_nodes)} storage node(s) running in background "
          f"(registering with {index_addrs}).")
    print(f"               Logs: {LOG_DIR}/storage{{1..{args.count}}}.log")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    # All subcommands. If the first arg isn't a known subcommand, default to 'run'.
    subcommands = {"run", "start", "kill", "wipe", "clean", "start-storage", "smoke"}
    if argv and argv[0] in subcommands:
        command = argv[0]
        rest = argv[1:]
    elif argv and argv[0] in ("-h", "--help"):
        command = None
        rest = argv
    else:
        command = "run"
        rest = argv

    parser = argparse.ArgumentParser(description="Arkel 3-node cluster runner & manager")
    subparsers = parser.add_subparsers(dest="command")

    # run
    run_parser = subparsers.add_parser("run", help="Run cluster + consensus checks (default)")
    run_parser.add_argument(
        "--performance",
        action="store_true",
        help="Run a throughput benchmark against the leader after consensus checks.",
    )
    run_parser.add_argument(
        "--perf-duration",
        type=float,
        default=10.0,
        help="Duration of the steady-state performance phase (default: 10s).",
    )
    run_parser.add_argument(
        "--perf-concurrency",
        type=int,
        default=256,
        help="Number of concurrent PUT clients in steady-state (default: 256).",
    )
    run_parser.add_argument(
        "--perf-burst",
        type=int,
        default=10,
        help="Multiplier applied to concurrency during the burst phase (default: 10).",
    )
    run_parser.add_argument(
        "--perf-burst-duration",
        type=float,
        default=2.0,
        help="Duration of the burst phase (default: 2s).",
    )
    run_parser.add_argument(
        "--perf-burst-target-rate",
        type=int,
        default=None,
        help="Target writes/sec for burst phase pass/fail check (default: no check).",
    )
    run_parser.add_argument(
        "--perf-target-rate",
        type=int,
        default=2200,
        help="Target writes/sec for the steady-state phase (default: 2200).",
    )
    run_parser.add_argument(
        "--perf-latency-ms",
        type=int,
        default=0,
        help="Simulated network latency in ms between nodes via tc-netem (default: 0, requires sudo).",
    )
    run_parser.add_argument(
        "--perf-jitter-ms",
        type=int,
        default=0,
        help="Simulated network jitter in ms (requires --perf-latency-ms > 0 and sudo).",
    )

    # start
    start_parser = subparsers.add_parser("start", help="Start nodes and leave them running")
    start_group = start_parser.add_mutually_exclusive_group()
    start_group.add_argument(
        "--fresh",
        action="store_true",
        help="Remove existing data dirs before starting",
    )
    start_group.add_argument(
        "--port",
        type=int,
        help="Start only the node on this port (for rejoining a wiped node)",
    )

    # kill
    kill_parser = subparsers.add_parser("kill", help="Stop running node(s)")
    kill_group = kill_parser.add_mutually_exclusive_group(required=True)
    kill_group.add_argument("--port", type=int, help="Port of the node to kill")
    kill_group.add_argument("--all", action="store_true", help="Kill all nodes")

    # wipe
    wipe_parser = subparsers.add_parser("wipe", help="Wipe data dir(s)")
    wipe_group = wipe_parser.add_mutually_exclusive_group(required=True)
    wipe_group.add_argument("--port", type=int, help="Port of the node to wipe")
    wipe_group.add_argument("--all", action="store_true", help="Wipe all node data dirs")
    wipe_parser.add_argument(
        "--full",
        action="store_true",
        help="Also remove identity.key (default: keep identity so the node can rejoin)",
    )
    wipe_parser.epilog = (
        "Note: rejoining a fully-wiped node is a catastrophic data-loss scenario. "
        "Start arkel with ARKEL_ALLOW_LOG_REVERSION=1 to enable it."
    )

    # clean
    subparsers.add_parser("clean", help="Stop all nodes and remove all data + logs")

    # start-storage
    storage_parser = subparsers.add_parser(
        "start-storage",
        help="Start storage nodes in the background",
    )
    storage_parser.add_argument(
        "--count",
        type=int,
        default=3,
        help="Number of storage nodes to start (default: 3, max: 14)",
    )
    storage_parser.add_argument(
        "--index-addrs",
        type=str,
        nargs="*",
        default=None,
        help="Index node HTTP URLs (space-separated) storage nodes register against "
             "(default: all three local index nodes)",
    )
    storage_parser.add_argument(
        "--gc-interval-secs",
        type=int,
        default=3600,
        help="Shard GC sweep interval in seconds (default: 3600)",
    )
    storage_parser.add_argument(
        "--capacity",
        type=str,
        default="",
        help="Storage allocation (e.g. 1TB) passed to each storage node",
    )

    # smoke
    smoke_parser = subparsers.add_parser(
        "smoke",
        help="Run a smoke scenario (scripts/smoke/<name>.py)",
    )
    smoke_parser.add_argument(
        "scenario",
        help="Scenario name, e.g. 'basic' for the data-plane roundtrip",
    )
    smoke_parser.add_argument(
        "--mb",
        type=float,
        default=2.0,
        help="Data size in MB for size-parameterized scenarios (default: 2)",
    )
    smoke_parser.add_argument(
        "--iters",
        type=int,
        default=5,
        help="Iterations for the perf scenario (default: 5)",
    )
    smoke_parser.add_argument(
        "--nodes",
        type=int,
        default=14,
        help="Storage node count for count-parameterized scenarios (default: 14)",
    )

    args = parser.parse_args([command, *rest] if command else rest)

    if args.command == "run":
        return cmd_run(args)
    elif args.command == "start":
        return cmd_start(args)
    elif args.command == "kill":
        return cmd_kill(args)
    elif args.command == "wipe":
        return cmd_wipe(args)
    elif args.command == "clean":
        return cmd_clean(args)
    elif args.command == "start-storage":
        return cmd_start_storage(args)
    elif args.command == "smoke":
        import importlib
        mod = importlib.import_module(f"smoke.{args.scenario}")
        return mod.run(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
