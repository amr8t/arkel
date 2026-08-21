"""Contribution smoke: storage nodes earn quota against real stored bytes.

Storj-style: the operator declares only --capacity; the index fills it, derives
occupied from manifests, and the leader's grant task credits `ratio x occupied`
hourly (capped at capacity) once past the online grace period. Sets fast
grant/audit intervals + zero grace via env (inherited by the index nodes spawned
by run_nodes.py), verifies /nodes reports capacity + derived occupied, waits for
the grant, checks idempotency, and confirms the audit loop doesn't wrongly mark
healthy nodes Offline.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.request

from ._common import (
    BINARY,
    INDEX_FLAG,
    REPO_ROOT,
    STORAGE_DATA_DIRS,
    bring_up_network,
    put,
    random_file,
    wait_for_nodes,
)

CLIENT_A = REPO_ROOT / ".arkel_client_data"

RATIO = 0.57


def nodes_payload(path: str = "nodes/all") -> list[dict]:
    with urllib.request.urlopen(f"http://127.0.0.1:8001/{path}", timeout=3) as r:
        return json.load(r)


def account_total(account_hex: str) -> tuple[int, int]:
    r = subprocess.run(
        [
            str(BINARY),
            "account",
            "--data-dir",
            str(CLIENT_A),
            "quota",
            "--account",
            account_hex,
            "--index-addrs",
            INDEX_FLAG,
        ],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"account quota failed: {r.stderr.strip()[-400:]}")
    line = r.stdout.strip().splitlines()[-1]
    used_s, total_s = line.split(":", 1)[1].replace("bytes used", "").split("/")
    return int(used_s.strip()), int(total_s.strip())


def run(args) -> int:
    print("[smoke:contribute] booting with fast grant/audit intervals...")
    os.environ["ARKEL_GRANT_INTERVAL_SECS"] = "5"
    os.environ["ARKEL_CONTRIBUTION_GRACE_SECS"] = "0"
    os.environ["ARKEL_AUDIT_INTERVAL_SECS"] = "5"

    for d in STORAGE_DATA_DIRS:
        shutil.rmtree(d, ignore_errors=True)
    shutil.rmtree(CLIENT_A, ignore_errors=True)
    bring_up_network(count=3)
    wait_for_nodes(3)

    size = max(1, int(args.mb * 1024 * 1024))
    put("smoke", "data.bin", random_file(size))

    # /nodes (healthy only) reports capacity + derived occupied.
    healthy = nodes_payload("nodes")
    if not healthy:
        print("[smoke:contribute] FAIL: /nodes empty")
        return 1
    cap = healthy[0].get("capacity_bytes", 0)
    if cap != 1_000_000_000_000:
        print(f"[smoke:contribute] FAIL: expected default capacity 1TB, got {cap}")
        return 1
    if not any(n.get("occupied_bytes", 0) > 0 for n in healthy):
        print("[smoke:contribute] FAIL: no node reports occupied_bytes > 0")
        return 1
    print("[smoke:contribute] /nodes reports capacity 1TB + derived occupied")

    # Wait for the leader's grant tick: total == ratio x occupied(node).
    node = healthy[0]
    node_hex = node["node_id"]
    occupied = node["occupied_bytes"]
    expected = int(occupied * RATIO)
    total = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        _, total = account_total(node_hex)
        if abs(total - expected) <= 1:
            break
        time.sleep(1)
    if abs(total - expected) > 1:
        print(f"[smoke:contribute] FAIL: grant total {total} != ~{expected} "
              f"(occupied {occupied} x {RATIO})")
        return 1
    print(f"[smoke:contribute] contribution grant: total={total} "
          f"({RATIO} x occupied {occupied})")

    # Idempotency: another grant tick must not double-credit.
    time.sleep(6)
    _, total2 = account_total(node_hex)
    if total2 != total:
        print(f"[smoke:contribute] FAIL: grant not idempotent {total} -> {total2}")
        return 1
    print("[smoke:contribute] grant idempotent (no double credit)")

    # Another put still lands (utilization-aware assign_shards).
    put("smoke", "more.bin", random_file(size))

    # Audit tick must not wrongly mark healthy nodes Offline.
    time.sleep(7)  # >= one audit interval
    nodes = nodes_payload()
    offline = [n for n in nodes if n.get("status") == "Offline"]
    if offline:
        print(f"[smoke:contribute] FAIL: audit marked healthy nodes offline: {offline}")
        return 1
    print("[smoke:contribute] audit ran; no healthy node marked Offline")

    print("[smoke:contribute] PASS: capacity/occupied, occupied-based grant, "
          "idempotency, audit")
    return 0
