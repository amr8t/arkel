"""Contribution smoke: storage nodes earn a one-time quota grant from their
declared --capacity.

The leader's grant task credits `min(capacity * ratio, cap)` once per node
(ratio = ARKEL_CONTRIBUTION_RATIO, default 1/3; cap = ARKEL_CONTRIBUTION_CAP,
default 50GB), gated by the online grace period. The stable ref_id makes the
quota_events idempotency key permanent, so each node is credited exactly once.

Sets fast grant intervals + zero grace via env (inherited by the index nodes
spawned by run_nodes.py), boots storage with a known --capacity, verifies /nodes
reports that capacity, waits for the grant, checks the credit == capacity / 3
(no upload required — the grant is capacity-based, not occupied-based), and
confirms idempotency.
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
    wait_for_nodes,
)

CLIENT_A = REPO_ROOT / ".arkel_client_data"

CAPACITY_MB = 1500
CAPACITY_BYTES = CAPACITY_MB * 1024 * 1024  # 1,572,864,000
EXPECTED_BYTES = CAPACITY_BYTES // 3        # 500MB, below the 50GB cap


def nodes_payload(path: str = "nodes") -> list[dict]:
    with urllib.request.urlopen(f"http://127.0.0.1:8001/{path}", timeout=3) as r:
        return json.load(r)


def account_total(account_hex: str) -> tuple[int, int]:
    r = subprocess.run(
        [
            str(BINARY),
            "quota",
            "--data-dir",
            str(CLIENT_A),
            "--identity",
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
    print("[smoke:contribute] booting with fast grant intervals...")
    os.environ["ARKEL_GRANT_INTERVAL_SECS"] = "5"
    os.environ["ARKEL_CONTRIBUTION_GRACE_SECS"] = "0"

    for d in STORAGE_DATA_DIRS:
        shutil.rmtree(d, ignore_errors=True)
    shutil.rmtree(CLIENT_A, ignore_errors=True)
    bring_up_network(count=3, capacity=f"{CAPACITY_MB}MB")
    wait_for_nodes(3)

    # /nodes reports the declared capacity.
    healthy = nodes_payload("nodes")
    if not healthy:
        print("[smoke:contribute] FAIL: /nodes empty")
        return 1
    cap = healthy[0].get("capacity_bytes", 0)
    if cap != CAPACITY_BYTES:
        print(f"[smoke:contribute] FAIL: expected capacity {CAPACITY_BYTES}, got {cap}")
        return 1
    print(f"[smoke:contribute] /nodes reports capacity {CAPACITY_BYTES} bytes")

    # Wait for the leader's grant tick: total == capacity / 3. No upload is
    # needed — the grant is based on the declared capacity, not occupied bytes.
    node_hex = healthy[0]["node_id"]
    total = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        _, total = account_total(node_hex)
        if abs(total - EXPECTED_BYTES) <= 1:
            break
        time.sleep(1)
    if abs(total - EXPECTED_BYTES) > 1:
        print(
            f"[smoke:contribute] FAIL: grant total {total} != ~{EXPECTED_BYTES} "
            f"(capacity {CAPACITY_BYTES} / 3)"
        )
        return 1
    print(
        f"[smoke:contribute] contribution grant: total={total} "
        f"(= capacity {CAPACITY_BYTES} / 3)"
    )

    # Idempotency: another grant tick must not double-credit (one-time grant).
    time.sleep(6)
    _, total2 = account_total(node_hex)
    if total2 != total:
        print(f"[smoke:contribute] FAIL: grant not idempotent {total} -> {total2}")
        return 1
    print("[smoke:contribute] grant idempotent (one-time, no double credit)")

    print("[smoke:contribute] PASS: capacity-based one-time grant (capacity / 3)")
    return 0
