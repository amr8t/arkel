"""Access control smoke: two identities, cross-user write/delete/read must fail.

Drives the real `arkel client` CLI with two data dirs (two iroh identities).
Verifies item-3 semantics plus the two launch-hardening gates: an invitee (B)
cannot rm/overwrite/read another user's (A) object, and an unsigned /register
(no node-bound signature) is rejected.

Flow:
  unsigned /register                     -> must be rejected (401/403)
  A put into bucket 'smoke'              -> ok
  B rm   A's 'smoke/data.bin'            -> must fail (forbidden)
  B put  'smoke/data.bin' (overwrite)    -> must fail (forbidden)
  B get  A's 'smoke/data.bin'            -> must fail (read privacy)
  A get  own 'smoke/data.bin'            -> ok
  B put  into own bucket 'smoke-b'       -> ok
  A rm   own 'smoke/data.bin'            -> ok
  A get  'smoke/data.bin'                -> must fail (404)
"""

from __future__ import annotations

import json
import shutil
import urllib.error
import urllib.request

from ._common import (
    bring_up_network,
    get,
    put,
    random_file,
    rm,
    REPO_ROOT,
)

CLIENT_A = REPO_ROOT / ".arkel_client_data"
CLIENT_B = REPO_ROOT / ".arkel_client_b_data"


def _expect_fail(fn, label: str) -> bool:
    try:
        fn()
    except RuntimeError:
        print(f"[smoke:access] {label}: rejected as expected")
        return True
    print(f"[smoke:access] FAIL: {label} was allowed")
    return False


def _unsigned_register_rejected() -> bool:
    payload = json.dumps(
        {
            "node_id": [0] * 32,
            "capacity_bytes": 0,
            "addr": "127.0.0.1:9999",
            "relay_url": None,
            "registered_at": 0,
        }
    ).encode()
    req = urllib.request.Request(
        "http://127.0.0.1:8001/register",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=3)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            print("[smoke:access] unsigned /register: rejected as expected")
            return True
    print("[smoke:access] FAIL: unsigned /register was accepted")
    return False


def _unsigned_gc_rejected() -> bool:
    try:
        urllib.request.urlopen(
            "http://127.0.0.1:8001/shards/gc-candidates?hashes=00", timeout=3
        )
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print("[smoke:access] unsigned /shards/gc-candidates: rejected as expected")
            return True
    print("[smoke:access] FAIL: unsigned /shards/gc-candidates was accepted")
    return False


def run(args) -> int:
    print("[smoke:access] booting 3 index nodes + 3 storage nodes...")

    # Fresh identities for both clients (distinct --data-dir).
    shutil.rmtree(CLIENT_A, ignore_errors=True)
    shutil.rmtree(CLIENT_B, ignore_errors=True)
    bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    ok = _unsigned_register_rejected()
    ok &= _unsigned_gc_rejected()

    put("smoke", "data.bin", data, data_dir=CLIENT_A)
    print("[smoke:access] A put into bucket 'smoke' OK")

    ok &= _expect_fail(
        lambda: rm("smoke", "data.bin", data_dir=CLIENT_B),
        "B rm A's object",
    )
    ok &= _expect_fail(
        lambda: put("smoke", "data.bin", data, data_dir=CLIENT_B),
        "B overwrite A's object",
    )
    ok &= _expect_fail(
        lambda: get("smoke", "data.bin", data_dir=CLIENT_B),
        "B get A's object (read privacy)",
    )

    _, out = get("smoke", "data.bin", data_dir=CLIENT_A)
    if data.read_bytes() != out.read_bytes():
        print("[smoke:access] FAIL: A could not read own object")
        ok = False
    else:
        print("[smoke:access] A get own object OK")

    put("smoke-b", "data.bin", data, data_dir=CLIENT_B)
    print("[smoke:access] B put into own bucket 'smoke-b' OK")

    rm("smoke", "data.bin", data_dir=CLIENT_A)
    print("[smoke:access] A rm own object OK")

    ok &= _expect_fail(
        lambda: get("smoke", "data.bin", data_dir=CLIENT_A),
        "get after rm (404)",
    )

    if ok:
        print(
            "[smoke:access] PASS: cross-user write/delete/read blocked, "
            "unsigned register rejected, owner ops work"
        )
        return 0
    print("[smoke:access] FAIL: one or more checks failed")
    return 1
