"""Access control smoke: two identities, cross-user write/delete must fail.

Drives the real `arkel client` CLI with two data dirs (two iroh identities).
Verifies item-3 semantics: an invitee (B) cannot rm or overwrite another
user's (A) object, but each user can write/delete their own bucket.

Flow:
  A put into bucket 'smoke'            -> ok
  B rm   A's 'smoke/data.bin'          -> must fail (forbidden)
  B put  'smoke/data.bin' (overwrite)  -> must fail (forbidden)
  B put  into own bucket 'smoke-b'     -> ok
  A rm   own 'smoke/data.bin'          -> ok
  A get  'smoke/data.bin'              -> must fail (404)
"""

from __future__ import annotations

import shutil

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


def run(args) -> int:
    print("[smoke:access] booting 3 index nodes + 3 storage nodes...")

    # Fresh identities for both clients (distinct --data-dir).
    shutil.rmtree(CLIENT_A, ignore_errors=True)
    shutil.rmtree(CLIENT_B, ignore_errors=True)
    bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    ok = True

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
            "[smoke:access] PASS: cross-user write/delete blocked, "
            "owner ops work"
        )
        return 0
    print("[smoke:access] FAIL: one or more checks failed")
    return 1
