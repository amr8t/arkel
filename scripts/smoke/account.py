"""Quota ledger smoke: credit, usage debit, 507 over limit, release, idempotency.

Flow:
  bring up network
  register payment operator (fresh identity P)
  read client A's account hex
  credit A 2MB (source grant, ref smoke-1)
  non-operator credit (fresh identity C) -> must be rejected
  A put 1MB       -> ok; quota shows used > 0
  A put 4MB       -> must fail 507 (used would exceed 2MB)
  A rm            -> quota shows used == 0
  re-credit same ref_id -> total unchanged (idempotent)
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ._common import (
    BINARY,
    INDEX_FLAG,
    REPO_ROOT,
    bring_up_network,
    put,
    random_file,
    rm,
)

PAYMENT_DATA = REPO_ROOT / ".arkel_quota_data"
CLIENT_A = REPO_ROOT / ".arkel_client_data"
CLIENT_C = REPO_ROOT / ".arkel_client_c_data"

CREDIT_BYTES = 2 * 1024 * 1024


def _expect_fail(fn, label: str) -> bool:
    try:
        fn()
    except RuntimeError:
        print(f"[smoke:account] {label}: rejected as expected")
        return True
    print(f"[smoke:account] FAIL: {label} was allowed")
    return False


def account_quota(data_dir: Path) -> tuple[str, int, int]:
    """Returns (account_hex, used, total) for the identity in `data_dir`."""
    r = subprocess.run(
        [
            str(BINARY),
            "account",
            "--data-dir",
            str(data_dir),
            "quota",
            "--index-addrs",
            INDEX_FLAG,
        ],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"account quota failed: {r.stderr.strip()[-400:]}")
    line = r.stdout.strip().splitlines()[-1]
    acct, rest = line.split(":", 1)
    used_s, total_s = rest.replace("bytes used", "").split("/")
    return acct, int(used_s.strip()), int(total_s.strip())


def payment(*args: str, data_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(BINARY), "payment", "--data-dir", str(data_dir), *args],
        capture_output=True,
        text=True,
    )


def run(args) -> int:
    print("[smoke:account] booting 3 index nodes + 3 storage nodes...")

    shutil.rmtree(PAYMENT_DATA, ignore_errors=True)
    shutil.rmtree(CLIENT_A, ignore_errors=True)
    shutil.rmtree(CLIENT_C, ignore_errors=True)
    bring_up_network()

    r = payment("register", "--index-addrs", INDEX_FLAG, data_dir=PAYMENT_DATA)
    if r.returncode != 0:
        print(f"[smoke:account] FAIL: payment operator register: {r.stderr.strip()[-300:]}")
        return 1
    print("[smoke:account] payment operator registered")

    acct_a, _, _ = account_quota(CLIENT_A)
    print(f"[smoke:account] client A account {acct_a[:16]}...")

    r = payment(
        "credit",
        "--account",
        acct_a,
        "--bytes",
        str(CREDIT_BYTES),
        "--source",
        "grant",
        "--ref-id",
        "smoke-1",
        "--index-addrs",
        INDEX_FLAG,
        data_dir=PAYMENT_DATA,
    )
    if r.returncode != 0:
        print(f"[smoke:account] FAIL: credit: {r.stderr.strip()[-300:]}")
        return 1
    print("[smoke:account] credited A with 2MB")

    ok = _expect_fail(
        lambda: _credit_or_raise(CLIENT_C, acct_a, "smoke-1"),
        "non-operator credit",
    )

    small = random_file(1024 * 1024)
    put("smoke", "data.bin", small, data_dir=CLIENT_A)
    print("[smoke:account] A put 1MB OK")

    _, used, total = account_quota(CLIENT_A)
    if not (0 < used < total):
        print(f"[smoke:account] FAIL: expected 0 < used < total, got used={used} total={total}")
        return 1
    print(f"[smoke:account] quota after put: used={used} total={total}")

    big = random_file(4 * 1024 * 1024)
    ok &= _expect_fail(
        lambda: put("smoke", "big.bin", big, data_dir=CLIENT_A),
        "put 4MB over 2MB quota (507)",
    )

    rm("smoke", "data.bin", data_dir=CLIENT_A)
    _, used, _ = account_quota(CLIENT_A)
    if used != 0:
        print(f"[smoke:account] FAIL: expected used == 0 after rm, got {used}")
        return 1
    print("[smoke:account] quota released after rm (used == 0)")

    r = payment(
        "credit",
        "--account",
        acct_a,
        "--bytes",
        str(CREDIT_BYTES),
        "--source",
        "grant",
        "--ref-id",
        "smoke-1",
        "--index-addrs",
        INDEX_FLAG,
        data_dir=PAYMENT_DATA,
    )
    if r.returncode != 0:
        print(f"[smoke:account] FAIL: idempotent re-credit: {r.stderr.strip()[-300:]}")
        return 1
    _, _, total2 = account_quota(CLIENT_A)
    if total2 != total:
        print(f"[smoke:account] FAIL: re-credit changed total {total} -> {total2}")
        return 1
    print("[smoke:account] idempotent re-credit: total unchanged")

    if ok:
        print(
            "[smoke:account] PASS: credit, debit, 507, release, idempotency, "
            "non-operator rejection all correct"
        )
        return 0
    print("[smoke:account] FAIL: one or more checks failed")
    return 1


def _credit_or_raise(data_dir: Path, account: str, ref_id: str) -> None:
    r = payment(
        "credit",
        "--account",
        account,
        "--bytes",
        str(CREDIT_BYTES),
        "--source",
        "grant",
        "--ref-id",
        ref_id,
        "--index-addrs",
        INDEX_FLAG,
        data_dir=data_dir,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip()[-300:])
