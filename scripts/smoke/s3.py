"""S3 gateway smoke: boot the local cluster + `arkel s3`, then drive the gateway
over plain HTTP and with `restic` as a real S3 client.

Never touches production. Everything runs on loopback against a fresh local
cluster (3 index + 3 storage), the same way the other smokes do.
"""

from __future__ import annotations

import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

from ._common import BINARY, INDEX_FLAG, REPO_ROOT, bring_up_network, random_file, run_nodes

GATEWAY_PORT = 9000
GATEWAY_DATA = REPO_ROOT / ".arkel_gateway_data"
BUCKET = "s3smoke"
RESTIC_BUCKET = "restic-repo"
MAX_OBJECT = 8 * 1024 * 1024
PREFIX = "[smoke:s3]"
FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"{PREFIX} {'PASS' if ok else 'FAIL'}: {name}{(' — ' + detail) if detail else ''}")
    if not ok:
        FAILURES.append(name)
    return ok


def req(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
    conn = http.client.HTTPConnection("127.0.0.1", GATEWAY_PORT, timeout=180)
    conn.request(method, path, body=body, headers=headers or {})
    r = conn.getresponse()
    data = r.read()
    hdrs = {k.lower(): v for k, v in r.getheaders()}
    conn.close()
    return r.status, hdrs, data


def url(path: str) -> str:
    """Percent-encode a key path the way an S3 client would, keeping `/`."""
    return "/" + urllib.parse.quote(path, safe="/")


def start_gateway(gc_interval: int = 5) -> subprocess.Popen:
    shutil.rmtree(GATEWAY_DATA / "blobs", ignore_errors=True)
    (REPO_ROOT / "logs").mkdir(exist_ok=True)
    log = open(REPO_ROOT / "logs" / "s3-gateway.log", "w")
    proc = subprocess.Popen(
        [
            str(BINARY), "s3",
            "--data-dir", str(GATEWAY_DATA),
            "--listen", f"127.0.0.1:{GATEWAY_PORT}",
            "--index-addrs", INDEX_FLAG,
            "--max-object-size", str(MAX_OBJECT),
            "--gc-interval-secs", str(gc_interval),
        ],
        cwd=REPO_ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"gateway exited: {(REPO_ROOT / 'logs' / 's3-gateway.log').read_text()[-800:]}")
        try:
            socket.create_connection(("127.0.0.1", GATEWAY_PORT), timeout=1).close()
            print(f"{PREFIX} gateway up on 127.0.0.1:{GATEWAY_PORT}")
            return proc
        except OSError:
            time.sleep(0.3)
    raise RuntimeError("gateway did not start listening")


def key_count(bucket: str, prefix: str = "") -> int:
    q = f"/{bucket}?prefix={urllib.parse.quote(prefix)}" if prefix else f"/{bucket}"
    status, _, body = req("GET", q)
    if status != 200:
        raise RuntimeError(f"list {bucket} failed: {status} {body[:200]!r}")
    return body.count(b"<Key>")


def protocol_checks() -> None:
    data = os.urandom(64 * 1024)
    key = "backup/data-0001.bin"

    status, hdrs, _ = req("PUT", url(f"{BUCKET}/{key}"), data)
    check("put object", status == 200, f"status={status}")
    # No ETag is advertised: arkel stores no MD5, and faking one per request
    # would only prove the response matches itself. Clients that validate
    # checksums must tolerate its absence (Duplicati does).
    check("put advertises no ETag (deliberate)", "etag" not in hdrs, str(hdrs.get("etag")))

    status, hdrs, body = req("GET", url(f"{BUCKET}/{key}"))
    check("get object bytes match", status == 200 and body == data, f"status={status}")
    check("get Content-Length", hdrs.get("content-length") == str(len(data)))
    check("get advertises no ETag (deliberate)", "etag" not in hdrs, str(hdrs.get("etag")))

    status, hdrs, body = req("HEAD", url(f"{BUCKET}/{key}"))
    check(
        "head object size (manifest read, no shard fetch)",
        status == 200 and hdrs.get("content-length") == str(len(data)) and body == b"",
        f"status={status} len={hdrs.get('content-length')}",
    )
    # restic's Stat parses Last-Modified and retries forever without it.
    check("head object Last-Modified", bool(hdrs.get("last-modified")), hdrs.get("last-modified", ""))

    # restic loads pack subtrees with byte ranges, so 206 is required.
    status, hdrs, part = req("GET", url(f"{BUCKET}/{key}"), headers={"Range": "bytes=10-19"})
    check(
        "range get (206 + Content-Range)",
        status == 206
        and part == data[10:20]
        and hdrs.get("content-range") == f"bytes 10-19/{len(data)}",
        f"status={status} len={len(part)}",
    )

    # SDK clients (minio-go/aws-sdk) frame uploads as `aws-chunked`; the framing
    # must be stripped or it becomes object content.
    framed = b"5;chunk-signature=deadbeef\r\nhello\r\n0;chunk-signature=deadbeef\r\n\r\n"
    status, _, _ = req(
        "PUT",
        url(f"{BUCKET}/chunked.bin"),
        framed,
        {"content-encoding": "aws-chunked", "x-amz-decoded-content-length": "5"},
    )
    status2, _, got = req("GET", url(f"{BUCKET}/chunked.bin"))
    check(
        "aws-chunked upload body decoded",
        status == 200 and status2 == 200 and got == b"hello",
        f"put={status} get={status2} body={got[:32]!r}",
    )

    status, hdrs, _ = req("HEAD", f"/{BUCKET}")
    check("head bucket", status == 200, f"status={status}")

    status, _, body = req("GET", "/")
    check(
        "list buckets",
        status == 200 and b"<ListAllMyBucketsResult" in body and BUCKET.encode() in body,
        f"status={status}",
    )

    status, _, body = req("GET", f"/{BUCKET}?location")
    check(
        "get bucket location",
        status == 200 and b"LocationConstraint" in body,
        body[:80].decode(errors="replace"),
    )

    status, _, body = req("GET", f"/{BUCKET}?prefix=backup%2F")
    text = body.decode(errors="replace")
    check(
        "list objects with prefix + metadata",
        status == 200
        and key in text
        and "<IsTruncated>false</IsTruncated>" in text
        and f"<Size>{len(data)}</Size>" in text
        and "<ETag>&quot;" not in text
        and "<ETag>" in text,
        f"status={status}",
    )

    # Nested keys and awkward names must round-trip byte-identically, or backup
    # tools see phantom/missing files.
    for awkward in ["dir/sub/deep.bin", "with space.bin", "plus+sign.bin", "percent%25.bin", "ünïcode-🍰.bin"]:
        payload = awkward.encode()
        st, _, _ = req("PUT", url(f"{BUCKET}/{awkward}"), payload)
        if st != 200:
            check(f"put awkward key {awkward!r}", False, f"status={st}")
            continue
        st, _, got = req("GET", url(f"{BUCKET}/{awkward}"))
        check(f"roundtrip awkward key {awkward!r}", st == 200 and got == payload, f"status={st}")
        status, _, body = req("GET", f"/{BUCKET}?prefix={urllib.parse.quote(awkward[:6])}")
        check(
            f"listed awkward key {awkward!r}",
            awkward.encode() in body,
            body.decode(errors="replace")[:160],
        )

    status, _, _ = req("DELETE", url(f"{BUCKET}/{key}"))
    check("delete object", status == 204, f"status={status}")
    status, _, _ = req("GET", url(f"{BUCKET}/{key}"))
    check("deleted object is gone", status == 404, f"status={status}")

    # Oversized objects must be refused without taking the gateway down.
    status, _, _ = req("PUT", url(f"{BUCKET}/too-big.bin"), b"x" * (MAX_OBJECT + 1024))
    check("oversized object rejected", status in (400, 413), f"status={status}")
    status, _, _ = req("PUT", url(f"{BUCKET}/after-reject.bin"), b"ok")
    check("gateway alive after reject", status == 200, f"status={status}")


def restic_checks() -> None:
    if shutil.which("restic") is None:
        print(f"{PREFIX} SKIP: restic not installed")
        return
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": "arkel",
        "AWS_SECRET_ACCESS_KEY": "arkel",
        "AWS_DEFAULT_REGION": "us-east-1",
        "RESTIC_PASSWORD": "arkel-smoke",
        "RESTIC_CACHE_DIR": str(REPO_ROOT / ".arkel_restic_cache"),
    }
    repo = f"s3:http://127.0.0.1:{GATEWAY_PORT}/{RESTIC_BUCKET}"
    base = ["restic", "-r", repo, "-o", "s3.bucket-lookup=path", "--no-cache"]

    def run(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [*base, *args], cwd=REPO_ROOT, env=env, capture_output=True, text=True
        )

    r = run("init")
    check("restic init", r.returncode == 0, (r.stderr or r.stdout).strip()[-300:])

    src = Path(tempfile.mkdtemp(prefix="arkel-restic-src-"))
    (src / "a.txt").write_text("hello arkel\n")
    (src / "sub").mkdir()
    (src / "sub" / "b.bin").write_bytes(os.urandom(48 * 1024))

    r = run("backup", str(src))
    check("restic backup", r.returncode == 0, (r.stderr or r.stdout).strip()[-300:])

    before = key_count(RESTIC_BUCKET, "data/")
    r = run("backup", str(src))
    after = key_count(RESTIC_BUCKET, "data/")
    # restic writes one tree pack even when nothing changed; what matters is that
    # it does not re-upload the data packs.
    check(
        "restic no-change backup re-uploads no data",
        r.returncode == 0 and before > 0 and 0 <= after - before <= 1,
        f"data/ packs {before} -> {after}",
    )

    (src / "a.txt").write_text("hello arkel, changed\n")
    r = run("backup", str(src))
    changed = key_count(RESTIC_BUCKET, "data/")
    check(
        "restic third backup uploads only the delta",
        r.returncode == 0 and changed > after,
        f"data/ packs {after} -> {changed}",
    )

    r = run("check")
    check("restic check", r.returncode == 0, (r.stderr or r.stdout).strip()[-300:])

    r = run("snapshots")
    snapshots = [
        line for line in r.stdout.splitlines()
        if line.split() and len(line.split()[0]) == 8 and line.split()[0].isalnum()
    ]
    check("restic snapshots", r.returncode == 0 and len(snapshots) >= 3,
          f"{len(snapshots)} snapshots")

    out = Path(tempfile.mkdtemp(prefix="arkel-restic-out-"))
    r = run("restore", "latest", "--target", str(out))
    restored = next(iter(out.rglob("a.txt")), None)
    check(
        "restic restore bytes match",
        r.returncode == 0
        and restored is not None
        and restored.read_text() == "hello arkel, changed\n",
        f"restored={restored}",
    )


def run(args) -> int:
    print(f"{PREFIX} booting 3 index + 3 storage nodes...")
    bring_up_network()
    gw = start_gateway()
    try:
        protocol_checks()
        restic_checks()
    finally:
        gw.terminate()
        try:
            gw.wait(timeout=10)
        except subprocess.TimeoutExpired:
            gw.kill()

    if FAILURES:
        print(f"{PREFIX} FAIL: {len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        return 1
    print(f"{PREFIX} PASS: S3 gateway protocol + restic roundtrip, check, restore, dedup")
    return 0
