"""Perf smoke: N put/get roundtrips at a configurable size, report throughput + latency.

This is the data-plane throughput baseline — encrypt-whole + EC + pull-based shard
transfer + Raft manifest (put) and manifest-driven download + reconstruct (get).
`smoke perf --mb 10 --iters 20` for a longer run. M6 will compare this against the
S3-proxy path.
"""

from __future__ import annotations

import statistics

from ._common import (
    bring_up_network,
    get,
    put,
    random_file,
)

BUCKET = "perf"


def run(args) -> int:
    print("[smoke:perf] booting network...")
    flag = bring_up_network()

    iters = max(1, args.iters)
    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)

    print(f"[smoke:perf] {iters} roundtrips of {size // (1024 * 1024)} MB...")
    put_times: list[float] = []
    get_times: list[float] = []
    for i in range(iters):
        put_s, _ = put(BUCKET, f"obj-{i}", data, storage_flag=flag)
        get_s, out = get(BUCKET, f"obj-{i}", storage_flag=flag)
        if out.read_bytes() != data.read_bytes():
            print("[smoke:perf] FAIL: bytes mismatch")
            return 1
        put_times.append(put_s)
        get_times.append(get_s)

    mb = size / (1024 * 1024)
    put_mbps = mb / statistics.mean(put_times)
    get_mbps = mb / statistics.mean(get_times)
    print(f"[smoke:perf] PUT: {put_mbps:.1f} MB/s  avg {statistics.mean(put_times) * 1000:.0f} ms "
          f"p50 {statistics.median(put_times) * 1000:.0f} ms")
    print(f"[smoke:perf] GET: {get_mbps:.1f} MB/s  avg {statistics.mean(get_times) * 1000:.0f} ms "
          f"p50 {statistics.median(get_times) * 1000:.0f} ms")
    print("[smoke:perf] PASS: all roundtrips BLAKE3-verified")
    return 0
