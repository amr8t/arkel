"""Basic data-plane smoke: boot index + storage, put -> get -> verify bytes match.

Drives the real `arkel client` CLI over real processes. The GET runs with a
wiped client cache so shards must come from the storage nodes over the network.
Uses auto-discovery (no --storage-addrs), so the pool comes from GET /nodes.
"""

from __future__ import annotations

from ._common import (
    bring_up_network,
    get,
    put,
    random_file,
    REPO_ROOT,
)


def run(args) -> int:
    print("[smoke:basic] booting 3 index nodes + 3 storage nodes...")
    bring_up_network()

    size = max(1, int(args.mb * 1024 * 1024))
    data = random_file(size)
    print(f"[smoke:basic] putting {size // (1024 * 1024)} MB (auto-assign)...")
    elapsed, etag = put("smoke", "data.bin", data)
    print(f"[smoke:basic] put ETag: {etag} ({elapsed:.2f}s)")

    print("[smoke:basic] getting (network, cache wiped)...")
    get_elapsed, out = get("smoke", "data.bin")

    if data.read_bytes() == out.read_bytes():
        print(f"[smoke:basic] PASS: {size // (1024 * 1024)} MB roundtrip verified "
              f"(encrypt-whole + EC + pull shards + Raft manifest + reconstruct, BLAKE3) "
              f"put={elapsed:.2f}s get={get_elapsed:.2f}s")
        return 0
    print("[smoke:basic] FAIL: bytes mismatch")
    return 1
