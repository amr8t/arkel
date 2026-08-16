+++
title = "Contribute to the network"
+++


## 1. As an operator — run a storage node

Running a storage node contributes its disk to the network. It registers with
the index, stores encrypted shards for others, and a background GC loop frees
shards that are no longer referenced.

```sh
arkel storage \
  --addr 0.0.0.0:9001 \
  --index-addrs http://index.example:8001 \
  --data-dir /var/lib/arkel/storage
```

- `--addr` — the QUIC endpoint to bind (default `127.0.0.1:9001`)
- `--index-addrs` — index node URLs to register against
- Identity is auto-generated on first boot into `--data-dir/identity.key`.

Erasure coding means the network survives node failures — your node just stores
encrypted shards, and no single node holds your data in the clear.

> **Planned** get credited for contributing storage to the network

## 2. As a user — buy credits

> **Planned** `buy credits for increasing usage limits.
