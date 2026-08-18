+++
title = "CLI reference"
+++

# CLI reference

> Auto-generated from the clap definitions (`examples/gen_cli_docs.rs`, run via
> `./scripts/gen_cli_docs.sh`). The `index` subcommand is maintainer-only and
> omitted. Do not hand-edit the generated sections below.

Global flag: `--data-dir <path>` (dedicated directory for this node's identity
and storage state). Defaults to `./.arkel_<mode>_data`.

Default index cluster: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`.

# Command-Line Help for `arkel`

This document contains the help content for the `arkel` command-line program.

**Command Overview:**

* [`arkel`↴](#arkel)
* [`arkel storage`↴](#arkel-storage)
* [`arkel client`↴](#arkel-client)
* [`arkel client put`↴](#arkel-client-put)
* [`arkel client get`↴](#arkel-client-get)
* [`arkel client rm`↴](#arkel-client-rm)
* [`arkel account`↴](#arkel-account)
* [`arkel account quota`↴](#arkel-account-quota)
* [`arkel payment`↴](#arkel-payment)
* [`arkel payment register`↴](#arkel-payment-register)
* [`arkel payment credit`↴](#arkel-payment-credit)
* [`arkel repair`↴](#arkel-repair)

## `arkel`

Arkel Distributed Blob Index & Storage Node

**Usage:** `arkel [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `storage` — Boot up as a high-throughput raw block storage endpoint
* `client` — Upload/download objects as an iroh-native client
* `account` — Account & quota tools
* `payment` — Payment-operator tooling (register the quota credit key; first-wins)
* `repair` — Heal objects below target k/m (standalone, idempotent; run by cron)

###### **Options:**

* `--data-dir <DATA_DIR>` — Dedicated data directory for this specific node's cryptographic identities and storage state
* `--config <CONFIG>` — Node config file (arkel-node.toml); command-line flags override it



## `arkel storage`

Boot up as a high-throughput raw block storage endpoint

**Usage:** `arkel storage [OPTIONS]`

###### **Options:**

* `--private-relay-url <PRIVATE_RELAY_URL>` — Optional private Iroh relay architecture URL override
* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs to register against (comma-separated; the registrar discovers the current Raft leader among them)
* `--addr <ADDR>` — Address the iroh QUIC endpoint binds to
* `--advertise-addr <ADVERTISE_ADDR>` — Address advertised for registration (defaults to --addr)
* `--gc-interval-secs <GC_INTERVAL_SECS>` — How often to scan and delete unreferenced shards (seconds)



## `arkel client`

Upload/download objects as an iroh-native client

**Usage:** `arkel client <COMMAND>`

###### **Subcommands:**

* `put` — Upload a file (EC + encrypt locally, shards to storage nodes, manifest via Raft)
* `get` — Download an object and write it to a file (or stdout)
* `rm` — Delete an object (removes its manifest via Raft; shards freed by GC)



## `arkel client put`

Upload a file (EC + encrypt locally, shards to storage nodes, manifest via Raft)

**Usage:** `arkel client put [OPTIONS] <FILE>`

###### **Arguments:**

* `<FILE>` — Path to the file to upload

###### **Options:**

* `--bucket <BUCKET>`

  Default value: `default`
* `--key <KEY>` — Object key; defaults to the file name
* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`
* `--storage-addrs <STORAGE_ADDRS>` — Storage nodes to distribute shards to, as pubkey@ip:port (comma-separated)



## `arkel client get`

Download an object and write it to a file (or stdout)

**Usage:** `arkel client get [OPTIONS] <BUCKET> <KEY>`

###### **Arguments:**

* `<BUCKET>`
* `<KEY>`

###### **Options:**

* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`
* `--storage-addrs <STORAGE_ADDRS>` — Storage nodes that may hold shards, as pubkey@ip:port (comma-separated)
* `--output <OUTPUT>`



## `arkel client rm`

Delete an object (removes its manifest via Raft; shards freed by GC)

**Usage:** `arkel client rm [OPTIONS] <BUCKET> <KEY>`

###### **Arguments:**

* `<BUCKET>`
* `<KEY>`

###### **Options:**

* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`



## `arkel account`

Account & quota tools

**Usage:** `arkel account <COMMAND>`

###### **Subcommands:**

* `quota` — Show quota and usage for an account (default: this identity)



## `arkel account quota`

Show quota and usage for an account (default: this identity)

**Usage:** `arkel account quota [OPTIONS]`

###### **Options:**

* `--account <ACCOUNT>` — Account (hex iroh pubkey); defaults to this node's identity
* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`



## `arkel payment`

Payment-operator tooling (register the quota credit key; first-wins)

**Usage:** `arkel payment <COMMAND>`

###### **Subcommands:**

* `register` — Register this identity as the payment operator (one-time, first-wins)
* `credit` — Credit quota to an account (idempotent on --ref-id)



## `arkel payment register`

Register this identity as the payment operator (one-time, first-wins)

**Usage:** `arkel payment register [OPTIONS]`

###### **Options:**

* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`



## `arkel payment credit`

Credit quota to an account (idempotent on --ref-id)

**Usage:** `arkel payment credit [OPTIONS] --account <ACCOUNT> --bytes <BYTES> --ref-id <REF_ID>`

###### **Options:**

* `--account <ACCOUNT>` — Account (hex iroh pubkey) to credit
* `--bytes <BYTES>` — Bytes of quota to add
* `--source <SOURCE>` — Credit source label (e.g. 'payment')

  Default value: `payment`
* `--ref-id <REF_ID>` — Idempotency reference (e.g. Stripe checkout id); replay is a no-op
* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`



## `arkel repair`

Heal objects below target k/m (standalone, idempotent; run by cron)

**Usage:** `arkel repair [OPTIONS]`

###### **Options:**

* `--index-addrs <INDEX_ADDRS>` — Index node HTTP URLs (comma-separated)

  Default value: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`
* `--register` — Register this identity as the repair operator (one-time), then exit
* `--k <K>` — Re-encode target data shards (default 4)

  Default value: `4`
* `--m <M>` — Re-encode target parity shards (default 2)

  Default value: `2`
* `--rate-limit <RATE_LIMIT>` — Max objects fixed per run (rate limit)

  Default value: `600`



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

