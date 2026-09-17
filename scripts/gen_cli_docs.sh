#!/usr/bin/env bash
# Regenerate the website's docs/cli.md from the clap definitions
# (via scripts/gen_cli_docs.rs, which excludes the maintainer-only `index`
# subcommand). The site lives in its own repo at ../arkel-site.
#
#   ./scripts/gen_cli_docs.sh
set -euo pipefail

OUT=../arkel-site/docs/cli.md

{
  cat <<'EOF'
---
title: CLI reference
---

# CLI reference

<!-- Auto-generated from the clap definitions (scripts/gen_cli_docs.rs, run via
./scripts/gen_cli_docs.sh). The `index` subcommand is maintainer-only and
omitted. Do not hand-edit the generated sections below. -->

Default index cluster: `http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`.

That is the default `--index-addrs` for the `client`, `account`, `payment`,
`repair`, and `storage` commands (and the `[storage] index_addrs` key in
`arkel-node.toml`) — point those at the index cluster you run.

EOF

  cargo run --quiet --example gen_cli_docs
} > "$OUT"

echo "wrote $OUT"
