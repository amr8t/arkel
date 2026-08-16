#!/usr/bin/env bash
# Regenerate website/content/cli.md from the clap definitions
# (via examples/gen_cli_docs.rs, which excludes the maintainer-only `index`
# subcommand).
#
#   ./scripts/gen_cli_docs.sh
set -euo pipefail

OUT=website/content/cli.md

{
  cat <<'EOF'
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

EOF

  cargo run --quiet --example gen_cli_docs
} > "$OUT"

echo "wrote $OUT"
