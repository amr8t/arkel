#!/usr/bin/env bash
# Cut a release: bump the version, run the checks, commit, tag, and push.
# Pushing the tag triggers .github/workflows/release.yml, which builds the
# release binary and publishes it to GitHub Releases.
#
#   ./scripts/release.sh            # patch bump (default)
#   ./scripts/release.sh minor      # or: major | minor | patch
#   ./scripts/release.sh 0.0.2      # or an explicit version
#
# The pre-tag smoke (python scripts/run_nodes.py smoke basic) is left to the
# operator; this script pauses for confirmation before pushing.
set -euo pipefail

bump_version() { # $1 = current x.y.z, $2 = major|minor|patch
  local major minor patch
  IFS=. read -r major minor patch <<<"$1"
  case "$2" in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "$major.$((minor + 1)).0" ;;
    patch) echo "$major.$minor.$((patch + 1))" ;;
  esac
}

current="$(cargo pkgid | sed -E 's/.*#//')"

case "${1:-patch}" in
  major | minor | patch) VERSION="$(bump_version "$current" "${1:-patch}")" ;;
  *) VERSION="${1#v}" ;; # accept v0.0.2 or 0.0.2
esac

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$VERSION' is not a bare semver like 0.0.2" >&2
  exit 1
fi

command -v cargo-set-version >/dev/null 2>&1 || {
  echo "error: cargo-edit missing; run: cargo install cargo-edit" >&2
  exit 1
}

if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree not clean; commit or stash changes first" >&2
  git status --short >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null; then
  echo "error: tag v$VERSION already exists" >&2
  exit 1
fi

if [ "$current" != "$VERSION" ] &&
  [ "$(printf '%s\n%s\n' "$current" "$VERSION" | sort -V | head -n1)" = "$VERSION" ]; then
  echo "error: cannot downgrade $current -> $VERSION with cargo-set-version" >&2
  echo "       hand-edit Cargo.toml, run 'cargo update -p arkel', then commit/tag manually" >&2
  exit 1
fi

echo "==> Releasing v$VERSION"
cargo fmt --check
cargo set-version "$VERSION"
cargo clippy --all-targets
cargo test
cargo build --release

echo
echo "Pre-tag smoke is manual. In another terminal run:"
echo "    python scripts/run_nodes.py smoke basic"
read -r -p "Smoke passed and ready to push v$VERSION? [y/N] " reply
[ "$reply" = "y" ] || {
  echo "aborted; run 'git checkout -- Cargo.toml Cargo.lock' to undo the bump"
  exit 1
}

git add Cargo.toml Cargo.lock
git diff --cached --stat
git commit -m "v$VERSION"

git push origin master
git tag "v$VERSION"
git push origin "v$VERSION"

echo "==> v$VERSION tagged; the release workflow will publish the binary"
