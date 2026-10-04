#!/usr/bin/env bash
# Packages and publishes this family's crate, yoke-sdk, at the version the tree states and no other — the
# two verbs `yoke`'s release verb asks a family's script for.
# Usage: package.sh package <version> <dir>      write yoke-sdk-<version>.crate into <dir>
#        package.sh publish-crate <version>      publish the crate, under CARGO_REGISTRY_TOKEN
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# A version asked for that is not the tree's is refused before anything is written: the tag, the crate
# and the line the library says it is cannot disagree.
stated() {
  local asked="$1" tree
  [[ "$asked" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "package: $asked is not a version: it is written X.Y.Z" >&2; exit 2; }
  tree="$(cargo pkgid -p yoke-sdk | sed -E 's/.*[@#]//')"
  [[ "$asked" == "$tree" ]] || { echo "package: the tree states $tree and $asked was asked for: nothing is packaged" >&2; exit 1; }
}

case "${1:-}" in
  package)
    version="${2:?usage: package.sh package <version> <dir>}"
    out="${3:?usage: package.sh package <version> <dir>}"
    stated "$version"
    mkdir -p "$out"
    cargo package -q --locked --allow-dirty --no-verify -p yoke-sdk
    cp "target/package/yoke-sdk-$version.crate" "$out/"
    ;;
  publish-crate)
    version="${2:?usage: package.sh publish-crate <version>}"
    stated "$version"
    [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]] || { echo "package: no crates.io credential: CARGO_REGISTRY_TOKEN is empty" >&2; exit 1; }
    cargo publish -q --locked -p yoke-sdk
    ;;
  *)
    echo "usage: package.sh package <version> <dir> | publish-crate <version>" >&2
    exit 2
    ;;
esac
