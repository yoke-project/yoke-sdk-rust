#!/usr/bin/env bash
# The checks described by the-release.std.md, one function per case.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# std: yoke-sdk-rust:the-release.01
check_one_crate_published() {
  local listed
  listed="$(cd "$root" && cargo package --list --allow-dirty --quiet -p yoke-sdk 2>&1)" \
    || { echo "the crate yoke-sdk cannot be listed: $listed"; return 1; }
  local file
  for file in Cargo.toml LICENSE NOTICE src/lib.rs src/base.rs src/plugin.rs; do
    grep -qx "$file" <<<"$listed" || { echo "the crate does not carry $file"; return 1; }
  done
  local stray
  stray="$(grep -E '^(harness|checks|ci|\.github)/' <<<"$listed" || true)"
  [[ -z "$stray" ]] || { echo "the crate carries $stray"; return 1; }
  grep -qE '^pub mod base;' "$root/src/lib.rs" && grep -qE '^pub mod plugin;' "$root/src/lib.rs" \
    || { echo "the crate does not declare the modules base and plugin"; return 1; }
  grep -qE '^publish = false' "$root/harness/Cargo.toml" || { echo "the harness's crate does not say it is unpublished"; return 1; }
  local members
  members="$(cd "$root" && cargo metadata --no-deps --format-version 1 --quiet | grep -oE '"name":"yoke-[a-z-]+","version"' | sort -u)"
  [[ "$members" == $'"name":"yoke-plugin-harness","version"\n"name":"yoke-sdk","version"' ]] \
    || { echo "the workspace's crates are: $members"; return 1; }
}

# std: yoke-sdk-rust:the-release.02
check_packaged_at_the_trees_version() {
  [[ -f "$root/ci/package.sh" ]] || { echo "no ci/package.sh"; return 1; }
  local version out said
  version="$(cd "$root" && cargo pkgid -p yoke-sdk | sed -E 's/.*[@#]//')"
  out="$(mktemp -d)"
  said="$(bash "$root/ci/package.sh" package "$version" "$out" 2>&1)" || { echo "packaging at $version failed: $said"; return 1; }
  [[ -f "$out/yoke-sdk-$version.crate" ]] || { echo "packaging at $version wrote no yoke-sdk-$version.crate"; return 1; }
  rm -rf "$out" && out="$(mktemp -d)"
  if said="$(bash "$root/ci/package.sh" package 9.9.9 "$out" 2>&1)"; then
    echo "packaging at 9.9.9 was not refused"; return 1
  fi
  [[ "$said" == *9.9.9* && "$said" == *"$version"* ]] || { echo "the refusal does not name both versions: $said"; return 1; }
  [[ -z "$(ls -A "$out")" ]] || { echo "a refused packaging wrote $(ls "$out")"; return 1; }
  rm -rf "$out"
  grep -qF 'concat!("yoke-sdk-rust ", env!("CARGO_PKG_VERSION"))' "$root/src/plugin.rs" \
    || { echo "the SDK line is not the crate's version"; return 1; }
}

# std: yoke-sdk-rust:the-release.03
check_released_by_yokes_verb() {
  local verb workflow="$root/.github/workflows/release.yml"
  verb="$(cd "$root" && just --show release)"
  [[ "$verb" == *"github.com/yoke-project/yoke/cmd/yoke-release@"* && "$verb" == *"-crate yoke-sdk"* ]] \
    || { echo "the release verb does not run yoke's verb for the crate yoke-sdk"; return 1; }
  [[ -f "$workflow" ]] || { echo "no release.yml"; return 1; }
  grep -qE "tags: \['v\*'\]" "$workflow" || { echo "the release run does not run at a v tag"; return 1; }
  grep -qE '^[[:space:]]+id-token: write' "$workflow" || { echo "the release run may not request an identity token"; return 1; }
  local exchange
  exchange="$(grep -A2 -E 'id: crates' "$workflow")"
  [[ "$exchange" == *"rust-lang/crates-io-auth-action@"* && "$exchange" == *"continue-on-error: true"* ]] \
    || { echo "the exchange is not a step that may be refused: $exchange"; return 1; }
  grep -qF 'CARGO_REGISTRY_TOKEN: ${{ steps.crates.outputs.token }}' "$workflow" || { echo "the verb is not given the credential"; return 1; }
  grep -qF 'just release > manifest-lines.jsonl' "$workflow" || { echo "the verb's lines are not kept"; return 1; }
  grep -qE '^[[:space:]]+name: manifest-lines$' "$workflow" || { echo "no artifact manifest-lines"; return 1; }
  if grep -qE 'secrets\.' "$workflow"; then echo "the release run reads a stored secret"; return 1; fi
}
