# The six verbs every repository defines.
# A verb with nothing to do says so in one line, so a fan-out can tell a gap from a statement.

# Build this repository's codebase.
build:
    cargo build --workspace --all-targets --locked

# Run this repository's own checks, with no sibling present.
test:
    #!/usr/bin/env bash
    # A run leaves its results where the record writer reads them, whatever it decided.
    set -uo pipefail
    mkdir -p .results
    date -u +%Y-%m-%dT%H:%M:%SZ > .results/started
    status=0
    if command -v yoke-verify > /dev/null; then
        yoke-verify descriptions --repository yoke-sdk-rust . > /dev/null || status=1
        yoke-verify markers --repository yoke-sdk-rust . > /dev/null || status=1
    else
        echo "test: yoke-verify is not on PATH; \`just develop\` puts it there"
        status=1
    fi
    bash checks/run.sh | tee .results/checks.txt || status=1
    cargo test --workspace --locked --no-fail-fast 2>&1 | tee .results/cargo.txt; (( PIPESTATUS[0] == 0 )) || status=1
    bash ci/cargo-results.sh .results/cargo.txt > .results/cargo.json
    date -u +%Y-%m-%dT%H:%M:%SZ > .results/finished
    exit "$status"

# This repository's static checks.
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    bash -n checks/run.sh checks/*/*.sh ci/*.sh
    echo "lint: every shell script parses"

# Fail, naming each file, when the tree is not formatted.
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    files="$(find . -name '*.rs' -not -path './.git/*' -not -path './target/*' | sort)"
    if [[ -z "$files" ]]; then echo "fmt: nothing to format yet"; exit 0; fi
    rustfmt --check --edition 2024 $files
    echo "fmt: every file is formatted"

# Verify the toolchain against the floor the workspace's fan-out passes, and put the verification
# tool on PATH at the version the workspace names — run alone, the newest published.
develop floor="" verify="":
    #!/usr/bin/env bash
    set -euo pipefail
    found="$(just --version | awk '{print $2}')"
    if [[ -z "{{floor}}" ]]; then
        echo "develop: no floor given, so none verified — the workspace passes it; found just $found"
    elif ! [[ "{{floor}}" =~ ^[0-9]+(\.[0-9]+)*$ ]]; then
        echo "develop: '{{floor}}' is not a version; pass it as \`just develop 1.58.0\`"
        exit 1
    else
        lowest="$(printf '%s\n%s\n' "{{floor}}" "$found" | sort -V | head -n 1)"
        if [[ "$lowest" != "{{floor}}" ]]; then
            echo "develop: just {{floor}} or newer is needed; found just $found"
            exit 1
        fi
        echo "develop: just $found meets the floor {{floor}}"
    fi
    bash ci/yoke-verify.sh "{{verify}}"

# Publish into this repository's ecosystem, one manifest line per publication.
release:
    #!/usr/bin/env bash
    set -euo pipefail
    # The published release verb of `yoke`, publishing this family's crate alone: it packages and
    # publishes it by ci/package.sh, which refuses a tag that is not the tree's version. It is resolved at
    # its source, since the proxy may still hold an older answer for a branch, with git's automatic
    # collection off, which would otherwise rewrite the shallow clone under go's second fetch.
    version="$(cd "$(mktemp -d)" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=gc.auto GIT_CONFIG_VALUE_0=0 GOPROXY=direct go list -m -f '{{{{.Version}}' github.com/yoke-project/yoke@main)"
    go run "github.com/yoke-project/yoke/cmd/yoke-release@$version" -crate yoke-sdk
