#!/usr/bin/env bash
# Turns what `cargo test` printed into the machine-readable lines the record writer reads: one
# {"Action":…,"Test":…} per test, keyed by the test function's own name, which is what a marker names;
# a failure carries what its test printed.
# Usage: cargo-results.sh <cargo test output>
set -euo pipefail

awk '
function esc(s) { gsub(/\\/, "\\\\", s); gsub(/"/, "\\\"", s); gsub(/\t/, "\\t", s); return s }
function short(name) { n = split(name, parts, "::"); return parts[n] }
/^test .* \.\.\. (ok|FAILED|ignored)$/ {
  name = short($2); verdict = $NF
  action = verdict == "ok" ? "pass" : (verdict == "FAILED" ? "fail" : "skip")
  results[name] = action; order[++count] = name; next
}
/^---- .* stdout ----$/ { current = short($2); next }
/^(failures:|successes:)$/ { current = ""; next }
current != "" { output[current] = output[current] esc($0) "\\n" }
END {
  for (i = 1; i <= count; i++) {
    name = order[i]
    if (name in output) printf "{\"Action\":\"output\",\"Test\":\"%s\",\"Output\":\"%s\"}\n", name, output[name]
    printf "{\"Action\":\"%s\",\"Test\":\"%s\"}\n", results[name], name
  }
}' "$1"
