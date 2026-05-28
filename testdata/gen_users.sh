#!/usr/bin/env bash
# Generate users.<N>.ndjson fixtures for latency / throughput tests.
# Output is "--plain" NDJSON: literal UTF-8 keys & values, one PUT per line.
# Pipe straight into:  little-db batch --plain - < testdata/users.1k.ndjson
set -euo pipefail

cd "$(dirname "$0")"

gen() {
  local n="$1" out="$2"
  awk -v n="$n" 'BEGIN {
    for (i = 1; i <= n; i++) {
      age = 18 + (i % 60)
      role = (i % 3 == 0) ? "admin" : (i % 3 == 1) ? "editor" : "viewer"
      printf "{\"op\":\"put\",\"key\":\"user:%d\",\"value\":\"{\\\"id\\\":%d,\\\"name\\\":\\\"User %d\\\",\\\"email\\\":\\\"user%d@example.com\\\",\\\"age\\\":%d,\\\"role\\\":\\\"%s\\\"}\"}\n", \
        i, i, i, i, age, role
    }
  }' > "$out"
  echo "wrote $out ($(wc -l < "$out" | tr -d ' ') lines, $(wc -c < "$out" | tr -d ' ') bytes)"
}

gen 10     users.10.ndjson
gen 100    users.100.ndjson
gen 1000   users.1k.ndjson
gen 10000  users.10k.ndjson
