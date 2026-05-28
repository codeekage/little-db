# little-db

[![CI](https://github.com/codeekage/little-db/actions/workflows/ci.yml/badge.svg)](https://github.com/codeekage/little-db/actions/workflows/ci.yml)
[![Go 1.22](https://img.shields.io/badge/go-1.22-00ADD8?logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A small, embeddable, crash-safe key/value store with a TCP server, written
in Go 1.22 with **standard library only**. Storage is a log-structured
hash-table (Bitcask-style): append-only segments on disk, an in-memory
keydir for `O(1)` point lookups, periodic compaction.

The full design rationale, requirements, and architecture decisions live
in [docs/SPEC.md](docs/SPEC.md). This README is the operator-facing
quick-start. Day-2 operations (sizing, fsync semantics, restart, failure
modes) live in [docs/ops.md](docs/ops.md).

---

## At a glance

- **Single static binary**, single data directory, POSIX-only.
- **Crash-safe**: writes survive `kill -9` when `--sync-on-put` is on
  (see [§ Durability and fsync](#durability-and-fsync)).
- **API**: `Put`, `Get`, `Delete`, `BatchPut`, `ReadKeyRange`, `Stats`.
- **Wire**: framed binary protocol over TCP (see
  [docs/SPEC.md §5](docs/SPEC.md)).
- **No external dependencies** at runtime or build time.

### What it is not

A deliberately short list of non-goals (see [docs/SPEC.md §17](docs/SPEC.md)):
no transactions across keys, no secondary indexes, no TTLs, no
encryption-at-rest, no HTTP/REST, no automatic failover, no Windows
support. Replication is deferred to a planned
[separate branch](#replication-bonus) and is **not implemented** on
`main`.

---

## Build & run

```bash
make build                              # produces ./bin/little-db
./bin/little-db serve --data-dir ./data # listens on 127.0.0.1:4242
```

In another shell:

```bash
./bin/little-db put   hello world
./bin/little-db get   hello              # → world
./bin/little-db stats
./bin/little-db ping
```

Keys and values are positional. Use `--key-hex` / `--value-hex` for
non-printable bytes, or `--value-stdin` to stream a value from stdin.

Run `./bin/little-db --help` (or any subcommand with `--help`) for the
full flag list. The **complete CLI reference** — every subcommand,
flag, env var, encoding rule, limit, and exit code, plus worked
recipes — lives in [docs/cli.md](docs/cli.md).

### CLI at a glance

| Subcommand | Purpose                                                  |
| ---------- | -------------------------------------------------------- |
| `serve`    | Run the engine + TCP server.                             |
| `put`      | Write one key (positional VALUE or `--value-stdin`).     |
| `get`      | Read one key to stdout (raw bytes).                      |
| `delete`   | Tombstone one key.                                       |
| `range`    | Stream `[--start, --end)` to stdout (`ndjson` or `raw`). |
| `batch`    | Atomic multi-op from NDJSON (`--plain` or base64).       |
| `stats`    | `key_count=… bytes_on_disk=…`.                           |
| `ping`     | `pong` on success.                                       |

Every client subcommand accepts `--addr` (default `127.0.0.1:4242`,
env `LITTLE_DB_ADDR`), `--dial-timeout`, and `--request-timeout`.
Flags may appear before, between, or after positionals; use `--` to
force a positional that starts with `-`.

Quick examples:

```bash
little-db put user:1 '{"name":"Alice"}'
little-db get user:1
little-db delete user:1

# Scan all keys with the "user:" prefix (';' is ':'+1 in ASCII).
little-db range --start 'user:' --end 'user;' --format raw

# Bulk load 10 000 records in a single atomic batch.
little-db batch --plain - < testdata/users.10k.ndjson
```

Exit codes: `0` ok · `1` transport · `2` usage · `3` server bad/internal
· `4` not found · `5` overload/closed. See
[docs/cli.md](docs/cli.md#exit-codes) for the full table.

### Loading a JSON file

`put` stores the value as raw bytes (no parsing, no schema). Pipe a
JSON file in with `--value-stdin`:

```bash
./bin/little-db put user:42 --value-stdin < user.json
./bin/little-db get user:42 > out.json
diff user.json out.json     # round-trips byte-for-byte
```

Max value size is **16 MiB**, max key size **64 KiB**.

### Bulk load (NDJSON via `batch`)

For many key/value pairs use the `batch` subcommand. Stdin is **NDJSON**
(one JSON object per line); `key` and `value` must be **base64-encoded**
because the wire protocol carries raw bytes:

```jsonc
{"op":"put",    "key":"<base64>", "value":"<base64>"}
{"op":"delete", "key":"<base64>"}
```

For hand-edited fixtures where keys/values are plain UTF-8 strings, pass
`--plain` and skip the base64 envelope:

```jsonc
{"op":"put",    "key":"user:1", "value":"{\"name\":\"Alice\"}"}
{"op":"delete", "key":"user:1"}
```

```bash
./bin/little-db batch --plain - < seed.plain.ndjson
```

Default (base64) mode handles arbitrary bytes; `--plain` is UTF-8 only
and will reject keys/values containing the JSON-unsafe bytes you can't
spell as a literal.

Convert an arbitrary `{"k": "v", ...}` JSON file with `jq`:

```bash
jq -r 'to_entries[]
       | {op:"put", key:(.key|@base64), value:(.value|tostring|@base64)}
       | @json' data.json \
  | ./bin/little-db batch -
```

Per-call limits: ≤ **65 536 entries**, ≤ **32 MiB** encoded body
(wire-frame cap; server-side engine cap is `--max-batch-encoded-size`,
default 64 MiB). For larger imports, chunk the file (`split -l 5000 …`)
and submit one batch per chunk.

A batch is **atomic**: either every entry lands or none do. See
[docs/SPEC.md §4.4](docs/SPEC.md) for the on-disk record format.

---

## Verification

One command runs the whole gate:

```bash
make verify
```

That expands to:

| Step                                            | Purpose                                        |
| ----------------------------------------------- | ---------------------------------------------- |
| `go vet ./...`                                  | static checks                                  |
| `go test ./... -race -count=1 -skip '^TestReq'` | full correctness suite under the race detector |
| `go test ./internal/engine -run TestReq -v`     | SPEC §2 G1–G7 compliance (no race)             |

`TestReq*` is intentionally excluded from the race pass — race
instrumentation slows sync writes enough to flake the SPEC §2 G3 perf
floor. Compliance runs without `-race` so the floors mean what the SPEC
says they mean.

Other targets:

| Command                 | What it does                                                    |
| ----------------------- | --------------------------------------------------------------- |
| `make compliance`       | SPEC §2 G1–G7 with loose ceilings (default workload)            |
| `make compliance-heavy` | Same with `LITTLEDB_HEAVY=1` — 1M keys, 5-minute mixed workload |
| `make bench`            | Micro + per-op latency benchmarks                               |
| `make help`             | The menu                                                        |

Default `make compliance` uses **loose order-of-magnitude ceilings** so
the gate is deterministic across reviewer hardware. Hard SPEC §2 perf
floors (≤ 1 ms p99 Get, ≥ 50k Put/sec async) live in
`make compliance-heavy` and `make bench`, which target expected
hardware.

---

## Durability and fsync

By default `serve` runs with `--sync-on-put=false`: writes hit the
kernel buffer and return immediately. Throughput is high; a kernel
crash or power loss can lose up to the last group-commit window
(typically < 10 ms of writes).

`--sync-on-put=true` makes every write durable before the response is
returned. On Linux this calls `fsync(2)`. **On macOS this calls
`fcntl(F_FULLFSYNC)`** — plain `fsync` on darwin only flushes to the
disk's volatile cache and lies about durability. This is the single
most common reason a "crash-safe" KV store passes its tests on a dev
laptop but loses data on real hardware; we handle it.

The SPEC §2 G1 compliance test exercises this end-to-end by
`os.Exit(0)`-ing a subprocess mid-write and verifying every persisted
record survives in the parent.

---

## Sizing

The keydir is in RAM. Per-key overhead is roughly **64 bytes + len(key)**
(see [docs/SPEC.md §3, §10](docs/SPEC.md) and the sizing table in
[docs/ops.md](docs/ops.md)).

Rule of thumb: 10 M keys × 32-byte keys ≈ **~1 GiB resident**. If your
working set is much bigger than that, this is the wrong database.

---

## Configuration

All knobs are CLI flags on `little-db serve`; defaults are sensible for a
developer laptop. The full list is in `little-db serve --help`. The
operationally interesting ones are documented with rationale in
[docs/ops.md](docs/ops.md#configuration-reference).

---

## Repository layout

```
cmd/little-db/         CLI binary (serve + data-plane subcommands)
internal/engine/       Storage engine: segments, keydir, writer, recovery, compaction
internal/server/       TCP server (framing, per-conn handler, range streaming)
internal/client/       Go client used by the CLI and tests
internal/wire/         Frozen wire-protocol codec (request/response frames)
docs/SPEC.md           Full design and requirements spec
docs/ops.md            Operator runbook
```

---

## Replication (bonus)

Replication is **not implemented**. It is deferred to a planned
`bonus/replication` branch as an async single-leader design (see
[docs/SPEC.md §6](docs/SPEC.md) for the integration point). `main` is
intentionally single-node so the core flow ships without half-finished
cluster code on the hot path.

---

## License

This is a take-home assignment artifact; treat as private unless
otherwise stated.
