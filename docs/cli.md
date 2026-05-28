# little-db CLI reference

This document is the **complete** reference for the `little-db` binary
— every subcommand, every flag, every environment variable, every
exit code. If something in the CLI behaves in a way this document
doesn't describe, that's a documentation bug; please open an issue.

For the high-level overview see [README.md](../README.md). For
day-2 operations (sizing, fsync, restart) see [ops.md](ops.md).

---

## Table of contents

1. [Synopsis](#synopsis)
2. [Global flags (every client subcommand)](#global-flags-every-client-subcommand)
3. [Flag parsing rules](#flag-parsing-rules)
4. [Key / value encoding](#key--value-encoding)
5. [Limits](#limits)
6. [Exit codes](#exit-codes)
7. Subcommands
   - [`serve`](#serve)
   - [`put`](#put)
   - [`get`](#get)
   - [`delete`](#delete)
   - [`range`](#range)
   - [`batch`](#batch)
   - [`stats`](#stats)
   - [`ping`](#ping)
8. [Recipes](#recipes)

---

## Synopsis

```
little-db <subcommand> [flags] [positionals]
little-db --help | -h | help
```

Run `little-db <subcommand> --help` for the canonical flag list emitted
by the binary itself.

---

## Global flags (every client subcommand)

These three flags are registered on every subcommand except `serve`
(which has its own listen-side flags). All three honor an environment
variable as the default; the CLI flag always wins over the env var.

| Flag                | Default          | Env                         | Meaning                             |
| ------------------- | ---------------- | --------------------------- | ----------------------------------- |
| `--addr`            | `127.0.0.1:4242` | `LITTLE_DB_ADDR`            | Server address (`host:port`).       |
| `--dial-timeout`    | `5s`             | `LITTLE_DB_DIAL_TIMEOUT`    | TCP dial deadline.                  |
| `--request-timeout` | `30s`            | `LITTLE_DB_REQUEST_TIMEOUT` | Per-request deadline (client-side). |

Durations use Go's [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration)
syntax: `500ms`, `5s`, `2m30s`, `1h`.

---

## Flag parsing rules

`little-db` uses a flag parser that allows **flags before, between, and
after positionals**, unlike Go's stdlib `flag` package which stops at
the first non-flag token. All of the following are equivalent:

```bash
little-db put --addr 127.0.0.1:4242 user:1 hello
little-db put user:1 hello --addr 127.0.0.1:4242
little-db put user:1 --addr 127.0.0.1:4242 hello
```

Two escape hatches for positionals that _look_ like flags:

- **`--`** ends flag parsing. Everything after it is a positional:
  ```bash
  little-db put -- weirdkey -starts-with-dash    # value is "-starts-with-dash"
  ```
- **`-`** by itself is a positional (the conventional "stdin"
  marker used by `batch`), not a flag.

Bool flags (e.g. `--value-stdin`, `--plain`, `--key-hex`) do **not**
consume the next token; non-bool flags do.

---

## Key / value encoding

Keys and values on the wire are **raw bytes**. The CLI offers three
ways to express them on the command line; pick one per call:

| Mode             | How to enable               | What the CLI takes                                      |
| ---------------- | --------------------------- | ------------------------------------------------------- |
| **UTF-8 string** | default (no flag)           | The positional argument's literal bytes.                |
| **Hex**          | `--key-hex` / `--value-hex` | The positional is decoded with `encoding/hex`.          |
| **Stdin**        | `--value-stdin` (put only)  | `VALUE` is read as raw bytes from stdin; no positional. |

For `batch`, the encoding is selected per _file_, not per entry, via
`--plain` (literal UTF-8) vs. the default (base64). See [`batch`](#batch).

Range bounds follow the same rule, with `--start-hex` / `--end-hex`
switching the corresponding `--start` / `--end` value to hex.

---

## Limits

All limits are wire-protocol-level and enforced both client-side
(returns exit `2`) and server-side (returns a `BAD_REQUEST`).

| Resource                | Limit                                            |
| ----------------------- | ------------------------------------------------ |
| Max key length          | **64 KiB**                                       |
| Max value length        | **16 MiB**                                       |
| Max entries per `batch` | **65 536**                                       |
| Max encoded batch body  | **64 MiB**                                       |
| Max range payload bytes | **64 MiB** (server `--max-range-response-bytes`) |

`batch` validates the encoded body size _as it parses_ each line and
fails with exit `2` before dialing, so an oversized batch never wastes
a connection.

---

## Exit codes

| Code | Meaning                                                                                        |
| ---: | ---------------------------------------------------------------------------------------------- |
|  `0` | Success.                                                                                       |
|  `1` | Transport failure (dial, timeout, broken connection, framing).                                 |
|  `2` | Bad usage: unknown subcommand, bad flag, malformed argument, over-limit, malformed batch line. |
|  `3` | Server returned `BAD_REQUEST` or `INTERNAL`.                                                   |
|  `4` | `NOT_FOUND` (only `get` returns this from a successful round trip).                            |
|  `5` | Server returned `OVERLOAD` or `CLOSED` (back off and retry).                                   |

All non-zero exits write a human-readable diagnostic to **stderr**.

---

## `serve`

Runs the engine and the TCP server. The only subcommand that does not
take a `--addr` client flag (it has its own listen `--addr`).

### Synopsis

```
little-db serve --data-dir DIR [flags]
```

`--data-dir` is the only required flag. The directory is created if it
does not exist; on first start the engine writes an empty manifest.

### Server / network flags

| Flag                             | Default          | Meaning                                                                                 |
| -------------------------------- | ---------------- | --------------------------------------------------------------------------------------- |
| `--addr`                         | `127.0.0.1:4242` | TCP listen address.                                                                     |
| `--read-deadline`                | `30s`            | Per-request read deadline on the wire.                                                  |
| `--write-deadline`               | `30s`            | Per-response write deadline on the wire.                                                |
| `--max-concurrent-range-streams` | `4`              | Server-wide cap on in-flight `range` streams. Excess requests get `OVERLOAD`.           |
| `--max-range-response-bytes`     | `64 MiB`         | Per-`range` payload byte cap (server truncates and returns `OVERLOAD` past this).       |
| `--shutdown-grace`               | `30s`            | Max time to wait for in-flight requests during graceful shutdown on `SIGINT`/`SIGTERM`. |

### Engine flags

| Flag                       | Default      | Meaning                                                                                   |
| -------------------------- | ------------ | ----------------------------------------------------------------------------------------- |
| `--data-dir`               | _(required)_ | Engine data directory.                                                                    |
| `--max-segment-size`       | `256 MiB`    | Active segment rotation threshold (bytes).                                                |
| `--sync-on-put`            | `false`      | If true, every write fsyncs (Linux `fsync`, macOS `F_FULLFSYNC`) before returning OK.     |
| `--write-queue-depth`      | `64`         | Engine writer request channel depth. Larger absorbs bursts; smaller backpressures sooner. |
| `--max-batch-encoded-size` | `64 MiB`     | Engine-side max encoded batch body. Must match or exceed the CLI's per-call cap.          |
| `--compaction-interval`    | `0`          | Background compaction interval (`0` disables; use e.g. `5m` in production).               |

### Observability flags

| Flag           | Default | Meaning                                                                                    |
| -------------- | ------- | ------------------------------------------------------------------------------------------ |
| `--log-level`  | `info`  | `debug` adds one structured line per request; `warn` / `error` quiet the lifecycle events. |
| `--log-format` | `text`  | `text` (key=value) or `json` (one JSON object per line, suitable for `jq` / shipping).     |

### Output

On successful bind, prints exactly one line to **stdout**:

```
little-db: listening on 127.0.0.1:4242 (data-dir=./data)
```

Structured log lines go to **stderr**. On `SIGINT`/`SIGTERM`, prints
`little-db: shutting down`, waits up to `--shutdown-grace`, then exits
`0`. A second signal during shutdown hard-kills the process.

### Example

```bash
little-db serve \
  --data-dir ./data \
  --addr 0.0.0.0:4242 \
  --sync-on-put \
  --compaction-interval 5m \
  --log-level info \
  --log-format json
```

---

## `put`

Write a single key.

### Synopsis

```
little-db put [global flags] [--key-hex] [--value-hex] KEY VALUE
little-db put [global flags] [--key-hex] --value-stdin KEY < FILE
```

### Flags

| Flag            | Meaning                                                                               |
| --------------- | ------------------------------------------------------------------------------------- |
| `--key-hex`     | Treat `KEY` as a hex string and decode it.                                            |
| `--value-hex`   | Treat `VALUE` as a hex string and decode it. Mutually exclusive with `--value-stdin`. |
| `--value-stdin` | Read `VALUE` bytes from stdin (raw, no decoding). Omit the `VALUE` positional.        |

### Output

On success, writes `OK put key=<klen> val=<vlen>` to **stderr** and
exits `0`. Nothing is written to stdout.

### Examples

```bash
# UTF-8 strings (the common case).
little-db put user:1 '{"name":"Alice"}'

# Binary key, UTF-8 value.
little-db put --key-hex 010203 hello

# Stream a large value from a file.
little-db put user:42 --value-stdin < user.json
```

---

## `get`

Read one key. Writes the raw value bytes to **stdout** with no trailing
newline (so it round-trips arbitrary binary data verbatim).

### Synopsis

```
little-db get [global flags] [--key-hex] KEY
```

### Flags

| Flag        | Meaning                                    |
| ----------- | ------------------------------------------ |
| `--key-hex` | Treat `KEY` as a hex string and decode it. |

### Exit codes

- `0` on hit (value written to stdout).
- `4` on miss (`little-db: not found` on stderr).

### Examples

```bash
little-db get user:1
little-db get user:42 > out.json
diff user.json out.json     # byte-for-byte equal

little-db get --key-hex 010203
```

---

## `delete`

Tombstone one key. Returns `0` whether or not the key existed (idempotent).

### Synopsis

```
little-db delete [global flags] [--key-hex] KEY
```

### Flags

| Flag        | Meaning                                    |
| ----------- | ------------------------------------------ |
| `--key-hex` | Treat `KEY` as a hex string and decode it. |

### Output

On success, writes `OK delete key=<klen>` to **stderr** and exits `0`.

### Examples

```bash
little-db delete user:3
little-db delete --key-hex deadbeef
```

---

## `range`

Stream a half-open key range `[start, end)` from the server. **Bounds
are flags, not positionals.** Empty bounds mean "unbounded" on that side.

### Synopsis

```
little-db range [global flags] \
  [--start KEY] [--end KEY] [--start-hex] [--end-hex] \
  [--format ndjson|raw]
```

### Flags

| Flag          | Default   | Meaning                                                 |
| ------------- | --------- | ------------------------------------------------------- |
| `--start`     | _(empty)_ | Range start, **inclusive**. Empty = from the first key. |
| `--end`       | _(empty)_ | Range end, **exclusive**. Empty = through the last key. |
| `--start-hex` | `false`   | Decode `--start` as hex.                                |
| `--end-hex`   | `false`   | Decode `--end` as hex.                                  |
| `--format`    | `ndjson`  | Output format. See below.                               |

Passing positional args returns exit `2` with
`range: use --start/--end flags, not positional args`. Passing
`--start > --end` is rejected locally with exit `2`.

### Output formats

- **`ndjson`** (default, binary-safe). One JSON object per line:

  ```jsonc
  { "key": "<base64>", "value": "<base64>" }
  ```

  This is the only lossless format for keys/values that contain
  newlines, tabs, or non-UTF-8 bytes.

- **`raw`** (human-readable, lossy). `KEY\tVALUE\n` per record. **Will
  produce garbage** for keys or values containing literal `\t` or `\n`.
  Use only when you control the keyspace.

### Prefix scans

A prefix `P` covers exactly the half-open interval `[P, P+1)` where
`P+1` is `P` with its last byte incremented by 1. The common ASCII
trick:

```bash
# All keys starting with "user:" — ':' is 0x3A, ';' is 0x3B.
little-db range --start 'user:' --end 'user;' --format raw
```

Quote `;` in shells where it's a separator (`bash`, `zsh`).

### Examples

```bash
# Unbounded scan, count keys.
little-db range --format raw | wc -l

# Bounded slice.
little-db range --start 'user:3' --end 'user:7' --format raw

# Binary bounds.
little-db range --start-hex 00 --end-hex 80

# Decode NDJSON output to "<key> -> <value>" lines.
little-db range --start 'user:' --end 'user;' \
  | jq -r '"\(.key|@base64d) -> \(.value|@base64d)"'

# Stop after the first matching record (cheap probe).
little-db range --start 'user:7' --end 'user:8' --format raw
```

---

## `batch`

Submit many `put` / `delete` operations atomically in a single request.
Either every entry lands or none do.

### Synopsis

```
little-db batch [global flags] [--plain] (- | FILE)
```

The positional is the input source: `-` for stdin, or a path. Either
way the input is **NDJSON** — one JSON object per line.

### Line format

```jsonc
{"op":"put",    "key":"<...>", "value":"<...>"}
{"op":"delete", "key":"<...>"}
```

Encoding of `key` and `value`:

- **Default mode**: both fields are **base64-encoded** (`encoding/base64`,
  `StdEncoding`). This is the only binary-safe option.
- **`--plain` mode**: both fields are **literal UTF-8** strings.
  Convenient for hand-edited fixtures; cannot carry non-UTF-8 bytes.

### Flags

| Flag      | Meaning                                                     |
| --------- | ----------------------------------------------------------- |
| `--plain` | Interpret `key`/`value` as literal UTF-8 instead of base64. |

### Validation

The CLI streams the file and rejects locally (exit `2`) on:

- Invalid JSON on any line.
- `op` not in `{"put", "delete"}`.
- Empty key or key over the 64 KiB cap.
- Value over the 16 MiB cap.
- Cumulative encoded body over 64 MiB (cap is `wire.MaxFramePayload - 1`).
- More than 10 000 entries.
- Zero entries.

### Output

On success, writes `OK batch entries=<N>` to **stderr** and exits `0`.

### Examples

```bash
# Base64 (default, binary-safe).
little-db batch - < seed.ndjson

# Literal UTF-8 (text only).
little-db batch --plain - < users.10.ndjson

# From a path.
little-db batch ./testdata/users.1k.ndjson --plain
```

See [Recipes](#recipes) for converting an arbitrary JSON file into a
batch file.

---

## `stats`

Print engine stats to **stdout**.

### Synopsis

```
little-db stats [global flags]
```

### Output

One line, `key=value` style:

```
key_count=10000 bytes_on_disk=1142038
```

---

## `ping`

Health-check the server. Writes `pong` to **stdout** on success.

### Synopsis

```
little-db ping [global flags]
```

---

## Recipes

### Load a JSON file as one opaque value

```bash
little-db put users:snapshot --value-stdin < users.10.json
little-db get users:snapshot > restored.json
diff users.10.json restored.json
```

You can only `get` the whole blob back; the DB has no concept of
"sub-keys" inside a JSON value.

### Load a JSON array as one key per record

This is what you want if you'll later need `get user:7` or scan by
prefix. Convert with `jq`:

```bash
# users.10.json is an array of {id, name, email, ...} objects.
jq -c '.[]
       | {op:"put",
          key:("user:" + (.id|tostring)),
          value: tostring}' \
   users.10.json \
  | little-db batch --plain -
```

For arbitrary `{"k": "v", ...}` JSON (binary-safe, base64 default mode):

```bash
jq -r 'to_entries[]
       | {op:"put",
          key:(.key|@base64),
          value:(.value|tostring|@base64)}
       | @json' data.json \
  | little-db batch -
```

### Chunked import for files larger than 10 000 records or 64 MiB

```bash
split -l 5000 huge.ndjson chunk_
for f in chunk_*; do
  little-db batch --plain "$f" || { echo "failed at $f" >&2; break; }
done
```

### Quick throughput / latency probe

```bash
for n in 10 100 1k 10k; do
  echo "=== users.$n.ndjson ==="
  /usr/bin/time -p little-db batch --plain - < testdata/users.$n.ndjson
done
little-db stats
little-db range --start 'user:' --end 'user;' --format raw | wc -l
```

### Tail every request (server-side)

```bash
little-db serve --data-dir ./data --log-level debug --log-format json \
  2> >(jq -r 'select(.msg=="request") | "\(.time) \(.op) klen=\(.key_len) status=\(.status)"')
```

### Pretty-print a binary-safe range scan

```bash
little-db range --start 'user:' --end 'user;' \
  | jq -r '"\(.key|@base64d)\t\(.value|@base64d)"' \
  | column -t -s $'\t'
```
