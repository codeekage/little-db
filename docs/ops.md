# little-db operator runbook

Day-2 operations. For design rationale see [SPEC.md](SPEC.md); for the
quick-start see [../README.md](../README.md).

---

## 1. Deploy

little-db is a single static binary, a single data directory, and a
single TCP port.

```bash
# build
make build

# run
./bin/little-db serve \
  --addr 0.0.0.0:4242 \
  --data-dir /var/lib/little-db \
  --sync-on-put=true \
  --compaction-interval 1h
```

The process logs **one structured line at boot** summarising the
resolved engine options (max segment size, sync mode, write-queue depth,
recovered segments and keys). That line is searchable in any
post-incident log dump — grep `msg="engine open"` (text format) or
`"msg":"engine open"` (JSON, `--log-format=json`) is the answer to
"what config was actually running?".

Send `SIGINT` or `SIGTERM` for graceful shutdown. The server stops
accepting new connections, drains in-flight requests up to
`--shutdown-grace` (default 30s), then closes the engine (flushes the
active segment, releases the directory `flock`).

A second `SIGINT` during shutdown hard-kills the process — by design, so
an operator wedged behind a slow client can always get out.

---

## 2. Configuration reference

All knobs are CLI flags on `little-db serve`. Defaults are dev-laptop
sane; production should at minimum review `--sync-on-put`,
`--max-segment-size`, and `--compaction-interval`.

### Server / network

| Flag                             | Default          | Notes                                                |
| -------------------------------- | ---------------- | ---------------------------------------------------- |
| `--addr`                         | `127.0.0.1:4242` | Use `0.0.0.0:4242` to accept off-host connections.   |
| `--read-deadline`                | `30s`            | Per-request read timeout.                            |
| `--write-deadline`               | `30s`            | Per-response write timeout.                          |
| `--max-concurrent-range-streams` | `4`              | Server-wide cap; range streams are O(N) in keyspace. |
| `--max-range-response-bytes`     | `64 MiB`         | Per-range payload cap; over-cap returns `OVERLOAD`.  |
| `--shutdown-grace`               | `30s`            | Drain budget on SIGINT/SIGTERM.                      |

### Engine

| Flag                       | Default    | Notes                                                                            |
| -------------------------- | ---------- | -------------------------------------------------------------------------------- |
| `--data-dir`               | _required_ | One process per directory (enforced by `flock`).                                 |
| `--max-segment-size`       | `256 MiB`  | Rotation threshold. Smaller = more files, faster compaction; larger = fewer fds. |
| `--sync-on-put`            | `false`    | See [§3 Durability](#3-durability-and-fsync).                                    |
| `--write-queue-depth`      | `64`       | Backpressure depth in front of the single writer goroutine.                      |
| `--max-batch-encoded-size` | `64 MiB`   | Hard cap on a single `BatchPut` payload.                                         |
| `--compaction-interval`    | `0` (off)  | See [§4 Compaction](#4-compaction).                                              |

### Observability

| Flag           | Default | Notes                                                                                                        |
| -------------- | ------- | ------------------------------------------------------------------------------------------------------------ |
| `--log-level`  | `info`  | `debug` / `info` / `warn` / `error`. `debug` enables one log line per request (op, sizes, status, duration). |
| `--log-format` | `text`  | `text` or `json`. Logs go to stderr. JSON is the recommended format for shipping to a log collector.         |

Default (`info`/`text`) emits lifecycle events only: `engine open`,
`server listening`, `segment rotation`, `compaction start`/`compaction
done`, `server shutdown begin`/`server shutdown done`, `engine close`.
Per-request logging is gated by a precomputed `Logger.Enabled` check so
the hot path is a single boolean comparison when `debug` is off —
turning telemetry on costs at most one structured line per RPC; turning
it off costs nothing.

Example for piping into `jq`:

```sh
little-db serve --data-dir /var/lib/little-db \
  --log-level=debug --log-format=json 2>&1 \
  | jq 'select(.msg == "request" and .duration_us > 1000)'
```

---

## 3. Durability and fsync

`--sync-on-put=false` (default) — writes are appended to the active
segment and acknowledged after a single `write(2)` to the kernel.
Throughput: ~400k Put/sec on a 2024 laptop. A kernel crash or power
loss can lose anything still in the page cache.

`--sync-on-put=true` — every write is durable before its response is
returned. The writer batches concurrent in-flight Puts into one syscall
(group commit), so latency stays bounded even under load.

- **Linux**: `fsync(2)` on the segment file.
- **macOS**: `fcntl(F_FULLFSYNC)`. Plain `fsync` on darwin flushes only
  to the disk's volatile cache and silently returns success even when
  the data is still in the drive's RAM. This is the most common reason
  "crash-safe" stores pass their tests on a MacBook and lose writes on
  real hardware. We use `F_FULLFSYNC` and validate it end-to-end in
  `TestReq1_CrashCorrectness`, which `os.Exit(0)`s a subprocess
  mid-write and reads back from the parent.

If you set `--sync-on-put=true`, expect single-key Put latency in the
**3–10 ms p99** range on consumer SSDs (the cost is one round-trip to
hardware). The compliance gate measures this; see `make
compliance-heavy`.

---

## 4. Compaction

Bitcask-style stores accumulate dead bytes as keys are overwritten or
deleted. Compaction rewrites immutable segments into one, dropping
superseded records.

- `--compaction-interval 0` (default): never auto-compact. Suitable for
  short-lived workloads, tests, and append-mostly use cases.
- `--compaction-interval 1h` (suggested production): the background
  compactor wakes hourly and, whenever there are at least two immutable
  segments, merges **all** of them into one. The policy is deliberately
  simple — no live-byte ratio scoring, no tier selection. Compaction is
  single-threaded, off the hot path, and never blocks reads or writes.

The compactor is crash-safe: it writes the merged segment + hint file,
then atomically swaps the manifest. A crash mid-compaction leaves both
the old and new segments on disk; recovery picks whichever the manifest
points at, and the unreferenced one is GC'd at next boot.

There is no manual `compact` subcommand — by design, the only knob is
the interval. If you need to force a compaction in an incident,
temporarily lower the interval and restart.

---

## 5. Sizing the keydir

The keydir is in RAM. One entry per live key:

```
per-entry ≈ 64 bytes + len(key)
```

| Keys  | Avg key size | Approx resident |
| ----- | ------------ | --------------- |
| 1 M   | 32 B         | ~100 MiB        |
| 10 M  | 32 B         | ~1 GiB          |
| 100 M | 32 B         | ~10 GiB         |
| 10 M  | 128 B        | ~2 GiB          |

If your working set materially exceeds host RAM, this is the wrong
database — the storage engine is fine, the index isn't. See SPEC §3 and
§10/A2.

Value bytes are **not** held in the keydir. Reads `pread` from the
segment file on demand, served by the OS page cache for hot data.

---

## 6. Restart and recovery

On boot, the engine:

1. Acquires an exclusive `flock` on the data directory. Another process
   on the same directory fails fast at this point — by design.
2. Reads the manifest to determine the live segment set.
3. For each immutable segment, replays its **hint file** (a sidecar
   carrying one keydir-update record per live key in the segment). Hint
   replay is `O(keys)` and skips value bytes.
4. For the active segment (no hint file yet), tail-scans records to
   rebuild keydir entries from the segment itself.
5. Logs the one-line boot summary, then accepts connections.

Recovery is deterministic and idempotent. A partial trailing record
(truncated by a mid-write crash) is detected by length+CRC mismatch and
truncated cleanly; the engine resumes appending after it.

Time to recover scales with **live keys**, not bytes on disk, because
of the hint files. A 10 GiB directory with 1 M live keys recovers in
under a second on an NVMe SSD.

---

## 7. Observable state

| Surface           | What it tells you                                             |
| ----------------- | ------------------------------------------------------------- |
| Boot log line     | Resolved config — sync mode, queue depth, segments, keys.     |
| `little-db stats` | `KeyCount` (live keys) and `BytesOnDisk` (sum of live segs).  |
| `little-db ping`  | TCP round-trip + server liveness.                             |
| Process exit code | Non-zero means the engine couldn't open or shut down cleanly. |

`KeyCount` and `BytesOnDisk` are individually consistent but not
mutually atomic — under load they're a snapshot, not a transaction.
Good enough for capacity dashboards; not a billing source of truth.

---

## 8. Failure modes and operator responses

| Symptom                                                   | What it means                                                                       | Action                                                                                                   |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `serve: open engine: ... directory is locked`             | Another `little-db` process holds the data-dir flock.                               | Find and stop the other process. Never delete the lockfile by hand.                                      |
| Client gets `OVERLOAD` on `range`                         | Too many concurrent range streams **or** payload over `--max-range-response-bytes`. | Increase the cap if intentional; otherwise the client should narrow the range or back off.               |
| Client gets `BAD_REQUEST` on `put`                        | Empty key, oversized batch, or malformed wire frame.                                | Fix the caller. Empty key is a hard sentinel (`engine.ErrEmptyKey`).                                     |
| `BytesOnDisk` keeps growing, `KeyCount` flat              | Dead bytes accumulating — compaction is off or behind.                              | Set `--compaction-interval` (e.g. `1h`) and restart.                                                     |
| Sync-put latency suddenly 10× worse                       | Disk degraded, or another tenant saturated the device.                              | Check `iostat` / cloud disk metrics; little-db is one `fsync` per write, no hidden amplification.        |
| Process exits with non-zero on shutdown                   | `Close` failed to flush — usually a full disk.                                      | Inspect logs; the engine refuses to ack writes it couldn't flush, but a stuck `fsync` can wedge `Close`. |
| Boot is slow                                              | Active segment has no hint file and contains many records — tail scan.              | Expected once after a hard crash; subsequent boots replay hint files.                                    |
| Crash mid-write, some recent records "missing" on restart | Expected with `--sync-on-put=false`. The records were never durable.                | Use `--sync-on-put=true` if you need at-rest guarantees for those writes.                                |

---

## 9. Backup

little-db has no built-in backup. The on-disk format is stable enough
that a file-level copy of the data directory while the server is **stopped**
is a full backup. To back up a running server:

1. Pause writes at the application layer.
2. `little-db stats` to record the current `KeyCount` / `BytesOnDisk`.
3. `cp -R /var/lib/little-db /backup/...` (or `rsync`, or your snapshot
   tool of choice).
4. Resume writes.

Hot backup of a running server without write-pause is **not safe** —
the active segment file is being appended to and the manifest can
rotate underneath you. There is no WAL-shipping or snapshot endpoint on
`main`.

---

## 10. Out of scope

These are documented non-goals (SPEC §17), not bugs:

- No automatic failover.
- No encryption at rest.
- No TLS on the wire (deploy on a trusted network or in front of an
  encrypting proxy).
- No authentication.
- No multi-tenant isolation.
- No HTTP / REST / gRPC.
- No Windows.

Replication is **not implemented** on `main`; it is deferred to a
planned `bonus/replication` branch as an async single-leader design.
`main` is intentionally single-node.
