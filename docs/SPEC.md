# little-db — Technical Specification

> **Status:** Draft 1 — written after batch 1 (core engine) is shipped and tested.
> **Audience:** the reviewer of this take-home assessment, and anyone debugging the engine later.
> **This document is the source of truth for scope.** If the code disagrees with this doc, one of them is wrong; flag it.

---

## 1. Product vision

Build a network-available, persistent key/value storage engine that a database
management system could plug in as its underlying storage layer. The engine
must be correct under crashes, predictable under load, and capable of holding
datasets much larger than RAM without degrading.

The vision is **operational simplicity**: one binary, one data directory, one
configuration knob group, one network port. A database engineer should be able
to read the entire codebase in an afternoon and know exactly what happens to
their bytes between the API call and the platter.

## 2. Goals (success criteria, in priority order)

| #            | Goal                              | Measurable target                                                                                                                   | How we verify                                                            |
| ------------ | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| G1           | Correctness under crash           | Every acknowledged `Put` survives `kill -9` and reopens; no torn record is ever surfaced as data                                    | `TestReq1_CrashCorrectness`                                              |
| G2           | Low per-op latency                | p99 `Get` < 1 ms, p99 `Put` < 5 ms (sync mode), with a warm 1M-key dataset on a developer laptop                                    | `TestReq2_LatencyP99`                                                    |
| G3           | High throughput for random writes | ≥ 50k `Put`/sec async, ≥ 10k `Put`/sec sync, single node, 256-byte values                                                           | `TestReq3_ThroughputAsync` + `TestReq3_ThroughputSync`                   |
| G4           | Dataset > RAM                     | Write 4× system RAM of data, then random `Get` across the full keyspace at < 2× the warm p99                                        | `TestReq4_DatasetLargerThanRAM` (heavy-only)                             |
| G5           | Predictable under load            | p99 stays ≤ 10 ms (Get) / ≤ 50 ms (Put) during a sustained mixed workload (70/20/10 read/write/delete); SPEC-scale run is 5 minutes | `TestReq5_PredictableUnderLoad`                                          |
| G6           | API surface complete              | All five required operations behave per §4                                                                                          | `TestReq6_APISurfaceComplete`                                            |
| G7           | Network-available                 | Engine reachable over TCP from a separate process                                                                                   | `TestReq7_NetworkAvailable`                                              |
| G8 _(bonus)_ | Replicates to followers           | Single-leader async replication; manual failover documented                                                                         | `TestReq8_ReplicationBonus` — **deferred to `bonus/replication` branch** |

## 3. Scope

### 3.1 In scope

- Bitcask-style append-only storage engine ([Sheehy & Smith, 2010](https://riak.com/assets/bitcask-intro.pdf)).
- In-memory keydir, on-disk segments, CRC32C-protected records.
- Background compaction with hint files.
- Crash recovery on `Open`.
- The five required API operations: `Put`, `Read`, `ReadKeyRange`, `BatchPut`, `Delete`.
- TCP server with a custom length-prefixed binary protocol.
- A minimal CLI client.
- Async single-leader replication — **planned bonus branch only**; integration point documented here, not implemented on `main`.

### 3.2 Out of scope

| Excluded                               | Why                                                                                                |
| -------------------------------------- | -------------------------------------------------------------------------------------------------- |
| LSM storage engine                     | Higher code volume, worse read latency, no clear win for this workload. See §10.                   |
| B+ tree                                | Random writes = random I/O. Fails G3.                                                              |
| Synchronous Raft replication           | ~2000 LOC stdlib-only; risk to submission completeness. We document the integration point instead. |
| Authentication, TLS                    | Take-home scope; would obscure the engine focus. Documented as a follow-up.                        |
| Multi-tenant isolation, quotas         | As above.                                                                                          |
| Secondary indexes, transactions, MVCC  | Beyond a KV storage engine's contract.                                                             |
| Cluster discovery / dynamic membership | Replication is statically configured.                                                              |
| HTTP/REST or gRPC interface            | Custom binary is smaller, faster, fewer dependencies. CLI wraps it.                                |

## 4. Functional requirements

Precise semantics for each operation. Where the assessment is ambiguous, we
**state our chosen interpretation explicitly** and flag the question in §11.

### 4.1 `Put(key, value) → error`

- Replaces any existing value for `key`.
- `key` must be non-empty and ≤ 64 KiB. `value` may be empty and must be ≤ 16 MiB.
- Returns only after the record is appended to the active segment **and visible
  to in-process readers**. Durability across crashes depends on `SyncOnPut`:
  - `SyncOnPut=true`: returns only after `fsync` (on Darwin, `F_FULLFSYNC`).
  - `SyncOnPut=false` (default): returns after kernel page-cache write. May be
    lost on host crash.

### 4.2 `Read(key) → (value, error)`

- Returns `ErrKeyNotFound` if the key has no live value (never written,
  deleted, or value lost to corruption).
- Returns a freshly-allocated `[]byte`; the caller owns it.
- One in-memory map lookup + one positioned read (`pread`) on the value's
  segment file. No seeks on the hot path.

### 4.3 `ReadKeyRange(startKey, endKey) → (iterator, error)`

- **Interpretation:** `startKey` inclusive, `endKey` exclusive (Go slice semantics).
- Returns keys in **ascending byte-lexicographic order**.
- Snapshot consistency: the iterator sees the keyspace at the moment of the
  call. Concurrent writes after that moment are not visible to the iterator.
- **Open question** — see §11/Q5: should this be a slice, a callback, or a
  channel? Currently planned as a callback (`func(key, value []byte) error`)
  to avoid materializing all results in memory.

### 4.4 `BatchPut(entries []BatchEntry) → error`

Where `BatchEntry` is:

```go
type BatchEntry struct {
    Key    []byte
    Value  []byte
    Delete bool // true = tombstone; Value is ignored
}
```

**Whole-batch atomicity** (confirmed by reviewer, 2026-05-26).

A batch is encoded as a **single physical "fat record"** rather than N individual
records:

```
| crc32c (4) | tstamp (8) | flag=BATCH (1) | key_len=0 (4) | val_len=body_size (4) | count (4) | [op_flag|k_len|v_len|key|value] × count |
   ^^^^^^^^^^^^^^^^^^^^^^^^^ shared 21-byte header ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^^ body (val_len bytes) ^^^^^^^^^^^^^^^^^^^^^
```

The header is identical in shape to PUT/TOMBSTONE; `key_len=0` and
`val_len=body_size` are the on-disk markers that the body is a batch payload
rather than a key+value pair. See §7.1 for the exhaustive record table.

- One CRC32C covers the entire batch payload. On recovery the batch is
  treated like any other record: a tail batch whose body extends past EOF
  is silently truncated away (the all-or-nothing crash-atomicity contract
  in action), but a CRC mismatch on a batch that fits entirely within the
  segment is treated as real corruption and aborts `Open` — we refuse to
  silently drop data on mid-file corruption. In-process visibility is
  atomic at snapshot granularity: every entry is published into the
  keydir under a single write lock, so a `ReadKeyRange` (which takes one
  RLock for the whole scan) observes either none or all of the batch.
  A caller-side loop of individual `Get` calls is not a snapshot — use
  `ReadKeyRange` for cross-key atomic reads.
- Mixed PUT + DELETE in the same batch is allowed via the inner `op_flag` byte
  (0 = PUT, 1 = TOMBSTONE).
- Single fsync at the end of the batch in sync mode (group commit).
- Per-entry validation up front: empty key, oversize key, oversize value,
  or a batch whose encoded size exceeds `Options.MaxBatchEncodedSize` /
  whose entry count exceeds `maxBatchEntries` all fail the entire call
  with no partial application. PUT and DELETE entries may be mixed
  freely; tombstones carry `Delete=true` and ignore `Value`.
- **Segment-overflow handling:** if the encoded batch would not fit in the
  active segment's remaining capacity, the active segment is rotated _first_
  and the batch is written contiguously into the fresh segment. A batch that
  exceeds `MaxSegmentSize` produces a single oversized segment — segment caps
  are advisory at batch boundaries, hard otherwise.
- **Trade-off:** individual records inside a batch are not independently
  checksummed. A single bit-flip anywhere in the body of a fully-written
  batch causes the outer CRC to fail; per the engine-wide policy this is
  mid-file corruption and aborts `Open` (the operator decides how to
  proceed). Only a _torn tail_ batch — one whose body extends past the
  segment's EOF — is silently truncated. This is the price of whole-batch
  atomicity. See §12.

### 4.5 `Delete(key) → error`

- Appends a tombstone record. Idempotent.
- Returns `nil` even if the key did not exist (no read-before-delete).
- Tombstones are reclaimed by compaction (§7.3) after a grace period.

## 5. Non-functional requirements

| ID  | Requirement           | Strategy                                                                                             |
| --- | --------------------- | ---------------------------------------------------------------------------------------------------- |
| NF1 | Low latency           | Append-only writes (sequential I/O), one-pread reads, no random seeks                                |
| NF2 | High write throughput | Single writer + group commit; one fsync per batch, not per record                                    |
| NF3 | Dataset > RAM         | Only the keydir is in RAM. Values live on disk. Keydir entry ≈ 32–48 bytes/key                       |
| NF4 | Crash recovery        | Append-only log + CRC32C per record + recovery scan on `Open`                                        |
| NF5 | Predictability        | Compaction runs in a background goroutine, never blocks writes                                       |
| NF6 | Observability         | `/health` and `/stats` over the TCP protocol; structured boot-time log line of resolved config       |
| NF7 | Operability           | Single static binary, single data directory, env-var-driven config with Joi-style validation at boot |

## 6. Architecture overview

```
                ┌─────────────────────────────────────────────────────────┐
                │                     TCP server                          │
                │  (net.Listen, one goroutine per connection)             │
                └────────────────┬─────────────────────┬──────────────────┘
                                 │                     │
                 ┌───────────────▼──────────┐  ┌───────▼──────────────┐
                 │   write requests         │  │   read requests       │
                 │   (Put/Delete/BatchPut)  │  │   (Read/Range)        │
                 └───────────────┬──────────┘  └───────┬──────────────┘
                                 │                     │
                                 ▼                     ▼
                 ┌────────────────────────────────────────────────────────┐
                 │                       DB                               │
                 │  ┌──────────────┐    ┌──────────────────────────┐      │
                 │  │   keydir     │◄───┤  writer goroutine        │      │
                 │  │ map+RWMutex  │    │  - encodes record        │      │
                 │  └──────┬───────┘    │  - appends to active seg │      │
                 │         │            │  - group-commit fsync    │      │
                 │         │            └──────────┬───────────────┘      │
                 │         │                       │                      │
                 │         │            ┌──────────▼───────────────┐      │
                 │         └───────────►│  segment files (disk)    │      │
                 │            pread     │  active.seg + N immut.   │      │
                 │                      └──────────┬───────────────┘      │
                 │                                 │ scan & merge          │
                 │                      ┌──────────▼───────────────┐      │
                 │                      │  compactor goroutine     │      │
                 │                      │  emits new seg+hint file │      │
                 │                      └──────────────────────────┘      │
                 └────────────────────────────────────────────────────────┘
                                                │
                                                ▼ (bonus, async)
                                        ┌──────────────┐
                                        │  follower(s) │
                                        └──────────────┘
```

### 6.1 Process model

- **Main goroutine** — owns the TCP listener; spawns per-connection handlers.
- **Per-connection handler** — reads requests, dispatches to engine, writes
  responses. Reads call into `DB.Get`/`DB.ReadKeyRange` directly (lock-free
  on the hot path).
- **Writer goroutine** _(batch 3 onward)_ — single owner of the active
  segment's append path. Drains a `chan writeRequest` and group-commits.
- **Compactor goroutine** _(batch 4)_ — wakes on a schedule or when dead-byte
  ratio crosses a threshold; merges old segments into one.
- **Replication goroutine** _(batch 8, bonus)_ — tails the local write stream
  and pushes to followers.

## 7. Data model and on-disk format

### 7.1 Record format (little-endian)

Every record on disk starts with the same 21-byte header:

```
| crc32c (4) | tstamp_ns (8) | flag (1) | key_len (4) | val_len (4) |
```

`CRC32C` covers every byte after itself (the header tail + the body). The
`flag` byte distinguishes record kinds; `key_len` and `val_len` describe the
body layout. Any other value of `flag`, or `key_len` / `val_len` outside the
per-kind bounds below, is treated as corruption by the recovery scanner.

| `flag` | Kind      | Body layout                                         | `key_len` / `val_len` constraints                                                                     |
| ------ | --------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `0`    | PUT       | `key` (key_len bytes) followed by `value` (val_len) | `1 <= key_len <= 64 KiB`; `0 <= val_len <= 16 MiB`                                                    |
| `1`    | TOMBSTONE | `key` (key_len bytes); no value                     | `1 <= key_len <= 64 KiB`; `val_len == 0`                                                              |
| `2`    | BATCH     | `count` (uint32) then `count` entries (see below)   | `key_len == 0`; `4 <= val_len <= 1 GiB` (`maxBatchBodyLen`); `count <= 1,048,576` (`maxBatchEntries`) |

For a BATCH record the body is a length-prefixed list:

```
body = | count (4) | entry × count |
entry = | inner_flag (1) | k_len (4) | v_len (4) | key (k_len) | value (v_len) |
```

where `inner_flag` is `0` (PUT) or `1` (TOMBSTONE); tombstone entries have
`v_len == 0` and no value bytes. The outer `val_len` in the BATCH header is
the total size of the body (count prefix + all entries), which is what the
recovery scanner uses to advance to the next record.

Sharing the 21-byte header across all three kinds keeps the scanner trivial:
read 21 bytes, branch on `flag`, read `val_len` body bytes, verify CRC. The
runtime cap `Options.MaxBatchEncodedSize` (default 64 MiB) is the _runtime_
ceiling enforced by `BatchPut`; `maxBatchBodyLen` (1 GiB) and
`maxBatchEntries` (~1M) are the _static_ sanity caps that bound recovery's
allocation against a crafted/corrupt header with a valid CRC.

### 7.2 Keydir entry (in-memory)

```go
type keydirEntry struct {
    fileID   uint32  // segment id
    valuePos int64   // byte offset of the *value* (not the record)
    valueLen uint32
    tstamp   int64   // for tie-breaking during recovery
}
```

Approximate memory cost: 24 bytes per entry + the key (Go map overhead
adds another ~24 bytes amortized). At 100M keys with 32-byte average key
length, expect ~8 GiB keydir.

### 7.3 Segment lifecycle

```
[active.seg] ──(rotates at MaxSegmentSize)──► [immutable_NNN.seg]
                                                        │
                                                        ▼
                                              [compactor merges N
                                               immutable segs into one
                                               new seg + hint file]
                                                        │
                                                        ▼
                                              old segs deleted after
                                               the new manifest is fsynced
```

### 7.4 Hint files _(batch 4)_

Sidecar files emitted by the compactor (and by future rotation-time emitters).
Every hint file is self-verifying: a torn or corrupted hint is rejected at
`Open` and recovery falls back to a full data-scan of the corresponding
segment. A hint that passes verification is treated as **trusted complete
metadata** for its segment.

Layout (little-endian):

```
header  | magic (4) = "HINT" | version (2) | reserved (2) |
entry   | key_len (4) | val_len (4) | value_pos (8 int64) | tstamp (8) | key |
footer  | entry_count (4) | crc32c (4) |
```

- `value_pos < 0` (sentinel `-1`) marks a TOMBSTONE entry; `val_len == 0`
  with `value_pos >= 0` is a legitimate empty-value PUT.
- The CRC32C (Castagnoli polynomial) covers every byte from offset 0
  through the end of `entry_count`, i.e. everything except the trailing
  4-byte CRC field itself.
- `entry_count` must match the number of entries actually parsed; a
  mismatch is treated as corruption.

On `Open`, for any segment other than the manifest-named active, recovery
reads the hint file when present and skips the data-bytes scan when the
file verifies. The active segment is always data-scanned to detect torn
tails; hints attached to it are ignored. (After compaction the merged
immutable segment can have a strictly higher id than the active, so
"non-active" is not the same as "non-newest".) Recovery time becomes
O(keys), not O(bytes-on-disk).

### 7.5 Manifest file _(batch 4)_

A small JSON file recording which segment IDs are live and which one is
the writer's active segment. Atomically swapped via `os.Rename` after
every rotation and after every compaction. Recovers from a crash
mid-compaction by trusting only the manifest.

Schema (v1):

```
{ "version": 1, "segments": [0, 2, 5, 6], "active": 5 }
```

`active` is required and must appear in `segments`. The engine refuses
to start on a manifest that omits it. The active id is not derivable
from `max(segments)` because compaction publishes an immutable merged
segment whose id is strictly greater than the active id; without an
explicit active marker, recovery would torn-tail-truncate the merged
segment and Open would promote it to the writer's append target,
invalidating its hint file on the next rotation.

## 8. Concurrency model

- **Writes are serialized** through a single writer goroutine. The active
  segment is touched by exactly one goroutine; no contention.
- **Reads are concurrent.** They take `keydir.RWMutex` for the lookup and
  then `pread` the value from the segment file. `ReadAt` is safe for
  concurrent callers (POSIX `pread`).
- **Compaction does not block writes.** It reads immutable segments, builds
  a new segment, then atomically swaps the manifest under a brief lock.
- **Replication is the planned integration point on the write path.** When
  shipped on the bonus branch, the writer pushes encoded records to a
  buffered channel consumed by a replication goroutine; backpressure on a
  slow follower does not stall local writes (records dropped from the tail
  buffer would increment a `replication_lag_dropped` counter). Not present
  on `main`.

## 9. Failure model

What we promise under each failure mode:

| Failure                                   | Guarantee                                                                                                                                                                                                                                                                                            |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Process `kill -9` mid-write               | All acked writes survive. A torn trailing record (header or body extending past EOF) is truncated away on recovery; its bytes were never acked. A mid-file CRC mismatch — i.e. on a record that fits entirely within the segment — aborts `Open` (see below).                                        |
| OS crash (sync mode)                      | All acked writes survive, modulo bugs in the OS's `fsync` implementation. On macOS we use `F_FULLFSYNC`.                                                                                                                                                                                             |
| OS crash (async mode)                     | Acked writes may be lost. Documented and configurable.                                                                                                                                                                                                                                               |
| Disk full                                 | Current write fails with the underlying I/O error. Engine remains usable for reads.                                                                                                                                                                                                                  |
| Power loss with disabled disk-cache flush | Out of our hands; documented.                                                                                                                                                                                                                                                                        |
| Single segment file corruption            | A trailing torn write is truncated away on recovery (its writes were never acked). A CRC mismatch on a record that fits entirely within the segment file aborts `Open` rather than silently dropping data — the operator decides whether to repair, restore, or run with `AllowCorruption` (future). |
| Keydir exceeds RAM                        | Engine OOMs. Bitcask trade-off; documented. Mitigation in §10.                                                                                                                                                                                                                                       |
| Process crashes mid-compaction            | Old segments still present, new segment partially written. Manifest still points to old set. On restart, the partial new segment is detected and deleted.                                                                                                                                            |

## 10. Assumptions

These are the things we are building **on top of** without verifying them
within the engine itself. If any of these is wrong, the engine will misbehave.

| #   | Assumption                                                                                                                                                                                                                                                     | Source / Justification                               | Consequence if wrong                                                                                  |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| A1  | **Resolved 2026-05-26**: stdlib strict. `golang.org/x/sys` is excluded; only packages under `std` (including `syscall`) are allowed. `F_FULLFSYNC` on darwin is invoked via `syscall.Syscall(syscall.SYS_FCNTL, fd, 51, 0)` with the constant inlined.         | Recruiter response to Q1                             | n/a — design adjusted                                                                                 |
| A2  | Total keyspace fits in RAM (keydir bound)                                                                                                                                                                                                                      | Bitcask's fundamental trade-off                      | At ~32-byte keys, ≥ 100M keys per node is feasible. For more, we'd need on-disk index — out of scope. |
| A3  | A single disk / single mount point per DB instance                                                                                                                                                                                                             | Common storage engine assumption                     | RAID / multi-disk is OS-level, not our concern                                                        |
| A4  | Values fit in 16 MiB                                                                                                                                                                                                                                           | Bitcask assumption; matches RocksDB's default        | Long-running calls if a 16 MiB value is fsynced                                                       |
| A5  | Reviewer will run benchmarks on macOS / Linux with SSD                                                                                                                                                                                                         | Assessment is silent on hardware                     | Numbers may not match on HDD or constrained VMs                                                       |
| A6  | The keydir's memory cost is acceptable for the target workload                                                                                                                                                                                                 | Implicit in choosing Bitcask                         | If the reviewer's mental model is "10B keys per node", we lose                                        |
| A7  | "Network-available" means TCP, not HTTP / gRPC                                                                                                                                                                                                                 | Assessment says interfaces, not protocols            | Reviewer expecting HTTP could be surprised. CLI mitigates.                                            |
| A8  | The assessment's terse "BatchPut(..keys, ..values)" wording is shorthand for "one network call that applies N operations atomically" — we surface it as `BatchPut(entries []BatchEntry)` so mixed PUT+DELETE batches are expressible in the same atomic record | Assessment is terse on the signature                 | We define the concrete API in §4.4.                                                                   |
| A9  | "Crash friendliness" implies single-record-level atomicity, not multi-record transactions                                                                                                                                                                      | Bitcask provides this; classical KV stores stop here | A reviewer expecting transactions would mark down. We mitigate via the BatchPut atomicity statement.  |
| A10 | Replication is **bonus**, not graded as a primary requirement                                                                                                                                                                                                  | Assessment lists it under "Bonus points"             | We deprioritize accordingly                                                                           |

## 11. Open questions for the reviewer

This is the most important section. We cannot ask the reviewer mid-assessment,
so we are stating **our chosen default** for each and inviting them to mark down
on the specific default if they disagree.

| #   | Question                                                  | Status                                             | Decision                                                                                          |
| --- | --------------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Q1  | **Stdlib strictness** — is `golang.org/x/sys` acceptable? | ✅ **Answered 2026-05-26**: strict. `std` only.    | `F_FULLFSYNC` on darwin via `syscall.Syscall` with inlined constant.                              |
| Q2  | **`BatchPut` atomicity** — within-segment or whole-batch? | ✅ **Answered 2026-05-26**: whole-batch preferred. | Fat-record encoding (§4.4, §7.1). Single CRC covers whole batch.                                  |
| Q3  | **`ReadKeyRange` return shape**                           | ⬜ Left to us.                                     | Streaming callback (`func(k, v []byte) bool`) in-process; streamed response frames over the wire. |
| Q4  | **Replication: async single-leader vs Raft**              | ⬜ Left to us.                                     | Async single-leader, manual failover. Raft integration point documented.                          |
| Q5  | **Benchmark dataset shape**                               | ⬜ Left to us.                                     | 1M / 10M keys, 256-byte values, uniform random + Zipf skewed workload.                            |
| —   | What is the default durability mode?                      | ⬜ Internal decision                               | `SyncOnPut=false` (async writes, throughput-optimized); `true` is opt-in.                         |
| —   | Network protocol: custom binary or Redis RESP?            | ⬜ Internal decision                               | Custom length-prefixed binary.                                                                    |
| —   | Tombstone retention: how long before reclaim?             | ⬜ Internal decision                               | One full compaction cycle.                                                                        |
| —   | Failover: manual or automatic?                            | ⬜ Internal decision (downstream of Q4)            | Manual.                                                                                           |
| —   | Authentication / TLS?                                     | ⬜ Internal decision                               | None (trusted network assumed). Out of scope; noted in README.                                    |
| —   | Dataset size for "much larger than RAM"?                  | ⬜ Internal decision                               | Benchmark up to 4× test machine RAM.                                                              |
| —   | Range scan ordering?                                      | ⬜ Internal decision                               | Byte-lex (`bytes.Compare`).                                                                       |

## 12. Trade-offs explicitly accepted

| Trade-off                                               | We choose                                           | We give up                                                                                                                                                                                                                             |
| ------------------------------------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sequential writes vs. in-place updates                  | Sequential append                                   | Disk space until compaction reclaims it                                                                                                                                                                                                |
| In-memory keydir                                        | O(1) point reads                                    | RAM grows linearly with key count                                                                                                                                                                                                      |
| `map[string]` keydir                                    | Simple, allocation-light                            | O(N) range scans — may upgrade to skiplist if benchmarks demand                                                                                                                                                                        |
| Single writer                                           | No contention, natural group commit                 | Cannot scale writes by adding cores                                                                                                                                                                                                    |
| Async replication                                       | Simple, never blocks local writes                   | Followers may lag; reads from followers may be stale                                                                                                                                                                                   |
| Custom binary protocol                                  | Compact, fast, explicit                             | No `redis-cli` compatibility                                                                                                                                                                                                           |
| 1-byte explicit tombstone flag                          | Self-documenting                                    | 1 byte more per record than Bitcask's sentinel-value-size approach                                                                                                                                                                     |
| CRC32C per record                                       | Hardware-accelerated, detects torn writes           | 4 bytes/record overhead                                                                                                                                                                                                                |
| BatchPut as a single fat record (whole-batch atomicity) | Required by reviewer; no two-phase commit           | Lose per-record CRC inside a batch — a single bit-flip in a fully-written batch aborts `Open` (mid-file CRC policy); a torn tail batch is silently truncated away. Either way: no partial-batch visibility, no two-phase commit logic. |
| Strict stdlib only                                      | No third-party dependency surface; clean `go build` | `F_FULLFSYNC` constant is inlined as `51` on darwin instead of imported from `x/sys/unix`                                                                                                                                              |

## 13. User and system flows

### 13.1 Put flow

```
Client                Server (handler)         Writer goroutine        Disk
  │   Put(k,v)            │                          │                  │
  ├──────────────────────►│                          │                  │
  │                       │  enqueue writeRequest    │                  │
  │                       ├─────────────────────────►│                  │
  │                       │                          │  encode record   │
  │                       │                          │  append to bufio │
  │                       │                          ├─────────────────►│
  │                       │                          │  flush bufio     │
  │                       │                          ├─────────────────►│
  │                       │                          │  (sync mode):    │
  │                       │                          │  fsync           │
  │                       │                          ├─────────────────►│
  │                       │                          │  update keydir   │
  │                       │  reply ok                │  signal done     │
  │                       │◄─────────────────────────┤                  │
  │   ok                  │                          │                  │
  │◄──────────────────────┤                          │                  │
```

### 13.2 Read flow

```
Client                Server (handler)         keydir            Segment file
  │   Read(k)             │                       │                   │
  ├──────────────────────►│                       │                   │
  │                       │  RLock + lookup       │                   │
  │                       ├──────────────────────►│                   │
  │                       │  entry{fileID, pos}   │                   │
  │                       │◄──────────────────────┤                   │
  │                       │   pread(pos, len)                          │
  │                       ├───────────────────────────────────────────►│
  │                       │   value bytes                              │
  │                       │◄───────────────────────────────────────────┤
  │   value               │                                            │
  │◄──────────────────────┤                                            │
```

### 13.3 Recovery flow (Open)

```
1. Acquire the data-dir flock (LOCK file) so two processes cannot share
   one DB. Done before any directory listing so a concurrent Open fails
   fast rather than racing against our recovery.
2. List segment files in the data dir, sort by id ascending. Read the
   MANIFEST to learn which segments are live and which one is the
   writer's active. If MANIFEST is missing (pre-4a fixture or freshly
   created dir), bootstrap-adopt every on-disk .seg file as live and
   pick `max(on-disk-id)` as the active — valid only because
   compaction cannot have run without a manifest. NOTE: "newest id" is
   NOT the same as "active". After compaction the merged immutable
   segment is published with a strictly higher id than the writer's
   active; recovery must use the manifest-named active, not max(id).
3. For each segment from oldest to newest:
   a. If the segment is NOT the manifest-named active AND a sibling
      hint file exists AND it verifies (magic, version, entry_count,
      CRC32C), apply the hinted entries to the keydir and skip the
      data-bytes scan. A hint that fails verification is logged and
      ignored — we fall through to (b). The active segment always
      falls through to (b) regardless of hint presence (its hint, if
      any, is stale by construction).
   b. Scan the data file record-by-record:
      - **Tail torn write on the active segment** (header or body
        extends past EOF): stop scanning this file and truncate to the
        last good record boundary. The torn bytes were never acked.
      - **Tail torn write on a non-active (sealed or merged) segment**:
        refuse to open. Sealed and merged segments were fsynced before
        manifest publish; a partial tail there is real corruption, not
        a torn write, and truncating would erase records previous Puts
        already returned success for.
      - **Mid-file CRC mismatch** (full record fits inside the file but
        checksum fails): abort `Open` with an error. We do not silently
        drop data on mid-file corruption.
      - **Bad flag / impossible lengths** mid-file: same — abort `Open`.
   c. For each valid PUT record, `keydir.putIfNewer(key, entry)`.
   d. For each valid tombstone, delete the key from the keydir only if its
      timestamp exceeds the existing entry's (stale tombstones are skipped).
   e. For each valid BATCH record, apply every inner entry in declared
      order via `keydir.applyBatch` — a torn tail batch is invisible per
      (b), a CRC-valid batch is published atomically.
4. Promote the manifest-named active as the writer's append target when
   it still has room; otherwise open a fresh active with id =
   max(on-disk-id) + 1. Using the on-disk maximum (not the manifest
   maximum) means orphan segment files left by a crashed rotation do
   not collide with the new active segment id.
5. If MANIFEST was bootstrapped or a fresh active was created, write
   the manifest with the resolved live set + active id BEFORE any Put
   is accepted.
6. Log resolved config (one line) — see NF7.
```

### 13.4 Compaction flow

Implemented in `internal/engine/compact.go`. Entry points are
`DB.Compact()` (synchronous, exposed for tests and operational use) and the
optional background loop spawned by `Open` when
`Options.CompactionInterval > 0`. Both call the same `compactOnce` path.

**Strategy.** Compaction inputs are _every immutable segment_. The active
segment is never compacted (it is still being appended to). This is the
simplest correct policy: it avoids per-segment dead-byte accounting and
makes tombstone reclamation trivially safe — a tombstone in an immutable
segment can only shadow a PUT in an older immutable segment, which is in
the same input set and has already been filtered out of the keydir, so
tombstones drop unconditionally.

**Algorithm.**

```
1. Snapshot immutable segments under segmentsMu.RLock; bail if <2.
2. Allocate merged segment id via nextID.Add(1) (shared atomic with the
   writer's rotateActive, so producers never collide).
3. Scan each candidate sequentially:
     - record is a tombstone → drop
     - record is a PUT and keydir entry exactly matches (segment, valuePos,
       valueLen) → re-encode into the merged segment, accumulate a
       (key, oldKeydir, newKeydir) rewrite
     - record is a BATCH → decompose, treat each live inner PUT as a
       standalone PUT in the merged segment (batch atomicity is not
       relevant post-write)
     - record is a PUT but keydir disagrees → drop (dead data)
4. If rewrites is empty (every candidate was already dead): unlink the
   merged segment file and submit a retire-only commit (newSeg = nil) so
   the candidates still come out of the manifest + segments map.
5. Otherwise: fsync merged segment → write + fsync hint sidecar →
   submit a commit (newSeg, rewrites, oldSegs) to the writer goroutine.
6. Writer's handleCompactCommit:
     a. Recompute newLive = current segments − candidates (+ merged if
        not nil) under RLock so segments born during compaction (e.g.
        rotations) are preserved.
     b. Write the manifest (atomic tmp → fsync → rename → fsync(dir)).
        This is the moment of commit; past this point the swap is durable.
     c. Under segmentsMu.Lock: publish merged segment, CAS each rewrite
        into keydir (CAS may fail if a fresher write landed for that key
        between scan and commit; that is correct — the newer write wins,
        the compacted copy becomes unreachable dead data, next pass
        reclaims), delete candidates from db.segments.
     d. Outside the lock: close + unlink candidate .seg and .hint files.
        Safe because no reader can hold a fresh pointer to them after
        the Lock (the Lock waited for in-flight RLocks to drain).
```

**Crash safety.** Crash points and their recovery behaviour:

| Crash point                                            | On restart                                                                                           |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Before merged segment fsync (step 5)                   | Merged segment is partial / absent; manifest unchanged. Cleanup on next Open's `sweepOrphans` pass.  |
| Between merged segment fsync and manifest write (5/6b) | Merged segment is durable but orphan; manifest still points to old candidates. Cleanup on next Open. |
| Between manifest write and candidate unlink (6c/6d)    | Manifest points to merged segment; old candidates are now orphans on disk. Cleanup on next Open.     |

**Orphan sweep.** `sweepOrphans` runs only at `Open`, while the DB is
still single-threaded (writer + compactor goroutines have not started).
Running it from the compactor goroutine would race with the writer's
`rotateActive`: `rotateActive` calls `createSegment(N)` (which lands the
new segment file on disk) and then `writeManifest`. A sweep that reads
the manifest between those two operations would see segment N as
unreferenced and unlink the file the writer is about to start using.
Open-only is the simplest race-free placement; the trade is that orphans
from a crashed compaction sit on disk until the next restart.

**Reader / compactor lifetime guarantees.** `Get` holds
`segmentsMu.RLock` across the keydir lookup so the keydir and segments
views are consistent with `handleCompactCommit`'s atomic swap.
`ReadKeyRange` goes further: it captures the keydir snapshot AND reads
every value into memory under a single `segmentsMu.RLock`, then releases
the lock before invoking any user callback. Holding the read lock across
all per-key preads is what guarantees true at-call snapshot semantics —
no compaction commit or rotation can change the segments map mid-scan,
so every snapshot entry is guaranteed to resolve to its exact bytes.
Peak memory during the call is O(sum of value sizes in range); this is
the deliberate trade for snapshot correctness without per-segment epoch
tracking.

**Compaction serialisation.** `compactOnce` takes `db.compactMu` for the
duration of a pass. Two concurrent compactions (e.g. a manual
`DB.Compact()` racing with the background loop) would otherwise each
snapshot the same candidate set, each produce a merged segment, and the
first to commit would unlink the candidates while the second was still
scanning them. The second caller is made to wait; with `<2` candidates
remaining after the first commit, the second pass becomes a no-op.

**Close vs. manual Compact.** A manual `DB.Compact()` call runs on the
caller's goroutine and is therefore not tracked by `compactorWG`. To stop
it from reading a closed segment fd, leaving a partial merged segment on
disk, or writing a hint file after the directory lock has been released,
`Close` acquires `compactMu` between `compactorWG.Wait()` and the
segment-close step, and releases it after `releaseLock()`. `compactOnce`
re-checks `db.closed` immediately after acquiring `compactMu`, so a
caller that slipped past the early closed-check in `DB.Compact()` but
had not yet acquired the mutex returns `ErrDBClosed` without touching
any state.

**Group-commit interaction.** `processBurst` tracks per-request whether
the request appended to the active segment (`appendedActive[i]`). Only
PUT / DELETE / BATCH requests set this flag; a compact-commit request
does not, so a fsync error on the active segment never poisons a
compact-commit reply (which would falsely tell the caller to roll back
an already-durable manifest swap).

**Configuration.**

| Option                            | Default | Meaning                                                                                                                                |
| --------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Options.CompactionInterval`      | `0`     | `0` disables background compaction. Positive → spawn `compactorLoop` that calls `compactOnce` on a tick. Negative is rejected at Open. |
| `compactionMinCandidates` (const) | `2`     | Below 2 immutable segments, a pass is a no-op.                                                                                         |

**Legacy outline.** The earlier high-level sketch was:

```
1. Trigger: either timer (every 60s) or dead-byte ratio threshold (> 40%).
2. Snapshot the set of immutable segments (active is never compacted live).
3. For each key live in the keydir whose entry points to a snapshot segment:
     append the live record to a new segment N+1
     emit hint-file entry
4. fsync new segment, fsync hint file.
5. Write a new manifest file (live-segments = {…, N+1, current_active}).
6. Atomic rename manifest → manifest.json.
7. Update in-memory keydir entries to point at N+1.
8. Delete the now-superseded immutable segments.
```

The dead-byte-ratio trigger and the 60 s default are not implemented:
batch 4c ships only the timer trigger (off by default) and the manual
`DB.Compact()` entry point. Choosing a default cadence and adding a
dead-byte trigger is deferred to a later operability pass.

## 14. Roadmap (implementation phases)

| Batch | Scope                                                             | Status                                           | Maps to requirements           |
| ----- | ----------------------------------------------------------------- | ------------------------------------------------ | ------------------------------ |
| 1     | Core engine: Put / Get / Delete, segment rotation, record codec   | ✅ done                                          | G2, G3 (partial), G6 (partial) |
| 2     | Recovery on Open + crash test + file lock + periodic fsync option | ✅ done                                          | G1, NF4                        |
| 3     | Writer goroutine + group commit + ReadKeyRange + BatchPut         | ✅ done                                          | G2, G3, G6                     |
| 4     | Compaction + hint files + manifest                                | ✅ done                                          | G4, G5, NF5                    |
| 5     | TCP server + wire protocol                                        | ✅ done                                          | G7                             |
| 6     | CLI client                                                        | ✅ done                                          | reviewer ergonomics            |
| 7     | Benchmarks + compliance test file                                 | ✅ done                                          | G2, G3, G4, G5 (proof)         |
| 8     | Async replication (bonus)                                         | ➖ deferred (planned `bonus/replication` branch) | G8                             |
| 9     | README + ops docs                                                 | ✅ done                                          | NF7, project completion        |

Core single-node flow is complete on `main`. Batch 8 is bonus scope and
intentionally lives on a separate branch so `main` ships without
half-finished cluster code on the hot path.

## 15. Verification plan

A reviewer should be able to run **one command** and see the full picture:

```
make verify
  ↳ go vet ./...
  ↳ go test ./... -race -count=1 -skip '^TestReq'   # correctness pass under race
  ↳ go test ./internal/engine -run TestReq -v       # SPEC §2 compliance, no race
```

`TestReq*` is intentionally excluded from the race pass and covered by
the compliance line: race instrumentation slows sync writes enough to
flake the SPEC §2 G3 perf floor (~1k Put/s margin). Compliance runs
once, without `-race`, so the floors mean what the SPEC says they mean.

`make verify` is the pass/fail gate. Benchmarks produce measurements
rather than booleans (no committed baseline to regress against), so
they run as a separate target:

```
make bench               # go test ./internal/engine -bench=. -benchmem -benchtime=3s
make compliance-heavy    # LITTLEDB_HEAVY=1 — full-scale 1M-key / 5-min workload + G4
```

Each of G1–G7 has a named test in `internal/engine/compliance_test.go`
(written in batch 7) that asserts the measurable target from §2. G8 is
the bonus; `TestReq8_ReplicationBonus` is present as a `t.Skip` marker
and exercised only on the `bonus/replication` branch.

## 16. Risks

| Risk                                                                             | Likelihood | Impact                                               | Mitigation                                                                                                               |
| -------------------------------------------------------------------------------- | ---------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Replication bonus consumes time the README needs                                 | Medium     | High — half-working replication is worse than none   | Replication is batch 8; if behind, ship documentation of the integration point and skip the code                         |
| Keydir RAM cost surprises the reviewer                                           | Medium     | Medium — perceived as a flaw rather than a trade-off | §3, §10/A2, §12 all flag it upfront; README has a sizing table                                                           |
| macOS `fsync` is a lie and the crash test passes on dev but real disks lose data | Medium     | High                                                 | Use `F_FULLFSYNC` on darwin when `SyncOnPut=true`; README calls this out                                                 |
| Compaction has a race that loses data                                            | Low        | Critical                                             | Manifest is the source of truth; old segments deleted only after manifest fsync. Tested by `TestCompactionPreservesData` |
| Range scan benchmark exposes O(N) keydir walk as too slow                        | Medium     | Medium                                               | Acceptable up to ~10M keys; upgrade path to skiplist is documented                                                       |
| Reviewer reads "stdlib only" strictly and dings us for `x/sys`                   | Low        | Low                                                  | Q2 in §11; we can hand-roll the syscall if needed                                                                        |
| GitHub repo is named anything other than what the reviewer expects               | Low        | Low                                                  | Repo name: `little-db`                                                                                                   |

## 17. Out-of-scope, listed loudly (for the README)

These are NOT bugs. They are explicit non-goals:

- No transactions across multiple keys (only batched atomic writes within a segment).
- No secondary indexes.
- No TTLs.
- No data encryption at rest.
- No multi-tenant isolation.
- No automatic failover.
- No dynamic cluster membership.
- No HTTP / REST / gRPC interface (CLI + TCP only).
- No Windows-specific code paths (POSIX assumptions throughout).

## 18. Glossary

- **Segment** — one append-only data file. Either _active_ (being written to)
  or _immutable_ (closed, read-only).
- **Keydir** — the in-memory index. One entry per live key.
- **Hint file** — sidecar to a segment carrying complete replay metadata for
  that segment: one entry per key the segment contributes (live PUTs and,
  when applicable, tombstones), encoding the keydir update without the
  value bytes. Lets recovery skip the value-bytes scan.
- **Manifest** — small file recording which segments are live. Authoritative
  source for the segment set after a crash.
- **Tombstone** — a record that marks a key as deleted.
- **Compaction / Merge** — background process that rewrites multiple immutable
  segments into one, dropping superseded and deleted records.
- **CRC32C** — Castagnoli-polynomial CRC, hardware-accelerated on arm64/amd64.

---

## Appendix A — Why not LSM

Direct comparison against the LSM paper (O'Neil et al., 1996):

| Dimension                  | LSM                                                 | Bitcask (us)                    |
| -------------------------- | --------------------------------------------------- | ------------------------------- |
| Random write throughput    | Excellent (memtable, periodic flush)                | Excellent (single append)       |
| Read latency               | Multiple SSTable probes; mitigated by bloom filters | One hash lookup + one pread     |
| Range scans                | Native (SSTables are sorted)                        | Requires sorted secondary index |
| Dataset > RAM              | Yes                                                 | Yes (keydir only)               |
| Crash recovery             | WAL + SSTable scan                                  | Data files _are_ the WAL        |
| Compaction stalls          | Risk of write stalls under L0 pressure              | Background, never blocks writes |
| Implementation LOC (rough) | ~3×                                                 | ~1×                             |

Bitcask wins on 4/5 stated requirements (G2, G4, G5 directly; G1 by simplicity).
LSM wins on native range scans. We close that gap with a sorted index option.

## Appendix B — Why not Raft for the bonus

Raft (Ongaro & Ousterhout, 2014) gives synchronous replication with automatic
failover. The integration point for Raft in our engine is **one line**:

```go
func (db *DB) appendRecord(rec *record) error {
    // ... encode ...
    // RAFT: replicate the encoded buffer to a majority of peers; block on quorum.
    // ... append to active segment ...
    // ... update keydir ...
}
```

That is the easy part. The hard parts — leader election, log matching,
snapshot installation, membership changes — are realistically 1500–2500 lines
of stdlib-only code with a non-trivial test surface. For a take-home, the
expected value of shipping a half-working Raft is negative.

Our plan: leave `main` single-node and complete; document the async
single-leader integration point above so a reviewer can see the
intended shape; ship the prototype itself on a separate
`bonus/replication` branch if scope allows. Raft is explicitly out.
