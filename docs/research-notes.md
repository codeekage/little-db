# Research notes

> Companion to [SPEC.md](SPEC.md) (what we built) and the replication design
> doc on the [`bonus/replication`](https://github.com/codeekage/little-db/blob/bonus/replication/docs/replication.md)
> branch. This document explains **why** the design looks the way it does —
> every major choice, the alternatives considered, and the reasons for
> the cut.
>
> Audience: a reviewer who wants to understand the engineering decisions
> without reading the code, and a future maintainer who wants to know
> what was deliberate vs. accidental.

---

## 0. Constraints that shaped everything

| Constraint                                | Consequence                                                                                                                                     |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Go 1.22 stdlib only — no third-party deps | No RocksDB, no embedded SQLite, no gRPC. Wire protocol, storage engine, and concurrency primitives are first-party.                             |
| Single-binary deployment                  | No background sidecars, no IPC, no shared-memory tricks.                                                                                        |
| Take-home time budget                     | "Do one thing very well, document the rest." Bias: depth over breadth, and explicit non-goals over hand-waving.                                 |
| Boring-correct over clever                | Prefer well-understood designs (Bitcask, single-writer, F_FULLFSYNC) and defend every scope cut in writing instead of inventing new primitives. |

These constraints are why the project does not use an LSM tree, a custom
network protocol-buffer dialect, or a goroutine-per-write model that
mutates shared storage state. Every "why didn't you do X" answer below
ultimately reduces to one of these four lines.

---

## 1. Storage engine: why Bitcask

### 1.1 Alternatives considered

| Option                        | Read latency  | Write throughput            | Recovery cost                                            | Implementation cost                                       | Fit for take-home                                                               |
| ----------------------------- | ------------- | --------------------------- | -------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------- |
| **Bitcask** (log + keydir)    | O(1) (1 seek) | very high (append)          | O(live keys / hint bytes) once the hint sidecar verifies | low                                                       | ✅                                                                              |
| LSM tree (LevelDB-style)      | O(log N)      | very high                   | O(WAL replay)                                            | high (compaction policy, bloom filters, level scheduling) | ❌ — months of work to get right                                                |
| B+tree                        | O(log N)      | moderate (in-place updates) | O(1) if WAL'd                                            | high (page management, split/merge)                       | ❌                                                                              |
| In-memory map + WAL (no segs) | O(1)          | high                        | O(WAL) — full replay                                     | trivial                                                   | ❌ — does not satisfy "data outlives process" at scale; recovery time unbounded |
| Sorted array + binary search  | O(log N)      | terrible (rewrite)          | O(file)                                                  | trivial                                                   | ❌                                                                              |

### 1.2 Why Bitcask wins for this scope

1. **The keydir-in-RAM, value-on-disk split is correctness-friendly.** A
   write is either fully in the keydir + on-disk, or it isn't. There is no
   "the index is ahead of the data" or "the data is ahead of the index"
   race that LSM trees and B+trees both have to engineer around.
2. **Recovery is bounded and observable.** With hint files (`§5` of SPEC),
   a clean restart is O(live keys / hint bytes) deserialisation —
   roughly the size of the keydir we need to rebuild — not O(record_count)
   scans of every segment. The cold-restart path is the one we have to
   defend in a take-home; Bitcask makes it short and provable.
3. **Append-only writes match the durability primitive we have.**
   `F_FULLFSYNC` on darwin is per-fd; an append + fsync of a single open
   file is the cleanest possible durability story. LSM compaction
   introduces multiple file fsyncs per commit that need careful ordering.
4. **Compaction is a well-understood background job, not a foreground
   concern.** Old segments are immutable. The merge produces one new
   immutable segment plus a manifest swap. No "level 0 stalls writes"
   pathology.
5. **It bounds memory.** RAM cost is proportional to live keys, not total
   writes. The user can reason about it from the schema, not from the
   workload.

### 1.3 Where Bitcask hurts (and we accept it)

- **Memory scales with key cardinality.** A 1 B key universe needs ~30 GiB
  of keydir even with the compact entry layout. Documented in SPEC §13 as
  the explicit upper bound; sharding is the answer, not a different
  engine.
- **Range scans are O(N log N).** The keydir is a hash map, not a sorted
  structure. Range queries are out of scope (§ non-goals); if needed, the
  fix is a secondary B-tree index over the keydir, not a new engine.
- **Tombstones live until compaction.** A delete-heavy workload bloats
  segments until the compactor runs. Mitigation: compaction is triggered
  by a dead-byte ratio, not a fixed schedule (SPEC §6).

---

## 2. Concurrency model: single writer, many readers

### 2.1 The choice

One writer goroutine owns the active segment + manifest mutations. Reads
use `segmentsMu.RLock()` plus a lock-free keydir lookup. PUT/DELETE/BATCH
requests are funnelled through a buffered channel to the writer.

### 2.2 Alternatives considered

| Option                               | Throughput          | Correctness story                                                   | Implementation cost |
| ------------------------------------ | ------------------- | ------------------------------------------------------------------- | ------------------- |
| **Single writer + buffered channel** | high (group commit) | trivial — no append-side race                                       | low                 |
| Goroutine-per-connection with mutex  | low                 | mutex contention dominates                                          | low                 |
| Sharded writers (one per key range)  | very high           | complicates compaction + manifest atomicity                         | high                |
| Optimistic / lock-free append        | very high           | requires CAS-based segment offset reservation + torn-write handling | very high           |

### 2.3 Why single-writer wins

- **Group commit is free.** The writer naturally accumulates a burst from
  the channel buffer, encodes it into one `write(2)` + one `fsync`, and
  replies to every requester. With `SyncOnPut=true`, p99 latency under
  load is dominated by _one_ fsync per burst, not one per request.
- **No append-side race.** The active segment's offset is mutated by
  exactly one goroutine. There is no "key A and key B both think they got
  offset 1024" pathology to defend.
- **Manifest mutations are serialised by construction.** Rotation and
  compaction-commit both run on the writer goroutine, so the manifest
  rename is never racing itself.
- **Cancellation is bounded.** `Close()` closes the request channel; the
  writer drains the buffer, replies to everyone, and exits. No "request in
  flight when the engine shut down" undefined behaviour.

### 2.4 Trade-offs we accept

- **Writes are single-threaded.** With `SyncOnPut=true` and a fast SSD
  this caps at ~10 k ops/s per engine. The remedy is sharding at the
  application layer (per-tenant DB), not parallel writers inside one
  engine. Sharding is mentioned as the scale-out story in SPEC §13.
- **The writer goroutine is a hotspot in profiles.** Acceptable: it is
  also the only place where data durability is decided, so concentrating
  work there is a correctness feature.

---

## 3. Durability: F_FULLFSYNC when sync durability is requested

### 3.1 The claim

Once `Put` returns `nil` with `SyncOnPut=true`, the value has reached the
storage medium's persistent domain. Not just the page cache, not just the
disk write cache — the platter, on darwin via `F_FULLFSYNC`.

### 3.2 Why this matters

`fsync(2)` on darwin only flushes to the disk's write cache by default.
Modern consumer SSDs and many enterprise drives lie about that cache
unless explicitly asked. Postgres got bitten by this badly enough in
2018 that they renamed the option `wal_sync_method = fsync_writethrough`.
SQLite uses `F_FULLFSYNC` on darwin for the same reason. We followed
their lead.

### 3.3 The cost

`F_FULLFSYNC` is ~3–10× slower than `fsync(2)` on a consumer SSD. With
group commit, this cost is amortised: 32 concurrent Puts pay for one
`F_FULLFSYNC` between them. Without group commit, p99 latency would be
catastrophic.

### 3.4 The portable fallback

On Linux and the BSDs, `fullSync` defers to `os.File.Sync()`, which in
Go's stdlib calls `syscall.Fsync` — plain `fsync(2)`. On commodity
filesystems (`ext4`, `xfs`) with default mount options, `fsync(2)` does
flush the drive write cache when the hardware honours its barrier, so
this is the strongest portable primitive we can reach from the stdlib.
The build tag `linux || freebsd || netbsd || openbsd` selects this
path; we did not pursue Linux-specific `sync_file_range` or `io_uring`
because the stdlib does not expose them and the project bans third-party
deps.

### 3.5 Group commit semantics

The writer accumulates up to N pending requests from `reqCh`, encodes
them, performs one write + one `F_FULLFSYNC`, and replies to all N. The
fsync cost is paid once per burst; throughput rises with concurrency
until the channel buffer saturates. See SPEC §3 for the exact ordering
guarantees.

---

## 4. Recovery: manifest + hint files

### 4.1 The shape

At `Open`:

1. Read the manifest (canonical live-segment set + active id).
2. For each **non-active** segment in the manifest, take the hint
   fast-path if its `.hint` sidecar exists and verifies (magic, version,
   `entry_count`, CRC32C); otherwise scan the `.seg` for records. The
   **active** segment is always data-scanned so a trailing torn write
   can be detected and truncated. (See SPEC §7.4.)
3. Reconcile with directory contents: orphaned `.seg` files (not in the
   manifest) are deleted; missing `.seg` files (in the manifest but not
   on disk) abort `Open` with a clear error.
4. Open the active segment for appends after its torn-tail trim.

### 4.2 Why a manifest at all?

Without one, recovery has to scan every `.seg` to know which records are
live. Scanning is O(total bytes), not O(live keys). On a 100 GiB database
with 1 % live data, that is two orders of magnitude wasted work at every
restart. Riak Bitcask documents the same fix.

### 4.3 Why hint files?

Hint files trade one inexpensive write (the hint sidecar) at compaction
time for a 10–100× faster cold start. The trade is well-documented in
SPEC §9 (data CRC is not re-checked when the hint fast-path is taken;
mid-segment corruption inside a hinted segment may become observable
when a later `Get` returns corrupted bytes — the engine does not
currently reverify the record CRC on `Get`). Strengthening hints with a
per-entry value digest is listed
as future work.

### 4.4 Atomic manifest swap

The manifest is replaced via `tmp → fsync(tmp) → rename → fsync(dir)`.
This is the standard idiom; we additionally introduced
`ErrManifestPublishedButUncertain` for the case where the rename
returned success but the directory fsync did not confirm (post-v0.1.0
review round 2–6). The engine enters a sticky write-disabled state in
that branch and preserves both old and new bytes on disk so the next
clean Open can converge. See SPEC §9 and the rotate / compact code
comments for the full reasoning.

---

## 5. Wire protocol: length-prefixed binary

### 5.1 The choice

A frame is `[4-byte big-endian length][1-byte opcode][payload]`. Opcodes
are a small enum. Status codes are a separate small enum. No headers, no
metadata, no extensions. Documented in SPEC §10.

### 5.2 Alternatives considered

| Option                                 | Why we rejected it                                                                                                                                                   |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| HTTP/JSON (REST)                       | Per-request HTTP overhead is ~30 % of total work for small values. JSON encode/decode is the bottleneck under load. Easy to reach for, wrong for a key-value engine. |
| gRPC                                   | Forbidden by the stdlib-only constraint. Also: protobuf-generated code is large, and the schema is overkill for 4 ops.                                               |
| Redis RESP                             | Tempting (RESP3 is compact and well-understood), but pulling in client compatibility means committing to a much larger API surface (~200 commands). Out of scope.    |
| Custom text protocol (memcached-style) | Easier to debug by hand, but parsing is slower and the size-prefix discipline matters more than human-readability for a binary KV.                                   |

### 5.3 Why length-prefixed binary

- **Trivially framed.** No state machine on the reader side; one `read(4)`
  gives you the rest of the frame size. Works the same in Go, Python, or
  `socat`.
- **Trivially benchmarked.** Encode/decode cost is a `binary.BigEndian`
  call. No reflection, no allocations beyond the payload buffer.
- **Frozen at v0.1.0.** Adding a field is a new opcode; the existing
  opcodes never change shape. This is a soft contract with future readers.
- **Explicit size cap (`MaxFramePayload` = 32 MiB).** Pathological clients
  cannot OOM the server with one frame, and the BATCH encoder (post-v0.1.0
  round 1 fix) checks the running size _before_ allocating.

### 5.4 What the protocol does NOT do

- **Mostly request-reply.** The base ops (`GET`, `PUT`, `DELETE`, `BATCH`)
  are exactly one request frame and one reply frame. `READKEYRANGE` is the
  one v0.1.0 exception: the server pushes a stream of range-page frames
  followed by an end-or-error terminator (see `internal/wire/response.go`'s
  `EncodeRangePage` / `EncodeRangeEnd` and SPEC §10). Replication on the
  bonus branch adds a second streaming path (`REPLICATE_RECORD` after a
  one-shot `REPLICATE_SUBSCRIBE`).
- No authentication. Trusted-network deployment. Documented in SPEC §10.
- No compression. Values are stored and transmitted as-is. A future
  compression opcode would be a separate frame format, not a flag on the
  existing one.

---

## 6. Observability: structured logs first, metrics later

### 6.1 The shape

Lifecycle events (`engine open`, `segment rotation`, `compaction start`,
`compaction done`, `manifest published`) are logged at `INFO` via
`log/slog`. Per-request work (encode, decode, append size, fsync
duration) is at `DEBUG`. Anomalies (`ErrManifestPublishedButUncertain`,
`writesDisabled`, hint mismatch, orphan sweep) are at `WARN` or `ERROR`
with structured fields.

### 6.2 Why not Prometheus / OpenTelemetry?

Both are out by the stdlib-only constraint. The honest answer is: in
production, the operator wires their log collector to whatever they
already run (Loki, Cloud Logging, Datadog). Structured logs are the
universal substrate; metrics are a derived view. We picked the substrate.

### 6.3 What's missing

- **No `/metrics` endpoint.** A follow-up would be a small endpoint
  emitting `expvar` JSON; gauges for keydir size, segment count, dead
  bytes, replication lag.
- **No latency histograms.** Bench output (`make bench`) gives p50/p99
  for write paths but the running server only exposes per-request
  duration in DEBUG logs.
- **No tracing.** Each request is single-step (decode → engine call →
  encode); a trace would not add information a log line does not already
  carry.

Env-tunable defaults (pool sizes, timeouts, batch caps) are logged once
at boot under a single structured log line, so any later "what was the
configured value?" question is grep-able from the incident's logs.

---

## 7. Replication (bonus branch)

This is covered in full in the design doc on the
[`bonus/replication`](https://github.com/codeekage/little-db/blob/bonus/replication/docs/replication.md)
branch. The relevant decisions for the design-rationale audience:

- **Async single-leader.** Strongest semantics achievable without
  consensus, which is out of scope.
- **No automatic failover.** Documented because "we couldn't" is a worse
  answer than "we chose not to and here's the runbook". Automatic
  failover without consensus is a split-brain machine; the only correct
  fix is Raft / Multi-Paxos / Viewstamped Replication, and that is a
  separate project.
- **Followers are read-capable but explicitly stale.** Clients that need
  fresh reads must talk to the leader. This is the same contract
  Postgres async streaming offers; not a defect.
- **Manual failover is operator-driven; the wire protocol carries the
  signal but does not perform the fence.** While a node runs as a
  follower it rejects writes with `FOLLOWER_READ_ONLY` (the freed status
  code from the v0.1.0 reservation). The `promote` CLI flips that node
  to leader and it then accepts writes — the wire signal does not
  reach the old leader at all. Fencing the old leader — ensuring it
  cannot keep accepting writes after promotion — is the operator’s job
  (kill the process, revoke network access, STONITH). The replication
  design doc is explicit that doing this in software without consensus
  is the classic source of split-brain.

---

## 8. What we explicitly did not build (and why)

| Feature                          | Why not                                                                                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Multi-tenancy / namespaces       | One engine per tenant is a sharper boundary than in-engine namespaces. Out of scope; the operator's problem.                                      |
| Sharding                         | Out of scope for a single-binary engine. The scale-out story is application-layer sharding (consistent hash, per-tenant DB).                      |
| Encryption at rest               | The OS already offers this (LUKS, FileVault, BitLocker). Re-implementing it inside the engine is a security risk, not a feature.                  |
| Backup tooling                   | `cp -a` over a paused engine is the documented procedure (`docs/ops.md`). A hot backup would require a snapshot opcode; that is a future project. |
| Snapshot bootstrap for followers | Listed in replication.md as the next phase. Requires a parallel transfer protocol; we ship the documented manual workaround instead.              |
| Range scans                      | Bitcask keydir is a hash, not a sorted structure. A secondary index is the right fix if needed; out of scope.                                     |
| TTL / expiration                 | Adds compaction complexity (timer-driven liveness in addition to overwrite-driven). Application-layer expiration is sufficient.                   |
| Multi-version / MVCC             | Out of scope. The engine has no transactions beyond BATCH atomicity (per SPEC §4).                                                                |

Every row is a deliberate cut. None is a defect.

---

## 9. References

The design is unoriginal by intent — every choice borrows from a
well-understood prior art. Reading list, in roughly the order a
reviewer would benefit from:

- **Riak Bitcask paper** — Sheehy & Smith, 2010. The keydir + hint
  file design we copied. <https://riak.com/assets/bitcask-intro.pdf>
- **LevelDB / RocksDB design notes** — explains the LSM-vs-Bitcask
  trade-off we did not take.
- **Postgres WAL durability discussion** — Bruce Momjian's
  `wal_sync_method` writeup is the canonical source on the
  fsync-vs-F_FULLFSYNC distinction we followed for `darwin`.
- **SQLite docs on F_FULLFSYNC** — independent confirmation from another
  embedded database that lived through the same disk-cache-lying problem.
- **Raft (Ongaro & Ousterhout, 2014)** — the consensus protocol we
  pointed at for the strong-semantics extension. <https://raft.github.io/>
- **Paxos Made Simple (Lamport, 2001)** — the alternative; chosen as a
  comparison point in replication.md §7.
- **Howard, "Distributed Consensus Revised" (2019)** — the modern
  unification; useful for the integration map.
- **EPaxos (Moraru et al., 2013)** — leaderless consensus; mentioned in
  replication.md to explain why we did not pick a leaderless approach
  even for the bonus.
- **Jepsen reports** (Kingsbury) — the right priors for what does and
  does not survive partitions. Internalised the "do not auto-failover
  without consensus" rule from these.
- **Designing Data-Intensive Applications (Kleppmann, 2017)** — the
  textbook reference for everything in §1, §3, §4, §7.

---

## 10. What "good" looks like for this codebase

A reviewer reading this document plus SPEC.md should be able to answer:

1. What guarantees does a successful `Put` give? (SPEC §3.)
2. What happens after `kill -9` of the server mid-write? (SPEC §9.)
3. What happens after `kill -9` of the host? (§3 above + SPEC §9.)
4. What is the recovery time bound? (§4 above.)
5. Why is the wire protocol shaped the way it is? (§5.)
6. What does the system not promise, and why? (§8.)
7. Where would consensus plug in? (replication design doc §7 on the
   [`bonus/replication`](https://github.com/codeekage/little-db/blob/bonus/replication/docs/replication.md)
   branch.)

If any of those answers feel under-specified after reading the docs, the
docs are the bug, not the code. Open an issue.
