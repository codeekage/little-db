# `scenarios/` — replication & failover demonstrations

Self-demonstration suite for the bonus replication work. **Build-tagged**
so the default test surface is untouched:

```sh
go test ./...                  # ignores everything in this package
```

Run the demos explicitly:

```sh
# 15 failure / edge scenarios (~5s total)
go test -tags replication_demo -v ./scenarios/...

# + 1M-record volume tests (V1, V2 — ~100s total, ~250 MiB per side)
go test -tags replication_demo,replication_demo_heavy -v ./scenarios/...

# Caller-selected record counts (useful for 5M, or any custom mix):
RUN_VOLUME_COUNTS=1000000,5000000,10000000 \
  go test -tags replication_demo,replication_demo_heavy \
    -run TestVolume_CustomRecordCounts -v -timeout 90m ./scenarios/...

# + 10M-record V3 (opt-in; ~10 min, ~2.5 GB per side)
RUN_10M=1 go test -tags replication_demo,replication_demo_heavy \
  -v -timeout 30m -run TestVolume_TenMillion ./scenarios/...
```

These tests are not part of the SPEC compliance surface (`internal/engine/compliance_test.go`).
They exist to exercise replication, failover, and durability under
adversarial conditions and at scale, to back the bonus work with
runnable evidence rather than prose.

## Failure scenarios

| # | Test | What it proves |
|---|------|----------------|
| 1 | `LeaderKilledAndReplaced` | Follower follows a new leader bound on the same port after the old one dies. |
| 2 | `ConnectionKilledMidRecord` | A TCP cut mid-record leaves the faulted follower internally consistent (no torn keydir, `Get` still safe); a clean follower against the same leader applies fully. Does **not** assert the faulted follower recovers the lost mid-record stream — v0.1 has no resume cursor; restart-from-tail is the supported recovery (see #7). |
| 3 | `LargeValuesReplicate` | 5 × 1 MiB values round-trip byte-identically. |
| 4 | `SlowFollowerCausesLeaderDrops` | Slow subscriber → leader drops via `ReplicationLagDropped`; writer never blocks. |
| 5 | `PromoteDuringWriteBurst` | PROMOTE mid-burst flips the gate cleanly; post-promote write succeeds. |
| 6 | `PromoteWhileLeaderDown` | Drain does not hang on a dead leader. |
| 7 | `FollowerRestartResumesFromTail` | A fresh follower starts at the live tail; writes during the gap window are not replayed (documented "no snapshot bootstrap"). |
| 8 | `TombstonesSurvivePromote` | DELETEs replicate; gone keys stay gone after PROMOTE. |
| 9 | `MixedBatchAtomicEndToEnd` | A PUT+DELETE batch lands atomically on the follower. |
| 10 | `SecondFollowerRunnerRejected` | The single-subscriber slot is enforced: a second runner applies zero records. |
| 11 | `SequentialPromotesIdempotent` | First PROMOTE OK, every subsequent PROMOTE → `BAD_REQUEST`. |
| 12 | `PromoteHookDeadlineThenRetry` | Hook timeout → `INTERNAL`; gate stays follower; retry succeeds. |
| 13 | `ReplicationSurvivesRotation` | 200 keys land on the follower across 8-KiB segment rotations; `KeyCount` parity. |
| 14 | `ReplicationUnaffectedByCompaction` | Leader compaction does not perturb follower state. |
| 15 | `RapidLeaderRestartCycles` | Follower survives 4 leader kill/rebind cycles on the same port. |

## Volume scenarios

| # | Test | Records | Result |
|---|------|---------|--------|
| V0 | `TestVolume_CustomRecordCounts` | caller-selected via `RUN_VOLUME_COUNTS=...` | runs `runVolume` for each count as a subtest |
| V1 | `TestVolume_OneMillion_PUT` | 1,000,000 PUT @ 256 B | full parity, 1000-key sample byte-equal |
| V2 | `TestVolume_OneMillion_PUT_with_DELETE` | 1,000,000 PUT + ~100,000 DELETE | end-state `KeyCount` exact match, 100 tombstones + 500 live samples verified |
| V3 | `TestVolume_TenMillion_PUT` | 10,000,000 PUT @ 256 B | opt-in (`RUN_10M=1`) |

Last observed throughput on M3 MacBook Pro: **~32,870 PUT/s** end-to-end
(client → leader → follower applied), with the follower fully caught up
on V1 the moment the writer finishes.
