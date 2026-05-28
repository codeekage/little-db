# Replication

> Status: **bonus branch (`bonus/replication`)**. Not present on `main`.
> Implementation scope: async single-leader, manual failover.
> Out of scope by explicit choice: consensus, snapshot bootstrap, automatic failover.

This document is the design contract for the replication feature on the
`bonus/replication` branch. It explains what we built, what we deliberately
did not build, and what a production extension would look like — with
enough detail that a reviewer can audit the safety claims without reading
the code.

For the broader project design rationale (storage engine, wire protocol,
concurrency model, observability), see [research-notes.md](research-notes.md).
This document picks up where §8 of [SPEC.md](SPEC.md) left off.

---

## 1. Goals

In priority order. Lower-priority goals are sacrificed for higher ones when
they conflict.

1. **A second node holds a converging copy of the primary's data**, so a
   single-node outage does not lose data after recovery completes.
2. **Local write latency is not impacted by replication.** Slow followers
   never block the leader.
3. **Failure modes are explicit and operator-visible.** Lag, drops, and
   disconnections surface in logs and stats; nothing fails silently.
4. **Promotion of a follower to leader is a documented, single-command
   operation.** No config-file editing under pressure.
5. **The integration point for stronger semantics (consensus) is mapped
   out**, so the transition from async to Raft is bounded engineering work,
   not a redesign.

## 2. Non-goals (and why)

| Non-goal                                          | Why not                                                                                                                                                                                                       |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Synchronous quorum / linearizable writes**      | Requires consensus. See §7. We document the integration point and ship the safe small thing instead.                                                                                                          |
| **Automatic failover**                            | Cannot be correct without consensus (split-brain on partition). Manual failover with fencing is the safe and well-understood alternative; this is what Postgres async streaming replication ships by default. |
| **Snapshot bootstrap for late-joining followers** | Requires either a parallel snapshot transfer protocol or a retained leader WAL. Both are real engineering. Documented as the next phase.                                                                      |
| **Multi-follower fan-out / chain replication**    | Single follower is sufficient to validate the design; the leader's publish channel naturally supports multiple subscribers, but we don't ship the auth and ordering machinery to make it production-credible. |
| **Stream encryption / mutual auth**               | Replication runs on a trusted network. TLS termination is the operator's responsibility (stunnel, mTLS sidecar, WireGuard).                                                                                   |
| **Replicated reads with bounded staleness**       | Followers are read-capable but staleness is unbounded under leader load. Clients that need fresh reads must read from the leader. Documented in the client docs.                                              |

Every non-goal in this table is a deliberate scope cut. None of them are
defects.

## 3. Architecture

```
                ┌──────────────────────────────────────┐
                │  leader (engine writer goroutine)    │
                │                                      │
   PUT/DELETE/  │  1. encode record                    │
   BATCH ─────► │  2. append to active segment         │
                │  3. fsync (group commit)             │
                │  4. publish to replication channel ──┼──┐
                │  5. ACK client                       │  │
                │                                      │  │
                └──────────────────────────────────────┘  │
                                                          │ buffered chan
                                                          │ (drops on full)
                              ┌───────────────────────────┘
                              ▼
                ┌──────────────────────────────────────┐
                │  leader (replication server goroutine)│
                │                                      │
                │  per subscribed follower:            │
                │    1. read encoded record from chan  │
                │    2. wrap in REPLICATE_RECORD frame │
                │    3. write to follower TCP conn     │
                │       (write deadline applies)       │
                │                                      │
                └──────────────────────────────────────┘
                              ▲
                              │ TCP
                              │
                ┌──────────────────────────────────────┐
                │  follower (replication client)       │
                │                                      │
                │  1. dial leader, send                │
                │     REPLICATE_SUBSCRIBE              │
                │  2. read REPLICATE_RECORD frames     │
                │  3. apply to local engine            │
                │     (same code path as a local PUT,  │
                │      no replication channel)         │
                │  4. reject client writes with        │
                │     FOLLOWER_READ_ONLY               │
                │                                      │
                └──────────────────────────────────────┘
```

### 3.1 Integration with the existing write path

The single substantive change to the engine is **step 4**: after a record
is durably appended on the leader, a copy of its encoded bytes is published
to a buffered channel. The publish is non-blocking. If the channel is full,
the record is dropped from the tail of the buffer and a `replication_lag_dropped`
counter is incremented.

**This is the SPEC §8 integration point**, written exactly as it was
specified before any replication code existed. The frozen wire format
guarantees that an encoded record is self-describing — the follower can
decode and apply it without metadata from the leader's keydir.

### 3.2 Why the channel drops instead of blocking

Three reasons, in priority order:

1. **G2 (low write latency) outranks G8 (replication)** in the SPEC. A
   slow or disconnected follower must not stall local writes.
2. **A blocking channel turns a follower outage into a leader outage.**
   This is the operational hazard that retired pre-Raft Redis Sentinel
   in many production deployments.
3. **The drop is observable** (`replication_lag_dropped`, surfaced in
   `/stats` and structured logs). An operator looking at the leader sees
   "follower is unhealthy"; an operator looking at the follower sees
   "I am behind." Neither sees "everything is slow."

The cost: a slow follower can become permanently divergent if its lag
exceeds the buffer size. Re-syncing it requires snapshot bootstrap, which
we do not ship. The mitigation is operational: size the buffer based on
the leader's peak write rate and the follower's reconnect SLA, monitor
the counter, and if it ever increments, promote-and-rebuild rather than
trust the follower.

## 4. Wire protocol extensions

Two new opcodes, in a reserved range that does not collide with `main`'s
frozen ops. See [SPEC.md §5](SPEC.md) for the framing model that the
following extends.

### 4.1 `REPLICATE_SUBSCRIBE` (follower → leader)

```
+--------+-----------------+-------------------+
| op (1) | u32 tag_len     | resume_tag (N)    |
+--------+-----------------+-------------------+
```

The resume-tag is currently always empty (the follower starts streaming
from "now", so `tag_len = 0` and the `resume_tag` field is zero bytes).
The field is present in the wire so a future snapshot-bootstrap
implementation does not require a wire protocol revision: the tag would
carry an opaque cursor that the leader can interpret. `tag_len` is
bounded by `wire.MaxResumeTagLen` (64 KiB).

**Server response**: a stream of `REPLICATE_RECORD` frames until the
connection closes. There is no terminating frame; subscription is
indefinite.

### 4.2 `REPLICATE_RECORD` (leader → follower)

```
+--------+--------------------+
| op (1) | encoded record (N) |
+--------+--------------------+
```

The encoded record is the _exact_ byte sequence the leader appended to its
own segment, including the CRC. This is a deliberate choice: the follower's
apply path validates the CRC again, so any bit flip in transit fails closed
rather than being silently applied.

### 4.3 `FOLLOWER_READ_ONLY` status code

Returned by a follower in response to any write op (PUT/DELETE/BATCH). The
error message includes the leader address (from `--replica-of`) so clients
have a hint about where to retry.

## 5. The `promote` command

Failover is operator-driven. The follower has one new subcommand:

```bash
little-db promote --addr <follower-addr>
```

Atomically:

1. Closes the replication client connection to the (dead) leader.
2. Drops follower-mode; the engine starts accepting writes.
3. Logs `promotion completed at <timestamp>` at `INFO` level.
4. Exits success.

The command is **idempotent**: running it on a node that is already a
leader is a no-op success.

The command **does not fence the old leader**. Fencing is the operator's
responsibility (typically a firewall rule on the leader's TCP port, or
killing the process). This is a manual step in the runbook precisely
because doing it wrong is the classic source of split-brain.

## 6. Failure model

What we promise under each failure mode. "Lost" means the leader returned
OK to a client and the data is not present on the follower.

| Failure                                                       | Leader behavior                                                | Follower behavior                                                                   | Lost data?                                                                                                                                                                |
| ------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Follower TCP disconnect                                       | Drops subscription; continues accepting writes                 | Reconnects with exponential backoff (100 ms → 30 s, capped)                         | Records written during the disconnect are buffered up to channel size, then dropped. **Lost on the follower if disconnect exceeds buffer drain time.**                    |
| Follower process restart                                      | Drops subscription                                             | Restarts with empty replication cursor; resumes from "now"                          | **All writes between the disconnect and resubscribe are lost on the follower.** Snapshot bootstrap would fix this; not shipped.                                           |
| Follower disk full / write error                              | Unaware; continues publishing                                  | Logs ERROR; subscription continues but apply fails                                  | All subsequent writes lost on the follower until disk is cleared.                                                                                                         |
| Leader process crash                                          | All in-flight writes accepted but unfsynced are lost (SPEC §9) | Replication channel closes; follower waits for reconnect                            | Same as single-node: writes that returned OK survive. Writes that did not return OK are lost.                                                                             |
| Leader power loss (storage flushed)                           | Same as crash                                                  | Same                                                                                | Same                                                                                                                                                                      |
| Leader power loss (storage not flushed, e.g. NVMe cache loss) | Up to last `F_FULLFSYNC`'d group commit survives               | Follower may be _ahead_ of recovered leader on records that were published but lost | After leader recovery, follower may have records the leader does not. **This is the case `promote` is for**: rather than rejoin a divergent leader, promote the follower. |
| Network partition leader↔follower                             | Unaware; channel fills, drops accumulate                       | Reconnect loop; logs lag                                                            | Same as disconnect.                                                                                                                                                       |
| `promote` run on follower while leader still alive            | Leader still alive and accepting writes                        | Follower becomes second leader; accepts writes                                      | **Split-brain.** Two divergent histories. **This is why the runbook mandates fencing the old leader before promotion.**                                                   |

The last row is the most important one. It is operationally avoidable but
not protocol-prevented; consensus is the only thing that prevents it
mechanically. See §7.

## 7. Why not consensus

The take-home brief flagged Raft and Multi-Paxos in its research material.
We engaged with both and made a deliberate choice not to implement either,
for reasons we want to make explicit so it does not read as ignorance of
the literature.

### 7.1 What consensus would buy

- **Mechanical prevention of split-brain.** Two would-be leaders cannot
  both win an election; the protocol guarantees at most one leader per
  term.
- **Automatic failover.** Follower nodes detect leader death via missed
  heartbeats and elect a new leader without operator intervention.
- **Stronger durability semantics.** A write that returns OK is durable
  on a majority of nodes, not just one.
- **Read-your-writes consistency** across the cluster (with appropriate
  client-side handling).

### 7.2 What it would cost

- **Raft**: ~1500–2500 LOC of correctness-critical code, plus partition
  testing harness (in the spirit of Jepsen / `etcd-io/raft`'s test suite).
  Implementing the basic algorithm is achievable; implementing it
  _correctly under -race and partition tests_ is days of work, not hours.
  Critical sub-features that take-home implementations routinely get
  wrong: log compaction (Raft §7), membership changes (§6), pre-vote
  optimization, the "Leader Completeness" property under crashed-leader-rejoin.
- **Multi-Paxos**: harder than Raft, not easier. The original Paxos paper
  leaves the leader role under-specified, which is why production
  implementations diverge significantly (Mencius, EPaxos, "Paxos Made
  Live") and are notoriously difficult to verify. Raft was explicitly
  designed to be more _understandable_ than Paxos for this exact reason
  (Ongaro & Ousterhout, "In Search of an Understandable Consensus
  Algorithm", USENIX ATC 2014).

### 7.3 The honest comparison for this codebase

For Bitcask-style storage specifically, Raft is the better fit:

| Consideration                       | Raft                                              | Multi-Paxos                                                                    |
| ----------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------ |
| Replicated log primitive            | Native to the protocol                            | Bolted on (Multi-Paxos atop classic Paxos)                                     |
| Match to append-only segments       | Excellent — segments _are_ the log                | Same, but the leader-instance ambiguity adds work                              |
| Implementation reviewability        | High — single-file impls exist (HashiCorp `raft`) | Lower — every prod impl diverges in non-trivial ways                           |
| Read-the-paper-to-working-code time | Days                                              | Weeks                                                                          |
| Throughput under steady state       | Single-leader serialization is the bottleneck     | EPaxos can beat Raft under low contention; under high contention they converge |
| Throughput under failure            | Election pause (~election timeout)                | Generally faster recovery in Multi-Paxos variants                              |

For a single-node-bonus track on a take-home, the marginal throughput of
EPaxos is dominated by the difficulty of implementing it correctly. Raft
is the unambiguously correct choice.

### 7.4 Why we ship neither

A buggy consensus implementation is **worse than honest async + manual
failover**. The Aphyr / Jepsen series (`https://jepsen.io/analyses`) is
substantially a catalogue of vendors who shipped "Raft" or "Paxos" and
discovered, under partition tests, that they actually shipped data loss.

The take-home is a fixed time budget. Spending the bonus on a partially
verified consensus implementation gambles credibility on a feature that
requires weeks of correctness testing to be defensible. Spending it on
**correctly scoped async + manual failover + a clear consensus integration
plan** demonstrates the same understanding without the same risk.

We mark the integration point precisely (§8) so a future contributor —
or a follow-up evaluation milestone — can pick up the work without
re-deriving the design.

## 8. Raft integration point (if we picked it up tomorrow)

Concrete map from current code to a Raft implementation. This is not
hand-waving; line numbers and types are stable enough on the
`bonus/replication` branch to act as a real plan.

### 8.1 Files that change

| File                                   | Change                                                                                                                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/engine/engine.go`            | `Subscribe()` becomes the Raft log's commit channel. The writer goroutine becomes the apply loop, applying only committed records.                                           |
| `internal/replication/raft.go` _(new)_ | State machine: Follower / Candidate / Leader. Election timer. AppendEntries / RequestVote RPCs over the existing wire framing.                                               |
| `internal/replication/log.go` _(new)_  | Persistent Raft log (term, index, command). Reuses the segment-and-CRC infrastructure for storage.                                                                           |
| `internal/wire/wire.go`                | Reserve opcodes for `RAFT_APPENDENTRIES`, `RAFT_REQUESTVOTE`, `RAFT_INSTALLSNAPSHOT`. The existing `REPLICATE_*` opcodes stay reserved for the async path or are repurposed. |
| `cmd/little-db/subcommands.go`         | `serve --peers <comma-separated>` boots the Raft cluster. `--replica-of` becomes deprecated. `promote` becomes the manual override for split-brain recovery.                 |

### 8.2 What we'd reuse vs. rewrite

**Reuse**: record encoding, CRC framing, segment storage, fsync infrastructure,
wire framing, the entire client.

**Rewrite**: the publish-channel + follower-mode logic in this branch.
Raft has its own log and its own apply semantics; the current async
machinery is a different shape.

**Open question for the Raft phase**: whether `Engine` becomes the
_state machine_ applied to by the Raft log, or whether Raft is layered
_above_ an unmodified Engine and writes are applied as a side effect of
log commits. The latter is cleaner but requires the Engine to expose an
"apply this pre-encoded record" entry point (which the follower path
already does).

## 9. Testing

### 9.1 `TestReq8_ReplicationBonus` (unskipped on this branch)

The compliance test, re-enabled with the following scenario:

1. Start leader on ephemeral port. Start follower with `--replica-of`
   pointing at leader.
2. Write 100 keys to the leader. Wait for follower convergence (poll
   `Stats().KeyCount` with a deadline).
3. Read all 100 keys from the follower. Assert they match.
4. Kill leader process. Promote follower via the `promote` subcommand.
5. Write 50 new keys to (formerly-follower, now-) leader. Assert
   `Stats().KeyCount == 150`.

This exercises convergence, manual failover, and post-promotion writes.
It does **not** test split-brain (which is operator-prevented, not
protocol-prevented).

### 9.2 Manual failover runbook test

A separate test asserts the runbook (§10) works mechanically: the
sequence of commands in the runbook succeeds in order on a real binary.
Failing this test means the runbook lies, which is worse than not having
one.

### 9.3 What we don't test (because we don't ship the property)

- **Split-brain prevention**: not a property of this implementation.
- **Bounded staleness on followers**: not promised.
- **Snapshot bootstrap**: not shipped.

## 10. Manual failover runbook

Operator-facing. Lives here because the design and the runbook must agree;
if they drift, this document is the source of truth.

### 10.1 Preconditions

- Operator has shell access to both nodes.
- Operator has confirmed the leader is unreachable (via TCP probe,
  ping, or out-of-band signal).
- Clients are configured with a list of candidate addresses or a routing
  layer (HAProxy, VIP, DNS) the operator can update.

### 10.2 Sequence

1. **Fence the old leader.** Choose one:
   - Firewall: `iptables -A INPUT -p tcp --dport 4242 -j DROP` on the
     leader host.
   - Process kill: `pkill -9 little-db` on the leader host.
   - VM/container stop: whatever your orchestrator provides.

   **The fence must complete before step 2.** A live old leader plus a
   promoted follower is split-brain.

2. **Promote the follower.**

   ```bash
   ssh follower-host
   little-db promote --addr 127.0.0.1:4242
   # → "promotion completed at 2026-05-28T..."
   ```

3. **Update client routing.** Whatever mechanism your clients use —
   update DNS, update HAProxy backend, update the candidate-list config —
   to point at the promoted follower.

4. **Verify.** Run a smoke test (`little-db ping`, then a known-key
   `get`) against the new leader.

5. **Disposition the old leader.** Do not restart it as a follower —
   it may contain writes the new leader does not, and the async
   protocol has no merge story. The supported recovery is: wipe its
   data directory, restart it as a follower with `--replica-of`
   pointing at the new leader.

### 10.3 Things that go wrong

| Symptom                                                           | Likely cause                                               | Recovery                                                                                                          |
| ----------------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Clients see intermittent errors after step 3                      | Routing not fully propagated (DNS TTL, connection pooling) | Drain client connections; wait for TTL.                                                                           |
| `promote` returns "already a leader"                              | The follower was never in follower mode                    | Verify the follower was started with `--replica-of`. The command is idempotent so this is informational.          |
| Follower's `KeyCount` is well below the leader's last known value | Replication was lagging at the time of failover            | Documented data loss. The drop counter on the leader (when available) is the audit trail. Scope this as an event. |

## 11. Open questions / future work

The unsorted backlog. Each one is real engineering, not aspiration.

- **Snapshot bootstrap.** Leader supports a `REPLICATE_SNAPSHOT` op that
  streams its current segment + hint files. Follower applies before
  tailing the live stream. Unlocks late-join and post-restart recovery.
- **Persistent follower cursor.** Follower writes the last applied
  offset to disk. Combined with snapshot bootstrap, makes restarts
  recover correctly.
- **Multi-follower fanout.** The leader's publish channel is one-to-many
  in shape; making it correct requires per-follower buffering and
  back-pressure isolation.
- **Sync-ack mode.** Optional `--sync-replication` flag on the leader:
  writes don't return OK until N followers have ACKed. Trades latency
  for stronger durability. Still not consensus.
- **Raft.** §8 is the integration plan. The cost is real; the benefit
  is correct automatic failover and linearizability.
- **TLS.** Replication stream over TLS with mTLS. Currently a sidecar
  responsibility.

## 12. References

For the protocol literature underlying §7 and §8:

- Ongaro, D., & Ousterhout, J. (2014). _In Search of an Understandable
  Consensus Algorithm._ USENIX ATC.
- Ongaro, D. (2014). _Consensus: Bridging Theory and Practice._ Stanford
  PhD thesis. The canonical reference, including log compaction and
  membership changes.
- Lamport, L. (2001). _Paxos Made Simple._ SIGACT News 32(4).
- Howard, H., & Mortier, R. (2020). _Paxos vs Raft: Have We Reached
  Consensus on Distributed Consensus?_ PaPoC.
- Moraru, I., Andersen, D. G., & Kaminsky, M. (2013). _There Is More
  Consensus in Egalitarian Parliaments._ SOSP. (EPaxos.)
- Sheehy, J., & Smith, D. (2010). _Bitcask: A Log-Structured Hash Table
  for Fast Key/Value Data._ The Bitcask paper, for the storage layer
  this replication design sits on.
- Kingsbury, K. _Jepsen_ (`jepsen.io/analyses`). The empirical record on
  why "we'll add consensus later" tends to ship data loss.

For Postgres async streaming replication — the production system whose
failure model most closely matches what we ship — the canonical reference
is the PostgreSQL administrator's guide, chapter 27 ("Reliability and the
Write-Ahead Log") and chapter 26 ("High Availability, Load Balancing, and
Replication"). The pattern of "async streaming + manual promotion + a
separate consensus-backed orchestrator (Patroni) when automation is
needed" is exactly the path this design leaves open.
