package engine

import (
	"bytes"
	"errors"
	"fmt"
)

// Follower-side replication apply.
//
// This file implements ApplyReplicatedRecord, the entry point a follower
// uses to ingest one record streamed from a leader's REPLICATE_RECORD
// frame. The wire-side payload is the byte-identical record the leader
// appended to its own active segment (header + body, including the
// leader's CRC); the follower's job is to land those bytes verbatim in
// its own active segment and reflect the implied keydir updates.
//
// Design points:
//
//   - The apply is routed through the same writer goroutine that serves
//     Put/Delete/BatchPut. That is what gives apply mutual exclusion
//     with segment rotation, compact-commit, and timestamp generation
//     without inventing a second writer.
//
//   - The bytes are appended verbatim. We do NOT re-encode (which would
//     mint a fresh CRC and timestamp) — preserving the leader's record
//     bytes is what lets a follower's reads match the leader's writes
//     down to the CRC, and it keeps any future operator-side record
//     diff meaningful. Note that segment IDs and segment boundaries
//     are NOT preserved: a follower that joins mid-stream, or rotates
//     on a different threshold, will package the same records into
//     different segment files. Identity is per-record, not per-segment.
//
//   - The CRC is revalidated on apply. The wire codec already trusts
//     the frame CRC inside ReadFrame, but the leader and follower are
//     independent processes potentially separated by a network and a
//     kernel buffer, and the on-disk record CRC is the storage
//     engine's contract with itself. A frame that round-trips a wire
//     CRC fine but has a corrupt embedded record CRC must fail the
//     apply, not silently land on disk.
//
//   - lastTstamp ratchets forward to max(local, leader). The follower
//     never goes backward, and once promoted it will generate
//     timestamps that strictly exceed every leader timestamp it has
//     seen — preserving the engine's "tstamp is monotonic per writer"
//     invariant across the failover boundary.
//
//   - Keydir updates are timestamp-gated (putIfNewer / tstamp-gated
//     delete), mirroring recovery. The leader-to-follower stream is
//     ordered TCP and in normal operation records arrive in append
//     order, but the engine-level API takes one record at a time and
//     a future resume / replay path can legitimately re-deliver an
//     older record. Unconditional put would let an older record roll
//     back a newer one in the in-memory keydir even though recovery
//     would later restore the newer record from disk — a visible
//     divergence between live and post-restart state. Gating keeps
//     the two views identical.
//
//   - publishRecord is intentionally NOT called on the apply path. A
//     follower does not republish what it received: chained replication
//     is out of scope for v0.1.0, and silently republishing would
//     create a feedback loop the moment someone enabled both
//     ReplicationBufferSize and --replica-of on the same DB. The CLI
//     forbids that combination, but the engine layer is the place to
//     fail closed.

var (
	// ErrReplicationCRC indicates the embedded record CRC did not match
	// the bytes the follower received. The record is rejected and the
	// follower's segment does not advance. Callers should treat this
	// as a hard protocol violation: drop the connection, the leader
	// will be re-dialled and the stream restarted.
	ErrReplicationCRC = errors.New("engine: replicated record failed CRC")

	// ErrReplicationMalformed indicates the bytes did not parse as a
	// valid bitcask record at all (truncated, bad header flag, out-of-
	// range lengths). Same disposition as ErrReplicationCRC.
	ErrReplicationMalformed = errors.New("engine: replicated record malformed")
)

// ApplyReplicatedRecord ingests a single record that was streamed from
// a leader as the payload of a wire REPLICATE_RECORD frame.
//
// The bytes must be a complete bitcask record exactly as the leader
// appended it. The CRC is revalidated on apply; a bit flip in transit
// returns ErrReplicationCRC before the local segment advances.
//
// Routes through the same writer goroutine as Put/Delete/BatchPut, so
// the apply is serialised against rotation and compact-commit. The
// follower honours its own Options.SyncOnPut: typical follower
// deployments set it to false because durability rides the leader's
// group fsync, but a paranoid operator may set it true to make the
// follower itself a durable replica.
//
// IMPORTANT: this is a follower-only API. Callers must not mix local
// Put/Delete/BatchPut with ApplyReplicatedRecord on the same DB. The
// lastTstamp ratchet means an interleaved local write would force the
// next leader record to be reassigned a later timestamp (or rejected),
// silently diverging the follower from the leader. The little-db CLI
// enforces this by rejecting writes with FOLLOWER_READ_ONLY whenever
// --replica-of is set.
func (db *DB) ApplyReplicatedRecord(raw []byte) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: empty record", ErrReplicationMalformed)
	}
	if len(raw) > maxBatchBodyLen+recordHeaderSize {
		// Defensive: wire layer caps inbound frames well below this,
		// but the engine should not trust a single byte from another
		// process. maxBatchBodyLen is the largest body the recovery
		// scanner will ever accept.
		return fmt.Errorf("%w: record_len=%d exceeds engine cap", ErrReplicationMalformed, len(raw))
	}
	req := &writeRequest{
		kind:  writeKindApply,
		raw:   raw,
		reply: make(chan error, 1),
	}
	return db.submit(req)
}

// handleReplicateApply runs on the writer goroutine. Same ownership
// rules as handleOne / handleBatch: writer-private state (active,
// lastTstamp, encBuf) is touched without locks, and segmentsMu.Lock is
// taken only via rotateActive.
//
// Returns (appendedActive=true, err) on a successful append. Failures
// before the append (CRC, malformed, rotation error) return
// (false, err) and the follower's on-disk state does not advance.
func (db *DB) handleReplicateApply(req *writeRequest) (bool, error) {
	raw := req.raw

	// Parse and revalidate the record. readRecord verifies the CRC,
	// rejects unknown flags, and rejects out-of-range key/value
	// lengths.
	rec, n, err := readRecord(bytes.NewReader(raw))
	if err != nil {
		// Distinguish CRC failure from anything else so operators see
		// a clear signal. errBadCRC is the package-private sentinel;
		// everything else is shape/framing.
		if errors.Is(err, errBadCRC) {
			return false, ErrReplicationCRC
		}
		return false, fmt.Errorf("%w: %v", ErrReplicationMalformed, err)
	}
	if n != len(raw) {
		// readRecord consumed less than the caller provided. The
		// remainder is either garbage or a second record concatenated
		// into one frame — both are protocol violations.
		return false, fmt.Errorf("%w: %d trailing bytes after record of %d", ErrReplicationMalformed, len(raw)-n, n)
	}

	// Pre-validate a BATCH body BEFORE we touch the segment. The
	// outer record CRC only covers the bytes; a leader-side bug could
	// produce a BATCH whose CRC matches but whose body is internally
	// inconsistent. Decoding now (with recordStart=0; we add the real
	// offset after append) lets us reject the record cleanly without
	// leaving a poison pill on disk that the next recovery scan would
	// fail on. The decoded slices alias rec.value, which itself is a
	// fresh allocation owned by this request, so retaining them across
	// the append is safe.
	var batchEntries []batchEntryDecoded
	if rec.flag == recordFlagBatch {
		decoded, derr := decodeBatchBody(rec.value, 0)
		if derr != nil {
			return false, fmt.Errorf("%w: batch body: %v", ErrReplicationMalformed, derr)
		}
		batchEntries = decoded
	}

	// Rotation. Same rule as handleBatch: if the active is non-empty
	// and appending this record would push past the threshold, rotate.
	// An empty active never rotates — a record larger than
	// MaxSegmentSize must live in one segment (and the leader already
	// validated it would fit under MaxBatchEncodedSize ≤
	// wire.MaxReplicationRecord at Open time).
	if db.active.size.Load() > 0 && db.active.size.Load()+int64(len(raw)) > db.opts.MaxSegmentSize {
		if err := db.rotateActive(); err != nil {
			return false, err
		}
	}

	// Append verbatim — the on-disk bytes must match the leader's
	// byte-for-byte (segment IDs/boundaries may differ; record bytes
	// do not). No re-encode, no CRC regeneration.
	offset, err := db.active.append(raw)
	if err != nil {
		return false, err
	}
	if err := db.active.flush(); err != nil {
		return false, err
	}

	// Keydir update mirrors handleOne / handleBatch by record flag,
	// but with recovery-style timestamp gating: an older record never
	// overwrites a newer one. This keeps the live in-memory keydir
	// identical to what a post-restart recovery would reconstruct.
	switch rec.flag {
	case recordFlagPut:
		db.keydir.putIfNewer(rec.key, keydirEntry{
			fileID:   db.active.id,
			valuePos: offset + int64(recordHeaderSize+len(rec.key)),
			valueLen: uint32(len(rec.value)),
			tstamp:   rec.tstamp,
		})
	case recordFlagTombstone:
		if existing, ok := db.keydir.get(rec.key); !ok || rec.tstamp >= existing.tstamp {
			db.keydir.delete(rec.key)
		}
	case recordFlagBatch:
		// batchEntries was decoded with recordStart=0 above; adjust
		// every valuePos to the real absolute offset now that we know
		// it, and hand the whole list to applyBatchIfNewer. One write
		// lock for the whole batch preserves BatchPut's whole-batch
		// atomic-visibility contract for snapshot readers; the
		// IfNewer variant keeps recovery-style timestamp gating so a
		// re-delivered older batch cannot roll the keydir back.
		ops := make([]keydirOp, len(batchEntries))
		for i := range batchEntries {
			e := &batchEntries[i]
			absValuePos := e.valuePos + offset
			switch e.flag {
			case recordFlagPut:
				ops[i] = keydirOp{
					key: append([]byte(nil), e.key...),
					entry: keydirEntry{
						fileID:   db.active.id,
						valuePos: absValuePos,
						valueLen: uint32(len(e.value)),
						tstamp:   rec.tstamp,
					},
				}
			case recordFlagTombstone:
				// On the delete branch only entry.tstamp matters;
				// applyBatchIfNewer uses it for gating and ignores
				// the rest.
				ops[i] = keydirOp{
					key:    append([]byte(nil), e.key...),
					delete: true,
					entry:  keydirEntry{tstamp: rec.tstamp},
				}
			}
		}
		db.keydir.applyBatchIfNewer(ops)
	default:
		// readRecord already rejected unknown flags; defensive.
		return true, fmt.Errorf("%w: unknown record flag %d", ErrReplicationMalformed, rec.flag)
	}

	// Ratchet lastTstamp ONLY on a successful apply. Doing this
	// earlier would let a malformed record (rejected by the pre-
	// validation path) silently advance the writer's monotonic
	// clock — a side-effect on a no-op outcome, which would push
	// every subsequent locally-generated timestamp forward for no
	// reason and complicate any future "rejected => fully rolled
	// back" reasoning.
	if rec.tstamp > db.lastTstamp {
		db.lastTstamp = rec.tstamp
	}

	// Intentionally NO publishRecord call here — see the file-level
	// comment for why a follower must not republish.
	return true, nil
}
