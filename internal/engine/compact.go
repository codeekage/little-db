package engine

// Background compaction.
//
// Compaction reclaims space occupied by superseded writes (later PUTs to the
// same key + tombstones). It runs as an opt-in background goroutine spawned
// by Open when Options.CompactionInterval > 0, and can also be triggered
// synchronously via DB.Compact() — used by tests and incident playbooks.
//
// Strategy (locked in batch 4c, the simplest correct variant):
//
//   - The set of compaction inputs is "every immutable segment". The active
//     segment is never compacted; it is still being appended to.
//   - For each live PUT in those inputs (live ≡ "keydir entry still points
//     to this exact (segment, valuePos)") we copy the full record to a
//     freshly-allocated merged segment and record a (key, oldKeydir,
//     newKeydir) rewrite.
//   - Tombstones are dropped unconditionally. This is safe under the
//     "compact every immutable" invariant: a tombstone in an immutable
//     segment can only have been shadowing a PUT in an OLDER immutable
//     segment, and that older PUT is in our input set too — its keydir
//     entry was removed (or replaced) by the same tombstone during the
//     original write, so liveness filtering already drops it. There is
//     no segment older than the compacted set for the tombstone to
//     continue shadowing.
//
// Concurrency / crash safety:
//
//  1. Scan candidates with no lock held; the compactor is read-only against
//     segment files (immutable bytes) and reads the keydir under its own
//     internal RLock per lookup.
//  2. Write the merged segment + its hint sidecar via the same atomic-publish
//     dance the rest of the engine uses (write → fsync → rename → fsync(dir)).
//     After this point the merged segment is durable on disk but the manifest
//     does not yet reference it — if we crash now, reconcileManifest treats
//     it as an orphan on restart and silently drops it.
//  3. Hand the commit to the writer goroutine via reqCh. The writer is the
//     single owner of nextID and the only mutator of db.segments outside
//     Close; serialising the commit through it eliminates any race against
//     a concurrent rotateActive. The writer:
//       a. writes the new manifest (live = current segments − candidates + merged),
//       b. publishes the merged segment to db.segments,
//       c. compareAndSwaps each rewrite into the keydir (so a concurrent
//          newer write — which landed BETWEEN scan and commit — is not
//          clobbered with a stale compacted location),
//       d. removes candidate segments from db.segments,
//       e. closes + unlinks the candidates' .seg and .hint files.
//  4. Crash points between (2) and (3): on restart the manifest still
//     names the old candidates; the merged segment is an orphan and is
//     silently ignored by reconcileManifest. Compaction is retried later
//     (or on the next manual Compact() call).
//  5. Crash points after the manifest write but before (3.e): on restart
//     the manifest names the merged segment; old candidates are orphans
//     and silently ignored. They sit on disk until the next Open's
//     sweepOrphans() pass cleans them up. Online cleanup from a
//     non-owning goroutine would race with rotateActive's
//     createSegment / writeManifest window, so sweepOrphans is Open-only.

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	// compactionMinCandidates is the minimum number of immutable segments
	// before a compaction run does anything. At <2 the freshly-rotated
	// state is normal and compacting buys nothing measurable.
	compactionMinCandidates = 2
)

// rewrite is one key whose latest-write location is being remapped by the
// compactor: it used to live at oldEntry (in a retiring segment), it now
// lives at newEntry (in the merged segment). The commit step CAS's keydir
// from oldEntry → newEntry so a concurrent newer write does not get
// overwritten by stale compacted data.
type rewrite struct {
	key      []byte
	oldEntry keydirEntry
	newEntry keydirEntry
}

// compactCommit is the payload of a writeKindCompactCommit request handed to
// the writer goroutine. It contains everything the writer needs to atomically
// swap the merged segment in for its inputs.
//
// newSeg may be nil when every candidate's contents were already dead at
// scan time (rewrites is empty). In that case the commit retires the
// candidates without publishing a new segment — the compaction reclaims
// space purely by deletion.
type compactCommit struct {
	newSeg   *segment // nil iff rewrites is empty
	rewrites []rewrite
	oldSegs  []*segment
}

// Compact triggers one synchronous compaction pass. Returns nil if there
// was nothing to compact OR after a successful pass. Exposed for tests and
// for operational ad-hoc invocation; the background compactor uses the same
// code path.
//
// A Compact call may race with the background loop (or with another
// Compact); both will observe each other's effects via the manifest and
// either become a no-op or pick up the remaining work.
func (db *DB) Compact() error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	return db.compactOnce()
}

// compactOnce runs the read + emit phases off the writer goroutine and
// hands the final commit to the writer via reqCh.
//
// Orphan cleanup is intentionally NOT performed here: directory sweeps
// from a non-owning goroutine race with the writer's createSegment /
// writeManifest window during rotation (a new .seg file exists on disk
// before the manifest publishes its id, so a sweep that reads the manifest
// in that window would unlink the active segment that the writer is
// using). sweepOrphans is therefore Open-only — see DB.Open in engine.go.
func (db *DB) compactOnce() error {
	// Serialise compaction passes. Two concurrent passes would each
	// snapshot the same candidate set, each produce a merged segment,
	// each submit a commit; the first commit would close + unlink the
	// candidates while the second was still scanning them, producing a
	// read error (or, worse, a successful scan of partially-truncated
	// data on filesystems that don't enforce unlink-after-close). The
	// mutex makes the second caller a no-op (it sees ≤ 1 candidate after
	// the first commit landed) without any extra logic.
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// Re-check closed AFTER acquiring compactMu. DB.Compact() did an
	// early closed-check, but Close races: it sets closed=true and then
	// blocks on compactMu before tearing down segments and the directory
	// lock. If a caller passed the early check just before closed=true
	// was published, it would otherwise proceed to read segment fds and
	// create files that Close is about to release.
	if db.closed.Load() {
		return ErrDBClosed
	}

	// 1. Snapshot candidates. We grab the slice under segmentsMu.RLock so
	//    Close cannot race the snapshot, but the actual scan happens with
	//    no lock — segment files are immutable once rotated.
	db.segmentsMu.RLock()
	if db.active == nil {
		db.segmentsMu.RUnlock()
		return nil
	}
	activeID := db.active.id
	candidates := make([]*segment, 0, len(db.segments))
	for id, seg := range db.segments {
		if id != activeID {
			candidates = append(candidates, seg)
		}
	}
	db.segmentsMu.RUnlock()
	if len(candidates) < compactionMinCandidates {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].id < candidates[j].id })
	start := time.Now()
	db.log.Info("compaction start", slog.Int("candidates", len(candidates)))

	// 2. Allocate the merged segment id. Atomic.Add coordinates with the
	//    writer's rotateActive so no two producers ever pick the same id.
	newID := db.nextID.Add(1) - 1
	newSeg, err := createSegment(db.opts.Dir, newID)
	if err != nil {
		return fmt.Errorf("compact: create merged segment: %w", err)
	}
	cleanupNewSeg := func() {
		_ = newSeg.close()
		_ = os.Remove(newSeg.path)
	}

	// 3. Read each candidate, filter live PUTs, append to merged segment,
	//    accumulate rewrites. Tombstones and dead PUTs are dropped.
	rewrites := make([]rewrite, 0, db.keydir.size())
	encBuf := make([]byte, 0, 64*1024)
	for _, c := range candidates {
		var err error
		rewrites, encBuf, err = db.compactSegmentInto(c, newSeg, rewrites, encBuf)
		if err != nil {
			cleanupNewSeg()
			return err
		}
	}

	// 3a. Empty-rewrites special case. Every PUT in every candidate was
	//     already dead, so the merged segment would be 0 records. Discard
	//     it now and submit a retire-only commit — the candidates still
	//     need to come out of the manifest + segments map.
	if len(rewrites) == 0 {
		cleanupNewSeg()
		cc := &compactCommit{newSeg: nil, rewrites: nil, oldSegs: candidates}
		req := &writeRequest{kind: writeKindCompactCommit, compact: cc, reply: make(chan error, 1)}
		if err := db.submit(req); err != nil {
			return err
		}
		db.log.Info("compaction done",
			slog.Int("candidates", len(candidates)),
			slog.Int("rewrites", 0),
			slog.Duration("duration", time.Since(start)))
		return nil
	}

	// 4. Fsync the merged segment data before publishing anything. After
	//    this point the file is durable; manifest swap is the moment of
	//    commit.
	if err := newSeg.sync(); err != nil {
		cleanupNewSeg()
		return fmt.Errorf("compact: fsync merged segment: %w", err)
	}

	// 5. Emit the hint sidecar. Same atomic-publish discipline as data;
	//    if we crash between (4) and (5), the merged seg is an orphan on
	//    restart and is dropped silently.
	hintEntries := make([]hintEntry, 0, len(rewrites))
	for _, r := range rewrites {
		hintEntries = append(hintEntries, hintEntry{
			key:      r.key,
			valuePos: r.newEntry.valuePos,
			valueLen: r.newEntry.valueLen,
			tstamp:   r.newEntry.tstamp,
		})
	}
	if err := writeHintFile(db.opts.Dir, newID, hintEntries); err != nil {
		cleanupNewSeg()
		return fmt.Errorf("compact: write hint: %w", err)
	}

	// 6. Hand the commit to the writer goroutine. submit() blocks until
	//    the writer replies, so on return we know whether the manifest
	//    swap succeeded. The reply channel lives on the writeRequest;
	//    no separate copy is needed on the compactCommit payload.
	cc := &compactCommit{newSeg: newSeg, rewrites: rewrites, oldSegs: candidates}
	req := &writeRequest{kind: writeKindCompactCommit, compact: cc, reply: make(chan error, 1)}
	if err := db.submit(req); err != nil {
		// Either the DB closed before the writer accepted the commit, or
		// the writer rejected it (e.g. manifest write failed). In both
		// cases the manifest never moved, so the on-disk merged segment
		// and hint file are orphans — unlink them now so the next Open
		// is not littered with our half-built artefacts.
		cleanupNewSeg()
		_ = removeHintFile(db.opts.Dir, newID)
		return err
	}
	db.log.Info("compaction done",
		slog.Int("candidates", len(candidates)),
		slog.Int("rewrites", len(rewrites)),
		slog.Uint64("merged_id", uint64(newID)),
		slog.Duration("duration", time.Since(start)))
	return nil
}

// compactSegmentInto scans every record of c, copies live PUTs into newSeg
// (appending their bytes), and accumulates rewrites for the commit step.
// encBuf is a caller-owned reusable buffer for encoding the copied records;
// it is returned grown if needed so the caller can reuse it across segments.
//
// Liveness rule: a record at (c.id, valuePos, valueLen) is live iff the
// keydir entry for its key equals exactly that triple. Anything else
// (newer overwrite, deletion, or duplicate-key-within-this-segment that
// lost to a later record) is dropped.
//
// Tombstones are dropped unconditionally — see file-level comment.
//
// A BATCH record is decomposed: each inner live PUT is copied as a fresh
// PUT record into the merged segment. BATCH atomicity is irrelevant past
// the original write (writes are durable; recovery never reconstructs the
// "this was a batch" property), so the merged form does not need to
// preserve it.
func (db *DB) compactSegmentInto(
	c *segment,
	newSeg *segment,
	rewrites []rewrite,
	encBuf []byte,
) ([]rewrite, []byte, error) {
	r := &segmentSequentialReader{seg: c, off: 0}
	for {
		recordStart := r.off
		rec, _, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, encBuf, fmt.Errorf("compact: read segment %d at offset %d: %w", c.id, recordStart, err)
		}

		switch rec.flag {
		case recordFlagTombstone:
			// Always drop.
			continue

		case recordFlagPut:
			valuePos := recordStart + int64(recordHeaderSize+len(rec.key))
			valLen := uint32(len(rec.value))
			cur, ok := db.keydir.get(rec.key)
			if !ok {
				continue
			}
			if cur.fileID != c.id || cur.valuePos != valuePos || cur.valueLen != valLen {
				continue
			}
			// Live. Copy the record into newSeg. We re-encode rather
			// than copying raw bytes so the merged file is a valid
			// log of PUT records (the source bytes are valid too,
			// but going through encode keeps the merged file
			// structurally identical to any other engine-written
			// segment — same CRC, same header layout, no need for
			// recovery to learn a "borrowed bytes" mode).
			need := rec.encodedSize()
			if cap(encBuf) < need {
				encBuf = make([]byte, need)
			} else {
				encBuf = encBuf[:need]
			}
			if _, err := rec.encode(encBuf); err != nil {
				return nil, encBuf, fmt.Errorf("compact: encode merged record for key (segment %d offset %d): %w", c.id, recordStart, err)
			}
			newOffset, err := newSeg.append(encBuf)
			if err != nil {
				return nil, encBuf, fmt.Errorf("compact: append to merged segment: %w", err)
			}
			rewrites = append(rewrites, rewrite{
				key: append([]byte(nil), rec.key...),
				oldEntry: keydirEntry{
					fileID:   c.id,
					valuePos: valuePos,
					valueLen: valLen,
					tstamp:   rec.tstamp,
				},
				newEntry: keydirEntry{
					fileID:   newSeg.id,
					valuePos: newOffset + int64(recordHeaderSize+len(rec.key)),
					valueLen: valLen,
					tstamp:   rec.tstamp,
				},
			})

		case recordFlagBatch:
			entries, err := decodeBatchBody(rec.value, recordStart)
			if err != nil {
				return nil, encBuf, fmt.Errorf("compact: decode batch at segment %d offset %d: %w", c.id, recordStart, err)
			}
			for _, e := range entries {
				if e.flag == recordFlagTombstone {
					continue
				}
				cur, ok := db.keydir.get(e.key)
				if !ok {
					continue
				}
				valLen := uint32(len(e.value))
				if cur.fileID != c.id || cur.valuePos != e.valuePos || cur.valueLen != valLen {
					continue
				}
				// Re-emit as a standalone PUT record.
				copyRec := record{
					tstamp: rec.tstamp,
					flag:   recordFlagPut,
					key:    e.key,
					value:  e.value,
				}
				need := copyRec.encodedSize()
				if cap(encBuf) < need {
					encBuf = make([]byte, need)
				} else {
					encBuf = encBuf[:need]
				}
				if _, err := copyRec.encode(encBuf); err != nil {
					return nil, encBuf, fmt.Errorf("compact: encode merged batch entry (segment %d offset %d): %w", c.id, recordStart, err)
				}
				newOffset, err := newSeg.append(encBuf)
				if err != nil {
					return nil, encBuf, fmt.Errorf("compact: append batch entry to merged segment: %w", err)
				}
				rewrites = append(rewrites, rewrite{
					key: append([]byte(nil), e.key...),
					oldEntry: keydirEntry{
						fileID:   c.id,
						valuePos: e.valuePos,
						valueLen: valLen,
						tstamp:   rec.tstamp,
					},
					newEntry: keydirEntry{
						fileID:   newSeg.id,
						valuePos: newOffset + int64(recordHeaderSize+len(copyRec.key)),
						valueLen: valLen,
						tstamp:   rec.tstamp,
					},
				})
			}

		default:
			return nil, encBuf, fmt.Errorf("compact: unknown record flag %d at segment %d offset %d", rec.flag, c.id, recordStart)
		}
	}
	return rewrites, encBuf, nil
}

// handleCompactCommit runs on the writer goroutine. It performs the
// manifest swap, publishes the merged segment to db.segments, CASes each
// keydir rewrite, retires the old segments, and unlinks their on-disk
// files. See file-level comment for the crash-safety argument.
func (db *DB) handleCompactCommit(cc *compactCommit) error {
	// Recompute the live set under segmentsMu.RLock so it includes any
	// segments born during compaction (e.g. a rotation that landed
	// between the candidate snapshot and this commit). Active stays in
	// the set; candidates drop out; merged is added.
	candidateSet := make(map[uint32]struct{}, len(cc.oldSegs))
	for _, s := range cc.oldSegs {
		candidateSet[s.id] = struct{}{}
	}
	db.segmentsMu.RLock()
	newLive := make([]uint32, 0, len(db.segments)+1)
	for id := range db.segments {
		if _, drop := candidateSet[id]; drop {
			continue
		}
		newLive = append(newLive, id)
	}
	// Capture the writer's current active id while we hold the lock.
	// Compaction never changes which segment is active — the merged
	// segment is published as immutable — so we pass through whatever
	// the writer chose at rotation time. Reading db.active is safe here:
	// this runs on the writer goroutine (via handleOne), so no concurrent
	// rotateActive can be mid-flight, and segmentsMu.RLock pins the
	// pointer against teardown.
	activeID := db.active.id
	db.segmentsMu.RUnlock()
	if cc.newSeg != nil {
		newLive = append(newLive, cc.newSeg.id)
	}

	manifestUncertain := false
	if err := writeManifest(db.opts.Dir, newLive, activeID); err != nil {
		if errors.Is(err, ErrManifestPublishedButUncertain) {
			// MANIFEST is visible referencing newSeg.id and excluding
			// oldSegs, but the directory fsync did not confirm. After a
			// host crash the rename may revert and the previous
			// manifest may reappear; that manifest still references
			// the old segments. We must therefore NOT unlink any
			// on-disk segment (old or new) in this branch — both sets
			// must survive the process. The next clean Open will
			// observe whichever manifest the kernel preserved and
			// sweep the loser as an orphan.
			//
			// In-memory state still moves to the post-compaction view
			// so the running engine matches what a successful publish
			// would have produced; this avoids reading the keydir and
			// segments map in an inconsistent state for the remainder
			// of this process's lifetime.
			manifestUncertain = true
			var newSegID uint64
			if cc.newSeg != nil {
				newSegID = uint64(cc.newSeg.id)
			}
			db.log.Warn("manifest published with uncertain durability; preserving old segments on disk",
				slog.String("op", "compact_commit"),
				slog.Uint64("new_seg_id", newSegID),
				slog.Int("old_seg_count", len(cc.oldSegs)),
				slog.String("err", err.Error()))
		} else {
			return fmt.Errorf("compact: persist manifest: %w", err)
		}
	}
	// Past this point: compaction is committed on disk (or, on the
	// uncertain branch above, recovery will converge to either committed
	// or "old manifest stands; new seg is an ignored orphan"). Failures
	// from here on are in-memory consistency bugs, not data loss.

	db.segmentsMu.Lock()
	if cc.newSeg != nil {
		db.segments[cc.newSeg.id] = cc.newSeg
	}
	for _, r := range cc.rewrites {
		// CAS may fail if a newer write landed for this key between scan
		// and commit. That is correct: the newer entry wins, the
		// compacted copy becomes unreachable dead data, the next
		// compaction reclaims it.
		db.keydir.compareAndSwap(r.key, r.oldEntry, r.newEntry)
	}
	for _, s := range cc.oldSegs {
		delete(db.segments, s.id)
	}
	db.segmentsMu.Unlock()

	// Close + unlink old files OUTSIDE the lock. They are unreachable
	// from db.segments now, so no reader can hold a fresh pointer to
	// them; any reader that picked up a pointer before our Lock has
	// already finished its pread (Lock waited for the RLock to drain).
	//
	// In the manifestUncertain branch we MUST NOT unlink: the previous
	// manifest may resurface after a crash and still reference these
	// segments. Close the fds (the running process is committed to the
	// new view and won't reopen them), but leave the bytes on disk for
	// the next clean Open's sweepOrphans() to disposition.
	for _, s := range cc.oldSegs {
		_ = s.close()
		if manifestUncertain {
			continue
		}
		_ = os.Remove(s.path)
		_ = removeHintFile(db.opts.Dir, s.id)
	}
	return nil
}

// sweepOrphans deletes .seg and .hint files in the data directory that
// are not referenced by the current manifest. Called once at Open, while
// the DB is still single-threaded (writer + compactor goroutines have not
// started yet), to clean up after a previously-crashed compactor.
//
// It is NOT called from compactOnce: running this from a non-owning
// goroutine would race with the writer's rotateActive, which creates the
// new segment file on disk before writeManifest publishes its id — a
// concurrent sweep that read the manifest in that window would unlink the
// active segment the writer is using. See file-level comment.
//
// Best-effort: any error is returned to the caller (Open logs and
// continues). Cleanup failure must not block Open.
//
// We never delete the LOCK file or non-engine files. Files are matched
// by their extension and the canonical segment-id filename format.
func (db *DB) sweepOrphans() error {
	manifest, _, err := readManifest(db.opts.Dir)
	if err != nil {
		// If the manifest is missing we are pre-bootstrap; nothing to
		// sweep against. Any other read error is reported.
		if errors.Is(err, errManifestMissing) {
			return nil
		}
		return err
	}
	live := make(map[uint32]struct{}, len(manifest))
	for _, id := range manifest {
		live[id] = struct{}{}
	}

	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := filepath.Ext(name)
		if ext != segmentFileExt && ext != hintFileExt {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(name, "%010d"+ext, &id); err != nil {
			continue
		}
		if _, ok := live[id]; ok {
			continue
		}
		// Orphan. Best-effort unlink.
		_ = os.Remove(filepath.Join(db.opts.Dir, name))
	}
	return nil
}

// compactorLoop is the optional background goroutine that calls
// compactOnce on a fixed interval. It is started by Open when
// Options.CompactionInterval > 0 and exits when db.done is closed.
//
// Errors from a single pass are logged and do not stop the loop — a
// transient I/O hiccup must not silently disable background compaction
// for the life of the process.
func (db *DB) compactorLoop(interval time.Duration) {
	defer db.compactorWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.done:
			return
		case <-ticker.C:
			if db.closed.Load() {
				return
			}
			if err := db.compactOnce(); err != nil {
				db.log.Warn("background compaction error",
					slog.String("err", err.Error()))
			}
		}
	}
}
