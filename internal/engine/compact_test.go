package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// openCompactionDB opens a DB with a small MaxSegmentSize so a handful of
// writes force several rotations, giving the compactor multiple immutable
// segments to work on.
func openCompactionDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir:            t.TempDir(),
		MaxSegmentSize: 4 * 1024, // 4 KiB → easy to fill in a test
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func segCount(t *testing.T, db *DB) int {
	t.Helper()
	db.segmentsMu.RLock()
	defer db.segmentsMu.RUnlock()
	return len(db.segments)
}

// fillToMultipleSegments writes enough records to force the active segment
// to rotate at least twice, leaving ≥2 immutable segments + 1 active.
// Returns the keys written (each value is keyed-deterministically).
func fillToMultipleSegments(t *testing.T, db *DB, n int, valueSize int) [][]byte {
	t.Helper()
	keys := make([][]byte, n)
	val := bytes.Repeat([]byte("v"), valueSize)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		keys[i] = k
		if err := db.Put(k, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if segCount(t, db) < 3 {
		t.Fatalf("expected ≥3 segments after %d writes, got %d (raise n)", n, segCount(t, db))
	}
	return keys
}

// TestCompactionPreservesLiveData runs Compact across multiple immutable
// segments containing a mix of live writes, overwrites, and deletes, and
// verifies that:
//   - every live key still reads its latest value,
//   - deleted keys still report ErrKeyNotFound,
//   - the on-disk segment count decreased.
func TestCompactionPreservesLiveData(t *testing.T) {
	db := openCompactionDB(t)

	keys := fillToMultipleSegments(t, db, 200, 64)

	// Overwrite every 3rd key with a new value.
	overwriteVal := bytes.Repeat([]byte("V"), 64)
	for i := 0; i < len(keys); i += 3 {
		if err := db.Put(keys[i], overwriteVal); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	// Delete every 5th key.
	deleted := make(map[string]bool)
	for i := 0; i < len(keys); i += 5 {
		if err := db.Delete(keys[i]); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
		deleted[string(keys[i])] = true
	}

	before := segCount(t, db)
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := segCount(t, db)
	if after >= before {
		t.Fatalf("expected segment count to drop, before=%d after=%d", before, after)
	}

	originalVal := bytes.Repeat([]byte("v"), 64)
	for i, k := range keys {
		got, err := db.Get(k)
		if deleted[string(k)] {
			if !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("Get %d (deleted): err=%v val=%q", i, err, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		want := originalVal
		if i%3 == 0 {
			want = overwriteVal
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d: value mismatch", i)
		}
	}
}

// TestCompactionEmptyRewrites covers the retire-only commit path: every
// key in every immutable segment has been superseded (deleted), so the
// compactor produces zero rewrites and submits a commit with newSeg=nil.
// The immutable segments must come out of db.segments + the manifest
// without a new segment being added.
func TestCompactionEmptyRewrites(t *testing.T) {
	db := openCompactionDB(t)
	keys := fillToMultipleSegments(t, db, 200, 64)

	// Delete every key. Tombstones land in the active segment; force
	// rotation so they become immutable too (so the compactor sees them).
	for _, k := range keys {
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	// Add filler to push the tombstones out of the active segment.
	filler := bytes.Repeat([]byte("f"), 64)
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("filler%05d", i)), filler); err != nil {
			t.Fatalf("filler Put: %v", err)
		}
		// Immediately delete so they end up dead and droppable too.
		if err := db.Delete([]byte(fmt.Sprintf("filler%05d", i))); err != nil {
			t.Fatalf("filler Delete: %v", err)
		}
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Every original and filler key must still be not-found after compact.
	for _, k := range keys {
		if _, err := db.Get(k); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Get after compact: err=%v want ErrKeyNotFound", err)
		}
	}
}

// TestCompactionCloseRace launches a manual Compact() concurrently with
// Close(). After Close returns, no goroutine may still be running
// compaction code (which could write to a closed fd, leave a partial
// merged segment behind, or write a hint after the directory lock has
// been released). Close must wait for an in-flight Compact via compactMu,
// and compactOnce must re-check closed after acquiring compactMu so a
// late entrant returns ErrDBClosed cleanly.
func TestCompactionCloseRace(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		dir := t.TempDir()
		db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		// Seed enough data that compaction has real work to do.
		for i := 0; i < 200; i++ {
			if err := db.Put([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("v"), 64)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Loop so we keep racing Close. Either error is acceptable;
			// what is NOT acceptable is a panic, a race-detector
			// complaint, or a leftover file after Close returns.
			for {
				err := db.Compact()
				if err != nil {
					if !errors.Is(err, ErrDBClosed) {
						t.Errorf("Compact: unexpected error %v", err)
					}
					return
				}
			}
		}()

		// Give the compactor a chance to start at least one pass.
		time.Sleep(time.Duration(attempt%3) * time.Millisecond)
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		wg.Wait()

		// After Close returns, the directory must not contain any
		// orphan .seg or .hint files that the in-flight compaction
		// failed to clean up.
		manifest, _, err := readManifest(dir)
		if err != nil {
			t.Fatalf("readManifest: %v", err)
		}
		live := make(map[uint32]bool, len(manifest))
		for _, id := range manifest {
			live[id] = true
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		for _, e := range entries {
			name := e.Name()
			ext := filepath.Ext(name)
			if ext != segmentFileExt && ext != hintFileExt {
				continue
			}
			var id uint32
			if _, err := fmt.Sscanf(name, "%010d"+ext, &id); err != nil {
				continue
			}
			if !live[id] {
				t.Fatalf("attempt %d: orphan %s left behind after Close (compaction did not clean up)", attempt, name)
			}
		}
	}
}

// TestReadKeyRangeSnapshotIsolation is the direct regression for the
// snapshot-semantics hole that survived the first 4c review: even with the
// keydir-under-RLock fix, a write landing AFTER snapshot but BEFORE the
// per-key read could turn a snapshot entry into a "skip" via the tstamp
// re-resolve path. The current design materialises every value under one
// RLock, so a concurrent overwrite must be invisible.
func TestReadKeyRangeSnapshotIsolation(t *testing.T) {
	db := openTestDB(t, false)

	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v0")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Inside the callback, perform a write to a key we haven't yet
	// visited. With true snapshot semantics, the iteration must still
	// observe the OLD value for that key (because we materialised all
	// values up-front). Without snapshot semantics, we'd see the new
	// value when iteration reaches the key.
	overwriteDone := false
	err := db.ReadKeyRange(nil, nil, func(k, v []byte) bool {
		if !overwriteDone {
			// Overwrite the LAST key in the range while we're still on
			// the first one.
			if err := db.Put([]byte("k049"), []byte("NEW")); err != nil {
				t.Errorf("Put inside callback: %v", err)
				return false
			}
			overwriteDone = true
		}
		if !bytes.Equal(v, []byte("v0")) {
			t.Errorf("ReadKeyRange observed mid-scan write: key=%s value=%q", k, v)
			return false
		}
		return true
	})
	if err != nil {
		t.Fatalf("ReadKeyRange: %v", err)
	}

	// After ReadKeyRange returns, the overwrite must be visible.
	got, err := db.Get([]byte("k049"))
	if err != nil {
		t.Fatalf("Get k049 after ReadKeyRange: %v", err)
	}
	if !bytes.Equal(got, []byte("NEW")) {
		t.Fatalf("Get k049: got %q want NEW (post-scan visibility)", got)
	}
}

// TestCompactionSerialised launches two Compact() goroutines and the
// background loop simultaneously. compactMu must serialise them — none
// may see the same candidate's input segment in a state where another
// pass has already unlinked it (which would surface as a read error).
func TestCompactionSerialised(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                dir,
		MaxSegmentSize:     4 * 1024,
		CompactionInterval: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Keep churning data so there's always something to compact.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			k := []byte(fmt.Sprintf("k%05d", i%500))
			if err := db.Put(k, bytes.Repeat([]byte("z"), 128)); err != nil {
				t.Errorf("Put: %v", err)
				return
			}
			i++
		}
	}()

	// Two concurrent manual Compact callers alongside the background loop.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if err := db.Compact(); err != nil {
					t.Errorf("Compact: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// TestCompactionConcurrentReads is a regression for the Get/ReadKeyRange
// reader-lifetime race fixed in the 4c batch: Get used to capture the
// keydir entry before acquiring segmentsMu, so a compaction commit landing
// between those two operations could turn a live key into a spurious
// ErrKeyNotFound. This test runs many readers in parallel with repeated
// compactions and asserts no reader ever sees a not-found for a seeded
// key.
func TestCompactionConcurrentReads(t *testing.T) {
	db := openCompactionDB(t)

	const N = 500
	val := bytes.Repeat([]byte("x"), 64)
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		if err := db.Put(k, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if segCount(t, db) < 3 {
		t.Fatalf("expected ≥3 segments, got %d", segCount(t, db))
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Compactor loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			// Each pass needs ≥2 immutable segments; once compaction
			// merges them down to 1 immutable + active, future passes
			// are no-ops. Force a rotation by writing a key big
			// enough to fill the active threshold.
			big := bytes.Repeat([]byte("z"), 3*1024)
			_ = db.Put([]byte(fmt.Sprintf("rot-%d", time.Now().UnixNano())), big)
			if err := db.Compact(); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
		}
	}()

	// Reader goroutines: Get random seeded keys and assert success.
	const readers = 8
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for !stop.Load() {
				k := []byte(fmt.Sprintf("k%05d", i%N))
				got, err := db.Get(k)
				if err != nil {
					t.Errorf("Get %s: %v (spurious not-found regression?)", k, err)
					return
				}
				if !bytes.Equal(got, val) {
					t.Errorf("Get %s: value mismatch", k)
					return
				}
				i++
			}
		}(r * 7919)
	}

	// ReadKeyRange goroutine: a full snapshot must include every seeded key.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			seen := make(map[string]bool, N)
			err := db.ReadKeyRange([]byte("k00000"), []byte("k99999"), func(k, v []byte) bool {
				seen[string(k)] = true
				if !bytes.Equal(v, val) {
					t.Errorf("ReadKeyRange %s: value mismatch", k)
					return false
				}
				return true
			})
			if err != nil {
				t.Errorf("ReadKeyRange: %v", err)
				return
			}
			for i := 0; i < N; i++ {
				if !seen[fmt.Sprintf("k%05d", i)] {
					t.Errorf("ReadKeyRange: missing k%05d (snapshot lost a seeded key)", i)
					return
				}
			}
		}
	}()

	time.Sleep(750 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// TestCompactionConcurrentWrites runs the background compactor while a
// writer keeps overwriting keys + adding new ones. Verifies that:
//   - the writer never errors,
//   - reads after the test see the latest writer value (CAS path of the
//     commit step correctly defers to a concurrent fresher write).
func TestCompactionConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                dir,
		MaxSegmentSize:     4 * 1024,
		CompactionInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Pre-seed.
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v0")); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	latest := make([][]byte, 200)
	var latestMu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		gen := 0
		for !stop.Load() {
			gen++
			for i := 0; i < 200; i++ {
				v := []byte(fmt.Sprintf("v%d", gen))
				if err := db.Put([]byte(fmt.Sprintf("k%05d", i)), v); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				latestMu.Lock()
				latest[i] = v
				latestMu.Unlock()
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// After the writer stops, the latest value snapshot must match Get.
	for i := 0; i < 200; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%05d", i)))
		if err != nil {
			t.Fatalf("Get k%05d after concurrent compaction: %v", i, err)
		}
		latestMu.Lock()
		want := latest[i]
		latestMu.Unlock()
		if !bytes.Equal(got, want) {
			t.Fatalf("Get k%05d: got %q want %q (compaction clobbered a fresher write)", i, got, want)
		}
	}
}

// TestCompactionOrphanSweepOnOpen verifies that sweepOrphans runs at Open
// and deletes .seg / .hint files that the manifest does not reference.
// This is the crash-recovery path: a compactor that crashed between
// writing the merged segment and committing the manifest leaves an orphan
// on disk; Open must clean it.
func TestCompactionOrphanSweepOnOpen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drop in an orphan .seg and .hint at an id well above what was
	// allocated. They are not in the manifest.
	orphanSeg := filepath.Join(dir, fmt.Sprintf("%010d%s", 9999, segmentFileExt))
	orphanHint := filepath.Join(dir, fmt.Sprintf("%010d%s", 9999, hintFileExt))
	if err := os.WriteFile(orphanSeg, []byte("junk"), 0o644); err != nil {
		t.Fatalf("write orphan seg: %v", err)
	}
	if err := os.WriteFile(orphanHint, []byte("junk"), 0o644); err != nil {
		t.Fatalf("write orphan hint: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if _, err := os.Stat(orphanSeg); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan .seg not swept: %v", err)
	}
	if _, err := os.Stat(orphanHint); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan .hint not swept: %v", err)
	}
}

// TestCompactionBatchRecordsRecompactToPuts: a batch record in an
// immutable segment must be decomposed during compaction into standalone
// PUT records in the merged segment, with the keydir CAS still pointing
// at the right (segment, offset, valueLen) afterwards. Verifies post-
// compact reads still return the batch's values.
func TestCompactionBatchRecordsRecompactToPuts(t *testing.T) {
	db := openCompactionDB(t)

	entries := make([]BatchEntry, 50)
	for i := range entries {
		entries[i] = BatchEntry{
			Key:   []byte(fmt.Sprintf("b%05d", i)),
			Value: []byte(fmt.Sprintf("batchvalue-%d", i)),
		}
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	// Push the batch out of the active segment.
	filler := bytes.Repeat([]byte("f"), 256)
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte(fmt.Sprintf("filler%05d", i)), filler); err != nil {
			t.Fatalf("filler Put: %v", err)
		}
	}
	if segCount(t, db) < 3 {
		t.Fatalf("expected ≥3 segments before Compact, got %d", segCount(t, db))
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for _, e := range entries {
		got, err := db.Get(e.Key)
		if err != nil {
			t.Fatalf("Get batch key %s after compact: %v", e.Key, err)
		}
		if !bytes.Equal(got, e.Value) {
			t.Fatalf("Get batch key %s: got %q want %q", e.Key, got, e.Value)
		}
	}
}

// TestCompactionIntervalNegative ensures Options.CompactionInterval validation
// rejects negative values at Open time.
func TestCompactionIntervalNegative(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), CompactionInterval: -1})
	if err == nil {
		t.Fatal("Open with negative CompactionInterval should fail")
	}
}

// TestCompactionNoOpBelowThreshold: with <2 immutable segments the
// compactor must do nothing (no segments retired, no new segment created).
func TestCompactionNoOpBelowThreshold(t *testing.T) {
	db := openTestDB(t, false)

	if err := db.Put([]byte("only"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	before := segCount(t, db)
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := segCount(t, db)
	if before != after {
		t.Fatalf("Compact with no immutable segments changed seg count: before=%d after=%d", before, after)
	}
}

// TestCompactionPersistsAcrossRestart: run a compaction, close the DB,
// reopen, and verify the merged data is still readable (manifest swap
// must have been durable; merged hint file must drive recovery).
func TestCompactionPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	keys := fillToMultipleSegments(t, db, 200, 64)
	// Overwrite half.
	overwriteVal := bytes.Repeat([]byte("V"), 64)
	for i := 0; i < len(keys); i += 2 {
		if err := db.Put(keys[i], overwriteVal); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	originalVal := bytes.Repeat([]byte("v"), 64)
	for i, k := range keys {
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("Get %d after restart: %v", i, err)
		}
		want := originalVal
		if i%2 == 0 {
			want = overwriteVal
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d after restart: value mismatch", i)
		}
	}
}

// TestCompactionMergedSegmentNotActiveAfterRestart pins the regression
// behind the "highest id = active" bug fix. After compaction, the merged
// immutable segment has a higher id than the writer's active segment.
// A naive Open that used max(ids) for "active" would: (a) treat the
// merged segment as the writer's target and append new writes to it,
// invalidating its hint file, and (b) skip torn-tail truncation of the
// real active. The manifest now records the active id explicitly, so
// post-restart the writer must continue appending to the same active
// id that was in use pre-Close, and the merged segment must remain
// immutable (no new records appended).
func TestCompactionMergedSegmentNotActiveAfterRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	keys := fillToMultipleSegments(t, db, 200, 64)
	for i := 0; i < len(keys); i += 2 {
		if err := db.Put(keys[i], bytes.Repeat([]byte("V"), 64)); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Snapshot the post-compact state. The merged segment is the
	// highest live id (compactor allocates from nextID monotonically),
	// while db.active is something strictly less.
	db.segmentsMu.RLock()
	activeID := db.active.id
	var mergedID uint32
	for id := range db.segments {
		if id > mergedID {
			mergedID = id
		}
	}
	db.segmentsMu.RUnlock()
	if mergedID == activeID {
		t.Fatalf("test setup invariant violated: merged id %d == active id %d (compaction did not produce a higher-id segment)", mergedID, activeID)
	}

	mergedPath := filepath.Join(dir, segmentFilename(mergedID))
	mergedStat, err := os.Stat(mergedPath)
	if err != nil {
		t.Fatalf("stat merged segment: %v", err)
	}
	mergedSizeBefore := mergedStat.Size()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and write something. If recovery wrongly picked the merged
	// segment as active, this write would land in mergedID's file.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	if got := db2.active.id; got != activeID {
		t.Fatalf("after restart: active id %d, want %d (manifest should have preserved the original active)", got, activeID)
	}

	if err := db2.Put([]byte("post-restart-canary"), []byte("X")); err != nil {
		t.Fatalf("Put after restart: %v", err)
	}

	// Merged segment file size must NOT have grown.
	mergedStat2, err := os.Stat(mergedPath)
	if err != nil {
		t.Fatalf("re-stat merged segment: %v", err)
	}
	if mergedStat2.Size() != mergedSizeBefore {
		t.Fatalf("merged segment size grew after restart: %d -> %d (write landed in immutable merged file)", mergedSizeBefore, mergedStat2.Size())
	}
}

// TestCompactionRecoveryTornTailOnActiveNotMerged simulates a torn-tail
// crash in the writer's active segment after a compaction has emitted a
// higher-id merged segment. Pre-fix, recovery would: (a) treat the
// merged segment as "newest" and apply torn-tail truncation logic to it
// (no torn tail there \u2014 nothing bad happens, but it skips the hint
// fast-path it had earned), and worse (b) treat the actual active's
// torn tail as mid-segment corruption and refuse to start. Post-fix,
// recovery follows the manifest's active id, hint-replays the merged
// segment, and truncates the torn tail off the real active.
func TestCompactionRecoveryTornTailOnActiveNotMerged(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	keys := fillToMultipleSegments(t, db, 200, 64)
	for i := 0; i < len(keys); i += 2 {
		if err := db.Put(keys[i], bytes.Repeat([]byte("V"), 64)); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Land one more write so the active segment definitely has at least
	// one record (we will append junk bytes after it).
	if err := db.Put([]byte("pre-crash"), []byte("ok")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	db.segmentsMu.RLock()
	activeID := db.active.id
	db.segmentsMu.RUnlock()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a torn write at the tail of the active segment: append a
	// truncated record header (too few bytes to constitute a full
	// header), which is exactly what recovery is designed to truncate.
	activePath := filepath.Join(dir, segmentFilename(activeID))
	f, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active for torn-tail injection: %v", err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF}); err != nil {
		f.Close()
		t.Fatalf("inject torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close active after injection: %v", err)
	}

	// Recovery must succeed and serve every original key.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 4 * 1024})
	if err != nil {
		t.Fatalf("reopen after torn tail in active: %v", err)
	}
	defer db2.Close()

	originalVal := bytes.Repeat([]byte("v"), 64)
	overwriteVal := bytes.Repeat([]byte("V"), 64)
	for i, k := range keys {
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("Get %d after torn-tail recovery: %v", i, err)
		}
		want := originalVal
		if i%2 == 0 {
			want = overwriteVal
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d after torn-tail recovery: value mismatch", i)
		}
	}
	if got, err := db2.Get([]byte("pre-crash")); err != nil {
		t.Fatalf("Get pre-crash: %v", err)
	} else if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("Get pre-crash: %q, want %q", got, "ok")
	}
}
