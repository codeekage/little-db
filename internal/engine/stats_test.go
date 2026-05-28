package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStatsFreshDBIsZero confirms a freshly opened DB has zero keys and a
// near-zero (only header-prefixed empty segment file) bytes count. We assert
// KeyCount == 0 strictly and BytesOnDisk == 0 since segment files start
// empty (we do not write a header at create time).
func TestStatsFreshDBIsZero(t *testing.T) {
	db := openTestDB(t, false)
	s := db.Stats()
	if s.KeyCount != 0 {
		t.Fatalf("fresh DB KeyCount: got %d, want 0", s.KeyCount)
	}
	if s.BytesOnDisk != 0 {
		t.Fatalf("fresh DB BytesOnDisk: got %d, want 0", s.BytesOnDisk)
	}
}

// TestStatsTracksPutsAndDeletes asserts:
//   - KeyCount goes up with unique Put and down with Delete of an existing key.
//   - BytesOnDisk monotonically grows with every successful mutation
//     (including tombstones, since the engine is append-only).
func TestStatsTracksPutsAndDeletes(t *testing.T) {
	db := openTestDB(t, false)

	prev := db.Stats()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		cur := db.Stats()
		if cur.KeyCount != prev.KeyCount+1 {
			t.Fatalf("Put %s: KeyCount %d -> %d (want +1)", key, prev.KeyCount, cur.KeyCount)
		}
		if cur.BytesOnDisk <= prev.BytesOnDisk {
			t.Fatalf("Put %s: BytesOnDisk did not grow (%d -> %d)", key, prev.BytesOnDisk, cur.BytesOnDisk)
		}
		prev = cur
	}

	// Delete half the keys.
	for i := 0; i < 25; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Delete(key); err != nil {
			t.Fatalf("Delete %s: %v", key, err)
		}
		cur := db.Stats()
		if cur.KeyCount != prev.KeyCount-1 {
			t.Fatalf("Delete %s: KeyCount %d -> %d (want -1)", key, prev.KeyCount, cur.KeyCount)
		}
		// Tombstones still append bytes.
		if cur.BytesOnDisk <= prev.BytesOnDisk {
			t.Fatalf("Delete %s: BytesOnDisk did not grow (tombstone should append): %d -> %d", key, prev.BytesOnDisk, cur.BytesOnDisk)
		}
		prev = cur
	}

	final := db.Stats()
	if final.KeyCount != 25 {
		t.Fatalf("final KeyCount: got %d, want 25", final.KeyCount)
	}
}

// TestStatsAfterCompactionShrinks asserts that after compaction reclaims
// space, BytesOnDisk reflects the merged-segment size, not the pre-compaction
// total. KeyCount is unchanged.
func TestStatsAfterCompactionShrinks(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 32 * 1024, // 32 KiB, forces many sealed segments
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// 10 distinct keys, each rewritten many times so the bulk of every
	// sealed segment is superseded bytes the compactor can reclaim.
	val := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("k%02d", i%10))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	before := db.Stats()
	if before.KeyCount != 10 {
		t.Fatalf("pre-compact KeyCount: got %d, want 10", before.KeyCount)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after := db.Stats()
	if after.KeyCount != 10 {
		t.Fatalf("post-compact KeyCount: got %d, want 10", after.KeyCount)
	}
	if after.BytesOnDisk >= before.BytesOnDisk {
		t.Fatalf("post-compact BytesOnDisk did not shrink: before=%d after=%d", before.BytesOnDisk, after.BytesOnDisk)
	}
}

// TestStatsAfterCloseReturnsLastKnown asserts the documented closed-DB
// contract: Stats() does NOT return ErrDBClosed (it has no error return);
// instead it returns the snapshot captured just before teardown.
func TestStatsAfterCloseReturnsLastKnown(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 7; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	preClose := db.Stats()
	if preClose.KeyCount != 7 {
		t.Fatalf("pre-close KeyCount: got %d, want 7", preClose.KeyCount)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	postClose := db.Stats()
	if postClose != preClose {
		t.Fatalf("post-Close Stats differs from pre-Close: pre=%+v post=%+v", preClose, postClose)
	}
}

// TestStatsConcurrentWithMutations exercises Stats under -race alongside
// concurrent Put/Delete/Compact and confirms the documented lock order
// (segmentsMu.RLock → keydir.mu.RLock) does not deadlock or trip the race
// detector. The assertions are intentionally weak — this test exists to
// catch races and deadlocks, not to pin specific counter values.
func TestStatsConcurrentWithMutations(t *testing.T) {
	db := openTestDB(t, false)

	const duration = 800 * time.Millisecond
	deadline := time.Now().Add(duration)
	var stop atomic.Bool

	var wg sync.WaitGroup
	// Several writer goroutines.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				key := []byte(fmt.Sprintf("w%d-k%d", id, i%100))
				if i%5 == 0 {
					_ = db.Delete(key)
				} else {
					_ = db.Put(key, []byte("value"))
				}
				i++
			}
		}(w)
	}

	// One compactor pumper.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := db.Compact(); err != nil && !errors.Is(err, ErrDBClosed) {
				// Compact failures during shutdown are tolerated; everything
				// else should be reported.
				t.Errorf("Compact: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Several Stats readers.
	var statsCalls atomic.Uint64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = db.Stats()
				statsCalls.Add(1)
			}
		}()
	}

	time.Sleep(time.Until(deadline))
	stop.Store(true)
	wg.Wait()

	if statsCalls.Load() == 0 {
		t.Fatal("Stats was never called")
	}
	// Final Stats call should still succeed against a live DB.
	_ = db.Stats()
}

// TestStatsConcurrentWithClose exercises the exact race the cachedStats
// fallback is designed to absorb: a tight Stats() loop on one goroutine
// while another goroutine calls Close(). Under -race this should never
// observe a torn read, panic on a nil segments map, or trip the detector.
//
// After Close returns, Stats() must keep returning the snapshot captured
// just before teardown — that's the documented contract for monitoring
// consumers that don't want to special-case shutdown.
func TestStatsConcurrentWithClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Seed some state so the snapshot has non-zero fields. A zero snapshot
	// would also be a "valid" but uninteresting answer; non-zero forces
	// the test to distinguish "cached" from "fell through to Stats{}".
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("value")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	var reads atomic.Uint64
	var lastSeen atomic.Pointer[Stats]

	// Several readers hammering Stats(). Each captures the most recent
	// non-zero observation so we can assert continuity across Close.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				s := db.Stats()
				if s.KeyCount > 0 || s.BytesOnDisk > 0 {
					sCopy := s
					lastSeen.Store(&sCopy)
				}
				reads.Add(1)
			}
		}()
	}

	// Let readers warm up so we have measurements pre-Close.
	time.Sleep(10 * time.Millisecond)

	// Close racing against the readers. The readers keep going AFTER
	// Close returns to confirm the cached snapshot is observable.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Let readers run against the closed DB for a beat, then stop.
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if reads.Load() == 0 {
		t.Fatal("Stats was never called")
	}

	// Post-Close Stats() must still report the cached snapshot —
	// non-zero key count, non-zero bytes — not the zero value.
	post := db.Stats()
	if post.KeyCount == 0 {
		t.Fatalf("post-Close Stats returned zero KeyCount; expected cached non-zero snapshot: %+v", post)
	}
	if post.BytesOnDisk == 0 {
		t.Fatalf("post-Close Stats returned zero BytesOnDisk; expected cached non-zero snapshot: %+v", post)
	}
	// And it must equal whatever the last live read observed (the
	// snapshot is captured under segmentsMu.Lock so any read that
	// resolved against a live segments map matches the cached value
	// only modulo writes that happened in between — but no writes
	// happen after Put returned and before Close, so equality holds).
	if seen := lastSeen.Load(); seen != nil && *seen != post {
		t.Fatalf("post-Close cached Stats differs from last live observation: cached=%+v live=%+v", post, *seen)
	}
}
