package engine

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCloseRacingWithGet is the reviewer-requested stress test for the
// segment-lifetime hazard fixed in batch 3b.i: prior to holding segmentsMu
// across the per-segment pread, Close could close a segment's file
// descriptor while a Get was mid-read, yielding EBADF or worse.
//
// Every key in this test is seeded before Close, so the acceptable
// outcomes for each concurrent Get are tight:
//   - the correct value bytes, or
//   - ErrDBClosed (the call observed closed=true under segmentsMu and bailed).
//
// In particular, ErrKeyNotFound is NOT acceptable here: that would mean
// Close has nilled out segments and Get is misreporting a present, seeded
// key as a miss. The Get path's re-check of db.closed under segmentsMu.RLock
// must turn that case into ErrDBClosed.
//
// Anything else \u2014 a panic, an EBADF, a wrong value, an unexpected error \u2014
// is a regression of the fix.
func TestCloseRacingWithGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 4 << 10, // 4 KiB, force many segment rotations
		SyncOnPut:      false,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Seed enough keys to occupy several segments. Each key's value is its
	// own decimal index so we can verify correctness on the reader side.
	const numKeys = 2000
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte(fmt.Sprintf("v%06d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	const readers = 32
	var (
		wg        sync.WaitGroup
		stop      atomic.Bool
		badResult atomic.Int64
	)

	// Spawn readers that hammer Get in tight loops across the keyspace.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// Deterministic-ish per-goroutine cursor.
			i := seed
			for !stop.Load() {
				k := []byte(fmt.Sprintf("k%06d", i%numKeys))
				want := []byte(fmt.Sprintf("v%06d", i%numKeys))
				got, err := db.Get(k)
				switch {
				case err == nil:
					if string(got) != string(want) {
						badResult.Add(1)
						return
					}
				case errors.Is(err, ErrDBClosed):
					// Expected once Close lands; stop this reader.
					return
				default:
					// Anything else \u2014 including ErrKeyNotFound for a seeded
					// key \u2014 is a regression.
					t.Errorf("unexpected Get error: %v", err)
					badResult.Add(1)
					return
				}
				i++
				runtime.Gosched()
			}
		}(r)
	}

	// Let readers warm up, then close while they're mid-flight.
	time.Sleep(5 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	if bad := badResult.Load(); bad != 0 {
		t.Fatalf("close-vs-get race: %d bad results", bad)
	}

	// Idempotency: second Close is a no-op and must not panic.
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
