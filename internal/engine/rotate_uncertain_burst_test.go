package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRotateUncertainBurstDisablesAllPeers exercises the round-5 finding
// that processBurst would keep servicing requests already drained from
// reqCh even after a peer hit the uncertain-rotation branch and flipped
// writesDisabled. submit() rejects new arrivals, but burst peers bypass
// that gate; they must also see ErrWritesDisabled.
//
// Contract under test: once any request in a burst observes the uncertain
// rotation, no later request — in the same burst OR any subsequent burst
// — may return nil. Returning nil would falsely acknowledge a write whose
// durability is unknown.
func TestRotateUncertainBurstDisablesAllPeers(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		// Tiny segment so every concurrent Put forces a rotation; this
		// maximises the chance that a burst contains both a triggering
		// request and one or more peers behind it.
		MaxSegmentSize:  256,
		SyncOnPut:       true,
		WriteQueueDepth: 64,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Arm the hook so the FIRST writeManifest after this point fails
	// with the uncertain-publish sentinel; later writeManifest calls (if
	// any) succeed.
	var hookFired atomic.Bool
	injected := errors.New("simulated dir fsync failure")
	testManifestPostRenameHook = func(string) error {
		if hookFired.CompareAndSwap(false, true) {
			return injected
		}
		return nil
	}
	t.Cleanup(func() { testManifestPostRenameHook = nil })

	const n = 32
	value := bytes.Repeat([]byte("b"), 200) // > MaxSegmentSize/2: each Put rotates

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		okCount     int
		disabled    int
		otherErr    error
		releaseGate = make(chan struct{})
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("k%02d", i))
			<-releaseGate
			err := db.Put(key, value)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
			case errors.Is(err, ErrWritesDisabled):
				disabled++
			default:
				if otherErr == nil {
					otherErr = err
				}
			}
		}(i)
	}
	close(releaseGate)
	wg.Wait()

	if otherErr != nil {
		t.Fatalf("unexpected error from concurrent Put: %v", otherErr)
	}
	if !hookFired.Load() {
		t.Fatalf("manifest hook never fired; test never exercised the uncertain branch")
	}
	if disabled == 0 {
		t.Fatalf("no Put returned ErrWritesDisabled; the uncertain branch was not surfaced to callers")
	}
	// At most one Put may have landed BEFORE the uncertain rotation
	// (the request that itself drove rotateActive into the hook). Every
	// subsequent burst peer must observe writesDisabled and bail. If we
	// see more than one nil, processBurst is still letting drained
	// peers append after a peer disabled writes — the bug this test
	// exists to catch.
	if okCount > 1 {
		t.Fatalf("processBurst kept servicing peers after writesDisabled flipped: %d Puts returned nil (want \u2264 1)", okCount)
	}

	// Sanity: the engine should stay disabled for any further call,
	// regardless of which burst they land in.
	if err := db.Put([]byte("tail"), []byte("x")); !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("post-burst Put: want ErrWritesDisabled, got %v", err)
	}
	if err := db.BatchPut([]BatchEntry{{Key: []byte("b"), Value: []byte("v")}}); !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("post-burst BatchPut: want ErrWritesDisabled, got %v", err)
	}
}
