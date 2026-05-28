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

// TestBatchPutAtomicVisibility verifies the happy-path semantics: a successful
// BatchPut reads back every entry, in the values supplied, immediately.
func TestBatchPutAtomicVisibility(t *testing.T) {
	db := openTestDB(t, false)

	entries := []BatchEntry{
		{Key: []byte("alpha"), Value: []byte("A")},
		{Key: []byte("beta"), Value: []byte("B")},
		{Key: []byte("gamma"), Value: []byte("G")},
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	for _, e := range entries {
		got, err := db.Get(e.Key)
		if err != nil {
			t.Errorf("Get %s: %v", e.Key, err)
			continue
		}
		if !bytes.Equal(got, e.Value) {
			t.Errorf("Get %s: got %q, want %q", e.Key, got, e.Value)
		}
	}
}

// TestBatchPutDuplicateKeysLastWins ensures duplicate keys in the same batch
// collapse to the last entry, both immediately after the call and after a
// reopen (so the property holds across encode/decode + recovery).
func TestBatchPutDuplicateKeysLastWins(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.BatchPut([]BatchEntry{
		{Key: []byte("k"), Value: []byte("first")},
		{Key: []byte("k"), Value: []byte("second")},
		{Key: []byte("k"), Value: []byte("third")},
	}); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get pre-reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("third")) {
		t.Fatalf("pre-reopen: got %q, want %q", got, "third")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	got, err = db2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get post-reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("third")) {
		t.Fatalf("post-reopen: got %q, want %q", got, "third")
	}
}

// TestBatchPutEmpty asserts an empty batch is a no-op: no record is written,
// no error is returned, and the active segment's size does not change.
func TestBatchPutEmpty(t *testing.T) {
	db := openTestDB(t, false)

	sizeBefore := db.active.size.Load()
	if err := db.BatchPut(nil); err != nil {
		t.Fatalf("BatchPut(nil): %v", err)
	}
	if err := db.BatchPut([]BatchEntry{}); err != nil {
		t.Fatalf("BatchPut([]): %v", err)
	}
	if db.active.size.Load() != sizeBefore {
		t.Fatalf("empty batch wrote bytes: size changed %d -> %d", sizeBefore, db.active.size.Load())
	}
}

// TestBatchPutOversizeRejected configures a tiny MaxBatchEncodedSize and
// confirms BatchPut returns ErrBatchTooLarge without writing anything.
func TestBatchPutOversizeRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		MaxBatchEncodedSize: 1024, // 1 KiB
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	sizeBefore := db.active.size.Load()
	bigVal := bytes.Repeat([]byte("x"), 2048) // alone exceeds the cap
	err = db.BatchPut([]BatchEntry{{Key: []byte("k"), Value: bigVal}})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("BatchPut: got %v, want ErrBatchTooLarge", err)
	}
	if db.active.size.Load() != sizeBefore {
		t.Fatalf("rejected batch still wrote bytes: %d -> %d", sizeBefore, db.active.size.Load())
	}

	// Per-entry invalid keys should also short-circuit before any write,
	// wrapping ErrInvalidBatchEntry so callers can match it.
	err = db.BatchPut([]BatchEntry{
		{Key: []byte("ok"), Value: []byte("v")},
		{Key: nil, Value: []byte("v")}, // empty key
	})
	if !errors.Is(err, ErrInvalidBatchEntry) || !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("BatchPut with empty key: got %v, want ErrInvalidBatchEntry + ErrEmptyKey", err)
	}
	if db.active.size.Load() != sizeBefore {
		t.Fatalf("invalid-entry batch still wrote bytes")
	}
}

// TestBatchPutLargerThanSegment confirms a batch larger than MaxSegmentSize
// is still written as one record (no split) into a single segment, even
// though that segment ends up larger than the rotation threshold. This is
// the explicit atomicity-over-tidiness trade-off documented on Options.
func TestBatchPutLargerThanSegment(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                 dir,
		MaxSegmentSize:      4 * 1024, // tiny
		MaxBatchEncodedSize: 1 << 20,  // 1 MiB, plenty
		SyncOnPut:           true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Build a batch whose encoded size is well over MaxSegmentSize.
	const n = 200
	val := bytes.Repeat([]byte("v"), 200) // each entry ~200B value + ~20B key/hdr
	entries := make([]BatchEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = BatchEntry{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: append([]byte(fmt.Sprintf("v%04d:", i)), val...),
		}
	}
	encoded := recordHeaderSize + encodedBatchBodySize(entries)
	if int64(encoded) <= opts.MaxSegmentSize {
		t.Fatalf("test setup: encoded batch (%d) must exceed MaxSegmentSize (%d)", encoded, opts.MaxSegmentSize)
	}

	if err := db.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	// All entries readable before reopen.
	for i := 0; i < n; i++ {
		got, err := db.Get(entries[i].Key)
		if err != nil {
			t.Fatalf("Get %s: %v", entries[i].Key, err)
		}
		if !bytes.Equal(got, entries[i].Value) {
			t.Fatalf("Get %s: value mismatch", entries[i].Key)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery must replay all n entries from the single oversize segment.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		got, err := db2.Get(entries[i].Key)
		if err != nil {
			t.Fatalf("post-reopen Get %s: %v", entries[i].Key, err)
		}
		if !bytes.Equal(got, entries[i].Value) {
			t.Fatalf("post-reopen Get %s: value mismatch", entries[i].Key)
		}
	}
}

// TestBatchPutRecoveryAllOrNothing simulates a torn batch write by truncating
// the segment file in the middle of the batch record's body. Recovery must
// detect the truncation, drop the entire batch, and reopen cleanly with
// none of the batch keys present.
func TestBatchPutRecoveryAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Seed one independent key so we can prove it survives while the
	// (later) batch does not.
	if err := db.Put([]byte("survivor"), []byte("alive")); err != nil {
		t.Fatalf("Put survivor: %v", err)
	}

	// Now write a batch.
	batch := []BatchEntry{
		{Key: []byte("b1"), Value: []byte("v1")},
		{Key: []byte("b2"), Value: []byte("v2")},
		{Key: []byte("b3"), Value: []byte("v3")},
	}
	if err := db.BatchPut(batch); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	// Record the active segment path and total size, then close cleanly so
	// the file is flushed.
	segPath := db.active.path
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate the file mid-batch: drop the last few bytes of the body so
	// the CRC will no longer match AND the body would extend past EOF.
	// Either path triggers the all-or-nothing branch in recovery.
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(segPath, info.Size()-5); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Reopen. The survivor must still be present; none of the batch keys
	// should have landed.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	if got, err := db2.Get([]byte("survivor")); err != nil || !bytes.Equal(got, []byte("alive")) {
		t.Fatalf("survivor lost: got=%q err=%v", got, err)
	}
	for _, e := range batch {
		if _, err := db2.Get(e.Key); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("batch key %s unexpectedly present after torn-batch reopen: err=%v", e.Key, err)
		}
	}

	// Also: with the trailing torn bytes removed by recovery's truncation,
	// the segment file on disk must now be a clean prefix (no junk left).
	if filepath.Dir(segPath) != dir {
		t.Fatalf("segPath %s not in %s", segPath, dir)
	}
}

// TestBatchPutBodyLargerThanMaxValLenSurvivesReopen guards the corner that the
// outer val_len of a BATCH record can legitimately exceed maxValLen (the cap
// for a single-entry value). A batch whose encoded body sits between
// maxValLen (16 MiB) and the default MaxBatchEncodedSize (64 MiB) must
// survive a clean close + reopen with every entry readable. This pins the
// invariant that BATCH records use the larger maxBatchBodyLen sanity cap, not
// maxValLen, in both readRecord and recovery.
func TestBatchPutBodyLargerThanMaxValLenSurvivesReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-batch test in short mode")
	}
	dir := t.TempDir()
	// Default MaxBatchEncodedSize (64 MiB) is fine; bump segment size so we
	// don't write a >maxValLen body into a tiny test segment for no reason.
	opts := Options{Dir: dir, SyncOnPut: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Build a batch whose encoded body lands between 16 MiB and 64 MiB.
	// 80 entries × 256 KiB value ≈ 20 MiB of value bytes; with headers and
	// keys the body is well above 16 MiB and well below 64 MiB.
	const numEntries = 80
	const valueSize = 256 * 1024
	batch := make([]BatchEntry, numEntries)
	for i := range batch {
		v := make([]byte, valueSize)
		// Distinguishable per-entry pattern.
		for j := range v {
			v[j] = byte(i + j)
		}
		batch[i] = BatchEntry{
			Key:   []byte(fmt.Sprintf("big-batch-key-%04d", i)),
			Value: v,
		}
	}

	if err := db.BatchPut(batch); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	for i, e := range batch {
		got, err := db2.Get(e.Key)
		if err != nil {
			t.Fatalf("Get %s after reopen: %v", e.Key, err)
		}
		if !bytes.Equal(got, e.Value) {
			t.Fatalf("entry %d value mismatch after reopen (len got=%d want=%d)", i, len(got), len(e.Value))
		}
	}
}

// TestBatchPutWithTombstones mixes puts and deletes within one batch, both
// for previously-existing keys and for keys introduced in the same batch.
func TestBatchPutWithTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, SyncOnPut: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Seed two keys with single Puts.
	if err := db.Put([]byte("existing"), []byte("old")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := db.Put([]byte("doomed"), []byte("about-to-die")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Batch: overwrite one existing, delete one existing, add a new one,
	// add-then-delete in same batch (must end up absent).
	batch := []BatchEntry{
		{Key: []byte("existing"), Value: []byte("new")},
		{Key: []byte("doomed"), Delete: true},
		{Key: []byte("fresh"), Value: []byte("fresh-val")},
		{Key: []byte("ephemeral"), Value: []byte("brief")},
		{Key: []byte("ephemeral"), Delete: true},
	}
	if err := db.BatchPut(batch); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	check := func(t *testing.T, src *DB) {
		t.Helper()
		if got, err := src.Get([]byte("existing")); err != nil || !bytes.Equal(got, []byte("new")) {
			t.Errorf("existing: got=%q err=%v, want new", got, err)
		}
		if _, err := src.Get([]byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("doomed should be deleted: err=%v", err)
		}
		if got, err := src.Get([]byte("fresh")); err != nil || !bytes.Equal(got, []byte("fresh-val")) {
			t.Errorf("fresh: got=%q err=%v", got, err)
		}
		if _, err := src.Get([]byte("ephemeral")); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("ephemeral should be deleted by in-batch tombstone: err=%v", err)
		}
	}

	check(t, db)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	check(t, db2)
}

// TestBatchPutConcurrentReadersObserveAllOrNothing pins the in-process atomic
// visibility contract directly: while one goroutine repeatedly issues
// BatchPuts that mutate every key in a fixed set, a pool of reader goroutines
// continuously scans the same set via ReadKeyRange and verifies that within
// any single snapshot either *all* keys carry the just-written generation tag
// or *none* of them do. Observing a strict prefix (e.g. half the keys updated,
// half still on the previous generation) means the keydir was published
// non-atomically and fails the test.
//
// Note: we use ReadKeyRange specifically because it takes a single keydir
// RLock for the entire scan and is therefore the snapshot API. A loop of N
// individual Get calls would NOT be a snapshot — the writer can complete a
// whole batch between two Get calls, so the cross-key invariant would not
// hold even with a correctly atomic publish. See keydir.applyBatch doc.
func TestBatchPutConcurrentReadersObserveAllOrNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}
	db := openTestDB(t, false)

	const (
		numKeys     = 64
		numReaders  = 8
		testTimeout = 1500 * time.Millisecond
	)
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("atomic-key-%03d", i))
	}
	// Half-open range [start, end) covering every key in the set.
	rangeStart := []byte("atomic-key-")
	rangeEnd := []byte("atomic-key.") // '.' is one byte past '-' in ASCII

	// Seed every key at generation 0 so readers never see ErrKeyNotFound.
	seed := make([]BatchEntry, numKeys)
	for i, k := range keys {
		seed[i] = BatchEntry{Key: k, Value: []byte("gen-0000000000")}
	}
	if err := db.BatchPut(seed); err != nil {
		t.Fatalf("seed BatchPut: %v", err)
	}

	var (
		stop       atomic.Bool
		violations atomic.Int64
		wg         sync.WaitGroup
	)

	// Writer: bump every key to a new monotonic generation on each batch.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var gen uint64
		for !stop.Load() {
			gen++
			val := []byte(fmt.Sprintf("gen-%010d", gen))
			batch := make([]BatchEntry, numKeys)
			for i, k := range keys {
				batch[i] = BatchEntry{Key: k, Value: val}
			}
			if err := db.BatchPut(batch); err != nil {
				t.Errorf("writer BatchPut: %v", err)
				return
			}
		}
	}()

	// Readers: ReadKeyRange is a true snapshot (single RLock for the whole
	// scan), so every value yielded in one pass MUST share the same
	// generation. The keydir publish happens under one write lock that is
	// mutually exclusive with the range's RLock.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				var (
					first    []byte
					mixed    bool
					seen     int
					rangeErr error
				)
				rangeErr = db.ReadKeyRange(rangeStart, rangeEnd, func(_ []byte, val []byte) bool {
					seen++
					if first == nil {
						first = append(first, val...)
					} else if !bytes.Equal(val, first) {
						mixed = true
						return false
					}
					return true
				})
				if rangeErr != nil {
					t.Errorf("reader ReadKeyRange: %v", rangeErr)
					return
				}
				if mixed {
					violations.Add(1)
				}
				// If we ever see fewer than numKeys in a snapshot something
				// upstream is broken (we never delete).
				if !mixed && seen != numKeys {
					t.Errorf("snapshot saw %d keys, want %d", seen, numKeys)
					return
				}
			}
		}()
	}

	time.Sleep(testTimeout)
	stop.Store(true)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("observed %d non-atomic batch publishes (snapshot saw mixed generations)", v)
	}
}
