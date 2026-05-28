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

func openTestDB(t *testing.T, syncOnPut bool) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 1 << 20, // 1 MiB, small to exercise rotation in tests
		SyncOnPut:      syncOnPut,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	dir := t.TempDir()

	// Negative MaxSegmentSize should be rejected.
	_, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: -1,
	})
	if err == nil {
		t.Fatal("Open with negative MaxSegmentSize should fail")
	}

	// Negative WriteQueueDepth should be rejected.
	_, err = Open(Options{
		Dir:             dir,
		WriteQueueDepth: -1,
	})
	if err == nil {
		t.Fatal("Open with negative WriteQueueDepth should fail")
	}
}

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, false)

	if err := db.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if err := db.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Put beta: %v", err)
	}

	got, err := db.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get alpha: %v", err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("alpha = %q, want %q", got, "one")
	}

	// Overwrite.
	if err := db.Put([]byte("alpha"), []byte("ONE")); err != nil {
		t.Fatalf("Put alpha v2: %v", err)
	}
	got, err = db.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get alpha v2: %v", err)
	}
	if !bytes.Equal(got, []byte("ONE")) {
		t.Fatalf("alpha v2 = %q, want %q", got, "ONE")
	}

	// Delete.
	if err := db.Delete([]byte("beta")); err != nil {
		t.Fatalf("Delete beta: %v", err)
	}
	if _, err := db.Get([]byte("beta")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get beta after delete: err = %v, want ErrKeyNotFound", err)
	}

	// Missing key.
	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrKeyNotFound", err)
	}
}

// TestReadAliasMatchesGet pins the public API surface documented in the
// assignment PDF and SPEC §4.2 (Read(key)). The engine exposes both `Read`
// and `Get`; they must be observationally identical for every input.
func TestReadAliasMatchesGet(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	gotGet, errGet := db.Get([]byte("k"))
	gotRead, errRead := db.Read([]byte("k"))
	if errGet != nil || errRead != nil {
		t.Fatalf("Get/Read errors: %v / %v", errGet, errRead)
	}
	if !bytes.Equal(gotGet, gotRead) {
		t.Fatalf("Read=%q != Get=%q", gotRead, gotGet)
	}

	// Same sentinel on miss.
	if _, err := db.Read([]byte("absent")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Read(absent): err = %v, want ErrKeyNotFound", err)
	}
	// Same validation on empty key.
	if _, err := db.Read(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Read(nil): err = %v, want ErrEmptyKey", err)
	}
}

func TestEmptyValue(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.Put([]byte("k"), []byte{}); err != nil {
		t.Fatalf("Put empty value: %v", err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty value: got %d bytes, want 0", len(got))
	}
}

func TestSegmentRotation(t *testing.T) {
	db := openTestDB(t, false)

	// Each record is roughly 21 (header) + 8 (key) + 1024 (value) ≈ 1053 bytes.
	// Writing ~2000 of them will force several segment rotations against the
	// 1 MiB cap configured in openTestDB.
	value := bytes.Repeat([]byte("x"), 1024)
	const n = 2000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%07d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	db.segmentsMu.RLock()
	segCount := len(db.segments)
	db.segmentsMu.RUnlock()
	if segCount < 2 {
		t.Fatalf("expected multiple segments after %d writes, got %d", n, segCount)
	}

	// Spot-check that the first, middle, and last writes are all readable —
	// covers values that live in already-rotated immutable segments and in
	// the current active segment.
	for _, i := range []int{0, n / 2, n - 1} {
		key := []byte(fmt.Sprintf("k%07d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("Get %d: value mismatch", i)
		}
	}
}

func TestRecordRoundTrip(t *testing.T) {
	// Direct test of the on-disk record codec, independent of the engine.
	cases := []struct {
		name string
		rec  record
	}{
		{"put", record{tstamp: 12345, flag: recordFlagPut, key: []byte("hello"), value: []byte("world")}},
		{"tombstone", record{tstamp: 67890, flag: recordFlagTombstone, key: []byte("gone"), value: nil}},
		{"empty value", record{tstamp: 1, flag: recordFlagPut, key: []byte("k"), value: []byte{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.rec.encodedSize())
			n, err := tc.rec.encode(buf)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if n != tc.rec.encodedSize() {
				t.Fatalf("encode wrote %d, want %d", n, tc.rec.encodedSize())
			}
			got, consumed, err := readRecord(bytes.NewReader(buf))
			if err != nil {
				t.Fatalf("readRecord: %v", err)
			}
			if consumed != n {
				t.Fatalf("readRecord consumed %d, want %d", consumed, n)
			}
			if got.flag != tc.rec.flag || got.tstamp != tc.rec.tstamp {
				t.Fatalf("header mismatch: got %+v", got)
			}
			if !bytes.Equal(got.key, tc.rec.key) {
				t.Fatalf("key mismatch: got %q want %q", got.key, tc.rec.key)
			}
			if !bytes.Equal(got.value, tc.rec.value) {
				t.Fatalf("value mismatch: got %q want %q", got.value, tc.rec.value)
			}
		})
	}
}

func TestCRCDetectsCorruption(t *testing.T) {
	rec := record{tstamp: 1, flag: recordFlagPut, key: []byte("k"), value: []byte("v")}
	buf := make([]byte, rec.encodedSize())
	if _, err := rec.encode(buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Flip a bit in the value.
	buf[len(buf)-1] ^= 0xFF
	if _, _, err := readRecord(bytes.NewReader(buf)); !errors.Is(err, errBadCRC) {
		t.Fatalf("expected errBadCRC, got %v", err)
	}
}

// TestConcurrentPutVsClose is a regression test for the submitMu fix: any
// Put that returns nil MUST land durably (visible after reopen with the
// expected value). Puts that observe closed=true return ErrDBClosed and
// must NOT land. We record every nil-returning Put into a sync.Map and
// assert exactly that set is present after reopen — catching both directions
// (a nil Put that vanishes, or an ErrDBClosed Put that landed anyway).
func TestConcurrentPutVsClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 1 << 20,
		SyncOnPut:      true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Don't use t.Cleanup since we reopen the DB below.

	const writers = 16
	const writesPerWriter = 100
	var (
		wg            sync.WaitGroup
		stop          atomic.Bool
		writesAborted int64
		// landed maps key -> expected value for every Put that returned nil.
		landed sync.Map
	)

	// Spawn writers that hammer Put while we Close.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter && !stop.Load(); i++ {
				k := fmt.Sprintf("w%02d_k%04d", seed, i)
				v := fmt.Sprintf("v%04d", i)
				err := db.Put([]byte(k), []byte(v))
				switch {
				case err == nil:
					landed.Store(k, v)
				case errors.Is(err, ErrDBClosed):
					atomic.AddInt64(&writesAborted, 1)
				default:
					t.Errorf("unexpected Put error: %v", err)
				}
			}
		}(w)
	}

	// Let writers warm up, then close while they're mid-flight.
	time.Sleep(5 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	// Verify EXACTLY the landed set is present after reopen — every key
	// for which Put returned nil must be present with the correct value,
	// AND every key for which Put returned ErrDBClosed (or never ran) must
	// be absent. Any drift either way is a correctness regression.
	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	var landedCount int64
	landed.Range(func(_, _ any) bool {
		landedCount++
		return true
	})

	for w := 0; w < writers; w++ {
		for i := 0; i < writesPerWriter; i++ {
			key := fmt.Sprintf("w%02d_k%04d", w, i)
			want, inLanded := landed.Load(key)
			got, err := db2.Get([]byte(key))
			switch {
			case inLanded && err != nil:
				t.Errorf("landed key %s missing after reopen: %v", key, err)
			case inLanded && string(got) != want.(string):
				t.Errorf("landed key %s: got %q, want %q", key, got, want)
			case !inLanded && err == nil:
				t.Errorf("key %s present after reopen but Put never returned nil", key)
			case !inLanded && !errors.Is(err, ErrKeyNotFound):
				t.Errorf("unexpected Get error for unlanded key %s: %v", key, err)
			}
		}
	}

	// Sanity: both sides of the race must fire, or the test isn't actually
	// exercising the submit/Close interleave it claims to test.
	if landedCount == 0 {
		t.Fatal("no writes landed; test did not exercise the race")
	}
	if writesAborted == 0 {
		t.Fatal("no writes aborted; test did not actually cross Close")
	}

	t.Logf("writers=%d, writes_landed=%d, writes_aborted=%d",
		writers, landedCount, writesAborted)
}

// TestReadKeyRangeCallbacksCanMutate is a regression test ensuring that
// ReadKeyRange releases segmentsMu before invoking the callback. The test
// exercises this by calling Put from inside the callback; without the
// release, Put would deadlock on submit -> writer -> segmentsMu.Lock.
// Delete and Close take the same submit/segmentsMu paths, so this single
// case covers the general "callback may re-enter the DB" contract.
func TestReadKeyRangeCallbacksCanMutate(t *testing.T) {
	db := openTestDB(t, false)

	// Seed a few keys.
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%02d", i))
		v := []byte(fmt.Sprintf("v%02d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// ReadKeyRange while mutating inside the callback.
	var processedCount int
	err := db.ReadKeyRange(nil, nil, func(key, value []byte) bool {
		processedCount++

		// Call Put from inside the callback. This should not deadlock
		// because ReadKeyRange releases segmentsMu before calling fn.
		mutateKey := []byte(fmt.Sprintf("mutated_%d", processedCount))
		mutateVal := []byte(fmt.Sprintf("val_%d", processedCount))
		if err := db.Put(mutateKey, mutateVal); err != nil {
			// ErrDBClosed is acceptable if Close raced; anything else fails.
			if !errors.Is(err, ErrDBClosed) {
				t.Errorf("Put from callback: %v", err)
			}
		}

		return true // continue iteration
	})
	if err != nil {
		t.Fatalf("ReadKeyRange: %v", err)
	}

	if processedCount != 10 {
		t.Fatalf("expected 10 keys, processed %d", processedCount)
	}

	// Verify the mutations landed.
	for i := 1; i <= 10; i++ {
		mutateKey := []byte(fmt.Sprintf("mutated_%d", i))
		if got, err := db.Get(mutateKey); err != nil {
			t.Errorf("Get mutated_%d: %v", i, err)
		} else if string(got) != fmt.Sprintf("val_%d", i) {
			t.Errorf("mutated_%d: got %q, want val_%d", i, got, i)
		}
	}
}
