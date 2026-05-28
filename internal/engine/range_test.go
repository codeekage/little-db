package engine

import (
	"bytes"
	"fmt"
	"testing"
)

// collectRange is a small helper: it runs ReadKeyRange and returns the visited
// keys (as strings) and values (as strings) in the order fn was invoked.
func collectRange(t *testing.T, db *DB, start, end []byte) (keys, values []string) {
	t.Helper()
	err := db.ReadKeyRange(start, end, func(k, v []byte) bool {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return true
	})
	if err != nil {
		t.Fatalf("ReadKeyRange(%q,%q): %v", start, end, err)
	}
	return
}

func TestReadKeyRangeSortedAndHalfOpen(t *testing.T) {
	db := openTestDB(t, false)

	// Insert in non-sorted order; ReadKeyRange must return sorted.
	pairs := []struct{ k, v string }{
		{"gamma", "g"},
		{"alpha", "a"},
		{"epsilon", "e"},
		{"beta", "b"},
		{"delta", "d"},
	}
	for _, p := range pairs {
		if err := db.Put([]byte(p.k), []byte(p.v)); err != nil {
			t.Fatalf("Put %s: %v", p.k, err)
		}
	}

	// Full scan: nil/nil.
	keys, vals := collectRange(t, db, nil, nil)
	wantKeys := []string{"alpha", "beta", "delta", "epsilon", "gamma"}
	wantVals := []string{"a", "b", "d", "e", "g"}
	if !equalStrings(keys, wantKeys) {
		t.Fatalf("full scan keys = %v, want %v", keys, wantKeys)
	}
	if !equalStrings(vals, wantVals) {
		t.Fatalf("full scan vals = %v, want %v", vals, wantVals)
	}

	// Half-open [beta, epsilon): includes beta, delta; excludes epsilon.
	keys, _ = collectRange(t, db, []byte("beta"), []byte("epsilon"))
	if !equalStrings(keys, []string{"beta", "delta"}) {
		t.Fatalf("[beta,epsilon) keys = %v, want [beta delta]", keys)
	}

	// nil start, bounded end: < epsilon.
	keys, _ = collectRange(t, db, nil, []byte("epsilon"))
	if !equalStrings(keys, []string{"alpha", "beta", "delta"}) {
		t.Fatalf("[nil,epsilon) keys = %v", keys)
	}

	// bounded start, nil end: >= delta.
	keys, _ = collectRange(t, db, []byte("delta"), nil)
	if !equalStrings(keys, []string{"delta", "epsilon", "gamma"}) {
		t.Fatalf("[delta,nil) keys = %v", keys)
	}
}

func TestReadKeyRangeInvertedAndEmpty(t *testing.T) {
	db := openTestDB(t, false)
	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Inverted range: start > end. Must visit nothing without error.
	keys, _ := collectRange(t, db, []byte("z"), []byte("a"))
	if len(keys) != 0 {
		t.Fatalf("inverted range visited %v, want empty", keys)
	}

	// Empty equal range: start == end. Half-open => empty.
	keys, _ = collectRange(t, db, []byte("b"), []byte("b"))
	if len(keys) != 0 {
		t.Fatalf("equal-bounds range visited %v, want empty", keys)
	}

	// Range that matches nothing in the keyspace.
	keys, _ = collectRange(t, db, []byte("p"), []byte("q"))
	if len(keys) != 0 {
		t.Fatalf("non-matching range visited %v, want empty", keys)
	}
}

func TestReadKeyRangeEarlyStop(t *testing.T) {
	db := openTestDB(t, false)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := db.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	var seen []string
	err := db.ReadKeyRange(nil, nil, func(k, v []byte) bool {
		seen = append(seen, string(k))
		return string(k) != "c" // stop after visiting "c"
	})
	if err != nil {
		t.Fatalf("ReadKeyRange: %v", err)
	}
	if !equalStrings(seen, []string{"a", "b", "c"}) {
		t.Fatalf("early-stop visited %v, want [a b c]", seen)
	}
}

func TestReadKeyRangeExcludesTombstones(t *testing.T) {
	db := openTestDB(t, false)
	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete b: %v", err)
	}

	keys, _ := collectRange(t, db, nil, nil)
	if !equalStrings(keys, []string{"a", "c"}) {
		t.Fatalf("tombstoned scan = %v, want [a c]", keys)
	}
}

func TestReadKeyRangeReturnsLatestValue(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	_, vals := collectRange(t, db, nil, nil)
	if len(vals) != 1 || vals[0] != "v2" {
		t.Fatalf("vals = %v, want [v2]", vals)
	}
}

func TestReadKeyRangeAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 256, // tiny: forces rotation after a few writes
		SyncOnPut:      false,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write enough keys (with non-trivial values) to span several segments.
	const n = 20
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		v := bytes.Repeat([]byte{byte('a' + i%26)}, 32)
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Sanity: we should actually have rotated.
	if len(db.segments) < 2 {
		t.Fatalf("expected segment rotation, got %d segments", len(db.segments))
	}

	keys, vals := collectRange(t, db, nil, nil)
	if len(keys) != n {
		t.Fatalf("scan returned %d keys, want %d", len(keys), n)
	}
	for i, k := range keys {
		wantK := fmt.Sprintf("key-%03d", i)
		if k != wantK {
			t.Fatalf("keys[%d] = %q, want %q", i, k, wantK)
		}
		wantV := bytes.Repeat([]byte{byte('a' + i%26)}, 32)
		if !bytes.Equal([]byte(vals[i]), wantV) {
			t.Fatalf("vals[%d] = %q, want %q", i, vals[i], wantV)
		}
	}

	// Bounded sub-range within the multi-segment dataset.
	keys, _ = collectRange(t, db, []byte("key-005"), []byte("key-010"))
	want := []string{"key-005", "key-006", "key-007", "key-008", "key-009"}
	if !equalStrings(keys, want) {
		t.Fatalf("bounded multi-seg keys = %v, want %v", keys, want)
	}
}

func TestReadKeyRangeNilCallback(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.ReadKeyRange(nil, nil, nil); err == nil {
		t.Fatalf("expected error for nil callback")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
