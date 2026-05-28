package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRecoveryAfterCleanClose writes a mix of puts, overwrites, and deletes,
// closes the DB cleanly, reopens, and asserts that every key reflects the
// last write it received. This is the "happy path" durability test.
func TestRecoveryAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 64 * 1024, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 500
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Overwrite half of them with a different value, delete the other half.
	overwritten := bytes.Repeat([]byte("w"), 64)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		if i%2 == 0 {
			if err := db.Put(key, overwritten); err != nil {
				t.Fatalf("overwrite %d: %v", i, err)
			}
		} else {
			if err := db.Delete(key); err != nil {
				t.Fatalf("delete %d: %v", i, err)
			}
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		got, err := db2.Get(key)
		if i%2 == 0 {
			if err != nil {
				t.Fatalf("after reopen Get %d: %v", i, err)
			}
			if !bytes.Equal(got, overwritten) {
				t.Fatalf("after reopen key %d: value mismatch", i)
			}
		} else {
			if !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("after reopen Get %d: got err %v, want ErrKeyNotFound", i, err)
			}
		}
	}
}

// TestRecoveryAfterUncleanClose simulates a crash by abandoning the *DB
// (skipping Close so no final fsync runs) and then opening fresh against the
// same directory. With SyncOnPut=true every Put must survive the crash.
func TestRecoveryAfterUncleanClose(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 32 * 1024, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 300
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("crash-%04d", i))
		val := []byte(fmt.Sprintf("value-for-%d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Skip db.Close(). We need to release the flock without flushing the
	// active segment's bufio buffer (which would defeat the test point).
	// Simulate a process crash by:
	//   1) stopping the writer goroutine cleanly (so the test process does
	//      not leak it into subsequent tests),
	//   2) flushing each segment's bufio buffer into the kernel page cache
	//      (matching the engine's own behaviour: Put always flushes after
	//      append, but never fsyncs unless SyncOnPut is set),
	//   3) closing the underlying files WITHOUT fsync, and
	//   4) releasing the flock.
	db.closed.Store(true)
	close(db.done)
	db.writerWG.Wait()
	db.segmentsMu.Lock()
	for _, s := range db.segments {
		_ = s.flush()
		_ = s.file.Close()
	}
	db.releaseLock()
	db.segmentsMu.Unlock()

	// Reopen.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db2.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("crash-%04d", i))
		want := []byte(fmt.Sprintf("value-for-%d", i))
		got, err := db2.Get(key)
		if err != nil {
			t.Fatalf("post-crash Get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("post-crash key %d: got %q, want %q", i, got, want)
		}
	}
}

// TestRecoveryDiscardsTornTailRecord manually corrupts the tail of the
// newest segment to simulate a torn write (power loss in the middle of an
// append). Recovery must truncate the partial record and reopen cleanly.
func TestRecoveryDiscardsTornTailRecord(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 1 << 20, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		val := []byte(fmt.Sprintf("v%03d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append junk bytes that look like a half-written record to the newest
	// segment file. They are shorter than recordHeaderSize so readRecord will
	// return io.ErrUnexpectedEOF, which recovery treats as a tail torn write.
	segFile := filepath.Join(dir, segmentFilename(0))
	info, err := os.Stat(segFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	sizeBefore := info.Size()

	f, err := os.OpenFile(segFile, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05}); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

	// Reopen — recovery should truncate the junk and the 50 keys survive.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		want := []byte(fmt.Sprintf("v%03d", i))
		got, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get %d after recovery: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("recovery key %d: got %q want %q", i, got, want)
		}
	}

	// Verify the segment was truncated back to the last good offset.
	info2, err := os.Stat(segFile)
	if err != nil {
		t.Fatalf("stat after recovery: %v", err)
	}
	if info2.Size() != sizeBefore {
		t.Fatalf("segment size after recovery = %d, want %d (truncate failed)",
			info2.Size(), sizeBefore)
	}
}

// TestRecoveryBootstrapTornTailOnActive simulates a torn-tail crash on a
// pre-manifest fixture: the directory has .seg files but no MANIFEST (the
// case we treat as "first boot of a pre-4a DB"). Pre-fix, Open's bootstrap
// path left haveActive=false, so recover() treated every segment as
// sealed and a torn tail on the real active became a "sealed-segment
// corruption" error. Post-fix, the bootstrap path names the highest-id
// on-disk segment as the active before recover runs, so torn-tail
// truncation works the same as on a manifest-recorded restart.
func TestRecoveryBootstrapTornTailOnActive(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 200, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Force at least two segments so the bug is meaningful (the bootstrap
	// path with len(existing) > 0 is what the fix targets).
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		val := []byte(fmt.Sprintf("v%03d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected >= 2 segments, got %d", len(ids))
	}
	activeID := ids[len(ids)-1]

	// Delete MANIFEST to drive the bootstrap branch on the next Open.
	if err := os.Remove(filepath.Join(dir, manifestFilename)); err != nil {
		t.Fatalf("remove MANIFEST: %v", err)
	}

	// Inject a partial header at the tail of the active segment file.
	activePath := filepath.Join(dir, segmentFilename(activeID))
	info, err := os.Stat(activePath)
	if err != nil {
		t.Fatalf("stat active: %v", err)
	}
	sizeBefore := info.Size()
	f, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open active: %v", err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF}); err != nil {
		f.Close()
		t.Fatalf("inject torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close active: %v", err)
	}

	// Recovery must succeed and serve every previously-acked key.
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after bootstrap torn-tail: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		want := []byte(fmt.Sprintf("v%03d", i))
		got, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d: got %q want %q", i, got, want)
		}
	}

	// The active file must have been truncated back to the last good offset.
	info2, err := os.Stat(activePath)
	if err != nil {
		t.Fatalf("stat after recovery: %v", err)
	}
	if info2.Size() != sizeBefore {
		t.Fatalf("active size after recovery = %d, want %d (torn-tail truncate failed)",
			info2.Size(), sizeBefore)
	}

	// A manifest must have been rewritten by Open (bootstrap path).
	if _, err := os.Stat(filepath.Join(dir, manifestFilename)); err != nil {
		t.Fatalf("MANIFEST not rewritten after bootstrap: %v", err)
	}
}

// TestRecoveryRejectsTornTailOnSealedSegment pins the policy that only the
// newest segment is allowed to have a torn tail. Sealed segments were
// fsynced at rotation, so any partial-header / body-past-EOF on them is
// real corruption: silently truncating would erase records that previous
// Puts already acked. Open must fail loud instead.
func TestRecoveryRejectsTornTailOnSealedSegment(t *testing.T) {
	dir := t.TempDir()
	// Cap forces rotation: records are 21+4+4 = 29 bytes. Cap=32 means
	// each Put rotates first, so seg 0 contains the first record and
	// becomes sealed by the second Put.
	opts := Options{Dir: dir, MaxSegmentSize: 32, SyncOnPut: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("aaaa"), []byte("vvvv")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := db.Put([]byte("bbbb"), []byte("vvvv")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected >=2 segments to make seg 0 non-newest, got %d", len(ids))
	}

	// Append junk to seg 0 (sealed). This shape would be tolerated on
	// the newest segment but must NOT be tolerated here.
	seg0Path := filepath.Join(dir, segmentFilename(0))
	f, err := os.OpenFile(seg0Path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open seg 0: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

	if _, err := Open(opts); err == nil {
		t.Fatalf("expected Open to fail because sealed seg 0 has trailing junk, got nil")
	}
}

// TestRecoveryRejectsMidSegmentCorruption flips a bit in the middle of a
// segment (not at the tail). This is real corruption rather than a torn
// write, and recovery must refuse to silently lose data — Open returns an
// error.
func TestRecoveryRejectsMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 1 << 20, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("payload")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte well inside the segment file, in the *body* of a record
	// that is followed by more records. This is the canonical "real
	// corruption" signal: the record's claimed length still fits inside the
	// file, but its CRC no longer matches its bytes.
	//
	// Layout per record: 21 (hdr) + 3 (key "kNN") + 7 ("payload") = 31 bytes.
	// Record 1 starts at byte 31; its value spans bytes 55..61. Offset 58 is
	// in the middle of that value.
	segFile := filepath.Join(dir, segmentFilename(0))
	f, err := os.OpenFile(segFile, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 58); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	_, err = Open(opts)
	if err == nil {
		t.Fatalf("expected Open to fail on mid-segment corruption, got nil")
	}
	if !strings.Contains(err.Error(), "corruption") && !errors.Is(err, errBadCRC) {
		t.Fatalf("expected corruption error, got: %v", err)
	}
}

// TestRecoveryAcrossSegments writes enough data to span several segments,
// closes uncleanly, and verifies every key survives. Exercises the
// oldest-to-newest scan order.
func TestRecoveryAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 4 * 1024, SyncOnPut: true}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		val := bytes.Repeat([]byte{byte(i)}, 100)
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Update a few keys so we exercise newer-segment-wins logic.
	for _, i := range []int{0, 50, 100, 150} {
		key := []byte(fmt.Sprintf("k%05d", i))
		if err := db.Put(key, []byte("UPDATED")); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// Sanity: more than one segment exists on disk.
	db2.segmentsMu.RLock()
	segCount := len(db2.segments)
	db2.segmentsMu.RUnlock()
	if segCount < 2 {
		t.Fatalf("expected multiple segments after %d writes, got %d", n, segCount)
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		got, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		switch i {
		case 0, 50, 100, 150:
			if !bytes.Equal(got, []byte("UPDATED")) {
				t.Fatalf("key %d: want UPDATED, got %q", i, got)
			}
		default:
			want := bytes.Repeat([]byte{byte(i)}, 100)
			if !bytes.Equal(got, want) {
				t.Fatalf("key %d: value mismatch", i)
			}
		}
	}
}

// TestDataDirLockPreventsConcurrentOpen ensures two DB handles on the same
// directory cannot coexist — a flock on a LOCK file in the data directory
// guarantees only one writer per directory.
func TestDataDirLockPreventsConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxSegmentSize: 1 << 20}

	db1, err := Open(opts)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer db1.Close()

	if _, err := Open(opts); err == nil {
		t.Fatalf("expected second Open to fail with flock contention, got nil")
	}

	// After closing the first, a second Open must succeed.
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("second Open after first Close: %v", err)
	}
	_ = db2.Close()
}
