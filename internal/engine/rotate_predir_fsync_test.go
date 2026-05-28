package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRotatePreManifestDirSyncFailureLeavesNoOrphan exercises the fix for
// the post-v0.1.0 review finding that rotateActive published the manifest
// without first fsyncing the parent directory of the freshly-created
// next .seg file. In that gap a host crash could leave a durable
// manifest pointing at a .seg dirent the kernel never persisted, so
// Open fails with "manifest live segment missing".
//
// Contract under test for the pre-manifest dir-fsync failure path:
//
//  1. The triggering write returns an error wrapping the syncDir failure.
//  2. No manifest move happens: the engine remains writable after the
//     hook is disarmed (the failure is recoverable, distinct from the
//     post-manifest uncertain branch which is sticky).
//  3. The would-be next .seg file is unlinked, so the next rotation can
//     reuse that id without "file exists" failures.
//  4. Reads of previously-written keys keep working.
func TestRotatePreManifestDirSyncFailureLeavesNoOrphan(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 256,
		SyncOnPut:      true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Put([]byte("seed"), bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Snapshot existing .seg files so we can tell whether the failed
	// rotation left an orphan behind.
	before := listSegFiles(t, dir)

	injected := errors.New("simulated pre-manifest dir fsync failure")
	testRotatePreManifestSyncHook = func() error { return injected }
	t.Cleanup(func() { testRotatePreManifestSyncHook = nil })

	// Trigger rotation. The hook fires AFTER createSegment but BEFORE
	// writeManifest, so the manifest never moves; rotateActive must
	// unlink the orphan next .seg and return an error.
	err = db.Put([]byte("trigger"), bytes.Repeat([]byte("b"), 200))
	if err == nil {
		t.Fatalf("Put: want error from pre-manifest dir-fsync failure, got nil")
	}
	if errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("Put: pre-manifest failure must be recoverable, got sticky ErrWritesDisabled: %v", err)
	}
	if !strings.Contains(err.Error(), "fsync data dir before manifest") {
		t.Fatalf("Put: want wrapped 'fsync data dir before manifest', got %v", err)
	}

	// No new .seg files: the orphan was unlinked.
	testRotatePreManifestSyncHook = nil
	after := listSegFiles(t, dir)
	if !sameStringSet(before, after) {
		t.Fatalf("unexpected .seg files after failed rotation:\n  before=%v\n  after=%v", before, after)
	}

	// Engine stays writable: the failure was bounded to one rotation
	// attempt, not the sticky uncertain branch.
	if err := db.Put([]byte("postfix"), []byte("ok")); err != nil {
		t.Fatalf("Put after disarmed pre-manifest hook: %v", err)
	}
	got, err := db.Get([]byte("seed"))
	if err != nil {
		t.Fatalf("Get(seed): %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("Get(seed): wrong value after failed rotation")
	}
}

func listSegFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			out = append(out, e.Name())
		}
	}
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}
