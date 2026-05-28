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

// TestRotateUncertainDisablesWrites exercises the fix for the post-v0.1.0
// review finding that rotateActive would install the new active segment
// and keep accepting writes even when the directory fsync after the
// manifest rename did not confirm. If the rename later reverts on a host
// crash, those acknowledged writes would silently vanish.
//
// Contract under test:
//
//  1. The triggering write (the one whose rotation hit the uncertain
//     manifest publish) returns ErrWritesDisabled.
//  2. Subsequent Put / Delete / BatchPut also return ErrWritesDisabled.
//  3. Reads against keys written before the rotation continue to succeed.
//  4. The newly-created segment file is NOT unlinked from disk, so the
//     next clean Open can reconcile whichever side of the rename survived.
func TestRotateUncertainDisablesWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		MaxSegmentSize: 256, // very small so the second Put forces a rotation
		SyncOnPut:      true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed one key so we have something to read back after writes are
	// disabled. This first Put fits inside the active segment and does
	// not trigger rotation.
	if err := db.Put([]byte("seed"), bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Arm the manifest hook so the NEXT writeManifest call (triggered by
	// the rotation below) returns ErrManifestPublishedButUncertain after
	// the rename, simulating a directory-fsync failure.
	injected := errors.New("simulated dir fsync failure")
	testManifestPostRenameHook = func(string) error { return injected }
	t.Cleanup(func() { testManifestPostRenameHook = nil })

	// This Put is large enough that appending it would exceed
	// MaxSegmentSize, forcing rotateActive. The hook fires, the rotation
	// hits the uncertain branch, and the engine enters the write-disabled
	// state. The Put itself must surface ErrWritesDisabled rather than
	// landing successfully (which would falsely tell the caller their
	// write was durable).
	err = db.Put([]byte("trigger"), bytes.Repeat([]byte("b"), 200))
	if !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("Put after uncertain rotation: want ErrWritesDisabled, got %v", err)
	}

	// Disarm so the assertion writes below don't keep tripping it; the
	// writesDisabled flag is sticky and the engine stays disabled
	// regardless.
	testManifestPostRenameHook = nil

	// Subsequent writes of every kind stay rejected.
	if err := db.Put([]byte("another"), []byte("x")); !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("follow-up Put: want ErrWritesDisabled, got %v", err)
	}
	if err := db.Delete([]byte("seed")); !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("follow-up Delete: want ErrWritesDisabled, got %v", err)
	}
	if err := db.BatchPut([]BatchEntry{{Key: []byte("k"), Value: []byte("v")}}); !errors.Is(err, ErrWritesDisabled) {
		t.Fatalf("follow-up BatchPut: want ErrWritesDisabled, got %v", err)
	}

	// Reads against pre-rotation data must still succeed: the prior
	// sealed segments are untouched and the keydir entry for "seed"
	// still points at a live file.
	got, err := db.Get([]byte("seed"))
	if err != nil {
		t.Fatalf("Get(seed) after writes disabled: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("Get(seed): wrong value")
	}

	// The next segment file must remain on disk. Reaching into the
	// segments map would be inappropriate (rotateActive deliberately
	// does NOT install next), so we infer the expected id from the
	// directory listing: the highest .seg file present after the failed
	// rotation is the one created and preserved.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var segFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".seg") {
			segFiles = append(segFiles, e.Name())
		}
	}
	// We expect at least two .seg files: the original active and the
	// freshly-created next that the failed rotation must NOT have
	// unlinked.
	if len(segFiles) < 2 {
		t.Fatalf("expected >= 2 segment files preserved on disk after uncertain rotation, found %d: %v",
			len(segFiles), segFiles)
	}

	// Sanity: every preserved .seg file is non-empty or at least
	// readable as a regular file. (The freshly-created next is allowed
	// to be empty — no writes landed in it.)
	for _, name := range segFiles {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("Stat %s: %v", name, err)
		}
		if !fi.Mode().IsRegular() {
			t.Fatalf("%s: not a regular file", name)
		}
	}

	_ = fmt.Sprintf // keep fmt import even if future edits trim usage
}
