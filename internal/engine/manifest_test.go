package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// readManifestRaw is a test-only helper that returns the on-disk manifest as
// raw bytes for content assertions. Production code reads via readManifest
// which validates the schema; tests sometimes need to look at the literal
// JSON to assert format invariants.
func readManifestRaw(t *testing.T, dir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, manifestFilename))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	return b
}

// TestManifestBootstrappedOnFirstOpen verifies that opening a fresh empty
// directory writes a manifest that lists the just-created active segment.
// Without this, a host crash immediately after the first Put would lose
// the data — the active segment file would be on disk but the manifest
// would be missing, so on restart we'd bootstrap a NEW manifest from the
// directory listing and pick up the existing segment. The bootstrap path
// is correct, but only if the first Open wrote a manifest at all.
func TestManifestBootstrappedOnFirstOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, _, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest after first Open: %v", err)
	}
	if len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("manifest after fresh Open: got %v, want [0]", ids)
	}
}

// TestManifestUpdatedOnRotation forces a rotation by making MaxSegmentSize
// tiny and writing enough data to overflow it. The manifest must list both
// the old (now immutable) segment and the new active one. Otherwise a crash
// right after rotation would orphan the new file and lose its writes.
func TestManifestUpdatedOnRotation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 256, SyncOnPut: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Each Put encodes header(21) + key(~3) + value(~64) ≈ 88 bytes.
	// Four Puts should trigger at least one rotation past 256 bytes.
	val := bytes.Repeat([]byte{'v'}, 64)
	for i := 0; i < 8; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, _, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("manifest after rotation: got %v, want at least 2 segments", ids)
	}
	// All listed ids must correspond to real files on disk.
	for _, id := range ids {
		path := filepath.Join(dir, segmentFilename(id))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("manifest lists segment %d but file missing: %v", id, err)
		}
	}
}

// TestManifestBootstrapsPre4aDirectory simulates a database created before
// the manifest existed: drop a .seg file in the directory with no MANIFEST,
// then Open. The expected behaviour is that the engine adopts the existing
// file and writes a manifest including it.
func TestManifestBootstrapsPre4aDirectory(t *testing.T) {
	dir := t.TempDir()

	// Set up: open + write + close (writes manifest), then DELETE the
	// manifest to simulate a pre-4a fixture.
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, manifestFilename)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	// Re-open: must bootstrap, must surface the existing key.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open #2 (bootstrap): %v", err)
	}
	v, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Read after bootstrap: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Read after bootstrap: got %q, want %q", v, "v1")
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}

	// Manifest must exist again.
	if _, err := os.Stat(filepath.Join(dir, manifestFilename)); err != nil {
		t.Fatalf("manifest not rewritten after bootstrap: %v", err)
	}
}

// TestManifestOrphanSegmentIgnored simulates the post-compaction-crash
// scenario: an extra .seg file exists on disk but is NOT listed in the
// manifest. The engine must ignore it (treat as orphan), not adopt it.
// This is the entire reason the manifest exists.
func TestManifestOrphanSegmentIgnored(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("real"), []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drop a stray .seg file with a much higher id than anything live.
	// It is invalid (zero-length, no records) but the engine must not even
	// try to open it, because the manifest never authorised it.
	orphanID := uint32(9999)
	orphanPath := filepath.Join(dir, segmentFilename(orphanID))
	if err := os.WriteFile(orphanPath, nil, 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open with orphan: %v", err)
	}
	defer db2.Close()

	v, err := db2.Get([]byte("real"))
	if err != nil {
		t.Fatalf("Read after orphan: %v", err)
	}
	if !bytes.Equal(v, []byte("data")) {
		t.Fatalf("Read after orphan: got %q, want %q", v, "data")
	}
	// The manifest should still NOT contain the orphan id.
	ids, _, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	for _, id := range ids {
		if id == orphanID {
			t.Fatalf("orphan segment %d was adopted into manifest %v", orphanID, ids)
		}
	}
}

// TestManifestRejectsMissingLiveSegment simulates someone deleting a live
// data file out from under the engine. Open must refuse to start rather
// than silently lose data.
func TestManifestRejectsMissingLiveSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Manifest should list segment 0; delete that file.
	if err := os.Remove(filepath.Join(dir, segmentFilename(0))); err != nil {
		t.Fatalf("remove segment 0: %v", err)
	}
	if _, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20}); err == nil {
		t.Fatal("Open succeeded with missing live segment; expected error")
	}
}

// TestManifestRejectsCorruptJSON ensures hand-edited or partially-written
// manifests do not slip through. The atomic-rename invariant should
// prevent partial writes from production code, but the engine should still
// refuse a manifest that violates the schema.
func TestManifestRejectsCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Overwrite with garbage.
	if err := os.WriteFile(filepath.Join(dir, manifestFilename), []byte("not json {"), 0o644); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	if _, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20}); err == nil {
		t.Fatal("Open accepted corrupt manifest JSON")
	}
}

// TestManifestRejectsUnsortedSegments guards the canonical-ordering
// invariant. A handwritten manifest with duplicate or out-of-order ids
// must be rejected so that stale entries cannot mask live ones during
// reconciliation.
func TestManifestRejectsUnsortedSegments(t *testing.T) {
	dir := t.TempDir()
	active := uint32(1)
	bad := manifestV1{Version: 1, Segments: []uint32{3, 1, 2}, Active: &active}
	b, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFilename), b, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, _, err := readManifest(dir); err == nil {
		t.Fatal("readManifest accepted unsorted segments")
	}
}

// TestManifestRejectsUnknownVersion ensures forward-incompatible manifests
// fail loud at boot rather than degrading silently.
func TestManifestRejectsUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	active := uint32(0)
	bad := manifestV1{Version: 999, Segments: []uint32{0}, Active: &active}
	b, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFilename), b, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	_, _, err = readManifest(dir)
	if err == nil {
		t.Fatal("readManifest accepted version 999")
	}
}

// TestManifestAtomicSwapNoTmpLeftover verifies the writer does not leave a
// MANIFEST.tmp file behind on the happy path. A stray tmp would be confusing
// (and would be silently overwritten on the next write, but the assertion
// pins the contract).
func TestManifestAtomicSwapNoTmpLeftover(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestTmpFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("MANIFEST.tmp leftover: %v", err)
	}
	// Also sanity-check the on-disk JSON parses as the v1 schema.
	raw := readManifestRaw(t, dir)
	var got manifestV1
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if got.Version != manifestVersion {
		t.Fatalf("manifest version: got %d, want %d", got.Version, manifestVersion)
	}
}

// TestOrphanSegmentDoesNotBlockRotation pins the fix for the
// orphan-rotation collision. Sequence: a previous run crashed inside
// rotateActive after createSegment(N) but before writeManifest published
// the new id, leaving segment N on disk but absent from the manifest.
// On the next Open, nextID must be set to max(onDisk)+1 (not
// max(manifest)+1) so the writer's first rotation does NOT try to
// recreate the existing orphan file (which would fail with "file
// exists" and wedge rotation forever).
func TestOrphanSegmentDoesNotBlockRotation(t *testing.T) {
	dir := t.TempDir()

	// Set up a one-segment DB with manifest [0].
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drop an orphan segment file at id=1 (next-expected) without
	// updating the manifest — the exact shape of a crashed rotation.
	orphanPath := filepath.Join(dir, segmentFilename(1))
	if err := os.WriteFile(orphanPath, nil, 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	// Reopen with a tiny segment cap so the next Put forces rotation.
	// If nextID were computed from the manifest alone it would be 1,
	// and createSegment(1) would fail because the orphan already
	// occupies that filename.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 16})
	if err != nil {
		t.Fatalf("Reopen with orphan: %v", err)
	}
	defer db2.Close()
	// Force at least one rotation. With MaxSegmentSize=16 and a
	// ~28-byte record (21 header + 1 key + 6 value), every Put
	// rotates first.
	if err := db2.Put([]byte("z"), []byte("longer")); err != nil {
		t.Fatalf("Put forcing rotation: %v", err)
	}
	if err := db2.Put([]byte("z"), []byte("longer")); err != nil {
		t.Fatalf("Put forcing second rotation: %v", err)
	}
}
