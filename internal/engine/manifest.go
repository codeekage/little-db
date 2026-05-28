package engine

// Manifest file: the durable list of segment ids that are part of the live
// database. Without it, every recovery has to trust the directory listing — a
// crashed compactor would leave both the old (about-to-be-replaced) segments
// AND the new (just-emitted) segment on disk, and recovery could not tell
// which set is canonical. The manifest answers exactly that question.
//
// The contract is intentionally minimal:
//
//   - JSON for human inspection at 2am during an incident.
//   - Versioned schema so we can evolve without flag days.
//   - Atomically replaced via fsync(tmp) → rename → fsync(dir). At any
//     instant a reader sees either the pre-state or the post-state, never
//     a partial write.
//   - Sorted, deduplicated segment ids so the on-disk representation is
//     canonical (diff-friendly; equality testing is trivial).
//
// Writer sites (today and planned):
//
//  1. Open(): bootstraps the manifest when a directory has segments but
//     no manifest (a pre-4a DB or a hand-assembled test fixture).
//  2. rotateActive(): records the new active segment id so post-crash
//     recovery does not drop just-acknowledged writes that landed in a
//     segment born after the last manifest update.
//  3. Compactor (batch 4c): atomically swaps the live set after emitting
//     a merged segment, before deleting the inputs.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	manifestFilename    = "MANIFEST"
	manifestTmpFilename = "MANIFEST.tmp"
	manifestVersion     = 1
)

// errManifestMissing is a sentinel returned by readManifest when no manifest
// file is present. The caller distinguishes this from a real I/O or parse
// error so it can bootstrap a manifest from the directory listing on an
// older DB.
var errManifestMissing = errors.New("manifest: not present")

// ErrManifestPublishedButUncertain is returned by writeManifest when the
// rename succeeded (so the new manifest is visible on disk) but a
// subsequent step in the durability sequence — opening the parent
// directory, fsync'ing it, or closing it — failed. The on-disk state is
// now in one of two safe configurations and the caller cannot tell which:
//
//  1. Host stays up or recovers cleanly: the new manifest is durable and
//     references the new segment(s). This is the intended outcome.
//  2. Host crashes before the directory entry is durable: the rename may
//     revert, the new segment is treated as a leftover-from-crashed-
//     compactor orphan by reconcileManifest, and the old manifest stands.
//
// In BOTH cases the new segment file must remain on disk. If the caller
// rolls back by unlinking the new segment, case 1 becomes unrecoverable:
// the next Open will read a manifest that references a missing segment
// and fail hard. Callers MUST check errors.Is(err, ErrManifestPublished
// ButUncertain) and, on a match, skip rollback and continue installing
// the new segment into the in-memory state.
var ErrManifestPublishedButUncertain = errors.New("manifest: rename ok, durability uncertain")

// manifestV1 is the on-disk schema. Keep it small and forward-compatible:
// unknown fields are ignored by encoding/json, so adding fields later (e.g.
// last-compaction timestamps) does not break older readers within v1.
//
// Active is REQUIRED. It names the writer's active segment so the
// invariant "highest segment id is the active segment" does not have to
// hold — compaction emits an immutable segment with a fresh id that is
// strictly greater than the active id, and recovery needs an explicit
// signal of which segment to torn-tail-truncate / data-scan as newest.
// A pointer distinguishes a present-but-zero id from a missing field;
// the engine refuses to start on a manifest that omits it.
type manifestV1 struct {
	Version  int      `json:"version"`
	Segments []uint32 `json:"segments"`
	Active   *uint32  `json:"active"`
}

// readManifest loads the manifest from dir. Returns errManifestMissing if
// the file does not exist; any other error (permission, parse, version
// mismatch, missing active id, active not in segments) is returned as-is
// and aborts Open. Recovery never silently "fixes" a corrupt manifest.
func readManifest(dir string) (segments []uint32, active uint32, err error) {
	path := filepath.Join(dir, manifestFilename)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, errManifestMissing
		}
		return nil, 0, fmt.Errorf("manifest: open: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, fmt.Errorf("manifest: read: %w", err)
	}
	var m manifestV1
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, 0, fmt.Errorf("manifest: parse: %w", err)
	}
	if m.Version != manifestVersion {
		return nil, 0, fmt.Errorf("manifest: unsupported version %d (this build understands %d)", m.Version, manifestVersion)
	}
	// Defend against hand-edited or partially-written manifests that slipped
	// past the atomic-rename invariant (e.g. someone ran `echo {} > MANIFEST`):
	// duplicates or unsorted ids would let a stale id mask a live one during
	// reconciliation.
	for i := 1; i < len(m.Segments); i++ {
		if m.Segments[i] <= m.Segments[i-1] {
			return nil, 0, fmt.Errorf("manifest: segments not strictly ascending (entry %d <= %d)", m.Segments[i], m.Segments[i-1])
		}
	}
	if m.Active == nil {
		return nil, 0, fmt.Errorf("manifest: missing active segment id")
	}
	activeID := *m.Active
	found := false
	for _, id := range m.Segments {
		if id == activeID {
			found = true
			break
		}
	}
	if !found {
		return nil, 0, fmt.Errorf("manifest: active segment %d not listed in segments %v", activeID, m.Segments)
	}
	return m.Segments, activeID, nil
}

// writeManifest atomically replaces the manifest in dir with the given list
// of segment ids. The list is sorted+deduplicated before write so callers do
// not need to maintain canonical order themselves.
//
// Durability sequence:
//
//  1. Sort+dedup → marshal JSON.
//  2. Write to MANIFEST.tmp with O_TRUNC (a stale tmp from a previous crash
//     would otherwise survive and confuse the next write).
//  3. fsync the tmp file (the file's contents must be on disk before the
//     rename publishes it).
//  4. Rename tmp → MANIFEST. POSIX rename is atomic within a directory.
//  5. fsync the directory so the rename itself is durable.
//
// On Darwin, fsync is replaced by F_FULLFSYNC (matches the rest of the
// engine's durability story).
//
// active MUST be one of ids; this is asserted before any I/O so a buggy
// caller fails fast rather than corrupting the on-disk state.
func writeManifest(dir string, ids []uint32, active uint32) error {
	sorted := append([]uint32(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	// Dedup in place. Crashed-compactor recovery may legitimately pass a
	// list that contains the same id twice (old + new merge target) — we
	// canonicalise rather than reject so the writer is robust.
	dedup := sorted[:0]
	for i, id := range sorted {
		if i == 0 || id != sorted[i-1] {
			dedup = append(dedup, id)
		}
	}
	activeOK := false
	for _, id := range dedup {
		if id == active {
			activeOK = true
			break
		}
	}
	if !activeOK {
		return fmt.Errorf("manifest: active %d not in segments %v", active, dedup)
	}

	activeCopy := active
	data, err := json.Marshal(manifestV1{Version: manifestVersion, Segments: dedup, Active: &activeCopy})
	if err != nil {
		return fmt.Errorf("manifest: marshal: %w", err)
	}

	tmpPath := filepath.Join(dir, manifestTmpFilename)
	finalPath := filepath.Join(dir, manifestFilename)
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: open tmp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("manifest: write tmp: %w", err)
	}
	if err := fullSync(tmp); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("manifest: fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("manifest: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("manifest: rename: %w", err)
	}
	// Past this point, the new MANIFEST is visible on disk. Any error
	// from here on is reported under ErrManifestPublishedButUncertain so
	// callers do NOT roll back the new segment — see the sentinel's
	// godoc for the full rationale.
	//
	// Fsync the directory so the rename is durable across a host crash.
	// Without this, a crash could revert to the previous MANIFEST contents
	// even though the rename returned success.
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: open dir for fsync: %v", ErrManifestPublishedButUncertain, err)
	}
	if err := fullSync(d); err != nil {
		d.Close()
		return fmt.Errorf("%w: fsync dir: %v", ErrManifestPublishedButUncertain, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("%w: close dir: %v", ErrManifestPublishedButUncertain, err)
	}
	return nil
}

// reconcileManifest reads the manifest (if any) and intersects it with the
// segment ids actually found on disk. It returns the canonical live set in
// ascending order plus a flag indicating that no manifest existed (the
// caller must bootstrap one before any write lands).
//
// Error conditions:
//
//   - Manifest references a segment id that is not on disk: hard error.
//     This means somebody deleted a live data file. Refusing to start is the
//     safest behaviour; the operator can either restore from backup or
//     hand-edit the manifest to drop the missing id.
//
// Files on disk that are NOT in the manifest are silently ignored ("orphans").
// They are the expected leftovers from a crashed compactor: the new merged
// segment had been written but the manifest swap had not yet happened, so the
// new file's id is not authoritative. The compactor cleans these up on its
// next run; for now they just sit there harmlessly.
//
// The active return is the manifest's recorded active segment id. When
// bootstrapped is true (no manifest present) the caller is responsible for
// choosing an active and writing the first manifest; the returned active is
// 0 in that case and must be ignored.
func reconcileManifest(dir string, onDisk []uint32) (live []uint32, active uint32, bootstrapped bool, err error) {
	manifest, manifestActive, err := readManifest(dir)
	if err != nil {
		if errors.Is(err, errManifestMissing) {
			// First boot of a pre-4a DB (or a freshly-created empty dir).
			// Adopt every .seg file we found; the caller will write the
			// manifest after Open finishes resolving the active segment.
			return append([]uint32(nil), onDisk...), 0, true, nil
		}
		return nil, 0, false, err
	}

	onDiskSet := make(map[uint32]struct{}, len(onDisk))
	for _, id := range onDisk {
		onDiskSet[id] = struct{}{}
	}
	for _, id := range manifest {
		if _, ok := onDiskSet[id]; !ok {
			return nil, 0, false, fmt.Errorf("manifest: live segment %d listed but file missing on disk", id)
		}
	}
	// Trust the manifest list verbatim (already sorted+validated by readManifest).
	return manifest, manifestActive, false, nil
}

// liveSegmentIDsLocked returns the current live segment ids in ascending
// order. Caller does not need to hold segmentsMu when called from Open
// (single-threaded boot) or from a path that already serialises with the
// writer; the writer goroutine's rotateActive captures the snapshot under
// RLock directly inline rather than going through this helper.
func (db *DB) liveSegmentIDsLocked() []uint32 {
	ids := make([]uint32, 0, len(db.segments))
	for id := range db.segments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
