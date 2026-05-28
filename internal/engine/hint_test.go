package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// TestHintFileRoundTrip writes a mix of PUT entries (with regular and empty
// values), tombstones, and a large key, then reads them back. The decoded
// slice must equal the input byte-for-byte. This is the load-bearing
// invariant for hint-accelerated recovery: anything the writer puts in a
// hint must be observable verbatim at Open.
func TestHintFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	bigKey := bytes.Repeat([]byte("a"), 4096)
	entries := []hintEntry{
		{key: []byte("alpha"), valuePos: 100, valueLen: 7, tstamp: 1_000_000},
		// Empty-value PUT: must NOT be confused with a tombstone on
		// readback. The valuePos sentinel is what discriminates the
		// two cases.
		{key: []byte("beta"), valuePos: 200, valueLen: 0, tstamp: 1_000_001},
		// Tombstone: valuePos negative.
		{key: []byte("gamma"), valuePos: hintValuePosTombstone, valueLen: 0, tstamp: 1_000_002},
		{key: bigKey, valuePos: 1 << 30, valueLen: 1 << 20, tstamp: 1_000_003},
	}

	if err := writeHintFile(dir, 42, entries); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	got, err := readHintFile(dir, 42)
	if err != nil {
		t.Fatalf("readHintFile: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("len got=%d want=%d", len(got), len(entries))
	}
	for i := range entries {
		if !bytes.Equal(got[i].key, entries[i].key) {
			t.Errorf("entry %d key mismatch", i)
		}
		if got[i].valuePos != entries[i].valuePos {
			t.Errorf("entry %d valuePos got=%d want=%d", i, got[i].valuePos, entries[i].valuePos)
		}
		if got[i].valueLen != entries[i].valueLen {
			t.Errorf("entry %d valueLen got=%d want=%d", i, got[i].valueLen, entries[i].valueLen)
		}
		if got[i].tstamp != entries[i].tstamp {
			t.Errorf("entry %d tstamp got=%d want=%d", i, got[i].tstamp, entries[i].tstamp)
		}
	}
}

// TestHintFileMissingReturnsSentinel pins the contract that recovery uses
// to decide between fast-path and data-scan: a missing hint MUST surface
// as errHintMissing, not a generic os error. If this regresses, recovery
// would treat absent hints as fatal and refuse to open older databases.
func TestHintFileMissingReturnsSentinel(t *testing.T) {
	dir := t.TempDir()
	_, err := readHintFile(dir, 7)
	if !errors.Is(err, errHintMissing) {
		t.Fatalf("got %v, want errHintMissing", err)
	}
}

// TestHintFileEmpty exercises the zero-entry case. The compactor will
// produce empty hints whenever a merged segment ends up with no live
// keys (e.g. all inputs were tombstones). The file must exist (so
// recovery knows to skip the scan) and parse back as an empty slice.
func TestHintFileEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := writeHintFile(dir, 1, nil); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	got, err := readHintFile(dir, 1)
	if err != nil {
		t.Fatalf("readHintFile: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

// TestHintFileCorruptHeaderRejected: a hint truncated mid-entry-header
// must fail to parse so recovery can fall back to a data scan.
func TestHintFileCorruptTruncatedHeader(t *testing.T) {
	dir := t.TempDir()
	// One valid entry, then strip the trailing few bytes.
	if err := writeHintFile(dir, 1, []hintEntry{{key: []byte("k"), valuePos: 0, valueLen: 1, tstamp: 1}}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	path := filepath.Join(dir, hintFilename(1))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Append a half header so the parser tries to decode and runs short.
	corrupt := append(b, 1, 2, 3)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readHintFile(dir, 1); err == nil {
		t.Fatalf("expected parse error, got nil")
	}
}

// TestHintFileCorruptOversizeKey: a key_len that exceeds maxKeyLen must
// be rejected before we attempt the allocation. Belt-and-braces against
// disk corruption that would otherwise let an attacker (or bit rot)
// drive recovery into a multi-GB malloc.
func TestHintFileCorruptOversizeKey(t *testing.T) {
	dir := t.TempDir()
	// Hand-build a valid file framing (magic + version + footer CRC)
	// wrapping a single entry whose key_len = maxKeyLen+1.
	entry := make([]byte, hintEntryHeaderSize)
	binary.LittleEndian.PutUint32(entry[0:4], uint32(maxKeyLen+1))
	binary.LittleEndian.PutUint32(entry[4:8], 0)
	binary.LittleEndian.PutUint64(entry[8:16], 0)
	binary.LittleEndian.PutUint64(entry[16:24], 0)

	buf := make([]byte, hintFileHeaderSize+len(entry)+hintFileFooterSize)
	copy(buf[0:4], []byte("HINT"))
	binary.LittleEndian.PutUint16(buf[4:6], hintVersion)
	copy(buf[hintFileHeaderSize:], entry)
	binary.LittleEndian.PutUint32(buf[hintFileHeaderSize+len(entry):], 1) // entry_count
	// CRC32C over header + entry + count, then write into final 4 bytes.
	crcOff := len(buf) - 4
	sum := crc32.Checksum(buf[:crcOff], crc32cTable)
	binary.LittleEndian.PutUint32(buf[crcOff:], sum)

	path := filepath.Join(dir, hintFilename(1))
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readHintFile(dir, 1); err == nil {
		t.Fatalf("expected oversize-key rejection, got nil")
	}
}

// TestHintFileAtomicNoTmpLeftover: after a successful write, only the
// final .hint file remains in the directory — the .hint.tmp must have
// been renamed away atomically.
func TestHintFileAtomicNoTmpLeftover(t *testing.T) {
	dir := t.TempDir()
	if err := writeHintFile(dir, 5, []hintEntry{{key: []byte("k"), valuePos: 0, valueLen: 0, tstamp: 1}}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("found leftover tmp file: %s", e.Name())
		}
	}
}

// TestHintFileRejectsEmptyKey: empty keys are illegal everywhere in the
// engine; the hint writer must reject them at the boundary, not silently
// produce a corrupt-looking hint that the reader would later reject.
func TestHintFileRejectsEmptyKey(t *testing.T) {
	dir := t.TempDir()
	err := writeHintFile(dir, 1, []hintEntry{{key: nil, valuePos: 0, valueLen: 0, tstamp: 1}})
	if err == nil {
		t.Fatalf("expected empty-key rejection, got nil")
	}
}

// TestRecoveryUsesHintFile is the integration test: open a DB, force a
// rotation so segment 0 becomes sealed (and therefore a hint-fast-path
// candidate at next Open), hand-emit a hint file for seg 0 with a
// sentinel tstamp that could not appear in any real record, reopen, and
// verify the hint was applied. We discriminate hint-path from scan-path
// by checking db.lastTstamp — the sentinel (1<<62 ns) is ~year 2116, far
// beyond any plausible wall-clock value at recovery time.
//
// Crucially, the hint MUST be attached to a non-newest segment: recovery
// always data-scans the newest segment to detect torn tails, so a
// single-segment database (segment 0 is both newest and oldest) would
// skip the hint entirely and falsely report "hint not used".
func TestRecoveryUsesHintFile(t *testing.T) {
	dir := t.TempDir()
	// MaxSegmentSize is the rotation THRESHOLD, not a hard cap: rotation
	// fires when (size + encoded_record) would exceed it. A record for
	// "kN"/"vN" encodes to 21 (header) + 2 + 2 = 25 bytes. With cap=64,
	// two records fit (50 bytes), the third triggers rotation. So seg 0
	// ends up with k1+k2, seg 1 with k3, seg 2 is the fresh active.
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := db.Put([]byte(k), []byte("v1")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
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
		t.Fatalf("expected >=2 segments to exercise hint fast-path, got %d", len(ids))
	}

	// Segment 0 layout: record 0 = header(21) + key("k1":2) + value("v1":2)
	// = 25 bytes. Record 1 = same shape for k2. So inside seg 0:
	//   k1 value at offset recordHeaderSize+2 = 23, length 2
	//   k2 value at offset 25 + recordHeaderSize+2 = 48, length 2
	// Per the trusted-metadata contract (SPEC §7.4), the hint MUST be
	// the complete keydir-update set for seg 0 (every PUT the segment
	// contributes, plus any superseding tombstones). A partial hint
	// (e.g. only k1) would silently drop k2 — the very failure mode
	// the format's count+CRC footer is designed to prevent on disk,
	// and that this test prevents at the writer-side.
	const sentinelTstamp = int64(1) << 62 // ~year 2116 in ns; safely above wall-clock.

	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("k1"), valuePos: int64(recordHeaderSize + 2), valueLen: 2, tstamp: sentinelTstamp},
		{key: []byte("k2"), valuePos: int64(25 + recordHeaderSize + 2), valueLen: 2, tstamp: sentinelTstamp + 1},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	for _, k := range []string{"k1", "k2"} {
		v, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !bytes.Equal(v, []byte("v1")) {
			t.Fatalf("Get %s value got=%q want=%q", k, v, "v1")
		}
	}
	if db2.lastTstamp < sentinelTstamp {
		t.Fatalf("lastTstamp=%d, expected >= sentinel %d — hint was not applied", db2.lastTstamp, sentinelTstamp)
	}
}

// TestRecoveryFallsBackWhenHintCorrupt: a hint that parses but references
// an offset past EOF must be rejected by tryRecoverFromHint so recovery
// falls back to the data scan. The DB must still open cleanly and serve
// the real data. The hinted segment must be non-newest, otherwise
// recovery skips hints entirely (newest segments always data-scan for
// torn-tail detection) and this test would pass for the wrong reason.
func TestRecoveryFallsBackWhenHintCorrupt(t *testing.T) {
	dir := t.TempDir()
	// Same layout as TestRecoveryUsesHintFile: 3 puts with 25-byte
	// records and cap=64 land k1+k2 in seg 0 (non-newest), k3 in seg 1.
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := db.Put([]byte(k), []byte("v1")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
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
		t.Fatalf("expected >=2 segments, got %d", len(ids))
	}

	// Hint claims k1 lives at an offset way past the actual file size.
	// tryRecoverFromHint must spot the out-of-bounds offset and refuse,
	// causing recover() to fall back to scanning segment 0 — which has
	// the real record at offset recordHeaderSize+2.
	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("k1"), valuePos: 1 << 30, valueLen: 2, tstamp: 1},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	v, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get got=%q want=%q", v, "v1")
	}
}

// TestRecoveryHintTombstoneRemovesKey: a tombstone entry in a hint file
// must remove the key from the keydir on recovery, exactly as a
// data-scan tombstone would.
func TestRecoveryHintTombstoneRemovesKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Force two segments: seg 0 contains the PUT, then rotation,
	// seg 1 holds the (real) tombstone. We hint seg 0 with a
	// synthetic tombstone whose tstamp is in the future, so even
	// if seg 0's PUT got applied first, the hint replay's own
	// >= comparison would remove it. Then we delete the data file
	// for seg 1 so the test only exercises seg 0 + hint.
	//
	// Actually simpler: a single segment with one PUT, and a hint
	// that overrides it with a future-dated tombstone for the same
	// key. After recovery the key must be absent.
	if err := db.Put([]byte("doomed"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// We hint a tombstone for "doomed" on the LAST segment id — but
	// the newest segment is always data-scanned, so a tombstone-only
	// hint would not actually be consulted. To exercise the hint
	// tombstone code-path, we hint a NON-newest segment. Reach this
	// by reopening, writing a second key (which rotates because of
	// MaxSegmentSize=64), closing, then hinting seg 0 with a
	// tombstone for "doomed".
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	// Pad until rotation happens. Use a value big enough that one
	// or two Puts will cross the 64-byte ceiling.
	for i := 0; i < 4; i++ {
		if err := db2.Put([]byte("pad"), bytes.Repeat([]byte("x"), 40)); err != nil {
			t.Fatalf("Put pad: %v", err)
		}
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Confirm at least two segments exist.
	ids, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected >=2 segments, got %d", len(ids))
	}

	// Hint a tombstone on segment 0 with a future tstamp.
	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("doomed"), valuePos: hintValuePosTombstone, valueLen: 0, tstamp: 1_000_000_000_000_000_000},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}

	db3, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen2: %v", err)
	}
	defer db3.Close()
	if _, err := db3.Get([]byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for tombstoned key, got %v", err)
	}
}

// TestRecoveryRejectsHintWithBadCRC pins the trusted-metadata contract:
// a hint file whose CRC32C footer does not match its contents (e.g. a
// single bit flipped anywhere in the entry stream) must be rejected by
// readHintFile so recovery falls back to the data-bytes scan. Without
// this check, a corrupt-but-structurally-valid hint could silently drop
// or remap keys.
func TestRecoveryRejectsHintWithBadCRC(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := db.Put([]byte(k), []byte("v1")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Write a valid hint for seg 0, then flip a byte inside the entries
	// section (before the footer). The CRC must no longer match.
	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("k1"), valuePos: int64(recordHeaderSize + 2), valueLen: 2, tstamp: 1},
		{key: []byte("k2"), valuePos: int64(25 + recordHeaderSize + 2), valueLen: 2, tstamp: 2},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	path := filepath.Join(dir, hintFilename(0))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip the first byte of the first entry's key_len. Anywhere in the
	// CRC-covered range works; this offset is the most obvious.
	b[hintFileHeaderSize] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// readHintFile must reject directly.
	if _, err := readHintFile(dir, 0); err == nil {
		t.Fatalf("readHintFile accepted bad-CRC hint")
	}

	// And full recovery must still open cleanly via the data-scan fallback.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	for _, k := range []string{"k1", "k2", "k3"} {
		v, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s after fallback: %v", k, err)
		}
		if !bytes.Equal(v, []byte("v1")) {
			t.Fatalf("Get %s got=%q want=v1", k, v)
		}
	}
}

// TestRecoveryRejectsHintWithEntryCountMismatch: writeHintFile + truncate
// the trailing CRC and overwrite entry_count with a wrong value but
// matching CRC. Simulates a writer bug that under-counts. The reader
// must catch the mismatch and fall back.
func TestRecoveryRejectsHintWithEntryCountMismatch(t *testing.T) {
	dir := t.TempDir()
	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("a"), valuePos: 0, valueLen: 1, tstamp: 1},
		{key: []byte("b"), valuePos: 10, valueLen: 1, tstamp: 2},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}
	path := filepath.Join(dir, hintFilename(0))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Overwrite entry_count (last 8 bytes are count|crc) with 99,
	// then recompute the CRC so it matches \u2014 isolating the count check.
	countOff := len(b) - 8
	binary.LittleEndian.PutUint32(b[countOff:countOff+4], 99)
	crcOff := len(b) - 4
	binary.LittleEndian.PutUint32(b[crcOff:], crc32.Checksum(b[:crcOff], crc32cTable))
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readHintFile(dir, 0); err == nil {
		t.Fatalf("readHintFile accepted entry_count mismatch")
	}
}

// TestHintBoundsFailureLeavesKeydirUntouched pins the two-pass contract
// in tryRecoverFromHint: when ANY entry in a hint fails the
// segment-bounds check, NO entry has yet been applied to the keydir.
// Otherwise an early valid-but-malicious entry (e.g. a remap with a
// future timestamp) would survive the fallback data-scan because
// putIfNewer/tombstone gating would keep the higher-tstamp hint state.
//
// Construction: hint seg 0 with two entries.
//   - entry[0] remaps k1 -> k2's on-disk offset, with a sentinel
//     timestamp (1<<62 ns). Pre-fix this would mutate the keydir, the
//     fallback scan would lose putIfNewer against the sentinel, and
//     Get(k1) would return "vZ" (k2's value).
//   - entry[1] references an out-of-bounds offset, causing
//     tryRecoverFromHint to return false.
//
// Post-fix: validation runs first, the whole hint is rejected, the
// data-scan rebuilds the keydir from scratch, and Get(k1) returns "v1".
func TestHintBoundsFailureLeavesKeydirUntouched(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Distinct values so a remap is observable.
	puts := []struct{ k, v string }{
		{"k1", "v1"},
		{"k2", "vZ"},
		{"k3", "v3"},
	}
	for _, p := range puts {
		if err := db.Put([]byte(p.k), []byte(p.v)); err != nil {
			t.Fatalf("Put %s: %v", p.k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Seg 0 layout (each record = 25 bytes):
	//   k1's value at offset recordHeaderSize+2 = 23, len 2
	//   k2's value at offset 25 + recordHeaderSize+2 = 48, len 2
	const k2ValuePos = int64(25 + recordHeaderSize + 2)
	const sentinelTstamp = int64(1) << 62

	if err := writeHintFile(dir, 0, []hintEntry{
		// Malicious-but-valid remap: k1 -> k2's bytes, with a future
		// timestamp so any later putIfNewer would lose.
		{key: []byte("k1"), valuePos: k2ValuePos, valueLen: 2, tstamp: sentinelTstamp},
		// Out-of-bounds entry: forces tryRecoverFromHint to return false.
		{key: []byte("k2"), valuePos: 1 << 30, valueLen: 2, tstamp: 1},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}

	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	got, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get k1 got=%q want=%q (the remap leaked through the rejected hint)", got, "v1")
	}
}

// TestHintBoundsCheckRejectsInt64Overflow pins the overflow-safe shape of
// the segment-bounds check in tryRecoverFromHint. With the naive form
// `end := valuePos + int64(valueLen)`, a CRC-valid hint carrying
// valuePos=math.MaxInt64 and valueLen=1 wraps `end` to math.MinInt64,
// which trivially compares <= seg.size and lets recovery accept a
// completely bogus offset into the keydir. The post-fix form compares
// valuePos and valueLen against seg.size independently, keeping every
// term non-negative.
func TestHintBoundsCheckRejectsInt64Overflow(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := db.Put([]byte(k), []byte("v1")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A single hint entry whose valuePos is MaxInt64 — nonsense, but
	// passes writeHintFile validation (valueLen=1 is well under maxValLen).
	if err := writeHintFile(dir, 0, []hintEntry{
		{key: []byte("k1"), valuePos: math.MaxInt64, valueLen: 1, tstamp: 1},
	}); err != nil {
		t.Fatalf("writeHintFile: %v", err)
	}

	// Recovery must reject the hint (overflow-safe bounds check), fall
	// back to the data scan, and serve the real values.
	db2, err := Open(Options{Dir: dir, MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	for _, k := range []string{"k1", "k2", "k3"} {
		v, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s after fallback: %v", k, err)
		}
		if !bytes.Equal(v, []byte("v1")) {
			t.Fatalf("Get %s got=%q want=v1 (overflow accepted the bogus hint)", k, v)
		}
	}
}
