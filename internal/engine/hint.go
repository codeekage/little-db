package engine

// Hint files: O(keys) recovery instead of O(bytes-on-disk).
//
// A hint file is a sidecar to a data segment that carries **complete
// replay metadata** for that segment: for every key the segment
// contributes to recovery (a live PUT, or a tombstone that must remove
// an older PUT), one hint entry encodes the keydir update without
// referencing any value bytes. On a 100M-key database the difference
// between scanning 100 GB of values and reading 4 GB of hint metadata
// is the difference between a 5-minute restart and a 10-second one.
//
// Format (per SPEC §7.4, little-endian):
//
//   header  = | magic (4) = "HINT" | version (2) | reserved (2) |
//   entry   = | key_len (4) | val_len (4) | value_pos (8, int64) | tstamp (8) | key (key_len) |
//   footer  = | entry_count (4) | crc32c (4) |
//   file    = header || entry × N || footer
//
// `value_pos` is signed: a negative value (sentinel: -1) marks a TOMBSTONE
// entry. We cannot use `val_len == 0` to mean "tombstone" because the engine
// legitimately allows empty-value PUTs, and the two cases must remain
// distinguishable at recovery (PUT-empty leaves the key live; TOMBSTONE
// removes it).
//
// Trust model:
//
//   - A hint that passes header check + entry_count match + CRC32C is
//     accepted as TRUSTED COMPLETE METADATA for its segment. Recovery
//     applies it and skips the data-bytes scan. This is the operating
//     assumption: writers (the compactor in batch 4c) always emit the
//     complete keydir-update set for the segment (live PUTs plus any
//     tombstones that supersede older PUTs), and the CRC+count footer
//     prevents a torn/truncated hint from being accepted as complete.
//   - If a hint is missing OR fails any structural / CRC / count check,
//     recovery falls back to a full data-scan of the segment. We never
//     refuse to open a database just because a hint file is corrupt;
//     the `.seg` data file remains the source of truth.
//   - Atomic publication via fsync(tmp) → rename → fsync(dir), exactly
//     like the manifest. Combined with the CRC footer, a hint that is
//     visible to recovery is either complete and verified, or rejected.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	hintFileExt = ".hint"

	// hintEntryHeaderSize is the fixed prefix of a hint entry:
	// 4 (key_len) + 4 (val_len) + 8 (value_pos) + 8 (tstamp) = 24 bytes.
	hintEntryHeaderSize = 24

	// hintFileHeaderSize is magic(4) + version(2) + reserved(2).
	hintFileHeaderSize = 8

	// hintFileFooterSize is entry_count(4) + crc32c(4).
	hintFileFooterSize = 8

	// hintVersion is bumped whenever the on-disk layout changes in a
	// reader-incompatible way.
	hintVersion uint16 = 1

	// hintValuePosTombstone is the value_pos sentinel that distinguishes
	// a tombstone entry from a PUT of an empty value. Real file offsets
	// are always >= 0, so any negative value would do; we pick -1 for
	// readability in hex dumps.
	hintValuePosTombstone int64 = -1
)

// hintMagic is the 4-byte file identifier ("HINT") at offset 0.
var hintMagic = [4]byte{'H', 'I', 'N', 'T'}

// errHintMissing is returned by readHintFile when no hint sidecar exists for
// the requested segment. Callers (recovery) treat this as "fall back to data
// scan", not as an error.
var errHintMissing = errors.New("hint: not present")

// hintEntry is the in-memory representation of one hint record. valuePos < 0
// marks a tombstone; valueLen is meaningful only for PUT entries.
type hintEntry struct {
	key      []byte
	valuePos int64
	valueLen uint32
	tstamp   int64
}

// hintFilename builds the canonical filename for a given segment id.
// Mirrors segmentFilename so the .seg and .hint pair sort adjacently in
// directory listings.
func hintFilename(id uint32) string {
	return fmt.Sprintf("%010d%s", id, hintFileExt)
}

// writeHintFile atomically writes a hint sidecar for segmentID into dir.
// The file is published via tmp + rename + dir-fsync so a reader either
// sees the complete file or nothing.
//
// Callers (the compactor in batch 4c, and one day a rotation-time emitter)
// are responsible for assembling the entries; this function just serialises
// them. It does NOT sort or dedup — the input order is preserved on disk,
// which lets callers control replay order during recovery.
func writeHintFile(dir string, segmentID uint32, entries []hintEntry) error {
	if len(entries) > int(^uint32(0)) {
		return fmt.Errorf("hint: too many entries (%d)", len(entries))
	}
	// Pre-validate every entry so partial writes are impossible: nothing
	// touches the filesystem until the full input has passed length checks.
	entriesBytes := 0
	for i, e := range entries {
		if len(e.key) == 0 {
			return fmt.Errorf("hint: entry %d has empty key", i)
		}
		if len(e.key) > maxKeyLen {
			return fmt.Errorf("hint: entry %d key length %d exceeds %d", i, len(e.key), maxKeyLen)
		}
		if e.valueLen > maxValLen {
			return fmt.Errorf("hint: entry %d value length %d exceeds %d", i, e.valueLen, maxValLen)
		}
		entriesBytes += hintEntryHeaderSize + len(e.key)
	}

	total := hintFileHeaderSize + entriesBytes + hintFileFooterSize
	buf := make([]byte, total)

	// Header.
	copy(buf[0:4], hintMagic[:])
	binary.LittleEndian.PutUint16(buf[4:6], hintVersion)
	// buf[6:8] reserved, already zero.

	// Entries.
	off := hintFileHeaderSize
	for _, e := range entries {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(e.key)))
		binary.LittleEndian.PutUint32(buf[off+4:off+8], e.valueLen)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], uint64(e.valuePos))
		binary.LittleEndian.PutUint64(buf[off+16:off+24], uint64(e.tstamp))
		copy(buf[off+hintEntryHeaderSize:], e.key)
		off += hintEntryHeaderSize + len(e.key)
	}

	// Footer: entry_count then CRC over everything written so far
	// (header + entries + entry_count). The CRC field itself is excluded.
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	off += 4
	sum := crc32.Checksum(buf[:off], crc32cTable)
	binary.LittleEndian.PutUint32(buf[off:off+4], sum)

	tmpPath := filepath.Join(dir, hintFilename(segmentID)+".tmp")
	finalPath := filepath.Join(dir, hintFilename(segmentID))
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("hint: open tmp: %w", err)
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hint: write tmp: %w", err)
	}
	if err := fullSync(tmp); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hint: fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hint: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hint: rename: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("hint: open dir for fsync: %w", err)
	}
	if err := fullSync(d); err != nil {
		d.Close()
		return fmt.Errorf("hint: fsync dir: %w", err)
	}
	return d.Close()
}

// readHintFile loads the hint sidecar for segmentID. Returns errHintMissing
// if the file does not exist; any parse error is returned wrapped so callers
// can log it and fall back to a data scan.
//
// Verification is multi-layered (SPEC §7.4): magic + version, declared
// entry_count must match the number of entries actually parsed, and a
// CRC32C over header + entries + entry_count must match the trailing
// CRC field. A hint truncated at any boundary, or with any byte flipped,
// fails one of these checks and is rejected — recovery then falls back
// to a full data-scan of the segment.
func readHintFile(dir string, segmentID uint32) ([]hintEntry, error) {
	path := filepath.Join(dir, hintFilename(segmentID))
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errHintMissing
		}
		return nil, fmt.Errorf("hint: open: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("hint: read: %w", err)
	}

	if len(data) < hintFileHeaderSize+hintFileFooterSize {
		return nil, fmt.Errorf("hint: file too short (%d bytes)", len(data))
	}
	if string(data[0:4]) != string(hintMagic[:]) {
		return nil, fmt.Errorf("hint: bad magic %x", data[0:4])
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != hintVersion {
		return nil, fmt.Errorf("hint: unsupported version %d (want %d)", v, hintVersion)
	}

	// Verify CRC before trusting any length field. The CRC covers every
	// byte except the trailing 4-byte CRC itself.
	crcOff := len(data) - 4
	gotCRC := binary.LittleEndian.Uint32(data[crcOff:])
	wantCRC := crc32.Checksum(data[:crcOff], crc32cTable)
	if gotCRC != wantCRC {
		return nil, fmt.Errorf("hint: CRC mismatch (got %08x want %08x)", gotCRC, wantCRC)
	}
	declaredCount := binary.LittleEndian.Uint32(data[crcOff-4 : crcOff])

	entriesEnd := crcOff - 4
	var entries []hintEntry
	for off := hintFileHeaderSize; off < entriesEnd; {
		if entriesEnd-off < hintEntryHeaderSize {
			return nil, fmt.Errorf("hint: truncated entry header at offset %d", off)
		}
		keyLen := binary.LittleEndian.Uint32(data[off : off+4])
		valLen := binary.LittleEndian.Uint32(data[off+4 : off+8])
		valPos := int64(binary.LittleEndian.Uint64(data[off+8 : off+16]))
		tstamp := int64(binary.LittleEndian.Uint64(data[off+16 : off+24]))
		// Same caps the data-scan path uses. A corrupt hint cannot wedge
		// recovery into an oversize allocation.
		if keyLen == 0 || keyLen > maxKeyLen {
			return nil, fmt.Errorf("hint: invalid key_len %d at offset %d", keyLen, off)
		}
		if valLen > maxValLen {
			return nil, fmt.Errorf("hint: invalid val_len %d at offset %d", valLen, off)
		}
		bodyStart := off + hintEntryHeaderSize
		bodyEnd := bodyStart + int(keyLen)
		if bodyEnd > entriesEnd {
			return nil, fmt.Errorf("hint: key extends past entries section (offset %d, key_len %d)", off, keyLen)
		}
		// Copy the key so the returned slice does not alias the read
		// buffer (which the caller may keep alive past this function).
		key := make([]byte, keyLen)
		copy(key, data[bodyStart:bodyEnd])
		entries = append(entries, hintEntry{
			key:      key,
			valuePos: valPos,
			valueLen: valLen,
			tstamp:   tstamp,
		})
		off = bodyEnd
	}
	if uint32(len(entries)) != declaredCount {
		return nil, fmt.Errorf("hint: entry_count mismatch (declared %d, parsed %d)", declaredCount, len(entries))
	}
	return entries, nil
}

// removeHintFile deletes the hint sidecar for segmentID, if present. Used
// by the compactor (batch 4c) when an input segment is being retired and
// its hint must not outlive its data file. A missing file is not an error.
func removeHintFile(dir string, segmentID uint32) error {
	path := filepath.Join(dir, hintFilename(segmentID))
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("hint: remove: %w", err)
	}
	return nil
}
