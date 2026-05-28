package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// recover scans every segment file in the data directory and rebuilds the
// in-memory keydir. It is idempotent and safe to call exactly once at Open.
//
// The contract under crash:
//
//  1. Every record that was acknowledged before the crash is reflected in the
//     keydir after recovery.
//  2. Any partial trailing record (torn write) at the tail of the
//     manifest-named active segment is detected — either by a truncated
//     header, or by a header whose claimed body extends past EOF — and the
//     segment is truncated at the last known-good offset so subsequent
//     appends start clean. "Active" is NOT the same as "highest id": after
//     compaction the merged immutable segment can have a strictly higher id.
//  3. A CRC failure on a record whose body fits within the file is treated
//     as real corruption (not a torn write). Recovery aborts; the operator
//     decides. Silently dropping mid-segment data is unacceptable.
//
// Segments are scanned in ascending id order so that later records correctly
// supersede earlier ones via keydir.putIfNewer.
//
// For each segment we try the hint-file fast-path first (O(keys)); if no
// hint exists or it fails to parse, we fall back to a full data scan
// (O(bytes)). The ACTIVE segment is special: it may have a torn-tail write
// from a crashed predecessor that must be truncated before it becomes the
// writer's append target again, and truncation requires seeing the actual
// file tail. So the active segment is ALWAYS data-scanned, regardless of
// hint presence. NOTE: "active" is NOT the same as "highest id" —
// compaction can publish an immutable merged segment with an id strictly
// greater than the active. The caller (Open) passes activeID explicitly
// based on the manifest's recorded active; recover() never derives it
// from max(ids).
//
// haveActive is false only during the bootstrap path on a directory with
// zero pre-existing segments; in that case there is nothing to torn-tail.
func (db *DB) recover(haveActive bool, activeID uint32) error {
	ids := make([]uint32, 0, len(db.segments))
	for id := range db.segments {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	for _, id := range ids {
		seg := db.segments[id]
		isActive := haveActive && id == activeID
		if !isActive {
			used, err := db.tryRecoverFromHint(seg)
			if err != nil {
				// Hint parse failed. Log and fall back to data scan —
				// hints are an optimisation, not a source of truth.
				db.log.Warn("hint file unusable, falling back to scan",
					slog.Uint64("segment", uint64(id)),
					slog.String("err", err.Error()))
			} else if used {
				continue
			}
		}
		if err := db.recoverSegment(seg, isActive); err != nil {
			return fmt.Errorf("recover segment %d: %w", id, err)
		}
	}
	return nil
}

// tryRecoverFromHint replays the hint sidecar for seg into the keydir.
// Returns (used=true, nil) on success, (false, nil) when no hint file
// exists (so the caller will scan), and (false, err) when a hint file
// exists but is malformed (caller logs + scans).
//
// The hint entries are applied in file order with the same putIfNewer /
// delete-on-tombstone semantics the data-scan path uses, so the keydir
// state after a hint replay is byte-for-byte equivalent to what a full
// scan would have produced.
//
// We also update db.lastTstamp from hint entries so the writer's monotonic
// clock does not regress after recovery. The data-scan path's tstamp
// tracking lives in applyRecordOnRecovery; we mirror that contract here.
func (db *DB) tryRecoverFromHint(seg *segment) (bool, error) {
	entries, err := readHintFile(db.opts.Dir, seg.id)
	if err != nil {
		if errors.Is(err, errHintMissing) {
			return false, nil
		}
		return false, err
	}
	// Two-pass: validate every entry against the segment's bounds BEFORE
	// touching the keydir. A bounds failure on entry N+1 must not leave
	// entries 0..N applied, because the caller's fallback is a full data
	// scan and the partial keydir state could shadow the scan's
	// putIfNewer / tombstone gating (e.g. a corrupt hint carrying a
	// future timestamp would survive the rescan). Once all entries are
	// validated, the apply pass is guaranteed to complete.
	for i, e := range entries {
		// Defence-in-depth: a hint must not reference bytes outside the
		// data file it describes. A mismatch means the hint and segment
		// drifted (e.g. someone restored an old .seg over a newer .hint),
		// and we cannot trust the metadata. Bail to the scan path.
		//
		// The bounds check is phrased to avoid int64 overflow: a CRC-valid
		// but malicious hint with valuePos near math.MaxInt64 and any
		// nonzero valueLen would wrap `valuePos + valueLen` to a negative
		// number that trivially compares <= seg.size. Comparing each term
		// independently (and against `seg.size - valuePos` for the length)
		// stays in non-negative territory.
		if e.valuePos != hintValuePosTombstone {
			segSize := seg.size.Load()
			if e.valuePos < 0 || e.valuePos > segSize || int64(e.valueLen) > segSize-e.valuePos {
				return false, fmt.Errorf("hint entry %d refs offset %d (len %d) outside segment size %d", i, e.valuePos, e.valueLen, segSize)
			}
		}
	}
	for _, e := range entries {
		if e.valuePos == hintValuePosTombstone {
			existing, ok := db.keydir.get(e.key)
			if !ok || e.tstamp >= existing.tstamp {
				db.keydir.delete(e.key)
			}
		} else {
			db.keydir.putIfNewer(e.key, keydirEntry{
				fileID:   seg.id,
				valuePos: e.valuePos,
				valueLen: e.valueLen,
				tstamp:   e.tstamp,
			})
		}
		if e.tstamp > db.lastTstamp {
			db.lastTstamp = e.tstamp
		}
	}
	return true, nil
}

// recoverSegment scans one segment file from offset 0 to EOF. It inlines the
// header parser (rather than calling readRecord) because recovery needs to
// distinguish "body extends past EOF" — a tail torn write that we truncate —
// from "claimed body fits but its CRC fails" — real mid-file corruption that
// we refuse to silently drop.
//
// allowTornTail must be true ONLY for the manifest-named active segment.
// A non-active (sealed or compacted-merged) segment was fsynced before its
// manifest publish, so any partial-header or body-past-EOF condition is
// real corruption, not a torn write, and is reported as a hard error
// rather than silently truncated. Truncating a non-active segment would
// erase records that previous Puts already returned success for.
func (db *DB) recoverSegment(seg *segment, allowTornTail bool) error {
	r := &segmentSequentialReader{seg: seg, off: 0}
	var lastGoodOffset int64

	for {
		recordStart := r.off

		// 1. Read 21-byte header. Clean EOF at start of a record → done.
		// Partial header → tail torn write, truncate (active segment only).
		var hdr [recordHeaderSize]byte
		nh, err := io.ReadFull(r, hdr[:])
		if err != nil {
			if errors.Is(err, io.EOF) && nh == 0 {
				lastGoodOffset = recordStart
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				if !allowTornTail {
					return fmt.Errorf("sealed segment %d has partial header at offset %d (read %d/%d bytes)", seg.id, recordStart, nh, recordHeaderSize)
				}
				lastGoodOffset = recordStart
				break
			}
			return fmt.Errorf("read header at offset %d: %w", recordStart, err)
		}

		// 2. Parse header.
		expectedCRC := binary.LittleEndian.Uint32(hdr[0:4])
		tstamp := int64(binary.LittleEndian.Uint64(hdr[4:12]))
		flag := hdr[12]
		keyLen := binary.LittleEndian.Uint32(hdr[13:17])
		valLen := binary.LittleEndian.Uint32(hdr[17:21])

		// 3. If the claimed body extends past EOF, treat as a tail torn
		// write (active segment only). For non-active (sealed or merged)
		// segments this is real corruption and we fail loud.
		bodyLen := int64(keyLen) + int64(valLen)
		segSize := seg.size.Load()
		if recordStart+int64(recordHeaderSize)+bodyLen > segSize {
			if !allowTornTail {
				return fmt.Errorf("sealed segment %d has record body past EOF at offset %d (need %d, have %d)", seg.id, recordStart, recordStart+int64(recordHeaderSize)+bodyLen, segSize)
			}
			lastGoodOffset = recordStart
			break
		}

		// 4. Header is fully within the file. Any anomaly past this point is
		// real corruption.
		if flag != recordFlagPut && flag != recordFlagTombstone && flag != recordFlagBatch {
			return fmt.Errorf("mid-segment unknown flag %d at offset %d", flag, recordStart)
		}
		if flag == recordFlagBatch {
			if keyLen != 0 {
				return fmt.Errorf("mid-segment batch record with key_len=%d at offset %d", keyLen, recordStart)
			}
			if valLen < batchCountSize {
				return fmt.Errorf("mid-segment batch body too short (%d) at offset %d", valLen, recordStart)
			}
			if valLen > maxBatchBodyLen {
				return fmt.Errorf("mid-segment batch body too large (%d) at offset %d", valLen, recordStart)
			}
		} else {
			if keyLen == 0 || keyLen > maxKeyLen {
				return fmt.Errorf("mid-segment invalid key length %d at offset %d", keyLen, recordStart)
			}
			if valLen > maxValLen {
				return fmt.Errorf("mid-segment invalid value length %d at offset %d", valLen, recordStart)
			}
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return fmt.Errorf("read body at offset %d: %w", recordStart, err)
		}

		h := crc32.New(crc32cTable)
		h.Write(hdr[4:])
		h.Write(body)
		if h.Sum32() != expectedCRC {
			// A trailing torn write is detected earlier by the
			// "body past EOF" check; mid-segment CRC failure means real
			// corruption — we refuse to silently drop user data and abort
			// recovery. The same policy applies to BATCH records: a torn
			// tail batch (body past EOF) is invisible, but a CRC mismatch
			// inside an otherwise complete batch fails recovery.
			return fmt.Errorf("mid-segment CRC mismatch at offset %d: %w", recordStart, errBadCRC)
		}

		// 5. Valid record. Apply it.
		if flag == recordFlagBatch {
			entries, err := decodeBatchBody(body, recordStart)
			if err != nil {
				return fmt.Errorf("decode batch at offset %d: %w", recordStart, err)
			}
			for _, e := range entries {
				rec := &record{tstamp: tstamp, flag: e.flag, key: e.key, value: e.value}
				db.applyBatchEntryOnRecovery(seg.id, e.valuePos, rec)
			}
		} else {
			rec := &record{tstamp: tstamp, flag: flag, key: body[:keyLen]}
			if flag == recordFlagPut {
				rec.value = body[keyLen:]
			}
			db.applyRecordOnRecovery(seg.id, recordStart, rec)
		}
		lastGoodOffset = r.off

		if tstamp > db.lastTstamp {
			db.lastTstamp = tstamp
		}
	}

	// Truncate trailing torn bytes so the segment is ready for appends if
	// it becomes the active segment. By construction (the early returns
	// above when !allowTornTail), this only fires for the active segment.
	if lastGoodOffset < seg.size.Load() {
		if !allowTornTail {
			return fmt.Errorf("sealed segment %d unexpectedly has %d trailing bytes after last record", seg.id, seg.size.Load()-lastGoodOffset)
		}
		if err := seg.file.Truncate(lastGoodOffset); err != nil {
			return fmt.Errorf("truncate segment %d to %d: %w", seg.id, lastGoodOffset, err)
		}
		seg.size.Store(lastGoodOffset)
	}
	return nil
}

// applyRecordOnRecovery updates the keydir from one valid record. Because we
// scan oldest-to-newest, putIfNewer ensures later writes override earlier
// ones correctly.
func (db *DB) applyRecordOnRecovery(fileID uint32, recordStart int64, rec *record) {
	switch rec.flag {
	case recordFlagPut:
		db.keydir.putIfNewer(rec.key, keydirEntry{
			fileID:   fileID,
			valuePos: recordStart + int64(recordHeaderSize+len(rec.key)),
			valueLen: uint32(len(rec.value)),
			tstamp:   rec.tstamp,
		})
	case recordFlagTombstone:
		existing, ok := db.keydir.get(rec.key)
		if !ok || rec.tstamp >= existing.tstamp {
			db.keydir.delete(rec.key)
		}
	}
}

// applyBatchEntryOnRecovery is the per-entry recovery hook for BATCH records.
// valuePos is the absolute file offset of the entry's value bytes (already
// computed by decodeBatchBody) so we do not have to re-derive offsets here.
// Duplicate keys within a single batch share rec.tstamp; we apply them in
// order so the last entry's keydirEntry overwrites earlier ones via putIfNewer's
// >= comparison.
func (db *DB) applyBatchEntryOnRecovery(fileID uint32, valuePos int64, rec *record) {
	switch rec.flag {
	case recordFlagPut:
		db.keydir.putIfNewer(rec.key, keydirEntry{
			fileID:   fileID,
			valuePos: valuePos,
			valueLen: uint32(len(rec.value)),
			tstamp:   rec.tstamp,
		})
	case recordFlagTombstone:
		existing, ok := db.keydir.get(rec.key)
		if !ok || rec.tstamp >= existing.tstamp {
			db.keydir.delete(rec.key)
		}
	}
}

// segmentSequentialReader implements io.Reader over a segment file via pread.
// We use ReadAt rather than Read so the file's kernel-tracked position is
// not disturbed — concurrent ReadAt callers (the running engine) share the
// same fd safely.
type segmentSequentialReader struct {
	seg *segment
	off int64
}

func (r *segmentSequentialReader) Read(p []byte) (int, error) {
	segSize := r.seg.size.Load()
	if r.off >= segSize {
		return 0, io.EOF
	}
	remaining := segSize - r.off
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.seg.file.ReadAt(p, r.off)
	r.off += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

// acquireDataDirLock places an advisory exclusive flock on the data
// directory so two processes cannot share one DB. Released on Close via fd
// close.
func acquireDataDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lockfile: %w", err)
	}
	if err := flockExclusiveNonblocking(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
