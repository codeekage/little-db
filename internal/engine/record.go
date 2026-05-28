package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// On-disk record layout (little-endian):
//
//   | crc32c (4) | tstamp (8) | flag (1) | key_len (4) | val_len (4) | key | value |
//
// CRC32C covers every byte after the CRC field itself (header tail + key + value).
// A tombstone has flag = recordFlagTombstone and val_len = 0 with no value bytes.
//
// A BATCH record uses the same 21-byte header with key_len=0 and val_len equal
// to the total body size. The body is:
//
//   | count (4) | entry | entry | ... |
//
// where each entry is:
//
//   | inner_flag (1) | k_len (4) | v_len (4) | key | value |
//
// inner_flag is recordFlagPut or recordFlagTombstone.
//
// Failure handling on recovery (locked policy):
//
//   - A BATCH record whose body extends past EOF is a torn tail write. It is
//     silently truncated away — the entire batch is invisible. This is the
//     all-or-nothing crash-atomicity contract.
//   - A BATCH record whose body fits within the file but whose CRC fails is
//     mid-file corruption. Recovery aborts and returns an error; the operator
//     decides. We do NOT silently drop the batch in this case.
//
// Both branches together preserve atomicity: callers never observe a partial
// batch. The difference is whether recovery is allowed to proceed silently.

const (
	recordHeaderSize = 4 + 8 + 1 + 4 + 4 // 21 bytes
	maxKeyLen        = 64 * 1024         // 64 KiB
	maxValLen        = 16 * 1024 * 1024  // 16 MiB (per single-entry value)

	batchCountSize       = 4         // uint32 count prefix on the body
	batchEntryHeaderSize = 1 + 4 + 4 // inner_flag + k_len + v_len

	// maxBatchBodyLen caps the outer val_len of a BATCH record (i.e. the
	// total body size: count prefix + all entries). This is intentionally
	// larger than maxValLen so a batch can group many full-size entries.
	// It is enforced by readRecord and recovery as a sanity bound against
	// corrupt headers; the runtime cap on a successfully appended batch is
	// Options.MaxBatchEncodedSize, which is always <= maxBatchBodyLen +
	// recordHeaderSize.
	maxBatchBodyLen = 1 << 30 // 1 GiB

	// maxBatchEntries hard-caps how many inner entries a single BATCH
	// record may declare. Without this cap, a CRC-valid batch near
	// maxBatchBodyLen with minimum-size entries (10 bytes each) could
	// legally declare ~100M entries, forcing decodeBatchBody to allocate
	// tens of GB of batchEntryDecoded structs before the per-entry loop
	// noticed anything wrong. 1 << 20 entries is well above any realistic
	// caller need and bounds the result slice at ~64 MiB worst case.
	maxBatchEntries = 1 << 20 // 1,048,576 entries
)

const (
	recordFlagPut       byte = 0
	recordFlagTombstone byte = 1
	recordFlagBatch     byte = 2
)

var (
	errBadCRC        = errors.New("record: crc mismatch")
	errBadFlag       = errors.New("record: unknown flag")
	errKeyTooLarge   = errors.New("record: key exceeds max size")
	errValueTooLarge = errors.New("record: value exceeds max size")
	errBadBatchBody  = errors.New("record: malformed batch body")
)

// crc32cTable is the Castagnoli polynomial table. CRC32C is hardware-accelerated
// on amd64 and arm64 via the standard library.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// record is the in-memory representation of one log entry.
type record struct {
	tstamp int64
	flag   byte
	key    []byte
	value  []byte // nil for tombstones
}

// encodedSize returns the total number of bytes the record will occupy on disk.
func (r *record) encodedSize() int {
	return recordHeaderSize + len(r.key) + len(r.value)
}

// encode serializes the record into buf, which must be at least r.encodedSize() bytes.
// It returns the number of bytes written. The CRC is computed and written last.
func (r *record) encode(buf []byte) (int, error) {
	if len(r.key) == 0 || len(r.key) > maxKeyLen {
		return 0, errKeyTooLarge
	}
	if len(r.value) > maxValLen {
		return 0, errValueTooLarge
	}
	n := r.encodedSize()
	if len(buf) < n {
		return 0, io.ErrShortBuffer
	}

	// Leave CRC (bytes 0..3) for last; fill the rest of the header first.
	binary.LittleEndian.PutUint64(buf[4:12], uint64(r.tstamp))
	buf[12] = r.flag
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(r.value)))
	copy(buf[recordHeaderSize:], r.key)
	copy(buf[recordHeaderSize+len(r.key):], r.value)

	sum := crc32.Checksum(buf[4:n], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	return n, nil
}

// readRecord reads exactly one record from r. On a clean EOF before any byte is
// consumed it returns io.EOF; a truncated record returns io.ErrUnexpectedEOF so
// recovery can distinguish a healthy end-of-segment from a torn write.
//
// The returned record's key/value slices reference fresh allocations; callers may
// retain them safely.
func readRecord(r io.Reader) (*record, int, error) {
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		// io.ReadFull converts a clean EOF on the first byte into io.EOF, but a
		// partial header into io.ErrUnexpectedEOF. We want that distinction.
		return nil, 0, err
	}

	expectedCRC := binary.LittleEndian.Uint32(hdr[0:4])
	tstamp := int64(binary.LittleEndian.Uint64(hdr[4:12]))
	flag := hdr[12]
	keyLen := binary.LittleEndian.Uint32(hdr[13:17])
	valLen := binary.LittleEndian.Uint32(hdr[17:21])

	if flag != recordFlagPut && flag != recordFlagTombstone && flag != recordFlagBatch {
		return nil, recordHeaderSize, errBadFlag
	}
	if flag == recordFlagBatch {
		if keyLen != 0 {
			return nil, recordHeaderSize, errBadFlag
		}
		if valLen < batchCountSize {
			return nil, recordHeaderSize, errBadBatchBody
		}
		if valLen > maxBatchBodyLen {
			return nil, recordHeaderSize, errBadBatchBody
		}
	} else {
		if keyLen == 0 || keyLen > maxKeyLen {
			return nil, recordHeaderSize, errKeyTooLarge
		}
		if valLen > maxValLen {
			return nil, recordHeaderSize, errValueTooLarge
		}
	}

	body := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, recordHeaderSize, err
	}

	// Verify CRC over the header tail + body.
	h := crc32.New(crc32cTable)
	h.Write(hdr[4:])
	h.Write(body)
	if h.Sum32() != expectedCRC {
		return nil, recordHeaderSize + len(body), errBadCRC
	}

	rec := &record{
		tstamp: tstamp,
		flag:   flag,
		key:    body[:keyLen],
		value:  nil,
	}
	if flag == recordFlagPut {
		rec.value = body[keyLen:]
	}
	if flag == recordFlagBatch {
		// For batch records the "value" carries the entire body
		// (count + entries). Callers iterate it via decodeBatchBody.
		rec.value = body
	}
	return rec, recordHeaderSize + len(body), nil
}

// batchEntryDecoded is one parsed entry from a BATCH record body. valuePos is
// the absolute file offset of the entry's value bytes (regardless of flag, so
// tombstones expose the byte position where the value would have been — used
// only for offset arithmetic, never read).
type batchEntryDecoded struct {
	flag     byte
	key      []byte
	value    []byte // nil for tombstones
	valuePos int64
}

// decodeBatchBody parses a BATCH record body (the bytes after the 21-byte
// header) and returns one decoded entry per inner record. recordStart is the
// absolute file offset of the BATCH record's header; it is used to compute
// each entry's absolute valuePos. Slices in the returned entries alias body;
// callers that retain them across reads must copy.
func decodeBatchBody(body []byte, recordStart int64) ([]batchEntryDecoded, error) {
	if len(body) < batchCountSize {
		return nil, fmt.Errorf("%w: body too short for count", errBadBatchBody)
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	// Validate count against TWO independent bounds before allocating the
	// result slice:
	//
	//   1. maxBatchEntries — a hard cap on declared entry count. Bounds the
	//      worst-case batchEntryDecoded slice allocation at ~64 MiB even
	//      when the body is near maxBatchBodyLen (1 GiB).
	//   2. The smallest possible per-entry footprint within this specific
	//      body (batchEntryHeaderSize + 1-byte key + 0-byte value = 10
	//      bytes). Catches a corrupt small body declaring an impossible
	//      count before we start the decode loop.
	//
	// Both comparisons go through uint64 so a 32-bit `int` on a hypothetical
	// constrained build cannot overflow when count is near uint32 max.
	if uint64(count) > uint64(maxBatchEntries) {
		return nil, fmt.Errorf("%w: count=%d exceeds maxBatchEntries=%d", errBadBatchBody, count, maxBatchEntries)
	}
	const minEntryBytes = batchEntryHeaderSize + 1
	maxPossibleEntries := uint64(len(body)-batchCountSize) / uint64(minEntryBytes)
	if uint64(count) > maxPossibleEntries {
		return nil, fmt.Errorf("%w: count=%d exceeds capacity of %d-byte body", errBadBatchBody, count, len(body))
	}
	out := make([]batchEntryDecoded, 0, count)
	off := batchCountSize
	for i := uint32(0); i < count; i++ {
		if off+batchEntryHeaderSize > len(body) {
			return nil, fmt.Errorf("%w: entry %d header past body end", errBadBatchBody, i)
		}
		innerFlag := body[off]
		kLen := binary.LittleEndian.Uint32(body[off+1 : off+5])
		vLen := binary.LittleEndian.Uint32(body[off+5 : off+9])
		if innerFlag != recordFlagPut && innerFlag != recordFlagTombstone {
			return nil, fmt.Errorf("%w: entry %d unknown inner flag %d", errBadBatchBody, i, innerFlag)
		}
		if kLen == 0 || kLen > maxKeyLen {
			return nil, fmt.Errorf("%w: entry %d invalid key length %d", errBadBatchBody, i, kLen)
		}
		if vLen > maxValLen {
			return nil, fmt.Errorf("%w: entry %d invalid value length %d", errBadBatchBody, i, vLen)
		}
		if innerFlag == recordFlagTombstone && vLen != 0 {
			return nil, fmt.Errorf("%w: entry %d tombstone with non-zero value", errBadBatchBody, i)
		}
		kvEnd := off + batchEntryHeaderSize + int(kLen) + int(vLen)
		if kvEnd > len(body) {
			return nil, fmt.Errorf("%w: entry %d body past body end", errBadBatchBody, i)
		}
		key := body[off+batchEntryHeaderSize : off+batchEntryHeaderSize+int(kLen)]
		var value []byte
		if innerFlag == recordFlagPut {
			value = body[off+batchEntryHeaderSize+int(kLen) : kvEnd]
		}
		absValuePos := recordStart + int64(recordHeaderSize+off+batchEntryHeaderSize+int(kLen))
		out = append(out, batchEntryDecoded{
			flag:     innerFlag,
			key:      key,
			value:    value,
			valuePos: absValuePos,
		})
		off = kvEnd
	}
	if off != len(body) {
		return nil, fmt.Errorf("%w: %d trailing bytes after %d entries", errBadBatchBody, len(body)-off, count)
	}
	return out, nil
}

// encodedBatchBodySize returns the size of the body of a BATCH record
// (count prefix + every entry's header + key + value).
func encodedBatchBodySize(entries []BatchEntry) int {
	n := batchCountSize
	for i := range entries {
		n += batchEntryHeaderSize + len(entries[i].Key)
		if !entries[i].Delete {
			n += len(entries[i].Value)
		}
	}
	return n
}

// encodeBatchRecord serializes a BATCH record into buf. buf must be at least
// recordHeaderSize + encodedBatchBodySize(entries) bytes. The caller has
// already validated each entry's key/value sizes; we re-check here defensively
// so any encoder bug surfaces at encode time, not as a corrupt segment.
func encodeBatchRecord(buf []byte, tstamp int64, entries []BatchEntry) (int, error) {
	bodyLen := encodedBatchBodySize(entries)
	total := recordHeaderSize + bodyLen
	if len(buf) < total {
		return 0, io.ErrShortBuffer
	}

	binary.LittleEndian.PutUint64(buf[4:12], uint64(tstamp))
	buf[12] = recordFlagBatch
	binary.LittleEndian.PutUint32(buf[13:17], 0) // key_len = 0
	binary.LittleEndian.PutUint32(buf[17:21], uint32(bodyLen))

	binary.LittleEndian.PutUint32(buf[recordHeaderSize:recordHeaderSize+4], uint32(len(entries)))
	off := recordHeaderSize + batchCountSize
	for i := range entries {
		e := &entries[i]
		if len(e.Key) == 0 || len(e.Key) > maxKeyLen {
			return 0, errKeyTooLarge
		}
		var innerFlag byte
		var v []byte
		if e.Delete {
			innerFlag = recordFlagTombstone
			v = nil
		} else {
			if len(e.Value) > maxValLen {
				return 0, errValueTooLarge
			}
			innerFlag = recordFlagPut
			v = e.Value
		}
		buf[off] = innerFlag
		binary.LittleEndian.PutUint32(buf[off+1:off+5], uint32(len(e.Key)))
		binary.LittleEndian.PutUint32(buf[off+5:off+9], uint32(len(v)))
		copy(buf[off+batchEntryHeaderSize:], e.Key)
		copy(buf[off+batchEntryHeaderSize+len(e.Key):], v)
		off += batchEntryHeaderSize + len(e.Key) + len(v)
	}

	sum := crc32.Checksum(buf[4:total], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	return total, nil
}
