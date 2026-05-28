package wire

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Request is the sum type of all decoded request bodies. Implementations
// expose Op() for dispatch and KeySpace() for the union of bytes the
// request will operate on (used by some tests; the server normally just
// type-switches).
type Request interface {
	Op() Op
}

// PutRequest carries a PUT command.
type PutRequest struct {
	Key   []byte
	Value []byte
}

func (*PutRequest) Op() Op { return OpPut }

// GetRequest carries a GET command.
type GetRequest struct {
	Key []byte
}

func (*GetRequest) Op() Op { return OpGet }

// DeleteRequest carries a DELETE command.
type DeleteRequest struct {
	Key []byte
}

func (*DeleteRequest) Op() Op { return OpDelete }

// BatchEntry is one entry inside a BATCH request.
type BatchEntry struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// BatchRequest carries a BATCH command.
type BatchRequest struct {
	Entries []BatchEntry
}

func (*BatchRequest) Op() Op { return OpBatch }

// ReadKeyRangeRequest carries a READKEYRANGE command. Start==nil means
// "open start" (begin at the smallest key); End==nil means "open end"
// (continue past the largest key). A request where Start and End are both
// non-nil with bytes.Compare(Start, End) > 0 is rejected with
// BAD_REQUEST.
type ReadKeyRangeRequest struct {
	Start []byte
	End   []byte
}

func (*ReadKeyRangeRequest) Op() Op { return OpReadKeyRange }

// PingRequest carries a PING command. Body is empty.
type PingRequest struct{}

func (*PingRequest) Op() Op { return OpPing }

// StatsRequest carries a STATS command. Body is empty.
type StatsRequest struct{}

func (*StatsRequest) Op() Op { return OpStats }

// DecodeRequest parses a request body (the bytes after the opcode in a
// frame) into a Request. The opcode is passed separately because the
// caller has already read the frame.
//
// Returns *ProtocolError on any semantic problem (truncated body, oversize
// key/value, unknown opcode, malformed batch entry, etc.). The server
// translates ProtocolError into a BAD_REQUEST response and keeps the
// connection alive.
func DecodeRequest(op Op, body []byte) (Request, error) {
	switch op {
	case OpPut:
		return decodePut(body)
	case OpGet:
		req, err := decodeKeyOnly(body, "GET")
		if err != nil {
			return nil, err
		}
		return &GetRequest{Key: req}, nil
	case OpDelete:
		req, err := decodeKeyOnly(body, "DELETE")
		if err != nil {
			return nil, err
		}
		return &DeleteRequest{Key: req}, nil
	case OpBatch:
		return decodeBatch(body)
	case OpReadKeyRange:
		return decodeReadKeyRange(body)
	case OpPing:
		if len(body) != 0 {
			return nil, asProtocolErr("PING: expected empty body, got %d bytes", len(body))
		}
		return &PingRequest{}, nil
	case OpStats:
		if len(body) != 0 {
			return nil, asProtocolErr("STATS: expected empty body, got %d bytes", len(body))
		}
		return &StatsRequest{}, nil
	default:
		return nil, asProtocolErr("unknown opcode 0x%02x", uint8(op))
	}
}

// ReadRequest reads one full request frame from r and decodes it.
// FrameError (transport-level, close the connection) and ProtocolError
// (semantically bad, reply BAD_REQUEST) are distinguishable via errors.As.
func ReadRequest(r io.Reader) (Request, error) {
	tag, body, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	return DecodeRequest(Op(tag), body)
}

func decodeKeyOnly(body []byte, opName string) ([]byte, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("%s: truncated key_len header (have %d, want >=4)", opName, len(body))
	}
	klen := binary.BigEndian.Uint32(body[0:4])
	if klen < 1 {
		return nil, asProtocolErr("%s: empty key", opName)
	}
	if klen > MaxKeyLen {
		return nil, asProtocolErr("%s: key_len=%d exceeds max=%d", opName, klen, MaxKeyLen)
	}
	if uint64(len(body)) != 4+uint64(klen) {
		return nil, asProtocolErr("%s: body length %d does not match key_len=%d (want %d)", opName, len(body), klen, 4+klen)
	}
	// Copy out so the caller doesn't depend on the lifetime of the
	// frame buffer (the server reuses or releases it after decode).
	key := make([]byte, klen)
	copy(key, body[4:])
	return key, nil
}

func decodePut(body []byte) (*PutRequest, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("PUT: truncated key_len header (have %d, want >=4)", len(body))
	}
	klen := binary.BigEndian.Uint32(body[0:4])
	if klen < 1 {
		return nil, asProtocolErr("PUT: empty key")
	}
	if klen > MaxKeyLen {
		return nil, asProtocolErr("PUT: key_len=%d exceeds max=%d", klen, MaxKeyLen)
	}
	if uint64(len(body)) < 4+uint64(klen)+4 {
		return nil, asProtocolErr("PUT: truncated before val_len (have %d, need >=%d)", len(body), 4+klen+4)
	}
	valLenOff := 4 + int(klen)
	vlen := binary.BigEndian.Uint32(body[valLenOff : valLenOff+4])
	if vlen > MaxValLen {
		return nil, asProtocolErr("PUT: val_len=%d exceeds max=%d", vlen, MaxValLen)
	}
	want := 4 + uint64(klen) + 4 + uint64(vlen)
	if uint64(len(body)) != want {
		return nil, asProtocolErr("PUT: body length %d does not match (key_len=%d, val_len=%d, want %d)", len(body), klen, vlen, want)
	}
	key := make([]byte, klen)
	copy(key, body[4:4+klen])
	val := make([]byte, vlen)
	copy(val, body[valLenOff+4:])
	return &PutRequest{Key: key, Value: val}, nil
}

func decodeBatch(body []byte) (*BatchRequest, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("BATCH: truncated count header (have %d, want >=4)", len(body))
	}
	count := binary.BigEndian.Uint32(body[0:4])
	if count == 0 {
		return nil, asProtocolErr("BATCH: count=0 (must be >=1)")
	}
	if count > MaxBatchEntries {
		return nil, asProtocolErr("BATCH: count=%d exceeds max=%d", count, MaxBatchEntries)
	}
	remaining := body[4:]
	// Pre-allocation guard: every entry is >= minBatchEntryBytes bytes
	// on the wire, so a malicious "count = 65_536" with only a handful
	// of bytes behind it must be rejected BEFORE we make a slice of
	// `count` entries. This is the count*10 <= remaining check from the
	// signed-off spec.
	if uint64(count)*minBatchEntryBytes > uint64(len(remaining)) {
		return nil, asProtocolErr("BATCH: count=%d implies >=%d body bytes, have %d", count, uint64(count)*minBatchEntryBytes, len(remaining))
	}
	entries := make([]BatchEntry, 0, count)
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+1+4 > len(remaining) {
			return nil, asProtocolErr("BATCH entry %d: truncated header at offset %d", i, off)
		}
		op := remaining[off]
		off++
		klen := binary.BigEndian.Uint32(remaining[off : off+4])
		off += 4
		if klen < 1 {
			return nil, asProtocolErr("BATCH entry %d: empty key", i)
		}
		if klen > MaxKeyLen {
			return nil, asProtocolErr("BATCH entry %d: key_len=%d exceeds max=%d", i, klen, MaxKeyLen)
		}
		if uint64(off)+uint64(klen)+4 > uint64(len(remaining)) {
			return nil, asProtocolErr("BATCH entry %d: truncated before val_len", i)
		}
		key := make([]byte, klen)
		copy(key, remaining[off:off+int(klen)])
		off += int(klen)
		vlen := binary.BigEndian.Uint32(remaining[off : off+4])
		off += 4
		if vlen > MaxValLen {
			return nil, asProtocolErr("BATCH entry %d: val_len=%d exceeds max=%d", i, vlen, MaxValLen)
		}
		if uint64(off)+uint64(vlen) > uint64(len(remaining)) {
			return nil, asProtocolErr("BATCH entry %d: truncated value (need %d more bytes)", i, vlen)
		}
		var val []byte
		isDelete := false
		switch op {
		case BatchOpPut:
			val = make([]byte, vlen)
			copy(val, remaining[off:off+int(vlen)])
		case BatchOpDelete:
			if vlen != 0 {
				return nil, asProtocolErr("BATCH entry %d: delete op must have val_len=0, got %d", i, vlen)
			}
			isDelete = true
		default:
			return nil, asProtocolErr("BATCH entry %d: unknown op 0x%02x", i, op)
		}
		off += int(vlen)
		entries = append(entries, BatchEntry{
			Delete: isDelete,
			Key:    key,
			Value:  val,
		})
	}
	if off != len(remaining) {
		return nil, asProtocolErr("BATCH: %d trailing bytes after %d entries", len(remaining)-off, count)
	}
	return &BatchRequest{Entries: entries}, nil
}

func decodeReadKeyRange(body []byte) (*ReadKeyRangeRequest, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("READKEYRANGE: truncated start_len header (have %d, want >=4)", len(body))
	}
	slen := binary.BigEndian.Uint32(body[0:4])
	if slen > MaxKeyLen {
		return nil, asProtocolErr("READKEYRANGE: start_len=%d exceeds max=%d", slen, MaxKeyLen)
	}
	if uint64(len(body)) < 4+uint64(slen)+4 {
		return nil, asProtocolErr("READKEYRANGE: truncated before end_len (have %d, need >=%d)", len(body), 4+slen+4)
	}
	endLenOff := 4 + int(slen)
	elen := binary.BigEndian.Uint32(body[endLenOff : endLenOff+4])
	if elen > MaxKeyLen {
		return nil, asProtocolErr("READKEYRANGE: end_len=%d exceeds max=%d", elen, MaxKeyLen)
	}
	want := 4 + uint64(slen) + 4 + uint64(elen)
	if uint64(len(body)) != want {
		return nil, asProtocolErr("READKEYRANGE: body length %d does not match (start_len=%d, end_len=%d, want %d)", len(body), slen, elen, want)
	}
	req := &ReadKeyRangeRequest{}
	if slen > 0 {
		req.Start = make([]byte, slen)
		copy(req.Start, body[4:4+slen])
	}
	if elen > 0 {
		req.End = make([]byte, elen)
		copy(req.End, body[endLenOff+4:])
	}
	// Documented boundary rule: when both bounds are present, start must
	// be <= end. start==end is allowed (and yields a zero-pair stream).
	if req.Start != nil && req.End != nil && bytes.Compare(req.Start, req.End) > 0 {
		return nil, asProtocolErr("READKEYRANGE: start > end")
	}
	return req, nil
}

// EncodeRequest encodes a Request into a full frame (length-prefixed). This
// is used by tests and clients; the server only decodes requests. Returns
// an error if any field exceeds its wire limit.
func EncodeRequest(req Request) ([]byte, error) {
	body, err := encodeRequestBody(req)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(uint8(req.Op()), body)
}

func encodeRequestBody(req Request) ([]byte, error) {
	switch r := req.(type) {
	case *PutRequest:
		if err := checkKey(r.Key, "PUT"); err != nil {
			return nil, err
		}
		if len(r.Value) > MaxValLen {
			return nil, asProtocolErr("PUT: val_len=%d exceeds max=%d", len(r.Value), MaxValLen)
		}
		buf := make([]byte, 4+len(r.Key)+4+len(r.Value))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(r.Key)))
		copy(buf[4:], r.Key)
		off := 4 + len(r.Key)
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.Value)))
		copy(buf[off+4:], r.Value)
		return buf, nil
	case *GetRequest:
		return encodeKeyOnly(r.Key, "GET")
	case *DeleteRequest:
		return encodeKeyOnly(r.Key, "DELETE")
	case *BatchRequest:
		return encodeBatch(r)
	case *ReadKeyRangeRequest:
		return encodeReadKeyRange(r)
	case *PingRequest:
		return nil, nil
	case *StatsRequest:
		return nil, nil
	default:
		return nil, asProtocolErr("unknown request type %T", req)
	}
}

func encodeKeyOnly(key []byte, opName string) ([]byte, error) {
	if err := checkKey(key, opName); err != nil {
		return nil, err
	}
	buf := make([]byte, 4+len(key))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	return buf, nil
}

func encodeBatch(r *BatchRequest) ([]byte, error) {
	if len(r.Entries) == 0 {
		return nil, asProtocolErr("BATCH: count=0 (must be >=1)")
	}
	if len(r.Entries) > MaxBatchEntries {
		return nil, asProtocolErr("BATCH: count=%d exceeds max=%d", len(r.Entries), MaxBatchEntries)
	}
	// Track the encoded body size in uint64 and enforce the frame cap
	// inside the validation loop, BEFORE the allocation below. Without
	// this check a client that sends many MaxValLen entries can force a
	// multi-GiB allocation here even though EncodeFrame would later
	// reject the frame: the allocation has already happened. The body
	// must fit alongside a 1-byte opcode in MaxFramePayload, hence the
	// `MaxFramePayload - 1` ceiling.
	const maxBody = uint64(MaxFramePayload) - 1
	size := uint64(4) // u32 count
	for i, e := range r.Entries {
		if err := checkKey(e.Key, "BATCH"); err != nil {
			return nil, asProtocolErr("BATCH entry %d: %s", i, err.Error())
		}
		if e.Delete && len(e.Value) != 0 {
			return nil, asProtocolErr("BATCH entry %d: delete op must have empty value", i)
		}
		if !e.Delete && len(e.Value) > MaxValLen {
			return nil, asProtocolErr("BATCH entry %d: val_len=%d exceeds max=%d", i, len(e.Value), MaxValLen)
		}
		// u8 op + u32 klen + key + u32 vlen + val
		entrySize := uint64(1) + 4 + uint64(len(e.Key)) + 4 + uint64(len(e.Value))
		if size+entrySize > maxBody {
			return nil, asProtocolErr(
				"BATCH: encoded body would exceed wire max (running=%d, entry=%d adds to %d, max=%d)",
				size, i, size+entrySize, maxBody)
		}
		size += entrySize
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(r.Entries)))
	off := 4
	for _, e := range r.Entries {
		if e.Delete {
			buf[off] = BatchOpDelete
		} else {
			buf[off] = BatchOpPut
		}
		off++
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.Key)))
		off += 4
		copy(buf[off:], e.Key)
		off += len(e.Key)
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.Value)))
		off += 4
		copy(buf[off:], e.Value)
		off += len(e.Value)
	}
	return buf, nil
}

func encodeReadKeyRange(r *ReadKeyRangeRequest) ([]byte, error) {
	if len(r.Start) > MaxKeyLen {
		return nil, asProtocolErr("READKEYRANGE: start_len=%d exceeds max=%d", len(r.Start), MaxKeyLen)
	}
	if len(r.End) > MaxKeyLen {
		return nil, asProtocolErr("READKEYRANGE: end_len=%d exceeds max=%d", len(r.End), MaxKeyLen)
	}
	if len(r.Start) > 0 && len(r.End) > 0 && bytes.Compare(r.Start, r.End) > 0 {
		return nil, asProtocolErr("READKEYRANGE: start > end")
	}
	buf := make([]byte, 4+len(r.Start)+4+len(r.End))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(r.Start)))
	copy(buf[4:], r.Start)
	off := 4 + len(r.Start)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.End)))
	copy(buf[off+4:], r.End)
	return buf, nil
}

func checkKey(key []byte, opName string) error {
	if len(key) == 0 {
		return asProtocolErr("%s: empty key", opName)
	}
	if len(key) > MaxKeyLen {
		return asProtocolErr("%s: key_len=%d exceeds max=%d", opName, len(key), MaxKeyLen)
	}
	return nil
}
