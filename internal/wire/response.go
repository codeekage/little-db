package wire

import (
	"encoding/binary"
	"errors"
	"io"
)

// KV is one (key, value) pair returned by READKEYRANGE pages.
type KV struct {
	Key   []byte
	Value []byte
}

// EncodeError encodes the body of a non-OK response (the error envelope:
// u16 msg_len | msg). The status byte is supplied as the frame tag at the
// WriteFrame layer, so it does not appear in the returned body.
//
// Long messages are truncated to MaxErrMsgLen so a single misbehaving
// caller cannot inflate response sizes.
func EncodeError(msg string) []byte {
	if len(msg) > MaxErrMsgLen {
		msg = msg[:MaxErrMsgLen]
	}
	buf := make([]byte, 2+len(msg))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(msg)))
	copy(buf[2:], msg)
	return buf
}

// DecodeError parses an error envelope body.
func DecodeError(body []byte) (string, error) {
	if len(body) < 2 {
		return "", asProtocolErr("error envelope: truncated msg_len (have %d, want >=2)", len(body))
	}
	mlen := binary.BigEndian.Uint16(body[0:2])
	if uint64(mlen) > MaxErrMsgLen {
		return "", asProtocolErr("error envelope: msg_len=%d exceeds max=%d", mlen, MaxErrMsgLen)
	}
	if uint64(len(body)) != 2+uint64(mlen) {
		return "", asProtocolErr("error envelope: body length %d does not match msg_len=%d", len(body), mlen)
	}
	return string(body[2:]), nil
}

// WriteError writes a non-OK response frame to w.
func WriteError(w io.Writer, status Status, msg string) error {
	if status == StatusOK {
		return asFrameErr("WriteError called with StatusOK")
	}
	return WriteFrame(w, uint8(status), EncodeError(msg))
}

// EncodeGetOK encodes the body of a successful GET response: u32 vlen | val.
func EncodeGetOK(value []byte) ([]byte, error) {
	if len(value) > MaxValLen {
		return nil, asProtocolErr("GET response: val_len=%d exceeds max=%d", len(value), MaxValLen)
	}
	buf := make([]byte, 4+len(value))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(value)))
	copy(buf[4:], value)
	return buf, nil
}

// DecodeGetOK parses a successful GET response body.
func DecodeGetOK(body []byte) ([]byte, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("GET response: truncated val_len (have %d, want >=4)", len(body))
	}
	vlen := binary.BigEndian.Uint32(body[0:4])
	if vlen > MaxValLen {
		return nil, asProtocolErr("GET response: val_len=%d exceeds max=%d", vlen, MaxValLen)
	}
	if uint64(len(body)) != 4+uint64(vlen) {
		return nil, asProtocolErr("GET response: body length %d does not match val_len=%d", len(body), vlen)
	}
	out := make([]byte, vlen)
	copy(out, body[4:])
	return out, nil
}

// EncodeStatsOK encodes the body of a successful STATS response.
// 16 bytes fixed: u64 key_count | u64 bytes_on_disk.
func EncodeStatsOK(keyCount, bytesOnDisk uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], keyCount)
	binary.BigEndian.PutUint64(buf[8:16], bytesOnDisk)
	return buf
}

// DecodeStatsOK parses a successful STATS response body.
func DecodeStatsOK(body []byte) (keyCount, bytesOnDisk uint64, err error) {
	if len(body) != 16 {
		return 0, 0, asProtocolErr("STATS response: expected 16 bytes, got %d", len(body))
	}
	return binary.BigEndian.Uint64(body[0:8]), binary.BigEndian.Uint64(body[8:16]), nil
}

// EncodeRangePage encodes a READKEYRANGE response page body:
//
//	u32 pair_count | (u32 klen | key | u32 vlen | val)*
//
// pair_count must be < RangeEndSentinel. An empty page (0 pairs) is legal
// and is sometimes useful as a keep-alive between long pauses, but the
// server is not required to emit them — the receiver MUST handle both
// pageful and empty-page frames identically.
func EncodeRangePage(pairs []KV) ([]byte, error) {
	if uint64(len(pairs)) >= uint64(RangeEndSentinel) {
		return nil, asProtocolErr("range page: pair_count=%d collides with end sentinel", len(pairs))
	}
	size := 4
	for i, p := range pairs {
		if len(p.Key) == 0 {
			return nil, asProtocolErr("range page pair %d: empty key", i)
		}
		if len(p.Key) > MaxKeyLen {
			return nil, asProtocolErr("range page pair %d: key_len=%d exceeds max=%d", i, len(p.Key), MaxKeyLen)
		}
		if len(p.Value) > MaxValLen {
			return nil, asProtocolErr("range page pair %d: val_len=%d exceeds max=%d", i, len(p.Value), MaxValLen)
		}
		size += 4 + len(p.Key) + 4 + len(p.Value)
	}
	if uint64(size)+1 > MaxFramePayload {
		return nil, asProtocolErr("range page: encoded size %d exceeds frame payload max %d", size, MaxFramePayload)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(pairs)))
	off := 4
	for _, p := range pairs {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(p.Key)))
		off += 4
		copy(buf[off:], p.Key)
		off += len(p.Key)
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(p.Value)))
		off += 4
		copy(buf[off:], p.Value)
		off += len(p.Value)
	}
	return buf, nil
}

// EncodeRangeEnd encodes the stream terminator body: u32 RangeEndSentinel.
func EncodeRangeEnd() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf[0:4], RangeEndSentinel)
	return buf
}

// DecodeRangeFrame parses the body of an OK-status frame in a READKEYRANGE
// stream. It returns (pairs, end, err): exactly one of (pairs!=nil, end)
// holds for a well-formed body, and err is non-nil for any malformation.
//
// A page with zero pairs returns (empty-slice, false, nil). A frame whose
// body is exactly u32 RangeEndSentinel returns (nil, true, nil).
func DecodeRangeFrame(body []byte) (pairs []KV, end bool, err error) {
	if len(body) < 4 {
		return nil, false, asProtocolErr("range frame: truncated header (have %d, want >=4)", len(body))
	}
	count := binary.BigEndian.Uint32(body[0:4])
	if count == RangeEndSentinel {
		if len(body) != 4 {
			return nil, false, asProtocolErr("range end frame: expected 4-byte body, got %d", len(body))
		}
		return nil, true, nil
	}
	remaining := body[4:]
	// Pre-allocation guard, mirrors the BATCH decoder: a pair_count that
	// can't possibly fit in the remaining body must be rejected before
	// make([]KV, 0, count) commits to a potentially huge allocation.
	if uint64(count)*minRangePairBytes > uint64(len(remaining)) {
		return nil, false, asProtocolErr("range page: pair_count=%d implies >=%d body bytes, have %d", count, uint64(count)*minRangePairBytes, len(remaining))
	}
	pairs = make([]KV, 0, count)
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+4 > len(remaining) {
			return nil, false, asProtocolErr("range pair %d: truncated key_len", i)
		}
		klen := binary.BigEndian.Uint32(remaining[off : off+4])
		off += 4
		if klen < 1 {
			return nil, false, asProtocolErr("range pair %d: empty key", i)
		}
		if klen > MaxKeyLen {
			return nil, false, asProtocolErr("range pair %d: key_len=%d exceeds max=%d", i, klen, MaxKeyLen)
		}
		if uint64(off)+uint64(klen)+4 > uint64(len(remaining)) {
			return nil, false, asProtocolErr("range pair %d: truncated before val_len", i)
		}
		key := make([]byte, klen)
		copy(key, remaining[off:off+int(klen)])
		off += int(klen)
		vlen := binary.BigEndian.Uint32(remaining[off : off+4])
		off += 4
		if vlen > MaxValLen {
			return nil, false, asProtocolErr("range pair %d: val_len=%d exceeds max=%d", i, vlen, MaxValLen)
		}
		if uint64(off)+uint64(vlen) > uint64(len(remaining)) {
			return nil, false, asProtocolErr("range pair %d: truncated value", i)
		}
		val := make([]byte, vlen)
		copy(val, remaining[off:off+int(vlen)])
		off += int(vlen)
		pairs = append(pairs, KV{Key: key, Value: val})
	}
	if off != len(remaining) {
		return nil, false, asProtocolErr("range page: %d trailing bytes after %d pairs", len(remaining)-off, count)
	}
	return pairs, false, nil
}

// ReadRangeStream reads frames from r until either an END sentinel arrives
// (returns nil), an error response arrives (returns *RemoteError carrying
// the status + message), or any framing/protocol error occurs.
//
// For each page, pageFn is invoked with the decoded pairs. If pageFn
// returns false, ReadRangeStream stops reading and returns ErrStreamStopped.
// The caller is then responsible for closing the connection — the server
// is still streaming and there is no in-band cancellation.
func ReadRangeStream(r io.Reader, pageFn func(pairs []KV) bool) error {
	for {
		tag, body, err := ReadFrame(r)
		if err != nil {
			return err
		}
		status := Status(tag)
		if status != StatusOK {
			msg, derr := DecodeError(body)
			if derr != nil {
				return derr
			}
			return &RemoteError{Status: status, Msg: msg}
		}
		pairs, end, err := DecodeRangeFrame(body)
		if err != nil {
			return err
		}
		if end {
			return nil
		}
		if !pageFn(pairs) {
			return ErrStreamStopped
		}
	}
}

// RemoteError carries a non-OK response received from the server.
type RemoteError struct {
	Status Status
	Msg    string
}

func (e *RemoteError) Error() string {
	return "wire: remote error: " + e.Status.String() + ": " + e.Msg
}

// ErrStreamStopped is returned by ReadRangeStream when the caller-supplied
// callback returns false. The connection state is undefined; the caller
// must close it.
var ErrStreamStopped = errors.New("wire: range stream stopped by caller")
