package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FrameError describes a transport-level framing problem. Server policy is
// to close the connection on any FrameError; the framing is desynchronized
// and there is no safe way to recover the byte stream.
type FrameError struct {
	Reason string
}

func (e *FrameError) Error() string { return "wire: frame: " + e.Reason }

// ProtocolError describes a semantically malformed but well-framed message.
// Server policy is to reply with a BAD_REQUEST response (or the closest
// applicable status) and keep the connection alive — one bad request must
// not desync the stream.
type ProtocolError struct {
	Reason string
}

func (e *ProtocolError) Error() string { return "wire: protocol: " + e.Reason }

// asFrameErr / asProtocolErr exist so callers (and tests) can switch on
// error classification with errors.As.
func asFrameErr(format string, args ...any) error {
	return &FrameError{Reason: fmt.Sprintf(format, args...)}
}

func asProtocolErr(format string, args ...any) error {
	return &ProtocolError{Reason: fmt.Sprintf(format, args...)}
}

// ReadFrame reads exactly one length-prefixed frame from r. It enforces
// 1 <= payload_len <= MaxFramePayload before any allocation.
//
// Errors:
//   - io.EOF (the bare, unwrapped sentinel) is returned if the connection
//     closes cleanly between frames — i.e. zero bytes of a new frame have
//     been read. The server treats this as graceful shutdown of the peer.
//   - io.ErrUnexpectedEOF (the bare sentinel) if the connection closes
//     in the middle of a frame.
//   - *FrameError if the payload length is out of range. The connection
//     must be closed; bytes after the bad header are not parseable.
//   - Any other io error from the reader is propagated unchanged.
//
// The returned body slice has length payload_len - 1; tag is the first
// byte of the payload (the opcode for a request, the status for a
// response).
func ReadFrame(r io.Reader) (tag uint8, body []byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		// Preserve the io.EOF / io.ErrUnexpectedEOF distinction.
		return 0, nil, err
	}
	payloadLen := binary.BigEndian.Uint32(hdr[:])
	if payloadLen < 1 {
		return 0, nil, asFrameErr("payload_len=0 (must be >= 1 to carry a tag)")
	}
	if payloadLen > MaxFramePayload {
		return 0, nil, asFrameErr("payload_len=%d exceeds max=%d", payloadLen, MaxFramePayload)
	}
	buf := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return buf[0], buf[1:], nil
}

// WriteFrame writes one length-prefixed frame to w. It enforces the same
// MaxFramePayload cap that ReadFrame applies, so a buggy server never
// produces a frame the client (or itself) would refuse to read.
func WriteFrame(w io.Writer, tag uint8, body []byte) error {
	if uint64(len(body))+1 > MaxFramePayload {
		return asFrameErr("body too large: %d bytes (max payload=%d)", len(body), MaxFramePayload)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(body)+1))
	hdr[4] = tag
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

// EncodeFrame returns the byte representation of one frame. Useful for
// callers that want to assemble a frame fully in memory (e.g. to write it
// in a single Write call to coalesce I/O, or to pre-compute a static
// response).
func EncodeFrame(tag uint8, body []byte) ([]byte, error) {
	if uint64(len(body))+1 > MaxFramePayload {
		return nil, asFrameErr("body too large: %d bytes (max payload=%d)", len(body), MaxFramePayload)
	}
	out := make([]byte, 5+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)+1))
	out[4] = tag
	copy(out[5:], body)
	return out, nil
}
