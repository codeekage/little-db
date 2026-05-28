package wire

import (
	"errors"
	"testing"
)

// FuzzDecodeRequest exercises the request decoder against arbitrary input.
// The contract is that DecodeRequest must never panic, and any error it
// returns must be classifiable as *ProtocolError (no leaks of internal
// error types). When it returns successfully, re-encoding and re-decoding
// must reproduce the same request.
func FuzzDecodeRequest(f *testing.F) {
	// Seed with one valid encoding of each opcode so the fuzzer starts
	// from interesting structures rather than random noise.
	seeds := []Request{
		&PutRequest{Key: []byte("k"), Value: []byte("v")},
		&GetRequest{Key: []byte("k")},
		&DeleteRequest{Key: []byte("k")},
		&BatchRequest{Entries: []BatchEntry{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Delete: true},
		}},
		&ReadKeyRangeRequest{Start: []byte("a"), End: []byte("z")},
		&PingRequest{},
		&StatsRequest{},
	}
	for _, s := range seeds {
		body, err := encodeRequestBody(s)
		if err != nil {
			f.Fatalf("seed encode: %v", err)
		}
		f.Add(uint8(s.Op()), body)
	}

	f.Fuzz(func(t *testing.T, opByte uint8, body []byte) {
		op := Op(opByte)
		req, err := DecodeRequest(op, body)
		if err != nil {
			// Any error must be a ProtocolError. FrameError is for
			// transport-level problems and should not surface here.
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("non-protocol error: %T %v", err, err)
			}
			return
		}
		if req == nil {
			t.Fatalf("nil request with nil error")
		}
		if req.Op() != op {
			t.Fatalf("op mismatch after decode: got %v want %v", req.Op(), op)
		}
		// Round-trip: re-encode and re-decode must succeed and yield
		// an equivalent request.
		body2, err := encodeRequestBody(req)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		req2, err := DecodeRequest(op, body2)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if !requestsEqual(req, req2) {
			t.Fatalf("round-trip mismatch:\nfirst:  %#v\nsecond: %#v", req, req2)
		}
	})
}
