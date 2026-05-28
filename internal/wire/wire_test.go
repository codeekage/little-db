package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// --- Frame I/O -----------------------------------------------------------

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		tag  uint8
		body []byte
	}{
		{"empty body", 0x42, nil},
		{"one byte", 0x01, []byte{0xff}},
		{"opcode put body", uint8(OpPut), []byte("hello world")},
		// max body that still fits (1 tag byte already consumed).
		{"near max", 0x07, bytes.Repeat([]byte{'a'}, 1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, tc.tag, tc.body); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			tag, body, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if tag != tc.tag {
				t.Fatalf("tag: got 0x%02x want 0x%02x", tag, tc.tag)
			}
			if !bytes.Equal(body, tc.body) {
				t.Fatalf("body mismatch: got %x want %x", body, tc.body)
			}
		})
	}
}

func TestFrameRejectsZeroPayloadLen(t *testing.T) {
	// A frame with payload_len=0 is illegal — there's no room for a tag.
	hdr := []byte{0, 0, 0, 0}
	_, _, err := ReadFrame(bytes.NewReader(hdr))
	var fe *FrameError
	if !errors.As(err, &fe) {
		t.Fatalf("want FrameError, got %T %v", err, err)
	}
}

func TestFrameRejectsOversizePayload(t *testing.T) {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, MaxFramePayload+1)
	_, _, err := ReadFrame(bytes.NewReader(hdr))
	var fe *FrameError
	if !errors.As(err, &fe) {
		t.Fatalf("want FrameError, got %T %v", err, err)
	}
}

func TestFrameEOFDistinction(t *testing.T) {
	// Clean EOF before any byte read → bare io.EOF.
	_, _, err := ReadFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
	// Partial header read → io.ErrUnexpectedEOF (from io.ReadFull on hdr).
	_, _, err = ReadFrame(bytes.NewReader([]byte{0x00, 0x00})) // 2 of 4 hdr bytes
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
	// Truncated mid-body → io.ErrUnexpectedEOF.
	var truncated bytes.Buffer
	binary.Write(&truncated, binary.BigEndian, uint32(10)) // expect 10 bytes payload, give 3
	truncated.Write([]byte{0x07, 0x01, 0x02})
	_, _, err = ReadFrame(&truncated)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF (mid-body), got %v", err)
	}
}

func TestWriteFrameRejectsOversize(t *testing.T) {
	body := make([]byte, MaxFramePayload) // +1 for tag exceeds cap
	var buf bytes.Buffer
	err := WriteFrame(&buf, 0x01, body)
	var fe *FrameError
	if !errors.As(err, &fe) {
		t.Fatalf("want FrameError, got %T %v", err, err)
	}
}

// --- Request round-trip --------------------------------------------------

func TestRequestRoundTrip(t *testing.T) {
	cases := []Request{
		&PutRequest{Key: []byte("k"), Value: []byte("v")},
		&PutRequest{Key: []byte("alpha"), Value: nil}, // zero-length value
		&GetRequest{Key: []byte("k")},
		&DeleteRequest{Key: []byte("k")},
		&BatchRequest{Entries: []BatchEntry{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Delete: true},
			{Key: []byte("c"), Value: bytes.Repeat([]byte{'x'}, 100)},
		}},
		&ReadKeyRangeRequest{Start: []byte("a"), End: []byte("z")},
		&ReadKeyRangeRequest{},                                     // both open
		&ReadKeyRangeRequest{Start: []byte("a")},                   // open end
		&ReadKeyRangeRequest{End: []byte("z")},                     // open start
		&ReadKeyRangeRequest{Start: []byte("k"), End: []byte("k")}, // equal
		&PingRequest{},
		&StatsRequest{},
	}
	for _, req := range cases {
		t.Run(req.Op().String(), func(t *testing.T) {
			frame, err := EncodeRequest(req)
			if err != nil {
				t.Fatalf("EncodeRequest: %v", err)
			}
			tag, body, err := ReadFrame(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			got, err := DecodeRequest(Op(tag), body)
			if err != nil {
				t.Fatalf("DecodeRequest: %v", err)
			}
			if got.Op() != req.Op() {
				t.Fatalf("op mismatch: got %v want %v", got.Op(), req.Op())
			}
			if !requestsEqual(got, req) {
				t.Fatalf("round-trip mismatch:\nwant %#v\n got %#v", req, got)
			}
		})
	}
}

func requestsEqual(a, b Request) bool {
	switch ar := a.(type) {
	case *PutRequest:
		br, ok := b.(*PutRequest)
		return ok && bytes.Equal(ar.Key, br.Key) && bytes.Equal(ar.Value, br.Value)
	case *GetRequest:
		br, ok := b.(*GetRequest)
		return ok && bytes.Equal(ar.Key, br.Key)
	case *DeleteRequest:
		br, ok := b.(*DeleteRequest)
		return ok && bytes.Equal(ar.Key, br.Key)
	case *BatchRequest:
		br, ok := b.(*BatchRequest)
		if !ok || len(ar.Entries) != len(br.Entries) {
			return false
		}
		for i := range ar.Entries {
			if ar.Entries[i].Delete != br.Entries[i].Delete ||
				!bytes.Equal(ar.Entries[i].Key, br.Entries[i].Key) ||
				!bytes.Equal(ar.Entries[i].Value, br.Entries[i].Value) {
				return false
			}
		}
		return true
	case *ReadKeyRangeRequest:
		br, ok := b.(*ReadKeyRangeRequest)
		return ok && bytes.Equal(ar.Start, br.Start) && bytes.Equal(ar.End, br.End)
	case *PingRequest:
		_, ok := b.(*PingRequest)
		return ok
	case *StatsRequest:
		_, ok := b.(*StatsRequest)
		return ok
	}
	return false
}

// --- Request invalid-input rejection ------------------------------------

func TestDecodeRequestRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		op   Op
		body []byte
	}{
		{"unknown opcode body", Op(0xff), nil},
		{"PUT truncated klen", OpPut, []byte{0x00, 0x00}},
		{"PUT empty key", OpPut, mustPack(uint32(0))},
		{"PUT key oversize", OpPut, mustPack(uint32(MaxKeyLen + 1))},
		{"PUT truncated vlen", OpPut, append(mustPack(uint32(3)), 'a', 'b', 'c')},
		{"PUT body too short for vlen", OpPut, func() []byte {
			b := mustPack(uint32(3))
			b = append(b, 'a', 'b', 'c')
			b = append(b, mustPack(uint32(5))...)
			b = append(b, 'h', 'i') // only 2 of 5 promised value bytes
			return b
		}()},
		{"PUT body too long", OpPut, func() []byte {
			b := mustPack(uint32(1))
			b = append(b, 'a')
			b = append(b, mustPack(uint32(0))...)
			b = append(b, 0xff) // trailing junk
			return b
		}()},
		{"GET empty body", OpGet, nil},
		{"GET empty key", OpGet, mustPack(uint32(0))},
		{"GET oversize key", OpGet, mustPack(uint32(MaxKeyLen + 1))},
		{"DELETE empty key", OpDelete, mustPack(uint32(0))},
		{"BATCH zero count", OpBatch, mustPack(uint32(0))},
		{"BATCH oversize count", OpBatch, mustPack(uint32(MaxBatchEntries + 1))},
		{"BATCH count exceeds remaining (pre-alloc check)", OpBatch, func() []byte {
			// claim 65_536 entries but only 5 bytes of body.
			b := mustPack(uint32(MaxBatchEntries))
			b = append(b, 0x00, 0x00, 0x00, 0x00, 0x00)
			return b
		}()},
		{"BATCH delete with non-zero val_len", OpBatch, func() []byte {
			b := mustPack(uint32(1))
			b = append(b, BatchOpDelete)
			b = append(b, mustPack(uint32(1))...)
			b = append(b, 'k')
			b = append(b, mustPack(uint32(2))...)
			b = append(b, 'v', 'v')
			return b
		}()},
		{"BATCH unknown op", OpBatch, func() []byte {
			b := mustPack(uint32(1))
			b = append(b, 0x77) // not put(0) or delete(1)
			b = append(b, mustPack(uint32(1))...)
			b = append(b, 'k')
			b = append(b, mustPack(uint32(0))...)
			return b
		}()},
		{"BATCH trailing bytes", OpBatch, func() []byte {
			b := mustPack(uint32(1))
			b = append(b, BatchOpPut)
			b = append(b, mustPack(uint32(1))...)
			b = append(b, 'k')
			b = append(b, mustPack(uint32(0))...)
			b = append(b, 0xff) // trailing
			return b
		}()},
		{"READKEYRANGE truncated", OpReadKeyRange, []byte{0x00}},
		{"READKEYRANGE start oversize", OpReadKeyRange, mustPack(uint32(MaxKeyLen + 1))},
		{"READKEYRANGE body shorter than start_len", OpReadKeyRange, func() []byte {
			b := mustPack(uint32(10))
			b = append(b, []byte("only5")...)
			return b
		}()},
		{"READKEYRANGE start > end", OpReadKeyRange, func() []byte {
			b := mustPack(uint32(1))
			b = append(b, 'z')
			b = append(b, mustPack(uint32(1))...)
			b = append(b, 'a')
			return b
		}()},
		{"PING with body", OpPing, []byte{0x01}},
		{"STATS with body", OpStats, []byte{0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRequest(tc.op, tc.body)
			if err == nil {
				t.Fatalf("expected error")
			}
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("expected *ProtocolError, got %T %v", err, err)
			}
		})
	}
}

// --- Response: GET / STATS / Error --------------------------------------

func TestGetOKRoundTrip(t *testing.T) {
	cases := [][]byte{nil, []byte(""), []byte("v"), bytes.Repeat([]byte{'x'}, 4096)}
	for _, val := range cases {
		body, err := EncodeGetOK(val)
		if err != nil {
			t.Fatalf("EncodeGetOK: %v", err)
		}
		got, err := DecodeGetOK(body)
		if err != nil {
			t.Fatalf("DecodeGetOK: %v", err)
		}
		if !bytes.Equal(got, val) {
			// nil vs empty: both decode to zero-length, that's fine.
			if !(len(got) == 0 && len(val) == 0) {
				t.Fatalf("mismatch: got %x want %x", got, val)
			}
		}
	}
}

func TestStatsOKRoundTrip(t *testing.T) {
	cases := []struct{ kc, bd uint64 }{
		{0, 0},
		{1, 2},
		{0xdeadbeef, 0xcafef00d},
		{^uint64(0), ^uint64(0)},
	}
	for _, c := range cases {
		body := EncodeStatsOK(c.kc, c.bd)
		if len(body) != 16 {
			t.Fatalf("stats body not 16 bytes: %d", len(body))
		}
		gotKC, gotBD, err := DecodeStatsOK(body)
		if err != nil {
			t.Fatalf("DecodeStatsOK: %v", err)
		}
		if gotKC != c.kc || gotBD != c.bd {
			t.Fatalf("stats mismatch: got (%d,%d) want (%d,%d)", gotKC, gotBD, c.kc, c.bd)
		}
	}
}

func TestStatsRejectsWrongSize(t *testing.T) {
	for _, sz := range []int{0, 1, 8, 15, 17, 32} {
		_, _, err := DecodeStatsOK(make([]byte, sz))
		if err == nil {
			t.Fatalf("size=%d: expected error", sz)
		}
	}
}

func TestErrorEnvelopeRoundTrip(t *testing.T) {
	msgs := []string{"", "bad request", "key too large", strings.Repeat("x", 500)}
	for _, m := range msgs {
		body := EncodeError(m)
		got, err := DecodeError(body)
		if err != nil {
			t.Fatalf("DecodeError: %v", err)
		}
		if got != m {
			t.Fatalf("mismatch: got %q want %q", got, m)
		}
	}
}

func TestErrorEnvelopeTruncatesOversizeMsg(t *testing.T) {
	long := strings.Repeat("x", MaxErrMsgLen+500)
	body := EncodeError(long)
	got, err := DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if len(got) != MaxErrMsgLen {
		t.Fatalf("expected truncation to %d, got %d", MaxErrMsgLen, len(got))
	}
}

func TestWriteErrorRejectsOK(t *testing.T) {
	var buf bytes.Buffer
	err := WriteError(&buf, StatusOK, "nope")
	if err == nil {
		t.Fatalf("expected error for WriteError(OK)")
	}
}

// --- READKEYRANGE stream -------------------------------------------------

func TestRangePageRoundTrip(t *testing.T) {
	pairs := []KV{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("bb"), Value: []byte("22")},
		{Key: []byte("ccc"), Value: nil},
	}
	body, err := EncodeRangePage(pairs)
	if err != nil {
		t.Fatalf("EncodeRangePage: %v", err)
	}
	got, end, err := DecodeRangeFrame(body)
	if err != nil || end {
		t.Fatalf("DecodeRangeFrame: end=%v err=%v", end, err)
	}
	if len(got) != len(pairs) {
		t.Fatalf("pair count: got %d want %d", len(got), len(pairs))
	}
	for i := range got {
		if !bytes.Equal(got[i].Key, pairs[i].Key) {
			t.Fatalf("pair %d key: got %x want %x", i, got[i].Key, pairs[i].Key)
		}
		if !bytes.Equal(got[i].Value, pairs[i].Value) && !(len(got[i].Value) == 0 && len(pairs[i].Value) == 0) {
			t.Fatalf("pair %d val: got %x want %x", i, got[i].Value, pairs[i].Value)
		}
	}
}

func TestRangeEmptyPage(t *testing.T) {
	body, err := EncodeRangePage(nil)
	if err != nil {
		t.Fatalf("EncodeRangePage(nil): %v", err)
	}
	got, end, err := DecodeRangeFrame(body)
	if err != nil || end || len(got) != 0 {
		t.Fatalf("empty page: got=%d end=%v err=%v", len(got), end, err)
	}
}

func TestRangeEndSentinel(t *testing.T) {
	body := EncodeRangeEnd()
	got, end, err := DecodeRangeFrame(body)
	if err != nil {
		t.Fatalf("DecodeRangeFrame(end): %v", err)
	}
	if !end || got != nil {
		t.Fatalf("expected end=true,pairs=nil; got end=%v pairs=%v", end, got)
	}
}

func TestReadRangeStreamHappyPath(t *testing.T) {
	pairs1 := []KV{{Key: []byte("a"), Value: []byte("1")}}
	pairs2 := []KV{{Key: []byte("b"), Value: []byte("2")}, {Key: []byte("c"), Value: []byte("3")}}
	var buf bytes.Buffer
	for _, p := range [][]KV{pairs1, pairs2} {
		body, err := EncodeRangePage(p)
		if err != nil {
			t.Fatalf("EncodeRangePage: %v", err)
		}
		if err := WriteFrame(&buf, uint8(StatusOK), body); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := WriteFrame(&buf, uint8(StatusOK), EncodeRangeEnd()); err != nil {
		t.Fatalf("WriteFrame(end): %v", err)
	}

	var collected []KV
	err := ReadRangeStream(&buf, func(p []KV) bool {
		collected = append(collected, p...)
		return true
	})
	if err != nil {
		t.Fatalf("ReadRangeStream: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("collected %d, want 3", len(collected))
	}
}

func TestReadRangeStreamErrorTermination(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteError(&buf, StatusOverload, "too many concurrent ranges"); err != nil {
		t.Fatalf("WriteError: %v", err)
	}
	err := ReadRangeStream(&buf, func(p []KV) bool { return true })
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("want *RemoteError, got %T %v", err, err)
	}
	if re.Status != StatusOverload {
		t.Fatalf("status: got %v want OVERLOAD", re.Status)
	}
}

func TestReadRangeStreamStoppedByCaller(t *testing.T) {
	var buf bytes.Buffer
	body, _ := EncodeRangePage([]KV{{Key: []byte("a"), Value: []byte("1")}})
	WriteFrame(&buf, uint8(StatusOK), body)
	WriteFrame(&buf, uint8(StatusOK), EncodeRangeEnd())

	err := ReadRangeStream(&buf, func(p []KV) bool { return false })
	if !errors.Is(err, ErrStreamStopped) {
		t.Fatalf("want ErrStreamStopped, got %v", err)
	}
}

func TestDecodeRangeFrameInvalid(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"truncated", []byte{0x00}},
		{"end sentinel wrong length", append(mustPack(RangeEndSentinel), 0xff)},
		{"oversize klen", append(mustPack(uint32(1)), mustPack(uint32(MaxKeyLen+1))...)},
		{"pair_count vastly exceeds remaining (pre-alloc guard)", func() []byte {
			// claim 100M pairs, give 5 bytes of body.
			b := mustPack(uint32(100_000_000))
			b = append(b, 0x00, 0x00, 0x00, 0x00, 0x00)
			return b
		}()},
		{"truncated value", func() []byte {
			b := mustPack(uint32(1))
			b = append(b, mustPack(uint32(1))...)
			b = append(b, 'k')
			b = append(b, mustPack(uint32(10))...)
			b = append(b, 'v') // only 1 of 10 promised bytes
			return b
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := DecodeRangeFrame(tc.body)
			if err == nil {
				t.Fatalf("expected error")
			}
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("want *ProtocolError, got %T %v", err, err)
			}
		})
	}
}

// --- helpers ------------------------------------------------------------

func mustPack(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
