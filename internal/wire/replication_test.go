package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// --- REPLICATE_SUBSCRIBE -------------------------------------------------

func TestReplicateSubscribeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		tag  []byte
	}{
		{"empty resume tag (stream from now)", nil},
		{"opaque cursor", []byte("seg=42;off=1024")},
		{"max length", bytes.Repeat([]byte{'x'}, MaxResumeTagLen)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteReplicateSubscribe(&buf, tc.tag); err != nil {
				t.Fatalf("WriteReplicateSubscribe: %v", err)
			}
			req, err := ReadRequest(&buf)
			if err != nil {
				t.Fatalf("ReadRequest: %v", err)
			}
			sub, ok := req.(*ReplicateSubscribeRequest)
			if !ok {
				t.Fatalf("decoded type: got %T, want *ReplicateSubscribeRequest", req)
			}
			if !bytes.Equal(sub.ResumeTag, tc.tag) {
				t.Fatalf("ResumeTag mismatch: got %q want %q", sub.ResumeTag, tc.tag)
			}
		})
	}
}

func TestReplicateSubscribeRejectsOversizeTag(t *testing.T) {
	_, err := EncodeReplicateSubscribe(bytes.Repeat([]byte{'x'}, MaxResumeTagLen+1))
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("want ProtocolError, got %T %v", err, err)
	}
}

func TestDecodeReplicateSubscribeRejectsTruncated(t *testing.T) {
	cases := map[string][]byte{
		"truncated tag_len header":    {0x00, 0x00, 0x01},
		"declared len exceeds bytes":  append([]byte{0x00, 0x00, 0x00, 0x10}, bytes.Repeat([]byte{'x'}, 8)...),
		"trailing bytes after tag":    append([]byte{0x00, 0x00, 0x00, 0x02}, []byte{'a', 'b', 'c'}...),
		"declared len above MaxLen": func() []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, MaxResumeTagLen+1)
			return b
		}(),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeReplicateSubscribe(body)
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("want ProtocolError, got %T %v", err, err)
			}
		})
	}
}

// --- REPLICATE_RECORD ----------------------------------------------------

func TestReplicateRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		record []byte
	}{
		{"tiny record", []byte{0x01, 0x02, 0x03, 0x04}},
		{"realistic 1 KiB", bytes.Repeat([]byte{'r'}, 1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteReplicateRecord(&buf, tc.record); err != nil {
				t.Fatalf("WriteReplicateRecord: %v", err)
			}
			got, err := ReadReplicateRecord(&buf)
			if err != nil {
				t.Fatalf("ReadReplicateRecord: %v", err)
			}
			if !bytes.Equal(got, tc.record) {
				t.Fatalf("record mismatch")
			}
		})
	}
}

func TestReplicateRecordRejectsEmpty(t *testing.T) {
	if _, err := EncodeReplicateRecord(nil); err == nil {
		t.Fatalf("encode empty: want error, got nil")
	}
	if _, err := DecodeReplicateRecord(nil); err == nil {
		t.Fatalf("decode empty: want error, got nil")
	}
}

func TestReplicateRecordRejectsOversize(t *testing.T) {
	_, err := EncodeReplicateRecord(bytes.Repeat([]byte{'x'}, MaxReplicationRecord+1))
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("want ProtocolError, got %T %v", err, err)
	}
}

func TestReplicateRecordCopiesOnDecode(t *testing.T) {
	// The decoded slice must not alias the frame buffer, since the
	// follower's apply path can hold the record past the next ReadFrame.
	body := []byte{0xde, 0xad, 0xbe, 0xef}
	out, err := DecodeReplicateRecord(body)
	if err != nil {
		t.Fatalf("DecodeReplicateRecord: %v", err)
	}
	out[0] = 0x00
	if body[0] != 0xde {
		t.Fatalf("decode did not copy: mutating output also mutated input (body=%x)", body)
	}
}

func TestReadReplicateRecordRejectsWrongOp(t *testing.T) {
	var buf bytes.Buffer
	// Write a PING frame; ReadReplicateRecord should reject it.
	if err := WriteFrame(&buf, uint8(OpPing), nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	_, err := ReadReplicateRecord(&buf)
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("want ProtocolError, got %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "REPLICATE_RECORD") {
		t.Fatalf("error should mention the expected op; got %q", err.Error())
	}
}

func TestReadReplicateRecordEOF(t *testing.T) {
	// Clean EOF before any frame → bare io.EOF (subscription closed
	// cleanly from the follower's perspective).
	_, err := ReadReplicateRecord(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
}

// --- opcode & status registry --------------------------------------------

func TestReplicationOpStrings(t *testing.T) {
	if got := OpReplicateSubscribe.String(); got != "REPLICATE_SUBSCRIBE" {
		t.Fatalf("OpReplicateSubscribe.String() = %q", got)
	}
	if got := OpReplicateRecord.String(); got != "REPLICATE_RECORD" {
		t.Fatalf("OpReplicateRecord.String() = %q", got)
	}
}

func TestFollowerReadOnlyStatusString(t *testing.T) {
	if got := StatusFollowerReadOnly.String(); got != "FOLLOWER_READ_ONLY" {
		t.Fatalf("StatusFollowerReadOnly.String() = %q", got)
	}
}

func TestReplicationOpcodesDoNotCollide(t *testing.T) {
	// The replication opcodes must sit above the v0.1.0 frozen range so
	// older clients/servers reject them cleanly as unknown.
	frozen := []Op{OpPut, OpGet, OpDelete, OpBatch, OpReadKeyRange, OpPing, OpStats}
	for _, fr := range frozen {
		if fr == OpReplicateSubscribe || fr == OpReplicateRecord {
			t.Fatalf("replication opcode collides with frozen op 0x%02x", uint8(fr))
		}
	}
}

func TestDecodeRequestDispatchesReplicateSubscribe(t *testing.T) {
	body, err := EncodeReplicateSubscribe([]byte("cursor"))
	if err != nil {
		t.Fatalf("EncodeReplicateSubscribe: %v", err)
	}
	req, err := DecodeRequest(OpReplicateSubscribe, body)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if _, ok := req.(*ReplicateSubscribeRequest); !ok {
		t.Fatalf("got %T, want *ReplicateSubscribeRequest", req)
	}
}
