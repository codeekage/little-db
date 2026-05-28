package wire

import (
	"encoding/binary"
	"io"
)

// Replication wire surface.
//
// This file extends the base wire codec (wire.go / request.go / response.go)
// with the two opcodes that drive leader→follower streaming:
//
//	0x08 REPLICATE_SUBSCRIBE  follower → leader, body = u32 tag_len | resume_tag
//	0x09 REPLICATE_RECORD     leader → follower, body = encoded bitcask record
//
// and one new status code:
//
//	0x06 FOLLOWER_READ_ONLY   error envelope carries the leader address hint
//
// The opcodes live in a reserved range above the v0.1.0 frozen ops so that
// follower↔leader compatibility is not entangled with client↔server
// compatibility. See docs/replication.md §4 for the full design rationale.
//
// Key design notes:
//
//   - REPLICATE_RECORD wraps the *exact* bytes the leader appended to its
//     segment, including the record CRC. Follower revalidates the CRC on
//     apply, so any bit flip in transit fails closed instead of being
//     silently committed. This is why the wire-level record body is not
//     re-parsed here; it is passed through as a uint8 buffer.
//
//   - REPLICATE_SUBSCRIBE carries an opaque "resume tag" so a future
//     snapshot-bootstrap implementation can add resume-from-cursor without
//     a wire protocol revision. v0.1.0 followers always send an empty tag
//     and the leader streams from "now".

// Replication wire limits.
const (
	// MaxResumeTagLen bounds the opaque cursor on REPLICATE_SUBSCRIBE.
	// 64 KiB is far more than any cursor we anticipate (segment id +
	// offset is ~16 bytes); the slack is for future schemes (e.g.
	// snapshot manifest hash + segment range). The cap is well under
	// MaxFramePayload so a malicious tag cannot inflate parser work.
	MaxResumeTagLen = 64 * 1024

	// MaxReplicationRecord caps the inner record bytes in a single
	// REPLICATE_RECORD frame. One opcode byte already consumes the frame
	// tag, so the record body must fit in MaxFramePayload - 1.
	//
	// Cross-package contract: the engine's default MaxBatchEncodedSize
	// (64 MiB) is intentionally larger than this cap, since the codec is
	// also used by single-node deployments without replication. When
	// replication is enabled the leader MUST be opened with
	// MaxBatchEncodedSize <= MaxReplicationRecord, otherwise an oversize
	// batch would be appended to the local segment successfully but the
	// publisher would have no legal frame to wrap it in. The engine
	// enforces this in Open when Options.ReplicationBufferSize > 0
	// (see internal/engine/engine.go: "exceeds wire.MaxReplicationRecord").
	// Chunked replication records were considered and rejected as out
	// of scope for v0.1.0. A single-PUT max record (~16.06 MiB: 64 KiB
	// key + 16 MiB value + ~28 B header) fits with comfortable slack.
	MaxReplicationRecord = MaxFramePayload - 1
)

// Replication opcodes. Numbered above the v0.1.0 frozen ops on purpose:
// older clients should never see these tags, and older servers must reject
// them with BAD_REQUEST as "unknown opcode".
const (
	OpReplicateSubscribe Op = 0x08
	OpReplicateRecord    Op = 0x09
)

// Replication status. Used by followers to reject writes with a payload
// that hints at the leader address.
const (
	StatusFollowerReadOnly Status = 0x06
)

// ReplicateSubscribeRequest carries a follower's initial subscribe frame.
// ResumeTag is opaque to the wire codec; the leader interprets it. An
// empty tag means "stream from now".
type ReplicateSubscribeRequest struct {
	ResumeTag []byte
}

// Op implements Request.
func (*ReplicateSubscribeRequest) Op() Op { return OpReplicateSubscribe }

// EncodeReplicateSubscribe returns the body (tag-less) of a
// REPLICATE_SUBSCRIBE frame: u32 tag_len | resume_tag.
func EncodeReplicateSubscribe(resumeTag []byte) ([]byte, error) {
	if uint64(len(resumeTag)) > MaxResumeTagLen {
		return nil, asProtocolErr("REPLICATE_SUBSCRIBE: resume_tag_len=%d exceeds max=%d", len(resumeTag), MaxResumeTagLen)
	}
	buf := make([]byte, 4+len(resumeTag))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(resumeTag)))
	copy(buf[4:], resumeTag)
	return buf, nil
}

// WriteReplicateSubscribe writes one REPLICATE_SUBSCRIBE frame to w.
func WriteReplicateSubscribe(w io.Writer, resumeTag []byte) error {
	body, err := EncodeReplicateSubscribe(resumeTag)
	if err != nil {
		return err
	}
	return WriteFrame(w, uint8(OpReplicateSubscribe), body)
}

// decodeReplicateSubscribe parses a REPLICATE_SUBSCRIBE body.
func decodeReplicateSubscribe(body []byte) (*ReplicateSubscribeRequest, error) {
	if len(body) < 4 {
		return nil, asProtocolErr("REPLICATE_SUBSCRIBE: truncated tag_len header (have %d, want >=4)", len(body))
	}
	tlen := binary.BigEndian.Uint32(body[0:4])
	if uint64(tlen) > MaxResumeTagLen {
		return nil, asProtocolErr("REPLICATE_SUBSCRIBE: resume_tag_len=%d exceeds max=%d", tlen, MaxResumeTagLen)
	}
	want := 4 + uint64(tlen)
	if uint64(len(body)) != want {
		return nil, asProtocolErr("REPLICATE_SUBSCRIBE: body length %d does not match resume_tag_len=%d (want %d)", len(body), tlen, want)
	}
	req := &ReplicateSubscribeRequest{}
	if tlen > 0 {
		req.ResumeTag = make([]byte, tlen)
		copy(req.ResumeTag, body[4:])
	}
	return req, nil
}

// EncodeReplicateRecord returns the body (tag-less) of a REPLICATE_RECORD
// frame. The record bytes are passed through verbatim — the follower
// revalidates the embedded CRC on apply.
func EncodeReplicateRecord(record []byte) ([]byte, error) {
	if len(record) == 0 {
		return nil, asProtocolErr("REPLICATE_RECORD: empty record")
	}
	if uint64(len(record)) > MaxReplicationRecord {
		return nil, asProtocolErr("REPLICATE_RECORD: record_len=%d exceeds max=%d", len(record), MaxReplicationRecord)
	}
	out := make([]byte, len(record))
	copy(out, record)
	return out, nil
}

// WriteReplicateRecord writes one REPLICATE_RECORD frame to w. Intended
// for the leader's replication publisher; it does not flush w (the
// caller's bufio.Writer policy applies).
func WriteReplicateRecord(w io.Writer, record []byte) error {
	body, err := EncodeReplicateRecord(record)
	if err != nil {
		return err
	}
	return WriteFrame(w, uint8(OpReplicateRecord), body)
}

// DecodeReplicateRecord parses the body of a REPLICATE_RECORD frame. The
// returned slice is a fresh copy so the caller can hold it past the
// frame's lifetime.
func DecodeReplicateRecord(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return nil, asProtocolErr("REPLICATE_RECORD: empty record")
	}
	if uint64(len(body)) > MaxReplicationRecord {
		return nil, asProtocolErr("REPLICATE_RECORD: record_len=%d exceeds max=%d", len(body), MaxReplicationRecord)
	}
	out := make([]byte, len(body))
	copy(out, body)
	return out, nil
}

// ReadReplicateRecord reads one frame from r and asserts that it is a
// REPLICATE_RECORD. Used by the follower's apply loop. Returns
// io.EOF if the leader closed the stream cleanly.
func ReadReplicateRecord(r io.Reader) ([]byte, error) {
	tag, body, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	if Op(tag) != OpReplicateRecord {
		return nil, asProtocolErr("expected REPLICATE_RECORD (0x%02x), got 0x%02x", uint8(OpReplicateRecord), tag)
	}
	return DecodeReplicateRecord(body)
}
