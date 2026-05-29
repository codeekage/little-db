// Package wire implements the binary request/response codec for the
// little-db TCP server. It is intentionally pure: every function in this
// package operates on byte slices or io.Reader/io.Writer, and the package
// has no dependency on internal/engine. That separation lets us unit-test
// and fuzz the codec without standing up an engine, and lets the server
// package layer transport policy (deadlines, pipelining rules, overload
// limits) on top of a stable parsing surface.
//
// Frame format (request and response use the same shell):
//
//	u32 payload_len  (big-endian, 1..MaxFramePayload)
//	u8  tag          (opcode for requests, status for responses)
//	bytes(payload_len - 1)
//
// All multi-byte integers on the wire are big-endian.
//
// Opcodes (request tags):
//
//	0x01 PUT             u32 klen | key | u32 vlen | val
//	0x02 GET             u32 klen | key
//	0x03 DELETE          u32 klen | key
//	0x04 BATCH           u32 count | entry*       (count 1..MaxBatchEntries)
//	                     entry = u8 op | u32 klen | key | u32 vlen | val
//	                       op: 0x00 put, 0x01 delete
//	                       vlen MUST be 0 when op=delete (so every entry is
//	                       >= 10 bytes on the wire, which makes the
//	                       count*10 <= remaining pre-allocation check sound)
//	0x05 READKEYRANGE    u32 slen | start | u32 elen | end
//	                       slen=0 means "open start"; elen=0 means "open end"
//	0x06 PING            (empty body)
//	0x07 STATS           (empty body)
//	0x08 REPLICATE_SUBSCRIBE  u32 tag_len | resume_tag   (follower→leader)
//	0x09 REPLICATE_RECORD     bytes(record)              (leader→follower)
//
// Statuses (response tags):
//
//	0x00 OK
//	0x01 NOT_FOUND
//	0x02 BAD_REQUEST
//	0x03 INTERNAL
//	0x04 CLOSED
//	0x05 OVERLOAD
//	0x06 FOLLOWER_READ_ONLY
//
// Non-OK responses carry an error envelope as their body:
//
//	u16 msg_len (0..MaxErrMsgLen) | msg
//
// OK response bodies are opcode-specific:
//
//	PUT/DELETE/BATCH/PING: empty
//	GET:    u32 vlen | val
//	STATS:  u64 key_count | u64 bytes_on_disk
//	READKEYRANGE: a stream of frames, each either:
//	  - a page: status=OK, body = u32 pair_count | (u32 klen|key|u32 vlen|val)*
//	  - the end: status=OK, body = u32 0xFFFFFFFF
//	  - an error termination: status!=OK, body = error envelope
//
// All limits are wire limits (server-side caps that are tighter than what
// the engine would technically accept). See the individual constants.
package wire

// Wire limits. These bound parser work and allocation before any decode
// touches user-controlled bytes.
const (
	// MaxFramePayload caps the total payload (tag + body) of a single
	// frame. 32 MiB is large enough to carry one max-value PUT
	// (16 MiB value + 64 KiB key + headers) with comfortable slack.
	MaxFramePayload = 32 * 1024 * 1024

	// MaxKeyLen mirrors the engine's keydir limit. Wire validates this
	// independently so a malformed request is rejected before any engine
	// call is made.
	MaxKeyLen = 64 * 1024

	// MaxValLen mirrors the engine's value limit (16 MiB).
	MaxValLen = 16 * 1024 * 1024

	// MaxBatchEntries caps entries per BATCH frame. Lower than the
	// engine's 1 Mi cap because batches must fit in one wire frame and
	// 65_536 * (10-byte entry minimum) leaves room for non-trivial keys
	// and values without approaching MaxFramePayload.
	MaxBatchEntries = 65_536

	// MaxErrMsgLen bounds the human-readable error message that
	// accompanies every non-OK status. Keeps responses cheap and stops
	// servers from accidentally leaking long stack traces over the wire.
	MaxErrMsgLen = 1024

	// minBatchEntryBytes is the smallest possible encoding of a batch
	// entry: u8 op + u32 klen + 1-byte key + u32 vlen (= 0 for delete) = 10.
	// This is the multiplier the BATCH pre-allocation check uses when
	// validating that `count` fits in the remaining bytes before sizing
	// the entries slice.
	minBatchEntryBytes = 10

	// minRangePairBytes is the smallest possible encoding of one
	// (key,value) pair inside a READKEYRANGE response page: u32 klen +
	// 1-byte key + u32 vlen (= 0) = 9. Used by the same pre-allocation
	// guard the BATCH decoder applies, so a corrupt or malicious page
	// with pair_count=4 billion can't cause a giant []KV allocation.
	minRangePairBytes = 9

	// RangeEndSentinel is the special pair_count value that marks the
	// end of a READKEYRANGE stream. Valid pages must use pair_count <
	// RangeEndSentinel, which still allows ~4 billion pairs per page —
	// well above any realistic page that fits inside MaxFramePayload.
	RangeEndSentinel uint32 = 0xFFFFFFFF
)

// Op is a request opcode.
type Op uint8

const (
	OpPut          Op = 0x01
	OpGet          Op = 0x02
	OpDelete       Op = 0x03
	OpBatch        Op = 0x04
	OpReadKeyRange Op = 0x05
	OpPing         Op = 0x06
	OpStats        Op = 0x07
)

// String returns a stable name for the opcode, useful for log lines.
func (o Op) String() string {
	switch o {
	case OpPut:
		return "PUT"
	case OpGet:
		return "GET"
	case OpDelete:
		return "DELETE"
	case OpBatch:
		return "BATCH"
	case OpReadKeyRange:
		return "READKEYRANGE"
	case OpPing:
		return "PING"
	case OpStats:
		return "STATS"
	case OpReplicateSubscribe:
		return "REPLICATE_SUBSCRIBE"
	case OpReplicateRecord:
		return "REPLICATE_RECORD"
	case OpPromote:
		return "PROMOTE"
	default:
		return "UNKNOWN"
	}
}

// Status is a response status code.
type Status uint8

const (
	StatusOK         Status = 0x00
	StatusNotFound   Status = 0x01
	StatusBadRequest Status = 0x02
	StatusInternal   Status = 0x03
	StatusClosed     Status = 0x04
	StatusOverload   Status = 0x05
)

// String returns a stable name for the status.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusBadRequest:
		return "BAD_REQUEST"
	case StatusInternal:
		return "INTERNAL"
	case StatusClosed:
		return "CLOSED"
	case StatusOverload:
		return "OVERLOAD"
	case StatusFollowerReadOnly:
		return "FOLLOWER_READ_ONLY"
	default:
		return "UNKNOWN"
	}
}

// Batch entry operations.
const (
	BatchOpPut    uint8 = 0x00
	BatchOpDelete uint8 = 0x01
)
