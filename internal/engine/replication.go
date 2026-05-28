package engine

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Replication publisher.
//
// The publisher is the leader-side hook in the engine's write path. After
// the writer goroutine successfully appends a record to the active segment
// (and updates the keydir), it hands the encoded record bytes to the
// publisher. A subscriber attached via DB.Subscribe receives those bytes
// on a buffered channel and forwards them as REPLICATE_RECORD frames over
// the network (server side; see internal/server). The bytes published are
// byte-identical to what landed on the leader's segment, including the
// record CRC, so the follower's apply path can revalidate end-to-end.
//
// Design rules (docs/replication.md §3):
//
//   - The publisher MUST NOT add latency to local writes. The hook is a
//     non-blocking channel send under a single mutex; if the subscriber's
//     buffer is full the record is dropped from the tail and a counter is
//     incremented. The writer never blocks.
//
//   - Drops are observable. Stats().ReplicationLagDropped surfaces the
//     count so a slow follower shows up on dashboards, not in a corrupted
//     replica.
//
//   - The hook runs after the keydir update and BEFORE the burst fsync.
//     A follower may therefore briefly observe a record that the leader
//     subsequently loses to a crash before fsync — this is the failure
//     mode promote is for (§6: "Leader power loss (storage not flushed) —
//     follower may be ahead of recovered leader"). Publishing before
//     fsync is the design choice that lets durability remain G2's
//     responsibility, not replication's.
//
//   - The publisher is opt-in. With ReplicationBufferSize=0 (the default)
//     the publish hook is a single inlined branch that returns
//     immediately, so single-node deployments pay nothing.

// Sentinels returned by DB.Subscribe.
var (
	// ErrReplicationDisabled is returned when Subscribe is called on a DB
	// opened with Options.ReplicationBufferSize == 0.
	ErrReplicationDisabled = errors.New("engine: replication is not enabled (Options.ReplicationBufferSize must be > 0)")

	// ErrAlreadySubscribed is returned when Subscribe is called while
	// another subscription is still active. The publisher serves a single
	// subscriber; the follower TCP handshake is the natural arbiter.
	ErrAlreadySubscribed = errors.New("engine: a subscription is already active")
)

// Subscription is the leader-side handle to a single replication stream.
// Records() returns the channel the writer publishes onto; the channel is
// closed when Close is called or when the DB itself shuts down. Callers
// must call Close exactly once when they are done consuming.
type Subscription struct {
	db       *DB
	ch       chan []byte
	closeMu  sync.Mutex
	closed   bool
	detached atomic.Bool
}

// Records returns the channel of byte-identical encoded records the leader
// has appended. The channel is closed (drained-then-closed) when the
// subscription is closed or when the DB closes; receiving from a closed
// channel is the normal end-of-stream signal.
func (s *Subscription) Records() <-chan []byte { return s.ch }

// Close detaches the subscription from the DB and closes the record
// channel. Idempotent; safe to call from the consumer goroutine or from a
// defer.
func (s *Subscription) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// Detach under db.subMu so we don't race with publishRecord taking
	// the same mutex. After this point publishRecord sees db.sub == nil
	// (or a different subscription, if the slot has been re-taken) and
	// will never send on s.ch again, so closing the channel is safe.
	s.db.subMu.Lock()
	if s.db.sub == s {
		s.db.sub = nil
	}
	s.db.subMu.Unlock()
	s.detached.Store(true)
	close(s.ch)
	return nil
}

// Subscribe attaches a single subscriber to the engine's replication
// stream. Returns ErrReplicationDisabled when the DB was opened without
// replication, or ErrAlreadySubscribed when another subscription is
// active.
//
// The returned subscription buffers ReplicationBufferSize records. When
// the buffer is full the writer drops new records and increments the
// Stats().ReplicationLagDropped counter; the writer never blocks. Callers
// that need to keep up MUST drain Records() promptly.
func (db *DB) Subscribe() (*Subscription, error) {
	if db.opts.ReplicationBufferSize <= 0 {
		return nil, ErrReplicationDisabled
	}
	// Refuse on a closed DB so the caller doesn't get a subscription that
	// will never deliver and never close cleanly (publishRecord is
	// quiescent after Close).
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	db.subMu.Lock()
	defer db.subMu.Unlock()
	if db.sub != nil {
		return nil, ErrAlreadySubscribed
	}
	sub := &Subscription{
		db: db,
		ch: make(chan []byte, db.opts.ReplicationBufferSize),
	}
	db.sub = sub
	return sub, nil
}

// publishRecord is the writer-side hook. Called from processBurst after a
// successful append + keydir update, with the encoded record bytes that
// landed on the active segment. The slice is borrowed from db.encBuf and
// MUST NOT be retained — publishRecord copies before publishing.
//
// Behavior:
//
//   - If replication is not enabled (ReplicationBufferSize == 0), returns
//     immediately. This is the fast path single-node deployments take on
//     every write.
//
//   - If no subscriber is attached, returns without copying. Not counted
//     as a drop — there is no buffer to fill, and the subscriber that
//     eventually attaches will see the next record. Matches docs §3:
//     drops are a *slow consumer* signal, not a *no consumer* signal.
//
//   - If a subscriber is attached and the buffer has room, copies the
//     record and sends it non-blockingly.
//
//   - If the buffer is full, increments ReplicationLagDropped and returns
//     without sending. The writer never blocks.
func (db *DB) publishRecord(encoded []byte) {
	if db.opts.ReplicationBufferSize <= 0 {
		return
	}
	db.subMu.Lock()
	sub := db.sub
	db.subMu.Unlock()
	if sub == nil {
		return
	}
	// Copy: encBuf is reused on the next append.
	rec := make([]byte, len(encoded))
	copy(rec, encoded)
	select {
	case sub.ch <- rec:
	default:
		db.replicationDropped.Add(1)
	}
}

// closeSubscriptionOnShutdown is called by DB.Close after the writer has
// exited, so the publisher is guaranteed quiescent. Detaches any active
// subscription and closes the record channel; the consumer observes
// channel-closed as a clean end-of-stream signal.
func (db *DB) closeSubscriptionOnShutdown() {
	db.subMu.Lock()
	sub := db.sub
	db.sub = nil
	db.subMu.Unlock()
	if sub == nil {
		return
	}
	sub.closeMu.Lock()
	if !sub.closed {
		sub.closed = true
		sub.detached.Store(true)
		close(sub.ch)
	}
	sub.closeMu.Unlock()
}
