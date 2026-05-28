package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"

	"little-db/internal/engine"
	"little-db/internal/logging"
	"little-db/internal/wire"
)

// Follower is the leader-dial loop that drives the engine's
// follower-side apply path. It owns one TCP connection to a leader at
// a time, sends one REPLICATE_SUBSCRIBE on connect, then loops
// ReadReplicateRecord → db.ApplyReplicatedRecord until the connection
// breaks or the supplied context cancels.
//
// Lifecycle:
//
//   - One Follower per follower process. Construct with NewFollower,
//     run with Run(ctx). Run blocks until ctx is Done; the caller
//     spawns it in its own goroutine.
//
//   - The Follower is NOT a Server. The HTTP-ish "this process serves
//     reads on port X while applying records from port Y" topology is
//     wired up by the CLI: it constructs both a Server (in
//     FollowerMode, rejecting writes) AND a Follower (dialing the
//     leader), and runs them as siblings.
//
// Reconnect policy:
//
//   - On any error (dial, subscribe ack, frame read, apply), the conn
//     is closed and the loop sleeps for a backoff interval before
//     redialing. Backoff is exponential with full jitter, capped at
//     MaxBackoff. The first retry starts at InitialBackoff; a
//     successful subscribe + first record received resets the backoff
//     to InitialBackoff again, so a flap during steady state recovers
//     quickly while a hard-down leader does not hot-loop.
//
//   - Backoff includes the dial timeout in the budget: a 5s dial
//     timeout followed by a 5s sleep is 10s of leader-down time, not
//     5s+5s+5s+ ... This is so an outage's recovery latency is
//     predictable in terms of MaxBackoff, not "MaxBackoff plus N dial
//     timeouts".
//
// Application policy:
//
//   - ApplyReplicatedRecord errors are classified into "drop the
//     stream and reconnect" (default — the leader could be sending
//     us corrupt data and we'd rather restart from "now" than land
//     poison records) vs "ErrDBClosed" (terminal — return from Run).
//     A corrupt record on the wire is rare enough that the
//     reconnect-and-resume-from-now disposition is acceptable; a
//     resume-from-cursor protocol is out of scope for v0.1.0.
//
// Observability:
//
//   - Connect / disconnect / apply errors are logged at Info; per-
//     record success is silent (high-volume hot path). The Stats
//     endpoint reports running keydir size which is what an operator
//     watches to confirm the follower is keeping up.

// FollowerOptions configures a Follower. Zero-value fields take
// documented defaults so the common case is NewFollower(leader, db, 0,
// nil) without filling in tuning knobs.
type FollowerOptions struct {
	// DialTimeout bounds one connect attempt to the leader. Zero
	// installs defaultFollowerDialTimeout.
	DialTimeout time.Duration

	// InitialBackoff is the first sleep between a failed attempt and
	// the next dial. Doubles per consecutive failure up to
	// MaxBackoff. Zero installs defaultFollowerInitialBackoff.
	InitialBackoff time.Duration

	// MaxBackoff caps the sleep. Zero installs defaultFollowerMaxBackoff.
	MaxBackoff time.Duration

	// SubscribeAckTimeout bounds the read of the OK ack frame that
	// the leader sends in response to REPLICATE_SUBSCRIBE. Zero
	// installs defaultFollowerSubscribeAckTimeout. Distinct from
	// DialTimeout because TCP connect + TLS-style handshake completed
	// successfully does not mean the leader is healthy enough to
	// reply; the ack timeout catches a leader that accepted the
	// connection but is hung.
	SubscribeAckTimeout time.Duration

	// Logger is where lifecycle events go. Nil installs the package
	// no-op logger.
	Logger *slog.Logger
}

const (
	defaultFollowerDialTimeout         = 5 * time.Second
	defaultFollowerInitialBackoff      = 100 * time.Millisecond
	defaultFollowerMaxBackoff          = 30 * time.Second
	defaultFollowerSubscribeAckTimeout = 5 * time.Second
)

// Follower runs the dial → subscribe → apply loop.
type Follower struct {
	leaderAddr string
	db         *engine.DB
	opts       FollowerOptions
	log        *slog.Logger
}

// NewFollower constructs a Follower. It does not dial; call Run.
func NewFollower(leaderAddr string, db *engine.DB, opts FollowerOptions) *Follower {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaultFollowerDialTimeout
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaultFollowerInitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultFollowerMaxBackoff
	}
	if opts.SubscribeAckTimeout <= 0 {
		opts.SubscribeAckTimeout = defaultFollowerSubscribeAckTimeout
	}
	if opts.Logger == nil {
		opts.Logger = logging.Nop()
	}
	return &Follower{
		leaderAddr: leaderAddr,
		db:         db,
		opts:       opts,
		log:        opts.Logger,
	}
}

// Run is the follower main loop. It returns nil on ctx cancellation or
// engine.ErrDBClosed when the local DB has been closed; any other
// return value is a bug.
func (f *Follower) Run(ctx context.Context) error {
	backoff := f.opts.InitialBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		// runSession dials, subscribes, applies until something
		// breaks. progressed=true means we got at least one record
		// (or a clean idle disconnect after a successful subscribe);
		// it is the signal that resets the backoff.
		progressed, err := f.runSession(ctx)
		if errors.Is(err, engine.ErrDBClosed) {
			f.log.Info("follower: local DB closed, exiting", slog.String("leader", f.leaderAddr))
			return err
		}
		if err != nil {
			f.log.Info("follower: session ended",
				slog.String("leader", f.leaderAddr),
				slog.String("err", err.Error()),
				slog.Bool("progressed", progressed),
			)
		}
		if progressed {
			backoff = f.opts.InitialBackoff
		}
		if ctx.Err() != nil {
			return nil
		}
		// Full jitter [0, backoff) per AWS architecture blog: spreads
		// retries from a flock of followers that lost a shared
		// leader at the same wall-clock instant.
		sleep := time.Duration(rand.Int64N(int64(backoff) + 1))
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > f.opts.MaxBackoff {
			backoff = f.opts.MaxBackoff
		}
	}
}

// runSession runs one connect → apply session and returns when it
// breaks. The first return value is true iff this session got at least
// past a successful subscribe ack (used by Run to reset backoff).
func (f *Follower) runSession(ctx context.Context) (bool, error) {
	d := net.Dialer{Timeout: f.opts.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", f.leaderAddr)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Send REPLICATE_SUBSCRIBE with an empty resume tag. v0.1.0
	// followers always start "from now"; a future snapshot-bootstrap
	// path will populate the tag.
	if err := conn.SetWriteDeadline(time.Now().Add(f.opts.SubscribeAckTimeout)); err != nil {
		return false, fmt.Errorf("set subscribe write deadline: %w", err)
	}
	if err := wire.WriteReplicateSubscribe(conn, nil); err != nil {
		return false, fmt.Errorf("send subscribe: %w", err)
	}
	// Clear the write deadline before the long-lived read loop.
	_ = conn.SetWriteDeadline(time.Time{})

	// Read the ack frame. A leader that rejects (BAD_REQUEST for
	// "not enabled", OVERLOAD for "slot busy", CLOSED for "engine
	// down") sends a non-OK frame whose tag is the status; we
	// surface that as the session error so Run can back off.
	if err := conn.SetReadDeadline(time.Now().Add(f.opts.SubscribeAckTimeout)); err != nil {
		return false, fmt.Errorf("set ack read deadline: %w", err)
	}
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		return false, fmt.Errorf("read subscribe ack: %w", err)
	}
	if wire.Status(tag) != wire.StatusOK {
		msg, _ := wire.DecodeError(body)
		return false, fmt.Errorf("leader rejected subscribe: status=%s msg=%q", wire.Status(tag), msg)
	}

	// Subscribe accepted. Clear the read deadline so the long-lived
	// idle stream is not subject to a per-read timeout; the engine
	// can be quiet indefinitely.
	_ = conn.SetReadDeadline(time.Time{})
	f.log.Info("follower: subscribed", slog.String("leader", f.leaderAddr))

	// ctx cancellation needs to unblock the blocking ReadReplicateRecord
	// below. We can't pass ctx through the wire codec, so we spawn a
	// goroutine that closes the conn on ctx.Done. Read will then
	// return ErrClosed which we treat the same as any other transport
	// error.
	stopCancel := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancel:
		}
	}()
	defer close(stopCancel)

	for {
		raw, err := wire.ReadReplicateRecord(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return true, nil
			}
			return true, fmt.Errorf("read record: %w", err)
		}
		if err := f.db.ApplyReplicatedRecord(raw); err != nil {
			if errors.Is(err, engine.ErrDBClosed) {
				return true, err
			}
			// CRC or malformed: drop the stream. The reconnect will
			// resubscribe from "now" and skip past whatever broke.
			return true, fmt.Errorf("apply: %w", err)
		}
	}
}
