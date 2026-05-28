// Package server is the TCP front-end for the little-db engine. It owns
// the listener, per-connection goroutines, request dispatch, error
// classification, and the streaming range-response protocol. Wire
// framing/codec lives in internal/wire; this package is the policy layer
// (deadlines, overload limits, shutdown semantics) that sits between the
// wire and the engine.
//
// Concurrency model:
//
//   - One goroutine per connection. The connection goroutine serially
//     reads one request, fully responds (including all stream frames for
//     READKEYRANGE), then reads the next. There is no in-connection
//     pipelining: a client that sends two requests back-to-back will see
//     them processed in order, with response 2 starting only after
//     response 1's last byte is on the wire.
//
//   - Cross-connection concurrency is unbounded for point operations
//     (PUT/GET/DELETE/BATCH/PING/STATS) because the engine handles its
//     own contention. Range streams ARE bounded by a server-wide
//     semaphore (Options.MaxConcurrentRangeStreams); a request that
//     can't immediately acquire a slot is rejected with OVERLOAD rather
//     than enqueued — at-most-N concurrent in-flight scans is a leading
//     indicator we want to protect, not a back-pressure signal.
//
// Shutdown:
//
//   - Shutdown closes the listener (so Accept returns), then pokes every
//     tracked connection with a past read deadline so any goroutine
//     blocked in ReadFrame between requests unblocks and exits. A
//     connection currently processing a request finishes that request
//     (subject to its own WriteDeadline) and only then notices the
//     drain flag.
//
//   - If the supplied context expires before all goroutines exit,
//     Shutdown hard-closes every tracked connection, which kills any
//     in-flight write. Callers that don't care about deadlines can pass
//     context.Background().
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"little-db/internal/engine"
	"little-db/internal/logging"
	"little-db/internal/wire"
)

// Options configures a Server.
type Options struct {
	// Addr is the TCP listen address (e.g. ":4242", "127.0.0.1:0").
	Addr string

	// ReadDeadline bounds the time a single ReadFrame may block waiting
	// for client bytes. Applied per request (re-set before each read).
	// Zero means use the default (30s).
	ReadDeadline time.Duration

	// WriteDeadline bounds the time a single response (or one frame of a
	// streaming response) may block waiting for the client to drain.
	// Re-set before each write. Zero means use the default (30s).
	WriteDeadline time.Duration

	// MaxConcurrentRangeStreams caps the number of READKEYRANGE
	// responses in flight server-wide. Excess requests get OVERLOAD.
	// Zero means use the default (4).
	MaxConcurrentRangeStreams int

	// MaxRangeResponseBytes caps the total payload (sum of page-frame
	// bodies) one range response may emit. When exceeded mid-stream the
	// server replies with an OVERLOAD error frame (which doubles as the
	// stream terminator) and stops iterating. Zero means use the
	// default (64 MiB).
	MaxRangeResponseBytes int64

	// EnableReplication exposes the REPLICATE_SUBSCRIBE endpoint. When
	// false (default) the server rejects subscribe requests with
	// BAD_REQUEST regardless of the underlying engine's replication
	// configuration — operators must opt in at the server boundary
	// because exposing the change stream is a security-sensitive
	// capability (every write becomes externally readable). When true,
	// the engine must also be opened with Options.ReplicationBufferSize
	// > 0; if it is not, subscribe still fails with BAD_REQUEST (the
	// distinction surfaces in the error message) so the operator can
	// tell server-side vs engine-side misconfiguration apart.
	EnableReplication bool

	// Logger receives lifecycle events (listen, shutdown) at Info and,
	// when Debug is enabled, one line per request (op, sizes, status,
	// duration). Nil installs a no-op logger; the per-request hot path
	// gates on Logger.Enabled so disabled DEBUG has zero overhead.
	Logger *slog.Logger
}

const (
	defaultReadDeadline              = 30 * time.Second
	defaultWriteDeadline             = 30 * time.Second
	defaultMaxConcurrentRangeStreams = 4
	defaultMaxRangeResponseBytes     = int64(64 * 1024 * 1024)

	// rangePageFlushBytes is the soft target for how large a single
	// range page body grows before we flush. Small enough that a slow
	// reader sees progress, large enough to amortise the per-frame
	// header cost. Independent of MaxRangeResponseBytes (the cap on the
	// whole stream).
	rangePageFlushBytes = 256 * 1024
)

// Server is the TCP front-end. Use New + Bind + Serve + Shutdown.
type Server struct {
	opts Options
	db   *engine.DB
	log  *slog.Logger
	// debugReq is precomputed once so the per-request hot path is a
	// single bool check when DEBUG is disabled.
	debugReq bool

	ln net.Listener

	rangeSem chan struct{} // capacity = MaxConcurrentRangeStreams

	shutdownCh chan struct{}
	draining   atomic.Bool
	wg         sync.WaitGroup

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

// New constructs a Server bound to db. Options defaults are applied.
func New(db *engine.DB, opts Options) *Server {
	if opts.ReadDeadline <= 0 {
		opts.ReadDeadline = defaultReadDeadline
	}
	if opts.WriteDeadline <= 0 {
		opts.WriteDeadline = defaultWriteDeadline
	}
	if opts.MaxConcurrentRangeStreams <= 0 {
		opts.MaxConcurrentRangeStreams = defaultMaxConcurrentRangeStreams
	}
	if opts.MaxRangeResponseBytes <= 0 {
		opts.MaxRangeResponseBytes = defaultMaxRangeResponseBytes
	}
	if opts.Logger == nil {
		opts.Logger = logging.Nop()
	}
	return &Server{
		opts:       opts,
		db:         db,
		log:        opts.Logger,
		debugReq:   opts.Logger.Enabled(context.Background(), slog.LevelDebug),
		rangeSem:   make(chan struct{}, opts.MaxConcurrentRangeStreams),
		shutdownCh: make(chan struct{}),
		conns:      make(map[net.Conn]struct{}),
	}
}

// Bind opens the TCP listener. Must be called exactly once, before Serve.
// Tests that need to know the bound address (e.g. when Addr=":0") can
// call Addr after Bind returns.
func (s *Server) Bind() error {
	if s.ln != nil {
		return errors.New("server: already bound")
	}
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("server: listen %q: %w", s.opts.Addr, err)
	}
	s.ln = ln
	s.log.Info("server listening", slog.String("addr", ln.Addr().String()))
	return nil
}

// Addr returns the bound listener address, or nil if Bind has not been
// called.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve runs the accept loop until Shutdown is called (or Bind has not
// been called). It returns nil on graceful shutdown, otherwise the
// underlying Accept error.
func (s *Server) Serve() error {
	if s.ln == nil {
		return errors.New("server: Serve called before Bind")
	}
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.draining.Load() {
				return nil
			}
			return fmt.Errorf("server: accept: %w", err)
		}
		// Track + wg.Add MUST happen under connsMu, and we must recheck
		// draining inside the lock. Otherwise Shutdown can observe a
		// zero waitgroup between Accept returning and wg.Add running,
		// then return before the handler is tracked — leaving a
		// goroutine alive past Shutdown. Shutdown takes connsMu AFTER
		// setting draining, which fences out any racing accept-add.
		s.connsMu.Lock()
		if s.draining.Load() {
			s.connsMu.Unlock()
			_ = conn.Close()
			return nil
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.connsMu.Unlock()
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrackConn(c)
			defer c.Close()
			s.handleConn(c)
		}(conn)
	}
}

// Shutdown stops accepting new connections, signals existing connection
// goroutines to drain after their current request, and waits for them to
// exit. If ctx expires first, Shutdown hard-closes every tracked
// connection (killing any in-flight write) and waits for the goroutines
// to unwind before returning ctx.Err().
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.draining.CompareAndSwap(false, true) {
		return errors.New("server: Shutdown already called")
	}
	s.log.Info("server shutdown begin")
	close(s.shutdownCh)
	if s.ln != nil {
		_ = s.ln.Close()
	}
	// Poke every idle ReadFrame so it unblocks with a deadline error.
	// A conn currently processing a request is unaffected by SetReadDeadline.
	s.connsMu.Lock()
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Unix(1, 0))
	}
	s.connsMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.log.Info("server shutdown done")
		return nil
	case <-ctx.Done():
		// Hard close.
		s.connsMu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.connsMu.Unlock()
		<-done
		s.log.Warn("server shutdown forced", slog.String("err", ctx.Err().Error()))
		return ctx.Err()
	}
}

func (s *Server) untrackConn(c net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

// errStreamComplete is returned by streaming handlers (currently only
// REPLICATE_SUBSCRIBE) after a successful ack to signal "the
// connection has done its job; close it without writing anything else."
//
// handleConn already returns on any non-nil dispatch error, which closes
// the conn via the deferred conn.Close in Serve. Using a distinct
// sentinel lets us:
//
//   - skip logging this as a transport error (dispatch debug log gates
//     on it);
//   - keep the contract that followers see io.EOF on their next
//     ReadReplicateRecord, not an in-band CLOSED frame. Returning nil
//     from the handler would loop handleConn back to the top, where the
//     draining check would write a CLOSED frame — which a follower
//     reading via ReadReplicateRecord interprets as a protocol error
//     ("expected REPLICATE_RECORD, got 0x04"), not clean end-of-stream.
var errStreamComplete = errors.New("server: streaming handler complete; close quietly")

// handleConn is the per-connection loop.
func (s *Server) handleConn(conn net.Conn) {
	for {
		if s.draining.Load() {
			// Best-effort polite close. Errors here are uninteresting;
			// the deferred conn.Close() in Serve will run regardless.
			_ = s.writeError(conn, wire.StatusClosed, "server shutting down")
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(s.opts.ReadDeadline)); err != nil {
			return
		}
		req, err := wire.ReadRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean client disconnect between requests.
				return
			}
			var pe *wire.ProtocolError
			if errors.As(err, &pe) {
				// Well-framed but semantically bad — reply BAD_REQUEST
				// and stay connected. This is the desync-resistance
				// guarantee the wire-classification split exists for.
				_ = conn.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline))
				if werr := wire.WriteError(conn, wire.StatusBadRequest, pe.Reason); werr != nil {
					return
				}
				continue
			}
			// FrameError, network error, deadline, or other read error.
			// Stream is desynchronized; close.
			return
		}
		if err := s.dispatch(conn, req); err != nil {
			// Any error from dispatch means we failed to write a
			// response (transport-level) — close the connection.
			return
		}
	}
}

// reqMeta returns observability attributes for a decoded request. Used
// only when DEBUG is enabled (gated by s.debugReq).
func reqMeta(req wire.Request) (op string, keyLen, valLen, entries int) {
	switch r := req.(type) {
	case *wire.PutRequest:
		return "put", len(r.Key), len(r.Value), 0
	case *wire.GetRequest:
		return "get", len(r.Key), 0, 0
	case *wire.DeleteRequest:
		return "delete", len(r.Key), 0, 0
	case *wire.BatchRequest:
		return "batch", 0, 0, len(r.Entries)
	case *wire.PingRequest:
		return "ping", 0, 0, 0
	case *wire.StatsRequest:
		return "stats", 0, 0, 0
	case *wire.ReadKeyRangeRequest:
		return "range", len(r.Start) + len(r.End), 0, 0
	default:
		return "unknown", 0, 0, 0
	}
}

// writeFrame sets WriteDeadline THEN writes — never before engine work,
// so a slow engine call can't burn the deadline before the response
// hits the wire.
func (s *Server) writeFrame(conn net.Conn, tag uint8, body []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline)); err != nil {
		return err
	}
	return wire.WriteFrame(conn, tag, body)
}

// writeError is the same as writeFrame but for error frames.
func (s *Server) writeError(conn net.Conn, status wire.Status, msg string) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline)); err != nil {
		return err
	}
	return wire.WriteError(conn, status, msg)
}

// dispatch handles one fully-decoded request. It returns nil on
// successful response write (including non-OK responses written
// successfully), and a non-nil error only if the write itself failed —
// in which case the connection is unrecoverable. When DEBUG logging is
// enabled it emits one line per request with op, sizes, status, and
// duration; the check is a single bool when DEBUG is off.
//
// WriteDeadline is set immediately before each write (after engine work
// completes), so a slow Put/Get/BatchPut can't eat the whole deadline
// budget before the response is even attempted.
func (s *Server) dispatch(conn net.Conn, req wire.Request) error {
	var start time.Time
	if s.debugReq {
		start = time.Now()
	}
	status, err := s.dispatchOnce(conn, req)
	if s.debugReq {
		op, kLen, vLen, entries := reqMeta(req)
		attrs := []slog.Attr{
			slog.String("op", op),
			slog.String("remote", conn.RemoteAddr().String()),
			slog.Int("key_len", kLen),
			slog.Int("val_len", vLen),
			slog.Int("status", int(status)),
			slog.Int64("duration_us", time.Since(start).Microseconds()),
		}
		if entries > 0 {
			attrs = append(attrs, slog.Int("entries", entries))
		}
		if err != nil && !errors.Is(err, errStreamComplete) {
			attrs = append(attrs, slog.String("transport_err", err.Error()))
		}
		s.log.LogAttrs(context.Background(), slog.LevelDebug, "request", attrs...)
	}
	return err
}

// dispatchOnce processes a single request and returns the wire status
// that was written along with any transport error. The status reflects
// what the client will observe; transport errors mean the response did
// not reach the wire.
func (s *Server) dispatchOnce(conn net.Conn, req wire.Request) (wire.Status, error) {
	switch r := req.(type) {
	case *wire.PutRequest:
		if err := s.db.Put(r.Key, r.Value); err != nil {
			return s.writeEngineErr(conn, err)
		}
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), nil)

	case *wire.GetRequest:
		val, err := s.db.Get(r.Key)
		if err != nil {
			if errors.Is(err, engine.ErrKeyNotFound) {
				return wire.StatusNotFound, s.writeError(conn, wire.StatusNotFound, "")
			}
			return s.writeEngineErr(conn, err)
		}
		body, err := wire.EncodeGetOK(val)
		if err != nil {
			return wire.StatusInternal, s.writeError(conn, wire.StatusInternal, err.Error())
		}
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), body)

	case *wire.DeleteRequest:
		// Delete is idempotent in the engine; missing keys are OK.
		if err := s.db.Delete(r.Key); err != nil {
			return s.writeEngineErr(conn, err)
		}
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), nil)

	case *wire.BatchRequest:
		entries := make([]engine.BatchEntry, len(r.Entries))
		for i, e := range r.Entries {
			entries[i] = engine.BatchEntry{Key: e.Key, Value: e.Value, Delete: e.Delete}
		}
		if err := s.db.BatchPut(entries); err != nil {
			return s.writeEngineErr(conn, err)
		}
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), nil)

	case *wire.PingRequest:
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), nil)

	case *wire.StatsRequest:
		st := s.db.Stats()
		return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), wire.EncodeStatsOK(st.KeyCount, st.BytesOnDisk))

	case *wire.ReadKeyRangeRequest:
		return s.handleRange(conn, r)

	case *wire.ReplicateSubscribeRequest:
		return s.handleReplicateSubscribe(conn, r)
	}
	// Unknown concrete type; defensive.
	return wire.StatusInternal, s.writeError(conn, wire.StatusInternal, "unknown request type")
}

// handleRange streams a READKEYRANGE response. Concurrency is bounded by
// rangeSem; bytes-on-the-wire are bounded by Options.MaxRangeResponseBytes.
// Either limit triggers an OVERLOAD error frame (which terminates the
// stream); the connection stays open.
func (s *Server) handleRange(conn net.Conn, req *wire.ReadKeyRangeRequest) (wire.Status, error) {
	// Non-blocking acquire — at-most-N is a protection, not a queue.
	select {
	case s.rangeSem <- struct{}{}:
		defer func() { <-s.rangeSem }()
	default:
		return wire.StatusOverload, s.writeError(conn, wire.StatusOverload, "concurrent range stream limit reached")
	}

	var (
		page          []wire.KV
		pageBytes     int
		streamedBytes int64
		writeErr      error
		overloaded    bool
	)

	flush := func() bool {
		if len(page) == 0 {
			return true
		}
		body, err := wire.EncodeRangePage(page)
		if err != nil {
			writeErr = err
			return false
		}
		if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline)); err != nil {
			writeErr = err
			return false
		}
		if err := wire.WriteFrame(conn, uint8(wire.StatusOK), body); err != nil {
			writeErr = err
			return false
		}
		streamedBytes += int64(len(body))
		page = page[:0]
		pageBytes = 0
		return true
	}

	scanErr := s.db.ReadKeyRange(req.Start, req.End, func(k, v []byte) bool {
		// Per-pair on-the-wire cost: u32 klen + key + u32 vlen + val.
		entryBytes := 4 + len(k) + 4 + len(v)
		// Check the stream-wide byte cap BEFORE buffering this pair, so
		// we never write a page that pushes us past the cap.
		projected := streamedBytes + int64(pageBytes) + 4 /*pair_count*/ + int64(entryBytes)
		if projected > s.opts.MaxRangeResponseBytes {
			overloaded = true
			return false
		}
		// Engine docs warn callback slices may not outlive fn; copy
		// before buffering across calls. (In practice the engine
		// materialises into fresh allocations, but be safe.)
		keyCopy := append([]byte(nil), k...)
		valCopy := append([]byte(nil), v...)
		page = append(page, wire.KV{Key: keyCopy, Value: valCopy})
		pageBytes += entryBytes
		if pageBytes >= rangePageFlushBytes {
			if !flush() {
				return false
			}
		}
		return true
	})

	// Order of error handling matters: a transport write error has
	// already corrupted the framing on this conn, so we surface that
	// FIRST (returning non-nil so handleConn closes). Engine and
	// overload conditions can both be reported in-band on a healthy
	// conn.
	if writeErr != nil {
		return wire.StatusInternal, writeErr
	}
	if scanErr != nil && !overloaded {
		return s.writeEngineErr(conn, scanErr)
	}
	// Flush whatever is left, unless we already gave up due to overload.
	if !overloaded {
		if !flush() {
			return wire.StatusInternal, writeErr
		}
	}
	if overloaded {
		// Flush whatever we already committed to (those bytes were
		// counted toward the projection that decided we're overloaded,
		// so the cap still holds), then emit the OVERLOAD terminator.
		// The client's stream reader processes any pages before
		// surfacing the non-OK frame as a RemoteError.
		if !flush() {
			return wire.StatusInternal, writeErr
		}
		return wire.StatusOverload, s.writeError(conn, wire.StatusOverload, "range response exceeds byte cap")
	}
	// End-of-stream sentinel.
	return wire.StatusOK, s.writeFrame(conn, uint8(wire.StatusOK), wire.EncodeRangeEnd())
}

// writeEngineErr maps an engine error to the corresponding wire status
// and writes the error frame. It returns the wire status and nil if the
// frame was written successfully (regardless of which status); a
// non-nil error indicates the network write itself failed and the conn
// is unrecoverable.
func (s *Server) writeEngineErr(conn net.Conn, err error) (wire.Status, error) {
	status, msg := classifyEngineErr(err)
	return status, s.writeError(conn, status, msg)
}

func classifyEngineErr(err error) (wire.Status, string) {
	switch {
	case errors.Is(err, engine.ErrKeyNotFound):
		return wire.StatusNotFound, ""
	case errors.Is(err, engine.ErrDBClosed):
		return wire.StatusClosed, err.Error()
	case errors.Is(err, engine.ErrWritesDisabled):
		// Engine has stopped accepting writes after an uncertain
		// manifest publish (see engine.rotateActive). Operationally
		// this is the same shape as a closed engine from the client's
		// perspective — the DB is up enough to answer, but won't
		// accept writes — so reuse StatusClosed rather than reporting
		// a generic INTERNAL. The full error string carries the
		// distinguishing detail for human operators.
		return wire.StatusClosed, err.Error()
	case errors.Is(err, engine.ErrEmptyKey),
		errors.Is(err, engine.ErrKeyTooLarge),
		errors.Is(err, engine.ErrValueTooLarge),
		errors.Is(err, engine.ErrBatchTooLarge),
		errors.Is(err, engine.ErrInvalidBatchEntry):
		// These shouldn't normally fire — wire validation rejects the
		// same conditions earlier. Defensive mapping for the case
		// where wire limits are looser than engine limits.
		return wire.StatusBadRequest, err.Error()
	default:
		return wire.StatusInternal, err.Error()
	}
}

// handleReplicateSubscribe streams REPLICATE_RECORD frames to a follower
// until the connection closes, the engine shuts down, or the server
// drains. Unlike every other endpoint, this one holds the connection
// for the lifetime of the subscription and, on completion, signals
// handleConn to close the conn quietly (via errStreamComplete) rather
// than loop back to ReadFrame. The reason: followers read this stream
// with wire.ReadReplicateRecord, which interprets a CLOSED frame as a
// protocol error, not as end-of-stream. The documented end-of-stream
// signal is plain io.EOF on the TCP connection.
//
// Error mapping:
//
//   - Server not opted in              → BAD_REQUEST ("server")
//   - Non-empty resume tag             → BAD_REQUEST ("resume tag not supported")
//   - Engine replication disabled      → BAD_REQUEST ("database")
//   - Slot already taken               → OVERLOAD (single-subscriber design)
//   - Engine closed                    → CLOSED
//
// Wire protocol on success:
//
//  1. Server writes one StatusOK frame (empty body) to ack the subscribe.
//     Followers should treat this as "you are now the live subscriber".
//  2. Server then writes zero or more REPLICATE_RECORD frames, one per
//     leader write, in the order the leader's writer committed them.
//  3. The stream ends with a clean connection close (DB shutdown,
//     server drain, or transport error). There is no in-band "end of
//     stream" frame — followers detect end via ReadFrame returning
//     io.EOF.
//
// The per-frame WriteDeadline policy is the same as every other write:
// reset immediately before the write. There is no overall stream
// deadline; followers stay subscribed indefinitely.
func (s *Server) handleReplicateSubscribe(conn net.Conn, req *wire.ReplicateSubscribeRequest) (wire.Status, error) {
	if !s.opts.EnableReplication {
		return wire.StatusBadRequest, s.writeError(conn, wire.StatusBadRequest, "replication not enabled on this server")
	}
	if len(req.ResumeTag) > 0 {
		// v0.1.0 always streams from "now"; resume tags are reserved
		// for a future snapshot-bootstrap path. Reject loudly rather
		// than silently ignore: a follower that thinks it asked to
		// resume from offset N but actually got "from now" would
		// silently lose records.
		return wire.StatusBadRequest, s.writeError(conn, wire.StatusBadRequest, "resume tag not supported in v0.1.0; send empty tag")
	}
	sub, err := s.db.Subscribe()
	if err != nil {
		switch {
		case errors.Is(err, engine.ErrReplicationDisabled):
			// Server flag is on but engine wasn't opened with a
			// replication buffer — distinct from the server-flag-off
			// case so operators can tell the two misconfigurations
			// apart from the error message alone.
			return wire.StatusBadRequest, s.writeError(conn, wire.StatusBadRequest, "replication not enabled on this database")
		case errors.Is(err, engine.ErrAlreadySubscribed):
			// Single-subscriber-per-DB is a deliberate v0.1.0 design
			// choice (docs/replication.md §3.3). OVERLOAD matches the
			// "capacity exhausted, retry later" shape we use for range
			// streams; followers should back off and retry.
			return wire.StatusOverload, s.writeError(conn, wire.StatusOverload, "replication slot busy; one follower at a time")
		case errors.Is(err, engine.ErrDBClosed):
			return wire.StatusClosed, s.writeError(conn, wire.StatusClosed, err.Error())
		default:
			return s.writeEngineErr(conn, err)
		}
	}
	// sub.Close detaches and closes the channel. Safe to call even if
	// the DB shutdown path already closed it (Close is idempotent).
	defer sub.Close()

	// Ack the subscribe so the follower can transition from
	// "dialing/handshake" to "applying records".
	if err := s.writeFrame(conn, uint8(wire.StatusOK), nil); err != nil {
		return wire.StatusOK, err
	}

	// Read-watcher: detect follower disconnect even when the leader is
	// idle. The protocol says followers send zero frames after
	// subscribe, so any Read returning (clean EOF or transport error)
	// means the conn is gone. Without this an idle leader would hold
	// the single subscription slot forever, bouncing every reconnect
	// attempt with OVERLOAD until some unrelated write happened to
	// surface the failed write through this handler.
	//
	// Concurrency: net.Conn permits one concurrent reader + one
	// concurrent writer, which is exactly the shape we have (watcher
	// reads, this goroutine writes).
	peerGone := make(chan struct{})
	var watcherWG sync.WaitGroup
	watcherWG.Add(1)
	go func() {
		defer watcherWG.Done()
		var buf [1]byte
		_, _ = conn.Read(buf[:])
		close(peerGone)
	}()
	defer func() {
		// Wake the watcher so it exits before we return. Setting a
		// past read deadline unblocks Read with a deadline error
		// without closing the conn (handleConn's deferred close does
		// that). The error is ignored: if conn is already torn down
		// SetReadDeadline returns ErrClosed, which is fine.
		_ = conn.SetReadDeadline(time.Unix(1, 0))
		watcherWG.Wait()
	}()

	// Stream loop. The connection is now dedicated to this stream until
	// one of four things happens:
	//
	//   1. sub.Records() closes — DB.Close fired closeSubscriptionOnShutdown.
	//   2. s.shutdownCh closes — server.Shutdown asked us to drain.
	//   3. peerGone closes — the read-watcher saw EOF or any other
	//      Read return; the follower is gone.
	//   4. A write fails — write deadline elapsed or transport error
	//      (e.g. the kernel surfaced the closed peer on the next send).
	//
	// In every case we return errStreamComplete so handleConn closes
	// the conn quietly (no in-band CLOSED frame); the follower sees
	// io.EOF on its next ReadReplicateRecord, which is the documented
	// end-of-stream signal.
	for {
		select {
		case <-s.shutdownCh:
			return wire.StatusOK, errStreamComplete
		case <-peerGone:
			return wire.StatusOK, errStreamComplete
		case rec, ok := <-sub.Records():
			if !ok {
				return wire.StatusOK, errStreamComplete
			}
			if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline)); err != nil {
				return wire.StatusOK, errStreamComplete
			}
			if err := wire.WriteReplicateRecord(conn, rec); err != nil {
				return wire.StatusOK, errStreamComplete
			}
		}
	}
}
