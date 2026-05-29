package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/engine"
	"little-db/internal/wire"
)

// --- helpers -------------------------------------------------------------

// startServer brings up an engine + server bound to a random port and
// returns the server, its address, and a teardown closure to register
// with t.Cleanup. The caller may also pass overrides for Options.
func startServer(t *testing.T, override func(o *Options)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{Dir: dir, MaxBatchEncodedSize: 64 * 1024 * 1024})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	opts := Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
	}
	if override != nil {
		override(&opts)
	}
	srv := New(db, opts)
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})
	return srv, addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// roundTrip writes one request and reads one response frame.
func roundTrip(t *testing.T, conn net.Conn, req wire.Request) (wire.Status, []byte) {
	t.Helper()
	st, body, err := tryRoundTrip(conn, req)
	if err != nil {
		t.Fatalf("roundTrip: %v", err)
	}
	return st, body
}

// tryRoundTrip mirrors roundTrip but returns errors instead of calling
// t.Fatalf, so callers running in a goroutine can report I/O failures
// without crashing the goroutine before delivering a result.
func tryRoundTrip(conn net.Conn, req wire.Request) (wire.Status, []byte, error) {
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		return 0, nil, fmt.Errorf("EncodeRequest: %w", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		return 0, nil, fmt.Errorf("write: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		return 0, nil, fmt.Errorf("ReadFrame: %w", err)
	}
	return wire.Status(tag), body, nil
}

// --- 1. End-to-end -------------------------------------------------------

func TestServerEndToEnd(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// PUT
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("hello"), Value: []byte("world")})
	if st != wire.StatusOK {
		t.Fatalf("PUT: got status %v want OK", st)
	}

	// GET hit
	st, body := roundTrip(t, conn, &wire.GetRequest{Key: []byte("hello")})
	if st != wire.StatusOK {
		t.Fatalf("GET: status %v want OK", st)
	}
	val, err := wire.DecodeGetOK(body)
	if err != nil {
		t.Fatalf("DecodeGetOK: %v", err)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("GET value: got %q want %q", val, "world")
	}

	// GET miss
	st, _ = roundTrip(t, conn, &wire.GetRequest{Key: []byte("missing")})
	if st != wire.StatusNotFound {
		t.Fatalf("GET missing: got %v want NOT_FOUND", st)
	}

	// DELETE
	st, _ = roundTrip(t, conn, &wire.DeleteRequest{Key: []byte("hello")})
	if st != wire.StatusOK {
		t.Fatalf("DELETE: %v", st)
	}
	st, _ = roundTrip(t, conn, &wire.GetRequest{Key: []byte("hello")})
	if st != wire.StatusNotFound {
		t.Fatalf("GET after delete: %v", st)
	}

	// BATCH
	st, _ = roundTrip(t, conn, &wire.BatchRequest{Entries: []wire.BatchEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}})
	if st != wire.StatusOK {
		t.Fatalf("BATCH: %v", st)
	}

	// PING
	st, _ = roundTrip(t, conn, &wire.PingRequest{})
	if st != wire.StatusOK {
		t.Fatalf("PING: %v", st)
	}

	// STATS
	st, body = roundTrip(t, conn, &wire.StatsRequest{})
	if st != wire.StatusOK {
		t.Fatalf("STATS: %v", st)
	}
	kc, bd, err := wire.DecodeStatsOK(body)
	if err != nil {
		t.Fatalf("DecodeStatsOK: %v", err)
	}
	if kc != 2 { // a, b (hello was deleted)
		t.Fatalf("STATS key_count: got %d want 2", kc)
	}
	if bd == 0 {
		t.Fatalf("STATS bytes_on_disk: got 0, want >0")
	}

	// READKEYRANGE
	_ = sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	collected := readRange(t, conn)
	keys := keysOf(collected)
	if !equalStringSets(keys, []string{"a", "b"}) {
		t.Fatalf("range keys: got %v want [a b]", keys)
	}
}

// --- 2. Shutdown drains in-flight ---------------------------------------

func TestServerShutdownDrainsInflight(t *testing.T) {
	srv, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Issue a PUT.
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("PUT: %v", st)
	}

	// Concurrently start Shutdown.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()

	// The server should respond to our existing connection with a
	// CLOSED frame (polite shutdown notice) and then close. Read until
	// we either see CLOSED or get EOF.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil && !errors.Is(err, io.EOF) {
		// Tolerate EOF (server closed without polite notice).
		// Anything else is a bug.
		var ne net.Error
		if !errors.As(err, &ne) {
			t.Fatalf("unexpected read error: %v", err)
		}
	}
	if err == nil {
		if wire.Status(tag) != wire.StatusClosed {
			t.Fatalf("expected CLOSED frame, got status %v body %x", wire.Status(tag), body)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// --- 3. Bad-frame classification ----------------------------------------

func TestServerBadFrameClassification(t *testing.T) {
	_, addr := startServer(t, nil)

	cases := []struct {
		name         string
		send         func(conn net.Conn)
		wantStatus   wire.Status // 0 + closeOK=true means server may close instead
		closeAllowed bool        // for FrameError cases, server may close
	}{
		{
			name: "PUT empty key (ProtocolError → BAD_REQUEST)",
			send: func(c net.Conn) {
				body := make([]byte, 8)
				binary.BigEndian.PutUint32(body[0:4], 0) // klen=0
				binary.BigEndian.PutUint32(body[4:8], 0) // vlen=0
				sendRaw(t, c, uint8(wire.OpPut), body)
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "Unknown opcode (ProtocolError)",
			send: func(c net.Conn) {
				sendRaw(t, c, 0x77, nil)
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "Truncated GET (ProtocolError)",
			send: func(c net.Conn) {
				sendRaw(t, c, uint8(wire.OpGet), []byte{0x00, 0x00}) // only 2 of 4 klen bytes
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "FrameError: payload_len = 0 (server closes)",
			send: func(c net.Conn) {
				hdr := []byte{0, 0, 0, 0}
				_, _ = c.Write(hdr)
			},
			closeAllowed: true,
		},
		{
			name: "FrameError: payload_len > MaxFramePayload (server closes)",
			send: func(c net.Conn) {
				hdr := make([]byte, 4)
				binary.BigEndian.PutUint32(hdr, wire.MaxFramePayload+1)
				_, _ = c.Write(hdr)
			},
			closeAllowed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dial(t, addr)
			tc.send(conn)
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			tag, _, err := wire.ReadFrame(conn)
			if tc.closeAllowed {
				if err == nil {
					t.Fatalf("expected close/EOF, got status %v", wire.Status(tag))
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if wire.Status(tag) != tc.wantStatus {
				t.Fatalf("status: got %v want %v", wire.Status(tag), tc.wantStatus)
			}
		})
	}
}

// TestServerReplicateSubscribeDisabledByDefault verifies the security
// boundary: even when the underlying engine HAS a replication buffer
// (so the publisher is live and records are being copied into the
// channel), the server still rejects subscribe requests because
// Options.EnableReplication defaults to false. Exposing the change
// stream is a security-sensitive capability — every write becomes
// externally readable — so the server boundary must fail closed
// independently of engine configuration.
//
// The test deliberately does NOT use startServer (which opens the
// engine without replication); a flag-off+engine-off setup would only
// prove that the request short-circuits, not that the server boundary
// holds when the engine is fully wired.
func TestServerReplicateSubscribeDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{
		Dir:                   dir,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 64, // engine-side publisher live
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := New(db, Options{
		Addr:          "127.0.0.1:0",
		ReadDeadline:  2 * time.Second,
		WriteDeadline: 2 * time.Second,
		// EnableReplication intentionally left false — server flag off.
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})

	conn := dial(t, addr)
	st, body := roundTrip(t, conn, &wire.ReplicateSubscribeRequest{})
	if st != wire.StatusBadRequest {
		t.Fatalf("status: got %v want BAD_REQUEST", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg != "replication not enabled on this server" {
		t.Fatalf("msg: got %q", msg)
	}
}

// startReplicationServer is like startServer but opens the engine with
// replication enabled (ReplicationBufferSize > 0, MaxBatchEncodedSize
// lowered to satisfy the wire-frame cap) and turns on
// Options.EnableReplication. The bufSize is exposed so drop-on-overflow
// tests can request a tiny buffer.
func startReplicationServer(t *testing.T, bufSize int) (*Server, *engine.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{
		Dir:                   dir,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: bufSize,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := New(db, Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		EnableReplication:         true,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})
	return srv, db, addr
}

// TestServerReplicateSubscribeStreamsRecords is the happy-path e2e: a
// follower subscribes, the leader writes, and the records arrive in
// order, framed as REPLICATE_RECORD, decoding back to the same KVs the
// engine published.
func TestServerReplicateSubscribeStreamsRecords(t *testing.T) {
	_, db, addr := startReplicationServer(t, 64)
	conn := dial(t, addr)

	// SUBSCRIBE.
	frame, err := wire.EncodeRequest(&wire.ReplicateSubscribeRequest{})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	// Read the ack.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame ack: %v", err)
	}
	if wire.Status(tag) != wire.StatusOK {
		t.Fatalf("ack: got %v want OK (body=%q)", wire.Status(tag), body)
	}
	if len(body) != 0 {
		t.Fatalf("ack body: got %d bytes, want 0", len(body))
	}

	// Drive the leader. Use a small enough N that the buffer (64) is
	// not the bottleneck; this test is about ordering and framing, not
	// drop-on-overflow.
	const N = 10
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("k-%02d", i))
		v := []byte(fmt.Sprintf("v-%02d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Read N REPLICATE_RECORD frames and assert order.
	for i := 0; i < N; i++ {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		raw, err := wire.ReadReplicateRecord(conn)
		if err != nil {
			t.Fatalf("ReadReplicateRecord[%d]: %v", i, err)
		}
		wantKey := fmt.Sprintf("k-%02d", i)
		wantVal := fmt.Sprintf("v-%02d", i)
		if !bytes.Contains(raw, []byte(wantKey)) || !bytes.Contains(raw, []byte(wantVal)) {
			t.Fatalf("record[%d]: missing key/value (want %q/%q, raw=%x)", i, wantKey, wantVal, raw)
		}
	}
}

// TestServerReplicateSubscribeEngineNotEnabled covers the half-config
// case: server flag is on, engine has no replication buffer. The error
// message must distinguish "server" vs "database" so operators can tell
// the two misconfigurations apart.
func TestServerReplicateSubscribeEngineNotEnabled(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{Dir: dir, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := New(db, Options{
		Addr:              "127.0.0.1:0",
		ReadDeadline:      2 * time.Second,
		WriteDeadline:     2 * time.Second,
		EnableReplication: true,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})

	conn := dial(t, addr)
	st, body := roundTrip(t, conn, &wire.ReplicateSubscribeRequest{})
	if st != wire.StatusBadRequest {
		t.Fatalf("status: got %v want BAD_REQUEST", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg != "replication not enabled on this database" {
		t.Fatalf("msg: got %q", msg)
	}
}

// TestServerReplicateSubscribeResumeTagRejected pins the v0.1.0
// contract: non-empty resume tags are rejected loudly. If we silently
// dropped the tag and streamed "from now", a follower expecting to
// resume from offset N would lose every record up to the subscribe
// point.
func TestServerReplicateSubscribeResumeTagRejected(t *testing.T) {
	_, _, addr := startReplicationServer(t, 64)
	conn := dial(t, addr)

	st, body := roundTrip(t, conn, &wire.ReplicateSubscribeRequest{ResumeTag: []byte("not-empty")})
	if st != wire.StatusBadRequest {
		t.Fatalf("status: got %v want BAD_REQUEST", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg != "resume tag not supported in v0.1.0; send empty tag" {
		t.Fatalf("msg: got %q", msg)
	}
}

// TestServerReplicateSubscribeSlotBusy covers the single-subscriber
// invariant: a second concurrent subscriber gets OVERLOAD. The first
// subscriber must already be parked in the stream loop when the second
// dials, which we enforce by reading the first ack before issuing the
// second subscribe.
func TestServerReplicateSubscribeSlotBusy(t *testing.T) {
	_, _, addr := startReplicationServer(t, 64)

	// First subscriber.
	c1 := dial(t, addr)
	frame, _ := wire.EncodeRequest(&wire.ReplicateSubscribeRequest{})
	if _, err := c1.Write(frame); err != nil {
		t.Fatalf("c1 write: %v", err)
	}
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("c1 ack: %v", err)
	}
	if wire.Status(tag) != wire.StatusOK {
		t.Fatalf("c1 ack: got %v want OK", wire.Status(tag))
	}

	// Second subscriber should bounce.
	c2 := dial(t, addr)
	st, body := roundTrip(t, c2, &wire.ReplicateSubscribeRequest{})
	if st != wire.StatusOverload {
		t.Fatalf("c2 status: got %v want OVERLOAD", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg != "replication slot busy; one follower at a time" {
		t.Fatalf("c2 msg: got %q", msg)
	}

	// After the first follower disconnects, the slot frees up and a
	// fresh subscriber can take over. This is the failover-recovery
	// shape the design depends on (docs/replication.md §6: promote
	// then re-subscribe). The server's read-watcher on c1 sees EOF
	// once we close c1, the handler exits, and sub.Close() detaches.
	// No leader write is required — see
	// TestServerReplicateSubscribeIdleLeaderReleasesSlot for the
	// explicit regression on the watcher.
	_ = c1.Close()
	deadline := time.Now().Add(2 * time.Second)
	var lastSt wire.Status
	c3 := dial(t, addr)
	for time.Now().Before(deadline) {
		st, _ := roundTrip(t, c3, &wire.ReplicateSubscribeRequest{})
		lastSt = st
		if st == wire.StatusOK {
			return
		}
		_ = c3.Close()
		c3 = dial(t, addr)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("c3 status after c1 disconnect: got %v want OK", lastSt)
}

// TestServerReplicateSubscribeIdleLeaderReleasesSlot pins the
// read-watcher behaviour: an idle leader (no writes ever published)
// must still release the single subscription slot when the follower
// disconnects. Before the watcher landed, the handler only noticed a
// dead follower on the next failed write — so a follower that crashed
// against an idle leader would hold the slot until the next unrelated
// write, bouncing every reconnect attempt with OVERLOAD in the
// meantime.
func TestServerReplicateSubscribeIdleLeaderReleasesSlot(t *testing.T) {
	_, _, addr := startReplicationServer(t, 64)

	c1 := dial(t, addr)
	frame, _ := wire.EncodeRequest(&wire.ReplicateSubscribeRequest{})
	if _, err := c1.Write(frame); err != nil {
		t.Fatalf("c1 write: %v", err)
	}
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("c1 ack: %v", err)
	}
	if wire.Status(tag) != wire.StatusOK {
		t.Fatalf("c1 ack: got %v want OK", wire.Status(tag))
	}

	// Hard close c1 without writing anything to the leader. The leader
	// is fully idle — no records will ever be published on this DB.
	_ = c1.Close()

	// A reconnect should succeed within a short window (one watcher
	// goroutine scheduling tick + ack round-trip).
	deadline := time.Now().Add(2 * time.Second)
	var lastSt wire.Status
	c2 := dial(t, addr)
	for time.Now().Before(deadline) {
		st, _ := roundTrip(t, c2, &wire.ReplicateSubscribeRequest{})
		lastSt = st
		if st == wire.StatusOK {
			return
		}
		_ = c2.Close()
		c2 = dial(t, addr)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("idle-leader slot reuse: got %v want OK", lastSt)
}

// TestServerReplicateSubscribeIgnoresPerRequestReadDeadline pins the
// "no overall stream deadline" contract: a healthy idle follower must
// remain subscribed indefinitely, regardless of the server's
// per-request ReadDeadline. Before the read-deadline-clear landed, the
// handler's read-watcher inherited the stale request deadline set by
// handleConn; on a healthy idle follower the watcher's Read would time
// out, peerGone would close, and the server would tear the
// subscription down — exactly the failure the contract forbids.
//
// Setup: a server with a tiny ReadDeadline (50ms). Subscribe, wait
// past the deadline (no writes), then Put on a separate connection and
// assert the follower receives the record. If the handler still
// honours the per-request deadline, the wait-past step ends the
// stream and the follower's read returns EOF instead of a record.
func TestServerReplicateSubscribeIgnoresPerRequestReadDeadline(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{
		Dir:                   dir,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 64,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := New(db, Options{
		Addr:              "127.0.0.1:0",
		ReadDeadline:      50 * time.Millisecond, // deliberately tiny
		WriteDeadline:     2 * time.Second,
		EnableReplication: true,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})

	// Subscribe.
	follower := dial(t, addr)
	frame, _ := wire.EncodeRequest(&wire.ReplicateSubscribeRequest{})
	if _, err := follower.Write(frame); err != nil {
		t.Fatalf("follower write: %v", err)
	}
	_ = follower.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(follower)
	if err != nil {
		t.Fatalf("follower ack: %v", err)
	}
	if wire.Status(tag) != wire.StatusOK {
		t.Fatalf("follower ack: got %v want OK", wire.Status(tag))
	}

	// Idle past the server's per-request ReadDeadline. If the handler
	// inherited it, the read-watcher fires here and the subscription
	// is gone by the time we do the Put below.
	time.Sleep(200 * time.Millisecond)

	// Drive one Put on a separate conn.
	writer := dial(t, addr)
	if st, _ := roundTrip(t, writer, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")}); st != wire.StatusOK {
		t.Fatalf("writer Put: got %v want OK", st)
	}

	// Follower should still receive the record.
	_ = follower.SetReadDeadline(time.Now().Add(2 * time.Second))
	raw, err := wire.ReadReplicateRecord(follower)
	if err != nil {
		t.Fatalf("ReadReplicateRecord after idle past ReadDeadline: %v", err)
	}
	if !bytes.Contains(raw, []byte("k")) || !bytes.Contains(raw, []byte("v")) {
		t.Fatalf("record missing key/value: %x", raw)
	}
}

// --- 4. Error doesn't desync the connection -----------------------------

func TestServerErrorDoesNotDesyncConnection(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Send a malformed (but well-framed) GET.
	sendRaw(t, conn, uint8(wire.OpGet), []byte{0x00, 0x00}) // truncated klen
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame after bad request: %v", err)
	}
	if wire.Status(tag) != wire.StatusBadRequest {
		t.Fatalf("status: got %v want BAD_REQUEST", wire.Status(tag))
	}

	// Now send a perfectly fine PUT and GET and confirm we still get
	// valid responses — the conn was not desynchronized.
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("PUT after error: %v", st)
	}
	st, body := roundTrip(t, conn, &wire.GetRequest{Key: []byte("k")})
	if st != wire.StatusOK {
		t.Fatalf("GET after error: %v", st)
	}
	val, _ := wire.DecodeGetOK(body)
	if !bytes.Equal(val, []byte("v")) {
		t.Fatalf("GET value mismatch: %q", val)
	}
}

// --- 5. Range streams and terminates ------------------------------------

func TestServerReadKeyRangeStreamsAndTerminates(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		// Force multi-page streaming.
		o.MaxRangeResponseBytes = 64 * 1024 * 1024
	})
	conn := dial(t, addr)

	// Populate.
	const N = 200
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := bytes.Repeat([]byte{'v'}, 2048) // 2 KiB each → ~400 KiB total > flush threshold
		st, _ := roundTrip(t, conn, &wire.PutRequest{Key: k, Value: v})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	pages := 0
	var collected []wire.KV
	err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		pages++
		collected = append(collected, p...)
		return true
	})
	if err != nil {
		t.Fatalf("ReadRangeStream: %v", err)
	}
	if len(collected) != N {
		t.Fatalf("collected %d, want %d", len(collected), N)
	}
	if pages < 2 {
		t.Fatalf("expected multi-page stream, got %d page(s)", pages)
	}
}

// --- 6. Overload on concurrent stream limit ------------------------------

func TestServerReadKeyRangeOverloadOnConcurrentLimit(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxConcurrentRangeStreams = 1
		// Give A a generous write deadline so it stays blocked long
		// enough for B's request to land while the semaphore is held.
		o.WriteDeadline = 10 * time.Second
	})

	// Seed with enough data that a single range stream produces many
	// pages and far exceeds TCP socket buffers — so the server stays
	// blocked on writes (semaphore held) once client A stops reading.
	seed := dial(t, addr)
	const seedCount = 200
	for i := 0; i < seedCount; i++ {
		st, _ := roundTrip(t, seed, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: bytes.Repeat([]byte{'x'}, 16*1024), // 16 KiB → ~3.2 MiB total
		})
		if st != wire.StatusOK {
			t.Fatalf("seed PUT %d: %v", i, st)
		}
	}

	// Client A holds the only slot.
	a := dial(t, addr)
	sendFrame(t, a, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	// Read one frame so we know the server has started streaming (it's
	// past the semaphore acquire). A intentionally does not drain
	// further; subsequent writes will fill TCP buffers and the server
	// will block, holding the semaphore.
	_ = a.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := wire.ReadFrame(a); err != nil {
		t.Fatalf("client A first frame: %v", err)
	}

	// Client B requests a range — must be rejected with OVERLOAD.
	b := dial(t, addr)
	sendFrame(t, b, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(b)
	if err != nil {
		t.Fatalf("client B ReadFrame: %v", err)
	}
	if wire.Status(tag) != wire.StatusOverload {
		t.Fatalf("client B: got status %v want OVERLOAD", wire.Status(tag))
	}
}

// --- 7. Overload on byte cap --------------------------------------------

func TestServerReadKeyRangeOverloadOnByteCap(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxRangeResponseBytes = 16 * 1024 // 16 KiB cap
	})
	conn := dial(t, addr)

	// Populate well past the cap.
	for i := 0; i < 100; i++ {
		st, _ := roundTrip(t, conn, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: bytes.Repeat([]byte{'x'}, 1024), // ~1 KiB each
		})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	var collected []wire.KV
	err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		collected = append(collected, p...)
		return true
	})
	var re *wire.RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("expected RemoteError, got %v", err)
	}
	if re.Status != wire.StatusOverload {
		t.Fatalf("expected OVERLOAD, got %v", re.Status)
	}
	if len(collected) == 0 {
		t.Fatalf("expected at least one page of pairs before overload")
	}
}

// --- 8. No pipelining (sequential per-connection processing) ------------

func TestServerNoPipelining(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxRangeResponseBytes = 64 * 1024 * 1024
	})
	conn := dial(t, addr)

	// Seed enough data that the range stream takes multiple frames.
	const N = 50
	for i := 0; i < N; i++ {
		st, _ := roundTrip(t, conn, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: bytes.Repeat([]byte{'v'}, 8*1024),
		})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	// Pipeline two requests: READKEYRANGE then PING.
	rangeFrame, err := wire.EncodeFrame(uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	if err != nil {
		t.Fatalf("encode range frame: %v", err)
	}
	pingFrame, err := wire.EncodeRequest(&wire.PingRequest{})
	if err != nil {
		t.Fatalf("encode ping frame: %v", err)
	}

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(append(append([]byte{}, rangeFrame...), pingFrame...)); err != nil {
		t.Fatalf("write pipelined: %v", err)
	}

	// We must read the entire range stream FIRST (multi-frame: pages + END),
	// only AFTER which the PING response should arrive. If responses were
	// interleaved the PING reply would appear in the middle of the stream.
	sawEnd := false
	pageCount := 0
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		tag, body, err := wire.ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		status := wire.Status(tag)
		// As long as the range stream is in flight, every frame must be
		// a range page or the END frame — never the PING OK with body=nil
		// in a position that would have arrived only after the stream END.
		if !sawEnd {
			if status != wire.StatusOK {
				t.Fatalf("mid-stream frame: status %v want OK", status)
			}
			_, end, derr := wire.DecodeRangeFrame(body)
			if derr != nil {
				t.Fatalf("mid-stream frame failed to decode as range: %v", derr)
			}
			pageCount++
			if end {
				sawEnd = true
			}
			continue
		}
		// First frame after END must be the PING reply: status=OK,
		// empty body.
		if status != wire.StatusOK || len(body) != 0 {
			t.Fatalf("post-stream frame: want PING OK (status=OK body=empty), got status=%v body_len=%d", status, len(body))
		}
		break
	}
	if pageCount < 2 {
		t.Fatalf("expected multi-page stream, got %d frame(s)", pageCount)
	}
}

// --- 9. Concurrent clients ----------------------------------------------

func TestServerConcurrentClients(t *testing.T) {
	_, addr := startServer(t, nil)

	const clients = 8
	const ops = 50
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for c := 0; c < clients; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("dial: %w", err)
				return
			}
			defer conn.Close()
			for i := 0; i < ops; i++ {
				key := []byte("c" + strconv.Itoa(c) + "-" + strconv.Itoa(i))
				val := []byte("v" + strconv.Itoa(i))
				// PUT
				st, _ := roundTripConn(conn, &wire.PutRequest{Key: key, Value: val})
				if st != wire.StatusOK {
					errCh <- fmt.Errorf("client %d PUT %d: status %v", c, i, st)
					return
				}
				// GET
				st, body := roundTripConn(conn, &wire.GetRequest{Key: key})
				if st != wire.StatusOK {
					errCh <- fmt.Errorf("client %d GET %d: status %v", c, i, st)
					return
				}
				gotVal, _ := wire.DecodeGetOK(body)
				if !bytes.Equal(gotVal, val) {
					errCh <- fmt.Errorf("client %d GET %d: val %q want %q", c, i, gotVal, val)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// roundTripConn is roundTrip without the testing.T (so a goroutine can
// use it and report errors via a channel).
func roundTripConn(conn net.Conn, req wire.Request) (wire.Status, []byte) {
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		return 0xff, nil
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		return 0xff, nil
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		return 0xff, nil
	}
	return wire.Status(tag), body
}

// --- shared helpers ------------------------------------------------------

func sendRaw(t *testing.T, conn net.Conn, tag uint8, body []byte) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := wire.WriteFrame(conn, tag, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
}

func sendFrame(t *testing.T, conn net.Conn, tag uint8, body []byte) int {
	t.Helper()
	sendRaw(t, conn, tag, body)
	return len(body) + 5
}

func encodeRange(t *testing.T, start, end []byte) []byte {
	t.Helper()
	body := make([]byte, 4+len(start)+4+len(end))
	binary.BigEndian.PutUint32(body[0:4], uint32(len(start)))
	copy(body[4:], start)
	off := 4 + len(start)
	binary.BigEndian.PutUint32(body[off:off+4], uint32(len(end)))
	copy(body[off+4:], end)
	return body
}

func readRange(t *testing.T, conn net.Conn) []wire.KV {
	t.Helper()
	var pairs []wire.KV
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		pairs = append(pairs, p...)
		return true
	}); err != nil {
		t.Fatalf("ReadRangeStream: %v", err)
	}
	return pairs
}

func keysOf(pairs []wire.KV) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = string(p.Key)
	}
	return out
}

func equalStringSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	gm := make(map[string]int)
	for _, s := range got {
		gm[s]++
	}
	for _, s := range want {
		gm[s]--
	}
	for _, c := range gm {
		if c != 0 {
			return false
		}
	}
	return true
}

// Compile-time guard that we don't accidentally remove the atomic import
// (kept for symmetry with engine tests that rely on it).
var _ = atomic.Int64{}

// --- 7. Follower mode + dial loop ----------------------------------------

// TestServerFollowerModeRejectsWrites verifies the dispatchOnce gate:
// when FollowerMode is on, every mutating op gets StatusFollowerReadOnly
// with a message that includes the leader address, while reads pass
// through. This is the "safety net" half of follower mode; the apply
// loop is the "useful work" half (covered below).
func TestServerFollowerModeRejectsWrites(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
	})
	conn := dial(t, addr)

	for _, tc := range []struct {
		name string
		req  wire.Request
	}{
		{"PUT", &wire.PutRequest{Key: []byte("k"), Value: []byte("v")}},
		{"DELETE", &wire.DeleteRequest{Key: []byte("k")}},
		{"BATCH", &wire.BatchRequest{Entries: []wire.BatchEntry{{Key: []byte("k"), Value: []byte("v")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, body := roundTrip(t, conn, tc.req)
			if st != wire.StatusFollowerReadOnly {
				t.Fatalf("got status %v want FOLLOWER_READ_ONLY", st)
			}
			msg, err := wire.DecodeError(body)
			if err != nil {
				t.Fatalf("DecodeError: %v", err)
			}
			if !bytes.Contains([]byte(msg), []byte("leader.example:4242")) {
				t.Fatalf("msg %q should mention leader address", msg)
			}
		})
	}

	// Reads still work.
	st, _ := roundTrip(t, conn, &wire.PingRequest{})
	if st != wire.StatusOK {
		t.Fatalf("PING in follower mode: got %v want OK", st)
	}
	st, _ = roundTrip(t, conn, &wire.GetRequest{Key: []byte("missing")})
	if st != wire.StatusNotFound {
		t.Fatalf("GET in follower mode: got %v want NOT_FOUND", st)
	}
}

// openFollowerDB opens a fresh engine without replication. Tests use it
// as the "apply target" for a Follower.
func openFollowerDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(engine.Options{
		Dir:                 t.TempDir(),
		MaxBatchEncodedSize: 16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("engine.Open follower: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestFollowerLoopAppliesStream is the end-to-end happy path: spin up a
// leader with replication enabled, point a Follower at it, write 3 keys
// on the leader, assert all 3 appear on the follower within a short
// deadline. This validates dial → subscribe → ack → apply round-trip.
func TestFollowerLoopAppliesStream(t *testing.T) {
	_, leaderDB, leaderAddr := startReplicationServer(t, 64)
	followerDB := openFollowerDB(t)

	follower := NewFollower(leaderAddr, followerDB, FollowerOptions{
		InitialBackoff:      10 * time.Millisecond,
		MaxBackoff:          50 * time.Millisecond,
		SubscribeAckTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- follower.Run(ctx) }()

	// The publisher drops records when no subscriber is attached, so
	// we can't write once and wait. Instead, write a key in a loop
	// and poll the follower until it observes the value; this races
	// the subscriber-attach without depending on internal signals.
	putUntilApplied := func(t *testing.T, key, val string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := leaderDB.Put([]byte(key), []byte(val)); err != nil {
				t.Fatalf("leader put %s: %v", key, err)
			}
			got, err := followerDB.Get([]byte(key))
			if err == nil && bytes.Equal(got, []byte(val)) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("follower never observed key %s", key)
	}

	for _, k := range []string{"a", "b", "c"} {
		putUntilApplied(t, k, "v-"+k)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after ctx cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after ctx cancel")
	}
}

// waitForApplied writes key=val repeatedly until the follower observes
// the value, or fails the test. The publisher drops records when no
// subscriber is attached, so a one-shot write can race the subscribe
// handshake; the retry pattern is the cheapest way to bridge that.
func waitForApplied(t *testing.T, leader, follower *engine.DB, key, val string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := leader.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("leader put: %v", err)
		}
		got, err := follower.Get([]byte(key))
		if err == nil && bytes.Equal(got, []byte(val)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("follower never observed key %s=%s", key, val)
}

// TestFollowerLoopReconnectsAfterDisconnect kills the leader-side conn
// once and verifies the follower reconnects and resumes applying. We
// don't have a "kick the subscriber" op, so we restart the whole
// leader: shutdown, then bring up a fresh one on the same address.
// Reusing the same listener port across two Bind calls is racy, so
// instead the leader stays up and we close its single subscriber by
// shutting the server down hard and re-binding; that's complex enough
// that this test instead just observes one round of reconnect by
// pointing the follower at a temporarily-closed port and asserting
// no permanent failure once the leader comes up.
func TestFollowerLoopReconnectsAfterDisconnect(t *testing.T) {
	// Reserve a port by binding+closing a listener, then bring up the
	// leader at the same address shortly after the follower has tried
	// (and failed) to dial.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	followerDB := openFollowerDB(t)
	follower := NewFollower(addr, followerDB, FollowerOptions{
		DialTimeout:         200 * time.Millisecond,
		InitialBackoff:      20 * time.Millisecond,
		MaxBackoff:          100 * time.Millisecond,
		SubscribeAckTimeout: 1 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- follower.Run(ctx) }()

	// Let the follower fail at least once.
	time.Sleep(100 * time.Millisecond)

	// Now bring the leader up on the same address. startReplicationServer
	// uses 127.0.0.1:0; we need to bind specifically, so do it inline.
	dir := t.TempDir()
	leaderDB, err := engine.Open(engine.Options{
		Dir:                   dir,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 64,
	})
	if err != nil {
		t.Fatalf("leader engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = leaderDB.Close() })

	srv := New(leaderDB, Options{
		Addr:                      addr,
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		EnableReplication:         true,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("leader Bind: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = srv.Shutdown(shutCtx)
		<-serveErr
	})

	waitForApplied(t, leaderDB, followerDB, "reconnect", "ok")

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after ctx cancel")
	}
}

// TestFollowerRunReturnsOnDBClose verifies that closing the local DB
// during a live session causes Run to return ErrDBClosed (terminal) so
// the CLI knows to exit instead of looping forever.
func TestFollowerRunReturnsOnDBClose(t *testing.T) {
	_, leaderDB, leaderAddr := startReplicationServer(t, 64)
	followerDB := openFollowerDB(t)

	follower := NewFollower(leaderAddr, followerDB, FollowerOptions{
		InitialBackoff:      10 * time.Millisecond,
		MaxBackoff:          50 * time.Millisecond,
		SubscribeAckTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- follower.Run(ctx) }()

	waitForApplied(t, leaderDB, followerDB, "k", "v")

	if err := followerDB.Close(); err != nil {
		t.Fatalf("followerDB.Close: %v", err)
	}
	// Need to keep writing so the follower's read loop sees a record
	// to apply, which triggers ErrDBClosed.
	for i := 0; i < 20; i++ {
		if err := leaderDB.Put([]byte("k"+strconv.Itoa(i)), []byte("v")); err != nil {
			// engine may already be torn down; ignore.
			break
		}
		select {
		case err := <-runDone:
			if !errors.Is(err, engine.ErrDBClosed) {
				t.Fatalf("Run returned %v, want ErrDBClosed", err)
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("Run did not return ErrDBClosed after follower DB close")
}

// --- 8. PROMOTE ----------------------------------------------------------

// TestPromoteRejectsOnLeader: PROMOTE against a non-follower must
// return BAD_REQUEST so misaimed operator invocations are obvious.
func TestPromoteRejectsOnLeader(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)
	st, body := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusBadRequest {
		t.Fatalf("got status %v want BAD_REQUEST", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if !bytes.Contains([]byte(msg), []byte("not a follower")) {
		t.Fatalf("msg %q should mention 'not a follower'", msg)
	}
}

// TestPromoteFlipsFollowerMode: write rejected → PROMOTE OK → write
// accepted. Exercises the full atomic-flip path through dispatchOnce.
func TestPromoteFlipsFollowerMode(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
	})
	conn := dial(t, addr)

	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusFollowerReadOnly {
		t.Fatalf("pre-PROMOTE PUT: got %v want FOLLOWER_READ_ONLY", st)
	}

	st, body := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusOK {
		msg, _ := wire.DecodeError(body)
		t.Fatalf("PROMOTE: got %v (%s) want OK", st, msg)
	}

	st, body = roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		msg, _ := wire.DecodeError(body)
		t.Fatalf("post-PROMOTE PUT: got %v (%s) want OK", st, msg)
	}
}

// TestPromoteRunsHookBeforeFlip: the hook must complete before
// followerMode is cleared. Asserted by checking the hook ran AND a
// subsequent PUT succeeds; the inverse (PUT before hook) cannot happen
// because dispatchOnce reads the gate after the hook returns.
func TestPromoteRunsHookBeforeFlip(t *testing.T) {
	hookRan := make(chan struct{}, 1)
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
		o.OnPromote = func(ctx context.Context) error {
			hookRan <- struct{}{}
			return nil
		}
	})
	conn := dial(t, addr)
	st, _ := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusOK {
		t.Fatalf("PROMOTE: got %v want OK", st)
	}
	select {
	case <-hookRan:
	default:
		t.Fatal("OnPromote hook did not run")
	}
	st, _ = roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("post-PROMOTE PUT: got %v want OK", st)
	}
}

// TestPromoteIdempotentSecondCall: sync.Once guards the flip, and the
// second PROMOTE sees followerMode=false and returns BAD_REQUEST.
func TestPromoteIdempotentSecondCall(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
	})
	conn := dial(t, addr)
	if st, _ := roundTrip(t, conn, &wire.PromoteRequest{}); st != wire.StatusOK {
		t.Fatalf("first PROMOTE: got %v want OK", st)
	}
	st, body := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusBadRequest {
		t.Fatalf("second PROMOTE: got %v want BAD_REQUEST", st)
	}
	msg, _ := wire.DecodeError(body)
	if !bytes.Contains([]byte(msg), []byte("not a follower")) {
		t.Fatalf("second PROMOTE msg %q should say 'not a follower'", msg)
	}
}

// TestPromoteHookErrorReturnsInternal: hook failure must surface as
// INTERNAL and leave the server in follower mode so the operator can
// retry without losing read-only safety.
func TestPromoteHookErrorReturnsInternal(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
		o.OnPromote = func(ctx context.Context) error {
			return errors.New("drain failed")
		}
	})
	conn := dial(t, addr)
	st, body := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusInternal {
		t.Fatalf("PROMOTE: got %v want INTERNAL", st)
	}
	msg, _ := wire.DecodeError(body)
	if !bytes.Contains([]byte(msg), []byte("drain failed")) {
		t.Fatalf("PROMOTE msg %q should include hook error", msg)
	}
	// Server must still reject writes.
	st, _ = roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusFollowerReadOnly {
		t.Fatalf("post-failed-PROMOTE PUT: got %v want FOLLOWER_READ_ONLY", st)
	}
}

// TestPromoteHookFailureAllowsRetry pins finding #1 from review: a
// failed OnPromote must NOT consume any one-shot guard. The operator
// (or a supervisor) must be able to retry, see the hook re-run, and
// see the gate flip on success. The original sync.Once design swallowed
// the second attempt and returned OK without promoting.
func TestPromoteHookFailureAllowsRetry(t *testing.T) {
	var calls atomic.Int32
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
		o.OnPromote = func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				return errors.New("transient drain failure")
			}
			return nil
		}
	})
	conn := dial(t, addr)
	if st, _ := roundTrip(t, conn, &wire.PromoteRequest{}); st != wire.StatusInternal {
		t.Fatalf("first PROMOTE: got %v want INTERNAL", st)
	}
	st, body := roundTrip(t, conn, &wire.PromoteRequest{})
	if st != wire.StatusOK {
		msg, _ := wire.DecodeError(body)
		t.Fatalf("retry PROMOTE: got %v (%s) want OK", st, msg)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("OnPromote called %d times, want 2 (retry must re-invoke)", got)
	}
	st, _ = roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("post-retry PUT: got %v want OK", st)
	}
}

// TestPromoteSerializesConcurrentCallers pins the other half of
// finding #1: two simultaneous PROMOTEs must not both pass the
// followerMode check, both run the hook, and both return OK. promoteMu
// serializes them so exactly one runs the hook; the loser sees the
// flipped gate and returns BAD_REQUEST.
func TestPromoteSerializesConcurrentCallers(t *testing.T) {
	var calls atomic.Int32
	gate := make(chan struct{})
	hookEntered := make(chan struct{}, 1)
	_, addr := startServer(t, func(o *Options) {
		o.FollowerMode = true
		o.LeaderAddr = "leader.example:4242"
		o.OnPromote = func(ctx context.Context) error {
			calls.Add(1)
			select {
			case hookEntered <- struct{}{}:
			default:
			}
			<-gate
			return nil
		}
	})
	c1 := dial(t, addr)
	c2 := dial(t, addr)

	type result struct {
		st  wire.Status
		err error
	}
	results := make(chan result, 2)
	for _, c := range []net.Conn{c1, c2} {
		conn := c
		go func() {
			st, _, err := tryRoundTrip(conn, &wire.PromoteRequest{})
			results <- result{st: st, err: err}
		}()
	}

	// Block until the winner is actually inside the hook. Without
	// this the test can race: gate closes before either caller
	// reaches handlePromote, the winner runs through and flips the
	// gate, and the loser arrives to a non-follower server and
	// returns BAD_REQUEST without ever contending on promoteMu \u2014
	// the test passes for the wrong reason and the mutex
	// serialization is never exercised. Waiting for hookEntered
	// guarantees the winner holds promoteMu; the small sleep gives
	// the loser time to land in handlePromote and block on the
	// mutex (we can't directly observe that wait without hooking
	// the mutex itself).
	<-hookEntered
	time.Sleep(50 * time.Millisecond)
	close(gate)

	var ok, badReq int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("roundTrip: %v", r.err)
		}
		switch r.st {
		case wire.StatusOK:
			ok++
		case wire.StatusBadRequest:
			badReq++
		default:
			t.Fatalf("unexpected status %v", r.st)
		}
	}
	if ok != 1 || badReq != 1 {
		t.Fatalf("got ok=%d badReq=%d, want exactly one of each", ok, badReq)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("OnPromote called %d times, want exactly 1", got)
	}
}
