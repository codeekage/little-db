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
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return wire.Status(tag), body
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

// TestServerReplicateSubscribeDisabledByDefault verifies that even when
// the underlying engine has replication enabled, the server requires an
// explicit opt-in (Options.EnableReplication) before exposing the
// subscribe endpoint. Exposing the change stream is a security-sensitive
// capability — every write becomes externally readable — so the server
// boundary fails closed.
func TestServerReplicateSubscribeDisabledByDefault(t *testing.T) {
	_, addr := startServer(t, nil)
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
	// shape the design depends on (docs/replication.md §6: promote then
	// re-subscribe).
	//
	// Mechanism: the server's stream loop only notices a dead follower
	// when its next write fails, so we drive a Put through a separate
	// writer connection to push a record at the dead conn. That write
	// errors, the handler returns, and the deferred sub.Close()
	// detaches.
	_ = c1.Close()
	writer := dial(t, addr)
	deadline := time.Now().Add(2 * time.Second)
	var lastSt wire.Status
	c3 := dial(t, addr)
	for time.Now().Before(deadline) {
		if st, _ := roundTrip(t, writer, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")}); st != wire.StatusOK {
			t.Fatalf("writer Put: got %v want OK", st)
		}
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
