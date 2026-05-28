package client

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"little-db/internal/wire"
)

// mockServer drives one side of a net.Pipe according to a handler
// function. It runs in a goroutine; the test waits for it via t.Cleanup.
type mockServer struct {
	t    *testing.T
	conn net.Conn
	done chan struct{}
}

// newMockPair returns (clientConn, server). The server's goroutine reads
// requests and dispatches to handler. handler returns the response frames
// to write (each as (tag, body)); returning a nil slice closes the conn.
func newMockPair(t *testing.T, handler func(req wire.Request) [][2]any) (net.Conn, *mockServer) {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	srv := &mockServer{t: t, conn: serverSide, done: make(chan struct{})}
	go func() {
		defer close(srv.done)
		defer serverSide.Close()
		for {
			req, err := wire.ReadRequest(serverSide)
			if err != nil {
				return
			}
			frames := handler(req)
			if frames == nil {
				return
			}
			for _, f := range frames {
				tag := f[0].(uint8)
				body, _ := f[1].([]byte)
				if err := wire.WriteFrame(serverSide, tag, body); err != nil {
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		_ = clientSide.Close()
		select {
		case <-srv.done:
		case <-time.After(2 * time.Second):
			t.Logf("mock server goroutine did not exit within 2s")
		}
	})
	return clientSide, srv
}

func okFrame(body []byte) [2]any { return [2]any{uint8(wire.StatusOK), body} }
func errFrame(s wire.Status, msg string) [2]any {
	return [2]any{uint8(s), wire.EncodeError(msg)}
}

func newTestClient(t *testing.T, conn net.Conn) *Client {
	t.Helper()
	return NewClient(conn, Options{RequestTimeout: 2 * time.Second})
}

func TestClientPutOK(t *testing.T) {
	var gotReq *wire.PutRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		gotReq = req.(*wire.PutRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if string(gotReq.Key) != "k" || string(gotReq.Value) != "v" {
		t.Fatalf("server saw key=%q value=%q", gotReq.Key, gotReq.Value)
	}
}

func TestClientGetOK(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.GetRequest)
		body, _ := wire.EncodeGetOK([]byte("hello"))
		return [][2]any{okFrame(body)}
	})
	c := newTestClient(t, conn)
	got, err := c.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("Get value = %q want %q", got, "hello")
	}
}

func TestClientGetNotFoundReturnsSentinel(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusNotFound, "")}
	})
	c := newTestClient(t, conn)
	_, err := c.Get([]byte("missing"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
}

func TestClientGetInternalReturnsRemoteError(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusInternal, "boom")}
	})
	c := newTestClient(t, conn)
	_, err := c.Get([]byte("k"))
	var re *wire.RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("Get: err = %v (%T), want *wire.RemoteError", err, err)
	}
	if re.Status != wire.StatusInternal || re.Msg != "boom" {
		t.Fatalf("RemoteError = %+v", re)
	}
}

func TestClientDeleteOK(t *testing.T) {
	var saw *wire.DeleteRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		saw = req.(*wire.DeleteRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	if err := c.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if string(saw.Key) != "k" {
		t.Fatalf("saw key=%q", saw.Key)
	}
}

func TestClientPing(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.PingRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	if err := c.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestClientStats(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.StatsRequest)
		return [][2]any{okFrame(wire.EncodeStatsOK(42, 9999))}
	})
	c := newTestClient(t, conn)
	kc, bd, err := c.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if kc != 42 || bd != 9999 {
		t.Fatalf("Stats: kc=%d bd=%d want 42, 9999", kc, bd)
	}
}

func TestClientBatch(t *testing.T) {
	var saw *wire.BatchRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		saw = req.(*wire.BatchRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	entries := []wire.BatchEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Delete: true},
	}
	if err := c.Batch(entries); err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if len(saw.Entries) != 2 || string(saw.Entries[0].Key) != "a" || !saw.Entries[1].Delete {
		t.Fatalf("server saw entries = %+v", saw.Entries)
	}
}

func TestClientReadKeyRangeMultiplePages(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.ReadKeyRangeRequest)
		p1, _ := wire.EncodeRangePage([]wire.KV{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
		})
		p2, _ := wire.EncodeRangePage([]wire.KV{
			{Key: []byte("c"), Value: []byte("3")},
		})
		return [][2]any{okFrame(p1), okFrame(p2), okFrame(wire.EncodeRangeEnd())}
	})
	c := newTestClient(t, conn)
	var keys []string
	err := c.ReadKeyRange(nil, nil, func(pairs []wire.KV) bool {
		for _, p := range pairs {
			keys = append(keys, string(p.Key))
		}
		return true
	})
	if err != nil {
		t.Fatalf("ReadKeyRange: %v", err)
	}
	if got := keys; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("keys = %v want [a b c]", got)
	}
}

func TestClientReadKeyRangeOverloadMidStream(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.ReadKeyRangeRequest)
		p1, _ := wire.EncodeRangePage([]wire.KV{{Key: []byte("a"), Value: []byte("1")}})
		return [][2]any{okFrame(p1), errFrame(wire.StatusOverload, "cap")}
	})
	c := newTestClient(t, conn)
	var pageCount int
	err := c.ReadKeyRange(nil, nil, func(pairs []wire.KV) bool {
		pageCount++
		return true
	})
	if pageCount != 1 {
		t.Fatalf("expected 1 page before overload, got %d", pageCount)
	}
	var re *wire.RemoteError
	if !errors.As(err, &re) || re.Status != wire.StatusOverload {
		t.Fatalf("err = %v want OVERLOAD RemoteError", err)
	}
}

func TestClientReadKeyRangeCallerStops(t *testing.T) {
	// Server keeps pushing pages until the pipe blocks. We just need to
	// know that returning false from pageFn surfaces ErrStreamStopped
	// without waiting for END.
	page, _ := wire.EncodeRangePage([]wire.KV{{Key: []byte("a"), Value: []byte("1")}})
	var mu sync.Mutex
	var sent int
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		_ = req.(*wire.ReadKeyRangeRequest)
		mu.Lock()
		sent++
		mu.Unlock()
		// Single page; we won't be asked for more because the client
		// stops reading after seeing one page. Returning just one
		// page-frame means the server goroutine then blocks in
		// ReadRequest (waiting for the next request) — that's fine,
		// t.Cleanup will close the conn.
		return [][2]any{okFrame(page)}
	})
	c := newTestClient(t, conn)
	err := c.ReadKeyRange(nil, nil, func(pairs []wire.KV) bool {
		return false // ask the client to stop after the first page
	})
	if !errors.Is(err, wire.ErrStreamStopped) {
		t.Fatalf("err = %v want ErrStreamStopped", err)
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any { return nil })
	c := newTestClient(t, conn)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestClientReadDeadlineFiresWhenServerSilent(t *testing.T) {
	// Server reads the request but never replies. The client's
	// per-request deadline must fire.
	block := make(chan struct{})
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		<-block // block forever; the cleanup-Close releases us
		return nil
	})
	t.Cleanup(func() { close(block) })
	c := NewClient(conn, Options{RequestTimeout: 50 * time.Millisecond})
	err := c.Ping()
	if err == nil {
		t.Fatalf("Ping: expected deadline error, got nil")
	}
	// We don't pin the exact error type — net.Pipe returns its own
	// timeout shape — but it must NOT be io.EOF and must NOT be a
	// RemoteError. It IS a deadline error of some flavour.
	if errors.Is(err, io.EOF) {
		t.Fatalf("Ping: unexpected io.EOF: %v", err)
	}
	var re *wire.RemoteError
	if errors.As(err, &re) {
		t.Fatalf("Ping: unexpected RemoteError: %v", err)
	}
}

func TestClientDialFailsFast(t *testing.T) {
	// 127.0.0.1:1 is reserved (tcpmux) and almost certainly refused.
	// If a test environment happens to listen there, this test will
	// false-positive — we accept that trade for not needing to spin
	// up a real listener just to be refused by it.
	_, err := Dial("127.0.0.1:1", Options{DialTimeout: 500 * time.Millisecond})
	if err == nil {
		t.Skip("something is actually listening on 127.0.0.1:1; skipping")
	}
}
