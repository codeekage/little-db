//go:build replication_demo

// Package scenarios contains exploration-only replication and
// failover demonstrations. Build-tagged so default `go test ./...`
// ignores them. Run with:
//
//	go test -tags replication_demo -v ./scenarios/...                 # failure scenarios
//	go test -tags replication_demo,replication_demo_heavy -v ./scenarios/...  # + volume
//
// None of this is shipped or graded; it's here so we can self-
// demonstrate the system holds up under failure modes the focused
// unit tests don't cover (network partitions, leader restarts,
// promote-during-burst, 1M-row replication, etc).
package scenarios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/client"
	"little-db/internal/engine"
	"little-db/internal/server"
	"little-db/internal/wire"
)

// ---- engine + server scaffolding ---------------------------------------

func openLeaderDB(t *testing.T, segmentBytes int64) *engine.DB {
	t.Helper()
	db, err := engine.Open(engine.Options{
		Dir:                   t.TempDir(),
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 65_536,
		MaxSegmentSize:        segmentBytes,
	})
	if err != nil {
		t.Fatalf("leader engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openFollowerDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(engine.Options{
		Dir:                 t.TempDir(),
		MaxBatchEncodedSize: 16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("follower engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// startLeaderServer brings up a writable leader on a random port and
// returns it + the address. Cleans up on test end.
func startLeaderServer(t *testing.T, db *engine.DB) (*server.Server, string) {
	t.Helper()
	srv := server.New(db, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              5 * time.Second,
		WriteDeadline:             5 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		EnableReplication:         true,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("leader Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
	})
	return srv, addr
}

// startLeaderServerAt is startLeaderServer pinned to a specific address
// (used for "kill-and-replace" scenarios where the new leader must bind
// the same port the follower is already retrying). Retries Bind briefly
// to absorb the kernel-level TIME_WAIT / reservePort TOCTOU window.
func startLeaderServerAt(t *testing.T, db *engine.DB, addr string) *server.Server {
	t.Helper()
	var srv *server.Server
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv = server.New(db, server.Options{
			Addr:                      addr,
			ReadDeadline:              5 * time.Second,
			WriteDeadline:             5 * time.Second,
			MaxConcurrentRangeStreams: 4,
			MaxRangeResponseBytes:     64 * 1024 * 1024,
			EnableReplication:         true,
		})
		if err := srv.Bind(); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("leader Bind %s after retries: %v", addr, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
	})
	return srv
}

// followerPair wires a follower engine + read-only server + replication
// runner pointed at leaderAddr. Returns everything the test needs to
// observe and drive failover.
type followerPair struct {
	db           *engine.DB
	srv          *server.Server
	addr         string
	runnerCancel context.CancelFunc
	runnerDone   chan error
	runnerExited chan struct{}
}

func startFollower(t *testing.T, leaderAddr string) *followerPair {
	t.Helper()
	db := openFollowerDB(t)

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	t.Cleanup(runnerCancel)
	runner := server.NewFollower(leaderAddr, db, server.FollowerOptions{
		DialTimeout:         500 * time.Millisecond,
		InitialBackoff:      10 * time.Millisecond,
		MaxBackoff:          100 * time.Millisecond,
		SubscribeAckTimeout: 2 * time.Second,
	})
	runnerDone := make(chan error, 1)
	runnerExited := make(chan struct{})
	go func() {
		runnerDone <- runner.Run(runnerCtx)
		close(runnerExited)
	}()

	srv := server.New(db, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              5 * time.Second,
		WriteDeadline:             5 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		FollowerMode:              true,
		LeaderAddr:                leaderAddr,
		OnPromote: func(ctx context.Context) error {
			runnerCancel()
			select {
			case <-runnerExited:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("follower Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
	})
	return &followerPair{
		db:           db,
		srv:          srv,
		addr:         addr,
		runnerCancel: runnerCancel,
		runnerDone:   runnerDone,
		runnerExited: runnerExited,
	}
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr, client.Options{
		DialTimeout:    2 * time.Second,
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// putUntilApplied PUT-loops on the leader until the follower observes
// the value (bridges the SUBSCRIBE race). Mirrors the pattern in the
// server package's waitForApplied helper.
func putUntilApplied(t *testing.T, leaderCli *client.Client, followerDB *engine.DB, key, val []byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := leaderCli.Put(key, val); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
		got, err := followerDB.Get(key)
		if err == nil && string(got) == string(val) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never observed %q: err=%v got=%q", key, err, got)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// waitFor polls fn until it returns nil or deadline elapses.
func waitFor(t *testing.T, dur time.Duration, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(dur)
	var last error
	for {
		if last = fn(); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waitFor %s: %v", what, last)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// ---- TCP fault-injection proxy ------------------------------------------

// faultProxy is a tiny TCP proxy used by scenarios that need to break
// the wire between leader and follower mid-stream. It accepts on
// frontAddr, dials backendAddr, and after totalBytes have flowed from
// backend → client it slams both halves shut. Useful for asserting the
// follower's reconnect logic handles a torn record without corrupting
// the keydir.
type faultProxy struct {
	listener     net.Listener
	frontAddr    string
	backend      string
	bytesAllowed int64
	closed       atomic.Bool
}

func startFaultProxy(t *testing.T, backendAddr string, bytesAllowed int64) *faultProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &faultProxy{
		listener:     l,
		frontAddr:    l.Addr().String(),
		backend:      backendAddr,
		bytesAllowed: bytesAllowed,
	}
	go p.acceptLoop()
	t.Cleanup(p.close)
	return p
}

func (p *faultProxy) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *faultProxy) handle(client net.Conn) {
	defer client.Close()
	backend, err := net.Dial("tcp", p.backend)
	if err != nil {
		return
	}
	defer backend.Close()

	// client → backend: pipe straight through (small).
	go func() { _, _ = io.Copy(backend, client) }()
	// backend → client: count bytes, slam shut after threshold.
	var written int64
	buf := make([]byte, 4096)
	for {
		n, rerr := backend.Read(buf)
		if n > 0 {
			toWrite := n
			if p.bytesAllowed > 0 && written+int64(n) > p.bytesAllowed {
				toWrite = int(p.bytesAllowed - written)
				if toWrite < 0 {
					toWrite = 0
				}
			}
			if toWrite > 0 {
				if _, werr := client.Write(buf[:toWrite]); werr != nil {
					return
				}
				written += int64(toWrite)
			}
			if p.bytesAllowed > 0 && written >= p.bytesAllowed {
				return // slams defers, both halves close
			}
		}
		if rerr != nil {
			return
		}
	}
}

func (p *faultProxy) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	_ = p.listener.Close()
}

// ---- value helpers ------------------------------------------------------

func bigValue(n int, seed byte) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = seed + byte(i%251)
	}
	return v
}

func keyN(i int) []byte { return []byte(fmt.Sprintf("k/%010d", i)) }

// expectRemoteStatus asserts err is a *wire.RemoteError with the given
// status; fails the test otherwise. Returns the *RemoteError for
// further inspection if needed.
func expectRemoteStatus(t *testing.T, err error, want wire.Status) *wire.RemoteError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with status %v, got nil", want)
	}
	var re *wire.RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("expected *wire.RemoteError with status %v, got %T: %v", want, err, err)
	}
	if re.Status != want {
		t.Fatalf("status: got %v want %v (msg=%q)", re.Status, want, re.Msg)
	}
	return re
}

// avoid unused-import lint when individual scenarios trim a dep.
var (
	_ = sync.Mutex{}
	_ = errors.New
)
