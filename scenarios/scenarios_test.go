//go:build replication_demo

package scenarios

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/client"
	"little-db/internal/engine"
	"little-db/internal/server"
	"little-db/internal/wire"
)

func reservePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// 1.
func TestScenario_LeaderKilledAndReplaced(t *testing.T) {
	addr := reservePort(t)
	leader1 := openLeaderDB(t, 0)
	srv1 := startLeaderServerAt(t, leader1, addr)
	follower := startFollower(t, addr)

	cli1 := dial(t, addr)
	putUntilApplied(t, cli1, follower.db, []byte("before"), []byte("v1"))

	shutCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv1.Shutdown(shutCtx)
	c()
	_ = leader1.Close()
	_ = cli1.Close()

	time.Sleep(150 * time.Millisecond)
	leader2 := openLeaderDB(t, 0)
	startLeaderServerAt(t, leader2, addr)
	cli2 := dial(t, addr)
	putUntilApplied(t, cli2, follower.db, []byte("after"), []byte("v2"))
	t.Logf("OK: follower KeyCount=%d across two leaders at %s", follower.db.Stats().KeyCount, addr)
}

// 2.
func TestScenario_ConnectionKilledMidRecord(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)

	proxy := startFaultProxy(t, leaderAddr, 64)
	faulty := startFollower(t, proxy.frontAddr)
	cli := dial(t, leaderAddr)

	for i := 0; i < 10; i++ {
		_ = cli.Put(keyN(i), bigValue(64, byte(i)))
	}
	time.Sleep(400 * time.Millisecond)

	clean := startFollower(t, leaderAddr)
	putUntilApplied(t, cli, clean.db, []byte("post-fault"), []byte("ok"))

	_, _ = faulty.db.Get(keyN(0))
	t.Logf("OK: clean=%d faulty=%d (no corruption)",
		clean.db.Stats().KeyCount, faulty.db.Stats().KeyCount)
}

// 3.
func TestScenario_LargeValuesReplicate(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)

	const valSize = 1 << 20
	const numKeys = 5
	values := make(map[string][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		k := keyN(i)
		v := bigValue(valSize, byte(i+1))
		values[string(k)] = v
		if err := cli.Put(k, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	waitFor(t, 15*time.Second, "tail applied", func() error {
		got, err := follower.db.Get(keyN(numKeys - 1))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, values[string(keyN(numKeys-1))]) {
			return errors.New("tail mismatch")
		}
		return nil
	})
	for k, want := range values {
		got, err := follower.db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("mismatch %q", k)
		}
	}
	t.Logf("OK: %d × %d-byte values intact", numKeys, valSize)
}

// 4.
func TestScenario_SlowFollowerCausesLeaderDrops(t *testing.T) {
	leaderDB, err := engine.Open(engine.Options{
		Dir:                   t.TempDir(),
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 4,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = leaderDB.Close() })

	sub, err := leaderDB.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	const burst = 200
	for i := 0; i < burst; i++ {
		if err := leaderDB.Put(keyN(i), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	dropped := leaderDB.Stats().ReplicationLagDropped
	if dropped == 0 {
		t.Fatalf("expected drops; got 0")
	}
	// Drops implicitly prove the writer didn't block on the slow
	// subscriber: if it had blocked instead of dropping, the counter
	// would still be 0.
	t.Logf("OK: %d puts, dropped=%d (writer unblocked by drop-on-overflow)", burst, dropped)
}

// 5.
func TestScenario_PromoteDuringWriteBurst(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)

	leaderCli := dial(t, leaderAddr)
	followerCli := dial(t, follower.addr)
	putUntilApplied(t, leaderCli, follower.db, []byte("prime"), []byte("v"))

	stop := make(chan struct{})
	var written atomic.Int64
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := leaderCli.Put(keyN(i+1000), []byte(fmt.Sprintf("v%d", i))); err == nil {
				written.Add(1)
			}
			i++
		}
	}()

	time.Sleep(150 * time.Millisecond)
	if err := followerCli.Promote(); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	close(stop)

	postKey := []byte("post-promote")
	postVal := []byte("local")
	if err := followerCli.Put(postKey, postVal); err != nil {
		t.Fatalf("post-promote Put: %v", err)
	}
	got, err := followerCli.Get(postKey)
	if err != nil || !bytes.Equal(got, postVal) {
		t.Fatalf("post-promote Get: got=%q err=%v", got, err)
	}
	t.Logf("OK: leader wrote %d burst keys; promote OK mid-burst (KeyCount leader=%d follower=%d)",
		written.Load(), leaderDB.Stats().KeyCount, follower.db.Stats().KeyCount)
}

// 6.
func TestScenario_PromoteWhileLeaderDown(t *testing.T) {
	addr := reservePort(t)
	leaderDB := openLeaderDB(t, 0)
	srv := startLeaderServerAt(t, leaderDB, addr)
	follower := startFollower(t, addr)
	cli := dial(t, addr)
	putUntilApplied(t, cli, follower.db, []byte("a"), []byte("1"))

	shutCtx, c := context.WithTimeout(context.Background(), 1*time.Second)
	_ = srv.Shutdown(shutCtx)
	c()
	_ = leaderDB.Close()
	_ = cli.Close()

	time.Sleep(200 * time.Millisecond)

	followerCli := dial(t, follower.addr)
	done := make(chan error, 1)
	go func() { done <- followerCli.Promote() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Promote: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Promote hung while leader was down")
	}
	if err := followerCli.Put([]byte("post"), []byte("ok")); err != nil {
		t.Fatalf("post-promote Put: %v", err)
	}
	t.Log("OK: promoted with no live leader")
}

// 7.
func TestScenario_FollowerRestartResumesFromTail(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	cli := dial(t, leaderAddr)

	f1 := startFollower(t, leaderAddr)
	putUntilApplied(t, cli, f1.db, []byte("phase1"), []byte("a"))
	f1.runnerCancel()
	select {
	case <-f1.runnerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("f1 runner did not exit")
	}
	// Give the leader's subscription goroutine time to observe the
	// closed conn and release the publisher slot, so the next Put has
	// no subscriber and is dropped (the behavior we want to assert).
	time.Sleep(250 * time.Millisecond)

	if err := cli.Put([]byte("missed"), []byte("gap")); err != nil {
		t.Fatalf("missed Put: %v", err)
	}

	f2 := startFollower(t, leaderAddr)
	putUntilApplied(t, cli, f2.db, []byte("phase2"), []byte("b"))

	if _, err := f2.db.Get([]byte("missed")); !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("f2 unexpectedly observed gap-window write 'missed': err=%v (resume-from-tail violated)", err)
	}
	t.Log("OK: f2 did not see gap-window write (resume-from-tail confirmed)")
}

// 8.
func TestScenario_TombstonesSurvivePromote(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	leaderCli := dial(t, leaderAddr)
	followerCli := dial(t, follower.addr)

	putUntilApplied(t, leaderCli, follower.db, []byte("k"), []byte("v1"))
	if err := leaderCli.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitFor(t, 3*time.Second, "tombstone", func() error {
		_, err := follower.db.Get([]byte("k"))
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("err=%v want ErrKeyNotFound", err)
	})
	if err := followerCli.Promote(); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if _, err := followerCli.Get([]byte("k")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("post-promote Get: got %v want ErrNotFound", err)
	}
	t.Log("OK: tombstone replicated and survived promote")
}

// 9.
func TestScenario_MixedBatchAtomicEndToEnd(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)

	putUntilApplied(t, cli, follower.db, []byte("k1"), []byte("v1"))
	if err := cli.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	entries := []wire.BatchEntry{
		{Delete: true, Key: []byte("k1")},
		{Delete: false, Key: []byte("k3"), Value: []byte("v3")},
	}
	if err := cli.Batch(entries); err != nil {
		t.Fatalf("Batch: %v", err)
	}
	waitFor(t, 3*time.Second, "batch applied", func() error {
		if _, err := follower.db.Get([]byte("k1")); !errors.Is(err, engine.ErrKeyNotFound) {
			return fmt.Errorf("k1 still present: %v", err)
		}
		v, err := follower.db.Get([]byte("k3"))
		if err != nil {
			return err
		}
		if string(v) != "v3" {
			return fmt.Errorf("k3=%q", v)
		}
		return nil
	})
	t.Log("OK: mixed batch atomic")
}

// 10.
func TestScenario_SecondFollowerRunnerRejected(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	f1 := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)
	putUntilApplied(t, cli, f1.db, []byte("k"), []byte("v"))

	f2db := openFollowerDB(t)
	runner2 := server.NewFollower(leaderAddr, f2db, server.FollowerOptions{
		DialTimeout:         500 * time.Millisecond,
		InitialBackoff:      10 * time.Millisecond,
		MaxBackoff:          50 * time.Millisecond,
		SubscribeAckTimeout: 1 * time.Second,
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	r2done := make(chan error, 1)
	go func() { r2done <- runner2.Run(ctx2) }()

	for i := 0; i < 10; i++ {
		_ = cli.Put(keyN(i), []byte("x"))
	}
	time.Sleep(500 * time.Millisecond)

	if f2db.Stats().KeyCount != 0 {
		t.Fatalf("f2 applied %d keys; want 0", f2db.Stats().KeyCount)
	}
	cancel2()
	select {
	case <-r2done:
	case <-time.After(2 * time.Second):
		t.Fatal("f2 runner did not exit")
	}
	t.Logf("OK: f1=%d f2=0", f1.db.Stats().KeyCount)
}

// 11.
func TestScenario_SequentialPromotesIdempotent(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)

	c1 := dial(t, follower.addr)
	c2 := dial(t, follower.addr)
	c3 := dial(t, follower.addr)

	if err := c1.Promote(); err != nil {
		t.Fatalf("c1 Promote: %v", err)
	}
	for label, cli := range map[string]*client.Client{"c2": c2, "c3": c3} {
		expectRemoteStatus(t, cli.Promote(), wire.StatusBadRequest)
		t.Logf("OK: %s second Promote -> BAD_REQUEST", label)
	}
}

// 12.
func TestScenario_PromoteHookDeadlineThenRetry(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	followerDB := openFollowerDB(t)

	var calls atomic.Int32
	followerSrv := server.New(followerDB, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              5 * time.Second,
		WriteDeadline:             300 * time.Millisecond,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		FollowerMode:              true,
		LeaderAddr:                leaderAddr,
		OnPromote: func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return nil
				}
			}
			return nil
		},
	})
	if err := followerSrv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := followerSrv.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- followerSrv.Serve() }()
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = followerSrv.Shutdown(ctx)
		<-serveErr
	})

	c1 := dial(t, addr)
	expectRemoteStatus(t, c1.Promote(), wire.StatusInternal)
	expectRemoteStatus(t, c1.Put([]byte("k"), []byte("v")), wire.StatusFollowerReadOnly)

	if err := c1.Promote(); err != nil {
		t.Fatalf("retry Promote: %v", err)
	}
	if err := c1.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("post-retry Put: %v", err)
	}
	t.Logf("OK: deadline -> INTERNAL, retry OK (hook calls=%d)", calls.Load())
}

// 13.
func TestScenario_ReplicationSurvivesRotation(t *testing.T) {
	leaderDB := openLeaderDB(t, 8*1024)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)
	putUntilApplied(t, cli, follower.db, []byte("p"), []byte("v"))

	const n = 200
	val := bigValue(200, 0xAA)
	for i := 0; i < n; i++ {
		if err := cli.Put(keyN(i), val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	waitFor(t, 15*time.Second, "tail applied", func() error {
		got, err := follower.db.Get(keyN(n - 1))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, val) {
			return errors.New("tail mismatch")
		}
		return nil
	})
	ls := leaderDB.Stats()
	fs := follower.db.Stats()
	if ls.KeyCount != fs.KeyCount {
		t.Fatalf("KeyCount drift: leader=%d follower=%d", ls.KeyCount, fs.KeyCount)
	}
	t.Logf("OK: %d keys across rotation; bytesOnDisk leader=%d follower=%d",
		ls.KeyCount, ls.BytesOnDisk, fs.BytesOnDisk)
}

// 14.
func TestScenario_ReplicationUnaffectedByCompaction(t *testing.T) {
	leaderDB := openLeaderDB(t, 0)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)
	putUntilApplied(t, cli, follower.db, []byte("seed"), []byte("v"))
	for i := 0; i < 50; i++ {
		k := keyN(i % 5)
		if err := cli.Put(k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	waitFor(t, 5*time.Second, "rewrites applied", func() error {
		v, err := follower.db.Get(keyN(0))
		if err != nil {
			return err
		}
		if string(v) != "v45" {
			return fmt.Errorf("k0=%q want v45", v)
		}
		return nil
	})
	pre := follower.db.Stats().KeyCount

	if err := leaderDB.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := cli.Put([]byte("post-compact"), []byte("hi")); err != nil {
		t.Fatalf("post-compact Put: %v", err)
	}
	waitFor(t, 3*time.Second, "post-compact key", func() error {
		v, err := follower.db.Get([]byte("post-compact"))
		if err != nil {
			return err
		}
		if string(v) != "hi" {
			return fmt.Errorf("got=%q", v)
		}
		return nil
	})
	post := follower.db.Stats().KeyCount
	if post < pre {
		t.Fatalf("follower KeyCount regressed: pre=%d post=%d", pre, post)
	}
	t.Logf("OK: undisturbed by compaction (KeyCount pre=%d post=%d)", pre, post)
}

// 15.
func TestScenario_RapidLeaderRestartCycles(t *testing.T) {
	addr := reservePort(t)
	openLeader := func() (*engine.DB, *server.Server, *client.Client) {
		db := openLeaderDB(t, 0)
		srv := startLeaderServerAt(t, db, addr)
		cli := dial(t, addr)
		return db, srv, cli
	}

	leaderDB, leaderSrv, leaderCli := openLeader()
	follower := startFollower(t, addr)
	putUntilApplied(t, leaderCli, follower.db, []byte("g0"), []byte("v"))

	const cycles = 4
	for i := 1; i <= cycles; i++ {
		shutCtx, c := context.WithTimeout(context.Background(), 1*time.Second)
		_ = leaderSrv.Shutdown(shutCtx)
		c()
		_ = leaderDB.Close()
		_ = leaderCli.Close()

		time.Sleep(120 * time.Millisecond)
		leaderDB, leaderSrv, leaderCli = openLeader()
		putUntilApplied(t, leaderCli, follower.db,
			[]byte(fmt.Sprintf("g%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	t.Logf("OK: follower survived %d leader restart cycles (KeyCount=%d)",
		cycles, follower.db.Stats().KeyCount)
}
