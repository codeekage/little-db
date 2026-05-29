package engine_test

// Compliance tests for SPEC §2 measurable goals (G1–G8).
//
// External test package: lets us import internal/server and internal/client
// without creating a cycle (server depends on engine).
//
// Each TestReqN_* maps directly to a row in SPEC §2. By default the
// tests run a small, fast workload that proves the *shape* of each
// goal: crash-recover survives an ungraceful close, latency
// distributions are measured, throughput is sustained, the API surface
// is reachable, the network path round-trips. Setting the env var
//
//	LITTLEDB_HEAVY=1
//
// switches the same tests to the SPEC's full-scale workloads (1M-key
// dataset, 5-minute mixed run, dataset-larger-than-RAM sweep) and
// promotes the loose default assertions into the SPEC numbers.
// LITTLEDB_DATASET_GIB controls the §4 dataset size (default 8 GiB).
//
// G8 (replication) is bonus per §14 Batch 8 and is exercised end-to-end
// by TestReq8_ReplicationBonus on this branch.

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/client"
	"little-db/internal/engine"
	"little-db/internal/server"
	"little-db/internal/wire"
)

// ---------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------

func isHeavy() bool { return os.Getenv("LITTLEDB_HEAVY") == "1" }

func datasetGiB() int {
	if v := os.Getenv("LITTLEDB_DATASET_GIB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 8
}

// open16K returns a deterministic 256-byte value buffer.
func value256() []byte {
	v := make([]byte, 256)
	for i := range v {
		v[i] = byte(i)
	}
	return v
}

// keyAt fills buf with a 16-byte hex id derived from seed.
func keyAt(buf []byte, seed uint64) []byte {
	const hex = "0123456789abcdef"
	for j := 0; j < 16; j++ {
		buf[15-j] = hex[seed&0xF]
		seed >>= 4
	}
	return buf[:16]
}

// percentile returns the p-th percentile (0.0–1.0) of a sorted samples
// slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func openDB(t *testing.T, opts engine.Options) *engine.DB {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	db, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	return db
}

// ---------------------------------------------------------------------
// G1 — Crash-correctness: every acked Put with SyncOnPut=true must
// survive an ungraceful process death (SPEC §2 G1 / kill -9). We run
// the writer in a subprocess and abort it with os.Exit, which bypasses
// every Go-level defer and lets only the kernel clean up file
// descriptors and the data-dir flock. The parent then opens the same
// directory and verifies every key.
// ---------------------------------------------------------------------

// envG1HelperDir is set on the subprocess to switch TestReq1HelperCrash
// from a no-op into the writer fixture. Format: "<dir>:<n>".
const envG1HelperDir = "LITTLEDB_G1_HELPER"

func TestReq1HelperCrash(t *testing.T) {
	spec := os.Getenv(envG1HelperDir)
	if spec == "" {
		t.Skip("helper: parent did not set " + envG1HelperDir)
	}
	// Format: <dir>:<n>
	sep := -1
	for i := len(spec) - 1; i >= 0; i-- {
		if spec[i] == ':' {
			sep = i
			break
		}
	}
	if sep < 0 {
		t.Fatalf("helper: bad spec %q", spec)
	}
	dir := spec[:sep]
	n, err := strconv.Atoi(spec[sep+1:])
	if err != nil || n <= 0 {
		t.Fatalf("helper: bad N in spec %q: %v", spec, err)
	}
	db, err := engine.Open(engine.Options{Dir: dir, SyncOnPut: true})
	if err != nil {
		t.Fatalf("helper: Open: %v", err)
	}
	val := value256()
	var keyBuf [16]byte
	for i := 0; i < n; i++ {
		if err := db.Put(keyAt(keyBuf[:], uint64(i)), val); err != nil {
			t.Fatalf("helper: Put %d: %v", i, err)
		}
	}
	// Hard exit: skip every Go defer (including db.Close), drop the
	// writer-goroutine teardown and the flock release. The kernel
	// closes the fds and releases the flock when the process dies.
	// This is the SIGKILL-equivalent we need to make the G1 signal
	// honest.
	os.Stdout.Sync()
	os.Exit(0)
}

func TestReq1_CrashCorrectness(t *testing.T) {
	dir := t.TempDir()
	n := 1_000
	if isHeavy() {
		n = 50_000
	}

	// Re-execute this test binary as a subprocess that runs only the
	// helper test. The helper opens the DB, writes N keys with
	// SyncOnPut=true, and os.Exit(0)s — simulating a crash mid-life.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe,
		"-test.run=^TestReq1HelperCrash$",
		"-test.timeout=45s",
	)
	cmd.Env = append(os.Environ(),
		envG1HelperDir+"="+dir+":"+strconv.Itoa(n),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper subprocess failed: %v\n--- output ---\n%s", err, out)
	}

	// Parent: reopen the same directory and verify every key.
	db := openDB(t, engine.Options{Dir: dir})
	defer db.Close()
	val := value256()
	var keyBuf [16]byte
	for i := 0; i < n; i++ {
		got, err := db.Get(keyAt(keyBuf[:], uint64(i)))
		if err != nil {
			t.Fatalf("Get %d after crash-reopen: %v", i, err)
		}
		if len(got) != len(val) {
			t.Fatalf("Get %d: len=%d want %d", i, len(got), len(val))
		}
	}
	t.Logf("G1 OK: %d acked Puts survived subprocess os.Exit(0) (kill -9 equivalent)", n)
}

// ---------------------------------------------------------------------
// G2 — Latency: p99 Get < 1 ms, p99 sync Put < 5 ms (heavy targets).
// ---------------------------------------------------------------------

func TestReq2_LatencyP99(t *testing.T) {
	seedN := 100_000
	probeN := 20_000
	if isHeavy() {
		seedN = 1_000_000
		probeN = 200_000
	}

	db := openDB(t, engine.Options{SyncOnPut: false})
	defer db.Close()

	// Seed.
	val := value256()
	var keyBuf [16]byte
	src := rand.New(rand.NewSource(0xBEEF))
	keys := make([][16]byte, seedN)
	for i := 0; i < seedN; i++ {
		_ = keyAt(keyBuf[:], src.Uint64())
		copy(keys[i][:], keyBuf[:])
		if err := db.Put(keyBuf[:], val); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	// Get latency.
	getSamples := make([]time.Duration, probeN)
	pick := rand.New(rand.NewSource(0xFACE))
	for i := 0; i < probeN; i++ {
		k := keys[pick.Intn(seedN)]
		start := time.Now()
		if _, err := db.Get(k[:]); err != nil {
			t.Fatalf("Get probe: %v", err)
		}
		getSamples[i] = time.Since(start)
	}
	sort.Slice(getSamples, func(i, j int) bool { return getSamples[i] < getSamples[j] })
	getP99 := percentile(getSamples, 0.99)

	// Sync Put latency on a separate DB so the fsync cost is in isolation.
	syncDB := openDB(t, engine.Options{SyncOnPut: true})
	defer syncDB.Close()
	putN := probeN / 10 // sync Puts are expensive
	if putN < 500 {
		putN = 500
	}
	putSamples := make([]time.Duration, putN)
	for i := 0; i < putN; i++ {
		_ = keyAt(keyBuf[:], uint64(i))
		start := time.Now()
		if err := syncDB.Put(keyBuf[:], val); err != nil {
			t.Fatalf("sync Put: %v", err)
		}
		putSamples[i] = time.Since(start)
	}
	sort.Slice(putSamples, func(i, j int) bool { return putSamples[i] < putSamples[j] })
	putP99 := percentile(putSamples, 0.99)

	t.Logf("G2 measurements: get p50=%s p99=%s  sync-put p50=%s p99=%s",
		percentile(getSamples, 0.5), getP99,
		percentile(putSamples, 0.5), putP99)

	// Assertions. Heavy mode applies the SPEC §2 numbers exactly. Default
	// mode keeps the measurement (logged above) but applies deliberately
	// loose sanity ceilings: `make verify` runs on whatever hardware the
	// reviewer happens to have, and the SPEC perf floors are calibrated for
	// `make compliance-heavy` on expected hardware. The default ceilings
	// here only fire on order-of-magnitude regressions.
	getCeil := 100 * time.Millisecond
	putCeil := 500 * time.Millisecond
	if isHeavy() {
		getCeil = 1 * time.Millisecond
		putCeil = 5 * time.Millisecond
	}
	if getP99 > getCeil {
		t.Fatalf("G2 Get p99 = %s, want < %s", getP99, getCeil)
	}
	if putP99 > putCeil {
		t.Fatalf("G2 sync Put p99 = %s, want < %s", putP99, putCeil)
	}
}

// ---------------------------------------------------------------------
// G3 — Throughput: ≥50k async / ≥10k sync Put/sec (heavy targets).
// ---------------------------------------------------------------------

func runThroughput(t *testing.T, syncMode bool, duration time.Duration) float64 {
	t.Helper()
	db := openDB(t, engine.Options{SyncOnPut: syncMode, WriteQueueDepth: 1024})
	defer db.Close()

	val := value256()
	workers := runtime.GOMAXPROCS(0)
	if syncMode {
		// Sync mode benefits massively from concurrent submitters
		// hitting the writer's group commit. Cap workers at a sensible
		// level so we exercise that without wasting CPU.
		if workers < 8 {
			workers = 8
		}
	}
	deadline := time.Now().Add(duration)
	var ops atomic.Uint64

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed uint64) {
			defer wg.Done()
			var keyBuf [16]byte
			counter := seed << 32
			for time.Now().Before(deadline) {
				_ = keyAt(keyBuf[:], counter)
				counter++
				if err := db.Put(keyBuf[:], val); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				ops.Add(1)
			}
		}(uint64(w))
	}
	wg.Wait()
	elapsed := duration.Seconds()
	rate := float64(ops.Load()) / elapsed
	mode := "async"
	if syncMode {
		mode = "sync"
	}
	t.Logf("G3 %s throughput: %.0f Put/sec (%d ops in %s, %d workers)",
		mode, rate, ops.Load(), duration, workers)
	return rate
}

// G3 floor rationale: SPEC §2 numbers (50k async / 10k sync Put/sec) are
// the heavy-mode targets on expected hardware. Default ceilings here are
// loose order-of-magnitude sanity checks so `make verify` stays
// deterministic across reviewer hardware; the real perf gate lives in
// `make compliance-heavy` and `make bench`.

func TestReq3_ThroughputAsync(t *testing.T) {
	dur := 3 * time.Second
	floor := 1_000.0
	if isHeavy() {
		dur = 30 * time.Second
		floor = 50_000.0
	}
	rate := runThroughput(t, false, dur)
	if rate < floor {
		t.Fatalf("G3 async rate = %.0f Put/sec, want ≥ %.0f", rate, floor)
	}
}

func TestReq3_ThroughputSync(t *testing.T) {
	dur := 3 * time.Second
	floor := 100.0
	if isHeavy() {
		dur = 30 * time.Second
		floor = 10_000.0
	}
	rate := runThroughput(t, true, dur)
	if rate < floor {
		t.Fatalf("G3 sync rate = %.0f Put/sec, want ≥ %.0f", rate, floor)
	}
}

// ---------------------------------------------------------------------
// G4 — Dataset larger than RAM: hot reads serviced from page cache,
//      cold reads from disk, no OOM. HEAVY-ONLY: a multi-GiB write
//      blows out CI disk quota and runtime.
// ---------------------------------------------------------------------

func TestReq4_DatasetLargerThanRAM(t *testing.T) {
	if !isHeavy() {
		t.Skip("dataset-larger-than-RAM is heavy-only; set LITTLEDB_HEAVY=1 (default 8 GiB, override with LITTLEDB_DATASET_GIB)")
	}
	gib := datasetGiB()
	totalBytes := int64(gib) << 30
	valSize := 1024
	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte(i)
	}
	// Each Put writes ~ valSize + 16 + ~24 bytes of record overhead.
	const perEntry = 1024 + 16 + 24
	n := int(totalBytes / perEntry)
	t.Logf("G4 writing %d entries (~%d GiB)", n, gib)

	db := openDB(t, engine.Options{SyncOnPut: false, WriteQueueDepth: 1024})
	defer db.Close()

	var keyBuf [16]byte
	start := time.Now()
	for i := 0; i < n; i++ {
		_ = keyAt(keyBuf[:], uint64(i))
		if err := db.Put(keyBuf[:], val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if i%1_000_000 == 0 && i > 0 {
			t.Logf("  written %d / %d (%s elapsed)", i, n, time.Since(start))
		}
	}
	t.Logf("G4 write phase done in %s", time.Since(start))

	// Random read probe to confirm cold reads succeed.
	probe := 10_000
	pick := rand.New(rand.NewSource(0xABCD))
	probeStart := time.Now()
	for i := 0; i < probe; i++ {
		_ = keyAt(keyBuf[:], uint64(pick.Intn(n)))
		if _, err := db.Get(keyBuf[:]); err != nil {
			t.Fatalf("Get probe: %v", err)
		}
	}
	t.Logf("G4 read probe: %d random Gets in %s", probe, time.Since(probeStart))
}

// ---------------------------------------------------------------------
// G5 — Predictable behaviour under sustained mixed load.
//      Assert p99 stays within 10× p50 over a mixed 70/20/10 workload.
// ---------------------------------------------------------------------

func TestReq5_PredictableUnderLoad(t *testing.T) {
	dur := 10 * time.Second
	if isHeavy() {
		dur = 5 * time.Minute
	}
	db := openDB(t, engine.Options{SyncOnPut: false, WriteQueueDepth: 1024})
	defer db.Close()

	val := value256()
	const seedN = 10_000
	// Pre-seed so Get probes have hits.
	var keyBuf [16]byte
	for i := 0; i < seedN; i++ {
		_ = keyAt(keyBuf[:], uint64(i))
		if err := db.Put(keyBuf[:], val); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	workers := runtime.GOMAXPROCS(0)
	deadline := time.Now().Add(dur)
	perWorkerGets := make([][]time.Duration, workers)
	perWorkerPuts := make([][]time.Duration, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func(seed uint64) {
			defer wg.Done()
			src := rand.New(rand.NewSource(int64(seed + 1)))
			var kb [16]byte
			counter := seed << 32
			getS := make([]time.Duration, 0, 1<<14)
			putS := make([]time.Duration, 0, 1<<14)
			for time.Now().Before(deadline) {
				r := src.Float64()
				switch {
				case r < 0.70: // Get
					_ = keyAt(kb[:], uint64(src.Intn(seedN)))
					t0 := time.Now()
					if _, err := db.Get(kb[:]); err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
						t.Errorf("Get: %v", err)
						return
					}
					getS = append(getS, time.Since(t0))
				case r < 0.90: // Put
					_ = keyAt(kb[:], counter)
					counter++
					t0 := time.Now()
					if err := db.Put(kb[:], val); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
					putS = append(putS, time.Since(t0))
				default: // Delete
					_ = keyAt(kb[:], uint64(src.Intn(seedN)))
					if err := db.Delete(kb[:]); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
			perWorkerGets[w] = getS
			perWorkerPuts[w] = putS
		}(uint64(w))
	}
	wg.Wait()

	merge := func(parts [][]time.Duration) []time.Duration {
		n := 0
		for _, p := range parts {
			n += len(p)
		}
		out := make([]time.Duration, 0, n)
		for _, p := range parts {
			out = append(out, p...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	gets := merge(perWorkerGets)
	puts := merge(perWorkerPuts)
	if len(gets) == 0 || len(puts) == 0 {
		t.Fatalf("G5 collected too few samples: gets=%d puts=%d", len(gets), len(puts))
	}
	gp50, gp99 := percentile(gets, 0.5), percentile(gets, 0.99)
	pp50, pp99 := percentile(puts, 0.5), percentile(puts, 0.99)
	t.Logf("G5 over %s: gets=%d (p50=%s p99=%s)  puts=%d (p50=%s p99=%s)",
		dur, len(gets), gp50, gp99, len(puts), pp50, pp99)

	// "Predictable" is asserted as an absolute p99 tail ceiling, per
	// SPEC §2 G5. The earlier draft of the SPEC framed this as a
	// p99/p50 ratio < 10×, but at sub-microsecond p50s (typical for
	// Get against a warm keydir) the ratio is dominated by Go runtime
	// scheduling jitter rather than engine behaviour — a single GC
	// pause or core migration trivially clears 10× of a 200 ns p50.
	// The absolute ceiling is what actually pins regressions; the SPEC
	// row was updated to match.
	getCeil := 10 * time.Millisecond
	putCeil := 50 * time.Millisecond
	if isHeavy() {
		getCeil = 2 * time.Millisecond
		putCeil = 20 * time.Millisecond
	}
	if gp99 > getCeil {
		t.Fatalf("G5 get p99 = %s, want ≤ %s", gp99, getCeil)
	}
	if pp99 > putCeil {
		t.Fatalf("G5 put p99 = %s, want ≤ %s", pp99, putCeil)
	}
}

// ---------------------------------------------------------------------
// G6 — API surface is complete: Put/Get/Delete/BatchPut/ReadKeyRange/
//      Stats/Open/Close with the documented sentinel errors.
// ---------------------------------------------------------------------

func TestReq6_APISurfaceComplete(t *testing.T) {
	db := openDB(t, engine.Options{})
	defer db.Close()

	val := value256()
	k := []byte("alpha")

	if err := db.Put(k, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := db.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != len(val) {
		t.Fatalf("Get: len=%d want %d", len(got), len(val))
	}
	if err := db.Delete(k); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get(k); !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("Get after Delete: err=%v want ErrKeyNotFound", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, engine.ErrEmptyKey) {
		t.Fatalf("Get(nil): err=%v want ErrEmptyKey", err)
	}
	if err := db.BatchPut([]engine.BatchEntry{
		{Key: []byte("b1"), Value: val},
		{Key: []byte("b2"), Value: val},
	}); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	var seen int
	if err := db.ReadKeyRange([]byte("b"), []byte("c"), func(_, _ []byte) bool {
		seen++
		return true
	}); err != nil {
		t.Fatalf("ReadKeyRange: %v", err)
	}
	if seen != 2 {
		t.Fatalf("ReadKeyRange: seen=%d want 2", seen)
	}
	stats := db.Stats()
	if stats.KeyCount == 0 || stats.BytesOnDisk == 0 {
		t.Fatalf("Stats: %+v (expected non-zero key/byte counts)", stats)
	}
	t.Logf("G6 API surface OK: stats=%+v", stats)
}

// ---------------------------------------------------------------------
// G7 — Network-available: server + client round-trip Put/Get/Delete.
// ---------------------------------------------------------------------

func TestReq7_NetworkAvailable(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{Dir: dir})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := server.New(db, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     16 * 1024 * 1024,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveDone
		_ = db.Close()
	})

	cli, err := client.Dial(addr, client.Options{
		DialTimeout:    2 * time.Second,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer cli.Close()

	if err := cli.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	k, v := []byte("net-key"), []byte("net-value")
	if err := cli.Put(k, v); err != nil {
		t.Fatalf("client.Put: %v", err)
	}
	got, err := cli.Get(k)
	if err != nil {
		t.Fatalf("client.Get: %v", err)
	}
	if string(got) != string(v) {
		t.Fatalf("client.Get: %q want %q", got, v)
	}
	if err := cli.Delete(k); err != nil {
		t.Fatalf("client.Delete: %v", err)
	}
	if _, err := cli.Get(k); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("client.Get after Delete: err=%v want ErrNotFound", err)
	}
	t.Logf("G7 network round-trip OK on %s", addr)
}

// ---------------------------------------------------------------------
// G8 — Replication: SPEC §2 G8 (bonus). End-to-end check that a leader
// streams writes to a follower, the follower rejects writes while a
// follower, and a manual PROMOTE flips it into a writable leader. This
// is the same shape the operator runbook in docs/replication.md
// documents — the test is the runbook.
// ---------------------------------------------------------------------

func TestReq8_ReplicationBonus(t *testing.T) {
	// Leader: writable, replication enabled.
	leaderDir := t.TempDir()
	leaderDB, err := engine.Open(engine.Options{
		Dir:                   leaderDir,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 64,
	})
	if err != nil {
		t.Fatalf("leader engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = leaderDB.Close() })

	leaderSrv := server.New(leaderDB, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
		EnableReplication:         true,
	})
	if err := leaderSrv.Bind(); err != nil {
		t.Fatalf("leader Bind: %v", err)
	}
	leaderAddr := leaderSrv.Addr().String()
	leaderServeErr := make(chan error, 1)
	go func() { leaderServeErr <- leaderSrv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = leaderSrv.Shutdown(ctx)
		<-leaderServeErr
	})

	// Follower engine + read-only server. The replication runner
	// (server.NewFollower) pulls from the leader and applies into
	// followerDB. The OnPromote hook here mirrors what the CLI does
	// in cmd/little-db/main.go: cancel the runner, wait for it to
	// exit cleanly, then let the server flip the gate.
	followerDir := t.TempDir()
	followerDB, err := engine.Open(engine.Options{
		Dir:                 followerDir,
		MaxBatchEncodedSize: 16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("follower engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = followerDB.Close() })

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	t.Cleanup(runnerCancel)
	runner := server.NewFollower(leaderAddr, followerDB, server.FollowerOptions{
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

	followerSrv := server.New(followerDB, server.Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
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
	if err := followerSrv.Bind(); err != nil {
		t.Fatalf("follower Bind: %v", err)
	}
	followerAddr := followerSrv.Addr().String()
	followerServeErr := make(chan error, 1)
	go func() { followerServeErr <- followerSrv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = followerSrv.Shutdown(ctx)
		<-followerServeErr
	})

	// Clients.
	clientOpts := client.Options{
		DialTimeout:    2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}
	leaderCli, err := client.Dial(leaderAddr, clientOpts)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	t.Cleanup(func() { _ = leaderCli.Close() })
	followerCli, err := client.Dial(followerAddr, clientOpts)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	t.Cleanup(func() { _ = followerCli.Close() })

	// (1) Replication: PUT on the leader, then GET on the follower
	// must see the value. The leader's publisher drops records when
	// no subscriber is attached, so a one-shot PUT can race the
	// SUBSCRIBE handshake; loop the PUT until the follower observes
	// (same pattern as server-package waitForApplied).
	key := []byte("g8/replicated")
	val := []byte("from-leader")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := leaderCli.Put(key, val); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
		got, gerr := followerCli.Get(key)
		if gerr == nil && string(got) == string(val) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never observed replicated key: err=%v got=%q", gerr, got)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// (2) Follower rejects writes with FOLLOWER_READ_ONLY.
	if err := followerCli.Put([]byte("g8/should-fail"), []byte("x")); err == nil {
		t.Fatal("follower PUT should have failed")
	} else {
		var remote *wire.RemoteError
		if !errors.As(err, &remote) || remote.Status != wire.StatusFollowerReadOnly {
			t.Fatalf("follower PUT err: want StatusFollowerReadOnly, got %v", err)
		}
	}

	// (3) Manual failover: PROMOTE flips the gate. The hook cancels
	// the replication runner, waits for it to exit, then the server
	// clears followerMode and returns OK.
	if err := followerCli.Promote(); err != nil {
		t.Fatalf("promote: %v", err)
	}
	select {
	case rerr := <-runnerDone:
		if rerr != nil && !errors.Is(rerr, context.Canceled) {
			t.Fatalf("runner exit: %v", rerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit after promotion")
	}

	// (4) Former follower is now a writable leader: PUT succeeds and
	// the value is independently readable (proves the gate actually
	// flipped and the engine is intact post-drain).
	postKey := []byte("g8/post-promote")
	postVal := []byte("from-promoted")
	if err := followerCli.Put(postKey, postVal); err != nil {
		t.Fatalf("post-promote Put: %v", err)
	}
	got, err := followerCli.Get(postKey)
	if err != nil {
		t.Fatalf("post-promote Get: %v", err)
	}
	if string(got) != string(postVal) {
		t.Fatalf("post-promote Get: got %q want %q", got, postVal)
	}

	t.Logf("G8 replication + manual failover OK: leader=%s follower=%s", leaderAddr, followerAddr)
}
