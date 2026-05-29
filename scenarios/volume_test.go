//go:build replication_demo && replication_demo_heavy

package scenarios

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"little-db/internal/client"
	"little-db/internal/engine"
)

// encodeKey produces an 8-byte big-endian key from i, so the keyspace
// is dense, sortable, and trivially regeneratable across processes.
func encodeKey(i uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, i)
	return k
}

// fillValue produces a deterministic 256-byte value derived from i so
// that a single key→value check confirms the exact byte sequence the
// leader wrote without storing 256 MiB of values in test RAM.
func fillValue(i uint64) []byte {
	v := make([]byte, 256)
	binary.BigEndian.PutUint64(v[:8], i)
	for j := 8; j < 256; j++ {
		v[j] = byte((i + uint64(j)) * 2654435761)
	}
	return v
}

func hashAll(t *testing.T, db *engine.DB, n uint64) string {
	t.Helper()
	h := sha256.New()
	buf := make([]byte, 0, 264)
	for i := uint64(0); i < n; i++ {
		k := encodeKey(i)
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		buf = append(buf[:0], k...)
		buf = append(buf, v...)
		_, _ = h.Write(buf)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// waitForKeyCount polls until follower's KeyCount matches want or
// timeout. Reports lag each second so a long run is not silent.
func waitForKeyCount(t *testing.T, db *engine.DB, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		got := db.Stats().KeyCount
		if got >= want {
			return
		}
		if time.Since(lastLog) > 1*time.Second {
			t.Logf("  ... follower=%d / want=%d (lag=%d, %.1f%%)",
				got, want, want-got, 100*float64(got)/float64(want))
			lastLog = time.Now()
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: follower=%d want=%d", got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForValue(t *testing.T, db *engine.DB, key, want []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		got, err := db.Get(key)
		if err == nil && bytes.Equal(got, want) {
			return
		}
		if time.Since(lastLog) > time.Second {
			t.Logf("  ... waiting for key %x: err=%v len(got)=%d", key, err, len(got))
			lastLog = time.Now()
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for key %x: err=%v got_len=%d", key, err, len(got))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func assertNoReplicationDrops(t *testing.T, db *engine.DB) {
	t.Helper()
	if dropped := db.Stats().ReplicationLagDropped; dropped != 0 {
		t.Fatalf("leader dropped %d replication records; volume scenario overloaded the v0.1 stream", dropped)
	}
}

// memMB returns alloc megabytes.
func memMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024 * 1024)
}

// runVolume is the shared driver: writes n 256B records on the leader,
// asserts replication parity, then runs a sampled byte-equality check.
func runVolume(t *testing.T, n uint64) {
	leaderDB := openLeaderDB(t, 64*1024*1024) // 64 MiB segments
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)

	t.Logf("== volume: %d records, 256B values (~%d MiB on disk per side)", n, n/4096)

	putUntilApplied(t, cli, follower.db, encodeKey(0), fillValue(0))

	start := time.Now()
	logEvery := uint64(50_000)
	if n >= 1_000_000 {
		logEvery = 100_000
	}
	waitEvery := uint64(10_000)
	for i := uint64(1); i < n; i++ {
		if err := cli.Put(encodeKey(i), fillValue(i)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if (i+1)%waitEvery == 0 || i+1 == n {
			waitForValue(t, follower.db, encodeKey(i), fillValue(i), 30*time.Second)
			assertNoReplicationDrops(t, leaderDB)
		}
		if (i+1)%logEvery == 0 {
			elapsed := time.Since(start)
			rps := float64(i+1) / elapsed.Seconds()
			t.Logf("  wrote %d/%d (%.0f put/s, mem=%.0f MiB)",
				i+1, n, rps, memMB())
		}
	}
	writeDur := time.Since(start)
	t.Logf("== writes done in %v (%.0f put/s)", writeDur, float64(n)/writeDur.Seconds())

	assertNoReplicationDrops(t, leaderDB)
	t.Logf("== follower caught up; KeyCount leader=%d follower=%d",
		leaderDB.Stats().KeyCount, follower.db.Stats().KeyCount)

	if leaderDB.Stats().KeyCount != follower.db.Stats().KeyCount {
		t.Fatalf("KeyCount drift: leader=%d follower=%d",
			leaderDB.Stats().KeyCount, follower.db.Stats().KeyCount)
	}

	// Sampled byte equality: 1000 random keys must round-trip identically.
	rng := rand.New(rand.NewSource(1))
	samples := 1000
	if samples > int(n) {
		samples = int(n)
	}
	for s := 0; s < samples; s++ {
		i := uint64(rng.Int63n(int64(n)))
		k := encodeKey(i)
		want := fillValue(i)
		got, err := follower.db.Get(k)
		if err != nil {
			t.Fatalf("follower Get sample %d (i=%d): %v", s, i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("sample mismatch i=%d: lengths follower=%d want=%d",
				i, len(got), len(want))
		}
	}
	t.Logf("OK: %d records replicated, %d-sample byte-equality OK", n, samples)
}

func parseVolumeCounts(t *testing.T, raw string) []uint64 {
	t.Helper()
	var out []uint64
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil || n == 0 {
			t.Fatalf("invalid RUN_VOLUME_COUNTS entry %q", part)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		t.Fatal("RUN_VOLUME_COUNTS did not contain any counts")
	}
	return out
}

// V0: caller-selected record counts, useful for 5M or quick smoke runs.
func TestVolume_CustomRecordCounts(t *testing.T) {
	raw := os.Getenv("RUN_VOLUME_COUNTS")
	if raw == "" {
		t.Skip("set RUN_VOLUME_COUNTS=1000000,5000000,10000000 to run custom volume scenarios")
	}
	for _, n := range parseVolumeCounts(t, raw) {
		t.Run(strconv.FormatUint(n, 10), func(t *testing.T) {
			runVolume(t, n)
		})
	}
}

// V1: 1M PUTs.
func TestVolume_OneMillion_PUT(t *testing.T) {
	runVolume(t, 1_000_000)
}

// V2: 1M PUTs interleaved with 100K DELETEs.
func TestVolume_OneMillion_PUT_with_DELETE(t *testing.T) {
	const n = 1_000_000
	const dels = 100_000

	leaderDB := openLeaderDB(t, 64*1024*1024)
	_, leaderAddr := startLeaderServer(t, leaderDB)
	follower := startFollower(t, leaderAddr)
	cli := dial(t, leaderAddr)

	// Deterministic schedule: every 5th key (0,5,10,...) is deleted
	// later in the run via the same iteration.
	t.Logf("== volume: %d PUT + %d DELETE", n, dels)
	putUntilApplied(t, cli, follower.db, encodeKey(0), fillValue(0))

	start := time.Now()
	delPlanned := uint64(0)
	waitEvery := uint64(10_000)
	for i := uint64(1); i < n; i++ {
		if err := cli.Put(encodeKey(i), fillValue(i)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		// Stagger deletes ~halfway through so the keydir has lived a bit.
		if i > n/2 && i%5 == 0 && delPlanned < dels {
			delKey := encodeKey(i - n/2)
			if err := cli.Delete(delKey); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			delPlanned++
		}
		if (i+1)%waitEvery == 0 || i+1 == n {
			waitForValue(t, follower.db, encodeKey(i), fillValue(i), 30*time.Second)
			assertNoReplicationDrops(t, leaderDB)
		}
		if (i+1)%100_000 == 0 {
			t.Logf("  wrote %d/%d (deletes %d/%d)", i+1, n, delPlanned, dels)
		}
	}
	writeDur := time.Since(start)
	wantKeys := uint64(n) - delPlanned
	t.Logf("== writes done in %v; expecting %d live keys (after %d deletes)",
		writeDur, wantKeys, delPlanned)

	waitForValue(t, follower.db, encodeKey(n-1), fillValue(n-1), 30*time.Second)
	assertNoReplicationDrops(t, leaderDB)

	if leaderDB.Stats().KeyCount != follower.db.Stats().KeyCount {
		t.Fatalf("KeyCount drift: leader=%d follower=%d",
			leaderDB.Stats().KeyCount, follower.db.Stats().KeyCount)
	}

	// Spot-check tombstoned keys on follower.
	// Deleted set is exactly {encodeKey(5), encodeKey(10), ..., encodeKey(5*delPlanned)}
	// because the writer deleted encodeKey(i - n/2) at writer-iter
	// i = n/2 + 5k for k = 1..delPlanned, and n/2 is a multiple of 5.
	deletedCheck := 0
	for k := uint64(1); k <= 100 && k <= delPlanned; k++ {
		delKey := encodeKey(5 * k)
		_, err := follower.db.Get(delKey)
		if !errors.Is(err, engine.ErrKeyNotFound) {
			t.Fatalf("deleted key %d still present on follower: err=%v", 5*k, err)
		}
		deletedCheck++
	}
	if deletedCheck == 0 {
		t.Fatal("no deleted keys to check (delPlanned was zero)")
	}

	// Spot-check live keys.
	rng := rand.New(rand.NewSource(2))
	live := 0
	for live < 500 {
		i := uint64(rng.Int63n(int64(n)))
		// Skip the deleted set.
		if i > 0 && i%5 == 0 && i/5 <= delPlanned {
			continue
		}
		got, err := follower.db.Get(encodeKey(i))
		if err != nil {
			t.Fatalf("live key %d Get: %v", i, err)
		}
		if !bytes.Equal(got, fillValue(i)) {
			t.Fatalf("live key %d mismatch", i)
		}
		live++
	}
	t.Logf("OK: %d live keys replicated; %d tombstones + %d live samples verified",
		wantKeys, deletedCheck, live)
}

// V3: 10M PUTs. Opt-in via RUN_10M=1 because it takes minutes and ~2.5 GB
// disk per side; otherwise skipped to keep `replication_demo_heavy` runs
// reasonable.
func TestVolume_TenMillion_PUT(t *testing.T) {
	if os.Getenv("RUN_10M") == "" {
		t.Skip("set RUN_10M=1 to run; uses ~2.5 GB disk per side, takes minutes")
	}
	runVolume(t, 10_000_000)
}

// Quick guard for an unused import in some build matrices.
var _ = context.Background
var _ = client.ErrNotFound
var _ = hashAll
