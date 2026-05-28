package engine

// Throughput micro-benchmarks for the engine. These exercise the hot
// paths the SPEC §2 goals call out: G2 (per-op latency, indirectly via
// ns/op), G3 (random-write throughput), and the O(N) keydir-walk risk
// in §16 (ReadKeyRange).
//
// All benchmarks use 256-byte values per §2 G3. Keys are 16-byte hex
// strings derived from a deterministic math/rand source so runs are
// reproducible. Each benchmark opens a fresh DB under b.TempDir() and
// drives it directly — no network, no client, no server — so the
// numbers measure the engine alone.
//
// The benchmarks are NOT run by `go test ./...`. Use:
//
//	go test ./internal/engine -bench=. -benchmem -benchtime=3s
//
// to surface them. The Makefile target `make bench` wraps that.

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

const benchValueSize = 256 // §2 G3

// benchKey writes a 16-byte hex-style identifier for index i into buf and
// returns buf[:16]. Avoids fmt allocations on the hot path.
func benchKey(buf []byte, i uint64) []byte {
	const hexDigits = "0123456789abcdef"
	_ = buf[15]
	for j := 0; j < 16; j++ {
		buf[15-j] = hexDigits[i&0xF]
		i >>= 4
	}
	return buf[:16]
}

// openBenchDB opens a fresh DB under b.TempDir() with reasonable
// defaults. sync controls SyncOnPut. Returns the DB and a 256-byte
// value buffer the caller can reuse.
func openBenchDB(b *testing.B, sync bool) (*DB, []byte) {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(Options{
		Dir:                dir,
		SyncOnPut:          sync,
		WriteQueueDepth:    256,
		CompactionInterval: 0, // explicit: no background compaction during bench
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	val := make([]byte, benchValueSize)
	for i := range val {
		val[i] = byte(i)
	}
	return db, val
}

// seedDB pre-populates a DB with n random-key entries. Returns the
// 16-byte key buffer (the last key written; not retained).
func seedDB(b *testing.B, db *DB, n int, val []byte) {
	b.Helper()
	src := rand.New(rand.NewSource(0xBEEF))
	var keyBuf [16]byte
	for i := 0; i < n; i++ {
		_ = benchKey(keyBuf[:], src.Uint64())
		if err := db.Put(keyBuf[:], val); err != nil {
			b.Fatalf("seed Put: %v", err)
		}
	}
}

// ---------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------

func BenchmarkPutSequential(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := "async"
		if sync {
			name = "sync"
		}
		b.Run(name, func(b *testing.B) {
			db, val := openBenchDB(b, sync)
			var keyBuf [16]byte
			b.ReportAllocs()
			b.SetBytes(int64(len(val) + 16))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = benchKey(keyBuf[:], uint64(i))
				if err := db.Put(keyBuf[:], val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

func BenchmarkPutRandomKeys(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := "async"
		if sync {
			name = "sync"
		}
		b.Run(name, func(b *testing.B) {
			db, val := openBenchDB(b, sync)
			src := rand.New(rand.NewSource(0xC0FFEE))
			var keyBuf [16]byte
			b.ReportAllocs()
			b.SetBytes(int64(len(val) + 16))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = benchKey(keyBuf[:], src.Uint64())
				if err := db.Put(keyBuf[:], val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------

// BenchmarkGetRandomKeys pre-seeds a warm dataset and measures random
// Get latency. The seed size (256 KiB-resident keydir at 100k keys) is
// kept modest so the bench is tractable in CI; the latency-class
// benchmark in bench_latency_test.go covers the SPEC §2 G2 1M-key shape.
func BenchmarkGetRandomKeys(b *testing.B) {
	const seedSize = 100_000
	db, val := openBenchDB(b, false)
	seedDB(b, db, seedSize, val)

	// Build a slice of every key we wrote so we can index in O(1).
	src := rand.New(rand.NewSource(0xBEEF))
	keys := make([][16]byte, seedSize)
	for i := range keys {
		_ = benchKey(keys[i][:], src.Uint64())
	}
	pick := rand.New(rand.NewSource(0xFACE))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[pick.Intn(seedSize)]
		got, err := db.Get(k[:])
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if len(got) != benchValueSize {
			b.Fatalf("len(got) = %d want %d", len(got), benchValueSize)
		}
	}
}

// ---------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------

func BenchmarkDeleteRandomKeys(b *testing.B) {
	db, val := openBenchDB(b, false)
	// Seed enough keys for b.N — overshoot slightly so we never run
	// out. Cap at 2M to bound test time.
	seed := b.N + 1024
	if seed > 2_000_000 {
		seed = 2_000_000
	}
	seedDB(b, db, seed, val)

	src := rand.New(rand.NewSource(0xBEEF))
	keys := make([][16]byte, seed)
	for i := range keys {
		_ = benchKey(keys[i][:], src.Uint64())
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Delete(keys[i%seed][:]); err != nil {
			b.Fatalf("Delete: %v", err)
		}
	}
}

// ---------------------------------------------------------------------
// BatchPut
// ---------------------------------------------------------------------

// BenchmarkBatchPut sweeps batch sizes to expose the per-entry vs
// per-batch fixed-cost tradeoff. ns/op is per-batch; divide by batch
// size for per-entry cost.
func BenchmarkBatchPut(b *testing.B) {
	for _, batchSize := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("size=%d", batchSize), func(b *testing.B) {
			db, val := openBenchDB(b, false)
			src := rand.New(rand.NewSource(0xBEEF))
			entries := make([]BatchEntry, batchSize)
			keyBufs := make([][16]byte, batchSize)
			for i := range entries {
				_ = benchKey(keyBufs[i][:], src.Uint64())
				entries[i] = BatchEntry{Key: keyBufs[i][:], Value: val}
			}
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * (len(val) + 16)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Rotate the first key per iteration so each batch
				// touches a distinct keydir slot rather than rewriting
				// the same key b.N times.
				binary.BigEndian.PutUint64(keyBufs[0][:8], uint64(i))
				if err := db.BatchPut(entries); err != nil {
					b.Fatalf("BatchPut: %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------
// ReadKeyRange
// ---------------------------------------------------------------------

// BenchmarkReadKeyRange measures the cost of an unbounded scan over a
// pre-seeded dataset. SPEC §16 flags ReadKeyRange's O(N) keydir walk as
// a known scaling risk; this benchmark documents the cost so a
// regression (or the future skiplist upgrade) shows up in CI numbers.
func BenchmarkReadKeyRange(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, val := openBenchDB(b, false)
			seedDB(b, db, n, val)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var seen int
				err := db.ReadKeyRange(nil, nil, func(_, _ []byte) bool {
					seen++
					return true
				})
				if err != nil {
					b.Fatalf("ReadKeyRange: %v", err)
				}
				if seen != n {
					b.Fatalf("ReadKeyRange yielded %d pairs, want %d", seen, n)
				}
			}
		})
	}
}
