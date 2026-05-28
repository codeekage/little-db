package engine

// Latency micro-benchmarks. Standard `go test -bench` reports only mean
// ns/op; for SPEC §2 G2 we need p50/p90/p99/p999. We collect per-op
// durations into a slice during the b.N loop, sort once at the end, and
// report the quantiles via b.ReportMetric so they appear in the standard
// benchmark output.
//
// These benchmarks intentionally do not call b.SetBytes — the headline
// number is latency, not throughput.

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

const latencySeedSize = 100_000

// reportLatency sorts samples in place and surfaces p50/p90/p99/p999
// (in microseconds) via b.ReportMetric so they show up in -bench output.
func reportLatency(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	q := func(p float64) float64 {
		idx := int(float64(len(samples)-1) * p)
		return float64(samples[idx]) / float64(time.Microsecond)
	}
	b.ReportMetric(q(0.50), "p50-us")
	b.ReportMetric(q(0.90), "p90-us")
	b.ReportMetric(q(0.99), "p99-us")
	b.ReportMetric(q(0.999), "p999-us")
}

func BenchmarkLatencyGet(b *testing.B) {
	db, val := openBenchDB(b, false)
	seedDB(b, db, latencySeedSize, val)

	src := rand.New(rand.NewSource(0xBEEF))
	keys := make([][16]byte, latencySeedSize)
	for i := range keys {
		_ = benchKey(keys[i][:], src.Uint64())
	}
	pick := rand.New(rand.NewSource(0xFACE))
	samples := make([]time.Duration, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[pick.Intn(latencySeedSize)]
		start := time.Now()
		got, err := db.Get(k[:])
		samples[i] = time.Since(start)
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		_ = got
	}
	b.StopTimer()
	reportLatency(b, samples)
}

// BenchmarkLatencyPutAsync measures single-threaded async Put latency.
// Group-commit savings only materialise under concurrent submitters;
// see BenchmarkLatencyPutSyncParallel for that shape.
func BenchmarkLatencyPutAsync(b *testing.B) {
	db, val := openBenchDB(b, false)
	src := rand.New(rand.NewSource(0xC0FFEE))
	samples := make([]time.Duration, b.N)
	var keyBuf [16]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchKey(keyBuf[:], src.Uint64())
		start := time.Now()
		err := db.Put(keyBuf[:], val)
		samples[i] = time.Since(start)
		if err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	b.StopTimer()
	reportLatency(b, samples)
}

// BenchmarkLatencyPutSync measures single-threaded sync Put latency.
// Each call pays a full fsync (F_FULLFSYNC on Darwin), so this number
// reflects raw fsync cost on the test disk.
func BenchmarkLatencyPutSync(b *testing.B) {
	db, val := openBenchDB(b, true)
	src := rand.New(rand.NewSource(0xC0FFEE))
	samples := make([]time.Duration, b.N)
	var keyBuf [16]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchKey(keyBuf[:], src.Uint64())
		start := time.Now()
		err := db.Put(keyBuf[:], val)
		samples[i] = time.Since(start)
		if err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	b.StopTimer()
	reportLatency(b, samples)
}
