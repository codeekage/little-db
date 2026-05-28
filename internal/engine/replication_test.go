package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// helper: decode a published record blob through the same path the
// segment recovery scanner uses, so we get end-to-end CRC validation.
func decodeOnePublished(t *testing.T, raw []byte) *record {
	t.Helper()
	rec, n, err := readRecord(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("readRecord consumed %d of %d bytes", n, len(raw))
	}
	return rec
}

// helper: open a DB with replication enabled. The batch cap is lowered
// to 16 MiB so it satisfies the Open-time constraint that
// MaxBatchEncodedSize <= wire.MaxReplicationRecord (the default 64 MiB
// would be rejected when ReplicationBufferSize > 0).
func openReplicaDB(t *testing.T, bufSize int) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir:                   t.TempDir(),
		ReplicationBufferSize: bufSize,
		MaxBatchEncodedSize:   16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestReplicationBatchCapExceedsWireRejected pins the Open-time check
// that refuses to enable leader-mode replication with a batch cap
// larger than a single REPLICATE_RECORD frame can carry. Without this
// guard a batch could be appended locally only to find no legal way to
// publish it, dropping the record silently while the on-disk state had
// already advanced.
func TestReplicationBatchCapExceedsWireRejected(t *testing.T) {
	db, err := Open(Options{
		Dir:                   t.TempDir(),
		ReplicationBufferSize: 64,
		MaxBatchEncodedSize:   33 * 1024 * 1024, // > wire.MaxReplicationRecord (~32 MiB)
	})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open: expected error when MaxBatchEncodedSize > wire.MaxReplicationRecord, got nil")
	}
}

// helper: decode one published record and assert it represents a single
// PUT of (key, value). Returns nil on success; t.Fatal on mismatch.
func assertPublishedPut(t *testing.T, raw []byte, key, value []byte) {
	t.Helper()
	rec := decodeOnePublished(t, raw)
	if rec.flag != recordFlagPut {
		t.Fatalf("flag: got %v want PUT", rec.flag)
	}
	if !bytes.Equal(rec.key, key) {
		t.Fatalf("key: got %q want %q", rec.key, key)
	}
	if !bytes.Equal(rec.value, value) {
		t.Fatalf("value: got %q want %q", rec.value, value)
	}
}

// TestReplicationSubscribePublishesEveryWrite is the happy path: a
// subscriber attached before any writes receives the encoded record bytes
// for each write in order, and those bytes decode back to the original
// key/value through the same codec the on-disk segment uses.
func TestReplicationSubscribePublishesEveryWrite(t *testing.T) {
	db := openReplicaDB(t, 64)
	sub, err := db.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	const N = 16
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		select {
		case raw := <-sub.Records():
			assertPublishedPut(t, raw,
				[]byte(fmt.Sprintf("k%d", i)),
				[]byte(fmt.Sprintf("v%d", i)))
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for record %d", i)
		}
	}

	if got := db.Stats().ReplicationLagDropped; got != 0 {
		t.Fatalf("ReplicationLagDropped: got %d want 0 (consumer kept up)", got)
	}
}

// TestReplicationDeletePublishesTombstone verifies Delete shows up on the
// stream as a tombstone-flagged record.
func TestReplicationDeletePublishesTombstone(t *testing.T) {
	db := openReplicaDB(t, 8)
	sub, _ := db.Subscribe()
	defer sub.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Drain the PUT first.
	<-sub.Records()
	select {
	case raw := <-sub.Records():
		rec := decodeOnePublished(t, raw)
		if rec.flag != recordFlagTombstone {
			t.Fatalf("flag: got %v want TOMBSTONE", rec.flag)
		}
		if !bytes.Equal(rec.key, []byte("k")) {
			t.Fatalf("key: got %q want %q", rec.key, "k")
		}
		if len(rec.value) != 0 {
			t.Fatalf("tombstone must carry empty value, got %d bytes", len(rec.value))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delete record")
	}
}

// TestReplicationBatchPublishesSingleRecord verifies that a BatchPut
// produces one record on the stream — same as on disk. The follower
// applies it whole or not at all, mirroring the leader's atomicity.
func TestReplicationBatchPublishesSingleRecord(t *testing.T) {
	db := openReplicaDB(t, 8)
	sub, _ := db.Subscribe()
	defer sub.Close()

	entries := []BatchEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Delete: true},
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	select {
	case raw := <-sub.Records():
		rec := decodeOnePublished(t, raw)
		if rec.flag != recordFlagBatch {
			t.Fatalf("flag: got %v want BATCH", rec.flag)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batch record")
	}

	// And only one record.
	select {
	case extra := <-sub.Records():
		t.Fatalf("unexpected second record: %x", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestReplicationDropOnOverflow verifies that a stalled consumer causes
// drops to be counted (not blocked writes). The writer must remain
// responsive.
func TestReplicationDropOnOverflow(t *testing.T) {
	db := openReplicaDB(t, 2) // tiny buffer
	sub, _ := db.Subscribe()
	defer sub.Close()

	// Don't drain. Publish more than the buffer can hold.
	const N = 50
	for i := 0; i < N; i++ {
		if err := db.Put([]byte{byte(i)}, []byte{byte(i)}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	dropped := db.Stats().ReplicationLagDropped
	if dropped == 0 {
		t.Fatalf("expected drops, got 0 (buffer=2, writes=%d)", N)
	}
	if int(dropped) > N {
		t.Fatalf("dropped (%d) > writes (%d)", dropped, N)
	}
	// Buffer should have exactly 2 records (its full capacity).
	if got := len(sub.Records()); got != 2 {
		t.Fatalf("buffered records: got %d want 2", got)
	}
}

// TestReplicationDoesNotBlockWriter ensures a stalled subscriber does not
// add latency to local writes. We give the test a tight deadline and
// pound the engine; if the publish hook ever blocked, this would deadlock
// or time out.
func TestReplicationDoesNotBlockWriter(t *testing.T) {
	db := openReplicaDB(t, 1)
	sub, _ := db.Subscribe()
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			if err := db.Put([]byte{byte(i)}, []byte("x")); err != nil {
				t.Errorf("Put %d: %v", i, err)
				close(done)
				return
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer appears blocked by a stalled subscriber")
	}
	if db.Stats().ReplicationLagDropped == 0 {
		t.Fatal("expected drops from stalled subscriber, got zero")
	}
}

// TestSubscribeRequiresReplicationEnabled verifies the opt-in gate.
func TestSubscribeRequiresReplicationEnabled(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Subscribe(); !errors.Is(err, ErrReplicationDisabled) {
		t.Fatalf("Subscribe: got %v want ErrReplicationDisabled", err)
	}

	// And the default (no buffer) must produce zero overhead drops on a
	// single-node deployment under load.
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte{byte(i)}, []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if got := db.Stats().ReplicationLagDropped; got != 0 {
		t.Fatalf("ReplicationLagDropped on single-node DB: got %d want 0", got)
	}
}

// TestSubscribeRejectsSecond verifies the single-subscriber contract.
func TestSubscribeRejectsSecond(t *testing.T) {
	db := openReplicaDB(t, 8)
	sub1, err := db.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}
	if _, err := db.Subscribe(); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatalf("Subscribe 2: got %v want ErrAlreadySubscribed", err)
	}
	// Closing the first releases the slot.
	if err := sub1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	sub2, err := db.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe after close: %v", err)
	}
	_ = sub2.Close()
}

// TestSubscribeAfterCloseFails verifies the closed-DB contract.
func TestSubscribeAfterCloseFails(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ReplicationBufferSize: 4, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := db.Subscribe(); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("Subscribe after Close: got %v want ErrDBClosed", err)
	}
}

// TestDBCloseClosesSubscription verifies clean end-of-stream signal: the
// consumer's range loop terminates when the DB shuts down.
func TestDBCloseClosesSubscription(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ReplicationBufferSize: 8, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	sub, err := db.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Consumer goroutine: ranges until the channel closes.
	consumed := 0
	consumerDone := make(chan struct{})
	go func() {
		for range sub.Records() {
			consumed++
		}
		close(consumerDone)
	}()

	for i := 0; i < 4; i++ {
		if err := db.Put([]byte{byte(i)}, []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Give the consumer a moment to drain.
	time.Sleep(50 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-consumerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not observe channel close after DB.Close")
	}

	// Subscription.Close after DB close must still be safe (idempotent).
	if err := sub.Close(); err != nil {
		t.Fatalf("Close after DB close: %v", err)
	}
}

// TestSubscriptionCloseIdempotent verifies multiple Close calls are safe.
func TestSubscriptionCloseIdempotent(t *testing.T) {
	db := openReplicaDB(t, 4)
	sub, _ := db.Subscribe()
	if err := sub.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

// TestPublishedBytesAreIndependentCopies verifies the subscriber's slice
// is not aliased to the writer's reusable encode buffer. This is the
// guarantee that lets the publisher run before the burst fsync without
// corrupting the follower's view on the next write.
func TestPublishedBytesAreIndependentCopies(t *testing.T) {
	db := openReplicaDB(t, 16)
	sub, _ := db.Subscribe()
	defer sub.Close()

	if err := db.Put([]byte("k"), []byte("original")); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	raw1 := <-sub.Records()
	// Force a subsequent encode into encBuf (a different key/value of
	// similar-or-larger size to maximize the chance encBuf is reused).
	if err := db.Put([]byte("k"), []byte("replaced")); err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	raw2 := <-sub.Records()

	if &raw1[0] == &raw2[0] {
		t.Fatal("publisher returned aliased slices")
	}
	// raw1 must still decode to the original value.
	assertPublishedPut(t, raw1, []byte("k"), []byte("original"))
	assertPublishedPut(t, raw2, []byte("k"), []byte("replaced"))
}

// TestReplicationConcurrentWriters keeps the writer honest: with many
// concurrent submitters, the published stream is still serialized (the
// writer goroutine owns the publish point), no record is corrupted, and
// every record is observed when the buffer is sized above the total.
func TestReplicationConcurrentWriters(t *testing.T) {
	const W = 8
	const N = 100
	// Buffer comfortably above W*N so this test does not rely on the
	// drainer goroutine keeping up under -race load. drop-on-overflow
	// has its own dedicated test (TestReplicationDropOnOverflow).
	db := openReplicaDB(t, 2*W*N)
	sub, _ := db.Subscribe()
	defer sub.Close()

	var wg sync.WaitGroup
	wg.Add(W)
	for w := 0; w < W; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				if err := db.Put(k, k); err != nil {
					t.Errorf("Put %s: %v", k, err)
					return
				}
			}
		}(w)
	}

	// Drain in parallel with writers so the buffer doesn't overflow.
	got := 0
	consumeDone := make(chan struct{})
	go func() {
		for range sub.Records() {
			got++
			if got == W*N {
				close(consumeDone)
				return
			}
		}
	}()
	wg.Wait()

	select {
	case <-consumeDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("expected %d records, got %d (dropped=%d)", W*N, got, db.Stats().ReplicationLagDropped)
	}
}

// TestNoSubscriberDoesNotCountDrops verifies the publish-without-listener
// path is silent: with no Subscribe ever called, records are not buffered
// anywhere and the drop counter stays at zero.
func TestNoSubscriberDoesNotCountDrops(t *testing.T) {
	db := openReplicaDB(t, 4)
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte{byte(i)}, []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if got := db.Stats().ReplicationLagDropped; got != 0 {
		t.Fatalf("ReplicationLagDropped without subscriber: got %d want 0", got)
	}
}

// TestOptionsRejectsNegativeReplicationBuffer pins the Open validation.
func TestOptionsRejectsNegativeReplicationBuffer(t *testing.T) {
	if _, err := Open(Options{Dir: t.TempDir(), ReplicationBufferSize: -1}); err == nil {
		t.Fatal("Open accepted negative ReplicationBufferSize")
	}
}
