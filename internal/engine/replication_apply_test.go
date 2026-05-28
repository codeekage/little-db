package engine

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
)

// captureLeaderRecord runs fn against a fresh leader DB (replication
// enabled, single subscriber) and returns the raw record bytes the
// publisher emitted, plus the keydir snapshot the leader ended with.
//
// fn must do exactly one engine call that publishes one record (one Put
// / Delete / BatchPut). If it publishes zero or more than one, the
// helper fails the test.
func captureLeaderRecord(t *testing.T, fn func(db *DB)) ([]byte, *DB) {
	t.Helper()
	leader, err := Open(Options{
		Dir:                   t.TempDir(),
		MaxBatchEncodedSize:   16 * 1024 * 1024,
		ReplicationBufferSize: 16,
	})
	if err != nil {
		t.Fatalf("leader Open: %v", err)
	}
	t.Cleanup(func() { _ = leader.Close() })

	sub, err := leader.Subscribe()
	if err != nil {
		t.Fatalf("leader Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	fn(leader)

	select {
	case rec, ok := <-sub.Records():
		if !ok {
			t.Fatal("subscription channel closed before record arrived")
		}
		return rec, leader
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replicated record")
		return nil, nil
	}
}

// openFollower opens a fresh DB suitable for apply tests. Replication
// is OFF on the follower itself (followers do not republish in v0.1.0).
func openFollower(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		MaxBatchEncodedSize: 16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("follower Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestApplyReplicatedRecordPut: the most basic case. Capture a single
// PUT from the leader, apply on a fresh follower, and verify Get
// returns the same bytes.
func TestApplyReplicatedRecordPut(t *testing.T) {
	raw, _ := captureLeaderRecord(t, func(db *DB) {
		if err := db.Put([]byte("k"), []byte("hello")); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
	})

	follower := openFollower(t)
	if err := follower.ApplyReplicatedRecord(raw); err != nil {
		t.Fatalf("ApplyReplicatedRecord: %v", err)
	}
	got, err := follower.Get([]byte("k"))
	if err != nil {
		t.Fatalf("follower Get: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("follower Get: got %q want %q", got, "hello")
	}
}

// TestApplyReplicatedRecordDelete: apply a tombstone, verify Get
// reports the key missing.
func TestApplyReplicatedRecordDelete(t *testing.T) {
	leader, err := Open(Options{Dir: t.TempDir(), MaxBatchEncodedSize: 16 * 1024 * 1024, ReplicationBufferSize: 16})
	if err != nil {
		t.Fatalf("leader Open: %v", err)
	}
	defer leader.Close()
	sub, err := leader.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if err := leader.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("leader Put: %v", err)
	}
	putRaw := waitRecord(t, sub)
	if err := leader.Delete([]byte("k")); err != nil {
		t.Fatalf("leader Delete: %v", err)
	}
	delRaw := waitRecord(t, sub)

	follower := openFollower(t)
	if err := follower.ApplyReplicatedRecord(putRaw); err != nil {
		t.Fatalf("apply put: %v", err)
	}
	if err := follower.ApplyReplicatedRecord(delRaw); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if _, err := follower.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("follower Get after tombstone: got %v want ErrKeyNotFound", err)
	}
}

// TestApplyReplicatedRecordBatch: apply a BATCH and verify every entry
// is visible at the right value. Tests both the PUT and tombstone
// paths inside one batch.
func TestApplyReplicatedRecordBatch(t *testing.T) {
	leader, err := Open(Options{Dir: t.TempDir(), MaxBatchEncodedSize: 16 * 1024 * 1024, ReplicationBufferSize: 16})
	if err != nil {
		t.Fatalf("leader Open: %v", err)
	}
	defer leader.Close()
	sub, err := leader.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if err := leader.Put([]byte("gone"), []byte("before")); err != nil {
		t.Fatalf("leader seed: %v", err)
	}
	seedRaw := waitRecord(t, sub)

	if err := leader.BatchPut([]BatchEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("two")},
		{Key: []byte("gone"), Delete: true},
	}); err != nil {
		t.Fatalf("leader BatchPut: %v", err)
	}
	batchRaw := waitRecord(t, sub)

	follower := openFollower(t)
	if err := follower.ApplyReplicatedRecord(seedRaw); err != nil {
		t.Fatalf("apply seed: %v", err)
	}
	if err := follower.ApplyReplicatedRecord(batchRaw); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	if v, err := follower.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("follower Get a: got (%q,%v) want (%q,nil)", v, err, "1")
	}
	if v, err := follower.Get([]byte("b")); err != nil || !bytes.Equal(v, []byte("two")) {
		t.Fatalf("follower Get b: got (%q,%v) want (%q,nil)", v, err, "two")
	}
	if _, err := follower.Get([]byte("gone")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("follower Get gone: got %v want ErrKeyNotFound", err)
	}
}

// waitRecord pulls one record from sub or fails the test on timeout.
func waitRecord(t *testing.T, sub *Subscription) []byte {
	t.Helper()
	select {
	case rec, ok := <-sub.Records():
		if !ok {
			t.Fatal("subscription closed before record arrived")
		}
		return rec
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replicated record")
		return nil
	}
}

// TestApplyReplicatedRecordBadCRC corrupts one byte of the record body
// and expects ErrReplicationCRC. The follower's on-disk state must not
// advance: a Get on a key never previously seen still returns
// ErrKeyNotFound, and active segment size is unchanged.
func TestApplyReplicatedRecordBadCRC(t *testing.T) {
	raw, _ := captureLeaderRecord(t, func(db *DB) {
		if err := db.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
	})
	// Make a mutable copy and flip a value byte (past the header).
	bad := make([]byte, len(raw))
	copy(bad, raw)
	bad[len(bad)-1] ^= 0xFF

	follower := openFollower(t)
	sizeBefore := follower.active.size.Load()
	err := follower.ApplyReplicatedRecord(bad)
	if !errors.Is(err, ErrReplicationCRC) {
		t.Fatalf("ApplyReplicatedRecord: got %v want ErrReplicationCRC", err)
	}
	if got := follower.active.size.Load(); got != sizeBefore {
		t.Fatalf("active grew on bad apply: before=%d after=%d", sizeBefore, got)
	}
	if _, err := follower.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("follower Get: got %v want ErrKeyNotFound", err)
	}
}

// TestApplyReplicatedRecordTruncated tests the malformed-framing branch:
// passing a record shorter than the header is rejected as malformed,
// not as bad CRC.
func TestApplyReplicatedRecordTruncated(t *testing.T) {
	follower := openFollower(t)
	err := follower.ApplyReplicatedRecord([]byte{0x00, 0x01, 0x02}) // way too short
	if !errors.Is(err, ErrReplicationMalformed) {
		t.Fatalf("ApplyReplicatedRecord: got %v want ErrReplicationMalformed", err)
	}
}

// TestApplyReplicatedRecordEmpty: zero-length is rejected at the
// boundary before the writer is even touched.
func TestApplyReplicatedRecordEmpty(t *testing.T) {
	follower := openFollower(t)
	if err := follower.ApplyReplicatedRecord(nil); !errors.Is(err, ErrReplicationMalformed) {
		t.Fatalf("nil: got %v want ErrReplicationMalformed", err)
	}
	if err := follower.ApplyReplicatedRecord([]byte{}); !errors.Is(err, ErrReplicationMalformed) {
		t.Fatalf("empty: got %v want ErrReplicationMalformed", err)
	}
}

// TestApplyReplicatedRecordAfterClose verifies the closed-DB contract:
// apply after Close returns ErrDBClosed, not a panic.
func TestApplyReplicatedRecordAfterClose(t *testing.T) {
	raw, _ := captureLeaderRecord(t, func(db *DB) {
		if err := db.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
	})

	follower, err := Open(Options{Dir: t.TempDir(), MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("follower Open: %v", err)
	}
	if err := follower.Close(); err != nil {
		t.Fatalf("follower Close: %v", err)
	}
	if err := follower.ApplyReplicatedRecord(raw); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("apply after Close: got %v want ErrDBClosed", err)
	}
}

// TestApplyReplicatedRecordRatchetsTimestamp pins the lastTstamp
// ratchet: after applying a leader record with timestamp T, a local Put
// generates a timestamp > T. This is what keeps timestamp monotonicity
// intact across a future promote.
func TestApplyReplicatedRecordRatchetsTimestamp(t *testing.T) {
	// Build a record by hand with a large timestamp so the local
	// wall clock can't satisfy it without a ratchet bump.
	farFuture := time.Now().Add(100 * 365 * 24 * time.Hour).UnixNano()
	rec := record{
		tstamp: farFuture,
		flag:   recordFlagPut,
		key:    []byte("k"),
		value:  []byte("v"),
	}
	raw := make([]byte, rec.encodedSize())
	if _, err := rec.encode(raw); err != nil {
		t.Fatalf("encode synthetic record: %v", err)
	}

	follower := openFollower(t)
	if err := follower.ApplyReplicatedRecord(raw); err != nil {
		t.Fatalf("ApplyReplicatedRecord: %v", err)
	}
	if got := follower.lastTstamp; got < farFuture {
		t.Fatalf("lastTstamp: got %d want >= %d", got, farFuture)
	}
	// A subsequent local Put must mint a timestamp strictly greater.
	if err := follower.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("post-apply Put: %v", err)
	}
	if got := follower.lastTstamp; got <= farFuture {
		t.Fatalf("post-Put lastTstamp: got %d want > %d", got, farFuture)
	}
}

// TestApplyReplicatedRecordSurvivesRestart writes a few applies, closes
// the follower, reopens it, and verifies the recovered state matches
// what apply produced. This is the regression for "apply writes a
// valid-looking segment that recovery can re-parse".
func TestApplyReplicatedRecordSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Capture two records from a leader.
	leader, err := Open(Options{Dir: t.TempDir(), MaxBatchEncodedSize: 16 * 1024 * 1024, ReplicationBufferSize: 16})
	if err != nil {
		t.Fatalf("leader Open: %v", err)
	}
	defer leader.Close()
	sub, err := leader.Subscribe()
	if err != nil {
		t.Fatalf("leader Subscribe: %v", err)
	}
	defer sub.Close()

	rawRecords := make([][]byte, 0, 2)
	for i := 0; i < 2; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := leader.Put(k, v); err != nil {
			t.Fatalf("leader Put %d: %v", i, err)
		}
		select {
		case rec, ok := <-sub.Records():
			if !ok {
				t.Fatal("subscription closed")
			}
			rawRecords = append(rawRecords, rec)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for record %d", i)
		}
	}

	follower, err := Open(Options{Dir: dir, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("follower Open: %v", err)
	}
	for i, raw := range rawRecords {
		if err := follower.ApplyReplicatedRecord(raw); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if err := follower.Close(); err != nil {
		t.Fatalf("follower Close: %v", err)
	}

	// Reopen and check.
	reopened, err := Open(Options{Dir: dir, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("follower reopen: %v", err)
	}
	defer reopened.Close()
	for i := 0; i < 2; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		want := []byte(fmt.Sprintf("v%d", i))
		got, err := reopened.Get(k)
		if err != nil {
			t.Fatalf("reopened Get k%d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopened Get k%d: got %q want %q", i, got, want)
		}
	}
}
