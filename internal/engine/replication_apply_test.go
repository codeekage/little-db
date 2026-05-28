package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
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

// encodeRawPutRecord hand-builds a PUT record with an arbitrary
// timestamp. We avoid record.encode for tests that need a specific
// tstamp because the writer goroutine normally controls it.
func encodeRawPutRecord(tstamp int64, key, value []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(key)+len(value))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(tstamp))
	buf[12] = recordFlagPut
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(value)))
	copy(buf[recordHeaderSize:], key)
	copy(buf[recordHeaderSize+len(key):], value)
	sum := crc32.Checksum(buf[4:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	return buf
}

// encodeRawTombstoneRecord hand-builds a tombstone with a specific
// timestamp.
func encodeRawTombstoneRecord(tstamp int64, key []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(key))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(tstamp))
	buf[12] = recordFlagTombstone
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[17:21], 0)
	copy(buf[recordHeaderSize:], key)
	sum := crc32.Checksum(buf[4:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	return buf
}

// TestApplyReplicatedRecordRejectsOlderPut pins the timestamp-gating
// contract: applying an older PUT after a newer PUT must NOT roll back
// the keydir. This mirrors recovery's putIfNewer semantics — the live
// in-memory state must match what post-restart recovery would produce.
func TestApplyReplicatedRecordRejectsOlderPut(t *testing.T) {
	follower := openFollower(t)
	tNew := int64(2000)
	tOld := int64(1000)
	newer := encodeRawPutRecord(tNew, []byte("k"), []byte("new"))
	older := encodeRawPutRecord(tOld, []byte("k"), []byte("old"))

	if err := follower.ApplyReplicatedRecord(newer); err != nil {
		t.Fatalf("apply newer: %v", err)
	}
	if err := follower.ApplyReplicatedRecord(older); err != nil {
		t.Fatalf("apply older: %v", err)
	}
	got, err := follower.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("older PUT rolled back keydir: got %q want %q", got, "new")
	}
}

// TestApplyReplicatedRecordRejectsOlderTombstone: same gating for the
// tombstone path. An older tombstone arriving after a newer PUT must
// not delete the key.
func TestApplyReplicatedRecordRejectsOlderTombstone(t *testing.T) {
	follower := openFollower(t)
	tNew := int64(2000)
	tOld := int64(1000)
	newer := encodeRawPutRecord(tNew, []byte("k"), []byte("new"))
	staleDelete := encodeRawTombstoneRecord(tOld, []byte("k"))

	if err := follower.ApplyReplicatedRecord(newer); err != nil {
		t.Fatalf("apply newer: %v", err)
	}
	if err := follower.ApplyReplicatedRecord(staleDelete); err != nil {
		t.Fatalf("apply stale delete: %v", err)
	}
	got, err := follower.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("stale tombstone deleted live key: got=%q err=%v", got, err)
	}
}

// TestApplyReplicatedRecordMalformedBatchNotPersisted pins the HIGH
// fix: a BATCH record whose outer CRC is valid but whose body declares
// more entries than it carries must be rejected BEFORE the bytes hit
// the segment. Otherwise the next restart would fail recovery on the
// poison-pill record we ourselves wrote.
func TestApplyReplicatedRecordMalformedBatchNotPersisted(t *testing.T) {
	// Body: count=2 but only one (tiny) entry follows. Decoding
	// fails with errBadBatchBody at entry 1.
	body := make([]byte, 0)
	count := make([]byte, 4)
	binary.LittleEndian.PutUint32(count, 2)
	body = append(body, count...)
	// One PUT entry: inner_flag=0, klen=1, vlen=1, key='a', value='1'
	entry := make([]byte, batchEntryHeaderSize+2)
	entry[0] = recordFlagPut
	binary.LittleEndian.PutUint32(entry[1:5], 1)
	binary.LittleEndian.PutUint32(entry[5:9], 1)
	entry[9] = 'a'
	entry[10] = '1'
	body = append(body, entry...)

	// Outer header: keylen=0, vallen=len(body), flag=batch.
	raw := make([]byte, recordHeaderSize+len(body))
	binary.LittleEndian.PutUint64(raw[4:12], 12345)
	raw[12] = recordFlagBatch
	binary.LittleEndian.PutUint32(raw[13:17], 0)
	binary.LittleEndian.PutUint32(raw[17:21], uint32(len(body)))
	copy(raw[recordHeaderSize:], body)
	sum := crc32.Checksum(raw[4:], crc32cTable)
	binary.LittleEndian.PutUint32(raw[0:4], sum)

	dir := t.TempDir()
	follower, err := Open(Options{Dir: dir, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	sizeBefore := follower.active.size.Load()
	if err := follower.ApplyReplicatedRecord(raw); !errors.Is(err, ErrReplicationMalformed) {
		t.Fatalf("apply malformed batch: got %v want ErrReplicationMalformed", err)
	}
	if got := follower.active.size.Load(); got != sizeBefore {
		t.Fatalf("active grew despite malformed batch: before=%d after=%d", sizeBefore, got)
	}
	if err := follower.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The real regression: a fresh Open must succeed. If we had
	// persisted the poison record, recovery would have aborted.
	reopened, err := Open(Options{Dir: dir, MaxBatchEncodedSize: 16 * 1024 * 1024})
	if err != nil {
		t.Fatalf("reopen after rejected malformed batch: %v", err)
	}
	_ = reopened.Close()
}

// TestApplyReplicatedRecordRejectedRecordDoesNotAdvanceClock pins the
// "rejected => fully rolled back" contract for lastTstamp. A malformed
// record that is rejected at the pre-validation stage must not bump
// the writer's monotonic clock — otherwise a stream of bad records
// could silently push every subsequent locally generated timestamp
// far into the future.
func TestApplyReplicatedRecordRejectedRecordDoesNotAdvanceClock(t *testing.T) {
	follower := openFollower(t)
	farFuture := time.Now().Add(100 * 365 * 24 * time.Hour).UnixNano()
	before := follower.lastTstamp

	// Build a BATCH whose outer CRC is valid but whose body is
	// malformed (count=2, one entry) AND whose timestamp is far in
	// the future. If the ratchet still happens before validation,
	// lastTstamp will jump to farFuture even though the record is
	// rejected.
	body := make([]byte, 0)
	count := make([]byte, 4)
	binary.LittleEndian.PutUint32(count, 2)
	body = append(body, count...)
	entry := make([]byte, batchEntryHeaderSize+2)
	entry[0] = recordFlagPut
	binary.LittleEndian.PutUint32(entry[1:5], 1)
	binary.LittleEndian.PutUint32(entry[5:9], 1)
	entry[9] = 'a'
	entry[10] = '1'
	body = append(body, entry...)

	raw := make([]byte, recordHeaderSize+len(body))
	binary.LittleEndian.PutUint64(raw[4:12], uint64(farFuture))
	raw[12] = recordFlagBatch
	binary.LittleEndian.PutUint32(raw[13:17], 0)
	binary.LittleEndian.PutUint32(raw[17:21], uint32(len(body)))
	copy(raw[recordHeaderSize:], body)
	sum := crc32.Checksum(raw[4:], crc32cTable)
	binary.LittleEndian.PutUint32(raw[0:4], sum)

	if err := follower.ApplyReplicatedRecord(raw); !errors.Is(err, ErrReplicationMalformed) {
		t.Fatalf("apply: got %v want ErrReplicationMalformed", err)
	}
	if got := follower.lastTstamp; got >= farFuture {
		t.Fatalf("lastTstamp advanced on rejected record: before=%d after=%d farFuture=%d", before, got, farFuture)
	}
	// And a subsequent local Put still mints a sane timestamp
	// (near now, not near farFuture).
	if err := follower.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("post-reject Put: %v", err)
	}
	if got := follower.lastTstamp; got >= farFuture {
		t.Fatalf("post-Put lastTstamp polluted by rejected record: got=%d farFuture=%d", got, farFuture)
	}
}

// TestApplyReplicatedRecordBatchAtomicVsConcurrentRange exercises the
// applyBatchIfNewer lock-coverage promise: a concurrent ReadKeyRange
// snapshot must observe either none or all of a replicated BATCH's
// keydir updates, never a prefix. A previous implementation walked
// entries with one putIfNewer per key under separate locks, which
// would let a snapshot see a partial batch.
func TestApplyReplicatedRecordBatchAtomicVsConcurrentRange(t *testing.T) {
	// Capture one big batch from a leader: 64 keys "bk00".."bk63",
	// every value the literal "v". The reader scans the [bk, bl)
	// range and must always see either 0 or 64 entries.
	const n = 64
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

	entries := make([]BatchEntry, n)
	for i := range entries {
		entries[i] = BatchEntry{
			Key:   []byte(fmt.Sprintf("bk%02d", i)),
			Value: []byte("v"),
		}
	}
	if err := leader.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	batchRaw := waitRecord(t, sub)

	follower := openFollower(t)

	// Reader goroutines hammer ReadKeyRange while the writer
	// applies the batch a small delay later. Each observation
	// records the number of keys in the range; a non-zero,
	// non-n observation is a prefix and a test failure.
	var stop atomic.Bool
	var wg sync.WaitGroup
	bad := make(chan int, 16)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				count := 0
				err := follower.ReadKeyRange([]byte("bk"), []byte("bl"), func(_, _ []byte) bool {
					count++
					return true
				})
				if err != nil {
					return
				}
				if count != 0 && count != n {
					select {
					case bad <- count:
					default:
					}
					return
				}
			}
		}()
	}

	// Give readers a moment to spin up.
	time.Sleep(20 * time.Millisecond)
	if err := follower.ApplyReplicatedRecord(batchRaw); err != nil {
		t.Fatalf("apply batch: %v", err)
	}
	// Let readers see the post-apply state.
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	close(bad)

	for got := range bad {
		t.Fatalf("ReadKeyRange observed prefix of batch: %d keys (want 0 or %d)", got, n)
	}
	// Sanity: the batch did actually land.
	finalCount := 0
	_ = follower.ReadKeyRange([]byte("bk"), []byte("bl"), func(_, _ []byte) bool {
		finalCount++
		return true
	})
	if finalCount != n {
		t.Fatalf("post-apply range count: got %d want %d", finalCount, n)
	}
}
