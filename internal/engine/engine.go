// Package engine implements a Bitcask-style append-only key/value storage engine.
//
// Design (one-paragraph version):
//
// Writes are appended to the tail of a single "active" segment file as
// length-prefixed, CRC32C-protected records. An in-memory index (the "keydir")
// maps every live key to the segment + byte offset of its latest value, so a
// read is one map lookup followed by one positioned read (pread) — no seeks on
// the hot path. When the active segment exceeds maxSegmentSize it is rotated:
// the current file becomes immutable, and a new active file is opened. Deletes
// append tombstones. Older data is reclaimed by background compaction (added
// in a later batch).
//
// Concurrency:
//
//   - All mutating operations (Put, Delete, BatchPut) are funneled through a
//     single writer goroutine via an internal request channel. The writer is
//     the sole owner of the active segment, the next-segment counter, the
//     monotonic timestamp counter, and the encode buffer; none of those need
//     a lock because no other goroutine touches them.
//   - The writer also performs group commit: when several requests pile up in
//     the channel it appends them all and issues a single fsync (under
//     SyncOnPut=true) before replying. This turns N independent fsyncs into
//     one for a burst — important on Darwin where F_FULLFSYNC dominates.
//   - Reads (Get, ReadKeyRange) hold a segmentsMu read lock across the
//     positional read so that Close cannot close the file descriptor out
//     from under them. They do not contend with each other or with the
//     writer except briefly during segment rotation.
//
// Public contract (locked in batch 3b.i):
//
//   - Durability. With SyncOnPut=false, a successful Put/Delete/BatchPut
//     return guarantees the write is visible to subsequent reads in this
//     process and will be flushed-and-synced by a later successful Close.
//     With SyncOnPut=true, the write is fsynced to stable storage before
//     return (Darwin: F_FULLFSYNC).
//   - Close. Close is idempotent. Calls that begin after Close has marked
//     the DB closed return ErrDBClosed. Calls that begin before or
//     concurrently with Close may either complete normally or return
//     ErrDBClosed.
//   - Reads during shutdown. Close waits for in-flight reads to finish
//     before closing segment files. No goroutine ever observes a closed
//     file descriptor.
package engine

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"little-db/internal/logging"
)

// Sentinel errors that are part of the engine's public contract.
// Callers (including future network layers) should match these with errors.Is
// rather than string-matching, so they can map validation errors to protocol
// responses without coupling to error messages.
var (
	// ErrKeyNotFound is returned by Get when the key has no live value.
	ErrKeyNotFound = errors.New("engine: key not found")

	// ErrDBClosed is returned by mutating and read operations that begin
	// after Close has marked the DB closed.
	ErrDBClosed = errors.New("engine: db closed")

	// ErrKeyTooLarge is returned when a key exceeds maxKeyLen (64 KiB).
	ErrKeyTooLarge = errors.New("engine: key exceeds 64 KiB")

	// ErrValueTooLarge is returned when a value exceeds maxValLen (16 MiB).
	ErrValueTooLarge = errors.New("engine: value exceeds 16 MiB")

	// ErrEmptyKey is returned when a caller passes a zero-length key.
	ErrEmptyKey = errors.New("engine: empty key")

	// ErrBatchTooLarge is returned by BatchPut when the batch exceeds an
	// engine-imposed size limit — either the encoded record would exceed
	// Options.MaxBatchEncodedSize, or the number of entries would exceed
	// the internal maxBatchEntries cap that bounds recovery allocation.
	// Both cases share this sentinel; the wrapped message distinguishes
	// them for humans.
	ErrBatchTooLarge = errors.New("engine: batch exceeds size limit")

	// ErrInvalidBatchEntry wraps the per-entry validation error from BatchPut
	// (empty key, oversize key, oversize value) so callers can both inspect the
	// underlying error (errors.Is) and identify the offending entry index from
	// the wrapped message.
	ErrInvalidBatchEntry = errors.New("engine: invalid batch entry")

	// ErrWritesDisabled is returned by Put / Delete / BatchPut after the
	// engine has entered a fatal write-disabled state. It is set when a
	// manifest publish reaches ErrManifestPublishedButUncertain during
	// segment rotation: the directory fsync did not confirm, so the
	// running process cannot safely accept further writes that depend on
	// the new active segment being durable. Reads continue to succeed
	// from the prior sealed segments. The operator should stop the
	// process, let the kernel finish flushing (or accept the loss), and
	// restart — Open's sweepOrphans will reconcile whichever side of
	// the rename survived.
	ErrWritesDisabled = errors.New("engine: writes disabled (manifest durability uncertain)")
)

// BatchEntry is one operation inside a BatchPut. A non-Delete entry writes
// Key=Value; Delete=true writes a tombstone for Key and Value is ignored.
type BatchEntry struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Options configures a DB.
type Options struct {
	// Dir is the directory that holds segment files. It will be created if it
	// does not exist.
	Dir string

	// MaxSegmentSize is the *rotation threshold* (not a hard cap) for the
	// active segment. The writer rotates when the next append would push the
	// active segment past this size. Batches larger than the threshold are
	// still written as a single record into a segment that exceeds the
	// threshold, to preserve whole-batch atomicity (see BatchPut).
	// Zero means use the default (256 MiB).
	MaxSegmentSize int64

	// SyncOnPut forces fsync (Darwin: F_FULLFSYNC) after every successful
	// write or write batch. Safe but slow. When false, writes are durable
	// only after the OS flushes the page cache or the engine is closed
	// cleanly.
	SyncOnPut bool

	// WriteQueueDepth bounds the internal request channel buffer. A small
	// value (default 64) means a stuck disk surfaces as caller latency
	// within a few requests rather than swallowing thousands of writes
	// silently; a larger value smooths over brief jitter. Zero means
	// default.
	WriteQueueDepth int

	// MaxBatchEncodedSize caps the encoded size (header + body) of a single
	// BatchPut record. Default 64 MiB. A batch larger than this returns
	// ErrBatchTooLarge without writing anything. The cap protects the
	// writer's encode buffer and segment files from a pathological caller;
	// it is independent of MaxSegmentSize, since a single batch is always
	// written atomically as one record even if it exceeds the segment
	// rotation threshold.
	MaxBatchEncodedSize int64

	// CompactionInterval controls the background compactor. A positive
	// value spawns a goroutine that runs one compaction pass per interval;
	// zero disables background compaction entirely (callers can still
	// invoke DB.Compact() manually). The default is disabled so test
	// suites with short lifetimes do not see surprise compactor activity.
	CompactionInterval time.Duration

	// Logger receives lifecycle events (open, close, segment rotation,
	// compaction start/finish/error, orphan sweep, hint replay fallback).
	// Nil installs a no-op logger — the engine never emits a per-request
	// log itself, so a Nop default has zero overhead.
	Logger *slog.Logger

	// ReplicationBufferSize installs an optional leader-side replication
	// publisher. Zero (the default) disables replication entirely: no
	// Subscribe handle, no per-write hook beyond a single inlined branch,
	// no counters. A positive value installs a single-subscriber
	// publisher whose buffered channel holds up to this many encoded
	// records. The writer never blocks: when the buffer is full new
	// records are dropped and Stats().ReplicationLagDropped is
	// incremented. See docs/replication.md §3 for the design rationale
	// and §6 for the failure-mode contract. Recommended starting value
	// for production: 1024.
	ReplicationBufferSize int
}

const (
	defaultMaxSegmentSize      int64 = 256 * 1024 * 1024
	defaultWriteQueueDepth           = 64
	defaultMaxBatchEncodedSize int64 = 64 * 1024 * 1024
)

// writeKind discriminates the request types that flow through the writer
// goroutine's request channel.
type writeKind uint8

const (
	writeKindPut writeKind = iota
	writeKindDelete
	writeKindBatch
	writeKindCompactCommit
)

// writeRequest is one in-flight mutating operation sent from a caller
// goroutine to the writer goroutine. The reply channel has capacity 1 so the
// writer never blocks publishing a result. For Put/Delete, key/value carry
// the operands. For Batch, entries carries the whole batch and key/value are
// unused.
type writeRequest struct {
	kind    writeKind
	key     []byte
	value   []byte
	entries []BatchEntry
	compact *compactCommit // non-nil only for writeKindCompactCommit
	reply   chan error
}

// DB is the storage engine handle. Safe for concurrent use.
//
// Field ownership:
//
//   - active, lastTstamp, encBuf: owned exclusively by the writer
//     goroutine. No lock; no other goroutine touches them.
//   - nextID: atomically allocated. The writer claims a fresh id on
//     rotation; the background compactor claims one when emitting a
//     merged segment. Atomic.Add guarantees both producers see disjoint
//     ids without coordination.
//   - segments: protected by segmentsMu. The writer takes the write lock
//     only on rotation (and on close). Readers (Get, ReadKeyRange) take
//     the read lock across the positional read so Close cannot pull the
//     file out from under them.
//   - keydir: internally synchronised (see keydir.go).
//   - opts, lockFile, reqCh, done, writerWG, closed, closeOnce, closeErr:
//     set once during Open / written only by Close.
type DB struct {
	opts Options
	log  *slog.Logger

	// Writer-goroutine private state. No synchronisation needed; only the
	// writer reads or writes these fields after Open returns.
	active     *segment
	nextID     atomic.Uint32
	lastTstamp int64
	encBuf     []byte

	// segmentsMu protects the segments map and the lifetime of the *os.File
	// handles inside each segment. Held briefly (RLock) across reader preads
	// and exclusively (Lock) by the writer when rotating and by Close when
	// sealing files.
	segmentsMu sync.RWMutex
	segments   map[uint32]*segment // includes the active segment

	keydir *keydir

	// lockFile holds an exclusive flock on the data directory so two
	// processes cannot open the same DB. Released on Close.
	lockFile *os.File

	// Writer goroutine plumbing.
	reqCh    chan *writeRequest
	done     chan struct{}  // closed by Close to signal shutdown
	writerWG sync.WaitGroup // tracks the writer goroutine

	// Compactor goroutine plumbing. compactorWG tracks the optional
	// background loop spawned when Options.CompactionInterval > 0; it is
	// idle and unused when compaction is disabled.
	compactorWG sync.WaitGroup

	// compactMu serialises compactOnce passes. Two concurrent passes
	// (e.g. background loop overlapping with a manual DB.Compact() call,
	// or two manual calls) would each snapshot the same candidate set;
	// one would commit and unlink the input segments while the other was
	// still reading them, producing read errors or worse. We make the
	// second concurrent caller wait — compaction is meant to be a
	// background space-reclamation pass, not a parallel-throughput
	// primitive.
	compactMu sync.Mutex

	// submitMu serializes mutating-call enqueue with Close. Submits hold
	// the read lock across (closed-check + send-on-reqCh); Close holds the
	// write lock across (closed=true + close(done)). This guarantees that
	// any submit which successfully enqueues a request happens-before Close
	// closes done, so the writer's drain path is guaranteed to dequeue and
	// reply to it. Without this, a submit could enqueue, then observe
	// done closed, and bail with ErrDBClosed while the write still lands.
	submitMu sync.RWMutex

	// closed is set to true by Close before close(done). Callers check it
	// to fail fast; the contract is that calls beginning after closed=true
	// always return ErrDBClosed, while calls beginning concurrently with
	// Close may either complete or fail.
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error // first-call error, returned by every subsequent Close

	// writesDisabled is set when rotateActive hits
	// ErrManifestPublishedButUncertain. From that moment on submit returns
	// ErrWritesDisabled instead of forwarding the request to the writer.
	// Once true it never reverts; the operator must restart the process
	// so Open can reconcile the manifest on disk. Reads are unaffected
	// and continue to serve from the prior sealed segments.
	writesDisabled atomic.Bool

	// cachedStats holds the last Stats snapshot, captured by Close just
	// before db.segments is nilled out. Stats() returns this value after
	// Close so dashboards and health endpoints don't observe a sudden
	// drop to zero during shutdown.
	cachedStats atomic.Pointer[Stats]

	// Replication publisher state. subMu protects the sub slot. When
	// ReplicationBufferSize == 0 these fields are unused and the publish
	// hook in processBurst becomes a single inlined branch.
	subMu              sync.Mutex
	sub                *Subscription
	replicationDropped atomic.Uint64
}

// Open creates or opens a DB rooted at opts.Dir.
//
// Open is responsible for:
//   - acquiring an exclusive advisory lock on the data directory (so two
//     processes cannot share one DB);
//   - discovering existing segment files;
//   - rebuilding the in-memory keydir by scanning every segment in ascending
//     id order, with torn-write detection at the tail of the manifest-named
//     active segment (NOT max(id) — compaction can publish a merged
//     immutable segment with a higher id than the active);
//   - reusing the manifest's active pre-existing segment when it still has
//     capacity, so a restart does not litter the data directory with empty
//     files.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("engine: Options.Dir is required")
	}
	if opts.MaxSegmentSize == 0 {
		opts.MaxSegmentSize = defaultMaxSegmentSize
	}
	if opts.MaxSegmentSize < 0 {
		return nil, errors.New("engine: Options.MaxSegmentSize must be >= 0")
	}
	if opts.WriteQueueDepth == 0 {
		opts.WriteQueueDepth = defaultWriteQueueDepth
	}
	if opts.WriteQueueDepth < 0 {
		return nil, errors.New("engine: Options.WriteQueueDepth must be >= 0")
	}
	if opts.MaxBatchEncodedSize == 0 {
		opts.MaxBatchEncodedSize = defaultMaxBatchEncodedSize
	}
	if opts.MaxBatchEncodedSize < 0 {
		return nil, errors.New("engine: Options.MaxBatchEncodedSize must be >= 0")
	}
	if opts.MaxBatchEncodedSize > int64(recordHeaderSize+maxBatchBodyLen) {
		return nil, fmt.Errorf("engine: Options.MaxBatchEncodedSize must be <= %d", recordHeaderSize+maxBatchBodyLen)
	}
	if opts.CompactionInterval < 0 {
		return nil, errors.New("engine: Options.CompactionInterval must be >= 0")
	}
	if opts.ReplicationBufferSize < 0 {
		return nil, errors.New("engine: Options.ReplicationBufferSize must be >= 0")
	}
	if opts.Logger == nil {
		opts.Logger = logging.Nop()
	}

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", opts.Dir, err)
	}

	lockFile, err := acquireDataDirLock(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("engine: %w", err)
	}

	db := &DB{
		opts:     opts,
		log:      opts.Logger,
		segments: make(map[uint32]*segment),
		keydir:   newKeydir(),
		lockFile: lockFile,
		encBuf:   make([]byte, 0, 64*1024),
		reqCh:    make(chan *writeRequest, opts.WriteQueueDepth),
		done:     make(chan struct{}),
	}

	onDisk, err := listSegments(opts.Dir)
	if err != nil {
		db.releaseLock()
		return nil, err
	}

	// Reconcile the directory listing against the manifest. The manifest is
	// the source of truth for which segments are live; anything in the
	// directory but not in the manifest is treated as an orphan (a leftover
	// from a crash mid-compaction in batch 4c — for now, none exist, but
	// the reconciliation logic is in place from day one so 4c is a one-line
	// change rather than a new mechanism).
	existing, manifestActive, manifestBootstrapped, err := reconcileManifest(opts.Dir, onDisk)
	if err != nil {
		db.releaseLock()
		return nil, err
	}

	// Open every live segment O_RDWR so recovery can truncate trailing
	// torn writes and so the manifest-named active segment can become the
	// writer's append target.
	for _, id := range existing {
		path := filepath.Join(opts.Dir, segmentFilename(id))
		seg, err := openSegmentReadWrite(path, id)
		if err != nil {
			_ = db.closeAllSegmentsLocked()
			db.releaseLock()
			return nil, err
		}
		db.segments[id] = seg
		if cur := db.nextID.Load(); id+1 > cur {
			db.nextID.Store(id + 1)
		}
	}

	// Skip past manifest-orphan segment ids on disk so the next rotation
	// cannot collide with a leftover file. Sequence to defend against: a
	// crash happens inside rotateActive after createSegment(N) lands the
	// new file but before writeManifest publishes it. On restart, segment
	// N is on disk but not in the manifest, reconcileManifest correctly
	// drops it from the live set — but if nextID is computed only from
	// the manifest-listed ids it stays at N, and the next rotation calls
	// createSegment(N) which fails with "file exists" forever. Orphan
	// files stay on disk (forensics + the compactor will sweep them in
	// 4c); nextID just has to step around them.
	if len(onDisk) > 0 {
		if maxOnDisk := onDisk[len(onDisk)-1]; maxOnDisk+1 > db.nextID.Load() {
			db.nextID.Store(maxOnDisk + 1)
		}
	}

	// Resolve which segment is the writer's active. When a manifest exists
	// it names the active explicitly — the invariant "max(id) is active"
	// no longer holds because compaction emits an immutable merged segment
	// whose id is strictly greater than the active id. On bootstrap (no
	// manifest) with pre-existing segments, we fall back to "newest on
	// disk is the active": that path is only reached for pre-4a fixtures
	// and other directories where compaction has never run, so max(id) is
	// definitionally the active. Either way we must hand recover() an
	// activeID whenever there is anything to torn-tail — otherwise a torn
	// tail on the real active is misread as sealed-segment corruption.
	var activeID uint32
	var haveActive bool
	if len(existing) > 0 {
		if manifestBootstrapped {
			activeID = existing[len(existing)-1]
		} else {
			activeID = manifestActive
		}
		haveActive = true
	}

	// Rebuild the keydir. recover() needs to know which segment is active
	// so it data-scans (and torn-tail-truncates) that one rather than
	// max(ids) — a compacted merged segment may legitimately have a
	// higher id than the active and must NOT receive torn-tail treatment.
	if err := db.recover(haveActive, activeID); err != nil {
		_ = db.closeAllSegmentsLocked()
		db.releaseLock()
		return nil, fmt.Errorf("engine: recover: %w", err)
	}

	// Promote the chosen active when it still has room. Otherwise open a
	// fresh one. A fresh segment is also opened on bootstrap of an empty
	// directory.
	var createdFreshActive bool
	if haveActive {
		active := db.segments[activeID]
		if active.size.Load() < opts.MaxSegmentSize {
			// recover() may have truncated this file. Seek to end so the
			// bufio writer appends at the right position.
			if _, err := active.file.Seek(0, io.SeekEnd); err != nil {
				_ = db.closeAllSegmentsLocked()
				db.releaseLock()
				return nil, err
			}
			active.bw = bufio.NewWriterSize(active.file, 64*1024)
			db.active = active
		}
	}
	if db.active == nil {
		freshID := db.nextID.Add(1) - 1
		active, err := createSegment(opts.Dir, freshID)
		if err != nil {
			_ = db.closeAllSegmentsLocked()
			db.releaseLock()
			return nil, err
		}
		db.segments[active.id] = active
		db.active = active
		createdFreshActive = true
	}

	// Persist the live set if either (a) we just bootstrapped a manifest
	// from the directory listing, or (b) we created a fresh active segment
	// not yet recorded in the manifest. Both cases must be durable before
	// any write lands, otherwise a host crash after the first acked Put
	// would lose data on restart (the new active id would be considered an
	// orphan and skipped).
	if manifestBootstrapped || createdFreshActive {
		live := db.liveSegmentIDsLocked()
		if err := writeManifest(opts.Dir, live, db.active.id); err != nil {
			_ = db.closeAllSegmentsLocked()
			db.releaseLock()
			return nil, fmt.Errorf("engine: write manifest: %w", err)
		}
	}

	// Sweep any .seg / .hint files that survived a previously crashed
	// rotation or compaction. The manifest is now authoritative, the DB
	// is still single-threaded (writer + compactor goroutines have not
	// started), so this is the only window where a directory sweep does
	// not race with an in-flight segment publish. Best-effort: a sweep
	// error is logged and not fatal — orphans are harmless beyond disk
	// space and will be retried next Open.
	if err := db.sweepOrphans(); err != nil {
		db.log.Warn("engine open: orphan sweep failed",
			slog.String("dir", db.opts.Dir),
			slog.String("err", err.Error()))
	}

	db.logBootConfig(len(existing))

	// Spin up the writer goroutine last, after all state is initialised.
	// Until this point the DB is single-threaded (only the calling goroutine
	// touches it), so we don't need locks during Open.
	db.writerWG.Add(1)
	go db.writerLoop()

	// Background compactor is optional. The loop exits cleanly on Close
	// via db.done; tests that don't want surprise activity simply leave
	// CompactionInterval at zero.
	if opts.CompactionInterval > 0 {
		db.compactorWG.Add(1)
		go db.compactorLoop(opts.CompactionInterval)
	}

	return db, nil
}

// logBootConfig writes one line at boot describing the resolved configuration
// and recovery outcome. This is the single searchable answer to "what was
// this process configured with?" during an incident.
func (db *DB) logBootConfig(existingSegments int) {
	db.log.Info("engine open",
		slog.String("dir", db.opts.Dir),
		slog.Int64("max_segment_size", db.opts.MaxSegmentSize),
		slog.Bool("sync_on_put", db.opts.SyncOnPut),
		slog.Int("write_queue_depth", db.opts.WriteQueueDepth),
		slog.Duration("compaction_interval", db.opts.CompactionInterval),
		slog.Int("segments", existingSegments),
		slog.Int("keys", db.keydir.size()),
		slog.Uint64("active_id", uint64(db.active.id)),
	)
}

// releaseLock unlocks and closes the data-dir lock file. Safe to call with
// a nil lockFile.
func (db *DB) releaseLock() {
	if db.lockFile != nil {
		_ = db.lockFile.Close() // closing the fd releases the flock on all unix
		db.lockFile = nil
	}
}

// Close stops the writer goroutine, syncs and seals the active segment, and
// releases all file handles and the data-dir flock. After Close returns the
// DB must not be used. Close is idempotent — subsequent calls return the
// first call's error.
//
// Shutdown sequence:
//  1. submitMu.Lock is taken and closed=true is published atomically. The
//     Lock waits for every in-flight submit (which holds submitMu.RLock)
//     to finish enqueuing on reqCh — those requests will be drained and
//     replied to by the writer. New callers entering submit after the
//     Lock is released see closed=true and bail with ErrDBClosed. done is
//     closed under the same critical section to signal the writer.
//  2. The writer goroutine sees done closed, drains any requests still in
//     the channel buffer, completes them (including a final fsync if
//     SyncOnPut), and exits. We Wait on it.
//  3. We take segmentsMu.Lock so no reader is mid-pread, then close every
//     segment file and release the data-dir lock.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		var firstErr error

		// Serialize with submit: take the write lock so no submit is mid-send.
		// Once we publish closed=true and close(done) under the lock, every
		// later submit either sees closed=true (and bails) or has already
		// successfully enqueued (and will be drained + replied to below).
		db.submitMu.Lock()
		db.closed.Store(true)
		close(db.done)
		db.submitMu.Unlock()

		db.writerWG.Wait()
		// The compactor (if any) shares db.done with the writer; wait for
		// it after the writer so a commit it had already sent is drained
		// and replied to first.
		db.compactorWG.Wait()

		// Writer is quiescent, so publishRecord can no longer fire. Detach
		// and close any active subscription; the consumer observes a
		// channel-close as the clean end-of-stream signal.
		db.closeSubscriptionOnShutdown()

		// A *manual* DB.Compact() caller is not tracked by compactorWG —
		// it runs on the caller's goroutine. It may be mid-scan against
		// segment fds, mid-write of a merged segment, or mid-write of a
		// hint file. We must wait for it to finish before closing segment
		// fds and releasing the directory lock; otherwise it can read
		// from a closed fd, leave a partial file behind, or run after
		// another process has reopened the DB. We take compactMu and
		// hold it through teardown, then release so any callers blocked
		// on compactMu wake up, observe closed=true via the re-check
		// inside compactOnce, and return ErrDBClosed.
		db.compactMu.Lock()

		// Once the writer has exited, the active segment is quiescent.
		// Take segmentsMu exclusively so any in-flight reader finishes its
		// pread before we close the descriptor.
		db.segmentsMu.Lock()
		// Snapshot stats under the same lock that protects the segments
		// map. After closeAllSegmentsLocked nils db.segments, Stats() falls
		// back to this cached value.
		snap := db.statsLocked()
		db.cachedStats.Store(&snap)
		if db.active != nil {
			if err := db.active.sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := db.closeAllSegmentsLocked(); err != nil && firstErr == nil {
			firstErr = err
		}
		db.segmentsMu.Unlock()
		db.releaseLock()
		db.compactMu.Unlock()

		// Store on DB so every subsequent Close call returns the same value.
		db.closeErr = firstErr
		if firstErr != nil {
			db.log.Warn("engine close: error during teardown",
				slog.String("dir", db.opts.Dir),
				slog.String("err", firstErr.Error()))
		} else {
			db.log.Info("engine close",
				slog.String("dir", db.opts.Dir),
				slog.Uint64("keys", snap.KeyCount),
				slog.Uint64("bytes_on_disk", snap.BytesOnDisk))
		}
	})
	return db.closeErr
}

// closeAllSegmentsLocked closes every open segment file. The caller must
// hold segmentsMu exclusively (or guarantee no concurrent access, e.g.
// during a failed Open before the DB is published).
func (db *DB) closeAllSegmentsLocked() error {
	var firstErr error
	for _, s := range db.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.segments = nil
	db.active = nil
	return firstErr
}

// validateKeyValue performs the cheap, deterministic input checks that any
// mutating call must satisfy. Called by Put/Delete/BatchPut on the caller
// goroutine, before enqueueing the request, so the writer can trust that
// every request it dequeues is well-formed.
func validateKeyValue(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > maxKeyLen {
		return ErrKeyTooLarge
	}
	if len(value) > maxValLen {
		return ErrValueTooLarge
	}
	return nil
}

// submit enqueues req and waits for the writer's reply.
//
// The submitMu read lock is held across both the closed-check and the send
// on reqCh. Close takes the write lock before publishing closed=true and
// closing done, so the two cannot interleave: every request that
// successfully reaches reqCh is guaranteed to be dequeued and replied to by
// the writer (either via the main loop or via drainAfterClose). This is the
// contract that lets the caller wait for req.reply unconditionally — a
// successfully enqueued write never goes unobserved.
func (db *DB) submit(req *writeRequest) error {
	db.submitMu.RLock()
	if db.closed.Load() {
		db.submitMu.RUnlock()
		return ErrDBClosed
	}
	// Fatal write-disable from a prior uncertain manifest publish (see
	// rotateActive). Reject before enqueue so callers fail fast and the
	// writer goroutine is not asked to do work it has already proven
	// cannot be made durable. Compaction-commit requests are also
	// gated: any further manifest writes would compound the uncertainty.
	if db.writesDisabled.Load() {
		db.submitMu.RUnlock()
		return ErrWritesDisabled
	}
	db.reqCh <- req
	db.submitMu.RUnlock()
	return <-req.reply
}

// Put writes (or overwrites) the value for key. The write is durable on
// return only if Options.SyncOnPut is true; otherwise it is visible to
// subsequent reads in this process and will be flushed-and-synced by a later
// successful Close.
func (db *DB) Put(key, value []byte) error {
	if err := validateKeyValue(key, value); err != nil {
		return err
	}
	req := &writeRequest{
		kind:  writeKindPut,
		key:   key,
		value: value,
		reply: make(chan error, 1),
	}
	return db.submit(req)
}

// Delete writes a tombstone for key. Idempotent: deleting a missing key is a
// no-op from the caller's perspective, but a tombstone is still appended so
// the deletion is durable and replicable.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > maxKeyLen {
		return ErrKeyTooLarge
	}
	req := &writeRequest{
		kind:  writeKindDelete,
		key:   key,
		reply: make(chan error, 1),
	}
	return db.submit(req)
}

// BatchPut atomically applies every entry in entries as a single fat record.
//
// Atomicity contract:
//   - In-process visibility: every entry's keydir update is published under
//     a single keydir write lock. A `ReadKeyRange` call (which takes one
//     RLock for the entire scan) therefore observes either none or all of
//     the batch across every key it touches — never a prefix. A single
//     `Get` observes either the pre- or post-batch value for its key.
//     Note: a caller-side loop of N separate `Get` calls is not a snapshot;
//     the writer can complete a whole batch between two of those calls.
//     Use `ReadKeyRange` when cross-key atomic reads matter.
//   - Crash atomicity (torn tail): if the process crashes mid-write so the
//     batch's body does not fully reach disk, recovery detects the
//     truncation at the segment tail and the entire batch is silently
//     dropped. Callers see no entries.
//   - Crash safety (mid-file corruption): a BATCH record whose body fits
//     within the segment but whose CRC fails after a later corruption is
//     treated as real corruption. Recovery aborts with an error rather
//     than silently dropping the batch. The operator decides how to
//     proceed; the engine refuses to lose data quietly.
//
// Other semantics:
//   - An empty batch is a no-op; nothing is written and nil is returned.
//   - Duplicate keys in the same batch: the LAST entry wins in the keydir,
//     mirroring the natural append-only ordering on disk.
//   - Entries are validated up front; if any entry has an empty/oversize
//     key or oversize value the entire call fails with an error wrapping
//     ErrInvalidBatchEntry and nothing is written.
//   - If the encoded record (header + body) exceeds Options.MaxBatchEncodedSize,
//     or the entry count exceeds the engine's internal maxBatchEntries cap
//     (which bounds recovery-time allocation), the call fails with
//     ErrBatchTooLarge and nothing is written.
//   - A batch larger than MaxSegmentSize is still written as one record
//     into a single segment; that segment ends up larger than the rotation
//     threshold by design (the threshold is a target, not a hard cap).
func (db *DB) BatchPut(entries []BatchEntry) error {
	if len(entries) == 0 {
		return nil
	}
	// Hard cap on entry count, mirroring decodeBatchBody's cap. We never
	// want to write a batch we couldn't decode back at recovery.
	if len(entries) > maxBatchEntries {
		return fmt.Errorf("%w: %d entries exceeds maxBatchEntries=%d", ErrBatchTooLarge, len(entries), maxBatchEntries)
	}
	for i := range entries {
		e := &entries[i]
		if len(e.Key) == 0 {
			return fmt.Errorf("%w: entry %d: %w", ErrInvalidBatchEntry, i, ErrEmptyKey)
		}
		if len(e.Key) > maxKeyLen {
			return fmt.Errorf("%w: entry %d: %w", ErrInvalidBatchEntry, i, ErrKeyTooLarge)
		}
		if !e.Delete && len(e.Value) > maxValLen {
			return fmt.Errorf("%w: entry %d: %w", ErrInvalidBatchEntry, i, ErrValueTooLarge)
		}
	}
	encodedSize := int64(recordHeaderSize + encodedBatchBodySize(entries))
	if encodedSize > db.opts.MaxBatchEncodedSize {
		return ErrBatchTooLarge
	}
	req := &writeRequest{
		kind:    writeKindBatch,
		entries: entries,
		reply:   make(chan error, 1),
	}
	return db.submit(req)
}

// Read is the public-spec name for a point lookup; it forwards to Get.
//
// Why both Read and Get exist (instead of a single rename):
//
//  1. `Read(key []byte) ([]byte, error)` looks like — but is not — the
//     `io.Reader` interface (`Read(p []byte) (int, error)`). A method on a
//     heavily-used type with that exact name reads as a misnamed reader to
//     any Go reviewer and confuses tooling that scans for `io.Reader`
//     conformance. Keeping the lookup verb as `Get` preserves Go idiom
//     (cf. `http.Header.Get`, `url.Values.Get`, `sync.Map.Load`).
//  2. The assignment PDF and SPEC §4.2 spell the method `Read(key)`. Code
//     written against that documented surface must compile, so we expose
//     `Read` as a thin forwarding alias.
//  3. Internal vocabulary stays consistent under a single noun: error
//     wrapping ("engine: read value: ..."), keydir helpers (`get`), and
//     ~12 test names all speak the same word, and a future rename touches
//     only one site rather than every comment.
//
// Both names are part of the public contract and are guaranteed to be
// observationally identical (see TestReadAliasMatchesGet).
func (db *DB) Read(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Get returns the latest value for key.
// The returned slice is a fresh copy; callers may mutate or retain it.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	// Hold segmentsMu.RLock across BOTH the keydir lookup and the pread.
	// handleCompactCommit performs its keydir CAS and segments-map delete
	// under segmentsMu.Lock; taking the read lock first guarantees we see
	// a consistent (keydir, segments) view — without this, a stale fileID
	// captured before compaction could be looked up in a post-compaction
	// segments map and produce a spurious ErrKeyNotFound. Also covers the
	// original 3b.i guarantee that Close cannot close the fd between our
	// lookup and pread.
	db.segmentsMu.RLock()
	defer db.segmentsMu.RUnlock()
	// Re-check closed under the lock. Close acquires segmentsMu.Lock
	// before nilling out segments, so by the time we hold the read lock
	// either (a) Close hasn't run — segments is intact and the lookup
	// proceeds, or (b) Close has fully torn down — we must surface
	// ErrDBClosed instead of mis-reporting a seeded key as ErrKeyNotFound
	// just because its segment file is gone.
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	entry, ok := db.keydir.get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	seg, ok := db.segments[entry.fileID]
	if !ok {
		// Should be unreachable under segmentsMu.RLock: handleCompactCommit
		// updates keydir and segments atomically under the write lock, so a
		// keydir entry the reader can see must have a live segments entry.
		// Treat as a miss defensively rather than panicking.
		return nil, ErrKeyNotFound
	}

	buf := make([]byte, entry.valueLen)
	if entry.valueLen > 0 {
		if err := seg.readAt(buf, entry.valuePos); err != nil {
			return nil, fmt.Errorf("engine: read value: %w", err)
		}
	}
	return buf, nil
}

// ReadKeyRange invokes fn(key, value) for every live key k satisfying
// start <= k < end, in ascending key order (bytes.Compare). A nil start means
// "from the smallest key"; a nil end means "to the largest key".
//
// Returning false from fn stops iteration early and ReadKeyRange returns nil.
// If fn returns true for every match, ReadKeyRange returns nil after the last
// match. Any I/O error while reading a value is returned immediately.
//
// Snapshot semantics: the set of keys AND values iterated is fixed at the
// moment ReadKeyRange is called. Writes that land after the snapshot are
// not observed. We achieve this by materialising every value into memory
// under a single segmentsMu.RLock, then releasing the lock before invoking
// any callback. Holding the read lock across all preads is what locks out
// concurrent compaction commits and segment rotation, which would
// otherwise be free to swap segments mid-iteration and force us to choose
// between "silently drop the key" (violates snapshot) and "observe a
// newer write" (also violates snapshot).
//
// Releasing the lock before invoking fn means user code may freely call
// back into the DB (Put, Delete, Close, ReadKeyRange) without risking a
// lock-order deadlock — and a long-running fn does not block writes.
//
// Trade: peak memory during the call is O(sum of value sizes in range)
// plus O(m) key bytes. For ranges that span gigabytes of values this is
// not free; if callers need streaming semantics they can chunk the range
// themselves. Snapshot correctness is the priority here.
//
// Slices passed to fn are owned by ReadKeyRange and only valid until fn
// returns; copy them if you need to retain them.
//
// Cost: O(total_keys) to filter the keydir + O(m * log m) to sort the m
// matches + O(m) preads + O(sum value bytes) memory.
func (db *DB) ReadKeyRange(start, end []byte, fn func(key, value []byte) bool) error {
	if fn == nil {
		return errors.New("engine: ReadKeyRange: nil callback")
	}
	if db.closed.Load() {
		return ErrDBClosed
	}
	// Caller convenience: an inverted range [b, a) where a < b is empty.
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}

	// Snapshot + materialise under a single RLock. While we hold the lock,
	// handleCompactCommit (which needs segmentsMu.Lock) cannot publish a
	// new segment set, rotateActive cannot retire the active segment, and
	// Close cannot pull files out from under us — so every fileID in the
	// snapshot is guaranteed to resolve.
	db.segmentsMu.RLock()
	if db.closed.Load() {
		db.segmentsMu.RUnlock()
		return ErrDBClosed
	}
	pairs := db.keydir.snapshotRange(start, end)
	if len(pairs) == 0 {
		db.segmentsMu.RUnlock()
		return nil
	}

	type materialisedPair struct {
		key   []byte
		value []byte
	}
	materialised := make([]materialisedPair, 0, len(pairs))
	var readErr error
	for _, p := range pairs {
		seg, ok := db.segments[p.entry.fileID]
		if !ok {
			// Impossible under RLock: handleCompactCommit and rotateActive
			// both require segmentsMu.Lock to remove a segment, and the
			// snapshot was taken under this same RLock. Treat as a hard
			// invariant violation rather than skipping silently.
			db.segmentsMu.RUnlock()
			return fmt.Errorf("engine: ReadKeyRange: keydir/segments out of sync for key %q (file %d)", p.key, p.entry.fileID)
		}
		n := int(p.entry.valueLen)
		val := make([]byte, n)
		if n > 0 {
			if err := seg.readAt(val, p.entry.valuePos); err != nil {
				readErr = fmt.Errorf("engine: ReadKeyRange read value for key %q: %w", p.key, err)
				break
			}
		}
		materialised = append(materialised, materialisedPair{key: p.key, value: val})
	}
	db.segmentsMu.RUnlock()
	if readErr != nil {
		return readErr
	}

	// Invoke callbacks without the lock so fn can mutate the DB.
	for _, m := range materialised {
		if !fn(m.key, m.value) {
			return nil
		}
	}
	return nil
}

// writerLoop is the single goroutine that owns the active segment and serves
// every mutating request. It is started by Open and exits when done is
// closed (after draining any requests it has already accepted on the
// channel).
//
// Group commit: after handling one request, the loop opportunistically pulls
// up to writeQueueDepth-1 additional requests already sitting in the channel
// and appends them too. At the end of the burst it issues exactly one fsync
// (under SyncOnPut=true) before replying to all of them. This turns a burst
// of N independent fsyncs into one — a substantial win on Darwin where
// F_FULLFSYNC is expensive.
func (db *DB) writerLoop() {
	defer db.writerWG.Done()

	// Reused across iterations so the slice header doesn't escape to the heap.
	burst := make([]*writeRequest, 0, db.opts.WriteQueueDepth)

	for {
		// Wait for the next request, or for shutdown.
		var first *writeRequest
		select {
		case first = <-db.reqCh:
		case <-db.done:
			// Shutdown: drain any requests already in the channel buffer so
			// callers that successfully sent are not stranded.
			db.drainAfterClose()
			return
		}

		burst = append(burst[:0], first)
		// Opportunistically grab more requests already queued. Non-blocking
		// drain so we do not wait for stragglers — group commit should never
		// add latency to the first writer in the burst.
	drain:
		for len(burst) < cap(burst) {
			select {
			case req := <-db.reqCh:
				burst = append(burst, req)
			default:
				break drain
			}
		}

		db.processBurst(burst)
	}
}

// processBurst appends every request in the burst to the active segment,
// updates the keydir for each, performs a single fsync if SyncOnPut is set,
// and then replies to every caller. If a per-request error occurs (e.g.
// encoding), only that caller is notified; the rest continue.
//
// Active-segment fsync accounting is per-request, not OR'd across the
// burst: only requests that actually appended bytes to the active segment
// depend on the burst fsync, so an fsync failure must only be reported to
// THOSE callers. A compaction-commit request (writeKindCompactCommit)
// does not touch the active segment at all — its durability barrier is
// its own manifest write inside handleCompactCommit — and so it must not
// be poisoned by an unrelated active-segment fsync error, which would
// otherwise cause the compactor's caller to roll back a manifest that is
// already committed on disk and corrupt the on-disk state.
func (db *DB) processBurst(burst []*writeRequest) {
	results := make([]error, len(burst))
	appendedActive := make([]bool, len(burst))
	anyActive := false

	for i, req := range burst {
		touchedActive, err := db.handleOne(req)
		results[i] = err
		if err == nil && touchedActive {
			appendedActive[i] = true
			anyActive = true
		}
	}

	// One fsync for the whole burst (if anything appended to the active).
	if anyActive && db.opts.SyncOnPut {
		if err := db.active.sync(); err != nil {
			// Only poison the callers whose append depended on this fsync.
			// Requests that did not touch the active (e.g. compact commits)
			// have already durably persisted their state via a different
			// path and must not be rolled back by an unrelated fsync error.
			wrapped := fmt.Errorf("engine: fsync: %w", err)
			for i := range results {
				if appendedActive[i] {
					results[i] = wrapped
				}
			}
		}
	}

	for i, req := range burst {
		req.reply <- results[i]
	}
}

// handleOne is the per-request append path. Runs only on the writer
// goroutine; no locks needed for writer-private state. Takes segmentsMu.Lock
// only when rotating.
//
// Returns (appendedActive, err). appendedActive is true iff the request
// caused bytes to be written to the active segment (and therefore depends
// on the burst's active-segment fsync for durability). A successful
// compaction commit, which writes only the manifest, returns false.
func (db *DB) handleOne(req *writeRequest) (bool, error) {
	// Sticky write-disable from a prior uncertain manifest publish.
	// submit() rejects new arrivals, but a burst already drained from
	// reqCh bypasses that gate, so every request in the burst must
	// re-check the flag before touching segments or the keydir. Any
	// further append (or rotation, or compact commit) would compound
	// the durability uncertainty we already cannot recover from.
	if db.writesDisabled.Load() {
		return false, ErrWritesDisabled
	}
	if req.kind == writeKindBatch {
		return db.handleBatch(req)
	}
	if req.kind == writeKindCompactCommit {
		// Compact commit has its own durability barrier (writeManifest);
		// it does not touch the active segment, so appendedActive=false.
		return false, db.handleCompactCommit(req.compact)
	}
	rec := record{
		key: req.key,
	}
	switch req.kind {
	case writeKindPut:
		rec.flag = recordFlagPut
		rec.value = req.value
	case writeKindDelete:
		rec.flag = recordFlagTombstone
	default:
		return false, fmt.Errorf("engine: writer: unknown kind %d", req.kind)
	}

	// Monotonic timestamp. Owned exclusively by the writer; no lock.
	now := time.Now().UnixNano()
	if now <= db.lastTstamp {
		now = db.lastTstamp + 1
	}
	db.lastTstamp = now
	rec.tstamp = now

	// Rotation. MaxSegmentSize is the *threshold*: rotate if appending this
	// record would push us past it. Single-record writes never need to
	// exceed the threshold (batches will, in 3b.ii).
	if db.active.size.Load()+int64(rec.encodedSize()) > db.opts.MaxSegmentSize {
		if err := db.rotateActive(); err != nil {
			return false, err
		}
	}

	// Grow the reusable encode buffer if needed.
	need := rec.encodedSize()
	if cap(db.encBuf) < need {
		db.encBuf = make([]byte, need)
	} else {
		db.encBuf = db.encBuf[:need]
	}
	if _, err := rec.encode(db.encBuf); err != nil {
		return false, err
	}

	offset, err := db.active.append(db.encBuf)
	if err != nil {
		return false, err
	}

	// Flush bufio so concurrent readers can pread the bytes we just appended.
	// This is NOT the durability barrier (that is fsync, in processBurst).
	// It only moves bytes from userspace into the kernel page cache, which is
	// enough for in-process readAt to observe them.
	if err := db.active.flush(); err != nil {
		return false, err
	}

	// Update the keydir last, so any concurrent reader that observes the
	// keydir entry is guaranteed to find the bytes via pread.
	if rec.flag == recordFlagTombstone {
		db.keydir.delete(rec.key)
	} else {
		db.keydir.put(rec.key, keydirEntry{
			fileID:   db.active.id,
			valuePos: offset + int64(recordHeaderSize+len(rec.key)),
			valueLen: uint32(len(rec.value)),
			tstamp:   rec.tstamp,
		})
	}
	// Replication hook: publish the byte-identical record the writer
	// just appended. No-op when ReplicationBufferSize == 0; never blocks
	// (overflow increments Stats().ReplicationLagDropped). See
	// internal/engine/replication.go for the design rationale.
	db.publishRecord(db.encBuf)
	return true, nil
}

// handleBatch is the writer-side append path for BatchPut. Same ownership
// rules as handleOne: writer-private state is touched without locks, and
// segmentsMu.Lock is taken only on rotation.
//
// Returns (appendedActive, err); appendedActive is true on a successful
// append (a batch always touches the active segment).
func (db *DB) handleBatch(req *writeRequest) (bool, error) {
	now := time.Now().UnixNano()
	if now <= db.lastTstamp {
		now = db.lastTstamp + 1
	}
	db.lastTstamp = now

	encodedSize := recordHeaderSize + encodedBatchBodySize(req.entries)

	// Rotate only if the active segment has already been written to and
	// appending this record would push it past the threshold. If the active
	// is empty, we never rotate — a batch larger than MaxSegmentSize must
	// live in one segment to preserve atomicity, and abandoning an empty
	// segment just to create another oversize one is wasted I/O.
	if db.active.size.Load() > 0 && db.active.size.Load()+int64(encodedSize) > db.opts.MaxSegmentSize {
		if err := db.rotateActive(); err != nil {
			return false, err
		}
	}

	if cap(db.encBuf) < encodedSize {
		db.encBuf = make([]byte, encodedSize)
	} else {
		db.encBuf = db.encBuf[:encodedSize]
	}
	if _, err := encodeBatchRecord(db.encBuf, now, req.entries); err != nil {
		return false, err
	}

	offset, err := db.active.append(db.encBuf)
	if err != nil {
		return false, err
	}
	if err := db.active.flush(); err != nil {
		return false, err
	}

	// Build the keydir ops for every entry, then apply them all under a
	// single keydir write lock. This is what makes the batch atomic to
	// concurrent readers: a Get racing with BatchPut observes either none
	// or all of the batch, never a prefix. Duplicate keys naturally
	// last-win because we apply in order.
	ops := make([]keydirOp, len(req.entries))
	bodyOff := batchCountSize // offset into the body (after the count prefix)
	for i := range req.entries {
		e := &req.entries[i]
		valLen := 0
		if !e.Delete {
			valLen = len(e.Value)
		}
		valuePos := offset + int64(recordHeaderSize+bodyOff+batchEntryHeaderSize+len(e.Key))
		if e.Delete {
			ops[i] = keydirOp{key: e.Key, delete: true}
		} else {
			ops[i] = keydirOp{
				key: e.Key,
				entry: keydirEntry{
					fileID:   db.active.id,
					valuePos: valuePos,
					valueLen: uint32(valLen),
					tstamp:   now,
				},
			}
		}
		bodyOff += batchEntryHeaderSize + len(e.Key) + valLen
	}
	db.keydir.applyBatch(ops)
	// Replication hook: a BATCH lands as one record on the wire, same as
	// it does on disk. See handleOne for the rationale.
	db.publishRecord(db.encBuf)
	return true, nil
}

// rotateActive seals the current active segment and opens a fresh one.
// Runs on the writer goroutine. Takes segmentsMu.Lock briefly to publish
// the new segment to readers.
//
// Manifest discipline: the new segment id must be durable in the manifest
// BEFORE any append lands on it. Otherwise a host crash after the first
// acked write to the new segment would lose data on restart — the new id
// would not be in the manifest and reconcileManifest would treat the file
// as an orphan. We therefore: create the file, fsync the data directory so
// the new .seg dirent is durable (syncDir, pre-manifest barrier), write
// the manifest with the new id appended, then publish under segmentsMu.
// The pre-manifest syncDir + the manifest tmp→rename→dir-fsync together
// form the durability barrier; an O(N) JSON serialization per rotation is
// trivial against the cost of the new-file fsync and is amortised over
// many writes per segment.
func (db *DB) rotateActive() error {
	if err := db.active.sync(); err != nil {
		return err
	}
	nextID := db.nextID.Add(1) - 1
	next, err := createSegment(db.opts.Dir, nextID)
	if err != nil {
		return err
	}
	// Durably commit the new .seg dirent BEFORE publishing the manifest.
	// createSegment only fsyncs the file's existence implicitly via
	// subsequent operations; without an explicit parent-dir fsync here
	// the manifest can land referencing next.id while the .seg dirent is
	// still in a writeback buffer. A host crash in that window leaves the
	// manifest pointing at a file the kernel never persisted — Open then
	// fails with "manifest live segment missing".
	dirSyncErr := syncDir(db.opts.Dir)
	if hook := testRotatePreManifestSyncHook; hook != nil && dirSyncErr == nil {
		dirSyncErr = hook()
	}
	if dirSyncErr != nil {
		_ = next.close()
		_ = os.Remove(next.path)
		return fmt.Errorf("rotate: fsync data dir before manifest: %w", dirSyncErr)
	}
	// Compute the new live set under the segments RLock so we observe a
	// consistent snapshot. The set includes the about-to-be-published new
	// segment id; if writeManifest fails, we delete the orphan file and
	// bail without mutating any in-memory state.
	db.segmentsMu.RLock()
	live := make([]uint32, 0, len(db.segments)+1)
	for id := range db.segments {
		live = append(live, id)
	}
	db.segmentsMu.RUnlock()
	live = append(live, next.id)
	if err := writeManifest(db.opts.Dir, live, next.id); err != nil {
		if errors.Is(err, ErrManifestPublishedButUncertain) {
			// The new MANIFEST is visible on disk and references
			// next.id, but the directory fsync did NOT confirm. A
			// host crash here can revert the rename, in which case
			// the previous manifest stands and any record we wrote
			// into next would become orphan data the next Open's
			// sweepOrphans would discard — silent loss of an
			// acknowledged write.
			//
			// We cannot unlink next.path either: if the rename
			// held, the manifest still references next.id and
			// unlinking it would turn a recoverable durability gap
			// into permanent corruption.
			//
			// The only safe action is to enter a fatal write-
			// disabled state. next.path stays on disk; we close
			// our fd and do NOT install next as the active
			// segment, so no further writes can land in it.
			// Reads continue to serve from the prior sealed
			// segments. The operator restarts; Open's
			// sweepOrphans reconciles whichever side of the
			// rename survived.
			_ = next.close()
			db.writesDisabled.Store(true)
			db.log.Error("manifest published with uncertain durability; disabling writes",
				slog.String("op", "rotate"),
				slog.Uint64("new_active_id", uint64(next.id)),
				slog.String("path", next.path),
				slog.String("err", err.Error()))
			return fmt.Errorf("rotate: %w: %v", ErrWritesDisabled, err)
		}
		_ = next.close()
		_ = os.Remove(next.path)
		return fmt.Errorf("rotate: persist manifest: %w", err)
	}
	db.segmentsMu.Lock()
	db.segments[next.id] = next
	prevID := db.active.id
	db.active = next
	db.segmentsMu.Unlock()
	db.log.Info("segment rotation",
		slog.Uint64("sealed_id", uint64(prevID)),
		slog.Uint64("active_id", uint64(next.id)))
	return nil
}

// drainAfterClose is called by the writer loop when done is closed. It
// drains and services any requests already in the channel buffer (callers
// who managed to enqueue before submitMu was acquired by Close), so they
// are not stranded with no reply. With submitMu serialization, no new
// requests can arrive after done is closed.
func (db *DB) drainAfterClose() {
	burst := make([]*writeRequest, 0, cap(db.reqCh))
drain:
	for {
		select {
		case req := <-db.reqCh:
			burst = append(burst, req)
		default:
			break drain
		}
	}
	if len(burst) > 0 {
		db.processBurst(burst)
	}
}

// listSegments returns segment ids found in dir, sorted ascending.
func listSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != segmentFileExt {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(name, "%010d"+segmentFileExt, &id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	// Sort ascending so recovery replays oldest-to-newest.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	return ids, nil
}
