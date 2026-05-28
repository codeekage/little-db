package engine

// Stats is a point-in-time observability snapshot of the engine.
//
// The fields are intended for dashboards, health endpoints, and ad-hoc
// debugging — they are NOT a transactional read. Mutating callers can land
// between the moment Stats samples the segment list and the moment it samples
// the keydir, so KeyCount and BytesOnDisk are individually consistent but
// not jointly snapshotted.
type Stats struct {
	// KeyCount is the number of live keys in the keydir (tombstones are
	// excluded; only keys with a current value are counted).
	KeyCount uint64

	// BytesOnDisk is the sum of every live segment's logical size in
	// bytes (header + body for every record ever appended to that
	// segment, including superseded writes and tombstones that have not
	// been compacted away yet). Bytes still buffered by bufio but not yet
	// flushed to the kernel are included — the writer's group-commit
	// path flushes after every burst, so the buffer is small and the
	// number is close to the on-disk file size at all times.
	BytesOnDisk uint64

	// ReplicationLagDropped counts records the leader's replication
	// publisher dropped because the subscriber buffer was full. Always
	// zero when Options.ReplicationBufferSize == 0. A non-zero and
	// growing value is the canonical signal that a follower is falling
	// behind faster than it can catch up. See docs/replication.md §3.
	ReplicationLagDropped uint64
}

// Stats returns an observability snapshot of the engine.
//
// Stats acquires segmentsMu (read) and then keydir.mu (read), in that order
// (matching the engine-wide lock order). It is safe to call concurrently
// with Put/Delete/BatchPut/Get/ReadKeyRange/Compact.
//
// Closed-DB semantics: unlike Get/Put/Delete which return ErrDBClosed after
// Close, Stats returns the last-known values captured immediately before
// teardown. Operators reading dashboards or scraping a /stats endpoint
// should not have to special-case shutdown — a closed engine reports its
// final counters and the consumer can treat those as authoritative.
//
// If Stats is called on a DB that has never been opened (zero-value DB),
// or on a closed DB whose snapshot was somehow never captured, it returns
// the zero Stats value.
func (db *DB) Stats() Stats {
	db.segmentsMu.RLock()
	defer db.segmentsMu.RUnlock()

	if db.segments == nil {
		if s := db.cachedStats.Load(); s != nil {
			return *s
		}
		return Stats{}
	}

	var bytes uint64
	for _, seg := range db.segments {
		bytes += uint64(seg.size.Load())
	}
	keys := uint64(db.keydir.size())
	return Stats{KeyCount: keys, BytesOnDisk: bytes, ReplicationLagDropped: db.replicationDropped.Load()}
}

// statsLocked computes a Stats snapshot. The caller must hold
// segmentsMu (at least read). Used by Close to populate cachedStats
// before tearing down db.segments.
func (db *DB) statsLocked() Stats {
	var bytes uint64
	for _, seg := range db.segments {
		bytes += uint64(seg.size.Load())
	}
	return Stats{
		KeyCount:              uint64(db.keydir.size()),
		BytesOnDisk:           bytes,
		ReplicationLagDropped: db.replicationDropped.Load(),
	}
}
