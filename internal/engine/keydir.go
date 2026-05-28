package engine

import (
	"bytes"
	"sort"
	"sync"
)

// keydirEntry locates the latest value for a key on disk.
// It is small and fixed-size so the in-memory index stays compact even for
// datasets much larger than RAM.
type keydirEntry struct {
	fileID   uint32 // segment file identifier
	valuePos int64  // byte offset of the *value* (not the record) within the segment
	valueLen uint32 // length of the value in bytes
	tstamp   int64  // for tie-breaking during recovery
}

// keydir is the in-memory index. It maps key bytes to the on-disk location of
// the latest write. The current implementation uses a plain map plus an RWMutex;
// range scans (ReadKeyRange) snapshot and sort matching keys on demand.
//
// Trade-off: O(1) point lookups and writes; range scans cost
// O(total_keys + matches * log matches) — a full pass over the keydir to
// filter, plus a sort of the matching subset. A skiplist would give
// O(log N + matches) ordered access; see README "Trade-offs" section.
type keydir struct {
	mu    sync.RWMutex
	index map[string]keydirEntry
}

func newKeydir() *keydir {
	return &keydir{index: make(map[string]keydirEntry)}
}

func (k *keydir) get(key []byte) (keydirEntry, bool) {
	k.mu.RLock()
	e, ok := k.index[string(key)] // string(key) on map lookup avoids allocation in modern Go
	k.mu.RUnlock()
	return e, ok
}

// put unconditionally replaces the entry. The caller is responsible for ensuring
// the entry corresponds to the latest write (during recovery this means comparing
// timestamps).
func (k *keydir) put(key []byte, e keydirEntry) {
	k.mu.Lock()
	k.index[string(key)] = e
	k.mu.Unlock()
}

// putIfNewer is used during recovery when records may be replayed out of order
// across segments (e.g. after compaction renames). Only the entry with the
// highest timestamp wins.
func (k *keydir) putIfNewer(key []byte, e keydirEntry) {
	k.mu.Lock()
	if existing, ok := k.index[string(key)]; !ok || e.tstamp >= existing.tstamp {
		k.index[string(key)] = e
	}
	k.mu.Unlock()
}

func (k *keydir) delete(key []byte) {
	k.mu.Lock()
	delete(k.index, string(key))
	k.mu.Unlock()
}

// compareAndSwap replaces the entry for key with newEntry, but only if the
// current entry equals expected (by full struct equality). Returns true if
// the swap happened.
//
// Used by the compactor to remap keys whose latest write lived in a
// retiring segment to the freshly-emitted merged segment. The CAS is
// essential because a concurrent writer can land a NEWER value for the
// same key between the moment compaction scanned it (and decided to copy
// it forward) and the moment of the commit swap. In that case keydir[key]
// no longer points to the retiring segment; we must not overwrite the
// newer entry with a now-stale compacted location, so CAS fails and the
// compacted copy becomes dead data (reclaimed by a future compaction).
func (k *keydir) compareAndSwap(key []byte, expected, newEntry keydirEntry) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.index[string(key)]
	if !ok || cur != expected {
		return false
	}
	k.index[string(key)] = newEntry
	return true
}

// keydirOp is one element of an atomic batch apply: either a put (delete=false)
// or a delete (delete=true). The entry field is meaningful only when delete is
// false.
type keydirOp struct {
	key    []byte
	entry  keydirEntry
	delete bool
}

// applyBatch unconditionally applies every op in order under a single write
// lock. This is the in-process side of BatchPut's atomicity contract:
//
//   - A single `get` call observes either the pre-batch or the post-batch
//     value for its key — never an intermediate state for that key.
//   - A `snapshotRange` call (used by ReadKeyRange) takes one RLock for the
//     entire scan and therefore observes either none or all of the batch
//     across every key in the range — never a prefix.
//
// Note: a caller that issues N separate `get` calls in a loop is NOT a
// snapshot — the writer can complete a whole batch between two of those
// calls. Cross-key atomic reads require ReadKeyRange.
//
// Cost trade-off: a long batch holds the keydir write lock for the duration
// of the apply, briefly stalling readers. We accept that because the public
// BatchPut contract promises whole-batch atomic visibility to snapshot
// readers, and BatchPut is already opt-in for callers willing to trade
// latency for atomicity.
func (k *keydir) applyBatch(ops []keydirOp) {
	if len(ops) == 0 {
		return
	}
	k.mu.Lock()
	for i := range ops {
		op := &ops[i]
		if op.delete {
			delete(k.index, string(op.key))
		} else {
			k.index[string(op.key)] = op.entry
		}
	}
	k.mu.Unlock()
}

func (k *keydir) size() int {
	k.mu.RLock()
	n := len(k.index)
	k.mu.RUnlock()
	return n
}

// rangePair is one keydir entry plus its key, used by snapshotRange to hand
// readers a self-contained, sorted view of a key interval.
type rangePair struct {
	key   []byte
	entry keydirEntry
}

// snapshotRange returns every (key, entry) where start <= key < end, sorted
// ascending by key. A nil start means "from the smallest key"; a nil end means
// "to the largest key" (i.e. open upper bound). The returned slice is owned by
// the caller and disjoint from the keydir's internal state, so subsequent
// writes do not mutate it.
//
// Cost: O(total_keys) to scan the map + O(matching_keys * log matching_keys)
// to sort the matches. The map scan is unavoidable because keydir is a hash
// map; only matches pay the sort cost. This is acceptable because Bitcask
// targets point lookups; large range scans should be infrequent.
func (k *keydir) snapshotRange(start, end []byte) []rangePair {
	k.mu.RLock()
	out := make([]rangePair, 0, len(k.index)/8+1) // rough guess; grows as needed
	for kstr, e := range k.index {
		kb := []byte(kstr)
		if start != nil && bytes.Compare(kb, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(kb, end) >= 0 {
			continue
		}
		out = append(out, rangePair{key: kb, entry: e})
	}
	k.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].key, out[j].key) < 0
	})
	return out
}
