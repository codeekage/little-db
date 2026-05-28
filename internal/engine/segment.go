package engine

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

const segmentFileExt = ".seg"

// segmentFilename builds the canonical filename for a given numeric segment id.
// Zero-padded so directory listings sort lexicographically by age.
func segmentFilename(id uint32) string {
	return fmt.Sprintf("%010d%s", id, segmentFileExt)
}

// segment represents one append-only data file.
//
// Concurrency model:
//   - Exactly one writer (the engine's writer goroutine) appends through
//     `bw` and mutates `size`. All append/flush/sync calls happen on that
//     one goroutine; no lock is taken on the segment itself.
//   - Any number of readers call readAt on the underlying file. POSIX
//     pread (which os.File.ReadAt uses under the hood) is safe for concurrent
//     callers because it does not mutate the shared file offset. Reader
//     synchronisation against Close is the DB's responsibility via
//     segmentsMu, not this type's.
//   - `size` is atomic so observability paths (e.g. DB.Stats) can read it
//     from outside the writer without racing the writer's appends.
type segment struct {
	id   uint32
	path string
	file *os.File
	bw   *bufio.Writer
	size atomic.Int64 // bytes appended (and possibly still in bw's buffer)
}

// createSegment opens a new segment file in dir with the given id, ready for appends.
// Fails if the file already exists, which protects against id collisions.
func createSegment(dir string, id uint32) (*segment, error) {
	path := filepath.Join(dir, segmentFilename(id))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment %d: %w", id, err)
	}
	return &segment{
		id:   id,
		path: path,
		file: f,
		bw:   bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// openSegmentReadOnly opens an existing segment for reads only (used for
// immutable segments and during recovery).
func openSegmentReadOnly(path string, id uint32) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &segment{
		id:   id,
		path: path,
		file: f,
	}
	s.size.Store(info.Size())
	return s, nil
}

// openSegmentReadWrite opens an existing segment for both reads and appends.
// Used at startup so that recovery can truncate trailing torn records, and so
// that the manifest-named active pre-existing segment can be reused for new
// writes (avoids creating an empty file on every restart).
func openSegmentReadWrite(path string, id uint32) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &segment{
		id:   id,
		path: path,
		file: f,
	}
	s.size.Store(info.Size())
	return s, nil
}

// append writes one encoded record and returns the byte offset at which it begins.
// It does NOT fsync; the caller decides when to flush+sync.
func (s *segment) append(buf []byte) (offset int64, err error) {
	offset = s.size.Load()
	n, err := s.bw.Write(buf)
	if err != nil {
		return 0, err
	}
	s.size.Add(int64(n))
	return offset, nil
}

// flush pushes buffered bytes to the OS without forcing a disk sync.
func (s *segment) flush() error {
	if s.bw == nil {
		return nil
	}
	return s.bw.Flush()
}

// sync flushes the bufio buffer and then fsyncs the underlying file descriptor.
// On Darwin this calls fcntl(F_FULLFSYNC) so the drive's write cache is
// flushed too; on Linux/BSD it is a plain fsync(2). See fsync_darwin.go and
// fsync_other.go for the platform-specific implementations.
//
// This is the durability barrier; callers must ensure no further writes are
// interleaved with this call.
func (s *segment) sync() error {
	if err := s.flush(); err != nil {
		return err
	}
	return fullSync(s.file)
}

// readAt reads exactly len(p) bytes at offset off from the segment file.
// Safe to call concurrently with appends to a different segment, and with other
// readAt calls on the same segment, because it uses pread under the hood.
func (s *segment) readAt(p []byte, off int64) error {
	_, err := s.file.ReadAt(p, off)
	return err
}

func (s *segment) close() error {
	if s.bw != nil {
		_ = s.bw.Flush()
	}
	return s.file.Close()
}
