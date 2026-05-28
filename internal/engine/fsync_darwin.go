//go:build darwin

package engine

import (
	"fmt"
	"os"
	"syscall"
)

// On Darwin, fsync(2) does NOT guarantee that the bytes have reached the
// physical medium — it returns once the bytes are queued to the drive's
// internal cache. F_FULLFSYNC asks the drive to flush its write cache, which
// is the real durability primitive on macOS.
//
// We invoke fcntl(fd, F_FULLFSYNC, 0) directly via the syscall package with the
// constant inlined, because the assessment restricts us to the standard
// library (no golang.org/x/sys/unix). The constant is stable in the macOS
// kernel headers (sys/fcntl.h: #define F_FULLFSYNC 51).
const darwinFFullFsync = 51

func fullSync(f *os.File) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), darwinFFullFsync, 0); errno != 0 {
		return fmt.Errorf("F_FULLFSYNC: %w", errno)
	}
	return nil
}

// flockExclusiveNonblocking takes an advisory exclusive lock on the file
// descriptor. Returns immediately with an error if another process holds it.
func flockExclusiveNonblocking(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock data dir: %w (another process may hold it)", err)
	}
	return nil
}
