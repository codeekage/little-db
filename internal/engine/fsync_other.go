//go:build linux || freebsd || netbsd || openbsd

package engine

import (
	"fmt"
	"os"
	"syscall"
)

// On Linux/BSD, plain fsync(2) is the real durability barrier on commodity
// filesystems (ext4, xfs, etc.) — it does flush the drive write cache when the
// hardware supports it and the OS is configured normally. So fullSync simply
// defers to os.File.Sync().
func fullSync(f *os.File) error {
	return f.Sync()
}

func flockExclusiveNonblocking(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock data dir: %w (another process may hold it)", err)
	}
	return nil
}
