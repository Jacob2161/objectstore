//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package filestore

import (
	"context"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// lockPollInterval is how often a lock wait tries again.
const lockPollInterval = 5 * time.Millisecond

// lockDirectory takes an exclusive flock on the open directory, waiting
// until it is free or ctx ends.
func lockDirectory(ctx context.Context, file *os.File) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			return err
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// directoryNotEmpty reports a removal that failed because the directory
// holds entries.
func directoryNotEmpty(err error) bool {
	return errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST)
}
