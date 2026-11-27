//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package filestore

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"runtime"
)

// errUnsupported means the system has no flock, which the store locks
// directories with.
var errUnsupported = errors.New("not supported on " + runtime.GOOS)

func lockDirectory(context.Context, *os.File) error { return errUnsupported }

func directoryNotEmpty(err error) bool { return errors.Is(err, fs.ErrExist) }
