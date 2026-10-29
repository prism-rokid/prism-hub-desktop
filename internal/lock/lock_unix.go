//go:build !windows

package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"rokid-prism-hub/internal/host"
)

type Lock struct {
	path string
	file *os.File
}

func DefaultPath() string {
	return host.DefaultDataPath("daemon.lock")
}

func Acquire(path string) (*Lock, error) {
	path = filepath.Clean(path)
	if path == "" || path == "." {
		return nil, fmt.Errorf("daemonlock: path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("daemonlock: mkdir %s: %w", filepath.Dir(path), err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemonlock: open %s: %w", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("daemonlock: flock %s: %w", path, err)
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("daemonlock: truncate %s: %w", path, err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("daemonlock: seek %s: %w", path, err)
	}
	if _, err := fmt.Fprintf(file, "%d\n", os.Getpid()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("daemonlock: write pid %s: %w", path, err)
	}
	return &Lock{path: path, file: file}, nil
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	_ = os.Remove(l.path)
	return err
}
