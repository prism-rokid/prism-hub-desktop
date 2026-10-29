//go:build windows

package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"rokid-prism-hub/internal/host"
)

type Lock struct {
	path string
	file *os.File
}

func DefaultPath() string {
	return host.DefaultDataPath("daemon.lock")
}

// processAlive reports whether a Windows process with the given PID is still
// running. The Windows hub lock is an exclusive lock file that records its
// owner PID; when that PID is gone the lock is stale and can be reclaimed.
func processAlive(pid uint32) bool {
	const (
		processQueryLimitedInformation = 0x1000
		stillActive                    = 259 // STILL_ACTIVE
	)
	handle, err := syscall.OpenProcess(processQueryLimitedInformation, false, pid)
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle)
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(handle, &exitCode); err != nil {
		return false
	}
	return exitCode == stillActive
}

// removeStaleLock reclaims a lock file whose recorded owner is no longer
// alive. Unlike the flock-based Unix lock, the Windows lock is an exclusive
// file creation (O_EXCL): a sidecar terminated without running Release (a
// force kill by the desktop shell, a crash, or a hard reboot) leaves the file
// behind, which would otherwise block every later Hub start forever.
func removeStaleLock(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		// A lock file without a readable PID may belong to a process that is
		// between creating the file and writing its PID. Only reclaim it once
		// it has had a grace period to complete that write.
		info, statErr := os.Stat(path)
		if statErr != nil || time.Since(info.ModTime()) < 5*time.Second {
			return false
		}
		return os.Remove(path) == nil
	}
	if processAlive(uint32(pid)) {
		return false
	}
	return os.Remove(path) == nil
}

func Acquire(path string) (*Lock, error) {
	path = filepath.Clean(path)
	if path == "" || path == "." {
		return nil, fmt.Errorf("daemonlock: path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("daemonlock: mkdir %s: %w", filepath.Dir(path), err)
	}
	for attempt := 0; ; attempt++ {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			if _, werr := fmt.Fprintf(file, "%d\n", os.Getpid()); werr != nil {
				_ = file.Close()
				_ = os.Remove(path)
				return nil, fmt.Errorf("daemonlock: write pid %s: %w", path, werr)
			}
			return &Lock{path: path, file: file}, nil
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("daemonlock: open %s: %w", path, err)
		}
		// The lock file exists; reclaim it once when its owner is gone.
		if attempt == 0 && removeStaleLock(path) {
			continue
		}
		return nil, ErrLocked
	}
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	_ = os.Remove(l.path)
	return err
}
