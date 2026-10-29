//go:build darwin || linux

package plugin

import (
	"errors"
	"syscall"
)

func runtimeLeaseProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func terminatePluginProcessPID(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
