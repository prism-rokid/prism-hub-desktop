//go:build darwin || linux

package plugin

import (
	"errors"
	"fmt"
	"syscall"
)

func terminatePluginProcessPID(pid int) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return fmt.Errorf("terminate plugin process group %d: %w", pid, err)
}
