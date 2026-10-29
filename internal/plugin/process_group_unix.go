//go:build darwin || linux

package plugin

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Keep a plugin and any helpers it launches in an isolated process group so a
// malformed protocol stream cannot leave a Node/App Server child behind.
func configurePluginProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminatePluginProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
