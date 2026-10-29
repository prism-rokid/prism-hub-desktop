//go:build !darwin && !linux

package plugin

import (
	"errors"
	"os"
	"os/exec"
)

// Windows process-tree management is validated separately. Until the Desktop
// shell owns a Job Object, always terminate the direct runtime process.
func configurePluginProcessGroup(_ *exec.Cmd) {}

func terminatePluginProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
