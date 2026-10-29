//go:build windows

package plugin

import (
	"os/exec"
	"syscall"
)

// configurePluginProcessAttributes 防止控制台子系统的插件在 Windows 上额外弹出窗口。
func configurePluginProcessAttributes(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000}
}
