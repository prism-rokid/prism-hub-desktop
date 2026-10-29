//go:build !windows

package plugin

import "os/exec"

// configurePluginProcessAttributes 在非 Windows 平台不需要额外的创建属性。
func configurePluginProcessAttributes(_ *exec.Cmd) {}
