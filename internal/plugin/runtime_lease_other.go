//go:build !darwin && !linux

package plugin

import "fmt"

// Windows 由 Desktop Job Object 回收插件进程树，沿用旧行为把遗留租约视为可清理。
// 占位标识仅用于让新记录可被 claim/release，不参与跨进程终止决策。
func runtimeLeaseProcessInspection(pid int) (runtimeLeaseProcess, error) {
	return runtimeLeaseProcess{StartIdentifier: fmt.Sprintf("unmanaged:%d", pid)}, nil
}

func terminatePluginProcessPID(int) error { return nil }
