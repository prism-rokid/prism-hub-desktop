//go:build !windows

package agentlocation

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

func Command(ctx context.Context, executable string, args ...string) (*exec.Cmd, error) {
	return exec.CommandContext(ctx, executable, args...), nil
}

// PrepareService 为 Hub 所有的前台服务创建独立进程组。
func PrepareService(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func StopService(cmd *exec.Cmd, done <-chan struct{}) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	// 该进程组只包含本次 Hub 服务启动的进程。
	time.AfterFunc(2*time.Second, func() {
		select {
		case <-done:
			return
		default:
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	})
}
