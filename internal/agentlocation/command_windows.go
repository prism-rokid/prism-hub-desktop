//go:build windows

package agentlocation

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// 批处理文件除了参数引号外还需要 cmd 的外层引号。
// 百分号展开和嵌入引号无法在这里安全表达。
func Command(ctx context.Context, executable string, args ...string) (*exec.Cmd, error) {
	lower := strings.ToLower(executable)
	if !strings.HasSuffix(lower, ".cmd") && !strings.HasSuffix(lower, ".bat") {
		return exec.CommandContext(ctx, executable, args...), nil
	}
	values := append([]string{executable}, args...)
	for i, value := range values {
		if strings.ContainsAny(value, "\"\r\n%!") {
			return nil, fmt.Errorf("unsupported character in batch launcher argument")
		}
		values[i] = "\"" + value + "\""
	}
	shell := os.Getenv("ComSpec")
	if shell == "" {
		shell = "cmd.exe"
	}
	cmd := exec.CommandContext(ctx, shell)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CmdLine: syscall.EscapeArg(shell) + ` /d /v:off /s /c "` + strings.Join(values, " ") + `"`}
	return cmd, nil
}

func PrepareService(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= 0x00000200 // CREATE_NEW_PROCESS_GROUP
	cmd.SysProcAttr.HideWindow = true
}
func StopService(cmd *exec.Cmd, done <-chan struct{}) {
	select {
	case <-done:
		return
	default:
	}
	if cmd.Process == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stop := exec.CommandContext(ctx, "taskkill.exe", "/PID", fmt.Sprint(cmd.Process.Pid), "/T", "/F")
	stop.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = stop.Run()
}
