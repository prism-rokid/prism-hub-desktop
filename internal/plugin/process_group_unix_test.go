//go:build darwin || linux

package plugin

import (
	"context"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestProcessRuntimeTerminatesPluginProcessGroup(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	root := t.TempDir()
	rt, err := startProcessRuntime(context.Background(), root, Manifest{
		ID:        "demo-child-process",
		Transport: "process",
		Mode:      "test",
		Command:   []string{os.Args[0], "-test.run", "TestProcessRuntimeTerminatesPluginProcessGroup"},
	}, nil, false)
	if err != nil {
		t.Fatalf("start process runtime: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.call(ctx, "adapter.probe", map[string]any{}, nil); err == nil {
		t.Fatal("malformed plugin stdout must fail the pending RPC")
	}
	select {
	case <-rt.processDone:
	case <-ctx.Done():
		t.Fatal("runtime did not exit after malformed stdout")
	}
	pidBytes, err := os.ReadFile(root + "/child.pid")
	if err != nil {
		t.Fatalf("read child pid: %v", err)
	}
	pid, err := strconv.Atoi(string(pidBytes))
	if err != nil {
		t.Fatalf("parse child pid: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if err == syscall.ESRCH {
			return
		}
		if err != nil {
			t.Fatalf("inspect plugin child process: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("plugin child process %d survived runtime termination", pid)
}
