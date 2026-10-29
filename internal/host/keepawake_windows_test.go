//go:build windows

package host

import (
	"context"
	"testing"
)

func TestDetectKeepAwakeCapabilityOnWindows(t *testing.T) {
	capability := DetectKeepAwakeCapability()
	if !capability.Supported || capability.Method != "SetThreadExecutionState" {
		t.Fatalf("expected SetThreadExecutionState keep-awake on Windows, got %+v", capability)
	}
}

func TestStartAndStopKeepAwakeOnWindows(t *testing.T) {
	handle, err := StartKeepAwake(context.Background(), nil)
	if err != nil {
		t.Fatalf("start keep-awake: %v", err)
	}
	if handle.Method() != "SetThreadExecutionState" {
		t.Fatalf("unexpected keep-awake method: %q", handle.Method())
	}
	if err := handle.Stop(); err != nil {
		t.Fatalf("stop keep-awake: %v", err)
	}
	// Stop must be idempotent: the bridge runtime also stops on shutdown.
	if err := handle.Stop(); err != nil {
		t.Fatalf("second stop must be a no-op: %v", err)
	}
}
