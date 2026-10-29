//go:build windows

package host

import (
	"fmt"
	"syscall"
)

// SetThreadExecutionState flags (winbase.h). ES_CONTINUOUS makes the flags
// persist until the next call; ES_SYSTEM_REQUIRED prevents the system from
// sleeping; ES_DISPLAY_REQUIRED keeps the display awake so remote desktop
// automation can still observe the screen instead of hitting a locked session.
const (
	esContinuous      = 0x80000000
	esSystemRequired  = 0x00000001
	esDisplayRequired = 0x00000002
)

var (
	kernel32DLL                 = syscall.NewLazyDLL("kernel32.dll")
	procSetThreadExecutionState = kernel32DLL.NewProc("SetThreadExecutionState")
)

type platformKeepAwake struct {
	active bool
}

func detectPlatformKeepAwakeCapability() (KeepAwakeCapability, bool) {
	// SetThreadExecutionState is part of every supported Windows version, so
	// keep-awake is always available on Windows.
	return KeepAwakeCapability{Supported: true, Method: "SetThreadExecutionState"}, true
}

func startPlatformKeepAwake() (*platformKeepAwake, bool, error) {
	// The execution state is per-thread and ES_CONTINUOUS keeps it active until
	// the thread ends or the flags are cleared. The caller's runtime goroutine
	// (and therefore its OS thread) lives for the whole bridge session.
	result, _, err := procSetThreadExecutionState.Call(uintptr(esContinuous | esSystemRequired | esDisplayRequired))
	if result == 0 {
		return nil, true, fmt.Errorf("keep-awake: SetThreadExecutionState failed: %w", err)
	}
	return &platformKeepAwake{active: true}, true, nil
}

func platformKeepAwakeArgs(method string) []string {
	return nil
}

func (h *platformKeepAwake) Stop() error {
	if h == nil || !h.active {
		return nil
	}
	h.active = false
	// ES_CONTINUOUS alone clears the required-state bits, letting the system
	// sleep again.
	_, _, _ = procSetThreadExecutionState.Call(uintptr(esContinuous))
	return nil
}
