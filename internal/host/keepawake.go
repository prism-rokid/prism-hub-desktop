package host

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

type KeepAwakeCapability struct {
	Supported bool
	Method    string
}

type KeepAwakeHandle struct {
	ctx     context.Context
	logger  *slog.Logger
	method  string
	mu      sync.Mutex
	cmd     *exec.Cmd
	native  *platformKeepAwake
	stopped bool
}

func DetectKeepAwakeCapability() KeepAwakeCapability {
	if capability, ok := detectPlatformKeepAwakeCapability(); ok {
		return capability
	}
	switch runtime.GOOS {
	case "darwin":
		if _, err := exec.LookPath("caffeinate"); err == nil {
			return KeepAwakeCapability{Supported: true, Method: "caffeinate"}
		}
	case "linux":
		if _, err := exec.LookPath("systemd-inhibit"); err == nil {
			return KeepAwakeCapability{Supported: true, Method: "systemd-inhibit"}
		}
	}
	return KeepAwakeCapability{}
}

func StartKeepAwake(ctx context.Context, logger *slog.Logger) (*KeepAwakeHandle, error) {
	capability := DetectKeepAwakeCapability()
	if !capability.Supported {
		return nil, fmt.Errorf("keep-awake unsupported on %s", runtime.GOOS)
	}
	handle := &KeepAwakeHandle{ctx: ctx, logger: logger, method: capability.Method}
	if native, handled, err := startPlatformKeepAwake(); handled {
		if err != nil {
			return nil, err
		}
		handle.native = native
		return handle, nil
	}

	var args []string
	switch capability.Method {
	case "caffeinate":
		args = platformKeepAwakeArgs(capability.Method)
	case "systemd-inhibit":
		args = []string{
			"--what=idle:sleep",
			"--why=Prism remote control",
			"--mode=block",
			"/bin/sh",
			"-lc",
			"while :; do sleep 3600; done",
		}
	default:
		return nil, fmt.Errorf("unknown keep-awake method: %s", capability.Method)
	}
	if err := handle.start(args); err != nil {
		return nil, err
	}
	go handle.supervise(args)
	return handle, nil
}

func (h *KeepAwakeHandle) start(args []string) error {
	// The process lifecycle is explicitly owned by Stop. Do not use
	// CommandContext here: a transient runtime-context cancellation must not
	// silently drop the operating-system power assertion while the Hub keeps
	// serving remote clients.
	cmd := exec.Command(h.method, args...)
	cmd.Args[0] = "prism-" + h.method
	if err := cmd.Start(); err != nil {
		return err
	}
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		_ = cmd.Process.Kill()
		return context.Canceled
	}
	h.cmd = cmd
	h.mu.Unlock()
	if h.logger != nil && cmd.Process != nil {
		h.logger.Info("keep-awake process started", "method", h.method, "pid", cmd.Process.Pid)
	}
	return nil
}

func (h *KeepAwakeHandle) supervise(args []string) {
	for {
		h.mu.Lock()
		cmd := h.cmd
		stopped := h.stopped
		h.mu.Unlock()
		if stopped || cmd == nil {
			return
		}
		err := cmd.Wait()
		h.mu.Lock()
		if h.cmd == cmd {
			h.cmd = nil
		}
		stopped = h.stopped
		h.mu.Unlock()
		if stopped {
			return
		}
		if h.ctx.Err() != nil {
			if h.logger != nil {
				h.logger.Info("keep-awake stopped", "method", h.method)
			}
			return
		}
		if h.logger != nil {
			h.logger.Warn("keep-awake exited unexpectedly; restarting", "method", h.method, "err", err)
		}
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			h.mu.Lock()
			stopped = h.stopped
			h.mu.Unlock()
			if stopped {
				return
			}
			if err := h.start(args); err != nil {
				if h.logger != nil {
					h.logger.Warn("keep-awake restart failed", "method", h.method, "err", err)
				}
				continue
			}
			break
		}
	}
}

func (h *KeepAwakeHandle) Stop() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	h.stopped = true
	cmd := h.cmd
	native := h.native
	h.native = nil
	h.mu.Unlock()
	if native != nil {
		return native.Stop()
	}
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}

func (h *KeepAwakeHandle) Method() string {
	if h == nil {
		return ""
	}
	return h.method
}
