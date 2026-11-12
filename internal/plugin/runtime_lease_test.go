package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"rokid-prism-hub/internal/host"
)

func stubRuntimeLeaseProcessInspection(t *testing.T, inspect func(int) (runtimeLeaseProcess, error)) {
	t.Helper()
	previous := inspectRuntimeLeaseProcess
	inspectRuntimeLeaseProcess = inspect
	t.Cleanup(func() { inspectRuntimeLeaseProcess = previous })
}

func stubRuntimeLeaseTermination(t *testing.T, terminate func(int) error) {
	t.Helper()
	previous := terminateRuntimeLeaseProcess
	terminateRuntimeLeaseProcess = terminate
	t.Cleanup(func() { terminateRuntimeLeaseProcess = previous })
}

func TestRuntimeLeaseClaimsAndReleasesOnlyItsOwnRecord(t *testing.T) {
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	stubRuntimeLeaseProcessInspection(t, func(pid int) (runtimeLeaseProcess, error) {
		if pid != 12345 {
			return runtimeLeaseProcess{}, fmt.Errorf("unexpected pid %d", pid)
		}
		return runtimeLeaseProcess{Alive: true, StartIdentifier: "2026-11-10 10:00:00"}, nil
	})
	lease, err := prepareRuntimeLease("demo", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.claim(12345); err != nil {
		t.Fatal(err)
	}
	lease.release()
	if _, err := os.Stat(lease.path); !os.IsNotExist(err) {
		t.Fatalf("lease file remains after release: %v", err)
	}
}

func TestRuntimeLeaseDropsDeadStaleRecord(t *testing.T) {
	base := t.TempDir()
	t.Setenv(host.PrismDataDirEnv, base)
	stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
		return runtimeLeaseProcess{}, nil
	})
	path := filepath.Join(base, "runtime", "plugins", "prism-plugin-demo.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(runtimeLeaseRecord{PID: 999999, PluginID: "demo", SessionToken: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	lease, err := prepareRuntimeLease("demo", "session-2")
	if err != nil {
		t.Fatal(err)
	}
	if lease.path != path {
		t.Fatalf("unexpected lease path: %s", lease.path)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stale lease was not removed: %v", err)
	}
}

func TestRuntimeLeasePIDReuseRemovesRecordWithoutTerminatingReplacement(t *testing.T) {
	base := t.TempDir()
	t.Setenv(host.PrismDataDirEnv, base)
	path := filepath.Join(base, "runtime", "plugins", "prism-plugin-codex.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(runtimeLeaseRecord{
		PID:             4242,
		PluginID:        "codex",
		SessionToken:    "old",
		StartIdentifier: "2026-11-10 09:00:00",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	stubRuntimeLeaseProcessInspection(t, func(pid int) (runtimeLeaseProcess, error) {
		if pid != 4242 {
			return runtimeLeaseProcess{}, fmt.Errorf("unexpected pid %d", pid)
		}
		return runtimeLeaseProcess{Alive: true, StartIdentifier: "2026-11-10 11:00:00", Command: "trustd"}, nil
	})
	terminated := false
	stubRuntimeLeaseTermination(t, func(pid int) error {
		terminated = true
		return nil
	})
	if _, err := prepareRuntimeLease("codex", "new"); err != nil {
		t.Fatal(err)
	}
	if terminated {
		t.Fatal("PID reuse must not terminate the replacement process")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("reused PID lease should be removed: %v", err)
	}
}

func TestRuntimeLeaseLegacyRecordOnlyTerminatesVerifiedPlugin(t *testing.T) {
	record := runtimeLeaseRecord{PID: 4242, PluginID: "codex", SessionToken: "legacy"}
	t.Run("verified plugin", func(t *testing.T) {
		stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
			return runtimeLeaseProcess{Alive: true, Command: "/Applications/Codex.app/Contents/MacOS/prism-plugin-codex-desktop", ProcessGroupID: 4242}, nil
		})
		removable, terminate, err := runtimeLeaseRecoveryDecision(record)
		if err != nil || !removable || !terminate {
			t.Fatalf("verified legacy lease decision = (%v, %v, %v)", removable, terminate, err)
		}
	})
	t.Run("unrelated process", func(t *testing.T) {
		stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
			return runtimeLeaseProcess{Alive: true, Command: "trustd"}, nil
		})
		removable, terminate, err := runtimeLeaseRecoveryDecision(record)
		if err != nil || !removable || terminate {
			t.Fatalf("unrelated legacy lease decision = (%v, %v, %v)", removable, terminate, err)
		}
	})
}

func TestRuntimeLeaseMatchingStartRequiresItsOwnProcessGroup(t *testing.T) {
	record := runtimeLeaseRecord{
		PID:             4242,
		PluginID:        "codex",
		SessionToken:    "old",
		StartIdentifier: "darwin:1:2",
		ProcessGroupID:  4242,
	}
	stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
		return runtimeLeaseProcess{
			Alive:           true,
			StartIdentifier: "darwin:1:2",
			ProcessGroupID:  9000,
		}, nil
	})
	removable, terminate, err := runtimeLeaseRecoveryDecision(record)
	if err == nil || removable || terminate {
		t.Fatalf("process group mismatch decision = (%v, %v, %v)", removable, terminate, err)
	}
}

func TestRuntimeLeaseLegacyRecordDoesNotMatchPluginPathArgument(t *testing.T) {
	record := runtimeLeaseRecord{PID: 4242, PluginID: "codex", SessionToken: "legacy"}
	stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
		return runtimeLeaseProcess{
			Alive:          true,
			Command:        "node /tmp/prism-plugin-codex-desktop/index.js",
			ProcessGroupID: 4242,
		}, nil
	})
	removable, terminate, err := runtimeLeaseRecoveryDecision(record)
	if err != nil || !removable || terminate {
		t.Fatalf("plugin path argument decision = (%v, %v, %v)", removable, terminate, err)
	}
}

func TestRuntimeLeaseLegacyPluginWithSharedProcessGroupBlocksReplacement(t *testing.T) {
	record := runtimeLeaseRecord{PID: 4242, PluginID: "codex", SessionToken: "legacy"}
	stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
		return runtimeLeaseProcess{
			Alive:          true,
			Command:        "prism-plugin-codex-desktop index.js",
			ProcessGroupID: 9000,
		}, nil
	})
	removable, terminate, err := runtimeLeaseRecoveryDecision(record)
	if err == nil || removable || terminate {
		t.Fatalf("legacy shared process group decision = (%v, %v, %v)", removable, terminate, err)
	}
}

// A staging probe runs beside the still-active runtime during an update. It
// must leave the runtime lease alone: claiming the file makes the probe and
// the active runtime terminate each other.
func TestStartProcessRuntimeSkipsLeaseForVerificationProcesses(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	leasePath := filepath.Join(t.TempDir(), "runtime", "plugins", "prism-plugin-demo.json")
	t.Setenv(host.PrismDataDirEnv, filepath.Dir(filepath.Dir(filepath.Dir(leasePath))))

	spawn := func(skip bool) *processRuntime {
		t.Helper()
		rt, err := startProcessRuntime(context.Background(), t.TempDir(), Manifest{
			ID:        "demo",
			Transport: "process",
			Mode:      "test",
			Command:   []string{os.Args[0], "-test.run", "TestStartProcessRuntimeSkipsLeaseForVerificationProcesses"},
		}, nil, skip)
		if err != nil {
			t.Fatalf("start process runtime (skip=%v): %v", skip, err)
		}
		return rt
	}

	probeRuntime := spawn(true)
	if probeRuntime.lease != nil {
		t.Fatalf("verification runtime must not hold a lease")
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("skip mode created a lease file: %v", err)
	}

	activeRuntime := spawn(false)
	defer activeRuntime.close(context.Background())
	if activeRuntime.lease == nil {
		t.Fatalf("active runtime must hold a lease")
	}
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("active runtime lease file missing: %v", err)
	}

	// The probe-style second instance stays alive beside the lease holder.
	if err := probeRuntime.call(context.Background(), "adapter.probe", map[string]any{}, nil); err != nil {
		t.Fatalf("probe alongside active lease holder: %v", err)
	}
}

func TestCleanupStaleRuntimeLeases(t *testing.T) {
	stubRuntimeLeaseProcessInspection(t, func(int) (runtimeLeaseProcess, error) {
		return runtimeLeaseProcess{}, nil
	})
	dir := filepath.Join(t.TempDir(), "runtime", "plugins")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(runtimeLeaseRecord{PID: 999999, PluginID: "demo-stale", SessionToken: "token-stale"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "prism-plugin-demo-stale.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	stale := CleanupStaleRuntimeLeases(dir)
	if len(stale) != 1 || stale[0].PluginID != "demo-stale" {
		t.Fatalf("expected 1 stale lease cleaned, got %+v", stale)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("lease file should be deleted, but still exists: %v", err)
	}
}
