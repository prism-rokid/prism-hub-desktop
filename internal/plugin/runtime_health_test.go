package plugin

import (
	"testing"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

// RuntimeHealth is the passive observability path used by the local doctor.
// It must reflect supervisor bookkeeping without probing or spawning the
// plugin process.
func TestProcessAdapterRuntimeHealthIsPassive(t *testing.T) {
	adapter := NewProcessAdapter(Discovered{
		Root: t.TempDir(),
		Manifest: Manifest{
			ID:              "health-probe",
			Version:         "1.0.0",
			Transport:       "process",
			IntegrationMode: "protocol-native",
			Command:         []string{"definitely-not-a-real-binary"},
		},
	})

	health := adapter.RuntimeHealth()
	if health.Running || health.Failures != 0 || !health.BackoffUntil.IsZero() {
		t.Fatalf("fresh adapter must report the lazy idle state, got %+v", health)
	}

	// Simulate supervisor bookkeeping without touching a process.
	adapter.mu.Lock()
	adapter.failures = 2
	adapter.lastError = "exit 1"
	adapter.nextStart = time.Now().Add(time.Second)
	adapter.mu.Unlock()

	health = adapter.RuntimeHealth()
	if health.Running || health.Failures != 2 || health.LastError != "exit 1" {
		t.Fatalf("expected failure bookkeeping in health, got %+v", health)
	}
	if health.BackoffUntil.IsZero() {
		t.Fatalf("expected backoff deadline in health, got %+v", health)
	}

	// The service exposes passive health for adapters implementing the
	// optional reporter interface.
	service := pluginruntime.New(adapter)
	snapshot := service.HealthSnapshot()
	if len(snapshot) != 1 || snapshot[0].ID != "health-probe" || snapshot[0].Health == nil {
		t.Fatalf("expected one passive health entry, got %+v", snapshot)
	}
	if snapshot[0].Health.Failures != 2 {
		t.Fatalf("unexpected snapshot health: %+v", snapshot[0].Health)
	}
}
