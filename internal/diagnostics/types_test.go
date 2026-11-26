package diagnostics

import (
	"testing"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestClassifyDesktopFacts(t *testing.T) {
	installed := true
	running := true
	reachable := false
	status := Classify(Input{PluginID: "dsh", Component: "DeepSeek Harness", ProbeKnown: true, Installed: &installed, Running: &running, Transport: "cdp", TransportReachable: &reachable, Revision: 3})
	if status.Code != "control_channel_unreachable" || status.State != StateUnavailable {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestClassifyDesktopInstallAndProcessStates(t *testing.T) {
	installed := false
	status := Classify(Input{PluginID: "dsh", Installed: &installed, Running: boolPtr(false), Revision: 1})
	if status.Code != "app_not_installed" || status.State != StateNotInstalled {
		t.Fatalf("unexpected not-installed status: %+v", status)
	}
	installed = true
	status = Classify(Input{PluginID: "dsh", Installed: &installed, Running: boolPtr(false), Revision: 2})
	if status.Code != "not_running" || status.State != StateNotRunning {
		t.Fatalf("unexpected not-running status: %+v", status)
	}
}

func TestClassifyPluginVersionDiagnostic(t *testing.T) {
	status := Classify(Input{PluginID: "claude", ProbeKnown: true, Revision: 4, Plugin: &pluginruntime.Diagnostic{
		State: string(StateIncompatible), Code: "dependency_version_mismatch", Component: "Claude Code", ActualVersion: "2.1.203", RequiredVersion: ">=2.1.287",
	}})
	if status.State != StateIncompatible || status.Code != "dependency_version_mismatch" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.Detail != "检测到 2.1.203，要求 >=2.1.287 或更新版本。" {
		t.Fatalf("unexpected detail: %q", status.Detail)
	}
}

func TestClassifyLegacyLocationCompatibility(t *testing.T) {
	status := Classify(Input{PluginID: "codex", LocationError: "missing", ManualLocation: false, Revision: 1})
	if status.Code != "program_not_found" || status.State != StateNotInstalled {
		t.Fatalf("unexpected status: %+v", status)
	}
}
