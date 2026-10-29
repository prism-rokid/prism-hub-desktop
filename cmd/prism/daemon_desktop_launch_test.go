package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/realtime"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSelectLaunchSpecPrefersProbeAndHonorsExplicitKind(t *testing.T) {
	manifestYAML := `
id: dual
transport: process
command: ["dual"]
integration_mode: protocol-native
runtime_modes: [desktop, native]
managed_launch_specs:
  - kind: desktop-app
    app: dual
    ready: probe
    runtime_modes: [desktop]
  - kind: service
    start: ["dual", "start"]
    ready: exit_ok
`
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "pluginbridge-plugin.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifestYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := pluginpkg.LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	// Desktop mode filters to the desktop-app spec.
	spec, err := selectLaunchSpec(manifest, "desktop", "")
	if err != nil || spec.Kind != "desktop-app" {
		t.Fatalf("expected desktop-app in desktop mode, got %v / %v", spec, err)
	}
	// Native mode filters the desktop-app spec out; the service spec is
	// exit_ok so it is the only candidate.
	spec, err = selectLaunchSpec(manifest, "native", "")
	if err != nil || spec.Kind != "service" {
		t.Fatalf("expected service in native mode, got %v / %v", spec, err)
	}
	// An explicit kind must be honored and unknown kinds rejected cleanly.
	if _, err := selectLaunchSpec(manifest, "native", "desktop-app"); err == nil || !strings.Contains(err.Error(), "declares no") {
		t.Fatalf("expected clean rejection for unknown kind, got %v", err)
	}
}

func TestHandleDesktopLaunchRequiresPluginID(t *testing.T) {
	client := realtime.New(realtime.Options{Logger: testLogger()})
	in := realtime.Inbound{MessageID: "msg-1", MessageType: realtime.MsgDesktopLaunch, Payload: &realtime.InboundPayload{}}
	err := handleDesktopLaunch(context.Background(), client, filepath.Join(t.TempDir(), "config.yaml"), nil, in)
	if err == nil || !strings.Contains(err.Error(), "invalid_desktop_launch") {
		t.Fatalf("expected invalid_desktop_launch error, got %v", err)
	}
}

func TestHandleDesktopLaunchRejectsPluginWithoutManagedApp(t *testing.T) {
	client := realtime.New(realtime.Options{Logger: testLogger()})
	// A manifest with the managed_launch env-contract bool but no
	// managed_launch_specs must not be remotely launchable.
	pluginDir := t.TempDir()
	pluginRoot := filepath.Join(pluginDir, "plain-plugin")
	if err := os.MkdirAll(pluginRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `
id: plain-plugin
transport: process
command: ["plain-plugin"]
integration_mode: protocol-native
managed_launch: true
capabilities:
  can_forward_sync: true
  can_reverse_sync: true
`
	if err := os.WriteFile(filepath.Join(pluginRoot, "pluginbridge-plugin.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := "bridge:\n  plugin_dirs:\n    - " + pluginDir + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	in := realtime.Inbound{MessageID: "msg-2", MessageType: realtime.MsgDesktopLaunch, Payload: &realtime.InboundPayload{PluginID: "plain-plugin"}}
	err := handleDesktopLaunch(context.Background(), client, configPath, nil, in)
	if err == nil || !strings.Contains(err.Error(), "desktop_launch_failed") {
		t.Fatalf("expected desktop_launch_failed error, got %v", err)
	}
	if !strings.Contains(err.Error(), "no managed launch target") {
		t.Fatalf("expected launch-target rejection, got %v", err)
	}
}

func TestHandleDesktopLaunchWithoutClientStillReportsFailure(t *testing.T) {
	in := realtime.Inbound{MessageID: "msg-3", MessageType: realtime.MsgDesktopLaunch, Payload: &realtime.InboundPayload{PluginID: "codex"}}
	err := handleDesktopLaunch(context.Background(), nil, filepath.Join(t.TempDir(), "config.yaml"), nil, in)
	if err == nil || !strings.Contains(err.Error(), "desktop_launch_unavailable") {
		t.Fatalf("expected desktop_launch_unavailable error, got %v", err)
	}
}
