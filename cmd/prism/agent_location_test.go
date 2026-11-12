package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"rokid-prism-hub/internal/agentlocation"
	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
)

func locationConfigFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{filepath.Join(root, "plugins")}
	pluginRoot := filepath.Join(root, "plugins", "hermes")
	if err := os.MkdirAll(pluginRoot, 0755); err != nil {
		t.Fatal(err)
	}
	manifest := "id: hermes\ndisplay_name: Hermes\nversion: 1.0.0\nintegration_mode: protocol-native\ntransport: process\ncommand: [\"node\", \"index.js\"]\nruntime_modes: [desktop, native]\ndefault_runtime_mode: desktop\n"
	if err := os.WriteFile(filepath.Join(pluginRoot, "pluginbridge-plugin.yaml"), []byte(manifest), 0644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.yaml")
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	return path, root
}
func TestAgentLocationAPIValidationAndCompatibility(t *testing.T) {
	configPath, root := locationConfigFixture(t)
	mux := http.NewServeMux()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registerAgentLocationAPI(mux, ctx, configPath, nil)
	name := "hermes"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	exe := filepath.Join(root, name)
	if err := os.WriteFile(exe, []byte("fixture"), 0755); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}
	base := "/api/agents/hermes/location"
	data, _ := json.Marshal(map[string]any{"mode": "native", "directory": root})
	if w := request("POST", base+"/validate", string(data)); w.Code != 200 {
		t.Fatalf("validate %d %s", w.Code, w.Body.String())
	}
	cfg, _ := bridgeconfig.Load(configPath)
	if len(cfg.Bridge.Plugins["hermes"].Locations) != 0 {
		t.Fatal("validation wrote config")
	}
	if w := request("PUT", base, string(data)); w.Code != 202 {
		t.Fatalf("save %d %s", w.Code, w.Body.String())
	}
	cfg, _ = bridgeconfig.Load(configPath)
	if cfg.Bridge.Plugins["hermes"].Locations["native"].Directory != root {
		t.Fatal("selection not persisted")
	}
	if w := request("PUT", base, `{"mode":"native","directory":"relative"}`); w.Code != 400 {
		t.Fatalf("invalid save: %d", w.Code)
	}
	after, _ := bridgeconfig.Load(configPath)
	if after.Bridge.Plugins["hermes"].Locations["native"].Directory != root {
		t.Fatal("invalid selection replaced good one")
	}
	if w := request("GET", "/api/agents/unknown/location", ""); w.Code != 404 {
		t.Fatalf("unknown plugin accepted: %d", w.Code)
	}
	if w := request("GET", base+"?mode=mod", ""); w.Code != 400 {
		t.Fatalf("unsupported mode accepted: %d", w.Code)
	}
	req := httptest.NewRequest("DELETE", base+"?mode=native", nil)
	req.Header.Set("Origin", "https://other.example")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("cross-origin mutation accepted")
	}
}
func TestAgentLocationEnvironmentUsesSelectionAndKeepsModesIsolated(t *testing.T) {
	cfg := bridgeconfig.Default()
	root := t.TempDir()
	name := "hermes"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	target := filepath.Join(root, name)
	if err := os.WriteFile(target, []byte("fixture"), 0755); err != nil {
		t.Fatal(err)
	}
	cfg.Bridge.Plugins["hermes"] = bridgeconfig.PluginConfig{Mode: "native", Locations: map[string]agentlocation.LocationSelection{"native": {Directory: root}}, Env: map[string]string{"HERMES_BIN": "/old/hermes", "PRISM_HERMES_APP_PATH": "/desktop/Hermes.app"}}
	loc, err := resolveAgentLocation(context.Background(), cfg, "hermes", "native")
	if err != nil || loc.Executable != target || loc.Source != "manual" {
		t.Fatalf("%+v %v", loc, err)
	}
	manifest := pluginpkg.Manifest{ID: "hermes", RuntimeModes: []string{"native", "desktop"}, DefaultRuntimeMode: "desktop"}
	env := agentLocationEnv(context.Background(), cfg, manifest)
	if env["HERMES_BIN"] != target {
		t.Fatalf("wrong env: %v", env)
	}
	if _, ok := env["PRISM_HERMES_APP_PATH"]; ok {
		t.Fatal("native location overwrote desktop")
	}
	pc := cfg.Bridge.Plugins["hermes"]
	pc.Locations["native"] = agentlocation.LocationSelection{Directory: filepath.Join(root, "gone")}
	cfg.Bridge.Plugins["hermes"] = pc
	env = agentLocationEnv(context.Background(), cfg, manifest)
	if !strings.Contains(env["HERMES_BIN"], "__prism_invalid_location__") {
		t.Fatal("invalid manual override fell back")
	}
}

func TestLocatedServiceLaunchUsesManualSelection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture; Windows batch tests run separately")
	}
	root := filepath.Join(t.TempDir(), "工具 Apps")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"${0}.args\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["opencode"] = bridgeconfig.PluginConfig{Mode: "native", Locations: map[string]agentlocation.LocationSelection{"native": {Directory: root}}}
	manifest := pluginpkg.Manifest{ID: "opencode", RuntimeModes: []string{"native"}, DefaultRuntimeMode: "native"}
	spec := pluginpkg.ManagedLaunchSpec{Kind: "service", Start: []string{"definitely-not-on-path", "serve", "--port", "4096"}, Ready: "exit_ok", ReadyTimeoutMs: 5000}
	if err := startManagedService(context.Background(), manifest, spec, nil, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(script + ".args")
	if err != nil || string(got) != "serve\n--port\n4096\n" {
		t.Fatalf("arguments=%q error=%v", got, err)
	}
}

func TestAgentLocationStatusDoesNotRequireLocalExecutableForOnlineService(t *testing.T) {
	previous := currentDashboardRuntimePlugins()
	defer setDashboardRuntimePlugins(previous)
	svc := pluginruntime.New(pluginruntime.StaticPlugin{PluginID: "opencode", Capability: pluginruntime.Capability{Available: true}})
	setDashboardRuntimePlugins(svc)
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["opencode"] = bridgeconfig.PluginConfig{Locations: map[string]agentlocation.LocationSelection{"native": {Directory: filepath.Join(t.TempDir(), "missing")}}}
	items := []map[string]any{{"plugin_id": "opencode", "current_runtime_mode": "native"}}
	annotateAgentLocations(cfg, items)
	status := items[0]["runtime_status"].(map[string]any)
	if status["code"] != "ready" || status["available"] != true {
		t.Fatalf("reachable service incorrectly unavailable: %+v", status)
	}
}

func TestAgentLocationResetPreservesOtherModeAndDataDirectory(t *testing.T) {
	configPath, root := locationConfigFixture(t)
	deadline := time.Now().Add(time.Second)
	for locationOperation("hermes").State == "connecting" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cfg, _ := bridgeconfig.Load(configPath)
	cfg.Bridge.Plugins["hermes"] = bridgeconfig.PluginConfig{Mode: "desktop", Locations: map[string]agentlocation.LocationSelection{"desktop": {Directory: root}, "native": {Directory: root}}, Env: map[string]string{"HERMES_BIN": "/legacy/hermes", "PRISM_HERMES_USER_DATA_DIR": "/keep/profile"}}
	cfg.Bridge.PluginEnv = map[string]string{"HERMES_BIN": "/legacy/global/hermes"}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registerAgentLocationAPI(mux, ctx, configPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("DELETE", "/api/agents/hermes/location?mode=native", nil))
	if w.Code != 202 {
		t.Fatalf("reset %d %s", w.Code, w.Body.String())
	}
	cfg, _ = bridgeconfig.Load(configPath)
	pc := cfg.Bridge.Plugins["hermes"]
	if _, exists := pc.Locations["native"]; exists {
		t.Fatal("native location retained")
	}
	if pc.Locations["desktop"].Directory != root || pc.Env["PRISM_HERMES_USER_DATA_DIR"] != "/keep/profile" {
		t.Fatal("reset changed unrelated paths")
	}
	if _, exists := pc.Env["HERMES_BIN"]; exists {
		t.Fatal("legacy override retained")
	}
	if _, exists := cfg.Bridge.PluginEnv["HERMES_BIN"]; exists {
		t.Fatal("global legacy override retained")
	}
}

func TestLegacyDesktopPathUsesSelectionStore(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	for locationOperation("hermes").State == "connecting" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	configPath, root := locationConfigFixture(t)
	cfg, _ := bridgeconfig.Load(configPath)
	name := "Hermes"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	target := filepath.Join(root, name)
	if err := os.WriteFile(target, []byte("fixture"), 0755); err != nil {
		t.Fatal(err)
	}
	apps := map[string]dashboardDesktopAppConfigRequest{"hermes": {AppPath: &target}}
	changed, err := prepareLegacyDesktopLocations(context.Background(), cfg, apps)
	if err != nil || len(changed) != 1 || apps["hermes"].AppPath != nil {
		t.Fatalf("legacy save failed: %v %v", changed, err)
	}
	loc, err := resolveAgentLocation(context.Background(), cfg, "hermes", "desktop")
	if err != nil || loc.Executable != target || loc.Source != "manual" {
		t.Fatalf("inconsistent legacy location: %+v %v", loc, err)
	}
}

type markerServiceAdapter struct {
	pluginruntime.StaticPlugin
	marker string
}

func (a markerServiceAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	_, err := os.Stat(a.marker)
	return pluginruntime.Capability{PluginID: a.PluginID, Available: err == nil}, nil
}
func TestLocatedForegroundServiceSurvivesLaunchRequest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX foreground service fixture")
	}
	previous := currentDashboardRuntimePlugins()
	defer setDashboardRuntimePlugins(previous)
	defer stopManagedAgentServices()
	root := t.TempDir()
	script := filepath.Join(root, "opencode")
	marker := script + ".ready"
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf ready > \"${0}.ready\"\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n"), 0755); err != nil {
		t.Fatal(err)
	}
	svc := pluginruntime.New(markerServiceAdapter{StaticPlugin: pluginruntime.StaticPlugin{PluginID: "opencode"}, marker: marker})
	setDashboardRuntimePlugins(svc)
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["opencode"] = bridgeconfig.PluginConfig{Mode: "native", Locations: map[string]agentlocation.LocationSelection{"native": {Directory: root}}}
	manifest := pluginpkg.Manifest{ID: "opencode", RuntimeModes: []string{"native"}, DefaultRuntimeMode: "native"}
	spec := pluginpkg.ManagedLaunchSpec{Kind: "service", Start: []string{"opencode", "serve"}, Ready: "probe", ReadyTimeoutMs: 5000}
	ctx, cancel := context.WithCancel(context.Background())
	if err := startManagedService(ctx, manifest, spec, nil, cfg); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	managedAgentServices.Lock()
	running := managedAgentServices.items["opencode"]
	count := len(managedAgentServices.items)
	managedAgentServices.Unlock()
	if running == nil || count != 1 {
		t.Fatal("foreground service was not tracked")
	}
	select {
	case <-running.done:
		t.Fatalf("service exited with request: %v", running.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := startManagedService(context.Background(), manifest, spec, nil, cfg); err != nil {
		t.Fatal(err)
	}
	managedAgentServices.Lock()
	same := managedAgentServices.items["opencode"] == running
	managedAgentServices.Unlock()
	if !same {
		t.Fatal("online service launched twice")
	}
	stopManagedAgentServices()
	select {
	case <-running.done:
	case <-time.After(3 * time.Second):
		t.Fatal("Hub-owned foreground service was not stopped")
	}
}
