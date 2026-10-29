package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"

	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
)

const fakePluginHelperEnv = "PRISM_CODEX_TEST_FAKE_PLUGIN_HELPER"

func TestBridgePluginConformanceHelperWithFakePlugin(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `
id: demo
transport: process
command: ["` + os.Args[0] + `", "-test.run", "TestBridgePluginCommandFakePlugin"]
mode: test
capabilities:
  native_visible_input: true
  native_visible_output: true
`
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	// Keep the direct conformance helper's visibility data out of the running
	// Hub's default config and SQLite database.
	t.Setenv("PRISM_CONFIG", filepath.Join(root, "config.yaml"))
	t.Setenv("PRISM_DB", filepath.Join(root, "prism.sqlite"))
	t.Setenv("PRISM_DATA_DIR", root)
	t.Setenv("PRISM_HOST_TOKEN", "host-token-should-not-leak")
	t.Setenv(fakePluginHelperEnv, "1")
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payloadMap, code := bridgePluginTestPayload(ctx, pluginDir, "", "bridge-plugin-test", "hello from direct test", root, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%s", code, stderr.String())
	}
	var payload struct {
		OK              bool   `json:"ok"`
		Visible         bool   `json:"visible"`
		Accepted        bool   `json:"accepted"`
		PluginID        string `json:"plugin_id"`
		NativeSessionID string `json:"native_session_id"`
		NativeMessageID string `json:"native_message_id"`
		RunStatus       string `json:"run_status"`
		Steps           []struct {
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"steps"`
	}
	raw, err := json.Marshal(payloadMap)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !payload.OK || !payload.Visible || !payload.Accepted {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.PluginID != "demo" {
		t.Fatalf("unexpected ids: %+v", payload)
	}
	if payload.NativeSessionID != "native-command-test" || payload.NativeMessageID != "run-command-test" {
		t.Fatalf("unexpected native ids: %+v", payload)
	}
	if payload.RunStatus != "completed" {
		t.Fatalf("unexpected run status: %+v", payload)
	}
	seen := map[string]bool{}
	for _, step := range payload.Steps {
		if !step.OK {
			t.Fatalf("step failed: %+v", step)
		}
		seen[step.Name] = true
	}
	for _, name := range []string{"manifest", "probe", "discover", "startSessionWithMessage", "persistStartBinding", "subscribe", "verifyVisibility", "subscriptionEvent", "waitForRun", "close"} {
		if !seen[name] {
			t.Fatalf("missing step %s in %+v", name, payload.Steps)
		}
	}
}

// TestBridgePluginConformancePersistsVisibilityCheck verifies that direct
// conformance testing records visibility evidence in an isolated local DB.
func TestBridgePluginTestPersistsVisibilityCheck(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `
id: demo
transport: process
command: ["` + os.Args[0] + `", "-test.run", "TestBridgePluginCommandFakePlugin"]
mode: test
capabilities:
  native_visible_input: true
  native_visible_output: true
`
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, "prism.sqlite")
	t.Setenv("PRISM_CONFIG", filepath.Join(root, "config.yaml"))
	t.Setenv("PRISM_DB", dbPath)
	t.Setenv("PRISM_DATA_DIR", root)
	t.Setenv("PRISM_HOST_TOKEN", "host-token-should-not-leak")
	t.Setenv(fakePluginHelperEnv, "1")

	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, code := bridgePluginTestPayload(ctx, pluginDir, "", "bridge-plugin-test", "persist-check-marker", root, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%s", code, stderr.String())
	}

	db, err := prismdb.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	check, err := db.LatestVisibilityCheck(ctx, "demo")
	if err != nil {
		t.Fatalf("LatestVisibilityCheck: %v", err)
	}
	if !check.Visible {
		t.Fatalf("expected persisted check Visible=true, got %+v", check)
	}
	if check.Marker != "persist-check-marker" {
		t.Fatalf("expected marker persist-check-marker, got %q", check.Marker)
	}
	if check.SessionID == "" {
		t.Fatal("expected persisted check to reference a session id, got empty")
	}
}

func TestPluginListPayloadScansPluginDir(t *testing.T) {
	pluginRoot := filepath.Join(t.TempDir(), "plugins")
	pluginDir := filepath.Join(pluginRoot, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(`
id: demo
display_name: Demo Adapter
version: 0.2.0
integration_mode: protocol-native
transport: process
command: ["demo-pluginbridge-plugin"]
mode: test
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	payloadMap, code := pluginListPayload(context.Background(), filepath.Join(t.TempDir(), "config.yaml"), pluginRoot, false, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%s", code, stderr.String())
	}
	var payload struct {
		OK      bool `json:"ok"`
		Plugins []struct {
			DisplayName     string   `json:"display_name"`
			Version         string   `json:"version"`
			PluginID        string   `json:"plugin_id"`
			IntegrationMode string   `json:"integration_mode"`
			Transport       string   `json:"transport"`
			Mode            string   `json:"mode"`
			ManifestPath    string   `json:"manifest_path"`
			Command         []string `json:"command"`
			Available       bool     `json:"available"`
			Visible         bool     `json:"visible"`
			Probed          bool     `json:"probed"`
		} `json:"plugins"`
	}
	raw, err := json.Marshal(payloadMap)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !payload.OK || len(payload.Plugins) != 1 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	got := payload.Plugins[0]
	if got.PluginID != "demo" || got.DisplayName != "Demo Adapter" || got.Version != "0.2.0" {
		t.Fatalf("unexpected plugin metadata: %+v", got)
	}
	if got.IntegrationMode != "protocol-native" {
		t.Fatalf("unexpected integration mode: %+v", got)
	}
	if got.Transport != "process" || got.Mode != "test" || !got.Visible || got.Probed || got.Available {
		t.Fatalf("unexpected plugin status: %+v", got)
	}
	if got.ManifestPath == "" || len(got.Command) != 1 {
		t.Fatalf("unexpected plugin paths: %+v", got)
	}
}

func TestActivatePluginConfigPreservesSupportedRuntimeMode(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{
		"demo": {Mode: "native"},
	}
	manifest := pluginpkg.Manifest{
		ID:                 "demo",
		Mode:               "gateway",
		RuntimeModes:       []string{"desktop", "native"},
		DefaultRuntimeMode: "desktop",
	}

	if err := activatePluginConfig(cfg, configPath, root, manifest); err != nil {
		t.Fatalf("activate plugin config: %v", err)
	}
	if got := cfg.Bridge.Plugins["demo"].Mode; got != "native" {
		t.Fatalf("supported runtime mode overwritten: got %q, want native", got)
	}
}

func TestActivatePluginConfigReplacesUnsupportedRuntimeModeWithDefault(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{
		"demo": {Mode: "gateway"},
	}
	manifest := pluginpkg.Manifest{
		ID:                 "demo",
		Mode:               "gateway",
		RuntimeModes:       []string{"desktop", "native"},
		DefaultRuntimeMode: "desktop",
	}

	if err := activatePluginConfig(cfg, configPath, root, manifest); err != nil {
		t.Fatalf("activate plugin config: %v", err)
	}
	if got := cfg.Bridge.Plugins["demo"].Mode; got != "desktop" {
		t.Fatalf("unsupported runtime mode not replaced: got %q, want desktop", got)
	}
}

func TestInstallPluginFromSourceZipURL(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	pluginRoot := filepath.Join(root, "plugins")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, "demo.zip")
	sourceRoot := filepath.Join(root, "source", "demo")
	writeMinimalPluginSource(t, sourceRoot, "demo")
	if err := writeZipFromDir(archivePath, sourceRoot); err != nil {
		t.Fatalf("write zip: %v", err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(root)))
	defer server.Close()

	var stderr bytes.Buffer
	payload, code := installPluginFromSource(configPath, "", server.URL+"/demo.zip", true, &stderr)
	if code != 0 {
		t.Fatalf("expected install success, got %d stderr=%s payload=%+v", code, stderr.String(), payload)
	}
	if payload["plugin_id"] != "demo" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload["source_kind"] != "url" {
		t.Fatalf("expected url source kind, got %+v", payload)
	}
}

func TestInstallPluginFromPathActivatesManagedVersion(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	pluginRoot := filepath.Join(root, "plugins")
	installRoot := filepath.Join(pluginRoot, "codex")
	oldVersionRoot := filepath.Join(installRoot, "versions", "0.1.0")
	if err := os.MkdirAll(oldVersionRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installRoot, "state.json"), []byte(`{"active_version":"0.1.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldVersionRoot, pluginpkg.ManifestYAML), []byte("id: codex\nversion: 0.1.0\ntransport: process\ncommand: [/bin/true]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	sourceRoot := filepath.Join(root, "source", "codex")
	writeMinimalPluginSource(t, sourceRoot, "codex")
	manifestPath := filepath.Join(sourceRoot, pluginpkg.ManifestYAML)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest = []byte(strings.Replace(string(manifest), "version: 0.1.0", "version: 0.2.0", 1))
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	payload, code := installPluginFromPath(configPath, "", sourceRoot, true, &stderr)
	if code != 0 {
		t.Fatalf("install plugin: code=%d stderr=%s payload=%+v", code, stderr.String(), payload)
	}
	stateData, err := os.ReadFile(filepath.Join(installRoot, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		ActiveVersion   string `json:"active_version"`
		PreviousVersion string `json:"previous_version"`
	}
	if err := json.Unmarshal(stateData, &state); err != nil {
		t.Fatal(err)
	}
	if state.ActiveVersion != "0.2.0" || state.PreviousVersion != "0.1.0" {
		t.Fatalf("unexpected managed plugin state: %+v", state)
	}
	discovered, err := pluginpkg.ScanDirs([]string{pluginRoot})
	if err != nil || len(discovered) != 1 || discovered[0].Manifest.Version != "0.2.0" {
		t.Fatalf("scanner did not select installed version: plugins=%+v err=%v", discovered, err)
	}
}

func TestRemoveInstalledPluginDeletesFilesAndConfig(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	pluginRoot := filepath.Join(root, "plugins")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	sourceRoot := filepath.Join(root, "source", "codex")
	writeMinimalPluginSource(t, sourceRoot, "codex")
	payload, code := installPluginFromPath(configPath, "", sourceRoot, true, &stderr)
	if code != 0 {
		t.Fatalf("install plugin: code=%d stderr=%s payload=%+v", code, stderr.String(), payload)
	}
	installPath := filepath.Join(pluginRoot, "codex")
	if _, err := os.Stat(installPath); err != nil {
		t.Fatalf("expected installed plugin path: %v", err)
	}
	payload, code = removeInstalledPlugin(configPath, "", "codex", &stderr)
	if code != 0 || payload["ok"] != true {
		t.Fatalf("remove plugin: code=%d stderr=%s payload=%+v", code, stderr.String(), payload)
	}
	if _, err := os.Stat(installPath); !os.IsNotExist(err) {
		t.Fatalf("expected plugin path removed, err=%v", err)
	}
	loaded, err := bridgeconfig.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if _, ok := loaded.Bridge.Plugins["codex"]; ok {
		t.Fatalf("expected plugin config removed, got %+v", loaded.Bridge.Plugins["codex"])
	}
}

func TestRemoveInstalledPluginDetachesDirectSourceWithoutDeletingRepository(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	directSource := filepath.Join(root, "prism-plugin-codex")
	writeMinimalPluginSource(t, directSource, "codex")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{directSource}
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{Source: "plugin"}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	payload, code := removeInstalledPlugin(configPath, "", "codex", &stderr)
	if code != 0 || payload["ok"] != true {
		t.Fatalf("卸载直接源码插件失败: code=%d stderr=%s payload=%+v", code, stderr.String(), payload)
	}
	if payload["plugin_source_preserved"] != true {
		t.Fatalf("直接源码仓库必须保留: %+v", payload)
	}
	if _, err := os.Stat(filepath.Join(directSource, pluginpkg.ManifestYAML)); err != nil {
		t.Fatalf("卸载后必须保留直接源码仓库: %v", err)
	}
	loaded, err := bridgeconfig.Load(configPath)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if containsPath(loaded.Bridge.PluginDirs, directSource) {
		t.Fatalf("必须从 Hub 中移除直接源码目录: %+v", loaded.Bridge.PluginDirs)
	}
	if _, ok := loaded.Bridge.Plugins["codex"]; ok {
		t.Fatalf("必须移除插件配置: %+v", loaded.Bridge.Plugins)
	}
}

func writeMinimalPluginSource(t *testing.T, root, pluginID string) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "id: " + pluginID + `
display_name: Demo
version: 0.1.0
integration_mode: protocol-native
transport: process
mode: test
command: ["/bin/sh", "-c", "true"]
`
	if err := os.WriteFile(filepath.Join(root, pluginpkg.ManifestYAML), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeZipFromDir(target, dir string) error {
	file, err := os.Create(target)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := zip.NewWriter(file)
	defer writer.Close()
	return filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		header.Method = zip.Deflate
		w, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	})
}

func TestBridgePluginCommandFakePlugin(t *testing.T) {
	if os.Getenv(fakePluginHelperEnv) != "1" || os.Getenv("PRISM_PLUGIN_SESSION_TOKEN") == "" {
		return
	}
	runFakePluginForBridgeCommandTest()
	os.Exit(0)
}

func runFakePluginForBridgeCommandTest() {
	if os.Getenv(fakePluginHelperEnv) != "1" || os.Getenv("PRISM_PLUGIN_SESSION_TOKEN") == "" {
		os.Exit(10)
	}
	if os.Getenv("PRISM_HOST_TOKEN") != "" {
		os.Exit(11)
	}
	type rpcRequest struct {
		ID     string          `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params,omitempty"`
	}
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		okPayload := func(payload any) {
			_ = enc.Encode(map[string]any{"id": req.ID, "ok": true, "payload": toSnakeWireForTest(payload)})
		}
		switch req.Method {
		case "adapter.probe":
			okPayload(pluginruntime.Capability{
				PluginID:            "demo",
				Available:           true,
				NativeVisibleInput:  true,
				NativeVisibleOutput: true,
				IntegrationMode:     pluginruntime.IntegrationModeProtocolNative,
				VisibilitySurface:   "test",
			})
		case "adapter.discover":
			okPayload(pluginruntime.DiscoveryResult{
				PluginID: "demo",
				Surface:  "test",
				Endpoint: "stdio://demo",
				Verified: true,
			})
		case "adapter.startSessionWithMessage":
			okPayload(pluginruntime.StartSessionWithMessageResult{
				Session:    pluginruntime.NativeSession{PluginID: "demo", NativeSessionID: "native-command-test", NativeThreadID: "thread-command-test", Surface: "test", Endpoint: "stdio://demo", Visible: true},
				Receipt:    pluginruntime.SendReceipt{NativeMessageID: "run-command-test", Accepted: true, Visible: true, Detail: "accepted by fake command plugin"},
				Visibility: pluginruntime.VisibilityResult{Visible: true, Marker: "hello from command test", Evidence: "marker found in fake visible surface", CheckedAt: time.Now().UTC()},
			})
		case "adapter.subscribe":
			var params struct {
				SubscriptionID string `json:"subscription_id"`
			}
			_ = json.Unmarshal(req.Params, &params)
			okPayload(map[string]any{"subscribed": true})
			_ = enc.Encode(toSnakeWireForTest(map[string]any{
				"event": "plugin.event",
				"payload": map[string]any{
					"subscription_id": params.SubscriptionID,
					"event": pluginruntime.PluginEvent{
						ID:        "event-command-test",
						Type:      "run.progress",
						Status:    "running",
						Summary:   "fake command plugin running",
						CreatedAt: time.Now().UTC(),
					},
				},
			}))
		case "adapter.send":
			okPayload(pluginruntime.SendReceipt{
				NativeMessageID: "run-command-test",
				Accepted:        true,
				Visible:         true,
				Detail:          "accepted by fake command plugin",
			})
		case "adapter.verifyVisibility":
			okPayload(pluginruntime.VisibilityResult{
				Visible:   true,
				Marker:    "hello from command test",
				Evidence:  "marker found in fake visible surface",
				CheckedAt: time.Now().UTC(),
			})
		case "adapter.waitForRun":
			okPayload(pluginruntime.PluginEvent{
				ID:      "run-command-test",
				Type:    "run.completed",
				Status:  "completed",
				Summary: "fake command plugin completed",
			})
		case "adapter.close", "adapter.unsubscribe", "adapter.interrupt":
			okPayload(map[string]any{"ok": true})
		default:
			_ = enc.Encode(map[string]any{
				"id": req.ID,
				"ok": false,
				"error": map[string]any{
					"message": "unknown method",
				},
			})
		}
	}
}

func toSnakeWireForTest(payload any) any {
	data, err := json.Marshal(payload)
	if err != nil {
		return payload
	}
	var decoded any
	if json.Unmarshal(data, &decoded) != nil {
		return payload
	}
	return snakeWireValueForTest(decoded)
}

func snakeWireValueForTest(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[snakeWireKeyForTest(key)] = snakeWireValueForTest(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = snakeWireValueForTest(item)
		}
		return out
	default:
		return value
	}
}

func snakeWireKeyForTest(key string) string {
	var out strings.Builder
	runes := []rune(key)
	for index, char := range runes {
		if unicode.IsUpper(char) && index > 0 && (unicode.IsLower(runes[index-1]) || (index+1 < len(runes) && unicode.IsLower(runes[index+1]))) {
			out.WriteByte('_')
		}
		out.WriteRune(unicode.ToLower(char))
	}
	return out.String()
}
