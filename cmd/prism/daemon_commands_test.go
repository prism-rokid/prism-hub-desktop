package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rokid-prism-hub/internal/buildinfo"

	prismapi "rokid-prism-hub/internal/api"
	"rokid-prism-hub/internal/bridge"
	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

func TestConversationStartFailureCodePreservesWorkspaceTrust(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("start native session: %w", &pluginruntime.AdapterError{Code: "workspace_trust_required", Message: "local trust needed"}), "workspace_trust_required"},
		{&pluginruntime.AdapterError{Code: "session_start_timeout", Message: "not ready"}, "session_start_timeout"},
		{&pluginruntime.AdapterError{Code: "private_internal", Message: "hidden"}, "conversation_start_failed"},
		{errors.New("workspace_trust_required: spoofed"), "conversation_start_failed"},
	} {
		if got := conversationStartFailureCode(test.err); got != test.want {
			t.Fatalf("failure code = %q, want %q for %v", got, test.want, test.err)
		}
	}
}

func TestPluginDirsDefaultsToCurrentPluginDirectoryOnly(t *testing.T) {
	for _, cfg := range []*bridgeconfig.Config{
		nil,
		func() *bridgeconfig.Config {
			cfg := bridgeconfig.Default()
			cfg.Bridge.PluginDirs = []string{"", "   "}
			return cfg
		}(),
	} {
		dirs := pluginDirs(cfg)
		if len(dirs) != 1 || dirs[0] != bridgeconfig.DefaultPluginDir() {
			t.Fatalf("expected only current plugin directory, got %+v", dirs)
		}
	}
}

func TestNewBridgeServiceFromConfigDiscoversPluginManifest(t *testing.T) {
	pluginRoot := filepath.Join(t.TempDir(), "plugins")
	itemDir := filepath.Join(pluginRoot, "demo")
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(itemDir, "pluginbridge-plugin.yaml"), []byte(`
id: demo
transport: process
command: ["demo-pluginbridge-plugin"]
mode: tui_server
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	svc := newBridgeServiceFromConfig(cfg)
	adapter := svc.Adapter("demo")
	if adapter == nil {
		t.Fatal("expected plugin adapter to be registered")
	}
}

func TestBridgePluginEnvMergesGlobalAndAgentEnv(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginEnv = map[string]string{
		"CUSTOM_FLAG":          "global",
		"PRISM_CODEX_CDP_PORT": "9222",
		"PRISM_HOST_TOKEN":     "blocked",
	}
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Env: map[string]string{
			"PRISM_CODEX_CDP_PORT":               "9333",
			"PRISM_CODEX_ALLOW_MANAGED_RELAUNCH": "1",
			"PRISM_PLUGIN_ID":                    "blocked",
		},
	}
	cfg.Normalize()
	env := bridgePluginEnv(cfg, pluginpkg.Discovered{
		Manifest: pluginpkg.Manifest{
			ID: "codex",
		},
	})
	if env["CUSTOM_FLAG"] != "global" {
		t.Fatalf("expected global env to be included, got %+v", env)
	}
	if env["PRISM_CODEX_CDP_PORT"] != "9333" {
		t.Fatalf("expected plugin env to override global env, got %+v", env)
	}
	if env["PRISM_CODEX_ALLOW_MANAGED_RELAUNCH"] != "1" {
		t.Fatalf("expected managed relaunch env, got %+v", env)
	}
	if _, ok := env["PRISM_HOST_TOKEN"]; ok {
		t.Fatalf("sensitive env leaked: %+v", env)
	}
	if _, ok := env["PRISM_PLUGIN_ID"]; ok {
		t.Fatalf("reserved env leaked: %+v", env)
	}
}

func TestBridgePluginEnvInjectsManifestRuntimeMode(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["demo"] = bridgeconfig.PluginConfig{
		Mode: "MANAGED",
		Env:  map[string]string{"PRISM_PLUGIN_MODE": "native"},
	}
	cfg.Normalize()
	env := bridgePluginEnv(cfg, pluginpkg.Discovered{Manifest: pluginpkg.Manifest{ID: "demo", RuntimeModes: []string{"native", "managed"}, DefaultRuntimeMode: "native"}})
	if env["PRISM_PLUGIN_MODE"] != "managed" {
		t.Fatalf("expected Hub-owned runtime mode, got %+v", env)
	}
}

func TestBridgePluginEnvDefaultsManifestRuntimeMode(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginEnv = map[string]string{"PRISM_PLUGIN_MODE": "managed"}
	env := bridgePluginEnv(cfg, pluginpkg.Discovered{Manifest: pluginpkg.Manifest{ID: "demo", RuntimeModes: []string{"native", "managed"}, DefaultRuntimeMode: "native"}})
	if env["PRISM_PLUGIN_MODE"] != "native" {
		t.Fatalf("expected Hub-owned runtime default, got %+v", env)
	}
}

func TestNewBridgeServiceFromConfigAllowsPluginSourceToOwnOpenClaw(t *testing.T) {
	pluginRoot := filepath.Join(t.TempDir(), "plugins")
	itemDir := filepath.Join(pluginRoot, "openclaw")
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(itemDir, "pluginbridge-plugin.yaml"), []byte(`
id: openclaw
transport: process
command: ["openclaw-pluginbridge-plugin"]
mode: gateway
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	cfg.Bridge.Plugins["openclaw"] = bridgeconfig.PluginConfig{
		Source: "plugin",
		Mode:   "gateway",
	}
	svc := newBridgeServiceFromConfig(cfg)
	capability, err := svc.Adapter("openclaw").Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if capability.UnavailableReason != "pluginbridge plugin runtime not started" && capability.Available {
		t.Fatalf("expected plugin adapter fallback capability before runtime starts, got %+v", capability)
	}
}

func TestNewBridgeServiceFromConfigRespectsConfiguredPluginID(t *testing.T) {
	pluginRoot := filepath.Join(t.TempDir(), "plugins")
	for _, item := range []struct {
		dir      string
		pluginID string
	}{
		{dir: "official", pluginID: "openclaw"},
		{dir: "local", pluginID: "openclaw.local"},
	} {
		itemDir := filepath.Join(pluginRoot, item.dir)
		if err := os.MkdirAll(itemDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(itemDir, "pluginbridge-plugin.yaml"), []byte(`
id: `+item.pluginID+`
transport: process
command: ["`+item.pluginID+`"]
mode: gateway
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	cfg.Bridge.Plugins["openclaw.local"] = bridgeconfig.PluginConfig{
		Source: "plugin",
		Mode:   "gateway",
	}
	svc := newBridgeServiceFromConfig(cfg)
	adapter := svc.Adapter("openclaw.local")
	if adapter == nil {
		t.Fatal("expected openclaw plugin adapter")
	}
	capability, err := adapter.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !strings.Contains(capability.UnavailableReason, "openclaw.local") {
		t.Fatalf("expected local plugin command to be selected, got %+v", capability)
	}
}

func TestNewBridgeServiceFromConfigRegistersDisabledPluginSource(t *testing.T) {
	pluginRoot := filepath.Join(t.TempDir(), "plugins")
	itemDir := filepath.Join(pluginRoot, "openclaw")
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(itemDir, "pluginbridge-plugin.yaml"), []byte(`
id: openclaw.local
transport: process
command: ["openclaw.local"]
mode: gateway
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginRoot}
	cfg.Bridge.Plugins["openclaw.local"] = bridgeconfig.PluginConfig{
		Source: "plugin",
		Mode:   "gateway",
	}
	svc := newBridgeServiceFromConfig(cfg)
	if svc.Adapter("openclaw.local") == nil {
		t.Fatal("expected remote-disabled plugin-source plugin to remain registered for catalog/diagnostics")
	}
}

func TestApplyHostPluginOverlayDisablesAgentsAbsentFromCloudSet(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{}
	cfg.Bridge.Plugins["hermes"] = bridgeconfig.PluginConfig{}
	svc := pluginruntime.New(
		pluginruntime.StaticPlugin{PluginID: "codex"},
		pluginruntime.StaticPlugin{PluginID: "hermes"},
	)
	overlay := newHostPluginOverlay()
	overlay.Replace(map[string]bool{
		"codex": true,
	})
	applied := applyHostPluginOverlay(cfg, svc, overlay)
	if !applied.Bridge.RemoteAuthorizationReady || !applied.Bridge.RemoteAuthorizations["codex"] {
		t.Fatalf("expected cloud authorization for codex: %+v", applied.Bridge)
	}
	if applied.Bridge.RemoteAuthorizations["hermes"] {
		t.Fatalf("expected hermes to be absent from cloud authorization: %+v", applied.Bridge.RemoteAuthorizations)
	}
	if cfg.Bridge.RemoteAuthorizationReady || len(cfg.Bridge.RemoteAuthorizations) != 0 {
		t.Fatalf("overlay must not mutate original config in place: %+v", cfg.Bridge)
	}
}

func TestApplyHostPluginOverlayDisablesAgentsUntilCloudSyncReady(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{}
	cfg.Bridge.Plugins["hermes"] = bridgeconfig.PluginConfig{}
	svc := pluginruntime.New(
		pluginruntime.StaticPlugin{PluginID: "codex"},
		pluginruntime.StaticPlugin{PluginID: "hermes"},
	)
	overlay := newHostPluginOverlay()
	applied := applyHostPluginOverlay(cfg, svc, overlay)
	if applied.Bridge.RemoteAuthorizationReady || len(applied.Bridge.RemoteAuthorizations) != 0 {
		t.Fatalf("expected no remote authorization until cloud sync is ready: %+v", applied.Bridge)
	}
	if cfg.Bridge.RemoteAuthorizationReady || len(cfg.Bridge.RemoteAuthorizations) != 0 {
		t.Fatalf("overlay must not mutate original config in place: %+v", cfg.Bridge)
	}
}

func TestHostPluginOverlayInvalidateIsNotSuccessfulEmptyRegistry(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{}
	svc := pluginruntime.New(pluginruntime.StaticPlugin{PluginID: "codex"})
	overlay := newHostPluginOverlay()

	overlay.Replace(map[string]bool{"codex": true})
	overlay.Invalidate()
	if _, ready := overlay.Snapshot(); ready {
		t.Fatal("expected failed registry read to invalidate the overlay")
	}
	if applied := applyHostPluginOverlay(cfg, svc, overlay); applied.Bridge.RemoteAuthorizationReady || len(applied.Bridge.RemoteAuthorizations) != 0 {
		t.Fatalf("expected invalid overlay to disable remote access: %+v", applied.Bridge)
	}

	overlay.Replace(map[string]bool{})
	if values, ready := overlay.Snapshot(); !ready || len(values) != 0 {
		t.Fatalf("expected successful empty registry to remain distinguishable, ready=%v values=%+v", ready, values)
	}
	if applied := applyHostPluginOverlay(cfg, svc, overlay); !applied.Bridge.RemoteAuthorizationReady || len(applied.Bridge.RemoteAuthorizations) != 0 {
		t.Fatalf("expected successful empty registry to disable remote access: %+v", applied.Bridge)
	}
}

func TestReconcileRemovedRemotePluginsWaitsForGraceThenDeregisters(t *testing.T) {
	now := time.Now()
	grace := 10 * time.Minute
	staleSince := map[string]time.Time{}
	var unregistered []string
	unregister := func(pluginID string) error {
		unregistered = append(unregistered, pluginID)
		return nil
	}

	// 首次发现本地缺失：只记录时间，不注销
	removed, err := reconcileRemovedRemotePlugins(
		[]prismapi.HostPlugin{{PluginID: "codex"}},
		map[string]bool{},
		staleSince, now, grace, unregister,
	)
	if err != nil || removed {
		t.Fatalf("first miss must only record, removed=%v err=%v", removed, err)
	}
	if len(unregistered) != 0 {
		t.Fatalf("unexpected deregistrations: %v", unregistered)
	}

	// 宽限期内再次同步：仍未注销
	removed, err = reconcileRemovedRemotePlugins(
		[]prismapi.HostPlugin{{PluginID: "codex"}},
		map[string]bool{},
		staleSince, now.Add(5*time.Minute), grace, unregister,
	)
	if err != nil || removed {
		t.Fatalf("within grace must not deregister, removed=%v err=%v", removed, err)
	}

	// 超过宽限期：注销
	removed, err = reconcileRemovedRemotePlugins(
		[]prismapi.HostPlugin{{PluginID: "codex"}},
		map[string]bool{},
		staleSince, now.Add(grace+time.Minute), grace, unregister,
	)
	if err != nil || !removed {
		t.Fatalf("expected deregistration after grace, removed=%v err=%v", removed, err)
	}
	if got := strings.Join(unregistered, ","); got != "codex" {
		t.Fatalf("unregistered = %q, want codex", got)
	}
	if _, tracked := staleSince["codex"]; tracked {
		t.Fatal("expected tracked entry to be cleared after deregistration")
	}
}

func TestReconcileRemovedRemotePluginsKeepsPresentPluginsAndClearsGoneRows(t *testing.T) {
	now := time.Now()
	grace := 10 * time.Minute
	staleSince := map[string]time.Time{"stale": now.Add(-2 * grace)}
	unregister := func(pluginID string) error {
		t.Fatalf("unexpected deregistration of %s", pluginID)
		return nil
	}

	removed, err := reconcileRemovedRemotePlugins(
		[]prismapi.HostPlugin{{PluginID: "codex"}},
		map[string]bool{"codex": true},
		staleSince, now, grace, unregister,
	)
	if err != nil || removed {
		t.Fatalf("present plugin must stay untouched, removed=%v err=%v", removed, err)
	}
	if _, tracked := staleSince["stale"]; tracked {
		t.Fatal("expected tracking for rows no longer in the cloud list to be cleared")
	}
}

func TestStartHostPluginSyncKeepsStaleAuthorizationWhenCloudFails(t *testing.T) {
	originalAPIURLCN := buildinfo.APIURLCN
	originalAPIURLGlobal := buildinfo.APIURLGlobal
	defer func() {
		buildinfo.APIURLCN = originalAPIURLCN
		buildinfo.APIURLGlobal = originalAPIURLGlobal
	}()

	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		items := `{"host_id":"host-1","items":[{"plugin_id":"codex","version":"0.1.0","integration_mode":"desktop-automation","capabilities":{"can_forward_sync":true}}]}`
		_, _ = w.Write([]byte(`{"success":true,"code":"ok","data":` + items + `}`))
	}))
	defer server.Close()
	buildinfo.APIURLCN = server.URL
	buildinfo.APIURLGlobal = server.URL

	cfg := bridgeconfig.Default()
	cfg.Account.AccessToken = "account-token-1"
	cfg.Account.HostID = "host-1"
	svc := pluginruntime.New(pluginruntime.StaticPlugin{PluginID: "codex"})
	overlay := newHostPluginOverlay()

	healthy.Store(true)
	sync := startHostPluginSync(context.Background(), cfg, "", svc, overlay, nil, nil)

	if values, ready := overlay.Snapshot(); !ready || !values["codex"] {
		t.Fatalf("expected successful sync to authorize codex, ready=%v values=%+v", ready, values)
	}

	healthy.Store(false)
	if sync() {
		t.Fatal("expected failed sync to report false")
	}
	if values, ready := overlay.Snapshot(); !ready || !values["codex"] {
		t.Fatalf("failed sync must keep the stale authorization, ready=%v values=%+v", ready, values)
	}

	healthy.Store(true)
	if !sync() {
		t.Fatal("expected recovery sync to succeed")
	}
	if values, ready := overlay.Snapshot(); !ready || !values["codex"] {
		t.Fatalf("recovered sync should repopulate authorization, ready=%v values=%+v", ready, values)
	}
}

func TestReconcileRegisteredRemotePluginMetadataRefreshesOnlyMismatchedLocalPlugins(t *testing.T) {
	plugins := []prismapi.HostPlugin{
		{
			PluginID:        "codex",
			Version:         "0.1.0",
			IntegrationMode: "desktop-automation",
			Capabilities:    map[string]any{"can_forward_sync": true, "can_approval": false},
		},
		{
			PluginID:        "remote-only",
			Version:         "0.1.0",
			IntegrationMode: "native",
			Capabilities:    map[string]any{"can_forward_sync": true},
		},
	}
	var built, registered []string
	changed, err := reconcileRegisteredRemotePluginMetadata(
		plugins,
		map[string]bool{"codex": true},
		func(pluginID string) (map[string]any, error) {
			built = append(built, pluginID)
			return map[string]any{
				"plugin_id":        pluginID,
				"version":          "0.1.1",
				"integration_mode": "desktop-automation",
				"capabilities":     map[string]any{"can_forward_sync": true, "can_approval": true},
			}, nil
		},
		func(payload map[string]any) error {
			registered = append(registered, payload["plugin_id"].(string))
			return nil
		},
	)
	if err != nil || !changed {
		t.Fatalf("expected metadata refresh, changed=%v err=%v", changed, err)
	}
	if got := strings.Join(built, ","); got != "codex" {
		t.Fatalf("payloads built for %q, want codex only", got)
	}
	if got := strings.Join(registered, ","); got != "codex" {
		t.Fatalf("registered %q, want codex only", got)
	}
}

func TestReconcileRegisteredRemotePluginMetadataSkipsMatchingRuntime(t *testing.T) {
	remote := prismapi.HostPlugin{
		PluginID:        "codex",
		Version:         "0.1.1",
		IntegrationMode: "desktop-automation",
		Capabilities:    map[string]any{"can_forward_sync": true, "can_approval": true},
	}
	payload := map[string]any{
		"plugin_id":        "codex",
		"version":          "0.1.1",
		"integration_mode": "desktop-automation",
		"capabilities":     map[string]any{"can_forward_sync": true, "can_approval": true},
	}
	changed, err := reconcileRegisteredRemotePluginMetadata(
		[]prismapi.HostPlugin{remote},
		map[string]bool{"codex": true},
		func(string) (map[string]any, error) { return payload, nil },
		func(map[string]any) error { t.Fatal("matching metadata must not register"); return nil },
	)
	if err != nil || changed {
		t.Fatalf("matching metadata should remain untouched, changed=%v err=%v", changed, err)
	}
}

func TestEmitBridgeForegroundDetailsQueuesFreshForegroundSnapshot(t *testing.T) {
	store := bridge.NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-foreground",
		"plugin_id":          "codex",
		"title":              "Foreground detail",
		"desktop_foreground": true,
		"detail_stale":       false,
		"actions":            []map[string]any{{"id": "model.switch", "label": "切换模型"}},
		"model_options":      []map[string]any{{"id": "gpt-5.6", "label": "GPT-5.6"}},
	})
	cfg := bridgeconfig.Default()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{"codex": true}
	sink := &daemonRecordingSink{}
	router := &bridge.Router{
		Config:          cfg,
		Sink:            sink,
		DetailSnapshots: store,
	}

	emitBridgeForegroundDetails(router, slog.Default())

	snapshots := sink.conversationDetailSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("expected one foreground detail snapshot, got %+v", snapshots)
	}
	if snapshots[0]["conversation_id"] != "conv-foreground" {
		t.Fatalf("unexpected foreground snapshot payload: %+v", snapshots[0])
	}
	if snapshots[0]["desktop_foreground"] != true || snapshots[0]["detail_stale"] != false {
		t.Fatalf("expected fresh foreground flags, got %+v", snapshots[0])
	}
	if actions, ok := snapshots[0]["actions"].([]map[string]any); !ok || len(actions) != 1 {
		t.Fatalf("expected actions to be preserved in keepalive payload, got %+v", snapshots[0]["actions"])
	}
}

func TestEmitBridgeForegroundDetailsSkipsRemoteDisabledPlugin(t *testing.T) {
	store := bridge.NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-disabled",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"detail_stale":       false,
	})
	cfg := bridgeconfig.Default()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}
	sink := &daemonRecordingSink{}
	router := &bridge.Router{
		Config:          cfg,
		Sink:            sink,
		DetailSnapshots: store,
	}

	emitBridgeForegroundDetails(router, slog.Default())

	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("expected no detail snapshot from remote-disabled plugin, got %+v", snapshots)
	}
}

func TestBoundedHistoryTurnLimit(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{input: 5, want: 5},
		{input: 1, want: 1},
		{input: 0, want: 20},
		{input: 21, want: 20},
	}
	for _, test := range tests {
		if got := boundedHistoryTurnLimit(test.input); got != test.want {
			t.Fatalf("boundedHistoryTurnLimit(%d) = %d, want %d", test.input, got, test.want)
		}
	}
}

func TestPublicHistoryAttachmentCountOnlyCountsPublicDescriptors(t *testing.T) {
	messages := []map[string]any{
		{"metadata": map[string]any{"attachments": []map[string]string{{"name": "safe.png"}}}},
		{"metadata": map[string]any{"attachments": []map[string]string{{"name": "safe.txt"}, {"name": "safe.pdf"}}}},
		{"metadata": map[string]any{"attachments": "not-a-public-descriptor-list"}},
	}
	if got := publicHistoryAttachmentCount(messages); got != 3 {
		t.Fatalf("publicHistoryAttachmentCount() = %d, want 3", got)
	}
}

type daemonRecordingSink struct {
	mu       sync.Mutex
	out      []prismtransport.Outbound
	controls []prismtransport.ControlOutbound
}

func (s *daemonRecordingSink) Send(out prismtransport.Outbound) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out = append(s.out, out)
	return nil
}

func (s *daemonRecordingSink) SendControl(out prismtransport.ControlOutbound) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controls = append(s.controls, out)
	return nil
}

func (s *daemonRecordingSink) conversationDetailSnapshots() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshots []map[string]any
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgConversationDetailSnapshot {
			continue
		}
		if payload, ok := item.Payload.(map[string]any); ok {
			snapshots = append(snapshots, payload)
		}
	}
	return snapshots
}

var _ bridge.PrismSink = (*daemonRecordingSink)(nil)
