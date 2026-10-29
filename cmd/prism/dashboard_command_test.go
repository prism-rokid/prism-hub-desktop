package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	prismapi "rokid-prism-hub/internal/api"
	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	daemonlock "rokid-prism-hub/internal/lock"
	"rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginregistry"
	"rokid-prism-hub/internal/pluginruntime"
)

type dashboardRegistryTestFetcher struct{}

func TestResolveManagedTerminalRequestUsesCanonicalBinding(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prism.sqlite")
	db, err := prismdb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	record, err := db.EnsureCanonicalSessionBinding(context.Background(), prismdb.SessionRecord{
		PluginID: "claudecode", NativeSessionID: "native-1", NativeThreadID: "thread-1", Cwd: "/project/original",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	request, err := resolveManagedTerminalRequest(context.Background(), dbPath, "claudecode", "/untrusted", record.PrismConversationID)
	if err != nil {
		t.Fatal(err)
	}
	if request.NativeSessionID != "native-1" || request.NativeThreadID != "thread-1" || request.Cwd != "/project/original" {
		t.Fatalf("unexpected managed terminal target: %+v", request)
	}
	if _, err := resolveManagedTerminalRequest(context.Background(), dbPath, "hermes", "", record.PrismConversationID); err == nil {
		t.Fatal("cross-plugin conversation target was accepted")
	}
	if _, err := resolveManagedTerminalRequest(context.Background(), dbPath, "claudecode", "", "conv_missing"); err == nil {
		t.Fatal("unknown conversation target was accepted")
	}
	legacy, err := resolveManagedTerminalRequest(context.Background(), dbPath, "hermes", "/project/hermes", "")
	if err != nil || legacy.NativeSessionID != "" || legacy.Cwd != "/project/hermes" {
		t.Fatalf("legacy launch changed: %+v, err=%v", legacy, err)
	}
}

func (dashboardRegistryTestFetcher) Fetch(_ context.Context, entry pluginregistry.Entry, _ string) (*pluginregistry.Release, string, bool, error) {
	platOS, arch := runtime.GOOS, runtime.GOARCH
	if platOS == "darwin" {
		platOS = "macos"
	}
	if arch == "amd64" {
		arch = "x64"
	}
	return &pluginregistry.Release{
		Repository: entry.Repository,
		Tag:        "v10.0.0",
		Version:    "10.0.0",
		Metadata: pluginregistry.ReleaseMetadata{
			PluginID: entry.ID, Version: "10.0.0", ProtocolVersion: "4",
			SupportedPlatforms: []string{platOS + "-" + arch},
		},
	}, `"etag"`, false, nil
}

func writeDashboardTestPlugin(t *testing.T, pluginsRoot, pluginID, displayName string) {
	t.Helper()
	pluginDir := filepath.Join(pluginsRoot, pluginID)
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatalf("mkdir plugin dir: %v", err)
	}
	manifest := `
id: ` + pluginID + `
display_name: ` + displayName + `
version: 9.8.7
integration_mode: desktop-automation
transport: process
mode: desktop
command: ["node", "index.js"]
capabilities:
  native_visible_input: true
  can_attach_session: true
  can_start_session_with_message: true
  can_list_sessions: true
  can_read_history: true
  can_forward_sync: true
  can_wait_run: true
`
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	// Registration probes the plugin before building the cloud payload. Keep the
	// fixture executable so this test covers the real manifest -> probe path.
	script := `const readline = require("node:readline");
const capability = {
  plugin_id: "` + pluginID + `",
  available: true,
  native_visible_input: true,
  native_visible_output: true,
  can_attach_session: true,
  can_start_session_with_message: true,
  can_list_sessions: true,
  can_read_history: true,
  can_forward_sync: true,
  can_reverse_sync: true,
  can_wait_run: true,
  can_read_status: true,
  can_control_session: true,
  integration_mode: "desktop-automation"
};
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const request = JSON.parse(line);
  if (request.method === "adapter.probe") {
    process.stdout.write(JSON.stringify({ id: request.id, ok: true, payload: capability }) + "\\n");
  }
});
`
	if err := os.WriteFile(filepath.Join(pluginDir, "index.js"), []byte(script), 0o600); err != nil {
		t.Fatalf("write plugin probe fixture: %v", err)
	}
}

func TestCollectDashboardStatusReflectsConfig(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	prevStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = prevStatePath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionGlobal
	cfg.Prism.HostToken = "token-1"
	cfg.Bridge.Enabled = true
	cfg.Bridge.KeepAwake = true
	cfg.Bridge.PluginEnv = map[string]string{"CUSTOM_GLOBAL": "1", "PRISM_HOST_TOKEN": "blocked"}
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Env: map[string]string{"PRISM_CODEX_ALLOW_MANAGED_RELAUNCH": "1", "PRISM_CONFIG": "blocked"},
	}
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "demo", "Demo")
	manifestPath := filepath.Join(pluginsRoot, "demo", "pluginbridge-plugin.yaml")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest = append(manifest, []byte("runtime_modes: [native, managed]\ndefault_runtime_mode: native\n")...)
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatalf("write runtime mode manifest: %v", err)
	}
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	cfg.Account.AccountID = "acc-1"
	cfg.Account.AccessToken = "account-token-1"
	cfg.Account.HostAlias = "Desk"
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "prism.sqlite")
	status := collectDashboardStatus(cfgPath, dbPath)
	if !status.OK {
		t.Fatalf("expected ok status, got error=%s", status.Error)
	}
	if status.Region != bridgeconfig.PrismRegionGlobal {
		t.Fatalf("unexpected region: %q", status.Region)
	}
	if !status.HostTokenSet {
		t.Fatal("expected host token flag")
	}
	if !status.KeepAwakeEnabled {
		t.Fatal("expected keep-awake enabled flag")
	}
	if status.Account["account_id"] != "acc-1" {
		t.Fatalf("unexpected account payload: %+v", status.Account)
	}
	configPayload := status.Config
	pluginEnv, _ := configPayload["plugin_env"].(map[string]string)
	if pluginEnv["CUSTOM_GLOBAL"] != "1" {
		t.Fatalf("unexpected plugin env payload: %+v", configPayload["plugin_env"])
	}
	pluginModes, _ := configPayload["plugin_modes"].(map[string]string)
	if pluginModes["demo"] != "native" {
		t.Fatalf("unexpected default plugin mode payload: %+v", configPayload["plugin_modes"])
	}
	if _, exists := configPayload["agent_env"]; exists {
		t.Fatalf("dashboard must not expose internal agent config: %+v", configPayload)
	}
	if _, exists := configPayload["remote_enabled"]; exists {
		t.Fatalf("dashboard must not expose a local remote-enabled fallback: %+v", configPayload)
	}
	if !status.LoggedIn {
		t.Fatal("expected logged in status")
	}
	if !status.AccountTokenSet || !status.RemoteControlReady {
		t.Fatalf("expected remote control ready status, got account_token_set=%v remote_control_ready=%v", status.AccountTokenSet, status.RemoteControlReady)
	}
	if status.ConnectionState != "disconnected" || status.ConnectionLabel != "未连接" {
		t.Fatalf("unexpected connection state: %q / %q", status.ConnectionState, status.ConnectionLabel)
	}
	if status.LifecycleState != "ready" && status.LifecycleState != "running" {
		t.Fatalf("unexpected lifecycle state: %q", status.LifecycleState)
	}
}

func TestDashboardConnectionUsesCanonicalLabelForKnownState(t *testing.T) {
	status := dashboardStatus{HostTokenSet: true, HubRunning: true, BridgeEnabled: true}
	runtimeState := &daemonRuntimeState{ConnectionState: "connected", ConnectionLabel: "未连接"}

	state, label, _ := dashboardConnection(status, runtimeState)
	if state != "connected" || label != "已连接" {
		t.Fatalf("expected canonical connected label, got %q / %q", state, label)
	}
}

func TestCollectDashboardStatusToleratesBrokenConfig(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	prevStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = prevStatePath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("bridge:\n  totally_unknown_key: 1\n"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "prism.sqlite")

	status := collectDashboardStatus(cfgPath, dbPath)
	if status.OK {
		t.Fatal("expected ok=false for a broken config")
	}
	if status.Error == "" {
		t.Fatal("expected the config error to be surfaced")
	}
	// The payload must stay usable enough for the Dashboard to render a
	// degraded view and route the user to the doctor panel.
	if status.LifecycleState == "" || status.ConnectionState == "" || status.RecommendedAction == "" {
		t.Fatalf("degraded status lost fillable fields: %+v", status)
	}
	if status.ConfigPath != cfgPath || status.DBPath != dbPath {
		t.Fatalf("degraded status lost paths: %+v", status)
	}
}

func TestDashboardManualConnectionRefreshRequiresReauthForExpiredToken(t *testing.T) {
	restarted := false
	payload := dashboardManualConnectionRefresh(dashboardStatus{
		HostTokenSet:    true,
		LoggedIn:        true,
		BridgeEnabled:   true,
		ConnectionState: "auth_expired",
	}, func() map[string]any {
		restarted = true
		return map[string]any{"action": "restart_scheduled"}
	})
	if payload["action"] != "reauth_required" {
		t.Fatalf("expected reauth action, got %+v", payload)
	}
	if restarted {
		t.Fatal("expired credentials must not restart the Hub")
	}
}

func TestDashboardHostPluginRequiresReauthForOwnershipFailure(t *testing.T) {
	if !dashboardHostPluginRequiresReauth(&prismapi.ResponseError{Code: "host_forbidden"}) {
		t.Fatal("host_forbidden must require a fresh device login")
	}
	if dashboardHostPluginRequiresReauth(&prismapi.ResponseError{Code: "invalid_parameter"}) {
		t.Fatal("unrelated API errors must not force a new login")
	}
}

func TestCollectDashboardStatusDisablesRemoteControlForExpiredHostToken(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	previousStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = previousStatePath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Prism.HostToken = "expired-token"
	cfg.Account.AccountID = "account-1"
	cfg.Account.AccessToken = "account-token-1"
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}
	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{
		ConnectionState:  "auth_expired",
		ConnectionDetail: "Host token expired",
	}); err != nil {
		t.Fatalf("write runtime state: %v", err)
	}

	status := collectDashboardStatus(cfgPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if status.ConnectionState != "auth_expired" {
		t.Fatalf("expected auth_expired, got %q", status.ConnectionState)
	}
	if status.RemoteControlReady {
		t.Fatal("expired credentials must not load account-owned host plugins")
	}
}

func TestCollectDashboardStatusExposesHostPluginSyncStatus(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	previousStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = previousStatePath }()

	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{
		HostPluginSyncState:   "stale",
		HostPluginSyncDetail:  "Host Plugin 同步暂时失败，继续使用上次成功的授权集。",
		HostPluginSyncUpdated: "2026-09-23T00:00:00Z",
		HostPluginCount:       3,
	}); err != nil {
		t.Fatalf("写入运行时状态失败: %v", err)
	}

	status := collectDashboardStatus(filepath.Join(t.TempDir(), "missing.yaml"), filepath.Join(t.TempDir(), "prism.sqlite"))
	if status.HostPluginSyncState != "stale" || status.HostPluginCount != 3 {
		t.Fatalf("Dashboard 未暴露 Host Plugin 同步状态: %+v", status)
	}
	if status.HostPluginSyncDetail == "" || status.HostPluginSyncUpdatedAt == "" {
		t.Fatalf("Dashboard 缺少 Host Plugin 同步详情: %+v", status)
	}
}

func TestDashboardManualConnectionRefreshRestartsDisconnectedHub(t *testing.T) {
	restarted := false
	payload := dashboardManualConnectionRefresh(dashboardStatus{
		HostTokenSet:    true,
		LoggedIn:        true,
		BridgeEnabled:   true,
		ConnectionState: "disconnected",
	}, func() map[string]any {
		restarted = true
		return map[string]any{"action": "restart_scheduled"}
	})
	if payload["action"] != "restart_requested" {
		t.Fatalf("expected restart action, got %+v", payload)
	}
	if !restarted {
		t.Fatal("disconnected Hub should be restarted")
	}
	if hubApply, _ := payload["hub_apply"].(map[string]any); hubApply["action"] != "restart_scheduled" {
		t.Fatalf("expected restart result, got %+v", payload)
	}
}

func TestDashboardTranslationJSON(t *testing.T) {
	source := []byte("\ufeffkey,zh-CN,en-US,ja-JP\ncommon.greeting,你好 {name},Hello {name},こんにちは {name}\ncommon.safe,</script>,</script>,</script>\n")
	payload, err := dashboardTranslationJSON(source)
	if err != nil {
		t.Fatalf("dashboardTranslationJSON: %v", err)
	}
	if !strings.Contains(string(payload), `"common.greeting":"Hello {name}"`) {
		t.Fatalf("unexpected translation payload: %s", payload)
	}
	if !strings.Contains(string(payload), `\u003c/script\u003e`) {
		t.Fatalf("translation JSON must escape script terminators: %s", payload)
	}
	page, err := template.ParseFS(dashboardAssets, "dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("parse dashboard page: %v", err)
	}
	var rendered bytes.Buffer
	if err := page.Execute(&rendered, dashboardPageData{Title: "test", Translations: payload, HubVersion: "1.2.3"}); err != nil {
		t.Fatalf("render dashboard page: %v", err)
	}
	if !strings.Contains(rendered.String(), `const dashboardTranslations = {`) {
		t.Fatal("dashboard page did not include translation dictionary")
	}
	if !strings.Contains(rendered.String(), `Hub v1.2.3`) {
		t.Fatal("dashboard page did not include the Hub version")
	}

	embeddedTranslations, err := dashboardAssets.ReadFile("dashboard_assets/translations.csv")
	if err != nil {
		t.Fatalf("read embedded dashboard translations: %v", err)
	}
	if _, err := dashboardTranslationJSON(embeddedTranslations); err != nil {
		t.Fatalf("validate embedded dashboard translations: %v", err)
	}
}

func TestCodexUsageFromRateLimitsResultProjectsFiveHourAndWeeklyWindows(t *testing.T) {
	usage := codexUsageFromRateLimitsResult(map[string]any{
		"rateLimitsByLimitId": map[string]any{
			"codex": map[string]any{
				"primary":   map[string]any{"usedPercent": 18.4, "windowDurationMins": 300, "resetsAt": int64(1700003600)},
				"secondary": map[string]any{"usedPercent": 42.2, "windowDurationMins": 10080, "resetsAt": int64(1700600000)},
			},
		},
	})
	if usage == nil {
		t.Fatal("expected usage projection")
	}
	fiveHour, _ := usage["five_hour"].(map[string]any)
	weekly, _ := usage["weekly"].(map[string]any)
	if fiveHour["used_percent"] != 18 || fiveHour["window_minutes"] != int64(300) || fiveHour["resets_at"] != int64(1700003600000) {
		t.Fatalf("unexpected five-hour projection: %#v", fiveHour)
	}
	if weekly["used_percent"] != 42 || weekly["window_minutes"] != int64(10080) || weekly["resets_at"] != int64(1700600000000) {
		t.Fatalf("unexpected weekly projection: %#v", weekly)
	}
}

func TestDashboardThemeUsesTokensWithoutComponentOverrides(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	css := string(page)
	if strings.Contains(css, ":root:not([data-theme=\"dark\"])") {
		t.Fatal("dashboard must not maintain a second component stylesheet for light theme")
	}
	if count := strings.Count(css, `:root[data-theme="dark"]`); count != 1 {
		t.Fatalf("dark theme must have one token block, found %d theme selectors", count)
	}
	darkStart := strings.Index(css, `:root[data-theme="dark"]`)
	darkEnd := strings.Index(css[darkStart:], "/* Theme application is shared.")
	if darkEnd < 0 {
		t.Fatal("dashboard theme contract marker is missing")
	}
	darkBlock := css[darkStart : darkStart+darkEnd]
	if regexp.MustCompile(`(?m)^\s*[.#][A-Za-z_-]`).MatchString(darkBlock) {
		t.Fatal("dark theme block must contain token declarations only, not component selectors")
	}
}

func TestDashboardTranslationJSONRejectsInvalidRows(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   string
	}{
		{"duplicate", "key,zh-CN,en-US,ja-JP\na,甲,A,あ\na,乙,B,い\n", "duplicate key"},
		{"empty", "key,zh-CN,en-US,ja-JP\na,,A,あ\n", "translation is required"},
		{"placeholders", "key,zh-CN,en-US,ja-JP\na,{name},Hello,名前 {name}\n", "placeholder set differs"},
		{"columns", "key,zh-CN,en-US,ja-JP\na,甲,A\n", "expected 4 columns"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dashboardTranslationJSON([]byte(tc.source))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDashboardRecoveryActionResources(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, want := range []string{
		`data-recovery-action="connection-refresh"`,
		`fetch("/api/connection/refresh"`,
		// Post-action state convergence: settle polling plus restart-aware
		// recovery keep the page from parking on a pre-action snapshot.
		`function scheduleStatusSettle(`,
		`function scheduleReloaderSettle()`,
		`function hubRestartScheduled(payload)`,
		`function handleScheduledHubRestart()`,
		`function watchForHubReturn(`,
		`function refreshAfterDashboardMutation(payload)`,
		`scheduleStatusSettle(() => cloudConnectionActive())`,
		`dashboardIsHidden() || state.awaitingHubRestart`,
		`id="agent-recovery-notices"`,
		`id="agent-recovery-notices-toggle"`,
		`id="agent-recovery-notice-count"`,
		`--recovery-notice-bg: #ffffff;`,
		`--recovery-notice-border: #f7cf7e80;`,
		`--recovery-notice-bg: #211916;`,
		`--recovery-notice-border: #62432d;`,
		`border: 1px solid var(--recovery-notice-border);`,
		`background: var(--recovery-notice-bg);`,
		`id="check-update-btn"`,
		`id="desktop-update-btn"`,
		`data-lucide="download"`,
		`desktopUpdate.state !== "available"`,
		`type: "prism:install-update"`,
		`desktop-update-label`,
		`type: "prism:check-for-update"`,
		`data-lucide="chevron-down"`,
		`recoveryNoticesCollapsed`,
		`recoveryNoticeCount: 0`,
		`state.recoveryNoticeCount === 0`,
		`function renderAgentRecoveryNotices(data)`,
		`agentRuntimeRecovery(item, data)`,
		`class="agent-recovery-notice-actions"`,
		`action: "managed-launch"`,
		`data-recovery-action="${escapeHtml(action)}"`,
		`launchDesktopApp(appId, { notify: false })`,
		`function scheduleManagedDesktopStatusRefresh(appId)`,
		`scheduleStatusSettle(() => Boolean(state.lastStatus?.desktop_apps?.[appId]?.cdp_reachable))`,
		`result?.message || (result ? t("agent.managed_recovered"`,
		`action: "agent-refresh"`,
		`data-recovery-toggle="remote-enabled"`,
		`updateHostPlugin(pluginId, true)`,
		`recoveryMenuMarkup({`,
		`const remoteLifecycleStep = remoteEnabled ?`,
		`${runtimeStatus}<i class="life-arrow"`,
		`overflow: visible;`,
		`background: transparent;`,
		`recovery-control.is-recoverable:hover .recovery-popover`,
		`top: calc(100% - 8px);`,
		`window.addEventListener("offline"`,
		`backdrop-filter: blur(12px)`,
		`recoveryControl.dataset.recoveryDismissed = "true"`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard recovery action resource missing %q", want)
		}
	}
	if noticesAt, agentsAt := strings.Index(source, `id="agent-recovery-notices"`), strings.Index(source, `data-i18n="agent.bridge_plugins">Agents`); noticesAt < 0 || agentsAt < 0 || noticesAt > agentsAt {
		t.Fatalf("pending notices must render above Agents: notices=%d agents=%d", noticesAt, agentsAt)
	}

	translations, err := dashboardAssets.ReadFile("dashboard_assets/translations.csv")
	if err != nil {
		t.Fatalf("read dashboard translations: %v", err)
	}
	for _, key := range []string{
		"action.refresh_connection,",
		"recovery.server_disconnected,",
		"recovery.pending,",
		"recovery.collapse,",
		"drawer.check_update,",
		"recovery.agent_interrupted,",
		"agent.refresh,",
		"agent.remote_enabled_success,",
		"agent.managed_recovered,",
	} {
		if !strings.Contains(string(translations), key) {
			t.Fatalf("dashboard recovery translation missing %q", key)
		}
	}
}

func TestDashboardLoginExternalAccountLinks(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, want := range []string{
		`--login-action: #3d3e45;`,
		`class="login-brand" aria-label="Prism HUB"`,
		`src="/assets/prism_logo_nobg@2x.png"`,
		`class="login-wordmark">Prism HUB</strong>`,
		`font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;`,
		`font-style: italic;`,
		`letter-spacing: -1.5px;`,
		`class="register-button" href="{{.RegisterURL}}"`,
		`id="forgot-password-link" href="{{.ForgotPasswordURL}}"`,
		`target="_blank" rel="noopener noreferrer"`,
		`type: "prism:open-forgot-password"`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard login resource missing %q", want)
		}
	}
	for _, unwanted := range []string{"fromPage=register", "source=web"} {
		if strings.Contains(source, unwanted) {
			t.Fatalf("dashboard forgot-password link must not contain %q", unwanted)
		}
	}
	logo, err := dashboardAssets.ReadFile("dashboard_assets/prism_logo_nobg@2x.png")
	if err != nil || len(logo) == 0 {
		t.Fatalf("read transparent Prism logo: %v", err)
	}

	translations, err := dashboardAssets.ReadFile("dashboard_assets/translations.csv")
	if err != nil {
		t.Fatalf("read dashboard translations: %v", err)
	}
	for _, key := range []string{"login.forgot_password,", "login.register_account,"} {
		if !strings.Contains(string(translations), key) {
			t.Fatalf("dashboard login translation missing %q", key)
		}
	}
}

func TestDashboardAgentCardsDoNotRenderOnlineStatus(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, unwanted := range []string{
		`.agent-online`,
		`const onlineBadge =`,
		`const onlineLabel =`,
		`${onlineBadge}`,
		`data-lucide="package-check"`,
		`${t("agent.installed")}`,
		`data-lucide="radio" aria-hidden="true"></i>${onlineLabel}`,
		`data-lucide="radio" aria-hidden="true"></i>${t(cloudOnline ? "agent.online" : "agent.offline")}`,
	} {
		if strings.Contains(source, unwanted) {
			t.Fatalf("agent card must not render online status %q", unwanted)
		}
	}
}

func TestDashboardEmptyAgentStateUsesIllustrationAndPluginCTA(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, want := range []string{
		`id="agent-empty-illustration" src="/assets/without_plugin.png"`,
		`.plugin-manager-entry.needs-plugin`,
		`top: 100%;`,
		`right: 100%;`,
		`width: min(166px, calc(100vw - 40px));`,
		`pluginManagerButton.classList.toggle("needs-plugin", noAgents);`,
		`emptyIllustration.hidden = !noAgents;`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard empty-agent state missing %q", want)
		}
	}
	if strings.Contains(source, `还没有发现本地 Agent。点击“安装 Agent”添加本地路径或 Git URL。`) {
		t.Fatal("dashboard must not render an instructional empty-agent message")
	}
	asset, err := dashboardAssets.ReadFile("dashboard_assets/without_plugin.png")
	if err != nil || len(asset) == 0 {
		t.Fatalf("read empty-agent illustration: %v", err)
	}
	config, err := png.DecodeConfig(bytes.NewReader(asset))
	if err != nil {
		t.Fatalf("decode empty-agent illustration: %v", err)
	}
	if config.Width != 352 || config.Height != 304 {
		t.Fatalf("empty-agent illustration resolution = %dx%d, want 352x304", config.Width, config.Height)
	}
}

func TestDashboardBrandAvatarUsesDeclaredURLsWithFixedFallback(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, want := range []string{
		`const defaultAgentIconURL = "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/codex.webp";`,
		`const catalogRaster = validAgentIconURL(item?.catalog_icon_url);`,
		`const raster = validAgentIconURL(item?.manifest_icon_url || item?.icon_url);`,
		`return [...new Set([custom, catalogRaster, raster, svg, defaultAgentIconURL].filter(Boolean))];`,
		`data-lucide="bot"`,
		`advanceAgentIcon(this)`,
		`const presentation = localPresentation(item, persisted);`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard plugin avatar resource missing %q", want)
		}
	}
}

func TestDashboardAgentMetadataFormSurvivesTranslation(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatalf("read dashboard page: %v", err)
	}
	source := string(page)
	for _, want := range []string{
		`<span data-i18n="agent.edit_icon_url">`,
		`<input id="agent-metadata-icon-url"`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("dashboard agent metadata form missing %q", want)
		}
	}
	for _, absent := range []string{
		`<span data-i18n="agent.edit_description">`,
		`<textarea id="agent-metadata-description"`,
	} {
		if strings.Contains(source, absent) {
			t.Fatalf("dashboard agent metadata form must not contain %q", absent)
		}
	}
}

func TestCollectServingHubDashboardStatusSkipsSelfHealthProbe(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	lock, err := daemonlock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("acquire hub lock: %v", err)
	}
	defer lock.Release()

	previousReachable := dashboardHubReachable
	calls := 0
	dashboardHubReachable = func(string) bool {
		calls++
		return false
	}
	defer func() { dashboardHubReachable = previousReachable }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := bridgeconfig.Default().Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}
	status := collectServingHubDashboardStatus(cfgPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if calls != 0 {
		t.Fatalf("serving Hub must not recursively probe its own status endpoint, calls=%d", calls)
	}
	if !status.HubRunning || !status.HubReachable {
		t.Fatalf("serving Hub status should be locally known as reachable: %+v", status)
	}
}

func TestDashboardCodexDesktopStatusReadsManagedLaunchFromManifest(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins", "codex")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatalf("mkdir plugin dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(`
id: codex
integration_mode: desktop-automation
transport: process
runtime_modes: [desktop]
default_runtime_mode: desktop
managed_launch: true
managed_launch_specs:
  - kind: desktop-app
    app: codex
    ready: probe
command: ["node", "index.js"]
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{filepath.Join(root, "plugins")}
	status := dashboardCodexDesktopStatus(cfg, cfg.Bridge.PluginDirs)
	if status["managed_enabled"] != true {
		t.Fatalf("expected managed enabled from manifest, got %+v", status)
	}
	if status["app_path"] != "" && status["can_launch_managed"] != true {
		t.Fatalf("expected launch enabled when app path exists, got %+v", status)
	}
	if status["managed_status_label"] == "" || status["managed_status_message"] == "" {
		t.Fatalf("expected managed status copy to be present, got %+v", status)
	}
}

func TestDashboardManagedStatusHelpersForRunningUnmanaged(t *testing.T) {
	if !dashboardDesktopManagedRestartRequired("running_unmanaged") {
		t.Fatal("expected running_unmanaged to require confirmation")
	}
	label := dashboardDesktopManagedStatusLabel("running_unmanaged")
	if label != "当前是手动启动实例" {
		t.Fatalf("unexpected managed status label: %q", label)
	}
	spec, _ := dashboardDesktopAppSpecByID("codex")
	message := dashboardDesktopManagedStatusMessage(spec, "running_unmanaged", "ChatGPT", "")
	if !strings.Contains(message, "没有暴露 Prism 需要的 CDP 端口") {
		t.Fatalf("unexpected managed status message: %q", message)
	}
	confirm := dashboardDesktopManagedConfirmationMessage(spec, "running_unmanaged", "ChatGPT")
	if !strings.Contains(confirm, "会先重启当前应用") {
		t.Fatalf("unexpected confirmation message: %q", confirm)
	}
}

func TestDashboardWindowsDesktopProcessIsMain(t *testing.T) {
	codex, _ := dashboardDesktopAppSpecByID("codex")
	if !dashboardWindowsDesktopProcessIsMain(codex, "Codex.exe", `C:\Users\me\AppData\Local\Programs\Codex\Codex.exe`) {
		t.Fatal("Codex main process was not recognized")
	}
	if dashboardWindowsDesktopProcessIsMain(codex, "Codex Helper.exe", `C:\Users\me\AppData\Local\Programs\Codex\Codex.exe`) {
		t.Fatal("Codex helper process must not be treated as main")
	}
	if dashboardWindowsDesktopProcessIsMain(codex, "Other.exe", `C:\Other\Other.exe`) {
		t.Fatal("unrelated process was recognized")
	}
}

func TestDashboardDesktopProcessIsMainMatchesWindowsCommandLines(t *testing.T) {
	codex, _ := dashboardDesktopAppSpecByID("codex")
	cases := []struct {
		command string
		want    bool
	}{
		// Microsoft Store main process, no arguments beyond the executable.
		{`"C:\Program Files\WindowsApps\OpenAI.Codex_26.1023.6123.0_x64__2p2nqsd0c76g0\app\ChatGPT.exe"`, true},
		// Managed launch of the Store build.
		{`"C:\Program Files\WindowsApps\OpenAI.Codex_26.1023.6123.0_x64__2p2nqsd0c76g0\app\ChatGPT.exe" --user-data-dir=C:\Users\me\AppData\Roaming\Codex\web\Codex --remote-debugging-port=0`, true},
		// Store helper renderer: same executable name, but --type= marks it.
		{`"C:\Program Files\WindowsApps\OpenAI.Codex_26.1023.6123.0_x64__2p2nqsd0c76g0\app\ChatGPT.exe" --type=renderer --user-data-dir=C:\Users\me\AppData\Roaming\Codex\web\Codex`, false},
		// Classic install.
		{`C:\Users\me\AppData\Local\Programs\Codex\Codex.exe --remote-debugging-port=0`, true},
	}
	for _, tc := range cases {
		if got := dashboardDesktopProcessIsMain(codex, tc.command); got != tc.want {
			t.Fatalf("dashboardDesktopProcessIsMain(%q) = %v, want %v", tc.command, got, tc.want)
		}
	}
}

func TestDashboardWindowsDesktopProcessSnapshot(t *testing.T) {
	previous := dashboardWindowsProcessSnapshot
	dashboardWindowsProcessSnapshot = func() ([]byte, error) {
		return []byte(`[{"ProcessId":12,"Name":"Codex.exe","CommandLine":"Codex.exe --remote-debugging-port=0 --user-data-dir=C:\\Codex"},{"ProcessId":13,"Name":"Codex Helper.exe","CommandLine":"helper"}]`), nil
	}
	t.Cleanup(func() { dashboardWindowsProcessSnapshot = previous })
	old := runtime.GOOS
	_ = old
	rows := []struct {
		ProcessID   int    `json:"ProcessId"`
		Name        string `json:"Name"`
		CommandLine string `json:"CommandLine"`
	}{}
	if err := json.Unmarshal([]byte(`[{"ProcessId":12,"Name":"Codex.exe","CommandLine":"Codex.exe --remote-debugging-port=0 --user-data-dir=C:\\Codex"}]`), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("snapshot fixture invalid: %v", err)
	}
}

func TestDashboardManagedDesktopCommandRunsBinaryWithManagedArgs(t *testing.T) {
	args := dashboardManagedDesktopLaunchArgs("/tmp/codex-profile")
	joinedArgs := strings.Join(args, " ")
	if !strings.Contains(joinedArgs, "--user-data-dir=/tmp/codex-profile") || !strings.Contains(joinedArgs, "--remote-debugging-port=0") {
		t.Fatalf("managed launch must preserve its profile and CDP arguments: %q", args)
	}

	if runtimeGOOS() != "darwin" {
		return
	}
	cmd := dashboardManagedDesktopCommand("/Applications/ChatGPT.app/Contents/MacOS/ChatGPT", args)
	joinedCommand := strings.Join(cmd.Args, " ")
	if cmd.Path != "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT" {
		t.Fatalf("managed launch must execute the app binary directly: %q", cmd.Path)
	}
	if !strings.Contains(joinedCommand, "--user-data-dir=/tmp/codex-profile") || !strings.Contains(joinedCommand, "--remote-debugging-port=0") {
		t.Fatalf("managed launch arguments were not passed to the app binary: %q", cmd.Args)
	}
}

func TestDashboardWaitForCDPWaitsUntilEndpointIsReachable(t *testing.T) {
	calls := 0
	port, ready := dashboardWaitForCDP("/tmp/DevToolsActivePort", 3, 0, func(file string) (string, bool) {
		if file != "/tmp/DevToolsActivePort" {
			t.Fatalf("unexpected DevToolsActivePort path: %q", file)
		}
		calls++
		return "9222", calls == 3
	})
	if !ready || port != "9222" || calls != 3 {
		t.Fatalf("managed launch must wait for a reachable CDP endpoint: port=%q ready=%v calls=%d", port, ready, calls)
	}
}

func TestDashboardLoopbackListeningPortsIgnoresNonLoopbackListeners(t *testing.T) {
	ports := dashboardLoopbackListeningPorts(`
ChatGPT 37074 xin.he 60u IPv4 0x1 0t0 TCP 127.0.0.1:54416 (LISTEN)
ChatGPT 37074 xin.he 61u IPv6 0x2 0t0 TCP [::1]:54417 (LISTEN)
ChatGPT 37074 xin.he 62u IPv4 0x3 0t0 TCP *:54418 (LISTEN)
ChatGPT 37074 xin.he 63u IPv4 0x4 0t0 TCP 127.0.0.1:54416 (LISTEN)
`)
	if got, want := strings.Join(ports, ","), "54416,54417"; got != want {
		t.Fatalf("unexpected loopback listening ports: got %q want %q", got, want)
	}
}

func TestDashboardCodexDesktopStatusReportsOverrideSource(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Env: map[string]string{
			"PRISM_CODEX_APP_PATH":      "/tmp/Codex.app/Contents/MacOS/Codex",
			"PRISM_CODEX_USER_DATA_DIR": "/tmp/codex-profile",
		},
	}
	status := dashboardCodexDesktopStatus(cfg, nil)
	if status["app_path_override"] != "/tmp/Codex.app/Contents/MacOS/Codex" {
		t.Fatalf("unexpected app path override: %+v", status)
	}
	if status["user_data_dir_override"] != "/tmp/codex-profile" {
		t.Fatalf("unexpected user data dir override: %+v", status)
	}
	if status["app_path_source"] != "configured" || status["user_data_dir_source"] != "configured" {
		t.Fatalf("expected configured sources, got %+v", status)
	}
}

func TestDashboardPluginReloaderApplyDoesNotRestartHub(t *testing.T) {
	result := dashboardPluginReloaderApply("plugin_installed")
	if result["requested"] != false || result["applied"] != false {
		t.Fatalf("unexpected plugin reloader result: %+v", result)
	}
	if result["action"] != "reloader_watching" {
		t.Fatalf("plugin mutation must use the running reloader: %+v", result)
	}
}

func TestDashboardScanLocalPlugins(t *testing.T) {
	root := t.TempDir()
	pluginsRoot := filepath.Join(root, "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "demo.local", "Demo Local")
	cfgPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	payload, err := dashboardScanLocalPlugins(cfgPath)
	if err != nil {
		t.Fatalf("scan local plugins: %v", err)
	}
	if payload["ok"] != true || payload["plugin_count"] != 1 {
		t.Fatalf("unexpected scan payload: %+v", payload)
	}
	pluginIDs, ok := payload["plugin_ids"].([]string)
	if !ok || len(pluginIDs) != 1 || pluginIDs[0] != "demo.local" {
		t.Fatalf("unexpected plugin ids: %+v", payload["plugin_ids"])
	}
}

func TestDashboardRegistryCatalogMergesInstallAndUpdateMetadata(t *testing.T) {
	root := t.TempDir()
	pluginsRoot := filepath.Join(root, "installed")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfgPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatal(err)
	}
	registry := pluginregistry.New(filepath.Join(root, "catalog.json"), "10.0.0", "4", dashboardRegistryTestFetcher{})
	registry.Refresh(context.Background())
	payload, err := dashboardPluginCatalogFromRegistry(cfgPath, registry, nil, newDashboardCatalogOperations())
	if err != nil {
		t.Fatal(err)
	}
	installed := payload["installed"].([]map[string]any)
	if len(installed) != 1 || installed[0]["plugin_id"] != "codex" || installed[0]["update_available"] != true {
		t.Fatalf("unexpected merged installed catalog: %+v", installed)
	}
	if installed[0]["latest_version"] != "10.0.0" || installed[0]["icon_svg_url"] == "" || installed[0]["catalog_icon_url"] == "" {
		t.Fatalf("latest version or icon fallback missing: %+v", installed[0])
	}
	library := payload["library"].([]map[string]any)
	if len(library) != 4 {
		t.Fatalf("fresh official library must expose remaining agents: %+v", library)
	}
}

func TestDashboardRegistryFallbackUsesOfficialColorAvatars(t *testing.T) {
	items := dashboardRegistryItems(nil)
	want := map[string]string{
		"codex":      "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/codex.webp",
		"claudecode": "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/claudecode.webp",
		"hermes":     "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/hermesagent.webp",
		"openclaw":   "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/openclaw.webp",
		"opencode":   "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/opencode.webp",
	}
	if len(items) != len(want) {
		t.Fatalf("fallback catalog count=%d", len(items))
	}
	for _, item := range items {
		if item.IconURL != want[item.ID] {
			t.Fatalf("fallback icon for %s = %q", item.ID, item.IconURL)
		}
	}
}

func TestDashboardStartCatalogOperationRefreshesFromEmptySnapshot(t *testing.T) {
	root := t.TempDir()
	registry := pluginregistry.New(filepath.Join(root, "catalog.json"), "10.0.0", "4", dashboardRegistryTestFetcher{})
	installer := plugininstall.New(filepath.Join(root, "plugins"))

	operation, err := dashboardStartCatalogOperation(context.Background(), registry, installer, "codex")
	if err != nil {
		t.Fatalf("cold-start catalog install should resolve official plugin: %v", err)
	}
	if operation.ID == "" || operation.PluginID != "codex" || operation.Version != "10.0.0" {
		t.Fatalf("unexpected install operation: %+v", operation)
	}
}

func TestDashboardScanLocalPluginsDoesNotInstallBundledPlugin(t *testing.T) {
	root := t.TempDir()
	installRoot := filepath.Join(root, "installed")

	cfgPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{installRoot}
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	payload, err := dashboardScanLocalPlugins(cfgPath)
	if err != nil {
		t.Fatalf("scan local plugins: %v", err)
	}
	if payload["plugin_count"] != 0 {
		t.Fatalf("fresh install must remain empty after scan, got %+v", payload)
	}
	installedIDs, ok := payload["installed_plugin_ids"].([]string)
	if !ok || len(installedIDs) != 0 {
		t.Fatalf("scan must never install bundled plugins: %+v", payload["installed_plugin_ids"])
	}
	if _, err := os.Stat(filepath.Join(installRoot, "demo.bundled")); !os.IsNotExist(err) {
		t.Fatalf("scan unexpectedly created bundled plugin installation: %v", err)
	}
}

func TestDashboardDesktopAppsIncludesOfficialSpecs(t *testing.T) {
	// Card membership is manifest-driven: an installed plugin gets a desktop
	// card only when its managed_launch_specs declare a desktop-app target
	// that the hub registry can resolve.
	root := t.TempDir()
	for _, pluginID := range []string{"codex", "hermes"} {
		pluginDir := filepath.Join(root, "plugins", pluginID)
		if err := os.MkdirAll(pluginDir, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest := fmt.Sprintf(`
id: %s
integration_mode: desktop-automation
transport: process
runtime_modes: [desktop]
managed_launch: true
managed_launch_specs:
  - kind: desktop-app
    app: %s
command: ["node", "index.js"]
capabilities:
  can_forward_sync: true
`, pluginID, pluginID)
		if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(manifest), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{filepath.Join(root, "plugins")}
	apps := dashboardDesktopApps(cfg, cfg.Bridge.PluginDirs)
	for _, appID := range []string{"codex", "hermes"} {
		item, ok := apps[appID].(map[string]any)
		if !ok {
			t.Fatalf("expected desktop app entry for %s, got %+v", appID, apps)
		}
		if item["app_id"] != appID {
			t.Fatalf("unexpected app_id for %s: %+v", appID, item)
		}
	}
	codex := apps["codex"].(map[string]any)
	if codex["display_name"] != "Codex" {
		t.Fatalf("expected Codex desktop entry to use the plugin display name, got %+v", codex)
	}
}

func TestDashboardCodexUserFacingCopyDoesNotUseHostBundleName(t *testing.T) {
	spec, ok := dashboardDesktopAppSpecByID("codex")
	if !ok {
		t.Fatal("Codex desktop app spec missing")
	}
	message := dashboardDesktopManagedStatusMessage(spec, "ready", "ChatGPT", "9222")
	confirmation := dashboardDesktopManagedConfirmationMessage(spec, "ready", "ChatGPT")
	if !strings.Contains(message, "Codex") || strings.Contains(message, "ChatGPT") {
		t.Fatalf("expected Codex-facing status copy, got %q", message)
	}
	if !strings.Contains(confirmation, "Codex") || strings.Contains(confirmation, "ChatGPT") {
		t.Fatalf("expected Codex-facing confirmation copy, got %q", confirmation)
	}
}

func TestDashboardCodexAppBundle(t *testing.T) {
	tests := map[string]string{
		"/Applications/ChatGPT.app/Contents/MacOS/ChatGPT": "/Applications/ChatGPT.app",
		"/Applications/Codex.app/Contents/MacOS/Codex":     "/Applications/Codex.app",
		"/Applications/ChatGPT.app":                        "/Applications/ChatGPT.app",
		"/tmp/not-an-app/binary":                           "",
	}
	for input, want := range tests {
		if got := dashboardCodexAppBundle(input); got != want {
			t.Fatalf("bundle mismatch for %q: got %q want %q", input, got, want)
		}
	}
}

func TestDashboardConfigHandlerSavesCoreFields(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Region        string `json:"region"`
				HostToken     string `json:"host_token"`
				BridgeEnabled *bool  `json:"bridge_enabled"`
				KeepAwake     *bool  `json:"keep_awake"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			cfg, err := bridgeconfig.Load(cfgPath)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			cfg.SelectRegion(req.Region)
			cfg.Prism.HostToken = req.HostToken
			if req.BridgeEnabled != nil {
				cfg.Bridge.Enabled = *req.BridgeEnabled
			}
			if req.KeepAwake != nil {
				cfg.Bridge.KeepAwake = *req.KeepAwake
			}
			if err := cfg.Save(cfgPath); err != nil {
				t.Fatalf("save config: %v", err)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	})

	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(`{
	  "region": "global",
	  "host_token": "token-9",
	  "bridge_enabled": false,
	  "keep_awake": true,
	  "default_plugin": "openclaw"
	}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", rec.Code)
	}

	loaded, err := bridgeconfig.Load(cfgPath)
	if err != nil {
		t.Fatalf("load saved config: %v", err)
	}
	if loaded.Prism.Region != bridgeconfig.PrismRegionGlobal {
		t.Fatalf("unexpected saved region: %q", loaded.Prism.Region)
	}
	if loaded.Prism.HostToken != "token-9" {
		t.Fatalf("unexpected saved token: %q", loaded.Prism.HostToken)
	}
	if loaded.Bridge.Enabled {
		t.Fatal("expected bridge.enabled false")
	}
	if !loaded.Bridge.KeepAwake {
		t.Fatal("expected bridge.keep_awake true")
	}
}

func TestDashboardPartialConfigUpdatePreservesOtherFields(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionGlobal
	cfg.Prism.HostToken = "token-1"
	cfg.Bridge.PluginEnv = map[string]string{"PRISM_SQLITE3_BIN": "/opt/homebrew/bin/sqlite3"}
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Env: map[string]string{
			"PRISM_CODEX_ALLOW_MANAGED_RELAUNCH": "1",
		},
	}

	payloadKeepAwake := true
	req := struct {
		KeepAwake *bool `json:"keep_awake"`
	}{
		KeepAwake: &payloadKeepAwake,
	}

	cfgBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}

	var decoded struct {
		Region        *string                      `json:"region"`
		HostToken     *string                      `json:"host_token"`
		BridgeEnabled *bool                        `json:"bridge_enabled"`
		KeepAwake     *bool                        `json:"keep_awake"`
		PluginEnv     map[string]string            `json:"plugin_env"`
		PluginEnvByID map[string]map[string]string `json:"plugin_env_by_id"`
	}
	if err := json.Unmarshal(cfgBytes, &decoded); err != nil {
		t.Fatalf("unmarshal req: %v", err)
	}

	if decoded.Region != nil || decoded.PluginEnv != nil {
		t.Fatalf("expected omitted fields to stay nil, got %+v", decoded)
	}

	if decoded.KeepAwake != nil {
		cfg.Bridge.KeepAwake = *decoded.KeepAwake
	}

	if cfg.Prism.Region != bridgeconfig.PrismRegionGlobal {
		t.Fatalf("region should be preserved, got %q", cfg.Prism.Region)
	}
	if cfg.Bridge.PluginEnv["PRISM_SQLITE3_BIN"] != "/opt/homebrew/bin/sqlite3" {
		t.Fatalf("plugin env should be preserved, got %+v", cfg.Bridge.PluginEnv)
	}
	if cfg.Bridge.Plugins["codex"].Env["PRISM_CODEX_ALLOW_MANAGED_RELAUNCH"] != "1" {
		t.Fatalf("plugin env should be preserved, got %+v", cfg.Bridge.Plugins["codex"])
	}
	if !cfg.Bridge.KeepAwake {
		t.Fatal("keep awake should be updated")
	}
}

func TestApplyDashboardHostPluginChangeUsesUserPluginAPI(t *testing.T) {
	var (
		postBodies []map[string]any
		deleteHits int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/user/hosts/host-1/plugins":
			if got := r.Header.Get("X-Account-Token"); got != "account-token-1" {
				t.Fatalf("unexpected account token: %q", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode post body: %v", err)
			}
			postBodies = append(postBodies, body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"accepted","data":{"accepted":true}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/user/hosts/host-1/plugins/codex":
			if got := r.Header.Get("X-Account-Token"); got != "account-token-1" {
				t.Fatalf("unexpected account token: %q", got)
			}
			deleteHits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"accepted","data":{"accepted":true,"deleted":true}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionCN
	cfg.Account.AccessToken = "account-token-1"
	cfg.Account.HostID = "host-1"
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	setDashboardRuntimePlugins(pluginruntime.New(pluginruntime.StaticPlugin{
		PluginID: "codex",
		Capability: pluginruntime.Capability{
			Available:                  true,
			CanForwardSync:             true,
			CanListSessions:            true,
			CanReadHistory:             true,
			CanWaitRun:                 true,
			CanAttachSession:           true,
			CanStartSessionWithMessage: true,
		},
	}))
	defer setDashboardRuntimePlugins(nil)

	payload, err := applyDashboardHostPluginChangeWithURL(
		cfg,
		server.URL,
		"codex",
		true,
	)
	if err != nil {
		t.Fatalf("applyDashboardHostPluginChangeWithURL: %v", err)
	}
	if payload["accepted"] != true {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if len(postBodies) != 1 {
		t.Fatalf("expected one register request, got %d", len(postBodies))
	}
	if postBodies[0]["display_name"] != "Codex" {
		t.Fatalf("expected register payload to include metadata, got %+v", postBodies[0])
	}
	if postBodies[0]["plugin_id"] != "codex" {
		t.Fatalf("expected register payload to carry plugin_id, got %+v", postBodies[0])
	}
	if postBodies[0]["version"] != "9.8.7" {
		t.Fatalf("expected register payload to carry manifest version, got %+v", postBodies[0])
	}
	if _, exists := postBodies[0]["agent_id"]; exists {
		t.Fatalf("expected register payload to avoid legacy agent identity, got %+v", postBodies[0])
	}
	if _, exists := postBodies[0]["agent_instance_id"]; exists {
		t.Fatalf("expected register payload to avoid composed identity, got %+v", postBodies[0])
	}

	payload, err = applyDashboardHostPluginChangeWithURL(
		cfg,
		server.URL,
		"codex",
		false,
	)
	if err != nil {
		t.Fatalf("disable applyDashboardHostPluginChangeWithURL: %v", err)
	}
	if payload["accepted"] != true || payload["enabled"] != false {
		t.Fatalf("expected disable payload to be accepted, got %+v", payload)
	}
	if deleteHits != 1 {
		t.Fatalf("expected one delete request, got %d", deleteHits)
	}
}

func TestApplyDashboardHostPluginChangeTreatsMissingDeleteAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/user/hosts/host-1/plugins/codex" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"success":false,"code":"plugin_not_found","message":"already removed"}`))
	}))
	defer server.Close()
	cfg := bridgeconfig.Default()
	cfg.Account.AccessToken = "account-token-1"
	cfg.Account.HostID = "host-1"
	payload, err := applyDashboardHostPluginChangeWithURL(cfg, server.URL, "codex", false)
	if err != nil {
		t.Fatalf("expected idempotent delete success: %v", err)
	}
	if payload["accepted"] != true || payload["enabled"] != false {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	result, ok := payload["result"].(map[string]any)
	if !ok || result["already_absent"] != true {
		t.Fatalf("expected already_absent result, got %+v", payload)
	}
}

func TestDashboardHostPluginRegistrationReportsRuntimeUnavailable(t *testing.T) {
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	svc := pluginruntime.New(pluginruntime.StaticPlugin{
		PluginID: "codex",
		Capability: pluginruntime.Capability{
			UnavailableReason: "hermes_desktop_renderer_or_state_db_unavailable",
		},
	})
	_, err := dashboardHostPluginRegistrationPayloadWithService(cfg, svc, "codex")
	if err == nil || !strings.Contains(err.Error(), "runtime unavailable") || strings.Contains(err.Error(), "missing_forward_sync") {
		t.Fatalf("expected runtime-unavailable error, got %v", err)
	}
}

func TestDashboardEnvOnlyPluginConfigStaysEnabled(t *testing.T) {
	pluginConfig := bridgeconfig.PluginConfig{
		Env: map[string]string{
			"PRISM_CODEX_ALLOW_MANAGED_RELAUNCH": "1",
		},
	}
	if dashboardPluginConfigEmpty(pluginConfig) {
		t.Fatalf("env-only dashboard plugin config should not be deleted: %+v", pluginConfig)
	}
}

func TestPersistDashboardPluginConfigKeepsExplicitDisable(t *testing.T) {
	cfg := bridgeconfig.Default()
	persistDashboardPluginConfig(cfg, "codex", bridgeconfig.PluginConfig{}, false)
	if _, ok := cfg.Bridge.Plugins["codex"]; ok {
		t.Fatalf("expected remote disable to no longer persist in local config, got %+v", cfg.Bridge.Plugins["codex"])
	}
}

func TestDashboardPluginModesExposeManifestDefaultAndConfiguredMode(t *testing.T) {
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	pluginDir := filepath.Join(pluginsRoot, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["node", "index.js"]
runtime_modes: [native, managed]
default_runtime_mode: native
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	if got := dashboardPluginModes(cfg)["demo"]; got != "native" {
		t.Fatalf("default manifest runtime mode = %q", got)
	}
	cfg.Bridge.Plugins["demo"] = bridgeconfig.PluginConfig{Mode: "managed"}
	if got := dashboardPluginModes(cfg)["demo"]; got != "managed" {
		t.Fatalf("configured manifest runtime mode = %q", got)
	}
}

func TestDashboardPluginsExposeManifestPresentation(t *testing.T) {
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	pluginDir := filepath.Join(pluginsRoot, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(`
id: demo
display_name: Demo Agent
description: Local manifest presentation
icon_url: https://example.test/demo.webp
version: 1.2.3
integration_mode: protocol-native
transport: process
command: ["node", "index.js"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	items := dashboardPlugins([]string{pluginsRoot}, nil)
	if len(items) != 1 {
		t.Fatalf("dashboard plugins = %+v", items)
	}
	item := items[0]
	for key, want := range map[string]string{
		"display_name":     "Demo Agent",
		"description":      "Local manifest presentation",
		"icon_url":         "https://example.test/demo.webp",
		"version":          "1.2.3",
		"integration_mode": "protocol-native",
	} {
		if got := item[key]; got != want {
			t.Fatalf("dashboard plugin %s = %v, want %q: %+v", key, got, want, item)
		}
	}
}

func TestDashboardPluginsExposeResolvedRuntimeMode(t *testing.T) {
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	pluginDir := filepath.Join(pluginsRoot, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "pluginbridge-plugin.yaml"), []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["node", "index.js"]
mode: legacy
runtime_modes: [desktop, acp]
default_runtime_mode: desktop
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Bridge.Plugins["demo"] = bridgeconfig.PluginConfig{Mode: "stale"}
	items := dashboardPlugins([]string{pluginsRoot}, cfg)
	if got := items[0]["mode"]; got != "desktop" {
		t.Fatalf("dashboard plugin mode = %v, want resolved default desktop: %+v", got, items[0])
	}
	if got := items[0]["current_runtime_mode"]; got != "desktop" {
		t.Fatalf("dashboard plugin current_runtime_mode = %v, want desktop: %+v", got, items[0])
	}
}

func TestDashboardPluginRuntimeIDDoesNotMutateLocalConfig(t *testing.T) {
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Mode: "desktop",
	}

	pluginID, err := dashboardPluginRuntimeID(cfg, "codex")
	if err != nil {
		t.Fatalf("dashboardPluginRuntimeID: %v", err)
	}
	if pluginID != "codex" {
		t.Fatalf("unexpected plugin id: %q", pluginID)
	}
	pluginConfig, ok := cfg.Bridge.Plugins[pluginID]
	if !ok || pluginConfig.Mode != "desktop" {
		t.Fatalf("expected local config to remain unchanged, got %+v", pluginConfig)
	}
}

func TestApplyDashboardDesktopAppOverridesStoresCodexOverrides(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginEnv = map[string]string{
		"PRISM_CODEX_APP_PATH":      "/legacy/global/Codex",
		"PRISM_CODEX_USER_DATA_DIR": "/legacy/global/profile",
	}
	applyDashboardDesktopAppOverrides(cfg, map[string]dashboardDesktopAppConfigRequest{
		"codex": {
			AppPath:     stringPtr("/custom/Codex.app/Contents/MacOS/Codex"),
			UserDataDir: stringPtr("/custom/profile"),
		},
	})
	pluginConfig := cfg.Bridge.Plugins["codex"]
	if pluginConfig.Env["PRISM_CODEX_APP_PATH"] != "/custom/Codex.app/Contents/MacOS/Codex" {
		t.Fatalf("unexpected codex app path override: %+v", pluginConfig.Env)
	}
	if pluginConfig.Env["PRISM_CODEX_USER_DATA_DIR"] != "/custom/profile" {
		t.Fatalf("unexpected codex profile override: %+v", pluginConfig.Env)
	}
	if _, ok := cfg.Bridge.PluginEnv["PRISM_CODEX_APP_PATH"]; ok {
		t.Fatalf("expected legacy global app path to be cleared: %+v", cfg.Bridge.PluginEnv)
	}
	if _, ok := cfg.Bridge.PluginEnv["PRISM_CODEX_USER_DATA_DIR"]; ok {
		t.Fatalf("expected legacy global profile override to be cleared: %+v", cfg.Bridge.PluginEnv)
	}
}

func TestApplyDashboardDesktopAppOverridesStoresHermesOverrides(t *testing.T) {
	cfg := bridgeconfig.Default()
	applyDashboardDesktopAppOverrides(cfg, map[string]dashboardDesktopAppConfigRequest{
		"hermes": {
			AppPath:     stringPtr("/custom/Hermes.app"),
			UserDataDir: stringPtr("/custom/hermes-profile"),
		},
	})
	pluginConfig := cfg.Bridge.Plugins["hermes"]
	if pluginConfig.Env["PRISM_HERMES_APP_PATH"] != "/custom/Hermes.app" {
		t.Fatalf("unexpected hermes app path override: %+v", pluginConfig.Env)
	}
	if pluginConfig.Env["PRISM_HERMES_USER_DATA_DIR"] != "/custom/hermes-profile" {
		t.Fatalf("unexpected hermes profile override: %+v", pluginConfig.Env)
	}
}

func TestApplyDashboardDesktopAppOverridesClearsCodexOverrides(t *testing.T) {
	cfg := bridgeconfig.Default()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{
		Env: map[string]string{
			"PRISM_CODEX_APP_PATH":               "/custom/Codex.app/Contents/MacOS/Codex",
			"PRISM_CODEX_USER_DATA_DIR":          "/custom/profile",
			"PRISM_CODEX_ALLOW_MANAGED_RELAUNCH": "1",
		},
	}
	applyDashboardDesktopAppOverrides(cfg, map[string]dashboardDesktopAppConfigRequest{
		"codex": {
			AppPath:     stringPtr(""),
			UserDataDir: stringPtr(""),
		},
	})
	pluginConfig := cfg.Bridge.Plugins["codex"]
	if _, ok := pluginConfig.Env["PRISM_CODEX_APP_PATH"]; ok {
		t.Fatalf("expected app path override removed: %+v", pluginConfig.Env)
	}
	if _, ok := pluginConfig.Env["PRISM_CODEX_USER_DATA_DIR"]; ok {
		t.Fatalf("expected user data dir override removed: %+v", pluginConfig.Env)
	}
	if pluginConfig.Env["PRISM_CODEX_ALLOW_MANAGED_RELAUNCH"] != "1" {
		t.Fatalf("expected unrelated env preserved: %+v", pluginConfig.Env)
	}
}

func stringPtr(value string) *string {
	return &value
}

func TestCollectDashboardStatusSuggestsLoginWhenMissingToken(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	prevStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = prevStatePath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionGlobal
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	status := collectDashboardStatus(cfgPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if status.LoggedIn {
		t.Fatal("expected logged out status")
	}
	if status.RecommendedAction != "login" {
		t.Fatalf("unexpected recommended action: %q", status.RecommendedAction)
	}
	if status.ConnectionState != "logged_out" {
		t.Fatalf("unexpected logged-out connection state: %q", status.ConnectionState)
	}
	if status.LoginCommand != "" {
		t.Fatalf("unexpected login command: %q", status.LoginCommand)
	}
	if status.LifecycleState != "logged_out" && status.LifecycleState != "running" {
		t.Fatalf("unexpected lifecycle state: %q", status.LifecycleState)
	}
}

func TestCollectDashboardStatusNeverReportsConnectedWithoutLogin(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	lock, err := daemonlock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("acquire Hub lock: %v", err)
	}
	defer lock.Release()

	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	previousStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = previousStatePath }()
	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{ConnectionState: "connected"}); err != nil {
		t.Fatalf("write stale runtime state: %v", err)
	}

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := bridgeconfig.Default().Save(configPath); err != nil {
		t.Fatalf("save config: %v", err)
	}
	status := collectDashboardStatus(configPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if !status.HubRunning {
		t.Fatal("expected locked sidecar to be reported as running")
	}
	if status.ConnectionState != "logged_out" || status.ConnectionLabel != "未登录" {
		t.Fatalf("sidecar without credentials must not report connected: %+v", status)
	}
}

func TestDashboardDesktopProcessIsSelfInspection(t *testing.T) {
	for _, command := range []string{
		`/bin/sh -lc ps ax -o pid=,command= | rg -i "Hermes" || true`,
		`rg -i Hermes`,
		`prism-plugin-hermes-desktop`,
		`/Applications/Prism Hub.app/Contents/Resources/prism-sidecar sidecar`,
	} {
		if !dashboardDesktopProcessIsSelfInspection(command) {
			t.Fatalf("expected self-inspection process to be filtered: %q", command)
		}
	}
	if dashboardDesktopProcessIsSelfInspection(`/Applications/Hermes.app/Contents/MacOS/Hermes`) {
		t.Fatal("expected real desktop process to remain visible")
	}
	spec, ok := dashboardDesktopAppSpecByID("hermes")
	if !ok {
		t.Fatal("Hermes desktop app spec missing")
	}
	pattern := regexp.MustCompile(`(?i)` + dashboardDesktopProcessPattern(spec))
	if pattern.MatchString(`/bin/zsh -lc curl http://127.0.0.1:18171/api/status | jq .desktop_apps.hermes`) {
		t.Fatal("Hermes process pattern must not match caller command text")
	}
	if !pattern.MatchString(`/Applications/Hermes.app/Contents/MacOS/Hermes`) {
		t.Fatal("Hermes process pattern must match the real app bundle")
	}
}

func TestDashboardDesktopProcessesRecognizeChatGPT154CodexHost(t *testing.T) {
	previous := dashboardDesktopProcessSnapshot
	dashboardDesktopProcessSnapshot = func() ([]byte, error) {
		return []byte("35984 /Applications/ChatGPT.app/Contents/MacOS/ChatGPT\n36002 /Applications/ChatGPT.app/Contents/Frameworks/ChatGPT Helper (Renderer).app/Contents/MacOS/ChatGPT Helper (Renderer)\n"), nil
	}
	t.Cleanup(func() { dashboardDesktopProcessSnapshot = previous })
	spec, _ := dashboardDesktopAppSpecByID("codex")
	processes, err := dashboardDesktopProcessesWithError(spec)
	if err != nil {
		t.Fatalf("read desktop process snapshot: %v", err)
	}
	if len(processes) != 1 || processes[0]["pid"] != 35984 {
		t.Fatalf("expected ChatGPT 154 main process only, got %+v", processes)
	}
	command := fmt.Sprint(processes[0]["command"])
	if !dashboardDesktopProcessBlocksManagedLaunch(spec, command, "/Users/test/.codex") {
		t.Fatal("unmanaged ChatGPT host without --user-data-dir must block a second managed launch")
	}
	if dashboardDesktopProcessHasManagedProfile(command, "/Users/test/.codex") {
		t.Fatal("an unmanaged ChatGPT host must not be eligible for profile-scoped forced termination")
	}
	if pids := dashboardDesktopProcessPIDsForTermination(processes); !reflect.DeepEqual(pids, []int{35984}) {
		t.Fatalf("unmanaged ChatGPT host must be force-terminated before relaunch, got pids %v", pids)
	}
}

func TestStopDashboardDesktopProcessesFailsClosedWhenProcessScanFails(t *testing.T) {
	previous := dashboardDesktopProcessSnapshot
	dashboardDesktopProcessSnapshot = func() ([]byte, error) { return nil, errors.New("ps unavailable") }
	t.Cleanup(func() { dashboardDesktopProcessSnapshot = previous })
	spec, _ := dashboardDesktopAppSpecByID("codex")
	err := stopDashboardDesktopProcesses(spec, "/Users/test/.codex")
	if err == nil || !strings.Contains(err.Error(), "cannot verify existing Codex processes") {
		t.Fatalf("expected process scan failure to block managed launch, got %v", err)
	}
}

func TestCollectDashboardStatusUsesDaemonRuntimeStateForAuthExpired(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionGlobal
	cfg.Prism.HostToken = "expired-token"
	cfg.Bridge.Enabled = true
	cfg.Account.AccountID = "acc-1"
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	prev := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = prev }()
	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{
		ConnectionState:  "auth_expired",
		ConnectionDetail: "Host token 已过期，请重新登录。",
	}); err != nil {
		t.Fatalf("write daemon runtime state: %v", err)
	}

	status := collectDashboardStatus(cfgPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if status.ConnectionState != "auth_expired" {
		t.Fatalf("unexpected connection state: %q", status.ConnectionState)
	}
	if status.ConnectionLabel != "登录已过期" {
		t.Fatalf("unexpected connection label: %q", status.ConnectionLabel)
	}
	if status.ConnectionDetail != "Host token 已过期，请重新登录。" {
		t.Fatalf("unexpected connection detail: %q", status.ConnectionDetail)
	}
	if status.RecommendedAction != "login" {
		t.Fatalf("unexpected recommended action: %q", status.RecommendedAction)
	}
}

func TestCollectDashboardStatusNormalizesStaleConnectedRuntimeState(t *testing.T) {
	t.Setenv("PRISM_DATA_DIR", t.TempDir())
	lockPath := filepath.Join(t.TempDir(), "hub.lock")
	previousHubLockPath := hubLockPathFor
	hubLockPathFor = func() string { return lockPath }
	defer func() { hubLockPathFor = previousHubLockPath }()
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	prevStatePath := daemonRuntimeStatePath
	daemonRuntimeStatePath = func() string { return statePath }
	defer func() { daemonRuntimeStatePath = prevStatePath }()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionGlobal
	cfg.Prism.HostToken = "token-1"
	cfg.Bridge.Enabled = true
	cfg.Account.AccountID = "acc-1"
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}
	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{
		DaemonPID:        12345,
		ConnectionState:  "connected",
		ConnectionDetail: "stale connected",
	}); err != nil {
		t.Fatalf("write daemon runtime state: %v", err)
	}

	status := collectDashboardStatus(cfgPath, filepath.Join(t.TempDir(), "prism.sqlite"))
	if status.HubRunning {
		t.Fatal("expected Hub not running")
	}
	if status.ConnectionState != "disconnected" {
		t.Fatalf("unexpected connection state: %q", status.ConnectionState)
	}
	if status.ConnectionLabel != "未连接" {
		t.Fatalf("unexpected connection label: %q", status.ConnectionLabel)
	}
	if status.ConnectionDetail == "" {
		t.Fatal("expected normalized connection detail")
	}

	normalized, err := readDaemonRuntimeState(statePath)
	if err != nil {
		t.Fatalf("read daemon runtime state: %v", err)
	}
	if normalized == nil || normalized.ConnectionState != "disconnected" || normalized.DaemonPID != 0 {
		t.Fatalf("unexpected normalized runtime state: %+v", normalized)
	}
}

func TestFetchDashboardHostPluginsUsesAccountToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/user/hosts/host-1/plugins":
			if got := r.Header.Get("X-Account-Token"); got != "account-token-1" {
				t.Fatalf("unexpected account token: %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"host_id":"host-1","items":[{"plugin_id":"codex","display_name":"Codex","icon_url":"https://example.test/codex.webp","integration_mode":"desktop-automation","capabilities":{}}]}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := bridgeconfig.Default()
	cfg.Prism.Region = bridgeconfig.PrismRegionCN
	cfg.Account.AccessToken = "account-token-1"
	cfg.Account.HostID = "host-1"
	payload, err := fetchDashboardHostPluginsWithURL(cfg, server.URL)
	if err != nil {
		t.Fatalf("fetchDashboardHostPlugins: %v", err)
	}
	if payload["ok"] != true {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	items, _ := payload["items"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("unexpected items payload: %+v", payload["items"])
	}
	if items[0]["plugin_id"] != "codex" {
		t.Fatalf("unexpected host plugin: %+v", items[0])
	}
	if items[0]["icon_url"] != "https://example.test/codex.webp" {
		t.Fatalf("host plugin icon URL was dropped: %+v", items[0])
	}
}

func TestDashboardLoginManagerDeviceFlowPersistsConfig(t *testing.T) {
	var pollCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/public/bind/start":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"device_code":"dc-1","user_code":"ABCD-1234","verification_uri":"https://example.com/bind","verification_uri_complete":"https://example.com/bind?code=ABCD-1234","expires_in":600,"interval":2,"status":"pending"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/public/bind/token":
			w.Header().Set("Content-Type", "application/json")
			if pollCount.Add(1) == 1 {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"device_code":"dc-1","user_code":"ABCD-1234","status":"pending","expires_in":598,"interval":2}}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"device_code":"dc-1","user_code":"ABCD-1234","status":"approved","expires_in":596,"interval":2,"account_id":"acc-1","user_id":"user-1","name":"Xin","access_token":"account-token-1","agent_info":"mobile-confirm","host_id":"host-1","host_token":"host-token-1","host_alias":"Office Mac"}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	dbPath := filepath.Join(t.TempDir(), "prism.sqlite")
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	manager := newDashboardLoginManager()
	login, err := manager.StartDevice(cfgPath, "", server.URL, "qr")
	if err != nil {
		t.Fatalf("start device login: %v", err)
	}
	if !login.Pending || login.LoginID == "" {
		t.Fatalf("unexpected login snapshot: %+v", login)
	}

	firstPoll, err := manager.PollDevice(cfgPath, dbPath, login.LoginID)
	if err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if firstPoll["approved"] != false {
		t.Fatalf("expected pending poll response, got %+v", firstPoll)
	}
	if manager.Snapshot() == nil || !manager.Snapshot().Pending {
		t.Fatalf("expected pending session after first poll, got %+v", manager.Snapshot())
	}

	secondPoll, err := manager.pollDevice(cfgPath, dbPath, login.LoginID, func(_, _, _ string) map[string]any {
		return map[string]any{"requested": true, "applied": true, "action": "test_stub"}
	})
	if err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if secondPoll["approved"] != true {
		t.Fatalf("expected approved poll response, got %+v", secondPoll)
	}
	if snapshot := manager.Snapshot(); snapshot != nil {
		t.Fatalf("expected pending session to be cleared after approval, got %+v", snapshot)
	}

	loaded, err := bridgeconfig.Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.Account.AccountID != "acc-1" || loaded.Account.HostID != "host-1" || loaded.Account.HostAlias != "Office Mac" {
		t.Fatalf("unexpected account payload after approval: %+v", loaded.Account)
	}
	if loaded.Prism.HostToken != "host-token-1" {
		t.Fatalf("expected host token to be persisted, got %q", loaded.Prism.HostToken)
	}
}

func TestDashboardLoginManagerPasswordLoginUsesDirectBind(t *testing.T) {
	var seenLoginBody map[string]any
	var seenPluginRegistration map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/public/bind/login":
			if err := json.NewDecoder(r.Body).Decode(&seenLoginBody); err != nil {
				t.Fatalf("decode login body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"status":"approved","account_id":"acc-2","user_id":"user-2","name":"Xin","access_token":"account-token-2","host_id":"host-2","host_alias":"Desk","host_token":"host-token-2","install_id":"install-2","machine_fingerprint":"machine-2"}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	dbPath := filepath.Join(t.TempDir(), "prism.sqlite")
	cfg := bridgeconfig.Default()
	pluginsRoot := filepath.Join(t.TempDir(), "plugins")
	writeDashboardTestPlugin(t, pluginsRoot, "codex", "Codex")
	cfg.Bridge.PluginDirs = []string{pluginsRoot}
	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	manager := newDashboardLoginManager()
	payload, err := manager.passwordLogin(
		cfgPath,
		dbPath,
		"",
		server.URL,
		"xin.he",
		"123456",
		"Desk",
		func(_, _, _ string) map[string]any {
			return map[string]any{"action": "test_stub"}
		},
	)
	if err != nil {
		t.Fatalf("password login: %v", err)
	}
	if payload["approved"] != true {
		t.Fatalf("expected approved payload, got %+v", payload)
	}
	if seenLoginBody["username"] != "xin.he" || seenLoginBody["password"] != "123456" || seenLoginBody["host_alias"] != "Desk" {
		t.Fatalf("unexpected direct bind login body: %+v", seenLoginBody)
	}
	if seenLoginBody["device_name"] == "" || seenLoginBody["platform"] == "" || seenLoginBody["client_version"] == "" {
		t.Fatalf("expected desktop metadata in direct bind login body: %+v", seenLoginBody)
	}
	if seenLoginBody["install_id"] == "" || seenLoginBody["machine_fingerprint"] == "" {
		t.Fatalf("expected machine identity in direct bind login body: %+v", seenLoginBody)
	}
	// 默认插件自动注册已移除：注册改为用户在 UI 上按插件显式开启远程
	if seenPluginRegistration != nil {
		t.Fatalf("login must not auto-register plugins, got %+v", seenPluginRegistration)
	}
}

func TestDashboardRestartCanBeDelegatedToDesktopShell(t *testing.T) {
	var gotReason string
	restart := func(_, _, reason string) map[string]any {
		gotReason = reason
		return map[string]any{"action": "desktop_restart_scheduled", "reason": reason}
	}

	payload := dashboardApplyConfigSaveWithRestart("config.yaml", "prism.sqlite", true, restart)
	if gotReason != "configuration_changed" {
		t.Fatalf("restart reason = %q, want configuration_changed", gotReason)
	}
	if payload["action"] != "desktop_restart_scheduled" {
		t.Fatalf("unexpected delegated restart payload: %+v", payload)
	}

	payload, err := handleDashboardLifecycleActionWithRestart("config.yaml", "prism.sqlite", "restart", restart)
	if err != nil {
		t.Fatalf("desktop restart lifecycle action: %v", err)
	}
	if gotReason != "manual_restart" {
		t.Fatalf("restart reason = %q, want manual_restart", gotReason)
	}
	if payload["action"] != "desktop_restart_scheduled" {
		t.Fatalf("unexpected lifecycle restart payload: %+v", payload)
	}
}

func TestDashboardPasswordLoginError(t *testing.T) {
	for _, code := range []string{"C100500", "other_server_error"} {
		upstream := &prismapi.ResponseError{Status: "500 Internal Server Error", Code: code, Message: "系统异常"}
		err := fmt.Errorf("prism bind login failed: %w", upstream)
		recorder := httptest.NewRecorder()
		writeDashboardPasswordLoginError(recorder, err)
		var payload struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
			Code  string `json:"error_code"`
		}
		if decodeErr := json.Unmarshal(recorder.Body.Bytes(), &payload); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if recorder.Code != http.StatusBadRequest || payload.OK {
			t.Fatal("expected failed login")
		}
		if code == "C100500" {
			if payload.Code != code || payload.Error != "账号或密码不匹配，请检查后重试。" {
				t.Fatalf("unexpected mismatch error: %+v", payload)
			}
		} else if payload.Code != "" || payload.Error != err.Error() {
			t.Fatalf("other errors must keep their diagnostic: %+v", payload)
		}
	}
}
