package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rokid-prism-hub/internal/host"
	"rokid-prism-hub/internal/pluginruntime"
)

func TestResponseErrorPreservesStructuredPluginCode(t *testing.T) {
	err := responseError("adapter.send", &rpcError{Code: "delivery_indeterminate", Message: "committed before crash"})
	var coded *pluginruntime.AdapterError
	if !errors.As(err, &coded) || coded.Code != "delivery_indeterminate" {
		t.Fatalf("Plugin RPC code was lost: %v", err)
	}
	if err := responseError("adapter.send", &rpcError{Message: "ordinary failure"}); errors.As(err, &coded) {
		t.Fatalf("uncoded failure gained a structured code: %v", err)
	}
}

func TestProcessAdapterRPC(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	t.Setenv("PRISM_HOST_TOKEN", "host-token-should-not-leak")
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	adapter := NewProcessAdapter(Discovered{
		Root: t.TempDir(),
		Manifest: Manifest{
			ID:        "demo",
			Transport: "process",
			Mode:      "test",
			Command:   []string{os.Args[0], "-test.run", "TestProcessAdapterRPC"},
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	capability, err := adapter.Probe(ctx)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !capability.Available || !capability.NativeVisibleInput {
		t.Fatalf("unexpected capability: %+v", capability)
	}
	if !capability.CanReverseSync {
		t.Fatalf("expected fake plugin reverse sync capability: %+v", capability)
	}
	if !capability.CanOpenManagedTerminal {
		t.Fatalf("expected fake plugin managed terminal capability: %+v", capability)
	}
	managedTerminal, err := adapter.OpenManagedTerminal(ctx, pluginruntime.ManagedTerminalRequest{
		PluginID: "demo", Cwd: "/tmp/prism-managed-terminal", NativeSessionID: "native-conv-1", NativeThreadID: "thread-1",
	})
	if err != nil {
		t.Fatalf("OpenManagedTerminal: %v", err)
	}
	if !managedTerminal.OK || managedTerminal.Message != "managed terminal opened" {
		t.Fatalf("unexpected managed terminal result: %+v", managedTerminal)
	}
	discovery, err := adapter.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !discovery.Verified || discovery.Endpoint != "stdio://demo" {
		t.Fatalf("unexpected discovery: %+v", discovery)
	}
	started, err := adapter.StartSessionWithMessage(ctx, pluginruntime.StartSessionWithMessageRequest{
		PluginID: "demo",
		Message:  pluginruntime.InboundMessage{PrismMessageID: "msg-start", Text: "start"},
	})
	if err != nil {
		t.Fatalf("StartSessionWithMessage: %v", err)
	}
	session := started.Session
	receipt, err := adapter.Send(ctx, session, pluginruntime.InboundMessage{PrismMessageID: "msg-1", Text: "hello"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !receipt.Accepted || receipt.NativeMessageID != "run-msg-1" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if err := adapter.AckEvent(ctx, session, "native-conv-1:hook-1"); err != nil {
		t.Fatalf("AckEvent: %v", err)
	}
	hints, err := adapter.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(hints) != 1 || hints[0].NativeSessionID != "native-conv-1" || !hints[0].Visible {
		t.Fatalf("unexpected session hints: %+v", hints)
	}
	attached, err := adapter.AttachSession(ctx, pluginruntime.AttachSessionRequest{NativeSessionID: hints[0].NativeSessionID})
	if err != nil {
		t.Fatalf("AttachSession: %v", err)
	}
	if attached.NativeSessionID != "native-conv-1" || !attached.Visible {
		t.Fatalf("unexpected attached session: %+v", attached)
	}
	history, err := adapter.ReadHistory(ctx, session, 20)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(history) != 2 || history[0].Role != "user" || history[1].Role != "assistant" {
		t.Fatalf("unexpected history: %+v", history)
	}
	historyEvents, err := adapter.ReadHistoryStream(ctx, session, pluginruntime.HistoryStreamRequest{StreamID: "history-test", Limit: 21})
	if err != nil {
		t.Fatalf("ReadHistoryStream: %v", err)
	}
	var historyStreamEvents []pluginruntime.HistoryStreamEvent
	for len(historyStreamEvents) < 2 {
		select {
		case event := <-historyEvents:
			historyStreamEvents = append(historyStreamEvents, event)
		case <-ctx.Done():
			t.Fatal("timed out waiting for history stream")
		}
	}
	if event := historyStreamEvents[0]; event.Type != "turn" || event.Turn == nil || event.Turn.TurnID != "turn-1" || len(event.Turn.Messages) != 2 {
		t.Fatalf("unexpected history stream turn: %+v", event)
	}
	if event := historyStreamEvents[1]; event.Type != "page_end" {
		t.Fatalf("expected history page_end after initial turn, got %+v", event)
	}
	events, err := adapter.Subscribe(ctx, session)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case event := <-events:
		if event.Status != "running" || event.Summary != "fake plugin running" {
			t.Fatalf("unexpected event: %+v", event)
		}
		detail, ok := event.Payload["detail_snapshot"].(map[string]any)
		if !ok || detail["messages_signature"] != "fake-message-signature" {
			t.Fatalf("dynamic plugin event payload must retain snake_case keys, got %+v", event.Payload)
		}
		model, ok := detail["current_model"].(map[string]any)
		if !ok || model["option_id"] != "gpt-5.6" {
			t.Fatalf("nested dynamic plugin payload must retain snake_case keys, got %+v", detail)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
	waited, err := adapter.WaitForRun(ctx, session, receipt.NativeMessageID)
	if err != nil {
		t.Fatalf("WaitForRun: %v", err)
	}
	if waited.Status != "completed" || waited.Summary != "fake plugin completed" {
		t.Fatalf("unexpected wait event: %+v", waited)
	}
	visibility, err := adapter.VerifyVisibility(ctx, session, "hello")
	if err != nil {
		t.Fatalf("VerifyVisibility: %v", err)
	}
	if !visibility.Visible {
		t.Fatalf("unexpected visibility: %+v", visibility)
	}
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessAdapterStrictProbeRequiresHandshake(t *testing.T) {
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	adapter := NewProcessAdapter(Discovered{Root: t.TempDir(), Manifest: Manifest{
		ID: "strict-demo", Transport: "process", Mode: "test", Command: []string{"./missing-adapter"},
	}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := adapter.ProbeStrict(ctx); err == nil {
		t.Fatal("strict install probe accepted a missing runtime handshake")
	}
	capability, err := adapter.Probe(ctx)
	if err != nil || capability.Available || capability.PluginID != "strict-demo" {
		t.Fatalf("normal status probe must preserve unavailable fallback: capability=%+v err=%v", capability, err)
	}
}

func TestPluginProcessLabel(t *testing.T) {
	tests := map[string]string{
		"codex":                "prism-plugin-codex",
		"Codex.Automation":     "prism-plugin-codex-automation",
		" openclaw / staging ": "prism-plugin-openclaw---staging",
		"":                     "prism-plugin",
	}
	for input, want := range tests {
		if got := pluginProcessLabel(input); got != want {
			t.Fatalf("pluginProcessLabel(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestPluginRuntimeProcessLabel(t *testing.T) {
	tests := []struct {
		manifest Manifest
		mode     string
		want     string
	}{
		{manifest: Manifest{ID: "codex", RuntimeModes: []string{"desktop"}}, mode: "desktop", want: "prism-plugin-codex-desktop"},
		{manifest: Manifest{ID: "Hermes", RuntimeModes: []string{"desktop", "native"}}, mode: "native", want: "prism-plugin-hermes-native"},
		{manifest: Manifest{ID: "openclaw"}, mode: "", want: "prism-plugin-openclaw"},
		{manifest: Manifest{ID: "demo", RuntimeModes: []string{"native", "managed"}}, mode: " local / test ", want: "prism-plugin-demo-local---test"},
	}
	for _, test := range tests {
		if got := pluginRuntimeProcessLabel(test.manifest, test.mode); got != test.want {
			t.Fatalf("pluginRuntimeProcessLabel(%q, %q) = %q, want %q", test.manifest.ID, test.mode, got, test.want)
		}
	}
}

func TestRuntimeLabelledExecutableUsesDarwinHardLink(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "prism-plugin-openclaw")
	if err := os.WriteFile(source, []byte("plugin"), 0o700); err != nil {
		t.Fatalf("write plugin executable: %v", err)
	}
	got := runtimeLabelledExecutable(source, "prism-plugin-openclaw-native")
	if runtime.GOOS != "darwin" {
		if got != source {
			t.Fatalf("non-darwin runtime executable = %q, want %q", got, source)
		}
		return
	}
	if want := filepath.Join(root, "prism-plugin-openclaw-native"); got != want {
		t.Fatalf("runtime executable = %q, want %q", got, want)
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		t.Fatalf("stat source executable: %v", err)
	}
	labelInfo, err := os.Stat(got)
	if err != nil {
		t.Fatalf("stat labelled executable: %v", err)
	}
	if !os.SameFile(sourceInfo, labelInfo) {
		t.Fatalf("labelled executable must be a hard link")
	}
}

func TestStartProcessRuntimeUsesRuntimeModeLabel(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	root := t.TempDir()
	rt, err := startProcessRuntime(context.Background(), root, Manifest{
		ID:                 "demo",
		Transport:          "process",
		Mode:               "native",
		RuntimeModes:       []string{"desktop", "acp"},
		DefaultRuntimeMode: "desktop",
		Command:            []string{os.Args[0], "-test.run", "TestStartProcessRuntimeUsesRuntimeModeLabel"},
	}, map[string]string{"PRISM_PLUGIN_MODE": "acp"}, false)
	if err != nil {
		t.Fatalf("start process runtime: %v", err)
	}
	defer rt.close(context.Background())
	if got, want := rt.cmd.Args[0], "prism-plugin-demo-acp"; got != want {
		t.Fatalf("runtime argv[0] = %q, want %q", got, want)
	}
	if got := envMap(rt.cmd.Env)[pluginProcessLabelEnv]; got != "prism-plugin-demo-acp" {
		t.Fatalf("runtime process label env = %q", got)
	}
}

func TestStartProcessRuntimeRefusesUnresolvedRuntimeToken(t *testing.T) {
	_, err := startProcessRuntime(context.Background(), t.TempDir(), Manifest{
		ID: "node-plugin", Version: "1.0.0", IntegrationMode: "protocol-native", Transport: "process",
		Command:      []string{"${runtime.node}", "dist/index.js"},
		Capabilities: ManifestCapability{},
	}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "node runtime unavailable") {
		t.Fatalf("runtime token must never fall back to system PATH, got %v", err)
	}
}

func TestProcessAdapterRuntimeSurvivesRequestContextCancellation(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	adapter := NewProcessAdapter(Discovered{
		Root: t.TempDir(),
		Manifest: Manifest{
			ID:        "demo-persistent-runtime",
			Transport: "process",
			Mode:      "test",
			Command:   []string{os.Args[0], "-test.run", "TestProcessAdapterRuntimeSurvivesRequestContextCancellation"},
		},
	})

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 2*time.Second)
	capability, err := adapter.Probe(firstCtx)
	firstCancel()
	if err != nil {
		t.Fatalf("first Probe: %v", err)
	}
	if !capability.Available {
		t.Fatalf("unexpected first capability: %+v", capability)
	}

	secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer secondCancel()
	discovery, err := adapter.Discover(secondCtx)
	if err != nil {
		t.Fatalf("Discover after first context cancellation: %v", err)
	}
	if !discovery.Verified || discovery.Endpoint != "stdio://demo" {
		t.Fatalf("unexpected discovery after cancellation: %+v", discovery)
	}
	if err := adapter.Close(secondCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessAdapterWaitForRunUsesCallerDeadline(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	adapter := NewProcessAdapter(Discovered{
		Root: t.TempDir(),
		Manifest: Manifest{
			ID:        "demo-long-wait",
			Transport: "process",
			Mode:      "test",
			Command:   []string{os.Args[0], "-test.run", "TestProcessAdapterWaitForRunUsesCallerDeadline"},
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := adapter.Probe(ctx); err != nil {
		t.Fatalf("Probe: %v", err)
	}
	adapter.mu.Lock()
	rt := adapter.runtime
	if rt != nil {
		rt.requestTimeout = 20 * time.Millisecond
	}
	adapter.mu.Unlock()
	if rt == nil {
		t.Fatal("expected running process runtime")
	}
	event, err := adapter.WaitForRun(ctx, pluginruntime.NativeSession{PluginID: "demo-long-wait"}, "run-long")
	if err != nil {
		t.Fatalf("WaitForRun must outlive the short RPC timeout: %v", err)
	}
	if event.Status != "completed" {
		t.Fatalf("unexpected terminal event: %+v", event)
	}
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPluginEnvironmentFiltersPrismSecrets(t *testing.T) {
	t.Setenv("PATH", "/tmp/bin")
	t.Setenv("PRISM_API_URL", "https://example.com/prism/api")
	t.Setenv("PRISM_REALTIME_URL", "wss://example.com/prism/realtime/host")
	t.Setenv("PRISM_HOST_TOKEN", "new-secret")
	t.Setenv("PRISM_CONFIG", "/tmp/prism-config.yaml")
	t.Setenv("PRISM_HOST_TOKEN", "secret")
	t.Setenv("PRISM_CONFIG", "/tmp/config.yaml")
	t.Setenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
	t.Setenv("CODEX_APP_SERVER_ENDPOINT", "ws://127.0.0.1:4500")
	t.Setenv("CODEX_APP_SERVER_SOCK", "/tmp/codex.sock")
	t.Setenv("CODEX_BIN", "/usr/local/bin/codex")
	t.Setenv("PRISM_CODEX_APP_PATH", "/Applications/Codex.app/Contents/MacOS/Codex")
	t.Setenv("PRISM_HERMES_CDP_URL", "http://127.0.0.1:9444")
	t.Setenv("PRISM_CLAUDE_CODE_EXECUTABLE", "/opt/claude/bin/claude")
	t.Setenv("PRISM_SQLITE3_BIN", "/opt/homebrew/bin/sqlite3")
	env := pluginEnvironment("/tmp/plugin", Manifest{
		ID:        "demo",
		Transport: "process",
	}, "session-token")
	values := envMap(env)
	if values[sessionTokenEnv] != "session-token" {
		t.Fatalf("expected session token env, got %+v", values)
	}
	if values[pluginIDEnv] != "demo" || values[pluginProtocolVersionEnv] != "4" {
		t.Fatalf("expected plugin metadata env, got %+v", values)
	}
	if values["PATH"] == "" {
		t.Fatalf("expected PATH to be preserved, got %+v", values)
	}
	for key, want := range map[string]string{
		"OPENCLAW_GATEWAY_URL":         "http://127.0.0.1:18789",
		"CODEX_APP_SERVER_ENDPOINT":    "ws://127.0.0.1:4500",
		"CODEX_APP_SERVER_SOCK":        "/tmp/codex.sock",
		"CODEX_BIN":                    "/usr/local/bin/codex",
		"PRISM_CODEX_APP_PATH":         "/Applications/Codex.app/Contents/MacOS/Codex",
		"PRISM_HERMES_CDP_URL":         "http://127.0.0.1:9444",
		"PRISM_CLAUDE_CODE_EXECUTABLE": "/opt/claude/bin/claude",
		"PRISM_SQLITE3_BIN":            "/opt/homebrew/bin/sqlite3",
		"PRISM_API_URL":                "https://example.com/prism/api",
		"PRISM_REALTIME_URL":           "wss://example.com/prism/realtime/host",
	} {
		if values[key] != want {
			t.Fatalf("expected %s=%s in plugin env, got %+v", key, want, values)
		}
	}
	if _, ok := values["PRISM_HOST_TOKEN"]; ok {
		t.Fatalf("host token leaked into plugin env: %+v", values)
	}
	if _, ok := values["PRISM_CONFIG"]; ok {
		t.Fatalf("prism config leaked into plugin env: %+v", values)
	}
}

func TestPluginEnvironmentKeepsWindowsHomeVariables(t *testing.T) {
	homes := map[string]string{
		"USERPROFILE":  `C:\Users\demo`,
		"APPDATA":      `C:\Users\demo\AppData\Roaming`,
		"LOCALAPPDATA": `C:\Users\demo\AppData\Local`,
		"HOMEDRIVE":    "C:",
		"HOMEPATH":     `\Users\demo`,
	}
	for key, value := range homes {
		t.Setenv(key, value)
	}
	env := pluginEnvironment("/tmp/plugin", Manifest{ID: "opencode", Transport: "process"}, "session-token")
	values := envMap(env)
	for key, want := range homes {
		if values[key] != want {
			t.Fatalf("expected %s=%s in plugin env, got %+v", key, want, values)
		}
	}
}

func TestPluginEnvironmentAllowListMatchesWindowsCasing(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("environment names are case-sensitive outside Windows")
	}
	allowed := map[string]bool{"PATH": true, "SystemRoot": true, "ComSpec": true}
	for _, key := range []string{"PATH", "Path", "path", "SystemRoot", "SYSTEMROOT", "ComSpec", "COMSPEC"} {
		if !isPluginEnvironmentAllowed(key, allowed) {
			t.Fatalf("expected %q to be allowed", key)
		}
	}
	for _, key := range []string{"PRISM_HOST_TOKEN", "PATHX", "SECRET"} {
		if isPluginEnvironmentAllowed(key, allowed) {
			t.Fatalf("expected %q to be rejected", key)
		}
	}
	if !isPluginEnvironmentAllowed("PRISM_CODEX_APP_PATH", allowed) {
		t.Fatal("expected PRISM_CODEX_* prefix to be allowed")
	}
}

func TestPluginEnvironmentAddsManagedLaunchEnvFromManifest(t *testing.T) {
	env := pluginEnvironment("/tmp/plugin", Manifest{
		ID:            "any-plugin",
		Transport:     "process",
		ManagedLaunch: true,
	}, "session-token")
	values := envMap(env)
	if values["PRISM_PLUGIN_MANAGED_LAUNCH"] != "1" {
		t.Fatalf("managed launch env should be injected from manifest, got %+v", values)
	}
	hermesEnv := pluginEnvironment("/tmp/plugin", Manifest{
		ID:            "hermes",
		Transport:     "process",
		ManagedLaunch: true,
	}, "session-token")
	if _, injected := envMap(hermesEnv)["PRISM_HERMES_ALLOW_MANAGED_RELAUNCH"]; injected {
		t.Fatal("Hermes must not receive implicit managed relaunch permission")
	}
}

func TestPluginEnvironmentAppliesExplicitConfigEnv(t *testing.T) {
	t.Setenv("CUSTOM_PLUGIN_FLAG", "from-shell")
	t.Setenv("PRISM_HOST_TOKEN", "secret")
	env := pluginEnvironment("/tmp/plugin", Manifest{
		ID:        "demo",
		Transport: "process",
	}, "session-token", map[string]string{
		"CUSTOM_PLUGIN_FLAG":   "from-config",
		"PRISM_CODEX_CDP_PORT": "9222",
		"PRISM_HOST_TOKEN":     "blocked",
		"PRISM_CONFIG":         "/tmp/blocked.yaml",
		"PRISM_PLUGIN_ID":      "spoof",
		"INVALID-ENV":          "blocked",
		"PRISM_HERMES_CDP_URL": "http://127.0.0.1:9444",
		"PRISM_SQLITE3_BIN":    "/opt/bin/sqlite3",
	})
	values := envMap(env)
	if values["CUSTOM_PLUGIN_FLAG"] != "from-config" {
		t.Fatalf("explicit config env should be applied, got %+v", values)
	}
	if values["PRISM_CODEX_CDP_PORT"] != "9222" || values["PRISM_HERMES_CDP_URL"] != "http://127.0.0.1:9444" {
		t.Fatalf("official CDP env should be applied, got %+v", values)
	}
	if values["PRISM_SQLITE3_BIN"] != "/opt/bin/sqlite3" {
		t.Fatalf("sqlite override should be applied, got %+v", values)
	}
	if _, ok := values["PRISM_HOST_TOKEN"]; ok {
		t.Fatalf("host token leaked into plugin env: %+v", values)
	}
	if values[pluginIDEnv] != "demo" {
		t.Fatalf("reserved plugin metadata should not be overridden, got %+v", values)
	}
	if _, ok := values["INVALID-ENV"]; ok {
		t.Fatalf("invalid env key leaked into plugin env: %+v", values)
	}
}

func TestProcessAdapterPreservesOnlyManifestResolvedRuntimeMode(t *testing.T) {
	adapter := NewProcessAdapterWithEnv(Discovered{
		Root:     t.TempDir(),
		Manifest: Manifest{ID: "demo", Transport: "process", Mode: "native", RuntimeModes: []string{"native", "managed"}, DefaultRuntimeMode: "native"},
	}, map[string]string{"PRISM_PLUGIN_MODE": "managed", "PRISM_PLUGIN_ID": "spoof"})
	if adapter.PluginEnv["PRISM_PLUGIN_MODE"] != "managed" || adapter.PluginEnv["PRISM_PLUGIN_ID"] != "" {
		t.Fatalf("unexpected ProcessAdapter runtime environment: %+v", adapter.PluginEnv)
	}
	values := envMap(pluginEnvironment("/tmp/plugin", adapter.Manifest, "session-token", adapter.PluginEnv))
	if values["PRISM_PLUGIN_MODE"] != "managed" {
		t.Fatalf("Hub-owned runtime mode was not passed to Plugin: %+v", values)
	}
	if values[pluginIDEnv] != "demo" {
		t.Fatalf("Plugin ID runtime metadata changed: %+v", values)
	}
	if _, ok := values[pluginProcessLabelEnv]; ok {
		t.Fatalf("process label is launch-owned and must not be caller-injected: %+v", values)
	}
}

func TestProcessAdapterMetadataExposesOnlyActiveRuntimeProfile(t *testing.T) {
	adapter := NewProcessAdapterWithEnv(Discovered{Manifest: Manifest{
		ID: "demo", Transport: "process", Mode: "desktop",
		RuntimeModes: []string{"desktop", "native"}, DefaultRuntimeMode: "desktop",
		RuntimeControls: map[string]pluginruntime.RuntimeControlProfile{
			"desktop": {Model: pluginruntime.RuntimeControlInteractive},
			"native":  {Model: pluginruntime.RuntimeControlStable},
		},
		RuntimeOperations: map[string]pluginruntime.RuntimeOperationProfile{
			"desktop": {Rename: pluginruntime.RuntimeOperationMenu},
			"native":  {Rename: pluginruntime.RuntimeOperationDirect},
		},
	}}, map[string]string{"PRISM_PLUGIN_MODE": "native"})
	metadata := adapter.LocalMetadata()
	if metadata.CurrentRuntimeMode != "native" || !metadata.RuntimeControlsDeclared || metadata.RuntimeControls.Model != pluginruntime.RuntimeControlStable {
		t.Fatalf("unexpected active runtime metadata: %+v", metadata)
	}
	if !metadata.RuntimeOperationsDeclared || metadata.RuntimeOperations.Rename != pluginruntime.RuntimeOperationDirect {
		t.Fatalf("unexpected active runtime operation metadata: %+v", metadata)
	}
}

func TestProcessAdapterRestartsAfterRuntimeExitWithBackoff(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	root := t.TempDir()
	adapter := NewProcessAdapter(Discovered{
		Root: root,
		Manifest: Manifest{
			ID:        "demo-restart",
			Transport: "process",
			Mode:      "test",
			Command: []string{
				os.Args[0],
				"-test.run",
				"TestProcessAdapterRestartsAfterRuntimeExitWithBackoff",
			},
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := adapter.Probe(ctx)
	if err != nil {
		t.Fatalf("first Probe: %v", err)
	}
	if !first.Available {
		t.Fatalf("first probe should be available, got %+v", first)
	}
	deadline := time.Now().Add(2 * time.Second)
	var backoff pluginruntime.Capability
	for time.Now().Before(deadline) {
		backoff, err = adapter.Probe(ctx)
		if err != nil {
			t.Fatalf("backoff Probe returned hard error: %v", err)
		}
		if !backoff.Available && strings.Contains(backoff.UnavailableReason, "restart backoff") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if backoff.Available || !strings.Contains(backoff.UnavailableReason, "restart backoff") {
		t.Fatalf("expected restart backoff after first runtime exit, got %+v", backoff)
	}
	time.Sleep(supervisorBaseBackoff + 75*time.Millisecond)
	restarted, err := adapter.Probe(ctx)
	if err != nil {
		t.Fatalf("restarted Probe: %v", err)
	}
	if !restarted.Available {
		t.Fatalf("expected probe after backoff to restart plugin, got %+v", restarted)
	}
}

func TestProcessAdapterMarksFailedAfterRepeatedStartFailures(t *testing.T) {
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	adapter := NewProcessAdapter(Discovered{
		Root: t.TempDir(),
		Manifest: Manifest{
			ID:        "demo-fail",
			Transport: "process",
			Mode:      "test",
			Command:   []string{"/path/to/missing-pluginbridge-plugin"},
		},
	})
	ctx := context.Background()
	var last pluginruntime.Capability
	var err error
	for i := 0; i < SupervisorMaxFailures; i++ {
		adapter.mu.Lock()
		adapter.nextStart = time.Time{}
		adapter.mu.Unlock()
		last, err = adapter.Probe(ctx)
		if err != nil {
			t.Fatalf("Probe should report unavailable capability, got hard err: %v", err)
		}
	}
	adapter.mu.Lock()
	adapter.nextStart = time.Time{}
	adapter.mu.Unlock()
	last, err = adapter.Probe(ctx)
	if err != nil {
		t.Fatalf("Probe after max failures should still return fallback capability, got err: %v", err)
	}
	if last.Available || !strings.Contains(last.UnavailableReason, "failed after") {
		t.Fatalf("expected failed-after-max fallback, got %+v", last)
	}
}

func TestProcessRuntimeTerminatesAndWaitsAfterInvalidStdout(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	assertRuntimeTerminatesAfterProtocolFailure(t, "demo-invalid-stdout", "TestProcessRuntimeTerminatesAndWaitsAfterInvalidStdout")
}

func TestProcessRuntimeTerminatesAndWaitsAfterOversizedStdout(t *testing.T) {
	if os.Getenv(sessionTokenEnv) != "" {
		runFakePluginForTest(t)
		return
	}
	assertRuntimeTerminatesAfterProtocolFailure(t, "demo-oversized-stdout", "TestProcessRuntimeTerminatesAndWaitsAfterOversizedStdout")
}

func assertRuntimeTerminatesAfterProtocolFailure(t *testing.T, pluginID, testName string) {
	t.Helper()
	rt, err := startProcessRuntime(context.Background(), t.TempDir(), Manifest{
		ID:        pluginID,
		Transport: "process",
		Mode:      "test",
		Command:   []string{os.Args[0], "-test.run", testName},
	}, nil, false)
	if err != nil {
		t.Fatalf("start process runtime: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.call(ctx, "adapter.probe", map[string]any{}, nil); err == nil {
		t.Fatal("invalid plugin stdout must fail the pending RPC")
	}
	select {
	case <-rt.processDone:
	case <-ctx.Done():
		t.Fatal("runtime did not reap the malformed plugin process")
	}
	if rt.cmd.ProcessState == nil {
		t.Fatalf("malformed plugin process was not reaped: %+v", rt.cmd.ProcessState)
	}
}

func TestProcessRuntimeSerializesConcurrentStdinWrites(t *testing.T) {
	writer := &serialRuntimeWriter{}
	rt := newProcessRuntime(nil, writer, "test")
	writer.runtime = rt
	go rt.writeLoop()
	defer rt.terminate(fmt.Errorf("test complete"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	const calls = 32
	errs := make(chan error, calls)
	var group sync.WaitGroup
	for i := 0; i < calls; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- rt.call(ctx, "adapter.probe", map[string]any{}, nil)
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent runtime call: %v", err)
		}
	}
	if writer.overlapped() {
		t.Fatal("concurrent runtime calls overlapped on plugin stdin")
	}
	if got := writer.count(); got != calls {
		t.Fatalf("serialized writer lost requests: got %d want %d", got, calls)
	}
}

func TestHistoryDeliveryCoalescesQueuedTurnRevisions(t *testing.T) {
	delivery := &historyDelivery{
		streamID: "history-1",
		out:      make(chan pluginruntime.HistoryStreamEvent),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	if overflow := delivery.enqueue(pluginruntime.HistoryStreamEvent{
		StreamID: "history-1", Type: "turn", Turn: &pluginruntime.HistoryTurn{TurnID: "turn-1", Revision: 1},
	}); overflow {
		t.Fatal("first turn must fit")
	}
	if overflow := delivery.enqueue(pluginruntime.HistoryStreamEvent{
		StreamID: "history-1", Type: "turn", Turn: &pluginruntime.HistoryTurn{TurnID: "turn-1", Revision: 2},
	}); overflow {
		t.Fatal("revision replacement must fit")
	}
	delivery.enqueue(pluginruntime.HistoryStreamEvent{StreamID: "history-1", Type: "end"})
	go delivery.run()

	first := <-delivery.out
	if first.Type != "turn" || first.Turn == nil || first.Turn.Revision != 2 {
		t.Fatalf("expected only the latest queued revision, got %+v", first)
	}
	second := <-delivery.out
	if second.Type != "end" {
		t.Fatalf("expected end after latest turn, got %+v", second)
	}
	if _, ok := <-delivery.out; ok {
		t.Fatal("delivery must close after terminal event")
	}
}

func TestHistoryDeliverySignalsRetryableErrorOnBackpressure(t *testing.T) {
	delivery := &historyDelivery{
		streamID: "history-overflow",
		out:      make(chan pluginruntime.HistoryStreamEvent),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	for i := 0; i < historyDeliveryQueueSize; i++ {
		if overflow := delivery.enqueue(pluginruntime.HistoryStreamEvent{
			StreamID: "history-overflow", Type: "turn", Turn: &pluginruntime.HistoryTurn{TurnID: fmt.Sprintf("turn-%d", i), Revision: 1},
		}); overflow {
			t.Fatalf("queue overflowed early at %d", i)
		}
	}
	if !delivery.enqueue(pluginruntime.HistoryStreamEvent{
		StreamID: "history-overflow", Type: "turn", Turn: &pluginruntime.HistoryTurn{TurnID: "turn-overflow", Revision: 1},
	}) {
		t.Fatal("queue overflow must be reported")
	}
	go delivery.run()

	event := <-delivery.out
	if event.Type != "error" || !event.Retryable || !strings.Contains(event.Error, "backpressure") {
		t.Fatalf("expected retryable backpressure error, got %+v", event)
	}
	if _, ok := <-delivery.out; ok {
		t.Fatal("overflow delivery must close after retryable error")
	}
}

type serialRuntimeWriter struct {
	runtime *processRuntime
	mu      sync.Mutex
	writing bool
	overlap bool
	writes  int
}

func (w *serialRuntimeWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	if w.writing {
		w.overlap = true
	}
	w.writing = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.writing = false
		w.writes++
		w.mu.Unlock()
	}()

	// Make overlapping writes deterministic if the runtime ever bypasses its writer.
	time.Sleep(5 * time.Millisecond)
	var request rpcRequest
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &request); err != nil {
		return 0, err
	}
	w.runtime.deliverResponse(rpcResponse{ID: request.ID, OK: true})
	return len(data), nil
}

func (w *serialRuntimeWriter) Close() error { return nil }

func (w *serialRuntimeWriter) overlapped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overlap
}

func (w *serialRuntimeWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

func runFakePluginForTest(t *testing.T) {
	t.Helper()
	if os.Getenv(sessionTokenEnv) == "" {
		t.Fatal("missing process adapter session token")
	}
	if os.Getenv("PRISM_HOST_TOKEN") != "" {
		t.Fatal("process adapter leaked host token to plugin")
	}
	pluginID := os.Getenv(pluginIDEnv)
	if pluginID == "" {
		t.Fatal("missing plugin id")
	}
	if pluginID == "demo-invalid-stdout" {
		_, _ = fmt.Fprintln(os.Stdout, "{invalid json")
		time.Sleep(30 * time.Second)
		return
	}
	if pluginID == "demo-oversized-stdout" {
		_, _ = fmt.Fprintln(os.Stdout, strings.Repeat("x", 2*1024*1024+1))
		time.Sleep(30 * time.Second)
		return
	}
	if pluginID == "demo-child-process" {
		child := exec.Command("sleep", "30")
		if err := child.Start(); err != nil {
			t.Fatalf("start fake plugin child: %v", err)
		}
		if err := os.WriteFile(filepath.Join(os.Getenv(pluginRootEnv), "child.pid"), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
			t.Fatalf("write fake plugin child pid: %v", err)
		}
		_, _ = fmt.Fprintln(os.Stdout, "{invalid json")
		time.Sleep(30 * time.Second)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		okPayload := func(payload any) {
			_ = enc.Encode(map[string]any{"id": req.ID, "ok": true, "payload": toSnakeWire(payload)})
		}
		switch req.Method {
		case "adapter.probe":
			okPayload(pluginruntime.Capability{PluginID: pluginID, Available: true, NativeVisibleInput: true, CanReverseSync: true, CanOpenManagedTerminal: true, VisibilitySurface: "test"})
			if os.Getenv(pluginIDEnv) == "demo-restart" {
				statePath := os.Getenv(pluginRootEnv) + "/probe-count"
				if _, err := os.Stat(statePath); os.IsNotExist(err) {
					_ = os.WriteFile(statePath, []byte("done"), 0o600)
					return
				}
			}
		case "adapter.discover":
			okPayload(pluginruntime.DiscoveryResult{PluginID: pluginID, Surface: "test", Endpoint: "stdio://demo", Verified: true})
		case "adapter.startSessionWithMessage":
			okPayload(pluginruntime.StartSessionWithMessageResult{
				Session:    pluginruntime.NativeSession{PluginID: pluginID, NativeSessionID: "native-conv-1", Surface: "test", Visible: true},
				Receipt:    pluginruntime.SendReceipt{NativeMessageID: "run-start", Accepted: true, Visible: true},
				Visibility: pluginruntime.VisibilityResult{Visible: true},
			})
		case "adapter.listSessions":
			okPayload([]pluginruntime.NativeSessionHint{{
				PluginID:        pluginID,
				NativeSessionID: "native-conv-1",
				Surface:         "test",
				Title:           "Fake Plugin Session",
				Active:          true,
				Visible:         true,
			}})
		case "adapter.attachSession":
			okPayload(pluginruntime.NativeSession{PluginID: pluginID, NativeSessionID: "native-conv-1", Surface: "test", Visible: true})
		case "adapter.readHistory":
			okPayload([]pluginruntime.HistoryMessage{
				{ID: "user-1", Role: "user", Type: "text", Content: "hello"},
				{ID: "assistant-1", Role: "assistant", Type: "text", Content: "world"},
			})
		case "adapter.readHistoryStream":
			var params struct {
				Request pluginruntime.HistoryStreamRequest `json:"request"`
			}
			b, _ := json.Marshal(req.Params)
			_ = json.Unmarshal(b, &params)
			okPayload(map[string]any{"stream_id": params.Request.StreamID})
			_ = enc.Encode(toSnakeWire(map[string]any{
				"event": "history.stream",
				"payload": pluginruntime.HistoryStreamEvent{
					StreamID:  params.Request.StreamID,
					Type:      "turn",
					Source:    "initial",
					Operation: "append",
					Turn: &pluginruntime.HistoryTurn{TurnID: "turn-1", OrderKey: "1", Revision: 1, Messages: []pluginruntime.HistoryMessage{
						{ID: "user-1", Role: "user", Content: "hello"}, {ID: "assistant-1", Role: "assistant", Content: "world"},
					}},
				},
			}))
			_ = enc.Encode(toSnakeWire(map[string]any{
				"event": "history.stream",
				"payload": pluginruntime.HistoryStreamEvent{
					StreamID:  params.Request.StreamID,
					Type:      "page_end",
					Source:    "initial",
					Operation: "append",
				},
			}))
		case "adapter.send":
			okPayload(pluginruntime.SendReceipt{NativeMessageID: "run-msg-1", Accepted: true, Visible: true})
		case "adapter.ackEvent":
			var params struct {
				Session pluginruntime.NativeSession `json:"session"`
				EventID string                      `json:"event_id"`
			}
			b, _ := json.Marshal(req.Params)
			_ = unmarshalWire(b, &params)
			if params.Session.NativeSessionID != "native-conv-1" || params.EventID != "native-conv-1:hook-1" {
				_ = enc.Encode(map[string]any{"id": req.ID, "ok": false, "error": map[string]any{"message": fmt.Sprintf("unexpected event acknowledgement: %+v", params)}})
				continue
			}
			okPayload(map[string]any{"ok": true})
		case "adapter.openManagedTerminal":
			var params pluginruntime.ManagedTerminalRequest
			b, _ := json.Marshal(req.Params)
			_ = json.Unmarshal(b, &params)
			if params.PluginID != pluginID || params.Cwd != "/tmp/prism-managed-terminal" || params.NativeSessionID != "native-conv-1" || params.NativeThreadID != "thread-1" {
				_ = enc.Encode(map[string]any{"id": req.ID, "ok": false, "error": map[string]any{"message": "unexpected managed terminal request"}})
				continue
			}
			okPayload(pluginruntime.ManagedTerminalResult{OK: true, Message: "managed terminal opened"})
		case "adapter.subscribe":
			var params struct {
				SubscriptionID string `json:"subscription_id"`
			}
			b, _ := json.Marshal(req.Params)
			_ = json.Unmarshal(b, &params)
			okPayload(map[string]any{"subscribed": true})
			_ = enc.Encode(toSnakeWire(map[string]any{
				"event": "plugin.event",
				"payload": map[string]any{
					"subscription_id": params.SubscriptionID,
					"event": pluginruntime.PluginEvent{
						ID:      "event-1",
						Type:    "run.progress",
						Status:  "running",
						Summary: "fake plugin running",
						Payload: map[string]any{
							"detail_snapshot": map[string]any{
								"messages_signature": "fake-message-signature",
								"current_model": map[string]any{
									"option_id": "gpt-5.6",
								},
							},
						},
					},
				},
			}))
		case "adapter.waitForRun":
			if pluginID == "demo-long-wait" {
				time.Sleep(75 * time.Millisecond)
			}
			okPayload(pluginruntime.PluginEvent{ID: "run-msg-1", Type: "run.completed", Status: "completed", Summary: "fake plugin completed"})
		case "adapter.verifyVisibility":
			okPayload(pluginruntime.VisibilityResult{Visible: true, Marker: "hello", Evidence: "hello"})
		case "adapter.close", "adapter.unsubscribe", "adapter.interrupt", "adapter.resolveApproval":
			okPayload(map[string]any{"ok": true})
		default:
			_ = enc.Encode(map[string]any{"id": req.ID, "ok": false, "error": map[string]any{"message": "unknown method"}})
		}
	}
	if err := scanner.Err(); err != nil {
		t.Errorf("fake plugin stdin: %v", err)
	}
}

func envMap(env []string) map[string]string {
	out := map[string]string{}
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}
