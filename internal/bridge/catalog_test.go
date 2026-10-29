package bridge

import (
	"context"
	"path/filepath"
	"testing"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	transport "rokid-prism-hub/internal/realtime"
)

// catalogProbeAdapter is a minimal adapter whose Probe result is fully
// controllable, letting BuildCatalogPlugins exercise the available/visible
// tiers independently (unlike recordingAdapter which couples them).
type catalogProbeAdapter struct {
	id        string
	available bool
	visible   bool
	surface   string
	reason    string
	reverse   bool
	list      bool
	history   bool
	forward   bool
	waitRun   bool
	attach    bool
	start     bool
	meta      pluginruntime.LocalMetadata
	usage     *pluginruntime.AgentUsage
	probes    int
}

func (a *catalogProbeAdapter) ID() string { return a.id }
func (a *catalogProbeAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	a.probes++
	return pluginruntime.Capability{
		PluginID:                   firstNonEmpty(a.meta.PluginID, a.id),
		Available:                  a.available,
		NativeVisibleInput:         a.visible,
		VisibilitySurface:          a.surface,
		UnavailableReason:          a.reason,
		CanReverseSync:             a.reverse,
		CanListSessions:            a.list,
		CanReadHistory:             a.history,
		CanForwardSync:             a.forward,
		CanWaitRun:                 a.waitRun,
		CanAttachSession:           a.attach,
		CanStartSessionWithMessage: a.start,
	}, nil
}
func (a *catalogProbeAdapter) LocalMetadata() pluginruntime.LocalMetadata { return a.meta }
func (a *catalogProbeAdapter) ReadAgentUsage(context.Context) (*pluginruntime.AgentUsage, error) {
	return a.usage, nil
}
func (a *catalogProbeAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: a.id, Surface: a.surface}, nil
}
func (a *catalogProbeAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, nil
}
func (a *catalogProbeAdapter) Subscribe(context.Context, pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	return nil, nil
}
func (a *catalogProbeAdapter) Interrupt(context.Context, pluginruntime.NativeSession, string) error {
	return nil
}
func (a *catalogProbeAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{}, nil
}
func (a *catalogProbeAdapter) Close(context.Context) error { return nil }

func TestBuildCatalogPluginsTiers(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// openclaw: probe-available + visibility-verified => supported (no reason)
	if _, err := db.InsertVisibilityCheck(ctx, bridgedb.VisibilityCheckRecord{
		PluginID: "openclaw",
		Marker:   "m1",
		Visible:  true,
		Evidence: "found",
	}); err != nil {
		t.Fatalf("insert check: %v", err)
	}
	// hermes: probe-available but NOT visibility-verified => detected_but_not_visible
	// opencode: probe-unavailable => shows reason
	svc := pluginruntime.New(
		&catalogProbeAdapter{
			id: "openclaw", available: true, visible: true, surface: "gateway", reverse: true,
			list: true, history: true, forward: true, waitRun: true, attach: true, start: true,
			meta: pluginruntime.LocalMetadata{DisplayName: "OpenClaw", Description: "Gateway adapter", IconURL: "https://example.test/openclaw.png", Version: "1.2.3", PluginID: "openclaw"},
		},
		&catalogProbeAdapter{
			id: "hermes", available: true, visible: true, surface: "tui", reverse: true,
			list: true, history: true, forward: true, waitRun: true, attach: true, start: true,
			meta: pluginruntime.LocalMetadata{DisplayName: "Hermes", PluginID: "hermes"},
		},
		&catalogProbeAdapter{id: "opencode", available: false, visible: true, surface: "tui_server", reason: "server_offline"},
	)
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.Plugins["openclaw"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncAllThreads}
	cfg.Bridge.Plugins["hermes"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncPrismManagedOnly}
	cfg.Bridge.Plugins["opencode"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncOff}

	plugins := BuildCatalogPlugins(ctx, svc, db, cfg)
	byID := map[string]transport.CatalogPlugin{}
	for _, a := range plugins {
		byID[a.PluginID] = a
	}

	oc := byID["openclaw"]
	if !oc.Available || !oc.VisibilityVerified || oc.AvailabilityReason != "" {
		t.Fatalf("openclaw expected supported (no reason), got %+v", oc)
	}
	if oc.Surface != "gateway" || oc.DisplayName != "OpenClaw" {
		t.Fatalf("openclaw surface/display unexpected: %+v", oc)
	}
	if !oc.Installed || !oc.Enabled {
		t.Fatalf("openclaw should be installed+enabled: %+v", oc)
	}
	if !oc.CanReverseSync || !oc.ReverseSyncEnabled {
		t.Fatalf("openclaw reverse sync capability/enabled unexpected: %+v", oc)
	}
	if !oc.RemoteSupported || !oc.VisibleInRemote || oc.RemoteSupportReason != "" {
		t.Fatalf("openclaw expected remote-supported and visible, got %+v", oc)
	}
	if oc.Description != "Gateway adapter" || oc.Version != "1.2.3" || oc.PluginID != "openclaw" {
		t.Fatalf("openclaw metadata unexpected: %+v", oc)
	}
	if oc.IconURL != "https://example.test/openclaw.png" {
		t.Fatalf("openclaw icon URL unexpected: %+v", oc)
	}
	if len(oc.RuntimeModes) != 0 || oc.CurrentRuntimeMode != "" {
		t.Fatalf("catalog must not invent runtime modes for adapters without manifest metadata: %+v", oc)
	}

	hermes := byID["hermes"]
	if !hermes.Available || hermes.VisibilityVerified {
		t.Fatalf("hermes expected available but not verified, got %+v", hermes)
	}
	if hermes.AvailabilityReason != ReasonDetectedNotVisible {
		t.Fatalf("hermes expected detected_but_not_visible reason, got %q", hermes.AvailabilityReason)
	}
	if !hermes.CanReverseSync || !hermes.ReverseSyncEnabled {
		t.Fatalf("hermes should support reverse sync and report enabled, got %+v", hermes)
	}
	if !hermes.RemoteSupported || !hermes.VisibleInRemote {
		t.Fatalf("hermes should still be remote supported and visible, got %+v", hermes)
	}

	opencode := byID["opencode"]
	if opencode.Available || opencode.VisibilityVerified {
		t.Fatalf("opencode expected unavailable, got %+v", opencode)
	}
	if opencode.AvailabilityReason != "server_offline" {
		t.Fatalf("opencode expected probe reason, got %q", opencode.AvailabilityReason)
	}
	if opencode.ReverseSyncEnabled {
		t.Fatalf("opencode reverse sync should reflect config off, got %+v", opencode)
	}
	if opencode.RemoteSupported || opencode.RemoteSupportReason != "missing_forward_sync" || opencode.VisibleInRemote {
		t.Fatalf("opencode remote support unexpected: %+v", opencode)
	}
}

func TestBuildCatalogPluginsProjectsDeclaredRuntimeModes(t *testing.T) {
	adapter := &catalogProbeAdapter{
		id: "demo", available: true, visible: true, list: true, history: true, forward: true, waitRun: true, attach: true, start: true,
		meta: pluginruntime.LocalMetadata{PluginID: "demo", RuntimeModes: []string{"native", "managed"}, CurrentRuntimeMode: "managed"},
	}
	plugins := BuildCatalogPlugins(context.Background(), pluginruntime.New(adapter), nil, remoteAuthorizedConfig())
	if len(plugins) != 1 || len(plugins[0].RuntimeModes) != 2 || plugins[0].RuntimeModes[0] != "native" || plugins[0].CurrentRuntimeMode != "managed" {
		t.Fatalf("catalog runtime projection mismatch: %+v", plugins)
	}
}

func TestBuildCatalogPluginsSkipsDisabledAgents(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	adapter := &catalogProbeAdapter{id: "openclaw", available: true, visible: true, list: true, history: true, forward: true, waitRun: true, attach: true, start: true}
	svc := pluginruntime.New(adapter)
	cfg := remoteAuthorizedConfig()
	// Cloud authorization is the only remote-access gate.
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}

	plugins := BuildCatalogPlugins(context.Background(), svc, db, cfg)
	if len(plugins) != 0 {
		t.Fatalf("disabled plugin should be excluded from remote catalog, got %+v", plugins)
	}
	if adapter.probes != 0 {
		t.Fatalf("disabled plugin should not be probed, got probes=%d", adapter.probes)
	}
}

func TestBuildCatalogPluginsNilDBTreatsUnverified(t *testing.T) {
	svc := pluginruntime.New(&catalogProbeAdapter{id: "openclaw", available: true, visible: true})
	plugins := BuildCatalogPlugins(context.Background(), svc, nil, remoteAuthorizedConfig())
	if len(plugins) != 1 || plugins[0].VisibilityVerified {
		t.Fatalf("nil DB should yield not-verified plugin: %+v", plugins)
	}
	if plugins[0].AvailabilityReason != ReasonDetectedNotVisible {
		t.Fatalf("expected detected_but_not_visible without DB, got %q", plugins[0].AvailabilityReason)
	}
}

func TestBuildCatalogPluginsIncludesOnlyAgentUsageProjection(t *testing.T) {
	adapter := &catalogProbeAdapter{
		id: "codex", available: true, visible: true,
		usage: &pluginruntime.AgentUsage{
			FiveHour:       &pluginruntime.AgentUsageFiveHour{UsedPercent: 18, WindowMinutes: 300, ResetsAt: 1700003600000},
			Weekly:         &pluginruntime.AgentUsageWeekly{UsedPercent: 42, WindowMinutes: 10080, ResetsAt: 1700000000000},
			AccountSummary: &pluginruntime.AccountUsageSummary{LifetimeTokens: 100, PeakDailyTokens: 20, CurrentStreakDays: 3, LongestStreakDays: 7},
			UpdatedAt:      1700000000000,
		},
	}
	plugins := BuildCatalogPlugins(context.Background(), pluginruntime.New(adapter), nil, remoteAuthorizedConfig())
	if len(plugins) != 1 || plugins[0].AgentUsage == nil {
		t.Fatalf("expected catalog usage projection, got %+v", plugins)
	}
	usage := plugins[0].AgentUsage
	if usage == nil || usage.FiveHour == nil || usage.FiveHour.WindowMinutes != 300 || usage.Weekly == nil || usage.Weekly.WindowMinutes != 10080 || usage.AccountSummary == nil {
		t.Fatalf("unexpected catalog usage: %#v", plugins[0].AgentUsage)
	}
}

func TestDisplayFromID(t *testing.T) {
	cases := map[string]string{
		"openclaw":      "Openclaw",
		"opencode.tui":  "Opencode.Tui",
		"my-plugin":     "My-Plugin",
		"hermes_plugin": "Hermes_Plugin",
		"":              "",
	}
	for in, want := range cases {
		if got := displayFromID(in); got != want {
			t.Errorf("displayFromID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRemoteConversationSupport(t *testing.T) {
	cases := []struct {
		name   string
		item   transport.CatalogPlugin
		want   bool
		reason string
	}{
		{
			name: "supported",
			item: transport.CatalogPlugin{Installed: true, CanForwardSync: true, CanListSessions: true, CanReadHistory: true, CanWaitRun: true, CanAttachSession: true, CanStartSessionWithMessage: true},
			want: true,
		},
		{
			name:   "missing history",
			item:   transport.CatalogPlugin{Installed: true, CanForwardSync: true, CanListSessions: true, CanWaitRun: true, CanAttachSession: true, CanStartSessionWithMessage: true},
			want:   false,
			reason: "missing_read_history",
		},
		{
			name:   "missing session entry",
			item:   transport.CatalogPlugin{Installed: true, CanForwardSync: true, CanListSessions: true, CanReadHistory: true, CanWaitRun: true, CanStartSessionWithMessage: true},
			want:   false,
			reason: "missing_attach_session",
		},
	}
	for _, tc := range cases {
		got, reason := remoteConversationSupport(tc.item)
		if got != tc.want || reason != tc.reason {
			t.Fatalf("%s: got (%v,%q), want (%v,%q)", tc.name, got, reason, tc.want, tc.reason)
		}
	}
}
