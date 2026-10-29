package doctor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	plugininstall "rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginruntime"
)

func runCheck(t *testing.T, check Check) []Finding {
	t.Helper()
	findings, err := check.Run(context.Background())
	if err != nil {
		t.Fatalf("check %s: %v", check.ID(), err)
	}
	return findings
}

func TestConfigParseCheck(t *testing.T) {
	dir := t.TempDir()
	broken := filepath.Join(dir, "config.yaml")
	write(t, broken, "bridge:\n  totally_unknown_key: 1\n")
	check := configParseCheck{deps: Deps{ConfigPath: broken}}
	findings := runCheck(t, check)
	if len(findings) != 1 || findings[0].Severity != SeverityCritical {
		t.Fatalf("expected critical finding for broken config, got %+v", findings)
	}

	valid := filepath.Join(dir, "valid.yaml")
	write(t, valid, "prism:\n  region: cn\n")
	check = configParseCheck{deps: Deps{ConfigPath: valid}}
	if findings := runCheck(t, check); len(findings) != 0 {
		t.Fatalf("expected no findings for valid config, got %+v", findings)
	}

	// Missing file uses defaults; not a doctor anomaly.
	check = configParseCheck{deps: Deps{ConfigPath: filepath.Join(dir, "missing.yaml")}}
	if findings := runCheck(t, check); len(findings) != 0 {
		t.Fatalf("expected no findings for missing config, got %+v", findings)
	}
}

func TestDBCheck(t *testing.T) {
	dir := t.TempDir()
	// Missing database is normal (created on demand).
	check := dbCheck{deps: Deps{DBPath: filepath.Join(dir, "missing.sqlite")}}
	if findings := runCheck(t, check); len(findings) != 0 {
		t.Fatalf("expected no findings for missing db, got %+v", findings)
	}
	// A directory at the db path is damage.
	dbPath := filepath.Join(dir, "is-a-dir")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatal(err)
	}
	check = dbCheck{deps: Deps{DBPath: dbPath}}
	findings := runCheck(t, check)
	if len(findings) != 1 || findings[0].Severity != SeverityWarning {
		t.Fatalf("expected warning for directory db path, got %+v", findings)
	}
	// A writable file is fine.
	good := filepath.Join(dir, "good.sqlite")
	write(t, good, "")
	check = dbCheck{deps: Deps{DBPath: good}}
	if findings := runCheck(t, check); len(findings) != 0 {
		t.Fatalf("expected no findings for writable db, got %+v", findings)
	}
}

func TestLogsCheck(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "logs", "hub.log"), strings.Repeat("x", 4096))
	check := logsCheck{deps: Deps{LogDir: func() string { return filepath.Join(dir, "logs") }, MaxLogDirBytes: 1024}}
	findings := runCheck(t, check)
	if len(findings) != 1 {
		t.Fatalf("expected logs size finding, got %+v", findings)
	}
	if _, ok := findings[0].SummaryParams["size_mb"]; !ok {
		t.Fatalf("expected size_mb param, got %+v", findings[0])
	}
	check = logsCheck{deps: Deps{LogDir: func() string { return filepath.Join(dir, "logs") }, MaxLogDirBytes: 1 << 20}}
	if findings := runCheck(t, check); len(findings) != 0 {
		t.Fatalf("expected no finding under limit, got %+v", findings)
	}
}

func TestRealtimeCheck(t *testing.T) {
	base := Deps{
		RuntimeState: func() (RuntimeStateSnapshot, bool) {
			return RuntimeStateSnapshot{State: "auth_expired", UpdatedAt: time.Now().Format(time.RFC3339)}, true
		},
	}
	findings := runCheck(t, realtimeCheck{deps: base})
	if len(findings) != 1 || findings[0].SummaryKey != "doctor.finding_conn_auth" {
		t.Fatalf("expected auth finding, got %+v", findings)
	}

	// Stuck reconnect older than the threshold.
	stuck := Deps{
		StuckConnectionAge: time.Minute,
		RuntimeState: func() (RuntimeStateSnapshot, bool) {
			return RuntimeStateSnapshot{
				State:     "reconnecting",
				UpdatedAt: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339),
			}, true
		},
	}
	findings = runCheck(t, realtimeCheck{deps: stuck})
	if len(findings) != 1 || findings[0].SummaryKey != "doctor.finding_conn_stuck" {
		t.Fatalf("expected stuck finding, got %+v", findings)
	}

	// Fresh reconnect is normal backoff, not a finding.
	fresh := Deps{
		RuntimeState: func() (RuntimeStateSnapshot, bool) {
			return RuntimeStateSnapshot{State: "reconnecting", UpdatedAt: time.Now().UTC().Format(time.RFC3339)}, true
		},
	}
	if findings := runCheck(t, realtimeCheck{deps: fresh}); len(findings) != 0 {
		t.Fatalf("expected no finding for fresh reconnect, got %+v", findings)
	}

	// No state file: bridge simply not running yet.
	none := Deps{RuntimeState: func() (RuntimeStateSnapshot, bool) { return RuntimeStateSnapshot{}, false }}
	if findings := runCheck(t, realtimeCheck{deps: none}); len(findings) != 0 {
		t.Fatalf("expected no finding without state file, got %+v", findings)
	}
}

type fakeRuntimePlugins struct{ health []pluginruntime.AdapterHealth }

func (f fakeRuntimePlugins) HealthSnapshot() []pluginruntime.AdapterHealth { return f.health }

func TestPluginRuntimeCheck(t *testing.T) {
	deps := Deps{RuntimePlugins: func() RuntimePlugins {
		return fakeRuntimePlugins{health: []pluginruntime.AdapterHealth{
			{ID: "capped-plugin", Health: &pluginruntime.RuntimeHealth{Failures: 5, LastError: "exit 1"}},
			{ID: "flapping-plugin", Health: &pluginruntime.RuntimeHealth{Failures: 3, LastError: "exit 1"}},
			{ID: "idle-plugin", Health: &pluginruntime.RuntimeHealth{}},
		}}
	}}
	findings := runCheck(t, pluginRuntimeCheck{deps: deps})
	if len(findings) != 2 {
		t.Fatalf("expected capped + flapping findings, got %+v", findings)
	}
	if findings[0].Subject != "capped-plugin" || findings[0].Severity != SeverityCritical {
		t.Fatalf("expected capped critical first, got %+v", findings[0])
	}

	// No bridge runtime: nothing to observe.
	absent := Deps{RuntimePlugins: func() RuntimePlugins { return nil }}
	if findings := runCheck(t, pluginRuntimeCheck{deps: absent}); len(findings) != 0 {
		t.Fatalf("expected no findings without runtime, got %+v", findings)
	}
}

func TestPluginLeaseCheck(t *testing.T) {
	t.Run("cleans stale lease and reports one notice cycle", func(t *testing.T) {
		dir := t.TempDir()
		// A very high PID is not alive on any supported platform.
		leasePath := filepath.Join(dir, "runtime", "plugins", "prism-plugin-demo.json")
		write(t, leasePath, `{"pid": 2100000000, "plugin_id": "demo", "session_token": "tok"}`)
		check := pluginLeaseCheck{deps: Deps{DataPath: func(name string) string {
			return filepath.Join(dir, name)
		}}}

		findings := runCheck(t, check)
		if len(findings) != 1 || findings[0].Subject != "demo" || findings[0].Severity != SeverityInfo {
			t.Fatalf("expected one cleaned-lease notice, got %+v", findings)
		}
		if findings[0].SummaryKey != "doctor.finding_lease_cleaned" {
			t.Fatalf("expected the cleaned summary, got %+v", findings[0])
		}
		if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
			t.Fatalf("stale lease file should have been removed: %v", err)
		}

		// The notice resolves on the next sweep.
		if findings := runCheck(t, check); len(findings) != 0 {
			t.Fatalf("expected no findings after cleanup, got %+v", findings)
		}
	})

	t.Run("keeps the finding with the reason when removal fails", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("chmod-based permission checks are not portable on windows")
		}
		dir := t.TempDir()
		pluginsDir := filepath.Join(dir, "runtime", "plugins")
		write(t, filepath.Join(pluginsDir, "prism-plugin-demo.json"),
			`{"pid": 2100000000, "plugin_id": "demo", "session_token": "tok"}`)
		if err := os.Chmod(pluginsDir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(pluginsDir, 0o700) })
		check := pluginLeaseCheck{deps: Deps{DataPath: func(name string) string {
			return filepath.Join(dir, name)
		}}}

		findings := runCheck(t, check)
		if len(findings) != 1 || findings[0].SummaryKey != "doctor.finding_lease_stale" {
			t.Fatalf("expected stale finding to persist on removal failure, got %+v", findings)
		}
		if !strings.Contains(findings[0].Detail, "remove:") {
			t.Fatalf("expected removal error in detail, got %+v", findings[0])
		}
	})

	t.Run("empty dir has no findings", func(t *testing.T) {
		empty := t.TempDir()
		deps := Deps{DataPath: func(name string) string { return filepath.Join(empty, name) }}
		if findings := runCheck(t, pluginLeaseCheck{deps: deps}); len(findings) != 0 {
			t.Fatalf("expected no findings, got %+v", findings)
		}
	})
}

func TestInstallStateCheck(t *testing.T) {
	root := t.TempDir()
	installer := plugininstall.New(root)

	// Corrupt state.json -> warning.
	write(t, filepath.Join(root, "broken", "state.json"), "{not json")

	// Active version without manifest -> warning.
	write(t, filepath.Join(root, "noversion", "state.json"),
		`{"active_version":"1.0.0","updated_at":"2026-01-01T00:00:00Z"}`)

	// Healthy install -> no finding.
	write(t, filepath.Join(root, "healthy", "state.json"),
		`{"active_version":"1.0.0","updated_at":"2026-01-01T00:00:00Z"}`)
	write(t, filepath.Join(root, "healthy", "versions", "1.0.0", "pluginbridge-plugin.yaml"), "id: healthy\n")

	findings := runCheck(t, installStateCheck{deps: Deps{Installer: installer}})
	if len(findings) != 2 {
		t.Fatalf("expected two install findings, got %+v", findings)
	}
	subjects := map[string]bool{}
	for _, item := range findings {
		subjects[item.Subject] = true
	}
	if !subjects["broken"] || !subjects["noversion"] {
		t.Fatalf("unexpected finding subjects: %+v", subjects)
	}
}

func TestActiveVersionCheck(t *testing.T) {
	root := t.TempDir()
	installer := plugininstall.New(root)
	// Activated plugin whose root no longer discovers (broken active build).
	write(t, filepath.Join(root, "gone", "state.json"),
		`{"active_version":"1.0.0","updated_at":"2026-01-01T00:00:00Z"}`)
	// Plugin dirs point somewhere with no discoverable plugins.
	emptyPluginDir := t.TempDir()

	deps := Deps{
		Installer:  installer,
		PluginDirs: func() ([]string, error) { return []string{emptyPluginDir}, nil },
	}
	findings := runCheck(t, activeVersionCheck{deps: deps})
	if len(findings) != 1 || findings[0].Subject != "gone" {
		t.Fatalf("expected active_version finding for gone, got %+v", findings)
	}

	// Broken config: config_parse owns the report, no finding here.
	deps.PluginDirs = func() ([]string, error) { return nil, errors.New("config broken") }
	if findings := runCheck(t, activeVersionCheck{deps: deps}); len(findings) != 0 {
		t.Fatalf("expected no findings with broken config, got %+v", findings)
	}
}

func TestNodeRuntimeCheck(t *testing.T) {
	emptyPluginDir := t.TempDir()
	// A plugin requiring node: a minimal valid manifest.
	pluginDir := t.TempDir()
	write(t, filepath.Join(pluginDir, "needs-node", "pluginbridge-plugin.yaml"),
		"id: needs-node\nversion: 1.0.0\ntransport: process\nintegration_mode: protocol-native\ncommand:\n  - ${runtime.node}\n  - dist/index.js\nruntime_dependencies:\n  node: '>=22 <23'\n")
	deps := Deps{
		PluginDirs: func() ([]string, error) { return []string{pluginDir}, nil },
		NodeBinary: func() string { return filepath.Join(t.TempDir(), "missing-node") },
	}
	findings := runCheck(t, nodeRuntimeCheck{deps: deps})
	if len(findings) != 1 || findings[0].SummaryKey != "doctor.finding_node_missing" {
		t.Fatalf("expected node missing finding, got %+v", findings)
	}

	// No node-dependent plugins: missing binary is irrelevant.
	deps.PluginDirs = func() ([]string, error) { return []string{emptyPluginDir}, nil }
	if findings := runCheck(t, nodeRuntimeCheck{deps: deps}); len(findings) != 0 {
		t.Fatalf("expected no findings without node requirement, got %+v", findings)
	}
}

func TestCatalogCacheCheck(t *testing.T) {
	dir := t.TempDir()
	cache := filepath.Join(dir, "catalog.json")
	write(t, cache, "{}")
	past := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(cache, past, past); err != nil {
		t.Fatal(err)
	}
	deps := Deps{CatalogCachePath: func() string { return cache }}
	findings := runCheck(t, catalogCacheCheck{deps: deps})
	if len(findings) != 1 || findings[0].Severity != SeverityInfo {
		t.Fatalf("expected stale catalog info finding, got %+v", findings)
	}
	if findings := runCheck(t, catalogCacheCheck{deps: Deps{CatalogCachePath: func() string { return filepath.Join(dir, "missing.json") }}}); len(findings) != 0 {
		t.Fatalf("missing cache is not a finding, got %+v", findings)
	}
}

func TestManagedAppsCheck(t *testing.T) {
	deps := Deps{ManagedApps: func() []ManagedAppSnapshot {
		return []ManagedAppSnapshot{
			{AppID: "codex", DisplayName: "Codex", ManagedEnabled: true, Running: true, CDPReachable: false},
			{AppID: "hermes", DisplayName: "Hermes", ManagedEnabled: true, Running: false, CDPReachable: false},
			{AppID: "other", DisplayName: "Other", ManagedEnabled: false, Running: true, CDPReachable: false},
		}
	}}
	findings := runCheck(t, managedAppsCheck{deps: deps})
	if len(findings) != 1 || findings[0].Subject != "codex" {
		t.Fatalf("expected one managed-app finding for running-but-unreachable app, got %+v", findings)
	}
}

// --- remedies ------------------------------------------------------------------

func TestConfigParseCheckRepairRemedy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	write(t, path, "bridge:\n  totally_unknown_key: 1\n")
	deps := Deps{
		ConfigPath: path,
		ConfigRepair: func(ctx context.Context) (string, error) {
			return bridgeconfig.RepairFile(path)
		},
	}
	check := configParseCheck{deps: deps}
	findings := runCheck(t, check)
	if len(findings) != 1 {
		t.Fatalf("expected config finding, got %+v", findings)
	}
	remedies := check.Remedies(findings[0])
	if len(remedies) != 1 || remedies[0].Risk != RiskConfirm || !remedies[0].Available {
		t.Fatalf("expected available confirm remedy, got %+v", remedies)
	}
	if err := check.ExecuteRemedy(context.Background(), findings[0], "repair"); err != nil {
		t.Fatalf("execute repair: %v", err)
	}
	if repaired := runCheck(t, check); len(repaired) != 0 {
		t.Fatalf("config must load after repair, got %+v", repaired)
	}
}

func TestPluginRuntimeCheckRiskEscalation(t *testing.T) {
	running := Deps{
		RuntimePlugins: func() RuntimePlugins {
			return fakeRuntimePlugins{health: []pluginruntime.AdapterHealth{
				{ID: "live-plugin", Health: &pluginruntime.RuntimeHealth{Failures: 3, Running: true}},
			}}
		},
		PluginRestart: func(ctx context.Context, pluginID string) error { return nil },
	}
	dead := Deps{
		RuntimePlugins: func() RuntimePlugins {
			return fakeRuntimePlugins{health: []pluginruntime.AdapterHealth{
				{ID: "dead-plugin", Health: &pluginruntime.RuntimeHealth{Failures: 5}},
			}}
		},
		PluginRestart: func(ctx context.Context, pluginID string) error { return nil },
	}

	liveFindings := runCheck(t, pluginRuntimeCheck{deps: running})
	remedy := pluginRuntimeCheck{deps: running}.Remedies(liveFindings[0])
	if len(remedy) != 1 || remedy[0].Risk != RiskConfirm || remedy[0].ImpactKey == "" {
		t.Fatalf("a running plugin's restart must be confirm-class with impact, got %+v", remedy)
	}

	deadFindings := runCheck(t, pluginRuntimeCheck{deps: dead})
	remedy = pluginRuntimeCheck{deps: dead}.Remedies(deadFindings[0])
	if len(remedy) != 1 || remedy[0].Risk != RiskTransparent {
		t.Fatalf("a dead plugin's restart must be transparent, got %+v", remedy)
	}
}

func TestActiveVersionCheckRollbackRemedy(t *testing.T) {
	root := t.TempDir()
	installer := plugininstall.New(root)
	write(t, filepath.Join(root, "gone", "state.json"),
		`{"active_version":"1.0.0","previous_version":"0.9.0","updated_at":"2026-01-01T00:00:00Z"}`)
	write(t, filepath.Join(root, "noprev", "state.json"),
		`{"active_version":"1.0.0","updated_at":"2026-01-01T00:00:00Z"}`)
	emptyPluginDir := t.TempDir()
	deps := Deps{
		Installer:     installer,
		PluginDirs:    func() ([]string, error) { return []string{emptyPluginDir}, nil },
		PluginRestart: func(ctx context.Context, pluginID string) error { return nil },
	}
	findings := runCheck(t, activeVersionCheck{deps: deps})
	if len(findings) != 2 {
		t.Fatalf("expected findings for both plugins, got %+v", findings)
	}
	bySubject := map[string]Finding{}
	for _, item := range findings {
		bySubject[item.Subject] = item
	}
	// Rebuild the check after swapping the restart provider: deps is a value.
	restarted := ""
	deps.PluginRestart = func(ctx context.Context, pluginID string) error {
		restarted = pluginID
		return nil
	}
	check := activeVersionCheck{deps: deps}
	withPrev := check.Remedies(bySubject["gone"])
	if len(withPrev) != 1 || !withPrev[0].Available || withPrev[0].Risk != RiskConfirm {
		t.Fatalf("rollback must be available with a previous version, got %+v", withPrev)
	}
	withoutPrev := check.Remedies(bySubject["noprev"])
	if len(withoutPrev) != 1 || withoutPrev[0].Available {
		t.Fatalf("rollback must be unavailable without a previous version, got %+v", withoutPrev)
	}
	// Executing rollback swaps the active version and reloads the plugin.
	if err := check.ExecuteRemedy(context.Background(), bySubject["gone"], "rollback"); err != nil {
		t.Fatalf("rollback execute: %v", err)
	}
	if restarted != "gone" {
		t.Fatalf("expected plugin reload after rollback, got %q", restarted)
	}
	state, err := installer.State("gone")
	if err != nil || state.ActiveVersion != "0.9.0" {
		t.Fatalf("expected active version swapped to previous, got %+v err=%v", state, err)
	}
}

func TestLogsCheckCleanRemedy(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	write(t, filepath.Join(logDir, "hub.log"), "current")
	old := filepath.Join(logDir, "hub-old.log")
	write(t, old, "rotated")
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	deps := Deps{
		LogDir:         func() string { return logDir },
		MaxLogDirBytes: 1,
	}
	check := logsCheck{deps: deps}
	findings := runCheck(t, check)
	if len(findings) != 1 {
		t.Fatalf("expected logs finding, got %+v", findings)
	}
	remedy := check.Remedies(findings[0])
	if len(remedy) != 1 || remedy[0].Risk != RiskConfirm || !remedy[0].Available {
		t.Fatalf("expected available confirm remedy, got %+v", remedy)
	}
	if err := check.ExecuteRemedy(context.Background(), findings[0], "clean"); err != nil {
		t.Fatalf("clean execute: %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old log must be removed")
	}
	if _, err := os.Stat(filepath.Join(logDir, "hub.log")); err != nil {
		t.Fatal("recent log must survive")
	}
}
