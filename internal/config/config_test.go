package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"rokid-prism-hub/internal/buildinfo"
)

func TestLoadMissingUsesBridgeDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if cfg.Prism.Region != DefaultPrismRegion {
		t.Fatalf("unexpected region: %q", cfg.Prism.Region)
	}
	if cfg.Prism.RealtimeURL() != "" || cfg.Prism.APIURL() != "" {
		t.Fatalf("source build must not contain deployment endpoints, got api=%q realtime=%q", cfg.Prism.APIURL(), cfg.Prism.RealtimeURL())
	}
	if len(cfg.Bridge.Plugins) != 0 {
		t.Fatalf("unexpected default plugins: %+v", cfg.Bridge.Plugins)
	}
	if !cfg.Bridge.KeepAwake {
		t.Fatal("expected keep_awake default to be enabled")
	}
}

func TestLoadNormalizesPluginKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`
prism:
  region: " GLOBAL "
bridge:
  plugin_dirs:
    - " /tmp/plugins "
  plugin_env:
    " CUSTOM_GLOBAL " : "  yes "
    PRISM_HOST_TOKEN: "secret"
    PRISM_PLUGIN_ID: "spoof"
  plugins:
    " OpenClaw ":
      enabled: true
      source: " BUILTIN "
      gateway_url: " auto "
    Demo:
      enabled: true
      source: " Process "
      reverse_sync: " ALL_THREADS "
      command: "demo"
      socket_path: " /tmp/demo.sock "
      env:
        " DEMO_FLAG " : "  on "
        PRISM_CONFIG: "/tmp/prism.yaml"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := cfg.Bridge.Plugins["openclaw"]; !ok {
		t.Fatalf("plugin key not normalized: %+v", cfg.Bridge.Plugins)
	}
	if cfg.Bridge.Plugins["demo"].Source != "process" || cfg.Bridge.Plugins["demo"].SocketPath != "/tmp/demo.sock" {
		t.Fatalf("plugin fields not normalized: %+v", cfg.Bridge.Plugins["demo"])
	}
	if cfg.Bridge.Plugins["demo"].ReverseSync != "all_threads" {
		t.Fatalf("plugin reverse sync not normalized: %+v", cfg.Bridge.Plugins["demo"])
	}
	if len(cfg.Bridge.PluginDirs) != 1 || cfg.Bridge.PluginDirs[0] != "/tmp/plugins" {
		t.Fatalf("plugin dirs not normalized: %+v", cfg.Bridge.PluginDirs)
	}
	if cfg.Bridge.PluginEnv["CUSTOM_GLOBAL"] != "yes" {
		t.Fatalf("global plugin env not normalized: %+v", cfg.Bridge.PluginEnv)
	}
	if _, ok := cfg.Bridge.PluginEnv["PRISM_HOST_TOKEN"]; ok {
		t.Fatalf("sensitive global env was not filtered: %+v", cfg.Bridge.PluginEnv)
	}
	if cfg.Bridge.Plugins["demo"].Env["DEMO_FLAG"] != "on" {
		t.Fatalf("plugin env not normalized: %+v", cfg.Bridge.Plugins["demo"].Env)
	}
	if _, ok := cfg.Bridge.Plugins["demo"].Env["PRISM_CONFIG"]; ok {
		t.Fatalf("sensitive plugin env was not filtered: %+v", cfg.Bridge.Plugins["demo"].Env)
	}
	if cfg.Prism.Region != PrismRegionGlobal {
		t.Fatalf("prism region not normalized: %+v", cfg.Prism)
	}
	if cfg.Prism.APIURL() != "" || cfg.Prism.RealtimeURL() != "" {
		t.Fatalf("source build must not contain deployment endpoints: %+v", cfg.Prism)
	}
}

func TestSaveRemovesLegacyPluginEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
bridge:
  plugins:
    codex:
      enabled: false
      mode: desktop
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load legacy plugin enabled: %v", err)
	}
	if cfg.Bridge.Plugins["codex"].Mode != "desktop" {
		t.Fatalf("legacy plugin fields were not retained: %+v", cfg.Bridge.Plugins["codex"])
	}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("save migrated config: %v", err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(saved), "enabled: false") {
		t.Fatalf("plugin enabled must not survive migration: %s", saved)
	}
}

func TestSaveMigratesLegacyIdentityToDeploymentAndRegion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
prism:
  region: global
  host_token: legacy-host-token
account:
  account_id: account-1
  access_token: access-token-1
  host_id: host-1
bridge:
  enabled: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load legacy config: %v", err)
	}
	key := IdentityKey(buildinfo.Environment(), PrismRegionGlobal)
	if got := cfg.Prism.Identities[key].HostToken; got != "legacy-host-token" {
		t.Fatalf("legacy identity was not migrated: %+v", cfg.Prism.Identities)
	}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save migrated config: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) == "" || containsAny(string(contents), "\naccount:", "\n  host_token: legacy-host-token") {
		t.Fatalf("legacy fields persisted after migration: %s", contents)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("reload migrated config: %v", err)
	}
	if reloaded.Account.HostID != "host-1" || reloaded.Prism.HostToken != "legacy-host-token" {
		t.Fatalf("migrated identity was not restored: %+v %+v", reloaded.Prism, reloaded.Account)
	}
}

func TestBuildEndpointsAreInjectedPerRegion(t *testing.T) {
	originalAPI := buildinfo.APIURLGlobal
	originalRealtime := buildinfo.RealtimeURLGlobal
	buildinfo.APIURLGlobal = "https://test.example/prism/api"
	buildinfo.RealtimeURLGlobal = "wss://test.example/prism/realtime/host"
	t.Cleanup(func() {
		buildinfo.APIURLGlobal = originalAPI
		buildinfo.RealtimeURLGlobal = originalRealtime
	})
	endpoints := ResolvePrismEndpoints(PrismRegionGlobal)
	if endpoints.APIURL != buildinfo.APIURLGlobal || endpoints.RealtimeURL != buildinfo.RealtimeURLGlobal {
		t.Fatalf("unexpected injected endpoints: %+v", endpoints)
	}
}

func TestLoadSelectsIdentityForPackagedEnvironmentAndRegion(t *testing.T) {
	originalEnvironment := buildinfo.DeploymentEnvironment
	buildinfo.DeploymentEnvironment = "test"
	t.Cleanup(func() { buildinfo.DeploymentEnvironment = originalEnvironment })
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
prism:
  region: cn
  identities:
    dev.cn:
      host_token: dev-token
      account:
        account_id: dev-account
    test.cn:
      host_token: test-token
      account:
        account_id: test-account
bridge:
  enabled: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load scoped identities: %v", err)
	}
	if cfg.Prism.HostToken != "test-token" || cfg.Account.AccountID != "test-account" {
		t.Fatalf("test build selected the wrong identity: %+v %+v", cfg.Prism, cfg.Account)
	}
	cfg.SelectRegion(PrismRegionGlobal)
	if cfg.Prism.HostToken != "" || cfg.Account.AccountID != "" {
		t.Fatalf("missing test.global identity must not reuse cn identity: %+v %+v", cfg.Prism, cfg.Account)
	}
	if cfg.Prism.Identities["test.cn"].HostToken != "test-token" {
		t.Fatalf("region switch failed to preserve prior scoped identity: %+v", cfg.Prism.Identities)
	}
}

func containsAny(value string, values ...string) bool {
	for _, candidate := range values {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func TestSaveRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.yaml")
	cfg := Default()
	cfg.Bridge.PluginDirs = []string{"/tmp/plugins"}
	cfg.Bridge.KeepAwake = true
	cfg.Bridge.Plugins["demo"] = PluginConfig{
		Source:             "plugin",
		Mode:               "test",
		ReverseSync:        "off",
		VisibilityRequired: true,
	}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.Bridge.KeepAwake {
		t.Fatal("expected keep_awake to survive round-trip")
	}
	if loaded.Bridge.Plugins["demo"].Source != "plugin" {
		t.Fatalf("unexpected saved config: %+v", loaded.Bridge.Plugins["demo"])
	}
	if loaded.Bridge.Plugins["demo"].ReverseSync != "off" {
		t.Fatalf("unexpected saved reverse sync: %+v", loaded.Bridge.Plugins["demo"])
	}
	if loaded.Prism.Region != DefaultPrismRegion {
		t.Fatalf("unexpected saved prism region: %+v", loaded.Prism)
	}
}

func TestLoadToleratesLegacyDefaultPlugin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`
bridge:
  enabled: true
  default_plugin: codex
  plugins:
    codex:
      mode: desktop
      source: plugin
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load with legacy default_plugin: %v", err)
	}
	if !cfg.Bridge.Enabled {
		t.Fatal("bridge should stay enabled")
	}
	if _, ok := cfg.Bridge.Plugins["codex"]; !ok {
		t.Fatalf("codex plugin missing after load: %+v", cfg.Bridge.Plugins)
	}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	saved, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if bytes.Contains(saved, []byte("default_plugin")) {
		t.Fatalf("saved config must not re-emit default_plugin: %s", saved)
	}
}

func TestSampleConfigLoads(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.sample.yaml"))
	if err != nil {
		t.Fatalf("Load sample config: %v", err)
	}
	if cfg.Prism.Region != DefaultPrismRegion {
		t.Fatalf("unexpected sample region: %q", cfg.Prism.Region)
	}
	if !cfg.Bridge.Enabled {
		t.Fatal("sample bridge should be enabled")
	}
	if len(cfg.Bridge.Plugins) != 0 {
		t.Fatalf("sample should not enable commented plugin examples: %+v", cfg.Bridge.Plugins)
	}
}
