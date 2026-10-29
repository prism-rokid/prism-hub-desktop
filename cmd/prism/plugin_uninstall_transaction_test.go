package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	bridgeconfig "rokid-prism-hub/internal/config"
)

func dashboardPluginUninstallTestConfig(t *testing.T, root string) string {
	t.Helper()
	configPath := filepath.Join(root, "config.yaml")
	cfg := bridgeconfig.Default()
	cfg.Account.AccessToken = "test-token"
	cfg.Account.HostID = "host-1"
	cfg.Bridge.PluginDirs = []string{filepath.Join(root, "plugins")}
	cfg.Bridge.Plugins["demo"] = bridgeconfig.PluginConfig{Source: "plugin"}
	if err := os.MkdirAll(filepath.Join(root, "plugins", "demo"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "plugins", "demo", "pluginbridge-plugin.yaml"), []byte("id: demo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func TestDashboardUninstallPluginKeepsLocalPluginWhenRemoteStillRegistered(t *testing.T) {
	root := t.TempDir()
	configPath := dashboardPluginUninstallTestConfig(t, root)
	remote := dashboardPluginUninstallRemoteFuncs{
		delete:     func(*bridgeconfig.Config, string) error { return errors.New("network unavailable") },
		registered: func(*bridgeconfig.Config, string) (bool, error) { return true, nil },
	}
	if _, err := dashboardUninstallPluginStrict(configPath, "", "demo", nil, remote); err == nil {
		t.Fatal("expected remote deletion failure")
	}
	pluginPath := filepath.Join(root, "plugins", "demo")
	if _, err := os.Stat(pluginPath); err != nil {
		t.Fatalf("local plugin must remain until cloud deletion is confirmed: %v", err)
	}
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Bridge.Plugins["demo"]; !ok {
		t.Fatal("local plugin configuration must remain until cloud deletion is confirmed")
	}
	if cfg.Bridge.Plugins["demo"].Uninstalling {
		t.Fatal("plugin must not be disabled before cloud deletion is confirmed")
	}
	store, err := loadDashboardPluginUninstallStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if operation := store.Operations["demo"]; operation.State != dashboardPluginUninstallStateRemotePending {
		t.Fatalf("unexpected transaction state: %+v", operation)
	}
}

func TestDashboardPluginUninstallRootFindsRequestedDirectSource(t *testing.T) {
	root := t.TempDir()
	codexRoot := filepath.Join(root, "prism-plugin-codex")
	hermesRoot := filepath.Join(root, "prism-plugin-hermes")
	for pluginRoot, pluginID := range map[string]string{codexRoot: "codex", hermesRoot: "hermes"} {
		if err := os.MkdirAll(pluginRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(pluginRoot, "pluginbridge-plugin.yaml"), []byte("id: "+pluginID+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := bridgeconfig.Default()
	cfg.Bridge.PluginDirs = []string{codexRoot, hermesRoot}

	resolved, err := dashboardPluginUninstallRoot(cfg, "", "hermes", nil)
	if err != nil {
		t.Fatalf("解析直接源码目录失败: %v", err)
	}
	if resolved != hermesRoot {
		t.Fatalf("应定位到 Hermes 源码目录，实际为 %q", resolved)
	}
}

func TestDashboardUninstallPluginConfirmsRemoteDeletionBeforeLocalRemoval(t *testing.T) {
	root := t.TempDir()
	configPath := dashboardPluginUninstallTestConfig(t, root)
	remote := dashboardPluginUninstallRemoteFuncs{
		delete:     func(*bridgeconfig.Config, string) error { return errors.New("response lost") },
		registered: func(*bridgeconfig.Config, string) (bool, error) { return false, nil },
	}
	payload, err := dashboardUninstallPluginStrict(configPath, "", "demo", nil, remote)
	if err != nil {
		t.Fatalf("remote list confirmation should permit local cleanup: %v", err)
	}
	if payload["remote_cleanup"] != "confirmed" || payload["transaction_state"] != "completed" {
		t.Fatalf("unexpected result: %+v", payload)
	}
	if _, err := os.Stat(filepath.Join(root, "plugins", "demo")); !os.IsNotExist(err) {
		t.Fatalf("expected local plugin removal after remote confirmation, err=%v", err)
	}
	store, err := loadDashboardPluginUninstallStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.Operations) != 0 {
		t.Fatalf("completed transaction must be removed: %+v", store)
	}
	if _, err := os.Stat(dashboardPluginUninstallStatePath(configPath)); !os.IsNotExist(err) {
		t.Fatalf("completed transaction file must be removed, err=%v", err)
	}
}

func TestDashboardUninstallPluginDetachesDirectSourceAfterRemoteConfirmation(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	directSource := filepath.Join(root, "prism-plugin-demo")
	if err := os.MkdirAll(directSource, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directSource, "pluginbridge-plugin.yaml"), []byte("id: demo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := bridgeconfig.Default()
	cfg.Account.AccessToken = "test-token"
	cfg.Account.HostID = "host-1"
	cfg.Bridge.PluginDirs = []string{directSource}
	cfg.Bridge.Plugins["demo"] = bridgeconfig.PluginConfig{Source: "plugin"}
	if err := cfg.Save(configPath); err != nil {
		t.Fatal(err)
	}
	remote := dashboardPluginUninstallRemoteFuncs{
		delete:     func(*bridgeconfig.Config, string) error { return nil },
		registered: func(*bridgeconfig.Config, string) (bool, error) { return false, nil },
	}
	payload, err := dashboardUninstallPluginStrict(configPath, "", "demo", nil, remote)
	if err != nil {
		t.Fatalf("卸载直接源码插件失败: %v", err)
	}
	if payload["plugin_source_preserved"] != true || payload["remote_cleanup"] != "confirmed" {
		t.Fatalf("直接源码插件卸载结果异常: %+v", payload)
	}
	if _, err := os.Stat(filepath.Join(directSource, "pluginbridge-plugin.yaml")); err != nil {
		t.Fatalf("必须保留直接源码仓库: %v", err)
	}
	loaded, err := bridgeconfig.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Bridge.PluginDirs) != 0 {
		t.Fatalf("必须从 Hub 中移除直接源码目录: %+v", loaded.Bridge.PluginDirs)
	}
	if _, ok := loaded.Bridge.Plugins["demo"]; ok {
		t.Fatalf("必须移除直接源码插件配置: %+v", loaded.Bridge.Plugins)
	}
}

func TestDashboardRecoverPluginUninstallsCompletesRemoteConfirmedLocalCleanup(t *testing.T) {
	root := t.TempDir()
	configPath := dashboardPluginUninstallTestConfig(t, root)
	store := dashboardPluginUninstallStore{Operations: map[string]dashboardPluginUninstallOperation{
		"demo": {PluginID: "demo", Root: filepath.Join(root, "plugins"), State: dashboardPluginUninstallStateRemoteDeleted},
	}}
	if err := saveDashboardPluginUninstallStore(configPath, store); err != nil {
		t.Fatal(err)
	}
	if err := dashboardRecoverPluginUninstalls(configPath, nil); err != nil {
		t.Fatalf("recover transaction: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "plugins", "demo")); !os.IsNotExist(err) {
		t.Fatalf("expected recovered local cleanup, err=%v", err)
	}
	store, err := loadDashboardPluginUninstallStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.Operations) != 0 {
		t.Fatalf("recovered transaction must be removed: %+v", store)
	}
	if _, err := os.Stat(dashboardPluginUninstallStatePath(configPath)); !os.IsNotExist(err) {
		t.Fatalf("recovered transaction file must be removed, err=%v", err)
	}
}

func TestDashboardMarkPluginUninstallingDisablesLocalRuntime(t *testing.T) {
	root := t.TempDir()
	configPath := dashboardPluginUninstallTestConfig(t, root)
	if err := dashboardMarkPluginUninstalling(configPath, "demo"); err != nil {
		t.Fatal(err)
	}
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Bridge.Plugins["demo"]
	if !item.Uninstalling {
		t.Fatal("plugin must be persisted as uninstalling")
	}
	if bridgePluginAllowed(cfg, "demo") {
		t.Fatal("uninstalling plugin must be excluded from local runtime")
	}
}
