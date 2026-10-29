package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginruntime"
	"rokid-prism-hub/internal/runtimes"
)

type pluginTestStep struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
	Error  string `json:"error,omitempty"`
}

func installPluginFromSource(configPath, pluginDir, source string, activate bool, stderr io.Writer) (map[string]any, int) {
	resolvedSource := strings.TrimSpace(source)
	if resolvedSource == "" {
		err := fmt.Errorf("plugin source required")
		fmt.Fprintf(stderr, "%v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	if !looksLikeRemotePluginSource(resolvedSource) {
		return installPluginFromPath(configPath, pluginDir, resolvedSource, activate, stderr)
	}
	localRoot, cleanup, err := materializePluginSource(resolvedSource, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "prepare plugin source failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	defer cleanup()
	payload, code := installPluginFromPath(configPath, pluginDir, localRoot, activate, stderr)
	if payload != nil {
		payload["source"] = resolvedSource
		payload["source_kind"] = "url"
	}
	return payload, code
}

func pluginListPayload(ctx context.Context, configPath, path string, probe bool, stderr io.Writer) (map[string]any, int) {
	dirs, cfgErr := pluginListDirs(configPath, path)
	cfg, _ := bridgeconfig.Load(configPath)
	payload := map[string]any{
		"ok":          cfgErr == nil,
		"config_path": configPath,
		"plugin_dirs": dirs,
		"checked_at":  time.Now().UTC().Format(time.RFC3339),
	}
	if cfgErr != nil {
		payload["error"] = cfgErr.Error()
		fmt.Fprintf(stderr, "%v\n", cfgErr)
		return payload, 1
	}
	discovered, scanErr := pluginpkg.ScanDirs(dirs)
	items := make([]map[string]any, 0, len(discovered))
	for _, plugin := range discovered {
		item := map[string]any{
			"display_name":         plugin.Manifest.DisplayName,
			"version":              plugin.Manifest.Version,
			"plugin_id":            plugin.Manifest.ID,
			"integration_mode":     plugin.Manifest.IntegrationMode,
			"transport":            plugin.Manifest.Transport,
			"mode":                 plugin.Manifest.Mode,
			"runtime_modes":        plugin.Manifest.RuntimeModes,
			"default_runtime_mode": plugin.Manifest.DefaultRuntimeMode,
			"current_runtime_mode": plugin.Manifest.RuntimeMode(cfg.Bridge.Plugins[plugin.Manifest.ID].Mode),
			"manifest_path":        plugin.Path,
			"plugin_root":          plugin.Root,
			"command":              plugin.Manifest.Command,
			"available":            false,
			"visible":              plugin.Manifest.Capabilities.NativeVisibleInput,
			"probed":               probe,
		}
		if probe {
			adapter := pluginpkg.NewProcessAdapterWithEnv(plugin, bridgePluginEnv(cfg, plugin))
			capability, err := adapter.Probe(ctx)
			item["capability"] = capability
			item["available"] = capability.Available
			item["visible"] = capability.NativeVisibleInput
			if err != nil {
				item["error"] = err.Error()
			} else if !capability.Available {
				item["error"] = capability.UnavailableReason
			}
			discovery, err := adapter.Discover(ctx)
			item["discovery"] = discovery
			if err != nil {
				item["error"] = err.Error()
			} else if discovery.Endpoint != "" {
				item["endpoint"] = discovery.Endpoint
			}
			_ = adapter.Close(context.Background())
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		left := fmt.Sprint(items[i]["plugin_id"])
		right := fmt.Sprint(items[j]["plugin_id"])
		return left < right
	})
	payload["plugins"] = items
	if scanErr != nil {
		payload["ok"] = false
		payload["error"] = scanErr.Error()
		fmt.Fprintf(stderr, "%v\n", scanErr)
		return payload, 1
	}
	payload["ok"] = true
	return payload, 0
}

func pluginListDirs(configPath, path string) ([]string, error) {
	if strings.TrimSpace(path) != "" {
		resolved, err := expandPath(path)
		if err != nil {
			return nil, err
		}
		return []string{resolved}, nil
	}
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load prism config failed: %w", err)
	}
	return pluginDirs(cfg), nil
}

func installPluginFromPath(configPath, pluginDir, sourcePath string, activate bool, stderr io.Writer) (map[string]any, int) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load prism config failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	root := strings.TrimSpace(pluginDir)
	if root == "" {
		root = pluginDirs(cfg)[0]
	}
	root, err = expandPath(root)
	if err != nil {
		fmt.Fprintf(stderr, "resolve plugin dir failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	discovered, err := resolvePluginForTest(sourcePath, "")
	if err != nil {
		fmt.Fprintf(stderr, "resolve plugin source failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	installRoot := filepath.Join(root, discovered.Manifest.ID)
	version := firstNonEmpty(strings.TrimSpace(discovered.Manifest.Version), "0.1.0")
	versionDir := filepath.Join(installRoot, "versions", version)
	if err := os.RemoveAll(versionDir); err != nil {
		fmt.Fprintf(stderr, "clean plugin version dir failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	if err := copyDir(discovered.Root, versionDir); err != nil {
		fmt.Fprintf(stderr, "copy plugin source package failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	if looksLikeNodePluginSource(versionDir) {
		if err := installNodePluginDependencies(versionDir); err != nil {
			fmt.Fprintf(stderr, "install node plugin dependencies failed: %v\n", err)
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
	}
	if looksLikeGoPluginSource(versionDir) {
		if err := buildGoPluginBinary(versionDir, discovered.Manifest, stderr); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
		if err := rewriteInstalledGoPluginManifest(versionDir, discovered.Manifest); err != nil {
			fmt.Fprintf(stderr, "rewrite installed go plugin manifest failed: %v\n", err)
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
	}
	currentPath := filepath.Join(installRoot, "current")
	_ = os.RemoveAll(currentPath)
	if err := os.Symlink(versionDir, currentPath); err != nil {
		if err := copyCurrentPluginVersion(versionDir, currentPath); err != nil {
			fmt.Fprintf(stderr, "activate plugin version failed: %v\n", err)
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
	}
	manifestPath := filepath.Join(currentPath, pluginpkg.ManifestYAML)
	if _, err := pluginpkg.LoadManifest(manifestPath); err != nil {
		fmt.Fprintf(stderr, "validate installed plugin manifest failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	if activate {
		if err := activatePluginConfig(cfg, configPath, root, discovered.Manifest); err != nil {
			fmt.Fprintf(stderr, "save bridge config failed: %v\n", err)
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
		if err := activateManagedPluginVersion(installRoot, version); err != nil {
			fmt.Fprintf(stderr, "activate installed plugin version failed: %v\n", err)
			return map[string]any{"ok": false, "error": err.Error()}, 1
		}
	}
	payload := map[string]any{
		"ok":            true,
		"plugin_id":     discovered.Manifest.ID,
		"version":       version,
		"manifest_path": manifestPath,
		"plugin_root":   currentPath,
		"version_path":  versionDir,
		"source_path":   discovered.Root,
		"config_path":   configPath,
		"activated":     activate,
	}
	if binaryPath := installedPluginBinaryPath(currentPath, discovered.Manifest); binaryPath != "" {
		payload["binary_path"] = binaryPath
	}
	return payload, 0
}

func activateManagedPluginVersion(root, version string) error {
	statePath := filepath.Join(root, "state.json")
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !plugininstallSafeVersionName(version) {
		return fmt.Errorf("invalid plugin version %q", version)
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		return err
	}
	var state plugininstall.State
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode plugin state: %w", err)
	}
	if state.ActiveVersion != version {
		state.PreviousVersion = state.ActiveVersion
		state.ActiveVersion = version
		state.UpdatedAt = time.Now().UTC()
	}
	data, err = json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(root, ".state-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, statePath)
}

func plugininstallSafeVersionName(version string) bool {
	return version != "" && version != "." && version != ".." && version == filepath.Base(version)
}

type pluginRemovalTarget struct {
	Root         string
	InstallPath  string
	DirectSource bool
}

func removeInstalledPlugin(configPath, pluginDir, pluginID string, stderr io.Writer) (map[string]any, int) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load prism config failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	normalizedPluginID := strings.TrimSpace(pluginID)
	if normalizedPluginID == "" {
		err := fmt.Errorf("plugin id required")
		fmt.Fprintf(stderr, "%v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	target, err := resolvePluginRemovalTarget(cfg, pluginDir, normalizedPluginID)
	if err != nil {
		fmt.Fprintf(stderr, "解析插件卸载目录失败: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	var removedPluginIDs []string
	if cfg.Bridge.Plugins != nil {
		for pluginID := range cfg.Bridge.Plugins {
			if strings.EqualFold(pluginID, normalizedPluginID) {
				removedPluginIDs = append(removedPluginIDs, pluginID)
				delete(cfg.Bridge.Plugins, pluginID)
			}
		}
	}
	removedPluginDirs := []string(nil)
	if target.DirectSource {
		removedPluginDirs = removeConfiguredPluginDir(cfg, target.Root)
	} else if err := os.RemoveAll(target.InstallPath); err != nil {
		fmt.Fprintf(stderr, "删除本地插件失败: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	if err := cfg.Save(configPath); err != nil {
		fmt.Fprintf(stderr, "save bridge config failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	return map[string]any{
		"ok":                      true,
		"plugin_id":               normalizedPluginID,
		"plugin_install":          target.InstallPath,
		"plugin_source_preserved": target.DirectSource,
		"removed_plugin_dirs":     removedPluginDirs,
		"removed_plugin_ids":      removedPluginIDs,
		"config_path":             configPath,
	}, 0
}

// resolvePluginRemovalTarget 通过清单识别插件目录的组织形式，避免把源码仓库误认为安装根目录。
func resolvePluginRemovalTarget(cfg *bridgeconfig.Config, requestedRoot, pluginID string) (pluginRemovalTarget, error) {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return pluginRemovalTarget{}, fmt.Errorf("缺少插件 ID")
	}
	roots := []string{strings.TrimSpace(requestedRoot)}
	if roots[0] == "" {
		roots = pluginDirs(cfg)
	}
	var scanErrors []string
	for _, root := range roots {
		resolvedRoot, err := expandPath(root)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: %v", root, err))
			continue
		}
		if pluginManifestMatchesID(resolvedRoot, pluginID) {
			return pluginRemovalTarget{
				Root:         resolvedRoot,
				InstallPath:  resolvedRoot,
				DirectSource: true,
			}, nil
		}
		managedPath := filepath.Join(resolvedRoot, pluginID)
		if pluginManifestMatchesID(managedPath, pluginID) {
			return pluginRemovalTarget{
				Root:        resolvedRoot,
				InstallPath: managedPath,
			}, nil
		}
		discovered, err := pluginpkg.ScanDirs([]string{resolvedRoot})
		if err != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: %v", resolvedRoot, err))
			continue
		}
		for _, plugin := range discovered {
			if !strings.EqualFold(strings.TrimSpace(plugin.Manifest.ID), pluginID) {
				continue
			}
			if samePluginPath(plugin.Root, resolvedRoot) {
				return pluginRemovalTarget{
					Root:         resolvedRoot,
					InstallPath:  resolvedRoot,
					DirectSource: true,
				}, nil
			}
			return pluginRemovalTarget{
				Root:        resolvedRoot,
				InstallPath: filepath.Join(resolvedRoot, pluginID),
			}, nil
		}
	}
	if len(scanErrors) > 0 {
		return pluginRemovalTarget{}, fmt.Errorf("未找到插件 %s；已检查的目录异常：%s", pluginID, strings.Join(scanErrors, "; "))
	}
	return pluginRemovalTarget{}, fmt.Errorf("未在已配置目录中找到插件 %s", pluginID)
}

// pluginManifestMatchesID 只读取清单身份字段；卸载不应因无关的运行时字段缺失而失败。
func pluginManifestMatchesID(root, pluginID string) bool {
	for _, filename := range []string{pluginpkg.ManifestYAML, pluginpkg.ManifestJSON, pluginpkg.LegacyManifestYAML, pluginpkg.LegacyManifestJSON} {
		data, err := os.ReadFile(filepath.Join(root, filename))
		if err != nil {
			continue
		}
		var manifest struct {
			ID       string `yaml:"id"`
			Identity string `yaml:"identity"`
		}
		if err := yaml.Unmarshal(data, &manifest); err != nil {
			continue
		}
		manifestID := firstNonEmpty(manifest.ID, manifest.Identity)
		if strings.EqualFold(strings.TrimSpace(manifestID), pluginID) {
			return true
		}
	}
	return false
}

// removeConfiguredPluginDir 仅将源码目录从 Hub 配置中脱离，不删除用户克隆的仓库。
func removeConfiguredPluginDir(cfg *bridgeconfig.Config, root string) []string {
	if cfg == nil || len(cfg.Bridge.PluginDirs) == 0 {
		return nil
	}
	kept := make([]string, 0, len(cfg.Bridge.PluginDirs))
	removed := make([]string, 0, 1)
	for _, configuredRoot := range cfg.Bridge.PluginDirs {
		resolvedRoot, err := expandPath(configuredRoot)
		if err == nil && samePluginPath(resolvedRoot, root) {
			removed = append(removed, configuredRoot)
			continue
		}
		kept = append(kept, configuredRoot)
	}
	cfg.Bridge.PluginDirs = kept
	return removed
}

func samePluginPath(left, right string) bool {
	return filepath.Clean(strings.TrimSpace(left)) == filepath.Clean(strings.TrimSpace(right))
}

func looksLikeGoPluginSource(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, pluginpkg.ManifestYAML)); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(dir, "cmd", "*-adapter", "main.go"))
	return err == nil && len(matches) > 0
}

func looksLikeNodePluginSource(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, pluginpkg.ManifestYAML)); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, "package.json")); err != nil {
		return false
	}
	manifest, err := pluginpkg.LoadManifest(filepath.Join(dir, pluginpkg.ManifestYAML))
	if err != nil {
		return false
	}
	if len(manifest.Command) == 0 {
		return false
	}
	command := strings.TrimSpace(manifest.Command[0])
	return filepath.Base(command) == "node" || command == "${runtime.node}"
}

func activatePluginConfig(cfg *bridgeconfig.Config, configPath, root string, manifest pluginpkg.Manifest) error {
	if cfg.Bridge.Plugins == nil {
		cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	}
	pluginID := strings.ToLower(strings.TrimSpace(manifest.ID))
	item := cfg.Bridge.Plugins[pluginID]
	item.Source = "plugin"
	item.Mode = manifest.RuntimeMode(item.Mode)
	item.VisibilityRequired = true
	cfg.Bridge.Plugins[pluginID] = item
	if !containsPath(pluginDirs(cfg), root) {
		cfg.Bridge.PluginDirs = append(cfg.Bridge.PluginDirs, root)
	}
	return cfg.Save(configPath)
}

func buildGoPluginBinary(pluginRoot string, manifest pluginpkg.Manifest, stderr io.Writer) error {
	binDir := filepath.Join(pluginRoot, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return fmt.Errorf("create plugin bin dir: %w", err)
	}
	output, err := goPluginBinaryPath(pluginRoot, manifest)
	if err != nil {
		return err
	}
	pkg, err := goPluginBuildPackage(pluginRoot)
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-o", output, pkg)
	cmd.Dir = pluginRoot
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build go plugin binary: %w", err)
	}
	return nil
}

func rewriteInstalledGoPluginManifest(pluginRoot string, manifest pluginpkg.Manifest) error {
	manifestPath := filepath.Join(pluginRoot, pluginpkg.ManifestYAML)
	loaded, err := pluginpkg.LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	output, err := goPluginBinaryPath(pluginRoot, manifest)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(pluginRoot, output)
	if err != nil {
		return fmt.Errorf("resolve installed go plugin binary: %w", err)
	}
	loaded.Command = []string{"./" + filepath.ToSlash(rel)}
	data, err := yaml.Marshal(&loaded)
	if err != nil {
		return fmt.Errorf("marshal installed go plugin manifest: %w", err)
	}
	return os.WriteFile(manifestPath, data, 0o600)
}

func installedPluginBinaryPath(pluginRoot string, manifest pluginpkg.Manifest) string {
	path, err := binaryPathFromManifest(pluginRoot, manifest)
	if looksLikeGoPluginSource(pluginRoot) {
		path, err = goPluginBinaryPath(pluginRoot, manifest)
	}
	if err != nil {
		return ""
	}
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return path
	}
	return ""
}

func binaryPathFromManifest(pluginRoot string, manifest pluginpkg.Manifest) (string, error) {
	command := manifest.Command
	if len(command) == 0 {
		return "", fmt.Errorf("plugin manifest command required")
	}
	binary := strings.TrimSpace(command[0])
	if binary == "" {
		return "", fmt.Errorf("plugin manifest command[0] required")
	}
	if strings.HasPrefix(binary, "./") {
		path := filepath.Join(pluginRoot, strings.TrimPrefix(binary, "./"))
		if runtime.GOOS == "windows" && filepath.Ext(path) == "" {
			path += ".exe"
		}
		return path, nil
	}
	if filepath.IsAbs(binary) {
		return binary, nil
	}
	return filepath.Join(pluginRoot, "bin", filepath.Base(binary)), nil
}

func goPluginBinaryPath(pluginRoot string, manifest pluginpkg.Manifest) (string, error) {
	if len(manifest.Command) > 0 {
		first := strings.TrimSpace(manifest.Command[0])
		if strings.HasPrefix(first, "./bin/") || strings.HasPrefix(first, ".\\bin\\") {
			return binaryPathFromManifest(pluginRoot, manifest)
		}
	}
	pkg, err := goPluginBuildPackage(pluginRoot)
	if err != nil {
		return "", err
	}
	name := filepath.Base(strings.TrimPrefix(pkg, "./"))
	if strings.TrimSpace(name) == "" || name == "." {
		return "", fmt.Errorf("resolve go plugin binary name from package %q", pkg)
	}
	path := filepath.Join(pluginRoot, "bin", name)
	if runtime.GOOS == "windows" && filepath.Ext(path) == "" {
		path += ".exe"
	}
	return path, nil
}

func goPluginBuildPackage(pluginRoot string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(pluginRoot, "cmd", "*-adapter", "main.go"))
	if err != nil {
		return "", fmt.Errorf("discover go plugin main package: %w", err)
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("expected exactly one go plugin adapter entry under cmd/*-adapter, got %d", len(matches))
	}
	dir := filepath.Dir(matches[0])
	rel, err := filepath.Rel(pluginRoot, dir)
	if err != nil {
		return "", fmt.Errorf("resolve go plugin package: %w", err)
	}
	return "./" + filepath.ToSlash(rel), nil
}

func installNodePluginDependencies(versionDir string) error {
	installCommand := "install"
	if _, err := os.Stat(filepath.Join(versionDir, "package-lock.json")); err == nil {
		installCommand = "ci"
	}
	npm := "npm"
	nodeManager := runtimes.NewNodeManager("", nil)
	// The managed Node distribution keeps its executables at the archive root
	// on Windows (node.exe, npm.cmd, npx.cmd) and under bin/ on Unix. Resolve
	// the managed npm with the platform-correct layout so a desktop Hub that
	// lacks a system npm can still install plugin dependencies.
	nodeBinDir := nodeManager.Dir()
	if runtime.GOOS != "windows" {
		nodeBinDir = filepath.Join(nodeManager.Dir(), "bin")
	}
	if runtime.GOOS == "windows" {
		if _, err := os.Stat(filepath.Join(nodeBinDir, "npm.cmd")); err == nil {
			npm = filepath.Join(nodeBinDir, "npm.cmd")
		}
	} else if _, err := os.Stat(filepath.Join(nodeBinDir, "npm")); err == nil {
		npm = filepath.Join(nodeBinDir, "npm")
	}
	// 桌面 Hub 的启动环境通常没有系统 npm，优先使用 Hub 管理的 Node runtime。
	install := exec.Command(npm, installCommand, "--registry=https://registry.npmmirror.com")
	install.Env = prependPath(os.Environ(), nodeBinDir)
	install.Dir = versionDir
	if output, err := install.CombinedOutput(); err != nil {
		return fmt.Errorf("npm %s: %w: %s", installCommand, err, strings.TrimSpace(string(output)))
	}
	build := exec.Command(npm, "run", "build", "--if-present")
	build.Env = prependPath(os.Environ(), nodeBinDir)
	build.Dir = versionDir
	if output, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("npm build: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func prependPath(env []string, dir string) []string {
	pathValue := dir
	for _, value := range env {
		if strings.HasPrefix(value, "PATH=") {
			pathValue += string(os.PathListSeparator) + strings.TrimPrefix(value, "PATH=")
			break
		}
	}
	result := make([]string, 0, len(env)+1)
	for _, value := range env {
		if !strings.HasPrefix(value, "PATH=") {
			result = append(result, value)
		}
	}
	return append(result, "PATH="+pathValue)
}

func copyCurrentPluginVersion(src, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	return copyDir(src, dst)
}

func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		b, err := os.ReadFile(srcPath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dstPath, b, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func containsPath(paths []string, want string) bool {
	want = filepath.Clean(strings.TrimSpace(want))
	for _, path := range paths {
		if filepath.Clean(strings.TrimSpace(path)) == want {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func looksLikeRemotePluginSource(source string) bool {
	parsed, err := neturl.Parse(strings.TrimSpace(source))
	if err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
	case "http", "https":
		return true
	default:
		return false
	}
}

func materializePluginSource(source string, stderr io.Writer) (string, func(), error) {
	parsed, err := neturl.Parse(strings.TrimSpace(source))
	if err != nil {
		return "", func() {}, err
	}
	workDir, err := os.MkdirTemp("", "prism-plugin-source-*")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(workDir) }
	lowerPath := strings.ToLower(parsed.Path)
	if strings.HasSuffix(lowerPath, ".zip") {
		archivePath := filepath.Join(workDir, "plugin.zip")
		if err := downloadPluginSource(source, archivePath); err != nil {
			cleanup()
			return "", func() {}, err
		}
		extractDir := filepath.Join(workDir, "extract")
		if err := unzipPluginArchive(archivePath, extractDir); err != nil {
			cleanup()
			return "", func() {}, err
		}
		root, err := locatePluginSourceRoot(extractDir)
		if err != nil {
			cleanup()
			return "", func() {}, err
		}
		return root, cleanup, nil
	}
	if strings.HasSuffix(lowerPath, ".tar.gz") || strings.HasSuffix(lowerPath, ".tgz") {
		archivePath := filepath.Join(workDir, "plugin.tgz")
		if err := downloadPluginSource(source, archivePath); err != nil {
			cleanup()
			return "", func() {}, err
		}
		extractDir := filepath.Join(workDir, "extract")
		if err := untarGzipPluginArchive(archivePath, extractDir); err != nil {
			cleanup()
			return "", func() {}, err
		}
		root, err := locatePluginSourceRoot(extractDir)
		if err != nil {
			cleanup()
			return "", func() {}, err
		}
		return root, cleanup, nil
	}
	cloneDir := filepath.Join(workDir, "repo")
	cmd := exec.Command("git", "clone", "--depth", "1", source, cloneDir)
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("git clone %s: %w", source, err)
	}
	root, err := locatePluginSourceRoot(cloneDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	return root, cleanup, nil
}

func downloadPluginSource(source, target string) error {
	resp, err := http.Get(source)
	if err != nil {
		return fmt.Errorf("download %s: %w", source, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download %s: http %s", source, resp.Status)
	}
	file, err := os.Create(target)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := io.Copy(file, resp.Body); err != nil {
		return err
	}
	return nil
}

func unzipPluginArchive(archivePath, dst string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, file := range reader.File {
		target := filepath.Join(dst, file.Name)
		if !strings.HasPrefix(target, filepath.Clean(dst)+string(os.PathSeparator)) && filepath.Clean(target) != filepath.Clean(dst) {
			return fmt.Errorf("zip entry escapes destination: %s", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := file.Open()
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			return readErr
		}
		mode := file.Mode()
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(target, data, mode); err != nil {
			return err
		}
	}
	return nil
}

func untarGzipPluginArchive(archivePath, dst string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	reader := tar.NewReader(gzr)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dst, header.Name)
		if !strings.HasPrefix(target, filepath.Clean(dst)+string(os.PathSeparator)) && filepath.Clean(target) != filepath.Clean(dst) {
			return fmt.Errorf("tar entry escapes destination: %s", header.Name)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			data, readErr := io.ReadAll(reader)
			if readErr != nil {
				return readErr
			}
			mode := os.FileMode(header.Mode)
			if mode == 0 {
				mode = 0o644
			}
			if err := os.WriteFile(target, data, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

func locatePluginSourceRoot(root string) (string, error) {
	if _, err := manifestInDir(root); err == nil {
		return root, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	var candidates []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		child := filepath.Join(root, entry.Name())
		if _, err := manifestInDir(child); err == nil {
			candidates = append(candidates, child)
		}
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	discovered, err := pluginpkg.ScanDirs([]string{root})
	if err == nil && len(discovered) == 1 {
		return discovered[0].Root, nil
	}
	return "", fmt.Errorf("plugin manifest not found in extracted source %s", root)
}

// bridgePluginTestPayload is the repository's direct Plugin conformance
// helper. It is intentionally not exposed from the Desktop sidecar binary.
func bridgePluginTestPayload(ctx context.Context, path, manifestPath, conversationID, message, sessionCwd string, stderr io.Writer) (map[string]any, int) {
	result := map[string]any{
		"ok":         false,
		"checked_at": time.Now().UTC().Format(time.RFC3339),
	}
	var steps []pluginTestStep
	// visibilityDB holds an optional open Prism DB so the visibility check can
	// be persisted (mirroring `bridge test`). nil when the DB is unavailable.
	var visibilityDB *prismdb.DB
	var visibilitySessionID string
	addStep := func(name string, ok bool, detail string, err error) {
		step := pluginTestStep{Name: name, OK: ok, Detail: strings.TrimSpace(detail)}
		if err != nil {
			step.Error = err.Error()
		}
		steps = append(steps, step)
	}
	fail := func(code int, err error) (map[string]any, int) {
		if err != nil {
			result["error"] = err.Error()
			fmt.Fprintf(stderr, "%v\n", err)
		}
		result["steps"] = steps
		return result, code
	}
	discovered, err := resolvePluginForTest(path, manifestPath)
	if err != nil {
		addStep("manifest", false, "", err)
		return fail(1, err)
	}
	addStep("manifest", true, discovered.Path, nil)
	result["plugin_id"] = discovered.Manifest.ID
	result["manifest_path"] = discovered.Path
	result["plugin_root"] = discovered.Root
	cfg, _ := bridgeconfig.Load(envOr("PRISM_CONFIG", bridgeconfig.DefaultPath()))
	adapter := pluginpkg.NewProcessAdapterWithEnv(discovered, bridgePluginEnv(cfg, discovered))
	defer adapter.Close(context.Background())

	capability, err := adapter.Probe(ctx)
	if err != nil {
		addStep("probe", false, "", err)
		return fail(1, err)
	}
	result["capability"] = capability
	if !capability.Available {
		err := fmt.Errorf("adapter unavailable: %s", strings.TrimSpace(capability.UnavailableReason))
		addStep("probe", false, capability.UnavailableReason, err)
		return fail(1, err)
	}
	addStep("probe", true, capability.VisibilitySurface, nil)

	discovery, err := adapter.Discover(ctx)
	if err != nil {
		addStep("discover", false, "", err)
		return fail(1, err)
	}
	result["discovery"] = discovery
	if !discovery.Verified {
		err := fmt.Errorf("adapter discovery not verified: %s", strings.TrimSpace(discovery.Detail))
		addStep("discover", false, discovery.Detail, err)
		return fail(1, err)
	}
	addStep("discover", true, firstNonEmpty(discovery.Endpoint, discovery.Surface, discovery.Detail), nil)

	marker := strings.TrimSpace(message)
	if marker == "" {
		marker = fmt.Sprintf("PRISM_PLUGIN_TEST_%s", strings.ReplaceAll(time.Now().UTC().Format("20060102T150405.000000000Z"), ".", "_"))
	}
	// A conformance run always validates the only supported new-conversation
	// entry point: startSessionWithMessage. It intentionally does not reuse the
	// CLI --conversation value to create or infer a native target.
	if db, dbErr := prismdb.Open(envOr("PRISM_DB", prismdb.DefaultPath())); dbErr == nil {
		visibilityDB = db
		defer func() { _ = db.Close() }()
	}
	messageID := "bridge-plugin-test-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	started, err := adapter.StartSessionWithMessage(ctx, pluginruntime.StartSessionWithMessageRequest{
		PluginID: discovered.Manifest.ID,
		Cwd:      sessionCwd,
		Message: pluginruntime.InboundMessage{
			PrismMessageID: messageID,
			Text:           marker,
			SourceDevice:   "bridge-plugin-test",
			Timestamp:      time.Now().UTC(),
		},
		SourceDevice: "bridge-plugin-test",
	})
	if err != nil {
		addStep("startSessionWithMessage", false, "", err)
		return fail(1, err)
	}
	session := started.Session
	receipt := started.Receipt
	visibility := started.Visibility
	if strings.TrimSpace(session.NativeSessionID) == "" && strings.TrimSpace(session.NativeThreadID) == "" {
		err := fmt.Errorf("startSessionWithMessage returned no native identity")
		addStep("startSessionWithMessage", false, "", err)
		return fail(1, err)
	}
	if !receipt.Accepted || !visibility.Visible {
		err := fmt.Errorf("startSessionWithMessage did not return an accepted visible first message")
		addStep("startSessionWithMessage", false, receipt.Detail, err)
		return fail(1, err)
	}
	result["native_session_id"] = session.NativeSessionID
	result["native_thread_id"] = session.NativeThreadID
	result["native_message_id"] = receipt.NativeMessageID
	result["accepted"] = receipt.Accepted
	result["visible"] = visibility.Visible
	result["evidence"] = visibility.Evidence
	result["failure_reason"] = visibility.FailureReason
	addStep("startSessionWithMessage", true, firstNonEmpty(receipt.Detail, session.NativeSessionID), nil)

	// Persist the exact same binding that production start uses when the local
	// DB is available; the test never fabricates a CLI conversation identity.
	if visibilityDB != nil {
		operation, beginErr := visibilityDB.BeginStartOperation(ctx, discovered.Manifest.ID, messageID, messageID)
		if beginErr != nil {
			addStep("persistStartBinding", false, "", beginErr)
			return fail(1, beginErr)
		}
		_, record, completeErr := visibilityDB.CompleteStartOperation(ctx, operation, prismdb.SessionRecord{
			PluginID:           discovered.Manifest.ID,
			NativeSessionID:    session.NativeSessionID,
			NativeThreadID:     session.NativeThreadID,
			Surface:            session.Surface,
			Endpoint:           session.Endpoint,
			Cwd:                session.Cwd,
			VisibilityVerified: true,
			VisibilityEvidence: visibility.Evidence,
		}, receipt.NativeMessageID)
		if completeErr != nil {
			addStep("persistStartBinding", false, "", completeErr)
			return fail(1, completeErr)
		}
		conversationID = record.PrismConversationID
		visibilitySessionID = record.ID
		addStep("persistStartBinding", true, conversationID, nil)
	}

	events, subscribeErr := adapter.Subscribe(ctx, session)
	if subscribeErr != nil {
		addStep("subscribe", true, "optional subscribe unavailable: "+subscribeErr.Error(), nil)
	} else {
		addStep("subscribe", true, "subscribed", nil)
	}

	// Persist the verification observation produced by the start operation.
	if visibilityDB != nil && visibilitySessionID != "" && marker != "" {
		_, _ = visibilityDB.InsertVisibilityCheck(ctx, prismdb.VisibilityCheckRecord{
			PluginID:      discovered.Manifest.ID,
			SessionID:     visibilitySessionID,
			Marker:        marker,
			Visible:       visibility.Visible,
			Evidence:      visibility.Evidence,
			FailureReason: visibility.FailureReason,
			CheckedAt:     visibility.CheckedAt,
		})
	}
	addStep("verifyVisibility", true, firstNonEmpty(visibility.Evidence, visibility.Marker), nil)

	if subscribeErr == nil && events != nil {
		select {
		case event, ok := <-events:
			if ok {
				result["subscription_event"] = event
				addStep("subscriptionEvent", true, firstNonEmpty(event.Status, event.Type, event.Summary), nil)
			} else {
				addStep("subscriptionEvent", true, "event stream closed", nil)
			}
		case <-time.After(300 * time.Millisecond):
			addStep("subscriptionEvent", true, "no immediate event", nil)
		case <-ctx.Done():
			addStep("subscriptionEvent", false, "", ctx.Err())
			return fail(1, ctx.Err())
		}
	}

	if receipt.NativeMessageID == "" {
		addStep("waitForRun", true, "skipped: send returned no native message id", nil)
	} else {
		if waiter, ok := any(adapter).(pluginruntime.RunWaiter); ok {
			runEvent, waitErr := waiter.WaitForRun(ctx, session, receipt.NativeMessageID)
			if waitErr != nil {
				result["run_wait_error"] = waitErr.Error()
				addStep("waitForRun", true, "run wait unavailable after visibility: "+waitErr.Error(), nil)
			}
			if waitErr == nil {
				result["run_status"] = runEvent.Status
				result["run_summary"] = runEvent.Summary
				addStep("waitForRun", true, firstNonEmpty(runEvent.Status, runEvent.Summary), nil)
			}
		} else {
			addStep("waitForRun", true, "skipped: adapter does not implement RunWaiter", nil)
		}
	}

	closeErr := adapter.Close(ctx)
	addStep("close", closeErr == nil, "", closeErr)
	if closeErr != nil {
		return fail(1, closeErr)
	}
	result["steps"] = steps
	result["ok"] = true
	return result, 0
}

func resolvePluginForTest(path, manifestPath string) (pluginpkg.Discovered, error) {
	path = strings.TrimSpace(path)
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		if path == "" {
			return pluginpkg.Discovered{}, fmt.Errorf("plugin path or manifest required")
		}
		resolved, err := expandExistingPath(path)
		if err != nil {
			return pluginpkg.Discovered{}, err
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return pluginpkg.Discovered{}, fmt.Errorf("stat plugin path %s: %w", resolved, err)
		}
		if info.IsDir() {
			manifestPath, err = manifestInDir(resolved)
			if err != nil {
				return pluginpkg.Discovered{}, err
			}
		} else {
			manifestPath = resolved
		}
	}
	resolvedManifest, err := expandExistingPath(manifestPath)
	if err != nil {
		return pluginpkg.Discovered{}, err
	}
	manifest, err := pluginpkg.LoadManifest(resolvedManifest)
	if err != nil {
		return pluginpkg.Discovered{}, err
	}
	return pluginpkg.Discovered{
		Path:     resolvedManifest,
		Root:     filepath.Dir(resolvedManifest),
		Manifest: manifest,
	}, nil
}

func manifestInDir(dir string) (string, error) {
	candidates := []string{
		filepath.Join(dir, pluginpkg.ManifestYAML),
		filepath.Join(dir, pluginpkg.ManifestJSON),
		filepath.Join(dir, "current", pluginpkg.ManifestYAML),
		filepath.Join(dir, "current", pluginpkg.ManifestJSON),
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("plugin manifest not found in %s", dir)
}

func expandPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("path required")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return filepath.Abs(path)
}

func expandExistingPath(path string) (string, error) {
	resolved, err := expandPath(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(resolved); err == nil {
		return resolved, nil
	}
	if filepath.IsAbs(strings.TrimSpace(path)) {
		return resolved, nil
	}
	return resolved, nil
}
