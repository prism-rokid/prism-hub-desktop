package plugin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestLoadManifestNormalizesAndValidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: " Demo.Plugin "
display_name: "Demo Plugin"
version: "0.1.0"
integration_mode: " protocol-native "
transport: "process"
command: ["demo-pluginbridge-plugin", "--stdio"]
capabilities:
  native_visible_input: true
  native_visible_output: true
  can_forward_sync: true
  can_reverse_sync: true
  can_start_session_with_message: true
  can_open_draft: true
  can_plugin_wide_watch: true
managed_launch: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if manifest.ID != "demo.plugin" || manifest.Transport != "process" {
		t.Fatalf("manifest not normalized: %+v", manifest)
	}
	if manifest.IntegrationMode != "protocol-native" {
		t.Fatalf("integration mode = %q, want protocol-native", manifest.IntegrationMode)
	}
	if got := manifest.RuntimeModes; len(got) != 1 || got[0] != "process" || manifest.DefaultRuntimeMode != "process" {
		t.Fatalf("legacy mode must become the universal runtime default: %+v", manifest)
	}
	if !manifest.ManagedLaunch {
		t.Fatalf("managed_launch should be parsed: %+v", manifest)
	}
	capability := manifest.Capability()
	if capability.Available || !capability.NativeVisibleInput || capability.UnavailableReason != "plugin_runtime_not_started" {
		t.Fatalf("unexpected capability: %+v", capability)
	}
	if !capability.CanPluginWideWatch {
		t.Fatalf("plugin-wide watch capability was not preserved: %+v", capability)
	}
	if !capability.CanOpenDraft {
		t.Fatalf("open-draft capability was not preserved: %+v", capability)
	}
	if capability.IntegrationMode != pluginruntime.IntegrationModeProtocolNative {
		t.Fatalf("capability integration mode = %q", capability.IntegrationMode)
	}
}

func TestManifestIconContractAndResolver(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	base := `id: demo
version: 1.0.0
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}
`
	runtimeBase := strings.Replace(base, "command: [demo]", "command: ['${runtime.node}', demo]", 1)
	if err := os.WriteFile(path, []byte(runtimeBase+"schema_version: 1\nicon_svg_url: https://example.test/logo.svg\nicon_url: https://example.test/logo.webp\nruntime_dependencies:\n  node: '>=22.0.0 <23.0.0'\nplugin_protocol_version: '4'\nmin_hub_version: 0.1.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.IconSVGURL == "" || m.RuntimeDependencies.Node == "" || m.ProtocolVersion != "4" || m.SchemaVersion != 1 {
		t.Fatalf("new manifest fields lost: %#v", m)
	}
	primary, fallback := m.ResolvedIconURLs("")
	if primary != m.IconSVGURL || fallback != m.IconURL {
		t.Fatalf("SVG fallback chain = %q, %q", primary, fallback)
	}
	primary, fallback = m.ResolvedIconURLs("https://user.example/icon.webp")
	if primary != "https://user.example/icon.webp" || fallback != m.IconSVGURL {
		t.Fatalf("user icon must win: %q, %q", primary, fallback)
	}
	m.IconURL, m.IconSVGURL = "", ""
	primary, _ = m.ResolvedIconURLs("")
	if primary != DefaultIconURL {
		t.Fatalf("default icon = %q", primary)
	}
	for _, value := range []string{"http://example.test/x", "data:image/svg+xml,x", "https://user@example.test/x", "file:///tmp/x", "https://example.test/" + strings.Repeat("a", 1025)} {
		if ValidateHTTPSURL(value) == nil {
			t.Fatalf("unsafe icon accepted: %q", value)
		}
	}
	if err := os.WriteFile(path, []byte(base+"icon_url: http://example.test/icon.webp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path); err == nil {
		t.Fatal("HTTP manifest icon accepted")
	}
	if err := os.WriteFile(path, []byte(base+"schema_version: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path); err == nil {
		t.Fatal("unsupported schema version accepted")
	}
}

func TestLoadManifestRejectsRemovedFamilyID(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo
family_id: legacy-demo
integration_mode: protocol-native
transport: process
command: ["demo"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path); err == nil {
		t.Fatal("LoadManifest accepted removed family_id")
	}
}

func TestLoadManifestNormalizesRuntimeModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [" Desktop ", acp, desktop]
default_runtime_mode: ACP
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if got := manifest.RuntimeModes; len(got) != 2 || got[0] != "desktop" || got[1] != "acp" {
		t.Fatalf("runtime_modes = %#v", got)
	}
	if got := manifest.RuntimeMode("missing"); got != "acp" {
		t.Fatalf("runtime fallback = %q", got)
	}
	if !manifest.AllowsRuntimeMode("desktop") || manifest.AllowsRuntimeMode("missing") {
		t.Fatalf("runtime declaration validation incorrect: %+v", manifest)
	}
}

func TestLoadManifestValidatesManagedTerminalModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [native]
managed_terminal_modes: [desktop]
capabilities:
  can_open_managed_terminal: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path); err == nil {
		t.Fatal("LoadManifest accepted a managed terminal mode outside runtime_modes")
	}
}

func TestLoadManifestNormalizesAndValidatesRuntimeControls(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [desktop, native]
default_runtime_mode: desktop
runtime_controls:
  Desktop:
    model: INTERACTIVE
    reasoning: interactive
  native:
    model: stable
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	desktop := manifest.RuntimeControlProfile("desktop")
	if desktop.Model != pluginruntime.RuntimeControlInteractive || desktop.Reasoning != pluginruntime.RuntimeControlInteractive {
		t.Fatalf("desktop runtime controls not normalized: %+v", desktop)
	}
	if native := manifest.RuntimeControlProfile("native"); native.Model != pluginruntime.RuntimeControlStable {
		t.Fatalf("native runtime controls not normalized: %+v", native)
	}

	invalidPath := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(invalidPath, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [native]
runtime_controls:
  native:
    model: both
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(invalidPath); err == nil {
		t.Fatal("LoadManifest accepted simultaneous/unknown runtime control implementation")
	}
}

func TestLoadManifestNormalizesAndValidatesRuntimeOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [desktop, native]
default_runtime_mode: desktop
runtime_operations:
  Desktop:
    rename: MENU
    pin: direct
  native:
    rename: unsupported
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	desktop := manifest.RuntimeOperationProfile("desktop")
	if desktop.Rename != pluginruntime.RuntimeOperationMenu || desktop.Pin != pluginruntime.RuntimeOperationDirect {
		t.Fatalf("desktop runtime operations not normalized: %+v", desktop)
	}
	if native := manifest.RuntimeOperationProfile("native"); native.Rename != pluginruntime.RuntimeOperationUnsupported {
		t.Fatalf("native runtime operations not normalized: %+v", native)
	}

	invalidPath := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(invalidPath, []byte(`
id: demo
integration_mode: protocol-native
transport: process
command: ["demo"]
runtime_modes: [native]
runtime_operations:
  native:
    delete: automatic
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(invalidPath); err == nil {
		t.Fatal("LoadManifest accepted unknown runtime operation implementation")
	}
}

func TestScanDirsFindsPluginManifests(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins")
	itemDir := filepath.Join(pluginDir, "demo")
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(itemDir, ManifestJSON), []byte(`{
  "id": "demo",
  "integration_mode": "desktop-automation",
  "transport": "process",
  "command": ["demo"],
  "capabilities": {"native_visible_input": true}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := ScanDirs([]string{pluginDir})
	if err != nil {
		t.Fatalf("ScanDirs: %v", err)
	}
	if len(items) != 1 || items[0].Manifest.ID != "demo" || items[0].Path == "" {
		t.Fatalf("unexpected discovered plugins: %+v", items)
	}
	if items[0].Manifest.IntegrationMode != "desktop-automation" {
		t.Fatalf("integration mode = %q", items[0].Manifest.IntegrationMode)
	}
}

func TestScanDirsFindsStateVersionManifest(t *testing.T) {
	root := t.TempDir()
	versionRoot := filepath.Join(root, "demo", "versions", "1.0.0")
	if err := os.MkdirAll(versionRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "demo", "state.json"), []byte(`{"active_version":"1.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(versionRoot, ManifestYAML), []byte(`id: demo
version: 1.0.0
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := ScanDirs([]string{root})
	if err != nil || len(items) != 1 || items[0].Root != versionRoot {
		t.Fatalf("state scan = %#v, %v", items, err)
	}
}

func TestScanDirsAcceptsDirectPluginDir(t *testing.T) {
	pluginDir := filepath.Join(t.TempDir(), "demo.plugin")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(`
id: demo.plugin
integration_mode: protocol-native
transport: process
command: ["./bin/demo"]
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := ScanDirs([]string{pluginDir})
	if err != nil {
		t.Fatalf("ScanDirs: %v", err)
	}
	if len(items) != 1 || items[0].Manifest.ID != "demo.plugin" {
		t.Fatalf("unexpected discovered plugins: %+v", items)
	}
	if items[0].Root != pluginDir {
		t.Fatalf("expected plugin root to be direct plugin dir, got %s", items[0].Root)
	}
}

func TestScanDirsFindsCurrentVersionManifests(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins")
	currentDir := filepath.Join(pluginDir, "demo.plugin", "current")
	if err := os.MkdirAll(currentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(currentDir, ManifestYAML), []byte(`
id: demo.plugin
integration_mode: protocol-native
transport: process
command: ["./bin/demo"]
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := ScanDirs([]string{pluginDir})
	if err != nil {
		t.Fatalf("ScanDirs: %v", err)
	}
	if len(items) != 1 || items[0].Manifest.ID != "demo.plugin" {
		t.Fatalf("unexpected discovered plugins: %+v", items)
	}
	if items[0].Root != currentDir {
		t.Fatalf("expected plugin root to be current dir, got %s", items[0].Root)
	}
}

func TestLoadManifestDefaultsIntegrationModeFromLegacyMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: desktop.demo
transport: process
mode: desktop
command: ["./bin/demo"]
capabilities:
  native_visible_input: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if manifest.IntegrationMode != "desktop-automation" {
		t.Fatalf("integration mode = %q, want desktop-automation", manifest.IntegrationMode)
	}
}

func TestLoadManifestAcceptsLegacyIdentityField(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: demo.plugin
agent_id: demo
integration_mode: protocol-native
transport: process
command: ["./bin/demo"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("expected legacy agent_id manifest to be normalized, got error: %v", err)
	}
	if manifest.ID != "demo.plugin" {
		t.Fatalf("expected id to take precedence over legacy agent_id, got id=%q", manifest.ID)
	}
}

func TestLoadManifestDropsRemovedCapabilityFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), LegacyManifestYAML)
	if err := os.WriteFile(path, []byte(`
id: legacy.plugin
integration_mode: protocol-native
transport: process
command: ["./bin/legacy"]
capabilities:
  can_create_session: true
  can_resume_session: true
  can_list_sessions: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("expected legacy manifest with removed capability fields to be normalized, got error: %v", err)
	}
	if manifest.ID != "legacy.plugin" {
		t.Fatalf("expected id from legacy manifest, got %q", manifest.ID)
	}
	if !manifest.Capabilities.CanListSessions {
		t.Fatalf("expected preserved can_list_sessions capability to stay true")
	}
}

func TestManifestPathInRootFindsLegacyFilename(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, LegacyManifestYAML), []byte("id: x\ntransport: process\ncommand: [\"./x\"]\nintegration_mode: protocol-native\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path, manifestRoot := manifestPathInRoot(root)
	if path == "" {
		t.Fatal("expected legacy manifest filename to be discovered")
	}
	if manifestRoot != root {
		t.Fatalf("expected manifest root to equal input root, got %q", manifestRoot)
	}
}

func TestScanDirsFindsLegacyManifestUnderCurrentDirectory(t *testing.T) {
	root := t.TempDir()
	currentDir := filepath.Join(root, "legacy.plugin", "current")
	if err := os.MkdirAll(currentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(currentDir, LegacyManifestYAML), []byte(`
agent_id: legacy.plugin
transport: process
command: ["./legacy"]
mode: desktop
capabilities:
  can_forward_sync: true
  can_create_session: true
  can_resume_session: true
  can_start_session_with_message: true
`), 0o600); err != nil {
		t.Fatal(err)
	}

	items, err := ScanDirs([]string{filepath.Join(root, "missing"), root})
	if err != nil {
		t.Fatalf("ScanDirs: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one legacy plugin, got %+v", items)
	}
	if items[0].Manifest.ID != "legacy.plugin" {
		t.Fatalf("legacy agent_id was not normalized: %+v", items[0].Manifest)
	}
	if items[0].Root != currentDir || filepath.Base(items[0].Path) != LegacyManifestYAML {
		t.Fatalf("unexpected legacy discovery location: %+v", items[0])
	}
}
