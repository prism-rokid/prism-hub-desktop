package scaffold

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	pluginpkg "rokid-prism-hub/internal/plugin"
)

func TestGenerateGoTemplateProducesValidManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "acme.tool")
	res, err := Generate(dir, Options{PluginID: "acme.tool", Template: GoTemplate})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	// manifest written and loadable
	m, err := pluginpkg.LoadManifest(res.Manifest)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if m.ID != "acme.tool" {
		t.Fatalf("manifest id = %q, want acme.tool", m.ID)
	}
	if m.IntegrationMode != "protocol-native" {
		t.Fatalf("integration mode = %q, want protocol-native", m.IntegrationMode)
	}
	if len(m.Command) == 0 || !strings.Contains(m.Command[0], "acme.tool-adapter") {
		t.Fatalf("command = %v, want acme.tool-adapter binary", m.Command)
	}
	if !m.DeclaresRuntimeControls(m.DefaultRuntimeMode) {
		t.Fatalf("scaffold manifest must explicitly declare runtime controls: %+v", m)
	}
	if !m.DeclaresRuntimeOperations(m.DefaultRuntimeMode) {
		t.Fatalf("scaffold manifest must explicitly declare runtime operations: %+v", m)
	}
	// go.mod + main.go present
	for _, want := range []string{"go.mod", "pluginbridge-plugin.yaml"} {
		if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
			t.Fatalf("expected %s: %v", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "cmd", "acme.tool-adapter", "main.go")); err != nil {
		t.Fatalf("expected main.go: %v", err)
	}
}

func TestGenerateGoTemplateCompiles(t *testing.T) {
	// The scaffolded adapter must compile from the formally versioned external
	// SDK module; the Hub repository intentionally contains no SDK copy.
	dir := filepath.Join(t.TempDir(), "acme.tool")
	if _, err := Generate(dir, Options{PluginID: "acme.tool", Template: GoTemplate}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	// Resolve the uuid dependency into a go.sum, then build.
	tidy := exec.Command("go", "mod", "tidy")
	tidy.Dir = dir
	tidy.Env = append(os.Environ(), "GOPROXY=https://goproxy.cn,direct", "GONOSUMDB=github.com/Rokid-Prism/*")
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy failed: %v\n%s", err, out)
	}
	build := exec.Command("go", "build", "-o", filepath.Join(dir, "bin", "acme.tool-adapter"), "./cmd/acme.tool-adapter")
	build.Dir = dir
	build.Env = append(os.Environ(), "GOPROXY=https://goproxy.cn,direct", "GONOSUMDB=github.com/Rokid-Prism/*")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build scaffolded adapter failed: %v\n%s", err, out)
	}
}

func TestGenerateNodeAndPythonTemplates(t *testing.T) {
	for _, tc := range []struct {
		name string
		tmpl Template
		want []string // expected files
	}{
		{"node", NodeTemplate, []string{"index.js", "package.json", "pluginbridge-plugin.yaml"}},
		{"python", PythonTemplate, []string{"adapter.py", "requirements.txt", "pluginbridge-plugin.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "x.tool")
			res, err := Generate(dir, Options{PluginID: "x.tool", Template: tc.tmpl})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			m, err := pluginpkg.LoadManifest(res.Manifest)
			if err != nil {
				t.Fatalf("LoadManifest: %v", err)
			}
			if m.ID != "x.tool" {
				t.Fatalf("plugin id = %q, want x.tool", m.ID)
			}
			if m.IntegrationMode != "protocol-native" {
				t.Fatalf("integration mode = %q, want protocol-native", m.IntegrationMode)
			}
			for _, f := range tc.want {
				if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
					t.Fatalf("expected %s: %v", f, err)
				}
			}
		})
	}
}

func TestGenerateSupportsDesktopAutomationMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "desk.tool")
	res, err := Generate(dir, Options{
		PluginID:        "desk.tool",
		Template:        NodeTemplate,
		IntegrationMode: "desktop-automation",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	m, err := pluginpkg.LoadManifest(res.Manifest)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if m.IntegrationMode != "desktop-automation" {
		t.Fatalf("integration mode = %q", m.IntegrationMode)
	}
	if m.Mode != "desktop" {
		t.Fatalf("mode = %q, want desktop", m.Mode)
	}
}

func TestGenerateValidatesInputs(t *testing.T) {
	if _, err := Generate(t.TempDir(), Options{Template: GoTemplate}); err == nil {
		t.Fatal("expected error for empty plugin id")
	}
	if _, err := Generate(t.TempDir(), Options{PluginID: "x", Template: Template("nope")}); err == nil {
		t.Fatal("expected error for unknown template")
	}
}

func TestGeneratePreservesExistingFilesOnRerun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "acme.tool")
	if _, err := Generate(dir, Options{PluginID: "acme.tool", Template: GoTemplate}); err != nil {
		t.Fatal(err)
	}
	// Edit main.go to simulate user changes.
	main := filepath.Join(dir, "cmd", "acme.tool-adapter", "main.go")
	custom := "// my custom edit\n"
	if err := os.WriteFile(main, []byte(custom), 0o644); err != nil {
		t.Fatal(err)
	}
	// Re-run init: source files preserved, manifest regenerated.
	if _, err := Generate(dir, Options{PluginID: "acme.tool", Template: GoTemplate}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(main)
	if string(got) != custom {
		t.Fatalf("re-run overwrote source; got %q", got)
	}
}
