package plugin

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestReloaderRegistersUpdatesAndRemovesPlugins(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifest := func(version string) {
		t.Helper()
		body := `
id: demo
version: ` + version + `
transport: process
command: ["demo-pluginbridge-plugin"]
mode: test
capabilities:
  native_visible_input: true
`
		if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest("0.1.0")
	svc := pluginruntime.New()
	var created []*testReloadAdapter
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    svc,
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			adapter := &testReloadAdapter{id: item.Manifest.ID, version: item.Manifest.Version}
			created = append(created, adapter)
			return adapter
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if got := svc.Adapter("demo"); got == nil {
		t.Fatal("expected plugin adapter to be registered")
	}
	if len(created) != 1 || created[0].version != "0.1.0" {
		t.Fatalf("unexpected created adapters: %+v", created)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("unchanged reload: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("unchanged reload should not recreate adapter, got %d", len(created))
	}
	time.Sleep(time.Millisecond)
	writeManifest("0.2.0")
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("updated reload: %v", err)
	}
	if len(created) != 2 || created[1].version != "0.2.0" {
		t.Fatalf("expected replacement adapter, got %+v", created)
	}
	if created[0].closed != 1 {
		t.Fatalf("expected old adapter to be closed, got %d", created[0].closed)
	}
	if err := os.Remove(filepath.Join(pluginDir, ManifestYAML)); err != nil {
		t.Fatal(err)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("removed reload: %v", err)
	}
	if got := svc.Adapter("demo"); got != nil {
		t.Fatalf("expected adapter to be removed, got %#v", got)
	}
	if created[1].closed != 1 {
		t.Fatalf("expected removed adapter to be closed, got %d", created[1].closed)
	}
}

func TestReloaderRemovePluginDetachesAndClosesAdapter(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte("id: demo\ntransport: process\ncommand: [\"demo\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter := &testReloadAdapter{id: "demo"}
	svc := pluginruntime.New()
	reloader := &Reloader{
		Dirs:    []string{root},
		Service: svc,
		NewAdapter: func(Discovered) pluginruntime.PluginAdapter {
			return adapter
		},
		CloseGrace: -1,
	}
	if err := reloader.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reloader.RemovePlugin(context.Background(), "demo"); err != nil {
		t.Fatalf("RemovePlugin: %v", err)
	}
	if svc.Adapter("demo") != nil || adapter.closed != 1 {
		t.Fatalf("expected adapter detached and closed, adapter=%#v closed=%d", svc.Adapter("demo"), adapter.closed)
	}
}

func TestReloaderAdoptsInitialServiceAdapter(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(`
id: demo
version: 0.1.0
transport: process
command: ["demo-pluginbridge-plugin"]
mode: test
`), 0o600); err != nil {
		t.Fatal(err)
	}
	initial := &testReloadAdapter{id: "demo", version: "0.1.0"}
	svc := pluginruntime.New(initial)
	created := 0
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    svc,
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			created++
			return &testReloadAdapter{id: item.Manifest.ID, version: item.Manifest.Version}
		},
	}
	if err := reloader.Reload(context.Background()); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if got := svc.Adapter("demo"); got != initial {
		t.Fatalf("expected existing adapter to be adopted, got %#v", got)
	}
	if created != 0 || initial.closed != 0 {
		t.Fatalf("initial adapter should not be replaced: created=%d closed=%d", created, initial.closed)
	}
}

func TestReloaderReloadPluginReplacesOnlyRequestedRuntime(t *testing.T) {
	root := t.TempDir()
	writePlugin := func(id string) {
		t.Helper()
		dir := filepath.Join(root, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, ManifestYAML), []byte("id: "+id+"\ntransport: process\ncommand: [\"demo\"]\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePlugin("alpha")
	writePlugin("beta")
	var created []*testReloadAdapter
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    pluginruntime.New(),
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			adapter := &testReloadAdapter{id: item.Manifest.ID}
			created = append(created, adapter)
			return adapter
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	alpha, beta := created[0], created[1]
	if err := reloader.ReloadPlugin(ctx, "alpha"); err != nil {
		t.Fatalf("ReloadPlugin: %v", err)
	}
	if len(created) != 3 || created[2].id != "alpha" || alpha.closed != 1 || beta.closed != 0 || reloader.Service.Adapter("beta") != beta {
		t.Fatalf("single-plugin reload replaced unrelated runtime: created=%+v alpha_closed=%d beta_closed=%d", created, alpha.closed, beta.closed)
	}
}

func TestReloaderReloadPluginTransitionRunsBeforeOnChange(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "alpha")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte("id: alpha\ntransport: process\ncommand: [\"demo\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	initial := &testReloadAdapter{id: "alpha"}
	service := pluginruntime.New(initial)
	order := make([]string, 0, 2)
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    service,
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			return &testReloadAdapter{id: item.Manifest.ID}
		},
		OnChange: func(pluginIDs []string) {
			if !reflect.DeepEqual(pluginIDs, []string{"alpha"}) {
				t.Fatalf("changed plugin IDs = %#v", pluginIDs)
			}
			order = append(order, "change")
		},
	}
	if err := reloader.ReloadPluginWithTransition(context.Background(), "alpha", func() {
		if service.Adapter("alpha") == initial {
			t.Fatal("transition ran before the replacement adapter owned the service slot")
		}
		order = append(order, "transition")
	}); err != nil {
		t.Fatalf("ReloadPluginWithTransition: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"transition", "change"}) {
		t.Fatalf("runtime transition order = %#v", order)
	}
}

func TestReloaderDefaultRetiresReplacementImmediately(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifest := func(version string) {
		t.Helper()
		body := "id: demo\nversion: " + version + "\ntransport: process\ncommand: [\"demo\"]\n"
		if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest("0.1.0")
	var created []*testReloadAdapter
	reloader := &Reloader{
		Dirs:    []string{root},
		Service: pluginruntime.New(),
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			adapter := &testReloadAdapter{id: item.Manifest.ID, version: item.Manifest.Version}
			created = append(created, adapter)
			return adapter
		},
	}
	if err := reloader.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	writeManifest("0.2.0")
	if err := reloader.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(created) != 2 || created[0].closed != 1 {
		t.Fatalf("replaced runtime must close immediately: %+v", created)
	}
}

func TestReloaderSkipsReservedAgents(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "openclaw")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(`
id: openclaw
transport: process
command: ["openclaw-pluginbridge-plugin"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	builtin := pluginruntime.StaticPlugin{PluginID: "openclaw"}
	svc := pluginruntime.New(builtin)
	reloader := &Reloader{
		Dirs:     []string{root},
		Service:  svc,
		Reserved: map[string]bool{"openclaw": true},
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			t.Fatalf("reserved plugin should not create adapter: %+v", item)
			return nil
		},
	}
	if err := reloader.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := svc.Adapter("openclaw"); got == nil {
		t.Fatal("expected builtin adapter to remain registered")
	}
}

func TestReloaderOnChangeFiresOnMutationOnly(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeDemoManifest := func(version string) {
		t.Helper()
		body := `
id: demo
version: ` + version + `
transport: process
command: ["demo-pluginbridge-plugin"]
`
		if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeDemoManifest("0.1.0")

	var changes [][]string
	svc := pluginruntime.New()
	reloader := &Reloader{
		Dirs:    []string{root},
		Service: svc,
		NewAdapter: func(Discovered) pluginruntime.PluginAdapter {
			return &testReloadAdapter{id: "demo"}
		},
		OnChange: func(pluginIDs []string) {
			changes = append(changes, append([]string(nil), pluginIDs...))
		},
	}
	ctx := context.Background()

	// Initial reload registers => OnChange fires.
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if !reflect.DeepEqual(changes, [][]string{{"demo"}}) {
		t.Fatalf("expected OnChange after register, got %#v", changes)
	}
	// Unchanged reload => OnChange must NOT fire.
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("unchanged reload: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("unchanged reload should not fire OnChange, got %#v", changes)
	}
	// Update => OnChange fires.
	writeDemoManifest("0.2.0")
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("updated reload: %v", err)
	}
	if !reflect.DeepEqual(changes, [][]string{{"demo"}, {"demo"}}) {
		t.Fatalf("expected OnChange after update, got %#v", changes)
	}
	// Remove => OnChange fires.
	if err := os.Remove(filepath.Join(pluginDir, ManifestYAML)); err != nil {
		t.Fatal(err)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("removed reload: %v", err)
	}
	if !reflect.DeepEqual(changes, [][]string{{"demo"}, {"demo"}, {"demo"}}) {
		t.Fatalf("expected OnChange after remove, got %#v", changes)
	}
	if svc.Adapter("demo") != nil {
		t.Fatal("expected demo adapter removed after delete")
	}
}

func TestReloaderReloadsWhenPluginDependencyChanges(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	runtimeDir := filepath.Join(pluginDir, "sdk", "node", "runtime")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(`
id: demo
version: 0.1.0
transport: process
command: ["index.js"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "index.js"), []byte("require('./sdk/node/runtime/cdp_client.cjs')\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dependency := filepath.Join(runtimeDir, "cdp_client.cjs")
	if err := os.WriteFile(dependency, []byte("module.exports = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var created int
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    pluginruntime.New(),
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			created++
			return &testReloadAdapter{id: item.Manifest.ID}
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected initial adapter, got %d", created)
	}
	time.Sleep(time.Millisecond)
	if err := os.WriteFile(dependency, []byte("module.exports = 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("dependency reload: %v", err)
	}
	if created != 2 {
		t.Fatalf("expected dependency change to recreate adapter, got %d", created)
	}
}

func TestReloaderReloadsWhenCurrentSymlinkRuntimeChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating plugin current symlinks requires elevated privileges on Windows")
	}
	root := t.TempDir()
	versionRoot := filepath.Join(root, "demo", "versions", "0.1.0")
	if err := os.MkdirAll(versionRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(versionRoot, ManifestYAML), []byte(`
id: demo
version: 0.1.0
transport: process
command: ["index.js"]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	entrypoint := filepath.Join(versionRoot, "index.js")
	if err := os.WriteFile(entrypoint, []byte("module.exports = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(versionRoot, filepath.Join(root, "demo", "current")); err != nil {
		t.Fatal(err)
	}

	var created int
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    pluginruntime.New(),
		CloseGrace: -1,
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			created++
			return &testReloadAdapter{id: item.Manifest.ID}
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected initial adapter, got %d", created)
	}
	time.Sleep(time.Millisecond)
	if err := os.WriteFile(entrypoint, []byte("module.exports = 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("symlink runtime reload: %v", err)
	}
	if created != 2 {
		t.Fatalf("expected current symlink runtime change to recreate adapter, got %d", created)
	}
}

func TestReloaderUsesDynamicDirsProvider(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	writePlugin := func(root, dir, pluginID string) {
		t.Helper()
		pluginDir := filepath.Join(root, dir)
		if err := os.MkdirAll(pluginDir, 0o755); err != nil {
			t.Fatal(err)
		}
		body := `
id: ` + pluginID + `
transport: process
command: ["` + pluginID + `-pluginbridge-plugin"]
`
		if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePlugin(rootA, "alpha", "alpha")
	writePlugin(rootB, "beta", "beta")

	currentDirs := []string{rootA}
	svc := pluginruntime.New()
	reloader := &Reloader{
		Service:    svc,
		CloseGrace: -1,
		DirsProvider: func() []string {
			return append([]string(nil), currentDirs...)
		},
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			return &testReloadAdapter{id: item.Manifest.ID}
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if svc.Adapter("alpha") == nil {
		t.Fatal("expected alpha adapter from initial dir")
	}
	if svc.Adapter("beta") != nil {
		t.Fatal("did not expect beta adapter before dir switch")
	}

	currentDirs = []string{rootB}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("dir switch reload: %v", err)
	}
	if svc.Adapter("alpha") != nil {
		t.Fatal("expected alpha adapter removed after dir switch")
	}
	if svc.Adapter("beta") == nil {
		t.Fatal("expected beta adapter registered after dir switch")
	}
}

func TestReloaderFilterRemovesDisallowedPlugin(t *testing.T) {
	root := t.TempDir()
	pluginDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, ManifestYAML), []byte(`
id: demo
transport: process
command: ["demo-pluginbridge-plugin"]
`), 0o600); err != nil {
		t.Fatal(err)
	}

	allowed := true
	svc := pluginruntime.New()
	reloader := &Reloader{
		Dirs:       []string{root},
		Service:    svc,
		CloseGrace: -1,
		Include: func(Discovered) bool {
			return allowed
		},
		NewAdapter: func(item Discovered) pluginruntime.PluginAdapter {
			return &testReloadAdapter{id: item.Manifest.ID}
		},
	}
	ctx := context.Background()
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if svc.Adapter("demo") == nil {
		t.Fatal("expected demo adapter to be registered")
	}

	allowed = false
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("filtered reload: %v", err)
	}
	if svc.Adapter("demo") != nil {
		t.Fatal("expected demo adapter removed when filter blocks it")
	}
}

type testReloadAdapter struct {
	id      string
	version string
	closed  int
}

func (a *testReloadAdapter) ID() string { return a.id }

func (a *testReloadAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{PluginID: a.id, Available: true, NativeVisibleInput: true}, nil
}

func (a *testReloadAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: a.id, Verified: true}, nil
}

func (a *testReloadAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, nil
}

func (a *testReloadAdapter) Subscribe(context.Context, pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	return nil, nil
}

func (a *testReloadAdapter) Interrupt(context.Context, pluginruntime.NativeSession, string) error {
	return nil
}

func (a *testReloadAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{}, nil
}

func (a *testReloadAdapter) Close(context.Context) error {
	a.closed++
	return nil
}
