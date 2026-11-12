package agentlocation

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func programFixture(t *testing.T, root, relative string) string {
	t.Helper()
	target := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("fixture"), 0755); err != nil {
		t.Fatal(err)
	}
	return target
}
func fixtureResolver(platform, home string, env map[string]string) *Resolver {
	return &Resolver{Platform: platform, Home: home, Getenv: func(k string) string { return env[k] }}
}
func TestValidateAgentLayouts(t *testing.T) {
	cases := []struct{ id, mode, platform, relative string }{
		{"codex", "desktop", "windows", "Codex.exe"},
		{"codex", "desktop", "windows", filepath.Join("app", "ChatGPT.exe")},
		{"codex", "desktop", "darwin", filepath.Join("Codex.app", "Contents", "MacOS", "Codex")},
		{"hermes", "desktop", "darwin", filepath.Join("Contents", "MacOS", "Hermes")},
		{"hermes", "native", "windows", "hermes.exe"},
		{"claudecode", "mod", "windows", "claude.cmd"},
		{"openclaw", "native", "windows", "openclaw.cmd"},
		{"opencode", "native", "linux", filepath.Join("bin", "opencode")},
	}
	for _, tt := range cases {
		t.Run(tt.id+"/"+tt.mode+"/"+tt.platform, func(t *testing.T) {
			if runtime.GOOS == "windows" && tt.platform != "windows" {
				t.Skip("POSIX permission fixture")
			}
			root := filepath.Join(t.TempDir(), "安装 目录")
			target := programFixture(t, root, tt.relative)
			rule, ok := Lookup(tt.id, tt.mode)
			if !ok {
				t.Fatal("rule missing")
			}
			candidates, err := fixtureResolver(tt.platform, t.TempDir(), nil).Validate(rule, LocationSelection{Directory: root})
			if err != nil || len(candidates) != 1 || candidates[0].Executable != target {
				t.Fatalf("candidates=%+v err=%v", candidates, err)
			}
		})
	}
}
func TestSelectionIsAuthoritativeAndBounded(t *testing.T) {
	root := t.TempDir()
	rule, _ := Lookup("codex", "desktop")
	r := fixtureResolver("windows", root, nil)
	first := programFixture(t, root, "Codex.exe")
	programFixture(t, root, filepath.Join("app", "ChatGPT.exe"))
	if _, err := r.Resolve(context.Background(), rule, &LocationSelection{Directory: root}, ""); Code(err) != "multiple_candidates" {
		t.Fatalf("expected ambiguity: %v", err)
	}
	selection := &LocationSelection{Directory: root, Executable: "Codex.exe"}
	loc, err := r.Resolve(context.Background(), rule, selection, "")
	if err != nil || loc.Executable != first || loc.Source != "manual" {
		t.Fatalf("%+v %v", loc, err)
	}
	for _, bad := range []string{"../Codex.exe", first, "other.exe"} {
		if _, err := r.Validate(rule, LocationSelection{Directory: root, Executable: bad}); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	missing := &LocationSelection{Directory: filepath.Join(root, "missing")}
	if _, err := r.Resolve(context.Background(), rule, missing, first); err == nil {
		t.Fatal("invalid selection fell back to legacy")
	}
	if _, err := r.Resolve(context.Background(), rule, nil, filepath.Join(root, "missing.exe")); Code(err) != "configured_location_invalid" {
		t.Fatalf("legacy fallback: %v", err)
	}
}
func TestProfilesFollowSelectedInstallation(t *testing.T) {
	root := t.TempDir()
	env := map[string]string{"APPDATA": filepath.Join(root, "Roaming"), "LOCALAPPDATA": filepath.Join(root, "Local")}
	r := fixtureResolver("windows", root, env)
	rule, _ := Lookup("codex", "desktop")
	classic := programFixture(t, root, filepath.Join("classic", "Codex.exe"))
	store := programFixture(t, root, filepath.Join("WindowsApps", "OpenAI.Codex_1", "app", "ChatGPT.exe"))
	for _, tt := range []struct{ exe, kind, profile string }{{classic, "classic", filepath.Join(env["APPDATA"], "Codex")}, {store, "store", filepath.Join(env["APPDATA"], "Codex", "web", "Codex")}} {
		loc, err := r.Resolve(context.Background(), rule, nil, tt.exe)
		if err != nil || loc.InstallKind != tt.kind || loc.UserDataDir != tt.profile {
			t.Fatalf("%+v %v", loc, err)
		}
	}
}
func TestAutoFindsStoreOnlyAfterClassic(t *testing.T) {
	root := t.TempDir()
	env := map[string]string{"LOCALAPPDATA": root}
	r := fixtureResolver("windows", root, env)
	rule, _ := Lookup("codex", "desktop")
	store := programFixture(t, root, filepath.Join("WindowsApps", "OpenAI.Codex_1", "app", "ChatGPT.exe"))
	queries := 0
	r.StorePath = func(context.Context) (string, error) { queries++; return store, nil }
	loc, err := r.Resolve(context.Background(), rule, nil, "")
	if err != nil || loc.Executable != store || queries != 1 {
		t.Fatalf("%+v %v queries=%d", loc, err, queries)
	}
	classic := programFixture(t, root, filepath.Join("Programs", "Codex", "Codex.exe"))
	loc, err = r.Resolve(context.Background(), rule, nil, "")
	if err != nil || loc.Executable != classic || queries != 1 {
		t.Fatalf("%+v %v queries=%d", loc, err, queries)
	}
}
func TestNoRecursiveScanOrNonExecutableFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable permission fixture")
	}
	root := t.TempDir()
	r := fixtureResolver("linux", root, nil)
	rule, _ := Lookup("opencode", "native")
	programFixture(t, root, filepath.Join("deep", "nested", "opencode"))
	if _, err := r.Validate(rule, LocationSelection{Directory: root}); Code(err) != "program_not_found" {
		t.Fatalf("unexpected recursive scan: %v", err)
	}
	file := programFixture(t, root, "opencode")
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Validate(rule, LocationSelection{Directory: root}); Code(err) != "program_not_found" {
		t.Fatalf("accepted non-executable: %v", err)
	}
}

func TestCLIUsesConfiguredPATHAndPreferredNvmVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX NVM fixture")
	}
	root := t.TempDir()
	binDir := filepath.Join(root, "custom tools")
	target := programFixture(t, binDir, "claude")
	env := map[string]string{"PATH": binDir}
	r := fixtureResolver("linux", root, env)
	rule, _ := Lookup("claudecode", "mod")
	loc, err := r.Resolve(context.Background(), rule, nil, "claude")
	if err != nil || loc.Executable != target {
		t.Fatalf("configured PATH ignored: %+v %v", loc, err)
	}
	env["PATH"] = ""
	programFixture(t, root, filepath.Join(".nvm", "versions", "node", "v9.0.0", "bin", "claude"))
	preferred := programFixture(t, root, filepath.Join(".nvm", "versions", "node", "v22.3.0", "bin", "claude"))
	programFixture(t, root, filepath.Join(".nvm", "versions", "node", "v22.23.0", "bin", "claude"))
	alias := filepath.Join(root, ".nvm", "alias")
	if err := os.MkdirAll(alias, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(alias, "default"), []byte("22.3.0\n"), 0644); err != nil {
		t.Fatal(err)
	}
	loc, err = r.Resolve(context.Background(), rule, nil, "")
	if err != nil || loc.Executable != preferred {
		t.Fatalf("preferred NVM version ignored: %+v %v", loc, err)
	}
}
