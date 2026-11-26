// agentlocation 包在不执行程序的前提下解析已安装的 Agent。
// 安装规则保持边界明确，并独立于 Dashboard 界面。
package agentlocation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LocationSelection struct {
	Directory  string `json:"directory" yaml:"directory"`
	Executable string `json:"executable,omitempty" yaml:"executable,omitempty"`
}
type ResolvedLocation struct {
	PluginID    string `json:"plugin_id"`
	RuntimeMode string `json:"runtime_mode"`
	Directory   string `json:"directory"`
	Executable  string `json:"executable"`
	LaunchKind  string `json:"launch_kind"`
	Source      string `json:"source"`
	InstallKind string `json:"install_kind"`
	UserDataDir string `json:"user_data_dir,omitempty"`
}
type Error struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *Error) Error() string { return e.Detail }
func Code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return "location_failed"
}
func failure(code, detail string) error { return &Error{code, detail} }

type Rule struct {
	PluginID    string   `json:"plugin_id"`
	RuntimeMode string   `json:"runtime_mode"`
	LaunchKind  string   `json:"launch_kind"`
	EnvKey      string   `json:"env_key"`
	Names       []string `json:"names"`
}

func Lookup(id, mode string) (Rule, bool) {
	r := Rule{PluginID: id, RuntimeMode: mode}
	switch {
	case id == "codex" && mode == "desktop":
		r.LaunchKind = "desktop-app"
		r.EnvKey = "PRISM_CODEX_APP_PATH"
		r.Names = []string{"Codex", "ChatGPT"}
	case id == "hermes" && mode == "desktop":
		r.LaunchKind = "desktop-app"
		r.EnvKey = "PRISM_HERMES_APP_PATH"
		r.Names = []string{"Hermes", "Hermes-Setup"}
	case id == "dsh" && mode == "desktop":
		r.LaunchKind = "desktop-app"
		r.EnvKey = "PRISM_DSH_APP_PATH"
		r.Names = []string{"DeepSeek Harness"}
	case id == "hermes" && mode == "native":
		r.LaunchKind = "cli"
		r.EnvKey = "HERMES_BIN"
		r.Names = []string{"hermes"}
	case id == "claudecode" && mode == "mod":
		r.LaunchKind = "cli"
		r.EnvKey = "PRISM_CLAUDE_CLI"
		r.Names = []string{"claude"}
	case id == "openclaw" && mode == "native":
		r.LaunchKind = "service"
		r.EnvKey = "PRISM_OPENCLAW_EXECUTABLE"
		r.Names = []string{"openclaw"}
	case id == "opencode" && mode == "native":
		r.LaunchKind = "service"
		r.EnvKey = "PRISM_OPENCODE_EXECUTABLE"
		r.Names = []string{"opencode"}
	default:
		return r, false
	}
	return r, true
}

type Resolver struct {
	Platform string
	Home     string
	Getenv   func(string) string
	// 测试可以注入 StorePath；默认查询有超时边界并带缓存。
	StorePath func(context.Context) (string, error)
}

func New() *Resolver {
	home, _ := os.UserHomeDir()
	return &Resolver{Platform: runtime.GOOS, Home: home, Getenv: os.Getenv, StorePath: windowsStorePath}
}
func (r *Resolver) names(rule Rule) []string {
	if r.Platform != "windows" {
		return rule.Names
	}
	var names []string
	for _, n := range rule.Names {
		names = append(names, n+".exe")
		if rule.LaunchKind != "desktop-app" {
			names = append(names, n+".cmd", n+".bat")
		}
	}
	return names
}
func (r *Resolver) relativePaths(rule Rule) []string {
	var out []string
	for _, n := range r.names(rule) {
		if rule.LaunchKind == "desktop-app" && r.Platform == "darwin" {
			out = append(out, n, filepath.Join("Contents", "MacOS", n))
			for _, bundle := range rule.Names {
				out = append(out, filepath.Join(bundle+".app", "Contents", "MacOS", n))
			}
		} else {
			out = append(out, n, filepath.Join("bin", n))
			if rule.LaunchKind == "desktop-app" {
				out = append(out, filepath.Join("app", n))
			}
		}
	}
	return out
}
func (r *Resolver) program(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	return r.Platform == "windows" || info.Mode().Perm()&0111 != 0
}

// Validate 返回所有匹配的程序。选择结果必须落在已知相对路径内，
// 任意命令注入和父目录穿越都会被拒绝。
func (r *Resolver) Validate(rule Rule, selection LocationSelection) ([]ResolvedLocation, error) {
	dir := filepath.Clean(strings.TrimSpace(selection.Directory))
	if !filepath.IsAbs(dir) {
		return nil, failure("invalid_directory", "An absolute folder on the Hub computer is required")
	}
	info, err := os.Stat(dir)
	if err != nil {
		code := "directory_missing"
		if os.IsPermission(err) {
			code = "permission_denied"
		}
		return nil, failure(code, err.Error())
	}
	if !info.IsDir() {
		return nil, failure("invalid_directory", "Select a folder, not a file")
	}
	// 单独检查目录可读性，以区分权限错误和没有候选程序。
	f, err := os.Open(dir)
	if err != nil {
		return nil, failure("permission_denied", err.Error())
	}
	f.Close()
	chosen := filepath.Clean(selection.Executable)
	if selection.Executable != "" && (filepath.IsAbs(chosen) || chosen == ".." || strings.HasPrefix(chosen, ".."+string(filepath.Separator))) {
		return nil, failure("invalid_executable", "Executable must be inside the selected folder")
	}
	var out []ResolvedLocation
	denied := false
	seen := map[string]bool{}
	for _, rel := range r.relativePaths(rule) {
		if selection.Executable != "" && chosen != rel {
			continue
		}
		target := filepath.Join(dir, rel)
		if seen[target] {
			continue
		}
		if !r.program(target) {
			if _, err := os.Stat(target); os.IsPermission(err) {
				denied = true
			}
			continue
		}
		seen[target] = true
		out = append(out, r.result(rule, dir, target, "manual"))
	}
	if len(out) == 0 {
		if denied {
			return nil, failure("permission_denied", "Cannot access the Agent executable")
		}
		return nil, failure("program_not_found", "No supported Agent executable found in this folder")
	}
	return out, nil
}
func (r *Resolver) result(rule Rule, dir, executable, source string) ResolvedLocation {
	kind := "cli"
	if rule.LaunchKind == "desktop-app" {
		kind = "classic"
		if strings.Contains(executable, ".app"+string(filepath.Separator)) {
			kind = "app-bundle"
		}
		if strings.Contains(strings.ToLower(executable), "windowsapps") {
			kind = "store"
		}
	}
	if source != "manual" && kind == "app-bundle" {
		if index := strings.Index(executable, ".app"+string(filepath.Separator)); index >= 0 {
			dir = executable[:index+4]
		}
	}
	out := ResolvedLocation{PluginID: rule.PluginID, RuntimeMode: rule.RuntimeMode, Directory: dir, Executable: executable, LaunchKind: rule.LaunchKind, Source: source, InstallKind: kind}
	if rule.LaunchKind == "desktop-app" {
		product := "Codex"
		if rule.PluginID == "hermes" {
			product = "Hermes"
		}
		switch r.Platform {
		case "windows":
			base := r.Getenv("APPDATA")
			if base == "" {
				base = r.Getenv("LOCALAPPDATA")
			}
			if base == "" {
				base = filepath.Join(r.Home, "AppData", "Roaming")
			}
			out.UserDataDir = filepath.Join(base, product)
			if rule.PluginID == "codex" && kind == "store" {
				out.UserDataDir = filepath.Join(out.UserDataDir, "web", "Codex")
			}
		case "darwin":
			out.UserDataDir = filepath.Join(r.Home, "Library", "Application Support", product)
		default:
			base := r.Getenv("XDG_CONFIG_HOME")
			if base == "" {
				base = filepath.Join(r.Home, ".config")
			}
			out.UserDataDir = filepath.Join(base, product)
		}
	}
	return out
}
func (r *Resolver) Resolve(ctx context.Context, rule Rule, selection *LocationSelection, legacy string) (ResolvedLocation, error) {
	if selection != nil {
		candidates, err := r.Validate(rule, *selection)
		if err != nil {
			return ResolvedLocation{}, err
		}
		if len(candidates) != 1 {
			return ResolvedLocation{}, failure("multiple_candidates", "Choose an executable from the matching programs")
		}
		return candidates[0], nil
	}
	if legacy = strings.TrimSpace(legacy); legacy != "" {
		if !filepath.IsAbs(legacy) && !strings.ContainsAny(legacy, "/\\") {
			if found, err := r.lookPath(legacy); err == nil {
				legacy = found
			}
		}
		if info, err := os.Stat(legacy); err == nil && info.IsDir() {
			candidates, err := r.Validate(rule, LocationSelection{Directory: legacy})
			if err != nil {
				return ResolvedLocation{}, err
			}
			if len(candidates) != 1 {
				return ResolvedLocation{}, failure("multiple_candidates", "Choose an executable from the matching programs")
			}
			out := candidates[0]
			out.Source = "legacy"
			return out, nil
		}
		if !r.program(legacy) {
			return ResolvedLocation{}, failure("configured_location_invalid", "Configured Agent executable is unavailable")
		}
		return r.result(rule, filepath.Dir(legacy), legacy, "legacy"), nil
	}
	for _, candidate := range r.autoCandidates(rule) {
		if r.program(candidate) {
			return r.result(rule, filepath.Dir(candidate), candidate, "auto"), nil
		}
	}
	if rule.PluginID == "codex" && r.Platform == "windows" && r.StorePath != nil {
		candidate, err := r.StorePath(ctx)
		if err == nil && r.program(candidate) {
			out := r.result(rule, filepath.Dir(candidate), candidate, "auto")
			if out.InstallKind != "store" {
				out.UserDataDir = filepath.Join(out.UserDataDir, "web", "Codex")
			}
			out.InstallKind = "store"
			return out, nil
		}
	}
	return ResolvedLocation{}, failure("program_not_found", "Agent executable was not found")
}
func (r *Resolver) autoCandidates(rule Rule) []string {
	var out []string
	names := r.names(rule)
	if rule.LaunchKind == "desktop-app" {
		if r.Platform == "darwin" {
			for _, bundle := range rule.Names {
				for _, n := range rule.Names {
					out = append(out, filepath.Join("/Applications", bundle+".app", "Contents", "MacOS", n), filepath.Join(r.Home, "Applications", bundle+".app", "Contents", "MacOS", n))
				}
			}
			if rule.PluginID == "hermes" {
				out = append([]string{filepath.Join(r.Home, ".hermes", "hermes-agent", "apps", "desktop", "release", "mac-arm64", "Hermes.app", "Contents", "MacOS", "Hermes")}, out...)
			}
		} else if r.Platform == "windows" {
			product := "Codex"
			if rule.PluginID == "hermes" {
				product = "Hermes"
			}
			for _, base := range []string{r.Getenv("LOCALAPPDATA"), r.Getenv("ProgramFiles")} {
				if base == "" {
					continue
				}
				prefix := base
				if base == r.Getenv("LOCALAPPDATA") {
					prefix = filepath.Join(base, "Programs")
				}
				for _, n := range names {
					out = append(out, filepath.Join(prefix, product, n))
				}
			}
		} else {
			for _, name := range names {
				if p, err := r.lookPath(strings.ToLower(name)); err == nil {
					out = append(out, p)
				}
			}
		}
		return out
	}
	pathValue := r.Getenv("PATH")
	if pathValue == "" {
		pathValue = r.Getenv("Path")
	}
	dirs := filepath.SplitList(pathValue)
	dirs = append(dirs, filepath.Join(r.Home, ".local", "bin"), filepath.Join(r.Home, ".claude", "local"))
	if r.Platform == "windows" {
		dirs = append(dirs, filepath.Join(r.Getenv("APPDATA"), "npm"), r.Getenv("NVM_SYMLINK"), r.Getenv("NVM_HOME"), filepath.Join(r.Home, ".volta", "bin"))
	}
	mise := r.Getenv("MISE_SHIMS_DIR")
	if mise == "" {
		data := r.Getenv("MISE_DATA_DIR")
		if data == "" {
			if r.Platform == "windows" {
				data = filepath.Join(r.Getenv("LOCALAPPDATA"), "mise")
			} else {
				data = filepath.Join(r.Home, ".local", "share", "mise")
			}
		}
		mise = filepath.Join(data, "shims")
	}
	dirs = append(dirs, mise)
	nvm := r.Getenv("NVM_DIR")
	if nvm == "" {
		nvm = filepath.Join(r.Home, ".nvm")
	}
	versions, _ := filepath.Glob(filepath.Join(nvm, "versions", "node", "v*", "bin"))
	sort.Slice(versions, func(i, j int) bool {
		left := strings.Split(strings.TrimPrefix(filepath.Base(filepath.Dir(versions[i])), "v"), ".")
		right := strings.Split(strings.TrimPrefix(filepath.Base(filepath.Dir(versions[j])), "v"), ".")
		for k := 0; k < 3 && k < len(left) && k < len(right); k++ {
			a, _ := strconv.Atoi(left[k])
			b, _ := strconv.Atoi(right[k])
			if a != b {
				return a > b
			}
		}
		return versions[i] > versions[j]
	})
	if alias, err := os.ReadFile(filepath.Join(nvm, "alias", "default")); err == nil {
		preferred := "v" + strings.TrimPrefix(strings.TrimSpace(string(alias)), "v")
		sort.SliceStable(versions, func(i, j int) bool {
			return filepath.Base(filepath.Dir(versions[i])) == preferred && filepath.Base(filepath.Dir(versions[j])) != preferred
		})
	}
	dirs = append(dirs, versions...)
	dirs = append(dirs, "/opt/homebrew/bin", "/usr/local/bin")
	for _, dir := range dirs {
		dir = strings.Trim(dir, "\"")
		if dir == "" {
			continue
		}
		for _, name := range names {
			out = append(out, filepath.Join(dir, name))
		}
	}
	return out
}

var storeCache struct {
	sync.Mutex
	path string
	at   time.Time
}

func Invalidate() { storeCache.Lock(); storeCache.at = time.Time{}; storeCache.Unlock() }
func windowsStorePath(ctx context.Context) (string, error) {
	storeCache.Lock()
	defer storeCache.Unlock()
	if !storeCache.at.IsZero() && time.Since(storeCache.at) < 30*time.Second {
		return storeCache.path, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd, err := Command(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$pkg=Get-AppxPackage -Name OpenAI.Codex -ErrorAction SilentlyContinue | Select-Object -First 1; if($pkg){Join-Path $pkg.InstallLocation 'app\ChatGPT.exe'}`)
	if err != nil {
		return "", err
	}
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("Store query: %w", err)
	}
	storeCache.path = strings.TrimSpace(string(out))
	storeCache.at = time.Now()
	return storeCache.path, nil
}

func WindowsStorePath(ctx context.Context) ([]byte, error) {
	path, err := windowsStorePath(ctx)
	return []byte(path), err
}

func (r *Resolver) lookPath(name string) (string, error) {
	pathValue := r.Getenv("PATH")
	if pathValue == "" {
		pathValue = r.Getenv("Path")
	}
	names := []string{name}
	if r.Platform == "windows" && filepath.Ext(name) == "" {
		names = []string{name + ".exe", name + ".cmd", name + ".bat"}
	}
	for _, dir := range filepath.SplitList(pathValue) {
		dir = strings.Trim(dir, "\"")
		if dir == "" || !filepath.IsAbs(dir) {
			continue
		}
		for _, candidate := range names {
			target := filepath.Join(dir, candidate)
			if r.program(target) {
				return target, nil
			}
		}
	}
	return "", failure("program_not_found", "Command was not found in PATH")
}
