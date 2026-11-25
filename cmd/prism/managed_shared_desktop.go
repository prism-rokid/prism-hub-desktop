package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"

	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
)

// Installed manifests identify clients of the managed ChatGPT window.
type sharedDesktopAgent struct {
	pluginID, displayName string
}

func sharedDesktopAgents(dirs []string) []*sharedDesktopAgent {
	items, _ := pluginpkg.ScanDirs(dirs)
	var agents []*sharedDesktopAgent
	for _, item := range items {
		spec, ok := dashboardManifestDesktopApp(item.Manifest, item.Manifest.ID)
		if ok && spec.SharedDesktop != nil {
			agents = append(agents, spec.SharedDesktop)
		}
	}
	return agents
}

type sharedDesktopLease struct {
	SharedWindow bool   `json:"shared_window,omitempty"`
	TargetID     string `json:"target_id"`
	CDPOrigin    string `json:"cdp_origin"`
	HostPID      int    `json:"host_pid"`
	HostStart    string `json:"host_start"`
	Phase        string `json:"phase"`
	Binding      string `json:"binding,omitempty"`
}

func supportsSharedDesktopHost(item pluginpkg.Discovered) bool {
	for _, spec := range item.Manifest.ManagedLaunchSpecs {
		if spec.SharedWindow == pluginpkg.SharedChatGPTWindow && spec.App == item.Manifest.ID {
			return true
		}
	}
	return false
}

// Older Codex versions cannot participate in the shared ChatGPT host lock.
func allowsSharedDesktopCompanion(item pluginpkg.Discovered, dirs []string) bool {
	if runtimeGOOS() != "darwin" || item.Manifest.ID != "codex" || supportsSharedDesktopHost(item) {
		return true
	}
	dir, err := desktopHostRuntimeDirectory()
	if err != nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, agent := range sharedDesktopAgents(dirs) {
		if agent.hasLiveLease(ctx, dir) {
			return false
		}
	}
	return true
}

func (agent *sharedDesktopAgent) hasLiveLease(ctx context.Context, dir string) bool {
	lease := agent.readLease(dir)
	if lease.HostPID <= 0 || lease.HostStart == "" || lease.TargetID == "" ||
		desktopHostProcessStart(ctx, lease.HostPID) != lease.HostStart {
		return false
	}
	targets, err := readDesktopHostTargets(ctx, lease.CDPOrigin)
	if err != nil {
		return true
	}
	for _, target := range targets {
		if target.ID == lease.TargetID {
			return true
		}
	}
	return false
}

func requireSharedDesktopCompanion(ctx context.Context, cfg *bridgeconfig.Config) error {
	const detail = "update the Codex plugin to the companion build before opening a shared ChatGPT window"
	discovered, _ := pluginpkg.ScanDirs(pluginDirs(cfg))
	for _, item := range discovered {
		if item.Manifest.ID == "codex" && bridgePluginAllowed(cfg, "codex") && !supportsSharedDesktopHost(item) {
			return errors.New(detail)
		}
	}
	service := currentDashboardRuntimePlugins()
	if service != nil {
		if adapter := service.Adapter("codex"); adapter != nil {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			discovery, err := adapter.Discover(probeCtx)
			if err != nil || discovery.SessionHints["desktop_host_coordination"] != pluginpkg.SharedChatGPTWindow {
				return errors.New(detail)
			}
		}
	}
	return nil
}

type desktopHostTarget struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	URL  string `json:"url"`
	WS   string `json:"webSocketDebuggerUrl"`
}

var desktopHostHTTP = &http.Client{
	Timeout:       800 * time.Millisecond,
	Transport:     &http.Transport{},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func desktopHostRuntimeDirectory() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".prism", "runtime", "desktop-host"), nil
}

func lockChatGPTDesktopHost(ctx context.Context, dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "chatgpt.sqlite")
	if file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600); err == nil {
		if err := file.Close(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "_busy_timeout=0"}).String()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	for {
		_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			return func() {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
				_ = conn.Close()
				_ = db.Close()
			}, nil
		}
		var sqliteErr sqlite3.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrBusy || !waitForDesktopHost(ctx, 50*time.Millisecond) {
			_ = conn.Close()
			_ = db.Close()
			if ctx.Err() != nil {
				return nil, fmt.Errorf("ChatGPT desktop is busy: %w", ctx.Err())
			}
			return nil, err
		}
	}
}

func (agent *sharedDesktopAgent) readLease(dir string) sharedDesktopLease {
	var lease sharedDesktopLease
	content, err := os.ReadFile(filepath.Join(dir, agent.pluginID+"-window.json"))
	if err != nil || len(content) > 16*1024 || json.Unmarshal(content, &lease) != nil {
		return sharedDesktopLease{}
	}
	return lease
}

func (agent *sharedDesktopAgent) writeLease(dir string, lease sharedDesktopLease) error {
	data, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, "."+agent.pluginID+"-window-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(dir, agent.pluginID+"-window.json"))
}

func (agent *sharedDesktopAgent) appPath(cfg *bridgeconfig.Config) string {
	candidates := []string{"/Applications/ChatGPT.app"}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, "Applications", "ChatGPT.app"))
	}
	if cfg != nil {
		local := firstNonEmptyMap(cfg.Bridge.Plugins["codex"].Env, "PRISM_CODEX_APP_PATH")
		global := firstNonEmptyMap(cfg.Bridge.PluginEnv, "PRISM_CODEX_APP_PATH")
		if override := firstNonEmpty(local, global); override != "" {
			candidates = []string{override}
		}
	}
	for _, path := range candidates {
		if strings.HasSuffix(path, ".app") {
			path = filepath.Join(path, "Contents", "MacOS", "ChatGPT")
		}
		path, err := filepath.EvalSymlinks(path)
		if err == nil && strings.HasSuffix(path, ".app/Contents/MacOS/ChatGPT") &&
			desktopHostNativeValidBundle(strings.TrimSuffix(path, "/Contents/MacOS/ChatGPT")) {
			return path
		}
	}
	return ""
}

func desktopHostProcessStart(ctx context.Context, pid int) string {
	command := exec.CommandContext(ctx, "/bin/ps", "-p", strconv.Itoa(pid), "-o", "lstart=")
	command.Env = append(os.Environ(), "LC_ALL=C")
	output, err := command.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func findDesktopHost(ctx context.Context, appPath string) (int, error) {
	// The process name used by pgrep can differ from the executable after
	// a managed ChatGPT relaunch. Enumerate executable paths without arguments.
	output, err := exec.CommandContext(ctx, "/bin/ps", "axww", "-o", "pid=,comm=").Output()
	if err != nil {
		return 0, fmt.Errorf("list ChatGPT host processes: %w", err)
	}
	return selectDesktopHostPID(string(output), appPath, desktopHostNativeProcessPath)
}

func selectDesktopHostPID(processes, appPath string, processPath func(int) string) (int, error) {
	var selected int
	for _, line := range strings.Split(processes, "\n") {
		line = strings.TrimSpace(line)
		separator := strings.IndexAny(line, " \t")
		if separator == -1 || filepath.Base(strings.TrimSpace(line[separator:])) != filepath.Base(appPath) {
			continue
		}
		pid, err := strconv.Atoi(line[:separator])
		if err != nil || pid <= 0 {
			continue
		}
		path, err := filepath.EvalSymlinks(processPath(pid))
		if err != nil || path != appPath {
			continue
		}
		if selected != 0 {
			return 0, errors.New("multiple ChatGPT host processes matched")
		}
		selected = pid
	}
	return selected, nil
}

func validDesktopHostOrigin(origin string) bool {
	u, err := url.Parse(origin)
	return err == nil && u.Scheme == "http" && u.Hostname() == "127.0.0.1" && u.Port() != "" &&
		u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func readDesktopHostTargets(ctx context.Context, origin string) ([]desktopHostTarget, error) {
	if !validDesktopHostOrigin(origin) {
		return nil, errors.New("invalid loopback DevTools origin")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/json/list", nil)
	if err != nil {
		return nil, err
	}
	response, err := desktopHostHTTP.Do(request)
	if err != nil {
		return nil, errors.New("ChatGPT's managed DevTools target list is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DevTools target discovery returned %d", response.StatusCode)
	}
	var targets []desktopHostTarget
	err = json.NewDecoder(io.LimitReader(response.Body, 2*1024*1024)).Decode(&targets)
	return targets, err
}

func findDesktopHostCDP(ctx context.Context, pid int) (string, error) {
	output, err := exec.CommandContext(ctx, "/usr/sbin/lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iTCP", "-sTCP:LISTEN").Output()
	if err != nil {
		return "", err
	}
	var selected string
	for _, port := range dashboardLoopbackListeningPorts(string(output)) {
		origin := "http://127.0.0.1:" + port
		targets, err := readDesktopHostTargets(ctx, origin)
		if err != nil {
			continue
		}
		for _, target := range targets {
			if target.workspace() && target.validWebSocket(origin) {
				if selected != "" && selected != origin {
					return "", errors.New("multiple ChatGPT DevTools listeners matched")
				}
				selected = origin
				break
			}
		}
	}
	return selected, nil
}

func (target desktopHostTarget) workspace() bool {
	href, err := url.Parse(target.URL)
	return err == nil && target.Type == "page" && target.ID != "" &&
		href.Scheme == "app" && href.Host == "-" && href.Path == "/index.html"
}

func (target desktopHostTarget) validWebSocket(origin string) bool {
	u, err := url.Parse(target.WS)
	host, _ := url.Parse(origin)
	return err == nil && host != nil && u.Scheme == "ws" && u.Host == host.Host && u.User == nil &&
		u.Path == "/devtools/page/"+target.ID && u.RawQuery == "" && u.Fragment == ""
}

func sharedChatGPTWorkspaceTarget(targets []desktopHostTarget, origin string, previous sharedDesktopLease) (desktopHostTarget, error) {
	var workspace desktopHostTarget
	count := 0
	for _, target := range targets {
		if !target.workspace() || !target.validWebSocket(origin) {
			continue
		}
		if previous.SharedWindow && previous.CDPOrigin == origin && previous.TargetID == target.ID {
			return target, nil
		}
		// Codex's desktop adapter selects the unqualified workspace URL.
		// A secondary window can advertise initialRoute=/ as well.
		if target.URL != "app://-/index.html" || (!previous.SharedWindow && previous.CDPOrigin == origin && previous.TargetID == target.ID) {
			continue
		}
		count++
		workspace = target
	}
	if count > 1 {
		return desktopHostTarget{}, errors.New("multiple ChatGPT workspaces are open; Prism could not identify the shared Codex window")
	}
	return workspace, nil
}

func waitForDesktopHost(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sharedDesktopLeaseTarget(ctx context.Context, lease sharedDesktopLease) desktopHostTarget {
	if lease.HostPID <= 0 || lease.HostStart == "" || !validDesktopHostOrigin(lease.CDPOrigin) ||
		desktopHostProcessStart(ctx, lease.HostPID) != lease.HostStart {
		return desktopHostTarget{}
	}
	targets, err := readDesktopHostTargets(ctx, lease.CDPOrigin)
	if err != nil {
		return desktopHostTarget{}
	}
	for _, target := range targets {
		if target.ID == lease.TargetID && target.workspace() && target.validWebSocket(lease.CDPOrigin) {
			return target
		}
	}
	return desktopHostTarget{}
}

func (agent *sharedDesktopAgent) ensureWindow(ctx context.Context, cfg *bridgeconfig.Config) (sharedDesktopLease, error) {
	return agent.bindWindow(ctx, cfg, false)
}

// Explicit remote selection may restore an existing local binding. Initial
// binding and launching the desktop application remain local Dashboard actions.
func prepareSharedDesktopSelection(ctx context.Context, cfg *bridgeconfig.Config, pluginID string) error {
	if runtimeGOOS() != "darwin" || filepath.Base(pluginID) != pluginID || pluginID == "" {
		return nil
	}
	dir, err := desktopHostRuntimeDirectory()
	if err != nil {
		return err
	}
	agent := sharedDesktopAgent{pluginID: pluginID}
	lease := agent.readLease(dir)
	if !lease.SharedWindow || lease.Phase != "ready" || lease.Binding == "" {
		return nil
	}
	if sharedDesktopLeaseTarget(ctx, lease).ID != "" {
		return nil
	}
	item, err := dashboardPluginManifest(cfg, pluginID)
	if err != nil || !supportsSharedDesktopHost(item) {
		return nil
	}
	agent.displayName = item.Manifest.DisplayName
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err = agent.bindWindow(ctx, cfg, true)
	return err
}

func (agent *sharedDesktopAgent) bindWindow(ctx context.Context, cfg *bridgeconfig.Config, attachOnly bool) (sharedDesktopLease, error) {
	if runtimeGOOS() != "darwin" {
		return sharedDesktopLease{}, errors.New("managed shared ChatGPT windows require macOS")
	}
	if !dashboardPluginManagedLaunch(pluginDirs(cfg), agent.pluginID) {
		return sharedDesktopLease{}, fmt.Errorf("%s has no installed managed window target", agent.displayName)
	}
	appPath := agent.appPath(cfg)
	if appPath == "" {
		return sharedDesktopLease{}, errors.New("ChatGPT.app was not found")
	}
	dir, err := desktopHostRuntimeDirectory()
	if err != nil {
		return sharedDesktopLease{}, err
	}
	service := currentDashboardRuntimePlugins()
	if service == nil {
		return sharedDesktopLease{}, errors.New("plugin runtime unavailable")
	}
	adapter := service.Adapter(agent.pluginID)
	if adapter == nil {
		reloader := currentDashboardRuntimePluginReloader()
		if reloader == nil {
			return sharedDesktopLease{}, errors.New("plugin runtime is starting")
		}
		if err := reloader.ReloadPlugin(ctx, agent.pluginID); err != nil {
			return sharedDesktopLease{}, err
		}
		adapter = service.Adapter(agent.pluginID)
	}
	controller, ok := adapter.(pluginruntime.SessionController)
	if !ok {
		return sharedDesktopLease{}, errors.New("installed plugin does not support shared window binding")
	}
	release, err := lockChatGPTDesktopHost(ctx, dir)
	if err != nil {
		return sharedDesktopLease{}, err
	}
	defer release()
	if err := requireSharedDesktopCompanion(ctx, cfg); err != nil {
		return sharedDesktopLease{}, err
	}
	previous := agent.readLease(dir)
	if attachOnly && (!previous.SharedWindow || previous.Phase != "ready" || previous.Binding == "") {
		return sharedDesktopLease{}, errors.New("shared window is no longer bound locally")
	}
	if previous.SharedWindow && previous.HostPID > 0 {
		hostPath, _ := filepath.EvalSymlinks(desktopHostNativeProcessPath(previous.HostPID))
		if hostPath == appPath {
			if target := sharedDesktopLeaseTarget(ctx, previous); target.ID != "" {
				if attachOnly {
					return previous, nil
				}
				return agent.finishBinding(ctx, dir, previous, controller)
			}
		}
	}
	pid, err := findDesktopHost(ctx, appPath)
	if err != nil {
		return sharedDesktopLease{}, err
	}
	if pid == 0 {
		if attachOnly {
			return sharedDesktopLease{}, errors.New("the managed ChatGPT host is not running; open it in Prism Hub")
		}
		if err := startSharedChatGPTHost(cfg, appPath); err != nil {
			return sharedDesktopLease{}, err
		}
	}
	if attachOnly {
		// A running ChatGPT can have its own debugger. Only adopt the instance
		// using the locally configured managed profile and its launch flags.
		args, err := exec.CommandContext(ctx, "/bin/ps", "-ww", "-p", strconv.Itoa(pid), "-o", "command=").Output()
		if err != nil || !sharedDesktopManagedCommand(string(args), sharedDesktopProfile(cfg)) {
			return sharedDesktopLease{}, errors.New("ChatGPT is not using the managed profile; rehost it in Prism Hub")
		}
	}
	var origin string
	for ctx.Err() == nil {
		if pid == 0 {
			pid, err = findDesktopHost(ctx, appPath)
			if err != nil {
				return sharedDesktopLease{}, err
			}
		}
		if pid != 0 {
			origin, _ = findDesktopHostCDP(ctx, pid)
			if origin != "" {
				break
			}
		}
		if !waitForDesktopHost(ctx, 300*time.Millisecond) {
			break
		}
	}
	if origin == "" {
		return sharedDesktopLease{}, errors.New("ChatGPT has no reachable managed DevTools listener")
	}
	start := desktopHostProcessStart(ctx, pid)
	if start == "" {
		return sharedDesktopLease{}, errors.New("could not identify the ChatGPT host process")
	}
	host := sharedDesktopLease{SharedWindow: true, CDPOrigin: origin, HostPID: pid, HostStart: start,
		Phase: "opening", Binding: previous.Binding}
	for ctx.Err() == nil {
		targets, err := readDesktopHostTargets(ctx, origin)
		if err == nil {
			hint, err := sharedDesktopTargetHint(dir, sharedDesktopAgents(pluginDirs(cfg)), agent, host, targets)
			if err != nil {
				return sharedDesktopLease{}, err
			}
			peer := hint
			if hint.TargetID == "" && previous.HostPID == pid && previous.HostStart == start {
				hint = previous
			}
			target, err := sharedChatGPTWorkspaceTarget(targets, origin, hint)
			if err != nil {
				return sharedDesktopLease{}, err
			}
			if target.ID != "" {
				host.TargetID = target.ID
				if attachOnly && previous.HostPID != pid {
					return agent.restoreBinding(ctx, dir, host, controller, peer)
				}
				return agent.finishBinding(ctx, dir, host, controller)
			}
		}
		if !waitForDesktopHost(ctx, 300*time.Millisecond) {
			break
		}
	}
	return sharedDesktopLease{}, errors.New("无法找到可共用的 Codex 托管页面")
}

func (agent *sharedDesktopAgent) restoreBinding(ctx context.Context, dir string, host sharedDesktopLease, controller pluginruntime.SessionController, peer sharedDesktopLease) (sharedDesktopLease, error) {
	bound, err := agent.finishBinding(ctx, dir, host, controller)
	if err != nil && agent.pluginID == "codex" && peer.SharedWindow && peer.Phase == "ready" && peer.Binding != "" &&
		peer.HostPID == host.HostPID && peer.HostStart == host.HostStart && peer.CDPOrigin == host.CDPOrigin &&
		peer.TargetID == host.TargetID && sharedDesktopLeaseTarget(ctx, peer).ID == host.TargetID {
		// The companion can select its requested thread via a verified peer.
		// Its read-only bind requires that thread in front. Leave its own
		// binding untouched until it can prove it on a later selection.
		return peer, nil
	}
	return bound, err
}

// A second surface can reuse a renderer already verified by a registered agent
// on this exact process and DevTools origin.
func sharedDesktopTargetHint(dir string, agents []*sharedDesktopAgent, current *sharedDesktopAgent, host sharedDesktopLease, targets []desktopHostTarget) (sharedDesktopLease, error) {
	var hint sharedDesktopLease
	for _, agent := range agents {
		if agent.pluginID == current.pluginID {
			continue
		}
		lease := agent.readLease(dir)
		if !lease.SharedWindow || lease.Phase != "ready" || lease.HostPID != host.HostPID ||
			lease.HostStart != host.HostStart || lease.CDPOrigin != host.CDPOrigin {
			continue
		}
		for _, target := range targets {
			if target.ID != lease.TargetID || !target.workspace() || !target.validWebSocket(host.CDPOrigin) {
				continue
			}
			if hint.TargetID != "" && hint.TargetID != lease.TargetID {
				return sharedDesktopLease{}, errors.New("shared ChatGPT agents disagree on the workspace renderer")
			}
			hint = lease
			break
		}
	}
	return hint, nil
}

func sharedDesktopManagedCommand(command, profile string) bool {
	command = " " + strings.TrimSpace(command) + " "
	return profile != "" && strings.Contains(command, " --user-data-dir="+profile+" --remote-debugging-port=0 ")
}

func sharedDesktopProfile(cfg *bridgeconfig.Config) string {
	codex, _ := dashboardDesktopAppSpecByID("codex")
	profile := dashboardDesktopProfileDir(codex)
	if cfg != nil {
		profile = firstNonEmpty(firstNonEmptyMap(cfg.Bridge.Plugins["codex"].Env, codex.UserDataEnvKey),
			firstNonEmptyMap(cfg.Bridge.PluginEnv, codex.UserDataEnvKey), profile)
	}
	return profile
}

func startSharedChatGPTHost(cfg *bridgeconfig.Config, appPath string) error {
	codex, _ := dashboardDesktopAppSpecByID("codex")
	processes, err := dashboardDesktopProcessesWithError(codex)
	if err != nil {
		return err
	}
	if len(processes) > 0 {
		return errors.New("another Codex or ChatGPT process is already running")
	}
	command := dashboardManagedDesktopCommand(appPath, dashboardManagedDesktopLaunchArgs(sharedDesktopProfile(cfg)))
	command.Dir = string(filepath.Separator)
	if err := command.Start(); err != nil {
		return err
	}
	go func() { _ = command.Wait() }()
	return nil
}

// The Hub holds the host lock throughout this local-only request. The plugin
// supplies the opaque binding only after verifying its own native surface.
func (agent *sharedDesktopAgent) finishBinding(ctx context.Context, dir string, lease sharedDesktopLease, controller pluginruntime.SessionController) (sharedDesktopLease, error) {
	result, err := controller.ControlSession(ctx, pluginruntime.SessionControlRequest{Action: pluginruntime.SharedWindowBindAction,
		Session: pluginruntime.NativeSession{PluginID: agent.pluginID}, Target: lease})
	if err != nil {
		return sharedDesktopLease{}, err
	}
	if !result.OK || result.Action != pluginruntime.SharedWindowBindAction || strings.TrimSpace(result.ThreadID) == "" || len(result.ThreadID) > 4096 ||
		(lease.Binding != "" && result.ThreadID != lease.Binding) {
		return sharedDesktopLease{}, errors.New("plugin could not confirm the requested shared window binding")
	}
	if sharedDesktopLeaseTarget(ctx, lease).ID == "" {
		return sharedDesktopLease{}, errors.New("shared window changed during binding")
	}
	lease.Binding, lease.Phase = result.ThreadID, "ready"
	return lease, agent.writeLease(dir, lease)
}

func (agent *sharedDesktopAgent) status(cfg *bridgeconfig.Config, managed bool) map[string]any {
	appPath := agent.appPath(cfg)
	ready := false
	shared := false
	if dir, err := desktopHostRuntimeDirectory(); runtimeGOOS() == "darwin" && err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()
		lease := agent.readLease(dir)
		shared = lease.SharedWindow
		ready = shared && lease.Phase == "ready" && lease.Binding != "" && sharedDesktopLeaseTarget(ctx, lease).ID != ""
	}
	status, message := "stopped", fmt.Sprintf("%s 尚未绑定到 Codex 的共用窗口。", agent.displayName)
	if ready {
		status, message = "ready", fmt.Sprintf("%s 已连接到 Codex 的共用窗口。", agent.displayName)
	} else if shared {
		message = fmt.Sprintf("%s 的共用窗口已断开。", agent.displayName)
	}
	return map[string]any{
		"app_id": agent.pluginID, "display_name": agent.displayName, "managed_enabled": managed,
		"app_path": appPath, "detected_host_app": "ChatGPT", "status": status, "running": ready,
		"cdp_reachable":             ready,
		"bind_before_remote_enable": true,
		"can_launch_managed":        managed && appPath != "" && runtimeGOOS() == "darwin",
		"launch_action_label":       dashboardCodexLaunchLabel(status), "managed_status_message": message,
	}
}
