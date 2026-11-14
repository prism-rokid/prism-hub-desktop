package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/agentlocation"
	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
)

// 配置只持久化选择结果；解析后的路径只存在于当前进程和插件环境中。
// 保存选择结果可以避免绑定到会变化的 Store 安装路径。
func resolveAgentLocation(ctx context.Context, cfg *bridgeconfig.Config, id, mode string) (agentlocation.ResolvedLocation, error) {
	rule, ok := agentlocation.Lookup(id, mode)
	if !ok {
		return agentlocation.ResolvedLocation{}, fmt.Errorf("unsupported Agent location: %s/%s", id, mode)
	}
	var selection *agentlocation.LocationSelection
	legacy := ""
	if cfg != nil {
		pc := cfg.Bridge.Plugins[id]
		if s, ok := pc.Locations[mode]; ok {
			selection = &s
		}
		legacy = firstNonEmpty(pc.Env[rule.EnvKey], cfg.Bridge.PluginEnv[rule.EnvKey])
		if id == "hermes" && mode == "desktop" {
			legacy = firstNonEmpty(pc.Env["PRISM_HERMES_APP_EXECUTABLE"], pc.Env[rule.EnvKey], cfg.Bridge.PluginEnv["PRISM_HERMES_APP_EXECUTABLE"], cfg.Bridge.PluginEnv[rule.EnvKey])
		}
	}
	if legacy == "" {
		legacy = os.Getenv(rule.EnvKey)
		if id == "hermes" && mode == "desktop" {
			legacy = firstNonEmpty(os.Getenv("PRISM_HERMES_APP_EXECUTABLE"), legacy)
		}
	}
	resolver := newAgentLocationResolver()
	if cfg != nil {
		resolver.Getenv = func(key string) string {
			pc := cfg.Bridge.Plugins[id]
			return firstNonEmpty(pc.Env[key], cfg.Bridge.PluginEnv[key], os.Getenv(key))
		}
	}
	return resolver.Resolve(ctx, rule, selection, legacy)
}
func agentLocationEnv(ctx context.Context, cfg *bridgeconfig.Config, manifest pluginpkg.Manifest) map[string]string {
	mode := manifest.RuntimeMode(cfg.Bridge.Plugins[manifest.ID].Mode)
	rule, ok := agentlocation.Lookup(manifest.ID, mode)
	if !ok {
		return nil
	}
	loc, err := resolveAgentLocation(ctx, cfg, manifest.ID, mode)
	if err != nil {
		// 无效的手动覆盖不能静默切换到另一份安装。
		if s, ok := cfg.Bridge.Plugins[manifest.ID].Locations[mode]; ok {
			out := map[string]string{rule.EnvKey: filepath.Join(s.Directory, "__prism_invalid_location__")}
			if manifest.ID == "hermes" && mode == "desktop" {
				out["PRISM_HERMES_APP_EXECUTABLE"] = out[rule.EnvKey]
			}
			return out
		}
		return nil
	}
	out := map[string]string{rule.EnvKey: loc.Executable}
	if rule.LaunchKind == "desktop-app" {
		prefix := "PRISM_CODEX"
		if manifest.ID == "hermes" {
			prefix = "PRISM_HERMES"
			out["PRISM_HERMES_APP_EXECUTABLE"] = loc.Executable
		}
		pc := cfg.Bridge.Plugins[manifest.ID]
		key := prefix + "_USER_DATA_DIR"
		out[key] = firstNonEmpty(pc.Env[key], cfg.Bridge.PluginEnv[key], os.Getenv(key), loc.UserDataDir)
	}
	return out
}

type agentRecovery struct {
	State string `json:"state"`
	Mode  string `json:"mode"`
	Error string `json:"error,omitempty"`
}

var agentLocationOperations = struct {
	sync.Mutex
	items map[string]agentRecovery
}{items: map[string]agentRecovery{}}
var agentLocationConfigMu sync.Mutex

func locationOperation(id string) agentRecovery {
	agentLocationOperations.Lock()
	defer agentLocationOperations.Unlock()
	return agentLocationOperations.items[id]
}
func setLocationOperation(id string, value agentRecovery) {
	agentLocationOperations.Lock()
	agentLocationOperations.items[id] = value
	agentLocationOperations.Unlock()
}
func beginLocationOperation(id, mode string) bool {
	agentLocationOperations.Lock()
	defer agentLocationOperations.Unlock()
	if agentLocationOperations.items[id].State == "connecting" {
		return false
	}
	agentLocationOperations.items[id] = agentRecovery{State: "connecting", Mode: mode}
	return true
}

func agentLocationPayload(ctx context.Context, cfg *bridgeconfig.Config, id, mode string) map[string]any {
	rule, _ := agentlocation.Lookup(id, mode)
	out := map[string]any{"rule": rule, "recovery": locationOperation(id)}
	if s, ok := cfg.Bridge.Plugins[id].Locations[mode]; ok {
		out["selection"] = s
	}
	loc, err := resolveAgentLocation(ctx, cfg, id, mode)
	if err != nil {
		out["error"] = &agentlocation.Error{Code: agentlocation.Code(err), Detail: err.Error()}
	} else {
		out["resolved"] = loc
	}
	return out
}

// 注册到同一个回环 Dashboard 多路复用器，并执行相同的来源校验。
func registerAgentLocationAPI(mux *http.ServeMux, ctx context.Context, configPath string, cache *dashboardStatusCache) {
	mux.HandleFunc("/assets/agent-location.js", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, 405, "method not allowed")
			return
		}
		asset, err := dashboardAssets.ReadFile("dashboard_assets/agent-location.js")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(asset)
	})
	mux.HandleFunc("/api/agents/", func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || parsed.Host != r.Host || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				writeDashboardError(w, 403, "invalid origin")
				return
			}
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 4 || len(parts) > 5 || parts[3] != "location" || (len(parts) == 5 && parts[4] != "validate" && parts[4] != "recover") {
			http.NotFound(w, r)
			return
		}
		id := parts[2]
		mode := r.URL.Query().Get("mode")
		var req struct {
			Mode string `json:"mode"`
			agentlocation.LocationSelection
		}
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
				writeDashboardError(w, 400, "invalid JSON")
				return
			}
			mode = req.Mode
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			writeDashboardError(w, 500, err.Error())
			return
		}
		installed, err := dashboardPluginManifest(cfg, id)
		if err != nil {
			writeDashboardError(w, 404, "local plugin not found")
			return
		}
		if mode == "" {
			mode = installed.Manifest.RuntimeMode(cfg.Bridge.Plugins[id].Mode)
		}
		rule, supported := agentlocation.Lookup(id, mode)
		if !supported || !installed.Manifest.AllowsRuntimeMode(mode) {
			writeDashboardError(w, 400, "unsupported Agent runtime mode")
			return
		}
		if r.Method == http.MethodGet && len(parts) == 4 {
			payload := agentLocationPayload(r.Context(), cfg, id, mode)
			payload["ok"] = true
			writeDashboardJSON(w, 200, payload)
			return
		}
		if len(parts) == 5 && parts[4] == "recover" && r.Method == http.MethodPost {
			if !beginLocationOperation(id, mode) {
				writeDashboardError(w, 409, "Agent recovery already in progress")
				return
			}
			agentlocation.Invalidate()
			writeDashboardJSON(w, 202, map[string]any{"ok": true, "saved": false, "recovery": locationOperation(id)})
			go recoverLocatedAgent(ctx, configPath, id, mode, cache)
			return
		}
		validate := len(parts) == 5 && parts[4] == "validate" && r.Method == http.MethodPost
		if !validate && (len(parts) != 4 || (r.Method != http.MethodPut && r.Method != http.MethodDelete)) {
			w.Header().Set("Allow", "GET, PUT, DELETE")
			writeDashboardError(w, 405, "method not allowed")
			return
		}
		if r.Method != http.MethodDelete {
			req.Directory = filepath.Clean(strings.TrimSpace(req.Directory))
			candidates, validationErr := newAgentLocationResolver().Validate(rule, req.LocationSelection)
			if validationErr != nil {
				writeDashboardJSON(w, 400, map[string]any{"ok": false, "code": agentlocation.Code(validationErr), "error": validationErr.Error()})
				return
			}
			if validate {
				writeDashboardJSON(w, 200, map[string]any{"ok": true, "candidates": candidates})
				return
			}
			if len(candidates) != 1 {
				writeDashboardJSON(w, 409, map[string]any{"ok": false, "code": "multiple_candidates", "candidates": candidates, "error": "Choose an executable"})
				return
			}
			if req.Executable != "" {
				req.Executable, _ = filepath.Rel(req.Directory, candidates[0].Executable)
			}
		}
		if !beginLocationOperation(id, mode) {
			writeDashboardError(w, 409, "Agent recovery already in progress")
			return
		}
		agentLocationConfigMu.Lock()
		// 写入前重新加载配置，保留其他插件刚保存的选择。
		cfg, err = bridgeconfig.Load(configPath)
		if err == nil {
			if cfg.Bridge.Plugins == nil {
				cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
			}
			pc := cfg.Bridge.Plugins[id]
			if pc.Locations == nil {
				pc.Locations = map[string]agentlocation.LocationSelection{}
			}
			if r.Method == http.MethodDelete {
				delete(pc.Locations, mode)
				delete(pc.Env, rule.EnvKey)
				delete(cfg.Bridge.PluginEnv, rule.EnvKey)
				if id == "hermes" && mode == "desktop" {
					delete(pc.Env, "PRISM_HERMES_APP_EXECUTABLE")
					delete(cfg.Bridge.PluginEnv, "PRISM_HERMES_APP_EXECUTABLE")
				}
			} else {
				pc.Locations[mode] = req.LocationSelection
			}
			cfg.Bridge.Plugins[id] = pc
			err = cfg.Save(configPath)
		}
		agentLocationConfigMu.Unlock()
		if err != nil {
			setLocationOperation(id, agentRecovery{State: "failed", Mode: mode, Error: err.Error()})
			writeDashboardError(w, 500, err.Error())
			return
		}
		agentlocation.Invalidate()
		writeDashboardJSON(w, http.StatusAccepted, map[string]any{"ok": true, "saved": true, "connected": false, "recovery": locationOperation(id)})
		go recoverLocatedAgent(ctx, configPath, id, mode, cache)
	})
}

func recoverLocatedAgent(parent context.Context, configPath, id, mode string, cache *dashboardStatusCache) {
	result := agentRecovery{State: "failed", Mode: mode}
	defer func() { setLocationOperation(id, result); refreshDashboardStatusCacheSoon(cache) }()
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		result.Error = err.Error()
		return
	}
	installed, err := dashboardPluginManifest(cfg, id)
	if err != nil {
		result.Error = err.Error()
		return
	}
	if installed.Manifest.RuntimeMode(cfg.Bridge.Plugins[id].Mode) != mode {
		result.State = "inactive"
		return
	}
	spec, specErr := selectLaunchSpec(installed.Manifest, mode, "")
	timeout := 30 * time.Second
	if specErr == nil && spec.ReadyTimeoutMs > 0 {
		timeout = time.Duration(spec.ReadyTimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	reloader := currentDashboardRuntimePluginReloader()
	if reloader == nil {
		result.Error = "Plugin runtime is unavailable"
		return
	}
	if err = reloader.ReloadPluginWithTransition(ctx, id, nil); err != nil {
		result.Error = err.Error()
		return
	}
	svc := currentDashboardRuntimePlugins()
	if svc == nil {
		result.Error = "Plugin runtime is unavailable"
		return
	}
	probe := func() (bool, string) {
		adapter := svc.Adapter(id)
		if adapter == nil {
			return false, "Plugin runtime is unavailable"
		}
		probeCtx, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		cap, probeErr := adapter.Probe(probeCtx)
		if probeErr != nil {
			return false, probeErr.Error()
		}
		return cap.Available, cap.UnavailableReason
	}
	if available, _ := probe(); available {
		result.State = "connected"
		return
	}
	// 启动前重新检查持久化模式，同时覆盖并发发生的模式切换。
	cfg, err = bridgeconfig.Load(configPath)
	if err != nil {
		result.Error = err.Error()
		return
	}
	if installed.Manifest.RuntimeMode(cfg.Bridge.Plugins[id].Mode) != mode {
		result.State = "inactive"
		return
	}
	loc, locErr := resolveAgentLocation(ctx, cfg, id, mode)
	if locErr != nil {
		result.Error = locErr.Error()
		return
	}
	if specErr == nil {
		if spec.Kind == pluginpkg.LaunchKindDesktopApp {
			appSpec, ok := dashboardDesktopAppSpecByID(spec.App)
			if !ok {
				result.Error = "Unknown desktop app"
				return
			}
			// 恢复流程不会终止已经运行的桌面进程。进程可能刚由托管启动完成，
			// 但 CDP 和插件运行时仍在异步初始化，因此这里保留等待探测的机会。
			if processes, processErr := dashboardDesktopProcessesWithError(appSpec); processErr != nil {
				result.Error = processErr.Error()
				return
			} else if len(processes) > 0 {
				// 已存在的进程由下面的统一探测循环等待连接收敛，避免把
				// “进程已启动但 CDP 尚未发布”误报成需要关闭进程。
			} else {
				_, launchErr := dashboardStartManagedDesktopAppWithoutStop(cfg, appSpec)
				if launchErr != nil && !strings.Contains(strings.ToLower(launchErr.Error()), "is running") {
					err = launchErr
				}
			}
		} else {
			err = startManagedService(ctx, installed.Manifest, spec, nil, cfg)
		}
	} else if installed.Manifest.AllowsManagedTerminal(mode) && loc.LaunchKind == "cli" {
		// 使用插件拥有的终端生命周期，不再启动第二个不受管理的 CLI。
		adapter := svc.Adapter(id)
		if opener, ok := adapter.(pluginruntime.ManagedTerminalLauncher); ok {
			var opened pluginruntime.ManagedTerminalResult
			opened, err = opener.OpenManagedTerminal(ctx, pluginruntime.ManagedTerminalRequest{PluginID: id})
			if err == nil && !opened.OK {
				err = fmt.Errorf("%s", opened.Message)
			}
		} else {
			result.Error = "Installation located; open the managed terminal to connect"
			return
		}
	} else {
		result.Error = "Installation located; start Agent to connect"
		return
	}
	if err != nil {
		result.Error = err.Error()
		return
	}
	for {
		if current, loadErr := bridgeconfig.Load(configPath); loadErr == nil && installed.Manifest.RuntimeMode(current.Bridge.Plugins[id].Mode) != mode {
			result.State = "inactive"
			return
		}
		available, reason := probe()
		if available {
			result.State = "connected"
			return
		}
		result.Error = reason
		select {
		case <-ctx.Done():
			if result.Error == "" {
				result.Error = "Agent connection timed out"
			}
			return
		case <-time.After(time.Second):
		}
	}
}

// 将运行时真实状态附加到已安装 manifest，包括当前不可用的 adapter。
// 服务可能已经可访问，即使本地没有可启动的可执行文件。
func annotateAgentLocations(cfg *bridgeconfig.Config, items []map[string]any) {
	var wg sync.WaitGroup
	for _, item := range items {
		id, _ := item["plugin_id"].(string)
		mode, _ := item["current_runtime_mode"].(string)
		if _, ok := agentlocation.Lookup(id, mode); !ok {
			continue
		}
		wg.Add(1)
		go func(item map[string]any, id, mode string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			item["location"] = agentLocationPayload(ctx, cfg, id, mode)
			recovery := locationOperation(id)
			item["location_recovery"] = recovery
			available := false
			known := false
			reason := ""
			if svc := currentDashboardRuntimePlugins(); svc != nil {
				if adapter := svc.Adapter(id); adapter != nil {
					cap, err := adapter.Probe(ctx)
					known = true
					available = err == nil && cap.Available
					reason = cap.UnavailableReason
					if err != nil {
						reason = err.Error()
					}
				}
			}
			_, locErr := resolveAgentLocation(ctx, cfg, id, mode)
			code := "runtime_offline"
			lower := strings.ToLower(reason)
			switch {
			case available:
				code = "ready"
			case strings.Contains(lower, "auth") || strings.Contains(lower, "token") || strings.Contains(lower, "鉴权") || strings.Contains(lower, "登录"):
				code = "authentication_failed"
			case locErr != nil:
				if _, manual := cfg.Bridge.Plugins[id].Locations[mode]; manual || agentlocation.Code(locErr) == "configured_location_invalid" {
					code = "location_invalid"
				} else {
					code = "program_not_found"
				}
			case strings.Contains(lower, "state_db") || strings.Contains(lower, "session data") || strings.Contains(lower, "会话数据") || strings.Contains(lower, "codex_home"):
				code = "data_unavailable"
			case !known:
				code = "runtime_unknown"
			}
			item["runtime_status"] = map[string]any{"available": available, "known": known, "code": code, "reason": reason}
		}(item, id, mode)
	}
	wg.Wait()
}

func newAgentLocationResolver() *agentlocation.Resolver {
	resolver := agentlocation.New()
	resolver.Platform = runtimeGOOS()
	resolver.StorePath = func(ctx context.Context) (string, error) {
		out, err := dashboardWindowsStoreCodexPath(ctx)
		return strings.TrimSpace(string(out)), err
	}
	return resolver
}

// 旧版桌面路径保存也进入与新目录选择对话框相同的选择存储。
func prepareLegacyDesktopLocations(ctx context.Context, cfg *bridgeconfig.Config, apps map[string]dashboardDesktopAppConfigRequest) ([]string, error) {
	var changed []string
	for appID, request := range apps {
		if request.AppPath == nil {
			continue
		}
		spec, ok := dashboardDesktopAppSpecByID(appID)
		if ok && locationOperation(spec.PluginID).State == "connecting" {
			return nil, fmt.Errorf("Agent recovery already in progress")
		}
		if !ok {
			return nil, fmt.Errorf("unsupported desktop app: %s", appID)
		}
		if _, err := dashboardPluginManifest(cfg, spec.PluginID); err != nil {
			return nil, err
		}
		rule, ok := agentlocation.Lookup(spec.PluginID, "desktop")
		if !ok {
			return nil, fmt.Errorf("unsupported Agent location")
		}
		pc := cfg.Bridge.Plugins[spec.PluginID]
		if pc.Locations == nil {
			pc.Locations = map[string]agentlocation.LocationSelection{}
		}
		if path := strings.TrimSpace(*request.AppPath); path != "" {
			resolved, err := newAgentLocationResolver().Resolve(ctx, rule, nil, path)
			if err != nil {
				return nil, err
			}
			relative, err := filepath.Rel(resolved.Directory, resolved.Executable)
			if err != nil {
				return nil, err
			}
			selection := agentlocation.LocationSelection{Directory: resolved.Directory, Executable: relative}
			if _, err := newAgentLocationResolver().Validate(rule, selection); err != nil {
				return nil, err
			}
			pc.Locations["desktop"] = selection
		} else {
			delete(pc.Locations, "desktop")
		}
		delete(pc.Env, rule.EnvKey)
		delete(cfg.Bridge.PluginEnv, rule.EnvKey)
		if spec.PluginID == "hermes" {
			delete(pc.Env, "PRISM_HERMES_APP_EXECUTABLE")
			delete(cfg.Bridge.PluginEnv, "PRISM_HERMES_APP_EXECUTABLE")
		}
		cfg.Bridge.Plugins[spec.PluginID] = pc
		request.AppPath = nil
		apps[appID] = request
		changed = append(changed, spec.PluginID)
	}
	return changed, nil
}
