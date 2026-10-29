package main

import (
	"bufio"
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	prismapi "rokid-prism-hub/internal/api"
	"rokid-prism-hub/internal/buildinfo"
	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/doctor"
	"rokid-prism-hub/internal/host"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginregistry"
	"rokid-prism-hub/internal/pluginruntime"
)

//go:embed dashboard_assets/*
var dashboardAssets embed.FS

type dashboardPageData struct {
	Title             string
	Translations      template.JS
	HubVersion        string
	RegisterURL       string
	ForgotPasswordURL string
}

var (
	dashboardTranslationPlaceholder  = regexp.MustCompile(`\{([A-Za-z][A-Za-z0-9_]*)\}`)
	dashboardLoopbackListenerPattern = regexp.MustCompile(`(?i)\bTCP\s+(?:127\.0\.0\.1|\[::1\]|localhost):(\d+)\s+\(LISTEN\)`)
	dashboardManagedDesktopLaunchMu  sync.Mutex
)

// dashboardTranslationJSON keeps the editable CSV as the single source of
// truth while embedding a ready-to-use, safely marshalled dictionary in the
// Dashboard document. json.Marshal escapes HTML-significant characters.
func dashboardTranslationJSON(source []byte) (template.JS, error) {
	source = []byte(strings.TrimPrefix(string(source), "\ufeff"))
	reader := csv.NewReader(strings.NewReader(string(source)))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("read translations CSV: %w", err)
	}
	if len(records) < 2 || len(records[0]) != 4 || strings.Join(records[0], ",") != "key,zh-CN,en-US,ja-JP" {
		return "", errors.New("translations CSV must use header: key,zh-CN,en-US,ja-JP")
	}
	locales := records[0][1:]
	dictionary := make(map[string]map[string]string, len(locales))
	for _, locale := range locales {
		dictionary[locale] = map[string]string{}
	}
	for index, row := range records[1:] {
		line := index + 2
		if len(row) != 4 {
			return "", fmt.Errorf("translations CSV line %d: expected 4 columns, got %d", line, len(row))
		}
		key := strings.TrimSpace(row[0])
		if key == "" {
			return "", fmt.Errorf("translations CSV line %d: key is required", line)
		}
		placeholders := ""
		for localeIndex, locale := range locales {
			value := strings.TrimSpace(row[localeIndex+1])
			if value == "" {
				return "", fmt.Errorf("translations CSV line %d: %s translation is required", line, locale)
			}
			found := dashboardTranslationPlaceholder.FindAllStringSubmatch(value, -1)
			current := make([]string, 0, len(found))
			for _, match := range found {
				current = append(current, match[1])
			}
			sort.Strings(current)
			joined := strings.Join(current, ",")
			if localeIndex == 0 {
				placeholders = joined
			} else if joined != placeholders {
				return "", fmt.Errorf("translations CSV line %d: placeholder set differs for %s", line, locale)
			}
			if _, exists := dictionary[locale][key]; exists {
				return "", fmt.Errorf("translations CSV line %d: duplicate key %q", line, key)
			}
			dictionary[locale][key] = value
		}
	}
	payload, err := json.Marshal(dictionary)
	if err != nil {
		return "", fmt.Errorf("marshal translations: %w", err)
	}
	return template.JS(payload), nil
}

type dashboardStatus struct {
	OK                      bool             `json:"ok"`
	CheckedAt               string           `json:"checked_at"`
	HubRunning              bool             `json:"hub_running"`
	HubReachable            bool             `json:"hub_reachable"`
	HubPID                  int              `json:"hub_pid,omitempty"`
	SidecarGeneration       string           `json:"sidecar_generation,omitempty"`
	HubURL                  string           `json:"hub_url,omitempty"`
	LocalDeviceName         string           `json:"local_device_name,omitempty"`
	LocalDeviceAddress      string           `json:"local_device_address,omitempty"`
	ConfigPath              string           `json:"config_path"`
	DBPath                  string           `json:"db_path"`
	PluginDirs              []string         `json:"plugin_dirs"`
	ConfigExists            bool             `json:"config_exists"`
	DBExists                bool             `json:"db_exists"`
	Region                  string           `json:"region"`
	ResolvedAPIURL          string           `json:"resolved_api_url"`
	ResolvedRealtime        string           `json:"resolved_realtime_url"`
	HostTokenSet            bool             `json:"host_token_set"`
	AccountTokenSet         bool             `json:"account_token_set"`
	LoggedIn                bool             `json:"logged_in"`
	RemoteControlReady      bool             `json:"remote_control_ready"`
	BridgeEnabled           bool             `json:"bridge_enabled"`
	KeepAwakeEnabled        bool             `json:"keep_awake_enabled"`
	KeepAwakeSupported      bool             `json:"keep_awake_supported"`
	KeepAwakeMethod         string           `json:"keep_awake_method,omitempty"`
	RecommendedAction       string           `json:"recommended_action"`
	LoginCommand            string           `json:"login_command"`
	CanRestartHub           bool             `json:"can_restart_hub"`
	LifecycleState          string           `json:"lifecycle_state"`
	LifecycleLabel          string           `json:"lifecycle_label"`
	ConnectionState         string           `json:"connection_state"`
	ConnectionLabel         string           `json:"connection_label"`
	ConnectionDetail        string           `json:"connection_detail,omitempty"`
	HostPluginSyncState     string           `json:"host_plugin_sync_state,omitempty"`
	HostPluginSyncDetail    string           `json:"host_plugin_sync_detail,omitempty"`
	HostPluginSyncUpdatedAt string           `json:"host_plugin_sync_updated_at,omitempty"`
	HostPluginCount         int              `json:"host_plugin_count,omitempty"`
	Account                 map[string]any   `json:"account"`
	Plugins                 []map[string]any `json:"plugins"`
	Warnings                []string         `json:"warnings"`
	Error                   string           `json:"error,omitempty"`
	Config                  map[string]any   `json:"config"`
	DesktopApps             map[string]any   `json:"desktop_apps,omitempty"`
	Login                   *dashboardLogin  `json:"login,omitempty"`
	Doctor                  *doctor.Summary  `json:"doctor,omitempty"`
}

type dashboardLogin struct {
	Pending                 bool   `json:"pending"`
	LoginID                 string `json:"login_id,omitempty"`
	Mode                    string `json:"mode,omitempty"`
	Status                  string `json:"status,omitempty"`
	UserCode                string `json:"user_code,omitempty"`
	VerificationURI         string `json:"verification_uri,omitempty"`
	VerificationURIComplete string `json:"verification_uri_complete,omitempty"`
	ExpiresIn               int64  `json:"expires_in,omitempty"`
	Interval                int    `json:"interval,omitempty"`
	StartedAt               string `json:"started_at,omitempty"`
	LastPolledAt            string `json:"last_polled_at,omitempty"`
	Message                 string `json:"message,omitempty"`
	Error                   string `json:"error,omitempty"`
	Region                  string `json:"region,omitempty"`

	APIURL     string `json:"-"`
	DeviceCode string `json:"-"`
}

type dashboardLoginManager struct {
	mu     sync.Mutex
	device *dashboardLogin
}

type dashboardDesktopAppConfigRequest struct {
	AppPath     *string `json:"app_path"`
	UserDataDir *string `json:"user_data_dir"`
}

type dashboardDesktopAppSpec struct {
	AppID          string
	PluginID       string
	DisplayName    string
	AppEnvKey      string
	UserDataEnvKey string
	MacQuitScripts []string
	LinuxCommand   string
}

type dashboardRestartFunc func(configPath, dbPath, reason string) map[string]any

type dashboardServerOptions struct {
	RestartHub      dashboardRestartFunc
	PluginRegistry  *pluginregistry.Manager
	PluginInstaller *plugininstall.Installer
	Doctor          *doctor.Service
}

const dashboardStatusRefreshInterval = 15 * time.Second

const (
	dashboardManagedCDPReadyAttempts = 80
	dashboardManagedCDPReadyPoll     = 250 * time.Millisecond
)

// dashboardStatusCache 将文件系统、进程和 CDP 检查移出 HTTP 请求路径。
// 刚变更本地状态的调用方可强制刷新；常规 Dashboard 轮询只读取最近一次完整采样。
type dashboardStatusCache struct {
	configPath string
	dbPath     string

	mu          sync.RWMutex
	refreshMu   sync.Mutex
	value       dashboardStatus
	initialized bool
	refreshedAt time.Time
	revision    uint64
}

func newDashboardStatusCache(configPath, dbPath string) *dashboardStatusCache {
	return &dashboardStatusCache{configPath: configPath, dbPath: dbPath}
}

// refreshDashboardStatusCacheSoon re-samples the status cache after a local
// mutation. Without it, the Dashboard's regular polling keeps serving the
// pre-action snapshot until the next 15-second ticker cycle. Async on
// purpose: the collect walks the filesystem, plugin dirs and CDP endpoints,
// which must not delay the mutation response the caller is waiting on.
func refreshDashboardStatusCacheSoon(statusCache *dashboardStatusCache) {
	if statusCache == nil {
		return
	}
	go statusCache.Refresh()
}

func (c *dashboardStatusCache) Refresh() dashboardStatus {
	if c == nil {
		return dashboardStatus{OK: false, Error: "dashboard status cache unavailable"}
	}
	c.mu.RLock()
	revision := c.revision
	c.mu.RUnlock()
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.RLock()
	if c.revision != revision {
		value := cloneDashboardStatus(c.value)
		c.mu.RUnlock()
		return value
	}
	c.mu.RUnlock()
	value := collectServingHubDashboardStatus(c.configPath, c.dbPath)
	c.mu.Lock()
	c.value = value
	c.initialized = true
	c.refreshedAt = time.Now()
	c.revision++
	c.mu.Unlock()
	return cloneDashboardStatus(value)
}

func (c *dashboardStatusCache) Snapshot() (dashboardStatus, bool) {
	if c == nil {
		return dashboardStatus{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneDashboardStatus(c.value), c.initialized && time.Since(c.refreshedAt) < dashboardStatusRefreshInterval
}

func cloneDashboardStatus(value dashboardStatus) dashboardStatus {
	cloned := value
	cloned.PluginDirs = append([]string(nil), value.PluginDirs...)
	cloned.Warnings = append([]string(nil), value.Warnings...)
	cloned.Plugins = make([]map[string]any, 0, len(value.Plugins))
	for _, plugin := range value.Plugins {
		item := make(map[string]any, len(plugin))
		for key, nested := range plugin {
			item[key] = nested
		}
		cloned.Plugins = append(cloned.Plugins, item)
	}
	return cloned
}

type dashboardCatalogOperations struct {
	sync.RWMutex
	byPlugin map[string]string
}

func newDashboardCatalogOperations() *dashboardCatalogOperations {
	return &dashboardCatalogOperations{byPlugin: map[string]string{}}
}

func (o *dashboardCatalogOperations) Track(pluginID, operationID string) {
	if o == nil {
		return
	}
	o.Lock()
	o.byPlugin[strings.ToLower(strings.TrimSpace(pluginID))] = operationID
	o.Unlock()
}

func (o *dashboardCatalogOperations) ID(pluginID string) string {
	if o == nil {
		return ""
	}
	o.RLock()
	defer o.RUnlock()
	return o.byPlugin[strings.ToLower(strings.TrimSpace(pluginID))]
}

func newDashboardLoginManager() *dashboardLoginManager {
	return &dashboardLoginManager{}
}

func (m *dashboardLoginManager) Snapshot() *dashboardLogin {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneDashboardLogin(m.device)
}

func (m *dashboardLoginManager) StartDevice(configPath, explicitRegion, explicitAPIURL, mode string) (*dashboardLogin, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}
	if strings.TrimSpace(explicitRegion) != "" {
		cfg.SelectRegion(explicitRegion)
	}
	apiURL := firstNonEmpty(strings.TrimSpace(explicitAPIURL), strings.TrimSpace(cfg.Prism.APIURL()))
	if apiURL == "" {
		return nil, fmt.Errorf("device login requires configured prism region")
	}
	hostname, _ := os.Hostname()
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "qr"
	}
	start, err := prismapi.StartDeviceLogin(apiURL, hostname, mode)
	if err != nil {
		return nil, fmt.Errorf("start device login failed: %w", err)
	}
	login := &dashboardLogin{
		Pending:                 true,
		LoginID:                 fmt.Sprintf("login-%d", time.Now().UnixNano()),
		Mode:                    mode,
		Status:                  firstNonEmpty(strings.TrimSpace(start.Status), "pending"),
		UserCode:                strings.TrimSpace(start.UserCode),
		VerificationURI:         strings.TrimSpace(start.VerificationURI),
		VerificationURIComplete: strings.TrimSpace(start.VerificationURIComplete),
		ExpiresIn:               start.ExpiresIn,
		Interval:                start.Interval,
		StartedAt:               time.Now().UTC().Format(time.RFC3339),
		Message:                 "等待本地 Dashboard 完成授权",
		Region:                  cfg.Prism.Region,
		APIURL:                  apiURL,
		DeviceCode:              strings.TrimSpace(start.DeviceCode),
	}
	if login.Interval <= 0 {
		login.Interval = 3
	}
	m.mu.Lock()
	m.device = login
	m.mu.Unlock()
	return m.Snapshot(), nil
}

func (m *dashboardLoginManager) PollDevice(configPath, dbPath, loginID string) (map[string]any, error) {
	return m.pollDevice(configPath, dbPath, loginID, requestDashboardHubRestart)
}

func (m *dashboardLoginManager) pollDevice(configPath, dbPath, loginID string, restartHub dashboardRestartFunc) (map[string]any, error) {
	session := m.Snapshot()
	if session == nil {
		return nil, fmt.Errorf("no pending device login")
	}
	if strings.TrimSpace(loginID) != "" && strings.TrimSpace(loginID) != session.LoginID {
		return nil, fmt.Errorf("device login session changed; refresh and retry")
	}
	result, err := prismapi.PollDeviceLogin(session.APIURL, session.DeviceCode)
	if err != nil {
		m.setError(err)
		return nil, fmt.Errorf("poll device login failed: %w", err)
	}
	session.Status = firstNonEmpty(strings.TrimSpace(result.Status), session.Status)
	session.LastPolledAt = time.Now().UTC().Format(time.RFC3339)
	session.ExpiresIn = result.ExpiresIn
	if result.Interval > 0 {
		session.Interval = result.Interval
	}
	switch strings.ToLower(strings.TrimSpace(session.Status)) {
	case "approved":
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			return nil, fmt.Errorf("load config failed: %w", err)
		}
		if strings.TrimSpace(session.Region) != "" {
			cfg.SelectRegion(session.Region)
		}
		prismapi.ApplyApprovedDeviceLogin(cfg, result)
		if err := cfg.Save(configPath); err != nil {
			return nil, fmt.Errorf("save config failed: %w", err)
		}
		localPluginScan, scanErr := dashboardScanLocalPlugins(configPath)
		if scanErr != nil {
			localPluginScan = map[string]any{
				"ok":    false,
				"error": scanErr.Error(),
			}
		}
		// Bundled installation can update plugin directories. Use the persisted
		// configuration when deriving the account payload.
		cfg, err = bridgeconfig.Load(configPath)
		if err != nil {
			return nil, fmt.Errorf("reload config after plugin scan: %w", err)
		}
		hubApply := restartHub(configPath, dbPath, "login_changed")
		m.clear(session.LoginID)
		payload := map[string]any{
			"ok":                true,
			"approved":          true,
			"status":            "approved",
			"approved_at":       time.Now().UTC().Format(time.RFC3339),
			"hub_apply":         hubApply,
			"local_plugin_scan": localPluginScan,
			"account": map[string]any{
				"account_id": cfg.Account.AccountID,
				"user_id":    cfg.Account.UserID,
				"name":       cfg.Account.Name,
				"host_id":    cfg.Account.HostID,
				"host_alias": cfg.Account.HostAlias,
			},
		}
		if scanErr != nil {
			payload["local_plugin_scan_error"] = scanErr.Error()
		}
		return payload, nil
	case "expired", "denied", "failed", "cancelled":
		session.Pending = false
		session.Message = "登录流程已结束，请重新发起登录。"
		m.clear(session.LoginID)
		return map[string]any{
			"ok":       true,
			"approved": false,
			"status":   session.Status,
			"login":    session,
		}, nil
	default:
		session.Pending = true
		session.Message = "等待本地 Dashboard 完成授权"
		m.store(session)
		return map[string]any{
			"ok":       true,
			"approved": false,
			"status":   session.Status,
			"login":    session,
		}, nil
	}
}

func (m *dashboardLoginManager) PasswordLogin(configPath, dbPath, explicitRegion, explicitAPIURL, username, password, hostAlias string) (map[string]any, error) {
	return m.passwordLogin(configPath, dbPath, explicitRegion, explicitAPIURL, username, password, hostAlias, requestDashboardHubRestart)
}

func (m *dashboardLoginManager) passwordLogin(configPath, dbPath, explicitRegion, explicitAPIURL, username, password, hostAlias string, restartHub dashboardRestartFunc) (map[string]any, error) {
	cfg, err := performAccountPasswordLogin(configPath, explicitRegion, explicitAPIURL, username, password, hostAlias)
	if err != nil {
		return nil, err
	}
	// A fresh Desktop intentionally has no local Plugin. The Host binding and
	// Plugin installation are separate user actions; login must never download
	// or install an adapter as a side effect.
	cfg, reloadErr := bridgeconfig.Load(configPath)
	if reloadErr != nil {
		return nil, fmt.Errorf("reload config after plugin scan: %w", reloadErr)
	}
	_ = writeDaemonRuntimeState(daemonRuntimeStatePath(), daemonRuntimeState{
		ConnectionState:  "disconnected",
		ConnectionDetail: "登录成功，可重新连接服务器。",
	})
	hubApply := restartHub(configPath, dbPath, "login_changed")
	m.clear("")
	payload := map[string]any{
		"ok":          true,
		"approved":    true,
		"status":      "approved",
		"approved_at": time.Now().UTC().Format(time.RFC3339),
		"hub_apply":   hubApply,
		"hub_debug": map[string]any{
			"hub_running":         hubRunning(hubLockPath()),
			"runtime_state_path":  daemonRuntimeStatePath(),
			"host_token_summary":  dashboardTokenSummary(cfg.Prism.HostToken),
			"account_host_id":     cfg.Account.HostID,
			"account_login_at":    cfg.Account.LoginAt,
			"account_bound_at":    cfg.Account.BoundAt,
			"bridge_enabled":      cfg.Bridge.Enabled,
			"runtime_state_after": dashboardRuntimeStateSnapshot(daemonRuntimeStatePath()),
		},
		"account": map[string]any{
			"account_id": cfg.Account.AccountID,
			"user_id":    cfg.Account.UserID,
			"name":       cfg.Account.Name,
			"host_id":    cfg.Account.HostID,
			"host_alias": cfg.Account.HostAlias,
		},
	}
	return payload, nil
}

func (m *dashboardLoginManager) Logout(configPath, dbPath string) (map[string]any, error) {
	return m.logout(configPath, dbPath, requestDashboardHubRestart)
}

func (m *dashboardLoginManager) logout(configPath, dbPath string, restartHub dashboardRestartFunc) (map[string]any, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}
	cfg.ClearActiveIdentity()
	if err := cfg.Save(configPath); err != nil {
		return nil, fmt.Errorf("save config failed: %w", err)
	}
	_ = removeDaemonRuntimeState(daemonRuntimeStatePath())
	hubApply := restartHub(configPath, dbPath, "logout_changed")
	m.clear("")
	payload := map[string]any{
		"ok":            true,
		"logged_out":    true,
		"logged_out_at": time.Now().UTC().Format(time.RFC3339),
		"hub_apply":     hubApply,
	}
	return payload, nil
}

func (m *dashboardLoginManager) store(login *dashboardLogin) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.device = cloneDashboardLogin(login)
}

func (m *dashboardLoginManager) clear(loginID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.TrimSpace(loginID) != "" && m.device != nil && strings.TrimSpace(m.device.LoginID) != strings.TrimSpace(loginID) {
		return
	}
	m.device = nil
}

func (m *dashboardLoginManager) setError(err error) {
	if m == nil || err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.device == nil {
		return
	}
	m.device.Error = err.Error()
	m.device.LastPolledAt = time.Now().UTC().Format(time.RFC3339)
}

func cloneDashboardLogin(login *dashboardLogin) *dashboardLogin {
	if login == nil {
		return nil
	}
	cloned := *login
	cloned.Pending = !dashboardLoginTerminal(cloned.Status)
	if strings.EqualFold(strings.TrimSpace(cloned.Status), "approved") {
		cloned.Pending = false
	}
	return &cloned
}

func dashboardLoginTerminal(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "approved", "expired", "denied", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func serveDashboardUI(configPath, dbPath, addr, title string, open bool, stdout, stderr io.Writer) int {
	return serveDashboardUIContext(context.Background(), configPath, dbPath, addr, title, open, stdout, stderr)
}

func serveDashboardUIContext(ctx context.Context, configPath, dbPath, addr, title string, open bool, stdout, stderr io.Writer) int {
	return serveDashboardUIContextWithOptions(ctx, configPath, dbPath, addr, title, open, dashboardServerOptions{}, stdout, stderr)
}

func serveDashboardUIContextWithOptions(ctx context.Context, configPath, dbPath, addr, title string, open bool, options dashboardServerOptions, stdout, stderr io.Writer) int {
	listener, err := net.Listen("tcp", strings.TrimSpace(addr))
	if err != nil {
		fmt.Fprintf(stderr, "start hub failed: %v\n", err)
		return 1
	}
	defer listener.Close()

	page, err := template.ParseFS(dashboardAssets, "dashboard_assets/index.html")
	if err != nil {
		fmt.Fprintf(stderr, "load dashboard template failed: %v\n", err)
		return 1
	}
	translationsSource, err := dashboardAssets.ReadFile("dashboard_assets/translations.csv")
	if err != nil {
		fmt.Fprintf(stderr, "load dashboard translations failed: %v\n", err)
		return 1
	}
	translations, err := dashboardTranslationJSON(translationsSource)
	if err != nil {
		fmt.Fprintf(stderr, "validate dashboard translations failed: %v\n", err)
		return 1
	}
	loginManager := newDashboardLoginManager()
	catalogOperations := newDashboardCatalogOperations()
	if err := dashboardRecoverPluginUninstalls(configPath, options.PluginInstaller); err != nil {
		fmt.Fprintf(stderr, "恢复未完成的插件卸载事务失败: %v\n", err)
	}
	restartHub := options.RestartHub
	if restartHub == nil {
		restartHub = requestDashboardHubRestart
	}
	hubUIURL := "http://" + listener.Addr().String()
	statusCache := newDashboardStatusCache(configPath, dbPath)
	statusCache.Refresh()
	// Full process/CDP scans are demand-driven. With no Dashboard requests,
	// an idle Hub does not continuously refresh this presentation cache.

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		_ = page.Execute(w, dashboardPageData{
			Title:             title,
			Translations:      translations,
			HubVersion:        buildinfo.Version(),
			RegisterURL:       buildinfo.AccountWebURL("register"),
			ForgotPasswordURL: buildinfo.AccountWebURL("forget"),
		})
	})
	mux.HandleFunc("/assets/panel_logo.png", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		asset, readErr := dashboardAssets.ReadFile("dashboard_assets/panel_logo.png")
		if readErr != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(asset)
	})
	mux.HandleFunc("/assets/prism_logo_nobg@2x.png", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		asset, readErr := dashboardAssets.ReadFile("dashboard_assets/prism_logo_nobg@2x.png")
		if readErr != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(asset)
	})
	mux.HandleFunc("/assets/without_plugin.png", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		asset, readErr := dashboardAssets.ReadFile("dashboard_assets/without_plugin.png")
		if readErr != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(asset)
	})
	mux.HandleFunc("/assets/remote-panel-logo.svg", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		asset, readErr := dashboardAssets.ReadFile("dashboard_assets/remote-panel-logo.svg")
		if readErr != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(asset)
	})
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		payload, ok := statusCache.Snapshot()
		lightweight := r.URL.Query().Get("lightweight") == "1"
		if r.URL.Query().Get("refresh") == "1" || (!ok && !lightweight) || payload.CheckedAt == "" {
			payload = statusCache.Refresh()
		}
		payload.HubRunning = true
		payload.HubReachable = true
		payload.HubPID = os.Getpid()
		payload.SidecarGeneration = strings.TrimSpace(os.Getenv("PRISM_SIDECAR_GENERATION"))
		payload.HubURL = hubUIURL
		if runtimeState, err := readDaemonRuntimeState(daemonRuntimeStatePath()); err == nil && runtimeState != nil {
			applyDashboardRuntimeConnection(&payload, runtimeState)
		}
		// Tray monitoring reads live transport/quota projections without starting
		// another process/CDP scan while the main window is hidden.
		if lightweight {
			codexUsageWatch.mu.Lock()
			usage := codexUsageWatch.value
			codexUsageWatch.mu.Unlock()
			if card, ok := payload.DesktopApps["codex"].(map[string]any); ok && usage != nil {
				cards := make(map[string]any, len(payload.DesktopApps))
				for id, value := range payload.DesktopApps {
					cards[id] = value
				}
				updated := make(map[string]any, len(card)+1)
				for key, value := range card {
					updated[key] = value
				}
				updated["agent_usage"] = usage
				cards["codex"] = updated
				payload.DesktopApps = cards
			}
		}
		payload.Login = loginManager.Snapshot()
		payload.Plugins = dashboardMergeRegistryPlugins(payload.Plugins, options.PluginRegistry, options.PluginInstaller, catalogOperations)
		if options.Doctor != nil {
			summary := options.Doctor.Summary()
			payload.Doctor = &summary
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/doctor/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if options.Doctor == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "doctor unavailable")
			return
		}
		writeDashboardJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"doctor": options.Doctor.Report(),
		})
	})
	mux.HandleFunc("/api/doctor/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if options.Doctor == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "doctor unavailable")
			return
		}
		report, err := options.Doctor.RunAll(r.Context())
		if err != nil {
			writeDashboardError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		summary := options.Doctor.Summary()
		writeDashboardJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"doctor":  report,
			"summary": summary,
		})
	})
	mux.HandleFunc("/api/doctor/findings/", func(w http.ResponseWriter, r *http.Request) {
		if options.Doctor == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "doctor unavailable")
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/api/doctor/findings/")
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			writeDashboardError(w, http.StatusNotFound, "finding not found")
			return
		}
		findingID := parts[0]
		switch parts[1] {
		case "dismiss":
			if r.Method != http.MethodPost {
				writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := options.Doctor.Dismiss(r.Context(), findingID); err != nil {
				writeDashboardError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeDashboardJSON(w, http.StatusOK, map[string]any{"ok": true})
		case "remedies":
			// POST /api/doctor/findings/{id}/remedies/{rid}
			if len(parts) != 3 || parts[2] == "" {
				writeDashboardError(w, http.StatusNotFound, "remedy not found")
				return
			}
			if r.Method != http.MethodPost {
				writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			var req struct {
				Confirmed bool `json:"confirmed"`
			}
			if r.Body != nil {
				_ = json.NewDecoder(r.Body).Decode(&req)
			}
			event, err := options.Doctor.ExecuteRemedy(r.Context(), findingID, parts[2], req.Confirmed)
			if err != nil {
				writeDashboardError(w, http.StatusBadRequest, err.Error())
				return
			}
			// Remedies can relaunch managed desktop apps or restart local
			// runtimes; polling readers must not keep the pre-remedy snapshot.
			refreshDashboardStatusCacheSoon(statusCache)
			writeDashboardJSON(w, http.StatusOK, map[string]any{
				"ok":    true,
				"event": event,
			})
		default:
			writeDashboardError(w, http.StatusNotFound, "unsupported finding action")
		}
	})
	mux.HandleFunc("/api/doctor/history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if options.Doctor == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "doctor unavailable")
			return
		}
		limit := 50
		if parsed, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit"))); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		}
		writeDashboardJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"events": options.Doctor.Events(limit),
		})
	})
	mux.HandleFunc("/api/connection/refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		// A Hub cannot mint a replacement Host token itself. When the server
		// reports an expired or rejected token, make the existing login flow the
		// explicit next action instead of restarting a doomed session.
		status := collectServingHubDashboardStatus(configPath, dbPath)
		payload := dashboardManualConnectionRefresh(status, func() map[string]any {
			return restartHub(configPath, dbPath, "manual_connection_refresh")
		})
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/login/device/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Mode string `json:"mode"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		login, err := loginManager.StartDevice(configPath, "", "", req.Mode)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeDashboardJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"login": login,
		})
	})
	mux.HandleFunc("/api/login/device/poll", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			LoginID string `json:"login_id"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		payload, err := loginManager.pollDevice(configPath, dbPath, req.LoginID, restartHub)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/login/password", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Username  string `json:"username"`
			Password  string `json:"password"`
			HostAlias string `json:"host_alias"`
			Region    string `json:"region"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		payload, err := loginManager.passwordLogin(configPath, dbPath, strings.TrimSpace(req.Region), "", strings.TrimSpace(req.Username), req.Password, strings.TrimSpace(req.HostAlias), restartHub)
		if err != nil {
			writeDashboardPasswordLoginError(w, err)
			return
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/logout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		payload, err := loginManager.logout(configPath, dbPath, restartHub)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/plugins/install", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Source   string `json:"source"`
			Root     string `json:"root"`
			Activate *bool  `json:"activate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		activate := true
		if req.Activate != nil {
			activate = *req.Activate
		}
		payload, code := installPluginFromSource(configPath, strings.TrimSpace(req.Root), strings.TrimSpace(req.Source), activate, io.Discard)
		if code != 0 || payload["ok"] != true {
			writeDashboardError(w, http.StatusBadRequest, fmt.Sprint(payload["error"]))
			return
		}
		payload["hub_apply"] = dashboardPluginReloaderApply("plugin_installed")
		refreshDashboardStatusCacheSoon(statusCache)
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/plugins/scan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		payload, err := dashboardScanLocalPlugins(configPath)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		payload["hub_apply"] = dashboardPluginReloaderApply("plugins_scanned")
		refreshDashboardStatusCacheSoon(statusCache)
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/plugins/catalog", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		payload, err := dashboardPluginCatalogFromRegistry(configPath, options.PluginRegistry, options.PluginInstaller, catalogOperations)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/plugins/catalog/refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if options.PluginRegistry == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "plugin registry unavailable")
			return
		}
		snapshot := options.PluginRegistry.Refresh(r.Context())
		writeDashboardJSON(w, http.StatusOK, map[string]any{"ok": true, "catalog": snapshot})
	})
	mux.HandleFunc("/api/plugins/catalog/install", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID string `json:"plugin_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		operation, err := dashboardStartCatalogOperation(ctx, options.PluginRegistry, options.PluginInstaller, req.PluginID)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		catalogOperations.Track(operation.PluginID, operation.ID)
		writeDashboardJSON(w, http.StatusAccepted, map[string]any{"ok": true, "operation_id": operation.ID, "plugin_id": operation.PluginID, "state": operation.State})
	})
	mux.HandleFunc("/api/plugins/catalog/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID string `json:"plugin_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		operation, err := dashboardStartCatalogOperation(ctx, options.PluginRegistry, options.PluginInstaller, req.PluginID)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		catalogOperations.Track(operation.PluginID, operation.ID)
		writeDashboardJSON(w, http.StatusAccepted, map[string]any{"ok": true, "operation_id": operation.ID, "plugin_id": operation.PluginID, "state": operation.State})
	})
	mux.HandleFunc("/api/plugins/operations/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if options.PluginInstaller == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "plugin installer unavailable")
			return
		}
		operationID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/plugins/operations/"))
		operation, ok := options.PluginInstaller.GetOperation(operationID)
		if !ok {
			writeDashboardError(w, http.StatusNotFound, "plugin operation not found")
			return
		}
		writeDashboardJSON(w, http.StatusOK, map[string]any{
			"ok": true, "operation_id": operation.ID, "plugin_id": operation.PluginID,
			"version": operation.Version, "state": operation.State, "error": operation.Error,
			"downloaded_bytes": operation.DownloadedBytes, "download_total_bytes": operation.DownloadTotalBytes,
			"started_at": operation.StartedAt, "updated_at": operation.UpdatedAt,
		})
	})
	mux.HandleFunc("/api/plugins/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID string `json:"plugin_id"`
			Root     string `json:"root"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		pluginID := strings.TrimSpace(req.PluginID)
		// 先脱离当前运行时，再执行云端确认和本地事务，避免运行中的进程锁住安装目录。
		var runtimeCloseErr error
		if reloader := currentDashboardRuntimePluginReloader(); reloader != nil {
			runtimeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			runtimeCloseErr = reloader.RemovePlugin(runtimeCtx, pluginID)
			cancel()
		}
		payload, err := dashboardUninstallPluginStrict(configPath, strings.TrimSpace(req.Root), pluginID, options.PluginInstaller, dashboardPluginUninstallRemoteAPI())
		if err != nil {
			if runtimeCloseErr != nil {
				err = fmt.Errorf("%w（运行时脱离失败: %v）", err, runtimeCloseErr)
			}
			writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("插件卸载失败: %v", err))
			return
		}
		// 事务完成后重新扫描运行时，并立即刷新状态缓存，页面无需等待定时刷新周期。
		var reloaderReloadErr error
		if reloader := currentDashboardRuntimePluginReloader(); reloader != nil {
			reloadCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			reloaderReloadErr = reloader.Reload(reloadCtx)
			cancel()
		}
		payload["hub_apply"] = dashboardPluginReloaderApply("plugin_removed")
		statusCache.Refresh()
		if runtimeCloseErr != nil {
			payload["runtime_close_warning"] = runtimeCloseErr.Error()
		}
		if reloaderReloadErr != nil {
			payload["reloader_warning"] = reloaderReloadErr.Error()
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			payload := collectDashboardStatus(configPath, dbPath)
			writeDashboardJSON(w, http.StatusOK, map[string]any{
				"ok":         payload.OK,
				"checked_at": payload.CheckedAt,
				"config":     payload.Config,
				"warnings":   payload.Warnings,
				"error":      payload.Error,
			})
		case http.MethodPost:
			var req struct {
				Region        *string                                     `json:"region"`
				HostToken     *string                                     `json:"host_token"`
				BridgeEnabled *bool                                       `json:"bridge_enabled"`
				KeepAwake     *bool                                       `json:"keep_awake"`
				PluginEnv     map[string]string                           `json:"plugin_env"`
				PluginConfig  map[string]map[string]string                `json:"plugin_config"`
				PluginModes   map[string]string                           `json:"plugin_modes"`
				DesktopApps   map[string]dashboardDesktopAppConfigRequest `json:"desktop_apps"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeDashboardError(w, http.StatusBadRequest, "invalid json body")
				return
			}
			cfg, err := bridgeconfig.Load(configPath)
			if err != nil {
				writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("load config failed: %v", err))
				return
			}
			if req.Region != nil {
				cfg.SelectRegion(strings.TrimSpace(*req.Region))
			}
			if req.HostToken != nil {
				cfg.Prism.HostToken = strings.TrimSpace(*req.HostToken)
			}
			if req.BridgeEnabled != nil {
				cfg.Bridge.Enabled = *req.BridgeEnabled
			}
			if req.KeepAwake != nil {
				cfg.Bridge.KeepAwake = *req.KeepAwake
			}
			if req.PluginEnv != nil {
				cfg.Bridge.PluginEnv = bridgeconfig.NormalizePluginEnv(req.PluginEnv)
			}
			if req.PluginConfig != nil {
				if cfg.Bridge.Plugins == nil {
					cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
				}
				for pluginID, env := range req.PluginConfig {
					key, resolveErr := dashboardPluginRuntimeID(cfg, pluginID)
					if resolveErr != nil {
						writeDashboardError(w, http.StatusBadRequest, resolveErr.Error())
						return
					}
					pluginConfig, existed := cfg.Bridge.Plugins[key]
					pluginConfig.Env = bridgeconfig.NormalizePluginEnv(env)
					persistDashboardPluginConfig(cfg, key, pluginConfig, existed)
				}
			}
			if req.PluginModes != nil {
				if cfg.Bridge.Plugins == nil {
					cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
				}
				for pluginID, mode := range req.PluginModes {
					plugin, resolveErr := dashboardPluginManifest(cfg, pluginID)
					if resolveErr != nil {
						writeDashboardError(w, http.StatusBadRequest, resolveErr.Error())
						return
					}
					mode = strings.ToLower(strings.TrimSpace(mode))
					if !dashboardPluginAllowsRuntimeMode(plugin.Manifest, mode) {
						writeDashboardError(w, http.StatusBadRequest, fmt.Sprintf("plugin %s does not declare runtime mode %q", plugin.Manifest.ID, mode))
						return
					}
					key := plugin.Manifest.ID
					pluginConfig, existed := cfg.Bridge.Plugins[key]
					pluginConfig.Mode = mode
					persistDashboardPluginConfig(cfg, key, pluginConfig, existed)
				}
			}
			if req.DesktopApps != nil {
				applyDashboardDesktopAppOverrides(cfg, req.DesktopApps)
			}
			if err := cfg.Save(configPath); err != nil {
				writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("save config failed: %v", err))
				return
			}
			hubApply := dashboardApplyConfigSaveWithRestart(configPath, dbPath, req.Region != nil || req.HostToken != nil || req.BridgeEnabled != nil || req.KeepAwake != nil || req.PluginConfig != nil || req.PluginModes != nil || req.DesktopApps != nil, restartHub)
			if fmt.Sprint(hubApply["action"]) != "desktop_restart_scheduled" {
				// A scheduled sidecar restart rebuilds the cache in the
				// replacement process; only live-applied saves need it here.
				refreshDashboardStatusCacheSoon(statusCache)
			}
			payload := map[string]any{
				"ok":          true,
				"saved_at":    time.Now().UTC().Format(time.RFC3339),
				"config_path": configPath,
				"hub_apply":   hubApply,
				"config": map[string]any{
					"region":         cfg.Prism.Region,
					"host_token_set": strings.TrimSpace(cfg.Prism.HostToken) != "",
					"bridge_enabled": cfg.Bridge.Enabled,
					"keep_awake":     cfg.Bridge.KeepAwake,
					"plugin_env":     cfg.Bridge.PluginEnv,
					"plugin_config":  dashboardPluginConfig(cfg),
					"plugin_modes":   dashboardPluginModes(cfg),
				},
			}
			writeDashboardJSON(w, http.StatusOK, payload)
		default:
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	mux.HandleFunc("/api/host/profile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Alias string `json:"alias"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		alias := strings.TrimSpace(req.Alias)
		if alias == "" {
			writeDashboardError(w, http.StatusBadRequest, "alias is required")
			return
		}
		if len(alias) > 64 {
			writeDashboardError(w, http.StatusBadRequest, "alias too long (max 64 chars)")
			return
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("load config failed: %v", err))
			return
		}
		if strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
			writeDashboardError(w, http.StatusBadRequest, "account session or host binding missing; please log in again from desktop")
			return
		}
		// Cloud is authoritative for the alias shown on the dashboard.
		// Apply the local config first so a transient cloud failure still
		// leaves a usable local override, then push the same value up.
		cfg.Account.HostAlias = alias
		if err := cfg.Save(configPath); err != nil {
			writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("save config failed: %v", err))
			return
		}
		_, cloudErr := prismapi.UpdateUserHostProfile(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, alias)
		payload := map[string]any{
			"ok":           true,
			"alias":        alias,
			"host_id":      cfg.Account.HostID,
			"cloud_synced": cloudErr == nil,
		}
		if cloudErr != nil {
			payload["cloud_error"] = cloudErr.Error()
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/lifecycle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		payload, err := handleDashboardLifecycleActionWithRestart(configPath, dbPath, req.Action, restartHub)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/host/plugins", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			payload, err := loadDashboardHostPlugins(configPath)
			if err != nil {
				if dashboardHostPluginRequiresReauth(err) {
					writeDashboardJSON(w, http.StatusUnauthorized, map[string]any{
						"ok":      false,
						"action":  "reauth_required",
						"message": "当前设备绑定不属于已登录账户，请重新登录并绑定此设备。",
					})
					return
				}
				writeDashboardError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeDashboardJSON(w, http.StatusOK, payload)
		default:
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	mux.HandleFunc("/api/host/plugins/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID string `json:"plugin_id"`
			Enabled  *bool  `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("load config failed: %v", err))
			return
		}
		if strings.TrimSpace(req.PluginID) == "" || req.Enabled == nil {
			writeDashboardError(w, http.StatusBadRequest, "plugin_id and enabled required")
			return
		}
		pluginID, err := dashboardPluginRuntimeID(cfg, strings.TrimSpace(req.PluginID))
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		payload, err := applyDashboardHostPluginChange(cfg, pluginID, *req.Enabled)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		// API Host Plugin state is authoritative; the running reloader absorbs
		// the change without restarting the Desktop-owned sidecar.
		payload["hub_apply"] = dashboardPluginReloaderApply("host_plugin_changed")
		refreshDashboardStatusCacheSoon(statusCache)
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/host/plugins/metadata", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID    string `json:"plugin_id"`
			DisplayName string `json:"display_name"`
			Description string `json:"description"`
			IconURL     string `json:"icon_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			writeDashboardError(w, http.StatusInternalServerError, fmt.Sprintf("load config failed: %v", err))
			return
		}
		pluginID, err := dashboardPluginRuntimeID(cfg, strings.TrimSpace(req.PluginID))
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		payload, err := updateDashboardHostPluginMetadata(cfg, pluginID, req.DisplayName, req.Description, req.IconURL)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		refreshDashboardStatusCacheSoon(statusCache)
		writeDashboardJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/api/plugins/terminal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			PluginID       string `json:"plugin_id"`
			Cwd            string `json:"cwd"`
			ConversationID string `json:"conversation_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDashboardError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		pluginID := strings.ToLower(strings.TrimSpace(req.PluginID))
		service := currentDashboardRuntimePlugins()
		if pluginID == "" || service == nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "Hub plugin runtime is unavailable")
			return
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			writeDashboardError(w, http.StatusServiceUnavailable, "Hub configuration is unavailable")
			return
		}
		manifest, err := dashboardPluginManifest(cfg, pluginID)
		if err != nil || !manifest.Manifest.AllowsManagedTerminal(manifest.Manifest.RuntimeMode(cfg.Bridge.Plugins[pluginID].Mode)) {
			writeDashboardError(w, http.StatusBadRequest, "plugin does not support a managed terminal in its current runtime mode")
			return
		}
		adapter := service.Adapter(pluginID)
		launcher, ok := adapter.(pluginruntime.ManagedTerminalLauncher)
		if !ok {
			writeDashboardError(w, http.StatusBadRequest, "plugin does not support a managed terminal in its current runtime mode")
			return
		}
		callCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		capability, err := adapter.Probe(callCtx)
		if err != nil || !capability.Available || !capability.CanOpenManagedTerminal {
			writeDashboardError(w, http.StatusBadRequest, "plugin does not support a managed terminal in its current runtime mode")
			return
		}
		terminalRequest, err := resolveManagedTerminalRequest(callCtx, dbPath, pluginID, req.Cwd, req.ConversationID)
		if err != nil {
			writeDashboardError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, err := launcher.OpenManagedTerminal(callCtx, terminalRequest)
		if err != nil || !result.OK {
			message := "open managed terminal failed"
			if err != nil {
				message = err.Error()
			} else if strings.TrimSpace(result.Message) != "" {
				message = result.Message
			}
			writeDashboardError(w, http.StatusBadRequest, message)
			return
		}
		writeDashboardJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("/api/desktop/", func(w http.ResponseWriter, r *http.Request) {
		appID := strings.TrimPrefix(r.URL.Path, "/api/desktop/")
		appID = strings.TrimSpace(appID)
		if appID == "" {
			writeDashboardError(w, http.StatusBadRequest, "desktop app id required")
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Action string `json:"action"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeDashboardError(w, http.StatusBadRequest, "invalid json body")
				return
			}
			payload, err := handleDashboardDesktopAction(configPath, appID, req.Action)
			if err != nil {
				writeDashboardError(w, http.StatusBadRequest, err.Error())
				return
			}
			// The managed launch stops and relaunches the desktop app; the
			// cache's CDP snapshot still describes the pre-launch state.
			refreshDashboardStatusCacheSoon(statusCache)
			writeDashboardJSON(w, http.StatusOK, payload)
		default:
			writeDashboardError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	fmt.Fprintf(stdout, "Prism Hub listening: %s\n", hubUIURL)
	if open {
		if err := openBrowser(hubUIURL); err != nil {
			fmt.Fprintf(stderr, "open browser failed: %v\n", err)
		}
	}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "dashboard stopped: %v\n", err)
		return 1
	}
	return 0
}

func requestDashboardHubRestart(configPath, dbPath, reason string) map[string]any {
	return map[string]any{
		"requested":   true,
		"applied":     false,
		"action":      "desktop_restart_required",
		"reason":      reason,
		"hub_running": hubRunning(hubLockPath()),
		"message":     "Prism Desktop is responsible for restarting the Hub.",
	}
}

// Installed plugins are replaced by the Hub's running reloader. They do not
// require a Desktop sidecar restart.
func dashboardPluginReloaderApply(reason string) map[string]any {
	return map[string]any{
		"requested": false,
		"applied":   false,
		"action":    "reloader_watching",
		"reason":    reason,
	}
}

type dashboardReleaseProber struct{}

func (dashboardReleaseProber) Probe(ctx context.Context, root string, manifest pluginpkg.Manifest) (plugininstall.ProbeResult, error) {
	manifestPath, err := manifestInDir(root)
	if err != nil {
		return plugininstall.ProbeResult{}, err
	}
	// The probe process is a short-lived verification instance staged beside
	// the still-active runtime during an update. It must not claim the
	// per-plugin runtime lease: the lease file identifies the one long-lived
	// Hub-owned process, and two processes claiming it terminate each other.
	adapter := pluginpkg.NewProcessAdapter(pluginpkg.Discovered{Path: manifestPath, Root: root, Manifest: manifest, SkipRuntimeLease: true})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adapter.Close(closeCtx)
	}()
	capability, err := adapter.ProbeStrict(ctx)
	if err != nil {
		return plugininstall.ProbeResult{}, err
	}
	return plugininstall.ProbeResult{
		PluginID:        capability.PluginID,
		ProtocolVersion: pluginregistry.DefaultProtocolVersion,
		Available:       capability.Available,
	}, nil
}

func dashboardStartCatalogOperation(ctx context.Context, registry *pluginregistry.Manager, installer *plugininstall.Installer, pluginID string) (plugininstall.Operation, error) {
	if registry == nil || installer == nil {
		return plugininstall.Operation{}, fmt.Errorf("plugin registry or installer unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return plugininstall.Operation{}, fmt.Errorf("plugin_id required")
	}
	var selected *pluginregistry.Item
	for _, item := range registry.Snapshot().Items {
		if item.ID == pluginID {
			copy := item
			selected = &copy
			break
		}
	}
	if selected == nil {
		// 冷启动且 catalog 缓存为空时，后台刷新可能尚未完成。先从内置
		// 官方白名单解析插件，再沿用下面的单次 Refresh 获取最新 Release，
		// 避免页面已经显示插件但立即点击安装却得到 not found。
		for _, entry := range pluginregistry.OfficialCatalog() {
			if entry.ID == pluginID {
				selected = &pluginregistry.Item{Entry: entry, Compatible: true, Supported: true}
				break
			}
		}
	}
	if selected == nil {
		return plugininstall.Operation{}, fmt.Errorf("official plugin not found: %s", pluginID)
	}
	if !selected.Compatible {
		return plugininstall.Operation{}, fmt.Errorf("plugin %s requires a newer Hub", pluginID)
	}
	if !selected.Supported {
		return plugininstall.Operation{}, fmt.Errorf("plugin %s does not support the current platform", pluginID)
	}
	if selected.Release == nil {
		// A cached snapshot can predate a just-published release — for
		// example a 404 cached before the plugin's first release existed.
		// Refresh once before failing so a fresh release is installable
		// without waiting out the periodic catalog interval or a manual
		// refresh.
		refreshed := registry.Refresh(ctx)
		for _, item := range refreshed.Items {
			if item.ID == pluginID {
				copy := item
				selected = &copy
				break
			}
		}
	}
	if selected.Release == nil {
		detail := selected.Error
		if detail == "" {
			detail = "no stable GitHub release has been resolved for this plugin yet"
		}
		return plugininstall.Operation{}, fmt.Errorf("plugin %s has no installable stable release: %s", pluginID, detail)
	}
	return installer.StartOperation(ctx, plugininstall.Request{
		Entry:            selected.Entry,
		Release:          selected.Release,
		ExpectedProtocol: pluginregistry.DefaultProtocolVersion,
		Prober:           dashboardReleaseProber{},
	})
}

func dashboardRegistryItems(registry *pluginregistry.Manager) []pluginregistry.Item {
	if registry != nil {
		if items := registry.Snapshot().Items; len(items) > 0 {
			return items
		}
	}
	entries := pluginregistry.OfficialCatalog()
	items := make([]pluginregistry.Item, 0, len(entries))
	for _, entry := range entries {
		items = append(items, pluginregistry.Item{Entry: entry, Compatible: true, Supported: true})
	}
	return items
}

func dashboardMergeRegistryPlugins(installed []map[string]any, registry *pluginregistry.Manager, installer *plugininstall.Installer, operations *dashboardCatalogOperations) []map[string]any {
	registryItems := map[string]pluginregistry.Item{}
	for _, item := range dashboardRegistryItems(registry) {
		registryItems[item.ID] = item
	}
	for _, pluginItem := range installed {
		pluginID := strings.ToLower(strings.TrimSpace(fmt.Sprint(pluginItem["plugin_id"])))
		catalogItem, ok := registryItems[pluginID]
		if !ok {
			continue
		}
		installedVersion := strings.TrimSpace(fmt.Sprint(pluginItem["version"]))
		pluginItem["installed_version"] = installedVersion
		pluginItem["latest_version"] = catalogItem.LatestVersion
		// Keep the official colourful avatar separate from the manifest URL so
		// the Dashboard can prefer it by default without overwriting a user's
		// cloud-hosted icon_url override.
		pluginItem["catalog_icon_url"] = catalogItem.IconURL
		pluginItem["manifest_icon_url"] = firstNonEmpty(strings.TrimSpace(fmt.Sprint(pluginItem["icon_url"])), catalogItem.IconURL)
		pluginItem["icon_svg_url"] = firstNonEmpty(strings.TrimSpace(fmt.Sprint(pluginItem["icon_svg_url"])), catalogItem.IconSVGURL)
		pluginItem["compatible"] = catalogItem.Compatible
		pluginItem["supported"] = catalogItem.Supported
		pluginItem["last_checked_at"] = catalogItem.LastCheckedAt
		pluginItem["update_available"] = catalogItem.Compatible && catalogItem.Supported && catalogItem.LatestVersion != "" && pluginregistry.CompareSemVer(catalogItem.LatestVersion, installedVersion) > 0
		if installer != nil {
			if state, err := installer.State(pluginID); err == nil && state.LegacyOfficial {
				pluginItem["legacy_official"] = true
			}
			if operationID := operations.ID(pluginID); operationID != "" {
				if operation, exists := installer.GetOperation(operationID); exists {
					pluginItem["operation_id"] = operation.ID
					pluginItem["install_state"] = operation.State
				}
			}
		}
	}
	return installed
}

func dashboardPluginCatalogFromRegistry(configPath string, registry *pluginregistry.Manager, installer *plugininstall.Installer, operations *dashboardCatalogOperations) (map[string]any, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load prism config: %w", err)
	}
	installed := dashboardMergeRegistryPlugins(dashboardPlugins(pluginDirs(cfg), cfg), registry, installer, operations)
	installedIDs := make(map[string]bool, len(installed))
	for _, item := range installed {
		installedIDs[strings.ToLower(strings.TrimSpace(fmt.Sprint(item["plugin_id"])))] = true
	}
	library := make([]map[string]any, 0)
	for _, item := range dashboardRegistryItems(registry) {
		if installedIDs[item.ID] {
			continue
		}
		row := map[string]any{
			"plugin_id": item.ID, "display_name": item.DisplayName, "description": item.Description,
			"icon_url": item.IconURL, "catalog_icon_url": item.IconURL, "icon_svg_url": item.IconSVGURL, "latest_version": item.LatestVersion,
			"version": item.LatestVersion, "compatible": item.Compatible, "supported": item.Supported,
			"last_checked_at": item.LastCheckedAt, "install_state": "not_installed",
		}
		if item.Error != "" {
			row["error"] = item.Error
		}
		if installer != nil {
			if operationID := operations.ID(item.ID); operationID != "" {
				if operation, exists := installer.GetOperation(operationID); exists {
					row["operation_id"] = operation.ID
					row["install_state"] = operation.State
				}
			}
		}
		library = append(library, row)
	}
	sort.Slice(library, func(i, j int) bool { return fmt.Sprint(library[i]["plugin_id"]) < fmt.Sprint(library[j]["plugin_id"]) })
	checkedAt := time.Time{}
	if registry != nil {
		checkedAt = registry.Snapshot().CheckedAt
	}
	return map[string]any{"ok": true, "installed": installed, "library": library, "checked_at": checkedAt}, nil
}

func dashboardScanLocalPlugins(configPath string) (map[string]any, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load prism config: %w", err)
	}
	dirs := pluginDirs(cfg)
	discovered, err := pluginpkg.ScanDirs(dirs)
	if err != nil {
		return nil, fmt.Errorf("scan local plugins: %w", err)
	}
	pluginIDs := make([]string, 0, len(discovered))
	for _, item := range discovered {
		if pluginID := strings.TrimSpace(item.Manifest.ID); pluginID != "" {
			pluginIDs = append(pluginIDs, pluginID)
		}
	}
	sort.Strings(pluginIDs)
	return map[string]any{
		"ok":                   true,
		"plugin_count":         len(pluginIDs),
		"plugin_ids":           pluginIDs,
		"plugin_dirs":          dirs,
		"installed_plugin_ids": []string{},
	}, nil
}

func dashboardApplyConfigSave(configPath, dbPath string, restartRequired bool) map[string]any {
	return dashboardApplyConfigSaveWithRestart(configPath, dbPath, restartRequired, requestDashboardHubRestart)
}

func dashboardApplyConfigSaveWithRestart(configPath, dbPath string, restartRequired bool, restartHub dashboardRestartFunc) map[string]any {
	if restartRequired {
		return restartHub(configPath, dbPath, "configuration_changed")
	}
	return map[string]any{"requested": false, "applied": true, "action": "not_required"}
}

func dashboardConfigTokenSummary(cfg *bridgeconfig.Config) map[string]any {
	if cfg == nil {
		return dashboardTokenSummary("")
	}
	return dashboardTokenSummary(cfg.Prism.HostToken)
}

func dashboardTokenSummary(token string) map[string]any {
	trimmed := strings.TrimSpace(token)
	return map[string]any{
		"present": trimmed != "",
		"length":  len(trimmed),
		"suffix":  tokenSuffix(trimmed, 8),
	}
}

func tokenSuffix(value string, size int) string {
	value = strings.TrimSpace(value)
	if value == "" || size <= 0 {
		return ""
	}
	if len(value) <= size {
		return value
	}
	return value[len(value)-size:]
}

func dashboardRuntimeStateSnapshot(path string) map[string]any {
	state, err := readDaemonRuntimeState(path)
	if err != nil {
		return map[string]any{"read_error": err.Error()}
	}
	if state == nil {
		return map[string]any{"present": false}
	}
	return map[string]any{
		"present":           true,
		"updated_at":        state.UpdatedAt,
		"hub_pid":           state.DaemonPID,
		"connection_state":  state.ConnectionState,
		"connection_label":  state.ConnectionLabel,
		"connection_detail": state.ConnectionDetail,
		"hub_running_now":   hubRunning(hubLockPath()),
	}
}

func dashboardOfficialDesktopApps() []dashboardDesktopAppSpec {
	return []dashboardDesktopAppSpec{
		{
			AppID:          "codex",
			PluginID:       "codex",
			DisplayName:    "Codex",
			AppEnvKey:      "PRISM_CODEX_APP_PATH",
			UserDataEnvKey: "PRISM_CODEX_USER_DATA_DIR",
			MacQuitScripts: []string{`tell application "Codex" to quit`, `tell application "ChatGPT" to quit`},
			LinuxCommand:   "codex",
		},
		{
			AppID:          "hermes",
			PluginID:       "hermes",
			DisplayName:    "Hermes",
			AppEnvKey:      "PRISM_HERMES_APP_PATH",
			UserDataEnvKey: "PRISM_HERMES_USER_DATA_DIR",
			MacQuitScripts: []string{`tell application id "com.nousresearch.hermes" to quit`},
			LinuxCommand:   "hermes-desktop",
		},
	}
}

func dashboardDesktopAppSpecByID(appID string) (dashboardDesktopAppSpec, bool) {
	appID = strings.ToLower(strings.TrimSpace(appID))
	for _, item := range dashboardOfficialDesktopApps() {
		if item.AppID == appID {
			return item, true
		}
	}
	return dashboardDesktopAppSpec{}, false
}

var dashboardHubReachable = hubReachable

func collectDashboardStatus(configPath, dbPath string) dashboardStatus {
	return collectDashboardStatusWithHubProbe(configPath, dbPath, true)
}

// collectServingHubDashboardStatus is used only by the Hub's own HTTP
// handlers. The process is already serving this request, so probing its
// listener again would recursively invoke the same handler.
func collectServingHubDashboardStatus(configPath, dbPath string) dashboardStatus {
	return collectDashboardStatusWithHubProbe(configPath, dbPath, false)
}

func collectDashboardStatusWithHubProbe(configPath, dbPath string, probeHub bool) dashboardStatus {
	status := dashboardStatus{
		OK:         true,
		CheckedAt:  time.Now().UTC().Format(time.RFC3339),
		ConfigPath: configPath,
		DBPath:     dbPath,
	}
	if _, err := os.Stat(configPath); err == nil {
		status.ConfigExists = true
	}
	if _, err := os.Stat(dbPath); err == nil {
		status.DBExists = true
	}
	status.HubRunning = hubRunning(hubLockPath())
	status.LocalDeviceName, _ = os.Hostname()
	status.LocalDeviceAddress = dashboardDeviceAddress(hubDefaultAddr())
	if status.HubRunning {
		status.HubPID = readPIDFromLockFile(hubLockPath())
		if probeHub {
			status.HubReachable = dashboardHubReachable(hubDefaultAddr())
		} else {
			status.HubReachable = true
		}
		status.HubURL = hubURLForAddr(hubDefaultAddr())
	}
	runtimeState, _ := readDaemonRuntimeState(daemonRuntimeStatePath())
	if runtimeState != nil {
		status.HostPluginSyncState = runtimeState.HostPluginSyncState
		status.HostPluginSyncDetail = runtimeState.HostPluginSyncDetail
		status.HostPluginSyncUpdatedAt = runtimeState.HostPluginSyncUpdated
		status.HostPluginCount = runtimeState.HostPluginCount
	}

	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		// A broken config must not take the whole status payload down: the
		// Dashboard still needs enough state to route the user to the doctor
		// panel. The hub.config_parse finding carries the damage details.
		status.OK = false
		status.Error = err.Error()
		status.LoginCommand = dashboardLoginCommand("cn")
		runtimeState = normalizeDashboardRuntimeState(&status, runtimeState)
		status.LifecycleState, status.LifecycleLabel = dashboardLifecycle(status)
		status.ConnectionState, status.ConnectionLabel, status.ConnectionDetail = dashboardConnection(status, runtimeState)
		status.RecommendedAction = dashboardRecommendedAction(status)
		status.Warnings = dashboardWarnings(nil, status)
		return status
	}
	status.PluginDirs = pluginDirs(cfg)
	status.Region = cfg.Prism.Region
	status.ResolvedAPIURL = cfg.Prism.APIURL()
	status.ResolvedRealtime = cfg.Prism.RealtimeURL()
	status.HostTokenSet = strings.TrimSpace(cfg.Prism.HostToken) != ""
	status.AccountTokenSet = strings.TrimSpace(cfg.Account.AccessToken) != ""
	status.LoggedIn = status.HostTokenSet && strings.TrimSpace(cfg.Account.AccountID) != ""
	status.RemoteControlReady = status.HostTokenSet
	status.BridgeEnabled = cfg.Bridge.Enabled
	status.KeepAwakeEnabled = cfg.Bridge.KeepAwake
	keepAwakeCapability := host.DetectKeepAwakeCapability()
	status.KeepAwakeSupported = keepAwakeCapability.Supported
	status.KeepAwakeMethod = keepAwakeCapability.Method
	status.LoginCommand = dashboardLoginCommand(cfg.Prism.Region)
	status.CanRestartHub = status.HubRunning && status.HostTokenSet && status.BridgeEnabled
	runtimeState = normalizeDashboardRuntimeState(&status, runtimeState)
	status.LifecycleState, status.LifecycleLabel = dashboardLifecycle(status)
	status.ConnectionState, status.ConnectionLabel, status.ConnectionDetail = dashboardConnection(status, runtimeState)
	if status.ConnectionState == "auth_expired" || status.ConnectionState == "unauthorized" || !status.LoggedIn {
		// A persisted token is not enough to expose account-owned Host operations.
		// The Dashboard must not query plugins while it is asking the user to log in.
		status.RemoteControlReady = false
	}
	status.RecommendedAction = dashboardRecommendedAction(status)
	status.Account = map[string]any{
		"account_id":          cfg.Account.AccountID,
		"user_id":             cfg.Account.UserID,
		"name":                cfg.Account.Name,
		"access_token_set":    status.AccountTokenSet,
		"agent_info":          cfg.Account.AgentInfo,
		"host_id":             cfg.Account.HostID,
		"host_alias":          cfg.Account.HostAlias,
		"install_id":          cfg.Account.InstallID,
		"machine_fingerprint": cfg.Account.MachineFingerprint,
		"login_at":            cfg.Account.LoginAt,
		"bound_at":            cfg.Account.BoundAt,
	}
	status.Config = map[string]any{
		"region":         cfg.Prism.Region,
		"host_token_set": status.HostTokenSet,
		"bridge_enabled": cfg.Bridge.Enabled,
		"keep_awake":     cfg.Bridge.KeepAwake,
		"plugin_env":     cfg.Bridge.PluginEnv,
		"plugin_config":  dashboardPluginConfig(cfg),
		"plugin_modes":   dashboardPluginModes(cfg),
	}
	discovered, discoveryErr := pluginpkg.ScanDirs(status.PluginDirs)
	status.Plugins = dashboardPluginsFromDiscovered(discovered, cfg, discoveryErr)
	managedPlugins := make(map[string]bool, len(discovered))
	for _, plugin := range discovered {
		managedPlugins[strings.ToLower(strings.TrimSpace(plugin.Manifest.ID))] = plugin.Manifest.ManagedLaunch
	}
	status.DesktopApps = dashboardDesktopAppsWithManagedPlugins(cfg, status.PluginDirs, managedPlugins)
	status.Warnings = dashboardWarnings(cfg, status)
	return status
}

func dashboardDeviceAddress(addr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil || strings.TrimSpace(host) == "" {
		host = strings.TrimSpace(addr)
	}
	host = strings.Trim(host, "[]")
	if host == "127.0.0.1" || host == "localhost" || host == "::1" {
		return "127.0.0.1 · localhost"
	}
	return host
}

// manifestDesktopAppID resolves the desktop app a manifest's launch specs
// point at (kind=desktop-app, app defaulting to the plugin id). Empty when the
// manifest declares no desktop-app target.
func manifestDesktopAppID(manifest pluginpkg.Manifest) string {
	for _, spec := range manifest.ManagedLaunchSpecs {
		if spec.Kind != pluginpkg.LaunchKindDesktopApp {
			continue
		}
		if app := strings.TrimSpace(spec.App); app != "" {
			return app
		}
		return manifest.ID
	}
	return ""
}

// dashboardDesktopAppSpecsForPlugins joins installed manifests (which declare
// desktop-app launch targets) with the hub desktop app registry (which owns
// the machine-level app knowledge). Membership is manifest-driven; the
// registry only supplies specs for apps a manifest actually references.
func dashboardDesktopAppSpecsForPlugins(pluginDirs []string) []dashboardDesktopAppSpec {
	discovered, err := pluginpkg.ScanDirs(pluginDirs)
	if err != nil && len(discovered) == 0 {
		return nil
	}
	out := make([]dashboardDesktopAppSpec, 0, len(discovered))
	for _, item := range discovered {
		appID := manifestDesktopAppID(item.Manifest)
		if appID == "" {
			continue
		}
		if spec, ok := dashboardDesktopAppSpecByID(appID); ok {
			out = append(out, spec)
		}
	}
	return out
}

// autoDetectedDesktopAppEnv resolves the desktop app path and user data
// directory for a plugin that declares a desktop-app launch spec, using the
// Hub's own installation detection (known install paths and Microsoft Store
// package lookup). Plugins receive these through the environment contract so
// a Hub install works without manual PRISM_*_APP_PATH configuration; explicit
// configuration always wins at the call site.
func autoDetectedDesktopAppEnv(manifest pluginpkg.Manifest) map[string]string {
	appID := strings.ToLower(strings.TrimSpace(manifestDesktopAppID(manifest)))
	if appID == "" {
		return nil
	}
	spec, ok := dashboardDesktopAppSpecByID(appID)
	if !ok {
		return nil
	}
	out := make(map[string]string, 2)
	if appPath := strings.TrimSpace(dashboardDesktopAppPath(spec)); appPath != "" {
		out[spec.AppEnvKey] = appPath
	}
	if userDataDir := strings.TrimSpace(dashboardDesktopProfileDir(spec)); userDataDir != "" {
		out[spec.UserDataEnvKey] = userDataDir
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func dashboardDesktopApps(cfg *bridgeconfig.Config, pluginDirs []string) map[string]any {
	return dashboardDesktopAppsWithManagedPlugins(cfg, pluginDirs, nil)
}

func dashboardDesktopAppsWithManagedPlugins(cfg *bridgeconfig.Config, pluginDirs []string, managedPlugins map[string]bool) map[string]any {
	out := map[string]any{}
	for _, item := range dashboardDesktopAppSpecsForPlugins(pluginDirs) {
		managed, known := managedPlugins[strings.ToLower(strings.TrimSpace(item.PluginID))]
		if !known {
			managed = dashboardPluginManagedLaunch(pluginDirs, item.PluginID)
		}
		out[item.AppID] = dashboardDesktopAppStatusWithManagedLaunch(cfg, pluginDirs, item, managed)
	}
	return out
}

func dashboardCodexDesktopStatus(cfg *bridgeconfig.Config, pluginDirs []string) map[string]any {
	if spec, ok := dashboardDesktopAppSpecByID("codex"); ok {
		return dashboardDesktopAppStatus(cfg, pluginDirs, spec)
	}
	return nil
}

func dashboardDesktopAppStatus(cfg *bridgeconfig.Config, pluginDirs []string, spec dashboardDesktopAppSpec) map[string]any {
	return dashboardDesktopAppStatusWithManagedLaunch(cfg, pluginDirs, spec, dashboardPluginManagedLaunch(pluginDirs, spec.PluginID))
}

func dashboardDesktopAppStatusWithManagedLaunch(cfg *bridgeconfig.Config, pluginDirs []string, spec dashboardDesktopAppSpec, managed bool) map[string]any {
	pluginEnv := map[string]string{}
	if cfg != nil && cfg.Bridge.Plugins != nil {
		pluginEnv = bridgeconfig.NormalizePluginEnv(cfg.Bridge.Plugins[spec.PluginID].Env)
	}
	globalEnv := map[string]string{}
	if cfg != nil {
		globalEnv = bridgeconfig.NormalizePluginEnv(cfg.Bridge.PluginEnv)
	}
	appPathOverride := firstNonEmptyMap(pluginEnv, spec.AppEnvKey)
	if appPathOverride == "" {
		appPathOverride = firstNonEmptyMap(globalEnv, spec.AppEnvKey)
	}
	appPathAuto := dashboardDesktopAppPath(spec)
	appPath := firstNonEmpty(appPathOverride, appPathAuto)
	userDataDirOverride := firstNonEmptyMap(pluginEnv, spec.UserDataEnvKey)
	if userDataDirOverride == "" {
		userDataDirOverride = firstNonEmptyMap(globalEnv, spec.UserDataEnvKey)
	}
	userDataDirAuto := dashboardDesktopProfileDir(spec)
	userDataDir := firstNonEmpty(userDataDirOverride, userDataDirAuto)
	devtoolsFile := filepath.Join(userDataDir, "DevToolsActivePort")
	processes, processErr := dashboardDesktopProcessesWithError(spec)
	running := len(processes) > 0
	cdpPort, cdpReachable := dashboardManagedCDPStatus(spec, userDataDir, devtoolsFile)
	status := "stopped"
	processDetectionError := ""
	if processErr != nil {
		status = "process_detection_unavailable"
		processDetectionError = processErr.Error()
	} else if running && cdpReachable {
		status = "ready"
	} else if running {
		status = "running_unmanaged"
	}
	appLabel := dashboardDetectedHostApp(spec, appPath)
	out := map[string]any{
		"app_id":                       spec.AppID,
		"display_name":                 spec.DisplayName,
		"managed_enabled":              managed,
		"app_path":                     appPath,
		"app_path_override":            appPathOverride,
		"app_path_auto":                appPathAuto,
		"app_path_source":              dashboardValueSource(appPathOverride),
		"user_data_dir":                userDataDir,
		"user_data_dir_override":       userDataDirOverride,
		"user_data_dir_auto":           userDataDirAuto,
		"user_data_dir_source":         dashboardValueSource(userDataDirOverride),
		"devtools_file":                devtoolsFile,
		"running":                      running,
		"cdp_reachable":                cdpReachable,
		"cdp_port":                     cdpPort,
		"process_count":                len(processes),
		"processes":                    processes,
		"detected_host_app":            appLabel,
		"status":                       status,
		"can_launch_managed":           managed && appPath != "" && (runtimeGOOS() == "darwin" || runtimeGOOS() == "windows") && processErr == nil,
		"process_detection_error":      processDetectionError,
		"launch_action_label":          dashboardCodexLaunchLabel(status),
		"managed_status_label":         dashboardDesktopManagedStatusLabel(status),
		"managed_status_message":       dashboardDesktopManagedStatusMessage(spec, status, appLabel, cdpPort),
		"managed_restart_required":     dashboardDesktopManagedRestartRequired(status),
		"managed_confirmation_message": dashboardDesktopManagedConfirmationMessage(spec, status, appLabel),
	}
	// Rate-limit usage is a read-only account diagnostic. It projects Codex's
	// five-hour and weekly windows without retaining the raw account response.
	// It is probed directly against the bundled Codex App Server (same source as the plugin),
	// cached and refreshed in the background so /api/status is never blocked.
	if spec.AppID == "codex" {
		if usage := dashboardCodexUsage(); usage != nil {
			out["agent_usage"] = usage
		}
	}
	return out
}

func dashboardDesktopProfileDir(spec dashboardDesktopAppSpec) string {
	home, _ := os.UserHomeDir()
	switch runtimeGOOS() {
	case "darwin":
		switch spec.AppID {
		case "hermes":
			return filepath.Join(home, "Library", "Application Support", "Hermes")
		default:
			return filepath.Join(home, "Library", "Application Support", "Codex")
		}
	case "windows":
		appData := strings.TrimSpace(os.Getenv("APPDATA"))
		if appData == "" {
			appData = strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
		}
		base := appData
		if base == "" {
			base = filepath.Join(home, "AppData", "Roaming")
		}
		switch spec.AppID {
		case "hermes":
			return filepath.Join(base, "Hermes")
		default:
			if dashboardWindowsStoreCodexAppPath() != "" {
				return filepath.Join(base, "Codex", "web", "Codex")
			}
			return filepath.Join(base, "Codex")
		}
	default:
		xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
		base := xdg
		if base == "" {
			base = filepath.Join(home, ".config")
		}
		switch spec.AppID {
		case "hermes":
			return filepath.Join(base, "Hermes")
		default:
			return filepath.Join(base, "Codex")
		}
	}
}

func dashboardDesktopAppPath(spec dashboardDesktopAppSpec) string {
	switch runtimeGOOS() {
	case "darwin":
		switch spec.AppID {
		case "hermes":
			home, _ := os.UserHomeDir()
			managed := filepath.Join(home, ".hermes", "hermes-agent", "apps", "desktop", "release", "mac-arm64", "Hermes.app", "Contents", "MacOS", "Hermes")
			if _, err := os.Stat(managed); err == nil {
				return managed
			}
			return "/Applications/Hermes.app/Contents/MacOS/Hermes-Setup"
		default:
			return dashboardCodexAppPath()
		}
	case "windows":
		localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
		programFiles := strings.TrimSpace(os.Getenv("ProgramFiles"))
		switch spec.AppID {
		case "hermes":
			return firstNonEmpty(
				chooseNonEmptyPath(localAppData, "Programs", "Hermes", "Hermes.exe"),
				chooseNonEmptyPath(programFiles, "Hermes", "Hermes.exe"),
			)
		default:
			return dashboardCodexAppPath()
		}
	default:
		switch spec.AppID {
		case "hermes":
			return firstNonEmpty(strings.TrimSpace(os.Getenv("PRISM_HERMES_LINUX_COMMAND")), spec.LinuxCommand, "hermes")
		default:
			return firstNonEmpty(strings.TrimSpace(os.Getenv("PRISM_CODEX_LINUX_COMMAND")), spec.LinuxCommand, "codex")
		}
	}
}

func chooseNonEmptyPath(base string, parts ...string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return ""
	}
	all := append([]string{base}, parts...)
	return filepath.Join(all...)
}

func dashboardDetectedHostApp(spec dashboardDesktopAppSpec, appPath string) string {
	if spec.AppID == "codex" && strings.Contains(strings.ToLower(appPath), "chatgpt") {
		return "ChatGPT"
	}
	if spec.AppID == "codex" {
		return "Codex"
	}
	return spec.DisplayName
}

func dashboardValueSource(override string) string {
	if strings.TrimSpace(override) != "" {
		return "configured"
	}
	return "auto"
}

func dashboardPluginManagedLaunch(pluginDirs []string, pluginID string) bool {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return false
	}
	discovered, err := pluginpkg.ScanDirs(pluginDirs)
	if err != nil && len(discovered) == 0 {
		return false
	}
	for _, item := range discovered {
		if strings.EqualFold(strings.TrimSpace(item.Manifest.ID), pluginID) {
			// The desktop card's launch gate is the manifest's desktop-app
			// launch spec, not the managed_launch env-contract bool.
			return manifestDesktopAppID(item.Manifest) != ""
		}
	}
	return false
}

func dashboardCodexLaunchLabel(status string) string {
	switch status {
	case "ready":
		return "重新托管启动"
	case "running_unmanaged":
		return "接管当前实例"
	case "process_detection_unavailable":
		return "暂不能启动"
	default:
		return "启动托管实例"
	}
}

func dashboardDesktopManagedStatusLabel(status string) string {
	switch status {
	case "ready":
		return "已由 Prism 托管"
	case "running_unmanaged":
		return "当前是手动启动实例"
	case "process_detection_unavailable":
		return "无法确认桌面实例状态"
	default:
		return "等待托管启动"
	}
}

func dashboardDesktopManagedStatusMessage(spec dashboardDesktopAppSpec, status, appLabel, cdpPort string) string {
	// The host bundle can be ChatGPT.app while the Prism agent is Codex. Keep
	// the detected host in diagnostics, but do not let it rename the agent.
	appName := spec.DisplayName
	switch status {
	case "ready":
		if strings.TrimSpace(cdpPort) != "" {
			return fmt.Sprintf("%s 当前已由 Prism 托管，并已暴露 CDP 端口 %s。", appName, strings.TrimSpace(cdpPort))
		}
		return fmt.Sprintf("%s 当前已由 Prism 托管，并已暴露可用的 CDP 端口。", appName)
	case "running_unmanaged":
		return fmt.Sprintf("检测到正在运行的 %s，但它没有暴露 Prism 需要的 CDP 端口。若要使用远程控制，需要切换到 Prism 托管启动。", appName)
	case "process_detection_unavailable":
		return fmt.Sprintf("Prism 无法确认 %s 当前是否运行。为避免启动第二个实例，暂时禁用托管启动。", appName)
	default:
		return fmt.Sprintf("当前未检测到运行中的 %s。需要从这里启动托管实例后，Prism 才能通过 CDP 控制。", appName)
	}
}

func dashboardDesktopManagedRestartRequired(status string) bool {
	switch status {
	case "ready", "running_unmanaged":
		return true
	default:
		return false
	}
}

func dashboardDesktopManagedConfirmationMessage(spec dashboardDesktopAppSpec, status, appLabel string) string {
	if !dashboardDesktopManagedRestartRequired(status) {
		return ""
	}
	appName := spec.DisplayName
	switch status {
	case "running_unmanaged":
		return fmt.Sprintf("%s 当前不是 Prism 托管实例，切换后会先重启当前应用，正在进行的对话可能被打断。确认继续吗？", appName)
	default:
		return fmt.Sprintf("这会重启当前由 Prism 托管的 %s 实例，正在进行的对话可能被打断。确认继续吗？", appName)
	}
}

func firstNonEmptyMap(values map[string]string, key string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[key])
}

func dashboardCodexProfileDir() string {
	spec, _ := dashboardDesktopAppSpecByID("codex")
	return dashboardDesktopProfileDir(spec)
}

func dashboardCodexAppPath() string {
	switch runtimeGOOS() {
	case "darwin":
		if _, err := os.Stat("/Applications/Codex.app/Contents/MacOS/Codex"); err == nil {
			return "/Applications/Codex.app/Contents/MacOS/Codex"
		}
		if _, err := os.Stat("/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"); err == nil {
			return "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
		}
	case "windows":
		localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
		if localAppData != "" {
			if path := filepath.Join(localAppData, "Programs", "Codex", "Codex.exe"); fileExists(path) {
				return path
			}
		}
		if programFiles := strings.TrimSpace(os.Getenv("ProgramFiles")); programFiles != "" {
			if path := filepath.Join(programFiles, "Codex", "Codex.exe"); fileExists(path) {
				return path
			}
		}
		return dashboardWindowsStoreCodexAppPath()
	}
	return ""
}

var dashboardWindowsStoreCodexPath = func() ([]byte, error) {
	return exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$pkg=Get-AppxPackage -Name OpenAI.Codex -ErrorAction SilentlyContinue; if($pkg){Join-Path $pkg.InstallLocation 'app\ChatGPT.exe'}`).Output()
}

var dashboardWindowsStoreCodexOnce sync.Once
var dashboardWindowsStoreCodexCached string

func dashboardWindowsStoreCodexAppPath() string {
	if runtimeGOOS() != "windows" {
		return ""
	}
	dashboardWindowsStoreCodexOnce.Do(func() {
		out, err := dashboardWindowsStoreCodexPath()
		if err == nil {
			dashboardWindowsStoreCodexCached = strings.TrimSpace(string(out))
		}
	})
	return dashboardWindowsStoreCodexCached
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dashboardDesktopAppBundle(appPath string) string {
	appPath = strings.TrimSpace(appPath)
	if appPath == "" {
		return ""
	}
	if strings.HasSuffix(appPath, ".app") {
		return appPath
	}
	marker := ".app/"
	index := strings.Index(appPath, marker)
	if index >= 0 {
		return appPath[:index+len(".app")]
	}
	return ""
}

func dashboardCodexAppBundle(appPath string) string {
	return dashboardDesktopAppBundle(appPath)
}

// dashboardCodexAppServerExecutable resolves the Codex App Server binary. The
// npm/PATH `codex` CLI does not share the ChatGPT desktop login and its
// app-server returns `-32600 chatgpt authentication required`; the desktop app
// bundles its own `codex` under Contents/Resources (macOS), which reuses
// ~/.codex/auth.json and authenticates.
func dashboardCodexAppServerExecutable() string {
	if override := strings.TrimSpace(os.Getenv("PRISM_CODEX_APP_SERVER_EXECUTABLE")); override != "" {
		return override
	}
	if runtime.GOOS == "darwin" {
		for _, bundle := range []string{"/Applications/Codex.app", "/Applications/ChatGPT.app"} {
			if cli := dashboardBundledCodexCli(bundle); cli != "" {
				return cli
			}
		}
		if bundle := dashboardCodexAppBundle(dashboardCodexAppPath()); bundle != "" {
			if cli := dashboardBundledCodexCli(bundle); cli != "" {
				return cli
			}
		}
	}
	return "codex"
}

// dashboardBundledCodexCli probes the bundled codex CLI layouts inside a macOS
// app bundle. ChatGPT desktop builds since late Sep 2026 ship
// `Contents/Resources/codex-cli/bin/codex` (a wrapper around CodexCLI.app);
// older builds ship a single `Contents/Resources/codex` file.
func dashboardBundledCodexCli(bundle string) string {
	for _, rel := range []string{
		filepath.Join("Contents", "Resources", "codex-cli", "bin", "codex"),
		filepath.Join("Contents", "Resources", "codex"),
	} {
		cli := filepath.Join(bundle, rel)
		if info, err := os.Stat(cli); err == nil && !info.IsDir() {
			return cli
		}
	}
	return ""
}

// codexUsageWatcher keeps ONE persistent Codex App Server connection for the
// dashboard's lifetime. Push notifications refresh immediately; a small
// periodic read prevents a quiet App Server connection from leaving the
// projection stale. It never respawns a process for a refresh.
type codexUsageWatcher struct {
	mu      sync.Mutex
	value   map[string]any
	started bool
}

var codexUsageWatch = &codexUsageWatcher{}

// dashboardCodexUsage returns the current five-hour and weekly projections. It never
// blocks: the first call lazily starts the background watcher; later calls
// just read the in-memory value that the watcher keeps fresh via push.
func dashboardCodexUsage() map[string]any {
	codexUsageWatch.start()
	codexUsageWatch.mu.Lock()
	defer codexUsageWatch.mu.Unlock()
	return codexUsageWatch.value
}

func (w *codexUsageWatcher) start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return
	}
	w.started = true
	go w.loop()
}

// loop reconnects after each disconnect, backing off on repeated fast failures
// (e.g. Codex not installed or not signed in) so it never spins hot.
func (w *codexUsageWatcher) loop() {
	const base, maxBackoff = 10 * time.Second, 60 * time.Second
	consecutive := 0
	for {
		ran := w.serve()
		if ran {
			consecutive = 0
		} else {
			consecutive++
		}
		wait := base * time.Duration(1<<min(consecutive, 5)) // 10s..up to max
		if wait > maxBackoff {
			wait = maxBackoff
		}
		time.Sleep(wait)
	}
}

// serve runs one app-server session. Push notifications trigger an immediate
// refresh; a single 60-second ticker is the fallback when no notification is
// emitted. Returns true if the session connected and ran long enough to be
// considered healthy (not a fast failure).
func (w *codexUsageWatcher) serve() bool {
	executable := dashboardCodexAppServerExecutable()
	cmd := exec.Command(executable, "app-server", "--listen", "stdio://")
	cmd.Stderr = nil
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return false
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false
	}
	started := time.Now()
	if err := cmd.Start(); err != nil {
		return false
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	stopScanner := make(chan struct{})
	defer close(stopScanner)
	lines := make(chan string)
	go func() {
		defer close(lines)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-stopScanner:
				return
			}
		}
	}()
	refreshTicker := time.NewTicker(time.Minute)
	defer refreshTicker.Stop()

	nextID := 1
	newID := func() int { id := nextID; nextID++; return id }
	pendingRead := 0 // id of the in-flight read, 0 when none
	pendingSince := time.Time{}

	write := func(msg map[string]any) bool {
		payload, err := json.Marshal(msg)
		if err != nil {
			return false
		}
		_, err = io.WriteString(stdin, string(payload)+"\n")
		return err == nil
	}
	sendRead := func() bool {
		id := newID()
		if !write(map[string]any{"id": id, "method": "account/rateLimits/read", "params": nil}) {
			return false
		}
		pendingRead = id
		pendingSince = time.Now()
		return true
	}
	adopt := func(msg map[string]any) {
		if msg["error"] != nil {
			return
		}
		result, _ := msg["result"].(map[string]any)
		if fresh := codexUsageFromRateLimitsResult(result); fresh != nil {
			w.mu.Lock()
			w.value = fresh
			w.mu.Unlock()
		}
	}

	initID := newID()
	if !write(map[string]any{"id": initID, "method": "initialize", "params": map[string]any{
		"clientInfo":   map[string]any{"name": "prism-dashboard", "version": "1.0.0"},
		"capabilities": map[string]any{"experimentalApi": true},
	}}) {
		return false
	}
	initialized := false
	for {
		select {
		case <-refreshTicker.C:
			// A read normally completes in milliseconds. If one gets stuck, let
			// the next minute retry rather than freezing the projection forever.
			if initialized && (pendingRead == 0 || time.Since(pendingSince) > 15*time.Second) {
				_ = sendRead()
			}
		case line, ok := <-lines:
			if !ok {
				return time.Since(started) > 5*time.Second
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var msg map[string]any
			if json.Unmarshal([]byte(line), &msg) != nil {
				continue
			}
			// Notification: the server says the limits changed -> re-read at most
			// one in flight, so a burst of notifications collapses to one refresh.
			if method, _ := msg["method"].(string); method == "account/rateLimits/updated" {
				if initialized && pendingRead == 0 {
					_ = sendRead()
				}
				continue
			}
			idStr := fmt.Sprint(msg["id"])
			switch idStr {
			case fmt.Sprint(initID):
				if msg["error"] != nil {
					return false
				}
				// `initialized` is a notification (no id); then prime the value.
				_ = write(map[string]any{"method": "initialized", "params": map[string]any{}})
				initialized = true
				_ = sendRead()
			case fmt.Sprint(pendingRead):
				pendingRead = 0
				pendingSince = time.Time{}
				adopt(msg)
			}
		}
	}
}

// codexUsageFromRateLimitsResult finds the five-hour (300 min) and weekly
// (10080 min) windows in either slot, mirroring the external Codex plugin. The same
// account can surface either window as primary or secondary.
func codexUsageFromRateLimitsResult(result map[string]any) map[string]any {
	candidates := []map[string]any{}
	if rl, ok := result["rateLimits"].(map[string]any); ok {
		candidates = append(candidates, rl)
	}
	if byID, ok := result["rateLimitsByLimitId"].(map[string]any); ok {
		for _, v := range byID {
			if m, ok := v.(map[string]any); ok {
				candidates = append(candidates, m)
			}
		}
	}
	usage := map[string]any{}
	for _, c := range candidates {
		for _, slot := range []string{"primary", "secondary"} {
			w, ok := c[slot].(map[string]any)
			if !ok {
				continue
			}
			mins := dashboardAnyToFloat(w["windowDurationMins"])
			used := dashboardAnyToFloat(w["usedPercent"])
			resets := dashboardAnyToInt64(w["resetsAt"])
			if (mins != 300 && mins != 10080) || used < 0 || resets <= 0 {
				continue
			}
			usedPercent := int(math.Round(math.Min(100, used)))
			resetsAtMillis := resets
			if resets < 10_000_000_000 {
				resetsAtMillis = resets * 1000
			}
			window := map[string]any{
				"used_percent":   usedPercent,
				"window_minutes": int64(mins),
				"resets_at":      resetsAtMillis,
			}
			if mins == 300 {
				usage["five_hour"] = window
			} else {
				usage["weekly"] = window
			}
		}
	}
	if len(usage) == 0 {
		return nil
	}
	usage["updated_at"] = time.Now().UnixMilli()
	return usage
}

func dashboardAnyToFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	}
	return -1
}

func dashboardAnyToInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return -1
}

func dashboardDesktopProcesses(spec dashboardDesktopAppSpec) []map[string]any {
	items, _ := dashboardDesktopProcessesWithError(spec)
	return items
}

// Keep process discovery independent of the user's login-shell PATH. The
// Dashboard runs as a sidecar and may not inherit Homebrew's ripgrep path.
var dashboardDesktopProcessSnapshot = func() ([]byte, error) {
	return exec.Command("ps", "ax", "-o", "pid=,command=").Output()
}

func dashboardDesktopProcessesWithError(spec dashboardDesktopAppSpec) ([]map[string]any, error) {
	switch runtimeGOOS() {
	case "darwin", "linux":
		out, err := dashboardDesktopProcessSnapshot()
		if err != nil {
			return nil, fmt.Errorf("list desktop processes: %w", err)
		}
		pattern, err := regexp.Compile(`(?i)` + dashboardDesktopProcessPattern(spec))
		if err != nil {
			return nil, fmt.Errorf("compile desktop process matcher: %w", err)
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		items := make([]map[string]any, 0, len(lines))
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			pid, _ := strconv.Atoi(fields[0])
			processCommand := strings.TrimSpace(strings.TrimPrefix(line, fields[0]))
			if dashboardDesktopProcessIsSelfInspection(processCommand) || !pattern.MatchString(processCommand) || !dashboardDesktopProcessIsMain(spec, processCommand) {
				continue
			}
			items = append(items, map[string]any{
				"pid":     pid,
				"command": processCommand,
			})
		}
		return items, nil
	case "windows":
		out, err := dashboardWindowsProcessSnapshot()
		if err != nil {
			return nil, fmt.Errorf("list desktop processes: %w", err)
		}
		var rows []struct {
			ProcessID      int    `json:"ProcessId"`
			Name           string `json:"Name"`
			CommandLine    string `json:"CommandLine"`
			ExecutablePath string `json:"ExecutablePath"`
		}
		if err := json.Unmarshal(out, &rows); err != nil {
			return nil, fmt.Errorf("decode Windows process snapshot: %w", err)
		}
		items := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			command := strings.TrimSpace(row.CommandLine)
			if row.ProcessID <= 0 || !dashboardWindowsDesktopProcessIsMain(spec, row.Name, row.ExecutablePath) {
				continue
			}
			items = append(items, map[string]any{"pid": row.ProcessID, "command": command})
		}
		return items, nil
	default:
		return nil, fmt.Errorf("desktop process discovery is unsupported on %s", runtimeGOOS())
	}
}

var dashboardWindowsProcessSnapshot = func() ([]byte, error) {
	// Filter inside the CIM query instead of enumerating every process and
	// filtering in PowerShell: the full enumeration takes seconds on machines
	// that run many Electron helper processes, and the managed launch polls it.
	// -InputObject keeps array serialization: piping would emit a bare object
	// when exactly one process matches, which the Go decoder does not accept.
	return exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$data = @(Get-CimInstance Win32_Process -Filter "Name='Codex.exe' OR Name='ChatGPT.exe' OR Name='Hermes.exe' OR Name='Codex Helper.exe' OR Name='ChatGPT Helper.exe' OR Name='Hermes Helper.exe'" | Select-Object ProcessId,Name,CommandLine,ExecutablePath); ConvertTo-Json -InputObject $data -Compress`).Output()
}

// dashboardWindowsExecutableBase extracts the lowercase executable base name
// from a Windows path. Both separators are handled explicitly so the helper
// (and its tests) behave identically on non-Windows hosts, where
// filepath.Base does not split backslash paths.
func dashboardWindowsExecutableBase(executable string) string {
	executable = strings.TrimSpace(executable)
	if index := strings.LastIndexAny(executable, `\/`); index >= 0 {
		executable = executable[index+1:]
	}
	return strings.ToLower(strings.TrimSuffix(executable, ".exe"))
}

func dashboardWindowsDesktopProcessIsMain(spec dashboardDesktopAppSpec, name, executable string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	base := dashboardWindowsExecutableBase(executable)
	switch spec.AppID {
	case "hermes":
		return name == "hermes.exe" || name == "hermes" || base == "hermes"
	default:
		return (name == "codex.exe" || name == "codex" || name == "chatgpt.exe" || name == "chatgpt") && (base == "" || base == "codex" || base == "chatgpt")
	}
}

func dashboardWindowsListeningPorts(pid int) ([]string, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("invalid process id")
	}
	command := fmt.Sprintf("Get-NetTCPConnection -State Listen -OwningProcess %d -ErrorAction Stop | Where-Object { $_.LocalAddress -in @('127.0.0.1','::1','::ffff:127.0.0.1') } | Select-Object -ExpandProperty LocalPort -Unique | ConvertTo-Json -Compress", pid)
	out, err := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command).Output()
	if err != nil {
		return nil, err
	}
	var values []string
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return nil, nil
	}
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(out, &values); err != nil {
			return nil, err
		}
	} else {
		var value any
		if err := json.Unmarshal(out, &value); err != nil {
			return nil, err
		}
		values = append(values, fmt.Sprint(value))
	}
	return values, nil
}

func dashboardDesktopProcessIsMain(spec dashboardDesktopAppSpec, command string) bool {
	command = strings.TrimSpace(command)
	lower := strings.ToLower(command)
	// Chromium helper processes (renderer, GPU, utility) always carry --type=.
	// The Microsoft Store ChatGPT build names every helper ChatGPT.exe, so the
	// executable name alone cannot tell helpers from the main process.
	if strings.Contains(lower, "--type=") {
		return false
	}
	switch spec.AppID {
	case "hermes":
		return regexp.MustCompile(`(?i)(?:^|/)Contents/MacOS/Hermes(?:$|\s+--)`).MatchString(command) ||
			regexp.MustCompile(`(?i)^hermes-desktop(?:$|\s+--)`).MatchString(command) ||
			regexp.MustCompile(`(?i)(?:^|[\\/])hermes-desktop\.exe(?:$|[\s"])`).MatchString(command)
	default:
		// Newer Codex Desktop builds run inside ChatGPT.app. Match its actual
		// main executable while excluding ChatGPT Helper renderer/GPU processes.
		return regexp.MustCompile(`(?i)(?:^|/)Contents/MacOS/(?:Codex|ChatGPT)(?:$|\s+--)`).MatchString(command) ||
			regexp.MustCompile(`(?i)^(?:Codex|ChatGPT)(?:$|\s+--)`).MatchString(command) ||
			// Windows installs (classic or Microsoft Store) run a Codex.exe /
			// ChatGPT.exe binary from an absolute path.
			regexp.MustCompile(`(?i)(?:^|[\\/])(?:Codex|ChatGPT)\.exe(?:$|[\s"])`).MatchString(command)
	}
}

func dashboardDesktopProcessIsSelfInspection(command string) bool {
	command = strings.ToLower(strings.TrimSpace(command))
	return strings.Contains(command, "ps ax -o pid=,command=") ||
		strings.HasPrefix(command, "rg -i ") ||
		strings.Contains(command, "prism-plugin-") ||
		strings.Contains(command, "prism-sidecar")
}

func dashboardDesktopProcessPattern(spec dashboardDesktopAppSpec) string {
	switch spec.AppID {
	case "hermes":
		return `Hermes(\.app| Helper| )|/Hermes$|/Contents/MacOS/Hermes|hermes-desktop`
	default:
		return `Codex(\.app| )|ChatGPT\.app|/Codex$|/Contents/MacOS/Codex|/Contents/MacOS/ChatGPT`
	}
}

func dashboardCodexProcesses() []map[string]any {
	spec, _ := dashboardDesktopAppSpecByID("codex")
	return dashboardDesktopProcesses(spec)
}

func dashboardCodexCDPStatus(devtoolsFile string) (string, bool) {
	b, err := os.ReadFile(devtoolsFile)
	if err != nil {
		return "", false
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return "", false
	}
	port := strings.TrimSpace(lines[0])
	return dashboardCDPStatusOnPort(port)
}

func dashboardCDPStatusOnPort(port string) (string, bool) {
	port = strings.TrimSpace(port)
	if port == "" {
		return "", false
	}
	client := &http.Client{Timeout: 1200 * time.Millisecond}
	resp, err := client.Get("http://127.0.0.1:" + port + "/json/list")
	if err != nil {
		return port, false
	}
	defer resp.Body.Close()
	return port, resp.StatusCode >= 200 && resp.StatusCode < 300
}

func dashboardLoopbackListeningPorts(lsofOutput string) []string {
	seen := map[string]bool{}
	ports := make([]string, 0)
	for _, line := range strings.Split(lsofOutput, "\n") {
		match := dashboardLoopbackListenerPattern.FindStringSubmatch(line)
		if len(match) != 2 || seen[match[1]] {
			continue
		}
		seen[match[1]] = true
		ports = append(ports, match[1])
	}
	return ports
}

func dashboardManagedCDPStatus(spec dashboardDesktopAppSpec, userDataDir, devtoolsFile string) (string, bool) {
	if port, reachable := dashboardCodexCDPStatus(devtoolsFile); reachable {
		return port, true
	}
	// The DevToolsActivePort file is a cache, not proof of state: a managed
	// instance can outlive this Hub (restart/upgrade) and Chromium removes the
	// file on some relaunches while its CDP listener stays alive. Rediscover
	// the port from the OS for every managed desktop app, so the health card
	// agrees with what the plugin can actually connect to. The guard below
	// keeps user-launched instances without debugging flags out: the command
	// line must advertise --remote-debugging-port and match the profile dir.
	for _, item := range dashboardDesktopProcesses(spec) {
		command := strings.TrimSpace(fmt.Sprint(item["command"]))
		if !strings.Contains(command, "--remote-debugging-port") || !dashboardDesktopProcessHasManagedProfile(command, userDataDir) {
			continue
		}
		pid, _ := strconv.Atoi(fmt.Sprint(item["pid"]))
		if pid <= 0 {
			continue
		}
		var ports []string
		if runtimeGOOS() == "windows" {
			ports, _ = dashboardWindowsListeningPorts(pid)
		} else {
			output, err := exec.Command("lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iTCP", "-sTCP:LISTEN").Output()
			if err != nil {
				continue
			}
			ports = dashboardLoopbackListeningPorts(string(output))
		}
		for _, port := range ports {
			if discoveredPort, reachable := dashboardCDPStatusOnPort(port); reachable {
				return discoveredPort, true
			}
		}
	}
	return "", false
}

// A managed launch is only useful after the Desktop CDP endpoint is actually
// reachable. Returning before then lets the mobile client race the Electron
// startup and permanently observe the plugin as unavailable.
func dashboardWaitForCDP(devtoolsFile string, attempts int, poll time.Duration, status func(string) (string, bool)) (string, bool) {
	if attempts < 1 {
		attempts = 1
	}
	var port string
	for attempt := 0; attempt < attempts; attempt++ {
		port, ready := status(devtoolsFile)
		if ready {
			return port, true
		}
		if attempt+1 < attempts && poll > 0 {
			time.Sleep(poll)
		}
	}
	return port, false
}

func codexProfileArtifacts(userDataDir string) []string {
	root := strings.TrimSpace(userDataDir)
	if root == "" {
		return nil
	}
	names := []string{
		"DevToolsActivePort",
		"SingletonCookie",
		"SingletonLock",
		"SingletonSocket",
		"RunningChromeVersion",
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, filepath.Join(root, name))
	}
	return out
}

func dashboardProfileArtifacts(userDataDir string) []string {
	return codexProfileArtifacts(userDataDir)
}

func cleanupDashboardCodexProfileArtifacts(userDataDir string) {
	cleanupDashboardProfileArtifacts(userDataDir)
}

func cleanupDashboardProfileArtifacts(userDataDir string) {
	root := strings.TrimSpace(userDataDir)
	if root == "" {
		return
	}
	for _, file := range dashboardProfileArtifacts(root) {
		_ = os.RemoveAll(file)
	}
}

func handleDashboardDesktopAction(configPath, appID, action string) (map[string]any, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}
	spec, ok := dashboardDesktopAppSpecByID(appID)
	if !ok {
		return nil, fmt.Errorf("unsupported desktop app: %s", appID)
	}
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "start", "restart", "launch":
		return dashboardStartManagedDesktopApp(cfg, spec)
	default:
		return nil, fmt.Errorf("unsupported desktop action: %s", action)
	}
}

func dashboardStartManagedCodex(cfg *bridgeconfig.Config) (map[string]any, error) {
	spec, _ := dashboardDesktopAppSpecByID("codex")
	return dashboardStartManagedDesktopApp(cfg, spec)
}

func dashboardStartManagedDesktopApp(cfg *bridgeconfig.Config, spec dashboardDesktopAppSpec) (map[string]any, error) {
	if runtimeGOOS() != "darwin" && runtimeGOOS() != "windows" {
		return nil, fmt.Errorf("managed %s launch is currently unsupported on %s", spec.DisplayName, runtimeGOOS())
	}
	// 同一时刻只能执行一次桌面托管切换，避免并发请求绕过旧实例退出检查。
	if !dashboardManagedDesktopLaunchMu.TryLock() {
		return nil, fmt.Errorf("正在切换到 Prism 托管的 %s，请等待当前操作完成", spec.DisplayName)
	}
	defer dashboardManagedDesktopLaunchMu.Unlock()
	pluginDirs := pluginDirs(cfg)
	status := dashboardDesktopAppStatus(cfg, pluginDirs, spec)
	if fmt.Sprint(status["managed_enabled"]) != "true" {
		return nil, fmt.Errorf("当前插件没有声明托管启动能力，不能执行托管操作")
	}
	appPath := strings.TrimSpace(fmt.Sprint(status["app_path"]))
	userDataDir := strings.TrimSpace(fmt.Sprint(status["user_data_dir"]))
	if appPath == "" {
		return nil, fmt.Errorf("%s app path not found", spec.DisplayName)
	}
	if userDataDir == "" {
		userDataDir = dashboardDesktopProfileDir(spec)
	}
	if err := stopDashboardDesktopProcesses(spec, userDataDir); err != nil {
		return nil, fmt.Errorf("stop existing %s before managed launch: %w", spec.DisplayName, err)
	}
	if err := os.MkdirAll(userDataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create user data dir failed: %w", err)
	}
	cleanupDashboardProfileArtifacts(userDataDir)
	args := dashboardManagedDesktopLaunchArgs(userDataDir)
	if runtime.GOOS == "windows" && dashboardIsWindowsStoreAppPath(appPath) {
		// A Microsoft Store (MSIX) executable cannot be started through
		// CreateProcess: Windows denies it with "Access is denied". Run the
		// packaged executable inside its package context instead so the
		// managed launch arguments (--remote-debugging-port) still apply.
		if err := dashboardLaunchWindowsStoreManagedApp(appPath, args); err != nil {
			return nil, fmt.Errorf("start managed Codex failed: %w", err)
		}
	} else {
		cmd := dashboardManagedDesktopCommand(appPath, args)
		cmd.Dir = string(filepath.Separator)
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start managed Codex failed: %w", err)
		}
		_ = cmd.Process.Release()
	}
	devtoolsFile := filepath.Join(userDataDir, "DevToolsActivePort")
	waitAttempts := 0
	port, reachable := dashboardWaitForCDP(
		devtoolsFile,
		dashboardManagedCDPReadyAttempts,
		dashboardManagedCDPReadyPoll,
		func(string) (string, bool) {
			waitAttempts++
			// Check the DevToolsActivePort file on every attempt: desktop
			// process enumeration is expensive on machines that run many
			// Electron helper processes (Windows CIM), so the full discovery
			// runs periodically and once more after the wait window.
			if port, ok := dashboardCodexCDPStatus(devtoolsFile); ok {
				return port, true
			}
			if waitAttempts%8 == 0 {
				return dashboardManagedCDPStatus(spec, userDataDir, devtoolsFile)
			}
			return "", false
		},
	)
	if !reachable {
		// One full fallback discovery in case Chromium dropped the file while
		// its CDP listener stayed up.
		port, reachable = dashboardManagedCDPStatus(spec, userDataDir, devtoolsFile)
	}
	if reachable {
		return map[string]any{
			"ok":            true,
			"action":        "start",
			"desktop_app":   spec.AppID,
			"app_path":      appPath,
			"user_data_dir": userDataDir,
			"cdp_port":      port,
			"started_at":    time.Now().UTC().Format(time.RFC3339),
			"message":       fmt.Sprintf("%s 已由 Prism 以托管模式启动。", spec.DisplayName),
		}, nil
	}
	return nil, fmt.Errorf("managed %s launch requested but CDP port was not observed", spec.DisplayName)
}

func stopDashboardCodexProcesses(userDataDir string) error {
	spec, _ := dashboardDesktopAppSpecByID("codex")
	return stopDashboardDesktopProcesses(spec, userDataDir)
}

// dashboardDesktopProcessPIDsForTermination 仅接收已通过应用主进程筛选的列表。
// 无论实例是否由 Prism 托管，主进程都可能占用应用单实例锁，必须被纳入终止目标。
func dashboardDesktopProcessPIDsForTermination(processes []map[string]any) []int {
	pids := make([]int, 0, len(processes))
	for _, item := range processes {
		pid, _ := strconv.Atoi(fmt.Sprint(item["pid"]))
		if pid <= 0 {
			continue
		}
		pids = append(pids, pid)
	}
	return pids
}

func stopDashboardDesktopProcesses(spec dashboardDesktopAppSpec, userDataDir string) error {
	// A failed process scan must never be interpreted as an empty process list;
	// that could start a second ChatGPT instance while Codex still owns the app.
	if _, err := dashboardDesktopProcessesWithError(spec); err != nil {
		return fmt.Errorf("cannot verify existing %s processes before managed launch: %w", spec.DisplayName, err)
	}
	if runtimeGOOS() == "windows" {
		processes, err := dashboardDesktopProcessesWithError(spec)
		if err != nil {
			return fmt.Errorf("cannot verify existing %s processes before managed launch: %w", spec.DisplayName, err)
		}
		for _, item := range processes {
			if !dashboardDesktopProcessBlocksManagedLaunch(spec, strings.TrimSpace(fmt.Sprint(item["command"])), userDataDir) {
				continue
			}
			pid, _ := strconv.Atoi(fmt.Sprint(item["pid"]))
			if pid <= 0 {
				continue
			}
			_ = exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", fmt.Sprintf("$p=Get-Process -Id %d -ErrorAction Stop; [void]$p.CloseMainWindow()", pid)).Run()
		}
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) {
			running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
			if err != nil {
				return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
			}
			if !running {
				cleanupDashboardProfileArtifacts(userDataDir)
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		processes, err = dashboardDesktopProcessesWithError(spec)
		if err != nil {
			return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
		}
		for _, item := range processes {
			if !dashboardDesktopProcessBlocksManagedLaunch(spec, strings.TrimSpace(fmt.Sprint(item["command"])), userDataDir) {
				continue
			}
			pid, _ := strconv.Atoi(fmt.Sprint(item["pid"]))
			if pid > 0 {
				_ = exec.Command("taskkill.exe", "/PID", strconv.Itoa(pid), "/T", "/F").Run()
			}
		}
		deadline = time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
			if err != nil {
				return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
			}
			if !running {
				cleanupDashboardProfileArtifacts(userDataDir)
				return nil
			}
			time.Sleep(250 * time.Millisecond)
		}
		return fmt.Errorf("existing instance is still running")
	}
	for _, script := range spec.MacQuitScripts {
		_ = exec.Command("/usr/bin/osascript", "-e", script).Run()
	}
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
		if err != nil {
			return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
		}
		if !running {
			cleanupDashboardProfileArtifacts(userDataDir)
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	processes, err := dashboardDesktopProcessesWithError(spec)
	if err != nil {
		return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
	}
	for _, pid := range dashboardDesktopProcessPIDsForTermination(processes) {
		process, err := os.FindProcess(pid)
		if err == nil {
			_ = process.Signal(os.Interrupt)
		}
	}
	secondDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(secondDeadline) {
		running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
		if err != nil {
			return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
		}
		if !running {
			cleanupDashboardProfileArtifacts(userDataDir)
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	processes, err = dashboardDesktopProcessesWithError(spec)
	if err != nil {
		return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
	}
	for _, pid := range dashboardDesktopProcessPIDsForTermination(processes) {
		process, err := os.FindProcess(pid)
		if err == nil {
			_ = process.Kill()
		}
	}
	finalDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(finalDeadline) {
		running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
		if err != nil {
			return fmt.Errorf("cannot verify %s shutdown before managed launch: %w", spec.DisplayName, err)
		}
		if !running {
			cleanupDashboardProfileArtifacts(userDataDir)
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("existing instance is still running")
}

func dashboardManagedDesktopLaunchArgs(userDataDir string) []string {
	return []string{
		"--user-data-dir=" + userDataDir,
		"--remote-debugging-port=0",
		"--no-first-run",
		"--no-default-browser-check",
	}
}

func dashboardManagedDesktopCommand(appPath string, args []string) *exec.Cmd {
	// 直接执行应用二进制，确保托管参数不会被 macOS LaunchServices 转发
	// 给仍在运行的旧实例。调用方已先停止该 Agent 的全部进程。
	return exec.Command(appPath, args...)
}

// dashboardIsWindowsStoreAppPath reports whether appPath points into a
// Microsoft Store (MSIX) package payload directory. Those executables cannot
// be started with CreateProcess: the OS refuses with "Access is denied".
func dashboardIsWindowsStoreAppPath(appPath string) bool {
	if runtimeGOOS() != "windows" {
		return false
	}
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(appPath), "/", `\`))
	return strings.Contains(normalized, `\windowsapps\`)
}

// dashboardLaunchWindowsStoreManagedApp starts a packaged (MSIX) desktop app
// with managed launch arguments inside its package context.
// Invoke-CommandInDesktopPackage is the documented way to run a packaged
// executable with custom arguments; a direct CreateProcess of a WindowsApps
// binary is denied by the OS.
func dashboardLaunchWindowsStoreManagedApp(appPath string, args []string) error {
	script, err := dashboardWindowsStoreLaunchScript(appPath, args)
	if err != nil {
		return err
	}
	cmd := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("store app launch failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// dashboardWindowsStoreLaunchScript resolves the package identity for a
// WindowsApps executable and launches it through Invoke-CommandInDesktopPackage
// with the given managed arguments (for example --remote-debugging-port).
func dashboardWindowsStoreLaunchScript(appPath string, args []string) (string, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(appPath), "/", `\`)
	marker := `\windowsapps\`
	index := strings.Index(strings.ToLower(normalized), marker)
	if index < 0 {
		return "", fmt.Errorf("not a WindowsApps path: %s", appPath)
	}
	rest := normalized[index+len(marker):]
	segments := strings.SplitN(rest, `\`, 2)
	directory := segments[0]
	name := directory
	if cut := strings.IndexByte(name, '_'); cut > 0 {
		name = name[:cut]
	}
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("cannot derive package name from %s", appPath)
	}
	quotePowerShell := func(value string) string {
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	argumentLine := strings.Join(args, " ")
	return strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		"$pkg = Get-AppxPackage -Name " + quotePowerShell(name) + " -ErrorAction SilentlyContinue",
		"if (-not $pkg) { Write-Output 'PRISM_STORE_PACKAGE_NOT_FOUND'; exit 1 }",
		"$apps = @(($pkg | Get-AppxPackageManifest).Package.Applications.Application)",
		"$appId = ($apps | Where-Object { $_.Id -eq 'App' } | Select-Object -First 1).Id",
		"if (-not $appId) { $appId = ($apps | Select-Object -First 1).Id }",
		"if (-not $appId) { Write-Output 'PRISM_STORE_APP_ID_NOT_FOUND'; exit 1 }",
		"Invoke-CommandInDesktopPackage -PackageFamilyName $pkg.PackageFamilyName -AppId $appId -Command " + quotePowerShell(normalized) + " -Args " + quotePowerShell(argumentLine) + " | Out-Null",
		"Write-Output 'PRISM_STORE_LAUNCH_OK'",
	}, "; "), nil
}

func dashboardDesktopRunningForUserDataDir(spec dashboardDesktopAppSpec, userDataDir string) bool {
	running, err := dashboardDesktopRunningForUserDataDirWithError(spec, userDataDir)
	return err != nil || running
}

func dashboardDesktopRunningForUserDataDirWithError(spec dashboardDesktopAppSpec, userDataDir string) (bool, error) {
	processes, err := dashboardDesktopProcessesWithError(spec)
	if err != nil {
		return false, err
	}
	for _, item := range processes {
		command := strings.TrimSpace(fmt.Sprint(item["command"]))
		if dashboardDesktopProcessBlocksManagedLaunch(spec, command, userDataDir) {
			return true, nil
		}
	}
	return false, nil
}

func dashboardDesktopProcessHasManagedProfile(command, userDataDir string) bool {
	return userDataDir == "" || strings.Contains(command, "--user-data-dir="+userDataDir)
}

func dashboardDesktopProcessBlocksManagedLaunch(spec dashboardDesktopAppSpec, command, userDataDir string) bool {
	if dashboardDesktopProcessHasManagedProfile(command, userDataDir) {
		return true
	}
	// ChatGPT.app can host Codex without a --user-data-dir argument. Its main
	// process still owns the single-instance lock, so it blocks a second launch.
	return dashboardDesktopProcessIsMain(spec, command)
}

func runtimeGOOS() string {
	return strings.ToLower(strings.TrimSpace(runtime.GOOS))
}

func dashboardPluginConfig(cfg *bridgeconfig.Config) map[string]map[string]string {
	if cfg == nil || len(cfg.Bridge.Plugins) == 0 {
		return nil
	}
	discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg))
	if err != nil {
		return nil
	}
	out := map[string]map[string]string{}
	for _, plugin := range discovered {
		pluginID := strings.ToLower(strings.TrimSpace(plugin.Manifest.ID))
		if pluginID == "" {
			continue
		}
		env := bridgeconfig.NormalizePluginEnv(cfg.Bridge.Plugins[pluginID].Env)
		if len(env) > 0 {
			out[pluginID] = env
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func dashboardPluginModes(cfg *bridgeconfig.Config) map[string]string {
	if cfg == nil {
		return nil
	}
	discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg))
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, plugin := range discovered {
		if mode := plugin.Manifest.RuntimeMode(cfg.Bridge.Plugins[plugin.Manifest.ID].Mode); mode != "" {
			out[plugin.Manifest.ID] = mode
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func dashboardPluginRuntimeID(cfg *bridgeconfig.Config, pluginID string) (string, error) {
	plugin, err := dashboardPluginManifest(cfg, pluginID)
	if err != nil {
		return "", err
	}
	return plugin.Manifest.ID, nil
}

// resolveManagedTerminalRequest accepts only a canonical conversation owned
// by the selected Plugin. Native IDs are loaded from Hub's local binding,
// never trusted from the Dashboard HTTP caller.
func resolveManagedTerminalRequest(ctx context.Context, dbPath, pluginID, cwd, conversationID string) (pluginruntime.ManagedTerminalRequest, error) {
	req := pluginruntime.ManagedTerminalRequest{PluginID: pluginID, Cwd: strings.TrimSpace(cwd)}
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return req, nil
	}
	db, err := prismdb.Open(dbPath)
	if err != nil {
		return req, fmt.Errorf("open session binding: %w", err)
	}
	defer db.Close()
	record, err := db.FindSession(ctx, conversationID, pluginID)
	if err != nil || strings.TrimSpace(record.NativeSessionID) == "" {
		return req, fmt.Errorf("managed terminal target conversation is unavailable")
	}
	req.Cwd = record.Cwd
	req.NativeSessionID = record.NativeSessionID
	req.NativeThreadID = record.NativeThreadID
	return req, nil
}

func dashboardPluginManifest(cfg *bridgeconfig.Config, pluginID string) (pluginpkg.Discovered, error) {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if cfg == nil || pluginID == "" {
		return pluginpkg.Discovered{}, fmt.Errorf("plugin_id required")
	}
	discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg))
	if err != nil {
		return pluginpkg.Discovered{}, fmt.Errorf("scan local plugins: %w", err)
	}
	for _, plugin := range discovered {
		if strings.EqualFold(plugin.Manifest.ID, pluginID) {
			if strings.TrimSpace(plugin.Manifest.ID) == "" {
				break
			}
			return plugin, nil
		}
	}
	return pluginpkg.Discovered{}, fmt.Errorf("local plugin not found: %s", pluginID)
}

func dashboardPluginAllowsRuntimeMode(manifest pluginpkg.Manifest, mode string) bool {
	if mode == "" {
		return false
	}
	return manifest.RuntimeMode(mode) == mode
}

func applyDashboardDesktopAppOverrides(cfg *bridgeconfig.Config, apps map[string]dashboardDesktopAppConfigRequest) {
	if cfg == nil || len(apps) == 0 {
		return
	}
	for appID, app := range apps {
		if spec, ok := dashboardDesktopAppSpecByID(appID); ok {
			applyDashboardDesktopAppOverridesForSpec(cfg, spec, app)
		}
	}
}

func applyDashboardCodexDesktopOverrides(cfg *bridgeconfig.Config, app dashboardDesktopAppConfigRequest) {
	spec, _ := dashboardDesktopAppSpecByID("codex")
	applyDashboardDesktopAppOverridesForSpec(cfg, spec, app)
}

func applyDashboardDesktopAppOverridesForSpec(cfg *bridgeconfig.Config, spec dashboardDesktopAppSpec, app dashboardDesktopAppConfigRequest) {
	if cfg == nil || strings.TrimSpace(spec.PluginID) == "" {
		return
	}
	if cfg.Bridge.Plugins == nil {
		cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	}
	pluginID := spec.PluginID
	pluginConfig, existed := cfg.Bridge.Plugins[pluginID]
	if app.AppPath != nil {
		pluginConfig.Env = setDashboardEnvOverride(pluginConfig.Env, spec.AppEnvKey, *app.AppPath)
		cfg.Bridge.PluginEnv = setDashboardEnvOverride(cfg.Bridge.PluginEnv, spec.AppEnvKey, "")
	}
	if app.UserDataDir != nil {
		pluginConfig.Env = setDashboardEnvOverride(pluginConfig.Env, spec.UserDataEnvKey, *app.UserDataDir)
		cfg.Bridge.PluginEnv = setDashboardEnvOverride(cfg.Bridge.PluginEnv, spec.UserDataEnvKey, "")
	}
	persistDashboardPluginConfig(cfg, pluginID, pluginConfig, existed)
}

func setDashboardEnvOverride(env map[string]string, key, value string) map[string]string {
	key = strings.TrimSpace(key)
	if key == "" {
		return env
	}
	value = strings.TrimSpace(value)
	if value == "" {
		if len(env) == 0 {
			return nil
		}
		out := map[string]string{}
		for existingKey, existingValue := range env {
			if strings.TrimSpace(existingKey) == key {
				continue
			}
			out[existingKey] = existingValue
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	if env == nil {
		env = map[string]string{}
	}
	env[key] = value
	return env
}

func persistDashboardPluginConfig(cfg *bridgeconfig.Config, key string, pluginConfig bridgeconfig.PluginConfig, existed bool) {
	if cfg == nil || strings.TrimSpace(key) == "" {
		return
	}
	if cfg.Bridge.Plugins == nil {
		cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	}
	pluginConfig.Env = bridgeconfig.NormalizePluginEnv(pluginConfig.Env)
	if len(pluginConfig.Env) == 0 {
		pluginConfig.Env = nil
	}
	if len(pluginConfig.Env) == 0 && dashboardPluginConfigEmpty(pluginConfig) {
		delete(cfg.Bridge.Plugins, key)
		return
	}
	cfg.Bridge.Plugins[key] = pluginConfig
}

func dashboardPluginConfigEmpty(pluginConfig bridgeconfig.PluginConfig) bool {
	return len(pluginConfig.Env) == 0 &&
		strings.TrimSpace(pluginConfig.Mode) == "" &&
		strings.TrimSpace(pluginConfig.ReverseSync) == "" &&
		strings.TrimSpace(pluginConfig.Source) == "" &&
		strings.TrimSpace(pluginConfig.GatewayURL) == "" &&
		strings.TrimSpace(pluginConfig.Discovery) == "" &&
		strings.TrimSpace(pluginConfig.ServerURL) == "" &&
		strings.TrimSpace(pluginConfig.Command) == "" &&
		strings.TrimSpace(pluginConfig.SocketPath) == "" &&
		!pluginConfig.UseTUIEndpoint &&
		strings.TrimSpace(pluginConfig.ChannelName) == "" &&
		!pluginConfig.Beta &&
		!pluginConfig.VisibilityRequired
}

func dashboardPlugins(dirs []string, cfg *bridgeconfig.Config) []map[string]any {
	discovered, err := pluginpkg.ScanDirs(dirs)
	return dashboardPluginsFromDiscovered(discovered, cfg, err)
}

func dashboardPluginsFromDiscovered(discovered []pluginpkg.Discovered, cfg *bridgeconfig.Config, err error) []map[string]any {
	items := make([]map[string]any, 0, len(discovered))
	for _, plugin := range discovered {
		desktopAppID := manifestDesktopAppID(plugin.Manifest)
		configuredMode := ""
		if cfg != nil {
			configuredMode = cfg.Bridge.Plugins[plugin.Manifest.ID].Mode
		}
		currentMode := plugin.Manifest.RuntimeMode(configuredMode)
		// Launchability is the desktop-app launch spec for the dashboard's
		// CDP launch affordance; targets carry the full kind list.
		launchTargets := make([]map[string]any, 0, len(plugin.Manifest.ManagedLaunchSpecs))
		for _, spec := range plugin.Manifest.ManagedLaunchSpecs {
			launchTargets = append(launchTargets, map[string]any{
				"kind":          spec.Kind,
				"ready":         spec.Ready,
				"runtime_modes": spec.RuntimeModes,
			})
		}
		items = append(items, map[string]any{
			"plugin_id":        plugin.Manifest.ID,
			"desktop_app_id":   desktopAppID,
			"display_name":     plugin.Manifest.DisplayName,
			"description":      plugin.Manifest.Description,
			"icon_url":         plugin.Manifest.IconURL,
			"icon_svg_url":     plugin.Manifest.IconSVGURL,
			"version":          plugin.Manifest.Version,
			"integration_mode": plugin.Manifest.IntegrationModeFor(configuredMode),
			"transport":        plugin.Manifest.Transport,
			// mode is the resolved runtime contract, not the legacy manifest mode.
			"mode":                      currentMode,
			"current_runtime_mode":      currentMode,
			"runtime_modes":             plugin.Manifest.RuntimeModes,
			"default_runtime_mode":      plugin.Manifest.DefaultRuntimeMode,
			"managed_launch":            desktopAppID != "",
			"managed_launch_targets":    launchTargets,
			"can_open_managed_terminal": plugin.Manifest.AllowsManagedTerminal(currentMode),
			"manifest_path":             plugin.Path,
			"plugin_root":               plugin.Root,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return fmt.Sprint(items[i]["plugin_id"]) < fmt.Sprint(items[j]["plugin_id"])
	})
	if err != nil {
		items = append(items, map[string]any{
			"plugin_id": "scan_error",
			"error":     err.Error(),
		})
	}
	return items
}

func dashboardWarnings(cfg *bridgeconfig.Config, status dashboardStatus) []string {
	var warnings []string
	if cfg == nil {
		return warnings
	}
	if !status.HostTokenSet {
		warnings = append(warnings, "还没有 host token，先在 dashboard 首页完成登录。")
	}
	if strings.TrimSpace(cfg.Account.AccountID) == "" {
		warnings = append(warnings, "当前还没有绑定 account，dashboard 只会显示本地配置状态。")
	}
	if !status.HubRunning && status.HostTokenSet && status.BridgeEnabled {
		warnings = append(warnings, "Hub 当前未运行；请在 Prism Desktop 中重试或查看诊断。")
	}
	if status.KeepAwakeEnabled && !status.KeepAwakeSupported {
		warnings = append(warnings, "当前平台还没有可用的保持唤醒实现；远程控制时模型/推理/置顶这类桌面交互在锁屏下仍可能失败。")
	}
	return warnings
}

func loadDashboardHostPlugins(configPath string) (map[string]any, error) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}
	return fetchDashboardHostPlugins(cfg)
}

func fetchDashboardHostPlugins(cfg *bridgeconfig.Config) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	return fetchDashboardHostPluginsWithURL(cfg, cfg.Prism.APIURL())
}

func fetchDashboardHostPluginsWithURL(cfg *bridgeconfig.Config, apiURL string) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	if strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
		return nil, fmt.Errorf("account session or host binding missing; please log in again from desktop")
	}
	plugins, err := prismapi.ListUserHostPlugins(apiURL, cfg.Account, cfg.Account.HostID)
	if err != nil {
		return nil, fmt.Errorf("load host plugins failed: %w", err)
	}
	items := make([]map[string]any, 0, len(plugins.Items))
	for _, plugin := range plugins.Items {
		items = append(items, map[string]any{
			"plugin_id":        plugin.PluginID,
			"display_name":     plugin.DisplayName,
			"icon_url":         plugin.IconURL,
			"host_id":          plugin.HostID,
			"host_alias":       plugin.HostAlias,
			"device_name":      plugin.DeviceName,
			"address":          plugin.Address,
			"host_address":     plugin.HostAddress,
			"description":      plugin.Description,
			"version":          plugin.Version,
			"integration_mode": plugin.IntegrationMode,
			"capabilities":     plugin.Capabilities,
			"created_at":       plugin.CreatedAt,
			"updated_at":       plugin.UpdatedAt,
		})
	}
	return map[string]any{
		"ok":           true,
		"host_id":      plugins.HostID,
		"host_alias":   plugins.HostAlias,
		"device_name":  plugins.DeviceName,
		"address":      plugins.Address,
		"host_address": plugins.HostAddress,
		"items":        items,
		"fetched_at":   time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func dashboardHostPluginRequiresReauth(err error) bool {
	var responseError *prismapi.ResponseError
	if !errors.As(err, &responseError) || responseError == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(responseError.Code)) {
	case "host_forbidden", "host_token_expired", "unauthorized":
		return true
	default:
		return false
	}
}

func dashboardHostPluginRegistrationPayload(cfg *bridgeconfig.Config, pluginID string) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return nil, fmt.Errorf("plugin_id required")
	}
	svc := currentDashboardRuntimePlugins()
	ownedService := svc == nil
	if ownedService {
		svc = newBridgeServiceFromConfig(cfg)
	}
	if ownedService {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			_ = svc.Close(shutdownCtx)
		}()
	}
	return dashboardHostPluginRegistrationPayloadWithService(cfg, svc, pluginID)
}

func dashboardHostPluginRegistrationPayloadWithService(cfg *bridgeconfig.Config, svc *pluginruntime.Service, pluginID string) (map[string]any, error) {
	if cfg == nil || svc == nil {
		return nil, fmt.Errorf("plugin runtime unavailable")
	}
	target, err := dashboardPluginManifest(cfg, pluginID)
	if err != nil {
		return nil, err
	}
	adapter := svc.Adapter(target.Manifest.ID)
	if adapter == nil {
		return nil, fmt.Errorf("local plugin runtime unavailable: %s", pluginID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status := svc.AdapterStatus(ctx, target.Manifest.ID, adapter)
	if !status.Available {
		reason := strings.TrimSpace(status.Capability.UnavailableReason)
		if reason == "" {
			reason = strings.TrimSpace(status.LastError)
		}
		if reason == "" {
			reason = "runtime unavailable"
		}
		return nil, fmt.Errorf("plugin %s runtime unavailable: %s", pluginID, reason)
	}
	if supported, reason := dashboardRemoteSupportFromCapability(status.Capability); !supported {
		return nil, fmt.Errorf("plugin %s does not satisfy remote session requirements: %s", pluginID, reason)
	}
	metadata := dashboardMetadataFromAdapter(adapter)
	capability := status.Capability
	return map[string]any{
		"plugin_id":        target.Manifest.ID,
		"display_name":     firstNonEmpty(metadata.DisplayName, target.Manifest.DisplayName, dashboardDisplayName(target.Manifest.ID)),
		"description":      firstNonEmpty(metadata.Description, target.Manifest.Description),
		"icon_url":         firstNonEmpty(metadata.IconURL, target.Manifest.IconURL),
		"version":          firstNonEmpty(metadata.Version, target.Manifest.Version),
		"integration_mode": firstNonEmpty(string(capability.IntegrationMode), target.Manifest.IntegrationMode),
		"capabilities": map[string]any{
			"can_forward_sync":               capability.CanForwardSync,
			"can_reverse_sync":               capability.CanReverseSync,
			"can_wait_run":                   capability.CanWaitRun,
			"can_list_sessions":              capability.CanListSessions,
			"can_read_history":               capability.CanReadHistory,
			"can_attach_session":             capability.CanAttachSession,
			"can_start_session_with_message": capability.CanStartSessionWithMessage,
			"can_open_draft":                 capability.CanOpenDraft,
			"can_interrupt":                  capability.CanInterrupt,
			"can_approval":                   capability.CanApproval,
			"can_read_status":                capability.CanReadStatus,
			"can_control_session":            capability.CanControlSession,
		},
	}, nil
}

func applyDashboardHostPluginChange(cfg *bridgeconfig.Config, pluginID string, enabled bool) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	return applyDashboardHostPluginChangeWithURL(cfg, cfg.Prism.APIURL(), pluginID, enabled)
}

func applyDashboardHostPluginChangeWithURL(cfg *bridgeconfig.Config, apiURL, pluginID string, enabled bool) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	if strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
		return nil, fmt.Errorf("account session or host binding missing; please log in again from desktop")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return nil, fmt.Errorf("plugin_id required")
	}
	var (
		data map[string]any
		err  error
	)
	if enabled {
		payload, payloadErr := dashboardHostPluginRegistrationPayload(cfg, pluginID)
		if payloadErr != nil {
			return nil, payloadErr
		}
		data, err = prismapi.RegisterUserHostPlugin(apiURL, cfg.Account, cfg.Account.HostID, payload)
	} else {
		data, err = prismapi.DeleteUserHostPlugin(apiURL, cfg.Account, cfg.Account.HostID, pluginID)
		if err != nil && prismapi.IsNotFoundError(err) {
			// A previous uninstall may already have removed the cloud row. Keep
			// cleanup idempotent so the local package can still be removed.
			data = map[string]any{"already_absent": true}
			err = nil
		}
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"accepted":   true,
		"plugin_id":  pluginID,
		"enabled":    enabled,
		"result":     data,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func updateDashboardHostPluginMetadata(cfg *bridgeconfig.Config, pluginID, displayName, description, iconURL string) (map[string]any, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	if strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
		return nil, fmt.Errorf("account session or host binding missing; please log in again from desktop")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return nil, fmt.Errorf("plugin_id required")
	}
	data, err := prismapi.UpdateUserHostPluginMetadata(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, pluginID, map[string]any{
		"display_name": strings.TrimSpace(displayName),
		"description":  strings.TrimSpace(description),
		"icon_url":     strings.TrimSpace(iconURL),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"accepted":   true,
		"plugin_id":  pluginID,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
		"result":     data,
	}, nil
}

func dashboardMetadataFromAdapter(adapter pluginruntime.PluginAdapter) pluginruntime.LocalMetadata {
	if provider, ok := adapter.(pluginruntime.MetadataProvider); ok {
		return provider.LocalMetadata()
	}
	return pluginruntime.LocalMetadata{}
}

func dashboardRemoteSupportFromCapability(capability pluginruntime.Capability) (bool, string) {
	switch {
	case !capability.CanForwardSync:
		return false, "missing_forward_sync"
	case !capability.CanListSessions:
		return false, "missing_list_sessions"
	case !capability.CanReadHistory:
		return false, "missing_read_history"
	case !capability.CanWaitRun:
		return false, "missing_wait_run"
	case !capability.CanAttachSession:
		return false, "missing_attach_session"
	case !capability.CanStartSessionWithMessage:
		return false, "missing_start_session_with_message"
	default:
		return true, ""
	}
}

func dashboardDisplayName(id string) string {
	displayName := strings.ReplaceAll(strings.TrimSpace(id), ".", " ")
	displayName = strings.ReplaceAll(displayName, "_", " ")
	displayName = strings.ReplaceAll(displayName, "-", " ")
	parts := strings.Fields(displayName)
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	if len(parts) == 0 {
		return id
	}
	return strings.Join(parts, " ")
}

func dashboardLoginCommand(region string) string {
	return ""
}

func dashboardRecommendedAction(status dashboardStatus) string {
	if status.ConnectionState == "auth_expired" || status.ConnectionState == "unauthorized" {
		return "login"
	}
	if !status.HostTokenSet {
		return "login"
	}
	if !status.BridgeEnabled {
		return "enable_bridge"
	}
	if status.HubRunning {
		return "monitor"
	}
	return "start_hub"
}

func dashboardLifecycle(status dashboardStatus) (string, string) {
	if status.HubRunning {
		return "running", "运行中"
	}
	if status.HostTokenSet {
		return "ready", "已登录未运行"
	}
	return "logged_out", "未登录"
}

// Transport changes must not wait for an expensive process/CDP rescan.
func applyDashboardRuntimeConnection(status *dashboardStatus, runtimeState *daemonRuntimeState) {
	status.ConnectionState, status.ConnectionLabel, status.ConnectionDetail = dashboardConnection(*status, runtimeState)
	status.HostPluginSyncState = runtimeState.HostPluginSyncState
	status.HostPluginSyncDetail = runtimeState.HostPluginSyncDetail
	status.HostPluginSyncUpdatedAt = runtimeState.HostPluginSyncUpdated
	status.HostPluginCount = runtimeState.HostPluginCount
	status.RemoteControlReady = status.LoggedIn && status.HostTokenSet && status.ConnectionState != "auth_expired" && status.ConnectionState != "unauthorized"
	status.RecommendedAction = dashboardRecommendedAction(*status)
}

func dashboardConnection(status dashboardStatus, runtimeState *daemonRuntimeState) (string, string, string) {
	// A listening Dashboard is not a Realtime connection. Login is a hard
	// prerequisite, so a sidecar waiting for credentials must never report a
	// connected transport merely because its local HTTP server is running.
	if !status.HostTokenSet {
		return "logged_out", "未登录", ""
	}
	if runtimeState != nil {
		state := strings.ToLower(strings.TrimSpace(runtimeState.ConnectionState))
		label := firstNonEmpty(daemonRuntimeStateLabel(state), strings.TrimSpace(runtimeState.ConnectionLabel))
		detail := strings.TrimSpace(runtimeState.ConnectionDetail)
		switch state {
		case "connected", "connecting", "reconnecting":
			if status.HubRunning {
				return state, label, detail
			}
		case "auth_expired", "unauthorized":
			return state, label, detail
		case "disconnected":
			if status.HostTokenSet && status.BridgeEnabled {
				return state, label, detail
			}
		}
	}
	if status.HubRunning {
		return "connected", "已连接", ""
	}
	if status.HostTokenSet && status.BridgeEnabled {
		return "disconnected", "未连接", ""
	}
	if status.HostTokenSet && !status.BridgeEnabled {
		return "disabled", "已禁用", ""
	}
	return "logged_out", "未登录", ""
}

func normalizeDashboardRuntimeState(status *dashboardStatus, runtimeState *daemonRuntimeState) *daemonRuntimeState {
	if status == nil || runtimeState == nil {
		return runtimeState
	}
	normalized := *runtimeState
	changed := false
	state := strings.ToLower(strings.TrimSpace(normalized.ConnectionState))
	if !status.HostTokenSet {
		if state != "logged_out" {
			normalized.ConnectionState = "logged_out"
			normalized.ConnectionLabel = "未登录"
			normalized.ConnectionDetail = "当前还没有登录完成。"
			normalized.DaemonPID = 0
			changed = true
		}
		if changed {
			_ = writeDaemonRuntimeState(daemonRuntimeStatePath(), normalized)
		}
		return &normalized
	}

	switch state {
	case "connected", "connecting", "reconnecting":
		if !status.HubRunning {
			if status.HostTokenSet && status.BridgeEnabled {
				normalized.ConnectionState = "disconnected"
				normalized.ConnectionLabel = daemonRuntimeStateLabel("disconnected")
				normalized.ConnectionDetail = "本地运行时已停止，已清理过期连接状态。"
			} else if status.HostTokenSet && !status.BridgeEnabled {
				normalized.ConnectionState = "disconnected"
				normalized.ConnectionLabel = daemonRuntimeStateLabel("disconnected")
				normalized.ConnectionDetail = "Bridge 当前已禁用。"
			} else {
				normalized.ConnectionState = "logged_out"
				normalized.ConnectionLabel = "未登录"
				normalized.ConnectionDetail = "登录信息已清除，已移除旧连接状态。"
			}
			normalized.DaemonPID = 0
			changed = true
		}
	case "stopped", "closed":
		if status.HostTokenSet && status.BridgeEnabled {
			normalized.ConnectionState = "disconnected"
			normalized.ConnectionLabel = daemonRuntimeStateLabel("disconnected")
			if strings.TrimSpace(normalized.ConnectionDetail) == "" {
				normalized.ConnectionDetail = "本地运行时已停止。"
			}
			normalized.DaemonPID = 0
			changed = true
		}
	case "logged_out":
		if status.HostTokenSet {
			normalized.ConnectionState = "disconnected"
			normalized.ConnectionLabel = daemonRuntimeStateLabel("disconnected")
			normalized.ConnectionDetail = "已重新获得登录信息，请按需重新连接服务器。"
			changed = true
		}
	default:
		if !status.HostTokenSet && (state == "auth_expired" || state == "unauthorized" || state == "disconnected") {
			normalized.ConnectionState = "logged_out"
			normalized.ConnectionLabel = "未登录"
			normalized.ConnectionDetail = "当前还没有登录完成。"
			changed = true
		}
	}

	if !status.HubRunning && normalized.DaemonPID != 0 {
		normalized.DaemonPID = 0
		changed = true
	}

	if changed {
		_ = writeDaemonRuntimeState(daemonRuntimeStatePath(), normalized)
	}
	return &normalized
}

func handleDashboardLifecycleAction(configPath, dbPath, action string) (map[string]any, error) {
	return handleDashboardLifecycleActionWithRestart(configPath, dbPath, action, requestDashboardHubRestart)
}

func handleDashboardLifecycleActionWithRestart(configPath, dbPath, action string, restartHub dashboardRestartFunc) (map[string]any, error) {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "restart":
		result := restartHub(configPath, dbPath, "manual_restart")
		result["ok"] = true
		result["message"] = "Hub 服务重启已请求。"
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported lifecycle action")
	}
}

// dashboardManualConnectionRefresh chooses the least disruptive result for a
// user-initiated connection refresh. An expired Host token can only be
// replaced through the existing account login/bind flow.
func dashboardManualConnectionRefresh(status dashboardStatus, restart func() map[string]any) map[string]any {
	state := strings.ToLower(strings.TrimSpace(status.ConnectionState))
	if state == "auth_expired" || state == "unauthorized" || !status.HostTokenSet || !status.LoggedIn {
		return map[string]any{
			"ok":      true,
			"action":  "reauth_required",
			"message": "登录凭证已失效，请重新登录以恢复服务器连接。",
		}
	}
	if !status.BridgeEnabled {
		return map[string]any{
			"ok":      true,
			"action":  "bridge_disabled",
			"message": "Bridge 当前已关闭，开启后才能连接服务器。",
		}
	}
	if state == "connected" {
		return map[string]any{
			"ok":      true,
			"action":  "already_connected",
			"message": "服务器连接正常。",
		}
	}
	result := map[string]any{}
	if restart != nil {
		result = restart()
	}
	return map[string]any{
		"ok":        true,
		"action":    "restart_requested",
		"message":   "正在重新连接服务器。",
		"hub_apply": result,
	}
}

func writeDashboardJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func writeDashboardError(w http.ResponseWriter, status int, message string) {
	writeDashboardJSON(w, status, map[string]any{
		"ok":    false,
		"error": strings.TrimSpace(message),
	})
}

func openDashboardLaunchLog() (*os.File, error) {
	dir := host.DefaultDataPath("logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "dashboard-launch.log")
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
}

func writeDashboardPasswordLoginError(w http.ResponseWriter, err error) {
	var responseError *prismapi.ResponseError
	if errors.As(err, &responseError) && responseError.Code == "C100500" {
		writeDashboardJSON(w, http.StatusBadRequest, map[string]any{
			"ok":         false,
			"error":      "账号或密码不匹配，请检查后重试。",
			"error_code": "C100500",
		})
		return
	}
	writeDashboardError(w, http.StatusBadRequest, err.Error())
}
