package doctor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	plugininstall "rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginruntime"
	"rokid-prism-hub/internal/runtimes"
)

// Check IDs double as the /api/doctor report keys and the finding prefix.
const (
	CheckConfigParse   = "hub.config_parse"
	CheckDB            = "hub.db"
	CheckDisk          = "hub.disk"
	CheckLogs          = "hub.logs"
	CheckRealtime      = "conn.realtime"
	CheckPluginRuntime = "plugin.runtime"
	CheckPluginLease   = "plugin.lease"
	CheckInstallState  = "plugin.install_state"
	CheckActiveVersion = "plugin.active_version"
	CheckNodeRuntime   = "plugin.node_runtime"
	CheckCatalogCache  = "plugin.catalog_cache"
	CheckManagedApps   = "agent.managed"
)

const (
	defaultMinFreeDiskBytes = 500 << 20
	defaultMaxLogDirBytes   = 512 << 20
	defaultStaleCatalogAge  = 7 * 24 * time.Hour
	defaultStuckConnection  = 10 * time.Minute
)

// RuntimePlugins is the passive window into the live plugin registry.
type RuntimePlugins interface {
	HealthSnapshot() []pluginruntime.AdapterHealth
}

// RuntimeStateSnapshot mirrors the persisted realtime connection state file.
type RuntimeStateSnapshot struct {
	State     string
	Detail    string
	UpdatedAt string
}

// ManagedAppSnapshot describes one hostable desktop application.
type ManagedAppSnapshot struct {
	AppID          string
	PluginID       string
	DisplayName    string
	ManagedEnabled bool
	Running        bool
	CDPReachable   bool
}

// Deps carries everything checks read. Providers may return zero values
// (nil service, missing state file) when their subsystem is not running;
// checks treat that as "not observable", never as an anomaly.
type Deps struct {
	ConfigPath string
	DBPath     string

	// DataPath resolves a path under the Prism data directory.
	DataPath func(name string) string
	// PluginDirs resolves configured plugin directories; an error means the
	// configuration could not be loaded at all.
	PluginDirs func() ([]string, error)
	// RuntimePlugins returns the live registry, or nil when the bridge
	// runtime is not running.
	RuntimePlugins func() RuntimePlugins
	// RuntimeState reads the persisted connection state; ok=false when no
	// state file exists yet.
	RuntimeState func() (RuntimeStateSnapshot, bool)
	// ManagedApps reports hostable desktop applications.
	ManagedApps func() []ManagedAppSnapshot
	// Installer reads official plugin install state; nil disables the install
	// checks.
	Installer *plugininstall.Installer
	// CatalogCachePath resolves the plugin catalog cache file.
	CatalogCachePath func() string
	// NodeBinary resolves the shared Node runtime binary path.
	NodeBinary func() string
	// LogDir resolves the hub log directory.
	LogDir func() string
	// Logger receives transparent-remedy traces; defaults to slog.Default().
	Logger *slog.Logger

	// Remedy providers (nil disables the corresponding remedy):
	// PluginRestart cleanly reloads one plugin adapter.
	PluginRestart func(ctx context.Context, pluginID string) error
	// ManagedRelaunch restarts a managed desktop application (重新托管).
	ManagedRelaunch func(ctx context.Context, appID string) error
	// CatalogRefresh refreshes the plugin catalog cache.
	CatalogRefresh func(ctx context.Context) error
	// NodeEnsure installs the shared Node runtime if missing.
	NodeEnsure func(ctx context.Context) error
	// ConfigRepair rebuilds a strict-decode-broken config file; it returns
	// the backup path.
	ConfigRepair func(ctx context.Context) (string, error)

	MinFreeDiskBytes   int64
	MaxLogDirBytes     int64
	StaleCatalogAge    time.Duration
	StuckConnectionAge time.Duration
}

func (d Deps) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

func (d Deps) minFreeDiskBytes() int64 {
	if d.MinFreeDiskBytes > 0 {
		return d.MinFreeDiskBytes
	}
	return defaultMinFreeDiskBytes
}

func (d Deps) maxLogDirBytes() int64 {
	if d.MaxLogDirBytes > 0 {
		return d.MaxLogDirBytes
	}
	return defaultMaxLogDirBytes
}

func (d Deps) staleCatalogAge() time.Duration {
	if d.StaleCatalogAge > 0 {
		return d.StaleCatalogAge
	}
	return defaultStaleCatalogAge
}

func (d Deps) stuckConnectionAge() time.Duration {
	if d.StuckConnectionAge > 0 {
		return d.StuckConnectionAge
	}
	return defaultStuckConnection
}

// DefaultChecks returns the M1 read-only check catalog in report order.
func DefaultChecks(deps Deps) []Check {
	return []Check{
		configParseCheck{deps: deps},
		dbCheck{deps: deps},
		diskCheck{deps: deps},
		logsCheck{deps: deps},
		realtimeCheck{deps: deps},
		pluginRuntimeCheck{deps: deps},
		pluginLeaseCheck{deps: deps},
		installStateCheck{deps: deps},
		activeVersionCheck{deps: deps},
		nodeRuntimeCheck{deps: deps},
		catalogCacheCheck{deps: deps},
		managedAppsCheck{deps: deps},
	}
}

func finding(check, subject string, severity Severity, summaryKey string, params map[string]string, detail string) Finding {
	return Finding{
		Check:         check,
		Subject:       subject,
		Severity:      severity,
		SummaryKey:    summaryKey,
		SummaryParams: params,
		Detail:        detail,
	}
}

// withHint attaches the friendly how-to-solve guidance shown in the panel.
func (f Finding) withHint(hintKey string, params map[string]string) Finding {
	f.HintKey = hintKey
	f.HintParams = params
	return f
}

var errRemedyProviderUnavailable = errors.New("remedy provider unavailable")

// errRiskEscalated marks an auto-heal aborted because its remedy left the
// transparent class while the execution waited on the serializer.
var errRiskEscalated = errors.New("remedy escalated before execution")

// remedyStillTransparent re-reads the carrier's current remedy metadata and
// reports whether the given remedy is still available and transparent.
func remedyStillTransparent(carrier RemedyCarrier, finding Finding, remedyID string) bool {
	for _, remedy := range carrier.Remedies(finding) {
		if remedy.ID == remedyID {
			return remedy.Available && remedy.Risk == RiskTransparent
		}
	}
	return false
}

// --- hub.config_parse -------------------------------------------------------

type configParseCheck struct{ deps Deps }

func (c configParseCheck) ID() string              { return CheckConfigParse }
func (c configParseCheck) Interval() time.Duration { return time.Minute }

func (c configParseCheck) Run(ctx context.Context) ([]Finding, error) {
	// Load treats a missing file as "use defaults"; only real damage lands here.
	if _, err := bridgeconfig.Load(c.deps.ConfigPath); err != nil {
		return []Finding{finding(CheckConfigParse, "config", SeverityCritical,
			"doctor.finding_config_parse", nil, err.Error()).
			withHint("doctor.hint_config_parse", nil)}, nil
	}
	return nil, nil
}

func (c configParseCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckConfigParse {
		return nil
	}
	available := c.deps.ConfigRepair != nil && bridgeconfig.Repairable(c.deps.ConfigPath)
	reason := ""
	if !available && c.deps.ConfigRepair == nil {
		reason = "repair provider unavailable"
	} else if !available {
		reason = "yaml syntax damage is not auto-repairable"
	}
	return []Remedy{{
		ID:                "repair",
		TitleKey:          "doctor.remedy_config_repair",
		Risk:              RiskConfirm,
		ImpactKey:         "doctor.impact_config_repair",
		Available:         available,
		UnavailableReason: reason,
	}}
}

func (c configParseCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "repair" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.ConfigRepair == nil {
		return errRemedyProviderUnavailable
	}
	backup, err := c.deps.ConfigRepair(ctx)
	if err != nil {
		return err
	}
	c.deps.logger().Info("doctor repaired config file", "backup", backup)
	return nil
}

// --- hub.db -----------------------------------------------------------------

type dbCheck struct{ deps Deps }

func (c dbCheck) ID() string              { return CheckDB }
func (c dbCheck) Interval() time.Duration { return time.Minute }

func (c dbCheck) Run(ctx context.Context) ([]Finding, error) {
	info, err := os.Stat(c.deps.DBPath)
	if os.IsNotExist(err) {
		// The database is created on first use.
		return nil, nil
	}
	if err != nil {
		return []Finding{finding(CheckDB, "db", SeverityWarning,
			"doctor.finding_db", nil, err.Error())}, nil
	}
	if info.IsDir() {
		return []Finding{finding(CheckDB, "db", SeverityWarning,
			"doctor.finding_db", nil, "database path is a directory").
			withHint("doctor.hint_db", nil)}, nil
	}
	handle, err := os.OpenFile(c.deps.DBPath, os.O_RDWR, 0)
	if err != nil {
		return []Finding{finding(CheckDB, "db", SeverityWarning,
			"doctor.finding_db", nil, "open read/write: "+err.Error()).
			withHint("doctor.hint_db", nil)}, nil
	}
	_ = handle.Close()
	return nil, nil
}

// --- hub.disk (platform-specific free-space probe) --------------------------

type diskCheck struct{ deps Deps }

func (c diskCheck) ID() string              { return CheckDisk }
func (c diskCheck) Interval() time.Duration { return 5 * time.Minute }

// --- hub.logs ----------------------------------------------------------------

type logsCheck struct{ deps Deps }

func (c logsCheck) ID() string              { return CheckLogs }
func (c logsCheck) Interval() time.Duration { return 5 * time.Minute }

func (c logsCheck) Run(ctx context.Context) ([]Finding, error) {
	dir := c.deps.LogDir
	if dir == nil {
		return nil, nil
	}
	logDir := strings.TrimSpace(dir())
	if logDir == "" {
		return nil, nil
	}
	var total int64
	_ = filepath.WalkDir(logDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, statErr := entry.Info(); statErr == nil {
			total += info.Size()
		}
		return nil
	})
	if total <= c.deps.maxLogDirBytes() {
		return nil, nil
	}
	return []Finding{finding(CheckLogs, "logs", SeverityWarning, "doctor.finding_logs_large",
		map[string]string{"size_mb": strconv.FormatInt(total>>20, 10)},
		logDir).
		withHint("doctor.hint_logs", nil)}, nil
}

// logCleanMaxAge bounds the one-click log cleanup: only rotated files older
// than this are removed, so an active log file is never touched.
const logCleanMaxAge = 24 * time.Hour

func (c logsCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckLogs {
		return nil
	}
	return []Remedy{{
		ID:        "clean",
		TitleKey:  "doctor.remedy_log_clean",
		Risk:      RiskConfirm,
		ImpactKey: "doctor.impact_log_clean",
		Available: c.deps.LogDir != nil,
		UnavailableReason: func() string {
			if c.deps.LogDir == nil {
				return "log directory unavailable"
			}
			return ""
		}(),
	}}
}

func (c logsCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "clean" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.LogDir == nil {
		return errRemedyProviderUnavailable
	}
	logDir := strings.TrimSpace(c.deps.LogDir())
	removed := 0
	_ = filepath.WalkDir(logDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil || time.Since(info.ModTime()) < logCleanMaxAge {
			return nil
		}
		if os.Remove(path) == nil {
			removed++
		}
		return nil
	})
	c.deps.logger().Info("doctor cleaned old log files", "dir", logDir, "removed", removed)
	return nil
}

// --- conn.realtime ------------------------------------------------------------

type realtimeCheck struct{ deps Deps }

func (c realtimeCheck) ID() string              { return CheckRealtime }
func (c realtimeCheck) Interval() time.Duration { return time.Minute }

func (c realtimeCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.RuntimeState == nil {
		return nil, nil
	}
	snapshot, ok := c.deps.RuntimeState()
	if !ok {
		return nil, nil
	}
	state := strings.ToLower(strings.TrimSpace(snapshot.State))
	detail := strings.TrimSpace(snapshot.Detail)
	switch state {
	case "auth_expired", "unauthorized":
		return []Finding{finding(CheckRealtime, "realtime", SeverityWarning,
			"doctor.finding_conn_auth",
			map[string]string{"state": state}, detail).
			withHint("doctor.hint_reauth", nil)}, nil
	case "gateway_rejected":
		return []Finding{finding(CheckRealtime, "realtime", SeverityWarning,
			"doctor.finding_conn_auth",
			map[string]string{"state": state}, detail).
			withHint("doctor.hint_gateway", nil)}, nil
	case "reconnecting", "disconnected":
		updatedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(snapshot.UpdatedAt))
		if err != nil {
			return nil, nil
		}
		if stuck := time.Since(updatedAt); stuck >= c.deps.stuckConnectionAge() {
			return []Finding{finding(CheckRealtime, "realtime", SeverityWarning,
				"doctor.finding_conn_stuck",
				map[string]string{"minutes": strconv.FormatInt(int64(stuck.Minutes()), 10)},
				detail).
				withHint("doctor.hint_network", nil)}, nil
		}
	}
	return nil, nil
}

// --- plugin.runtime -----------------------------------------------------------

type pluginRuntimeCheck struct{ deps Deps }

func (c pluginRuntimeCheck) ID() string              { return CheckPluginRuntime }
func (c pluginRuntimeCheck) Interval() time.Duration { return time.Minute }

func (c pluginRuntimeCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.RuntimePlugins == nil {
		return nil, nil
	}
	registry := c.deps.RuntimePlugins()
	if registry == nil {
		// The bridge runtime is not running; nothing to observe.
		return nil, nil
	}
	var findings []Finding
	for _, adapter := range registry.HealthSnapshot() {
		if adapter.Health == nil {
			continue
		}
		health := *adapter.Health
		switch {
		case health.Failures >= pluginpkg.SupervisorMaxFailures:
			// The adapter stopped restarting this plugin; without doctor it
			// only recovers via reload or a Hub restart.
			findings = append(findings, finding(CheckPluginRuntime, adapter.ID,
				SeverityCritical, "doctor.finding_plugin_capped",
				map[string]string{"name": adapter.ID}, health.LastError).
				withHint("doctor.hint_plugin_capped", map[string]string{"name": adapter.ID}))
		case health.Failures >= 2:
			findings = append(findings, finding(CheckPluginRuntime, adapter.ID,
				SeverityWarning, "doctor.finding_plugin_flapping",
				map[string]string{"name": adapter.ID},
				"failures="+strconv.Itoa(health.Failures)+" last_error="+health.LastError).
				withHint("doctor.hint_plugin_flapping", map[string]string{"name": adapter.ID}))
		}
	}
	return findings, nil
}

func (c pluginRuntimeCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckPluginRuntime {
		return nil
	}
	available := c.deps.PluginRestart != nil
	reason := ""
	if !available {
		reason = "plugin reloader unavailable"
	}
	// Impact escalation (docs/14 §2.1): restarting a dead plugin is free; a
	// running one may be serving a conversation, so it needs confirmation.
	risk := RiskTransparent
	impactKey := ""
	if c.pluginRunning(finding.Subject) {
		risk = RiskConfirm
		impactKey = "doctor.impact_plugin_restart"
	}
	return []Remedy{{
		ID:                "restart",
		TitleKey:          "doctor.remedy_plugin_restart",
		Risk:              risk,
		ImpactKey:         impactKey,
		ImpactParams:      map[string]string{"name": finding.Subject},
		Available:         available,
		UnavailableReason: reason,
	}}
}

func (c pluginRuntimeCheck) pluginRunning(pluginID string) bool {
	if c.deps.RuntimePlugins == nil {
		return false
	}
	registry := c.deps.RuntimePlugins()
	if registry == nil {
		return false
	}
	for _, adapter := range registry.HealthSnapshot() {
		if adapter.ID != pluginID || adapter.Health == nil {
			continue
		}
		return adapter.Health.Running
	}
	return false
}

func (c pluginRuntimeCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "restart" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.PluginRestart == nil {
		return errRemedyProviderUnavailable
	}
	return c.deps.PluginRestart(ctx, finding.Subject)
}

// --- plugin.lease --------------------------------------------------------------

type pluginLeaseCheck struct{ deps Deps }

func (c pluginLeaseCheck) ID() string              { return CheckPluginLease }
func (c pluginLeaseCheck) Interval() time.Duration { return time.Minute }

func (c pluginLeaseCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.DataPath == nil {
		return nil, nil
	}
	dir := c.deps.DataPath(filepath.Join("runtime", "plugins"))
	var findings []Finding
	for _, item := range pluginpkg.StaleRuntimeLeases(dir) {
		// A dead-PID lease file is pure garbage: prepareRuntimeLease performs
		// the identical removal the next time this plugin starts. Removing it
		// here is the one transparent remedy pulled forward from M2 — an
		// orphaned plugin (removed while its lease lingered) would otherwise
		// never trigger that path and keep the file forever.
		// Re-verify ownership right before removal: a plugin start racing
		// this sweep could have replaced the file with a fresh live lease.
		if !pluginpkg.RuntimeLeaseMatches(item) {
			continue
		}
		if err := os.Remove(item.Path); err != nil {
			// Removal failed: keep the stale-lease finding with the reason so
			// the user sees why it is still around.
			findings = append(findings, finding(CheckPluginLease, item.PluginID,
				SeverityInfo, "doctor.finding_lease_stale",
				map[string]string{"name": item.PluginID},
				"pid="+strconv.Itoa(item.PID)+" remove: "+err.Error()))
			continue
		}
		c.deps.logger().Info("doctor cleaned stale plugin runtime lease",
			"plugin", item.PluginID, "pid", item.PID, "path", item.Path)
		// Successful cleanup reports one final notice cycle, then resolves.
		findings = append(findings, finding(CheckPluginLease, item.PluginID,
			SeverityInfo, "doctor.finding_lease_cleaned",
			map[string]string{"name": item.PluginID},
			"pid="+strconv.Itoa(item.PID)+" removed "+item.Path))
	}
	return findings, nil
}

// --- plugin.install_state -------------------------------------------------------

type installStateCheck struct{ deps Deps }

func (c installStateCheck) ID() string              { return CheckInstallState }
func (c installStateCheck) Interval() time.Duration { return 5 * time.Minute }

func (c installStateCheck) Run(ctx context.Context) ([]Finding, error) {
	installer := c.deps.Installer
	if installer == nil || strings.TrimSpace(installer.Root) == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(installer.Root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var findings []Finding
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pluginID := entry.Name()
		state, stateErr := installer.State(pluginID)
		if stateErr != nil {
			findings = append(findings, finding(CheckInstallState, pluginID,
				SeverityWarning, "doctor.finding_install_state",
				map[string]string{"name": pluginID}, stateErr.Error()).
				withHint("doctor.hint_install_state", map[string]string{"name": pluginID}))
			continue
		}
		if state.ActiveVersion == "" {
			continue
		}
		versionDir := filepath.Join(installer.Root, pluginID, "versions", state.ActiveVersion)
		if state.ActiveVersion == "current" {
			versionDir = filepath.Join(installer.Root, pluginID, "current")
		}
		if _, err := os.Stat(filepath.Join(versionDir, pluginpkg.ManifestYAML)); err != nil {
			findings = append(findings, finding(CheckInstallState, pluginID,
				SeverityWarning, "doctor.finding_install_state",
				map[string]string{"name": pluginID},
				"active version manifest missing: "+versionDir).
				withHint("doctor.hint_install_state", map[string]string{"name": pluginID}))
		}
	}
	return findings, nil
}

// --- plugin.active_version --------------------------------------------------------

type activeVersionCheck struct{ deps Deps }

func (c activeVersionCheck) ID() string              { return CheckActiveVersion }
func (c activeVersionCheck) Interval() time.Duration { return 5 * time.Minute }

func (c activeVersionCheck) Run(ctx context.Context) ([]Finding, error) {
	installer := c.deps.Installer
	if installer == nil || c.deps.PluginDirs == nil {
		return nil, nil
	}
	dirs, err := c.deps.PluginDirs()
	if err != nil {
		// The config_parse check owns configuration damage.
		return nil, nil
	}
	discovered, err := pluginpkg.ScanDirs(dirs)
	if err != nil {
		return nil, nil
	}
	discoveredIDs := map[string]bool{}
	for _, item := range discovered {
		discoveredIDs[strings.ToLower(strings.TrimSpace(item.Manifest.ID))] = true
	}
	entries, err := os.ReadDir(installer.Root)
	if err != nil {
		return nil, nil
	}
	var findings []Finding
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pluginID := entry.Name()
		state, stateErr := installer.State(pluginID)
		if stateErr != nil || state.ActiveVersion == "" {
			continue
		}
		if !discoveredIDs[strings.ToLower(pluginID)] {
			// An activated version that no longer discovers is the rollback
			// precondition: the active build is broken.
			findings = append(findings, finding(CheckActiveVersion, pluginID,
				SeverityWarning, "doctor.finding_active_version",
				map[string]string{"name": pluginID},
				"active="+state.ActiveVersion).
				withHint("doctor.hint_active_version", map[string]string{"name": pluginID}))
		}
	}
	return findings, nil
}

func (c activeVersionCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckActiveVersion || c.deps.Installer == nil {
		return nil
	}
	available := false
	reason := ""
	if c.deps.PluginRestart == nil {
		reason = "plugin reloader unavailable"
	} else if state, err := c.deps.Installer.State(finding.Subject); err != nil {
		reason = "install state unreadable"
	} else if state.PreviousVersion == "" {
		reason = "no previous version to roll back to"
	} else {
		available = true
	}
	return []Remedy{{
		ID:                "rollback",
		TitleKey:          "doctor.remedy_rollback",
		Risk:              RiskConfirm,
		ImpactKey:         "doctor.impact_rollback",
		ImpactParams:      map[string]string{"name": finding.Subject},
		Available:         available,
		UnavailableReason: reason,
	}}
}

func (c activeVersionCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "rollback" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.Installer == nil || c.deps.PluginRestart == nil {
		return errRemedyProviderUnavailable
	}
	if err := c.deps.Installer.Rollback(finding.Subject); err != nil {
		return fmt.Errorf("rollback %s: %w", finding.Subject, err)
	}
	return c.deps.PluginRestart(ctx, finding.Subject)
}

// --- plugin.node_runtime -------------------------------------------------------------

type nodeRuntimeCheck struct{ deps Deps }

func (c nodeRuntimeCheck) ID() string              { return CheckNodeRuntime }
func (c nodeRuntimeCheck) Interval() time.Duration { return 5 * time.Minute }

func (c nodeRuntimeCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.NodeBinary == nil || c.deps.PluginDirs == nil {
		return nil, nil
	}
	dirs, err := c.deps.PluginDirs()
	if err != nil {
		return nil, nil
	}
	discovered, err := pluginpkg.ScanDirs(dirs)
	if err != nil {
		return nil, nil
	}
	required := false
	for _, item := range discovered {
		if runtimes.RequiresNode(item.Manifest.RuntimeDependencies.Node) {
			required = true
			break
		}
	}
	if !required {
		return nil, nil
	}
	binary := strings.TrimSpace(c.deps.NodeBinary())
	if binary == "" {
		return nil, nil
	}
	if _, err := os.Stat(binary); err == nil {
		return nil, nil
	}
	return []Finding{finding(CheckNodeRuntime, "node", SeverityWarning,
		"doctor.finding_node_missing", nil, binary).
		withHint("doctor.hint_node", nil)}, nil
}

func (c nodeRuntimeCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckNodeRuntime {
		return nil
	}
	return []Remedy{{
		ID:        "install",
		TitleKey:  "doctor.remedy_node_install",
		Risk:      RiskTransparent,
		Available: c.deps.NodeEnsure != nil,
		UnavailableReason: func() string {
			if c.deps.NodeEnsure == nil {
				return "node runtime provider unavailable"
			}
			return ""
		}(),
	}}
}

func (c nodeRuntimeCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "install" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.NodeEnsure == nil {
		return errRemedyProviderUnavailable
	}
	return c.deps.NodeEnsure(ctx)
}

// --- plugin.catalog_cache ---------------------------------------------------------------

type catalogCacheCheck struct{ deps Deps }

func (c catalogCacheCheck) ID() string              { return CheckCatalogCache }
func (c catalogCacheCheck) Interval() time.Duration { return 5 * time.Minute }

func (c catalogCacheCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.CatalogCachePath == nil {
		return nil, nil
	}
	path := strings.TrimSpace(c.deps.CatalogCachePath())
	if path == "" {
		return nil, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		// A missing cache only delays the first catalog fetch.
		return nil, nil
	}
	age := time.Since(info.ModTime())
	if age < c.deps.staleCatalogAge() {
		return nil, nil
	}
	return []Finding{finding(CheckCatalogCache, "catalog", SeverityInfo,
		"doctor.finding_catalog_stale",
		map[string]string{"days": strconv.FormatInt(int64(age/(24*time.Hour)), 10)},
		path).
		withHint("doctor.hint_catalog", nil)}, nil
}

func (c catalogCacheCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckCatalogCache {
		return nil
	}
	return []Remedy{{
		ID:        "refresh",
		TitleKey:  "doctor.remedy_catalog_refresh",
		Risk:      RiskTransparent,
		Available: c.deps.CatalogRefresh != nil,
		UnavailableReason: func() string {
			if c.deps.CatalogRefresh == nil {
				return "catalog provider unavailable"
			}
			return ""
		}(),
	}}
}

func (c catalogCacheCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "refresh" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.CatalogRefresh == nil {
		return errRemedyProviderUnavailable
	}
	return c.deps.CatalogRefresh(ctx)
}

// --- agent.managed --------------------------------------------------------------------

type managedAppsCheck struct{ deps Deps }

func (c managedAppsCheck) ID() string              { return CheckManagedApps }
func (c managedAppsCheck) Interval() time.Duration { return time.Minute }

func (c managedAppsCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.ManagedApps == nil {
		return nil, nil
	}
	var findings []Finding
	for _, app := range c.deps.ManagedApps() {
		// A stopped app is a normal user action; only a running app Prism
		// cannot control is an anomaly.
		if !app.ManagedEnabled || !app.Running || app.CDPReachable {
			continue
		}
		subject := app.AppID
		if subject == "" {
			subject = app.PluginID
		}
		name := app.DisplayName
		if name == "" {
			name = subject
		}
		findings = append(findings, finding(CheckManagedApps, subject,
			SeverityWarning, "doctor.finding_managed_cdp",
			map[string]string{"name": name}, "").
			withHint("doctor.hint_managed", map[string]string{"name": name}))
	}
	return findings, nil
}

func (c managedAppsCheck) Remedies(finding Finding) []Remedy {
	if finding.Check != CheckManagedApps {
		return nil
	}
	return []Remedy{{
		ID:           "relaunch",
		TitleKey:     "doctor.remedy_relaunch",
		Risk:         RiskConfirm,
		ImpactKey:    "doctor.impact_relaunch",
		ImpactParams: finding.SummaryParams,
		Available:    c.deps.ManagedRelaunch != nil,
		UnavailableReason: func() string {
			if c.deps.ManagedRelaunch == nil {
				return "managed launch unsupported here"
			}
			return ""
		}(),
	}}
}

func (c managedAppsCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	if remedyID != "relaunch" {
		return fmt.Errorf("unknown remedy %q", remedyID)
	}
	if c.deps.ManagedRelaunch == nil {
		return errRemedyProviderUnavailable
	}
	return c.deps.ManagedRelaunch(ctx, finding.Subject)
}
