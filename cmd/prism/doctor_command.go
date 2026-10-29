package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	bridgeconfig "rokid-prism-hub/internal/config"
	"rokid-prism-hub/internal/doctor"
	"rokid-prism-hub/internal/host"
	pluginpkg "rokid-prism-hub/internal/plugin"
	plugininstall "rokid-prism-hub/internal/plugininstall"
	pluginregistry "rokid-prism-hub/internal/pluginregistry"
	"rokid-prism-hub/internal/runtimes"
)

// doctorStatePath keeps doctor findings across Hub restarts, mirroring the
// daemon runtime state file contract (atomic temp+rename under the data dir).
func doctorStatePath() string {
	return host.DefaultDataPath("doctor-state.json")
}

// newDoctorService wires the check catalog and remedy providers to live Hub
// state. Checks observe passively; remedies run only through the doctor
// executor (transparent auto-heal or explicit user confirmation).
func newDoctorService(ctx context.Context, configPath, dbPath string, installer *plugininstall.Installer, registry *pluginregistry.Manager) *doctor.Service {
	deps := doctor.Deps{
		ConfigPath: configPath,
		DBPath:     dbPath,
		DataPath:   host.DefaultDataPath,
		PluginDirs: func() ([]string, error) {
			cfg, err := bridgeconfig.Load(configPath)
			if err != nil {
				return nil, err
			}
			return pluginDirs(cfg), nil
		},
		RuntimePlugins: func() doctor.RuntimePlugins {
			return currentDashboardRuntimePlugins()
		},
		RuntimeState: func() (doctor.RuntimeStateSnapshot, bool) {
			state, err := readDaemonRuntimeState(daemonRuntimeStatePath())
			if err != nil || state == nil {
				return doctor.RuntimeStateSnapshot{}, false
			}
			return doctor.RuntimeStateSnapshot{
				State:     state.ConnectionState,
				Detail:    state.ConnectionDetail,
				UpdatedAt: state.UpdatedAt,
			}, true
		},
		ManagedApps: func() []doctor.ManagedAppSnapshot {
			return doctorManagedApps(configPath)
		},
		Installer:        installer,
		CatalogCachePath: pluginregistry.DefaultCachePath,
		NodeBinary: func() string {
			return runtimes.NewNodeManager("", nil).Binary()
		},
		LogDir: func() string {
			return host.DefaultDataPath("logs")
		},
		Logger: slog.Default(),
		PluginRestart: func(restartCtx context.Context, pluginID string) error {
			reloader := currentDashboardRuntimePluginReloader()
			if reloader == nil {
				return fmt.Errorf("plugin runtime is not running; log in first")
			}
			return reloader.ReloadPlugin(restartCtx, pluginID)
		},
		ManagedRelaunch: func(relaunchCtx context.Context, appID string) error {
			// Same managed-launch path the Dashboard's relaunch button uses.
			_, err := handleDashboardDesktopAction(configPath, appID, "restart")
			return err
		},
		CatalogRefresh: func(refreshCtx context.Context) error {
			if registry == nil {
				return fmt.Errorf("plugin catalog unavailable")
			}
			registry.Refresh(refreshCtx)
			return nil
		},
		NodeEnsure: func(ensureCtx context.Context) error {
			_, err := runtimes.NewNodeManager("", nil).Ensure(ensureCtx)
			return err
		},
		ConfigRepair: func(repairCtx context.Context) (string, error) {
			return bridgeconfig.RepairFile(configPath)
		},
	}
	service := doctor.NewService(doctorStatePath(), slog.Default(), doctor.Gates{
		Enabled: func() bool {
			cfg, err := bridgeconfig.Load(configPath)
			if err != nil {
				return true
			}
			return cfg.Doctor.IsEnabled()
		},
		AutoHeal: func() bool {
			cfg, err := bridgeconfig.Load(configPath)
			if err != nil {
				return true
			}
			return cfg.Doctor.IsAutoHealEnabled()
		},
	})
	service.Register(doctor.DefaultChecks(deps)...)
	go service.Run(ctx)
	return service
}

// doctorManagedApps projects the Dashboard's desktop-app status into doctor
// snapshots. It reuses the same read-only probes the status page already runs.
func doctorManagedApps(configPath string) []doctor.ManagedAppSnapshot {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil
	}
	dirs := pluginDirs(cfg)
	discovered, scanErr := pluginpkg.ScanDirs(dirs)
	managedPlugins := map[string]bool{}
	if scanErr == nil {
		for _, item := range discovered {
			managedPlugins[strings.ToLower(strings.TrimSpace(item.Manifest.ID))] = item.Manifest.ManagedLaunch
		}
	}
	apps := dashboardDesktopAppsWithManagedPlugins(cfg, dirs, managedPlugins)
	snapshots := make([]doctor.ManagedAppSnapshot, 0, len(apps))
	for appID, value := range apps {
		detail, _ := value.(map[string]any)
		if detail == nil {
			continue
		}
		snapshot := doctor.ManagedAppSnapshot{
			AppID:          doctorStringField(detail, "app_id"),
			DisplayName:    doctorStringField(detail, "display_name"),
			ManagedEnabled: doctorBoolField(detail, "managed_enabled"),
			Running:        doctorBoolField(detail, "running"),
			CDPReachable:   doctorBoolField(detail, "cdp_reachable"),
		}
		if spec, ok := dashboardDesktopAppSpecByID(appID); ok {
			snapshot.PluginID = spec.PluginID
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func doctorStringField(detail map[string]any, key string) string {
	value, _ := detail[key].(string)
	return value
}

func doctorBoolField(detail map[string]any, key string) bool {
	value, _ := detail[key].(bool)
	return value
}
