package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/host"
	daemonlock "rokid-prism-hub/internal/lock"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginregistry"
	"rokid-prism-hub/internal/runtimes"
)

const defaultHubAddr = "127.0.0.1:17171"

// desktopManagedRestartExitCode tells the desktop shell to immediately start a
// fresh Hub after Dashboard login or configuration changes. It is deliberately
// distinct from a crash exit code so the shell can report real failures.
const desktopManagedRestartExitCode = 75

var hubLockPathFor = func() string {
	return host.DefaultDataPath("hub.lock")
}

func hubDefaultAddr() string {
	return envOr("PRISM_HUB_ADDR", envOr("PRISM_DASHBOARD_ADDR", defaultHubAddr))
}

func hubLockPath() string {
	return hubLockPathFor()
}

func hubURLForAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		addr = hubDefaultAddr()
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}

func hubHealthURL(addr string) string {
	return strings.TrimRight(hubURLForAddr(addr), "/") + "/api/status"
}

func hubRunning(lockPath string) bool {
	lock, err := daemonlock.Acquire(lockPath)
	if err == nil {
		_ = lock.Release()
		return false
	}
	return errors.Is(err, daemonlock.ErrLocked)
}

func readPIDFromLockFile(lockPath string) int {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return 0
	}
	line := strings.TrimSpace(strings.SplitN(string(b), "\n", 2)[0])
	if line == "" {
		return 0
	}
	pid, err := strconv.Atoi(line)
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func hubReachable(addr string) bool {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Get(hubHealthURL(addr))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func hubStatusPayload(configPath, dbPath, addr string) map[string]any {
	status := collectDashboardStatus(configPath, dbPath)
	lockPath := hubLockPath()
	running := hubRunning(lockPath)
	pid := readPIDFromLockFile(lockPath)
	reachable := false
	if running {
		reachable = hubReachable(addr)
	}
	return map[string]any{
		"ok":                          true,
		"hub_running":                 running,
		"hub_reachable":               reachable,
		"hub_pid":                     pid,
		"hub_addr":                    strings.TrimSpace(addr),
		"hub_url":                     hubURLForAddr(addr),
		"connection_state":            status.ConnectionState,
		"connection_label":            status.ConnectionLabel,
		"connection_detail":           status.ConnectionDetail,
		"host_plugin_sync_state":      status.HostPluginSyncState,
		"host_plugin_sync_detail":     status.HostPluginSyncDetail,
		"host_plugin_sync_updated_at": status.HostPluginSyncUpdatedAt,
		"host_plugin_count":           status.HostPluginCount,
		"logged_in":                   status.LoggedIn,
		"remote_control_ready":        status.RemoteControlReady,
		"can_restart_hub":             status.CanRestartHub,
		"lifecycle_state":             status.LifecycleState,
		"lifecycle_label":             status.LifecycleLabel,
	}
}

func runHubServeCommand(configPath, dbPath, addr string, desktopManaged bool, stdout, stderr io.Writer) int {
	lock, err := daemonlock.Acquire(hubLockPath())
	if err != nil {
		if errors.Is(err, daemonlock.ErrLocked) {
			fmt.Fprintln(stderr, "Prism Hub already running")
			return 1
		}
		fmt.Fprintf(stderr, "acquire Prism Hub lock failed: %v\n", err)
		return 1
	}
	defer lock.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if desktopManaged {
		if ownerPID := desktopManagedOwnerPID(); ownerPID > 0 {
			go watchDesktopOwner(ctx, cancel, ownerPID)
		}
	}
	var restartRequested atomic.Bool
	restartHub := requestDashboardHubRestart
	if desktopManaged {
		restartHub = func(_, _, reason string) map[string]any {
			result := map[string]any{
				"requested":   true,
				"applied":     true,
				"action":      "desktop_restart_scheduled",
				"reason":      reason,
				"hub_running": true,
			}
			if restartRequested.CompareAndSwap(false, true) {
				go func() {
					time.Sleep(150 * time.Millisecond)
					cancel()
				}()
			}
			return result
		}
	}
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	registryRegion := strings.TrimSpace(os.Getenv("PRISM_REGION"))
	if cfg, loadErr := bridgeconfig.Load(configPath); loadErr == nil {
		registryRegion = cfg.Prism.Region
	}
	registry := pluginregistry.NewDefaultForRegion(registryRegion)
	if err := registry.Load(); err != nil {
		fmt.Fprintf(stderr, "load plugin catalog cache failed: %v\n", err)
	}
	registry.Start(ctx)
	installer := plugininstall.New("")
	if err := runtimes.NewNodeManager("", nil).GC(dashboardNodeRuntimeReferences(configPath, installer), time.Now(), 30*24*time.Hour); err != nil {
		fmt.Fprintf(stderr, "clean shared plugin runtimes failed: %v\n", err)
	}
	pluginpkg.CleanupStaleRuntimeLeases(host.DefaultDataPath(filepath.Join("runtime", "plugins")))
	installer.OnActivated = func(reloadCtx context.Context, pluginID string) error {
		activeRoot, err := plugininstall.ActivePath(installer.Root, pluginID)
		if err != nil {
			return err
		}
		manifestPath, err := manifestInDir(activeRoot)
		if err != nil {
			return err
		}
		manifest, err := pluginpkg.LoadManifest(manifestPath)
		if err != nil {
			return err
		}
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			return err
		}
		// Keep the stable installer root in configuration. Plugin discovery reads
		// state.json and resolves the active version beneath this directory.
		if err := activatePluginConfig(cfg, configPath, installer.Root, manifest); err != nil {
			return err
		}
		if reloader := currentDashboardRuntimePluginReloader(); reloader != nil {
			return reloader.ReloadPlugin(reloadCtx, pluginID)
		}
		return nil
	}
	// The embedded runtime owns plugin process groups. Dashboard shutdown must
	// wait for its defers to close them before the sidecar itself exits;
	// otherwise macOS reparents surviving plugin children to launchd.
	runtimeDone := make(chan struct{})
	go func() {
		defer close(runtimeDone)
		runEmbeddedHubRuntime(ctx, configPath, dbPath, stdout, stderr)
	}()
	doctorService := newDoctorService(ctx, configPath, dbPath, installer, registry)
	code := serveDashboardUIContextWithOptions(ctx, configPath, dbPath, addr, "Prism Hub", false, dashboardServerOptions{
		RestartHub:      restartHub,
		PluginRegistry:  registry,
		PluginInstaller: installer,
		Doctor:          doctorService,
	}, stdout, stderr)
	// The dashboard only returns once ctx is cancelled. Wait for the bridge's
	// bounded adapter shutdown so the Desktop supervisor never races it.
	select {
	case <-runtimeDone:
	case <-time.After(12 * time.Second):
		fmt.Fprintln(stderr, "Prism Hub runtime shutdown timed out")
	}
	if desktopManaged && restartRequested.Load() && code == 0 {
		return desktopManagedRestartExitCode
	}
	return code
}

func dashboardNodeRuntimeReferences(configPath string, installer *plugininstall.Installer) map[string]bool {
	referenced := map[string]bool{}
	checkRoot := func(root string) {
		manifestPath, err := manifestInDir(root)
		if err != nil {
			return
		}
		manifest, err := pluginpkg.LoadManifest(manifestPath)
		if err == nil && runtimes.RequiresNode(manifest.RuntimeDependencies.Node) {
			referenced[runtimes.NodeVersion] = true
		}
	}
	if cfg, err := bridgeconfig.Load(configPath); err == nil {
		if discovered, scanErr := pluginpkg.ScanDirs(pluginDirs(cfg)); scanErr == nil {
			for _, item := range discovered {
				if runtimes.RequiresNode(item.Manifest.RuntimeDependencies.Node) {
					referenced[runtimes.NodeVersion] = true
				}
			}
		}
	}
	if installer == nil {
		return referenced
	}
	for _, entry := range pluginregistry.OfficialCatalog() {
		state, err := installer.State(entry.ID)
		if err != nil {
			continue
		}
		for _, version := range []string{state.ActiveVersion, state.PreviousVersion} {
			if version == "" {
				continue
			}
			root := filepath.Join(installer.Root, entry.ID, "versions", version)
			if version == "current" {
				root = filepath.Join(installer.Root, entry.ID, "current")
			}
			checkRoot(root)
		}
	}
	return referenced
}

func runEmbeddedHubRuntime(ctx context.Context, configPath, dbPath string, stdout, stderr io.Writer) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "Hub runtime waiting for configuration: %v\n", err)
		return
	}
	cfg.Normalize()
	if !cfg.Bridge.Enabled || strings.TrimSpace(cfg.Prism.HostToken) == "" {
		fmt.Fprintln(stderr, "Hub runtime waiting for login or bridge enablement")
		return
	}
	db, err := prismdb.Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "open Hub runtime database failed: %v\n", err)
		return
	}
	defer db.Close()
	apiURL := cfg.Prism.APIURL()
	realtimeURL := cfg.Prism.RealtimeURL()
	if code := runBridgeRuntime(ctx, cfg, db, apiURL, realtimeURL, configPath, dbPath, false, stdout, stderr); code != 0 && ctx.Err() == nil {
		fmt.Fprintf(stderr, "Hub runtime stopped: exit=%d\n", code)
	}
}
