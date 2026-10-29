package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	prismapi "rokid-prism-hub/internal/api"
	bridgeconfig "rokid-prism-hub/internal/config"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
)

// hostPluginOverlay contains the latest cloud-authoritative set of plugin IDs
// registered for this Host. It is never derived from local enabled config.
type hostPluginOverlay struct {
	mu     sync.RWMutex
	values map[string]bool
	ready  bool
}

func newHostPluginOverlay() *hostPluginOverlay {
	return &hostPluginOverlay{}
}

func (o *hostPluginOverlay) Replace(values map[string]bool) bool {
	if o == nil {
		return false
	}
	copied := make(map[string]bool, len(values))
	for id, enabled := range values {
		id = strings.ToLower(strings.TrimSpace(id))
		if id == "" {
			continue
		}
		copied[id] = enabled
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	changed := !o.ready || !samePluginSet(o.values, copied)
	o.values = copied
	o.ready = true
	return changed
}

// Invalidate clears the last successful registry read. A failed cloud read is
// not equivalent to a successful empty registry: remote access must remain
// disabled until the next successful replacement.
func (o *hostPluginOverlay) Invalidate() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	changed := o.ready || len(o.values) > 0
	o.values = nil
	o.ready = false
	return changed
}

func (o *hostPluginOverlay) Snapshot() (map[string]bool, bool) {
	if o == nil {
		return nil, false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.ready {
		return nil, false
	}
	copied := make(map[string]bool, len(o.values))
	for id, enabled := range o.values {
		copied[id] = enabled
	}
	return copied, true
}

// IsReady reports whether the initial cloud read has completed at least
// once (kept from the doctor branch; the sync loop marks readiness).
func (o *hostPluginOverlay) IsReady() bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ready
}
func isPermanentAuthError(err error) bool {
	if err == nil {
		return false
	}
	var responseError *prismapi.ResponseError
	if errors.As(err, &responseError) && responseError != nil {
		if strings.Contains(responseError.Status, "401") || strings.Contains(responseError.Status, "403") {
			return true
		}
		switch strings.ToLower(strings.TrimSpace(responseError.Code)) {
		case "host_forbidden", "host_token_expired", "unauthorized", "token_expired", "invalid_token":
			return true
		default:
			return false
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "401 unauthorized") || strings.Contains(errStr, "host_forbidden")
}

// startHostPluginSync performs the mandatory initial cloud read. Later reads
// are triggered by host.plugin.refresh, a realtime reconnect, a plugin reload,
// or the built-in retry timer. A failed cloud read keeps the last successful
// authorization (fail stale) so a transient API outage cannot silence every
// plugin; the retry timer keeps re-syncing until the cloud list is readable
// again, and the next successful read replaces the stale allow-list with the
// cloud-authoritative set.
type hostPluginSyncStatus struct {
	State   string
	Detail  string
	Count   int
	Updated time.Time
}

func startHostPluginSync(_ context.Context, fallback *bridgeconfig.Config, configPath string, svc *pluginruntime.Service, overlay *hostPluginOverlay, logger *slog.Logger, onChanged func()) func() bool {
	return startHostPluginSyncWithStatus(fallback, configPath, svc, overlay, logger, onChanged, nil)
}

func startHostPluginSyncWithStatus(fallback *bridgeconfig.Config, configPath string, svc *pluginruntime.Service, overlay *hostPluginOverlay, logger *slog.Logger, onChanged func(), onStatus func(hostPluginSyncStatus)) func() bool {
	if overlay == nil || svc == nil {
		return func() bool { return false }
	}

	const (
		retryBaseDelay  = 30 * time.Second
		retryMaxDelay   = 5 * time.Minute
		deregisterGrace = 10 * time.Minute
	)

	var (
		runMu           sync.Mutex
		recheckTimer    *time.Timer
		retryDelay      time.Duration
		lastSyncedCount int
		// staleSince records the first local-miss time per cloud-registered
		// plugin. A registration is only removed after the local plugin has
		// been missing for deregisterGrace, so upgrades and hot reloads with a
		// briefly absent directory can never wipe cloud authorizations.
		staleSince = map[string]time.Time{}
	)

	// syncOnce is referenced by the retry timer before it is declared, so it
	// must stay a variable rather than a function literal binding.
	var syncOnce func() bool

	scheduleRecheck := func(delay time.Duration) {
		if recheckTimer != nil {
			recheckTimer.Stop()
		}
		recheckTimer = time.AfterFunc(delay, func() { _ = syncOnce() })
	}

	nextRetryDelay := func() time.Duration {
		if retryDelay == 0 {
			retryDelay = retryBaseDelay
			return retryDelay
		}
		retryDelay *= 2
		if retryDelay > retryMaxDelay {
			retryDelay = retryMaxDelay
		}
		return retryDelay
	}

	syncOnce = func() bool {
		runMu.Lock()
		defer runMu.Unlock()
		if onStatus != nil {
			onStatus(hostPluginSyncStatus{State: "syncing", Detail: "正在同步 Host Plugin 授权集。", Count: lastSyncedCount, Updated: time.Now().UTC()})
		}
		cfg := latestBridgeConfigRaw(fallback, configPath)
		if cfg == nil {
			overlay.Invalidate()
			if onStatus != nil {
				onStatus(hostPluginSyncStatus{State: "error", Detail: "读取 Hub 配置失败，无法同步 Host Plugin。", Count: lastSyncedCount, Updated: time.Now().UTC()})
			}
			return false
		}
		plugins, err := prismapi.ListUserHostPlugins(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID)
		if err != nil {
			if logger != nil {
				logger.Warn("host plugin sync failed; keeping last known authorization", "err", err, "permanent_auth", isPermanentAuthError(err))
			}
			if isPermanentAuthError(err) {
				// Retrying a rejected credential cannot succeed; keep the last
				// authorization and wait for an explicit refresh (re-login,
				// host.plugin.refresh, reconnect). Deliberately no recheck
				// timer here: pending staleSince deregistrations resume on
				// the next external trigger rather than on this dead loop.
				if onStatus != nil {
					onStatus(hostPluginSyncStatus{State: "auth_required", Detail: "Host Plugin 同步鉴权失败，请重新登录。", Count: lastSyncedCount, Updated: time.Now().UTC()})
				}
				return false
			}
			if onStatus != nil {
				onStatus(hostPluginSyncStatus{State: "stale", Detail: "Host Plugin 同步暂时失败，继续使用上次成功的授权集。", Count: lastSyncedCount, Updated: time.Now().UTC()})
			}
			scheduleRecheck(nextRetryDelay())
			return false
		}
		localIDs, scanErr := localManagedPluginIDs(cfg)
		if scanErr != nil {
			// A failed local scan must never drive deregistrations; keep the
			// cloud list authoritative and retry on the next trigger.
			if logger != nil {
				logger.Warn("scan local plugins failed; skip deregistration reconcile", "err", scanErr)
			}
			localIDs = nil
		} else {
			removed, removeErr := reconcileRemovedRemotePlugins(
				plugins.Items,
				localIDs,
				staleSince,
				time.Now(),
				deregisterGrace,
				func(pluginID string) error {
					_, err := prismapi.DeleteUserHostPlugin(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, pluginID)
					if err != nil && prismapi.IsNotFoundError(err) {
						return nil
					}
					return err
				},
			)
			if removeErr != nil && logger != nil {
				logger.Warn("deregister removed local plugins failed", "err", removeErr)
			}
			if removed {
				if plugins, err = prismapi.ListUserHostPlugins(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID); err != nil {
					if logger != nil {
						logger.Warn("host plugin sync refresh failed; keeping last known authorization", "err", err)
					}
					if onStatus != nil {
						onStatus(hostPluginSyncStatus{State: "stale", Detail: "Host Plugin 同步暂时失败，继续使用上次成功的授权集。", Count: lastSyncedCount, Updated: time.Now().UTC()})
					}
					scheduleRecheck(nextRetryDelay())
					return false
				}
			}
		}
		// Registration is also the runtime metadata refresh endpoint. Restrict
		// refreshes to plugins already authorized in the cloud and present in
		// the local managed runtime so discovering a local manifest can never
		// expose a new plugin remotely.
		if localIDs != nil {
			metadataChanged, metadataErr := reconcileRegisteredRemotePluginMetadata(
				plugins.Items,
				localIDs,
				func(pluginID string) (map[string]any, error) {
					return dashboardHostPluginRegistrationPayloadWithService(cfg, svc, pluginID)
				},
				func(payload map[string]any) error {
					_, registerErr := prismapi.RegisterUserHostPlugin(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, payload)
					return registerErr
				},
			)
			if metadataErr != nil {
				// Existing cloud authorization remains valid if a best-effort runtime
				// metadata refresh fails. Keep remote routing available and retry on
				// the next reconnect or plugin reload.
				if logger != nil {
					logger.Warn("host plugin runtime metadata refresh failed", "err", metadataErr)
				}
			} else if metadataChanged {
				if plugins, err = prismapi.ListUserHostPlugins(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID); err != nil {
					if logger != nil {
						logger.Warn("host plugin metadata refresh readback failed; keeping last known authorization", "err", err)
					}
					if onStatus != nil {
						onStatus(hostPluginSyncStatus{State: "stale", Detail: "Host Plugin 同步暂时失败，继续使用上次成功的授权集。", Count: lastSyncedCount, Updated: time.Now().UTC()})
					}
					scheduleRecheck(nextRetryDelay())
					return false
				}
			}
		}
		if changed := overlay.Replace(remotePluginIDs(plugins.Items)); changed && onChanged != nil {
			// Authorization set or readiness transitioned: let the caller
			// republish snapshots so remote views converge immediately.
			onChanged()
		}
		lastSyncedCount = len(plugins.Items)
		if onStatus != nil {
			onStatus(hostPluginSyncStatus{State: "synced", Detail: "Host Plugin 授权集已同步。", Count: lastSyncedCount, Updated: time.Now().UTC()})
		}
		retryDelay = 0
		if len(staleSince) > 0 {
			// Pending deregistrations must re-check even without external
			// triggers, otherwise a deleted plugin would linger until the next
			// reconnect.
			earliest := time.Now().Add(deregisterGrace)
			for _, first := range staleSince {
				if deadline := first.Add(deregisterGrace); deadline.Before(earliest) {
					earliest = deadline
				}
			}
			scheduleRecheck(time.Until(earliest) + time.Second)
		} else if recheckTimer != nil {
			recheckTimer.Stop()
			recheckTimer = nil
		}
		return true
	}
	_ = syncOnce()
	return syncOnce
}

func localManagedPluginIDs(cfg *bridgeconfig.Config) (map[string]bool, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config missing")
	}
	discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg))
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(discovered))
	for _, item := range discovered {
		pluginID := strings.ToLower(strings.TrimSpace(item.Manifest.ID))
		if pluginID != "" && bridgePluginAllowed(cfg, pluginID) {
			ids[pluginID] = true
		}
	}
	return ids, nil
}

// reconcileRegisteredRemotePluginMetadata refreshes only existing cloud
// registrations. It intentionally does not use discovered local IDs as a
// source of new authorizations.
func reconcileRegisteredRemotePluginMetadata(
	plugins []prismapi.HostPlugin,
	localPluginIDs map[string]bool,
	payloadFor func(pluginID string) (map[string]any, error),
	register func(payload map[string]any) error,
) (bool, error) {
	if payloadFor == nil || register == nil {
		return false, fmt.Errorf("metadata refresh callbacks missing")
	}
	changed := false
	for _, remote := range plugins {
		pluginID := strings.ToLower(strings.TrimSpace(remote.PluginID))
		if pluginID == "" || !localPluginIDs[pluginID] {
			continue
		}
		payload, err := payloadFor(pluginID)
		if err != nil {
			return changed, fmt.Errorf("build metadata for %s: %w", pluginID, err)
		}
		if hostPluginRuntimeMetadataMatches(remote, payload) {
			continue
		}
		if err := register(payload); err != nil {
			return changed, fmt.Errorf("refresh metadata for %s: %w", pluginID, err)
		}
		changed = true
	}
	return changed, nil
}

func hostPluginRuntimeMetadataMatches(remote prismapi.HostPlugin, payload map[string]any) bool {
	if strings.TrimSpace(remote.Version) != strings.TrimSpace(payloadString(payload["version"])) ||
		strings.TrimSpace(remote.IntegrationMode) != strings.TrimSpace(payloadString(payload["integration_mode"])) {
		return false
	}
	expected, ok := payload["capabilities"].(map[string]any)
	if !ok {
		return false
	}
	for key, value := range expected {
		expectedValue, ok := value.(bool)
		if !ok {
			return false
		}
		actual, ok := remote.Capabilities[key].(bool)
		if !ok || actual != expectedValue {
			return false
		}
	}
	return true
}

func payloadString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func remotePluginIDs(plugins []prismapi.HostPlugin) map[string]bool {
	values := make(map[string]bool, len(plugins))
	for _, plugin := range plugins {
		if pluginID := strings.ToLower(strings.TrimSpace(plugin.PluginID)); pluginID != "" {
			values[pluginID] = true
		}
	}
	return values
}

// reconcileRemovedRemotePlugins deregisters cloud rows whose local plugin has
// been missing for longer than grace. Registration is an explicit remote-access
// opt-in per plugin, so a locally deleted plugin must not linger as a dead
// card on remote clients. Only the current host's registrations are passed in,
// so rows of other hosts are never touched. A failed local scan must not call
// this at all: missing scan data would look like every plugin was removed.
// samePluginSet reports set equality for the lowercase plugin-id maps the
// overlay stores.
func samePluginSet(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for id, enabled := range left {
		if value, ok := right[id]; !ok || value != enabled {
			return false
		}
	}
	return true
}

// reconcileRemovedRemotePlugins deregisters cloud registrations whose local
// plugin has stayed missing past the grace window.
func reconcileRemovedRemotePlugins(
	plugins []prismapi.HostPlugin,
	localPluginIDs map[string]bool,
	staleSince map[string]time.Time,
	now time.Time,
	grace time.Duration,
	unregister func(pluginID string) error,
) (bool, error) {
	var firstErr error
	removed := false
	cloudIDs := make(map[string]bool, len(plugins))
	for _, remote := range plugins {
		pluginID := strings.ToLower(strings.TrimSpace(remote.PluginID))
		if pluginID == "" {
			continue
		}
		cloudIDs[pluginID] = true
		if localPluginIDs[pluginID] {
			delete(staleSince, pluginID)
			continue
		}
		first, tracked := staleSince[pluginID]
		if !tracked {
			staleSince[pluginID] = now
			continue
		}
		if now.Sub(first) < grace {
			continue
		}
		if unregister == nil {
			return removed, fmt.Errorf("unregister callback missing for %s", pluginID)
		}
		if err := unregister(pluginID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("deregister removed plugin %s: %w", pluginID, err)
			}
			continue
		}
		delete(staleSince, pluginID)
		removed = true
	}
	for pluginID := range staleSince {
		if !cloudIDs[pluginID] {
			delete(staleSince, pluginID)
		}
	}
	return removed, firstErr
}

func cloneBridgeConfig(cfg *bridgeconfig.Config) *bridgeconfig.Config {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.Bridge.PluginDirs = append([]string(nil), cfg.Bridge.PluginDirs...)
	if len(cfg.Bridge.PluginEnv) > 0 {
		cloned.Bridge.PluginEnv = map[string]string{}
		for k, v := range cfg.Bridge.PluginEnv {
			cloned.Bridge.PluginEnv[k] = v
		}
	}
	if len(cfg.Bridge.Plugins) > 0 {
		cloned.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
		for id, item := range cfg.Bridge.Plugins {
			copyItem := item
			if len(item.Env) > 0 {
				copyItem.Env = map[string]string{}
				for k, v := range item.Env {
					copyItem.Env[k] = v
				}
			}
			cloned.Bridge.Plugins[id] = copyItem
		}
	} else {
		cloned.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	}
	if len(cfg.Bridge.RemoteAuthorizations) > 0 {
		cloned.Bridge.RemoteAuthorizations = map[string]bool{}
		for id, enabled := range cfg.Bridge.RemoteAuthorizations {
			cloned.Bridge.RemoteAuthorizations[id] = enabled
		}
	}
	return &cloned
}

func applyHostPluginOverlay(cfg *bridgeconfig.Config, svc *pluginruntime.Service, overlay *hostPluginOverlay) *bridgeconfig.Config {
	if cfg == nil || overlay == nil {
		return cfg
	}
	values, ready := overlay.Snapshot()
	cloned := cloneBridgeConfig(cfg)
	cloned.Bridge.RemoteAuthorizationReady = ready
	cloned.Bridge.RemoteAuthorizations = map[string]bool{}
	if ready {
		for pluginID, enabled := range values {
			cloned.Bridge.RemoteAuthorizations[pluginID] = enabled
		}
	}
	return cloned
}
