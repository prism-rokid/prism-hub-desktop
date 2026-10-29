package pluginruntime

import (
	"strings"

	bridgeconfig "rokid-prism-hub/internal/config"
)

const (
	ReverseSyncOff              = "off"
	ReverseSyncPrismManagedOnly = "prism_managed_only"
	ReverseSyncAllThreads       = "all_threads"
)

// RemoteEnabled returns whether the cloud-authoritative host-plugin registry
// permits a plugin to participate in Prism remote routing. This authorization
// is runtime-only: local config can control how a plugin starts, but never
// whether it is exposed to a remote client. Until the registry is successfully
// read, access is fail-closed.
func RemoteEnabled(cfg *bridgeconfig.Config, pluginID string) bool {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return false
	}
	if cfg == nil || !cfg.Bridge.RemoteAuthorizationReady {
		return false
	}
	return cfg.Bridge.RemoteAuthorizations[pluginID]
}

func ReverseSyncMode(cfg *bridgeconfig.Config, pluginID string) string {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return ReverseSyncOff
	}
	if cfg == nil {
		return defaultReverseSyncMode(pluginID)
	}
	item, ok := cfg.Bridge.Plugins[pluginID]
	if !ok {
		return defaultReverseSyncMode(pluginID)
	}
	switch strings.ToLower(strings.TrimSpace(item.ReverseSync)) {
	case ReverseSyncOff, ReverseSyncPrismManagedOnly, ReverseSyncAllThreads:
		return strings.ToLower(strings.TrimSpace(item.ReverseSync))
	case "":
		return defaultReverseSyncMode(pluginID)
	default:
		return defaultReverseSyncMode(pluginID)
	}
}

func ReverseSyncEnabled(cfg *bridgeconfig.Config, pluginID string) bool {
	return ReverseSyncMode(cfg, pluginID) != ReverseSyncOff
}

func defaultReverseSyncMode(pluginID string) string {
	// The Syncer upgrades this conservative default when a live adapter
	// advertises CanListSessions. Keep policy independent of a plugin name.
	return ReverseSyncPrismManagedOnly
}
