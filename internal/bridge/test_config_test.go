package bridge

import bridgeconfig "rokid-prism-hub/internal/config"

// remoteAuthorizedConfig is explicit test setup for remote-facing bridge
// behavior. Production config stays fail-closed until the cloud host-plugin
// registry has been read successfully.
func remoteAuthorizedConfig(pluginIDs ...string) *bridgeconfig.Config {
	cfg := bridgeconfig.Default()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{
		"codex":    true,
		"openclaw": true,
		"hermes":   true,
		"opencode": true,
		"demo":     true,
	}
	for _, pluginID := range pluginIDs {
		cfg.Bridge.RemoteAuthorizations[pluginID] = true
	}
	return cfg
}
