package bridge

import (
	"context"
	"fmt"
	"strings"

	"rokid-prism-hub/internal/pluginruntime"
)

// RequirePlugin resolves a Plugin by its only identity. The service key,
// adapter ID, metadata and probe result must all agree on this PluginID.
func RequirePlugin(ctx context.Context, service *pluginruntime.Service, pluginID string) (string, error) {
	if service == nil {
		return "", fmt.Errorf("plugin service unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return "", fmt.Errorf("plugin_id required")
	}
	adapter := service.Adapter(pluginID)
	if adapter == nil {
		return "", fmt.Errorf("plugin not found: %s", pluginID)
	}
	if !strings.EqualFold(strings.TrimSpace(adapter.ID()), pluginID) {
		return "", fmt.Errorf("plugin registry identity mismatch: requested=%s adapter=%s", pluginID, adapter.ID())
	}
	declared, err := declaredPluginID(ctx, adapter)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(declared, pluginID) {
		return "", fmt.Errorf("plugin declaration mismatch: requested=%s declared=%s", pluginID, declared)
	}
	return pluginID, nil
}

func declaredPluginID(ctx context.Context, adapter pluginruntime.PluginAdapter) (string, error) {
	if adapter == nil {
		return "", fmt.Errorf("plugin adapter unavailable")
	}
	if pluginID := declaredPluginIDFromCapability(adapter, pluginruntime.Capability{}); pluginID != "" {
		return pluginID, nil
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return "", err
	}
	if pluginID := declaredPluginIDFromCapability(adapter, capability); pluginID != "" {
		return pluginID, nil
	}
	return "", fmt.Errorf("plugin_id not declared by runtime adapter %s", strings.TrimSpace(adapter.ID()))
}

func declaredPluginIDFromCapability(adapter pluginruntime.PluginAdapter, capability pluginruntime.Capability) string {
	if metadata := metadataFromAdapter(adapter); strings.TrimSpace(metadata.PluginID) != "" {
		return strings.TrimSpace(metadata.PluginID)
	}
	return strings.TrimSpace(capability.PluginID)
}
