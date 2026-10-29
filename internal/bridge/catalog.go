package bridge

import (
	"context"
	"strings"
	"unicode"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	transport "rokid-prism-hub/internal/realtime"
)

// ReasonDetectedNotVisible is reported when an adapter probes available and
// claims native-visible input, but no end-to-end visibility check has confirmed
// a marker reached the native visible session yet. This mirrors the
// "detected_but_not_visible" status from the design doc.
const ReasonDetectedNotVisible = "detected_but_not_visible"

// BuildCatalogPlugins maps the live PluginBridge adapter states into the Prism
// catalog.snapshot plugin list.
//
// Only plugins currently enabled for the remote mainline are published. A local
// plugin may still be loaded in the managed runtime even when it is omitted
// here; omission means "do not advertise remotely", not "plugin absent".
//
// Field mapping:
//   - Available:   Probe-reported (service.go Status()) when enabled.
//   - VisibilityVerified: most recent bridge_visibility_checks row for the plugin.
//   - AvailabilityReason: empty when supported; "detected_but_not_visible" when
//     probed but not visibility-verified; otherwise the probe failure reason.
//
// visibilityDB may be nil; in that case visibility is treated as unverified.
func BuildCatalogPlugins(ctx context.Context, svc *pluginruntime.Service, visibilityDB *bridgedb.DB, cfg *bridgeconfig.Config) []transport.CatalogPlugin {
	if svc == nil {
		return nil
	}
	ids := svc.AdapterIDs()
	out := make([]transport.CatalogPlugin, 0, len(ids))
	for _, id := range ids {
		enabled := pluginruntime.RemoteEnabled(cfg, id)
		if !enabled {
			continue
		}
		adapter := svc.Adapter(id)
		if adapter == nil {
			continue
		}
		st := svc.AdapterStatus(ctx, id, adapter)
		pluginID := declaredPluginIDFromCapability(adapter, st.Capability)
		if pluginID == "" {
			continue
		}
		out = append(out, catalogPluginFromStatus(ctx, st, adapter, pluginID, visibilityDB, cfg))
	}
	return out
}

func catalogPluginFromStatus(ctx context.Context, st pluginruntime.PluginStatus, adapter pluginruntime.PluginAdapter, pluginID string, visibilityDB *bridgedb.DB, cfg *bridgeconfig.Config) transport.CatalogPlugin {
	visibilityVerified := false
	if visibilityDB != nil {
		if check, err := visibilityDB.LatestVisibilityCheck(ctx, st.ID); err == nil {
			visibilityVerified = check.Visible
		}
	}
	metadata := metadataFromAdapter(adapter)
	// Integration mode is resolved for the active runtime mode: per-mode
	// runtime_integration wins, so dual-mode plugins (CDP desktop + WS-gateway
	// native) report the mode-accurate kind. The probe capability carries the
	// manifest's top-level declaration as fallback.
	integrationMode := string(st.Capability.IntegrationMode)
	if resolved := strings.TrimSpace(metadata.RuntimeIntegration[metadata.CurrentRuntimeMode]); resolved != "" {
		integrationMode = resolved
	}
	// Launchability comes solely from the manifest's managed_launch_specs
	// projection; the managed_launch env-contract bool deliberately does not
	// imply a launchable target.
	item := transport.CatalogPlugin{
		PluginID:             pluginID,
		DisplayName:          firstNonEmpty(metadata.DisplayName, displayFromID(pluginID)),
		Description:          metadata.Description,
		IconURL:              metadata.IconURL,
		IconSVGURL:           metadata.IconSVGURL,
		Version:              metadata.Version,
		Installed:            true, // present in the Service registry => installed
		Available:            st.Available,
		Enabled:              true,
		VisibilityVerified:   visibilityVerified,
		Surface:              st.Surface,
		IntegrationMode:      integrationMode,
		ManagedLaunch:        len(metadata.ManagedLaunchTargets) > 0,
		ManagedLaunchTargets: metadata.ManagedLaunchTargets,
		RuntimeModes:         append([]string(nil), metadata.RuntimeModes...),
		CurrentRuntimeMode:   metadata.CurrentRuntimeMode,
	}
	item.CanAttachSession = st.Capability.CanAttachSession
	item.CanStartSessionWithMessage = st.Capability.CanStartSessionWithMessage
	item.CanOpenDraft = st.Capability.CanOpenDraft
	item.CanListSessions = st.Capability.CanListSessions
	item.CanReadHistory = st.Capability.CanReadHistory
	item.CanInterrupt = st.Capability.CanInterrupt
	item.CanApproval = st.Capability.CanApproval
	item.CanForwardSync = st.Capability.CanForwardSync
	item.CanReverseSync = st.Capability.CanReverseSync
	item.CanWaitRun = st.Capability.CanWaitRun
	item.CanReadStatus = st.Capability.CanReadStatus
	item.CanControlSession = st.Capability.CanControlSession
	item.ReverseSyncEnabled = st.Capability.CanReverseSync && pluginruntime.ReverseSyncEnabled(cfg, st.ID)
	if reader, ok := adapter.(pluginruntime.AgentUsageReader); ok {
		if usage, err := reader.ReadAgentUsage(ctx); err == nil && usage != nil {
			item.AgentUsage = catalogAgentUsage(usage)
		}
	}
	item.RemoteSupported, item.RemoteSupportReason = remoteConversationSupport(item)
	item.VisibleInRemote = item.Enabled && item.RemoteSupported

	// AvailabilityReason distinguishes the three client-facing tiers:
	//  1. available + visibility-verified    -> supported, no reason
	//  2. available + not verified           -> detected_but_not_visible
	//  3. unavailable                        -> probe/unavailable reason (or disabled)
	switch {
	case !st.Available:
		if st.LastError != "" {
			item.AvailabilityReason = st.LastError
		} else {
			item.AvailabilityReason = "unavailable"
		}
	case !visibilityVerified:
		item.AvailabilityReason = ReasonDetectedNotVisible
	}
	return item
}

func catalogAgentUsage(usage *pluginruntime.AgentUsage) *transport.CatalogAgentUsage {
	if usage == nil {
		return nil
	}
	result := &transport.CatalogAgentUsage{UpdatedAt: usage.UpdatedAt}
	if fiveHour := usage.FiveHour; fiveHour != nil && fiveHour.UsedPercent >= 0 && fiveHour.UsedPercent <= 100 && fiveHour.WindowMinutes == 300 && fiveHour.ResetsAt >= 0 {
		result.FiveHour = &transport.CatalogAgentUsageFiveHour{
			UsedPercent: fiveHour.UsedPercent, WindowMinutes: fiveHour.WindowMinutes, ResetsAt: fiveHour.ResetsAt,
		}
	}
	if weekly := usage.Weekly; weekly != nil && weekly.UsedPercent >= 0 && weekly.UsedPercent <= 100 && weekly.WindowMinutes == 10_080 && weekly.ResetsAt >= 0 {
		result.Weekly = &transport.CatalogAgentUsageWeekly{
			UsedPercent: weekly.UsedPercent, WindowMinutes: weekly.WindowMinutes, ResetsAt: weekly.ResetsAt,
		}
	}
	if summary := usage.AccountSummary; summary != nil && summary.LifetimeTokens >= 0 && summary.PeakDailyTokens >= 0 && summary.CurrentStreakDays >= 0 && summary.LongestStreakDays >= 0 {
		result.AccountSummary = &transport.CatalogAccountUsageSummary{
			LifetimeTokens: summary.LifetimeTokens, PeakDailyTokens: summary.PeakDailyTokens,
			CurrentStreakDays: summary.CurrentStreakDays, LongestStreakDays: summary.LongestStreakDays,
		}
	}
	if result.FiveHour == nil && result.Weekly == nil && result.AccountSummary == nil {
		return nil
	}
	return result
}

func metadataFromAdapter(adapter pluginruntime.PluginAdapter) pluginruntime.LocalMetadata {
	if provider, ok := adapter.(pluginruntime.MetadataProvider); ok {
		return provider.LocalMetadata()
	}
	return pluginruntime.LocalMetadata{}
}

func remoteConversationSupport(item transport.CatalogPlugin) (bool, string) {
	switch {
	case !item.Installed:
		return false, "not_installed"
	case !item.CanForwardSync:
		return false, "missing_forward_sync"
	case !item.CanListSessions:
		return false, "missing_list_sessions"
	case !item.CanReadHistory:
		return false, "missing_read_history"
	case !item.CanWaitRun:
		return false, "missing_wait_run"
	case !item.CanAttachSession:
		return false, "missing_attach_session"
	case !item.CanStartSessionWithMessage:
		return false, "missing_start_session_with_message"
	default:
		return true, ""
	}
}

func remoteConversationSupportFromCapability(capability pluginruntime.Capability) (bool, string) {
	return remoteConversationSupport(transport.CatalogPlugin{
		Installed:                  true,
		CanForwardSync:             capability.CanForwardSync,
		CanListSessions:            capability.CanListSessions,
		CanReadHistory:             capability.CanReadHistory,
		CanWaitRun:                 capability.CanWaitRun,
		CanReadStatus:              capability.CanReadStatus,
		CanControlSession:          capability.CanControlSession,
		CanAttachSession:           capability.CanAttachSession,
		CanStartSessionWithMessage: capability.CanStartSessionWithMessage,
		CanOpenDraft:               capability.CanOpenDraft,
	})
}

// displayFromID turns an adapter id like "openclaw" into "OpenClaw" by
// capitalizing the first letter and any letter following a separator.
func displayFromID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return id
	}
	var b strings.Builder
	capitalize := true
	for _, r := range id {
		if capitalize && unicode.IsLetter(r) {
			b.WriteRune(unicode.ToTitle(r))
			capitalize = false
			continue
		}
		if r == '.' || r == '_' || r == '-' || r == ' ' {
			b.WriteRune(r)
			capitalize = true
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
