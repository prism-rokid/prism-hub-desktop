package bridge

import (
	"context"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
)

func buildLiveConversationDetailSnapshot(
	ctx context.Context,
	db *bridgedb.DB,
	service *pluginruntime.Service,
	detailSnapshots *DetailSnapshotStore,
	conversationID string,
	pluginID string,
	session pluginruntime.NativeSession,
	fallbackCwd string,
	details map[string]any,
) (map[string]any, error) {
	query := &QueryService{
		DB:              db,
		History:         &HistoryService{DB: db},
		Service:         service,
		DetailSnapshots: detailSnapshots,
	}
	if service != nil {
		// Watcher events have already been resolved to a durable canonical binding.
		// Re-enumerating every Plugin session here turns each detail update into a
		// directory rebuild and blocks live state behind a local SQLite read.
		if conversation, err := query.resolveConversationForRuntime(ctx, conversationID, pluginID); err == nil {
			pluginID, pluginErr := RequirePlugin(ctx, service, pluginID)
			if pluginErr != nil {
				return nil, pluginErr
			}
			return query.detailSnapshotEnvelope(ctx, conversation, pluginID, details), nil
		}
	}
	base := map[string]any{}
	if detailSnapshots != nil {
		if cached, ok := detailSnapshots.GetForRuntime(pluginID, conversationID); ok {
			for key, value := range cached {
				base[key] = value
			}
		}
	}
	base["conversation_id"] = conversationID
	pluginID, pluginErr := RequirePlugin(ctx, service, pluginID)
	if pluginErr != nil {
		return nil, pluginErr
	}
	base["plugin_id"] = pluginID
	base["cwd"] = firstNonEmpty(
		stringFromAny(details["cwd"]),
		stringFromAny(base["cwd"]),
		session.Cwd,
		fallbackCwd,
	)
	base["title"] = firstNonEmpty(
		stringFromAny(details["title"]),
		stringFromAny(base["title"]),
	)
	payload := canonicalDetailSnapshot(base, details, firstNonEmpty(
		stringFromAny(details["messages_signature"]),
		stringFromAny(base["messages_signature"]),
	))
	payload = applyRuntimeProfile(payload, runtimeProfileMetadata(service, pluginID))
	if detailSnapshots != nil {
		detailSnapshots.PutForRuntime(pluginID, payload)
	}
	return payload, nil
}
