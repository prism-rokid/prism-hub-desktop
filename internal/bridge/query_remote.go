package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
)

type resolvedConversation struct {
	ConversationID string
	PluginID       string
	Session        pluginruntime.NativeSession
	Hint           pluginruntime.NativeSessionHint
	Record         *bridgedb.SessionRecord
	Capability     pluginruntime.Capability
}

// ConversationHistoryStream is one short-lived relay from a Plugin's native
// body iterator to Realtime. It holds no durable client cursor state.
type ConversationHistoryStream struct {
	ConversationID string
	PluginID       string
	Events         <-chan pluginruntime.HistoryStreamEvent
}

func (s *QueryService) OpenConversationHistoryStream(ctx context.Context, conversationID, pluginID string, req pluginruntime.HistoryStreamRequest) (ConversationHistoryStream, error) {
	if s == nil || s.Service == nil {
		return ConversationHistoryStream{}, fmt.Errorf("query service unavailable")
	}
	if strings.TrimSpace(conversationID) == "" {
		return ConversationHistoryStream{}, fmt.Errorf("conversation_id required")
	}
	if strings.TrimSpace(pluginID) == "" {
		return ConversationHistoryStream{}, fmt.Errorf("plugin_id required")
	}
	if !pluginruntime.RemoteEnabled(s.CurrentConfig(), pluginID) {
		return ConversationHistoryStream{}, fmt.Errorf("%s remote access disabled", pluginID)
	}
	conversation, err := s.resolveConversationForRuntime(ctx, conversationID, pluginID)
	if err != nil {
		return ConversationHistoryStream{}, err
	}
	adapter, err := s.runtimeAdapter(conversation.PluginID)
	if err != nil {
		return ConversationHistoryStream{}, err
	}
	streamer, ok := adapter.(pluginruntime.HistoryStreamer)
	if !ok {
		return ConversationHistoryStream{}, fmt.Errorf("%s adapter does not implement readHistoryStream", conversation.PluginID)
	}
	events, err := streamer.ReadHistoryStream(ctx, conversation.Session, req)
	if err != nil {
		return ConversationHistoryStream{}, err
	}
	return ConversationHistoryStream{
		ConversationID: conversation.ConversationID,
		PluginID:       conversation.PluginID,
		Events:         events,
	}, nil
}

func (s *QueryService) listConversations(ctx context.Context, pluginID string, limit int) (map[string]any, error) {
	conversations, err := s.listResolvedConversations(ctx, pluginID)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(conversations) > limit {
		conversations = conversations[:limit]
	}
	items := make([]map[string]any, 0, len(conversations))
	for _, conversation := range conversations {
		items = append(items, s.conversationIndexEnvelope(conversation, pluginID))
	}
	return map[string]any{
		"plugin_id": pluginID,
		"items":     items,
	}, nil
}

func (s *QueryService) getConversationDetail(ctx context.Context, conversationID, pluginID string) (map[string]any, error) {
	conversation, err := s.resolveConversation(ctx, conversationID, pluginID)
	if err != nil {
		return nil, err
	}
	var cached map[string]any
	if s != nil && s.DetailSnapshots != nil {
		if existing, ok := s.DetailSnapshots.GetForRuntime(conversation.PluginID, conversation.ConversationID); ok {
			cached = existing
		}
	}
	if cached != nil {
		detail := applyRuntimeProfile(cached, runtimeProfileMetadata(s.Service, conversation.PluginID))
		detail = s.decorateDetailSnapshot(detail, conversation, false)
		if s != nil && s.DetailSnapshots != nil {
			s.DetailSnapshots.PutForRuntime(conversation.PluginID, detail)
		}
		return detail, nil
	}
	if conversation.Capability.IntegrationMode == pluginruntime.IntegrationModeProtocolNative {
		if adapter, adapterErr := s.runtimeAdapter(conversation.PluginID); adapterErr == nil {
			if reader, ok := adapter.(pluginruntime.DetailReader); ok {
				readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				if detail, readErr := reader.ReadDetail(readCtx, conversation.Session); readErr == nil {
					return s.detailSnapshotEnvelope(ctx, conversation, pluginID, detail), nil
				}
			}
		}
	}
	return s.detailSnapshotEnvelope(ctx, conversation, pluginID, nil), nil
}

func (s *QueryService) listResolvedConversations(ctx context.Context, pluginID string) ([]resolvedConversation, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("conversation query requires bridge db")
	}
	adapter, capability, err := s.requireAdapter(ctx, pluginID)
	if err != nil {
		return nil, err
	}
	lister, ok := adapter.(pluginruntime.SessionLister)
	if !ok {
		return nil, fmt.Errorf("%s adapter does not implement listSessions", pluginID)
	}
	hints, err := lister.ListSessions(ctx)
	if err != nil {
		return nil, err
	}
	recordsByNative, _, err := s.sessionRecordIndex(ctx, pluginID)
	if err != nil {
		return nil, err
	}
	out := make([]resolvedConversation, 0, len(hints))
	seenConversationIDs := map[string]struct{}{}
	for _, hint := range hints {
		if conversationArchivedFromHint(hint) {
			continue
		}
		hint.PluginID = firstNonEmpty(hint.PluginID, pluginID)
		if hint.PluginID == "" {
			hint.PluginID = pluginID
		}
		nativeKey := nativeSessionLookupKey(hint.NativeSessionID, hint.NativeThreadID)
		if nativeKey == "" {
			// A session without a stable native identity cannot be attached after
			// restart, so it is not a remote conversation candidate.
			continue
		}
		record := recordsByNative[nativeKey]
		if record == nil {
			persisted, persistErr := s.ensureConversationHintBinding(ctx, pluginID, hint)
			if persistErr != nil {
				return nil, persistErr
			}
			record = &persisted
		}
		conversationID := record.PrismConversationID
		session := sessionFromHint(hint)
		if record != nil {
			session = applySessionRecord(session, *record)
		}
		if _, exists := seenConversationIDs[conversationID]; exists {
			continue
		}
		seenConversationIDs[conversationID] = struct{}{}
		out = append(out, resolvedConversation{
			ConversationID: conversationID,
			PluginID:       pluginID,
			Session:        session,
			Hint:           hint,
			Record:         record,
			Capability:     capability,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		leftPinned := conversationPinned(out[i].Hint)
		rightPinned := conversationPinned(out[j].Hint)
		if leftPinned != rightPinned {
			return leftPinned
		}
		left := conversationSortTime(out[i]).UnixMilli()
		right := conversationSortTime(out[j]).UnixMilli()
		if left != right {
			return left > right
		}
		return out[i].ConversationID < out[j].ConversationID
	})
	return out, nil
}

func (s *QueryService) resolveConversation(ctx context.Context, conversationID, pluginID string) (resolvedConversation, error) {
	normalizedConversationID := strings.TrimSpace(conversationID)
	normalizedPluginID := strings.TrimSpace(pluginID)
	if normalizedConversationID == "" {
		return resolvedConversation{}, fmt.Errorf("conversation_id required")
	}
	if normalizedPluginID == "" {
		return resolvedConversation{}, fmt.Errorf("runtime adapter unavailable for conversation query")
	}
	if s != nil && s.DB != nil {
		if record, err := s.DB.FindSession(ctx, normalizedConversationID, normalizedPluginID); err == nil {
			_, capability, adapterErr := s.requireAdapter(ctx, normalizedPluginID)
			if adapterErr != nil {
				return resolvedConversation{}, adapterErr
			}
			if capability.IntegrationMode == pluginruntime.IntegrationModeProtocolNative {
				session := applySessionRecord(pluginruntime.NativeSession{
					PluginID: normalizedPluginID,
					Visible:  true,
				}, record)
				return resolvedConversation{
					ConversationID: normalizedConversationID,
					PluginID:       normalizedPluginID,
					Session:        session,
					Hint: pluginruntime.NativeSessionHint{
						PluginID:        normalizedPluginID,
						NativeSessionID: record.NativeSessionID,
						NativeThreadID:  record.NativeThreadID,
						Cwd:             record.Cwd,
						Visible:         true,
					},
					Record:     &record,
					Capability: capability,
				}, nil
			}
			recordNativeKey := nativeSessionLookupKey(record.NativeSessionID, record.NativeThreadID)
			if recordNativeKey != "" {
				if conversations, listErr := s.listResolvedConversations(ctx, normalizedPluginID); listErr == nil {
					for _, conversation := range conversations {
						conversationNativeKey := nativeSessionLookupKey(conversation.Session.NativeSessionID, conversation.Session.NativeThreadID)
						if conversationNativeKey == recordNativeKey {
							return conversation, nil
						}
					}
				}
			}
			session := applySessionRecord(pluginruntime.NativeSession{
				PluginID: normalizedPluginID,
				Visible:  true,
			}, record)
			return resolvedConversation{
				ConversationID: normalizedConversationID,
				PluginID:       normalizedPluginID,
				Session:        session,
				Hint: pluginruntime.NativeSessionHint{
					PluginID:        normalizedPluginID,
					NativeSessionID: record.NativeSessionID,
					NativeThreadID:  record.NativeThreadID,
					Cwd:             record.Cwd,
					Title:           firstNonEmpty(record.NativeThreadID, record.NativeSessionID, normalizedConversationID),
					Visible:         true,
				},
				Record:     &record,
				Capability: capability,
			}, nil
		} else if err != sql.ErrNoRows {
			return resolvedConversation{}, err
		}
	}
	conversations, err := s.listResolvedConversations(ctx, normalizedPluginID)
	if err != nil {
		return resolvedConversation{}, err
	}
	for _, conversation := range conversations {
		if conversation.ConversationID == normalizedConversationID {
			return conversation, nil
		}
	}
	return resolvedConversation{}, fmt.Errorf("conversation not found: %s", normalizedConversationID)
}

func (s *QueryService) resolveConversationForRuntime(ctx context.Context, conversationID, pluginID string) (resolvedConversation, error) {
	normalizedConversationID := strings.TrimSpace(conversationID)
	normalizedPluginID := strings.TrimSpace(pluginID)
	if normalizedConversationID == "" {
		return resolvedConversation{}, fmt.Errorf("conversation_id required")
	}
	if normalizedPluginID == "" {
		return resolvedConversation{}, fmt.Errorf("runtime adapter unavailable for conversation query")
	}
	if s != nil && s.DB != nil {
		if record, err := s.DB.FindSession(ctx, normalizedConversationID, normalizedPluginID); err == nil {
			_, adapterErr := s.runtimeAdapter(normalizedPluginID)
			if adapterErr != nil {
				return resolvedConversation{}, adapterErr
			}
			session := applySessionRecord(pluginruntime.NativeSession{
				PluginID: normalizedPluginID,
				Visible:  true,
			}, record)
			return resolvedConversation{
				ConversationID: normalizedConversationID,
				PluginID:       normalizedPluginID,
				Session:        session,
				Hint: pluginruntime.NativeSessionHint{
					PluginID:        normalizedPluginID,
					NativeSessionID: record.NativeSessionID,
					NativeThreadID:  record.NativeThreadID,
					Cwd:             record.Cwd,
					Title:           firstNonEmpty(record.NativeThreadID, record.NativeSessionID, normalizedConversationID),
					Visible:         true,
				},
				Record: &record,
			}, nil
		} else if err != sql.ErrNoRows {
			return resolvedConversation{}, err
		}
	}
	return s.resolveConversation(ctx, conversationID, pluginID)
}

func (s *QueryService) readConversationMessages(ctx context.Context, conversation resolvedConversation, limit int) ([]pluginruntime.HistoryMessage, error) {
	if limit <= 0 {
		limit = 200
	}
	adapter, err := s.runtimeAdapter(conversation.PluginID)
	if err != nil {
		return nil, err
	}
	reader, ok := adapter.(pluginruntime.HistoryReader)
	if !ok {
		return nil, fmt.Errorf("%s adapter does not implement readHistory", conversation.PluginID)
	}
	messages, err := reader.ReadHistory(ctx, conversation.Session, limit)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

// Runtime operations use an Adapter that was already registered by the Hub.
// Calling Probe here may invoke Codex CDP and turns every message read into a
// multi-second UI check. Probe remains part of catalog/detail refresh paths.
func (s *QueryService) runtimeAdapter(pluginID string) (pluginruntime.PluginAdapter, error) {
	if s == nil || s.Service == nil {
		return nil, fmt.Errorf("plugin runtime service unavailable")
	}
	normalizedPluginID := strings.ToLower(strings.TrimSpace(pluginID))
	if normalizedPluginID == "" {
		return nil, fmt.Errorf("runtime adapter required")
	}
	adapter := s.Service.Adapter(normalizedPluginID)
	if adapter == nil {
		return nil, fmt.Errorf("plugin adapter not found: %s", normalizedPluginID)
	}
	return adapter, nil
}

func (s *QueryService) conversationIndexEnvelope(conversation resolvedConversation, pluginID string) map[string]any {
	status := conversationStatusFromHint(conversation.Hint)
	// A native conversation ID is transport identity, never user-visible copy.
	// Keep title empty when the Plugin has not provided a real title; terminals
	// may render their own neutral placeholder without leaking the ID.
	title := canonicalConversationTitle(conversation.Hint.Title, conversation.ConversationID)
	summary := compactEnvelopeText(title, 220)
	return map[string]any{
		"conversation_id":      conversation.ConversationID,
		"plugin_id":            pluginID,
		"title":                compactEnvelopeText(title, 140),
		"cwd":                  firstNonEmpty(conversation.Session.Cwd, conversation.Hint.Cwd),
		"summary":              summary,
		"status":               status,
		"updated_at":           conversationSortTime(conversation).UnixMilli(),
		"pinned":               conversationPinned(conversation.Hint),
		"metadata":             compactEnvelopeMetadata(conversation.Hint.Metadata),
		"has_running_task":     status == StageRunning,
		"has_pending_approval": status == StageWaitingApproval,
	}
}

func (s *QueryService) detailSnapshotEnvelope(ctx context.Context, conversation resolvedConversation, pluginID string, live map[string]any) map[string]any {
	base := s.conversationIndexEnvelope(conversation, pluginID)
	messagesSignature := strings.TrimSpace(stringFromAny(live["messages_signature"]))
	if s != nil && s.DetailSnapshots != nil {
		if cached, ok := s.DetailSnapshots.GetForRuntime(conversation.PluginID, conversation.ConversationID); ok {
			for key, value := range cached {
				base[key] = value
			}
		}
	}
	base["conversation_id"] = conversation.ConversationID
	base["plugin_id"] = pluginID
	base["title"] = firstNonEmpty(
		stringFromAny(live["title"]),
		stringFromAny(base["title"]),
		conversation.Hint.Title,
	)
	base["cwd"] = firstNonEmpty(
		stringFromAny(live["cwd"]),
		stringFromAny(base["cwd"]),
		conversation.Session.Cwd,
		conversation.Hint.Cwd,
	)
	base["pinned"] = firstNonEmptyBool(
		live["pinned"],
		base["pinned"],
		conversationPinned(conversation.Hint),
	)
	base["updated_at"] = firstNonZeroInt64(
		int64Value(live["updated_at"]),
		int64Value(base["updated_at"]),
		conversationSortTime(conversation).UnixMilli(),
	)
	if messagesSignature == "" {
		messagesSignature = strings.TrimSpace(stringFromAny(base["messages_signature"]))
	}
	snapshot := canonicalDetailSnapshot(base, live, messagesSignature)
	snapshot = applyRuntimeProfile(snapshot, runtimeProfileMetadata(s.Service, conversation.PluginID))
	snapshot = s.decorateDetailSnapshot(snapshot, conversation, detailSnapshotHasForegroundState(live))
	if s != nil && s.DetailSnapshots != nil {
		s.DetailSnapshots.PutForRuntime(conversation.PluginID, snapshot)
	}
	return snapshot
}

func (s *QueryService) isConversationForeground(pluginID, conversationID string) bool {
	if s == nil || s.DetailSnapshots == nil {
		return false
	}
	return s.DetailSnapshots.IsForegroundForRuntime(pluginID, conversationID)
}

func (s *QueryService) decorateDetailSnapshot(snapshot map[string]any, conversation resolvedConversation, hasLiveForegroundState bool) map[string]any {
	if len(snapshot) == 0 || conversation.Capability.IntegrationMode != pluginruntime.IntegrationModeDesktopAutomation {
		return snapshot
	}
	if hasLiveForegroundState {
		return snapshot
	}
	snapshot["desktop_foreground"] = s.isConversationForeground(conversation.PluginID, conversation.ConversationID)
	snapshot["detail_stale"] = !firstNonEmptyBool(snapshot["desktop_foreground"])
	return snapshot
}

func compactEnvelopeText(value string, max int) string {
	text := strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if max <= 0 || len([]rune(text)) <= max {
		return text
	}
	runes := []rune(text)
	return string(runes[:max]) + "..."
}

func compactEnvelopeMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return metadata
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if nativeIdentityPayloadKey(key) {
			continue
		}
		limit := 180
		switch key {
		case "sandbox_policy":
			limit = 260
		case "context_window", "context_window_total", "context_tokens_used", "context_window_usage_percent":
			limit = 80
		}
		out[key] = compactEnvelopeText(value, limit)
	}
	return out
}

func (s *QueryService) requireAdapter(ctx context.Context, pluginID string) (pluginruntime.PluginAdapter, pluginruntime.Capability, error) {
	if s == nil || s.Service == nil {
		return nil, pluginruntime.Capability{}, fmt.Errorf("plugin runtime service unavailable")
	}
	normalizedPluginID := strings.ToLower(strings.TrimSpace(pluginID))
	if normalizedPluginID == "" {
		return nil, pluginruntime.Capability{}, fmt.Errorf("runtime adapter required")
	}
	adapter := s.Service.Adapter(normalizedPluginID)
	if adapter == nil {
		return nil, pluginruntime.Capability{}, fmt.Errorf("plugin adapter not found: %s", normalizedPluginID)
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return nil, pluginruntime.Capability{}, err
	}
	return adapter, capability, nil
}

func (s *QueryService) sessionRecordIndex(ctx context.Context, pluginID string) (map[string]*bridgedb.SessionRecord, map[string]*bridgedb.SessionRecord, error) {
	byNative := map[string]*bridgedb.SessionRecord{}
	byConversation := map[string]*bridgedb.SessionRecord{}
	if s == nil || s.DB == nil {
		return byNative, byConversation, nil
	}
	records, err := s.DB.ListSessions(ctx, pluginID)
	if err != nil {
		return nil, nil, err
	}
	for index := range records {
		record := records[index]
		copyRecord := record
		byConversation[record.PrismConversationID] = &copyRecord
		if key := nativeSessionLookupKey(record.NativeSessionID, record.NativeThreadID); key != "" {
			if _, exists := byNative[key]; !exists {
				// ListSessions is ordered by updated_at desc, so the first record for a
				// native thread is the newest binding. Older remote-* bindings must not
				// override the fresh native state returned by the plugin.
				byNative[key] = &copyRecord
			}
		}
	}
	return byNative, byConversation, nil
}

func (s *QueryService) ensureConversationHintBinding(ctx context.Context, pluginID string, hint pluginruntime.NativeSessionHint) (bridgedb.SessionRecord, error) {
	if s == nil || s.DB == nil {
		return bridgedb.SessionRecord{}, fmt.Errorf("conversation binding requires bridge db")
	}
	return s.DB.EnsureCanonicalSessionBinding(ctx, bridgedb.SessionRecord{
		PluginID:        pluginID,
		NativeSessionID: hint.NativeSessionID,
		NativeThreadID:  hint.NativeThreadID,
		Surface:         hint.Surface,
		Endpoint:        hint.Endpoint,
		Cwd:             hint.Cwd,
	})
}

func sessionFromHint(hint pluginruntime.NativeSessionHint) pluginruntime.NativeSession {
	return pluginruntime.NativeSession{
		PluginID:        hint.PluginID,
		NativeSessionID: hint.NativeSessionID,
		NativeThreadID:  hint.NativeThreadID,
		Surface:         hint.Surface,
		Endpoint:        hint.Endpoint,
		Cwd:             hint.Cwd,
		Visible:         hint.Visible,
	}
}

func applySessionRecord(session pluginruntime.NativeSession, record bridgedb.SessionRecord) pluginruntime.NativeSession {
	session.NativeSessionID = firstNonEmpty(session.NativeSessionID, record.NativeSessionID)
	session.NativeThreadID = firstNonEmpty(session.NativeThreadID, record.NativeThreadID)
	session.Surface = firstNonEmpty(session.Surface, record.Surface)
	session.Endpoint = firstNonEmpty(session.Endpoint, record.Endpoint)
	session.Cwd = firstNonEmpty(session.Cwd, record.Cwd)
	session.Visible = true
	return session
}

func nativeSessionLookupKey(nativeSessionID, nativeThreadID string) string {
	if trimmed := strings.TrimSpace(nativeThreadID); trimmed != "" {
		return "thread:" + trimmed
	}
	if trimmed := strings.TrimSpace(nativeSessionID); trimmed != "" {
		return "session:" + trimmed
	}
	return ""
}

func conversationSortTime(conversation resolvedConversation) time.Time {
	if !conversation.Hint.LastActivityAt.IsZero() {
		return conversation.Hint.LastActivityAt
	}
	if conversation.Record != nil && !conversation.Record.UpdatedAt.IsZero() {
		return conversation.Record.UpdatedAt
	}
	return time.UnixMilli(0).UTC()
}

func conversationPinned(hint pluginruntime.NativeSessionHint) bool {
	for _, key := range []string{"pinned", "is_pinned", "pin"} {
		value := strings.TrimSpace(strings.ToLower(hint.Metadata[key]))
		if value == "true" || value == "1" || value == "yes" {
			return true
		}
	}
	return false
}

func asMap(value any) map[string]any {
	if raw, ok := value.(map[string]any); ok {
		return raw
	}
	return map[string]any{}
}

func listOfMapsValue(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if mapped := asMap(item); len(mapped) > 0 {
				out = append(out, mapped)
			}
		}
		return out
	}
	return []map[string]any{}
}

func int64Value(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case time.Time:
		return v.UnixMilli()
	case string:
		if strings.TrimSpace(v) == "" {
			return 0
		}
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(v)); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonEmptyBool(values ...any) bool {
	for _, value := range values {
		switch v := value.(type) {
		case bool:
			return v
		case string:
			text := strings.TrimSpace(strings.ToLower(v))
			if text == "true" || text == "1" || text == "yes" {
				return true
			}
			if text == "false" || text == "0" || text == "no" {
				return false
			}
		}
	}
	return false
}
