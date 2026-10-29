package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	transport "rokid-prism-hub/internal/realtime"
)

// BuildConversationIndex builds the lightweight project/conversation summary
// used by mobile/glasses list pages. It must not include message body content.
func BuildConversationIndex(ctx context.Context, svc *pluginruntime.Service, db *bridgedb.DB, cfg *bridgeconfig.Config, pluginIDs ...string) []transport.ConversationIndexPlugin {
	if svc == nil || db == nil {
		return nil
	}
	ids := pluginIDs
	if len(ids) == 0 {
		ids = svc.AdapterIDs()
	}
	out := make([]transport.ConversationIndexPlugin, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		enabled := pluginruntime.RemoteEnabled(cfg, id)
		if !enabled {
			continue
		}
		adapter := svc.Adapter(id)
		if adapter == nil {
			continue
		}
		st := svc.AdapterStatus(ctx, id, adapter)
		// A conversation index is a complete snapshot. A desktop-automation
		// plugin may lose its live-control surface while its durable local
		// session store remains readable (Codex is the concrete case). Keep
		// rebuilding that read-only index after a Hub reinstall, but never turn
		// a failed listSessions call into an empty snapshot.
		if !st.Available && !st.Capability.CanListSessions {
			continue
		}
		lister, ok := adapter.(pluginruntime.SessionLister)
		if !ok {
			continue
		}
		hints, err := lister.ListSessions(ctx)
		if err != nil {
			continue
		}
		meta := metadataFromAdapter(adapter)
		pluginID := declaredPluginIDFromCapability(adapter, st.Capability)
		if pluginID == "" {
			continue
		}
		item := transport.ConversationIndexPlugin{
			PluginID:    pluginID,
			DisplayName: firstNonEmpty(meta.DisplayName, displayFromID(pluginID)),
			Available:   enabled && st.Available,
			Enabled:     enabled,
		}
		item.Conversations = conversationIndexItems(ctx, db, pluginID, hints)
		item.UpdatedAt = newestConversationUpdatedAt(item.Conversations)
		item.Signature = conversationIndexAgentSignature(item)
		out = append(out, item)
	}
	return out
}

func ConversationIndexSnapshotSignature(items []transport.ConversationIndexPlugin) string {
	if len(items) == 0 {
		return ""
	}
	hasher := sha256.New()
	for _, item := range items {
		writeConversationIndexString(hasher, item.PluginID)
		writeConversationIndexString(hasher, item.Signature)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func conversationIndexAgentSignature(item transport.ConversationIndexPlugin) string {
	hasher := sha256.New()
	writeConversationIndexString(hasher, item.PluginID)
	writeConversationIndexString(hasher, item.DisplayName)
	writeConversationIndexBool(hasher, item.Available)
	writeConversationIndexBool(hasher, item.Enabled)
	writeConversationIndexString(hasher, item.UpdatedAt)
	for _, conversation := range item.Conversations {
		writeConversationIndexItem(hasher, conversation)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func writeConversationIndexItem(hasher interface{ Write([]byte) (int, error) }, item transport.ConversationIndexItem) {
	writeConversationIndexString(hasher, item.ID)
	writeConversationIndexString(hasher, item.Title)
	writeConversationIndexString(hasher, item.Cwd)
	writeConversationIndexString(hasher, item.Status)
	writeConversationIndexString(hasher, item.UpdatedAt)
	writeConversationIndexString(hasher, item.SortAt)
	writeConversationIndexBool(hasher, item.Pinned)
	keys := make([]string, 0, len(item.Metadata))
	for key := range item.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeConversationIndexString(hasher, key)
		writeConversationIndexString(hasher, item.Metadata[key])
	}
}

func writeConversationIndexString(hasher interface{ Write([]byte) (int, error) }, value string) {
	hasher.Write([]byte(strconv.Itoa(len(value))))
	hasher.Write([]byte{':'})
	hasher.Write([]byte(value))
	hasher.Write([]byte{'|'})
}

func writeConversationIndexBool(hasher interface{ Write([]byte) (int, error) }, value bool) {
	if value {
		hasher.Write([]byte("1|"))
		return
	}
	hasher.Write([]byte("0|"))
}

func conversationIndexItems(ctx context.Context, db *bridgedb.DB, pluginID string, hints []pluginruntime.NativeSessionHint) []transport.ConversationIndexItem {
	items := make([]transport.ConversationIndexItem, 0, len(hints))
	seenConversationIDs := make(map[string]struct{}, len(hints))
	for _, hint := range hints {
		if nativeSessionLookupKey(hint.NativeSessionID, hint.NativeThreadID) == "" {
			continue
		}
		record, err := db.EnsureCanonicalSessionBinding(ctx, bridgedb.SessionRecord{
			PluginID:        pluginID,
			NativeSessionID: hint.NativeSessionID,
			NativeThreadID:  hint.NativeThreadID,
			Surface:         hint.Surface,
			Endpoint:        hint.Endpoint,
			Cwd:             hint.Cwd,
		})
		if err != nil {
			continue
		}
		item, ok := conversationIndexItemFromHint(hint, record)
		if !ok {
			continue
		}
		if _, seen := seenConversationIDs[item.ID]; seen {
			continue
		}
		seenConversationIDs[item.ID] = struct{}{}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Pinned != items[j].Pinned {
			return items[i].Pinned
		}
		left := conversationIndexSortTime(items[i])
		right := conversationIndexSortTime(items[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func conversationIndexSortTime(item transport.ConversationIndexItem) time.Time {
	for _, value := range []string{item.SortAt, item.UpdatedAt} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
			return time.UnixMilli(millis).UTC()
		}
		if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.UnixMilli(0).UTC()
}

func conversationIndexItemFromHint(hint pluginruntime.NativeSessionHint, record bridgedb.SessionRecord) (transport.ConversationIndexItem, bool) {
	if conversationArchivedFromHint(hint) {
		return transport.ConversationIndexItem{}, false
	}
	conversationID := strings.TrimSpace(record.PrismConversationID)
	if conversationID == "" {
		return transport.ConversationIndexItem{}, false
	}
	updatedAt := ""
	if !hint.LastActivityAt.IsZero() {
		updatedAt = hint.LastActivityAt.UTC().Format(time.RFC3339Nano)
	}
	return transport.ConversationIndexItem{
		ID:        conversationID,
		Title:     strings.TrimSpace(hint.Title),
		Cwd:       strings.TrimSpace(hint.Cwd),
		Status:    conversationStatusFromHint(hint),
		UpdatedAt: updatedAt,
		SortAt:    firstNonEmpty(strings.TrimSpace(hint.Metadata["sort_at"]), updatedAt),
		Pinned:    conversationPinnedFromHint(hint),
		Metadata:  indexMetadataSummary(hint.Metadata),
	}, true
}

func conversationArchivedFromHint(hint pluginruntime.NativeSessionHint) bool {
	for _, key := range []string{"archived", "is_archived"} {
		if value, ok := hint.Metadata[key]; ok {
			text := strings.TrimSpace(strings.ToLower(value))
			if text == "true" || text == "1" || text == "yes" {
				return true
			}
		}
	}
	return false
}

func conversationStatusFromHint(hint pluginruntime.NativeSessionHint) string {
	return normalizeConversationIndexStatus(
		firstNonEmpty(
			strings.TrimSpace(hint.Metadata["status"]),
			strings.TrimSpace(hint.Metadata["task_status"]),
		),
	)
}

func normalizeConversationIndexStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "waiting_approval":
		return "waiting_approval"
	case "running", "waiting":
		return "running"
	default:
		return "idle"
	}
}

func conversationPinnedFromHint(hint pluginruntime.NativeSessionHint) bool {
	for _, key := range []string{"pinned", "is_pinned", "pin"} {
		value := strings.TrimSpace(strings.ToLower(hint.Metadata[key]))
		if value == "true" || value == "1" || value == "yes" {
			return true
		}
	}
	return false
}

func newestConversationUpdatedAt(items []transport.ConversationIndexItem) string {
	newest := ""
	for _, item := range items {
		value := firstNonEmpty(item.SortAt, item.UpdatedAt)
		if value > newest {
			newest = value
		}
	}
	return newest
}

func indexMetadataSummary(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	allow := map[string]struct{}{
		"model":                        {},
		"provider":                     {},
		"reasoning_effort":             {},
		"approval_mode":                {},
		"approval_request_id":          {},
		"primary_action":               {},
		"can_queue":                    {},
		"sandbox_policy":               {},
		"context_window_total":         {},
		"context_window_usage_percent": {},
		"context_tokens_used":          {},
		"thread_source":                {},
		"session_kind":                 {},
		"category":                     {},
		"chat_type":                    {},
		"origin_provider":              {},
		"origin_surface":               {},
		"origin_chat_type":             {},
		"pinned":                       {},
		"is_pinned":                    {},
		"sort_at_ms":                   {},
		"recency_at_ms":                {},
	}
	out := map[string]string{}
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		if _, ok := allow[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
