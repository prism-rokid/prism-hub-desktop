package bridge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

type conversationEventContext struct {
	RequestID       string
	ConversationID  string
	PluginID        string
	SourceDevice    string
	RunID           string
	ConversationCwd string
	Bound           bool
	SuppressMessage bool
	MessageRole     string
	MessageText     string
	MessageDelta    string
	Session         pluginruntime.NativeSession
}

func buildConversationEventPayload(input conversationEventContext, event pluginruntime.PluginEvent) prismtransport.ConversationEventPayload {
	session := input.Session
	if strings.TrimSpace(session.PluginID) == "" {
		session.PluginID = strings.TrimSpace(input.PluginID)
	}
	return prismtransport.ConversationEventPayload{
		ConversationID: strings.TrimSpace(input.ConversationID),
		PluginID:       firstNonEmpty(session.PluginID, input.PluginID),
		EventID:        publicConversationEventID(input, event),
		EventType:      strings.TrimSpace(event.Type),
		Status:         normalizedConversationEventStatus(event),
		Summary:        strings.TrimSpace(event.Summary),
		SourceDevice:   firstNonEmpty(input.SourceDevice, "plugin-desktop"),
		Payload:        buildConversationEventEnvelope(input, event),
	}
}

func buildConversationEventEnvelope(input conversationEventContext, event pluginruntime.PluginEvent) map[string]any {
	session := input.Session
	if strings.TrimSpace(session.PluginID) == "" {
		session.PluginID = strings.TrimSpace(input.PluginID)
	}
	runID := publicConversationRunID(input, event)
	messageRole, messageText, messageDelta := resolveConversationMessageParts(input, event)
	conversationID := strings.TrimSpace(input.ConversationID)
	sessionTitle := firstNonEmpty(conversationID, "Prism conversation")
	payload := map[string]any{
		"schema_version": "conversation.v1",
		"bound":          input.Bound,
		"request_id":     strings.TrimSpace(input.RequestID),
		"session": map[string]any{
			"id":         conversationID,
			"title":      sessionTitle,
			"cwd":        firstNonEmpty(session.Cwd, input.ConversationCwd),
			"source":     "desktop",
			"plugin_id":  firstNonEmpty(session.PluginID, input.PluginID),
			"surface":    session.Surface,
			"endpoint":   session.Endpoint,
			"updated_at": eventTimeOrNow(event).Format(time.RFC3339Nano),
		},
		"run": map[string]any{
			"id":     runID,
			"status": normalizedConversationEventStatus(event),
		},
	}
	if messageRole != "" || messageText != "" || messageDelta != "" {
		messageID := assistantMessageID(strings.TrimSpace(input.RequestID), runID, event.ID)
		if strings.EqualFold(messageRole, "user") {
			messageID = userMessageID(strings.TrimSpace(input.RequestID))
		}
		message := map[string]any{
			"id":    messageID,
			"role":  firstNonEmpty(messageRole, "assistant"),
			"index": 0,
			"text":  messageText,
			"delta": messageDelta,
		}
		payload["message"] = message
	}
	if approval, ok := approvalPayloadFromEvent(input, event); ok {
		payload["approval"] = approval
	}
	if resolution, ok := approvalResolutionPayloadFromEvent(event); ok {
		payload["approval_resolution"] = resolution
	}
	if strings.EqualFold(strings.TrimSpace(event.Type), "conversation.control_failed") {
		payload["control_failure"] = map[string]any{
			"request_id": firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["request_id"])), strings.TrimSpace(input.RequestID)),
			"action":     strings.TrimSpace(stringFromAny(event.Payload["action"])),
			"code":       firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["code"])), "control_failed"),
			"message":    firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["message"])), strings.TrimSpace(event.Summary)),
		}
	}
	if strings.EqualFold(strings.TrimSpace(event.Type), "conversation.send_failed") {
		payload["send_failure"] = map[string]any{
			"request_id": firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["request_id"])), strings.TrimSpace(input.RequestID)),
			"message_id": firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["message_id"])), strings.TrimSpace(input.RequestID)),
			"code":       firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["code"])), "send_failed"),
			"message":    firstNonEmpty(strings.TrimSpace(stringFromAny(event.Payload["message"])), strings.TrimSpace(event.Summary)),
		}
	}
	payload["event_dedupe_id"] = stableConversationEventDedupeID(input, event)
	return payload
}

// stableConversationEventDedupeID identifies the semantic event for push
// deduplication downstream. Unlike publicConversationEventID it excludes
// timestamps and transport ids on purpose: a terminal event re-sent after a
// Hub or Plugin restart must hash to the same key so Realtime collapses the
// replay instead of pushing it twice, while a genuinely new run's plugin
// event ID hashes to a new key.
func stableConversationEventDedupeID(input conversationEventContext, event pluginruntime.PluginEvent) string {
	parts := []string{
		strings.TrimSpace(input.ConversationID),
		strings.TrimSpace(event.Type),
		strings.TrimSpace(event.Status),
		strings.TrimSpace(event.ID),
	}
	// Plugin event IDs identify a semantic event. Different producers can
	// describe the same terminal event with slightly different summaries, so
	// summary text must not split its push idempotency identity.
	if strings.TrimSpace(event.ID) == "" {
		parts = append(parts, strings.TrimSpace(event.Summary))
	}
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "evt_" + hex.EncodeToString(hash[:12])
}

// publicConversationEventID deliberately excludes plugin-native IDs. Native
// IDs remain available to the Hub for routing but never become a Realtime key.
func publicConversationEventID(input conversationEventContext, event pluginruntime.PluginEvent) string {
	return publicConversationOpaqueID("event", input, event)
}

func publicConversationRunID(input conversationEventContext, event pluginruntime.PluginEvent) string {
	return publicConversationOpaqueID("run", input, event)
}

func publicConversationOpaqueID(prefix string, input conversationEventContext, event pluginruntime.PluginEvent) string {
	parts := []string{
		strings.TrimSpace(input.ConversationID),
		strings.TrimSpace(input.RequestID),
		strings.TrimSpace(event.Type),
		strings.TrimSpace(event.Status),
		strings.TrimSpace(event.Summary),
		eventTimeOrNow(event).UTC().Format(time.RFC3339Nano),
		strings.TrimSpace(input.MessageRole),
		strings.TrimSpace(input.MessageText),
		strings.TrimSpace(input.MessageDelta),
	}
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + "_" + hex.EncodeToString(hash[:12])
}

func normalizedConversationEventStatus(event pluginruntime.PluginEvent) string {
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if status != "" {
		return status
	}
	return StageRunning
}

func resolveConversationMessageParts(input conversationEventContext, event pluginruntime.PluginEvent) (string, string, string) {
	if input.SuppressMessage {
		return "", "", ""
	}
	role := strings.TrimSpace(input.MessageRole)
	text := strings.TrimSpace(input.MessageText)
	delta := strings.TrimSpace(input.MessageDelta)
	if role == "" {
		role = messageRoleFromEvent(event)
	}
	if text == "" && delta == "" {
		text = assistantTextFromEvent(event)
		if strings.EqualFold(strings.TrimSpace(event.Type), "message.delta") {
			delta = text
			text = ""
		}
	}
	return role, text, delta
}

func messageRoleFromEvent(event pluginruntime.PluginEvent) string {
	eventType := strings.ToLower(strings.TrimSpace(event.Type))
	switch {
	case strings.HasPrefix(eventType, "message.user"):
		return "user"
	case strings.HasPrefix(eventType, "message.system"):
		return "system"
	case strings.HasPrefix(eventType, "message.assistant"), strings.HasPrefix(eventType, "message.delta"), strings.HasPrefix(eventType, "message.completed"), strings.HasPrefix(eventType, "message.started"):
		return "assistant"
	default:
		if event.Payload != nil {
			if role := strings.ToLower(strings.TrimSpace(stringFromAny(event.Payload["role"]))); role != "" {
				return role
			}
			if message, ok := event.Payload["message"].(map[string]any); ok {
				if role := strings.ToLower(strings.TrimSpace(stringFromAny(message["role"]))); role != "" {
					return role
				}
			}
		}
	}
	return ""
}

func approvalPayloadFromEvent(input conversationEventContext, event pluginruntime.PluginEvent) (map[string]any, bool) {
	eventType := strings.ToLower(strings.TrimSpace(event.Type))
	method := ""
	if event.Payload != nil {
		method = strings.TrimSpace(stringFromAny(event.Payload["method"]))
	}
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if eventType != "approval.required" && status != StageWaitingApproval {
		if method == "" {
			return nil, false
		}
		switch method {
		case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		default:
			return nil, false
		}
	}
	approvalID := firstNonEmpty(
		stringFromAny(event.Payload["approval_request_id"]),
		stringFromAny(event.Payload["itemId"]),
		stringFromAny(event.Payload["turn_id"]),
		event.ID,
	)
	payload := map[string]any{
		"id":      approvalID,
		"method":  method,
		"status":  normalizedConversationEventStatus(event),
		"summary": strings.TrimSpace(event.Summary),
	}
	for _, key := range []string{"approval_request_id", "approval_kind", "title", "description", "command", "allow_permanent", "source_device", "actions", "input", "created_at"} {
		if value, ok := event.Payload[key]; ok && value != nil {
			payload[key] = value
		}
	}
	if payload["approval_request_id"] == nil {
		payload["approval_request_id"] = approvalID
	}
	return payload, true
}

func approvalResolutionPayloadFromEvent(event pluginruntime.PluginEvent) (map[string]any, bool) {
	if !strings.EqualFold(strings.TrimSpace(event.Type), "approval.resolve") || len(event.Payload) == 0 {
		return nil, false
	}
	payload := map[string]any{}
	for _, key := range []string{
		"accepted",
		"request_id",
		"approval_request_id",
		"action_id",
		"action_label",
		"input",
		"message",
		"source_device",
		"updated_at",
	} {
		if value, ok := event.Payload[key]; ok && value != nil {
			payload[key] = value
		}
	}
	if len(payload) == 0 {
		return nil, false
	}
	return payload, true
}

func cloneConversationPayloadMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func sanitizePublicPayloadValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			if nativeIdentityPayloadKey(key) {
				continue
			}
			out[key] = sanitizePublicPayloadValue(child)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, child := range typed {
			out = append(out, sanitizePublicPayloadValue(child))
		}
		return out
	default:
		return value
	}
}

func sanitizePublicPayloadMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	value, ok := sanitizePublicPayloadValue(input).(map[string]any)
	if !ok {
		return nil
	}
	return value
}

func nativeIdentityPayloadKey(key string) bool {
	normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.TrimSpace(key)))
	if strings.Contains(normalized, "native") {
		return true
	}
	switch normalized {
	case "sessionid", "threadid", "sessionkey", "threadkey":
		return true
	default:
		return false
	}
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
