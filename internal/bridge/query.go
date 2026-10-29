package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

// A Codex rollout can place image data directly in message text or a tool
// detail. Remote bodies are metadata-only for attachments; forwarding a data
// URI would both leak bytes and let clients allocate an unbounded image.
var inlineDataURI = regexp.MustCompile(`(?i)data:[a-z][a-z0-9.+-]*/[a-z0-9.+-]+(?:;[a-z0-9.+-]+)*,[a-z0-9+/=_-]+`)

func publicHistoryText(value string) string {
	return inlineDataURI.ReplaceAllString(value, "[附件内容已省略]")
}

type QueryService struct {
	DB              *bridgedb.DB
	History         *HistoryService
	Service         *pluginruntime.Service
	Config          *bridgeconfig.Config
	ConfigProvider  func() *bridgeconfig.Config
	DetailSnapshots *DetailSnapshotStore
	Logger          *slog.Logger
}

func (s *QueryService) Execute(ctx context.Context, payload *prismtransport.InboundPayload) (map[string]any, error) {
	if s == nil || (s.DB == nil && s.Service == nil) {
		return nil, fmt.Errorf("query service unavailable")
	}
	if payload == nil {
		return nil, fmt.Errorf("query payload required")
	}
	queryType := strings.TrimSpace(payload.QueryType)
	conversationID := strings.TrimSpace(payload.ConversationID)
	pluginID := strings.TrimSpace(payload.PluginID)
	if queryType == "" {
		return nil, fmt.Errorf("query_type required")
	}
	pluginID, err := RequirePlugin(ctx, s.Service, pluginID)
	if err != nil {
		return nil, err
	}
	if !pluginruntime.RemoteEnabled(s.CurrentConfig(), pluginID) {
		return nil, fmt.Errorf("%s remote access disabled", pluginID)
	}
	switch queryType {
	case "conversations.list":
		return s.listConversations(ctx, pluginID, payload.Limit)
	case "conversation.select":
		if conversationID == "" {
			return nil, fmt.Errorf("conversation_id required")
		}
		return s.selectConversation(ctx, payload, conversationID, pluginID)
	case "conversation.detail.get":
		if conversationID == "" {
			return nil, fmt.Errorf("conversation_id required")
		}
		return s.getConversationDetail(ctx, conversationID, pluginID)
	case "conversation.messages":
		if conversationID == "" {
			return nil, fmt.Errorf("conversation_id required")
		}
		return s.listConversationMessages(ctx, conversationID, pluginID, payload.Limit)
	default:
		return nil, fmt.Errorf("unsupported query_type: %s", queryType)
	}
}

// selectConversation is intentionally an explicit query action. Detail and
// history reads must remain passive so reconnects and background refreshes do
// not steal focus from a user working in another Desktop conversation.
func (s *QueryService) selectConversation(ctx context.Context, payload *prismtransport.InboundPayload, conversationID, pluginID string) (map[string]any, error) {
	conversation, err := s.resolveConversationForRuntime(ctx, conversationID, pluginID)
	if err != nil {
		return nil, err
	}
	adapter, err := s.runtimeAdapter(conversation.PluginID)
	if err != nil {
		return nil, err
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return nil, fmt.Errorf("probe native session: %w", err)
	}
	controller, ok := adapter.(pluginruntime.SessionController)
	if !ok || !capability.CanControlSession {
		// Protocol-native plugins may have no Desktop foreground surface. The
		// selection is still valid browsing context, with no local side effect.
		return map[string]any{
			"conversation_id":  conversation.ConversationID,
			"plugin_id":        conversation.PluginID,
			"selected":         true,
			"desktop_selected": false,
		}, nil
	}
	metadata := map[string]string{}
	if payload != nil {
		metadata["request_id"] = strings.TrimSpace(payload.RequestID)
		if payload.Metadata != nil {
			metadata["source_device"] = strings.TrimSpace(payload.Metadata.SourceDevice)
		}
	}
	control, err := controller.ControlSession(ctx, pluginruntime.SessionControlRequest{
		Session:  conversation.Session,
		Action:   "conversation.select",
		Metadata: metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("select native session: %w", err)
	}
	if !control.OK {
		return nil, fmt.Errorf("native session selection was not accepted")
	}
	return map[string]any{
		"conversation_id":  conversation.ConversationID,
		"plugin_id":        conversation.PluginID,
		"selected":         true,
		"desktop_selected": true,
		"details":          control.Details,
	}, nil
}

func (s *QueryService) CurrentConfig() *bridgeconfig.Config {
	if s == nil {
		return nil
	}
	if s.ConfigProvider != nil {
		if cfg := s.ConfigProvider(); cfg != nil {
			return cfg
		}
	}
	return s.Config
}

func (s *QueryService) listConversationMessages(ctx context.Context, conversationID, pluginID string, limit int) (map[string]any, error) {
	startedAt := time.Now()
	resolved, err := s.resolveConversationForRuntime(ctx, conversationID, pluginID)
	if err != nil {
		return nil, err
	}
	resolvedAt := time.Now()
	messages, err := s.readConversationMessages(ctx, resolved, limit)
	if err != nil {
		return nil, err
	}
	if s.Logger != nil {
		s.Logger.Info("conversation messages read completed",
			"conversation_id", resolved.ConversationID,
			"plugin_id", pluginID,
			"resolve_ms", resolvedAt.Sub(startedAt).Milliseconds(),
			"read_history_ms", time.Since(resolvedAt).Milliseconds(),
			"total_ms", time.Since(startedAt).Milliseconds(),
			"items", len(messages))
	}
	items := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		items = append(items, map[string]any{
			"id":         message.ID,
			"role":       message.Role,
			"type":       firstNonEmpty(strings.TrimSpace(message.Type), "text"),
			"content":    publicHistoryText(message.Content),
			"status":     message.Status,
			"created_at": message.CreatedAt.UnixMilli(),
			"updated_at": message.UpdatedAt.UnixMilli(),
			"metadata":   publicHistoryMetadata(message.Metadata),
		})
	}
	truncated := historyMessagesTruncated(messages)
	return map[string]any{
		"conversation_id": resolved.ConversationID,
		"plugin_id":       pluginID,
		"has_more":        truncated,
		"truncated":       truncated,
		"items":           items,
	}, nil
}

func historyMessagesTruncated(messages []pluginruntime.HistoryMessage) bool {
	for _, message := range messages {
		for _, key := range []string{"truncated", "has_more"} {
			if value, ok := message.Metadata[key]; ok && truthyMetadataValue(value) {
				return true
			}
		}
	}
	return false
}

func truthyMetadataValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		text := strings.TrimSpace(strings.ToLower(v))
		return text == "true" || text == "1" || text == "yes"
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}

func publicHistoryMetadata(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, key := range []string{"truncated", "has_more", "omitted_count", "synthetic"} {
		if value, ok := input[key]; ok {
			out[key] = value
		}
	}
	attachments := publicAttachments(input["attachments"])
	if len(attachments) > 0 {
		out["attachments"] = attachments
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// PublicHistoryMessages is the only conversion from Plugin-native history
// structs to the remote body wire. Keep names snake_case and discard all
// attachment transport except display metadata before a frame reaches Mobile.
func PublicHistoryMessages(messages []pluginruntime.HistoryMessage) []map[string]any {
	items := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		item := map[string]any{
			"id":         message.ID,
			"role":       message.Role,
			"type":       firstNonEmpty(strings.TrimSpace(message.Type), "text"),
			"content":    publicHistoryText(message.Content),
			"status":     message.Status,
			"created_at": message.CreatedAt.UnixMilli(),
			"updated_at": message.UpdatedAt.UnixMilli(),
			"metadata":   publicHistoryMetadata(message.Metadata),
		}
		if progress := publicHistoryProgress(message.Progress); progress != nil {
			item["progress"] = progress
		}
		items = append(items, item)
	}
	return items
}

func publicHistoryProgress(progress *pluginruntime.HistoryProgress) map[string]any {
	if progress == nil {
		return nil
	}
	result := map[string]any{
		"status": strings.TrimSpace(progress.Status),
		"steps":  make([]map[string]any, 0, len(progress.Steps)),
	}
	if !progress.StartedAt.IsZero() {
		result["started_at"] = progress.StartedAt.UnixMilli()
	}
	if !progress.CompletedAt.IsZero() {
		result["completed_at"] = progress.CompletedAt.UnixMilli()
	}
	steps := result["steps"].([]map[string]any)
	for _, step := range progress.Steps {
		item := map[string]any{
			"id":     step.ID,
			"kind":   step.Kind,
			"title":  step.Title,
			"detail": publicHistoryText(step.Detail),
			"status": step.Status,
		}
		if callID := strings.TrimSpace(step.CallID); callID != "" {
			item["call_id"] = callID
		}
		if !step.CreatedAt.IsZero() {
			item["created_at"] = step.CreatedAt.UnixMilli()
		}
		steps = append(steps, item)
	}
	result["steps"] = steps
	return result
}

func publicAttachments(value any) []map[string]string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := strings.TrimSpace(stringFromAny(entry["name"]))
		if name == "" {
			continue
		}
		attachment := map[string]string{"name": name}
		if mimeType := strings.TrimSpace(stringFromAny(entry["mime_type"])); mimeType != "" {
			attachment["mime_type"] = mimeType
		}
		if kind := strings.TrimSpace(stringFromAny(entry["kind"])); kind == "image" || kind == "file" {
			attachment["kind"] = kind
		}
		out = append(out, attachment)
	}
	return out
}

func runtimeStatusToRunState(conversationID string, status pluginruntime.RuntimeStatus) map[string]any {
	summary := strings.TrimSpace(status.Final)
	if summary == "" {
		summary = strings.TrimSpace(status.Preview)
	}
	if summary == "" {
		summary = strings.TrimSpace(status.ProcessText)
	}
	approvalRequestID := strings.TrimSpace(status.ApprovalRequestID)
	if approvalRequestID == "" {
		approvalRequestID = approvalRequestIDFromDesktopState(status.DesktopState)
	}
	return map[string]any{
		"conversation_id":     conversationID,
		"task_id":             strings.TrimSpace(status.TurnID),
		"status":              normalizeRuntimeStatus(status),
		"summary":             summary,
		"started_at":          parseRuntimeMillis(status.StartedAt),
		"updated_at":          parseRuntimeMillis(firstNonEmpty(strings.TrimSpace(status.UpdatedAt), strings.TrimSpace(status.CompletedAt))),
		"completed_at":        parseRuntimeMillis(status.CompletedAt),
		"error":               strings.TrimSpace(status.Error),
		"active":              status.Active,
		"interruptible":       status.Interruptible,
		"approval_blocked":    status.ApprovalBlocked,
		"approval_request_id": approvalRequestID,
		"primary_action":      strings.TrimSpace(status.PrimaryAction),
		"can_queue":           status.CanQueue,
		"desktop_state":       status.DesktopState,
		"phase":               status.Phase,
		"preview":             strings.TrimSpace(status.Preview),
		"final":               strings.TrimSpace(status.Final),
		"duration_ms":         status.DurationMs,
		"context":             status.Context,
		"model":               status.Model,
		"reasoning_mode":      status.ReasoningMode,
		"steps":               status.Steps,
		"session_file":        strings.TrimSpace(status.SessionFile),
	}
}

func approvalRequestIDFromDesktopState(desktopState map[string]any) string {
	if len(desktopState) == 0 {
		return ""
	}
	approval, _ := desktopState["approval"].(map[string]any)
	if len(approval) == 0 {
		return ""
	}
	return firstNonEmpty(
		strings.TrimSpace(stringFromAny(approval["approval_request_id"])),
		strings.TrimSpace(stringFromAny(approval["id"])),
	)
}

func normalizeRuntimeStatus(status pluginruntime.RuntimeStatus) string {
	if status.ApprovalBlocked {
		return StageWaitingApproval
	}
	switch strings.ToLower(strings.TrimSpace(status.Status)) {
	case "complete", StageCompleted:
		return StageCompleted
	case "error", StageFailed:
		return StageFailed
	case StageWaitingApproval:
		return StageWaitingApproval
	case "waiting":
		return StageRunning
	case StageRunning:
		return StageRunning
	default:
		if status.Active {
			return StageRunning
		}
		return firstNonEmpty(strings.ToLower(strings.TrimSpace(status.Status)), "idle")
	}
}

func parseRuntimeMillis(value string) int64 {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return parsed.UnixMilli()
}

func deriveRunState(events []bridgedb.EventRecord, conversationID, _ string) map[string]any {
	status := "idle"
	summary := ""
	taskID := ""
	errorText := ""
	var startedAt int64
	var updatedAt int64

	for _, event := range events {
		if !runStateEventRelevant(event) {
			continue
		}
		eventType := strings.ToLower(strings.TrimSpace(event.EventType))
		lifecycleEvent := eventType == "desktop.state.changed"
		if startedAt == 0 {
			startedAt = event.CreatedAt.UnixMilli()
		}
		updatedAt = event.CreatedAt.UnixMilli()
		if !lifecycleEvent {
			if id := strings.TrimSpace(event.NativeEventID); id != "" {
				taskID = id
			}
			if trimmed := strings.TrimSpace(event.Summary); trimmed != "" {
				summary = trimmed
			}
		}
		switch normalizeRunEventStatus(event) {
		case "idle":
			status = "idle"
		case StageWaitingApproval:
			status = StageWaitingApproval
		case StageFailed:
			status = StageFailed
			if trimmed := strings.TrimSpace(event.Summary); trimmed != "" {
				errorText = trimmed
			}
		case StageCompleted:
			status = StageCompleted
		case StageInterrupted:
			status = StageInterrupted
		case StageRunning:
			if status != StageWaitingApproval && status != StageFailed && status != StageCompleted && status != StageInterrupted {
				status = StageRunning
			}
		}
	}

	return map[string]any{
		"conversation_id": conversationID,
		"task_id":         taskID,
		"status":          status,
		"summary":         summary,
		"started_at":      startedAt,
		"updated_at":      updatedAt,
		"error":           errorText,
	}
}

func runStateEventRelevant(event bridgedb.EventRecord) bool {
	eventType := strings.ToLower(strings.TrimSpace(event.EventType))
	switch {
	case eventType == "desktop.state.changed":
		status := strings.ToLower(strings.TrimSpace(event.Status))
		return status == "idle" || status == StageRunning
	case eventType == "":
		return false
	case eventType == "approval.required":
		return true
	case eventType == "approval.resolve":
		return false
	case eventType == "session.control":
		return false
	case strings.HasPrefix(eventType, "message."):
		return true
	case strings.HasPrefix(eventType, "run."):
		return true
	case strings.Contains(eventType, "interrupt"):
		return true
	case strings.HasSuffix(eventType, ".failed"), strings.Contains(eventType, "failed"):
		return true
	case strings.HasSuffix(eventType, ".completed"), strings.Contains(eventType, "completed"):
		return true
	default:
		return false
	}
}

func normalizeRunEventStatus(event bridgedb.EventRecord) string {
	status := strings.ToLower(strings.TrimSpace(event.Status))
	switch status {
	case "idle":
		return "idle"
	case StageReceived, StageResolving, StageNativeSessionReady, StageAccepted, StageVisible, StageRunning:
		return StageRunning
	case StageWaitingApproval, StageCompleted, StageFailed, StageInterrupted:
		return status
	}
	eventType := strings.ToLower(strings.TrimSpace(event.EventType))
	switch {
	case eventType == "approval.required":
		return StageWaitingApproval
	case eventType == "task.interrupt" || strings.Contains(eventType, "interrupt"):
		return StageInterrupted
	case strings.HasSuffix(eventType, ".failed") || strings.Contains(eventType, "failed"):
		return StageFailed
	case strings.HasSuffix(eventType, ".completed") || strings.Contains(eventType, "completed"):
		return StageCompleted
	case strings.HasPrefix(eventType, "message.") || strings.HasPrefix(eventType, "run."):
		return StageRunning
	default:
		return status
	}
}
