package bridge

import (
	"context"
	"strings"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
)

type ConversationMessage struct {
	ID        string         `json:"id"`
	Role      string         `json:"role"`
	Type      string         `json:"type"`
	Content   string         `json:"content"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type HistoryService struct {
	DB *bridgedb.DB
}

func (s *HistoryService) ListConversationMessages(ctx context.Context, prismConversationID, pluginID string, limit int) ([]ConversationMessage, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	events, err := s.DB.ListConversationEvents(ctx, prismConversationID, pluginID, limit)
	if err != nil {
		return nil, err
	}
	type indexedMessage struct {
		order int
		item  ConversationMessage
	}
	ordered := make([]indexedMessage, 0, len(events))
	byID := map[string]int{}
	for _, event := range events {
		message, ok := messageFromEventRecord(event)
		if !ok {
			continue
		}
		if idx, exists := byID[message.ID]; exists {
			existing := &ordered[idx].item
			if existing.Role == "" {
				existing.Role = message.Role
			}
			if existing.Type == "" {
				existing.Type = message.Type
			}
			if len(strings.TrimSpace(message.Content)) >= len(strings.TrimSpace(existing.Content)) {
				existing.Content = message.Content
			}
			if strings.TrimSpace(message.Status) != "" {
				existing.Status = message.Status
			}
			if !message.UpdatedAt.IsZero() {
				existing.UpdatedAt = message.UpdatedAt
			}
			if existing.Metadata == nil {
				existing.Metadata = map[string]any{}
			}
			for key, value := range message.Metadata {
				existing.Metadata[key] = value
			}
			continue
		}
		byID[message.ID] = len(ordered)
		ordered = append(ordered, indexedMessage{
			order: len(ordered),
			item:  message,
		})
	}
	out := make([]ConversationMessage, 0, len(ordered))
	for _, entry := range ordered {
		out = append(out, entry.item)
	}
	return out, nil
}

func messageFromEventRecord(event bridgedb.EventRecord) (ConversationMessage, bool) {
	payload := cloneMap(event.Payload)
	message := readMap(payload, "message")
	if len(message) == 0 {
		return ConversationMessage{}, false
	}
	messageID := strings.TrimSpace(stringFromAny(message["id"]))
	if messageID == "" {
		return ConversationMessage{}, false
	}
	content := strings.TrimSpace(stringFromAny(message["text"]))
	if content == "" {
		content = strings.TrimSpace(stringFromAny(message["delta"]))
	}
	if content == "" {
		content = strings.TrimSpace(event.Summary)
	}
	role := firstNonEmpty(
		strings.TrimSpace(stringFromAny(message["role"])),
		assistantRoleFromType(event.EventType),
	)
	if role == "" {
		role = "assistant"
	}
	msgType := "text"
	if role == "system" {
		msgType = "system"
	}
	return ConversationMessage{
		ID:        messageID,
		Role:      role,
		Type:      msgType,
		Content:   content,
		Status:    strings.TrimSpace(event.Status),
		CreatedAt: event.CreatedAt,
		UpdatedAt: event.CreatedAt,
		Metadata:  payload,
	}, true
}

func assistantRoleFromType(eventType string) string {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	switch {
	case strings.HasPrefix(eventType, "message.user"):
		return "user"
	case strings.HasPrefix(eventType, "message.system"):
		return "system"
	default:
		return "assistant"
	}
}

func readMap(value map[string]any, key string) map[string]any {
	raw, ok := value[key].(map[string]any)
	if ok {
		return raw
	}
	return map[string]any{}
}

func cloneMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
