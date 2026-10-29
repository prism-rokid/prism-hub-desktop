package bridge

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
)

func TestHistoryServiceReconstructsConversationMessages(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	at := time.Now().UTC()
	records := []bridgedb.EventRecord{
		{
			SessionID:           "session-1",
			PrismConversationID: "conv-history-1",
			PluginID:            "openclaw",
			NativeEventID:       "user-1",
			EventType:           "message.user.accepted",
			Status:              StageAccepted,
			Summary:             "user accepted",
			Payload: map[string]any{
				"message": map[string]any{
					"id":   "user-msg-1",
					"role": "user",
					"text": "你好",
				},
			},
			CreatedAt: at,
		},
		{
			SessionID:           "session-1",
			PrismConversationID: "conv-history-1",
			PluginID:            "openclaw",
			NativeEventID:       "assistant-delta-1",
			EventType:           "message.delta",
			Status:              StageRunning,
			Summary:             "streaming",
			Payload: map[string]any{
				"message": map[string]any{
					"id":    "assistant-msg-1",
					"role":  "assistant",
					"delta": "Hello",
				},
			},
			CreatedAt: at.Add(time.Second),
		},
		{
			SessionID:           "session-1",
			PrismConversationID: "conv-history-1",
			PluginID:            "openclaw",
			NativeEventID:       "assistant-final-1",
			EventType:           "run.completed",
			Status:              StageCompleted,
			Summary:             "Hello world",
			Payload: map[string]any{
				"message": map[string]any{
					"id":   "assistant-msg-1",
					"role": "assistant",
					"text": "Hello world",
				},
			},
			CreatedAt: at.Add(2 * time.Second),
		},
	}
	for _, record := range records {
		if _, err := db.InsertEvent(ctx, record); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}

	service := &HistoryService{DB: db}
	messages, err := service.ListConversationMessages(ctx, "conv-history-1", "openclaw", 50)
	if err != nil {
		t.Fatalf("ListConversationMessages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 reconstructed messages, got %+v", messages)
	}
	if messages[0].ID != "user-msg-1" || messages[0].Role != "user" || messages[0].Content != "你好" {
		t.Fatalf("unexpected user message: %+v", messages[0])
	}
	if messages[1].ID != "assistant-msg-1" || messages[1].Role != "assistant" || messages[1].Content != "Hello world" || messages[1].Status != StageCompleted {
		t.Fatalf("unexpected assistant message: %+v", messages[1])
	}
}
