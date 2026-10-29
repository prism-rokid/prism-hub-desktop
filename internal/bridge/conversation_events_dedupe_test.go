package bridge

import (
	"testing"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestStableConversationEventDedupeIDUsesPluginEventIdentity(t *testing.T) {
	input := conversationEventContext{ConversationID: "conv-push-qa"}
	first := pluginruntime.PluginEvent{
		ID:        "codex-task:thread-qa:turn-1:completed",
		Type:      "run.completed",
		Status:    "completed",
		Summary:   "Codex 已完成。",
		CreatedAt: time.Date(2026, 10, 13, 11, 0, 0, 0, time.UTC),
	}
	replayed := first
	replayed.Summary = "这次运行的最终回答正文"
	replayed.CreatedAt = first.CreatedAt.Add(time.Minute)

	firstID := stableConversationEventDedupeID(input, first)
	if replayedID := stableConversationEventDedupeID(input, replayed); replayedID != firstID {
		t.Fatalf("re-emitted plugin event should keep its dedupe ID: first=%q replay=%q", firstID, replayedID)
	}

	newRun := replayed
	newRun.ID = "codex-task:thread-qa:turn-2:completed"
	if newRunID := stableConversationEventDedupeID(input, newRun); newRunID == firstID {
		t.Fatalf("new plugin event should get a new dedupe ID: %q", newRunID)
	}
}
