package bridge

import (
	"testing"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestRunTrackerSuppressesMatchingRunID(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-1",
		ConversationID:  "conv-1",
		PluginID:        "codex",
		NativeSessionID: "session-1",
		NativeThreadID:  "thread-1",
		NativeRunID:     "turn-1",
	})
	if !tracker.ShouldSuppress("codex", "", pluginruntime.NativeSession{}, pluginruntime.PluginEvent{
		ID:      "turn-1",
		Type:    "run.completed",
		Status:  "completed",
		Payload: map[string]any{"turn_id": "turn-1"},
	}) {
		t.Fatal("expected matching run id to be suppressed")
	}
}

func TestRunTrackerSuppressesConversationWhileForwardRunIsActive(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-2",
		ConversationID:  "conv-2",
		PluginID:        "hermes",
		NativeSessionID: "stored-2",
		NativeThreadID:  "live-2",
	})
	if !tracker.ShouldSuppress("hermes", "conv-2", pluginruntime.NativeSession{NativeSessionID: "stored-2"}, pluginruntime.PluginEvent{
		ID:      "evt-1",
		Type:    "message.delta",
		Status:  "running",
		Payload: map[string]any{"session_id": "live-2"},
	}) {
		t.Fatal("expected active forward conversation to suppress reverse event")
	}
}

func TestRunTrackerSuppressesInterruptingEventTypesInActiveConversation(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-2b",
		ConversationID:  "conv-2b",
		PluginID:        "hermes",
		NativeSessionID: "stored-2b",
		NativeThreadID:  "live-2b",
	})
	for _, eventType := range []string{"message.delta", "message.started", "run.completed", "run.failed", "approval.requested"} {
		if !tracker.ShouldSuppress("hermes", "conv-2b", pluginruntime.NativeSession{NativeSessionID: "stored-2b"}, pluginruntime.PluginEvent{
			ID:      "evt-" + eventType,
			Type:    eventType,
			Status:  "running",
			Payload: map[string]any{"session_id": "live-2b"},
		}) {
			t.Fatalf("expected active forward conversation to suppress event type %q", eventType)
		}
	}
}

func TestRunTrackerDoesNotSuppressStatusUpdateInActiveConversation(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-2c",
		ConversationID:  "conv-2c",
		PluginID:        "hermes",
		NativeSessionID: "stored-2c",
		NativeThreadID:  "live-2c",
		NativeRunID:     "run-forward-2c",
	})
	if tracker.ShouldSuppress("hermes", "conv-2c", pluginruntime.NativeSession{
		NativeSessionID: "stored-2c",
		NativeThreadID:  "live-2c",
	}, pluginruntime.PluginEvent{
		ID:      "evt-status",
		Type:    "status.update",
		Status:  "running",
		Payload: map[string]any{"session_id": "live-2c"},
	}) {
		t.Fatal("expected status update to remain visible during active forward run")
	}
}

func TestRunTrackerDoesNotSuppressDifferentConversationWhenForwardRunIsActive(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-3",
		ConversationID:  "conv-3",
		PluginID:        "hermes",
		NativeSessionID: "stored-3",
		NativeThreadID:  "live-3",
		NativeRunID:     "run-forward-1",
	})
	if tracker.ShouldSuppress("hermes", "conv-999", pluginruntime.NativeSession{
		NativeSessionID: "stored-3",
		NativeThreadID:  "live-3",
	}, pluginruntime.PluginEvent{
		ID:      "evt-2",
		Type:    "message.delta",
		Status:  "running",
		Payload: map[string]any{"session_id": "live-3"},
	}) {
		t.Fatal("expected different conversation event to remain visible while active run belongs elsewhere")
	}
}

func TestRunTrackerSuppressesOnlyMatchingRunAfterTerminal(t *testing.T) {
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-4",
		ConversationID:  "conv-4",
		PluginID:        "hermes",
		NativeSessionID: "stored-4",
		NativeThreadID:  "live-4",
		NativeRunID:     "run-forward-4",
	})
	tracker.MarkTerminal("req-4")
	if !tracker.ShouldSuppress("hermes", "conv-4", pluginruntime.NativeSession{
		NativeSessionID: "stored-4",
		NativeThreadID:  "live-4",
	}, pluginruntime.PluginEvent{
		ID:      "run-forward-4",
		Type:    "run.completed",
		Status:  "completed",
		Payload: map[string]any{"run_id": "run-forward-4"},
	}) {
		t.Fatal("expected matching terminal run to stay suppressed during grace period")
	}
	if tracker.ShouldSuppress("hermes", "conv-4", pluginruntime.NativeSession{
		NativeSessionID: "stored-4",
		NativeThreadID:  "live-4",
	}, pluginruntime.PluginEvent{
		ID:      "evt-later",
		Type:    "message.delta",
		Status:  "running",
		Payload: map[string]any{"session_id": "live-4"},
	}) {
		t.Fatal("expected later different event in same session to remain visible after terminal when run id differs")
	}
}

func TestRunTrackerReleaseAndExpiry(t *testing.T) {
	base := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	tracker := NewRunTracker()
	tracker.now = func() time.Time { return base }
	tracker.activeTTL = time.Second
	tracker.Track(ForwardRun{
		RequestID:   "req-4",
		PluginID:    "openclaw",
		NativeRunID: "run-4",
	})
	tracker.Release("req-4")
	if tracker.ShouldSuppress("openclaw", "", pluginruntime.NativeSession{}, pluginruntime.PluginEvent{ID: "run-4"}) {
		t.Fatal("expected released run to stop suppressing")
	}
	tracker.Track(ForwardRun{
		RequestID:   "req-5",
		PluginID:    "openclaw",
		NativeRunID: "run-5",
	})
	tracker.now = func() time.Time { return base.Add(2 * time.Second) }
	if tracker.ShouldSuppress("openclaw", "", pluginruntime.NativeSession{}, pluginruntime.PluginEvent{ID: "run-5"}) {
		t.Fatal("expected expired run to stop suppressing")
	}
}
