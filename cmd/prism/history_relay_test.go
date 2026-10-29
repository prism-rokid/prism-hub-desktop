package main

import (
	"context"
	"testing"
)

func TestCancelConversationHistoryRelayRequiresMatchingIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	streamID := "test-history-relay-cancel"
	relay, cancelled := registerConversationHistoryRelay(streamID, "conversation-1", "codex", cancel)
	if cancelled {
		t.Fatal("unexpected pre-cancelled relay")
	}
	defer func() {
		unregisterConversationHistoryRelay(streamID, relay)
		cancel()
	}()

	if cancelConversationHistoryRelay(streamID, "conversation-2", "codex") {
		t.Fatal("mismatched conversation cancelled relay")
	}
	if cancelConversationHistoryRelay(streamID, "conversation-1", "other.plugin") {
		t.Fatal("mismatched plugin cancelled relay")
	}
	if !cancelConversationHistoryRelay(streamID, "conversation-1", "codex") {
		t.Fatal("matching relay was not cancelled")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("matching cancellation did not cancel stream context")
	}
	if cancelConversationHistoryRelay(streamID, "conversation-1", "codex") {
		t.Fatal("repeated cancellation was not idempotent")
	}
}

func TestCancelledConversationHistoryRelayPreventsLateStreamRegistration(t *testing.T) {
	streamID := "test-history-relay-pre-cancel"
	if cancelConversationHistoryRelay(streamID, "conversation-1", "codex") {
		t.Fatal("pre-cancel must not report a missing relay as active")
	}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	relay, cancelled := registerConversationHistoryRelay(streamID, "conversation-1", "codex", cancel)
	if !cancelled || relay != nil {
		t.Fatalf("late stream must be rejected after pre-cancel: relay=%+v cancelled=%v", relay, cancelled)
	}
}
