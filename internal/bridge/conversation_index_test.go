package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	transport "rokid-prism-hub/internal/realtime"
)

type conversationIndexTestAdapter struct {
	pluginruntime.StaticPlugin
	hints   []pluginruntime.NativeSessionHint
	listErr error
}

func (a *conversationIndexTestAdapter) ListSessions(context.Context) ([]pluginruntime.NativeSessionHint, error) {
	if a.listErr != nil {
		return nil, a.listErr
	}
	return append([]pluginruntime.NativeSessionHint(nil), a.hints...), nil
}

func TestBuildConversationIndexAssignsStableSignature(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{
			{
				PluginID:        "codex",
				NativeSessionID: "sess-1",
				NativeThreadID:  "thread-1",
				Cwd:             "/tmp/project-a",
				Title:           "Task A",
				LastActivityAt:  now,
				Metadata: map[string]string{
					"model":               "gpt-5.6",
					"approval_mode":       "on-request",
					"context_tokens_used": "100",
					"session_kind":        "direct",
					"origin_provider":     "wechat",
					"agent_id":            "must-not-leak",
				},
			},
		},
	}
	svc := pluginruntime.New(adapter)
	cfg := remoteAuthorizedConfig()

	db := openConversationIndexTestDB(t)
	first := BuildConversationIndex(context.Background(), svc, db, cfg)
	second := BuildConversationIndex(context.Background(), svc, db, cfg)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected single plugin index, got first=%+v second=%+v", first, second)
	}
	if first[0].Signature == "" {
		t.Fatalf("expected plugin signature to be set: %+v", first[0])
	}
	if first[0].Signature != second[0].Signature {
		t.Fatalf("expected stable plugin signature, got %q vs %q", first[0].Signature, second[0].Signature)
	}
	metadata := first[0].Conversations[0].Metadata
	if metadata["session_kind"] != "direct" || metadata["origin_provider"] != "wechat" {
		t.Fatalf("expected session classification metadata, got %+v", metadata)
	}
	if _, leaked := metadata["agent_id"]; leaked {
		t.Fatalf("native agent identity must not enter the public index: %+v", metadata)
	}
	sigA := ConversationIndexSnapshotSignature(first)
	sigB := ConversationIndexSnapshotSignature(second)
	if sigA == "" || sigA != sigB {
		t.Fatalf("expected stable snapshot signature, got %q vs %q", sigA, sigB)
	}
}

func TestBuildConversationIndexDeduplicatesCanonicalConversationIDs(t *testing.T) {
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{
			{
				PluginID:        "codex",
				NativeSessionID: "session-1",
				NativeThreadID:  "thread-1",
				Title:           "First title wins",
			},
			{
				PluginID:        "codex",
				NativeSessionID: "session-1",
				NativeThreadID:  "thread-1",
				Title:           "Duplicate hint",
			},
		},
	}

	items := BuildConversationIndex(
		context.Background(), pluginruntime.New(adapter), openConversationIndexTestDB(t), remoteAuthorizedConfig())
	if len(items) != 1 || len(items[0].Conversations) != 1 {
		t.Fatalf("expected one canonical conversation, got %+v", items)
	}
	if items[0].Conversations[0].Title != "First title wins" {
		t.Fatalf("expected first canonical hint to win, got %+v", items[0].Conversations[0])
	}
}

func TestConversationIndexSerializesOnlyFlatConversations(t *testing.T) {
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{{
			PluginID:        "codex",
			NativeSessionID: "session-1",
			NativeThreadID:  "thread-1",
			Cwd:             "/tmp/project-a",
			Title:           "One flat item",
		}},
	}

	items := BuildConversationIndex(
		context.Background(), pluginruntime.New(adapter), openConversationIndexTestDB(t), remoteAuthorizedConfig())
	encoded, err := json.Marshal(items[0])
	if err != nil {
		t.Fatalf("marshal index: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatalf("unmarshal index: %v", err)
	}
	if _, exists := payload["projects"]; exists {
		t.Fatalf("index must not serialize nested projects: %s", encoded)
	}
	if conversations, ok := payload["conversations"].([]any); !ok || len(conversations) != 1 {
		t.Fatalf("expected one flat conversation: %s", encoded)
	}
}

func TestConversationIndexSnapshotSignatureChangesOnContentUpdate(t *testing.T) {
	base := []transport.ConversationIndexPlugin{
		{
			PluginID:    "codex",
			DisplayName: "Codex",
			Signature:   "sig-a",
		},
	}
	changed := []transport.ConversationIndexPlugin{
		{
			PluginID:    "codex",
			DisplayName: "Codex",
			Signature:   "sig-b",
		},
	}
	left := ConversationIndexSnapshotSignature(base)
	right := ConversationIndexSnapshotSignature(changed)
	if left == "" || right == "" {
		t.Fatalf("expected non-empty signatures: left=%q right=%q", left, right)
	}
	if left == right {
		t.Fatalf("expected snapshot signature to change when plugin signature changes: %q", left)
	}
}

func TestBuildConversationIndexSkipsDisabledAgents(t *testing.T) {
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{
			{
				PluginID:        "codex",
				NativeSessionID: "sess-1",
				NativeThreadID:  "thread-1",
				Title:           "Task A",
			},
		},
	}
	svc := pluginruntime.New(adapter)
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}

	items := BuildConversationIndex(context.Background(), svc, openConversationIndexTestDB(t), cfg)
	if len(items) != 0 {
		t.Fatalf("disabled plugin should be excluded from conversation index snapshot, got %+v", items)
	}
}

func TestBuildConversationIndexDoesNotCreateEmptySnapshotWhenListFails(t *testing.T) {
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		listErr: errors.New("local state unavailable"),
	}

	items := BuildConversationIndex(
		context.Background(), pluginruntime.New(adapter), openConversationIndexTestDB(t), remoteAuthorizedConfig())
	if len(items) != 0 {
		t.Fatalf("failed listSessions must not become an empty snapshot: %+v", items)
	}
}

func TestBuildConversationIndexRetainsReadableSessionsWhenDesktopControlIsUnavailable(t *testing.T) {
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       false,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{{
			PluginID:        "codex",
			NativeSessionID: "session-1",
			NativeThreadID:  "session-1",
			Title:           "Retained desktop session",
		}},
	}

	items := BuildConversationIndex(
		context.Background(), pluginruntime.New(adapter), openConversationIndexTestDB(t), remoteAuthorizedConfig())
	if len(items) != 1 || len(items[0].Conversations) != 1 {
		t.Fatalf("readable local sessions should remain indexed while control is unavailable, got %+v", items)
	}
	if items[0].Available {
		t.Fatalf("index must still report live control unavailable, got %+v", items[0])
	}
}

func TestConversationIndexNormalizesStatusToCoarseStates(t *testing.T) {
	now := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID: "codex",
			Capability: pluginruntime.Capability{
				PluginID:        "codex",
				Available:       true,
				CanListSessions: true,
			},
		},
		hints: []pluginruntime.NativeSessionHint{
			{
				PluginID:        "codex",
				NativeSessionID: "sess-running",
				NativeThreadID:  "thread-running",
				Title:           "Running",
				LastActivityAt:  now,
				Metadata:        map[string]string{"status": "waiting"},
			},
			{
				PluginID:        "codex",
				NativeSessionID: "sess-approval",
				NativeThreadID:  "thread-approval",
				Title:           "Approval",
				LastActivityAt:  now.Add(-time.Minute),
				Metadata:        map[string]string{"task_status": "waiting_approval"},
			},
			{
				PluginID:        "codex",
				NativeSessionID: "sess-done",
				NativeThreadID:  "thread-done",
				Title:           "Done",
				LastActivityAt:  now.Add(-2 * time.Minute),
				Metadata:        map[string]string{"status": "completed"},
			},
		},
	}
	svc := pluginruntime.New(adapter)
	cfg := remoteAuthorizedConfig()

	items := BuildConversationIndex(context.Background(), svc, openConversationIndexTestDB(t), cfg)
	if len(items) != 1 || len(items[0].Conversations) != 3 {
		t.Fatalf("unexpected conversation index payload: %+v", items)
	}
	statuses := map[string]string{}
	for _, conversation := range items[0].Conversations {
		statuses[conversation.Title] = conversation.Status
	}
	if statuses["Approval"] != "waiting_approval" {
		t.Fatalf("expected waiting_approval coarse status, got %+v", statuses)
	}
	if statuses["Running"] != "running" {
		t.Fatalf("expected running coarse status, got %+v", statuses)
	}
	if statuses["Done"] != "idle" {
		t.Fatalf("expected idle coarse status, got %+v", statuses)
	}
}

func TestConversationIndexSortsPinnedThenCodexRecency(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{PluginID: "codex", Capability: pluginruntime.Capability{
			PluginID: "codex", Available: true, CanListSessions: true,
		}},
		hints: []pluginruntime.NativeSessionHint{
			{PluginID: "codex", NativeSessionID: "old", NativeThreadID: "old", Title: "old", LastActivityAt: now.Add(-time.Hour)},
			{PluginID: "codex", NativeSessionID: "pinned", NativeThreadID: "pinned", Title: "pinned", LastActivityAt: now.Add(-2 * time.Hour), Metadata: map[string]string{"pinned": "true"}},
			{PluginID: "codex", NativeSessionID: "new", NativeThreadID: "new", Title: "new", LastActivityAt: now},
		},
	}
	items := BuildConversationIndex(context.Background(), pluginruntime.New(adapter), openConversationIndexTestDB(t), remoteAuthorizedConfig())
	if len(items) != 1 || len(items[0].Conversations) != 3 {
		t.Fatalf("unexpected conversation index payload: %+v", items)
	}
	got := []string{items[0].Conversations[0].Title, items[0].Conversations[1].Title, items[0].Conversations[2].Title}
	want := []string{"pinned", "new", "old"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("conversation order = %v, want %v", got, want)
	}
}

func TestConversationIndexEnvelopeNeverUsesConversationIDAsDisplayTitle(t *testing.T) {
	service := &QueryService{}
	conversation := resolvedConversation{
		ConversationID: "conv_internal_123",
		Hint: pluginruntime.NativeSessionHint{
			Title: "conv_internal_123",
		},
	}

	envelope := service.conversationIndexEnvelope(conversation, "codex")
	if title := envelope["title"]; title != "" {
		t.Fatalf("conversation ID must not become an index title: %#v", title)
	}
	if summary := envelope["summary"]; summary != "" {
		t.Fatalf("conversation ID must not become an index summary: %#v", summary)
	}
}

func openConversationIndexTestDB(t *testing.T) *bridgedb.DB {
	t.Helper()
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
