package bridge

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

func TestQueryServiceConversationMessages(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindQueryConversation(t, db)
	ctx := context.Background()
	now := time.Now().UTC()

	records := []bridgedb.EventRecord{
		{
			SessionID:           "session-1",
			PrismConversationID: "conv-query-1",
			PluginID:            "codex",
			NativeEventID:       "user-event-1",
			EventType:           "message.user.accepted",
			Status:              StageAccepted,
			Summary:             "你好",
			Payload: map[string]any{
				"message": map[string]any{
					"id":   "user:req-1",
					"role": "user",
					"text": "你好",
				},
			},
			CreatedAt: now,
		},
		{
			SessionID:           "session-1",
			PrismConversationID: "conv-query-1",
			PluginID:            "codex",
			NativeEventID:       "assistant-event-1",
			EventType:           "run.completed",
			Status:              StageCompleted,
			Summary:             "你好，我在。",
			Payload: map[string]any{
				"message": map[string]any{
					"id":   "assistant:req-1",
					"role": "assistant",
					"text": "你好，我在。",
				},
			},
			CreatedAt: now.Add(time.Second),
		},
	}
	for _, record := range records {
		if _, err := db.InsertEvent(ctx, record); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}

	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		DB:      db,
		History: &HistoryService{DB: db},
		Service: pluginruntime.New(&queryHistoryAdapter{}),
	}
	result, err := service.Execute(ctx, &prismtransport.InboundPayload{
		QueryType:      "conversation.messages",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	items, ok := result["items"].([]map[string]any)
	if !ok {
		t.Fatalf("expected items payload, got %#v", result["items"])
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 messages, got %#v", items)
	}
	if metadata, ok := items[0]["metadata"].(map[string]any); ok && len(metadata) != 0 {
		t.Fatalf("expected non-attachment legacy metadata to be removed, got %#v", metadata)
	}
	if _, exists := result["run_status"]; exists {
		t.Fatalf("did not expect run_status in messages payload: %#v", result["run_status"])
	}
}

func TestQueryServiceOpenConversationHistoryStream(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindQueryConversation(t, db)
	service := &QueryService{Config: remoteAuthorizedConfig(), DB: db, History: &HistoryService{DB: db}, Service: pluginruntime.New(&queryHistoryAdapter{})}
	stream, err := service.OpenConversationHistoryStream(context.Background(), "conv-query-1", "codex", pluginruntime.HistoryStreamRequest{StreamID: "body-1", Limit: 21})
	if err != nil {
		t.Fatalf("OpenConversationHistoryStream: %v", err)
	}
	event := <-stream.Events
	if event.Type != "turn" || event.Turn == nil || event.Turn.TurnID != "turn-query-1" {
		t.Fatalf("unexpected stream event: %+v", event)
	}
}

func TestPublicHistoryMetadataOnlyExposesAttachmentDescriptors(t *testing.T) {
	metadata := publicHistoryMetadata(map[string]any{
		"rollout_file": "/Users/example/.codex/session.jsonl",
		"image_url":    "data:image/png;base64,secret",
		"attachments": []any{
			map[string]any{"name": "diagram.png", "mime_type": "image/png", "kind": "image", "url": "file:///private/secret.png"},
		},
	})
	attachments, ok := metadata["attachments"].([]map[string]string)
	if !ok || len(attachments) != 1 {
		t.Fatalf("expected one public attachment descriptor, got %#v", metadata)
	}
	if attachments[0]["name"] != "diagram.png" || attachments[0]["mime_type"] != "image/png" || attachments[0]["kind"] != "image" || attachments[0]["url"] != "" {
		t.Fatalf("attachment leaked unsafe data: %#v", attachments[0])
	}
}

func TestPublicHistoryMessagesPreservesStructuredProgress(t *testing.T) {
	started := time.Date(2026, time.July, 28, 10, 0, 0, 0, time.UTC)
	completed := started.Add(3 * time.Second)
	items := PublicHistoryMessages([]pluginruntime.HistoryMessage{{
		ID: "progress:user-1", Role: "assistant", Type: "progress", Content: "已处理 0 分 3 秒",
		Progress: &pluginruntime.HistoryProgress{
			Status: "completed", StartedAt: started, CompletedAt: completed,
			Steps: []pluginruntime.HistoryProgressStep{{
				ID: "step-1", Kind: "tool", CallID: "call-1", Title: "正在执行", Detail: "读取 src/main.js", Status: "completed", CreatedAt: started.Add(time.Second),
			}},
		},
	}})
	progress, ok := items[0]["progress"].(map[string]any)
	if !ok || progress["status"] != "completed" || progress["started_at"] != started.UnixMilli() || progress["completed_at"] != completed.UnixMilli() {
		t.Fatalf("unexpected public progress: %#v", items[0]["progress"])
	}
	steps, ok := progress["steps"].([]map[string]any)
	if !ok || len(steps) != 1 || steps[0]["kind"] != "tool" || steps[0]["call_id"] != "call-1" || steps[0]["detail"] != "读取 src/main.js" {
		t.Fatalf("unexpected public progress steps: %#v", progress["steps"])
	}
}

func TestPublicHistoryMessagesRedactsInlineDataURI(t *testing.T) {
	items := PublicHistoryMessages([]pluginruntime.HistoryMessage{{
		ID: "assistant:image-1", Role: "assistant", Type: "progress",
		Content: "结果 ![图](data:image/png;base64,aGVsbG8=)",
		Progress: &pluginruntime.HistoryProgress{Steps: []pluginruntime.HistoryProgressStep{{
			ID: "step-image-1", Kind: "tool", Title: "读取图片",
			Detail: "data:image/jpeg;base64,d29ybGQ=",
		}}},
	}})
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	content, _ := items[0]["content"].(string)
	if strings.Contains(content, "data:image") || !strings.Contains(content, "[附件内容已省略]") {
		t.Fatalf("inline content URI leaked: %q", content)
	}
	progress, _ := items[0]["progress"].(map[string]any)
	steps, _ := progress["steps"].([]map[string]any)
	detail, _ := steps[0]["detail"].(string)
	if strings.Contains(detail, "data:image") || detail != "[附件内容已省略]" {
		t.Fatalf("inline progress URI leaked: %q", detail)
	}
}

func TestQueryServiceConversationsList(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		DB:      db,
		Service: pluginruntime.New(&queryHistoryAdapter{}),
	}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType: "conversations.list",
		PluginID:  "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	items, ok := result["items"].([]map[string]any)
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected items payload: %#v", result["items"])
	}
	if items[0]["conversation_id"] == "" || items[0]["plugin_id"] != "codex" {
		t.Fatalf("unexpected conversation item: %#v", items[0])
	}
}

func TestQueryServiceRejectsDeprecatedConversationControlsQuery(t *testing.T) {
	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		Service: pluginruntime.New(&queryHistoryAdapter{}),
	}
	_, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.controls.get",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported query_type") {
		t.Fatalf("expected deprecated controls query to be rejected, got %v", err)
	}
}

func TestQueryServiceConversationSelectUsesStandardSessionControl(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-select-query", PluginID: "codex",
		NativeSessionID: "native-select", NativeThreadID: "thread-select",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &selectingQueryAdapter{}
	service := &QueryService{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter)}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType: "conversation.select", ConversationID: "conv-select-query", PluginID: "codex",
		RequestID: "select-request", Metadata: &prismtransport.InboundMetadata{SourceDevice: "mobile"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !adapter.selected || adapter.request.Action != "conversation.select" || adapter.request.Metadata["source_device"] != "mobile" {
		t.Fatalf("unexpected selection request: %+v", adapter.request)
	}
	if result["desktop_selected"] != true || result["conversation_id"] != "conv-select-query" {
		t.Fatalf("unexpected selection result: %#v", result)
	}
}

func TestQueryServiceConversationSelectSkipsUnadvertisedSessionControl(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-select-query", PluginID: "codex",
		NativeSessionID: "native-select", NativeThreadID: "thread-select",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &queryHistoryAdapter{}
	service := &QueryService{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter)}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType: "conversation.select", ConversationID: "conv-select-query", PluginID: "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if adapter.controlCalls != 0 || result["selected"] != true || result["desktop_selected"] != false {
		t.Fatalf("selection should remain passive without advertised control: result=%#v calls=%d", result, adapter.controlCalls)
	}
}

func TestQueryServiceCanonicalConversationDetailUsesListedHint(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		DB:      db,
		Service: pluginruntime.New(&queryHistoryAdapter{}),
	}
	listed, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType: "conversations.list",
		PluginID:  "codex",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items := listed["items"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("unexpected list: %#v", listed)
	}
	conversationID, _ := items[0]["conversation_id"].(string)
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: conversationID,
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result["title"] != "Stub Query Session" {
		t.Fatalf("expected listed title, got %#v", result["title"])
	}
	if result["cwd"] != "/tmp/prism-query-test" {
		t.Fatalf("expected listed cwd, got %#v", result["cwd"])
	}
	if _, exists := result["metadata"]; exists {
		t.Fatalf("detail must not expose indexed metadata: %#v", result["metadata"])
	}
	if result["current_model"] != nil {
		t.Fatalf("detail query must not read live controls, got %#v", result["current_model"])
	}
}

func TestQueryServiceDetailUsesWatcherSnapshotWithoutRuntimeDetailQuery(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindQueryConversation(t, db)
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-query-1",
		"plugin_id":          "codex",
		"current_model":      map[string]any{"id": "gpt-5.6", "label": "GPT-5.6"},
		"current_reasoning":  map[string]any{"id": "high", "label": "高"},
		"current_permission": map[string]any{"id": "on-request", "label": "请求批准"},
		"model_options":      []map[string]any{{"id": "gpt-5.6", "label": "GPT-5.6", "available": true}},
		"reasoning_options":  []map[string]any{{"id": "high", "label": "高", "available": true}},
		"permission_options": []map[string]any{{"id": "on-request", "label": "请求批准", "available": true}},
		"actions":            []map[string]any{{"id": "pin", "label": "置顶会话", "available": true}},
		"run":                map[string]any{"status": StageRunning, "summary": "watcher reported running"},
		"desktop_foreground": true,
		"detail_stale":       false,
	})
	adapter := &queryHistoryAdapter{}
	service := &QueryService{
		Config:          remoteAuthorizedConfig(),
		DB:              db,
		Service:         pluginruntime.New(adapter),
		DetailSnapshots: store,
	}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	model, ok := result["current_model"].(map[string]any)
	if !ok || model["id"] != "gpt-5.6" {
		t.Fatalf("expected watcher current model, got %#v", result["current_model"])
	}
	run, ok := result["run"].(map[string]any)
	if !ok || run["status"] != StageRunning {
		t.Fatalf("expected watcher run state, got %#v", result["run"])
	}
	if adapter.controlCalls != 0 || adapter.readStatusCalls != 0 {
		t.Fatalf("detail query must not call control or read status, controls=%d read_status=%d", adapter.controlCalls, adapter.readStatusCalls)
	}
}

func TestQueryServiceProtocolNativeDetailReadsDurableProjectionWithoutListingSessions(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindProtocolNativeQueryConversation(t, db)
	adapter := &protocolNativeDetailAdapter{}
	service := &QueryService{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter)}

	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: "conv-query-1",
		PluginID:       "hermes",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if adapter.listCalls != 0 {
		t.Fatalf("protocol-native detail must use the existing binding, listSessions=%d", adapter.listCalls)
	}
	if adapter.detailCalls != 1 {
		t.Fatalf("expected one durable detail read, got %d", adapter.detailCalls)
	}
	model := asMap(result["current_model"])
	if model["id"] != "GPT-5.6" {
		t.Fatalf("unexpected durable current model: %#v", model)
	}
}

func TestQueryServiceDetailReusesCachedMessageSignatureWithoutHistoryRead(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindQueryConversation(t, db)
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-query-1",
		"plugin_id":          "codex",
		"messages_signature": "msgsig-cached-1",
	})
	adapter := &queryCachedSignatureAdapter{}
	service := &QueryService{
		Config:          remoteAuthorizedConfig(),
		DB:              db,
		Service:         pluginruntime.New(adapter),
		DetailSnapshots: store,
	}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if adapter.historyCalls != 0 {
		t.Fatalf("expected cached messages_signature to skip history read, got %d calls", adapter.historyCalls)
	}
	if result["messages_signature"] != "msgsig-cached-1" {
		t.Fatalf("expected cached messages_signature preserved, got %#v", result["messages_signature"])
	}
}

func TestQueryServiceCanonicalConversationMessagesUseBoundSession(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &countingQueryHistoryAdapter{}
	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		DB:      db,
		Service: pluginruntime.New(adapter),
	}
	listed, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType: "conversations.list",
		PluginID:  "codex",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	conversationID := listed["items"].([]map[string]any)[0]["conversation_id"].(string)
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.messages",
		ConversationID: conversationID,
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	items, ok := result["items"].([]map[string]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected message items, got %#v", result["items"])
	}
	if adapter.listCalls == 0 {
		t.Fatalf("expected list to establish a canonical binding")
	}
}

func TestQueryServiceConversationMessagesRejectsUnboundDetailSnapshot(t *testing.T) {
	adapter := &countingQueryHistoryAdapter{}
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":        "conv-missing",
		"plugin_id":              "codex",
		"native_conversation_id": "native-conv-missing",
		"title":                  "Missing Conversation",
		"cwd":                    "/tmp/prism-query-missing",
		"messages_signature":     "cached-msgsig",
		"updated_at":             time.Now().UnixMilli(),
	})
	service := &QueryService{
		Config:          remoteAuthorizedConfig(),
		Service:         pluginruntime.New(adapter),
		DetailSnapshots: store,
	}
	_, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.messages",
		ConversationID: "conv-missing",
		PluginID:       "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "conversation query requires bridge db") {
		t.Fatalf("expected unbound detail snapshot rejection, got %v", err)
	}
}

func TestQueryServiceRejectsRemoteDisabledPluginBeforeRuntimeQuery(t *testing.T) {
	adapter := &countingQueryHistoryAdapter{}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}
	service := &QueryService{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
	}

	_, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.messages",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "remote access disabled") {
		t.Fatalf("expected remote-disabled query rejection, got %v", err)
	}
	if adapter.listCalls != 0 {
		t.Fatalf("expected query gate before adapter ListSessions, got %d calls", adapter.listCalls)
	}
}

func TestQueryServiceConversationsPreferFreshListedHintOverStoredRecord(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "remote-stale",
		PluginID:            "codex",
		NativeSessionID:     "native-conv-1",
		NativeThreadID:      "native-conv-1",
		Cwd:                 "/Users/example/.prism/projects/default",
		Surface:             "codex-desktop",
		Endpoint:            "codex://threads/native-conv-1",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	service := &QueryService{
		Config:  remoteAuthorizedConfig(),
		DB:      db,
		Service: pluginruntime.New(&queryHistoryAdapter{}),
	}
	result, err := service.Execute(ctx, &prismtransport.InboundPayload{
		QueryType: "conversations.list",
		PluginID:  "codex",
	})
	if err != nil {
		t.Fatalf("Execute list: %v", err)
	}
	items, ok := result["items"].([]map[string]any)
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected items payload: %#v", result["items"])
	}
	if items[0]["cwd"] != "/tmp/prism-query-test" {
		t.Fatalf("expected fresh listed cwd, got %#v", items[0]["cwd"])
	}

	detail, err := service.Execute(ctx, &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: "remote-stale",
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute detail: %v", err)
	}
	if detail["cwd"] != "/tmp/prism-query-test" {
		t.Fatalf("expected detail to resolve through fresh listed cwd, got %#v", detail["cwd"])
	}
	if detail["title"] != "Stub Query Session" {
		t.Fatalf("expected fresh listed title, got %#v", detail["title"])
	}
}

func TestDeriveRunStateWaitingApproval(t *testing.T) {
	now := time.Now().UTC()
	state := deriveRunState([]bridgedb.EventRecord{
		{
			PrismConversationID: "conv-query-2",
			PluginID:            "codex",
			NativeEventID:       "task-1",
			EventType:           "message.sent",
			Status:              StageAccepted,
			Summary:             "sent",
			CreatedAt:           now,
		},
		{
			PrismConversationID: "conv-query-2",
			PluginID:            "codex",
			NativeEventID:       "task-1",
			EventType:           "approval.required",
			Status:              StageWaitingApproval,
			Summary:             "need approval",
			CreatedAt:           now.Add(time.Second),
		},
	}, "conv-query-2", "codex")

	if state["status"] != StageWaitingApproval {
		t.Fatalf("expected waiting approval, got %#v", state)
	}
}

func TestDeriveRunStateInterruptedOverridesWaitTimeout(t *testing.T) {
	now := time.Now().UTC()
	state := deriveRunState([]bridgedb.EventRecord{
		{
			PrismConversationID: "conv-query-interrupt",
			PluginID:            "codex",
			NativeEventID:       "run-1",
			EventType:           "run.wait_timeout",
			Status:              StageRunning,
			Summary:             "wait timed out",
			CreatedAt:           now,
		},
		{
			PrismConversationID: "conv-query-interrupt",
			PluginID:            "codex",
			NativeEventID:       "run-1",
			EventType:           "task.interrupt",
			Status:              StageInterrupted,
			Summary:             "user_interrupt",
			CreatedAt:           now.Add(time.Second),
		},
	}, "conv-query-interrupt", "codex")

	if state["status"] != StageInterrupted {
		t.Fatalf("expected interrupted to override running timeout, got %#v", state)
	}
}

func TestDeriveRunStateUsesDesktopIdleWithoutReplacingRunIdentity(t *testing.T) {
	now := time.Now().UTC()
	state := deriveRunState([]bridgedb.EventRecord{
		{
			PrismConversationID: "conv-query-idle",
			PluginID:            "claudecode",
			NativeEventID:       "request-1",
			EventType:           "run.started",
			Status:              StageRunning,
			Summary:             "Claude turn started",
			CreatedAt:           now,
		},
		{
			PrismConversationID: "conv-query-idle",
			PluginID:            "claudecode",
			NativeEventID:       "mod-state-1",
			EventType:           "desktop.state.changed",
			Status:              "idle",
			Summary:             "Native Claude Mod session connected",
			CreatedAt:           now.Add(time.Second),
		},
	}, "conv-query-idle", "claudecode")

	if state["status"] != "idle" {
		t.Fatalf("expected desktop idle snapshot to end the active run, got %#v", state["status"])
	}
	if state["task_id"] != "request-1" || state["summary"] != "Claude turn started" {
		t.Fatalf("lifecycle state must not replace run identity or summary: %#v", state)
	}
}

func TestDeriveRunStateIgnoresSessionControlEvents(t *testing.T) {
	now := time.Now().UTC()
	state := deriveRunState([]bridgedb.EventRecord{
		{
			PrismConversationID: "conv-query-control",
			PluginID:            "codex",
			NativeEventID:       "task-1",
			EventType:           "message.sent",
			Status:              StageAccepted,
			Summary:             "user sent",
			CreatedAt:           now,
		},
		{
			PrismConversationID: "conv-query-control",
			PluginID:            "codex",
			NativeEventID:       "ctrl-1",
			EventType:           "session.control",
			Status:              StageCompleted,
			Summary:             "已切换模型到 GPT-5.6",
			CreatedAt:           now.Add(time.Second),
		},
	}, "conv-query-control", "codex")

	if state["summary"] != "user sent" {
		t.Fatalf("expected run summary to ignore control event, got %#v", state)
	}
	if state["task_id"] != "task-1" {
		t.Fatalf("expected run task id to ignore control event, got %#v", state)
	}
}

func TestDeriveRunStateIgnoresApprovalResolveEvents(t *testing.T) {
	now := time.Now().UTC()
	state := deriveRunState([]bridgedb.EventRecord{
		{
			PrismConversationID: "conv-query-approval-resolve",
			PluginID:            "codex",
			NativeEventID:       "task-1",
			EventType:           "approval.required",
			Status:              StageWaitingApproval,
			Summary:             "等待审批",
			CreatedAt:           now,
		},
		{
			PrismConversationID: "conv-query-approval-resolve",
			PluginID:            "codex",
			NativeEventID:       "approval-1",
			EventType:           "approval.resolve",
			Status:              StageRunning,
			Summary:             "已批准",
			CreatedAt:           now.Add(time.Second),
		},
	}, "conv-query-approval-resolve", "codex")

	if state["summary"] != "等待审批" {
		t.Fatalf("expected run summary to ignore approval.resolve, got %#v", state)
	}
	if state["status"] != StageWaitingApproval {
		t.Fatalf("expected waiting approval to remain until real run event arrives, got %#v", state)
	}
}

func TestQueryServiceDetailMarksNonForegroundWatcherSnapshotStale(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindQueryConversation(t, db)
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-other",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"detail_stale":       false,
		"updated_at":         time.Now().UnixMilli(),
	})
	store.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-query-1",
		"plugin_id":       "codex",
		"current_model":   map[string]any{"id": "gpt-cached"},
		"updated_at":      time.Now().UnixMilli(),
	})
	adapter := &queryHistoryAdapter{}
	service := &QueryService{
		Config:          remoteAuthorizedConfig(),
		DB:              db,
		Service:         pluginruntime.New(adapter),
		DetailSnapshots: store,
	}
	result, err := service.Execute(context.Background(), &prismtransport.InboundPayload{
		QueryType:      "conversation.detail.get",
		ConversationID: "conv-query-1",
		PluginID:       "codex",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if firstNonEmptyBool(result["desktop_foreground"]) || !firstNonEmptyBool(result["detail_stale"]) {
		t.Fatalf("expected old watcher snapshot to be stale, got %#v", result)
	}
	if adapter.controlCalls != 0 || adapter.readStatusCalls != 0 {
		t.Fatalf("detail query must not refresh an old snapshot, controls=%d read_status=%d", adapter.controlCalls, adapter.readStatusCalls)
	}
}

func bindQueryConversation(t *testing.T, db *bridgedb.DB) {
	t.Helper()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-query-1",
		PluginID:            "codex",
		NativeSessionID:     "native-conv-1",
		NativeThreadID:      "native-conv-1",
		Surface:             "test",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
}

func bindProtocolNativeQueryConversation(t *testing.T, db *bridgedb.DB) {
	t.Helper()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-query-1",
		PluginID:            "hermes",
		NativeSessionID:     "native-conv-1",
		NativeThreadID:      "native-conv-1",
		Surface:             "test",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
}

type queryHistoryAdapter struct {
	controlCalls    int
	readStatusCalls int
}

type selectingQueryAdapter struct {
	queryHistoryAdapter
	selected bool
	request  pluginruntime.SessionControlRequest
}

func (a *selectingQueryAdapter) Probe(ctx context.Context) (pluginruntime.Capability, error) {
	capability, err := a.queryHistoryAdapter.Probe(ctx)
	capability.CanControlSession = true
	return capability, err
}

func (a *selectingQueryAdapter) ControlSession(_ context.Context, req pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	a.selected = true
	a.request = req
	return pluginruntime.SessionControlResult{OK: true, Action: req.Action, Details: map[string]any{"selected": true}}, nil
}

type protocolNativeDetailAdapter struct {
	queryHistoryAdapter
	listCalls   int
	detailCalls int
}

func (a *protocolNativeDetailAdapter) ID() string { return "hermes" }

func (a *protocolNativeDetailAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{
		PluginID:         "hermes",
		Available:        true,
		CanListSessions:  true,
		CanReadHistory:   true,
		CanAttachSession: true,
		IntegrationMode:  pluginruntime.IntegrationModeProtocolNative,
	}, nil
}

func (a *protocolNativeDetailAdapter) ListSessions(context.Context) ([]pluginruntime.NativeSessionHint, error) {
	a.listCalls++
	return nil, nil
}

func (a *protocolNativeDetailAdapter) ReadDetail(context.Context, pluginruntime.NativeSession) (map[string]any, error) {
	a.detailCalls++
	return map[string]any{
		"current_model":     map[string]any{"option_id": "openai/gpt-5.6", "label": "GPT-5.6"},
		"current_reasoning": map[string]any{"option_id": "high", "label": "High"},
		"actions":           []map[string]any{{"id": "message.send", "available": true}},
		"run":               map[string]any{"status": "idle", "primary_action": "send"},
	}, nil
}

func (a *queryHistoryAdapter) ID() string { return "codex" }

func (a *queryHistoryAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{
		PluginID:           "codex",
		Available:          true,
		CanForwardSync:     true,
		CanListSessions:    true,
		CanReadHistory:     true,
		CanWaitRun:         true,
		CanAttachSession:   true,
		NativeVisibleInput: true,
	}, nil
}

func (a *queryHistoryAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: "codex", Verified: true}, nil
}

func (a *queryHistoryAdapter) ListSessions(context.Context) ([]pluginruntime.NativeSessionHint, error) {
	return []pluginruntime.NativeSessionHint{{
		PluginID:            "codex",
		NativeSessionID:     "native-conv-1",
		NativeThreadID:      "native-conv-1",
		PrismConversationID: "conv-query-1",
		Cwd:                 "/tmp/prism-query-test",
		Title:               "Stub Query Session",
		Visible:             true,
		Active:              true,
		LastActivityAt:      time.Now().UTC(),
		Metadata: map[string]string{
			"model": "gpt-test",
		},
	}}, nil
}

func (a *queryHistoryAdapter) AttachSession(context.Context, pluginruntime.AttachSessionRequest) (pluginruntime.NativeSession, error) {
	return pluginruntime.NativeSession{PluginID: "codex", NativeSessionID: "native-conv-1", NativeThreadID: "native-conv-1", Visible: true}, nil
}

func (a *queryHistoryAdapter) ReadHistory(context.Context, pluginruntime.NativeSession, int) ([]pluginruntime.HistoryMessage, error) {
	now := time.Now().UTC()
	return []pluginruntime.HistoryMessage{
		{ID: "user:req-1", Role: "user", Type: "text", Content: "你好", CreatedAt: now, UpdatedAt: now, Metadata: map[string]any{
			"safe":              "value",
			"native_session_id": "native-conv-1",
			"nested":            map[string]any{"thread_id": "native-thread-1"},
		}},
		{ID: "assistant:req-1", Role: "assistant", Type: "text", Content: "你好，我在。", CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)},
	}, nil
}

func (a *queryHistoryAdapter) ReadHistoryStream(_ context.Context, _ pluginruntime.NativeSession, req pluginruntime.HistoryStreamRequest) (<-chan pluginruntime.HistoryStreamEvent, error) {
	ch := make(chan pluginruntime.HistoryStreamEvent, 2)
	ch <- pluginruntime.HistoryStreamEvent{StreamID: req.StreamID, Type: "turn", Source: "initial", Operation: "append", Turn: &pluginruntime.HistoryTurn{
		TurnID: "turn-query-1", OrderKey: "1", Revision: 1, Messages: []pluginruntime.HistoryMessage{{ID: "user-1", Role: "user", Content: "hello"}, {ID: "assistant-1", Role: "assistant", Content: "world"}},
	}}
	ch <- pluginruntime.HistoryStreamEvent{StreamID: req.StreamID, Type: "end"}
	close(ch)
	return ch, nil
}

func (a *queryHistoryAdapter) ReadStatus(context.Context, pluginruntime.NativeSession, string) (pluginruntime.RuntimeStatus, error) {
	a.readStatusCalls++
	return pluginruntime.RuntimeStatus{
		Available:   true,
		Active:      true,
		Status:      "running",
		TurnID:      "turn-runtime-1",
		StartedAt:   time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano),
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		DurationMs:  2000,
		Preview:     "Codex 正在回复…",
		ProcessText: "开始：开始处理这条消息",
		Model: map[string]any{
			"displayName": "GPT-5.4",
		},
		ReasoningMode: map[string]any{
			"displayName": "高",
		},
		Context: map[string]any{
			"percent": 18,
		},
	}, nil
}

func (a *queryHistoryAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, nil
}

func (a *queryHistoryAdapter) Subscribe(context.Context, pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	ch := make(chan pluginruntime.PluginEvent)
	close(ch)
	return ch, nil
}

func (a *queryHistoryAdapter) Interrupt(context.Context, pluginruntime.NativeSession, string) error {
	return nil
}

func (a *queryHistoryAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{Visible: true}, nil
}

func (a *queryHistoryAdapter) WaitForRun(context.Context, pluginruntime.NativeSession, string) (pluginruntime.PluginEvent, error) {
	return pluginruntime.PluginEvent{Status: StageCompleted}, nil
}

func (a *queryHistoryAdapter) ControlSession(_ context.Context, req pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	a.controlCalls++
	scope := ""
	if req.Metadata != nil {
		scope = req.Metadata["control_scope"]
	}
	details := map[string]any{
		"supported": true,
	}
	switch scope {
	case "model":
		details["model_options"] = []map[string]any{{"id": "gpt-test", "label": "GPT Test", "available": true}}
		details["current_model"] = map[string]any{"id": "gpt-test", "label": "GPT Test"}
	case "reasoning":
		details["reasoning_options"] = []map[string]any{{"id": "high", "label": "高", "available": true}}
		details["current_reasoning"] = map[string]any{"id": "high", "label": "高"}
	case "approval":
		details["permission_options"] = []map[string]any{{"id": "on-request", "label": "请求批准", "available": true}}
		details["current_permission"] = map[string]any{"id": "on-request", "label": "请求批准"}
	case "actions":
		details["actions"] = []map[string]any{{"id": "pin", "label": "置顶会话", "available": true}}
	default:
		details["actions"] = []map[string]any{{"id": "pin", "label": "置顶会话", "available": true}}
		details["model_options"] = []map[string]any{{"id": "gpt-test", "label": "GPT Test", "available": true}}
		details["reasoning_options"] = []map[string]any{{"id": "high", "label": "高", "available": true}}
		details["permission_options"] = []map[string]any{{"id": "on-request", "label": "请求批准", "available": true}}
	}
	return pluginruntime.SessionControlResult{
		OK:       true,
		Action:   req.Action,
		ThreadID: req.Session.NativeThreadID,
		Details:  details,
	}, nil
}

func (a *queryHistoryAdapter) Close(context.Context) error { return nil }

type countingQueryHistoryAdapter struct {
	queryHistoryAdapter
	listCalls int
}

func (a *countingQueryHistoryAdapter) ListSessions(ctx context.Context) ([]pluginruntime.NativeSessionHint, error) {
	a.listCalls++
	return a.queryHistoryAdapter.ListSessions(ctx)
}

type queryCachedSignatureAdapter struct {
	queryHistoryAdapter
	historyCalls int
}

func (a *queryCachedSignatureAdapter) ReadHistory(context.Context, pluginruntime.NativeSession, int) ([]pluginruntime.HistoryMessage, error) {
	a.historyCalls++
	return []pluginruntime.HistoryMessage{
		{ID: "assistant-1", Role: "assistant", Type: "text", Content: "hello", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()},
	}, nil
}
