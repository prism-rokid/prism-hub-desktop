package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

func bindExistingConversation(t *testing.T, db *bridgedb.DB, pluginID, conversationID string) bridgedb.SessionRecord {
	t.Helper()
	record, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: conversationID,
		PluginID:            pluginID,
		NativeSessionID:     "native-" + conversationID,
		NativeThreadID:      "thread-" + conversationID,
		Surface:             "test",
		Endpoint:            "test://plugin",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	return record
}

func TestRouterStartsCanonicalConversationFromFirstMessage(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
	}
	in := prismtransport.Inbound{
		MessageID:   "request-start-1",
		MessageType: prismtransport.MsgConversationStart,
		Payload: &prismtransport.InboundPayload{
			RequestID:       "request-start-1",
			ClientMessageID: "mobile-message-1",
			PluginID:        "openclaw",
			Text:            "start from mobile",
			Cwd:             "/tmp/project",
		},
	}
	result, err := router.HandleConversationStart(context.Background(), in)
	if err != nil {
		t.Fatalf("HandleConversationStart: %v", err)
	}
	if !result.Visible || !strings.HasPrefix(result.ConversationID, "conv_") || result.ConversationID == "mobile-message-1" {
		t.Fatalf("expected a visible canonical conversation, got %+v", result)
	}
	if adapter.startCount() != 1 {
		t.Fatalf("expected one native start, got %d", adapter.startCount())
	}
	record, err := db.FindSession(context.Background(), result.ConversationID, "openclaw")
	if err != nil || record.NativeSessionID == "" || record.NativeThreadID == "" {
		t.Fatalf("expected durable native binding, record=%+v err=%v", record, err)
	}
	receipt, err := db.FindMessageReceipt(context.Background(), "openclaw", "mobile-message-1")
	if err != nil || !receipt.Visible || receipt.PrismConversationID != result.ConversationID {
		t.Fatalf("expected visible first-message receipt, receipt=%+v err=%v", receipt, err)
	}

	duplicate, err := router.HandleConversationStart(context.Background(), in)
	if err != nil {
		t.Fatalf("HandleConversationStart duplicate: %v", err)
	}
	if !duplicate.Duplicate || duplicate.ConversationID != result.ConversationID || adapter.startCount() != 1 {
		t.Fatalf("expected duplicate to reuse completed canonical binding, result=%+v starts=%d", duplicate, adapter.startCount())
	}
}

func TestRouterConcurrentConversationStartCallsNativeOnce(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
	}
	in := prismtransport.Inbound{
		MessageID:   "request-concurrent-start",
		MessageType: prismtransport.MsgConversationStart,
		Payload: &prismtransport.InboundPayload{
			RequestID:       "request-concurrent-start",
			ClientMessageID: "mobile-concurrent-first-message",
			PluginID:        "openclaw",
			Text:            "start exactly once",
		},
	}

	const workers = 50
	type result struct {
		value RouteResult
		err   error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value, startErr := router.HandleConversationStart(context.Background(), in)
			results <- result{value: value, err: startErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	canonicalID := ""
	for item := range results {
		if item.err != nil {
			t.Fatalf("concurrent conversation.start: %v", item.err)
		}
		if canonicalID == "" {
			canonicalID = item.value.ConversationID
		}
		if item.value.ConversationID != canonicalID || !item.value.Visible {
			t.Fatalf("concurrent start did not converge: %+v", item.value)
		}
	}
	if adapter.startCount() != 1 {
		t.Fatalf("same client_message_id started native conversation %d times", adapter.startCount())
	}
	sessions, err := db.ListSessions(context.Background(), "openclaw")
	if err != nil || len(sessions) != 1 || sessions[0].PrismConversationID != canonicalID {
		t.Fatalf("expected one canonical binding, sessions=%+v err=%v", sessions, err)
	}
}

func TestRouterDraftLifecycleNeverBindsDraftID(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	router := &Router{Service: pluginruntime.New(adapter), DB: db, Config: remoteAuthorizedConfig()}
	opened, err := router.HandleConversationDraftOpen(context.Background(), prismtransport.Inbound{
		MessageID: "draft-open-1", MessageType: prismtransport.MsgConversationDraftOpen,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", Cwd: "/tmp/project"},
	})
	if err != nil || !strings.HasPrefix(opened.DraftID, "draft_") || opened.ConversationID != "" {
		t.Fatalf("unexpected draft open result=%+v err=%v", opened, err)
	}
	if sessions, err := db.ListSessions(context.Background(), "openclaw"); err != nil || len(sessions) != 0 {
		t.Fatalf("draft open must not create a binding, sessions=%+v err=%v", sessions, err)
	}
	router.draftMu.Lock()
	record := router.drafts[opened.DraftID]
	router.draftMu.Unlock()
	if record.DraftFingerprint != "test-draft-fingerprint" {
		t.Fatalf("draft fingerprint must stay only in the transient Hub record, got %+v", record)
	}
	started, err := router.HandleConversationDraftStart(context.Background(), prismtransport.Inbound{
		MessageID: "draft-start-1", MessageType: prismtransport.MsgConversationDraftStart,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", DraftID: opened.DraftID, ClientMessageID: "draft-message-1", Text: "hello"},
	})
	if err != nil || !strings.HasPrefix(started.ConversationID, "conv_") || started.DraftID != opened.DraftID {
		t.Fatalf("unexpected draft start result=%+v err=%v", started, err)
	}
	if _, err := db.FindSession(context.Background(), started.ConversationID, "openclaw"); err != nil {
		t.Fatalf("expected canonical binding after draft first send: %v", err)
	}
}

func TestRouterDraftControlMarksStaleAndAllowsOnlyReopen(t *testing.T) {
	adapter := &recordingAdapter{id: "openclaw", visible: true, draftControlErr: draftStaleError{}}
	router := &Router{Service: pluginruntime.New(adapter), Config: remoteAuthorizedConfig()}
	opened, err := router.HandleConversationDraftOpen(context.Background(), prismtransport.Inbound{
		MessageID: "draft-open-2", MessageType: prismtransport.MsgConversationDraftOpen,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw"},
	})
	if err != nil {
		t.Fatalf("open draft: %v", err)
	}
	_, err = router.HandleConversationDraftControl(context.Background(), prismtransport.Inbound{
		MessageID: "draft-control-1", MessageType: prismtransport.MsgConversationDraftControl,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", DraftID: opened.DraftID, Action: "model.switch", Target: map[string]any{"option_id": "model"}},
	})
	if err == nil || !strings.Contains(err.Error(), "draft_stale") {
		t.Fatalf("expected stale draft control error, got %v", err)
	}
	reopened, err := router.HandleConversationDraftReopen(context.Background(), prismtransport.Inbound{
		MessageID: "draft-reopen-1", MessageType: prismtransport.MsgConversationDraftReopen,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", DraftID: opened.DraftID},
	})
	if err != nil || reopened.DraftID == opened.DraftID || reopened.DraftState != "open" {
		t.Fatalf("expected a fresh reopened draft, result=%+v err=%v", reopened, err)
	}
}

func TestRouterDraftMenuDescribeOnlyMarksExplicitlyStaleErrors(t *testing.T) {
	tests := []struct {
		name       string
		controlErr error
		stale      bool
	}{
		{name: "menu target changed", controlErr: errors.New("control_target_stale: intelligence menu has no readable groups")},
		{name: "draft left foreground", controlErr: draftStaleError{}, stale: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &recordingAdapter{id: "codex", visible: true, draftControlErr: tt.controlErr}
			router := &Router{Service: pluginruntime.New(adapter), Config: remoteAuthorizedConfig()}
			opened, err := router.HandleConversationDraftOpen(context.Background(), prismtransport.Inbound{
				MessageID: "draft-open-menu-" + tt.name, MessageType: prismtransport.MsgConversationDraftOpen,
				Payload: &prismtransport.InboundPayload{PluginID: "codex"},
			})
			if err != nil {
				t.Fatalf("open draft: %v", err)
			}
			router.draftMu.Lock()
			record := router.drafts[opened.DraftID]
			record.Controls = map[string]any{"interactive_controls": []any{map[string]any{"control_id": "codex.intelligence", "menu_available": true}}}
			router.drafts[opened.DraftID] = record
			router.draftMu.Unlock()

			_, err = router.HandleConversationDraftMenuSessionDescribe(context.Background(), prismtransport.Inbound{
				MessageID: "draft-menu-describe-" + tt.name, MessageType: prismtransport.MsgConversationDraftMenuSessionDescribe,
				Payload: &prismtransport.InboundPayload{PluginID: "codex", DraftID: opened.DraftID, Owner: map[string]any{"kind": "interactive_control", "control_id": "codex.intelligence"}},
			})
			if err == nil {
				t.Fatal("expected plugin menu describe failure")
			}
			if got := strings.Contains(strings.ToLower(err.Error()), "draft_stale"); got != tt.stale {
				t.Fatalf("draft stale classification=%v, want %v (err=%v)", got, tt.stale, err)
			}
			_, current, lookupErr := router.draftForInbound(prismtransport.Inbound{Payload: &prismtransport.InboundPayload{DraftID: opened.DraftID}})
			if (tt.stale && (lookupErr == nil || current.State != "draft_stale")) || (!tt.stale && (lookupErr != nil || current.State != "open")) {
				t.Fatalf("draft state after menu failure=%+v err=%v, stale=%v", current, lookupErr, tt.stale)
			}
		})
	}
}

func TestRouterDraftWatchOnlyEmitsMatchingTransientStaleEvent(t *testing.T) {
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	router := &Router{Service: pluginruntime.New(adapter), Config: remoteAuthorizedConfig(), Sink: sink}
	opened, err := router.HandleConversationDraftOpen(context.Background(), prismtransport.Inbound{
		MessageID: "draft-open-watch", MessageType: prismtransport.MsgConversationDraftOpen,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw"},
	})
	if err != nil {
		t.Fatalf("open draft: %v", err)
	}
	watch, err := router.HandleConversationDraftWatch(context.Background(), prismtransport.Inbound{
		MessageID: "draft-watch", MessageType: prismtransport.MsgConversationDraftWatch,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", DraftID: opened.DraftID, DraftStreamID: "draft-stream-1"},
	})
	if err != nil || watch.DraftState != "open" || watch.Controls == nil {
		t.Fatalf("watch result=%+v err=%v", watch, err)
	}
	router.MarkPluginDraftStale("openclaw", opened.DraftID, "desktop foreground changed")
	if got := draftEvents(sink); len(got) != 1 || got[0].DraftID != opened.DraftID || got[0].DraftStreamID != "draft-stream-1" || got[0].EventType != "draft.stale" {
		t.Fatalf("unexpected draft events: %+v", got)
	}
	if _, err := router.HandleConversationDraftWatch(context.Background(), prismtransport.Inbound{
		MessageID: "draft-watch-stale", MessageType: prismtransport.MsgConversationDraftWatch,
		Payload: &prismtransport.InboundPayload{PluginID: "openclaw", DraftID: opened.DraftID, DraftStreamID: "draft-stream-2"},
	}); err == nil || !strings.Contains(err.Error(), "draft_stale") {
		t.Fatalf("watch must reject stale draft, err=%v", err)
	}
}

func TestRouterRejectsIncompleteConversationStart(t *testing.T) {
	tests := []struct {
		name   string
		result pluginruntime.StartSessionWithMessageResult
		match  string
	}{
		{
			name: "native identity missing",
			result: pluginruntime.StartSessionWithMessageResult{
				Receipt:    pluginruntime.SendReceipt{Accepted: true, Visible: true},
				Visibility: pluginruntime.VisibilityResult{Visible: true},
			},
			match: "no native identity",
		},
		{
			name: "first message rejected",
			result: pluginruntime.StartSessionWithMessageResult{
				Session:    pluginruntime.NativeSession{NativeSessionID: "native-start", Visible: true},
				Receipt:    pluginruntime.SendReceipt{Accepted: false, Visible: true},
				Visibility: pluginruntime.VisibilityResult{Visible: true},
			},
			match: "did not accept",
		},
		{
			name: "first message not visible",
			result: pluginruntime.StartSessionWithMessageResult{
				Session:    pluginruntime.NativeSession{NativeSessionID: "native-start", Visible: true},
				Receipt:    pluginruntime.SendReceipt{Accepted: true, Visible: false, Detail: "not rendered"},
				Visibility: pluginruntime.VisibilityResult{Visible: true},
			},
			match: "did not confirm first-message visibility",
		},
		{
			name: "visibility check failed",
			result: pluginruntime.StartSessionWithMessageResult{
				Session:    pluginruntime.NativeSession{NativeSessionID: "native-start", Visible: true},
				Receipt:    pluginruntime.SendReceipt{Accepted: true, Visible: true},
				Visibility: pluginruntime.VisibilityResult{Visible: false, FailureReason: "not rendered"},
			},
			match: "not visible",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
			if err != nil {
				t.Fatalf("Open db: %v", err)
			}
			defer db.Close()
			adapter := &recordingAdapter{id: "openclaw", visible: true, startResult: &tt.result}
			router := &Router{Service: pluginruntime.New(adapter), DB: db, Config: remoteAuthorizedConfig()}
			_, err = router.HandleConversationStart(context.Background(), prismtransport.Inbound{
				MessageID:   "request-start-" + strings.ReplaceAll(tt.name, " ", "-"),
				MessageType: prismtransport.MsgConversationStart,
				Payload: &prismtransport.InboundPayload{
					ClientMessageID: "mobile-message-" + strings.ReplaceAll(tt.name, " ", "-"),
					PluginID:        "openclaw",
					Text:            "first message",
				},
			})
			if err == nil || !strings.Contains(err.Error(), tt.match) {
				t.Fatalf("expected %q rejection, got %v", tt.match, err)
			}
			if sessions, listErr := db.ListSessions(context.Background(), "openclaw"); listErr != nil || len(sessions) != 0 {
				t.Fatalf("rejected start must not create a binding, sessions=%+v err=%v", sessions, listErr)
			}
		})
	}
}

func TestRouterRejectsConversationStartWithConversationID(t *testing.T) {
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	router := &Router{Service: pluginruntime.New(adapter), Config: remoteAuthorizedConfig()}
	_, err := router.HandleConversationStart(context.Background(), prismtransport.Inbound{
		MessageType: prismtransport.MsgConversationStart,
		Payload: &prismtransport.InboundPayload{
			ClientMessageID: "mobile-message-1",
			PluginID:        "openclaw",
			ConversationID:  "conv_existing",
			Text:            "must not continue through start",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "must not include conversation_id") || adapter.startCount() != 0 {
		t.Fatalf("expected start identity rejection before native call, err=%v starts=%d", err, adapter.startCount())
	}
}

func TestRouterRoutesChatSendAndDedupesMessageID(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "openclaw", "conv-1")
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	in := prismtransport.Inbound{
		MessageID:   "msg-1",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "hello openclaw",
			PluginID:       "openclaw",
			ConversationID: "conv-1",
			Metadata:       &prismtransport.InboundMetadata{SourceDevice: "glasses"},
		},
	}
	result, err := router.HandleInbound(context.Background(), in)
	if err != nil {
		t.Fatalf("HandleInbound first: %v", err)
	}
	if result.Duplicate || adapter.sendCount() != 1 || !result.Visible {
		t.Fatalf("unexpected first route result=%+v sendCount=%d", result, adapter.sendCount())
	}
	again, err := router.HandleInbound(context.Background(), in)
	if err != nil {
		t.Fatalf("HandleInbound duplicate: %v", err)
	}
	if !again.Duplicate {
		t.Fatalf("expected duplicate result: %+v", again)
	}
	if adapter.sendCount() != 1 {
		t.Fatalf("expected no second native send, got %d", adapter.sendCount())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !containsAll(sink.eventStatuses(), StageCompleted) {
		time.Sleep(10 * time.Millisecond)
	}
	events := sink.eventStatuses()
	if !containsAll(events, StageReceived, StageResolving, StageNativeSessionReady, StageAccepted, StageVisible, StageDuplicate, StageCompleted) {
		t.Fatalf("missing expected task events: %+v", events)
	}
	if summaries := sink.eventSummaries(); !containsString(summaries, "openclaw streaming") {
		t.Fatalf("expected streaming summary, got %+v", summaries)
	}
	if ids := sink.eventRequestIDs(); !containsAll(ids, "msg-1") {
		t.Fatalf("expected task events to carry request_id=msg-1, got %+v", ids)
	}
	if finals := sink.finalContents(); len(finals) != 1 || !strings.Contains(finals[0], "openclaw") {
		t.Fatalf("unexpected final summaries: %+v", finals)
	}
	if ids := sink.finalRequestIDs(); !containsAll(ids, "msg-1") {
		t.Fatalf("expected final to carry request_id=msg-1, got %+v", ids)
	}
}

func TestRouterPersistsSessionReceiptsAndEventsOnly(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "openclaw", "conv-persist-1")
	adapter := &recordingAdapter{
		id:      "openclaw",
		visible: true,
		subscribeEvents: []pluginruntime.PluginEvent{
			{ID: "delta-1", Type: "message.delta", Status: StageRunning, Summary: "streaming", Payload: map[string]any{"text": "Hello "}},
		},
	}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-persist-1",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "hello persistence",
			PluginID:       "openclaw",
			ConversationID: "conv-persist-1",
			Cwd:            t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	session, err := db.FindSession(context.Background(), "conv-persist-1", "openclaw")
	if err != nil {
		t.Fatalf("FindSession: %v", err)
	}
	if session.PluginID != "openclaw" || session.NativeSessionID == "" {
		t.Fatalf("unexpected session record: %+v", session)
	}
	receipt, err := db.FindMessageReceipt(context.Background(), "openclaw", "msg-persist-1")
	if err != nil {
		t.Fatalf("FindMessageReceipt: %v", err)
	}
	if receipt.PluginID != "openclaw" || receipt.PrismConversationID != "conv-persist-1" || receipt.NativeMessageID == "" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	events, err := db.ListRecentEvents(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected persisted bridge events")
	}
}

func TestRouterEmitsStructuredConversationEventsForForwardFlow(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "openclaw", "conv-forward-events")
	adapter := &recordingAdapter{
		id:      "openclaw",
		visible: true,
		subscribeEvents: []pluginruntime.PluginEvent{
			{
				ID:        "delta-forward-1",
				Type:      "message.delta",
				Status:    StageRunning,
				Summary:   "streaming",
				CreatedAt: time.Now().UTC(),
				Payload:   map[string]any{"delta": "Hello "},
			},
		},
	}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-forward-events",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "hello structured flow",
			PluginID:       "openclaw",
			ConversationID: "conv-forward-events",
			Metadata:       &prismtransport.InboundMetadata{SourceDevice: "glasses"},
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events := sink.conversationEvents()
		if findConversationEvent(events, "run.completed") != nil && findConversationEvent(events, "message.user.accepted") != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	events := sink.conversationEvents()
	if len(events) == 0 {
		t.Fatal("expected structured conversation events")
	}
	accepted := findConversationEvent(events, "message.user.accepted")
	if accepted == nil {
		t.Fatalf("expected message.user.accepted event, got %+v", events)
	}
	if accepted.ConversationID != "conv-forward-events" || accepted.PluginID != "openclaw" || accepted.SourceDevice != "glasses" {
		t.Fatalf("unexpected accepted event metadata: %+v", accepted)
	}
	if accepted.Payload["schema_version"] != "conversation.v1" || accepted.Payload["bound"] != true || accepted.Payload["request_id"] != "msg-forward-events" {
		t.Fatalf("unexpected accepted envelope: %+v", accepted.Payload)
	}
	acceptedMessage, ok := accepted.Payload["message"].(map[string]any)
	if !ok || acceptedMessage["role"] != "user" || acceptedMessage["text"] != "hello structured flow" {
		t.Fatalf("unexpected accepted message payload: %+v", accepted.Payload["message"])
	}
	visible := findConversationEvent(events, "run.visible")
	if visible == nil || visible.Status != StageVisible {
		t.Fatalf("expected visible conversation event, got %+v", events)
	}
	if _, ok := visible.Payload["message"]; ok {
		t.Fatalf("visible event should not duplicate message payload: %+v", visible.Payload)
	}
	session, ok := accepted.Payload["session"].(map[string]any)
	if !ok || session["id"] != "conv-forward-events" {
		t.Fatalf("expected canonical session id in event payload, got %+v", accepted.Payload["session"])
	}
	encoded, err := json.Marshal(accepted)
	if err != nil || strings.Contains(string(encoded), "native-") || strings.Contains(string(encoded), "thread-") {
		t.Fatalf("realtime conversation event leaked native identity: %s (%v)", encoded, err)
	}
	delta := findConversationEvent(events, "message.delta")
	if delta == nil {
		t.Fatalf("expected assistant delta event, got %+v", events)
	}
	deltaMessage, ok := delta.Payload["message"].(map[string]any)
	if !ok || deltaMessage["role"] != "assistant" || deltaMessage["delta"] != "Hello" {
		t.Fatalf("unexpected delta payload: %+v", delta.Payload["message"])
	}
	completed := findConversationEvent(events, "run.completed")
	if completed == nil || completed.Status != StageCompleted {
		t.Fatalf("expected completed event, got %+v", events)
	}
	completedMessage, ok := completed.Payload["message"].(map[string]any)
	if !ok || !strings.Contains(stringFromAny(completedMessage["text"]), "openclaw completed") {
		t.Fatalf("unexpected completed message payload: %+v", completed.Payload["message"])
	}
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("ordinary conversation events must not synthesize detail snapshots: %+v", snapshots)
	}
}

func TestRouterWaitStopsAfterTerminalCompletionEvent(t *testing.T) {
	adapter := &recordingAdapter{id: "codex"}
	router := &Router{
		Service: pluginruntime.New(adapter),
		Config:  remoteAuthorizedConfig("codex"),
		Sink:    &recordingSink{},
	}
	session := pluginruntime.NativeSession{PluginID: "codex"}

	terminal := pluginruntime.PluginEvent{
		ID: "codex-task:thread-qa:turn-1:completed", Type: "run.completed", Status: StageCompleted,
	}
	if !router.handleNativeEvent("", "request-1", "conv-qa", "codex", "task-1", "plugin-desktop", session, "run-1", terminal) {
		t.Fatal("terminal run.completed event should stop the concurrent WaitForRun path")
	}

	projection := pluginruntime.PluginEvent{
		ID: "desktop-state-qa", Type: "desktop.state.changed", Status: StageCompleted,
	}
	if router.handleNativeEvent("", "request-1", "conv-qa", "codex", "task-1", "plugin-desktop", session, "run-1", projection) {
		t.Fatal("desktop state projection must not stop waiting for the lifecycle terminal event")
	}
}

func TestRouterReusesExistingSessionViaAttachWhenSupported(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-reuse",
		PluginID:            "openclaw",
		NativeSessionID:     "native-existing",
		NativeThreadID:      "thread-existing",
		Surface:             "test",
		Endpoint:            "test://plugin",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	result, err := router.HandleInbound(ctx, prismtransport.Inbound{
		MessageID:   "msg-reuse",
		MessageType: prismtransport.MsgChatSend,
		Payload: &prismtransport.InboundPayload{
			Text:           "continue",
			PluginID:       "openclaw",
			ConversationID: "conv-reuse",
			Cwd:            "/tmp/project",
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	if !result.SessionReused {
		t.Fatalf("expected session reuse, got %+v", result)
	}
	if adapter.attachCount() != 1 {
		t.Fatalf("expected attach path only, attach=%d", adapter.attachCount())
	}
	if result.Session.NativeSessionID != "native-existing" || result.Session.NativeThreadID != "thread-existing" {
		t.Fatalf("expected attached session ids, got %+v", result.Session)
	}
	if !containsString(sink.eventSummaries(), "已连接已绑定") {
		t.Fatalf("expected bound-session summary, got %+v", sink.eventSummaries())
	}
	if payloads := sink.taskEvents(); len(payloads) == 0 || !containsReusedReadyEvent(payloads) {
		t.Fatalf("expected ready event for a reused canonical binding, got %+v", payloads)
	}
}

func TestRouterLeavesCwdEmptyWhenMissing(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "openclaw", "conv-default-cwd")
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-default-cwd",
		MessageType: prismtransport.MsgChatSend,
		Payload: &prismtransport.InboundPayload{
			Text:           "hello",
			PluginID:       "openclaw",
			ConversationID: "conv-default-cwd",
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	if got := adapter.lastAttachReq.Cwd; got != "" {
		t.Fatalf("expected empty bound cwd when inbound cwd is missing, got %q", got)
	}
}

func TestRouterRejectsNonVisibleAdapterWithoutFallbackSend(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "openclaw", visible: false}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-not-visible",
		MessageType: prismtransport.MsgChatSend,
		Payload: &prismtransport.InboundPayload{
			Text:           "hello",
			PluginID:       "openclaw",
			ConversationID: "conv-2",
		},
	})
	if err == nil {
		t.Fatal("expected non-visible adapter error")
	}
	if adapter.sendCount() != 0 {
		t.Fatalf("router should not send when adapter is invisible: send=%d", adapter.sendCount())
	}
	if got := sink.eventStatuses(); !containsAll(got, StageReceived, StageResolving, StageFailed) {
		t.Fatalf("expected failed task event, got %+v", got)
	}
	if errors := sink.errorContents(); len(errors) != 1 || !strings.Contains(errors[0], "unavailable") {
		t.Fatalf("unexpected chat errors: %+v", errors)
	}
	if ids := sink.errorRequestIDs(); !containsAll(ids, "msg-not-visible") {
		t.Fatalf("expected error to carry request_id=msg-not-visible, got %+v", ids)
	}
	failed := findConversationEvent(sink.conversationEvents(), "conversation.send_failed")
	if failed == nil {
		t.Fatalf("expected send failure event, got %+v", sink.conversationEvents())
	}
	failure, _ := failed.Payload["send_failure"].(map[string]any)
	if failure["request_id"] != "msg-not-visible" || failure["message_id"] != "msg-not-visible" || failure["code"] != "send_failed" {
		t.Fatalf("unexpected send failure payload: %+v", failure)
	}
}

type erroringSendAdapter struct {
	*recordingAdapter
	err error
}

func (a *erroringSendAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, a.err
}

func TestRouterSendFailureOnlyExposesAllowlistedStructuredCode(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{"structured indeterminate", &pluginruntime.AdapterError{Code: "delivery_indeterminate", Message: "write committed before crash"}, "delivery_indeterminate"},
		{"structured trust requirement", &pluginruntime.AdapterError{Code: "workspace_trust_required", Message: "local trust needed"}, "workspace_trust_required"},
		{"legacy owner still active", &pluginruntime.AdapterError{Code: "legacy_session_active", Message: "close old process"}, "legacy_session_active"},
		{"message spoof", errors.New("delivery_indeterminate: try again"), "send_failed"},
		{"other structured code", &pluginruntime.AdapterError{Code: "secret_internal_code", Message: "internal failure"}, "send_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			bindExistingConversation(t, db, "openclaw", "conv-send-error")
			sink := &recordingSink{}
			router := &Router{Service: pluginruntime.New(&erroringSendAdapter{
				recordingAdapter: &recordingAdapter{id: "openclaw", visible: true}, err: test.err,
			}), DB: db, Config: remoteAuthorizedConfig(), Sink: sink}
			_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
				MessageID: "msg-send-error", MessageType: prismtransport.MsgChatSend,
				Payload: &prismtransport.InboundPayload{PluginID: "openclaw", ConversationID: "conv-send-error", Text: "hello"},
			})
			if err == nil {
				t.Fatal("expected send failure")
			}
			event := findConversationEvent(sink.conversationEvents(), "conversation.send_failed")
			if event == nil || asMap(event.Payload["send_failure"])["code"] != test.want {
				t.Fatalf("send failure code = %#v, want %s", event, test.want)
			}
		})
	}
}

func TestRouterRejectsDisabledAgentWithoutProbeOrSend(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  cfg,
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-disabled",
		MessageType: prismtransport.MsgChatSend,
		Payload: &prismtransport.InboundPayload{
			Text:           "hello",
			PluginID:       "openclaw",
			ConversationID: "conv-disabled",
		},
	})
	if err == nil {
		t.Fatal("expected remote-disabled plugin error")
	}
	if adapter.probeCount() != 0 || adapter.sendCount() != 0 {
		t.Fatalf("router should stop before probe/send when remote access is disabled: probe=%d send=%d",
			adapter.probeCount(), adapter.sendCount())
	}
	if errors := sink.errorContents(); len(errors) != 1 || !strings.Contains(errors[0], "remote access disabled") {
		t.Fatalf("unexpected chat errors: %+v", errors)
	}
}

func TestRouterRejectsRemoteDisabledControlRoutes(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-disabled-control",
		PluginID:            "codex",
		NativeSessionID:     "native-disabled-control",
		NativeThreadID:      "thread-disabled-control",
		Surface:             "test",
		Endpoint:            "test://plugin",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{id: "codex", visible: true, canApproval: true}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  cfg,
		Sink:    &recordingSink{},
	}

	tests := []struct {
		name   string
		handle func() error
	}{
		{
			name: "interrupt",
			handle: func() error {
				_, err := router.HandleInterrupt(ctx, prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-disabled-control", TaskID: "task-disabled",
				}})
				return err
			},
		},
		{
			name: "approval resolve",
			handle: func() error {
				_, err := router.HandleApprovalResolve(ctx, prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-disabled-control", TaskID: "task-disabled",
					ApprovalRequestID: "approval-disabled", ActionID: "approve",
				}})
				return err
			},
		},
		{
			name: "session control",
			handle: func() error {
				_, err := router.HandleSessionControl(ctx, prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-disabled-control", TaskID: "task-disabled", Action: "conversation.select",
				}})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.handle(); err == nil || !strings.Contains(err.Error(), "remote access disabled") {
				t.Fatalf("expected remote-disabled route rejection, got %v", err)
			}
		})
	}
	if adapter.interruptCount() != 0 || adapter.approvalResolveCount() != 0 || adapter.controlCount() != 0 {
		t.Fatalf("disabled control routes reached adapter: interrupt=%d approval=%d control=%d", adapter.interruptCount(), adapter.approvalResolveCount(), adapter.controlCount())
	}
}

func TestRouterRejectsLiveControlsWhenForegroundWatcherUnavailable(t *testing.T) {
	adapter := &recordingAdapter{id: "codex", visible: true, canApproval: true}
	store := NewDetailSnapshotStore()
	store.MarkForegroundWatcherUnavailable("codex")
	router := &Router{
		Service:         pluginruntime.New(adapter),
		Config:          remoteAuthorizedConfig(),
		Sink:            &recordingSink{},
		DetailSnapshots: store,
	}

	tests := []struct {
		name   string
		handle func() error
	}{
		{
			name: "interrupt",
			handle: func() error {
				_, err := router.HandleInterrupt(context.Background(), prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-1", TaskID: "task-1",
				}})
				return err
			},
		},
		{
			name: "approval",
			handle: func() error {
				_, err := router.HandleApprovalResolve(context.Background(), prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-1", ApprovalRequestID: "approval-1", ActionID: "approve",
				}})
				return err
			},
		},
		{
			name: "model control",
			handle: func() error {
				_, err := router.HandleSessionControl(context.Background(), prismtransport.Inbound{Payload: &prismtransport.InboundPayload{
					PluginID: "codex", ConversationID: "conv-1", Action: "model.switch",
				}})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.handle()
			if err == nil || !strings.Contains(err.Error(), "foreground watcher unavailable") {
				t.Fatalf("expected watcher-unavailable rejection, got %v", err)
			}
		})
	}
	if adapter.interruptCount() != 0 || adapter.approvalResolveCount() != 0 || adapter.controlCount() != 0 {
		t.Fatalf("live control reached adapter while watcher unavailable: interrupt=%d approval=%d control=%d", adapter.interruptCount(), adapter.approvalResolveCount(), adapter.controlCount())
	}
	if err := router.requireForegroundWatcherAvailable("other"); err != nil {
		t.Fatalf("unrelated Plugin should remain available: %v", err)
	}
}

func TestRouterValidatesQueueItemMenuOwnerAgainstCurrentDetail(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-queue-menu",
		"desktop_foreground": true,
		"queue": map[string]any{
			"items": []any{
				map[string]any{"id": "native-queue-1", "has_more_actions": true},
				map[string]any{"id": "native-queue-2", "has_more_actions": false},
			},
		},
	})
	router := &Router{DetailSnapshots: store}

	if err := router.validateConversationMenuOwner("codex", "conv-queue-menu", map[string]any{
		"kind": "queue_item", "queue_item_id": "native-queue-1",
	}); err != nil {
		t.Fatalf("expected declared queue menu owner to validate: %v", err)
	}
	for _, owner := range []map[string]any{
		{"kind": "queue_item", "queue_item_id": "native-queue-2"},
		{"kind": "queue_item", "queue_item_id": "native-queue-missing"},
		{"kind": "queue_item"},
	} {
		if err := router.validateConversationMenuOwner("codex", "conv-queue-menu", owner); err == nil || !strings.Contains(err.Error(), "control_target_stale") {
			t.Fatalf("expected stale queue menu owner %v, got %v", owner, err)
		}
	}
}

func TestRouterValidatesConversationHeaderMenuOwnerAgainstCurrentDetail(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":             "conv-header-menu",
		"desktop_foreground":          true,
		"conversation_menu_available": true,
	})
	router := &Router{DetailSnapshots: store}
	owner := map[string]any{"kind": "conversation"}
	if err := router.validateConversationMenuOwner("codex", "conv-header-menu", owner); err != nil {
		t.Fatalf("expected declared conversation menu owner to validate: %v", err)
	}

	store.PutForRuntime("codex", map[string]any{
		"conversation_id":             "conv-header-menu",
		"desktop_foreground":          true,
		"conversation_menu_available": false,
	})
	if err := router.validateConversationMenuOwner("codex", "conv-header-menu", owner); err == nil || !strings.Contains(err.Error(), "control_target_stale") {
		t.Fatalf("expected unavailable conversation menu owner to be stale, got %v", err)
	}
}

func TestRouterConversationMenuSelectionRequiresConfirmedExactThread(t *testing.T) {
	session := pluginruntime.NativeSession{
		PluginID:        "codex",
		NativeSessionID: "thread-selection-target",
		NativeThreadID:  "thread-selection-target",
	}
	in := prismtransport.Inbound{MessageID: "conversation-menu-select-1"}

	tests := []struct {
		name    string
		result  pluginruntime.SessionControlResult
		wantErr string
	}{
		{
			name: "confirmed exact target",
			result: pluginruntime.SessionControlResult{
				OK: true, ThreadID: "thread-selection-target",
				Details: map[string]any{"selected": true},
			},
		},
		{
			name: "selection rejected",
			result: pluginruntime.SessionControlResult{
				OK: false, ThreadID: "thread-selection-target",
				Details: map[string]any{"selected": false, "reason": "thread_row_not_visible"},
			},
			wantErr: "thread_row_not_visible",
		},
		{
			name: "different thread selected",
			result: pluginruntime.SessionControlResult{
				OK: true, ThreadID: "another-thread",
				Details: map[string]any{"selected": true},
			},
			wantErr: "selected thread mismatch",
		},
		{
			name: "selection not confirmed",
			result: pluginruntime.SessionControlResult{
				OK: true, ThreadID: "thread-selection-target",
				Details: map[string]any{"selected": false},
			},
			wantErr: "did not confirm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &fixedSessionControlController{result: tt.result}
			router := &Router{}
			err := router.selectConversationForMenuSession(
				context.Background(), controller, session, "codex", "conv-selection-target", in,
			)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected confirmed selection, got %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
			if controller.request.Action != "conversation.select" {
				t.Fatalf("expected conversation.select request, got %+v", controller.request)
			}
		})
	}
}

func TestRouterRoutesQueueItemMenuSessionAsOpaqueCapability(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	const pluginID = "codex"
	const conversationID = "conv-queue-menu-session"
	bindExistingConversation(t, db, pluginID, conversationID)
	store := NewDetailSnapshotStore()
	store.PutForRuntime(pluginID, map[string]any{
		"conversation_id":    conversationID,
		"desktop_foreground": true,
		"queue": map[string]any{
			"items": []any{map[string]any{
				"id": "native-queue-1", "has_more_actions": true,
			}},
		},
	})
	adapter := &recordingAdapter{
		id:      pluginID,
		visible: true,
		controlResult: pluginruntime.SessionControlResult{
			OK: true,
			Details: map[string]any{
				"menu_session_id": "plugin-queue-menu-1",
				"entries": []any{map[string]any{
					"entry_id": "plugin-entry-1", "label": "Edit message", "available": true,
				}},
			},
		},
	}
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		DetailSnapshots: store,
	}
	described, err := router.HandleConversationMenuSessionDescribe(context.Background(), prismtransport.Inbound{
		MessageID: "queue-menu-describe-1",
		Payload: &prismtransport.InboundPayload{
			PluginID: pluginID, ConversationID: conversationID,
			Owner: map[string]any{"kind": "queue_item", "queue_item_id": "native-queue-1"},
		},
	})
	if err != nil {
		t.Fatalf("HandleConversationMenuSessionDescribe: %v", err)
	}
	if described.MenuSession == nil || described.MenuSession.ID == "" || described.MenuSession.ID == "plugin-queue-menu-1" {
		t.Fatalf("expected a Hub-owned public session id, result=%+v", described)
	}
	if described.MenuSession.Owner["queue_item_id"] != "native-queue-1" {
		t.Fatalf("expected queue item owner to stay bound, session=%+v", described.MenuSession)
	}
	if got := adapter.lastControlReq.Action; got != "menu_session.describe" {
		t.Fatalf("expected describe to reach Plugin, got action=%q", got)
	}
	adapter.controlResult = pluginruntime.SessionControlResult{OK: true}
	applied, err := router.HandleConversationMenuSessionApply(context.Background(), prismtransport.Inbound{
		MessageID: "queue-menu-apply-1",
		Payload: &prismtransport.InboundPayload{
			PluginID: pluginID, ConversationID: conversationID,
			MenuSessionID: described.MenuSession.ID, EntryID: "plugin-entry-1",
		},
	})
	if err != nil {
		t.Fatalf("HandleConversationMenuSessionApply: %v", err)
	}
	if applied.MenuSession == nil || applied.MenuSession.ID != described.MenuSession.ID {
		t.Fatalf("expected apply to retain public session binding, result=%+v", applied)
	}
	if got := applied.Controls; got != nil {
		t.Fatalf("expected queue menu apply without live controls, got=%#v", got)
	}
	request := adapter.lastControlReq
	target, ok := request.Target.(map[string]any)
	if request.Action != "menu_session.apply" || !ok || target["menu_session_id"] != "plugin-queue-menu-1" || target["entry_id"] != "plugin-entry-1" {
		t.Fatalf("expected opaque apply target to use Plugin session id, request=%+v", request)
	}
}

func TestRouterMenuSessionApplyPublishesConfirmedInteractiveControls(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	const pluginID = "codex"
	const conversationID = "conv-intelligence-menu-session"
	bindExistingConversation(t, db, pluginID, conversationID)
	store := NewDetailSnapshotStore()
	store.PutForRuntime(pluginID, map[string]any{
		"conversation_id":    conversationID,
		"desktop_foreground": true,
		"interactive_controls": []any{map[string]any{
			"control_id": "codex.intelligence", "display_label": "Model and reasoning", "current_value": "GPT-5", "menu_available": true,
		}},
	})
	adapter := &recordingAdapter{
		id: pluginID, visible: true,
		controlResult: pluginruntime.SessionControlResult{OK: true, Details: map[string]any{
			"menu_session_id": "plugin-intelligence-menu-1",
			"entries": []any{map[string]any{
				"entry_id": "plugin-entry-1", "label": "GPT-5.7", "available": true,
			}},
		}},
	}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter), DB: db, Config: remoteAuthorizedConfig(),
		Sink: sink, DetailSnapshots: store,
	}
	described, err := router.HandleConversationMenuSessionDescribe(context.Background(), prismtransport.Inbound{
		MessageID: "intelligence-menu-describe-1",
		Payload: &prismtransport.InboundPayload{PluginID: pluginID, ConversationID: conversationID,
			Owner: map[string]any{"kind": "interactive_control", "control_id": "codex.intelligence"}},
	})
	if err != nil {
		t.Fatalf("HandleConversationMenuSessionDescribe: %v", err)
	}
	adapter.controlResult = pluginruntime.SessionControlResult{OK: true, Details: map[string]any{
		"interactive_controls": []any{map[string]any{
			"control_id": "codex.intelligence", "display_label": "Model and reasoning", "current_value": "GPT-5.7",
		}},
	}}
	applied, err := router.HandleConversationMenuSessionApply(context.Background(), prismtransport.Inbound{
		MessageID: "intelligence-menu-apply-1",
		Payload: &prismtransport.InboundPayload{PluginID: pluginID, ConversationID: conversationID,
			MenuSessionID: described.MenuSession.ID, EntryID: "plugin-entry-1"},
	})
	if err != nil {
		t.Fatalf("HandleConversationMenuSessionApply: %v", err)
	}
	if applied.Controls == nil {
		t.Fatal("expected confirmed interactive controls in the apply response")
	}
	detail := waitForConversationDetailSnapshot(t, sink, conversationID, 2*time.Second)
	controls := listOfMapsValue(detail["interactive_controls"])
	if len(controls) != 1 || stringFromAny(controls[0]["current_value"]) != "GPT-5.7" {
		t.Fatalf("expected immediately published confirmed control, detail=%+v", detail)
	}
}

func TestRouterRunWaitTimeoutDoesNotFailVisibleMessage(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "openclaw", "conv-wait-timeout")
	adapter := &recordingAdapter{id: "openclaw", visible: true, waitErr: context.DeadlineExceeded}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	result, err := router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-wait-timeout",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "hello openclaw",
			PluginID:       "openclaw",
			ConversationID: "conv-wait-timeout",
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	if !result.Visible {
		t.Fatalf("expected visible route result: %+v", result)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !containsString(sink.finalContents(), "未在等待窗口内收到") {
		time.Sleep(10 * time.Millisecond)
	}
	if statuses := sink.eventStatuses(); !containsAll(statuses, StageVisible, StageRunning) || containsString(statuses, StageFailed) {
		t.Fatalf("unexpected task statuses after wait timeout: %+v", statuses)
	}
	if errors := sink.errorContents(); len(errors) != 0 {
		t.Fatalf("wait timeout should not emit chat error: %+v", errors)
	}
	if finals := sink.finalContents(); !containsString(finals, "未在等待窗口内收到") {
		t.Fatalf("expected wait timeout final, got %+v", finals)
	}
}

func TestEventHasTimeoutFallback(t *testing.T) {
	if !eventHasTimeoutFallback(pluginruntime.PluginEvent{Payload: map[string]any{"timeout_fallback": true}}) {
		t.Fatalf("expected bool timeout_fallback to be detected")
	}
	if !eventHasTimeoutFallback(pluginruntime.PluginEvent{Payload: map[string]any{"timeout_fallback": "true"}}) {
		t.Fatalf("expected string timeout_fallback to be detected")
	}
	if eventHasTimeoutFallback(pluginruntime.PluginEvent{Payload: map[string]any{"timeout_fallback": false}}) {
		t.Fatalf("did not expect false timeout_fallback to be detected")
	}
}

func TestRouterHandlesTaskInterruptForExistingSession(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		NativeSessionID:     "native-conv-1",
		NativeThreadID:      "thread-conv-1",
		Surface:             "test",
		Endpoint:            "test://plugin",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{id: "openclaw", visible: true}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	result, err := router.HandleInterrupt(ctx, prismtransport.Inbound{
		MessageID:   "interrupt-msg-1",
		MessageType: prismtransport.MsgTaskInterrupt,
		Payload: &prismtransport.InboundPayload{
			TaskID:         "bridge-task-msg-1",
			PluginID:       "openclaw",
			ConversationID: "conv-1",
			Reason:         "user requested stop",
			Metadata:       &prismtransport.InboundMetadata{SourceDevice: "glasses"},
		},
	})
	if err != nil {
		t.Fatalf("HandleInterrupt: %v", err)
	}
	if result.PluginID != "openclaw" || result.TaskID != "bridge-task-msg-1" {
		t.Fatalf("unexpected interrupt result: %+v", result)
	}
	if adapter.interruptCount() != 1 {
		t.Fatalf("expected one interrupt call, got %d", adapter.interruptCount())
	}
	if adapter.lastInterruptTaskID() != "bridge-task-msg-1" {
		t.Fatalf("expected interrupt task id bridge-task-msg-1, got %q", adapter.lastInterruptTaskID())
	}
	if statuses := sink.eventStatuses(); !containsAll(statuses, "interrupted") {
		t.Fatalf("expected interrupted task event, got %+v", statuses)
	}
	if summaries := sink.eventSummaries(); !containsString(summaries, "user requested stop") {
		t.Fatalf("expected interrupt summary, got %+v", summaries)
	}
}

func TestValidateInteractiveSurfaceActionAllowsOptionalInput(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-rename",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"interactive_surface": map[string]any{
			"surface_id": "surface-rename",
			"actions": []map[string]any{{
				"action_id": "save", "label": "Save", "available": true,
				"requires_input": false, "accepts_input": true,
			}},
		},
	})
	router := &Router{DetailSnapshots: store}
	if err := router.validateInteractiveSurfaceAction("codex", "conv-rename", map[string]any{
		"surface_id": "surface-rename", "action_id": "save", "input": "Renamed thread",
	}); err != nil {
		t.Fatalf("expected optional interactive surface input to be accepted: %v", err)
	}
}

func TestValidateGoalPlanControlUsesCurrentCanonicalActions(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("plugin-1", map[string]any{
		"conversation_id": "conv-goal-plan",
		"plugin_id":       "plugin-1",
		"plan_mode": map[string]any{
			"actions": []map[string]any{{
				"id": "plan.set", "label": "关闭计划模式", "available": true,
				"target": map[string]any{"enabled": false},
			}},
		},
		"goal": map[string]any{
			"actions": []map[string]any{
				{"id": "goal.edit", "label": "编辑目标", "available": true, "requires_input": true},
				{"id": "goal.pause", "label": "暂停目标", "available": true},
			},
		},
	})
	router := &Router{DetailSnapshots: store}
	if err := router.validateGoalPlanControl("plugin-1", "conv-goal-plan", "plan.set", map[string]any{"enabled": false}); err != nil {
		t.Fatalf("expected current plan target to pass: %v", err)
	}
	if err := router.validateGoalPlanControl("plugin-1", "conv-goal-plan", "goal.edit", map[string]any{"objective": "Ship release"}); err != nil {
		t.Fatalf("expected current goal edit to pass: %v", err)
	}
	if err := router.validateGoalPlanControl("plugin-1", "conv-goal-plan", "goal.pause", map[string]any{}); err != nil {
		t.Fatalf("expected current goal lifecycle action to pass: %v", err)
	}
}

func TestValidateGoalPlanControlRejectsStaleActionAndTarget(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("plugin-1", map[string]any{
		"conversation_id": "conv-goal-plan",
		"plugin_id":       "plugin-1",
		"plan_mode": map[string]any{
			"actions": []map[string]any{{
				"id": "plan.set", "label": "关闭计划模式", "available": true,
				"target": map[string]any{"enabled": false},
			}},
		},
		"goal": map[string]any{
			"actions": []map[string]any{{"id": "goal.pause", "label": "暂停目标", "available": true}},
		},
	})
	router := &Router{DetailSnapshots: store}
	cases := []struct {
		action string
		target map[string]any
	}{
		{action: "plan.set", target: map[string]any{"enabled": true}},
		{action: "goal.edit", target: map[string]any{"objective": "stale action"}},
		{action: "goal.pause", target: map[string]any{"objective": "unexpected"}},
	}
	for _, testCase := range cases {
		if err := router.validateGoalPlanControl("plugin-1", "conv-goal-plan", testCase.action, testCase.target); err == nil || !strings.Contains(err.Error(), "control_target_stale") {
			t.Fatalf("expected stale %s request to fail, got %v", testCase.action, err)
		}
	}
}

func TestRouterRelaysGoalObjectiveBodyToSessionController(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()

	const pluginID = "codex"
	const conversationID = "conv-goal-body-relay"
	objective := "  Preserve this goal body exactly, including its trailing newline.\n"
	bindExistingConversation(t, db, pluginID, conversationID)

	adapter := &recordingAdapter{id: pluginID, visible: true}
	snapshots := NewDetailSnapshotStore()
	snapshots.PutForRuntime(pluginID, map[string]any{
		"conversation_id": conversationID,
		"plugin_id":       pluginID,
		"goal": map[string]any{
			"status": "none", "available": true,
			"actions": []map[string]any{{
				"id": "goal.set", "label": "设置目标", "available": true, "requires_input": true,
			}},
		},
	})
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            &recordingSink{},
		DetailSnapshots: snapshots,
	}

	_, err = router.HandleSessionControl(context.Background(), prismtransport.Inbound{
		MessageID:   "goal-body-relay-1",
		MessageType: prismtransport.MsgSessionControl,
		Payload: &prismtransport.InboundPayload{
			PluginID:       pluginID,
			ConversationID: conversationID,
			Action:         "goal.set",
			Text:           "this chat text must not replace the goal objective",
			Target:         map[string]any{"objective": objective},
			Metadata:       &prismtransport.InboundMetadata{SourceDevice: "mobile"},
		},
	})
	if err != nil {
		t.Fatalf("HandleSessionControl: %v", err)
	}
	if adapter.controlCount() != 1 {
		t.Fatalf("expected one relayed goal.set action, got %d control calls", adapter.controlCount())
	}
	if adapter.lastControlReq.Action != "goal.set" {
		t.Fatalf("expected goal.set action at plugin boundary, got %+v", adapter.lastControlReq)
	}
	target, ok := adapter.lastControlReq.Target.(map[string]any)
	if !ok || len(target) != 1 || target["objective"] != objective {
		t.Fatalf("expected original goal objective body to reach plugin, got %#v", adapter.lastControlReq.Target)
	}
	if adapter.lastControlReq.Metadata["source_device"] != "mobile" {
		t.Fatalf("expected source metadata to survive relay, got %#v", adapter.lastControlReq.Metadata)
	}
}

func TestRouterHandlesApprovalResolveForExistingSession(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-approval-resolve",
		PluginID:            "codex",
		NativeSessionID:     "native-approval-1",
		NativeThreadID:      "thread-approval-1",
		Surface:             "test",
		Endpoint:            "test://plugin",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{id: "codex", visible: true, canApproval: true}
	sink := &recordingSink{}
	detailSnapshots := NewDetailSnapshotStore()
	detailSnapshots.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-approval-resolve",
		"plugin_id":       "codex",
		"approval": map[string]any{
			"approval_request_id": "approval-request-1",
			"actions": []map[string]any{
				{"id": "approve_with_note", "label": "批准并附言", "available": true, "requires_input": true},
			},
		},
	})
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: detailSnapshots,
	}
	result, err := router.HandleApprovalResolve(ctx, prismtransport.Inbound{
		MessageID:   "approval-resolve-msg-1",
		MessageType: prismtransport.MsgApprovalResolve,
		Payload: &prismtransport.InboundPayload{
			TaskID:            "bridge-task-approval-resolve-1",
			PluginID:          "codex",
			ConversationID:    "conv-approval-resolve",
			ApprovalRequestID: "approval-request-1",
			ActionID:          "approve_with_note",
			Input:             "仅执行当前目录内的变更",
			Metadata:          &prismtransport.InboundMetadata{SourceDevice: "glasses"},
		},
	})
	if err != nil {
		t.Fatalf("HandleApprovalResolve: %v", err)
	}
	if result.Session.NativeSessionID != "native-approval-1" {
		t.Fatalf("unexpected approval resolve session: %+v", result.Session)
	}
	if adapter.approvalResolveCount() != 1 {
		t.Fatalf("expected one approval resolve call, got %d", adapter.approvalResolveCount())
	}
	req := adapter.lastApprovalResolve()
	if req.ApprovalRequestID != "approval-request-1" || req.ActionID != "approve_with_note" || req.Input != "仅执行当前目录内的变更" {
		t.Fatalf("unexpected approval resolve request: %+v", req)
	}
	if req.Session.NativeSessionID != "native-approval-1" {
		t.Fatalf("expected bound session in approval resolve request, got %+v", req.Session)
	}
	if statuses := sink.eventStatuses(); !containsString(statuses, StageRunning) {
		t.Fatalf("expected running task event after approval resolve, got %+v", statuses)
	}
	if summaries := sink.eventSummaries(); !containsString(summaries, "批准并附言") {
		t.Fatalf("expected approval resolve summary, got %+v", summaries)
	}
	resolveEvent := findConversationEvent(sink.conversationEvents(), "approval.resolve")
	if resolveEvent == nil {
		t.Fatalf("expected approval.resolve conversation event, got %+v", sink.conversationEvents())
	}
	resolution, ok := resolveEvent.Payload["approval_resolution"].(map[string]any)
	if !ok {
		t.Fatalf("expected approval_resolution payload, got %+v", resolveEvent.Payload)
	}
	if resolution["accepted"] != true || resolution["request_id"] != "approval-resolve-msg-1" {
		t.Fatalf("unexpected approval.resolve structured payload: %+v", resolution)
	}
	if resolveEvent.ConversationID != "conv-approval-resolve" || resolveEvent.PluginID != "codex" {
		t.Fatalf("unexpected approval.resolve routing envelope: %+v", resolveEvent)
	}
	if resolution["action_label"] != "批准并附言" || resolution["message"] != "审批命令已提交到桌面端。" {
		t.Fatalf("unexpected approval.resolve label/message payload: %+v", resolution)
	}
	if _, ok := resolution["updated_at"].(int64); !ok {
		t.Fatalf("expected approval.resolve updated_at millis, got %+v", resolution["updated_at"])
	}
}

func TestRouterRefreshesMissingApprovalSnapshotBeforeResolve(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-approval-refresh",
		PluginID:            "hermes",
		NativeSessionID:     "native-approval-refresh",
		NativeThreadID:      "native-approval-refresh",
		Surface:             "hermes-native-gateway",
		Endpoint:            "test://hermes",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	baseAdapter := &recordingAdapter{id: "hermes", visible: true, canApproval: true}
	adapter := &approvalDetailAdapter{
		recordingAdapter: baseAdapter,
		detail: map[string]any{
			"approval": map[string]any{
				"approval_request_id": "hermes.native.approval.safe-id",
				"actions": []map[string]any{
					{"id": "deny", "label": "Reject", "available": true},
				},
			},
		},
	}
	store := NewDetailSnapshotStore()
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            &recordingSink{},
		DetailSnapshots: store,
	}

	_, err = router.HandleApprovalResolve(ctx, prismtransport.Inbound{
		MessageID:   "approval-refresh-msg-1",
		MessageType: prismtransport.MsgApprovalResolve,
		Payload: &prismtransport.InboundPayload{
			PluginID:          "hermes",
			ConversationID:    "conv-approval-refresh",
			ApprovalRequestID: "hermes.native.approval.safe-id",
			ActionID:          "deny",
		},
	})
	if err != nil {
		t.Fatalf("HandleApprovalResolve: %v", err)
	}
	if baseAdapter.approvalResolveCount() != 1 {
		t.Fatalf("expected refreshed approval to reach adapter once, got %d", baseAdapter.approvalResolveCount())
	}
	detail, ok := store.GetForRuntime("hermes", "conv-approval-refresh")
	if !ok || stringFromAny(asMap(detail["approval"])["approval_request_id"]) != "hermes.native.approval.safe-id" {
		t.Fatalf("expected refreshed approval detail to be cached, got %#v", detail)
	}
}

func TestRouterRejectsForwardSendWhileApprovalPending(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "codex", visible: true}
	sink := &recordingSink{}
	detailSnapshots := NewDetailSnapshotStore()
	detailSnapshots.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-send-approval",
		"plugin_id":       "codex",
		"primary_action":  "approval",
		"approval": map[string]any{
			"approval_request_id": "approval-live-1",
		},
		"run": map[string]any{
			"status":              StageWaitingApproval,
			"approval_request_id": "approval-live-1",
			"primary_action":      "approval",
		},
	})
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: detailSnapshots,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-send-approval",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "继续执行",
			PluginID:       "codex",
			ConversationID: "conv-send-approval",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "resolve approval before sending") {
		t.Fatalf("expected approval-blocked send error, got %v", err)
	}
	if adapter.sendCount() != 0 {
		t.Fatalf("expected no native send while approval pending, got %d", adapter.sendCount())
	}
	if statuses := sink.eventStatuses(); !containsString(statuses, StageFailed) {
		t.Fatalf("expected failed task event for blocked send, got %+v", statuses)
	}
}

func TestRouterDefersRunningQueueDecisionToPlugin(t *testing.T) {
	detailSnapshots := NewDetailSnapshotStore()
	detailSnapshots.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-send-interrupt",
		"plugin_id":       "codex",
		"primary_action":  "interrupt",
		"run": map[string]any{
			"status":         StageRunning,
			"primary_action": "interrupt",
			"can_queue":      false,
		},
	})
	router := &Router{
		DetailSnapshots: detailSnapshots,
	}
	if err := router.validateForwardSend("codex", "conv-send-interrupt"); err != nil {
		t.Fatalf("running native composer must be validated by the plugin after text insertion, got %v", err)
	}
}

func TestRouterEmitConversationEventPreservesLiveDetailSnapshot(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-live-detail",
		PluginID:            "codex",
		NativeSessionID:     "native-live-detail",
		NativeThreadID:      "thread-live-detail",
		Surface:             "test",
		Endpoint:            "test://plugin",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{
		id:      "codex",
		visible: true,
	}
	sink := &recordingSink{}
	store := NewDetailSnapshotStore()
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: store,
	}

	router.emitConversationEvent(conversationEventContext{
		ConversationID: "conv-live-detail",
		PluginID:       "codex",
		Session: pluginruntime.NativeSession{
			PluginID:        "codex",
			NativeSessionID: "native-live-detail",
			NativeThreadID:  "thread-live-detail",
			Cwd:             "/tmp/project",
			Surface:         "test",
		},
	}, pluginruntime.PluginEvent{
		ID:      "desktop-state-1",
		Type:    "desktop.state.changed",
		Status:  StageRunning,
		Summary: "Codex 会话状态已更新。",
		Payload: map[string]any{
			"detail_snapshot": map[string]any{
				"primary_action":     "queue",
				"messages_signature": "msgsig-live",
				"current_model":      map[string]any{"id": "gpt-5.6", "label": "GPT-5.6"},
				"current_reasoning":  map[string]any{"key": "high", "value": "high", "label": "高"},
				"run": map[string]any{
					"status":         StageRunning,
					"primary_action": "queue",
					"can_queue":      true,
				},
			},
		},
	})

	detail := waitForConversationDetailSnapshot(t, sink, "conv-live-detail", 2*time.Second)
	if detail == nil {
		t.Fatal("expected queried detail snapshot to be emitted")
	}
	if stringFromAny(detail["primary_action"]) != "queue" {
		t.Fatalf("unexpected primary_action in emitted detail: %+v", detail)
	}
	run := asMap(detail["run"])
	if run["status"] != StageRunning || run["primary_action"] != "queue" {
		t.Fatalf("unexpected run payload in emitted detail: %+v", run)
	}
	currentModel := asMap(detail["current_model"])
	if currentModel["id"] != "gpt-5.6" {
		t.Fatalf("unexpected current model in emitted detail: %+v", currentModel)
	}
	currentReasoning := asMap(detail["current_reasoning"])
	if currentReasoning["id"] != "high" {
		t.Fatalf("unexpected current reasoning in emitted detail: %+v", currentReasoning)
	}
	if stringFromAny(detail["messages_signature"]) != "msgsig-live" {
		t.Fatalf("expected live messages_signature to be preserved, got %+v", detail)
	}
	stored, ok := store.GetForRuntime("codex", "conv-live-detail")
	if !ok || stringFromAny(stored["primary_action"]) != "queue" {
		t.Fatalf("expected detail snapshot store updated from queried detail: %+v", stored)
	}
}

func TestRouterSessionControlDoesNotEmitConversationIndexSnapshot(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-control-1",
		PluginID:            "codex",
		NativeSessionID:     "native-control-1",
		NativeThreadID:      "thread-control-1",
		Surface:             "test",
		Endpoint:            "test://plugin",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{
		id:      "codex",
		visible: true,
		controlResult: pluginruntime.SessionControlResult{
			OK:       true,
			Action:   "thread.archive",
			ThreadID: "thread-control-1",
			Message:  "archived",
		},
		sessionHints: []pluginruntime.NativeSessionHint{{
			PluginID:        "codex",
			NativeSessionID: "native-control-1",
			NativeThreadID:  "thread-control-1",
			Cwd:             "/tmp/project",
			Title:           "Task",
			Visible:         true,
			LastActivityAt:  time.Now().UTC(),
			Metadata: map[string]string{
				"status": "idle",
			},
		}},
	}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleSessionControl(ctx, prismtransport.Inbound{
		MessageID:   "control-msg-1",
		MessageType: prismtransport.MsgSessionControl,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			PluginID:       "codex",
			ConversationID: "conv-control-1",
			Action:         "thread.archive",
		},
	})
	if err != nil {
		t.Fatalf("HandleSessionControl: %v", err)
	}
	if adapter.lastControlReq.Action != "thread.archive" {
		t.Fatalf("unexpected control request: %+v", adapter.lastControlReq)
	}
	if snapshots := sink.conversationIndexSnapshots(); len(snapshots) != 0 {
		t.Fatalf("control acceptance must wait for a watcher state change, got %+v", snapshots)
	}
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("unconfirmed control detail must wait for a watcher state change, got %+v", snapshots)
	}
}

func TestRouterSessionControlPublishesConfirmedNativeDetailImmediately(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	const pluginID = "hermes"
	const conversationID = "conv-native-control"
	bindExistingConversation(t, db, pluginID, conversationID)
	adapter := &recordingAdapter{
		id:      pluginID,
		visible: true,
		controlResult: pluginruntime.SessionControlResult{
			OK:               true,
			Action:           "reasoning.switch",
			Message:          "native control applied",
			DetailsConfirmed: true,
			Details: map[string]any{
				"desktop_foreground": false,
				"detail_stale":       false,
				"current_reasoning": map[string]any{
					"id": "high", "label": "high", "current": true, "available": true,
				},
				"reasoning_options": []any{map[string]any{
					"id": "high", "label": "high", "current": true, "available": true,
					"target": map[string]any{"option_id": "high"},
				}},
			},
		},
	}
	sink := &recordingSink{}
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: NewDetailSnapshotStore(),
	}
	_, err = router.HandleSessionControl(context.Background(), prismtransport.Inbound{
		MessageID:   "native-control-msg",
		MessageType: prismtransport.MsgSessionControl,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			PluginID:       pluginID,
			ConversationID: conversationID,
			Action:         "reasoning.switch",
			Target:         map[string]any{"option_id": "high"},
		},
	})
	if err != nil {
		t.Fatalf("HandleSessionControl: %v", err)
	}
	detail := waitForConversationDetailSnapshot(t, sink, conversationID, 2*time.Second)
	current := asMap(detail["current_reasoning"])
	if stringFromAny(current["id"]) != "high" || firstNonEmptyBool(detail["desktop_foreground"]) || firstNonEmptyBool(detail["detail_stale"]) {
		t.Fatalf("expected confirmed native detail to publish immediately, got %+v", detail)
	}
}

func TestRouterSessionDeleteRebuildsConversationIndex(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	const pluginID = "openclaw"
	const conversationID = "conv-delete-index"
	bindExistingConversation(t, db, pluginID, conversationID)
	recording := &recordingAdapter{
		id:      pluginID,
		visible: true,
		sessionHints: []pluginruntime.NativeSessionHint{{
			PluginID:        pluginID,
			NativeSessionID: "native-delete-index",
			NativeThreadID:  "native-delete-index",
			Surface:         "gateway",
			Endpoint:        "gateway://test",
			Title:           "Prism QA delete index",
			Visible:         true,
		}},
		controlResult: pluginruntime.SessionControlResult{
			OK:      true,
			Action:  "delete",
			Message: "deleted",
		},
	}
	adapter := &deleteIndexRecordingAdapter{recordingAdapter: recording}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}

	_, err = router.HandleSessionControl(context.Background(), prismtransport.Inbound{
		MessageID:   "delete-index-control",
		MessageType: prismtransport.MsgSessionControl,
		Payload: &prismtransport.InboundPayload{
			PluginID:       pluginID,
			ConversationID: conversationID,
			Action:         "delete",
		},
	})
	if err != nil {
		t.Fatalf("HandleSessionControl: %v", err)
	}
	snapshots := sink.conversationIndexSnapshots()
	if len(snapshots) != 1 || len(snapshots[0].Plugins) != 1 {
		t.Fatalf("expected one rebuilt OpenClaw index snapshot, got %+v", snapshots)
	}
	if got := snapshots[0].Plugins[0].Conversations; len(got) != 0 {
		t.Fatalf("deleted conversation remained in rebuilt index: %+v", got)
	}
}

type deleteIndexRecordingAdapter struct {
	*recordingAdapter
}

func (a *deleteIndexRecordingAdapter) ControlSession(
	ctx context.Context,
	req pluginruntime.SessionControlRequest,
) (pluginruntime.SessionControlResult, error) {
	result, err := a.recordingAdapter.ControlSession(ctx, req)
	if err != nil {
		return result, err
	}
	a.recordingAdapter.mu.Lock()
	a.recordingAdapter.sessionHints = nil
	a.recordingAdapter.mu.Unlock()
	return result, nil
}

func TestRouterEmitsApprovalRequiredForApprovalEvent(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "codex", "conv-approval")
	adapter := &recordingAdapter{id: "codex", visible: true, subscribeEvents: []pluginruntime.PluginEvent{
		{
			ID:      "approval-1",
			Type:    "thread.status",
			Status:  StageRunning,
			Summary: "Codex 正在等待桌面端确认。",
			Payload: map[string]any{
				"method":      "item/commandExecution/requestApproval",
				"thread_id":   "thread-conv-approval",
				"itemId":      "item-approval-1",
				"title":       "允许执行命令",
				"description": "将执行 git commit。",
				"actions": []map[string]any{
					{"id": "desktop:0:cancel", "label": "取消", "style": "danger", "available": true},
					{"id": "desktop:1:approve", "label": "允许", "style": "primary", "available": true},
				},
				"input": map[string]any{"enabled": true, "label": "审批说明"},
				"item": map[string]any{
					"type":    "commandExecution",
					"command": "git commit",
				},
			},
		},
	}}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	result, err := router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-approval",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "帮我提交代码",
			PluginID:       "codex",
			ConversationID: "conv-approval",
			Metadata:       &prismtransport.InboundMetadata{SourceDevice: "glasses"},
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	if !result.Visible {
		t.Fatalf("expected visible route result: %+v", result)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(sink.approvalEvents()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if statuses := sink.eventStatuses(); !containsAll(statuses, StageVisible, StageRunning, StageWaitingApproval) {
		t.Fatalf("expected waiting approval task event, got %+v", statuses)
	}
	approvals := sink.approvalEvents()
	if len(approvals) != 1 {
		t.Fatalf("expected one approval event, got %+v", approvals)
	}
	if approvals[0].ConversationID != "conv-approval" || approvals[0].RequestID != "msg-approval" || approvals[0].TaskID != "bridge-task-msg-approval" {
		t.Fatalf("unexpected approval payload: %+v", approvals[0])
	}
	if approvals[0].ApprovalRequestID != "item-approval-1" || approvals[0].Metadata["approval_kind"] != "command_execution" {
		t.Fatalf("unexpected approval payload details: %+v", approvals[0])
	}
	if len(approvals[0].Actions) != 2 || approvals[0].Actions[1]["id"] != "desktop:1:approve" || approvals[0].Input["label"] != "审批说明" {
		t.Fatalf("approval actions and input must survive the Hub transport: %+v", approvals[0])
	}
	approvalEvent := findConversationEvent(sink.conversationEvents(), "thread.status")
	if approvalEvent == nil {
		t.Fatalf("expected approval-related conversation event, got %+v", sink.conversationEvents())
	}
	approvalPayload, ok := approvalEvent.Payload["approval"].(map[string]any)
	if !ok || approvalPayload["id"] != "item-approval-1" || approvalPayload["method"] != "item/commandExecution/requestApproval" {
		t.Fatalf("unexpected structured approval payload: %+v", approvalEvent.Payload["approval"])
	}
	approvalActions, ok := approvalPayload["actions"].([]map[string]any)
	if !ok || len(approvalActions) != 2 || approvalActions[1]["id"] != "desktop:1:approve" {
		t.Fatalf("conversation event dropped approval actions: %+v", approvalPayload)
	}
}

func TestRouterPrefersNormalizedApprovalEventShape(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	bindExistingConversation(t, db, "codex", "conv-approval-normalized")
	adapter := &recordingAdapter{id: "codex", visible: true, subscribeEvents: []pluginruntime.PluginEvent{
		{
			ID:      "approval-norm-1",
			Type:    "approval.required",
			Status:  StageWaitingApproval,
			Summary: "Codex 请求在桌面端确认执行命令：git commit",
			Payload: map[string]any{
				"thread_id":           "thread-conv-approval",
				"native_thread_id":    "thread-conv-approval",
				"native_session_id":   "session-conv-approval",
				"approval_kind":       "command_execution",
				"approval_request_id": "approval-request-1",
				"item": map[string]any{
					"type":    "commandExecution",
					"command": "git commit",
				},
			},
		},
	}}
	sink := &recordingSink{}
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    sink,
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-approval-normalized",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "帮我提交代码",
			PluginID:       "codex",
			ConversationID: "conv-approval-normalized",
		},
	})
	if err != nil {
		t.Fatalf("HandleInbound: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(sink.approvalEvents()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	approvals := sink.approvalEvents()
	if len(approvals) != 1 {
		t.Fatalf("expected one approval event, got %+v", approvals)
	}
	if approvals[0].ApprovalRequestID != "approval-request-1" {
		t.Fatalf("unexpected approval request id: %+v", approvals[0])
	}
	encoded, err := json.Marshal(approvals[0])
	if err != nil || strings.Contains(string(encoded), "session-conv-approval") || strings.Contains(string(encoded), "thread-conv-approval") {
		t.Fatalf("approval payload leaked native identity: %s (%v)", encoded, err)
	}
	if approvals[0].Metadata["approval_kind"] != "command_execution" {
		t.Fatalf("unexpected approval metadata: %+v", approvals[0])
	}
}

func TestRouterTaskInterruptFailsWhenSessionMissing(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	router := &Router{
		Service: pluginruntime.New(&recordingAdapter{id: "openclaw", visible: true}),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    &recordingSink{},
	}
	_, err = router.HandleInterrupt(context.Background(), prismtransport.Inbound{
		MessageID:   "interrupt-missing",
		MessageType: prismtransport.MsgTaskInterrupt,
		Payload: &prismtransport.InboundPayload{
			TaskID:         "bridge-task-missing",
			PluginID:       "openclaw",
			ConversationID: "conv-missing",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("expected missing session error, got %v", err)
	}
}

func TestRouterRejectsPluginWithoutRemoteConversationContract(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	router := &Router{
		Service: pluginruntime.New(&limitedCapabilityAdapter{recordingAdapter: &recordingAdapter{id: "demo", visible: true}}),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    &recordingSink{},
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-limited-1",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "hello",
			PluginID:       "demo",
			ConversationID: "conv-limited-1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "remote conversation unsupported (missing_list_sessions)") {
		t.Fatalf("expected remote conversation contract error, got %v", err)
	}
}

func TestRouterRejectsUnboundConversation(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{id: "codex", visible: true}
	conversationID := "conv-unbound"
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    &recordingSink{},
	}
	_, err = router.HandleInbound(context.Background(), prismtransport.Inbound{
		MessageID:   "msg-synthetic-1",
		MessageType: prismtransport.MsgChatSend,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			Text:           "继续",
			PluginID:       "codex",
			ConversationID: conversationID,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "conversation binding not found") {
		t.Fatalf("expected unbound conversation rejection, got %v", err)
	}
	if adapter.attachCount() != 0 {
		t.Fatalf("unbound conversation must not attach, got %d", adapter.attachCount())
	}
}

func TestRouterSessionControlRejectsUnboundConversation(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &recordingAdapter{
		id:      "codex",
		visible: true,
		controlResult: pluginruntime.SessionControlResult{
			OK:       true,
			Action:   "conversation.select",
			ThreadID: "thread-synthetic-1",
			Message:  "selected",
		},
	}
	conversationID := "conv-unbound-control"
	router := &Router{
		Service: pluginruntime.New(adapter),
		DB:      db,
		Config:  remoteAuthorizedConfig(),
		Sink:    &recordingSink{},
	}
	_, err = router.HandleSessionControl(context.Background(), prismtransport.Inbound{
		MessageID:   "control-synthetic-1",
		MessageType: prismtransport.MsgSessionControl,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			PluginID:       "codex",
			ConversationID: conversationID,
			Action:         "conversation.select",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "conversation not found") {
		t.Fatalf("expected unbound control rejection, got %v", err)
	}
	if adapter.lastControlReq.Action != "" {
		t.Fatalf("unbound conversation must not issue control: %+v", adapter.lastControlReq)
	}
}

func TestRouterConversationSelectDoesNotEmitControlDetailSnapshot(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-select",
		PluginID:            "codex",
		NativeSessionID:     "native-select",
		NativeThreadID:      "thread-select",
		Surface:             "test",
		Endpoint:            "test://codex",
		Cwd:                 "/tmp/project",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &recordingAdapter{
		id:      "codex",
		visible: true,
		controlResult: pluginruntime.SessionControlResult{
			OK:       true,
			Action:   "conversation.select",
			ThreadID: "thread-select",
			Message:  "selected",
			Details: map[string]any{
				"selected":    true,
				"selected_at": time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	}
	sink := &recordingSink{}
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: NewDetailSnapshotStore(),
	}
	_, err = router.HandleSessionControl(ctx, prismtransport.Inbound{
		MessageID:   "control-select-1",
		MessageType: prismtransport.MsgSessionControl,
		Timestamp:   time.Now().UnixMilli(),
		Payload: &prismtransport.InboundPayload{
			PluginID:       "codex",
			ConversationID: "conv-select",
			Action:         "conversation.select",
		},
	})
	if err != nil {
		t.Fatalf("HandleSessionControl: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("expected conversation.select control path to defer detail truth to watcher, got %+v", snapshots)
	}
}

func TestRouterControlTargetStaleEmitsFailureEventWithoutDetailMutation(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	conversationID := "conv-control-stale"
	bindExistingConversation(t, db, "codex", conversationID)
	adapter := &recordingAdapter{
		id:         "codex",
		visible:    true,
		controlErr: errors.New("control_target_stale: model option changed"),
	}
	sink := &recordingSink{}
	router := &Router{
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: NewDetailSnapshotStore(),
	}
	_, err = router.HandleSessionControl(context.Background(), prismtransport.Inbound{
		MessageID:   "request-control-stale",
		MessageType: prismtransport.MsgSessionControl,
		Payload: &prismtransport.InboundPayload{
			PluginID:       "codex",
			ConversationID: conversationID,
			Action:         "model.switch",
			Target:         map[string]any{"option_id": "gpt-5.4-mini"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "control_target_stale") {
		t.Fatalf("expected stale control error, got %v", err)
	}
	event := findConversationEvent(sink.conversationEvents(), "conversation.control_failed")
	if event == nil {
		t.Fatalf("expected control failure event, got %+v", sink.conversationEvents())
	}
	failure, _ := event.Payload["control_failure"].(map[string]any)
	if failure["request_id"] != "request-control-stale" || failure["code"] != "control_target_stale" {
		t.Fatalf("unexpected control failure payload: %+v", failure)
	}
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("control failure must not mutate detail snapshot, got %+v", snapshots)
	}
}

type recordingAdapter struct {
	id              string
	visible         bool
	waitErr         error
	canApproval     bool
	sessionHints    []pluginruntime.NativeSessionHint
	controlResult   pluginruntime.SessionControlResult
	controlErr      error
	draftControlErr error
	startResult     *pluginruntime.StartSessionWithMessageResult
	lastControlReq  pluginruntime.SessionControlRequest

	mu               sync.Mutex
	probes           int
	attaches         int
	starts           int
	sends            int
	interrupts       int
	approvalResolves int
	controls         int
	lastInterrupt    string
	lastAttachReq    pluginruntime.AttachSessionRequest
	lastApprovalReq  pluginruntime.ApprovalResolutionRequest
	messages         []pluginruntime.InboundMessage
	subscribeEvents  []pluginruntime.PluginEvent
}

type approvalDetailAdapter struct {
	*recordingAdapter
	detail map[string]any
}

func (a *approvalDetailAdapter) Probe(ctx context.Context) (pluginruntime.Capability, error) {
	capability, err := a.recordingAdapter.Probe(ctx)
	capability.IntegrationMode = pluginruntime.IntegrationModeProtocolNative
	return capability, err
}

func (a *approvalDetailAdapter) ReadDetail(context.Context, pluginruntime.NativeSession) (map[string]any, error) {
	return cloneDetailMap(a.detail), nil
}

func (a *recordingAdapter) ID() string { return a.id }

func (a *recordingAdapter) LocalMetadata() pluginruntime.LocalMetadata {
	return pluginruntime.LocalMetadata{PluginID: a.id}
}

func (a *recordingAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	a.mu.Lock()
	a.probes++
	a.mu.Unlock()
	return pluginruntime.Capability{
		PluginID:                   a.id,
		Available:                  a.visible,
		NativeVisibleInput:         a.visible,
		CanAttachSession:           a.visible,
		CanStartSessionWithMessage: a.visible,
		CanOpenDraft:               a.visible,
		CanListSessions:            a.visible,
		CanReadHistory:             a.visible,
		CanApproval:                a.canApproval,
		CanForwardSync:             a.visible,
		CanWaitRun:                 true, // recordingAdapter implements RunWaiter
		VisibilitySurface:          "test",
		UnavailableReason:          mapReason(a.visible),
	}, nil
}

func (a *recordingAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: a.id, Endpoint: "test://plugin", Verified: a.visible}, nil
}

func (a *recordingAdapter) AttachSession(_ context.Context, req pluginruntime.AttachSessionRequest) (pluginruntime.NativeSession, error) {
	a.mu.Lock()
	a.attaches++
	a.lastAttachReq = req
	a.mu.Unlock()
	return pluginruntime.NativeSession{
		PluginID:        a.id,
		NativeSessionID: req.NativeSessionID,
		NativeThreadID:  firstNonEmpty(req.NativeThreadID, req.NativeSessionID),
		Surface:         "test",
		Endpoint:        "test://plugin",
		Cwd:             req.Cwd,
		Visible:         true,
	}, nil
}

func (a *recordingAdapter) StartSessionWithMessage(_ context.Context, req pluginruntime.StartSessionWithMessageRequest) (pluginruntime.StartSessionWithMessageResult, error) {
	a.mu.Lock()
	a.starts++
	configured := a.startResult
	a.mu.Unlock()
	if configured != nil {
		return *configured, nil
	}
	nativeID := "native-start-" + req.Message.PrismMessageID
	return pluginruntime.StartSessionWithMessageResult{
		Session: pluginruntime.NativeSession{
			PluginID:        a.id,
			NativeSessionID: nativeID,
			NativeThreadID:  nativeID,
			Surface:         "test",
			Endpoint:        "test://plugin",
			Cwd:             req.Cwd,
			Visible:         true,
		},
		Receipt: pluginruntime.SendReceipt{
			NativeMessageID: "native-message-" + req.Message.PrismMessageID,
			Accepted:        true,
			Visible:         true,
		},
		Visibility: pluginruntime.VisibilityResult{Visible: true, Evidence: "test"},
	}, nil
}

func (a *recordingAdapter) OpenDraft(_ context.Context, req pluginruntime.DraftOpenRequest) (pluginruntime.DraftOpenResult, error) {
	return pluginruntime.DraftOpenResult{
		DraftID: req.DraftID, Cwd: req.Cwd, Controls: map[string]any{"actions": []any{}}, DraftFingerprint: "test-draft-fingerprint",
	}, nil
}

func (a *recordingAdapter) ControlDraft(_ context.Context, req pluginruntime.DraftControlRequest) (pluginruntime.DraftControlResult, error) {
	if a.draftControlErr != nil {
		return pluginruntime.DraftControlResult{}, a.draftControlErr
	}
	return pluginruntime.DraftControlResult{DraftID: req.DraftID, Controls: map[string]any{"actions": []any{}}}, nil
}

func (a *recordingAdapter) StartDraftWithMessage(ctx context.Context, req pluginruntime.StartDraftWithMessageRequest) (pluginruntime.StartSessionWithMessageResult, error) {
	return a.StartSessionWithMessage(ctx, pluginruntime.StartSessionWithMessageRequest{
		PluginID: req.PluginID, Cwd: req.Cwd, Message: req.Message, SourceDevice: req.SourceDevice, Metadata: req.Metadata,
	})
}

type draftStaleError struct{}

func (draftStaleError) Error() string { return "draft_stale: test desktop changed" }

func (a *recordingAdapter) ReadHistory(context.Context, pluginruntime.NativeSession, int) ([]pluginruntime.HistoryMessage, error) {
	return []pluginruntime.HistoryMessage{
		{ID: "user-1", Role: "user", Type: "text", Content: "hello"},
		{ID: "assistant-1", Role: "assistant", Type: "text", Content: "world"},
	}, nil
}

func (a *recordingAdapter) ListSessions(context.Context) ([]pluginruntime.NativeSessionHint, error) {
	if len(a.sessionHints) == 0 {
		return nil, nil
	}
	return append([]pluginruntime.NativeSessionHint(nil), a.sessionHints...), nil
}

func (a *recordingAdapter) Send(_ context.Context, session pluginruntime.NativeSession, msg pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	a.mu.Lock()
	a.sends++
	a.messages = append(a.messages, msg)
	a.mu.Unlock()
	return pluginruntime.SendReceipt{NativeMessageID: "native-msg-" + msg.PrismMessageID, Accepted: true, Visible: session.Visible, Detail: "started"}, nil
}

func (a *recordingAdapter) Subscribe(context.Context, pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	events := a.subscribeEvents
	if len(events) == 0 {
		events = []pluginruntime.PluginEvent{{
			ID:      "stream-1",
			Type:    "message.assistant",
			Status:  StageRunning,
			Summary: "openclaw streaming",
		}}
	}
	ch := make(chan pluginruntime.PluginEvent, len(events))
	for _, event := range events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func (a *recordingAdapter) WaitForRun(_ context.Context, _ pluginruntime.NativeSession, runID string) (pluginruntime.PluginEvent, error) {
	if a.waitErr != nil {
		return pluginruntime.PluginEvent{}, a.waitErr
	}
	return pluginruntime.PluginEvent{
		ID:      runID,
		Type:    "run.completed",
		Status:  StageCompleted,
		Summary: "openclaw completed " + runID,
	}, nil
}

func (a *recordingAdapter) Interrupt(_ context.Context, _ pluginruntime.NativeSession, taskID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.interrupts++
	a.lastInterrupt = taskID
	return nil
}

type fixedSessionControlController struct {
	result  pluginruntime.SessionControlResult
	request pluginruntime.SessionControlRequest
	err     error
}

func (c *fixedSessionControlController) ControlSession(_ context.Context, req pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	c.request = req
	return c.result, c.err
}

func (a *recordingAdapter) ControlSession(_ context.Context, req pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	a.mu.Lock()
	a.controls++
	a.lastControlReq = req
	result := a.controlResult
	err := a.controlErr
	a.mu.Unlock()
	if err != nil {
		return pluginruntime.SessionControlResult{}, err
	}
	if !result.OK {
		result.OK = true
	}
	if result.Action == "" {
		result.Action = req.Action
	}
	return result, nil
}

func (a *recordingAdapter) ResolveApproval(_ context.Context, req pluginruntime.ApprovalResolutionRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.approvalResolves++
	a.lastApprovalReq = req
	return nil
}

func (a *recordingAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{}, nil
}

func (a *recordingAdapter) Close(context.Context) error { return nil }

type limitedCapabilityAdapter struct {
	*recordingAdapter
}

func (a *limitedCapabilityAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{
		PluginID:                   a.id,
		Available:                  true,
		NativeVisibleInput:         true,
		CanStartSessionWithMessage: true,
		CanForwardSync:             true,
		CanWaitRun:                 true,
		VisibilitySurface:          "test",
	}, nil
}

func (a *recordingAdapter) sendCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sends
}

func (a *recordingAdapter) attachCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attaches
}

func (a *recordingAdapter) startCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.starts
}

func (a *recordingAdapter) probeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.probes
}

func (a *recordingAdapter) interruptCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.interrupts
}

func (a *recordingAdapter) lastInterruptTaskID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastInterrupt
}

func (a *recordingAdapter) approvalResolveCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.approvalResolves
}

func (a *recordingAdapter) controlCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.controls
}

func (a *recordingAdapter) lastApprovalResolve() pluginruntime.ApprovalResolutionRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastApprovalReq
}

type recordingSink struct {
	mu       sync.Mutex
	out      []prismtransport.Outbound
	controls []prismtransport.ControlOutbound
}

func draftEvents(s *recordingSink) []prismtransport.ConversationDraftEventPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []prismtransport.ConversationDraftEventPayload
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgConversationDraftEvent {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.ConversationDraftEventPayload); ok {
			events = append(events, payload)
		}
	}
	return events
}

func (s *recordingSink) Send(out prismtransport.Outbound) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out = append(s.out, out)
	return nil
}

func (s *recordingSink) SendControl(out prismtransport.ControlOutbound) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controls = append(s.controls, out)
	return nil
}

func (s *recordingSink) eventStatuses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var statuses []string
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgTaskEvent {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.TaskEventPayload); ok {
			statuses = append(statuses, payload.Status)
		}
	}
	return statuses
}

func (s *recordingSink) eventSummaries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var summaries []string
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgTaskEvent {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.TaskEventPayload); ok {
			summaries = append(summaries, payload.Summary)
		}
	}
	return summaries
}

func (s *recordingSink) eventRequestIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgTaskEvent {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.TaskEventPayload); ok {
			ids = append(ids, payload.RequestID)
		}
	}
	return ids
}

func (s *recordingSink) finalContents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, item := range s.out {
		if item.MessageType == prismtransport.MsgChatFinal && item.Payload != nil && item.Payload.Message != nil {
			out = append(out, item.Payload.Message.Content)
		}
	}
	return out
}

func (s *recordingSink) finalRequestIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, item := range s.out {
		if item.MessageType == prismtransport.MsgChatFinal && item.Payload != nil {
			ids = append(ids, item.Payload.RequestID)
		}
	}
	return ids
}

func (s *recordingSink) errorContents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, item := range s.out {
		if item.MessageType == prismtransport.MsgChatError && item.Payload != nil && item.Payload.Message != nil {
			out = append(out, item.Payload.Message.Content)
		}
	}
	return out
}

func (s *recordingSink) errorRequestIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, item := range s.out {
		if item.MessageType == prismtransport.MsgChatError && item.Payload != nil {
			ids = append(ids, item.Payload.RequestID)
		}
	}
	return ids
}

func (s *recordingSink) taskEvents() []prismtransport.TaskEventPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []prismtransport.TaskEventPayload
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgTaskEvent {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.TaskEventPayload); ok {
			events = append(events, payload)
		}
	}
	return events
}

func isConversationEventType(t prismtransport.MessageType) bool {
	return t.IsConversationEvent()
}

func (s *recordingSink) conversationEvents() []prismtransport.ConversationEventPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []prismtransport.ConversationEventPayload
	for _, item := range s.controls {
		if !isConversationEventType(item.MessageType) {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.ConversationEventPayload); ok {
			events = append(events, payload)
		}
	}
	return events
}

func (s *recordingSink) conversationSummaries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var summaries []string
	for _, item := range s.controls {
		if !isConversationEventType(item.MessageType) {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.ConversationEventPayload); ok {
			summaries = append(summaries, payload.Summary)
		}
	}
	return summaries
}

func (s *recordingSink) approvalEvents() []prismtransport.ApprovalRequiredPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []prismtransport.ApprovalRequiredPayload
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgApprovalRequired {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.ApprovalRequiredPayload); ok {
			events = append(events, payload)
		}
	}
	return events
}

func (s *recordingSink) conversationIndexSnapshots() []prismtransport.ConversationIndexSnapshotPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshots []prismtransport.ConversationIndexSnapshotPayload
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgConversationIndexSnapshot {
			continue
		}
		if payload, ok := item.Payload.(prismtransport.ConversationIndexSnapshotPayload); ok {
			snapshots = append(snapshots, payload)
		}
	}
	return snapshots
}

func (s *recordingSink) conversationDetailSnapshots() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshots []map[string]any
	for _, item := range s.controls {
		if item.MessageType != prismtransport.MsgConversationDetailSnapshot {
			continue
		}
		if payload, ok := item.Payload.(map[string]any); ok {
			snapshots = append(snapshots, payload)
		}
	}
	return snapshots
}

func waitForConversationDetailSnapshot(t *testing.T, sink *recordingSink, conversationID string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, item := range sink.conversationDetailSnapshots() {
			if stringFromAny(item["conversation_id"]) == conversationID {
				return item
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func containsReusedReadyEvent(events []prismtransport.TaskEventPayload) bool {
	for _, event := range events {
		if event.Status != StageNativeSessionReady {
			continue
		}
		if event.SessionReused {
			return true
		}
	}
	return false
}

func findConversationEvent(events []prismtransport.ConversationEventPayload, eventType string) *prismtransport.ConversationEventPayload {
	for _, event := range events {
		if event.EventType == eventType {
			copy := event
			return &copy
		}
	}
	return nil
}

func containsAll(values []string, wants ...string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		seen[value] = true
	}
	for _, want := range wants {
		if !seen[want] {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func mapReason(visible bool) string {
	if visible {
		return ""
	}
	return "native_visible_input_false"
}
