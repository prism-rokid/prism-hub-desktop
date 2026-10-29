package bridge

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

func TestReverseSyncerSubscribesByDefaultWhenCapabilityAllows(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-default",
		PluginID:            "openclaw",
		NativeSessionID:     "native-default",
		NativeThreadID:      "thread-default",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &reverseAdapter{id: "openclaw", events: make(chan pluginruntime.PluginEvent, 1)}
	syncer := &ReverseSyncer{
		Config:  remoteAuthorizedConfig(),
		Service: pluginruntime.New(adapter),
		DB:      db,
		Sink:    &recordingSink{},
	}
	syncer.sync(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 1 {
		t.Fatalf("reverse sync should subscribe by default, got %d", adapter.subscribeCountValue())
	}
}

func TestReverseSyncerResubscribesAfterSubscriptionChannelCloses(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-resubscribe",
		PluginID:            "openclaw",
		NativeSessionID:     "native-resubscribe",
		NativeThreadID:      "thread-resubscribe",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &restartingReverseAdapter{id: "openclaw"}
	store := NewDetailSnapshotStore()
	store.PutForRuntime("openclaw", map[string]any{
		"plugin_id": "openclaw", "conversation_id": "conv-resubscribe",
		"terminal": map[string]any{"status": "attached", "input_owner": "panel", "can_send": true},
	})
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config:          remoteAuthorizedConfig(),
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Sink:            sink,
		DetailSnapshots: store,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncer.sync(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 1 {
		t.Fatalf("expected initial reverse sync subscription, got %d", adapter.subscribeCountValue())
	}
	adapter.closeLatest()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		syncer.mu.Lock()
		activeCount := len(syncer.active)
		syncer.mu.Unlock()
		if activeCount == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	syncer.mu.Lock()
	activeCount := len(syncer.active)
	syncer.mu.Unlock()
	if activeCount != 0 {
		t.Fatalf("expected closed subscription to be released from active set, got %d", activeCount)
	}
	if cached, ok := store.GetForRuntime("openclaw", "conv-resubscribe"); !ok || asMap(cached["terminal"])["can_send"] != false {
		t.Fatalf("closed subscription retained terminal send control: %#v", cached)
	}
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) == 0 || asMap(snapshots[len(snapshots)-1]["terminal"])["can_send"] != false {
		t.Fatalf("closed subscription was not broadcast as unavailable: %#v", snapshots)
	}
	syncer.sync(ctx)
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 2 {
		t.Fatalf("expected reverse sync to resubscribe after channel close, got %d", adapter.subscribeCountValue())
	}
}

func TestReverseSyncerOffModeSkipsSubscriptions(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-off",
		PluginID:            "codex",
		NativeSessionID:     "native-off",
		NativeThreadID:      "thread-off",
		Surface:             "app-server",
		Endpoint:            "test://codex",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncOff}
	adapter := &reverseAdapter{id: "codex", events: make(chan pluginruntime.PluginEvent, 1)}
	syncer := &ReverseSyncer{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
		DB:      db,
		Sink:    &recordingSink{},
	}
	syncer.sync(context.Background())
	time.Sleep(100 * time.Millisecond)
	if adapter.subscribeCountValue() != 0 {
		t.Fatalf("reverse sync off should skip subscriptions, got %d", adapter.subscribeCountValue())
	}
}

func TestReverseSyncerAgentUsageEventPublishesOnlyCatalog(t *testing.T) {
	adapter := &reverseAdapter{id: "codex", events: make(chan pluginruntime.PluginEvent)}
	sink := &recordingSink{}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{"codex": true}
	syncer := &ReverseSyncer{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
		Sink:    sink,
		SnapshotPublisher: NewSnapshotPublisher(
			pluginruntime.New(adapter), nil, cfg, nil, sink, nil, nil),
	}
	syncer.handleEvent(context.Background(), reverseSyncTarget{
		pluginWide: true,
		session:    pluginruntime.NativeSession{PluginID: "codex"},
	}, pluginruntime.PluginEvent{Type: "agent.usage.updated"})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.controls) != 1 || sink.controls[0].MessageType != prismtransport.MsgCatalogSnapshot {
		t.Fatalf("usage update should publish one catalog snapshot, got %#v", sink.controls)
	}
	for _, outbound := range sink.controls {
		if outbound.MessageType == prismtransport.MsgConversationEvent {
			t.Fatalf("usage update must not create a conversation event: %#v", sink.controls)
		}
	}
}

func TestReverseSyncerSubscribesBoundSessionsAndEmitsConversationEvent(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		NativeSessionID:     "native-1",
		NativeThreadID:      "thread-1",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	}); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &reverseAdapter{id: "openclaw", events: make(chan pluginruntime.PluginEvent, 1)}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config:  remoteAuthorizedConfig(),
		Service: pluginruntime.New(adapter),
		DB:      db,
		Sink:    sink,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	syncer.sync(runCtx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 1 || adapter.lastSessionValue().NativeSessionID != "native-1" {
		t.Fatalf("expected one native subscription, count=%d session=%+v", adapter.subscribeCountValue(), adapter.lastSessionValue())
	}
	adapter.events <- pluginruntime.PluginEvent{
		ID:      "event-1",
		Type:    "message.assistant",
		Status:  StageRunning,
		Summary: "OpenClaw desktop activity",
		Payload: map[string]any{"task_id": "native-task-1"},
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !containsString(sink.conversationSummaries(), "OpenClaw desktop activity") {
		time.Sleep(10 * time.Millisecond)
	}
	if !containsString(sink.conversationSummaries(), "OpenClaw desktop activity") {
		t.Fatalf("expected conversation event summary, got %+v", sink.conversationSummaries())
	}
	conversationEvents := sink.conversationEvents()
	if len(conversationEvents) == 0 {
		t.Fatalf("expected reverse sync conversation events")
	}
	if conversationEvents[0].ConversationID != "conv-1" || conversationEvents[0].PluginID != "openclaw" {
		t.Fatalf("unexpected conversation event payload: %+v", conversationEvents[0])
	}
	envelope := conversationEvents[0].Payload
	if envelope["schema_version"] != "conversation.v1" || envelope["bound"] != true {
		t.Fatalf("expected structured conversation envelope, got %+v", envelope)
	}
	sessionPayload, ok := envelope["session"].(map[string]any)
	if !ok || sessionPayload["source"] != "desktop" {
		t.Fatalf("expected session payload in conversation envelope, got %+v", envelope["session"])
	}
	messagePayload, ok := envelope["message"].(map[string]any)
	if !ok || messagePayload["role"] != "assistant" {
		t.Fatalf("expected assistant message payload, got %+v", envelope["message"])
	}
	runPayload, ok := envelope["run"].(map[string]any)
	if !ok || runPayload["status"] != StageRunning {
		t.Fatalf("expected run payload, got %+v", envelope["run"])
	}
	if snapshots := sink.conversationDetailSnapshots(); len(snapshots) != 0 {
		t.Fatalf("ordinary reverse-sync events must not synthesize detail snapshots: %+v", snapshots)
	}
	if snapshots := sink.conversationIndexSnapshots(); len(snapshots) != 0 {
		t.Fatalf("ordinary reverse-sync events must not rebuild the full conversation index: %+v", snapshots)
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) == 0 || events[0].NativeEventID != "event-1" || events[0].PrismConversationID != "conv-1" {
		t.Fatalf("unexpected persisted events: %+v", events)
	}
}

func TestReverseSyncerDoesNotForwardReplayedNativeEventTwice(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-hook-replay", PluginID: "openclaw",
		NativeSessionID: "native-hook-replay", NativeThreadID: "native-hook-replay",
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), DB: db, Sink: sink}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-hook-replay"}}
	event := pluginruntime.PluginEvent{ID: "native-hook-event-1", Type: "message.assistant", Status: StageRunning, Summary: "replayed output", CreatedAt: time.Now().UTC()}
	syncer.handleEvent(ctx, target, event)
	syncer.handleEvent(ctx, target, event)
	if got := len(sink.conversationEvents()); got != 1 {
		t.Fatalf("expected one forwarded event after replay, got %d", got)
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].NativeEventID != event.ID {
		t.Fatalf("expected one persisted native event, got %+v", events)
	}
}

func TestReverseSyncerRetriesDurableEventAfterTransportFailure(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-durable", PluginID: "openclaw",
		NativeSessionID: "native-durable", NativeThreadID: "native-durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-durable"}}
	event := pluginruntime.PluginEvent{ID: "hook-durable-1", Type: "conversation.history.changed", Status: StageRunning, Summary: "offline content", Payload: map[string]any{"prism_outbox": true}, CreatedAt: time.Now().UTC()}
	adapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter), Sink: &failingControlSink{err: errors.New("transport unavailable")}}
	syncer.handleEvent(ctx, target, event)
	if len(adapter.acked) != 0 {
		t.Fatalf("failed transport must not ack plugin event: %+v", adapter.acked)
	}
	delivered, err := db.NativeEventDelivered(ctx, record.ID, event.ID)
	if err != nil || delivered {
		t.Fatalf("failed transport must leave native event pending, delivered=%v err=%v", delivered, err)
	}
	sink := &recordingSink{}
	syncer.Sink = sink
	syncer.handleEvent(ctx, target, event)
	syncer.handleEvent(ctx, target, event)
	if got := len(sink.conversationEvents()); got != 1 {
		t.Fatalf("expected one successful retry, got %d", got)
	}
	if len(adapter.acked) != 2 || adapter.acked[0] != event.ID || adapter.acked[1] != event.ID {
		t.Fatalf("successful and duplicate retries must ack the same event ID: %+v", adapter.acked)
	}
	delivered, err = db.NativeEventDelivered(ctx, record.ID, event.ID)
	if err != nil || !delivered {
		t.Fatalf("successful retry must be durably marked, delivered=%v err=%v", delivered, err)
	}
}

func TestReverseSyncerAcknowledgesOnlyAfterConfirmedWrite(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-confirmed", PluginID: "openclaw",
		NativeSessionID: "native-confirmed", NativeThreadID: "native-confirmed",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-confirmed"}}
	event := pluginruntime.PluginEvent{ID: "hook-confirmed-1", Type: "conversation.history.changed", Status: StageRunning, Summary: "event", Payload: map[string]any{"prism_outbox": true}, CreatedAt: time.Now().UTC()}
	adapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	sink := &confirmedRecordingSink{err: errors.New("socket write failed")}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter), Sink: sink}
	syncer.handleEvent(ctx, target, event)
	if len(adapter.acked) != 0 || sink.fallbackCount != 0 || len(sink.messages) != 1 {
		t.Fatalf("failed WebSocket write must retain outbox and avoid fallback: acked=%v fallback=%d messages=%d", adapter.acked, sink.fallbackCount, len(sink.messages))
	}
	delivered, err := db.NativeEventDelivered(ctx, record.ID, event.ID)
	if err != nil || delivered {
		t.Fatalf("failed write marked delivered: delivered=%v err=%v", delivered, err)
	}
	sink.err = nil
	syncer.handleEvent(ctx, target, event)
	if len(adapter.acked) != 1 || adapter.acked[0] != event.ID || len(sink.messages) != 2 || sink.messages[0].MessageID != sink.messages[1].MessageID {
		t.Fatalf("retry must use stable ID and ack only on confirmed write: acked=%v messages=%+v", adapter.acked, sink.messages)
	}
	delivered, err = db.NativeEventDelivered(ctx, record.ID, event.ID)
	if err != nil || !delivered {
		t.Fatalf("confirmed write not marked delivered: delivered=%v err=%v", delivered, err)
	}
}

func TestReverseSyncerPrefersServerReceiptOverSocketWrite(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-server-receipt", PluginID: "openclaw",
		NativeSessionID: "native-server-receipt", NativeThreadID: "native-server-receipt",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-server-receipt"}}
	event := pluginruntime.PluginEvent{ID: "hook-server-receipt-1", Type: "conversation.history.changed", Status: StageRunning, Summary: "event", Payload: map[string]any{"prism_outbox": true}, CreatedAt: time.Now().UTC()}
	adapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	sink := &acknowledgedRecordingSink{ackErr: errors.New("server receipt missing")}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(adapter), Sink: sink}
	syncer.handleEvent(ctx, target, event)
	if len(adapter.acked) != 0 || sink.writeOnlyCount != 0 || sink.ackCount != 1 {
		t.Fatalf("local socket write must not ack plugin event: acked=%v writeOnly=%d receipts=%d", adapter.acked, sink.writeOnlyCount, sink.ackCount)
	}
	sink.ackErr = nil
	syncer.handleEvent(ctx, target, event)
	if len(adapter.acked) != 1 || adapter.acked[0] != event.ID || sink.ackCount != 2 {
		t.Fatalf("server receipt must complete plugin event: acked=%v receipts=%d", adapter.acked, sink.ackCount)
	}
}

func TestReverseSyncerReplaysPendingEventAcrossDatabaseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prism.sqlite")
	ctx := context.Background()
	firstDB, err := bridgedb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record, err := firstDB.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-restart", PluginID: "openclaw",
		NativeSessionID: "native-restart", NativeThreadID: "native-restart",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-restart"}}
	event := pluginruntime.PluginEvent{ID: "hook-restart-1", Type: "conversation.history.changed", Status: StageRunning, Summary: "offline content", Payload: map[string]any{"prism_outbox": true}, CreatedAt: time.Now().UTC()}
	firstAdapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	(&ReverseSyncer{Config: remoteAuthorizedConfig(), DB: firstDB, Service: pluginruntime.New(firstAdapter), Sink: &acknowledgedRecordingSink{ackErr: errors.New("receipt lost")}}).handleEvent(ctx, target, event)
	if len(firstAdapter.acked) != 0 {
		t.Fatal("lost server receipt acknowledged plugin before restart")
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	secondDB, err := bridgedb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	secondAdapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	secondSink := &acknowledgedRecordingSink{}
	(&ReverseSyncer{Config: remoteAuthorizedConfig(), DB: secondDB, Service: pluginruntime.New(secondAdapter), Sink: secondSink}).handleEvent(ctx, target, event)
	if secondSink.ackCount != 1 || len(secondAdapter.acked) != 1 {
		t.Fatalf("pending event not forwarded after DB reopen: receipt=%d pluginAck=%v", secondSink.ackCount, secondAdapter.acked)
	}
	if err := secondDB.Close(); err != nil {
		t.Fatal(err)
	}

	thirdDB, err := bridgedb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer thirdDB.Close()
	thirdAdapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	thirdSink := &acknowledgedRecordingSink{}
	(&ReverseSyncer{Config: remoteAuthorizedConfig(), DB: thirdDB, Service: pluginruntime.New(thirdAdapter), Sink: thirdSink}).handleEvent(ctx, target, event)
	if thirdSink.ackCount != 0 || len(thirdAdapter.acked) != 1 {
		t.Fatalf("delivered event was forwarded after DB reopen: receipt=%d pluginAck=%v", thirdSink.ackCount, thirdAdapter.acked)
	}
}

func TestReverseSyncerRetriesAfterLostWebSocketReceipt(t *testing.T) {
	frames := make(chan prismtransport.ControlOutbound, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for i := 0; i < 2; i++ {
			var frame prismtransport.ControlOutbound
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			frames <- frame
			if i == 1 {
				_ = conn.WriteJSON(map[string]any{
					"message_id": frame.MessageID, "message_type": prismtransport.MsgConversationEventAck,
					"payload": map[string]any{"accepted": true},
				})
			}
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	connected := make(chan struct{}, 1)
	client := prismtransport.New(prismtransport.Options{
		URL:       "ws" + strings.TrimPrefix(server.URL, "http"),
		OnConnect: func(context.Context, *prismtransport.Client) { connected <- struct{}{} },
	})
	clientCtx, stopClient := context.WithCancel(context.Background())
	defer stopClient()
	go func() { _ = client.Run(clientCtx) }()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("Realtime client did not connect")
	}
	path := filepath.Join(t.TempDir(), "prism.sqlite")
	db, err := bridgedb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
		PrismConversationID: "conv-lost-receipt", PluginID: "openclaw",
		NativeSessionID: "native-lost-receipt", NativeThreadID: "native-lost-receipt",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reverseSyncTarget{record: record, session: pluginruntime.NativeSession{PluginID: "openclaw", NativeSessionID: "native-lost-receipt"}}
	event := pluginruntime.PluginEvent{ID: "hook-lost-receipt-1", Type: "conversation.history.changed", Status: StageRunning, Summary: "event", Payload: map[string]any{"prism_outbox": true}, CreatedAt: time.Now().UTC()}
	firstAdapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		(&ReverseSyncer{Config: remoteAuthorizedConfig(), DB: db, Service: pluginruntime.New(firstAdapter), Sink: client}).handleEvent(firstCtx, target, event)
		close(firstDone)
	}()
	var firstFrame prismtransport.ControlOutbound
	select {
	case firstFrame = <-frames:
	case <-time.After(2 * time.Second):
		t.Fatal("first event was not written")
	}
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("lost receipt did not release first attempt")
	}
	if len(firstAdapter.acked) != 0 {
		t.Fatal("lost receipt acknowledged plugin event")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := bridgedb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	secondAdapter := &acknowledgingReverseAdapter{reverseAdapter: &reverseAdapter{id: "openclaw"}}
	(&ReverseSyncer{Config: remoteAuthorizedConfig(), DB: reopened, Service: pluginruntime.New(secondAdapter), Sink: client}).handleEvent(context.Background(), target, event)
	select {
	case secondFrame := <-frames:
		if secondFrame.MessageID != firstFrame.MessageID || secondFrame.MessageType != prismtransport.MsgConversationEvent {
			t.Fatalf("retry changed event identity: first=%+v second=%+v", firstFrame, secondFrame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending event was not retried")
	}
	if len(secondAdapter.acked) != 1 {
		t.Fatalf("server receipt did not acknowledge plugin event: %v", secondAdapter.acked)
	}
}

type acknowledgedRecordingSink struct {
	ackErr         error
	ackCount       int
	writeOnlyCount int
}

func (s *acknowledgedRecordingSink) Send(prismtransport.Outbound) error { return nil }
func (s *acknowledgedRecordingSink) SendControl(prismtransport.ControlOutbound) error {
	return nil
}
func (s *acknowledgedRecordingSink) SendControlConfirmed(context.Context, prismtransport.ControlOutbound) error {
	s.writeOnlyCount++
	return nil
}
func (s *acknowledgedRecordingSink) SendControlAcknowledged(context.Context, prismtransport.ControlOutbound) error {
	s.ackCount++
	return s.ackErr
}

type confirmedRecordingSink struct {
	err           error
	messages      []prismtransport.ControlOutbound
	fallbackCount int
}

func (s *confirmedRecordingSink) Send(prismtransport.Outbound) error { return nil }

func (s *confirmedRecordingSink) SendControl(prismtransport.ControlOutbound) error {
	s.fallbackCount++
	return nil
}

func (s *confirmedRecordingSink) SendControlConfirmed(_ context.Context, msg prismtransport.ControlOutbound) error {
	s.messages = append(s.messages, msg)
	return s.err
}

type acknowledgingReverseAdapter struct {
	*reverseAdapter
	acked []string
}

func (a *acknowledgingReverseAdapter) AckEvent(_ context.Context, _ pluginruntime.NativeSession, eventID string) error {
	a.acked = append(a.acked, eventID)
	return nil
}

func TestReverseSyncerUsesOnePluginWideWatcher(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	for _, id := range []string{"1", "2", "3"} {
		if _, err := db.UpsertSession(context.Background(), bridgedb.SessionRecord{
			PrismConversationID: "conv-" + id,
			PluginID:            "codex",
			NativeSessionID:     "native-" + id,
			NativeThreadID:      "thread-" + id,
			Surface:             "codex-desktop",
		}); err != nil {
			t.Fatalf("UpsertSession %s: %v", id, err)
		}
	}
	adapter := &reverseAdapter{
		id:         "codex",
		wide:       true,
		events:     make(chan pluginruntime.PluginEvent),
		wideEvents: make(chan pluginruntime.PluginEvent, 1),
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), Service: pluginruntime.New(adapter), DB: db, Sink: sink}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncer.sync(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.wideSubscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.wideSubscribeCountValue() != 1 || adapter.subscribeCountValue() != 0 {
		t.Fatalf("expected one plugin-wide watcher and no per-session subscriptions, wide=%d session=%d", adapter.wideSubscribeCountValue(), adapter.subscribeCountValue())
	}
	adapter.wideEvents <- pluginruntime.PluginEvent{
		ID: "wide-event-2", Type: "desktop.state.changed", Status: StageRunning, Summary: "Codex active",
		Payload: map[string]any{"native_session": map[string]any{
			"plugin_id": "codex", "native_session_id": "native-2", "native_thread_id": "thread-2", "surface": "codex-desktop",
		}},
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(sink.conversationEvents()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	events := sink.conversationEvents()
	if len(events) != 1 || events[0].ConversationID != "conv-2" {
		t.Fatalf("expected plugin-wide event routed to native binding conv-2, got %+v", events)
	}
}

func TestReverseSyncerReplacesPluginWideWatcherAfterAdapterReload(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	first := &reverseAdapter{
		id:         "codex",
		wide:       true,
		wideEvents: make(chan pluginruntime.PluginEvent),
	}
	second := &reverseAdapter{
		id:         "codex",
		wide:       true,
		wideEvents: make(chan pluginruntime.PluginEvent),
	}
	service := pluginruntime.New(first)
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), Service: service, DB: db, Sink: &recordingSink{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncer.sync(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && first.wideSubscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if first.wideSubscribeCountValue() != 1 {
		t.Fatalf("expected initial plugin-wide subscription, got %d", first.wideSubscribeCountValue())
	}

	service.Replace(second)
	syncer.sync(ctx)
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && second.wideSubscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if second.wideSubscribeCountValue() != 1 {
		t.Fatalf("expected watcher to move to replacement adapter, got %d", second.wideSubscribeCountValue())
	}

	syncer.mu.Lock()
	handle := syncer.active["codex/@plugin-wide"]
	syncer.mu.Unlock()
	if handle == nil || !samePluginAdapter(handle.adapter, second) {
		t.Fatalf("active watcher must belong to replacement adapter, handle=%+v", handle)
	}
}

func TestReverseSyncerDropsBufferedEventFromReplacedAdapter(t *testing.T) {
	oldEvents := make(chan pluginruntime.PluginEvent, 1)
	oldAdapter := &reverseAdapter{id: "codex", wide: true, wideEvents: oldEvents}
	currentAdapter := &reverseAdapter{id: "codex", wide: true, wideEvents: make(chan pluginruntime.PluginEvent)}
	service := pluginruntime.New(currentAdapter)
	sink := &recordingSink{}
	syncer := &ReverseSyncer{Service: service, Sink: sink}
	oldEvents <- pluginruntime.PluginEvent{ID: "stale-usage", Type: "agent.usage.updated"}

	syncer.runTarget(
		context.Background(),
		"codex/@plugin-wide",
		&reverseSyncHandle{adapter: oldAdapter},
		reverseSyncTarget{pluginWide: true, session: pluginruntime.NativeSession{PluginID: "codex"}},
	)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.controls) != 0 {
		t.Fatalf("replaced runtime published a buffered event: %+v", sink.controls)
	}
}

func TestReverseSyncerDoesNotPublishEventAfterPluginIsRemoteDisabled(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-disabled-reverse",
		PluginID:            "openclaw",
		NativeSessionID:     "native-disabled-reverse",
		NativeThreadID:      "thread-disabled-reverse",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.RemoteAuthorizationReady = true
	cfg.Bridge.RemoteAuthorizations = map[string]bool{}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{Config: cfg, DB: db, Sink: sink}

	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "openclaw",
			NativeSessionID: "native-disabled-reverse",
			NativeThreadID:  "thread-disabled-reverse",
		},
	}, pluginruntime.PluginEvent{
		ID:      "event-disabled-reverse",
		Type:    "message.assistant",
		Status:  StageRunning,
		Summary: "must remain local",
	})

	if events := sink.conversationEvents(); len(events) != 0 {
		t.Fatalf("expected disabled plugin event to stay out of realtime, got %+v", events)
	}
	persisted, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(persisted) != 1 || persisted[0].NativeEventID != "event-disabled-reverse" {
		t.Fatalf("expected local reverse event persistence, got %+v", persisted)
	}
}

func TestReverseSyncerPersistsEventWhenConversationEventSendFails(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-fail",
		PluginID:            "openclaw",
		NativeSessionID:     "native-fail",
		NativeThreadID:      "thread-fail",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	sink := &failingControlSink{err: errors.New("send failed")}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
	}

	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "openclaw",
			NativeSessionID: "native-fail",
			NativeThreadID:  "thread-fail",
		},
	}, pluginruntime.PluginEvent{
		ID:      "event-fail",
		Type:    "message.assistant",
		Status:  StageRunning,
		Summary: "conversation event failed",
		Payload: map[string]any{"task_id": "native-task-fail"},
	})

	if sink.sendControlCount() != 1 {
		t.Fatalf("expected one conversation event send attempt, got %d", sink.sendControlCount())
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].NativeEventID != "event-fail" || events[0].PrismConversationID != "conv-fail" {
		t.Fatalf("expected persisted event despite conversation event send failure, got %+v", events)
	}
}

func TestReverseSyncerSuppressesConversationEventForTrackedForwardRun(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-forward",
		PluginID:            "openclaw",
		NativeSessionID:     "native-forward",
		NativeThreadID:      "thread-forward",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-forward",
		ConversationID:  "conv-forward",
		PluginID:        "openclaw",
		NativeSessionID: "native-forward",
		NativeThreadID:  "thread-forward",
		NativeRunID:     "run-forward",
	})
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
		Runs:   tracker,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "openclaw",
			NativeSessionID: "native-forward",
			NativeThreadID:  "thread-forward",
		},
	}, pluginruntime.PluginEvent{
		ID:      "run-forward",
		Type:    "run.completed",
		Status:  StageCompleted,
		Summary: "suppressed final",
		Payload: map[string]any{"runId": "run-forward"},
	})
	if len(sink.conversationEvents()) != 0 {
		t.Fatalf("expected tracked forward run to suppress conversation event, got %+v", sink.conversationEvents())
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].NativeEventID != "run-forward" {
		t.Fatalf("expected suppressed event persisted, got %+v", events)
	}
}

func TestReverseSyncerSuppressesActiveConversationTerminalEventEvenWhenRunIDDoesNotMatch(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-hermes-active",
		PluginID:            "hermes",
		NativeSessionID:     "native-hermes-active",
		NativeThreadID:      "thread-hermes-active",
		Surface:             "gateway",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-hermes-active",
		ConversationID:  "conv-hermes-active",
		PluginID:        "hermes",
		NativeSessionID: "native-hermes-active",
		NativeThreadID:  "thread-hermes-active",
		NativeRunID:     "req-hermes-active",
	})
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
		Runs:   tracker,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "hermes",
			NativeSessionID: "native-hermes-active",
			NativeThreadID:  "thread-hermes-active",
		},
	}, pluginruntime.PluginEvent{
		ID:      "evt-hermes-complete",
		Type:    "run.completed",
		Status:  StageCompleted,
		Summary: "Hermes complete",
		Payload: map[string]any{"step": "planning"},
	})
	if len(sink.conversationEvents()) != 0 {
		t.Fatalf("expected active conversation terminal event suppressed, got %+v", sink.conversationEvents())
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].NativeEventID != "evt-hermes-complete" {
		t.Fatalf("expected suppressed active terminal event persisted, got %+v", events)
	}
}

func TestReverseSyncerAllowsConversationEventForUntrackedEvent(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-later",
		PluginID:            "openclaw",
		NativeSessionID:     "native-other",
		NativeThreadID:      "thread-other",
		Surface:             "gateway",
		Endpoint:            "test://openclaw",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	tracker := NewRunTracker()
	tracker.Track(ForwardRun{
		RequestID:       "req-other",
		ConversationID:  "conv-other",
		PluginID:        "openclaw",
		NativeSessionID: "native-other",
		NativeThreadID:  "thread-other",
		NativeRunID:     "run-owned",
	})
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
		Runs:   tracker,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "openclaw",
			NativeSessionID: "native-other-2",
			NativeThreadID:  "thread-other-2",
		},
	}, pluginruntime.PluginEvent{
		ID:      "run-later",
		Type:    "message.assistant",
		Status:  StageRunning,
		Summary: "desktop follow-up",
		Payload: map[string]any{"runId": "run-later"},
	})
	if !containsString(sink.conversationSummaries(), "desktop follow-up") {
		t.Fatalf("expected untracked event to emit conversation event, got %+v", sink.conversationSummaries())
	}
}

func TestReverseSyncerSuppressesSessionInfoEventButStillPersists(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-session-info",
		PluginID:            "hermes",
		NativeSessionID:     "native-session-info",
		NativeThreadID:      "thread-session-info",
		Surface:             "gateway",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "hermes",
			NativeSessionID: "native-session-info",
			NativeThreadID:  "thread-session-info",
		},
	}, pluginruntime.PluginEvent{
		ID:      "session-info-1",
		Type:    "session.info",
		Status:  StageRunning,
		Summary: "Hermes session metadata updated",
	})
	if len(sink.conversationEvents()) != 0 {
		t.Fatalf("expected session.info to suppress conversation event, got %+v", sink.conversationEvents())
	}
	events, err := db.ListRecentEvents(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "session.info" {
		t.Fatalf("expected session.info persisted, got %+v", events)
	}
}

func TestReverseSyncerBuildsStructuredApprovalEnvelope(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-approval",
		PluginID:            "codex",
		NativeSessionID:     "native-approval",
		NativeThreadID:      "thread-approval",
		Surface:             "app-server",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config: remoteAuthorizedConfig(),
		DB:     db,
		Sink:   sink,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "codex",
			NativeSessionID: "native-approval",
			NativeThreadID:  "thread-approval",
			Surface:         "app-server",
		},
	}, pluginruntime.PluginEvent{
		ID:      "approval-1",
		Type:    "approval.required",
		Status:  StageWaitingApproval,
		Summary: "need desktop approval",
		Payload: map[string]any{
			"method":              "item/commandExecution/requestApproval",
			"approval_request_id": "approval-1",
			"turn_id":             "turn-1",
		},
	})
	events := sink.conversationEvents()
	if len(events) != 1 {
		t.Fatalf("expected one approval conversation event, got %+v", events)
	}
	approvalPayload, ok := events[0].Payload["approval"].(map[string]any)
	if !ok {
		t.Fatalf("expected approval payload, got %+v", events[0].Payload)
	}
	if approvalPayload["id"] != "approval-1" || approvalPayload["method"] != "item/commandExecution/requestApproval" {
		t.Fatalf("unexpected approval payload: %+v", approvalPayload)
	}
}

func TestReverseSyncerPreservesLiveDetailSnapshot(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-live-detail",
		PluginID:            "codex",
		NativeSessionID:     "native-live-detail",
		NativeThreadID:      "thread-live-detail",
		Surface:             "app-server",
		Cwd:                 "/tmp/project",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	adapter := &reverseAdapter{
		id: "codex",
	}
	sink := &recordingSink{}
	store := NewDetailSnapshotStore()
	syncer := &ReverseSyncer{
		Config:          remoteAuthorizedConfig(),
		Service:         pluginruntime.New(adapter),
		DB:              db,
		Sink:            sink,
		DetailSnapshots: store,
	}
	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "codex",
			NativeSessionID: "native-live-detail",
			NativeThreadID:  "thread-live-detail",
			Cwd:             "/tmp/project",
			Surface:         "app-server",
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
		t.Fatal("expected reverse sync live detail snapshot")
	}
	if stringFromAny(detail["primary_action"]) != "queue" {
		t.Fatalf("unexpected primary_action in reverse sync detail: %+v", detail)
	}
	run := asMap(detail["run"])
	if run["status"] != StageRunning || run["primary_action"] != "queue" {
		t.Fatalf("unexpected run in reverse sync detail: %+v", run)
	}
	currentModel := asMap(detail["current_model"])
	if currentModel["id"] != "gpt-5.6" {
		t.Fatalf("unexpected model in reverse sync detail: %+v", currentModel)
	}
	currentReasoning := asMap(detail["current_reasoning"])
	if currentReasoning["id"] != "high" {
		t.Fatalf("unexpected reasoning in reverse sync detail: %+v", currentReasoning)
	}
	if stringFromAny(detail["messages_signature"]) != "msgsig-live" {
		t.Fatalf("expected live messages_signature in reverse sync detail: %+v", detail)
	}
	if adapter.listSessionsCount() != 0 {
		t.Fatalf("live watcher detail must not rebuild the conversation directory, listSessions=%d", adapter.listSessionsCount())
	}
	stored, ok := store.GetForRuntime("codex", "conv-live-detail")
	if !ok || stringFromAny(stored["primary_action"]) != "queue" {
		t.Fatalf("expected reverse sync detail snapshot store updated: %+v", stored)
	}
}

func TestReverseSyncerRebuildsIndexOnlyForPluginWideDirectoryReconciliation(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &reverseAdapter{
		id: "codex",
		hints: []pluginruntime.NativeSessionHint{{
			PluginID: "codex", NativeSessionID: "native-directory", NativeThreadID: "thread-directory", Title: "Desktop thread",
		}},
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config:  remoteAuthorizedConfig(),
		Service: pluginruntime.New(adapter),
		DB:      db,
		Sink:    sink,
	}
	syncer.handleEvent(context.Background(), reverseSyncTarget{
		pluginWide: true,
		session:    pluginruntime.NativeSession{PluginID: "codex"},
	}, pluginruntime.PluginEvent{Type: "desktop.session.directory.reconciled"})
	if got := adapter.listSessionsCount(); got != 1 {
		t.Fatalf("directory reconciliation must rebuild exactly once, listSessions=%d", got)
	}
	if snapshots := sink.conversationIndexSnapshots(); len(snapshots) != 1 {
		t.Fatalf("expected one complete index snapshot, got %+v", snapshots)
	}
	if events := sink.conversationEvents(); len(events) != 0 {
		t.Fatalf("directory reconciliation must not emit a synthetic conversation event, got %+v", events)
	}
}

func TestReverseSyncerPublishesPluginWideDirectoryItemWithoutConversationEvent(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	adapter := &reverseAdapter{
		id: "codex",
		hints: []pluginruntime.NativeSessionHint{{
			PluginID: "codex", NativeSessionID: "native-directory-item", NativeThreadID: "thread-directory-item", Title: "Original title",
		}},
	}
	sink := &recordingSink{}
	syncer := &ReverseSyncer{Config: remoteAuthorizedConfig(), Service: pluginruntime.New(adapter), DB: db, Sink: sink}
	root := reverseSyncTarget{pluginWide: true, session: pluginruntime.NativeSession{PluginID: "codex"}}
	syncer.handleEvent(context.Background(), root, pluginruntime.PluginEvent{Type: "desktop.session.directory.reconciled"})
	syncer.handleEvent(context.Background(), root, pluginruntime.PluginEvent{
		Type: "desktop.session.index.changed",
		Payload: map[string]any{
			"native_session": map[string]any{
				"plugin_id": "codex", "native_session_id": "native-directory-item", "native_thread_id": "thread-directory-item", "surface": "codex-desktop",
			},
			"session_hint": map[string]any{
				"plugin_id": "codex", "native_session_id": "native-directory-item", "native_thread_id": "thread-directory-item", "surface": "codex-desktop", "title": "Renamed title",
			},
		},
	})
	if got := adapter.listSessionsCount(); got != 1 {
		t.Fatalf("directory item update must use the initialized index, listSessions=%d", got)
	}
	if events := sink.conversationEvents(); len(events) != 0 {
		t.Fatalf("directory item update must not emit a conversation event, got %+v", events)
	}
	snapshots := sink.conversationIndexSnapshots()
	if len(snapshots) != 2 || snapshots[1].Plugins[0].Conversations[0].Title != "Renamed title" {
		t.Fatalf("directory item did not update the complete index: %+v", snapshots)
	}
}

func TestReverseSyncerClearsDetailWhenForegroundWatcherIsUnavailable(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-watcher-unavailable",
		"desktop_foreground": true,
		"detail_stale":       false,
	})
	sink := &recordingSink{}
	syncer := &ReverseSyncer{
		Config:          remoteAuthorizedConfig(),
		Sink:            sink,
		DetailSnapshots: store,
	}
	syncer.handleEvent(context.Background(), reverseSyncTarget{
		record: bridgedb.SessionRecord{
			PrismConversationID: "conv-watcher-unavailable",
			PluginID:            "codex",
		},
		session: pluginruntime.NativeSession{PluginID: "codex"},
	}, pluginruntime.PluginEvent{
		Type: "watcher.foreground.unavailable",
		Payload: map[string]any{
			"watcher": "foreground",
			"code":    "foreground_watcher_unavailable",
		},
	})
	if !store.IsForegroundWatcherUnavailable("codex") {
		t.Fatal("expected reverse sync watcher failure to mark the Plugin unavailable")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.controls) != 1 || sink.controls[0].MessageType != prismtransport.MsgConversationDetailUnavailable {
		t.Fatalf("expected only detail unavailable control, got %+v", sink.controls)
	}
}

func TestReverseSyncerComposerDetailCarriesAndClearsCanonicalDraft(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	record, err := db.UpsertSession(ctx, bridgedb.SessionRecord{
		PrismConversationID: "conv-draft-canonical-0801",
		PluginID:            "codex",
		NativeSessionID:     "native-draft-canonical-0801",
		NativeThreadID:      "thread-draft-canonical-0801",
		Surface:             "app-server",
		Cwd:                 "/tmp/project",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	sink := &recordingSink{}
	store := NewDetailSnapshotStore()
	syncer := &ReverseSyncer{
		Config:          remoteAuthorizedConfig(),
		Service:         pluginruntime.New(&reverseAdapter{id: "codex"}),
		DB:              db,
		Sink:            sink,
		DetailSnapshots: store,
	}

	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "codex",
			NativeSessionID: "native-draft-canonical-0801",
			NativeThreadID:  "thread-draft-canonical-0801",
			Cwd:             "/tmp/project",
			Surface:         "app-server",
		},
	}, pluginruntime.PluginEvent{
		ID:      "desktop-state-draft-1",
		Type:    "desktop.state.changed",
		Status:  StageRunning,
		Summary: "desktop composer mirrored queued draft",
		Payload: map[string]any{
			"detail_snapshot": map[string]any{
				"composer": map[string]any{
					"content":  "MOBILE_QUEUE_DRAFT_EDIT_0801",
					"editable": true,
				},
			},
		},
	})

	syncer.handleEvent(ctx, reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        "codex",
			NativeSessionID: "native-draft-canonical-0801",
			NativeThreadID:  "thread-draft-canonical-0801",
			Cwd:             "/tmp/project",
			Surface:         "app-server",
		},
	}, pluginruntime.PluginEvent{
		ID:      "desktop-state-draft-2",
		Type:    "desktop.state.changed",
		Status:  "idle",
		Summary: "desktop composer cleared after native send",
		Payload: map[string]any{
			"detail_snapshot": map[string]any{
				"composer": map[string]any{
					"content":  "",
					"editable": false,
				},
			},
		},
	})

	snapshots := sink.conversationDetailSnapshots()
	if len(snapshots) != 2 {
		t.Fatalf("expected two detail snapshots, got %+v", snapshots)
	}
	firstComposer := asMap(snapshots[0]["composer"])
	if firstComposer["content"] != "MOBILE_QUEUE_DRAFT_EDIT_0801" || !firstNonEmptyBool(firstComposer["editable"]) {
		t.Fatalf("expected first live composer to mirror queued draft, got %#v", firstComposer)
	}
	secondComposer := asMap(snapshots[1]["composer"])
	if secondComposer["content"] != "" || firstNonEmptyBool(secondComposer["editable"]) {
		t.Fatalf("expected second live composer to clear the canonical draft, got %#v", secondComposer)
	}
	stored, ok := store.GetForRuntime("codex", "conv-draft-canonical-0801")
	if !ok {
		t.Fatal("expected canonical detail snapshot stored")
	}
	composer := asMap(stored["composer"])
	if composer["content"] != "" || firstNonEmptyBool(composer["editable"]) {
		t.Fatalf("expected stored canonical composer cleared after empty desktop content, got %#v", composer)
	}
}

func TestReverseSyncerIgnoresUnmanagedSessionHints(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.Plugins["openclaw"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncAllThreads}
	adapter := &reverseAdapter{
		id:     "openclaw",
		events: make(chan pluginruntime.PluginEvent, 1),
		hints: []pluginruntime.NativeSessionHint{
			{PluginID: "openclaw", NativeSessionID: "private-1", Visible: true},
			{PluginID: "openclaw", NativeSessionID: "agent:main:prism:conv-managed", Visible: true},
		},
	}
	syncer := &ReverseSyncer{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
		DB:      db,
		Sink:    &recordingSink{},
	}
	syncer.sync(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 1 || adapter.lastSessionValue().NativeSessionID != "agent:main:prism:conv-managed" {
		t.Fatalf("expected only managed hint to be subscribed, count=%d session=%+v", adapter.subscribeCountValue(), adapter.lastSessionValue())
	}
	sessions, err := db.ListSessions(ctx, "openclaw")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].PrismConversationID != "conv-managed" {
		t.Fatalf("expected managed hint persisted with conversation id, got %+v", sessions)
	}
}

func TestReverseSyncerPrismManagedOnlySkipsHints(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncPrismManagedOnly}
	adapter := &reverseAdapter{
		id:     "codex",
		events: make(chan pluginruntime.PluginEvent, 1),
		hints: []pluginruntime.NativeSessionHint{
			{PluginID: "codex", NativeSessionID: "agent:main:prism:conv-managed", Visible: true},
		},
	}
	syncer := &ReverseSyncer{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
		DB:      db,
		Sink:    &recordingSink{},
	}
	syncer.sync(context.Background())
	time.Sleep(100 * time.Millisecond)
	if adapter.subscribeCountValue() != 0 {
		t.Fatalf("prism_managed_only should not subscribe unmanaged hints, got %d", adapter.subscribeCountValue())
	}
	sessions, err := db.ListSessions(context.Background(), "codex")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("prism_managed_only should not persist hints, got %+v", sessions)
	}
}

func TestReverseSyncerAllThreadsAcceptsManagedHints(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer db.Close()
	cfg := remoteAuthorizedConfig()
	cfg.Bridge.Plugins["codex"] = bridgeconfig.PluginConfig{ReverseSync: pluginruntime.ReverseSyncAllThreads}
	adapter := &reverseAdapter{
		id:     "codex",
		events: make(chan pluginruntime.PluginEvent, 1),
		hints: []pluginruntime.NativeSessionHint{
			{PluginID: "codex", NativeSessionID: "private-1", Visible: true},
			{PluginID: "codex", NativeSessionID: "agent:main:prism:conv-managed", NativeThreadID: "thread-managed", Visible: true},
		},
	}
	syncer := &ReverseSyncer{
		Service: pluginruntime.New(adapter),
		Config:  cfg,
		DB:      db,
		Sink:    &recordingSink{},
	}
	syncer.sync(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && adapter.subscribeCountValue() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if adapter.subscribeCountValue() != 1 || adapter.lastSessionValue().NativeSessionID != "agent:main:prism:conv-managed" {
		t.Fatalf("expected only managed hint subscribed in all_threads mode, count=%d session=%+v", adapter.subscribeCountValue(), adapter.lastSessionValue())
	}
}

func TestTargetFromRecordClearsStaleLoopbackEndpointForAnyPlugin(t *testing.T) {
	first := targetFromRecord(bridgedb.SessionRecord{
		PluginID:        "first-plugin",
		NativeSessionID: "s1",
		NativeThreadID:  "t1",
		Endpoint:        "http://127.0.0.1:64295",
	})
	if first.session.Endpoint != "" {
		t.Fatalf("expected loopback endpoint cleared, got %q", first.session.Endpoint)
	}
	second := targetFromRecord(bridgedb.SessionRecord{
		PluginID:        "another-plugin",
		NativeSessionID: "s2",
		NativeThreadID:  "t2",
		Endpoint:        "http://localhost:18789",
	})
	if second.session.Endpoint != "" {
		t.Fatalf("expected localhost endpoint cleared, got %q", second.session.Endpoint)
	}
	codex := targetFromRecord(bridgedb.SessionRecord{
		PluginID:        "codex",
		NativeSessionID: "s3",
		NativeThreadID:  "t3",
		Endpoint:        "appserver://cached",
	})
	if codex.session.Endpoint != "appserver://cached" {
		t.Fatalf("expected codex endpoint kept, got %q", codex.session.Endpoint)
	}
}

type reverseAdapter struct {
	id     string
	events chan pluginruntime.PluginEvent
	hints  []pluginruntime.NativeSessionHint

	controlResult pluginruntime.SessionControlResult
	wide          bool
	wideEvents    chan pluginruntime.PluginEvent

	mu                 sync.Mutex
	subscribeCount     int
	wideSubscribeCount int
	listSessionsCalls  int
	lastSession        pluginruntime.NativeSession
}

func (a *reverseAdapter) ID() string { return a.id }

func (a *reverseAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{
		PluginID:           a.id,
		Available:          true,
		NativeVisibleInput: true,
		CanReverseSync:     true,
		CanPluginWideWatch: a.wide,
		VisibilitySurface:  "test",
	}, nil
}

func (a *reverseAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: a.id, Verified: true}, nil
}

func (a *reverseAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, nil
}

func (a *reverseAdapter) Subscribe(_ context.Context, session pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subscribeCount++
	a.lastSession = session
	return a.events, nil
}

func (a *reverseAdapter) SubscribePlugin(context.Context) (<-chan pluginruntime.PluginEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.wideSubscribeCount++
	return a.wideEvents, nil
}

func (a *reverseAdapter) subscribeCountValue() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.subscribeCount
}

func (a *reverseAdapter) lastSessionValue() pluginruntime.NativeSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastSession
}

func (a *reverseAdapter) wideSubscribeCountValue() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.wideSubscribeCount
}

func (a *reverseAdapter) ListSessions(context.Context) ([]pluginruntime.NativeSessionHint, error) {
	a.mu.Lock()
	a.listSessionsCalls++
	a.mu.Unlock()
	return a.hints, nil
}

func (a *reverseAdapter) listSessionsCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.listSessionsCalls
}

func (a *reverseAdapter) ControlSession(context.Context, pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	result := a.controlResult
	if !result.OK {
		result.OK = true
	}
	return result, nil
}

func (a *reverseAdapter) Interrupt(context.Context, pluginruntime.NativeSession, string) error {
	return nil
}

func (a *reverseAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{}, nil
}

func (a *reverseAdapter) Close(context.Context) error { return nil }

type restartingReverseAdapter struct {
	id string

	mu             sync.Mutex
	subscribeCount int
	lastSession    pluginruntime.NativeSession
	lastEvents     chan pluginruntime.PluginEvent
}

func (a *restartingReverseAdapter) ID() string { return a.id }

func (a *restartingReverseAdapter) Probe(context.Context) (pluginruntime.Capability, error) {
	return pluginruntime.Capability{
		PluginID:           a.id,
		Available:          true,
		NativeVisibleInput: true,
		CanReverseSync:     true,
		VisibilitySurface:  "test",
	}, nil
}

func (a *restartingReverseAdapter) Discover(context.Context) (pluginruntime.DiscoveryResult, error) {
	return pluginruntime.DiscoveryResult{PluginID: a.id, Verified: true}, nil
}

func (a *restartingReverseAdapter) Send(context.Context, pluginruntime.NativeSession, pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	return pluginruntime.SendReceipt{}, nil
}

func (a *restartingReverseAdapter) Subscribe(_ context.Context, session pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subscribeCount++
	a.lastSession = session
	a.lastEvents = make(chan pluginruntime.PluginEvent)
	return a.lastEvents, nil
}

func (a *restartingReverseAdapter) subscribeCountValue() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.subscribeCount
}

func (a *restartingReverseAdapter) closeLatest() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastEvents != nil {
		close(a.lastEvents)
		a.lastEvents = nil
	}
}

func (a *restartingReverseAdapter) Interrupt(context.Context, pluginruntime.NativeSession, string) error {
	return nil
}

func (a *restartingReverseAdapter) VerifyVisibility(context.Context, pluginruntime.NativeSession, string) (pluginruntime.VisibilityResult, error) {
	return pluginruntime.VisibilityResult{}, nil
}

func (a *restartingReverseAdapter) Close(context.Context) error { return nil }

type failingControlSink struct {
	err   error
	mu    sync.Mutex
	count int
}

func (s *failingControlSink) Send(prismtransport.Outbound) error { return nil }

func (s *failingControlSink) SendControl(prismtransport.ControlOutbound) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return s.err
}

func (s *failingControlSink) sendControlCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
