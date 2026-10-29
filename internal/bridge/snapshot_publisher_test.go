package bridge

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

func TestSnapshotPublisherSuppressesObservationTimeOnlyChanges(t *testing.T) {
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, nil, nil, sink, nil, nil)
	first := map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-1",
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.6"},
		"updated_at":         int64(100),
		"run":                map[string]any{"status": "running", "updated_at": int64(100)},
	}
	if err := publisher.publishDetail(first, false); err != nil {
		t.Fatalf("publish first detail: %v", err)
	}
	second := cloneDetailMap(first)
	second["updated_at"] = int64(200)
	second["run"].(map[string]any)["updated_at"] = int64(200)
	if err := publisher.publishDetail(second, false); err != nil {
		t.Fatalf("publish unchanged detail: %v", err)
	}
	if got := len(sink.conversationDetailSnapshots()); got != 1 {
		t.Fatalf("updated_at-only change should be suppressed, got %d snapshots", got)
	}
	third := cloneDetailMap(second)
	third["current_model"].(map[string]any)["id"] = "gpt-5.7"
	if err := publisher.publishDetail(third, false); err != nil {
		t.Fatalf("publish changed detail: %v", err)
	}
	if got := len(sink.conversationDetailSnapshots()); got != 2 {
		t.Fatalf("semantic change should publish a new snapshot, got %d", got)
	}
}

func TestPublisherRevokesTerminalControlsWhenWatcherDisconnects(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("claudecode", map[string]any{
		"plugin_id": "claudecode", "conversation_id": "conv-1",
		"terminal": map[string]any{"status": "attached", "input_owner": "panel", "can_send": true},
	})
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, remoteAuthorizedConfig("claudecode"), nil, sink, store, nil)
	if err := publisher.PublishTerminalUnavailable("claudecode"); err != nil {
		t.Fatal(err)
	}
	snapshots := sink.conversationDetailSnapshots()
	if len(snapshots) != 1 || asMap(snapshots[0]["terminal"])["can_send"] != false {
		t.Fatalf("stale control was not revoked remotely: %#v", snapshots)
	}
	if err := publisher.PublishCachedLiveDetails(true); err != nil {
		t.Fatal(err)
	}
	snapshots = sink.conversationDetailSnapshots()
	if len(snapshots) != 2 || asMap(snapshots[1]["terminal"])["can_send"] != false {
		t.Fatalf("reconnect replay reintroduced stale control: %#v", snapshots)
	}
}

func TestPublisherRuntimeReplacementRevokesNativeTerminal(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("claudecode", map[string]any{
		"plugin_id": "claudecode", "conversation_id": "conv-1",
		"terminal": map[string]any{"status": "attached", "input_owner": "panel", "can_send": true},
	})
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, remoteAuthorizedConfig("claudecode"), nil, sink, store, nil)
	if err := publisher.InvalidatePluginRuntime("claudecode"); err != nil {
		t.Fatal(err)
	}
	snapshots := sink.conversationDetailSnapshots()
	if len(snapshots) != 1 || asMap(snapshots[0]["terminal"])["can_send"] != false {
		t.Fatalf("runtime replacement did not revoke remote terminal: %#v", snapshots)
	}
	if _, exists := store.GetForRuntime("claudecode", "conv-1"); exists {
		t.Fatal("replaced runtime retained terminal detail")
	}
}

func TestSnapshotPublisherUpdatesCompleteIndexFromOneWatcherHint(t *testing.T) {
	ctx := context.Background()
	db := openConversationIndexTestDB(t)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID:   "codex",
			Capability: pluginruntime.Capability{PluginID: "codex", Available: true, CanListSessions: true},
		},
		hints: []pluginruntime.NativeSessionHint{{
			PluginID: "codex", NativeSessionID: "native-1", NativeThreadID: "thread-1", Title: "Original", Cwd: "/tmp/a",
		}},
	}
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, remoteAuthorizedConfig(), nil, sink, nil, nil)
	if err := publisher.PublishIndex(ctx, true); err != nil {
		t.Fatalf("initialize index: %v", err)
	}
	records, err := db.ListSessions(ctx, "codex")
	if err != nil || len(records) != 1 {
		t.Fatalf("expected one initialized binding, records=%+v err=%v", records, err)
	}
	if err := publisher.PublishIndexHint("codex", pluginruntime.NativeSessionHint{
		PluginID: "codex", NativeSessionID: "native-1", NativeThreadID: "thread-1", Title: "Renamed", Cwd: "/tmp/a",
	}, records[0], false); err != nil {
		t.Fatalf("publish watcher index hint: %v", err)
	}
	snapshots := sink.conversationIndexSnapshots()
	if len(snapshots) != 2 {
		t.Fatalf("expected initial and watcher-updated complete index, got %+v", snapshots)
	}
	latest := snapshots[1]
	if len(latest.Plugins) != 1 || len(latest.Plugins[0].Conversations) != 1 || latest.Plugins[0].Conversations[0].Title != "Renamed" {
		t.Fatalf("watcher hint did not replace the matching complete index item: %+v", latest)
	}
}

func TestSnapshotPublisherEnsuresOnlyMissingConversationIndexes(t *testing.T) {
	ctx := context.Background()
	db := openConversationIndexTestDB(t)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID:   "codex",
			Capability: pluginruntime.Capability{PluginID: "codex", Available: true, CanListSessions: true},
		},
		hints: []pluginruntime.NativeSessionHint{{
			PluginID: "codex", NativeSessionID: "native-1", NativeThreadID: "thread-1", Title: "Thread", Cwd: "/tmp/a",
		}},
	}
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, remoteAuthorizedConfig(), nil, sink, nil, nil)
	if publisher.HasConversationIndex("codex") {
		t.Fatal("index must not exist before complete initialization")
	}
	if err := publisher.EnsureConversationIndexes(ctx); err != nil {
		t.Fatalf("ensure missing index: %v", err)
	}
	if !publisher.HasConversationIndex("codex") {
		t.Fatal("complete index was not initialized")
	}
	if got := len(sink.conversationIndexSnapshots()); got != 1 {
		t.Fatalf("expected one complete index snapshot, got %d", got)
	}
	if err := publisher.EnsureConversationIndexes(ctx); err != nil {
		t.Fatalf("ensure initialized index: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != 1 {
		t.Fatalf("initialized index must not be rebuilt, got %d snapshots", got)
	}
}

func TestSnapshotPublisherInvalidatesConversationIndex(t *testing.T) {
	ctx := context.Background()
	db := openConversationIndexTestDB(t)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{PluginID: "hermes", Capability: pluginruntime.Capability{PluginID: "hermes", Available: true, CanListSessions: true}},
		hints:        []pluginruntime.NativeSessionHint{{PluginID: "hermes", NativeSessionID: "desktop-session", NativeThreadID: "desktop-session"}},
	}
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, remoteAuthorizedConfig(), nil, &recordingSink{}, nil, nil)
	if err := publisher.PublishIndex(ctx, true, "hermes"); err != nil {
		t.Fatalf("initialize index: %v", err)
	}
	if !publisher.HasConversationIndex("hermes") {
		t.Fatal("expected initialized index")
	}
	publisher.InvalidateConversationIndex("hermes")
	if publisher.HasConversationIndex("hermes") {
		t.Fatal("runtime replacement must discard the previous mode index")
	}
}

func TestSnapshotPublisherRebuildsConversationIndexWhenAvailabilityChanges(t *testing.T) {
	ctx := context.Background()
	db := openConversationIndexTestDB(t)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID:   "codex",
			Capability: pluginruntime.Capability{PluginID: "codex", Available: false, CanListSessions: true},
		},
		hints: []pluginruntime.NativeSessionHint{{PluginID: "codex", NativeSessionID: "native-1", NativeThreadID: "thread-1"}},
	}
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, remoteAuthorizedConfig(), nil, sink, nil, nil)
	// The index is built while the desktop control surface is unavailable.
	if err := publisher.EnsureConversationIndexes(ctx); err != nil {
		t.Fatalf("initialize index: %v", err)
	}
	before := len(sink.conversationIndexSnapshots())
	if err := publisher.RebuildConversationIndexIfStale(ctx, "codex", false); err != nil {
		t.Fatalf("rebuild with unchanged availability: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != before {
		t.Fatalf("unchanged availability must not rebuild the index, got %d snapshots", got)
	}
	// The managed launch brings the plugin back; the index must republish.
	adapter.Capability.Available = true
	if err := publisher.RebuildConversationIndexIfStale(ctx, "codex", true); err != nil {
		t.Fatalf("rebuild after availability change: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != before+1 {
		t.Fatalf("availability change must republish the index, got %d snapshots", got)
	}
}

func TestSnapshotPublisherInvalidatesPreviousRuntimeDetail(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("hermes", map[string]any{
		"plugin_id": "hermes", "conversation_id": "desktop-conv", "desktop_foreground": true,
	})
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, remoteAuthorizedConfig(), nil, sink, store, nil)
	if err := publisher.InvalidatePluginRuntime("hermes"); err != nil {
		t.Fatalf("invalidate runtime detail: %v", err)
	}
	if _, ok := store.GetForRuntime("hermes", "desktop-conv"); ok || store.ForegroundConversationID("hermes") != "" {
		t.Fatal("runtime replacement retained previous detail")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.controls) != 1 || sink.controls[0].MessageType != prismtransport.MsgConversationDetailUnavailable {
		t.Fatalf("runtime replacement must clear remote detail first: %+v", sink.controls)
	}
	payload, ok := sink.controls[0].Payload.(prismtransport.ConversationDetailUnavailablePayload)
	if !ok || payload.Code != "plugin_runtime_replaced" {
		t.Fatalf("unexpected runtime invalidation payload: %#v", sink.controls[0].Payload)
	}
}

func TestSnapshotPublisherMarksPreviousForegroundDetailStale(t *testing.T) {
	db, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, record := range []bridgedb.SessionRecord{
		{PrismConversationID: "conv-old", PluginID: "codex", NativeSessionID: "native-old", NativeThreadID: "thread-old"},
		{PrismConversationID: "conv-new", PluginID: "codex", NativeSessionID: "native-new", NativeThreadID: "thread-new"},
	} {
		if _, err := db.UpsertSession(ctx, record); err != nil {
			t.Fatalf("upsert session: %v", err)
		}
	}
	adapter := &recordingAdapter{id: "codex", visible: true}
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-old",
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.6"},
	})
	sink := &recordingSink{}
	cfg := remoteAuthorizedConfig()
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, cfg, nil, sink, store, nil)
	if err := publisher.PublishLiveDetail(ctx, "conv-new", "codex", pluginruntime.NativeSession{
		PluginID:        "codex",
		NativeSessionID: "native-new",
		NativeThreadID:  "thread-new",
	}, "", map[string]any{
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.7"},
		"run":                map[string]any{"status": "idle"},
	}); err != nil {
		t.Fatalf("publish new foreground detail: %v", err)
	}
	snapshots := sink.conversationDetailSnapshots()
	if len(snapshots) != 2 {
		t.Fatalf("expected stale old detail and fresh new detail, got %+v", snapshots)
	}
	if snapshots[0]["conversation_id"] != "conv-old" || snapshots[0]["desktop_foreground"] != false || snapshots[0]["detail_stale"] != true {
		t.Fatalf("old foreground was not marked stale first: %+v", snapshots[0])
	}
	if snapshots[1]["conversation_id"] != "conv-new" || snapshots[1]["desktop_foreground"] != true || snapshots[1]["detail_stale"] != false {
		t.Fatalf("new foreground detail unexpected: %+v", snapshots[1])
	}
}

func TestSnapshotPublisherPublishesDetailUnavailableAndMarksStore(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-1",
		"desktop_foreground": true,
		"detail_stale":       false,
	})
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, remoteAuthorizedConfig(), nil, sink, store, nil)
	if err := publisher.PublishDetailUnavailable("codex", "conv-1", "foreground_watcher_unavailable"); err != nil {
		t.Fatalf("publish detail unavailable: %v", err)
	}
	if !store.IsForegroundWatcherUnavailable("codex") {
		t.Fatal("expected unavailable watcher state to be retained locally")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.controls) != 1 || sink.controls[0].MessageType != prismtransport.MsgConversationDetailUnavailable {
		t.Fatalf("expected one detail unavailable control, got %+v", sink.controls)
	}
	payload, ok := sink.controls[0].Payload.(prismtransport.ConversationDetailUnavailablePayload)
	if !ok || payload.PluginID != "codex" || payload.ConversationID != "conv-1" || payload.Code != "foreground_watcher_unavailable" {
		t.Fatalf("unexpected detail unavailable payload: %#v", sink.controls[0].Payload)
	}
}

func TestSnapshotPublisherRepublishesAfterLeaseExpiry(t *testing.T) {
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(nil, nil, nil, nil, sink, nil, nil)
	detail := map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-1",
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.6"},
	}
	if err := publisher.publishDetail(detail, false); err != nil {
		t.Fatalf("publish first detail: %v", err)
	}
	if got := len(sink.conversationDetailSnapshots()); got != 1 {
		t.Fatalf("expected 1 snapshot, got %d", got)
	}

	// Immediate republish with identical payload should be suppressed within lease duration
	if err := publisher.publishDetail(detail, false); err != nil {
		t.Fatalf("publish unchanged detail: %v", err)
	}
	if got := len(sink.conversationDetailSnapshots()); got != 1 {
		t.Fatalf("expected still 1 snapshot within lease duration, got %d", got)
	}

	// Age the lastPublished timestamp beyond maxSnapshotLeaseDuration
	publisher.mu.Lock()
	key := "detail:codex:conv-1"
	publisher.lastPublished[key] = time.Now().Add(-maxSnapshotLeaseDuration - time.Second)
	publisher.mu.Unlock()

	// Republish with identical payload should now succeed to renew the cloud lease
	if err := publisher.publishDetail(detail, false); err != nil {
		t.Fatalf("publish detail after lease expiry: %v", err)
	}
	if got := len(sink.conversationDetailSnapshots()); got != 2 {
		t.Fatalf("expected 2 snapshots after lease expiry, got %d", got)
	}
}

func TestSnapshotPublisherRefreshConversationIndexes(t *testing.T) {
	ctx := context.Background()
	db := openConversationIndexTestDB(t)
	adapter := &conversationIndexTestAdapter{
		StaticPlugin: pluginruntime.StaticPlugin{
			PluginID:   "codex",
			Capability: pluginruntime.Capability{PluginID: "codex", Available: true, CanListSessions: true},
		},
		hints: []pluginruntime.NativeSessionHint{{
			PluginID: "codex", NativeSessionID: "native-1", NativeThreadID: "thread-1", Title: "Thread", Cwd: "/tmp/a",
		}},
	}
	sink := &recordingSink{}
	publisher := NewSnapshotPublisher(pluginruntime.New(adapter), db, remoteAuthorizedConfig(), nil, sink, nil, nil)
	if err := publisher.PublishIndex(ctx, true); err != nil {
		t.Fatalf("initialize index: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != 1 {
		t.Fatalf("expected 1 initial snapshot, got %d", got)
	}

	// RefreshConversationIndexes within lease duration should be suppressed
	if err := publisher.RefreshConversationIndexes(ctx); err != nil {
		t.Fatalf("refresh index: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != 1 {
		t.Fatalf("expected still 1 snapshot within lease duration, got %d", got)
	}

	// Age the lastPublished timestamp beyond maxSnapshotLeaseDuration
	publisher.mu.Lock()
	publisher.lastPublished["index:codex"] = time.Now().Add(-maxSnapshotLeaseDuration - time.Second)
	publisher.mu.Unlock()

	// RefreshConversationIndexes after lease duration should republish from memory
	if err := publisher.RefreshConversationIndexes(ctx); err != nil {
		t.Fatalf("refresh index after lease expiry: %v", err)
	}
	if got := len(sink.conversationIndexSnapshots()); got != 2 {
		t.Fatalf("expected 2 snapshots after lease expiry, got %d", got)
	}
}
