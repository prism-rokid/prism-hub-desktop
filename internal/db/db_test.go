package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOpenCreatesBridgeTables(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, table := range []string{
		"bridge_plugins",
		"bridge_sessions",
		"bridge_events",
		"bridge_visibility_checks",
		"bridge_message_receipts",
	} {
		var count int
		if err := db.SQL().QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatalf("query table %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("expected table %s to exist", table)
		}
	}
}

func TestNativeEventHandoffSurvivesDiagnosticEventPruning(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.InsertNativeEventOnce(ctx, EventRecord{
		ID: "native-event-1", SessionID: "session-1", PrismConversationID: "conv-1",
		PluginID: "claudecode", NativeEventID: "event-1", EventType: "run.completed", CreatedAt: time.Now().Add(-48 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkNativeEventDelivered(ctx, "session-1", "event-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PruneEvents(ctx, time.Now().Add(-24*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	delivered, err := db.NativeEventDelivered(ctx, "session-1", "event-1")
	if err != nil || !delivered {
		t.Fatalf("handoff marker must survive diagnostic pruning: delivered=%v err=%v", delivered, err)
	}
}

func TestOpenUsesOneConnectionAndConfiguresBusyTimeout(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var timeoutMillis int
	if err := db.SQL().QueryRow(`PRAGMA busy_timeout;`).Scan(&timeoutMillis); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if timeoutMillis != int(sqliteBusyTimeout.Milliseconds()) {
		t.Fatalf("busy_timeout=%d, want %d", timeoutMillis, sqliteBusyTimeout.Milliseconds())
	}
	if max := db.SQL().Stats().MaxOpenConnections; max != 1 {
		t.Fatalf("max open connections=%d, want 1", max)
	}
}

func TestCompleteStartOperationSerializesConcurrentBindings(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const workers = 50
	operations := make([]StartOperationRecord, 0, workers)
	for i := range workers {
		operation, beginErr := db.BeginStartOperation(context.Background(), "codex", fmt.Sprintf("first-message-%d", i), fmt.Sprintf("request-%d", i))
		if beginErr != nil {
			t.Fatalf("begin operation %d: %v", i, beginErr)
		}
		operations = append(operations, operation)
	}

	type result struct {
		record SessionRecord
		err    error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, operation := range operations {
		wg.Add(1)
		go func(index int, operation StartOperationRecord) {
			defer wg.Done()
			<-start
			_, record, completeErr := db.CompleteStartOperation(context.Background(), operation, SessionRecord{
				PluginID:        "codex",
				NativeSessionID: "native-concurrent-start",
				NativeThreadID:  "native-concurrent-start",
				Surface:         "test",
			}, fmt.Sprintf("native-message-%d", index))
			results <- result{record: record, err: completeErr}
		}(i, operation)
	}
	close(start)
	wg.Wait()
	close(results)

	canonicalID := ""
	for item := range results {
		if item.err != nil {
			t.Fatalf("complete concurrent start: %v", item.err)
		}
		if canonicalID == "" {
			canonicalID = item.record.PrismConversationID
		}
		if item.record.PrismConversationID != canonicalID {
			t.Fatalf("same native start produced multiple canonical ids: %q and %q", canonicalID, item.record.PrismConversationID)
		}
	}
	sessions, err := db.ListSessions(context.Background(), "codex")
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].PrismConversationID != canonicalID {
		t.Fatalf("expected one durable concurrent binding, got %+v", sessions)
	}
	for _, operation := range operations {
		completed, findErr := db.FindStartOperation(context.Background(), operation.PluginID, operation.PrismMessageID)
		if findErr != nil || completed.Status != "completed" || completed.PrismConversationID != canonicalID {
			t.Fatalf("operation did not complete durably: operation=%+v completed=%+v err=%v", operation, completed, findErr)
		}
	}
}

func TestStartOperationWaitsForExternalWriteLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prism.sqlite")
	writer, err := Open(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	contender, err := Open(path)
	if err != nil {
		t.Fatalf("open contender: %v", err)
	}
	defer contender.Close()
	if _, err := writer.UpsertPlugin(context.Background(), PluginRecord{PluginID: "codex"}); err != nil {
		t.Fatalf("seed plugin: %v", err)
	}

	tx, err := writer.SQL().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin external write: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE bridge_plugins SET updated_at = ? WHERE plugin_id = ?`, formatTime(time.Now().UTC()), "codex"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("hold external write: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, beginErr := contender.BeginStartOperation(context.Background(), "codex", "waiting-message", "waiting-request")
		result <- beginErr
	}()
	select {
	case beginErr := <-result:
		t.Fatalf("start operation returned before external lock release: %v", beginErr)
	case <-time.After(120 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("release external write: %v", err)
	}
	select {
	case beginErr := <-result:
		if beginErr != nil {
			t.Fatalf("start operation did not wait for external lock: %v", beginErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("start operation did not resume after external lock release")
	}
}

func TestSessionRepositoryUpsertFindAndEvents(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	record, err := db.UpsertSession(ctx, SessionRecord{
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		NativeSessionID:     "agent:main:prism:conv-1",
		NativeThreadID:      "session-1",
		Surface:             "gateway",
		Endpoint:            "http://127.0.0.1:18789",
	})
	if err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	if record.ID == "" || record.NativeSessionID == "" {
		t.Fatalf("unexpected session record: %+v", record)
	}
	record.VisibilityVerified = true
	record.VisibilityEvidence = "marker"
	updated, err := db.UpsertSession(ctx, record)
	if err != nil {
		t.Fatalf("UpsertSession update: %v", err)
	}
	if updated.ID != record.ID || !updated.VisibilityVerified {
		t.Fatalf("expected same verified record, got %+v", updated)
	}
	found, err := db.FindSession(ctx, "conv-1", "openclaw")
	if err != nil {
		t.Fatalf("FindSession: %v", err)
	}
	if found.ID != record.ID || found.NativeThreadID != "session-1" {
		t.Fatalf("unexpected found session: %+v", found)
	}
	sessions, err := db.ListSessions(ctx, "openclaw")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != found.ID || !sessions[0].VisibilityVerified {
		t.Fatalf("unexpected listed sessions: %+v", sessions)
	}
	if _, err := db.InsertEvent(ctx, EventRecord{
		SessionID:           found.ID,
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		EventType:           "message.sent",
		Status:              "accepted",
		Payload:             map[string]any{"marker": "abc"},
	}); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := db.InsertEvent(ctx, EventRecord{
		SessionID:           found.ID,
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		EventType:           "run.completed",
		Status:              "completed",
		Summary:             "done",
		Payload:             map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("InsertEvent second: %v", err)
	}
	events, err := db.ListRecentEvents(ctx, 1)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "run.completed" || events[0].Payload["ok"] != true {
		t.Fatalf("unexpected recent events: %+v", events)
	}
	if _, err := db.InsertVisibilityCheck(ctx, VisibilityCheckRecord{
		PluginID:  "openclaw",
		SessionID: found.ID,
		Marker:    "abc",
		Visible:   true,
		Evidence:  "abc",
	}); err != nil {
		t.Fatalf("InsertVisibilityCheck: %v", err)
	}
	receipt, err := db.UpsertMessageReceipt(ctx, MessageReceiptRecord{
		PrismMessageID:      "msg-1",
		PrismConversationID: "conv-1",
		PluginID:            "openclaw",
		SessionID:           found.ID,
		NativeMessageID:     "run-1",
		Status:              "accepted",
		Visible:             true,
	})
	if err != nil {
		t.Fatalf("UpsertMessageReceipt: %v", err)
	}
	if receipt.PrismMessageID != "msg-1" || !receipt.Visible {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	receipt.Status = "visible"
	updatedReceipt, err := db.UpsertMessageReceipt(ctx, receipt)
	if err != nil {
		t.Fatalf("UpsertMessageReceipt update: %v", err)
	}
	if updatedReceipt.Status != "visible" || updatedReceipt.NativeMessageID != "run-1" {
		t.Fatalf("unexpected updated receipt: %+v", updatedReceipt)
	}
	foundReceipt, err := db.FindMessageReceipt(ctx, "openclaw", "msg-1")
	if err != nil {
		t.Fatalf("FindMessageReceipt: %v", err)
	}
	if foundReceipt.SessionID != found.ID || foundReceipt.PluginID != "openclaw" {
		t.Fatalf("unexpected found receipt: %+v", foundReceipt)
	}
}

func TestMessageReceiptsAreScopedByPlugin(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, pluginID := range []string{"codex", "claudecode"} {
		if _, err := db.UpsertMessageReceipt(ctx, MessageReceiptRecord{
			PrismMessageID:      "shared-message-id",
			PrismConversationID: "conv-" + pluginID,
			PluginID:            pluginID,
			NativeMessageID:     "native-" + pluginID,
			Status:              "visible",
			Visible:             true,
		}); err != nil {
			t.Fatalf("UpsertMessageReceipt(%s): %v", pluginID, err)
		}
	}
	for _, pluginID := range []string{"codex", "claudecode"} {
		receipt, err := db.FindMessageReceipt(ctx, pluginID, "shared-message-id")
		if err != nil {
			t.Fatalf("FindMessageReceipt(%s): %v", pluginID, err)
		}
		if receipt.PluginID != pluginID || receipt.NativeMessageID != "native-"+pluginID {
			t.Fatalf("receipt crossed plugin boundary: %+v", receipt)
		}
	}
}

func TestMessageReceiptMigrationPreservesLegacyRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prism.sqlite")
	raw, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatalf("open legacy sqlite: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT '');`); err != nil {
		t.Fatalf("create legacy migrations: %v", err)
	}
	for _, migrationID := range []string{
		"0001_pluginbridge_core_v3",
		"0002_pluginbridge_message_receipts_v3",
		"0003_pluginbridge_drop_obsolete_companion_tables_v3",
		"0004_pluginbridge_conversation_start_v3",
	} {
		if _, err := raw.Exec(`INSERT INTO schema_migrations(id) VALUES (?)`, migrationID); err != nil {
			t.Fatalf("mark legacy migration %s: %v", migrationID, err)
		}
	}
	if _, err := raw.Exec(`CREATE TABLE bridge_message_receipts (
		prism_message_id TEXT PRIMARY KEY,
		prism_conversation_id TEXT NOT NULL,
		plugin_id TEXT NOT NULL,
		session_id TEXT NOT NULL DEFAULT '',
		native_message_id TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT '',
		visible INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		last_error TEXT NOT NULL DEFAULT ''
	);`); err != nil {
		t.Fatalf("create legacy receipts: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO bridge_message_receipts (
		prism_message_id, prism_conversation_id, plugin_id, native_message_id, status, visible, created_at, updated_at
	) VALUES ('legacy-message', 'legacy-conversation', 'codex', 'legacy-native', 'visible', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed legacy receipt: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy sqlite: %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated db: %v", err)
	}
	defer db.Close()
	receipt, err := db.FindMessageReceipt(context.Background(), "codex", "legacy-message")
	if err != nil {
		t.Fatalf("FindMessageReceipt after migration: %v", err)
	}
	if receipt.PrismConversationID != "legacy-conversation" || receipt.NativeMessageID != "legacy-native" || !receipt.Visible {
		t.Fatalf("legacy receipt was not preserved: %+v", receipt)
	}
	if _, err := db.UpsertMessageReceipt(context.Background(), MessageReceiptRecord{
		PrismMessageID:      "legacy-message",
		PrismConversationID: "claude-conversation",
		PluginID:            "claudecode",
		NativeMessageID:     "claude-native",
	}); err != nil {
		t.Fatalf("insert same message id for second plugin: %v", err)
	}
}

func TestEnsureCanonicalSessionBindingIsStableForRepeatedConcurrentDiscovery(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const workers = 8
	start := make(chan struct{})
	type result struct {
		record SessionRecord
		err    error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			record, bindErr := db.EnsureCanonicalSessionBinding(context.Background(), SessionRecord{
				PluginID:        "openclaw",
				NativeSessionID: "native-session-1",
				NativeThreadID:  "native-thread-1",
				Surface:         "gateway",
			})
			results <- result{record: record, err: bindErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	canonicalID := ""
	for item := range results {
		if item.err != nil {
			t.Fatalf("EnsureCanonicalSessionBinding: %v", item.err)
		}
		if canonicalID == "" {
			canonicalID = item.record.PrismConversationID
		}
		if item.record.PrismConversationID != canonicalID {
			t.Fatalf("same native identity produced multiple canonical ids: %q and %q", canonicalID, item.record.PrismConversationID)
		}
	}
	if len(canonicalID) < len("conv_")+1 || canonicalID[:len("conv_")] != "conv_" {
		t.Fatalf("expected opaque canonical id, got %q", canonicalID)
	}
	sessions, err := db.ListSessions(context.Background(), "openclaw")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].PrismConversationID != canonicalID {
		t.Fatalf("expected one durable binding, got %+v", sessions)
	}

	repeated, err := db.EnsureCanonicalSessionBinding(context.Background(), SessionRecord{
		PluginID:        "openclaw",
		NativeSessionID: "native-session-1",
		NativeThreadID:  "native-thread-1",
		Cwd:             "/workspace",
	})
	if err != nil || repeated.PrismConversationID != canonicalID || repeated.Cwd != "/workspace" {
		t.Fatalf("expected repeated discovery to preserve binding and enrich metadata, record=%+v err=%v", repeated, err)
	}
}

func TestLatestVisibilityCheck(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// No record yet => ErrNoRows (caller treats as "not verified").
	if _, err := db.LatestVisibilityCheck(ctx, "openclaw"); err == nil {
		t.Fatal("expected error when no visibility check exists")
	}

	// Insert an older failed check, then a newer passed check.
	if _, err := db.InsertVisibilityCheck(ctx, VisibilityCheckRecord{
		PluginID:      "openclaw",
		Marker:        "m-old",
		Visible:       false,
		FailureReason: "marker missing",
		CheckedAt:     parseTime("2026-06-24T10:00:00Z"),
	}); err != nil {
		t.Fatalf("insert old check: %v", err)
	}
	if _, err := db.InsertVisibilityCheck(ctx, VisibilityCheckRecord{
		PluginID:  "openclaw",
		Marker:    "m-new",
		Visible:   true,
		Evidence:  "marker found",
		CheckedAt: parseTime("2026-06-24T11:00:00Z"),
	}); err != nil {
		t.Fatalf("insert new check: %v", err)
	}

	latest, err := db.LatestVisibilityCheck(ctx, "openclaw")
	if err != nil {
		t.Fatalf("LatestVisibilityCheck: %v", err)
	}
	if !latest.Visible || latest.Marker != "m-new" {
		t.Fatalf("expected newest passed check, got %+v", latest)
	}

	// A different plugin still has no record.
	if _, err := db.LatestVisibilityCheck(ctx, "hermes"); err == nil {
		t.Fatal("expected error for plugin without checks")
	}
}
