package bridge

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
)

func TestCodexForwardReverseShareTaskIdentity(t *testing.T) {
	for _, order := range []string{"forward-first", "reverse-first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			database, err := bridgedb.Open(filepath.Join(t.TempDir(), "prism.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			binding := bindExistingConversation(t, database, "codex", "conv-test")
			router := &Router{DB: database}
			reverse := &ReverseSyncer{DB: database}
			for task := 0; task < 2; task++ {
				event := pluginruntime.PluginEvent{ID: fmt.Sprintf("codex-task:thread:turn-%d:completed", task), Type: "run.completed", Status: "completed", Summary: "same answer"}
				fallback := event
				fallback.Summary = "Codex completed."
				forwardKey := stableConversationEventDedupeID(conversationEventContext{ConversationID: binding.PrismConversationID, RequestID: "request"}, event)
				reverseKey := stableConversationEventDedupeID(conversationEventContext{ConversationID: binding.PrismConversationID}, fallback)
				if forwardKey != reverseKey {
					t.Fatal("waiter and watcher must share notification identity even with different summaries")
				}
				forward := func() {
					router.handleWaitResult(event, nil, binding.ID, "request", binding.PrismConversationID, "codex", "task", "test", targetFromRecord(binding).session, "receipt-run")
				}
				backward := func() {
					if _, err := reverse.persistEvent(ctx, targetFromRecord(binding), fallback); err != nil {
						t.Error(err)
					}
				}
				switch order {
				case "forward-first":
					forward()
					backward()
				case "reverse-first":
					backward()
					forward()
				default:
					var wg sync.WaitGroup
					wg.Add(2)
					go func() { defer wg.Done(); forward() }()
					go func() { defer wg.Done(); backward() }()
					wg.Wait()
				}
			}
			var count int
			if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM bridge_events WHERE session_id = ? AND event_type = 'run.completed'`, binding.ID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 2 {
				t.Fatalf("two native tasks must persist exactly twice, got %d", count)
			}
		})
	}
}
