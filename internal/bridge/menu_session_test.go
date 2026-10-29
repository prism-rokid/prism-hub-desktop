package bridge

import (
	"errors"
	"testing"
	"time"
)

func TestMenuSessionStoreBindsOwnerSubjectAndExpiry(t *testing.T) {
	now := time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)
	store := NewMenuSessionStore()
	store.now = func() time.Time { return now }
	store.newID = func() string { return "menu-test" }
	owner := map[string]any{"kind": "interactive_control", "control_id": "control-a"}
	created, err := store.Create(MenuSessionCreate{
		PluginMenuSessionID: "plugin-menu-a",
		PluginID:            "codex",
		SubjectKind:         "conversation",
		SubjectID:           "conv-a",
		Owner:               owner,
		Entries:             []MenuSessionEntry{{EntryID: "entry-a", Label: "Desktop option", Available: true, Current: true}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID != "menu-test" || created.ExpiresAt.Sub(now) != menuSessionTTL {
		t.Fatalf("unexpected created session: %+v", created)
	}
	if !created.Entries[0].Current {
		t.Fatalf("expected selected marker to survive session creation: %+v", created)
	}
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-a", owner, true); err != nil {
		t.Fatalf("Resolve bound session: %v", err)
	}
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-a", nil, true); err != nil {
		t.Fatalf("Resolve must permit apply without resupplying the stored owner: %v", err)
	}
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-b", owner, true); !errors.Is(err, errMenuSessionStale) {
		t.Fatalf("expected subject stale, got %v", err)
	}
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-a", map[string]any{"kind": "interactive_control", "control_id": "control-b"}, true); !errors.Is(err, errMenuSessionStale) {
		t.Fatalf("expected owner stale, got %v", err)
	}
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-a", nil, false); !errors.Is(err, errMenuSessionStale) {
		t.Fatalf("expected foreground stale, got %v", err)
	}
	now = now.Add(menuSessionTTL)
	if _, err := store.Resolve(created.ID, "codex", "conversation", "conv-a", nil, true); !errors.Is(err, errMenuSessionStale) {
		t.Fatalf("expected expiry stale, got %v", err)
	}
}

func TestMenuSessionStoreDoesNotExposeInternalPluginIdentity(t *testing.T) {
	store := NewMenuSessionStore()
	store.newID = func() string { return "menu-public" }
	created, err := store.Create(MenuSessionCreate{
		PluginMenuSessionID: "plugin-secret",
		PluginID:            "codex",
		SubjectKind:         "draft",
		SubjectID:           "draft-a",
		Owner:               map[string]any{"kind": "interactive_control", "control_id": "control-a"},
		Entries:             []MenuSessionEntry{{EntryID: "entry-a", Label: "Option", Available: true}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.PluginMenuSessionID != "plugin-secret" {
		t.Fatalf("test setup lost plugin identity: %+v", created)
	}
	if created.ID == created.PluginMenuSessionID {
		t.Fatalf("public id must not reuse plugin menu identity")
	}
}

func TestMenuSessionStorePreservesGroupedEntriesWithoutChangingApplyIdentity(t *testing.T) {
	store := NewMenuSessionStore()
	store.newID = func() string { return "menu-grouped" }
	created, err := store.Create(MenuSessionCreate{
		PluginMenuSessionID: "plugin-menu-grouped",
		PluginID:            "codex",
		SubjectKind:         "conversation",
		SubjectID:           "conv-grouped",
		Owner:               map[string]any{"kind": "interactive_control", "control_id": "codex.intelligence"},
		Entries: []MenuSessionEntry{
			{EntryID: "entry-model", Label: "Desktop model", Available: true},
			{EntryID: "entry-reasoning", Label: "Desktop reasoning", Available: true},
		},
		Groups: []MenuSessionGroup{
			{GroupID: "group-model", Label: "Desktop model root", Entries: []MenuSessionEntry{{EntryID: "entry-model", Label: "Desktop model", Available: true}}},
			{GroupID: "group-reasoning", Label: "Desktop reasoning root", Entries: []MenuSessionEntry{{EntryID: "entry-reasoning", Label: "Desktop reasoning", Available: true}}},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(created.Groups) != 2 || created.Groups[0].Entries[0].EntryID != "entry-model" {
		t.Fatalf("expected grouped entries to survive create, session=%+v", created)
	}
	resolved, err := store.Resolve(created.ID, "codex", "conversation", "conv-grouped", nil, true)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !menuSessionHasAvailableEntry(resolved, "entry-reasoning") {
		t.Fatalf("grouped entry must remain a normal opaque apply target: %+v", resolved)
	}
}

func TestMenuSessionStoreRejectsGroupEntryOutsideFlattenedCapability(t *testing.T) {
	store := NewMenuSessionStore()
	_, err := store.Create(MenuSessionCreate{
		PluginMenuSessionID: "plugin-menu-invalid-group",
		PluginID:            "codex",
		SubjectKind:         "conversation",
		SubjectID:           "conv-invalid-group",
		Owner:               map[string]any{"kind": "interactive_control", "control_id": "codex.intelligence"},
		Entries:             []MenuSessionEntry{{EntryID: "entry-present", Label: "Present", Available: true}},
		Groups: []MenuSessionGroup{{
			GroupID: "group-invalid", Label: "Invalid", Entries: []MenuSessionEntry{{EntryID: "entry-missing", Label: "Missing", Available: true}},
		}},
	})
	if err == nil {
		t.Fatal("expected group entry outside flattened capability to be rejected")
	}
}
