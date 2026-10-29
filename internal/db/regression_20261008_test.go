package db

import (
	"context"
	"path/filepath"
	"testing"
)

func TestVerifiedStartPromotesDiscoveredBindingAndSurvivesRefresh(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "prism.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	native := SessionRecord{PluginID: "codex", NativeSessionID: "native-a", NativeThreadID: "native-a"}
	discovered, err := database.EnsureCanonicalSessionBinding(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	op, err := database.BeginStartOperation(ctx, "codex", "first-message", "request")
	if err != nil {
		t.Fatal(err)
	}
	native.VisibilityVerified = true
	native.VisibilityEvidence = "unique-first-message"
	_, completed, err := database.CompleteStartOperation(ctx, op, native, "native-message")
	if err != nil {
		t.Fatal(err)
	}
	if completed.ID != discovered.ID || !completed.VisibilityVerified || completed.VisibilityEvidence != native.VisibilityEvidence {
		t.Fatalf("start must promote existing binding: %+v", completed)
	}
	completed.VisibilityVerified = false
	completed.VisibilityEvidence = ""
	refreshed, err := database.UpsertSession(ctx, completed)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed.VisibilityVerified || refreshed.VisibilityEvidence != "unique-first-message" {
		t.Fatalf("discovery refresh lost successful verification: %+v", refreshed)
	}
	completed.NativeSessionID = "native-b"
	completed.NativeThreadID = "native-b"
	rebound, err := database.UpsertSession(ctx, completed)
	if err != nil {
		t.Fatal(err)
	}
	if rebound.VisibilityVerified || rebound.VisibilityEvidence != "" {
		t.Fatalf("new identity inherited old evidence: %+v", rebound)
	}
}
