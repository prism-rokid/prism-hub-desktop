package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	prismtransport "rokid-prism-hub/internal/realtime"
)

func TestRestoredPanelBodyRequiresExplicitLivePageAndBody(t *testing.T) {
	base := prismtransport.InboundPayload{QueryType: "conversation.history.stream", PageID: "page", BodyStreamID: "body", Live: true}
	if !isRestoredPanelBody(&base) {
		t.Fatal("active body did not qualify")
	}
	for _, remove := range []func(*prismtransport.InboundPayload){
		func(p *prismtransport.InboundPayload) { p.Live = false },
		func(p *prismtransport.InboundPayload) { p.BodyStreamID = "" },
		func(p *prismtransport.InboundPayload) { p.PageID = "" },
		func(p *prismtransport.InboundPayload) { p.QueryType = "conversation.messages" },
	} {
		p := base
		remove(&p)
		if isRestoredPanelBody(&p) {
			t.Fatal("ordinary history qualified for desktop recovery")
		}
	}
}

func TestSharedSelectionRestoresOnlyLastAcknowledgedConversationAfterProcessReplacement(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS shared window")
	}
	ctx, dir := context.Background(), t.TempDir()
	previous := exec.Command("/bin/sleep", "60")
	if err := previous.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if previous.ProcessState == nil {
			_ = previous.Process.Kill()
			_ = previous.Wait()
		}
	})
	old := sharedDesktopLease{SharedWindow: true, Phase: "ready", Binding: "opaque-native-room", TargetID: "window", CDPOrigin: "http://127.0.0.1:1234",
		HostPID: previous.Process.Pid, HostStart: desktopHostProcessStart(ctx, previous.Process.Pid)}
	current := old
	current.HostPID, current.HostStart = os.Getpid(), desktopHostProcessStart(ctx, os.Getpid())
	first, second := sharedDesktopAgent{pluginID: "first"}, sharedDesktopAgent{pluginID: "second"}
	if err := first.writeLease(dir, old); err != nil {
		t.Fatal(err)
	}
	if err := second.writeLease(dir, old); err != nil {
		t.Fatal(err)
	}
	req := &prismtransport.InboundPayload{QueryType: "conversation.select", PluginID: first.pluginID, ConversationID: "conv-a"}
	calls := 0
	pick := func(_ context.Context, p *prismtransport.InboundPayload) (map[string]any, error) {
		calls++
		return map[string]any{"desktop_selected": true, "conversation_id": p.ConversationID, "plugin_id": p.PluginID}, nil
	}
	run := func(s *sharedDesktopSelections, restoring bool, op desktopSessionQuery, wantCalls int) {
		t.Helper()
		if _, err := s.selectSession(ctx, nil, req, restoring, op); err != nil {
			t.Fatal(err)
		}
		if calls != wantCalls {
			t.Fatalf("native operations: got %d, want %d", calls, wantCalls)
		}
	}
	s := loadSharedDesktopSelections(dir)
	run(s, true, pick, 0) // Upgrade without an acknowledged selection.
	run(s, false, pick, 1)
	req.PluginID, req.ConversationID = second.pluginID, "conv-b"
	run(s, false, pick, 2)
	s = loadSharedDesktopSelections(dir)
	run(s, true, pick, 2) // Only the daemon restarted.
	if err := previous.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = previous.Wait()

	s = loadSharedDesktopSelections(dir)
	req.PluginID, req.ConversationID = first.pluginID, "conv-a"
	run(s, true, pick, 2) // Stale subscription for the previous owner.
	req.PluginID = second.pluginID
	run(s, true, pick, 2) // Right agent, wrong conversation.
	req.ConversationID = "conv-b"
	if err := first.writeLease(dir, current); err != nil {
		t.Fatal(err)
	}
	run(s, true, pick, 2) // A local action already claimed the new process.
	if err := first.writeLease(dir, old); err != nil {
		t.Fatal(err)
	}
	swapped := old
	swapped.Binding = "different-native-room"
	if err := second.writeLease(dir, swapped); err != nil {
		t.Fatal(err)
	}
	run(loadSharedDesktopSelections(dir), true, pick, 2)
	if err := second.writeLease(dir, old); err != nil {
		t.Fatal(err)
	}

	s = loadSharedDesktopSelections(dir)
	run(s, true, func(ctx context.Context, p *prismtransport.InboundPayload) (map[string]any, error) {
		if err := second.writeLease(dir, current); err != nil {
			t.Fatal(err)
		}
		return pick(ctx, p)
	}, 3)
	run(s, true, pick, 3)
	run(loadSharedDesktopSelections(dir), true, pick, 3)
	req.PluginID, req.ConversationID = first.pluginID, "conv-c"
	_, err := s.selectSession(ctx, nil, req, false, func(context.Context, *prismtransport.InboundPayload) (map[string]any, error) {
		if loadSharedDesktopSelections(dir).last.PluginID != "" {
			t.Fatal("superseded intent survived during native operation")
		}
		return nil, errors.New("desktop busy")
	})
	if err == nil || loadSharedDesktopSelections(dir).last.PluginID != "" {
		t.Fatal("a failed switch must not replay a prior window")
	}
}

func TestSharedSelectionInvalidationFailurePreventsNativeSwitch(t *testing.T) {
	dir := t.TempDir()
	s := loadSharedDesktopSelections(dir)
	s.last = selectedDesktopSession{PluginID: "first", ConversationID: "conv-a"}
	lease := sharedDesktopLease{SharedWindow: true, Phase: "ready", Binding: "room", TargetID: "window",
		CDPOrigin: "http://127.0.0.1:1234", HostPID: 1, HostStart: "old"}
	if err := (&sharedDesktopAgent{pluginID: "second"}).writeLease(dir, lease); err != nil {
		t.Fatal(err)
	}
	// Simulate a prior record the daemon cannot clear.
	path := filepath.Join(dir, selectedDesktopSessionFile)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "keep"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	p := &prismtransport.InboundPayload{PluginID: "second", ConversationID: "conv-b"}
	_, err := s.selectSession(context.Background(), nil, p, false, func(context.Context, *prismtransport.InboundPayload) (map[string]any, error) {
		t.Fatal("changed native selection despite durable-state failure")
		return nil, nil
	})
	if err == nil {
		t.Fatal("missing persistence error")
	}
}
