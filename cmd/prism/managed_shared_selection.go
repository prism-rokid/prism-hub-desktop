package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	prismtransport "rokid-prism-hub/internal/realtime"
)

const selectedDesktopSessionFile = "selected-session.json"

// The canonical Panel ID identifies browsing intent. The fingerprint ties that
// intent to the exact locally approved binding without storing its contents.
type selectedDesktopSession struct {
	PluginID       string `json:"plugin_id"`
	ConversationID string `json:"conversation_id"`
	HostPID        int    `json:"host_pid"`
	HostStart      string `json:"host_start"`
	BindingHash    string `json:"binding_sha256"`
}

type sharedDesktopSelections struct {
	mu        sync.Mutex
	dir       string
	last      selectedDesktopSession
	nextCheck time.Time
}

func loadSharedDesktopSelections(dir string) *sharedDesktopSelections {
	s := &sharedDesktopSelections{dir: dir}
	data, err := os.ReadFile(filepath.Join(dir, selectedDesktopSessionFile))
	var last selectedDesktopSession
	if err == nil && len(data) < 16*1024 && json.Unmarshal(data, &last) == nil {
		s.last = last
	}
	return s
}

func isRestoredPanelBody(p *prismtransport.InboundPayload) bool {
	return p != nil && p.QueryType == "conversation.history.stream" && p.Live &&
		strings.TrimSpace(p.BodyStreamID) != "" && strings.TrimSpace(p.PageID) != ""
}

func sharedSelectionLease(lease sharedDesktopLease) bool {
	return lease.SharedWindow && lease.Phase == "ready" && lease.HostPID > 0 && lease.HostStart != "" &&
		lease.TargetID != "" && validDesktopHostOrigin(lease.CDPOrigin) && lease.Binding != ""
}

func (s *sharedDesktopSelections) forget() error {
	err := os.Remove(filepath.Join(s.dir, selectedDesktopSessionFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.last = selectedDesktopSession{}
	s.nextCheck = time.Time{}
	return nil
}

func (s *sharedDesktopSelections) canRestore(ctx context.Context, agent *sharedDesktopAgent, conversationID string) bool {
	last := s.last
	if last.PluginID != agent.pluginID || last.ConversationID != conversationID || last.HostPID <= 0 ||
		last.HostStart == "" || time.Now().Before(s.nextCheck) {
		return false
	}
	// A healthy host costs no lease I/O. Throttle failed recovery only, so a
	// subscription immediately after a successful process restart still runs.
	if desktopHostProcessStart(ctx, last.HostPID) == last.HostStart || ctx.Err() != nil {
		return false
	}
	s.nextCheck = time.Now().Add(5 * time.Second)
	lease := agent.readLease(s.dir)
	if !sharedSelectionLease(lease) || lease.HostPID != last.HostPID || lease.HostStart != last.HostStart ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(lease.Binding))) != last.BindingHash {
		return false
	}
	// A local rehost or an explicit selection through another participant may
	// have claimed the replacement window. A restored body must yield to it.
	paths, err := filepath.Glob(filepath.Join(s.dir, "*-window.json"))
	if err != nil {
		return false
	}
	for _, path := range paths {
		id := strings.TrimSuffix(filepath.Base(path), "-window.json")
		peer := (&sharedDesktopAgent{pluginID: id}).readLease(s.dir)
		if sharedSelectionLease(peer) && (peer.HostPID != last.HostPID || peer.HostStart != last.HostStart) &&
			desktopHostProcessStart(ctx, peer.HostPID) == peer.HostStart {
			return false
		}
	}
	return ctx.Err() == nil
}

type desktopSessionQuery func(context.Context, *prismtransport.InboundPayload) (map[string]any, error)

func (s *sharedDesktopSelections) selectSession(ctx context.Context, cfg *bridgeconfig.Config, p *prismtransport.InboundPayload,
	restoring bool, execute desktopSessionQuery) (map[string]any, error) {
	pluginID, conversationID := strings.TrimSpace(p.PluginID), strings.TrimSpace(p.ConversationID)
	if pluginID == "" || filepath.Base(pluginID) != pluginID || conversationID == "" {
		if restoring {
			return nil, nil
		}
		return execute(ctx, p)
	}
	agent := sharedDesktopAgent{pluginID: pluginID}
	if !restoring && !sharedSelectionLease(agent.readLease(s.dir)) {
		item, err := dashboardPluginManifest(cfg, pluginID)
		if err != nil || !supportsSharedDesktopHost(item) {
			return execute(ctx, p)
		}
	}
	if restoring {
		// History remains readable while an explicit selection or another
		// subscription already has the shared window.
		if !s.mu.TryLock() {
			return nil, nil
		}
	} else {
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if restoring {
		if !s.canRestore(ctx, &agent, conversationID) {
			return nil, nil
		}
	}
	// Invalidate superseded intent before changing the surface. A daemon crash
	// between native selection and its acknowledgement cannot replay an old room.
	if s.last.PluginID != "" && (s.last.PluginID != pluginID || s.last.ConversationID != conversationID) {
		if err := s.forget(); err != nil {
			return nil, fmt.Errorf("clear previous shared window selection: %w", err)
		}
	}
	result, err := execute(ctx, p)
	if err != nil || result["desktop_selected"] != true {
		return result, err
	}
	lease := agent.readLease(s.dir)
	if !sharedSelectionLease(lease) {
		return result, nil
	}
	if desktopHostProcessStart(ctx, lease.HostPID) != lease.HostStart {
		// A companion can select via its peer's new lease. Now that the native
		// thread is in front, its own read-only bind can verify the replacement.
		if err := prepareSharedDesktopSelection(ctx, cfg, pluginID); err != nil {
			slog.Warn("record shared desktop selection", "plugin_id", pluginID, "err", err)
			return result, nil
		}
		lease = agent.readLease(s.dir)
		if !sharedSelectionLease(lease) || desktopHostProcessStart(ctx, lease.HostPID) != lease.HostStart {
			return result, nil
		}
	}
	canonical, _ := result["conversation_id"].(string)
	if canonical == "" || result["plugin_id"] != pluginID {
		return result, nil
	}
	next := selectedDesktopSession{PluginID: pluginID, ConversationID: canonical, HostPID: lease.HostPID, HostStart: lease.HostStart,
		BindingHash: fmt.Sprintf("%x", sha256.Sum256([]byte(lease.Binding)))}
	if s.last == next {
		return result, nil
	}
	if err := writeDesktopHostState(s.dir, selectedDesktopSessionFile, next); err != nil {
		slog.Warn("persist shared desktop selection", "plugin_id", pluginID, "err", err)
		return result, nil
	}
	s.last = next
	s.nextCheck = time.Time{}
	return result, nil
}
