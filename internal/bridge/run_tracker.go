package bridge

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

const (
	defaultForwardRunTTL      = 20 * time.Minute
	defaultForwardRunTerminal = 15 * time.Second
)

type ForwardRun struct {
	RequestID       string
	ConversationID  string
	PluginID        string
	NativeSessionID string
	NativeThreadID  string
	NativeRunID     string
	StartedAt       time.Time
}

type trackedForwardRun struct {
	ForwardRun
	ExpiresAt     time.Time
	TerminalUntil time.Time
}

type RunTracker struct {
	mu            sync.Mutex
	runs          map[string]trackedForwardRun
	now           func() time.Time
	activeTTL     time.Duration
	terminalGrace time.Duration
}

func NewRunTracker() *RunTracker {
	return &RunTracker{
		runs:          map[string]trackedForwardRun{},
		now:           time.Now,
		activeTTL:     defaultForwardRunTTL,
		terminalGrace: defaultForwardRunTerminal,
	}
}

func (t *RunTracker) Track(run ForwardRun) {
	if t == nil {
		return
	}
	requestID := strings.TrimSpace(run.RequestID)
	if requestID == "" {
		return
	}
	now := t.timeNow().UTC()
	if run.StartedAt.IsZero() {
		run.StartedAt = now
	}
	tracked := trackedForwardRun{
		ForwardRun: run,
		ExpiresAt:  now.Add(t.activeTTLValue()),
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureRuns()
	t.gcLocked(now)
	t.runs[requestID] = tracked
}

func (t *RunTracker) MarkTerminal(requestID string) {
	if t == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	now := t.timeNow().UTC()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcLocked(now)
	run, ok := t.runs[requestID]
	if !ok {
		return
	}
	run.TerminalUntil = now.Add(t.terminalGraceValue())
	run.ExpiresAt = run.TerminalUntil
	t.runs[requestID] = run
}

func (t *RunTracker) Release(requestID string) {
	if t == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.runs, requestID)
}

func (t *RunTracker) ShouldSuppress(pluginID, conversationID string, session pluginruntime.NativeSession, event pluginruntime.PluginEvent) bool {
	if t == nil {
		return false
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	conversationID = strings.TrimSpace(conversationID)
	if pluginID == "" {
		return false
	}
	now := t.timeNow().UTC()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcLocked(now)
	for _, run := range t.runs {
		if strings.ToLower(strings.TrimSpace(run.PluginID)) != pluginID {
			continue
		}
		if run.TerminalUntil.IsZero() {
			if conversationID != "" && conversationID == strings.TrimSpace(run.ConversationID) && shouldSuppressActiveConversationEvent(event) {
				return true
			}
			if !runMatchesEvent(run, session, event) {
				continue
			}
			return true
		}
		if !runMatchesEvent(run, session, event) {
			continue
		}
		return true
	}
	return false
}

func shouldSuppressActiveConversationEvent(event pluginruntime.PluginEvent) bool {
	switch strings.ToLower(strings.TrimSpace(event.Type)) {
	case "message.delta", "message.started", "run.completed", "run.failed", "run.interrupted", "approval.required", "approval.requested":
		return true
	default:
		return false
	}
}

func runMatchesEvent(run trackedForwardRun, session pluginruntime.NativeSession, event pluginruntime.PluginEvent) bool {
	runID := strings.TrimSpace(run.NativeRunID)
	if runID != "" {
		for _, candidate := range eventRunCandidates(event) {
			if candidate != "" && candidate == runID {
				return true
			}
		}
		return false
	}
	sessionID := strings.TrimSpace(run.NativeSessionID)
	threadID := strings.TrimSpace(run.NativeThreadID)
	sessionCandidates := append(eventSessionCandidates(event), strings.TrimSpace(session.NativeSessionID))
	threadCandidates := append(eventThreadCandidates(event), strings.TrimSpace(session.NativeThreadID))
	if sessionID != "" {
		for _, candidate := range sessionCandidates {
			if candidate != "" && candidate == sessionID {
				return true
			}
		}
	}
	if threadID != "" {
		for _, candidate := range threadCandidates {
			if candidate != "" && candidate == threadID {
				return true
			}
		}
	}
	return false
}

func eventRunCandidates(event pluginruntime.PluginEvent) []string {
	values := []string{
		strings.TrimSpace(event.ID),
		payloadString(event.Payload, "run_id", "runId", "native_run_id", "turn_id", "turnId", "task_id"),
	}
	return normalizeCandidates(values)
}

func eventSessionCandidates(event pluginruntime.PluginEvent) []string {
	values := []string{
		payloadString(event.Payload, "native_session_id", "session_id", "sessionId", "sessionKey", "session_key"),
	}
	return normalizeCandidates(values)
}

func eventThreadCandidates(event pluginruntime.PluginEvent) []string {
	values := []string{
		payloadString(event.Payload, "native_thread_id", "thread_id", "threadId"),
	}
	return normalizeCandidates(values)
}

func payloadString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if payload == nil {
			return ""
		}
		if text, ok := payload[key].(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
		if payload[key] != nil {
			text := strings.TrimSpace(toString(payload[key]))
			if text != "" {
				return text
			}
		}
	}
	return ""
}

func normalizeCandidates(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func (t *RunTracker) ensureRuns() {
	if t.runs == nil {
		t.runs = map[string]trackedForwardRun{}
	}
}

func (t *RunTracker) gcLocked(now time.Time) {
	for key, run := range t.runs {
		if !run.ExpiresAt.IsZero() && now.After(run.ExpiresAt) {
			delete(t.runs, key)
		}
	}
}

func (t *RunTracker) timeNow() time.Time {
	if t != nil && t.now != nil {
		return t.now()
	}
	return time.Now()
}

func (t *RunTracker) activeTTLValue() time.Duration {
	if t != nil && t.activeTTL > 0 {
		return t.activeTTL
	}
	return defaultForwardRunTTL
}

func (t *RunTracker) terminalGraceValue() time.Duration {
	if t != nil && t.terminalGrace > 0 {
		return t.terminalGrace
	}
	return defaultForwardRunTerminal
}

func toString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}
