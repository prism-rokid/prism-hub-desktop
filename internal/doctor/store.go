package doctor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// maxTrackedFindings bounds the persisted set. Resolved findings are dropped
// oldest-first; open findings are never dropped.
const maxTrackedFindings = 100

// dismissedReopenDelay keeps a dismissed finding closed for a while even when
// its check still detects the anomaly, so dismissal is not instantly undone.
const dismissedReopenDelay = time.Hour

// Auto-heal guardrails (docs/14 §2.2): minimum spacing between automatic
// attempts per finding, attempts allowed per rolling window, and the window.
const (
	autoHealCooldown     = 10 * time.Minute
	autoHealWindow       = time.Hour
	autoHealMaxPerWindow = 3
)

// Audit-event ring bounds (docs/14 §6.3).
const (
	maxEvents   = 1000
	maxEventAge = 30 * 24 * time.Hour
)

// store persists findings and the audit-event ring across Hub restarts in one
// JSON file, written with the same temp-file+rename contract as
// daemon-runtime-state.json. The Hub's SQLite connection is owned by the
// bridge runtime and may not even exist when configuration is broken, so
// doctor state must not depend on it.
type store struct {
	path string

	mu       sync.Mutex
	findings map[string]*Finding
	events   []Event
	// saveMu serializes the whole persist (payload build through rename) so
	// concurrent savers cannot rename an older payload over a newer one.
	saveMu sync.Mutex
}

type storePayload struct {
	SavedAt  string    `json:"saved_at"`
	Findings []Finding `json:"findings"`
	Events   []Event   `json:"events,omitempty"`
}

func openStore(path string) *store {
	s := &store{path: path, findings: map[string]*Finding{}, events: []Event{}}
	payload, err := os.ReadFile(path)
	if err != nil {
		return s
	}
	var parsed storePayload
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return s
	}
	now := time.Now()
	for _, finding := range parsed.Findings {
		finding := finding
		id := strings.TrimSpace(finding.ID)
		if id == "" {
			id = finding.findingID()
		}
		if _, exists := s.findings[id]; exists {
			continue
		}
		switch finding.State {
		case FindingOpen:
			// Anything mid-action from a previous process cannot still be running.
			s.findings[id] = &finding
		case FindingDismissed:
			// A fresh dismissal survives restart until dismissedReopenDelay
			// passes; expired dismissals are dropped so the next sweep
			// redetects the anomaly as open.
			if !s.dismissalExpired(&finding, now) {
				s.findings[id] = &finding
			}
		}
	}
	s.events = append(s.events, parsed.Events...)
	s.pruneEventsLocked(time.Now())
	return s
}

func (s *store) snapshot() []Finding {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Finding, 0, len(s.findings))
	for _, finding := range s.findings {
		out = append(out, *finding)
	}
	return out
}

func (s *store) get(id string) (Finding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	finding, ok := s.findings[id]
	if !ok {
		return Finding{}, false
	}
	return *finding, true
}

// reconcile merges one sweep's detected findings into the store. Detected IDs
// that were open stay open (Occurrences++ when carried over, remedies and
// hints refreshed); previously open IDs missing from detected are resolved.
// Dismissed findings stay dismissed until dismissedReopenDelay passes. It
// returns true when persisted state changed.
func (s *store) reconcile(now time.Time, checkID string, detected []Finding) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	detectedByID := map[string]Finding{}
	for _, finding := range detected {
		detectedByID[finding.findingID()] = finding
	}
	changed := false
	for id, finding := range s.findings {
		if finding.Check != checkID {
			continue
		}
		current, stillDetected := detectedByID[id]
		switch {
		case stillDetected && finding.State == FindingOpen:
			// The occurrence bump itself is a persisted change, so always
			// mark dirty: otherwise the incremented counter is lost when the
			// save is skipped and does not survive restart.
			changed = true
			finding.Severity = current.Severity
			finding.Detail = current.Detail
			finding.SummaryKey = current.SummaryKey
			finding.SummaryParams = current.SummaryParams
			finding.HintKey = current.HintKey
			finding.HintParams = current.HintParams
			finding.Remedies = current.Remedies
			finding.Occurrences++
			finding.UpdatedAt = now
		case stillDetected && (finding.State == FindingResolved || finding.State == FindingDismissed && s.dismissalExpired(finding, now)):
			// A resolved (or long-dismissed) anomaly came back: reopen as a
			// fresh detection with fresh auto-heal budget.
			finding.State = FindingOpen
			finding.Severity = current.Severity
			finding.Detail = current.Detail
			finding.SummaryKey = current.SummaryKey
			finding.SummaryParams = current.SummaryParams
			finding.HintKey = current.HintKey
			finding.HintParams = current.HintParams
			finding.Remedies = current.Remedies
			finding.DetectedAt = now
			finding.UpdatedAt = now
			finding.Occurrences = 1
			finding.ResolvedAt = nil
			finding.Escalated = false
			finding.AutoHealAttempts = 0
			finding.LastAutoHealAt = time.Time{}
			finding.AutoHealHistory = nil
			changed = true
		case !stillDetected && finding.State == FindingOpen:
			finding.State = FindingResolved
			finding.UpdatedAt = now
			resolvedAt := now
			finding.ResolvedAt = &resolvedAt
			changed = true
		}
		delete(detectedByID, id)
	}
	for id, finding := range detectedByID {
		finding.ID = id
		finding.State = FindingOpen
		finding.DetectedAt = now
		finding.UpdatedAt = now
		finding.Occurrences = 1
		s.findings[id] = &finding
		changed = true
	}
	if changed {
		s.trimLocked()
	}
	return changed
}

func (s *store) dismissalExpired(finding *Finding, now time.Time) bool {
	var dismissedAt time.Time
	if finding.ResolvedAt != nil {
		dismissedAt = *finding.ResolvedAt
	} else {
		dismissedAt = finding.UpdatedAt
	}
	return now.Sub(dismissedAt) >= dismissedReopenDelay
}

// Dismiss closes a finding by user choice. Returns false when the finding
// does not exist or is not open.
func (s *store) Dismiss(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	finding, ok := s.findings[id]
	if !ok || finding.State != FindingOpen {
		return false
	}
	finding.State = FindingDismissed
	finding.UpdatedAt = now
	resolvedAt := now
	finding.ResolvedAt = &resolvedAt
	s.findings[id] = finding
	return true
}

// canAutoHeal evaluates the guardrails for one automatic attempt. The
// returned reason explains a refusal (cooldown, window cap, escalation) so
// the audit event can carry it. Reaching the window cap escalates the finding
// to confirm-only right here (docs/14 §2.2).
func (s *store) canAutoHeal(id string, now time.Time) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	finding, ok := s.findings[id]
	if !ok || finding.State != FindingOpen {
		return false, "finding not open"
	}
	if finding.Escalated {
		return false, "escalated after repeated failed auto-heals"
	}
	if !finding.LastAutoHealAt.IsZero() && now.Sub(finding.LastAutoHealAt) < autoHealCooldown {
		return false, "cooldown"
	}
	if autoHealAttemptsInWindow(finding, now) >= autoHealMaxPerWindow {
		finding.Escalated = true
		s.appendEventLocked(Event{
			At:      now,
			Finding: finding.ID,
			Check:   finding.Check,
			Subject: finding.Subject,
			Trigger: TriggerSystem,
			Result:  ResultEscalated,
			Detail:  "auto-heal hourly attempt cap reached",
		})
		return false, "hourly attempt cap reached"
	}
	return true, ""
}

func (s *store) appendEventLocked(event Event) {
	s.events = append(s.events, event)
	s.pruneEventsLocked(event.At)
}

func autoHealAttemptsInWindow(finding *Finding, now time.Time) int {
	count := 0
	for _, at := range finding.AutoHealHistory {
		if now.Sub(at) <= autoHealWindow {
			count++
		}
	}
	return count
}

// recordAutoAttempt books one automatic attempt. When the rolling-window cap
// is hit the finding escalates (confirm-only) and reports the escalation.
func (s *store) recordAutoAttempt(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	finding, ok := s.findings[id]
	if !ok {
		return false
	}
	finding.AutoHealAttempts++
	finding.LastAutoHealAt = now
	history := make([]time.Time, 0, len(finding.AutoHealHistory)+1)
	for _, at := range finding.AutoHealHistory {
		if now.Sub(at) <= autoHealWindow {
			history = append(history, at)
		}
	}
	history = append(history, now)
	finding.AutoHealHistory = history
	if len(history) >= autoHealMaxPerWindow && !finding.Escalated {
		finding.Escalated = true
		s.appendEventLocked(Event{
			At:      now,
			Finding: finding.ID,
			Check:   finding.Check,
			Subject: finding.Subject,
			Trigger: TriggerSystem,
			Result:  ResultEscalated,
			Detail:  "auto-heal hourly attempt cap reached",
		})
	}
	s.findings[id] = finding
	return finding.Escalated
}

func (s *store) appendEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendEventLocked(event)
}

func (s *store) pruneEventsLocked(now time.Time) {
	if len(s.events) > maxEvents {
		s.events = append([]Event(nil), s.events[len(s.events)-maxEvents:]...)
	}
	cut := 0
	for cut < len(s.events) && now.Sub(s.events[cut].At) > maxEventAge {
		cut++
	}
	if cut > 0 {
		s.events = append([]Event(nil), s.events[cut:]...)
	}
}

// Events returns the newest-first audit trail, bounded by limit (<=0 → all).
func (s *store) Events(limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// LastSuccessfulAutoHeal returns the newest successful auto event, if any.
func (s *store) LastSuccessfulAutoHeal() *Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.events) - 1; i >= 0; i-- {
		event := s.events[i]
		if event.Trigger == TriggerAuto && event.Result == ResultSuccess {
			event := event
			return &event
		}
	}
	return nil
}

func (s *store) trimLocked() {
	if len(s.findings) <= maxTrackedFindings {
		return
	}
	var resolvedIDs []string
	for id, finding := range s.findings {
		if finding.State == FindingResolved {
			resolvedIDs = append(resolvedIDs, id)
		}
	}
	// Oldest resolved first.
	for i := 0; i < len(resolvedIDs); i++ {
		for j := i + 1; j < len(resolvedIDs); j++ {
			if s.findings[resolvedIDs[j]].UpdatedAt.Before(s.findings[resolvedIDs[i]].UpdatedAt) {
				resolvedIDs[i], resolvedIDs[j] = resolvedIDs[j], resolvedIDs[i]
			}
		}
	}
	overflow := len(s.findings) - maxTrackedFindings
	for _, id := range resolvedIDs {
		if overflow <= 0 {
			break
		}
		delete(s.findings, id)
		overflow--
	}
}

func (s *store) save(now time.Time) error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.Lock()
	payload := storePayload{
		SavedAt:  now.UTC().Format(time.RFC3339),
		Findings: make([]Finding, 0, len(s.findings)),
		Events:   make([]Event, len(s.events)),
	}
	for _, finding := range s.findings {
		payload.Findings = append(payload.Findings, *finding)
	}
	copy(payload.Events, s.events)
	s.mu.Unlock()
	path := strings.TrimSpace(s.path)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".doctor-state-*.json")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

func (s *store) openFindings() []Finding {
	items := s.snapshot()
	out := make([]Finding, 0, len(items))
	for _, finding := range items {
		if finding.State == FindingOpen {
			out = append(out, finding)
		}
	}
	sortFindings(out)
	return out
}

func severityRank(severity Severity) int {
	switch severity {
	case SeverityCritical:
		return 0
	case SeverityWarning:
		return 1
	default:
		return 2
	}
}

func sortFindings(items []Finding) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0; j-- {
			previous, current := items[j-1], items[j]
			if severityRank(previous.Severity) < severityRank(current.Severity) {
				break
			}
			if severityRank(previous.Severity) == severityRank(current.Severity) &&
				previous.ID <= current.ID {
				break
			}
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
}
