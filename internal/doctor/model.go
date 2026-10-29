// Package doctor runs Prism Hub's local self-checks. M1 scope is read-only:
// checks observe filesystem, configuration and passive runtime state, and
// never probe or restart anything. Healing actions arrive in later milestones.
package doctor

import "time"

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type FindingState string

const (
	FindingOpen      FindingState = "open"
	FindingResolved  FindingState = "resolved"
	FindingDismissed FindingState = "dismissed"
)

// Finding is one anomaly attached to a subject (plugin id, "hub", "config",
// ...). The same Check+Subject reuses one Finding across sweeps; Occurrences
// counts how many sweeps re-detected it.
type Finding struct {
	ID            string            `json:"id"`
	Check         string            `json:"check"`
	Subject       string            `json:"subject,omitempty"`
	Severity      Severity          `json:"severity"`
	State         FindingState      `json:"state"`
	SummaryKey    string            `json:"summary_i18n"`
	SummaryParams map[string]string `json:"summary_params,omitempty"`
	// HintKey/HintParams carry the friendly "how to solve this" guidance.
	HintKey    string            `json:"hint_i18n,omitempty"`
	HintParams map[string]string `json:"hint_params,omitempty"`
	// Detail is technical, non-localized context (error text, paths, sizes).
	Detail string `json:"detail,omitempty"`
	// Remedies are refreshed by the owning check on every sweep.
	Remedies []Remedy `json:"remedies,omitempty"`
	// Guardrail bookkeeping for automatic healing (docs/14 §2.2).
	Escalated        bool        `json:"escalated,omitempty"`
	AutoHealAttempts int         `json:"auto_heal_attempts,omitempty"`
	LastAutoHealAt   time.Time   `json:"last_auto_heal_at,omitempty"`
	AutoHealHistory  []time.Time `json:"auto_heal_history,omitempty"`
	DetectedAt       time.Time   `json:"detected_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
	ResolvedAt       *time.Time  `json:"resolved_at,omitempty"`
	Occurrences      int         `json:"occurrences"`
}

// RiskClass mirrors docs/14 §2.1: transparent remedies run automatically,
// confirm remedies need an explicit user click, manual is guidance only.
type RiskClass string

const (
	RiskTransparent RiskClass = "transparent"
	RiskConfirm     RiskClass = "confirm"
	RiskManual      RiskClass = "manual"
)

// Remedy is one actionable fix attached to a finding. Risk is the *current*
// class after impact escalation (a normally-transparent action touching a
// live plugin reports confirm here).
type Remedy struct {
	ID                string            `json:"id"`
	TitleKey          string            `json:"title_i18n"`
	Risk              RiskClass         `json:"risk"`
	ImpactKey         string            `json:"impact_i18n,omitempty"`
	ImpactParams      map[string]string `json:"impact_params,omitempty"`
	Available         bool              `json:"available"`
	UnavailableReason string            `json:"unavailable_reason,omitempty"`
}

// Event records one doctor action for the audit trail (docs/14 §2.2).
type Event struct {
	At      time.Time `json:"at"`
	Finding string    `json:"finding"`
	Check   string    `json:"check"`
	Subject string    `json:"subject,omitempty"`
	Remedy  string    `json:"remedy,omitempty"`
	Risk    string    `json:"risk,omitempty"`
	// Trigger: auto | manual | dismiss.
	Trigger string `json:"trigger"`
	// Result: success | failed | rejected.
	Result string `json:"result"`
	Error  string `json:"error,omitempty"`
	Detail string `json:"detail,omitempty"`
}

const (
	TriggerAuto    = "auto"
	TriggerManual  = "manual"
	TriggerDismiss = "dismiss"
	TriggerSystem  = "system"

	ResultSuccess   = "success"
	ResultFailed    = "failed"
	ResultRejected  = "rejected"
	ResultEscalated = "escalated"
)

// AutoHealNotice is the compact "we fixed something" signal the Dashboard
// toasts once per timestamp.
type AutoHealNotice struct {
	At            time.Time         `json:"at"`
	SummaryKey    string            `json:"summary_i18n"`
	SummaryParams map[string]string `json:"summary_params,omitempty"`
}

func (f Finding) findingID() string {
	if f.Subject == "" {
		return f.Check
	}
	return f.Check + ":" + f.Subject
}

// CheckStatus is the per-check bookkeeping exposed in reports.
type CheckStatus struct {
	ID              string    `json:"id"`
	IntervalSeconds int       `json:"interval_seconds"`
	LastRunAt       time.Time `json:"last_run_at,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	OpenFindings    int       `json:"open_findings"`
}

// Report is the full doctor snapshot served by /api/doctor/report.
type Report struct {
	GeneratedAt time.Time     `json:"generated_at"`
	State       string        `json:"state"`
	Checks      []CheckStatus `json:"checks"`
	Findings    []Finding     `json:"findings"`
}

// Summary is the compact block embedded in /api/status.
type Summary struct {
	State                string          `json:"state"`
	OpenFindings         int             `json:"open_findings"`
	RequiresConfirmation int             `json:"requires_confirmation"`
	LastRunAt            string          `json:"last_run_at,omitempty"`
	LastAutoHeal         *AutoHealNotice `json:"last_auto_heal,omitempty"`
}
