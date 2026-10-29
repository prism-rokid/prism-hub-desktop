package doctor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// Check is one health check. Run observes state; checks that also offer
// remedies implement RemedyCarrier. Per-check execution is bounded by
// checkTimeout.
type Check interface {
	ID() string
	Interval() time.Duration
	Run(ctx context.Context) ([]Finding, error)
}

// RemedyCarrier is implemented by checks that can fix their own findings.
// Remedies refreshes the serializable remedy metadata attached to a finding
// (risk already reflects impact escalation); ExecuteRemedy performs the fix.
type RemedyCarrier interface {
	Remedies(finding Finding) []Remedy
	ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error
}

const (
	// schedulerTick evaluates which checks are due.
	schedulerTick = 15 * time.Second
	// initialDelay keeps the first sweep off the Hub startup path.
	initialDelay = 30 * time.Second
	// checkTimeout bounds a single check; sweepTimeout bounds a full sweep.
	checkTimeout = 2 * time.Second
	sweepTimeout = 10 * time.Second
	// remedyTimeout bounds one remedy execution. Transparent auto-heals may
	// download a Node runtime, so this is deliberately generous.
	remedyTimeout = 10 * time.Minute
)

// Gates carries the runtime kill switches (config `doctor.*`, docs/15 §2.2).
// Nil functions default to enabled.
type Gates struct {
	Enabled  func() bool
	AutoHeal func() bool
}

func (g Gates) enabled() bool {
	return g.Enabled == nil || g.Enabled()
}

func (g Gates) autoHeal() bool {
	return g.AutoHeal == nil || g.AutoHeal()
}

// Service owns the check registry, scheduling, persisted findings, and the
// remedy executor (transparent auto-heal + manual one-click fixes).
type Service struct {
	store  *store
	logger *slog.Logger
	gates  Gates
	now    func() time.Time

	mu        sync.RWMutex
	checks    []Check
	lastRun   map[string]time.Time
	lastError map[string]string
	lastSweep time.Time

	// sweepMu serializes sweeps; a concurrent sweep request is coalesced by
	// TryLock instead of queueing. executorMu serializes remedy execution.
	sweepMu    sync.Mutex
	executorMu sync.Mutex
}

func NewService(statePath string, logger *slog.Logger, gates Gates) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		store:     openStore(statePath),
		logger:    logger,
		gates:     gates,
		now:       time.Now,
		lastRun:   map[string]time.Time{},
		lastError: map[string]string{},
	}
}

// Register adds checks. It must be called before Run.
func (s *Service) Register(checks ...Check) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, check := range checks {
		if check == nil {
			continue
		}
		s.checks = append(s.checks, check)
	}
}

// Run drives periodic sweeps until ctx is cancelled.
func (s *Service) Run(ctx context.Context) {
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if s.gates.enabled() {
			s.sweepDue(ctx)
		}
		timer.Reset(schedulerTick)
	}
}

func (s *Service) sweepDue(ctx context.Context) {
	now := s.now()
	// Recheck known problems on each scheduler tick so recovery and transient
	// check errors do not linger for the normal one-to-five-minute interval.
	openChecks := map[string]bool{}
	for _, finding := range s.store.openFindings() {
		openChecks[finding.Check] = true
	}
	s.mu.RLock()
	var due []Check
	for _, check := range s.checks {
		if last, ok := s.lastRun[check.ID()]; !ok || now.Sub(last) >= check.Interval() || openChecks[check.ID()] || s.lastError[check.ID()] != "" {
			due = append(due, check)
		}
	}
	s.mu.RUnlock()
	if len(due) == 0 {
		return
	}
	s.runChecks(ctx, due)
}

// RunAll forces every check to run immediately and returns the fresh report.
// It backs POST /api/doctor/run.
func (s *Service) RunAll(ctx context.Context) (Report, error) {
	if !s.gates.enabled() {
		return Report{GeneratedAt: s.now().UTC(), State: "ok", Checks: []CheckStatus{}}, nil
	}
	s.mu.RLock()
	checks := append([]Check(nil), s.checks...)
	s.mu.RUnlock()
	// Manual checks must actually run, even when a periodic sweep is busy.
	s.sweepMu.Lock()
	defer s.sweepMu.Unlock()
	if err := ctx.Err(); err != nil {
		return s.Report(), err
	}
	s.runChecksLocked(ctx, checks)
	return s.Report(), nil
}

func (s *Service) runChecks(ctx context.Context, checks []Check) {
	if !s.sweepMu.TryLock() {
		return
	}
	defer s.sweepMu.Unlock()
	s.runChecksLocked(ctx, checks)
}

func (s *Service) runChecksLocked(ctx context.Context, checks []Check) {
	sweepCtx, cancel := context.WithTimeout(ctx, sweepTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, check := range checks {
		check := check
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runOne(sweepCtx, check)
		}()
	}
	wg.Wait()
}

func (s *Service) runOne(ctx context.Context, check Check) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.recordRun(check, fmt.Errorf("check panicked: %v", recovered))
		}
	}()
	runCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	findings, err := check.Run(runCtx)
	if err != nil {
		// A failed check must not resolve its previous findings: the anomaly
		// is simply unobserved this round.
		s.recordRun(check, err)
		return
	}
	if carrier, ok := check.(RemedyCarrier); ok {
		for index := range findings {
			findings[index].Remedies = carrier.Remedies(findings[index])
		}
	}
	now := s.now()
	if changed := s.store.reconcile(now, check.ID(), findings); changed {
		if err := s.store.save(now); err != nil {
			s.logger.Warn("doctor state persist failed", "err", err)
		}
	}
	s.recordRun(check, nil)
	s.maybeAutoHeal(ctx, check)
}

// maybeAutoHeal starts at most one transparent remedy per sweep for findings
// of the check that just ran (docs/15 §2: 自动恢复先行). Execution is
// serialized globally and runs off the sweep path.
func (s *Service) maybeAutoHeal(ctx context.Context, check Check) {
	if !s.gates.enabled() || !s.gates.autoHeal() {
		return
	}
	carrier, ok := check.(RemedyCarrier)
	if !ok {
		return
	}
	for _, finding := range s.store.openFindings() {
		if finding.Check != check.ID() || finding.Escalated {
			continue
		}
		remedy := firstAvailableRemedy(finding, RiskTransparent)
		if remedy == nil {
			continue
		}
		allowed, reason := s.store.canAutoHeal(finding.ID, s.now())
		if !allowed {
			if reason != "cooldown" && reason != "finding not open" {
				_ = s.store.save(s.now())
			}
			continue
		}
		// recordAutoAttempt writes the escalation event itself when the
		// rolling-window cap is hit.
		s.store.recordAutoAttempt(finding.ID, s.now())
		go s.executeRemedyNow(context.WithoutCancel(ctx), check, carrier, finding, remedy.ID, TriggerAuto, true)
		return
	}
}

func firstAvailableRemedy(finding Finding, risk RiskClass) *Remedy {
	for index := range finding.Remedies {
		if finding.Remedies[index].Available && finding.Remedies[index].Risk == risk {
			remedy := finding.Remedies[index]
			return &remedy
		}
	}
	return nil
}

// executeRemedyNow runs one remedy with global serialization and bounded
// timeout, records the audit event, then re-runs the owning check so the
// finding state reflects the outcome immediately.
func (s *Service) executeRemedyNow(ctx context.Context, check Check, carrier RemedyCarrier, finding Finding, remedyID, trigger string, confirmed bool) (Event, error) {
	executorCtx, cancel := context.WithTimeout(ctx, remedyTimeout)
	defer cancel()
	event := Event{
		At:      s.now(),
		Finding: finding.ID,
		Check:   finding.Check,
		Subject: finding.Subject,
		Remedy:  remedyID,
		Trigger: trigger,
	}
	execErr := func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("remedy panicked: %v", recovered)
			}
		}()
		s.executorMu.Lock()
		defer s.executorMu.Unlock()
		// Re-evaluate the risk class only after the serializer is held
		// (docs/15 §6): this execution may have waited behind a long remedy
		// (a Node download can hold the executor for minutes), and a
		// transparent authorization captured at sweep time must never run
		// against an impact context that escalated during the wait.
		if trigger == TriggerAuto && !remedyStillTransparent(carrier, finding, remedyID) {
			return errRiskEscalated
		}
		return carrier.ExecuteRemedy(executorCtx, finding, remedyID)
	}()
	switch {
	case errors.Is(execErr, errRiskEscalated):
		event.Result = ResultRejected
		event.Detail = "remedy escalated before execution; manual confirmation required"
		execErr = nil
		s.logger.Warn("doctor auto-heal aborted after risk escalation",
			"finding", finding.ID, "remedy", remedyID)
	case execErr != nil:
		event.Result = ResultFailed
		event.Error = execErr.Error()
	default:
		event.Result = ResultSuccess
	}
	s.store.appendEvent(event)
	_ = s.store.save(s.now())
	if execErr != nil {
		s.logger.Warn("doctor remedy failed", "finding", finding.ID, "remedy", remedyID, "trigger", trigger, "err", execErr)
	} else {
		s.logger.Info("doctor remedy applied", "finding", finding.ID, "remedy", remedyID, "trigger", trigger)
	}
	// Re-observe: a successful fix resolves the finding on the spot.
	s.runOne(ctx, check)
	return event, execErr
}

// ExecuteRemedy backs the one-click fix endpoints: confirmed=true authorizes
// confirm-class remedies (the confirmation dialog has already shown the
// impact); transparent remedies may run without it.
func (s *Service) ExecuteRemedy(ctx context.Context, findingID, remedyID string, confirmed bool) (Event, error) {
	if !s.gates.enabled() {
		return Event{}, errors.New("doctor disabled")
	}
	finding, ok := s.store.get(findingID)
	if !ok || finding.State != FindingOpen {
		return Event{}, fmt.Errorf("finding not open: %s", findingID)
	}
	check := s.checkByID(finding.Check)
	if check == nil {
		return Event{}, fmt.Errorf("check unavailable: %s", finding.Check)
	}
	carrier, ok := check.(RemedyCarrier)
	if !ok {
		return Event{}, fmt.Errorf("check offers no remedies: %s", finding.Check)
	}
	remedies := carrier.Remedies(finding)
	var remedy *Remedy
	for index := range remedies {
		if remedies[index].ID == remedyID {
			candidate := remedies[index]
			remedy = &candidate
			break
		}
	}
	if remedy == nil {
		return Event{}, fmt.Errorf("unknown remedy %q for %s", remedyID, findingID)
	}
	if !remedy.Available {
		reason := strings.TrimSpace(remedy.UnavailableReason)
		if reason == "" {
			reason = errRemedyProviderUnavailable.Error()
		}
		return Event{}, fmt.Errorf("remedy unavailable: %s", reason)
	}
	// Impact-escalation guard: if the remedy has become confirm-class since
	// the card was rendered, a stale transparent authorization must not run.
	if remedy.Risk == RiskConfirm && !confirmed {
		return Event{}, errors.New("confirmation required")
	}
	return s.executeRemedyNow(ctx, check, carrier, finding, remedyID, TriggerManual, confirmed)
}

// Dismiss closes a finding by user choice and records the audit event.
func (s *Service) Dismiss(ctx context.Context, findingID string) error {
	if !s.gates.enabled() {
		return errors.New("doctor disabled")
	}
	now := s.now()
	if !s.store.Dismiss(findingID, now) {
		return fmt.Errorf("finding not open: %s", findingID)
	}
	if finding, ok := s.store.get(findingID); ok {
		s.store.appendEvent(Event{
			At:      now,
			Finding: findingID,
			Check:   finding.Check,
			Subject: finding.Subject,
			Trigger: TriggerDismiss,
			Result:  ResultSuccess,
		})
	}
	return s.store.save(now)
}

// Events exposes the audit trail for /api/doctor/history.
func (s *Service) Events(limit int) []Event {
	return s.store.Events(limit)
}

func (s *Service) checkByID(id string) Check {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, check := range s.checks {
		if check.ID() == id {
			return check
		}
	}
	return nil
}

func (s *Service) recordRun(check Check, err error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRun[check.ID()] = now
	if err != nil {
		s.lastError[check.ID()] = err.Error()
		s.logger.Warn("doctor check failed", "check", check.ID(), "err", err)
	} else {
		delete(s.lastError, check.ID())
	}
	s.lastSweep = now
}

// Report returns the full snapshot. Open findings are ordered by severity.
func (s *Service) Report() Report {
	s.mu.RLock()
	checks := append([]Check(nil), s.checks...)
	lastRun := make(map[string]time.Time, len(s.lastRun))
	for id, at := range s.lastRun {
		lastRun[id] = at
	}
	lastError := make(map[string]string, len(s.lastError))
	for id, message := range s.lastError {
		lastError[id] = message
	}
	s.mu.RUnlock()

	open := s.store.openFindings()
	openByCheck := map[string]int{}
	requiresConfirmation := 0
	for _, finding := range open {
		openByCheck[finding.Check]++
		if firstAvailableRemedy(finding, RiskConfirm) != nil {
			requiresConfirmation++
		}
	}
	statuses := make([]CheckStatus, 0, len(checks))
	for _, check := range checks {
		statuses = append(statuses, CheckStatus{
			ID:              check.ID(),
			IntervalSeconds: int(check.Interval().Seconds()),
			LastRunAt:       lastRun[check.ID()],
			LastError:       lastError[check.ID()],
			OpenFindings:    openByCheck[check.ID()],
		})
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	state := "ok"
	if len(open) > 0 {
		state = "findings"
	}
	if requiresConfirmation > 0 || anyEscalated(open) {
		state = "attention"
	}
	return Report{
		GeneratedAt: s.now().UTC(),
		State:       state,
		Checks:      statuses,
		Findings:    open,
	}
}

func anyEscalated(findings []Finding) bool {
	for _, finding := range findings {
		if finding.Escalated {
			return true
		}
	}
	return false
}

// Summary returns the compact block embedded in /api/status.
func (s *Service) Summary() Summary {
	report := s.Report()
	summary := Summary{
		State:                report.State,
		OpenFindings:         len(report.Findings),
		RequiresConfirmation: 0,
	}
	for _, finding := range report.Findings {
		if firstAvailableRemedy(finding, RiskConfirm) != nil {
			summary.RequiresConfirmation++
		}
	}
	if !s.lastSweepAt().IsZero() {
		summary.LastRunAt = s.lastSweepAt().UTC().Format(time.RFC3339)
	}
	if event := s.store.LastSuccessfulAutoHeal(); event != nil {
		if finding, ok := s.store.get(event.Finding); ok {
			summary.LastAutoHeal = &AutoHealNotice{
				At:            event.At,
				SummaryKey:    finding.SummaryKey,
				SummaryParams: finding.SummaryParams,
			}
		}
	}
	return summary
}

func (s *Service) lastSweepAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSweep
}
