package doctor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeCheck struct {
	id       string
	interval time.Duration
	findings []Finding
	err      error
	runs     int
}

func (c *fakeCheck) ID() string              { return c.id }
func (c *fakeCheck) Interval() time.Duration { return c.interval }
func (c *fakeCheck) Run(ctx context.Context) ([]Finding, error) {
	c.runs++
	return append([]Finding(nil), c.findings...), c.err
}

func newTestService(t *testing.T) (*Service, *fakeCheck) {
	t.Helper()
	service := NewService(filepath.Join(t.TempDir(), "doctor-state.json"), nil, Gates{})
	check := &fakeCheck{id: "fake.check", interval: time.Minute}
	service.Register(check)
	return service, check
}

func TestServiceTracksOpenAndResolved(t *testing.T) {
	service, check := newTestService(t)
	check.findings = []Finding{
		finding("fake.check", "a", SeverityWarning, "fake.one", nil, "boom"),
	}
	service.RunAll(context.Background())
	report := service.Report()
	if report.State != "findings" || len(report.Findings) != 1 {
		t.Fatalf("expected one open finding, got %+v", report)
	}
	if report.Findings[0].Occurrences != 1 || report.Findings[0].State != FindingOpen {
		t.Fatalf("unexpected finding state: %+v", report.Findings[0])
	}

	// Second sweep with the same anomaly carries the finding over.
	service.RunAll(context.Background())
	report = service.Report()
	if len(report.Findings) != 1 || report.Findings[0].Occurrences != 2 {
		t.Fatalf("expected carried-over finding with occurrences=2, got %+v", report.Findings)
	}

	// Anomaly disappears -> resolved.
	check.findings = nil
	service.RunAll(context.Background())
	report = service.Report()
	if report.State != "ok" || len(report.Findings) != 0 {
		t.Fatalf("expected resolved state, got %+v", report)
	}

	// Anomaly returns -> reopened as a fresh detection.
	check.findings = []Finding{finding("fake.check", "a", SeverityWarning, "fake.one", nil, "boom")}
	service.RunAll(context.Background())
	report = service.Report()
	if len(report.Findings) != 1 || report.Findings[0].Occurrences != 1 {
		t.Fatalf("expected reopened finding, got %+v", report.Findings)
	}
}

func TestServiceCheckErrorPreservesFindings(t *testing.T) {
	service, check := newTestService(t)
	check.findings = []Finding{finding("fake.check", "a", SeverityWarning, "fake.one", nil, "")}
	service.RunAll(context.Background())

	check.err = errors.New("probe exploded")
	check.findings = nil
	service.RunAll(context.Background())
	report := service.Report()
	if len(report.Findings) != 1 {
		t.Fatalf("failed check must keep previous findings, got %+v", report.Findings)
	}
	if report.Checks[0].LastError == "" {
		t.Fatalf("expected check error surfaced in report, got %+v", report.Checks[0])
	}
}

func TestServiceSummary(t *testing.T) {
	service, check := newTestService(t)
	summary := service.Summary()
	if summary.State != "ok" || summary.OpenFindings != 0 || summary.LastRunAt != "" {
		// Before any run there is no LastRunAt.
		if summary.LastRunAt != "" {
			t.Fatalf("unexpected last_run_at before first sweep: %+v", summary)
		}
	}
	check.findings = []Finding{finding("fake.check", "a", SeverityCritical, "fake.one", nil, "")}
	service.RunAll(context.Background())
	summary = service.Summary()
	if summary.State != "findings" || summary.OpenFindings != 1 || summary.LastRunAt == "" {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.RequiresConfirmation != 0 {
		t.Fatalf("M1 must not report confirmations: %+v", summary)
	}
}

func TestStorePersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "doctor-state.json")
	first := openStore(path)
	now := time.Now()
	first.reconcile(now, "fake.check", []Finding{
		finding("fake.check", "a", SeverityWarning, "fake.one", nil, "boom"),
	})
	if err := first.save(now); err != nil {
		t.Fatalf("save: %v", err)
	}

	reloaded := openStore(path)
	if _, ok := reloaded.get("fake.check:a"); !ok {
		t.Fatalf("expected open finding to survive reload")
	}
	// The anomaly is gone after reload: reconcile resolves it.
	if !reloaded.reconcile(time.Now(), "fake.check", nil) {
		t.Fatalf("expected state change to resolved")
	}
	items := reloaded.snapshot()
	if len(items) != 1 || items[0].State != FindingResolved {
		t.Fatalf("expected resolved finding, got %+v", items)
	}
}

func TestStoreOccurrencesPersistAcrossReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "doctor-state.json")
	store := openStore(path)
	detected := []Finding{finding("fake.check", "a", SeverityWarning, "fake.one", nil, "boom")}
	now := time.Now()
	store.reconcile(now, "fake.check", detected)
	// Same anomaly again: Occurrences bumps to 2 and must be persisted.
	if changed := store.reconcile(now.Add(time.Minute), "fake.check", detected); !changed {
		t.Fatal("occurrence bump must mark the store dirty so the save happens")
	}
	if err := store.save(now.Add(time.Minute)); err != nil {
		t.Fatalf("save: %v", err)
	}

	reloaded := openStore(path)
	item, ok := reloaded.get("fake.check:a")
	if !ok {
		t.Fatal("expected finding to survive reload")
	}
	if item.Occurrences != 2 {
		t.Fatalf("expected occurrences=2 after reload, got %d", item.Occurrences)
	}
}

func TestStoreDismissedPersistsAcrossReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "doctor-state.json")
	store := openStore(path)
	now := time.Now()
	detected := []Finding{finding("fake.check", "a", SeverityWarning, "fake.one", nil, "")}
	store.reconcile(now, "fake.check", detected)
	if !store.Dismiss("fake.check:a", now) {
		t.Fatal("dismiss failed")
	}
	if err := store.save(now); err != nil {
		t.Fatalf("save: %v", err)
	}

	reloaded := openStore(path)
	item, ok := reloaded.get("fake.check:a")
	if !ok || item.State != FindingDismissed {
		t.Fatalf("expected dismissed finding to survive reload, got %+v ok=%v", item, ok)
	}
	// A still-detected, unexpired dismissal must not reopen as open.
	if changed := reloaded.reconcile(now.Add(10*time.Minute), "fake.check", detected); changed {
		t.Fatal("unexpired dismissed finding must stay dismissed after reload")
	}

	// Expired dismissal: build a payload whose dismissal is older than
	// dismissedReopenDelay and verify it is dropped at load time.
	fresh := openStore(filepath.Join(dir, "expired.json"))
	fresh.reconcile(now, "fake.check", detected)
	fresh.Dismiss("fake.check:a", now.Add(-dismissedReopenDelay-5*time.Minute))
	if err := fresh.save(now); err != nil {
		t.Fatalf("save expired: %v", err)
	}
	expired := openStore(filepath.Join(dir, "expired.json"))
	if _, ok := expired.get("fake.check:a"); ok {
		t.Fatal("expired dismissal must not be restored at load")
	}
}

func TestStoreDismissedStaysClosed(t *testing.T) {
	store := openStore(filepath.Join(t.TempDir(), "doctor-state.json"))
	now := time.Now()
	detected := []Finding{finding("fake.check", "a", SeverityWarning, "fake.one", nil, "")}
	store.reconcile(now, "fake.check", detected)

	// Simulate dismissal: state -> dismissed.
	items := store.snapshot()
	dismissed := items[0]
	dismissed.State = FindingDismissed
	dismissed.ResolvedAt = &now
	store.mu.Lock()
	store.findings[dismissed.ID] = &dismissed
	store.mu.Unlock()

	if changed := store.reconcile(now.Add(10*time.Minute), "fake.check", detected); changed {
		t.Fatalf("dismissed finding must stay dismissed inside the cooldown")
	}
	if changed := store.reconcile(now.Add(dismissedReopenDelay+time.Minute), "fake.check", detected); !changed {
		t.Fatalf("dismissed finding must reopen after the cooldown")
	}
}

func TestCheckPanicIsRecorded(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	panicking := &panicCheck{}
	service.Register(panicking)
	report, _ := service.RunAll(context.Background())
	if report.Checks[0].LastError == "" {
		t.Fatalf("expected panic recorded as check error, got %+v", report.Checks[0])
	}
}

type panicCheck struct{}

func (c *panicCheck) ID() string              { return "fake.panic" }
func (c *panicCheck) Interval() time.Duration { return time.Minute }
func (c *panicCheck) Run(ctx context.Context) ([]Finding, error) {
	panic("boom")
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// --- remedy executor ---------------------------------------------------------

type fakeCarrierCheck struct {
	id          string
	findings    []Finding
	remedyRisk  RiskClass
	available   bool
	execErr     error
	clearsOnFix bool
	executed    []string
	execMu      sync.Mutex
}

func (c *fakeCarrierCheck) ID() string              { return c.id }
func (c *fakeCarrierCheck) Interval() time.Duration { return time.Minute }
func (c *fakeCarrierCheck) Run(ctx context.Context) ([]Finding, error) {
	return append([]Finding(nil), c.findings...), nil
}

func (c *fakeCarrierCheck) Remedies(finding Finding) []Remedy {
	return []Remedy{{
		ID:        "fix",
		TitleKey:  "doctor.remedy_run",
		Risk:      c.remedyRisk,
		Available: c.available,
	}}
}

func (c *fakeCarrierCheck) ExecuteRemedy(ctx context.Context, finding Finding, remedyID string) error {
	c.execMu.Lock()
	c.executed = append(c.executed, remedyID)
	// A successful fix clears the anomaly so the post-execution re-observe
	// sweep resolves the finding instead of keeping the auto-heal loop hot.
	if c.execErr == nil && c.clearsOnFix {
		c.findings = nil
	}
	c.execMu.Unlock()
	return c.execErr
}

func (c *fakeCarrierCheck) executionCount() int {
	c.execMu.Lock()
	defer c.execMu.Unlock()
	return len(c.executed)
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestAutoHealRunsTransparentRemedy(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	check := &fakeCarrierCheck{id: "fake.heal", remedyRisk: RiskTransparent, available: true, clearsOnFix: true}
	check.findings = []Finding{finding("fake.heal", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)

	service.RunAll(context.Background())
	waitFor(t, 2*time.Second, func() bool { return check.executionCount() >= 1 })

	// The executed remedy resolves the anomaly: the next run is clean.
	waitFor(t, 2*time.Second, func() bool { return len(service.Events(10)) >= 1 })
	events := service.Events(10)
	if events[0].Trigger != TriggerAuto || events[0].Result != ResultSuccess {
		t.Fatalf("expected successful auto event, got %+v", events[0])
	}
	// Wait for the post-execution re-observe sweep to settle so the detached
	// goroutine's state persist cannot race the test's TempDir cleanup.
	waitFor(t, 2*time.Second, func() bool { return service.Summary().OpenFindings == 0 })
}

func TestAutoHealDisabledByGate(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{AutoHeal: func() bool { return false }})
	check := &fakeCarrierCheck{id: "fake.gated", remedyRisk: RiskTransparent, available: true}
	check.findings = []Finding{finding("fake.gated", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)

	service.RunAll(context.Background())
	time.Sleep(100 * time.Millisecond)
	if check.executionCount() != 0 {
		t.Fatalf("auto-heal must not run when disabled")
	}
	// Manual execution still works.
	if _, err := service.ExecuteRemedy(context.Background(), "fake.gated:a", "fix", false); err != nil {
		t.Fatalf("manual transparent remedy should run: %v", err)
	}
}

func TestExecuteRemedyRequiresConfirmation(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	check := &fakeCarrierCheck{id: "fake.confirm", remedyRisk: RiskConfirm, available: true}
	check.findings = []Finding{finding("fake.confirm", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)
	service.RunAll(context.Background())

	if _, err := service.ExecuteRemedy(context.Background(), "fake.confirm:a", "fix", false); err == nil {
		t.Fatal("confirm-class remedy must refuse without confirmed=true")
	}
	if _, err := service.ExecuteRemedy(context.Background(), "fake.confirm:a", "fix", true); err != nil {
		t.Fatalf("confirmed remedy should run: %v", err)
	}
}

func TestSummaryCountsConfirmRemedies(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	check := &fakeCarrierCheck{id: "fake.confirm2", remedyRisk: RiskConfirm, available: true}
	check.findings = []Finding{finding("fake.confirm2", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)
	service.RunAll(context.Background())

	summary := service.Summary()
	if summary.OpenFindings != 1 || summary.RequiresConfirmation != 1 || summary.State != "attention" {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestDismissRecordsEvent(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	check := &fakeCarrierCheck{id: "fake.dismiss", remedyRisk: RiskConfirm, available: false}
	check.findings = []Finding{finding("fake.dismiss", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)
	service.RunAll(context.Background())

	if err := service.Dismiss(context.Background(), "fake.dismiss:a"); err != nil {
		t.Fatalf("dismiss: %v", err)
	}
	summary := service.Summary()
	if summary.OpenFindings != 0 {
		t.Fatalf("dismissed finding must leave the open set: %+v", summary)
	}
	events := service.Events(10)
	if len(events) == 0 || events[0].Trigger != TriggerDismiss {
		t.Fatalf("expected dismiss event, got %+v", events)
	}
}

func TestAutoHealGuardrailsEscalate(t *testing.T) {
	store := openStore(filepath.Join(t.TempDir(), "state.json"))
	now := time.Now()
	store.reconcile(now, "fake.cap", []Finding{finding("fake.cap", "a", SeverityWarning, "fake.one", nil, "")})

	if ok, reason := store.canAutoHeal("fake.cap:a", now.Add(time.Second)); !ok {
		t.Fatalf("first attempt must be allowed, got %q", reason)
	}
	store.recordAutoAttempt("fake.cap:a", now.Add(time.Second))
	if ok, _ := store.canAutoHeal("fake.cap:a", now.Add(2*time.Second)); ok {
		t.Fatal("cooldown must block the immediate second attempt")
	}
	if ok, _ := store.canAutoHeal("fake.cap:a", now.Add(autoHealCooldown+2*time.Second)); !ok {
		t.Fatal("attempt after cooldown must be allowed")
	}
	store.recordAutoAttempt("fake.cap:a", now.Add(autoHealCooldown+2*time.Second))
	store.recordAutoAttempt("fake.cap:a", now.Add(2*autoHealCooldown+2*time.Second))
	ok, reason := store.canAutoHeal("fake.cap:a", now.Add(3*autoHealCooldown+2*time.Second))
	if ok || reason != "hourly attempt cap reached" && reason != "escalated after repeated failed auto-heals" {
		t.Fatalf("expected cap/escalation refusal, got ok=%v reason=%q", ok, reason)
	}
	item, _ := store.get("fake.cap:a")
	if !item.Escalated {
		t.Fatal("expected finding to escalate after the window cap")
	}
	events := store.Events(10)
	if len(events) == 0 || events[0].Result != ResultEscalated {
		t.Fatalf("expected escalation event, got %+v", events)
	}
}

func TestAutoHealAbortsWhenRiskEscalatesBeforeExecution(t *testing.T) {
	service := NewService(filepath.Join(t.TempDir(), "state.json"), nil, Gates{})
	check := &escalatingCarrierCheck{fakeCarrierCheck: fakeCarrierCheck{id: "fake.escalate", available: true}}
	check.findings = []Finding{finding("fake.escalate", "a", SeverityWarning, "fake.one", nil, "")}
	service.Register(check)
	service.RunAll(context.Background())

	waitFor(t, 2*time.Second, func() bool {
		for _, event := range service.Events(10) {
			if event.Trigger == TriggerAuto && event.Result == ResultRejected {
				return true
			}
		}
		return false
	})
	if check.executionCount() != 0 {
		t.Fatalf("escalated remedy must never execute, ran %d times", check.executionCount())
	}
}

// escalatingCarrierCheck classifies its remedy as transparent on the first
// Remedies call (sweep time) but confirm-class afterwards, simulating the
// plugin coming back alive while the execution waited on the serializer.
type escalatingCarrierCheck struct {
	fakeCarrierCheck
	remedyCalls int32
}

func (c *escalatingCarrierCheck) Remedies(finding Finding) []Remedy {
	risk := RiskTransparent
	if atomic.AddInt32(&c.remedyCalls, 1) > 1 {
		risk = RiskConfirm
	}
	return []Remedy{{
		ID:        "fix",
		TitleKey:  "doctor.remedy_run",
		Risk:      risk,
		Available: true,
	}}
}

func TestSweepDueRechecksProblemsBeforeNormalInterval(t *testing.T) {
	for _, previous := range []string{"finding", "check error"} {
		t.Run(previous, func(t *testing.T) {
			service, check := newTestService(t)
			now := time.Now()
			service.now = func() time.Time { return now }
			check.interval = 5 * time.Minute
			if previous == "finding" {
				check.findings = []Finding{finding(check.id, "a", SeverityWarning, "fake.one", nil, "")}
			} else {
				check.err = errors.New("temporary failure")
			}
			service.RunAll(context.Background())
			check.findings, check.err = nil, nil
			now = now.Add(schedulerTick)
			service.sweepDue(context.Background())
			if check.runs != 2 {
				t.Fatalf("problem must be rechecked on next tick, runs=%d", check.runs)
			}
			report := service.Report()
			if len(report.Findings) != 0 || report.Checks[0].LastError != "" {
				t.Fatalf("recovery must clear old findings and errors: %+v", report)
			}
			now = now.Add(schedulerTick)
			service.sweepDue(context.Background())
			if check.runs != 2 {
				t.Fatalf("healthy check must retain its normal interval, runs=%d", check.runs)
			}
		})
	}
}

func TestRunAllWaitsForBusySweep(t *testing.T) {
	service, check := newTestService(t)
	service.sweepMu.Lock()
	result := make(chan Report, 1)
	go func() {
		report, _ := service.RunAll(context.Background())
		result <- report
	}()
	select {
	case report := <-result:
		service.sweepMu.Unlock()
		t.Fatalf("manual check returned stale report while sweep busy: %+v", report)
	case <-time.After(20 * time.Millisecond):
	}
	service.sweepMu.Unlock()
	select {
	case report := <-result:
		if check.runs != 1 || report.Checks[0].LastRunAt.IsZero() {
			t.Fatalf("manual check must run after busy sweep: %+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("manual check did not finish")
	}
}
