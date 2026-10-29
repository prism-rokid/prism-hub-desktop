package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"rokid-prism-hub/internal/pluginruntime"
	"rokid-prism-hub/internal/runtimes"
)

const (
	rpcTimeout = 30 * time.Second
	// SupervisorMaxFailures caps consecutive start failures before the adapter
	// stops restarting the plugin process. Passive observers use it to report
	// a permanently down plugin.
	SupervisorMaxFailures    = 5
	supervisorBaseBackoff    = 100 * time.Millisecond
	supervisorMaxBackoff     = 5 * time.Second
	runtimeWriteQueueSize    = 64
	historyDeliveryQueueSize = 32
	sessionTokenEnv          = "PRISM_PLUGIN_SESSION_TOKEN"
	pluginIDEnv              = "PRISM_PLUGIN_ID"
	pluginRootEnv            = "PRISM_PLUGIN_ROOT"
	pluginTransportEnv       = "PRISM_PLUGIN_TRANSPORT"
	pluginProtocolVersionEnv = "PRISM_PLUGIN_PROTOCOL_VERSION"
	pluginProcessLabelEnv    = "PRISM_PLUGIN_PROCESS_LABEL"
)

type ProcessAdapter struct {
	Root      string
	Manifest  Manifest
	PluginEnv map[string]string
	// skipRuntimeLease marks short-lived verification processes: they never
	// claim the per-plugin runtime lease file (see Discovered.SkipRuntimeLease).
	skipRuntimeLease bool

	mu        sync.Mutex
	runtime   *processRuntime
	failures  int
	nextStart time.Time
	lastError string
}

type processRuntime struct {
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	sessionToken   string
	requestTimeout time.Duration
	pending        map[string]chan rpcResponse
	events         map[string]chan pluginruntime.PluginEvent
	history        map[string]*historyDelivery
	writes         chan *stdinWrite
	closed         chan struct{}
	processDone    chan struct{}
	lease          *runtimeLease
	mu             sync.Mutex
	closeOnce      sync.Once
	terminateOnce  sync.Once
	terminalMu     sync.Mutex
	terminalErr    error
	exitMu         sync.Mutex
	exited         bool
	exitErr        error
	onExit         func(error)
}

type stdinWrite struct {
	data      []byte
	done      chan error
	cancelled atomic.Bool
}

// historyDelivery isolates the plugin stdout reader from a slow remote relay.
// It preserves turn order, replaces queued revisions for the same turn, and
// turns unrecoverable queue pressure into an explicit retryable terminal event.
type historyDelivery struct {
	streamID string
	out      chan pluginruntime.HistoryStreamEvent
	wake     chan struct{}
	done     chan struct{}

	mu       sync.Mutex
	queued   []pluginruntime.HistoryStreamEvent
	closed   bool
	stopOnce sync.Once
}

type rpcRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type rpcResponse struct {
	ID      string          `json:"id"`
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type rpcEvent struct {
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewProcessAdapter(item Discovered) *ProcessAdapter {
	// Keep direct construction aligned with Hub-managed construction so every
	// Plugin receives its manifest-resolved default runtime mode.
	return NewProcessAdapterWithEnv(item, nil)
}

func NewProcessAdapterWithEnv(item Discovered, env map[string]string) *ProcessAdapter {
	// Configuration values are filtered, but PRISM_PLUGIN_MODE is a Hub-owned
	// runtime selector. Preserve only its manifest-resolved value separately
	// from arbitrary user-provided PRISM_PLUGIN_* environment variables.
	filtered := normalizeExplicitPluginEnvironment(env)
	if mode := item.Manifest.RuntimeMode(env["PRISM_PLUGIN_MODE"]); mode != "" {
		if filtered == nil {
			filtered = map[string]string{}
		}
		filtered["PRISM_PLUGIN_MODE"] = mode
	}
	return &ProcessAdapter{Root: item.Root, Manifest: item.Manifest, PluginEnv: filtered, skipRuntimeLease: item.SkipRuntimeLease}
}

func (a *ProcessAdapter) ID() string { return a.Manifest.ID }

func (a *ProcessAdapter) LocalMetadata() pluginruntime.LocalMetadata {
	if a == nil {
		return pluginruntime.LocalMetadata{}
	}
	return pluginruntime.LocalMetadata{PluginID: a.Manifest.ID,
		DisplayName:               a.Manifest.DisplayName,
		Description:               a.Manifest.Description,
		IconURL:                   a.Manifest.IconURL,
		IconSVGURL:                a.Manifest.IconSVGURL,
		Version:                   a.Manifest.Version,
		ManagedLaunch:             a.Manifest.ManagedLaunch,
		ManagedLaunchTargets:      managedLaunchTargets(a.Manifest),
		RuntimeIntegration:        a.Manifest.RuntimeIntegration,
		RuntimeModes:              append([]string(nil), a.Manifest.RuntimeModes...),
		CurrentRuntimeMode:        a.Manifest.RuntimeMode(a.PluginEnv["PRISM_PLUGIN_MODE"]),
		RuntimeControls:           a.Manifest.RuntimeControlProfile(a.PluginEnv["PRISM_PLUGIN_MODE"]),
		RuntimeControlsDeclared:   a.Manifest.DeclaresRuntimeControls(a.PluginEnv["PRISM_PLUGIN_MODE"]),
		RuntimeOperations:         a.Manifest.RuntimeOperationProfile(a.PluginEnv["PRISM_PLUGIN_MODE"]),
		RuntimeOperationsDeclared: a.Manifest.DeclaresRuntimeOperations(a.PluginEnv["PRISM_PLUGIN_MODE"]),
	}
}

func (a *ProcessAdapter) Probe(ctx context.Context) (pluginruntime.Capability, error) {
	capability, err := a.ProbeStrict(ctx)
	if err == nil {
		return capability, nil
	}
	fallback := a.Manifest.Capability()
	fallback.Available = false
	fallback.UnavailableReason = err.Error()
	return fallback, nil
}

// ProbeStrict requires a successful PluginBridge handshake and matching
// plugin identity. Release installation uses it before activation; normal Hub
// status probes retain the unavailable fallback above when an installed
// agent's native application is not currently running.
func (a *ProcessAdapter) ProbeStrict(ctx context.Context) (pluginruntime.Capability, error) {
	var capability pluginruntime.Capability
	if err := a.call(ctx, "adapter.probe", map[string]any{}, &capability); err != nil {
		return pluginruntime.Capability{}, err
	}
	if err := a.requirePluginID("adapter.probe", capability.PluginID); err != nil {
		return pluginruntime.Capability{}, err
	}
	return capability, nil
}

func (a *ProcessAdapter) Discover(ctx context.Context) (pluginruntime.DiscoveryResult, error) {
	var result pluginruntime.DiscoveryResult
	if err := a.call(ctx, "adapter.discover", map[string]any{}, &result); err != nil {
		return pluginruntime.DiscoveryResult{
			PluginID: a.Manifest.ID,
			Surface:  a.Manifest.Mode,
			Endpoint: firstNonEmpty(a.Manifest.Endpoint, a.Manifest.SocketPath),
			Verified: false,
			Detail:   err.Error(),
		}, nil
	}
	if err := a.requirePluginID("adapter.discover", result.PluginID); err != nil {
		return pluginruntime.DiscoveryResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) StartSessionWithMessage(ctx context.Context, req pluginruntime.StartSessionWithMessageRequest) (pluginruntime.StartSessionWithMessageResult, error) {
	var result pluginruntime.StartSessionWithMessageResult
	if err := a.call(ctx, "adapter.startSessionWithMessage", req, &result); err != nil {
		return pluginruntime.StartSessionWithMessageResult{}, err
	}
	if err := a.requirePluginID("adapter.startSessionWithMessage", result.Session.PluginID); err != nil {
		return pluginruntime.StartSessionWithMessageResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) OpenDraft(ctx context.Context, req pluginruntime.DraftOpenRequest) (pluginruntime.DraftOpenResult, error) {
	var result pluginruntime.DraftOpenResult
	if err := a.call(ctx, "adapter.openDraft", req, &result); err != nil {
		return result, err
	}
	if strings.TrimSpace(result.DraftID) != strings.TrimSpace(req.DraftID) {
		return result, fmt.Errorf("pluginbridge adapter.openDraft draft_id mismatch")
	}
	return result, nil
}

func (a *ProcessAdapter) ControlDraft(ctx context.Context, req pluginruntime.DraftControlRequest) (pluginruntime.DraftControlResult, error) {
	var result pluginruntime.DraftControlResult
	if err := a.call(ctx, "adapter.controlDraft", req, &result); err != nil {
		return result, err
	}
	if strings.TrimSpace(result.DraftID) != strings.TrimSpace(req.DraftID) {
		return result, fmt.Errorf("pluginbridge adapter.controlDraft draft_id mismatch")
	}
	return result, nil
}

func (a *ProcessAdapter) StartDraftWithMessage(ctx context.Context, req pluginruntime.StartDraftWithMessageRequest) (pluginruntime.StartSessionWithMessageResult, error) {
	var result pluginruntime.StartSessionWithMessageResult
	if err := a.call(ctx, "adapter.startDraftWithMessage", req, &result); err != nil {
		return result, err
	}
	if err := a.requirePluginID("adapter.startDraftWithMessage", result.Session.PluginID); err != nil {
		return pluginruntime.StartSessionWithMessageResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) ListSessions(ctx context.Context) ([]pluginruntime.NativeSessionHint, error) {
	var sessions []pluginruntime.NativeSessionHint
	if err := a.call(ctx, "adapter.listSessions", map[string]any{}, &sessions); err != nil {
		return nil, err
	}
	for i := range sessions {
		if err := a.requirePluginID("adapter.listSessions", sessions[i].PluginID); err != nil {
			return nil, err
		}
	}
	return sessions, nil
}

func (a *ProcessAdapter) AttachSession(ctx context.Context, req pluginruntime.AttachSessionRequest) (pluginruntime.NativeSession, error) {
	var session pluginruntime.NativeSession
	if err := a.call(ctx, "adapter.attachSession", req, &session); err != nil {
		return pluginruntime.NativeSession{}, err
	}
	if err := a.requirePluginID("adapter.attachSession", session.PluginID); err != nil {
		return pluginruntime.NativeSession{}, err
	}
	return session, nil
}

func (a *ProcessAdapter) requirePluginID(method, pluginID string) error {
	got := strings.ToLower(strings.TrimSpace(pluginID))
	want := strings.ToLower(strings.TrimSpace(a.ID()))
	if got == want && got != "" {
		return nil
	}
	return fmt.Errorf("pluginbridge %s plugin_id mismatch: manifest=%s response=%s", method, want, got)
}

func (a *ProcessAdapter) ReadHistory(ctx context.Context, session pluginruntime.NativeSession, limit int) ([]pluginruntime.HistoryMessage, error) {
	var messages []pluginruntime.HistoryMessage
	if err := a.call(ctx, "adapter.readHistory", map[string]any{"session": session, "limit": limit}, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// ReadHistoryStream establishes an event-backed body stream. Unlike the old
// readHistory RPC, no conversation body is buffered in a single response.
func (a *ProcessAdapter) ReadHistoryStream(ctx context.Context, session pluginruntime.NativeSession, req pluginruntime.HistoryStreamRequest) (<-chan pluginruntime.HistoryStreamEvent, error) {
	rt, err := a.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.StreamID) == "" {
		req.StreamID = uuid.NewString()
	}
	out := rt.registerHistory(req.StreamID)
	var ack struct {
		StreamID string `json:"stream_id"`
	}
	if err := rt.call(ctx, "adapter.readHistoryStream", map[string]any{"session": session, "request": req}, &ack); err != nil {
		rt.unregisterHistory(req.StreamID)
		return nil, err
	}
	if ack.StreamID != "" && ack.StreamID != req.StreamID {
		rt.unregisterHistory(req.StreamID)
		return nil, fmt.Errorf("pluginbridge readHistoryStream stream_id mismatch: requested=%s acknowledged=%s", req.StreamID, ack.StreamID)
	}
	go func() {
		<-ctx.Done()
		_ = rt.call(context.Background(), "adapter.cancelHistoryStream", map[string]any{"stream_id": req.StreamID}, nil)
		rt.unregisterHistory(req.StreamID)
	}()
	return out, nil
}

func (a *ProcessAdapter) ReadStatus(ctx context.Context, session pluginruntime.NativeSession, runID string) (pluginruntime.RuntimeStatus, error) {
	var status pluginruntime.RuntimeStatus
	if err := a.call(ctx, "adapter.readStatus", map[string]any{"session": session, "run_id": runID}, &status); err != nil {
		return pluginruntime.RuntimeStatus{}, err
	}
	return status, nil
}

func (a *ProcessAdapter) ReadDetail(ctx context.Context, session pluginruntime.NativeSession) (map[string]any, error) {
	var detail map[string]any
	if err := a.call(ctx, "adapter.readDetail", map[string]any{"session": session}, &detail); err != nil {
		return nil, err
	}
	return detail, nil
}

func (a *ProcessAdapter) ControlSession(ctx context.Context, req pluginruntime.SessionControlRequest) (pluginruntime.SessionControlResult, error) {
	var result pluginruntime.SessionControlResult
	if err := a.call(ctx, "adapter.controlSession", req, &result); err != nil {
		return pluginruntime.SessionControlResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) OpenManagedTerminal(ctx context.Context, req pluginruntime.ManagedTerminalRequest) (pluginruntime.ManagedTerminalResult, error) {
	var result pluginruntime.ManagedTerminalResult
	if err := a.call(ctx, "adapter.openManagedTerminal", req, &result); err != nil {
		return pluginruntime.ManagedTerminalResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) ResolveApproval(ctx context.Context, req pluginruntime.ApprovalResolutionRequest) error {
	return a.call(ctx, "adapter.resolveApproval", req, nil)
}

func (a *ProcessAdapter) Send(ctx context.Context, session pluginruntime.NativeSession, msg pluginruntime.InboundMessage) (pluginruntime.SendReceipt, error) {
	var receipt pluginruntime.SendReceipt
	params := map[string]any{"session": session, "message": msg}
	if err := a.call(ctx, "adapter.send", params, &receipt); err != nil {
		return pluginruntime.SendReceipt{}, err
	}
	return receipt, nil
}

func (a *ProcessAdapter) Subscribe(ctx context.Context, session pluginruntime.NativeSession) (<-chan pluginruntime.PluginEvent, error) {
	rt, err := a.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}
	subID := uuid.NewString()
	out := make(chan pluginruntime.PluginEvent, 32)
	rt.registerEvents(subID, out)
	params := map[string]any{"subscription_id": subID, "session": session}
	var ack map[string]any
	if err := rt.call(ctx, "adapter.subscribe", params, &ack); err != nil {
		rt.unregisterEvents(subID)
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = rt.call(context.Background(), "adapter.unsubscribe", map[string]any{"subscription_id": subID}, nil)
		rt.unregisterEvents(subID)
	}()
	return out, nil
}

func (a *ProcessAdapter) AckEvent(ctx context.Context, session pluginruntime.NativeSession, eventID string) error {
	return a.call(ctx, "adapter.ackEvent", map[string]any{"session": session, "event_id": eventID}, nil)
}

func (a *ProcessAdapter) SubscribePlugin(ctx context.Context) (<-chan pluginruntime.PluginEvent, error) {
	rt, err := a.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}
	subID := uuid.NewString()
	out := make(chan pluginruntime.PluginEvent, 32)
	rt.registerEvents(subID, out)
	var ack map[string]any
	if err := rt.call(ctx, "adapter.subscribePlugin", map[string]any{"subscription_id": subID}, &ack); err != nil {
		rt.unregisterEvents(subID)
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = rt.call(context.Background(), "adapter.unsubscribe", map[string]any{"subscription_id": subID}, nil)
		rt.unregisterEvents(subID)
	}()
	return out, nil
}

func (a *ProcessAdapter) ReadAgentUsage(ctx context.Context) (*pluginruntime.AgentUsage, error) {
	var usage pluginruntime.AgentUsage
	if err := a.call(ctx, "adapter.readAgentUsage", map[string]any{}, &usage); err != nil {
		return nil, err
	}
	return &usage, nil
}

func (a *ProcessAdapter) Interrupt(ctx context.Context, session pluginruntime.NativeSession, taskID string) error {
	return a.call(ctx, "adapter.interrupt", map[string]any{"session": session, "task_id": taskID}, nil)
}

func (a *ProcessAdapter) VerifyVisibility(ctx context.Context, session pluginruntime.NativeSession, marker string) (pluginruntime.VisibilityResult, error) {
	var result pluginruntime.VisibilityResult
	if err := a.call(ctx, "adapter.verifyVisibility", map[string]any{"session": session, "marker": marker}, &result); err != nil {
		return pluginruntime.VisibilityResult{}, err
	}
	return result, nil
}

func (a *ProcessAdapter) WaitForRun(ctx context.Context, session pluginruntime.NativeSession, runID string) (pluginruntime.PluginEvent, error) {
	rt, err := a.ensureRuntime(ctx)
	if err != nil {
		return pluginruntime.PluginEvent{}, err
	}
	var event pluginruntime.PluginEvent
	// A native run may legitimately take minutes. The Router already supplies
	// the 15-minute lifecycle deadline, so do not apply the short per-RPC timeout
	// used by probe/send/control on top of it.
	if err := rt.callLong(ctx, "adapter.waitForRun", map[string]any{"session": session, "run_id": runID}, &event); err != nil {
		return pluginruntime.PluginEvent{}, err
	}
	return event, nil
}

func (a *ProcessAdapter) Close(ctx context.Context) error {
	a.mu.Lock()
	rt := a.runtime
	a.runtime = nil
	a.mu.Unlock()
	if rt == nil {
		return nil
	}
	_ = rt.call(ctx, "adapter.close", map[string]any{}, nil)
	return rt.close(ctx)
}

func (a *ProcessAdapter) call(ctx context.Context, method string, params any, out any) error {
	rt, err := a.ensureRuntime(ctx)
	if err != nil {
		return err
	}
	return rt.call(ctx, method, params, out)
}

func (a *ProcessAdapter) ensureRuntime(ctx context.Context) (*processRuntime, error) {
	a.mu.Lock()
	if a.runtime != nil {
		rt := a.runtime
		if !rt.isClosed() {
			a.mu.Unlock()
			return rt, nil
		}
		a.runtime = nil
		a.noteRuntimeFailureLocked(rt.terminalError(fmt.Errorf("pluginbridge plugin runtime closed")))
	}
	if a.failures >= SupervisorMaxFailures {
		err := fmt.Errorf("pluginbridge plugin %s failed after %d restart attempts: %s", a.Manifest.ID, a.failures, a.lastError)
		a.mu.Unlock()
		return nil, err
	}
	if !a.nextStart.IsZero() && time.Now().Before(a.nextStart) {
		wait := time.Until(a.nextStart).Round(time.Millisecond)
		err := fmt.Errorf("pluginbridge plugin %s restart backoff active (%s): %s", a.Manifest.ID, wait, a.lastError)
		a.mu.Unlock()
		return nil, err
	}
	a.mu.Unlock()
	rt, err := startProcessRuntime(ctx, a.Root, a.Manifest, a.PluginEnv, a.skipRuntimeLease)
	if err != nil {
		a.noteRuntimeFailure(err)
		return nil, err
	}
	a.mu.Lock()
	if a.runtime != nil {
		existing := a.runtime
		a.mu.Unlock()
		_ = rt.close(context.Background())
		return existing, nil
	}
	a.runtime = rt
	a.failures = 0
	a.nextStart = time.Time{}
	a.lastError = ""
	a.mu.Unlock()
	rt.setOnExit(func(err error) {
		a.handleRuntimeExit(rt, err)
	})
	return rt, nil
}

func (a *ProcessAdapter) handleRuntimeExit(rt *processRuntime, err error) {
	if err == nil {
		err = fmt.Errorf("pluginbridge plugin exited")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.runtime == rt {
		a.runtime = nil
		a.noteRuntimeFailureLocked(err)
	}
}

// RuntimeHealth reports supervisor bookkeeping without touching the plugin
// process. It backs the Hub's passive health checks.
func (a *ProcessAdapter) RuntimeHealth() pluginruntime.RuntimeHealth {
	if a == nil {
		return pluginruntime.RuntimeHealth{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	health := pluginruntime.RuntimeHealth{
		Failures:     a.failures,
		BackoffUntil: a.nextStart,
		LastError:    a.lastError,
	}
	if a.runtime != nil && !a.runtime.isClosed() {
		health.Running = true
	}
	return health
}

func (a *ProcessAdapter) noteRuntimeFailure(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.noteRuntimeFailureLocked(err)
}

func (a *ProcessAdapter) noteRuntimeFailureLocked(err error) {
	a.failures++
	if err != nil {
		a.lastError = err.Error()
	}
	backoff := supervisorBaseBackoff
	for i := 1; i < a.failures; i++ {
		backoff *= 2
		if backoff >= supervisorMaxBackoff {
			backoff = supervisorMaxBackoff
			break
		}
	}
	a.nextStart = time.Now().Add(backoff)
}

func startProcessRuntime(ctx context.Context, root string, manifest Manifest, extraEnv map[string]string, skipRuntimeLease bool) (*processRuntime, error) {
	command := trimEmpty(manifest.Command)
	if len(command) == 0 {
		return nil, fmt.Errorf("pluginbridge plugin %s command required", manifest.ID)
	}
	if required := manifest.RuntimeDependencies.Node; required != "" {
		if !runtimes.RequiresNode(required) {
			return nil, fmt.Errorf("pluginbridge plugin %s has unsupported node runtime range %q", manifest.ID, required)
		}
		node, err := runtimes.NewNodeManager("", nil).Ensure(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolve plugin node runtime: %w", err)
		}
		command, err = runtimes.ExpandCommand(command, node)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		command, err = runtimes.ExpandCommand(command, "")
		if err != nil {
			return nil, fmt.Errorf("pluginbridge plugin %s: %w", manifest.ID, err)
		}
	}
	name := command[0]
	if !filepath.IsAbs(name) && strings.Contains(name, string(filepath.Separator)) {
		name = filepath.Join(root, name)
	}
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".exe"
	}
	// Keep the plugin process alive independently of any single RPC context.
	// A request-scoped context may time out or be canceled after a successful
	// call, but the plugin runtime must stay up so later requests can reuse the
	// same process/session.
	sessionToken := uuid.NewString()
	var lease *runtimeLease
	if !skipRuntimeLease {
		var err error
		lease, err = prepareRuntimeLease(manifest.ID, sessionToken)
		if err != nil {
			return nil, fmt.Errorf("prepare plugin runtime lease %s: %w", manifest.ID, err)
		}
	}
	cmd := exec.Command(name, command[1:]...)
	configurePluginProcessAttributes(cmd)
	// Keep the executable path intact while giving Hub-owned plugin processes a
	// recognizable argv[0] in host process viewers. The mode is presentation
	// only: runtime leases remain keyed by plugin ID so a mode switch replaces,
	// rather than duplicates, the existing plugin runtime.
	processLabel := pluginRuntimeProcessLabel(manifest, manifest.RuntimeMode(extraEnv["PRISM_PLUGIN_MODE"]))
	name = runtimeLabelledExecutable(name, processLabel)
	cmd.Args[0] = processLabel
	cmd.Dir = root
	configurePluginProcessGroup(cmd)
	cmd.Env = upsertEnv(
		pluginEnvironment(root, manifest, sessionToken, extraEnv),
		pluginProcessLabelEnv,
		processLabel,
	)
	// Plugin stdout is reserved for the JSON-RPC protocol. Preserve stderr so
	// adapter failures are visible in the Hub service log instead of vanishing.
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pluginbridge plugin stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pluginbridge plugin stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		lease.release()
		return nil, fmt.Errorf("pluginbridge plugin start %s: %w", manifest.ID, err)
	}
	if lease != nil {
		if err := lease.claim(cmd.Process.Pid); err != nil {
			_ = terminatePluginProcess(cmd)
			_ = cmd.Wait()
			lease.release()
			return nil, fmt.Errorf("claim plugin runtime lease %s: %w", manifest.ID, err)
		}
	}
	rt := newProcessRuntime(cmd, stdin, sessionToken)
	rt.lease = lease
	go rt.writeLoop()
	go rt.readLoop(stdout)
	go rt.waitLoop()
	return rt, nil
}

func pluginProcessLabel(pluginID string) string {
	var builder strings.Builder
	builder.WriteString("prism-plugin-")
	for _, character := range strings.ToLower(strings.TrimSpace(pluginID)) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' {
			builder.WriteRune(character)
			continue
		}
		builder.WriteByte('-')
	}
	return strings.TrimRight(builder.String(), "-")
}

// pluginRuntimeProcessLabel identifies the selected managed runtime in process
// viewers. It must not be used for lease files: one plugin ID owns one process
// group regardless of its selected runtime mode.
func pluginRuntimeProcessLabel(manifest Manifest, runtimeMode string) string {
	label := pluginProcessLabel(manifest.ID)
	runtimeMode = sanitizePluginProcessLabelPart(runtimeMode)
	if runtimeMode == "" {
		return label
	}
	return label + "-" + runtimeMode
}

func sanitizePluginProcessLabelPart(value string) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(strings.TrimSpace(value)) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' {
			builder.WriteRune(character)
			continue
		}
		builder.WriteByte('-')
	}
	return strings.Trim(builder.String(), "-")
}

// runtimeLabelledExecutable gives macOS process viewers a real executable name
// that includes the selected runtime. Changing argv[0] alone only changes ps;
// Activity Monitor reads the executable name. Plugin versions have separate
// directories, so the sibling hard link naturally follows a plugin update.
func runtimeLabelledExecutable(name, processLabel string) string {
	if runtime.GOOS != "darwin" || strings.TrimSpace(processLabel) == "" || filepath.Base(name) == processLabel {
		return name
	}
	source, err := os.Stat(name)
	if err != nil || !source.Mode().IsRegular() {
		return name
	}
	labelled := filepath.Join(filepath.Dir(name), processLabel)
	if existing, err := os.Stat(labelled); err == nil {
		if os.SameFile(source, existing) {
			return labelled
		}
		return name
	}
	if err := os.Link(name, labelled); err != nil {
		return name
	}
	return labelled
}

func newProcessRuntime(cmd *exec.Cmd, stdin io.WriteCloser, sessionToken string) *processRuntime {
	return &processRuntime{
		cmd:            cmd,
		stdin:          stdin,
		sessionToken:   sessionToken,
		requestTimeout: rpcTimeout,
		pending:        map[string]chan rpcResponse{},
		events:         map[string]chan pluginruntime.PluginEvent{},
		history:        map[string]*historyDelivery{},
		writes:         make(chan *stdinWrite, runtimeWriteQueueSize),
		closed:         make(chan struct{}),
		processDone:    make(chan struct{}),
	}
}

func (rt *processRuntime) setOnExit(onExit func(error)) {
	if rt == nil || onExit == nil {
		return
	}
	rt.exitMu.Lock()
	rt.onExit = onExit
	exited := rt.exited
	err := rt.exitErr
	rt.exitMu.Unlock()
	if exited {
		onExit(err)
	}
}

func (rt *processRuntime) waitLoop() {
	err := rt.cmd.Wait()
	if rt.lease != nil {
		rt.lease.release()
	}
	if err == nil {
		err = fmt.Errorf("pluginbridge plugin exited")
	}
	rt.closePending(err)
	rt.markExited(rt.terminalError(err))
}

func (rt *processRuntime) terminalError(fallback error) error {
	rt.terminalMu.Lock()
	defer rt.terminalMu.Unlock()
	if rt.terminalErr != nil {
		return rt.terminalErr
	}
	return fallback
}

func (rt *processRuntime) markExited(err error) {
	rt.exitMu.Lock()
	if rt.exited {
		rt.exitMu.Unlock()
		return
	}
	rt.exited = true
	rt.exitErr = err
	onExit := rt.onExit
	close(rt.processDone)
	rt.exitMu.Unlock()
	if onExit != nil {
		onExit(err)
	}
}

func pluginEnvironment(root string, manifest Manifest, sessionToken string, extraEnv ...map[string]string) []string {
	allowed := map[string]bool{
		"PATH":                           true,
		"HOME":                           true,
		"USER":                           true,
		"USERNAME":                       true,
		"SHELL":                          true,
		"TMPDIR":                         true,
		"TEMP":                           true,
		"TMP":                            true,
		"LANG":                           true,
		"LC_ALL":                         true,
		"LC_CTYPE":                       true,
		"GOCACHE":                        true,
		"GOMODCACHE":                     true,
		"GOPATH":                         true,
		"GOROOT":                         true,
		"GOENV":                          true,
		"OPENCLAW_GATEWAY_URL":           true,
		"OPENCLAW_GATEWAY_TOKEN":         true,
		"OPENCLAW_GATEWAY_PORT":          true,
		"OPENCLAW_HOME":                  true,
		"HERMES_GATEWAY_ENDPOINT":        true,
		"HERMES_GATEWAY_HOST":            true,
		"HERMES_GATEWAY_PORT":            true,
		"HERMES_GATEWAY_STARTUP_TIMEOUT": true,
		"HERMES_GATEWAY_SKIP_BUILD":      true,
		"HERMES_DASHBOARD_SESSION_TOKEN": true,
		"HERMES_BIN":                     true,
		"HERMES_HOME":                    true,
		"HERMES_GATEWAY_DEBUG":           true,
		"CODEX_APP_SERVER_ENDPOINT":      true,
		"CODEX_APP_SERVER_SOCK":          true,
		"CODEX_BIN":                      true,
		"PRISM_SQLITE3_BIN":              true,
		"PRISM_API_URL":                  true,
		"PRISM_REALTIME_URL":             true,
		"SystemRoot":                     true,
		"WINDIR":                         true,
		"ComSpec":                        true,
		"PATHEXT":                        true,
		"PROCESSOR_ARCHITECTURE":         true,
		// Windows counterparts of HOME. Plugins resolve their own per-user
		// state/profile directories from these (for example opencode reads
		// %USERPROFILE%\.local\state\opencode\service.json), and dropping
		// them makes discovery silently fall back to a default endpoint.
		"USERPROFILE":                    true,
		"HOMEDRIVE":                      true,
		"HOMEPATH":                       true,
		"APPDATA":                        true,
		"LOCALAPPDATA":                   true,
	}
	env := make([]string, 0, len(allowed)+6)
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if !ok || !isPluginEnvironmentAllowed(key, allowed) {
			continue
		}
		env = append(env, item)
	}
	for _, explicit := range extraEnv {
		normalized := normalizeExplicitPluginEnvironment(explicit)
		for key, value := range normalized {
			env = upsertEnv(env, key, value)
		}
		// Runtime mode is the only PRISM_PLUGIN_* value a ProcessAdapter may
		// receive from Hub. The constructor above derives it from the manifest,
		// so an untrusted caller cannot inject another reserved key here.
		if mode := strings.TrimSpace(explicit["PRISM_PLUGIN_MODE"]); mode != "" {
			env = upsertEnv(env, "PRISM_PLUGIN_MODE", mode)
		}
	}
	for key, value := range managedLaunchEnvironment(manifest) {
		env = upsertEnv(env, key, value)
	}
	env = append(env,
		sessionTokenEnv+"="+sessionToken,
		pluginIDEnv+"="+manifest.ID,
		pluginRootEnv+"="+root,
		pluginTransportEnv+"="+manifest.Transport,
		pluginProtocolVersionEnv+"=4",
	)
	return env
}

// managedLaunchTargets projects the manifest's launch specs for the catalog.
// Kind/ready/mode filters only — start/stop argv stay hub-side execution
// details and never cross the wire to remote clients.
func managedLaunchTargets(manifest Manifest) []pluginruntime.ManagedLaunchTarget {
	if len(manifest.ManagedLaunchSpecs) == 0 {
		return nil
	}
	out := make([]pluginruntime.ManagedLaunchTarget, 0, len(manifest.ManagedLaunchSpecs))
	for _, spec := range manifest.ManagedLaunchSpecs {
		out = append(out, pluginruntime.ManagedLaunchTarget{
			Kind:         spec.Kind,
			Ready:        spec.Ready,
			RuntimeModes: append([]string(nil), spec.RuntimeModes...),
		})
	}
	return out
}

func managedLaunchEnvironment(manifest Manifest) map[string]string {
	if !manifest.ManagedLaunch {
		return nil
	}
	// This is a lifecycle fact about the Plugin process, not a product-specific
	// launch instruction. Plugins may opt into their own recovery behavior from
	// this standard contract without Hub recognizing a plugin ID.
	return map[string]string{"PRISM_PLUGIN_MANAGED_LAUNCH": "1"}
}

func normalizeExplicitPluginEnvironment(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if !isExplicitPluginEnvironmentAllowed(key) {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isExplicitPluginEnvironmentAllowed(key string) bool {
	key = strings.TrimSpace(key)
	if !validPluginEnvironmentKey(key) {
		return false
	}
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, "AGENTBRIDGE_") || strings.HasPrefix(upper, "PRISM_PLUGIN_") {
		return false
	}
	switch upper {
	case "PRISM_HOST_TOKEN", "PRISM_CONFIG", "PRISM_DB", "PRISM_ACCOUNT_TOKEN", "PRISM_ACCESS_TOKEN":
		return false
	default:
		return true
	}
}

func validPluginEnvironmentKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	first := key[0]
	return first == '_' || first >= 'A' && first <= 'Z' || first >= 'a' && first <= 'z'
}

func upsertEnv(env []string, key, value string) []string {
	prefix := key + "="
	item := prefix + value
	for i, existing := range env {
		if strings.HasPrefix(existing, prefix) {
			env[i] = item
			return env
		}
	}
	return append(env, item)
}

func isPluginEnvironmentAllowed(key string, exact map[string]bool) bool {
	if exact[key] {
		return true
	}
	// Windows reports environment names with their original casing (commonly
	// "Path" and "ComSpec" rather than "PATH"/"COMSPEC"), so the allow-list has
	// to match case-insensitively there or PATH/SystemRoot are dropped from
	// Plugin processes and shell-based adapters fail with ENOENT. Unix
	// environment names are case-sensitive, so exact matching stays
	// authoritative on every other platform.
	lookup := key
	if runtime.GOOS == "windows" {
		lookup = strings.ToUpper(key)
		for allowedKey := range exact {
			if strings.ToUpper(allowedKey) == lookup {
				return true
			}
		}
	}
	for _, prefix := range []string{
		"PRISM_CODEX_",
		"PRISM_HERMES_",
		"PRISM_CLAUDE_",
	} {
		if strings.HasPrefix(lookup, prefix) {
			return true
		}
	}
	return false
}

func (rt *processRuntime) call(ctx context.Context, method string, params any, out any) error {
	timeout := rt.requestTimeout
	if timeout <= 0 {
		timeout = rpcTimeout
	}
	return rt.callWithTimeout(ctx, timeout, method, params, out)
}

func (rt *processRuntime) callLong(ctx context.Context, method string, params any, out any) error {
	return rt.callWithTimeout(ctx, 0, method, params, out)
}

func (rt *processRuntime) callWithTimeout(ctx context.Context, timeout time.Duration, method string, params any, out any) error {
	if rt == nil {
		return fmt.Errorf("pluginbridge plugin runtime unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	id := uuid.NewString()
	ch := make(chan rpcResponse, 1)
	rt.mu.Lock()
	rt.pending[id] = ch
	rt.mu.Unlock()
	req := rpcRequest{ID: id, Method: method, Params: toSnakeWire(params)}
	data, err := json.Marshal(req)
	if err != nil {
		rt.forget(id)
		return err
	}
	write := &stdinWrite{
		data: append(data, '\n'),
		done: make(chan error, 1),
	}
	select {
	case rt.writes <- write:
	case <-ctx.Done():
		rt.forget(id)
		return ctx.Err()
	case <-rt.closed:
		rt.forget(id)
		return fmt.Errorf("pluginbridge plugin runtime closed")
	}
	select {
	case err := <-write.done:
		if err != nil {
			rt.forget(id)
			return fmt.Errorf("pluginbridge plugin write %s: %w", method, err)
		}
	case <-ctx.Done():
		write.cancelled.Store(true)
		rt.forget(id)
		return ctx.Err()
	case <-rt.closed:
		rt.forget(id)
		return fmt.Errorf("pluginbridge plugin runtime closed")
	}
	select {
	case res := <-ch:
		if !res.OK {
			return responseError(method, res.Error)
		}
		if out != nil && len(res.Payload) > 0 {
			if err := unmarshalWire(res.Payload, out); err != nil {
				return fmt.Errorf("pluginbridge plugin decode %s: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		rt.forget(id)
		return ctx.Err()
	case <-rt.closed:
		rt.forget(id)
		return fmt.Errorf("pluginbridge plugin runtime closed")
	}
}

func (rt *processRuntime) writeLoop() {
	for {
		select {
		case <-rt.closed:
			return
		case write := <-rt.writes:
			if write == nil || write.cancelled.Load() {
				continue
			}
			if rt.isClosed() {
				write.done <- fmt.Errorf("pluginbridge plugin runtime closed")
				continue
			}
			n, err := rt.stdin.Write(write.data)
			if err == nil && n != len(write.data) {
				err = io.ErrShortWrite
			}
			if err != nil {
				write.done <- err
				rt.terminate(fmt.Errorf("pluginbridge plugin stdin write: %w", err))
				return
			}
			write.done <- nil
		}
	}
}

func (rt *processRuntime) isClosed() bool {
	if rt == nil {
		return true
	}
	select {
	case <-rt.closed:
		return true
	default:
		return false
	}
}

func (rt *processRuntime) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var head struct {
			ID    string `json:"id,omitempty"`
			Event string `json:"event,omitempty"`
		}
		if err := json.Unmarshal(line, &head); err != nil {
			rt.terminate(fmt.Errorf("pluginbridge plugin stdout protocol: %w", err))
			return
		}
		if head.ID != "" {
			var res rpcResponse
			if err := json.Unmarshal(line, &res); err != nil {
				rt.terminate(fmt.Errorf("pluginbridge plugin stdout response: %w", err))
				return
			}
			rt.deliverResponse(res)
			continue
		}
		if head.Event != "" {
			var event rpcEvent
			if err := json.Unmarshal(line, &event); err != nil {
				rt.terminate(fmt.Errorf("pluginbridge plugin stdout event: %w", err))
				return
			}
			rt.deliverEvent(event)
			continue
		}
		rt.terminate(fmt.Errorf("pluginbridge plugin stdout protocol: missing id or event"))
		return
	}
	if err := scanner.Err(); err != nil {
		rt.terminate(fmt.Errorf("pluginbridge plugin stdout read: %w", err))
		return
	}
	rt.terminate(fmt.Errorf("pluginbridge plugin stdout closed"))
}

func newHistoryDelivery(streamID string) *historyDelivery {
	delivery := &historyDelivery{
		streamID: streamID,
		out:      make(chan pluginruntime.HistoryStreamEvent),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go delivery.run()
	return delivery
}

func (d *historyDelivery) enqueue(event pluginruntime.HistoryStreamEvent) bool {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return false
	}
	if event.Type == "turn" && event.Turn != nil && event.Turn.TurnID != "" {
		for i := len(d.queued) - 1; i >= 0; i-- {
			queued := d.queued[i]
			if queued.Type != "turn" || queued.Turn == nil || queued.Turn.TurnID != event.Turn.TurnID {
				continue
			}
			if queued.Turn.Revision < event.Turn.Revision {
				d.queued[i] = event
			}
			d.mu.Unlock()
			d.signal()
			return false
		}
	}
	if len(d.queued) >= historyDeliveryQueueSize {
		d.queued = []pluginruntime.HistoryStreamEvent{{
			StreamID:  d.streamID,
			Type:      "error",
			Source:    "relay",
			Operation: "replace",
			Error:     "history stream backpressure; reconnect required",
			Retryable: true,
		}}
		d.closed = true
		d.mu.Unlock()
		d.signal()
		return true
	}
	d.queued = append(d.queued, event)
	if event.Type == "end" || event.Type == "error" {
		d.closed = true
	}
	d.mu.Unlock()
	d.signal()
	return false
}

func (d *historyDelivery) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *historyDelivery) stop() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() {
		close(d.done)
	})
}

func (d *historyDelivery) run() {
	defer close(d.out)
	for {
		d.mu.Lock()
		if len(d.queued) == 0 {
			closed := d.closed
			d.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-d.wake:
			case <-d.done:
				return
			}
			continue
		}
		event := d.queued[0]
		d.queued = d.queued[1:]
		d.mu.Unlock()
		select {
		case d.out <- event:
		case <-d.done:
			return
		}
	}
}

func (rt *processRuntime) registerEvents(id string, ch chan pluginruntime.PluginEvent) {
	rt.mu.Lock()
	rt.events[id] = ch
	rt.mu.Unlock()
}

func (rt *processRuntime) registerHistory(id string) <-chan pluginruntime.HistoryStreamEvent {
	delivery := newHistoryDelivery(id)
	rt.mu.Lock()
	rt.history[id] = delivery
	rt.mu.Unlock()
	return delivery.out
}

func (rt *processRuntime) unregisterEvents(id string) {
	rt.mu.Lock()
	ch := rt.events[id]
	delete(rt.events, id)
	if ch != nil {
		close(ch)
	}
	rt.mu.Unlock()
}

func (rt *processRuntime) unregisterHistory(id string) {
	rt.mu.Lock()
	delivery := rt.history[id]
	delete(rt.history, id)
	rt.mu.Unlock()
	if delivery != nil {
		delivery.stop()
	}
}

func (rt *processRuntime) detachHistory(id string, delivery *historyDelivery) {
	rt.mu.Lock()
	if rt.history[id] == delivery {
		delete(rt.history, id)
	}
	rt.mu.Unlock()
}

func (rt *processRuntime) deliverResponse(res rpcResponse) {
	rt.mu.Lock()
	ch := rt.pending[res.ID]
	delete(rt.pending, res.ID)
	rt.mu.Unlock()
	if ch != nil {
		ch <- res
	}
}

func (rt *processRuntime) deliverEvent(event rpcEvent) {
	if event.Event == "history.stream" {
		rt.deliverHistoryEvent(event.Payload)
		return
	}
	if event.Event != "plugin.event" {
		return
	}
	var envelope struct {
		SubscriptionID string          `json:"subscription_id"`
		Event          json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(event.Payload, &envelope); err != nil {
		return
	}
	var pluginEvent pluginruntime.PluginEvent
	if err := unmarshalWire(envelope.Event, &pluginEvent); err != nil {
		return
	}
	if pluginEvent.CreatedAt.IsZero() {
		pluginEvent.CreatedAt = time.Now().UTC()
	}
	rt.mu.Lock()
	ch := rt.events[envelope.SubscriptionID]
	if ch == nil {
		rt.mu.Unlock()
		return
	}
	select {
	case ch <- pluginEvent:
	default:
	}
	rt.mu.Unlock()
}

func (rt *processRuntime) deliverHistoryEvent(payload json.RawMessage) {
	var event pluginruntime.HistoryStreamEvent
	if err := unmarshalWire(payload, &event); err != nil || strings.TrimSpace(event.StreamID) == "" {
		return
	}
	rt.mu.Lock()
	delivery := rt.history[event.StreamID]
	if delivery == nil {
		rt.mu.Unlock()
		return
	}
	rt.mu.Unlock()
	overflow := delivery.enqueue(event)
	if overflow {
		rt.detachHistory(event.StreamID, delivery)
		go func(streamID string) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			_ = rt.call(ctx, "adapter.cancelHistoryStream", map[string]any{"stream_id": streamID}, nil)
		}(event.StreamID)
		return
	}
	if event.Type == "end" || event.Type == "error" {
		rt.detachHistory(event.StreamID, delivery)
	}
}

func (rt *processRuntime) forget(id string) {
	rt.mu.Lock()
	delete(rt.pending, id)
	rt.mu.Unlock()
}

func (rt *processRuntime) closePending(err error) {
	rt.closeOnce.Do(func() {
		rt.terminalMu.Lock()
		rt.terminalErr = err
		rt.terminalMu.Unlock()
		close(rt.closed)
		rt.mu.Lock()
		for id, ch := range rt.pending {
			delete(rt.pending, id)
			ch <- rpcResponse{ID: id, OK: false, Error: &rpcError{Message: err.Error()}}
		}
		for id, ch := range rt.events {
			delete(rt.events, id)
			close(ch)
		}
		for id, delivery := range rt.history {
			delete(rt.history, id)
			delivery.stop()
		}
		rt.mu.Unlock()
	})
}

func (rt *processRuntime) terminate(err error) {
	if rt == nil {
		return
	}
	rt.closePending(err)
	rt.terminateOnce.Do(func() {
		if rt.stdin != nil {
			_ = rt.stdin.Close()
		}
		_ = terminatePluginProcess(rt.cmd)
	})
}

func (rt *processRuntime) close(ctx context.Context) error {
	if rt == nil {
		return nil
	}
	rt.terminate(fmt.Errorf("pluginbridge plugin runtime closed"))
	if rt.cmd == nil {
		return nil
	}
	select {
	case <-rt.processDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func responseError(method string, errShape *rpcError) error {
	if errShape == nil {
		return fmt.Errorf("pluginbridge plugin %s failed", method)
	}
	msg := strings.TrimSpace(errShape.Message)
	if msg == "" {
		msg = "request failed"
	}
	if errShape.Code != "" {
		return fmt.Errorf("pluginbridge plugin %s failed [%s]: %w", method, errShape.Code, &pluginruntime.AdapterError{
			Code: strings.TrimSpace(errShape.Code), Message: msg,
		})
	}
	return fmt.Errorf("pluginbridge plugin %s failed: %s", method, msg)
}

func trimEmpty(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
