// Package pluginruntime defines the Prism-side PluginBridge runtime contract.
package pluginruntime

import (
	"context"
	"time"
)

type PluginAdapter interface {
	ID() string
	Probe(ctx context.Context) (Capability, error)
	Discover(ctx context.Context) (DiscoveryResult, error)
	Send(ctx context.Context, session NativeSession, msg InboundMessage) (SendReceipt, error)
	Subscribe(ctx context.Context, session NativeSession) (<-chan PluginEvent, error)
	Interrupt(ctx context.Context, session NativeSession, taskID string) error
	VerifyVisibility(ctx context.Context, session NativeSession, marker string) (VisibilityResult, error)
	Close(ctx context.Context) error
}

// PluginWideSubscriber observes one Plugin-wide native event source. It is
// optional because gateway plugins may expose true per-session subscriptions.
// Desktop automation plugins with one foreground watcher use this instead of
// creating an identical subscription for every native session.
type PluginWideSubscriber interface {
	SubscribePlugin(ctx context.Context) (<-chan PluginEvent, error)
}

// AgentUsage is the smallest account-level usage projection a plugin may
// expose for display on its Agent card. The runtime never persists it.
type AgentUsage struct {
	FiveHour       *AgentUsageFiveHour  `json:"five_hour,omitempty"`
	Weekly         *AgentUsageWeekly    `json:"weekly,omitempty"`
	AccountSummary *AccountUsageSummary `json:"account_summary,omitempty"`
	UpdatedAt      int64                `json:"updated_at,omitempty"`
}

type AgentUsageFiveHour struct {
	UsedPercent   int64 `json:"used_percent"`
	WindowMinutes int64 `json:"window_minutes"`
	ResetsAt      int64 `json:"resets_at"`
}

type AgentUsageWeekly struct {
	UsedPercent   int64 `json:"used_percent"`
	WindowMinutes int64 `json:"window_minutes"`
	ResetsAt      int64 `json:"resets_at"`
}

type AccountUsageSummary struct {
	LifetimeTokens    int64 `json:"lifetime_tokens"`
	PeakDailyTokens   int64 `json:"peak_daily_tokens"`
	CurrentStreakDays int64 `json:"current_streak_days"`
	LongestStreakDays int64 `json:"longest_streak_days"`
}

// AgentUsageReader is optional. A nil result means the usage source is not
// currently available and must not affect conversation capabilities.
type AgentUsageReader interface {
	ReadAgentUsage(ctx context.Context) (*AgentUsage, error)
}

type RunWaiter interface {
	WaitForRun(ctx context.Context, session NativeSession, runID string) (PluginEvent, error)
}

type SessionLister interface {
	ListSessions(ctx context.Context) ([]NativeSessionHint, error)
}

type SessionAttacher interface {
	AttachSession(ctx context.Context, req AttachSessionRequest) (NativeSession, error)
}

// SessionStarter creates a native conversation by delivering its first message.
// No canonical conversation ID exists until the returned native identity is
// durably bound by the Hub.
type SessionStarter interface {
	StartSessionWithMessage(ctx context.Context, req StartSessionWithMessageRequest) (StartSessionWithMessageResult, error)
}

// DraftOpener opens the native product's real anonymous new-conversation UI.
// DraftID is Hub-scoped and must never be promoted to a conversation identity.
type DraftOpener interface {
	OpenDraft(ctx context.Context, req DraftOpenRequest) (DraftOpenResult, error)
}

type DraftController interface {
	ControlDraft(ctx context.Context, req DraftControlRequest) (DraftControlResult, error)
}

type DraftStarter interface {
	StartDraftWithMessage(ctx context.Context, req StartDraftWithMessageRequest) (StartSessionWithMessageResult, error)
}

// HistoryStreamRequest describes the fixed latest-turn body window.
type HistoryStreamRequest struct {
	StreamID string `json:"stream_id"`
	Limit    int    `json:"limit"`
	Live     bool   `json:"live,omitempty"`
}

// HistoryTurn is the atomic body unit. A turn contains one user input and its
// corresponding assistant output, including an incomplete assistant message
// while Codex is still running.
type HistoryTurn struct {
	TurnID   string           `json:"turn_id"`
	OrderKey string           `json:"order_key"`
	Revision int64            `json:"revision"`
	Messages []HistoryMessage `json:"messages"`
}

// HistoryStreamEvent is emitted asynchronously after readHistoryStream has
// been acknowledged. Type is either "turn", "end", or "error".
type HistoryStreamEvent struct {
	StreamID  string       `json:"stream_id"`
	Type      string       `json:"type"`
	Source    string       `json:"source,omitempty"`
	Operation string       `json:"operation,omitempty"`
	Turn      *HistoryTurn `json:"turn,omitempty"`
	Error     string       `json:"error,omitempty"`
	Retryable bool         `json:"retryable,omitempty"`
}

// HistoryStreamer is the only body synchronization contract. Its events are
// intentionally separate from desktop detail/watch events so a model change
// never looks like a body refresh.
type HistoryStreamer interface {
	ReadHistoryStream(ctx context.Context, session NativeSession, req HistoryStreamRequest) (<-chan HistoryStreamEvent, error)
}

// HistoryReader remains only for the pre-stream query route while API clients
// migrate. New remote body paths must use HistoryStreamer.
type HistoryReader interface {
	ReadHistory(ctx context.Context, session NativeSession, limit int) ([]HistoryMessage, error)
}

type StatusReader interface {
	ReadStatus(ctx context.Context, session NativeSession, runID string) (RuntimeStatus, error)
}

// DetailReader returns a non-interactive detail projection for one native
// conversation. Implementations must not select, resume, or take ownership of
// the Desktop conversation merely because a remote client opened its detail.
type DetailReader interface {
	ReadDetail(ctx context.Context, session NativeSession) (map[string]any, error)
}

type SessionController interface {
	ControlSession(ctx context.Context, req SessionControlRequest) (SessionControlResult, error)
}

// EventAcker acknowledges a durable plugin event after Hub has completed its
// local persistence and transport handoff. Implementations must be idempotent.
type EventAcker interface {
	AckEvent(ctx context.Context, session NativeSession, eventID string) error
}

// ManagedTerminalLauncher opens a local terminal backed by the Plugin's
// existing native transport. It is intentionally separate from session
// control: a terminal is a local Desktop surface, never a remote client API.
type ManagedTerminalLauncher interface {
	OpenManagedTerminal(ctx context.Context, req ManagedTerminalRequest) (ManagedTerminalResult, error)
}

type LocalMetadata struct {
	PluginID    string
	DisplayName string
	Description string
	IconURL     string
	IconSVGURL  string
	Version     string
	// ManagedLaunch mirrors the manifest's managed_launch declaration: the
	// plugin process is under Hub lifecycle management. This is the env
	// contract (PRISM_PLUGIN_MANAGED_LAUNCH), NOT the launchable-targets
	// signal — see ManagedLaunchTargets.
	ManagedLaunch bool
	// ManagedLaunchTargets projects the manifest's managed_launch_specs:
	// externally launchable targets. Presence is the sole "can be launched"
	// signal for catalog clients and hub launch dispatch.
	ManagedLaunchTargets []ManagedLaunchTarget
	// RuntimeIntegration declares the per-runtime-mode integration kind
	// (protocol-native / desktop-automation). Modes not listed fall back to
	// the manifest's top-level integration_mode.
	RuntimeIntegration        map[string]string
	RuntimeModes              []string
	CurrentRuntimeMode        string
	RuntimeControls           RuntimeControlProfile
	RuntimeControlsDeclared   bool
	RuntimeOperations         RuntimeOperationProfile
	RuntimeOperationsDeclared bool
}

// ManagedLaunchTarget is the catalog projection of one manifest
// managed_launch_spec. Kind selects the hub launch path (desktop-app = CDP
// managed launch via the hub desktop app registry; service = manifest-declared
// daemon/service start command). Ready selects the success policy: probe waits
// for the plugin to report Available (the launch restores remote control);
// exit_ok accepts a zero exit of the start command.
type ManagedLaunchTarget struct {
	Kind         string   `json:"kind"`
	Ready        string   `json:"ready,omitempty"`
	RuntimeModes []string `json:"runtime_modes,omitempty"`
}

// RuntimeControlImplementation selects the one wire representation that owns
// a semantic composer control in the active Plugin runtime.
type RuntimeControlImplementation string

const (
	RuntimeControlStable      RuntimeControlImplementation = "stable"
	RuntimeControlInteractive RuntimeControlImplementation = "interactive"
)

// RuntimeControlProfile is mode-scoped. Empty fields mean that the active
// runtime does not expose that semantic control.
type RuntimeControlProfile struct {
	Model      RuntimeControlImplementation `json:"model,omitempty" yaml:"model,omitempty"`
	Reasoning  RuntimeControlImplementation `json:"reasoning,omitempty" yaml:"reasoning,omitempty"`
	Permission RuntimeControlImplementation `json:"permission,omitempty" yaml:"permission,omitempty"`
}

// RuntimeOperationImplementation selects the one wire representation that
// owns a standard conversation operation in the active Plugin runtime.
type RuntimeOperationImplementation string

const (
	RuntimeOperationDirect      RuntimeOperationImplementation = "direct"
	RuntimeOperationMenu        RuntimeOperationImplementation = "menu"
	RuntimeOperationUnsupported RuntimeOperationImplementation = "unsupported"
)

// RuntimeOperationProfile is mode-scoped. Direct operations are published in
// detail actions; menu operations are exposed through the opaque conversation
// menu-session flow. Empty fields have the same fail-closed meaning as
// unsupported for legacy/scaffold compatibility.
type RuntimeOperationProfile struct {
	Rename  RuntimeOperationImplementation `json:"rename,omitempty" yaml:"rename,omitempty"`
	Pin     RuntimeOperationImplementation `json:"pin,omitempty" yaml:"pin,omitempty"`
	Archive RuntimeOperationImplementation `json:"archive,omitempty" yaml:"archive,omitempty"`
	Delete  RuntimeOperationImplementation `json:"delete,omitempty" yaml:"delete,omitempty"`
}

type MetadataProvider interface {
	LocalMetadata() LocalMetadata
}

type ApprovalResolver interface {
	ResolveApproval(ctx context.Context, req ApprovalResolutionRequest) error
}

type IntegrationMode string

const (
	IntegrationModeProtocolNative    IntegrationMode = "protocol-native"
	IntegrationModeDesktopAutomation IntegrationMode = "desktop-automation"
)

// JSON field names are fixed explicitly to lock the on-wire protocol.
// Changing a struct field name without updating its json tag would silently
// break the Prism<->plugin contract. Keep these in sync with the plugin-side
// SDKs under sdk/.
type Capability struct {
	PluginID                   string `json:"PluginID"`
	Available                  bool   `json:"Available"`
	NativeVisibleInput         bool   `json:"NativeVisibleInput"`
	NativeVisibleOutput        bool   `json:"NativeVisibleOutput"`
	CanAttachSession           bool   `json:"CanAttachSession"`
	CanStartSessionWithMessage bool   `json:"CanStartSessionWithMessage"`
	CanOpenDraft               bool   `json:"CanOpenDraft"`
	// CanListSessions declares the plugin can enumerate real native sessions
	// that a remote client may choose and continue.
	CanListSessions bool `json:"CanListSessions"`
	// CanReadHistory declares the plugin can read native conversation bodies /
	// history for a chosen session, which is required for Codex-Mini style
	// remote conversation browsing.
	CanReadHistory bool `json:"CanReadHistory"`
	CanInterrupt   bool `json:"CanInterrupt"`
	CanApproval    bool `json:"CanApproval"`
	// CanForwardSync: forward path (Prism -> native application) supported.
	CanForwardSync bool `json:"CanForwardSync"`
	// CanReverseSync: reverse path (native application -> Prism) supported.
	CanReverseSync bool `json:"CanReverseSync"`
	// CanPluginWideWatch declares that reverse sync is one Plugin-wide watcher,
	// not an independent subscription for each native session.
	CanPluginWideWatch bool `json:"CanPluginWideWatch"`
	// CanWaitRun: adapter implements RunWaiter (adapter.waitForRun). Gates the
	// Hub's waitForRun call so it is not attempted on adapters that lack it.
	CanWaitRun bool `json:"CanWaitRun"`
	// CanReadStatus: adapter implements StatusReader (adapter.readStatus), the
	// run-process view (phase/steps/preview/context) complementing detail_snapshot.
	CanReadStatus bool `json:"CanReadStatus"`
	// CanControlSession: adapter implements SessionController (adapter.controlSession),
	// the writable half of the unified capability contract.
	CanControlSession bool `json:"CanControlSession"`
	// CanOpenManagedTerminal declares a local TUI can be attached to this
	// Plugin's already-owned native transport.
	CanOpenManagedTerminal bool `json:"CanOpenManagedTerminal"`
	// IntegrationMode declares how the plugin talks to the native application:
	// protocol-native for formal APIs/gateways/app-servers, desktop-automation
	// for UI/CDP/deeplink/accessibility driven control.
	IntegrationMode   IntegrationMode `json:"IntegrationMode"`
	VisibilitySurface string          `json:"VisibilitySurface"`
	UnavailableReason string          `json:"UnavailableReason"`
}

type DiscoveryResult struct {
	PluginID     string            `json:"PluginID"`
	Surface      string            `json:"Surface"`
	Endpoint     string            `json:"Endpoint"`
	ProcessID    int               `json:"ProcessID"`
	SessionHints map[string]string `json:"SessionHints"`
	Verified     bool              `json:"Verified"`
	Detail       string            `json:"Detail"`
}

type StartSessionWithMessageRequest struct {
	PluginID     string            `json:"PluginID"`
	Cwd          string            `json:"Cwd"`
	Message      InboundMessage    `json:"Message"`
	SourceDevice string            `json:"SourceDevice"`
	Metadata     map[string]string `json:"Metadata"`
}

type StartSessionWithMessageResult struct {
	Session    NativeSession    `json:"Session"`
	Receipt    SendReceipt      `json:"Receipt"`
	Visibility VisibilityResult `json:"Visibility"`
}

type DraftOpenRequest struct {
	DraftID      string            `json:"DraftID"`
	PluginID     string            `json:"PluginID"`
	Cwd          string            `json:"Cwd"`
	SourceDevice string            `json:"SourceDevice"`
	Metadata     map[string]string `json:"Metadata"`
}

type DraftOpenResult struct {
	DraftID          string         `json:"DraftID"`
	Cwd              string         `json:"Cwd"`
	Controls         map[string]any `json:"Controls"`
	DraftFingerprint string         `json:"draft_fingerprint"`
}

type DraftControlRequest struct {
	DraftID  string            `json:"DraftID"`
	PluginID string            `json:"PluginID"`
	Action   string            `json:"Action"`
	Target   any               `json:"Target,omitempty"`
	Name     string            `json:"Name,omitempty"`
	Metadata map[string]string `json:"Metadata,omitempty"`
}

type DraftControlResult struct {
	DraftID  string         `json:"DraftID"`
	Cwd      string         `json:"Cwd"`
	Controls map[string]any `json:"Controls"`
	Details  map[string]any `json:"Details,omitempty"`
}

type StartDraftWithMessageRequest struct {
	DraftID      string            `json:"DraftID"`
	PluginID     string            `json:"PluginID"`
	Cwd          string            `json:"Cwd"`
	Message      InboundMessage    `json:"Message"`
	SourceDevice string            `json:"SourceDevice"`
	Metadata     map[string]string `json:"Metadata"`
}

type AttachSessionRequest struct {
	PrismConversationID string            `json:"PrismConversationID"`
	PluginID            string            `json:"PluginID"`
	NativeSessionID     string            `json:"NativeSessionID"`
	NativeThreadID      string            `json:"NativeThreadID"`
	Cwd                 string            `json:"Cwd"`
	SourceDevice        string            `json:"SourceDevice"`
	Metadata            map[string]string `json:"Metadata"`
}

type ApprovalResolutionRequest struct {
	PrismConversationID string            `json:"PrismConversationID"`
	PluginID            string            `json:"PluginID"`
	Session             NativeSession     `json:"Session"`
	ApprovalRequestID   string            `json:"ApprovalRequestID"`
	ActionID            string            `json:"ActionID"`
	Input               string            `json:"Input,omitempty"`
	SourceDevice        string            `json:"SourceDevice"`
	Metadata            map[string]string `json:"Metadata"`
}

type NativeSession struct {
	PluginID        string `json:"PluginID"`
	NativeSessionID string `json:"NativeSessionID"`
	NativeThreadID  string `json:"NativeThreadID"`
	Surface         string `json:"Surface"`
	Endpoint        string `json:"Endpoint"`
	Cwd             string `json:"Cwd"`
	Visible         bool   `json:"Visible"`
}

type NativeSessionHint struct {
	PluginID            string            `json:"PluginID"`
	NativeSessionID     string            `json:"NativeSessionID"`
	NativeThreadID      string            `json:"NativeThreadID"`
	Surface             string            `json:"Surface"`
	Endpoint            string            `json:"Endpoint"`
	Cwd                 string            `json:"Cwd"`
	Title               string            `json:"Title"`
	PrismConversationID string            `json:"PrismConversationID"`
	Active              bool              `json:"Active"`
	Visible             bool              `json:"Visible"`
	LastActivityAt      time.Time         `json:"LastActivityAt"`
	Metadata            map[string]string `json:"Metadata"`
}

type InboundMessage struct {
	PrismMessageID string            `json:"PrismMessageID"`
	Text           string            `json:"Text"`
	Attachments    []Attachment      `json:"Attachments,omitempty"`
	SourceDevice   string            `json:"SourceDevice"`
	Timestamp      time.Time         `json:"Timestamp"`
	Metadata       map[string]string `json:"Metadata"`
}

// Attachment is command-only local input. Hub materializes the OSS object
// before invoking a Plugin, so URLs and encoded bytes never cross this RPC.
type Attachment struct {
	Name      string `json:"Name"`
	MIMEType  string `json:"MIMEType"`
	LocalPath string `json:"LocalPath"`
}

type SendReceipt struct {
	NativeMessageID          string `json:"NativeMessageID"`
	CanonicalNativeSessionID string `json:"CanonicalNativeSessionID,omitempty"`
	CanonicalNativeThreadID  string `json:"CanonicalNativeThreadID,omitempty"`
	Accepted                 bool   `json:"Accepted"`
	Visible                  bool   `json:"Visible"`
	PendingConfirmation      bool   `json:"PendingConfirmation,omitempty"`
	QueuePending             bool   `json:"QueuePending,omitempty"`
	Detail                   string `json:"Detail"`
}

type VisibilityResult struct {
	Visible       bool      `json:"Visible"`
	Marker        string    `json:"Marker"`
	Evidence      string    `json:"Evidence"`
	CheckedAt     time.Time `json:"CheckedAt"`
	FailureReason string    `json:"FailureReason"`
}

type PluginEvent struct {
	ID      string `json:"ID"`
	Type    string `json:"Type"`
	Status  string `json:"Status"`
	Summary string `json:"Summary"`
	// Attachment-bearing events should prefer lightweight metadata in Payload,
	// for example attachments[].name, rather than embedding image binary data.
	Payload   map[string]any `json:"Payload"`
	CreatedAt time.Time      `json:"CreatedAt"`
}

type HistoryMessage struct {
	ID        string    `json:"ID"`
	Role      string    `json:"Role"`
	Type      string    `json:"Type"`
	Content   string    `json:"Content"`
	Status    string    `json:"Status"`
	CreatedAt time.Time `json:"CreatedAt"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	// Progress is the one structured assistant execution unit for a turn. It
	// deliberately is not metadata: the remote body contract must preserve it.
	Progress *HistoryProgress `json:"Progress,omitempty"`
	// Attachments should be exposed here as metadata so remote clients can show
	// attachment/file names without requiring binary image sync.
	Metadata map[string]any `json:"Metadata,omitempty"`
}

type HistoryProgress struct {
	Status      string                `json:"Status"`
	StartedAt   time.Time             `json:"StartedAt"`
	CompletedAt time.Time             `json:"CompletedAt,omitempty"`
	Steps       []HistoryProgressStep `json:"Steps,omitempty"`
}

type HistoryProgressStep struct {
	ID        string    `json:"ID"`
	Kind      string    `json:"Kind"`
	CallID    string    `json:"CallID,omitempty"`
	Title     string    `json:"Title"`
	Detail    string    `json:"Detail"`
	Status    string    `json:"Status"`
	CreatedAt time.Time `json:"CreatedAt"`
}

type RuntimeStep struct {
	Kind   string `json:"kind,omitempty"`
	Label  string `json:"label,omitempty"`
	Text   string `json:"text,omitempty"`
	CallID string `json:"call_id,omitempty"`
	Time   string `json:"time,omitempty"`
}

type RuntimePhase struct {
	ID        string `json:"id,omitempty"`
	Label     string `json:"label,omitempty"`
	Detail    string `json:"detail,omitempty"`
	Source    string `json:"source,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type RuntimeStatus struct {
	Available         bool           `json:"available"`
	Active            bool           `json:"active"`
	Status            string         `json:"status,omitempty"`
	TurnID            string         `json:"turn_id,omitempty"`
	ThreadID          string         `json:"thread_id,omitempty"`
	SessionFile       string         `json:"session_file,omitempty"`
	UpdatedAt         string         `json:"updated_at,omitempty"`
	StartedAt         string         `json:"started_at,omitempty"`
	CompletedAt       string         `json:"completed_at,omitempty"`
	DurationMs        int64          `json:"duration_ms,omitempty"`
	Context           map[string]any `json:"context,omitempty"`
	Model             map[string]any `json:"model,omitempty"`
	ReasoningMode     map[string]any `json:"reasoning_mode,omitempty"`
	Interruptible     bool           `json:"interruptible,omitempty"`
	ApprovalBlocked   bool           `json:"approval_blocked,omitempty"`
	ApprovalRequestID string         `json:"approval_request_id,omitempty"`
	PrimaryAction     string         `json:"primary_action,omitempty"`
	CanQueue          bool           `json:"can_queue,omitempty"`
	DesktopState      map[string]any `json:"desktop_state,omitempty"`
	Phase             *RuntimePhase  `json:"phase,omitempty"`
	ProcessText       string         `json:"process_text,omitempty"`
	Preview           string         `json:"preview,omitempty"`
	Final             string         `json:"final,omitempty"`
	Error             string         `json:"error,omitempty"`
	Steps             []RuntimeStep  `json:"steps,omitempty"`
}

type SessionControlRequest struct {
	Session NativeSession `json:"session"`
	Action  string        `json:"action"`
	// Target is action-specific. Control option actions carry the opaque
	// Opaque option_id object from the latest detail snapshot.
	Target   any               `json:"target,omitempty"`
	Name     string            `json:"name,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type SessionControlResult struct {
	OK       bool           `json:"ok"`
	Action   string         `json:"action,omitempty"`
	ThreadID string         `json:"thread_id,omitempty"`
	Message  string         `json:"message,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
	// DetailsConfirmed is a per-result assertion, not a manifest capability.
	// Mutations may set it only after an authoritative native operation succeeds.
	DetailsConfirmed bool `json:"details_confirmed,omitempty"`
}

type ManagedTerminalRequest struct {
	PluginID        string `json:"plugin_id"`
	Cwd             string `json:"cwd,omitempty"`
	NativeSessionID string `json:"native_session_id,omitempty"`
	NativeThreadID  string `json:"native_thread_id,omitempty"`
}

type ManagedTerminalResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

type PluginStatus struct {
	ID         string     `json:"id"`
	PluginID   string     `json:"plugin_id"`
	Available  bool       `json:"available"`
	Surface    string     `json:"surface,omitempty"`
	Endpoint   string     `json:"endpoint,omitempty"`
	Visible    bool       `json:"visible"`
	LastError  string     `json:"last_error,omitempty"`
	Capability Capability `json:"-"`
}

// RuntimeHealth is a passive, side-effect-free snapshot of a supervised plugin
// process. It exposes supervisor bookkeeping only: reading it never probes or
// spawns the plugin process. Running=false with Failures=0 is the normal lazy
// idle state and is not an anomaly.
type RuntimeHealth struct {
	Running      bool
	Failures     int
	BackoffUntil time.Time
	LastError    string
}

// RuntimeHealthReporter is implemented by adapters that can expose their
// supervised process state without side effects.
type RuntimeHealthReporter interface {
	RuntimeHealth() RuntimeHealth
}
