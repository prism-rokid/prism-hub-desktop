// Package transport speaks the Prism Realtime JSON protocol used by the
// headless PluginBridge runtime.
package realtime

import (
	"fmt"
	"strconv"
	"strings"

	"rokid-prism-hub/internal/pluginruntime"
)

// Protocol version constants for capability negotiation.
const (
	ProtocolVersion    = "3.0"
	MinProtocolVersion = "3.0"
)

// CheckVersionCompatibility checks whether the remote version is compatible.
// Returns (isCompatible, shouldDowngrade, errorMessage).
func CheckVersionCompatibility(remoteVersion, remoteMinVersion string) (bool, bool, string) {
	if remoteVersion == "" {
		return false, false, "remote does not support protocol version negotiation (legacy client)"
	}
	if VersionLessThan(ProtocolVersion, remoteMinVersion) {
		return false, false, fmt.Sprintf("Prism Hub version %s is below remote minimum %s", ProtocolVersion, remoteMinVersion)
	}
	if VersionLessThan(remoteVersion, MinProtocolVersion) {
		return false, false, fmt.Sprintf("remote version %s is below Prism Hub minimum %s", remoteVersion, MinProtocolVersion)
	}
	return true, false, ""
}

// VersionLessThan compares two version strings (for example "2.1" < "2.2").
func VersionLessThan(v1, v2 string) bool {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")
	for i := 0; i < len(parts1) && i < len(parts2); i++ {
		n1, _ := strconv.Atoi(parts1[i])
		n2, _ := strconv.Atoi(parts2[i])
		if n1 != n2 {
			return n1 < n2
		}
	}
	return len(parts1) < len(parts2)
}

// MessageType enumerates protocol message_type values.
type MessageType string

const (
	MsgChatSend                                   MessageType = "chat.send"
	MsgChatSendResult                             MessageType = "chat.send.result"
	MsgChatFinal                                  MessageType = "chat.final"
	MsgChatError                                  MessageType = "chat.error"
	MsgCatalogSnapshot                            MessageType = "catalog.snapshot"
	MsgConversationIndexSnapshot                  MessageType = "conversation.index.snapshot"
	MsgConversationDetailSnapshot                 MessageType = "conversation.detail.snapshot"
	MsgConversationDetailUnavailable              MessageType = "conversation.detail.unavailable"
	MsgConversationStart                          MessageType = "conversation.start"
	MsgConversationStartResult                    MessageType = "conversation.start.result"
	MsgConversationDraftOpen                      MessageType = "conversation.draft.open"
	MsgConversationDraftOpenResult                MessageType = "conversation.draft.open.result"
	MsgConversationDraftControl                   MessageType = "conversation.draft.control"
	MsgConversationDraftControlResult             MessageType = "conversation.draft.control.result"
	MsgConversationDraftReopen                    MessageType = "conversation.draft.reopen"
	MsgConversationDraftStart                     MessageType = "conversation.draft.start"
	MsgConversationDraftStartResult               MessageType = "conversation.draft.start.result"
	MsgConversationDraftWatch                     MessageType = "conversation.draft.watch"
	MsgConversationDraftWatchResult               MessageType = "conversation.draft.watch.result"
	MsgConversationDraftUnwatch                   MessageType = "conversation.draft.unwatch"
	MsgConversationDraftUnwatchResult             MessageType = "conversation.draft.unwatch.result"
	MsgConversationDraftEvent                     MessageType = "conversation.draft.event"
	MsgConversationMenuSessionDescribe            MessageType = "conversation.menu_session.describe"
	MsgConversationMenuSessionDescribeResult      MessageType = "conversation.menu_session.describe.result"
	MsgConversationMenuSessionApply               MessageType = "conversation.menu_session.apply"
	MsgConversationMenuSessionApplyResult         MessageType = "conversation.menu_session.apply.result"
	MsgConversationDraftMenuSessionDescribe       MessageType = "conversation.draft.menu_session.describe"
	MsgConversationDraftMenuSessionDescribeResult MessageType = "conversation.draft.menu_session.describe.result"
	MsgConversationDraftMenuSessionApply          MessageType = "conversation.draft.menu_session.apply"
	MsgConversationDraftMenuSessionApplyResult    MessageType = "conversation.draft.menu_session.apply.result"
	MsgQueryRequest                               MessageType = "query.request"
	MsgQueryResponse                              MessageType = "query.response"
	MsgTaskEvent                                  MessageType = "task.event"
	MsgApprovalRequired                           MessageType = "approval.required"
	MsgApprovalResolve                            MessageType = "approval.resolve"
	MsgTaskInterrupt                              MessageType = "task.interrupt"
	MsgSessionControl                             MessageType = "session.control"
	MsgPluginRuntimeSwitch                        MessageType = "plugin.runtime.switch"
	MsgPluginRuntimeSwitchResult                  MessageType = "plugin.runtime.switch.result"
	MsgDesktopLaunch                              MessageType = "desktop.launch"
	MsgDesktopLaunchResult                        MessageType = "desktop.launch.result"
	MsgHostPluginRefresh                          MessageType = "host.plugin.refresh"
	MsgConversationEvent                          MessageType = "conversation.event"
	MsgConversationEventAck                       MessageType = "conversation.event.ack"
	MsgConversationBody                           MessageType = "conversation.body"
	MsgPing                                       MessageType = "ping"
	MsgPong                                       MessageType = "pong"
)

// Canonical returns the v3 protocol message type unchanged. V3 intentionally
// has no legacy wire aliases.
func (t MessageType) Canonical() MessageType {
	return t
}

// WireCompatible returns the v3 protocol message type unchanged.
func (t MessageType) WireCompatible() MessageType {
	return t
}

func (t MessageType) IsConversationEvent() bool {
	return t.Canonical() == MsgConversationEvent
}

// Inbound is a chat message received from Prism Realtime.
type Inbound struct {
	MessageID   string          `json:"message_id"`
	MessageType MessageType     `json:"message_type"`
	Timestamp   int64           `json:"timestamp,omitempty"`
	Payload     *InboundPayload `json:"payload,omitempty"`
}

// InboundPayload is the payload object of an inbound chat.
type InboundPayload struct {
	Text              string   `json:"text,omitempty"`
	Attachments       []string `json:"attachments,omitempty"`
	RequestID         string   `json:"request_id,omitempty"`
	QueryType         string   `json:"query_type,omitempty"`
	TaskID            string   `json:"task_id,omitempty"`
	ApprovalRequestID string   `json:"approval_request_id,omitempty"`
	ActionID          string   `json:"action_id,omitempty"`
	Input             string   `json:"input,omitempty"`
	Reason            string   `json:"reason,omitempty"`
	Action            string   `json:"action,omitempty"`
	Target            any      `json:"target,omitempty"`
	Name              string   `json:"name,omitempty"`
	PluginID          string   `json:"plugin_id,omitempty"`
	RuntimeMode       string   `json:"runtime_mode,omitempty"`
	// LaunchKind optionally disambiguates the managed launch target when the
	// plugin declares several probe-linked specs (desktop-app vs service).
	LaunchKind      string           `json:"launch_kind,omitempty"`
	Cwd             string           `json:"cwd,omitempty"`
	ConversationID  string           `json:"conversation_id,omitempty"`
	DraftID         string           `json:"draft_id,omitempty"`
	DraftStreamID   string           `json:"draft_stream_id,omitempty"`
	ClientMessageID string           `json:"message_id,omitempty"`
	InputMode       string           `json:"input_mode,omitempty"`
	Limit           int              `json:"limit,omitempty"`
	Live            bool             `json:"live,omitempty"`
	BodyStreamID    string           `json:"body_stream_id,omitempty"`
	PageID          string           `json:"page_id,omitempty"`
	MenuSessionID   string           `json:"menu_session_id,omitempty"`
	EntryID         string           `json:"entry_id,omitempty"`
	Owner           map[string]any   `json:"owner,omitempty"`
	Metadata        *InboundMetadata `json:"metadata,omitempty"`
}

type InboundMetadata struct {
	SourceDevice string `json:"source_device,omitempty"`
	RequestKind  string `json:"request_kind,omitempty"`
	ControlScope string `json:"control_scope,omitempty"`
}

// Outbound is a chat/error message sent to Prism Realtime.
type Outbound struct {
	MessageID   string           `json:"message_id"`
	MessageType MessageType      `json:"message_type"`
	Timestamp   int64            `json:"timestamp"`
	Payload     *OutboundPayload `json:"payload,omitempty"`
}

// OutboundPayload is the payload object of an outbound chat reply.
type OutboundPayload struct {
	RequestID      string               `json:"request_id,omitempty"`
	Step           string               `json:"step,omitempty"`
	Status         string               `json:"status,omitempty"`
	Finalized      bool                 `json:"finalized"`
	ConversationID string               `json:"conversation_id,omitempty"`
	Message        *OutboundChatMessage `json:"message,omitempty"`
}

type OutboundChatMessage struct {
	Type     string                 `json:"type,omitempty"`
	Content  string                 `json:"content,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ControlOutbound is a generic control-plane frame sent to Prism Realtime.
type ControlOutbound struct {
	MessageID   string      `json:"message_id"`
	MessageType MessageType `json:"message_type"`
	Timestamp   int64       `json:"timestamp"`
	Payload     any         `json:"payload,omitempty"`
}

func (m Outbound) wireCompatible() Outbound {
	m.MessageType = m.MessageType.WireCompatible()
	return m
}

func (m ControlOutbound) wireCompatible() ControlOutbound {
	m.MessageType = m.MessageType.WireCompatible()
	return m
}

type CatalogSnapshotPayload struct {
	// Plugins is the host-reported remote-mainline candidate set. Plugins
	// excluded from the current remote mainline are omitted before publish.
	Plugins []CatalogPlugin `json:"plugins"`
}

type CatalogPlugin struct {
	PluginID           string `json:"plugin_id"`
	DisplayName        string `json:"display_name"`
	Description        string `json:"description,omitempty"`
	IconURL            string `json:"icon_url,omitempty"`
	IconSVGURL         string `json:"icon_svg_url,omitempty"`
	Version            string `json:"version,omitempty"`
	Installed          bool   `json:"installed,omitempty"`
	Available          bool   `json:"available,omitempty"`
	AvailabilityReason string `json:"availability_reason,omitempty"`
	// Enabled means this plugin is currently admitted into the host's remote
	// mainline candidate set, not merely that a local plugin exists.
	Enabled             bool   `json:"enabled,omitempty"`
	RemoteSupported     bool   `json:"remote_supported,omitempty"`
	RemoteSupportReason string `json:"remote_support_reason,omitempty"`
	// VisibleInRemote is the host-side "eligible to be remotely shown" result
	// before any server-side quota/admission policy is applied.
	VisibleInRemote    bool   `json:"visible_in_remote,omitempty"`
	VisibilityVerified bool   `json:"visibility_verified,omitempty"`
	Surface            string `json:"surface,omitempty"`
	IntegrationMode    string `json:"integration_mode,omitempty"`
	// ManagedLaunch means the manifest declares managed_launch_specs: the hub
	// can launch an external target (desktop app or daemon service) on
	// explicit user request. The manifest's managed_launch env-contract bool
	// does NOT imply this. Targets carry kind/ready/mode details for remote
	// UIs; start/stop argv never cross the wire.
	ManagedLaunch              bool                                `json:"managed_launch,omitempty"`
	ManagedLaunchTargets       []pluginruntime.ManagedLaunchTarget `json:"managed_launch_targets,omitempty"`
	RuntimeModes               []string                            `json:"runtime_modes,omitempty"`
	CurrentRuntimeMode         string                              `json:"current_runtime_mode,omitempty"`
	CanAttachSession           bool                                `json:"can_attach_session,omitempty"`
	CanStartSessionWithMessage bool                                `json:"can_start_session_with_message,omitempty"`
	CanOpenDraft               bool                                `json:"can_open_draft,omitempty"`
	CanListSessions            bool                                `json:"can_list_sessions,omitempty"`
	CanReadHistory             bool                                `json:"can_read_history,omitempty"`
	CanInterrupt               bool                                `json:"can_interrupt,omitempty"`
	CanApproval                bool                                `json:"can_approval,omitempty"`
	CanForwardSync             bool                                `json:"can_forward_sync,omitempty"`
	CanReverseSync             bool                                `json:"can_reverse_sync,omitempty"`
	CanWaitRun                 bool                                `json:"can_wait_run,omitempty"`
	CanReadStatus              bool                                `json:"can_read_status,omitempty"`
	CanControlSession          bool                                `json:"can_control_session,omitempty"`
	ReverseSyncEnabled         bool                                `json:"reverse_sync_enabled,omitempty"`
	AgentUsage                 *CatalogAgentUsage                  `json:"agent_usage,omitempty"`
}

// PluginRuntimeSwitchResultPayload is the result of a plugin-scoped runtime
// replacement. Accepted means Hub saved a manifest-declared mode and replaced
// only that Plugin adapter; the following catalog snapshot remains the source
// of availability after the new native runtime probes.
type PluginRuntimeSwitchResultPayload struct {
	RequestID   string `json:"request_id,omitempty"`
	PluginID    string `json:"plugin_id,omitempty"`
	RuntimeMode string `json:"runtime_mode,omitempty"`
	Accepted    bool   `json:"accepted"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
}

// DesktopLaunchResultPayload is the result of a remote managed desktop launch.
// Accepted means the managed app reached CDP readiness; the following catalog
// snapshot remains the source of plugin availability after the gateway probe.
// The launch target is identified by the plugin that owns the desktop app.
type DesktopLaunchResultPayload struct {
	RequestID string `json:"request_id,omitempty"`
	PluginID  string `json:"plugin_id,omitempty"`
	Accepted  bool   `json:"accepted"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message,omitempty"`
	CDPPort   string `json:"cdp_port,omitempty"`
}

// CatalogAgentUsage is a transient, display-only account projection. It
// deliberately excludes account identity, plan and credit fields.
type CatalogAgentUsage struct {
	FiveHour       *CatalogAgentUsageFiveHour  `json:"five_hour,omitempty"`
	Weekly         *CatalogAgentUsageWeekly    `json:"weekly,omitempty"`
	AccountSummary *CatalogAccountUsageSummary `json:"account_summary,omitempty"`
	UpdatedAt      int64                       `json:"updated_at,omitempty"`
}

type CatalogAgentUsageFiveHour struct {
	UsedPercent   int64 `json:"used_percent"`
	WindowMinutes int64 `json:"window_minutes"`
	ResetsAt      int64 `json:"resets_at"`
}

type CatalogAgentUsageWeekly struct {
	UsedPercent   int64 `json:"used_percent"`
	WindowMinutes int64 `json:"window_minutes"`
	ResetsAt      int64 `json:"resets_at"`
}

type CatalogAccountUsageSummary struct {
	LifetimeTokens    int64 `json:"lifetime_tokens"`
	PeakDailyTokens   int64 `json:"peak_daily_tokens"`
	CurrentStreakDays int64 `json:"current_streak_days"`
	LongestStreakDays int64 `json:"longest_streak_days"`
}

// ConversationIndexSnapshotPayload is the lightweight list cache pushed by Hub
// for mobile/glasses conversation navigation. It intentionally excludes message
// body content; detail/body still has to be fetched online from the plugin.
type ConversationIndexSnapshotPayload struct {
	// Plugins is the host-reported conversation index for plugins still in
	// the remote mainline.
	Plugins   []ConversationIndexPlugin `json:"plugins"`
	Signature string                    `json:"signature,omitempty"`
}

type ConversationIndexPlugin struct {
	PluginID    string `json:"plugin_id"`
	DisplayName string `json:"display_name,omitempty"`
	Available   bool   `json:"available,omitempty"`
	// Enabled means the conversation index belongs to a plugin still admitted
	// into the host's remote mainline at snapshot time.
	Enabled       bool                    `json:"enabled,omitempty"`
	UpdatedAt     string                  `json:"updated_at,omitempty"`
	Signature     string                  `json:"signature,omitempty"`
	Conversations []ConversationIndexItem `json:"conversations,omitempty"`
}

type ConversationIndexItem struct {
	ID        string            `json:"id"`
	Title     string            `json:"title,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	Status    string            `json:"status,omitempty"`
	UpdatedAt string            `json:"updated_at,omitempty"`
	SortAt    string            `json:"sort_at,omitempty"`
	Pinned    bool              `json:"pinned,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// TaskEventPayload is an incremental task event emitted by the bridge path.
type TaskEventPayload struct {
	ConversationID string `json:"conversation_id"`
	PluginID       string `json:"plugin_id"`
	RequestID      string `json:"request_id,omitempty"`
	TaskID         string `json:"task_id"`
	EventType      string `json:"event_type"`
	Status         string `json:"status,omitempty"`
	Step           string `json:"step,omitempty"`
	Summary        string `json:"summary,omitempty"`
	SourceDevice   string `json:"source_device,omitempty"`
	SessionReused  bool   `json:"session_reused,omitempty"`
}

type ApprovalRequiredPayload struct {
	ConversationID    string           `json:"conversation_id"`
	PluginID          string           `json:"plugin_id"`
	RequestID         string           `json:"request_id,omitempty"`
	TaskID            string           `json:"task_id,omitempty"`
	ApprovalRequestID string           `json:"approval_request_id,omitempty"`
	Title             string           `json:"title,omitempty"`
	Summary           string           `json:"summary,omitempty"`
	Description       string           `json:"description,omitempty"`
	Actions           []map[string]any `json:"actions,omitempty"`
	Input             map[string]any   `json:"input,omitempty"`
	CreatedAt         int64            `json:"created_at,omitempty"`
	SourceDevice      string           `json:"source_device,omitempty"`
	Metadata          map[string]any   `json:"metadata,omitempty"`
}

type ConversationEventPayload struct {
	ConversationID string         `json:"conversation_id"`
	PluginID       string         `json:"plugin_id"`
	EventID        string         `json:"event_id,omitempty"`
	EventType      string         `json:"event_type,omitempty"`
	Status         string         `json:"status,omitempty"`
	Summary        string         `json:"summary,omitempty"`
	SourceDevice   string         `json:"source_device,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
}

// ConversationDetailUnavailablePayload removes one strict live detail
// projection. The API then emits conversation.unavailable from the missing
// projection rather than exposing any Hub-local stale cache.
type ConversationDetailUnavailablePayload struct {
	ConversationID string `json:"conversation_id"`
	PluginID       string `json:"plugin_id"`
	Code           string `json:"code"`
}

type QueryResponsePayload struct {
	RequestID      string         `json:"request_id,omitempty"`
	QueryType      string         `json:"query_type,omitempty"`
	ConversationID string         `json:"conversation_id,omitempty"`
	PluginID       string         `json:"plugin_id,omitempty"`
	Success        bool           `json:"success"`
	Error          string         `json:"error,omitempty"`
	Result         map[string]any `json:"result,omitempty"`
}

// ConversationBodyPayload is a turn-level body stream frame. Detail and body
// intentionally remain separate so a control/status refresh cannot be
// rendered as a body reload by Mobile or glasses clients.
type ConversationBodyPayload struct {
	RequestID      string         `json:"request_id"`
	BodyStreamID   string         `json:"body_stream_id"`
	PageID         string         `json:"page_id,omitempty"`
	ConversationID string         `json:"conversation_id"`
	PluginID       string         `json:"plugin_id"`
	Source         string         `json:"source"`
	Operation      string         `json:"operation"`
	EventType      string         `json:"event_type"`
	Turn           map[string]any `json:"turn,omitempty"`
	Error          string         `json:"error,omitempty"`
	Retryable      bool           `json:"retryable,omitempty"`
}

type ConversationStartResultPayload struct {
	RequestID      string `json:"request_id,omitempty"`
	PluginID       string `json:"plugin_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	MessageID      string `json:"message_id,omitempty"`
	Accepted       bool   `json:"accepted"`
	Code           string `json:"code,omitempty"`
	Message        string `json:"message,omitempty"`
}

// ChatSendResultPayload is the synchronous receipt for an existing
// conversation send. It carries no Desktop state: queue and transcript state
// continue to arrive only from watcher snapshots and body events.
type ChatSendResultPayload struct {
	RequestID           string `json:"request_id,omitempty"`
	PluginID            string `json:"plugin_id,omitempty"`
	ConversationID      string `json:"conversation_id,omitempty"`
	MessageID           string `json:"message_id,omitempty"`
	Accepted            bool   `json:"accepted"`
	Visible             bool   `json:"visible"`
	PendingConfirmation bool   `json:"pending_confirmation,omitempty"`
	QueuePending        bool   `json:"queue_pending,omitempty"`
	Code                string `json:"code,omitempty"`
	Message             string `json:"message,omitempty"`
}

// ConversationDraftResultPayload is the synchronous result for draft open,
// control, reopen and first-send promotion. DraftID is opaque and is never a
// canonical conversation identity.
type ConversationDraftResultPayload struct {
	RequestID      string         `json:"request_id,omitempty"`
	PluginID       string         `json:"plugin_id,omitempty"`
	DraftID        string         `json:"draft_id,omitempty"`
	DraftStreamID  string         `json:"draft_stream_id,omitempty"`
	DraftState     string         `json:"draft_state,omitempty"`
	Cwd            string         `json:"cwd,omitempty"`
	Controls       map[string]any `json:"controls,omitempty"`
	ConversationID string         `json:"conversation_id,omitempty"`
	MessageID      string         `json:"message_id,omitempty"`
	Accepted       bool           `json:"accepted"`
	Code           string         `json:"code,omitempty"`
	Message        string         `json:"message,omitempty"`
}

// ConversationDraftEventPayload is a transient notification for one opaque
// draft stream. It deliberately has no canonical conversation identity.
type ConversationDraftEventPayload struct {
	PluginID      string `json:"plugin_id"`
	DraftID       string `json:"draft_id"`
	DraftStreamID string `json:"draft_stream_id"`
	EventType     string `json:"event_type"`
	Code          string `json:"code,omitempty"`
	Message       string `json:"message,omitempty"`
}

// MenuSessionResultPayload is the synchronous response to a one-time native
// menu describe/apply exchange. Entries are display-only opaque data and are
// never included in a detail snapshot or state stream.
type MenuSessionResultPayload struct {
	RequestID      string             `json:"request_id,omitempty"`
	PluginID       string             `json:"plugin_id,omitempty"`
	ConversationID string             `json:"conversation_id,omitempty"`
	DraftID        string             `json:"draft_id,omitempty"`
	MenuSessionID  string             `json:"menu_session_id,omitempty"`
	Owner          map[string]any     `json:"owner,omitempty"`
	Entries        []MenuSessionEntry `json:"entries,omitempty"`
	Groups         []MenuSessionGroup `json:"groups,omitempty"`
	ExpiresAt      int64              `json:"expires_at,omitempty"`
	// Controls contains an immediately observed transient result, such as a
	// confirmation surface opened by the selected native menu entry. It is not
	// a detail projection; the watcher remains the source of the next detail.
	Controls map[string]any `json:"controls,omitempty"`
	Accepted bool           `json:"accepted"`
	Code     string         `json:"code,omitempty"`
	Message  string         `json:"message,omitempty"`
}

// MenuSessionEntry deliberately carries only raw desktop display data. Its ID
// is valid only together with MenuSessionID until the fixed expiry.
type MenuSessionEntry struct {
	EntryID   string `json:"entry_id"`
	Label     string `json:"label"`
	Available bool   `json:"available"`
	Current   bool   `json:"current"`
}

// MenuSessionGroup retains a native menu's visible hierarchy. The entry ID is
// still opaque and is valid only with the parent menu_session_id.
type MenuSessionGroup struct {
	GroupID string             `json:"group_id"`
	Label   string             `json:"label"`
	Entries []MenuSessionEntry `json:"entries"`
}
