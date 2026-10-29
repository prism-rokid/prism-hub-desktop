package bridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

const (
	StageReceived            = "received"
	StageResolving           = "resolving"
	StageNativeSessionReady  = "native_session_ready"
	StageAccepted            = "accepted"
	StagePendingConfirmation = "pending_confirmation"
	StageVisible             = "visible"
	StageRunning             = "running"
	StageWaitingApproval     = "waiting_approval"
	StageCompleted           = "completed"
	StageFailed              = "failed"
	StageInterrupted         = "interrupted"
	StageDuplicate           = "duplicate"
)

type PrismSink interface {
	Send(prismtransport.Outbound) error
	SendControl(prismtransport.ControlOutbound) error
}

type Router struct {
	Service                *pluginruntime.Service
	DB                     *bridgedb.DB
	Config                 *bridgeconfig.Config
	ConfigProvider         func() *bridgeconfig.Config
	Sink                   PrismSink
	Logger                 *slog.Logger
	Runs                   *RunTracker
	DetailSnapshots        *DetailSnapshotStore
	SnapshotPublisher      *SnapshotPublisher
	AttachmentMaterializer AttachmentMaterializer
	startMu                sync.Mutex
	draftMu                sync.Mutex
	drafts                 map[string]draftRecord
	draftWatchers          map[string]map[string]struct{}
	menuSessionMu          sync.Mutex
	MenuSessions           *MenuSessionStore
	snapshotMu             sync.Mutex
}

type draftRecord struct {
	PluginID         string
	Cwd              string
	State            string
	Controls         map[string]any
	DraftFingerprint string
}

// HandleConversationStart starts a new native conversation from its first
// message. It never receives or synthesizes a canonical conversation ID; the
// ID becomes available only after the native session and visible first message
// are durably bound in the local database.
func (r *Router) HandleConversationStart(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	messageID := ""
	text := ""
	attachmentInputs := []string(nil)
	if in.Payload != nil {
		messageID = strings.TrimSpace(in.Payload.ClientMessageID)
		text = strings.TrimSpace(in.Payload.Text)
		attachmentInputs = in.Payload.Attachments
		if strings.TrimSpace(in.Payload.ConversationID) != "" {
			return RouteResult{}, fmt.Errorf("conversation.start must not include conversation_id")
		}
	}
	if messageID == "" || (text == "" && len(attachmentInputs) == 0) {
		return RouteResult{}, fmt.Errorf("conversation.start requires client_message_id and text or attachments")
	}
	pluginID, err := r.resolvePlugin(ctx, in)
	result := RouteResult{TaskID: stableTaskID(firstNonEmpty(in.MessageID, messageID)), PluginID: pluginID}
	if err != nil {
		return result, err
	}
	if r.DB == nil {
		return result, fmt.Errorf("conversation.start requires bridge db")
	}
	if !r.remoteAccessEnabled(pluginID) {
		return result, fmt.Errorf("%s remote access disabled", pluginID)
	}
	adapter := r.adapter(pluginID)
	if adapter == nil {
		return result, fmt.Errorf("plugin adapter not found: %s", pluginID)
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return result, fmt.Errorf("probe %s: %w", pluginID, err)
	}
	if !capability.Available || !capability.CanStartSessionWithMessage {
		reason := firstNonEmpty(strings.TrimSpace(capability.UnavailableReason), "startSessionWithMessage unavailable")
		return result, fmt.Errorf("%s unavailable: %s", pluginID, reason)
	}
	starter, ok := adapter.(pluginruntime.SessionStarter)
	if !ok {
		return result, fmt.Errorf("%s declares startSessionWithMessage but does not implement it", pluginID)
	}
	cwd, err := resolveInboundCwd(in)
	if err != nil {
		return result, fmt.Errorf("resolve cwd: %w", err)
	}

	// Serialize starts in one Hub process. The durable operation ledger then
	// carries the same idempotency key across reconnects and restarts.
	r.startMu.Lock()
	defer r.startMu.Unlock()
	operation, err := r.DB.BeginStartOperation(ctx, pluginID, messageID, firstNonEmpty(in.Payload.RequestID, in.MessageID))
	if err != nil {
		return result, err
	}
	if operation.Status == "completed" {
		record, findErr := r.DB.FindSession(ctx, operation.PrismConversationID, pluginID)
		if findErr != nil {
			return result, fmt.Errorf("load completed start binding: %w", findErr)
		}
		result.ConversationID = record.PrismConversationID
		result.Session = nativeSessionFromRecord(record)
		result.Receipt = pluginruntime.SendReceipt{NativeMessageID: operation.NativeMessageID, Accepted: true, Visible: true, Detail: "duplicate conversation.start"}
		result.Visible = true
		result.Duplicate = true
		return result, nil
	}
	attachments, cleanup, err := r.materializeAttachments(ctx, attachmentInputs)
	if err != nil {
		return result, fmt.Errorf("materialize start attachments: %w", err)
	}
	started, err := starter.StartSessionWithMessage(ctx, pluginruntime.StartSessionWithMessageRequest{
		PluginID: pluginID,
		Cwd:      cwd,
		Message: pluginruntime.InboundMessage{
			PrismMessageID: messageID,
			Text:           text,
			Attachments:    attachments,
			SourceDevice:   inboundSourceDevice(in),
			Timestamp:      inboundTimestamp(in),
			Metadata:       map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID)},
		},
		SourceDevice: inboundSourceDevice(in),
		Metadata:     map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID)},
	})
	if err != nil {
		cleanup()
		return result, fmt.Errorf("start native session: %w", err)
	}
	if strings.TrimSpace(started.Session.NativeSessionID) == "" && strings.TrimSpace(started.Session.NativeThreadID) == "" {
		return result, fmt.Errorf("start native session returned no native identity")
	}
	if !started.Receipt.Accepted {
		return result, fmt.Errorf("start native session did not accept first message: %s", strings.TrimSpace(started.Receipt.Detail))
	}
	if !started.Receipt.Visible {
		return result, fmt.Errorf("start native session receipt did not confirm first-message visibility: %s", strings.TrimSpace(started.Receipt.Detail))
	}
	if !started.Visibility.Visible {
		return result, fmt.Errorf("start native session first message not visible: %s", strings.TrimSpace(started.Visibility.FailureReason))
	}
	started.Session.PluginID = firstNonEmpty(started.Session.PluginID, pluginID)
	record, stored, err := r.DB.CompleteStartOperation(ctx, operation, bridgedb.SessionRecord{
		PluginID:           pluginID,
		Cwd:                started.Session.Cwd,
		NativeSessionID:    started.Session.NativeSessionID,
		NativeThreadID:     started.Session.NativeThreadID,
		Surface:            started.Session.Surface,
		Endpoint:           started.Session.Endpoint,
		VisibilityVerified: true,
		VisibilityEvidence: started.Visibility.Evidence,
	}, started.Receipt.NativeMessageID)
	if err != nil {
		return result, err
	}
	result.ConversationID = record.PrismConversationID
	result.Session = nativeSessionFromRecord(stored)
	result.Receipt = started.Receipt
	result.Visible = true
	// The binding is now durable and can be exposed through the Plugin index.
	// Command acceptance elsewhere is not a state change and does not publish.
	r.emitConversationIndex(pluginID)
	return result, nil
}

// HandleConversationDraftOpen opens a native anonymous draft. The resulting
// DraftID remains in-memory only and is deliberately excluded from bindings,
// indexes, selections and detail projections.
func (r *Router) HandleConversationDraftOpen(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	if in.Payload == nil {
		return RouteResult{}, fmt.Errorf("conversation.draft.open requires payload")
	}
	if strings.TrimSpace(in.Payload.ConversationID) != "" || strings.TrimSpace(in.Payload.DraftID) != "" {
		return RouteResult{}, fmt.Errorf("conversation.draft.open must not include conversation_id or draft_id")
	}
	pluginID, err := r.resolvePlugin(ctx, in)
	result := RouteResult{PluginID: pluginID}
	if err != nil {
		return result, err
	}
	if !r.remoteAccessEnabled(pluginID) {
		return result, fmt.Errorf("%s remote access disabled", pluginID)
	}
	adapter := r.adapter(pluginID)
	if adapter == nil {
		return result, fmt.Errorf("plugin adapter not found: %s", pluginID)
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return result, fmt.Errorf("probe %s: %w", pluginID, err)
	}
	if !capability.Available || !capability.CanOpenDraft {
		reason := firstNonEmpty(strings.TrimSpace(capability.UnavailableReason), "openDraft unavailable")
		return result, fmt.Errorf("%s unavailable: %s", pluginID, reason)
	}
	opener, ok := adapter.(pluginruntime.DraftOpener)
	if !ok {
		return result, fmt.Errorf("%s declares openDraft but does not implement it", pluginID)
	}
	cwd, err := resolveInboundCwd(in)
	if err != nil {
		return result, fmt.Errorf("resolve cwd: %w", err)
	}
	draftID := "draft_" + uuid.NewString()
	opened, err := opener.OpenDraft(ctx, pluginruntime.DraftOpenRequest{
		DraftID: draftID, PluginID: pluginID, Cwd: cwd,
		SourceDevice: inboundSourceDevice(in),
		Metadata:     map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID)},
	})
	if err != nil {
		return result, fmt.Errorf("open native draft: %w", err)
	}
	if strings.TrimSpace(opened.DraftID) != draftID {
		return result, fmt.Errorf("open native draft returned mismatched draft_id")
	}
	r.draftMu.Lock()
	if r.drafts == nil {
		r.drafts = make(map[string]draftRecord)
	}
	r.drafts[draftID] = draftRecord{
		PluginID: pluginID, Cwd: firstNonEmpty(opened.Cwd, cwd), State: "open", Controls: opened.Controls,
		DraftFingerprint: strings.TrimSpace(opened.DraftFingerprint),
	}
	r.draftMu.Unlock()
	result.DraftID = draftID
	result.DraftState = "open"
	result.Cwd = firstNonEmpty(opened.Cwd, cwd)
	result.Controls = opened.Controls
	return result, nil
}

func (r *Router) draftForInbound(in prismtransport.Inbound) (string, draftRecord, error) {
	if in.Payload == nil || strings.TrimSpace(in.Payload.ConversationID) != "" {
		return "", draftRecord{}, fmt.Errorf("draft command must not include conversation_id")
	}
	draftID := strings.TrimSpace(in.Payload.DraftID)
	if draftID == "" {
		return "", draftRecord{}, fmt.Errorf("draft command requires draft_id")
	}
	r.draftMu.Lock()
	record, ok := r.drafts[draftID]
	r.draftMu.Unlock()
	if !ok {
		return draftID, draftRecord{}, fmt.Errorf("draft_stale: desktop draft is no longer available")
	}
	if record.State == "draft_stale" {
		return draftID, record, fmt.Errorf("draft_stale: desktop draft is no longer available")
	}
	if requested := strings.TrimSpace(in.Payload.PluginID); requested != "" && requested != record.PluginID {
		return draftID, draftRecord{}, fmt.Errorf("draft plugin_id mismatch")
	}
	return draftID, record, nil
}

func (r *Router) discardDraft(draftID string) {
	r.draftMu.Lock()
	delete(r.drafts, draftID)
	delete(r.draftWatchers, draftID)
	r.draftMu.Unlock()
}

func (r *Router) markDraftStale(draftID, reason string) {
	var pluginID string
	var streamIDs []string
	r.draftMu.Lock()
	if record, ok := r.drafts[draftID]; ok {
		if record.State == "draft_stale" {
			r.draftMu.Unlock()
			return
		}
		record.State = "draft_stale"
		r.drafts[draftID] = record
		pluginID = record.PluginID
		for streamID := range r.draftWatchers[draftID] {
			streamIDs = append(streamIDs, streamID)
		}
		delete(r.draftWatchers, draftID)
	}
	r.draftMu.Unlock()
	for _, streamID := range streamIDs {
		r.emitDraftEvent(pluginID, draftID, streamID, "draft.stale", "draft_stale", firstNonEmpty(reason, "desktop draft is no longer available"))
	}
}

// MarkPluginDraftStale is invoked only by the plugin-wide foreground watcher.
// Drafts stay local to Hub and never become a conversation projection.
func (r *Router) MarkPluginDraftStale(pluginID, draftID, reason string) {
	pluginID = strings.TrimSpace(pluginID)
	draftID = strings.TrimSpace(draftID)
	if pluginID == "" {
		return
	}
	if draftID != "" {
		r.draftMu.Lock()
		record, ok := r.drafts[draftID]
		r.draftMu.Unlock()
		if ok && record.PluginID == pluginID {
			r.markDraftStale(draftID, reason)
		}
		return
	}
	r.draftMu.Lock()
	ids := make([]string, 0, len(r.drafts))
	for id, record := range r.drafts {
		if record.PluginID == pluginID && record.State != "draft_stale" {
			ids = append(ids, id)
		}
	}
	r.draftMu.Unlock()
	for _, id := range ids {
		r.markDraftStale(id, reason)
	}
}

func (r *Router) emitDraftEvent(pluginID, draftID, streamID, eventType, code, message string) {
	if r == nil || r.Sink == nil || strings.TrimSpace(streamID) == "" {
		return
	}
	err := r.Sink.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgConversationDraftEvent,
		Payload: prismtransport.ConversationDraftEventPayload{
			PluginID: pluginID, DraftID: draftID, DraftStreamID: streamID,
			EventType: eventType, Code: code, Message: message,
		},
	})
	if err != nil && r.Logger != nil {
		r.Logger.Warn("draft event send failed", "plugin_id", pluginID, "draft_id", draftID, "draft_stream_id", streamID, "err", err)
	}
}

func (r *Router) HandleConversationDraftWatch(_ context.Context, in prismtransport.Inbound) (RouteResult, error) {
	draftID, record, err := r.draftForInbound(in)
	result := RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: "open", Cwd: record.Cwd, Controls: record.Controls}
	if err != nil {
		result.DraftState = "draft_stale"
		return result, err
	}
	streamID := strings.TrimSpace(in.Payload.DraftStreamID)
	if streamID == "" {
		return result, fmt.Errorf("conversation.draft.watch requires draft_stream_id")
	}
	r.draftMu.Lock()
	if r.draftWatchers == nil {
		r.draftWatchers = make(map[string]map[string]struct{})
	}
	if r.draftWatchers[draftID] == nil {
		r.draftWatchers[draftID] = make(map[string]struct{})
	}
	r.draftWatchers[draftID][streamID] = struct{}{}
	r.draftMu.Unlock()
	return result, nil
}

func (r *Router) HandleConversationDraftUnwatch(_ context.Context, in prismtransport.Inbound) (RouteResult, error) {
	if in.Payload == nil {
		return RouteResult{}, fmt.Errorf("conversation.draft.unwatch requires payload")
	}
	draftID := strings.TrimSpace(in.Payload.DraftID)
	streamID := strings.TrimSpace(in.Payload.DraftStreamID)
	if draftID == "" || streamID == "" {
		return RouteResult{}, fmt.Errorf("conversation.draft.unwatch requires draft_id and draft_stream_id")
	}
	r.draftMu.Lock()
	if watchers := r.draftWatchers[draftID]; watchers != nil {
		delete(watchers, streamID)
		if len(watchers) == 0 {
			delete(r.draftWatchers, draftID)
		}
	}
	record := r.drafts[draftID]
	r.draftMu.Unlock()
	return RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: record.State, Cwd: record.Cwd}, nil
}

func (r *Router) HandleConversationDraftReopen(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	if in.Payload == nil || strings.TrimSpace(in.Payload.ConversationID) != "" {
		return RouteResult{}, fmt.Errorf("conversation.draft.reopen requires draft_id without conversation_id")
	}
	draftID := strings.TrimSpace(in.Payload.DraftID)
	r.draftMu.Lock()
	record, ok := r.drafts[draftID]
	r.draftMu.Unlock()
	if !ok || record.State != "draft_stale" {
		return RouteResult{DraftID: draftID}, fmt.Errorf("draft_reopen_requires_stale")
	}
	copyIn := in
	payload := *in.Payload
	payload.DraftID = ""
	payload.Cwd = firstNonEmpty(payload.Cwd, record.Cwd)
	payload.PluginID = record.PluginID
	copyIn.Payload = &payload
	return r.HandleConversationDraftOpen(ctx, copyIn)
}

func (r *Router) HandleConversationDraftControl(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	draftID, record, err := r.draftForInbound(in)
	result := RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: "open", Cwd: record.Cwd}
	if err != nil {
		return result, err
	}
	adapter := r.adapter(record.PluginID)
	controller, ok := adapter.(pluginruntime.DraftController)
	if !ok {
		return result, fmt.Errorf("%s does not implement controlDraft", record.PluginID)
	}
	controlled, err := controller.ControlDraft(ctx, pluginruntime.DraftControlRequest{
		DraftID: draftID, PluginID: record.PluginID, Action: strings.TrimSpace(in.Payload.Action),
		Target: in.Payload.Target, Name: in.Payload.Name,
		Metadata: map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID), "source_device": inboundSourceDevice(in)},
	})
	if err != nil {
		if strings.Contains(err.Error(), "draft_stale") {
			r.markDraftStale(draftID, err.Error())
			result.DraftState = "draft_stale"
		}
		return result, err
	}
	result.Cwd = firstNonEmpty(controlled.Cwd, record.Cwd)
	result.Controls = controlled.Controls
	r.draftMu.Lock()
	if current, ok := r.drafts[draftID]; ok && current.State == "open" {
		current.Cwd = result.Cwd
		current.Controls = controlled.Controls
		r.drafts[draftID] = current
	}
	r.draftMu.Unlock()
	return result, nil
}

func (r *Router) HandleConversationDraftStart(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	draftID, record, err := r.draftForInbound(in)
	result := RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: "open", Cwd: record.Cwd}
	if err != nil {
		return result, err
	}
	messageID := strings.TrimSpace(in.Payload.ClientMessageID)
	text := strings.TrimSpace(in.Payload.Text)
	attachmentInputs := in.Payload.Attachments
	if messageID == "" || (text == "" && len(attachmentInputs) == 0) {
		return result, fmt.Errorf("conversation.draft.start requires client_message_id and text or attachments")
	}
	if r.DB == nil {
		return result, fmt.Errorf("conversation.draft.start requires bridge db")
	}
	adapter := r.adapter(record.PluginID)
	starter, ok := adapter.(pluginruntime.DraftStarter)
	if !ok {
		return result, fmt.Errorf("%s does not implement startDraftWithMessage", record.PluginID)
	}
	r.startMu.Lock()
	defer r.startMu.Unlock()
	operation, err := r.DB.BeginStartOperation(ctx, record.PluginID, messageID, firstNonEmpty(in.Payload.RequestID, in.MessageID))
	if err != nil {
		return result, err
	}
	if operation.Status == "completed" {
		recordDB, findErr := r.DB.FindSession(ctx, operation.PrismConversationID, record.PluginID)
		if findErr != nil {
			return result, fmt.Errorf("load completed draft start binding: %w", findErr)
		}
		result.ConversationID = recordDB.PrismConversationID
		result.Session = nativeSessionFromRecord(recordDB)
		result.Visible = true
		result.Duplicate = true
		r.discardDraft(draftID)
		return result, nil
	}
	attachments, cleanup, err := r.materializeAttachments(ctx, attachmentInputs)
	if err != nil {
		return result, fmt.Errorf("materialize draft attachments: %w", err)
	}
	started, err := starter.StartDraftWithMessage(ctx, pluginruntime.StartDraftWithMessageRequest{
		DraftID: draftID, PluginID: record.PluginID, Cwd: record.Cwd,
		Message:      pluginruntime.InboundMessage{PrismMessageID: messageID, Text: text, Attachments: attachments, SourceDevice: inboundSourceDevice(in), Timestamp: inboundTimestamp(in), Metadata: map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID)}},
		SourceDevice: inboundSourceDevice(in), Metadata: map[string]string{"request_id": firstNonEmpty(in.Payload.RequestID, in.MessageID)},
	})
	if err != nil {
		cleanup()
		if strings.Contains(err.Error(), "draft_stale") {
			r.markDraftStale(draftID, err.Error())
			result.DraftState = "draft_stale"
		}
		return result, fmt.Errorf("start native draft: %w", err)
	}
	if strings.TrimSpace(started.Session.NativeSessionID) == "" && strings.TrimSpace(started.Session.NativeThreadID) == "" {
		return result, fmt.Errorf("start native draft returned no native identity")
	}
	if !started.Receipt.Accepted || !started.Receipt.Visible || !started.Visibility.Visible {
		return result, fmt.Errorf("start native draft did not verify visible first message")
	}
	started.Session.PluginID = firstNonEmpty(started.Session.PluginID, record.PluginID)
	recordDB, stored, err := r.DB.CompleteStartOperation(ctx, operation, bridgedb.SessionRecord{
		PluginID: record.PluginID, Cwd: started.Session.Cwd, NativeSessionID: started.Session.NativeSessionID, NativeThreadID: started.Session.NativeThreadID,
		Surface: started.Session.Surface, Endpoint: started.Session.Endpoint, VisibilityVerified: true, VisibilityEvidence: started.Visibility.Evidence,
	}, started.Receipt.NativeMessageID)
	if err != nil {
		return result, err
	}
	result.ConversationID = recordDB.PrismConversationID
	result.Session = nativeSessionFromRecord(stored)
	result.Receipt = started.Receipt
	result.Visible = true
	r.discardDraft(draftID)
	r.emitConversationIndex(record.PluginID)
	// Draft starts use the same asynchronous ACP prompt as an ordinary send.
	// Start tracking it here as well so its terminal state republished the
	// conversation index after Claude has persisted the native session.
	if waiter, ok := adapter.(pluginruntime.RunWaiter); ok && strings.TrimSpace(started.Receipt.NativeMessageID) != "" {
		requestID := firstNonEmpty(in.Payload.RequestID, in.MessageID, messageID)
		taskID := stableTaskID(firstNonEmpty(in.MessageID, messageID))
		if r.Runs != nil {
			r.Runs.Track(ForwardRun{
				RequestID: requestID, ConversationID: result.ConversationID, PluginID: record.PluginID,
				NativeSessionID: result.Session.NativeSessionID, NativeThreadID: result.Session.NativeThreadID,
				NativeRunID: started.Receipt.NativeMessageID, StartedAt: time.Now().UTC(),
			})
		}
		events, _ := adapter.Subscribe(context.WithoutCancel(ctx), result.Session)
		go r.waitForNativeRun(context.WithoutCancel(ctx), waiter, events, stored.ID, requestID, result.ConversationID, record.PluginID, taskID, inboundSourceDevice(in), result.Session, started.Receipt.NativeMessageID)
	}
	return result, nil
}

type RouteResult struct {
	TaskID         string
	ConversationID string
	PluginID       string
	Session        pluginruntime.NativeSession
	Receipt        pluginruntime.SendReceipt
	Duplicate      bool
	Visible        bool
	SessionReused  bool
	DraftID        string
	DraftState     string
	Cwd            string
	Controls       map[string]any
	MenuSession    *MenuSession
}

func (r *Router) materializeAttachments(ctx context.Context, input []string) ([]pluginruntime.Attachment, func(), error) {
	materializer := r.AttachmentMaterializer
	if materializer == nil {
		materializer = defaultAttachmentMaterializer()
	}
	return materializer.Materialize(ctx, input)
}

func attachmentSummary(attachments []pluginruntime.Attachment) string {
	if len(attachments) == 0 {
		return "附件消息"
	}
	if len(attachments) == 1 {
		return firstNonEmpty(strings.TrimSpace(attachments[0].Name), "附件消息")
	}
	return fmt.Sprintf("%d 个附件", len(attachments))
}

func nativeSessionFromRecord(record bridgedb.SessionRecord) pluginruntime.NativeSession {
	return pluginruntime.NativeSession{
		PluginID:        record.PluginID,
		NativeSessionID: record.NativeSessionID,
		NativeThreadID:  record.NativeThreadID,
		Surface:         record.Surface,
		Endpoint:        record.Endpoint,
		Cwd:             record.Cwd,
		Visible:         true,
	}
}

func (r *Router) HandleInbound(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	if in.MessageType == prismtransport.MsgTaskInterrupt {
		return r.HandleInterrupt(ctx, in)
	}
	if in.MessageType == prismtransport.MsgApprovalResolve {
		return r.HandleApprovalResolve(ctx, in)
	}
	if in.MessageType == prismtransport.MsgSessionControl {
		return r.HandleSessionControl(ctx, in)
	}
	taskID := stableTaskID(in.MessageID)
	conversationID := inboundConversationID(in)
	sourceDevice := inboundSourceDevice(in)
	pluginID, resolveErr := r.resolvePlugin(ctx, in)
	result := RouteResult{
		TaskID:         taskID,
		ConversationID: conversationID,
		PluginID:       pluginID,
	}
	if resolveErr != nil {
		return result, resolveErr
	}
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageReceived, "收到 Prism 消息。", sourceDevice)
	if r.DB != nil && strings.TrimSpace(in.MessageID) != "" {
		existing, err := r.DB.FindMessageReceipt(ctx, result.PluginID, in.MessageID)
		if err == nil {
			result.Duplicate = true
			result.PluginID = existing.PluginID
			result.Visible = existing.Visible
			result.Receipt = pluginruntime.SendReceipt{
				NativeMessageID: existing.NativeMessageID,
				Accepted:        existing.Status != StageFailed,
				Visible:         existing.Visible,
				Detail:          "duplicate prism message ignored",
			}
			r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageDuplicate, "这条 Prism 消息已经处理过，已跳过去重。", sourceDevice)
			return result, nil
		}
		if err != sql.ErrNoRows {
			return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("check message receipt: %w", err))
		}
	}
	text := strings.TrimSpace("")
	attachmentInputs := []string(nil)
	if in.Payload != nil {
		text = strings.TrimSpace(in.Payload.Text)
		attachmentInputs = in.Payload.Attachments
	}
	if text == "" && len(attachmentInputs) == 0 {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("empty Prism chat.send payload"))
	}
	if !r.remoteAccessEnabled(result.PluginID) {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("%s remote access disabled", result.PluginID))
	}
	adapter := r.adapter(result.PluginID)
	if adapter == nil {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("plugin adapter not found: %s", result.PluginID))
	}
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageResolving, "正在解析原生会话。", sourceDevice)
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("probe %s: %w", result.PluginID, err))
	}
	// Forward-sync gate. CanForwardSync must be explicitly declared; there is no
	// fallback to NativeVisibleInput (which describes UI visibility, not the
	// forward path). A plugin that wants to receive Prism messages must set it.
	if !capability.Available || !capability.CanForwardSync {
		reason := strings.TrimSpace(capability.UnavailableReason)
		if reason == "" {
			reason = "forward sync unavailable (capability CanForwardSync not declared)"
		}
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("%s unavailable: %s", result.PluginID, reason))
	}
	if supported, reason := remoteConversationSupportFromCapability(capability); !supported {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("%s unavailable: remote conversation unsupported (%s)", result.PluginID, reason))
	}
	if err := r.validateForwardSend(result.PluginID, conversationID); err != nil {
		return result, r.fail(ctx, result, in, sourceDevice, err)
	}
	if r.DB == nil {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("chat.send requires bridge db"))
	}
	sessionRecord, err := r.DB.FindSession(ctx, conversationID, result.PluginID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("conversation binding not found: conversation=%s plugin_id=%s", conversationID, result.PluginID))
		}
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("find conversation binding: %w", err))
	}
	attacher, ok := adapter.(pluginruntime.SessionAttacher)
	if !capability.CanAttachSession || !ok {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("%s unavailable: attachSession required for canonical conversation", result.PluginID))
	}
	session, err := attacher.AttachSession(ctx, pluginruntime.AttachSessionRequest{
		PrismConversationID: conversationID,
		PluginID:            result.PluginID,
		NativeSessionID:     sessionRecord.NativeSessionID,
		NativeThreadID:      sessionRecord.NativeThreadID,
		Cwd:                 sessionRecord.Cwd,
		SourceDevice:        sourceDevice,
		Metadata: map[string]string{
			"request_id": in.MessageID,
		},
	})
	if err != nil {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("attach bound native session: %w", err))
	}
	session.PluginID = firstNonEmpty(session.PluginID, result.PluginID)
	result.Session = session
	result.SessionReused = true
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageNativeSessionReady, "已连接已绑定的原生会话。", sourceDevice, true)
	attachments, cleanup, err := r.materializeAttachments(ctx, attachmentInputs)
	if err != nil {
		return result, r.fail(ctx, result, in, sourceDevice, fmt.Errorf("materialize attachments: %w", err))
	}
	receipt, err := adapter.Send(ctx, session, pluginruntime.InboundMessage{
		PrismMessageID: in.MessageID,
		Text:           text,
		Attachments:    attachments,
		SourceDevice:   sourceDevice,
		Timestamp:      inboundTimestamp(in),
		Metadata: map[string]string{
			"conversation_id": conversationID,
		},
	})
	if err != nil {
		cleanup()
		return result, r.failWithCode(ctx, result, in, sourceDevice, fmt.Errorf("send to native session: %w", err), sendFailureCode(err))
	}
	result.Receipt = receipt
	result.Visible = receipt.Visible
	result.Session = session
	receiptStatus := StageAccepted
	if receipt.PendingConfirmation {
		receiptStatus = StagePendingConfirmation
	}
	r.persistReceipt(ctx, in.MessageID, conversationID, result.PluginID, sessionRecord.ID, receipt.NativeMessageID, receiptStatus, receipt.Visible, "")
	if receipt.PendingConfirmation {
		// The native dialog is the only user-visible outcome until its watcher
		// confirms a choice. Do not insert a user message or start a run early.
		return result, nil
	}
	userEvent := pluginruntime.PluginEvent{
		ID:        userMessageID(in.MessageID),
		Type:      "message.user.accepted",
		Status:    StageAccepted,
		Summary:   summarizeForConversation(firstNonEmpty(text, attachmentSummary(attachments))),
		CreatedAt: time.Now().UTC(),
	}
	userPayload := buildConversationEventPayload(conversationEventContext{
		RequestID:       in.MessageID,
		ConversationID:  conversationID,
		PluginID:        result.PluginID,
		SourceDevice:    sourceDevice,
		RunID:           receipt.NativeMessageID,
		ConversationCwd: session.Cwd,
		Bound:           true,
		MessageRole:     "user",
		MessageText:     text,
		Session:         session,
	}, userEvent)
	r.insertEvent(ctx, sessionRecord.ID, conversationID, result.PluginID, userEvent.ID, userEvent.Type, userEvent.Status, userEvent.Summary, userPayload.Payload)
	r.insertEvent(ctx, sessionRecord.ID, conversationID, result.PluginID, receipt.NativeMessageID, "message.sent", StageAccepted, "Prism 消息已被 Plugin 接收。", map[string]any{
		"detail":  receipt.Detail,
		"visible": receipt.Visible,
	})
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageAccepted, "Plugin 已接收消息。", sourceDevice, true)
	r.emitConversationEvent(conversationEventContext{
		RequestID:       in.MessageID,
		ConversationID:  conversationID,
		PluginID:        result.PluginID,
		SourceDevice:    sourceDevice,
		RunID:           receipt.NativeMessageID,
		ConversationCwd: session.Cwd,
		Bound:           true,
		MessageRole:     "user",
		MessageText:     text,
		Session:         session,
	}, pluginruntime.PluginEvent{
		ID:        firstNonEmpty(receipt.NativeMessageID, in.MessageID),
		Type:      "message.user.accepted",
		Status:    StageAccepted,
		Summary:   "Plugin 已接收消息。",
		CreatedAt: inboundTimestamp(in),
		Payload: map[string]any{
			"role":    "user",
			"text":    text,
			"detail":  receipt.Detail,
			"visible": receipt.Visible,
		},
	})
	if receipt.Visible {
		if r.Runs != nil {
			r.Runs.Track(ForwardRun{
				RequestID:       in.MessageID,
				ConversationID:  conversationID,
				PluginID:        result.PluginID,
				NativeSessionID: session.NativeSessionID,
				NativeThreadID:  session.NativeThreadID,
				NativeRunID:     receipt.NativeMessageID,
				StartedAt:       time.Now().UTC(),
			})
		}
		r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageVisible, "消息已进入原生可见会话。", sourceDevice, true)
		r.emitConversationEvent(conversationEventContext{
			RequestID:       in.MessageID,
			ConversationID:  conversationID,
			PluginID:        result.PluginID,
			SourceDevice:    sourceDevice,
			RunID:           receipt.NativeMessageID,
			ConversationCwd: session.Cwd,
			Bound:           true,
			SuppressMessage: true,
			Session:         session,
		}, pluginruntime.PluginEvent{
			ID:        firstNonEmpty(receipt.NativeMessageID, in.MessageID) + ":visible",
			Type:      "run.visible",
			Status:    StageVisible,
			Summary:   "消息已进入原生可见会话。",
			CreatedAt: time.Now().UTC(),
		})
		r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageRunning, "原生应用正在处理，请在桌面端查看完整过程。", sourceDevice, true)
		r.emitConversationEvent(conversationEventContext{
			RequestID:       in.MessageID,
			ConversationID:  conversationID,
			PluginID:        result.PluginID,
			SourceDevice:    sourceDevice,
			RunID:           receipt.NativeMessageID,
			ConversationCwd: session.Cwd,
			Bound:           true,
			SuppressMessage: true,
			Session:         session,
		}, pluginruntime.PluginEvent{
			ID:        firstNonEmpty(receipt.NativeMessageID, in.MessageID) + ":running",
			Type:      "run.running",
			Status:    StageRunning,
			Summary:   "原生应用正在处理，请在桌面端查看完整过程。",
			CreatedAt: time.Now().UTC(),
		})
		// Only wait on the run when the adapter declares CanWaitRun. The interface
		// assertion is a second-line guard: an adapter that advertises the flag but
		// does not implement RunWaiter falls through to the chat.final branch rather
		// than triggering an "adapter does not implement waitForRun" error.
		waiter, waiterOK := adapter.(pluginruntime.RunWaiter)
		if capability.CanWaitRun && waiterOK && strings.TrimSpace(receipt.NativeMessageID) != "" {
			events, _ := adapter.Subscribe(context.WithoutCancel(ctx), session)
			go r.waitForNativeRun(context.WithoutCancel(ctx), waiter, events, sessionRecord.ID, in.MessageID, conversationID, result.PluginID, taskID, sourceDevice, session, receipt.NativeMessageID)
		} else {
			r.emitChatFinal(in.MessageID, conversationID, StageVisible, "消息已发送到 "+result.PluginID+" 的原生会话，桌面端会显示完整执行过程。")
		}
	}
	return result, nil
}

func (r *Router) validateForwardSend(pluginID, conversationID string) error {
	if r == nil || r.DetailSnapshots == nil {
		return nil
	}
	detail, ok := r.DetailSnapshots.GetForRuntime(pluginID, conversationID)
	if !ok {
		return nil
	}
	approval := asMap(detail["approval"])
	approvalRequestID := firstNonEmpty(
		stringFromAny(approval["approval_request_id"]),
		stringFromAny(approval["id"]),
	)
	primaryAction := canonicalPrimaryAction(firstNonEmpty(
		stringFromAny(detail["primary_action"]),
		stringFromAny(asMap(detail["run"])["primary_action"]),
	))
	if approvalRequestID != "" || primaryAction == "approval" {
		return fmt.Errorf("conversation is waiting for approval; resolve approval before sending")
	}
	// An empty native composer shows Stop while Codex is running. Whether a new
	// remote message can be queued is only knowable after the Plugin has filled
	// that text and re-read the native primary action. Do not reject it here.
	// The Plugin must return queue_unavailable rather than clicking Stop when the
	// freshly populated desktop composer does not become Submit.
	return nil
}

func (r *Router) requireForegroundWatcherAvailable(pluginID string) error {
	if r == nil || r.DetailSnapshots == nil || !r.DetailSnapshots.IsForegroundWatcherUnavailable(pluginID) {
		return nil
	}
	return fmt.Errorf("foreground watcher unavailable: plugin_id=%s", strings.TrimSpace(pluginID))
}

func controlRequiresForegroundWatcher(action string) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(action)), "queue.") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "model.switch", "model_switch", "reasoning.switch", "reasoning_switch", "permission.switch", "permission_switch", "approval.switch", "approval_switch", "interactive.surface.apply", "plan.set", "goal.set", "goal.edit", "goal.pause", "goal.resume", "goal.clear":
		return true
	default:
		return false
	}
}

// HandleConversationMenuSessionDescribe opens exactly one native menu through
// the Plugin. The resulting opaque capability is kept only in Hub memory and
// is never written to a detail projection.
func (r *Router) HandleConversationMenuSessionDescribe(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	conversationID := inboundConversationID(in)
	pluginID, err := r.resolvePlugin(ctx, in)
	result := RouteResult{ConversationID: conversationID, PluginID: pluginID}
	if err != nil {
		return result, err
	}
	if conversationID == "" {
		return result, fmt.Errorf("conversation.menu_session.describe requires conversation_id")
	}
	if !r.remoteAccessEnabled(pluginID) {
		return result, fmt.Errorf("%s remote access disabled", pluginID)
	}
	if err := r.requireForegroundWatcherAvailable(pluginID); err != nil {
		return result, err
	}
	owner, err := inboundMenuSessionOwner(in)
	if err != nil {
		return result, err
	}
	if err := r.validateConversationMenuOwner(pluginID, conversationID, owner); err != nil {
		return result, err
	}
	resolved, controller, err := r.resolveMenuSessionController(ctx, conversationID, pluginID)
	if err != nil {
		return result, err
	}
	result.Session = resolved.Session
	if err := r.selectConversationForMenuSession(ctx, controller, resolved.Session, pluginID, conversationID, in); err != nil {
		return result, err
	}
	controlled, err := controller.ControlSession(ctx, pluginruntime.SessionControlRequest{
		Session: resolved.Session,
		Action:  "menu_session.describe",
		Target:  map[string]any{"owner": owner},
		Metadata: map[string]string{
			"request_id":    in.MessageID,
			"source_device": inboundSourceDevice(in),
		},
	})
	if err != nil {
		return result, menuSessionControlError(err, false)
	}
	menuSession, err := r.menuSessionStore().Create(menuSessionCreateFromPluginResult(controlled.Details, pluginID, "conversation", conversationID, owner))
	if err != nil {
		return result, menuSessionControlError(err, false)
	}
	result.MenuSession = &menuSession
	return result, nil
}

// HandleConversationMenuSessionApply only accepts a session/entry pair issued
// by describe. It intentionally does not accept a control_id or option_id.
func (r *Router) HandleConversationMenuSessionApply(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	conversationID := inboundConversationID(in)
	pluginID, err := r.resolvePlugin(ctx, in)
	result := RouteResult{ConversationID: conversationID, PluginID: pluginID}
	if err != nil {
		return result, err
	}
	if conversationID == "" {
		return result, fmt.Errorf("conversation.menu_session.apply requires conversation_id")
	}
	if !r.remoteAccessEnabled(pluginID) {
		return result, fmt.Errorf("%s remote access disabled", pluginID)
	}
	if err := r.requireForegroundWatcherAvailable(pluginID); err != nil {
		return result, err
	}
	menuSessionID, entryID, err := inboundMenuSessionApplyTarget(in)
	if err != nil {
		return result, err
	}
	// describe selected this exact native conversation when necessary. The
	// detail watcher can lag that selection by one poll, so it must not reject
	// a valid immediate apply. The Plugin rechecks the actual selected native
	// thread before reopening the menu and is the final foreground authority.
	menuSession, err := r.menuSessionStore().Resolve(menuSessionID, pluginID, "conversation", conversationID, nil, true)
	if err != nil {
		return result, errMenuSessionStale
	}
	if !menuSessionHasAvailableEntry(menuSession, entryID) {
		return result, errMenuSessionStale
	}
	resolved, controller, err := r.resolveMenuSessionController(ctx, conversationID, pluginID)
	if err != nil {
		return result, err
	}
	result.Session = resolved.Session
	controlled, err := controller.ControlSession(ctx, pluginruntime.SessionControlRequest{
		Session: resolved.Session,
		Action:  "menu_session.apply",
		Target: map[string]any{
			"menu_session_id": menuSession.PluginMenuSessionID,
			"entry_id":        entryID,
		},
		Metadata: map[string]string{
			"request_id":    in.MessageID,
			"source_device": inboundSourceDevice(in),
		},
	})
	if err != nil {
		return result, menuSessionControlError(err, false)
	}
	result.MenuSession = &menuSession
	if len(controlled.Details) > 0 {
		result.Controls = controlled.Details
		r.emitConversationDetailFromLive(conversationID, pluginID, resolved.Session, controlled.Details)
	}
	return result, nil
}

func (r *Router) HandleConversationDraftMenuSessionDescribe(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	draftID, record, err := r.draftForInbound(in)
	result := RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: "open", Cwd: record.Cwd}
	if err != nil {
		return result, err
	}
	owner, err := inboundMenuSessionOwner(in)
	if err != nil {
		return result, draftMenuSessionError(err)
	}
	if err := validateDraftMenuOwner(record, owner); err != nil {
		return result, draftMenuSessionError(err)
	}
	adapter := r.adapter(record.PluginID)
	controller, ok := adapter.(pluginruntime.DraftController)
	if !ok {
		return result, fmt.Errorf("%s does not implement controlDraft", record.PluginID)
	}
	controlled, err := controller.ControlDraft(ctx, pluginruntime.DraftControlRequest{
		DraftID: draftID, PluginID: record.PluginID, Action: "menu_session.describe",
		Target:   map[string]any{"owner": owner},
		Metadata: map[string]string{"request_id": in.MessageID, "source_device": inboundSourceDevice(in)},
	})
	if err != nil {
		if isExplicitDraftStaleError(err) {
			r.markDraftStale(draftID, err.Error())
		}
		return result, draftMenuSessionError(err)
	}
	menuSession, err := r.menuSessionStore().Create(menuSessionCreateFromPluginResult(controlled.Details, record.PluginID, "draft", draftID, owner))
	if err != nil {
		return result, draftMenuSessionError(err)
	}
	result.MenuSession = &menuSession
	return result, nil
}

func (r *Router) HandleConversationDraftMenuSessionApply(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	draftID, record, err := r.draftForInbound(in)
	result := RouteResult{PluginID: record.PluginID, DraftID: draftID, DraftState: "open", Cwd: record.Cwd}
	if err != nil {
		return result, err
	}
	menuSessionID, entryID, err := inboundMenuSessionApplyTarget(in)
	if err != nil {
		return result, draftMenuSessionError(err)
	}
	menuSession, err := r.menuSessionStore().Resolve(menuSessionID, record.PluginID, "draft", draftID, nil, true)
	if err != nil || !menuSessionHasAvailableEntry(menuSession, entryID) {
		return result, draftMenuSessionError(errMenuSessionStale)
	}
	adapter := r.adapter(record.PluginID)
	controller, ok := adapter.(pluginruntime.DraftController)
	if !ok {
		return result, fmt.Errorf("%s does not implement controlDraft", record.PluginID)
	}
	controlled, err := controller.ControlDraft(ctx, pluginruntime.DraftControlRequest{
		DraftID: draftID, PluginID: record.PluginID, Action: "menu_session.apply",
		Target: map[string]any{
			"menu_session_id": menuSession.PluginMenuSessionID,
			"entry_id":        entryID,
		},
		Metadata: map[string]string{"request_id": in.MessageID, "source_device": inboundSourceDevice(in)},
	})
	if err != nil {
		if isExplicitDraftStaleError(err) {
			r.markDraftStale(draftID, err.Error())
		}
		return result, draftMenuSessionError(err)
	}
	result.Cwd = firstNonEmpty(controlled.Cwd, record.Cwd)
	result.Controls = controlled.Controls
	result.MenuSession = &menuSession
	r.draftMu.Lock()
	if current, ok := r.drafts[draftID]; ok && current.State == "open" {
		current.Cwd = result.Cwd
		current.Controls = controlled.Controls
		r.drafts[draftID] = current
	}
	r.draftMu.Unlock()
	return result, nil
}

func (r *Router) resolveMenuSessionController(ctx context.Context, conversationID, pluginID string) (resolvedConversation, pluginruntime.SessionController, error) {
	if r == nil || r.DB == nil {
		return resolvedConversation{}, nil, fmt.Errorf("menu session requires bridge db")
	}
	adapter := r.adapter(pluginID)
	if adapter == nil {
		return resolvedConversation{}, nil, fmt.Errorf("plugin adapter not found: %s", pluginID)
	}
	controller, ok := adapter.(pluginruntime.SessionController)
	if !ok {
		return resolvedConversation{}, nil, fmt.Errorf("plugin adapter does not implement session control: %s", pluginID)
	}
	query := &QueryService{DB: r.DB, Service: r.Service, DetailSnapshots: r.DetailSnapshots}
	resolved, err := query.resolveConversationForRuntime(ctx, conversationID, pluginID)
	if err != nil {
		return resolvedConversation{}, nil, fmt.Errorf("menu session target session not found: conversation=%s plugin_id=%s: %w", conversationID, pluginID, err)
	}
	resolved.Session.PluginID = firstNonEmpty(resolved.Session.PluginID, pluginID)
	resolved.Session.Visible = true
	if strings.TrimSpace(resolved.Session.NativeThreadID) == "" {
		resolved.Session.NativeThreadID = resolved.Session.NativeSessionID
	}
	if strings.TrimSpace(resolved.Session.NativeSessionID) == "" {
		resolved.Session.NativeSessionID = resolved.Session.NativeThreadID
	}
	if strings.TrimSpace(resolved.Session.NativeSessionID) == "" {
		return resolvedConversation{}, nil, fmt.Errorf("menu session target session has no native identity")
	}
	return resolved, controller, nil
}

func (r *Router) selectConversationForMenuSession(ctx context.Context, controller pluginruntime.SessionController, session pluginruntime.NativeSession, pluginID, conversationID string, in prismtransport.Inbound) error {
	if r != nil && r.DetailSnapshots != nil && r.DetailSnapshots.IsForegroundForRuntime(pluginID, conversationID) {
		return nil
	}
	selection, err := controller.ControlSession(ctx, pluginruntime.SessionControlRequest{
		Session:  session,
		Action:   "conversation.select",
		Metadata: map[string]string{"request_id": in.MessageID, "source_device": inboundSourceDevice(in)},
	})
	if err != nil {
		return fmt.Errorf("control_target_stale: cannot select target conversation: %w", err)
	}
	expectedThreadID := strings.TrimSpace(firstNonEmpty(session.NativeThreadID, session.NativeSessionID))
	actualThreadID := strings.TrimSpace(selection.ThreadID)
	if !selection.OK {
		reason := strings.TrimSpace(stringFromAny(asMap(selection.Details)["reason"]))
		if reason == "" {
			reason = "selection was not accepted"
		}
		return fmt.Errorf("control_target_stale: cannot select target conversation: %s", reason)
	}
	if actualThreadID == "" || actualThreadID != expectedThreadID {
		return fmt.Errorf("control_target_stale: selected thread mismatch: expected %s, got %s", expectedThreadID, actualThreadID)
	}
	if !hasTrueValue(selection.Details, "selected") {
		return fmt.Errorf("control_target_stale: Plugin did not confirm the target conversation is selected")
	}
	return nil
}

func (r *Router) validateConversationMenuOwner(pluginID, conversationID string, owner map[string]any) error {
	if r == nil || r.DetailSnapshots == nil {
		return fmt.Errorf("control_target_stale: current conversation detail unavailable")
	}
	detail, ok := r.DetailSnapshots.GetForRuntime(pluginID, conversationID)
	if !ok {
		return fmt.Errorf("control_target_stale: current conversation detail unavailable")
	}
	switch strings.TrimSpace(stringFromAny(owner["kind"])) {
	case "interactive_control":
		controlID := strings.TrimSpace(stringFromAny(owner["control_id"]))
		if controlID == "" {
			return fmt.Errorf("control_target_stale: interactive control owner requires control_id")
		}
		for _, control := range listOfMapsValue(detail["interactive_controls"]) {
			if strings.TrimSpace(stringFromAny(control["control_id"])) == controlID && hasTrueValue(control, "menu_available") {
				return nil
			}
		}
	case "conversation":
		if hasTrueValue(detail, "conversation_menu_available") {
			return nil
		}
	case "queue_item":
		queueItemID := strings.TrimSpace(stringFromAny(owner["queue_item_id"]))
		for _, item := range listOfMapsValue(asMap(detail["queue"])["items"]) {
			if strings.TrimSpace(stringFromAny(item["id"])) == queueItemID && hasTrueValue(item, "has_more_actions") {
				return nil
			}
		}
	}
	return fmt.Errorf("control_target_stale: menu owner is no longer declared by the current detail")
}

func validateDraftMenuOwner(record draftRecord, owner map[string]any) error {
	if strings.TrimSpace(stringFromAny(owner["kind"])) != "interactive_control" {
		return fmt.Errorf("draft_stale: unsupported draft menu owner")
	}
	controlID := strings.TrimSpace(stringFromAny(owner["control_id"]))
	for _, control := range listOfMapsValue(record.Controls["interactive_controls"]) {
		if strings.TrimSpace(stringFromAny(control["control_id"])) == controlID && hasTrueValue(control, "menu_available") {
			return nil
		}
	}
	return fmt.Errorf("draft_stale: menu owner is no longer declared by the active draft")
}

func (r *Router) menuSessionStore() *MenuSessionStore {
	if r == nil {
		return nil
	}
	r.menuSessionMu.Lock()
	defer r.menuSessionMu.Unlock()
	if r.MenuSessions == nil {
		r.MenuSessions = NewMenuSessionStore()
	}
	return r.MenuSessions
}

func inboundMenuSessionOwner(in prismtransport.Inbound) (map[string]any, error) {
	if in.Payload == nil || len(in.Payload.Owner) == 0 {
		return nil, fmt.Errorf("control_target_stale: menu session owner required")
	}
	owner := cloneMenuSessionOwner(in.Payload.Owner)
	if len(owner) == 0 || strings.TrimSpace(stringFromAny(owner["kind"])) == "" {
		return nil, fmt.Errorf("control_target_stale: menu session owner invalid")
	}
	return owner, nil
}

func inboundMenuSessionApplyTarget(in prismtransport.Inbound) (string, string, error) {
	if in.Payload == nil {
		return "", "", errMenuSessionStale
	}
	menuSessionID := strings.TrimSpace(in.Payload.MenuSessionID)
	entryID := strings.TrimSpace(in.Payload.EntryID)
	if menuSessionID == "" || entryID == "" {
		return "", "", errMenuSessionStale
	}
	return menuSessionID, entryID, nil
}

func menuSessionCreateFromPluginResult(details map[string]any, pluginID, subjectKind, subjectID string, owner map[string]any) MenuSessionCreate {
	entries := make([]MenuSessionEntry, 0)
	for _, item := range listOfMapsValue(details["entries"]) {
		entries = append(entries, MenuSessionEntry{
			EntryID:   strings.TrimSpace(stringFromAny(item["entry_id"])),
			Label:     strings.TrimSpace(stringFromAny(item["label"])),
			Available: !hasFalseValue(item, "available"),
			Current:   hasTrueValue(item, "current"),
		})
	}
	groups := make([]MenuSessionGroup, 0)
	for _, item := range listOfMapsValue(details["groups"]) {
		groupEntries := make([]MenuSessionEntry, 0)
		for _, entry := range listOfMapsValue(item["entries"]) {
			groupEntries = append(groupEntries, MenuSessionEntry{
				EntryID:   strings.TrimSpace(stringFromAny(entry["entry_id"])),
				Label:     strings.TrimSpace(stringFromAny(entry["label"])),
				Available: !hasFalseValue(entry, "available"),
				Current:   hasTrueValue(entry, "current"),
			})
		}
		groups = append(groups, MenuSessionGroup{
			GroupID: strings.TrimSpace(stringFromAny(item["group_id"])),
			Label:   strings.TrimSpace(stringFromAny(item["label"])),
			Entries: groupEntries,
		})
	}
	return MenuSessionCreate{
		PluginMenuSessionID: strings.TrimSpace(stringFromAny(details["menu_session_id"])),
		PluginID:            pluginID,
		SubjectKind:         subjectKind,
		SubjectID:           subjectID,
		Owner:               owner,
		Entries:             entries,
		Groups:              groups,
	}
}

func menuSessionHasAvailableEntry(session MenuSession, entryID string) bool {
	for _, entry := range session.Entries {
		if entry.EntryID == strings.TrimSpace(entryID) && entry.Available {
			return true
		}
	}
	return false
}

func hasTrueValue(values map[string]any, key string) bool {
	value, exists := values[key]
	return exists && firstNonEmptyBool(value)
}

func menuSessionControlError(err error, draft bool) error {
	if draft {
		return draftMenuSessionError(err)
	}
	if err == nil || errors.Is(err, errMenuSessionStale) || strings.Contains(strings.ToLower(err.Error()), "control_target_stale") {
		return errMenuSessionStale
	}
	return fmt.Errorf("control_target_stale: %w", err)
}

func draftMenuSessionError(err error) error {
	if err == nil || errors.Is(err, errMenuSessionStale) {
		return fmt.Errorf("draft_stale: menu session is no longer valid")
	}
	return err
}

func isExplicitDraftStaleError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "draft_stale")
}

func (r *Router) HandleInterrupt(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	conversationID := inboundConversationID(in)
	sourceDevice := inboundSourceDevice(in)
	taskID := strings.TrimSpace(inboundTaskID(in))
	if taskID == "" {
		taskID = stableTaskID(in.MessageID)
	}
	pluginID, resolveErr := r.resolvePlugin(ctx, in)
	result := RouteResult{
		TaskID:         taskID,
		ConversationID: conversationID,
		PluginID:       pluginID,
	}
	if resolveErr != nil {
		return result, resolveErr
	}
	if !r.remoteAccessEnabled(result.PluginID) {
		return result, fmt.Errorf("%s remote access disabled", result.PluginID)
	}
	if err := r.requireForegroundWatcherAvailable(result.PluginID); err != nil {
		return result, err
	}
	if r.DB == nil {
		return result, fmt.Errorf("interrupt requires bridge db")
	}
	sessionRecord, err := r.DB.FindSession(ctx, conversationID, result.PluginID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, fmt.Errorf("interrupt target session not found: conversation=%s plugin_id=%s", conversationID, result.PluginID)
		}
		return result, fmt.Errorf("find interrupt target session: %w", err)
	}
	adapter := r.adapter(result.PluginID)
	if adapter == nil {
		return result, fmt.Errorf("plugin adapter not found: %s", result.PluginID)
	}
	session := pluginruntime.NativeSession{
		PluginID:        result.PluginID,
		NativeSessionID: sessionRecord.NativeSessionID,
		NativeThreadID:  sessionRecord.NativeThreadID,
		Surface:         sessionRecord.Surface,
		Endpoint:        sessionRecord.Endpoint,
		Cwd:             sessionRecord.Cwd,
		Visible:         true,
	}
	result.Session = session
	reason := strings.TrimSpace(inboundInterruptReason(in))
	if err := adapter.Interrupt(ctx, session, taskID); err != nil {
		return result, fmt.Errorf("interrupt native session: %w", err)
	}
	r.insertEvent(ctx, sessionRecord.ID, conversationID, result.PluginID, taskID, "task.interrupt", "interrupted", firstNonEmpty(reason, "任务已中断。"), map[string]any{
		"source_device": sourceDevice,
		"task_id":       taskID,
		"reason":        reason,
	})
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, "interrupted", firstNonEmpty(reason, "任务已中断。"), sourceDevice)
	r.emitConversationEvent(conversationEventContext{
		RequestID:       in.MessageID,
		ConversationID:  conversationID,
		PluginID:        result.PluginID,
		SourceDevice:    sourceDevice,
		RunID:           taskID,
		ConversationCwd: session.Cwd,
		Bound:           true,
		SuppressMessage: true,
		Session:         session,
	}, pluginruntime.PluginEvent{
		ID:        taskID,
		Type:      "task.interrupt",
		Status:    "interrupted",
		Summary:   firstNonEmpty(reason, "任务已中断。"),
		CreatedAt: time.Now().UTC(),
		Payload: map[string]any{
			"reason": reason,
		},
	})
	return result, nil
}

func (r *Router) HandleApprovalResolve(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	conversationID := inboundConversationID(in)
	sourceDevice := inboundSourceDevice(in)
	taskID := strings.TrimSpace(inboundTaskID(in))
	if taskID == "" {
		taskID = stableTaskID(in.MessageID)
	}
	pluginID, resolveErr := r.resolvePlugin(ctx, in)
	result := RouteResult{
		TaskID:         taskID,
		ConversationID: conversationID,
		PluginID:       pluginID,
	}
	if resolveErr != nil {
		return result, resolveErr
	}
	if !r.remoteAccessEnabled(result.PluginID) {
		return result, fmt.Errorf("%s remote access disabled", result.PluginID)
	}
	if err := r.requireForegroundWatcherAvailable(result.PluginID); err != nil {
		return result, err
	}
	if r.DB == nil {
		return result, fmt.Errorf("approval resolve requires bridge db")
	}
	approvalRequestID := inboundApprovalRequestID(in)
	if approvalRequestID == "" {
		return result, fmt.Errorf("approval_request_id required")
	}
	actionID := inboundApprovalActionID(in)
	if actionID == "" {
		return result, fmt.Errorf("approval action_id required")
	}
	input := inboundApprovalInput(in)
	action, err := r.resolveApprovalAction(ctx, result.PluginID, conversationID, approvalRequestID, actionID, input)
	if err != nil {
		return result, err
	}
	sessionRecord, err := r.DB.FindSession(ctx, conversationID, result.PluginID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, fmt.Errorf("approval resolve target session not found: conversation=%s plugin_id=%s", conversationID, result.PluginID)
		}
		return result, fmt.Errorf("find approval resolve target session: %w", err)
	}
	adapter := r.adapter(result.PluginID)
	if adapter == nil {
		return result, fmt.Errorf("plugin adapter not found: %s", result.PluginID)
	}
	capability, err := adapter.Probe(ctx)
	if err != nil {
		return result, fmt.Errorf("probe %s: %w", result.PluginID, err)
	}
	if !capability.Available || !capability.CanApproval {
		reason := strings.TrimSpace(capability.UnavailableReason)
		if reason == "" {
			reason = "approval handling unavailable (capability CanApproval not declared)"
		}
		return result, fmt.Errorf("%s unavailable: %s", result.PluginID, reason)
	}
	resolver, ok := adapter.(pluginruntime.ApprovalResolver)
	if !ok {
		return result, fmt.Errorf("plugin adapter does not implement approval resolution: %s", result.PluginID)
	}
	session := pluginruntime.NativeSession{
		PluginID:        result.PluginID,
		NativeSessionID: sessionRecord.NativeSessionID,
		NativeThreadID:  sessionRecord.NativeThreadID,
		Surface:         sessionRecord.Surface,
		Endpoint:        sessionRecord.Endpoint,
		Cwd:             sessionRecord.Cwd,
		Visible:         true,
	}
	result.Session = session
	req := pluginruntime.ApprovalResolutionRequest{
		PrismConversationID: conversationID,
		PluginID:            result.PluginID,
		Session:             session,
		ApprovalRequestID:   approvalRequestID,
		ActionID:            actionID,
		Input:               input,
		SourceDevice:        sourceDevice,
		Metadata: map[string]string{
			"request_id": in.MessageID,
		},
	}
	if err := resolver.ResolveApproval(ctx, req); err != nil {
		return result, fmt.Errorf("resolve native approval: %w", err)
	}
	actionLabel := firstNonEmpty(stringFromAny(action["label"]), actionID)
	now := time.Now().UTC()
	summary := approvalResolutionSummary(actionLabel)
	payload := map[string]any{
		"accepted":            true,
		"request_id":          in.MessageID,
		"conversation_id":     conversationID,
		"plugin_id":           result.PluginID,
		"approval_request_id": approvalRequestID,
		"action_id":           actionID,
		"action_label":        actionLabel,
		"message":             "审批命令已提交到桌面端。",
		"source_device":       sourceDevice,
		"updated_at":          now.UnixMilli(),
	}
	if input != "" {
		payload["input"] = input
	}
	r.insertEvent(ctx, sessionRecord.ID, conversationID, result.PluginID, approvalRequestID, "approval.resolve", StageRunning, summary, payload)
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageRunning, summary, sourceDevice)
	r.emitConversationEvent(conversationEventContext{
		RequestID:       in.MessageID,
		ConversationID:  conversationID,
		PluginID:        result.PluginID,
		SourceDevice:    sourceDevice,
		RunID:           taskID,
		ConversationCwd: session.Cwd,
		Bound:           true,
		SuppressMessage: true,
		Session:         session,
	}, pluginruntime.PluginEvent{
		ID:        approvalRequestID,
		Type:      "approval.resolve",
		Status:    StageRunning,
		Summary:   summary,
		CreatedAt: now,
		Payload:   payload,
	})
	return result, nil
}

func (r *Router) HandleSessionControl(ctx context.Context, in prismtransport.Inbound) (RouteResult, error) {
	startedAt := time.Now()
	conversationID := inboundConversationID(in)
	sourceDevice := inboundSourceDevice(in)
	taskID := strings.TrimSpace(inboundTaskID(in))
	if taskID == "" {
		taskID = stableTaskID(in.MessageID)
	}
	pluginID, resolveErr := r.resolvePlugin(ctx, in)
	result := RouteResult{
		TaskID:         taskID,
		ConversationID: conversationID,
		PluginID:       pluginID,
	}
	if resolveErr != nil {
		return result, resolveErr
	}
	if !r.remoteAccessEnabled(result.PluginID) {
		return result, fmt.Errorf("%s remote access disabled", result.PluginID)
	}
	action := inboundControlAction(in)
	if action == "" {
		return result, fmt.Errorf("session control action required")
	}
	if controlRequiresForegroundWatcher(action) {
		if err := r.requireForegroundWatcherAvailable(result.PluginID); err != nil {
			return result, err
		}
	}
	if r.DB == nil {
		return result, fmt.Errorf("session control requires bridge db")
	}
	adapter := r.adapter(result.PluginID)
	if adapter == nil {
		return result, fmt.Errorf("plugin adapter not found: %s", result.PluginID)
	}
	controller, ok := adapter.(pluginruntime.SessionController)
	if !ok {
		return result, fmt.Errorf("plugin adapter does not implement session control: %s", result.PluginID)
	}
	query := &QueryService{
		DB:              r.DB,
		Service:         r.Service,
		DetailSnapshots: r.DetailSnapshots,
	}
	resolved, err := query.resolveConversationForRuntime(ctx, conversationID, result.PluginID)
	if err != nil {
		return result, fmt.Errorf("session control target session not found: conversation=%s plugin_id=%s: %w", conversationID, result.PluginID, err)
	}
	session := resolved.Session
	session.PluginID = firstNonEmpty(session.PluginID, result.PluginID)
	session.PluginID = firstNonEmpty(session.PluginID, result.PluginID)
	session.Visible = true
	if strings.TrimSpace(session.NativeThreadID) == "" {
		session.NativeThreadID = session.NativeSessionID
	}
	if strings.TrimSpace(session.NativeSessionID) == "" {
		session.NativeSessionID = session.NativeThreadID
	}
	if strings.TrimSpace(session.NativeSessionID) == "" {
		return result, fmt.Errorf("session control target session not found: conversation=%s plugin_id=%s", conversationID, result.PluginID)
	}
	result.Session = session
	if strings.EqualFold(action, "interactive.surface.apply") {
		if err := r.validateInteractiveSurfaceAction(result.PluginID, conversationID, inboundControlTarget(in)); err != nil {
			return result, err
		}
	}
	if err := r.validateGoalPlanControl(result.PluginID, conversationID, action, inboundControlTarget(in)); err != nil {
		return result, err
	}
	controlReq := pluginruntime.SessionControlRequest{
		Session:  session,
		Action:   action,
		Target:   inboundControlTarget(in),
		Name:     inboundControlName(in),
		Metadata: map[string]string{"request_id": in.MessageID, "source_device": sourceDevice},
	}
	if r.Logger != nil {
		r.Logger.Info(
			"session control request",
			"conversation_id", conversationID,
			"plugin_id", result.PluginID,
			"action", controlReq.Action,
			"target", controlReq.Target,
			"name", controlReq.Name,
			"native_session_id", session.NativeSessionID,
			"native_thread_id", session.NativeThreadID,
			"source_device", sourceDevice,
			"request_id", in.MessageID,
		)
	}
	controlResult, err := controller.ControlSession(ctx, controlReq)
	if err != nil {
		code := controlFailureCode(err)
		message := strings.TrimSpace(err.Error())
		if r.Logger != nil {
			r.Logger.Warn(
				"session control failed",
				"conversation_id", conversationID,
				"plugin_id", result.PluginID,
				"action", controlReq.Action,
				"target", controlReq.Target,
				"name", controlReq.Name,
				"native_session_id", session.NativeSessionID,
				"native_thread_id", session.NativeThreadID,
				"source_device", sourceDevice,
				"request_id", in.MessageID,
				"err", err,
			)
		}
		failurePayload := map[string]any{
			"request_id": in.MessageID,
			"action":     action,
			"code":       code,
			"message":    message,
		}
		r.insertEvent(ctx, sessionRecordID(resolved), conversationID, result.PluginID, taskID, "session.control", StageFailed, message, failurePayload)
		r.emitConversationEvent(conversationEventContext{
			RequestID:       in.MessageID,
			ConversationID:  conversationID,
			PluginID:        result.PluginID,
			SourceDevice:    sourceDevice,
			RunID:           taskID,
			Session:         session,
			SuppressMessage: true,
		}, pluginruntime.PluginEvent{
			ID:        in.MessageID,
			Type:      "conversation.control_failed",
			Status:    StageFailed,
			Summary:   message,
			Payload:   failurePayload,
			CreatedAt: time.Now(),
		})
		return result, fmt.Errorf("control native session: %w", err)
	}
	if r.Logger != nil {
		r.Logger.Info(
			"session control completed",
			"conversation_id", conversationID,
			"plugin_id", result.PluginID,
			"action", controlReq.Action,
			"target", controlReq.Target,
			"native_session_id", session.NativeSessionID,
			"native_thread_id", session.NativeThreadID,
			"source_device", sourceDevice,
			"request_id", in.MessageID,
			"cost_ms", time.Since(startedAt).Milliseconds(),
		)
	}
	if controlResult.DetailsConfirmed {
		if len(controlResult.Details) == 0 {
			if r.Logger != nil {
				r.Logger.Warn(
					"plugin returned details_confirmed without details",
					"conversation_id", conversationID,
					"plugin_id", result.PluginID,
					"action", controlReq.Action,
				)
			}
		} else {
			r.emitConversationDetailFromLive(
				conversationID,
				result.PluginID,
				session,
				controlResult.Details,
			)
		}
	}
	if strings.EqualFold(strings.TrimSpace(action), "delete") {
		if err := r.snapshotPublisher().PublishIndex(ctx, true, result.PluginID); err != nil && r.Logger != nil {
			r.Logger.Warn(
				"publish conversation index after session delete failed",
				"conversation_id", conversationID,
				"plugin_id", result.PluginID,
				"err", err,
			)
		}
	}
	summary := firstNonEmpty(strings.TrimSpace(controlResult.Message), "控制命令已提交到 Plugin。")
	payload := map[string]any{
		"action":        action,
		"target":        controlReq.Target,
		"name":          controlReq.Name,
		"source_device": sourceDevice,
		"task_id":       taskID,
		"details":       controlResult.Details,
	}
	sessionRecordID := ""
	if resolved.Record != nil {
		sessionRecordID = resolved.Record.ID
	}
	r.insertEvent(ctx, sessionRecordID, conversationID, result.PluginID, taskID, "session.control", StageCompleted, summary, payload)
	r.emitTaskEvent(result.PluginID, in.MessageID, conversationID, taskID, StageCompleted, summary, sourceDevice)
	return result, nil
}

func controlFailureCode(err error) string {
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "control_target_stale:") {
		return "control_target_stale"
	}
	return "control_failed"
}

func sessionRecordID(resolved resolvedConversation) string {
	if resolved.Record == nil {
		return ""
	}
	return resolved.Record.ID
}

func canonicalSessionFromReceipt(session pluginruntime.NativeSession, receipt pluginruntime.SendReceipt) pluginruntime.NativeSession {
	if id := strings.TrimSpace(receipt.CanonicalNativeSessionID); id != "" {
		session.NativeSessionID = id
	}
	if threadID := strings.TrimSpace(receipt.CanonicalNativeThreadID); threadID != "" {
		session.NativeThreadID = threadID
	}
	if strings.TrimSpace(session.NativeThreadID) == "" {
		session.NativeThreadID = session.NativeSessionID
	}
	return session
}

func (r *Router) waitForNativeRun(ctx context.Context, waiter pluginruntime.RunWaiter, events <-chan pluginruntime.PluginEvent, sessionID, requestID, conversationID, pluginID, taskID, sourceDevice string, session pluginruntime.NativeSession, runID string) {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	waitDone := make(chan struct {
		event pluginruntime.PluginEvent
		err   error
	}, 1)
	go func() {
		event, err := waiter.WaitForRun(waitCtx, session, runID)
		waitDone <- struct {
			event pluginruntime.PluginEvent
			err   error
		}{event: event, err: err}
	}()
	if events != nil {
		for {
			select {
			case event, ok := <-events:
				if !ok {
					events = nil
					continue
				}
				if r.handleNativeEvent(sessionID, requestID, conversationID, pluginID, taskID, sourceDevice, session, runID, event) {
					return
				}
			case result := <-waitDone:
				r.handleWaitResult(result.event, result.err, sessionID, requestID, conversationID, pluginID, taskID, sourceDevice, session, runID)
				return
			case <-waitCtx.Done():
				r.handleWaitResult(pluginruntime.PluginEvent{}, waitCtx.Err(), sessionID, requestID, conversationID, pluginID, taskID, sourceDevice, session, runID)
				return
			}
		}
	}
	result := <-waitDone
	r.handleWaitResult(result.event, result.err, sessionID, requestID, conversationID, pluginID, taskID, sourceDevice, session, runID)
}

func (r *Router) handleNativeEvent(sessionID, requestID, conversationID, pluginID, taskID, sourceDevice string, session pluginruntime.NativeSession, runID string, event pluginruntime.PluginEvent) bool {
	if code, watcherUnavailable := foregroundWatcherUnavailableCode(event); watcherUnavailable {
		r.emitForegroundWatcherUnavailable(pluginID, conversationID, code)
		return false
	}
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if status == "" {
		status = StageRunning
	}
	summary := strings.TrimSpace(event.Summary)
	if summary == "" {
		summary = "原生应用正在处理。"
	}
	r.insertEvent(context.Background(), sessionID, conversationID, pluginID, firstNonEmpty(event.ID, runID), event.Type, status, summary, event.Payload)
	r.emitConversationEvent(conversationEventContext{
		RequestID:      requestID,
		ConversationID: conversationID,
		PluginID:       pluginID,
		SourceDevice:   sourceDevice,
		RunID:          runID,
		Bound:          true,
		Session:        session,
	}, event)
	if event.Type == "subscription.error" {
		return false
	}
	if approval, ok := r.approvalFromPluginEvent(event, requestID, conversationID, pluginID, taskID, sourceDevice); ok {
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageWaitingApproval, approval.Summary, sourceDevice)
		r.emitApprovalRequired(approval)
		return false
	}
	switch status {
	case StageCompleted:
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageCompleted, summary, sourceDevice)
		return isTerminalCompletionEvent(event.Type)
	case StageFailed:
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageFailed, summary, sourceDevice)
		r.emitChatError(requestID, conversationID, summary)
		return true
	default:
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageRunning, summary, sourceDevice)
	}
	return false
}

func isTerminalCompletionEvent(eventType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(eventType))
	return !strings.HasPrefix(normalized, "desktop.") && strings.HasSuffix(normalized, ".completed")
}

func (r *Router) handleWaitResult(event pluginruntime.PluginEvent, err error, sessionID, requestID, conversationID, pluginID, taskID, sourceDevice string, session pluginruntime.NativeSession, runID string) {
	// Claude ACP persists a just-created session asynchronously. The initial
	// draft-start index publish can therefore run before listSessions sees it;
	// refresh once the native run settles so the Panel immediately gets the
	// canonical conversation instead of requiring a Hub restart.
	defer r.emitConversationIndex(pluginID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			summary := "未在等待窗口内收到原生应用完成事件，请在桌面端查看最新状态。"
			r.insertEvent(context.Background(), sessionID, conversationID, pluginID, runID, "run.wait_timeout", StageRunning, summary, map[string]any{
				"error": err.Error(),
			})
			r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageRunning, summary, sourceDevice)
			r.emitConversationEvent(conversationEventContext{
				RequestID:       requestID,
				ConversationID:  conversationID,
				PluginID:        pluginID,
				SourceDevice:    sourceDevice,
				RunID:           runID,
				Bound:           true,
				SuppressMessage: true,
				Session:         session,
			}, pluginruntime.PluginEvent{
				ID:        runID + ":timeout",
				Type:      "run.wait_timeout",
				Status:    StageRunning,
				Summary:   summary,
				CreatedAt: time.Now().UTC(),
				Payload: map[string]any{
					"error": err.Error(),
				},
			})
			r.emitChatFinal(requestID, conversationID, StageRunning, summary)
			if r.Runs != nil {
				r.Runs.Release(requestID)
			}
			return
		}
		r.insertEvent(context.Background(), sessionID, conversationID, pluginID, runID, "run.wait_error", StageFailed, err.Error(), map[string]any{
			"error": err.Error(),
		})
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageFailed, err.Error(), sourceDevice)
		r.emitConversationEvent(conversationEventContext{
			RequestID:       requestID,
			ConversationID:  conversationID,
			PluginID:        pluginID,
			SourceDevice:    sourceDevice,
			RunID:           runID,
			Bound:           true,
			SuppressMessage: true,
			Session:         session,
		}, pluginruntime.PluginEvent{
			ID:        runID,
			Type:      "run.failed",
			Status:    StageFailed,
			Summary:   err.Error(),
			CreatedAt: time.Now().UTC(),
			Payload: map[string]any{
				"error": err.Error(),
			},
		})
		r.emitChatError(requestID, conversationID, err.Error())
		if r.Runs != nil {
			r.Runs.MarkTerminal(requestID)
		}
		return
	}
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if status == "" {
		status = StageCompleted
	}
	summary := strings.TrimSpace(event.Summary)
	if summary == "" {
		summary = "原生应用已结束。"
	}
	if eventHasTimeoutFallback(event) {
		r.insertEvent(context.Background(), sessionID, conversationID, pluginID, runID, "run.wait_timeout", StageRunning, summary, event.Payload)
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageRunning, summary, sourceDevice)
		r.emitConversationEvent(conversationEventContext{
			RequestID:       requestID,
			ConversationID:  conversationID,
			PluginID:        pluginID,
			SourceDevice:    sourceDevice,
			RunID:           runID,
			Bound:           true,
			SuppressMessage: true,
			Session:         session,
		}, pluginruntime.PluginEvent{
			ID:        firstNonEmpty(event.ID, runID) + ":timeout-fallback",
			Type:      "run.wait_timeout",
			Status:    StageRunning,
			Summary:   summary,
			CreatedAt: time.Now().UTC(),
			Payload:   event.Payload,
		})
		r.emitChatFinal(requestID, conversationID, StageRunning, summary)
		if r.Runs != nil {
			r.Runs.Release(requestID)
		}
		return
	}
	r.insertEvent(context.Background(), sessionID, conversationID, pluginID, firstNonEmpty(event.ID, runID), event.Type, status, summary, event.Payload)
	if status == StageFailed {
		r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageFailed, summary, sourceDevice)
		r.emitConversationEvent(conversationEventContext{
			RequestID:      requestID,
			ConversationID: conversationID,
			PluginID:       pluginID,
			SourceDevice:   sourceDevice,
			RunID:          runID,
			Bound:          true,
			Session:        session,
		}, event)
		r.emitChatError(requestID, conversationID, summary)
		if r.Runs != nil {
			r.Runs.MarkTerminal(requestID)
		}
		return
	}
	r.emitTaskEvent(pluginID, requestID, conversationID, taskID, StageCompleted, summary, sourceDevice)
	r.emitConversationEvent(conversationEventContext{
		RequestID:      requestID,
		ConversationID: conversationID,
		PluginID:       pluginID,
		SourceDevice:   sourceDevice,
		RunID:          runID,
		Bound:          true,
		Session:        session,
	}, event)
	r.emitChatFinal(requestID, conversationID, StageCompleted, summary)
	if r.Runs != nil {
		r.Runs.MarkTerminal(requestID)
	}
}

func eventHasTimeoutFallback(event pluginruntime.PluginEvent) bool {
	if event.Payload == nil {
		return false
	}
	value, ok := event.Payload["timeout_fallback"]
	if !ok {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func (r *Router) fail(ctx context.Context, result RouteResult, in prismtransport.Inbound, sourceDevice string, err error) error {
	return r.failWithCode(ctx, result, in, sourceDevice, err, "send_failed")
}

func sendFailureCode(err error) string {
	var coded *pluginruntime.AdapterError
	if errors.As(err, &coded) {
		switch coded.Code {
		case "delivery_indeterminate", "workspace_trust_required", "session_start_timeout", "legacy_session_active":
			return coded.Code
		}
	}
	return "send_failed"
}

func (r *Router) failWithCode(ctx context.Context, result RouteResult, in prismtransport.Inbound, sourceDevice string, err error, code string) error {
	if err == nil {
		return nil
	}
	r.persistReceipt(ctx, in.MessageID, result.ConversationID, result.PluginID, "", "", StageFailed, false, err.Error())
	r.emitTaskEvent(result.PluginID, in.MessageID, result.ConversationID, result.TaskID, StageFailed, err.Error(), sourceDevice)
	r.emitConversationEvent(conversationEventContext{
		RequestID:       in.MessageID,
		ConversationID:  result.ConversationID,
		PluginID:        result.PluginID,
		SourceDevice:    sourceDevice,
		RunID:           result.TaskID,
		Bound:           true,
		SuppressMessage: true,
		Session:         result.Session,
	}, pluginruntime.PluginEvent{
		ID:        result.TaskID,
		Type:      "conversation.send_failed",
		Status:    StageFailed,
		Summary:   err.Error(),
		CreatedAt: time.Now().UTC(),
		Payload: map[string]any{
			"request_id": in.MessageID,
			"message_id": in.MessageID,
			"code":       code,
			"message":    err.Error(),
			"text":       inboundText(in),
		},
	})
	r.emitChatError(in.MessageID, result.ConversationID, err.Error())
	return err
}

func (r *Router) adapter(pluginID string) pluginruntime.PluginAdapter {
	if r == nil || r.Service == nil {
		return nil
	}
	return r.Service.Adapter(pluginID)
}

func (r *Router) CurrentConfig() *bridgeconfig.Config {
	if r == nil {
		return nil
	}
	if r.ConfigProvider != nil {
		if cfg := r.ConfigProvider(); cfg != nil {
			return cfg
		}
	}
	return r.Config
}

func (r *Router) remoteAccessEnabled(pluginID string) bool {
	return pluginruntime.RemoteEnabled(r.CurrentConfig(), pluginID)
}

func (r *Router) resolvePlugin(ctx context.Context, in prismtransport.Inbound) (string, error) {
	if r == nil || r.Service == nil {
		return "", fmt.Errorf("plugin service unavailable")
	}
	if in.Payload == nil {
		return "", fmt.Errorf("plugin_id required")
	}
	pluginID := strings.TrimSpace(in.Payload.PluginID)
	pluginID, err := RequirePlugin(ctx, r.Service, pluginID)
	if err != nil {
		return "", err
	}
	return pluginID, nil
}

func (r *Router) persistReceipt(ctx context.Context, prismMessageID, conversationID, pluginID, sessionID, nativeMessageID, status string, visible bool, lastErr string) {
	if r == nil || r.DB == nil || strings.TrimSpace(prismMessageID) == "" {
		return
	}
	_, err := r.DB.UpsertMessageReceipt(ctx, bridgedb.MessageReceiptRecord{
		PrismMessageID:      strings.TrimSpace(prismMessageID),
		PrismConversationID: conversationID,
		PluginID:            pluginID,
		SessionID:           sessionID,
		NativeMessageID:     nativeMessageID,
		Status:              status,
		Visible:             visible,
		LastError:           lastErr,
	})
	if err != nil && r.Logger != nil {
		r.Logger.Warn("persist bridge message receipt failed", "err", err)
	}
}

func (r *Router) insertEvent(ctx context.Context, sessionID, conversationID, pluginID, nativeEventID, eventType, status, summary string, payload map[string]any) {
	if r == nil || r.DB == nil {
		return
	}
	record := bridgedb.EventRecord{
		SessionID:           sessionID,
		PrismConversationID: conversationID,
		PluginID:            pluginID,
		NativeEventID:       nativeEventID,
		EventType:           eventType,
		Status:              status,
		Summary:             summary,
		Payload:             payload,
	}
	var err error
	if strings.HasPrefix(nativeEventID, "codex-task:") {
		digest := sha256.Sum256([]byte(sessionID + "\x00" + nativeEventID))
		record.ID = fmt.Sprintf("native_%x", digest[:16])
		_, err = r.DB.InsertNativeEventOnce(ctx, record)
	} else {
		_, err = r.DB.InsertEvent(ctx, record)
	}
	if err != nil && r.Logger != nil {
		r.Logger.Warn("persist bridge event failed", "err", err)
	}
}

func (r *Router) emitTaskEvent(pluginID, requestID, conversationID, taskID, status, summary, sourceDevice string, sessionReuse ...bool) {
	if r == nil || r.Sink == nil {
		return
	}
	pluginID, err := RequirePlugin(context.Background(), r.Service, pluginID)
	if err != nil || strings.TrimSpace(pluginID) == "" {
		if r.Logger != nil {
			r.Logger.Warn("task event dropped: plugin identity unavailable", "conversation_id", conversationID, "runtime", pluginID, "err", err)
		}
		return
	}
	if !r.remoteAccessEnabled(pluginID) {
		return
	}
	sessionReused := len(sessionReuse) > 0 && sessionReuse[0]
	_ = r.Sink.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgTaskEvent,
		Payload: prismtransport.TaskEventPayload{
			ConversationID: conversationID,
			PluginID:       pluginID,
			RequestID:      strings.TrimSpace(requestID),
			TaskID:         taskID,
			EventType:      status,
			Status:         status,
			Step:           status,
			Summary:        summary,
			SourceDevice:   sourceDevice,
			SessionReused:  sessionReused,
		},
	})
}

func (r *Router) emitConversationEvent(input conversationEventContext, event pluginruntime.PluginEvent) {
	if r == nil || r.Sink == nil || strings.TrimSpace(input.ConversationID) == "" {
		return
	}
	if strings.TrimSpace(input.PluginID) == "" {
		if r.Logger != nil {
			r.Logger.Warn("conversation event dropped: plugin identity unavailable", "conversation_id", input.ConversationID, "plugin_id", input.PluginID)
		}
		return
	}
	if !r.remoteAccessEnabled(input.PluginID) {
		return
	}
	input.Session.PluginID = firstNonEmpty(input.Session.PluginID, input.PluginID)
	payload := buildConversationEventPayload(input, event)
	if err := r.Sink.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgConversationEvent,
		Payload:     payload,
	}); err != nil && r.Logger != nil {
		r.Logger.Warn("conversation event delivery failed", "event_type", payload.EventType, "conversation_id", input.ConversationID, "plugin_id", input.PluginID, "err", err)
	}
	if details, ok := detailSnapshotPayloadFromPluginEvent(event); ok {
		r.emitConversationDetailFromLive(input.ConversationID, input.PluginID, input.Session, details)
	}
}

func (r *Router) emitConversationIndex(pluginIDs ...string) {
	publisher := r.snapshotPublisher()
	if publisher == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := publisher.PublishIndex(ctx, false, pluginIDs...); err != nil {
			publisher.warn("publish conversation index snapshot failed", "err", err)
		}
	}()
}

func (r *Router) emitConversationDetailFromLive(conversationID, pluginID string, session pluginruntime.NativeSession, details map[string]any) bool {
	publisher := r.snapshotPublisher()
	if publisher == nil || len(details) == 0 {
		return false
	}
	conversationID = strings.TrimSpace(conversationID)
	pluginID = strings.TrimSpace(pluginID)
	if conversationID == "" || pluginID == "" {
		return false
	}
	if !r.remoteAccessEnabled(pluginID) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := publisher.PublishLiveDetail(ctx, conversationID, pluginID, session, "", details); err != nil {
		publisher.warn("publish live conversation detail failed", "conversation_id", conversationID, "plugin_id", pluginID, "err", err)
		return false
	}
	return true
}

func (r *Router) emitForegroundWatcherUnavailable(pluginID, conversationID, code string) {
	publisher := r.snapshotPublisher()
	if publisher == nil {
		return
	}
	if err := publisher.PublishDetailUnavailable(pluginID, conversationID, code); err != nil {
		publisher.warn("publish foreground watcher unavailable failed", "conversation_id", conversationID, "plugin_id", pluginID, "err", err)
	}
}

func (r *Router) snapshotPublisher() *SnapshotPublisher {
	if r == nil {
		return nil
	}
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	if r.SnapshotPublisher == nil {
		r.SnapshotPublisher = NewSnapshotPublisher(
			r.Service,
			r.DB,
			r.Config,
			r.ConfigProvider,
			r.Sink,
			r.DetailSnapshots,
			r.Logger,
		)
	}
	return r.SnapshotPublisher
}

func (r *Router) emitChatFinal(requestID, conversationID, status, content string) {
	if r == nil || r.Sink == nil {
		return
	}
	_ = r.Sink.Send(prismtransport.Outbound{
		MessageType: prismtransport.MsgChatFinal,
		Payload: &prismtransport.OutboundPayload{
			RequestID:      strings.TrimSpace(requestID),
			Status:         status,
			Step:           status,
			Finalized:      true,
			ConversationID: conversationID,
			Message: &prismtransport.OutboundChatMessage{
				Type:    "summary",
				Content: content,
			},
		},
	})
}

func (r *Router) emitChatError(requestID, conversationID, content string) {
	if r == nil || r.Sink == nil {
		return
	}
	_ = r.Sink.Send(prismtransport.Outbound{
		MessageType: prismtransport.MsgChatError,
		Payload: &prismtransport.OutboundPayload{
			RequestID:      strings.TrimSpace(requestID),
			Status:         StageFailed,
			Step:           StageFailed,
			Finalized:      true,
			ConversationID: conversationID,
			Message: &prismtransport.OutboundChatMessage{
				Type:    "error",
				Content: content,
			},
		},
	})
}

func (r *Router) emitApprovalRequired(payload prismtransport.ApprovalRequiredPayload) {
	if r == nil || r.Sink == nil {
		return
	}
	_ = r.Sink.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgApprovalRequired,
		Payload:     payload,
	})
}

func stableTaskID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return "bridge-task-" + uuid.NewString()
	}
	return "bridge-task-" + messageID
}

func defaultConversationTitle(text string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) > 48 {
		return string(runes[:48]) + "..."
	}
	return text
}

func summarizeForConversation(text string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) > 120 {
		return string(runes[:120]) + "..."
	}
	return text
}

func readySummaryForReuse(reused bool) string {
	if reused {
		return "已复用现有原生会话，继续当前对话。"
	}
	return "已经进入新的原生会话。"
}

func userMessageID(requestID string) string {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return ""
	}
	return "user:" + requestID
}

func assistantMessageID(requestID, runID, fallback string) string {
	if value := strings.TrimSpace(requestID); value != "" {
		return "assistant:" + value
	}
	if value := strings.TrimSpace(runID); value != "" {
		return "assistant:" + value
	}
	if value := strings.TrimSpace(fallback); value != "" {
		return "assistant:" + value
	}
	return ""
}

func assistantRunIDFromEvent(event pluginruntime.PluginEvent) string {
	if event.Payload == nil {
		return ""
	}
	for _, key := range []string{"runId", "run_id", "turn_id", "turnId", "messageId", "message_id", "itemId", "item_id", "thread_id", "threadId"} {
		if value := strings.TrimSpace(fmt.Sprint(event.Payload[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func eventTimeOrNow(event pluginruntime.PluginEvent) time.Time {
	if !event.CreatedAt.IsZero() {
		return event.CreatedAt.UTC()
	}
	return time.Now().UTC()
}

func assistantTextFromEvent(event pluginruntime.PluginEvent) string {
	if event.Payload != nil {
		if text := flattenEventText(event.Payload["text"]); text != "" {
			return text
		}
		if delta := flattenEventText(event.Payload["delta"]); delta != "" {
			return delta
		}
		if message := flattenEventText(event.Payload["message"]); message != "" {
			return message
		}
		if content := flattenEventText(event.Payload["content"]); content != "" {
			return content
		}
		if item := flattenEventText(event.Payload["item"]); item != "" {
			return item
		}
	}
	return strings.TrimSpace(event.Summary)
}

func flattenEventText(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := flattenEventText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	case map[string]any:
		keys := []string{"text", "content", "message", "body", "delta"}
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			if text := flattenEventText(v[key]); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) > 0 {
			return strings.TrimSpace(strings.Join(parts, "\n"))
		}
		mapKeys := make([]string, 0, len(v))
		for key := range v {
			mapKeys = append(mapKeys, key)
		}
		sort.Strings(mapKeys)
		for _, key := range mapKeys {
			if text := flattenEventText(v[key]); text != "" {
				return text
			}
		}
	}
	return ""
}

func inboundConversationID(in prismtransport.Inbound) string {
	if in.Payload != nil {
		if conv := strings.TrimSpace(in.Payload.ConversationID); conv != "" {
			return conv
		}
	}
	if messageID := strings.TrimSpace(in.MessageID); messageID != "" {
		return "prism:" + messageID
	}
	return "prism:" + uuid.NewString()
}

func inboundText(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.Text)
}

func inboundCwd(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.Cwd)
}

func resolveInboundCwd(in prismtransport.Inbound) (string, error) {
	if cwd := inboundCwd(in); cwd != "" {
		return cwd, nil
	}
	return "", nil
}

func inboundSourceDevice(in prismtransport.Inbound) string {
	if in.Payload != nil && in.Payload.Metadata != nil {
		if source := strings.ToLower(strings.TrimSpace(in.Payload.Metadata.SourceDevice)); source != "" {
			return source
		}
	}
	return "prism"
}

func inboundTimestamp(in prismtransport.Inbound) time.Time {
	if in.Timestamp > 0 {
		return time.UnixMilli(in.Timestamp).UTC()
	}
	return time.Now().UTC()
}

func inboundTaskID(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.TaskID)
}

func inboundInterruptReason(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	if reason := strings.TrimSpace(in.Payload.Reason); reason != "" {
		return reason
	}
	if in.Payload.Metadata != nil {
		if reason := strings.TrimSpace(in.Payload.Metadata.RequestKind); reason != "" {
			return reason
		}
	}
	return ""
}

func inboundApprovalRequestID(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.ApprovalRequestID)
}

func inboundApprovalActionID(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.ActionID)
}

func inboundApprovalInput(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.Input)
}

var errCurrentApprovalDetailUnavailable = errors.New("current conversation detail snapshot unavailable")

func (r *Router) resolveApprovalAction(ctx context.Context, pluginID, conversationID, approvalRequestID, actionID, input string) (map[string]any, error) {
	action, err := r.validateApprovalAction(pluginID, conversationID, approvalRequestID, actionID, input)
	if err == nil || !errors.Is(err, errCurrentApprovalDetailUnavailable) {
		return action, err
	}
	query := &QueryService{DB: r.DB, Service: r.Service, DetailSnapshots: r.DetailSnapshots}
	detail, refreshErr := query.getConversationDetail(ctx, conversationID, pluginID)
	if refreshErr != nil {
		return nil, fmt.Errorf("refresh current approval detail: %w", refreshErr)
	}
	return validateApprovalActionDetail(detail, approvalRequestID, actionID, input)
}

func (r *Router) validateApprovalAction(pluginID, conversationID, approvalRequestID, actionID, input string) (map[string]any, error) {
	if r == nil || r.DetailSnapshots == nil {
		return nil, errCurrentApprovalDetailUnavailable
	}
	detail, ok := r.DetailSnapshots.GetForRuntime(pluginID, conversationID)
	if !ok {
		return nil, fmt.Errorf("%w: conversation=%s plugin_id=%s", errCurrentApprovalDetailUnavailable, conversationID, pluginID)
	}
	return validateApprovalActionDetail(detail, approvalRequestID, actionID, input)
}

func validateApprovalActionDetail(detail map[string]any, approvalRequestID, actionID, input string) (map[string]any, error) {
	approval := asMap(detail["approval"])
	currentRequestID := strings.TrimSpace(stringFromAny(approval["approval_request_id"]))
	if currentRequestID == "" {
		return nil, fmt.Errorf("conversation is not waiting for approval")
	}
	if currentRequestID != approvalRequestID {
		return nil, fmt.Errorf("stale approval request: current=%s requested=%s", currentRequestID, approvalRequestID)
	}
	for _, action := range listOfMapsValue(approval["actions"]) {
		if strings.TrimSpace(stringFromAny(action["id"])) != actionID {
			continue
		}
		if hasFalseValue(action, "available") {
			return nil, fmt.Errorf("approval action unavailable: %s", actionID)
		}
		requiresInput := firstNonEmptyBool(action["requires_input"])
		if requiresInput && input == "" {
			return nil, fmt.Errorf("approval action input required: %s", actionID)
		}
		acceptsInput := requiresInput || firstNonEmptyBool(action["accepts_input"])
		if !acceptsInput && input != "" {
			return nil, fmt.Errorf("approval action does not accept input: %s", actionID)
		}
		return action, nil
	}
	return nil, fmt.Errorf("approval action is not present in current detail snapshot: %s", actionID)
}

func (r *Router) validateInteractiveSurfaceAction(pluginID, conversationID string, target any) error {
	if r == nil || r.DetailSnapshots == nil {
		return fmt.Errorf("control_target_stale: current interaction surface unavailable")
	}
	detail, ok := r.DetailSnapshots.GetForRuntime(pluginID, conversationID)
	if !ok || !firstNonEmptyBool(detail["desktop_foreground"]) {
		return fmt.Errorf("control_target_stale: interaction surface is not on the current desktop conversation")
	}
	surface := asMap(detail["interactive_surface"])
	requested := asMap(target)
	surfaceID := strings.TrimSpace(stringFromAny(requested["surface_id"]))
	actionID := strings.TrimSpace(stringFromAny(requested["action_id"]))
	input := strings.TrimSpace(stringFromAny(requested["input"]))
	if surfaceID == "" || actionID == "" || surfaceID != strings.TrimSpace(stringFromAny(surface["surface_id"])) {
		return fmt.Errorf("control_target_stale: interaction surface changed")
	}
	for _, action := range listOfMapsValue(surface["actions"]) {
		if strings.TrimSpace(stringFromAny(action["action_id"])) != actionID || hasFalseValue(action, "available") {
			continue
		}
		requiresInput := firstNonEmptyBool(action["requires_input"])
		if requiresInput && input == "" {
			return fmt.Errorf("control_target_stale: interaction action input required")
		}
		acceptsInput := requiresInput || firstNonEmptyBool(action["accepts_input"])
		if !acceptsInput && input != "" {
			return fmt.Errorf("control_target_stale: interaction action does not accept input")
		}
		return nil
	}
	return fmt.Errorf("control_target_stale: interaction action changed")
}

func (r *Router) validateGoalPlanControl(pluginID, conversationID, action string, target any) error {
	action = strings.ToLower(strings.TrimSpace(action))
	section := ""
	switch action {
	case "plan.set":
		section = "plan_mode"
	case "goal.set", "goal.edit", "goal.pause", "goal.resume", "goal.clear":
		section = "goal"
	default:
		return nil
	}
	if r == nil || r.DetailSnapshots == nil {
		return fmt.Errorf("control_target_stale: current %s detail unavailable", section)
	}
	detail, ok := r.DetailSnapshots.GetForRuntime(pluginID, conversationID)
	if !ok {
		return fmt.Errorf("control_target_stale: current %s detail unavailable", section)
	}
	state := asMap(detail[section])
	for _, declared := range listOfMapsValue(state["actions"]) {
		if strings.ToLower(strings.TrimSpace(stringFromAny(declared["id"]))) != action || hasFalseValue(declared, "available") {
			continue
		}
		requestedTarget := asMap(target)
		switch action {
		case "plan.set":
			declaredTarget := asMap(declared["target"])
			declaredEnabled, declaredOK := declaredTarget["enabled"].(bool)
			requestedEnabled, requestedOK := requestedTarget["enabled"].(bool)
			if !declaredOK || !requestedOK || len(requestedTarget) != 1 || declaredEnabled != requestedEnabled {
				return fmt.Errorf("control_target_stale: plan target changed")
			}
		case "goal.set", "goal.edit":
			objective, objectiveOK := requestedTarget["objective"].(string)
			objective = strings.TrimSpace(objective)
			if !objectiveOK || objective == "" || len([]rune(objective)) > 8000 || len(requestedTarget) != 1 {
				return fmt.Errorf("control_target_stale: goal objective target invalid")
			}
		default:
			if len(requestedTarget) != 0 {
				return fmt.Errorf("control_target_stale: goal lifecycle action does not accept target")
			}
		}
		return nil
	}
	return fmt.Errorf("control_target_stale: %s action is no longer available", action)
}

func inboundControlAction(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.Action)
}

func inboundControlTarget(in prismtransport.Inbound) any {
	if in.Payload == nil {
		return nil
	}
	return in.Payload.Target
}

func inboundControlName(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.Name)
}

func approvalResolutionSummary(actionLabel string) string {
	return fmt.Sprintf("审批动作“%s”已提交到 Plugin，等待桌面状态同步。", strings.TrimSpace(actionLabel))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (r *Router) approvalFromPluginEvent(event pluginruntime.PluginEvent, requestID, conversationID, pluginID, taskID, sourceDevice string) (prismtransport.ApprovalRequiredPayload, bool) {
	if r == nil {
		return prismtransport.ApprovalRequiredPayload{}, false
	}
	pluginID, err := RequirePlugin(context.Background(), r.Service, pluginID)
	if err != nil || strings.TrimSpace(pluginID) == "" {
		if r != nil && r.Logger != nil {
			r.Logger.Warn("approval event dropped: plugin identity unavailable", "conversation_id", conversationID, "runtime", pluginID, "err", err)
		}
		return prismtransport.ApprovalRequiredPayload{}, false
	}
	method := strings.TrimSpace(approvalStringFromAny(event.Payload["method"]))
	eventType := strings.ToLower(strings.TrimSpace(event.Type))
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if eventType != "approval.required" && status != StageWaitingApproval {
		if method == "" {
			return prismtransport.ApprovalRequiredPayload{}, false
		}
		switch method {
		case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		default:
			return prismtransport.ApprovalRequiredPayload{}, false
		}
	}
	summary := strings.TrimSpace(event.Summary)
	if summary == "" {
		summary = "当前任务正在等待桌面端确认。"
	}
	approvalID := firstNonEmpty(
		approvalStringFromAny(event.Payload["approval_request_id"]),
		approvalStringFromAny(event.Payload["itemId"]),
		approvalStringFromAny(event.Payload["turn_id"]),
		event.ID,
	)
	approvalKind := firstNonEmpty(
		approvalStringFromAny(event.Payload["approval_kind"]),
		approvalKindFromMethod(method),
	)
	actions := canonicalApprovalActions(event.Payload["actions"])
	input := asMap(event.Payload["input"])
	metadata := map[string]any{
		"approval_kind": approvalKind,
	}
	if method != "" {
		metadata["method"] = method
	}
	if item, ok := event.Payload["item"]; ok && item != nil {
		metadata["item"] = sanitizePublicPayloadValue(item)
	}
	return prismtransport.ApprovalRequiredPayload{
		ConversationID:    conversationID,
		PluginID:          pluginID,
		RequestID:         strings.TrimSpace(requestID),
		TaskID:            strings.TrimSpace(taskID),
		ApprovalRequestID: approvalID,
		Title:             strings.TrimSpace(approvalStringFromAny(event.Payload["title"])),
		Summary:           summary,
		Description:       strings.TrimSpace(approvalStringFromAny(event.Payload["description"])),
		Actions:           actions,
		Input:             input,
		CreatedAt:         int64Value(event.Payload["created_at"]),
		SourceDevice:      sourceDevice,
		Metadata:          metadata,
	}, true
}

func approvalKindFromMethod(method string) string {
	switch strings.TrimSpace(method) {
	case "item/commandExecution/requestApproval":
		return "command_execution"
	case "item/fileChange/requestApproval":
		return "file_change"
	default:
		return "unknown"
	}
}

func approvalStringFromAny(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
