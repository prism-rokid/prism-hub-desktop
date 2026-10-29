package bridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

const (
	reverseSyncPollInterval        = 15 * time.Second
	reverseSyncMaxTargetsPerPlugin = 128
)

type confirmedControlSink interface {
	SendControlConfirmed(context.Context, prismtransport.ControlOutbound) error
}

type acknowledgedControlSink interface {
	SendControlAcknowledged(context.Context, prismtransport.ControlOutbound) error
}

type ReverseSyncer struct {
	Service           *pluginruntime.Service
	Config            *bridgeconfig.Config
	ConfigProvider    func() *bridgeconfig.Config
	DB                *bridgedb.DB
	Sink              PrismSink
	Logger            *slog.Logger
	Every             time.Duration
	Runs              *RunTracker
	DetailSnapshots   *DetailSnapshotStore
	SnapshotPublisher *SnapshotPublisher
	Drafts            *Router
	// MaxTargetsPerPlugin 限制不能提供插件级监听器的适配器所创建的会话订阅数。
	// 值为零时使用默认上限。
	MaxTargetsPerPlugin int

	mu          sync.Mutex
	active      map[string]*reverseSyncHandle
	publisherMu sync.Mutex
}

type reverseSyncHandle struct {
	cancel  context.CancelFunc
	adapter pluginruntime.PluginAdapter
}

func (s *ReverseSyncer) Run(ctx context.Context) {
	if s == nil || s.Service == nil || s.DB == nil {
		return
	}
	interval := s.Every
	if interval <= 0 {
		interval = reverseSyncPollInterval
	}
	s.sync(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer s.stopAll()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sync(ctx)
		}
	}
}

func (s *ReverseSyncer) TriggerSync(ctx context.Context) {
	if s == nil || s.Service == nil || s.DB == nil {
		return
	}
	s.sync(ctx)
}

func (s *ReverseSyncer) sync(ctx context.Context) {
	defer s.publishCatalog(ctx)
	defer s.refreshIndexes(ctx)
	cfg := s.currentConfig()
	desired := map[string]reverseSyncTarget{}
	for _, pluginID := range s.Service.AdapterIDs() {
		adapter := s.Service.Adapter(pluginID)
		if adapter == nil {
			continue
		}
		if !pluginruntime.RemoteEnabled(cfg, pluginID) {
			continue
		}
		mode := pluginruntime.ReverseSyncMode(cfg, pluginID)
		if mode == pluginruntime.ReverseSyncOff {
			continue
		}
		capability, err := adapter.Probe(ctx)
		if err != nil {
			s.warn("reverse sync probe failed", "plugin_id", pluginID, "err", err)
			continue
		}
		// A desktop-automation plugin may have built its conversation index
		// while its live-control surface was unavailable (for example before a
		// managed launch). Rebuild the complete index once the plugin becomes
		// available again so remote clients stop seeing a stale unavailable
		// agent and receive the current sessions.
		if publisher := s.snapshotPublisher(); publisher != nil {
			if err := publisher.RebuildConversationIndexIfStale(ctx, pluginID, capability.Available); err != nil {
				s.warn("refresh conversation index failed", "plugin_id", pluginID, "err", err)
			}
		}
		if !capability.Available || !capability.CanReverseSync {
			continue
		}
		// A plugin that declares reverse sync AND can list real native sessions
		// should surface those native sessions to mobile clients, not only the
		// Prism-managed ones. Upgrade the default PrismManagedOnly mode to
		// AllThreads so targets() pulls every native session via ListSessions.
		if mode == pluginruntime.ReverseSyncPrismManagedOnly && capability.CanListSessions {
			mode = pluginruntime.ReverseSyncAllThreads
		}
		if capability.CanPluginWideWatch {
			if _, ok := adapter.(pluginruntime.PluginWideSubscriber); ok {
				// A plugin-wide watcher observes sessions which may not have been
				// present when the Hub connected. Initialize its directory from a
				// complete ListSessions snapshot before accepting its hints.
				if publisher := s.snapshotPublisher(); publisher != nil {
					if err := publisher.EnsureConversationIndexes(ctx, pluginID); err != nil {
						s.warn("initialize plugin-wide conversation index failed", "plugin_id", pluginID, "err", err)
					}
				}
				target := reverseSyncTarget{
					pluginWide: true,
					session:    pluginruntime.NativeSession{PluginID: pluginID},
				}
				desired[target.key()] = target
				continue
			}
			s.warn("plugin declares wide watcher without runtime support", "plugin_id", pluginID)
		}
		for _, target := range s.targets(ctx, pluginID, adapter, mode) {
			if target.key() != "" {
				desired[target.key()] = target
			}
		}
	}
	s.applyDesired(ctx, desired)
}

func (s *ReverseSyncer) currentConfig() *bridgeconfig.Config {
	if s == nil {
		return nil
	}
	if s.ConfigProvider != nil {
		if cfg := s.ConfigProvider(); cfg != nil {
			return cfg
		}
	}
	return s.Config
}

func (s *ReverseSyncer) targets(ctx context.Context, pluginID string, adapter pluginruntime.PluginAdapter, mode string) []reverseSyncTarget {
	seen := map[string]bool{}
	var out []reverseSyncTarget
	maxTargets := s.MaxTargetsPerPlugin
	if maxTargets <= 0 {
		maxTargets = reverseSyncMaxTargetsPerPlugin
	}
	add := func(target reverseSyncTarget) {
		if target.session.PluginID == "" {
			target.session.PluginID = pluginID
		}
		if target.session.NativeSessionID == "" {
			return
		}
		key := target.key()
		if key == "" || seen[key] {
			return
		}
		if len(out) >= maxTargets {
			return
		}
		seen[key] = true
		out = append(out, target)
	}
	recordsByNative := map[string]bridgedb.SessionRecord{}
	if sessions, err := s.DB.ListSessions(ctx, pluginID); err == nil {
		for _, record := range sessions {
			recordsByNative[nativeSessionLookupKey(record.NativeSessionID, record.NativeThreadID)] = record
			add(targetFromRecord(record))
		}
	} else {
		s.warn("list reverse sync sessions failed", "plugin_id", pluginID, "err", err)
	}
	if mode != pluginruntime.ReverseSyncAllThreads {
		return out
	}
	if lister, ok := adapter.(pluginruntime.SessionLister); ok {
		hints, err := lister.ListSessions(ctx)
		if err != nil {
			s.warn("adapter list sessions failed", "plugin_id", pluginID, "err", err)
		}
		for _, hint := range hints {
			if len(out) >= maxTargets {
				break
			}
			if !isAllowedReverseHint(hint) {
				continue
			}
			target := targetFromHint(pluginID, hint)
			if record, ok := recordsByNative[nativeSessionLookupKey(target.session.NativeSessionID, target.session.NativeThreadID)]; ok {
				target.record = record
			} else if target.record.PrismConversationID != "" {
				record, err := s.DB.UpsertSession(ctx, target.record)
				if err != nil {
					s.warn("persist reverse sync session hint failed", "plugin_id", pluginID, "native_session_id", target.session.NativeSessionID, "err", err)
				} else {
					target.record = record
					recordsByNative[nativeSessionLookupKey(record.NativeSessionID, record.NativeThreadID)] = record
				}
			}
			add(target)
		}
	}
	return out
}

func (s *ReverseSyncer) applyDesired(ctx context.Context, desired map[string]reverseSyncTarget) {
	s.mu.Lock()
	if s.active == nil {
		s.active = map[string]*reverseSyncHandle{}
	}
	for key, handle := range s.active {
		target, wanted := desired[key]
		if wanted && samePluginAdapter(handle.adapter, s.Service.Adapter(target.session.PluginID)) {
			continue
		}
		handle.cancel()
		delete(s.active, key)
	}
	for key, target := range desired {
		if _, ok := s.active[key]; ok {
			continue
		}
		adapter := s.Service.Adapter(target.session.PluginID)
		if adapter == nil {
			continue
		}
		subCtx, cancel := context.WithCancel(ctx)
		handle := &reverseSyncHandle{cancel: cancel, adapter: adapter}
		s.active[key] = handle
		go s.runTarget(subCtx, key, handle, target)
	}
	s.mu.Unlock()
}

func (s *ReverseSyncer) runTarget(ctx context.Context, key string, handle *reverseSyncHandle, target reverseSyncTarget) {
	defer s.releaseActiveTarget(key, handle)
	adapter := handle.adapter
	if adapter == nil {
		return
	}
	defer func() {
		if s.Service == nil || !samePluginAdapter(adapter, s.Service.Adapter(target.session.PluginID)) {
			return
		}
		if publisher := s.snapshotPublisher(); publisher != nil {
			var err error
			if target.pluginWide {
				err = publisher.PublishTerminalUnavailable(target.session.PluginID)
			} else {
				err = publisher.PublishTerminalUnavailableForConversation(target.session.PluginID, target.record.PrismConversationID)
			}
			if err != nil {
				s.warn("publish terminal unavailable detail failed", "plugin_id", target.session.PluginID, "err", err)
			}
		}
	}()
	var events <-chan pluginruntime.PluginEvent
	var err error
	if target.pluginWide {
		watcher, ok := adapter.(pluginruntime.PluginWideSubscriber)
		if !ok {
			s.warn("reverse sync plugin-wide watcher unavailable", "plugin_id", target.session.PluginID)
			return
		}
		events, err = watcher.SubscribePlugin(ctx)
	} else {
		events, err = adapter.Subscribe(ctx, target.session)
	}
	if err != nil {
		s.warn("reverse sync subscribe failed", "plugin_id", target.session.PluginID, "session", target.session.NativeSessionID, "err", err)
		s.persistEvent(context.Background(), target, pluginruntime.PluginEvent{
			ID:        "reverse-sync-subscribe-" + target.session.NativeSessionID,
			Type:      "reverse_sync.subscribe_failed",
			Status:    StageFailed,
			Summary:   err.Error(),
			CreatedAt: time.Now().UTC(),
			Payload:   map[string]any{"error": err.Error()},
		})
		return
	}
	if s.Logger != nil {
		s.Logger.Info("reverse sync subscribed",
			"plugin_id", target.session.PluginID,
			"session", target.session.NativeSessionID,
			"plugin_wide", target.pluginWide,
			"thread", target.session.NativeThreadID,
			"conversation", target.record.PrismConversationID,
		)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			// A replaced process may leave one buffered event readable after its
			// subscription is cancelled. Never let that previous runtime write
			// detail or index state after the service slot has changed owners.
			if !samePluginAdapter(adapter, s.Service.Adapter(target.session.PluginID)) {
				return
			}
			s.handleEvent(ctx, target, event)
		}
	}
}

// samePluginAdapter identifies the runtime instance that owns a subscription.
// A plugin reload replaces the adapter under the same plugin_id, so keeping a
// subscription solely by plugin_id would leave its watcher in the old process.
func samePluginAdapter(left, right pluginruntime.PluginAdapter) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	return left == right
}

func (s *ReverseSyncer) releaseActiveTarget(key string, handle *reverseSyncHandle) {
	if s == nil || strings.TrimSpace(key) == "" || handle == nil {
		return
	}
	s.mu.Lock()
	current := s.active[key]
	if current == handle {
		delete(s.active, key)
	}
	s.mu.Unlock()
}

func (s *ReverseSyncer) handleEvent(ctx context.Context, target reverseSyncTarget, event pluginruntime.PluginEvent) {
	if target.pluginWide && strings.EqualFold(strings.TrimSpace(event.Type), "agent.usage.updated") {
		s.publishCatalog(ctx)
		return
	}
	if target.pluginWide && strings.EqualFold(strings.TrimSpace(event.Type), "desktop.session.directory.reconciled") {
		publisher := s.snapshotPublisher()
		if publisher == nil {
			return
		}
		reconcileCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := publisher.PublishIndex(reconcileCtx, false, target.session.PluginID)
		cancel()
		if err != nil {
			s.warn("reconcile plugin conversation directory failed", "plugin_id", target.session.PluginID, "err", err)
		}
		return
	}
	if target.pluginWide && strings.EqualFold(strings.TrimSpace(event.Type), "desktop.session.index.changed") {
		resolved, err := s.resolvePluginWideTarget(ctx, target, event)
		if err != nil {
			s.warn("plugin-wide directory item ignored", "plugin_id", target.session.PluginID, "err", err)
			return
		}
		publisher := s.snapshotPublisher()
		if publisher == nil {
			return
		}
		if !publisher.HasConversationIndex(resolved.session.PluginID) {
			indexCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = publisher.EnsureConversationIndexes(indexCtx, resolved.session.PluginID)
			cancel()
			if err != nil {
				s.warn("initialize directory item index failed", "plugin_id", resolved.session.PluginID, "err", err)
				return
			}
		}
		hint, _, ok := indexHintFromPluginEvent(event, resolved.session)
		if !ok {
			s.warn("plugin-wide directory item missing session hint", "plugin_id", resolved.session.PluginID)
			return
		}
		if err := publisher.PublishIndexHint(resolved.session.PluginID, hint, resolved.record, false); err != nil {
			s.warn("publish directory item index update failed", "plugin_id", resolved.session.PluginID, "conversation_id", resolved.record.PrismConversationID, "err", err)
		}
		return
	}
	if target.pluginWide && strings.EqualFold(strings.TrimSpace(event.Type), "desktop.draft.stale") {
		if s.Drafts != nil {
			draftID := ""
			if event.Payload != nil {
				draftID = stringFromAny(event.Payload["draft_id"])
			}
			s.Drafts.MarkPluginDraftStale(target.session.PluginID, draftID, strings.TrimSpace(event.Summary))
		}
		return
	}
	if target.pluginWide {
		resolved, err := s.resolvePluginWideTarget(ctx, target, event)
		if err != nil {
			s.warn("plugin-wide watcher event ignored", "plugin_id", target.session.PluginID, "err", err)
			return
		}
		target = resolved
	}
	if code, watcherUnavailable := foregroundWatcherUnavailableCode(event); watcherUnavailable {
		s.handleForegroundWatcherUnavailable(target, code)
		return
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if strings.TrimSpace(event.Status) == "" {
		event.Status = StageRunning
	}
	if strings.TrimSpace(event.Summary) == "" {
		event.Summary = "Plugin 会话有新活动。"
	}
	durable := event.Payload != nil && event.Payload["prism_outbox"] == true && strings.TrimSpace(event.ID) != ""
	inserted, persistErr := s.persistEvent(ctx, target, event)
	if persistErr != nil {
		s.warn("persist reverse sync event failed", "plugin_id", target.session.PluginID, "err", persistErr)
		return
	}
	if durable {
		if s.DB == nil || strings.TrimSpace(target.record.ID) == "" {
			return
		}
		delivered, err := s.DB.NativeEventDelivered(ctx, target.record.ID, event.ID)
		if err != nil {
			s.warn("check native event delivery failed", "plugin_id", target.session.PluginID, "err", err)
			return
		}
		if delivered {
			s.ackNativeEvent(target, event)
			return
		}
	} else if !inserted {
		return
	}
	if s.Logger != nil {
		s.Logger.Info("reverse sync event received",
			"plugin_id", target.session.PluginID,
			"session", target.session.NativeSessionID,
			"conversation", target.record.PrismConversationID,
			"type", event.Type,
			"status", event.Status,
		)
	}
	if shouldSuppressConversationEvent(event) {
		s.debugReverse("conversation event dropped: event-type filter", target, event)
		s.completeNativeEvent(target, event)
		return
	}
	if s.Runs != nil && s.Runs.ShouldSuppress(target.session.PluginID, target.record.PrismConversationID, target.session, event) {
		s.debugReverse("conversation event dropped: matches active forward run (echo suppression)", target, event)
		s.completeNativeEvent(target, event)
		return
	}
	if target.record.PrismConversationID == "" || s.Sink == nil {
		s.debugReverse("conversation event dropped: no conversation id or sink", target, event)
		return
	}
	if !pluginruntime.RemoteEnabled(s.currentConfig(), target.session.PluginID) {
		s.debugReverse("conversation event dropped: remote access disabled", target, event)
		s.completeNativeEvent(target, event)
		return
	}
	pluginID := strings.TrimSpace(target.session.PluginID)
	if pluginID == "" {
		resolvedPluginID, pluginErr := RequirePlugin(context.Background(), s.Service, target.session.PluginID)
		if pluginErr != nil {
			s.warn("conversation event dropped: plugin identity unavailable", "plugin_id", target.session.PluginID, "err", pluginErr)
			return
		}
		pluginID = resolvedPluginID
	}
	target.session.PluginID = firstNonEmpty(target.session.PluginID, pluginID)
	eventContext := conversationEventContext{
		ConversationID:  target.record.PrismConversationID,
		PluginID:        pluginID,
		SourceDevice:    "plugin-desktop",
		ConversationCwd: target.record.Cwd,
		Bound:           strings.TrimSpace(target.record.PrismConversationID) != "",
		Session:         target.session,
	}
	payload := prismtransport.ConversationEventPayload{
		ConversationID: target.record.PrismConversationID,
		PluginID:       pluginID,
		EventID:        event.ID,
		EventType:      event.Type,
		Status:         event.Status,
		Summary:        event.Summary,
		SourceDevice:   "plugin-desktop",
		Payload:        buildConversationEventEnvelope(eventContext, event),
	}
	outbound := prismtransport.ControlOutbound{
		MessageID:   stableConversationEventDedupeID(eventContext, event),
		MessageType: prismtransport.MsgConversationEvent,
		Payload:     payload,
	}
	var sendErr error
	if durable {
		if acknowledged, ok := s.Sink.(acknowledgedControlSink); ok {
			writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			sendErr = acknowledged.SendControlAcknowledged(writeCtx, outbound)
			cancel()
		} else if confirmed, ok := s.Sink.(confirmedControlSink); ok {
			writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			sendErr = confirmed.SendControlConfirmed(writeCtx, outbound)
			cancel()
		} else {
			sendErr = s.Sink.SendControl(outbound)
		}
	} else {
		sendErr = s.Sink.SendControl(outbound)
	}
	if err := sendErr; err != nil {
		s.warn(
			"conversation event send failed",
			"plugin_id", target.session.PluginID,
			"conversation_id", target.record.PrismConversationID,
			"session", target.session.NativeSessionID,
			"event_type", event.Type,
			"status", event.Status,
			"err", err,
		)
		return
	}
	s.completeNativeEvent(target, event)
	if s != nil && s.Logger != nil {
		s.Logger.Info(
			"conversation event sent",
			"plugin_id", target.session.PluginID,
			"conversation_id", target.record.PrismConversationID,
			"session", target.session.NativeSessionID,
			"event_type", event.Type,
			"status", event.Status,
		)
	}
	publisher := s.snapshotPublisher()
	if publisher == nil {
		return
	}
	if details, ok := detailSnapshotPayloadFromPluginEvent(event); ok {
		liveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := publisher.PublishLiveDetail(liveCtx, target.record.PrismConversationID, target.session.PluginID, target.session, target.record.Cwd, details)
		cancel()
		if err != nil {
			s.warn("publish watcher detail snapshot failed", "conversation_id", target.record.PrismConversationID, "plugin_id", target.session.PluginID, "err", err)
		}
	}
	if hint, deleted, ok := indexHintFromPluginEvent(event, target.session); ok {
		if target.pluginWide && !publisher.HasConversationIndex(target.session.PluginID) {
			indexCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := publisher.EnsureConversationIndexes(indexCtx, target.session.PluginID)
			cancel()
			if err != nil {
				s.warn("initialize watcher conversation index failed", "plugin_id", target.session.PluginID, "err", err)
			}
		}
		if err := publisher.PublishIndexHint(target.session.PluginID, hint, target.record, deleted); err != nil {
			s.warn("publish watcher conversation index update failed", "plugin_id", target.session.PluginID, "conversation_id", target.record.PrismConversationID, "err", err)
		}
	}
}

func (s *ReverseSyncer) resolvePluginWideTarget(ctx context.Context, root reverseSyncTarget, event pluginruntime.PluginEvent) (reverseSyncTarget, error) {
	if s == nil || s.DB == nil {
		return reverseSyncTarget{}, fmt.Errorf("plugin-wide watcher requires bridge db")
	}
	pluginID := strings.TrimSpace(root.session.PluginID)
	session, ok := nativeSessionFromPluginEvent(event, pluginID)
	if !ok {
		return reverseSyncTarget{}, fmt.Errorf("plugin-wide event missing native_session")
	}
	if record, err := s.findRecordForNativeIdentity(ctx, pluginID, session); err == nil {
		return reverseSyncTarget{record: record, session: applySessionRecord(session, record)}, nil
	}
	record, err := s.DB.EnsureCanonicalSessionBinding(ctx, bridgedb.SessionRecord{
		PluginID:        pluginID,
		NativeSessionID: session.NativeSessionID,
		NativeThreadID:  session.NativeThreadID,
		Surface:         session.Surface,
		Endpoint:        session.Endpoint,
		Cwd:             session.Cwd,
	})
	if err != nil {
		return reverseSyncTarget{}, fmt.Errorf("create native binding: %w", err)
	}
	return reverseSyncTarget{
		record:  record,
		session: applySessionRecord(session, record),
	}, nil
}

func indexHintFromPluginEvent(event pluginruntime.PluginEvent, session pluginruntime.NativeSession) (pluginruntime.NativeSessionHint, bool, bool) {
	if event.Payload == nil {
		return pluginruntime.NativeSessionHint{}, false, false
	}
	raw, ok := event.Payload["session_hint"].(map[string]any)
	if !ok || raw == nil {
		return pluginruntime.NativeSessionHint{}, false, false
	}
	read := func(keys ...string) string {
		for _, key := range keys {
			if value := stringFromAny(raw[key]); value != "" {
				return value
			}
		}
		return ""
	}
	metadata := map[string]string{}
	if rawMetadata, ok := raw["metadata"].(map[string]any); ok {
		for key, value := range rawMetadata {
			if text := stringFromAny(value); text != "" {
				metadata[key] = text
			}
		}
	}
	deleted := strings.EqualFold(read("change", "event"), "deleted") || strings.EqualFold(metadata["archived"], "true")
	updatedAt, _ := time.Parse(time.RFC3339Nano, read("last_activity_at", "LastActivityAt"))
	hint := pluginruntime.NativeSessionHint{
		PluginID:        firstNonEmpty(read("plugin_id", "PluginID"), session.PluginID),
		NativeSessionID: firstNonEmpty(read("native_session_id", "NativeSessionID"), session.NativeSessionID),
		NativeThreadID:  firstNonEmpty(read("native_thread_id", "NativeThreadID"), session.NativeThreadID),
		Surface:         firstNonEmpty(read("surface", "Surface"), session.Surface),
		Endpoint:        firstNonEmpty(read("endpoint", "Endpoint"), session.Endpoint),
		Cwd:             firstNonEmpty(read("cwd", "Cwd"), session.Cwd),
		Title:           read("title", "Title"),
		Visible:         true,
		LastActivityAt:  updatedAt,
		Metadata:        metadata,
	}
	return hint, deleted, nativeSessionLookupKey(hint.NativeSessionID, hint.NativeThreadID) != ""
}

func nativeSessionFromPluginEvent(event pluginruntime.PluginEvent, fallbackPluginID string) (pluginruntime.NativeSession, bool) {
	payload := event.Payload
	if payload == nil {
		return pluginruntime.NativeSession{}, false
	}
	raw, ok := payload["native_session"].(map[string]any)
	if !ok || raw == nil {
		return pluginruntime.NativeSession{}, false
	}
	read := func(keys ...string) string {
		for _, key := range keys {
			if value := stringFromAny(raw[key]); value != "" {
				return value
			}
		}
		return ""
	}
	session := pluginruntime.NativeSession{
		PluginID:        firstNonEmpty(read("plugin_id", "PluginID"), fallbackPluginID),
		NativeSessionID: read("native_session_id", "NativeSessionID"),
		NativeThreadID:  read("native_thread_id", "NativeThreadID"),
		Surface:         read("surface", "Surface"),
		Endpoint:        read("endpoint", "Endpoint"),
		Cwd:             read("cwd", "Cwd"),
		Visible:         true,
	}
	if nativeSessionLookupKey(session.NativeSessionID, session.NativeThreadID) == "" {
		return pluginruntime.NativeSession{}, false
	}
	return session, true
}

func (s *ReverseSyncer) handleForegroundWatcherUnavailable(target reverseSyncTarget, code string) {
	conversationID := strings.TrimSpace(target.record.PrismConversationID)
	pluginID := strings.TrimSpace(target.session.PluginID)
	if conversationID == "" || pluginID == "" {
		return
	}
	publisher := s.snapshotPublisher()
	if publisher == nil {
		return
	}
	if err := publisher.PublishDetailUnavailable(pluginID, conversationID, code); err != nil {
		s.warn("publish watcher unavailable detail failed", "conversation_id", conversationID, "plugin_id", pluginID, "err", err)
	}
}

func (s *ReverseSyncer) publishCatalog(ctx context.Context) {
	publisher := s.snapshotPublisher()
	if publisher == nil {
		return
	}
	if err := publisher.PublishCatalog(ctx, false); err != nil {
		s.warn("publish plugin catalog snapshot failed", "err", err)
	}
}

func (s *ReverseSyncer) refreshIndexes(ctx context.Context) {
	publisher := s.snapshotPublisher()
	if publisher == nil {
		return
	}
	if err := publisher.RefreshConversationIndexes(ctx); err != nil {
		s.warn("refresh conversation index lease failed", "err", err)
	}
}

func (s *ReverseSyncer) snapshotPublisher() *SnapshotPublisher {
	if s == nil {
		return nil
	}
	s.publisherMu.Lock()
	defer s.publisherMu.Unlock()
	if s.SnapshotPublisher == nil {
		s.SnapshotPublisher = NewSnapshotPublisher(
			s.Service,
			s.DB,
			s.Config,
			s.ConfigProvider,
			s.Sink,
			s.DetailSnapshots,
			s.Logger,
		)
	}
	return s.SnapshotPublisher
}

func shouldSuppressConversationEvent(event pluginruntime.PluginEvent) bool {
	switch strings.ToLower(strings.TrimSpace(event.Type)) {
	case "session.info":
		return true
	default:
		return false
	}
}

func (s *ReverseSyncer) persistEvent(ctx context.Context, target reverseSyncTarget, event pluginruntime.PluginEvent) (bool, error) {
	if s.DB == nil {
		return true, nil
	}
	if strings.TrimSpace(target.record.ID) == "" || strings.TrimSpace(target.record.PrismConversationID) == "" {
		return true, nil
	}
	record := bridgedb.EventRecord{
		SessionID:           target.record.ID,
		PrismConversationID: target.record.PrismConversationID,
		PluginID:            target.session.PluginID,
		NativeEventID:       event.ID,
		EventType:           event.Type,
		Status:              event.Status,
		Summary:             event.Summary,
		Payload:             event.Payload,
		CreatedAt:           event.CreatedAt,
	}
	if strings.TrimSpace(event.ID) != "" {
		digest := sha256.Sum256([]byte(target.record.ID + "\x00" + event.ID))
		record.ID = "native_" + hex.EncodeToString(digest[:16])
		inserted, err := s.DB.InsertNativeEventOnce(ctx, record)
		return inserted, err
	}
	_, err := s.DB.InsertEvent(ctx, record)
	return err == nil, err
}

func (s *ReverseSyncer) completeNativeEvent(target reverseSyncTarget, event pluginruntime.PluginEvent) {
	if event.Payload == nil || event.Payload["prism_outbox"] != true || s.DB == nil || target.record.ID == "" || event.ID == "" {
		return
	}
	if err := s.DB.MarkNativeEventDelivered(context.Background(), target.record.ID, event.ID); err != nil {
		s.warn("mark native event delivered failed", "plugin_id", target.session.PluginID, "err", err)
		return
	}
	s.ackNativeEvent(target, event)
}

func (s *ReverseSyncer) ackNativeEvent(target reverseSyncTarget, event pluginruntime.PluginEvent) {
	if s.Service == nil {
		return
	}
	acker, ok := s.Service.Adapter(target.session.PluginID).(pluginruntime.EventAcker)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := acker.AckEvent(ctx, target.session, event.ID); err != nil {
		s.warn("acknowledge native event failed", "plugin_id", target.session.PluginID, "err", err)
	}
}

func (s *ReverseSyncer) findRecordForNativeSession(ctx context.Context, pluginID, nativeSessionID string) (bridgedb.SessionRecord, error) {
	return s.findRecordForNativeIdentity(ctx, pluginID, pluginruntime.NativeSession{NativeSessionID: nativeSessionID})
}

func (s *ReverseSyncer) findRecordForNativeIdentity(ctx context.Context, pluginID string, session pluginruntime.NativeSession) (bridgedb.SessionRecord, error) {
	sessions, err := s.DB.ListSessions(ctx, pluginID)
	if err != nil {
		return bridgedb.SessionRecord{}, err
	}
	for _, record := range sessions {
		if nativeSessionLookupKey(record.NativeSessionID, record.NativeThreadID) == nativeSessionLookupKey(session.NativeSessionID, session.NativeThreadID) {
			return record, nil
		}
	}
	return bridgedb.SessionRecord{}, sql.ErrNoRows
}

func (s *ReverseSyncer) stopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, handle := range s.active {
		handle.cancel()
		delete(s.active, key)
	}
}

func (s *ReverseSyncer) warn(msg string, args ...any) {
	if s != nil && s.Logger != nil {
		s.Logger.Warn(msg, args...)
	}
}

func (s *ReverseSyncer) debugReverse(msg string, target reverseSyncTarget, event pluginruntime.PluginEvent) {
	if s == nil || s.Logger == nil {
		return
	}
	s.Logger.Info(msg,
		"plugin_id", target.session.PluginID,
		"session", target.session.NativeSessionID,
		"conversation", target.record.PrismConversationID,
		"type", event.Type,
		"status", event.Status,
	)
}

type reverseSyncTarget struct {
	record     bridgedb.SessionRecord
	session    pluginruntime.NativeSession
	pluginWide bool
}

func (t reverseSyncTarget) key() string {
	if t.pluginWide {
		return strings.ToLower(strings.TrimSpace(t.session.PluginID)) + "/@plugin-wide"
	}
	return strings.ToLower(strings.TrimSpace(t.session.PluginID)) + "/" + strings.TrimSpace(t.session.NativeSessionID)
}

func targetFromRecord(record bridgedb.SessionRecord) reverseSyncTarget {
	return reverseSyncTarget{
		record: record,
		session: pluginruntime.NativeSession{
			PluginID:        record.PluginID,
			NativeSessionID: record.NativeSessionID,
			NativeThreadID:  record.NativeThreadID,
			Surface:         record.Surface,
			// A persisted local HTTP endpoint belongs to the previous native
			// process. Each adapter must rediscover its current transport rather
			// than reconnecting a stale loopback address from the session mirror.
			Endpoint: persistedSessionEndpoint(record.Endpoint),
			Cwd:      record.Cwd,
			Visible:  true,
		},
	}
}

func persistedSessionEndpoint(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil {
		return value
	}
	hostname := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
		return ""
	}
	return value
}

func targetFromHint(pluginID string, hint pluginruntime.NativeSessionHint) reverseSyncTarget {
	if hint.PluginID != "" {
		pluginID = hint.PluginID
	}
	prismConversationID := firstNonEmpty(hint.PrismConversationID, prismConversationIDFromNativeSessionID(hint.NativeSessionID))
	return reverseSyncTarget{
		record: bridgedb.SessionRecord{
			PrismConversationID: prismConversationID,
			PluginID:            pluginID,
			Cwd:                 hint.Cwd,
			NativeSessionID:     hint.NativeSessionID,
			NativeThreadID:      hint.NativeThreadID,
			Surface:             hint.Surface,
			Endpoint:            hint.Endpoint,
			UpdatedAt:           hint.LastActivityAt,
		},
		session: pluginruntime.NativeSession{
			PluginID:        pluginID,
			NativeSessionID: hint.NativeSessionID,
			NativeThreadID:  hint.NativeThreadID,
			Surface:         hint.Surface,
			Endpoint:        hint.Endpoint,
			Cwd:             hint.Cwd,
			Visible:         hint.Visible,
		},
	}
}

func isAllowedReverseHint(hint pluginruntime.NativeSessionHint) bool {
	if strings.TrimSpace(hint.NativeSessionID) == "" {
		return false
	}
	if strings.TrimSpace(hint.PrismConversationID) != "" {
		return true
	}
	if prismConversationIDFromNativeSessionID(hint.NativeSessionID) != "" {
		return true
	}
	if hint.Metadata != nil && strings.EqualFold(strings.TrimSpace(hint.Metadata["prism_managed"]), "true") {
		return true
	}
	return false
}

func prismConversationIDFromNativeSessionID(nativeSessionID string) string {
	const prefix = "agent:main:prism:"
	nativeSessionID = strings.TrimSpace(nativeSessionID)
	if !strings.HasPrefix(nativeSessionID, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(nativeSessionID, prefix))
}
