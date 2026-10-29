package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	prismapi "rokid-prism-hub/internal/api"
	bridgeconfig "rokid-prism-hub/internal/config"
	bridgedb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

// maxSnapshotLeaseDuration is the maximum duration a snapshot projection
// can be assumed to remain valid in Cloud Redis without re-affirming it.
// Realtime projection keys expire in ~120s if not renewed; republishing
// within 45s guarantees projections never expire during periods of inactivity.
const maxSnapshotLeaseDuration = 45 * time.Second

// SnapshotPublisher is the only Hub path that writes catalog, index, or detail
// snapshots to Realtime. It publishes complete scopes and suppresses duplicate
// semantic states within maxSnapshotLeaseDuration; periodic lease renewal keeps
// the resulting Cloud Redis projection alive even when idle.
type SnapshotPublisher struct {
	Service         *pluginruntime.Service
	DB              *bridgedb.DB
	Config          *bridgeconfig.Config
	ConfigProvider  func() *bridgeconfig.Config
	Sink            PrismSink
	Logger          *slog.Logger
	DetailSnapshots *DetailSnapshotStore

	mu            sync.Mutex
	fingerprints  map[string]string
	lastPublished map[string]time.Time
	indexMu       sync.Mutex
	indexes       map[string]prismtransport.ConversationIndexPlugin
}

func NewSnapshotPublisher(service *pluginruntime.Service, db *bridgedb.DB, cfg *bridgeconfig.Config, configProvider func() *bridgeconfig.Config, sink PrismSink, detailSnapshots *DetailSnapshotStore, logger *slog.Logger) *SnapshotPublisher {
	return &SnapshotPublisher{
		Service:         service,
		DB:              db,
		Config:          cfg,
		ConfigProvider:  configProvider,
		Sink:            sink,
		Logger:          logger,
		DetailSnapshots: detailSnapshots,
		fingerprints:    map[string]string{},
		lastPublished:   map[string]time.Time{},
		indexes:         map[string]prismtransport.ConversationIndexPlugin{},
	}
}

func (p *SnapshotPublisher) CurrentConfig() *bridgeconfig.Config {
	if p == nil {
		return nil
	}
	if p.ConfigProvider != nil {
		if cfg := p.ConfigProvider(); cfg != nil {
			return cfg
		}
	}
	return p.Config
}

// PublishAll rebuilds the complete state after Hub start, reconnect, or a
// registration-set replacement. Force is intentional here: Realtime may have
// cleared the Host projection while this Hub process retained its local state.
func (p *SnapshotPublisher) PublishAll(ctx context.Context) {
	if p == nil {
		return
	}
	catalogPublished := true
	indexPublished := true
	detailPublished := true
	if err := p.PublishCatalog(ctx, true); err != nil {
		catalogPublished = false
		p.warn("publish catalog snapshot failed", "err", err)
	}
	if err := p.PublishIndex(ctx, true); err != nil {
		indexPublished = false
		p.warn("publish conversation index snapshot failed", "err", err)
	}
	if err := p.PublishCachedLiveDetails(true); err != nil {
		detailPublished = false
		p.warn("publish foreground detail snapshots failed", "err", err)
	}
	if p.Logger != nil {
		indexScopes, indexedConversations := p.ConversationIndexSummary()
		p.Logger.Info("complete Hub snapshot republished",
			"catalog", catalogPublished,
			"index", indexPublished,
			"index_plugin_scopes", indexScopes,
			"indexed_conversations", indexedConversations,
			"foreground_detail", detailPublished)
	}
}

// PublishCachedLiveDetails republishes both desktop foreground detail and
// managed native terminal detail after the Realtime connection is rebuilt.
func (p *SnapshotPublisher) PublishCachedLiveDetails(force bool) error {
	if p == nil || p.DetailSnapshots == nil {
		return nil
	}
	seen := map[string]bool{}
	for _, snapshot := range append(p.DetailSnapshots.ForegroundSnapshots(), p.DetailSnapshots.TerminalSnapshots()...) {
		pluginID := strings.TrimSpace(stringFromAny(snapshot["plugin_id"]))
		conversationID := strings.TrimSpace(stringFromAny(snapshot["conversation_id"]))
		key := pluginID + "\x00" + conversationID
		if seen[key] || pluginID == "" || conversationID == "" || !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) {
			continue
		}
		seen[key] = true
		if err := p.publishDetail(snapshot, force); err != nil {
			return err
		}
	}
	return nil
}

func (p *SnapshotPublisher) PublishCatalog(ctx context.Context, force bool) error {
	if p == nil || p.Sink == nil {
		return nil
	}
	payload := prismtransport.CatalogSnapshotPayload{
		Plugins: BuildCatalogPlugins(ctx, p.Service, p.DB, p.CurrentConfig()),
	}
	return p.publish("catalog", prismtransport.MsgCatalogSnapshot, payload, force)
}

// PublishIndex publishes complete index scopes. Supplying plugin IDs limits the
// rebuild to watcher-confirmed Plugin scopes; no IDs rebuilds all enabled scopes.
func (p *SnapshotPublisher) PublishIndex(ctx context.Context, force bool, pluginIDs ...string) error {
	if p == nil || p.Sink == nil {
		return nil
	}
	snapshot := BuildConversationIndex(ctx, p.Service, p.DB, p.CurrentConfig(), pluginIDs...)
	for _, plugin := range snapshot {
		pluginID := strings.TrimSpace(plugin.PluginID)
		if pluginID == "" {
			continue
		}
		if p.Logger != nil {
			p.Logger.Debug("publish conversation index snapshot", "plugin_id", pluginID, "conversation_count", len(plugin.Conversations), "force", force)
		}
		plugins := []prismtransport.ConversationIndexPlugin{plugin}
		p.storeConversationIndex(plugin)
		payload := prismtransport.ConversationIndexSnapshotPayload{
			Plugins:   plugins,
			Signature: ConversationIndexSnapshotSignature(plugins),
		}
		if err := p.publish("index:"+pluginID, prismtransport.MsgConversationIndexSnapshot, payload, force); err != nil {
			return err
		}
	}
	return nil
}

// EnsureConversationIndexes initializes only Plugin scopes which do not yet
// have a complete in-memory index. Initialization always goes through
// ListSessions and never derives an index from an individual watcher event.
// This covers the startup race where a Plugin becomes ready just after the
// initial PublishAll attempt.
func (p *SnapshotPublisher) EnsureConversationIndexes(ctx context.Context, pluginIDs ...string) error {
	if p == nil {
		return nil
	}
	ids := pluginIDs
	if len(ids) == 0 && p.Service != nil {
		ids = p.Service.AdapterIDs()
	}
	missing := make([]string, 0, len(ids))
	for _, pluginID := range ids {
		pluginID = strings.TrimSpace(pluginID)
		if pluginID == "" || !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) {
			continue
		}
		if !p.HasConversationIndex(pluginID) {
			missing = append(missing, pluginID)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return p.PublishIndex(ctx, false, missing...)
}

func (p *SnapshotPublisher) HasConversationIndex(pluginID string) bool {
	if p == nil {
		return false
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return false
	}
	p.indexMu.Lock()
	defer p.indexMu.Unlock()
	_, ok := p.indexes[pluginID]
	return ok
}

// RebuildConversationIndexIfStale rebuilds a cached complete index when the
// Plugin's current availability no longer matches the cached projection. A
// desktop-automation plugin can build its index while its live-control surface
// is unavailable (for example before a managed launch): the read-only session
// list stays valid, but remote clients would keep seeing an unavailable agent
// after the runtime recovers. Rebuilding republishes the complete scope with
// the current availability and sessions.
func (p *SnapshotPublisher) RebuildConversationIndexIfStale(ctx context.Context, pluginID string, available bool) error {
	if p == nil {
		return nil
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return nil
	}
	p.indexMu.Lock()
	cached, ok := p.indexes[pluginID]
	p.indexMu.Unlock()
	if !ok {
		// Creating a missing index belongs to the startup and refresh paths.
		// Building one from an ordinary reverse-sync tick would bind every
		// native session (managed or not) in the sessions DB, and the reverse
		// sync target scan subscribes DB records without the managed-hint
		// filter, leaking unmanaged sessions into event streaming.
		return nil
	}
	if cached.Available == available {
		return nil
	}
	p.InvalidateConversationIndex(pluginID)
	return p.EnsureConversationIndexes(ctx, pluginID)
}

// RefreshConversationIndexes sends cached in-memory conversation indexes to Realtime
// for all initialized, remote-enabled plugins when their lease duration has expired.
// This renews Cloud Redis index projections during idle periods without needing
// to re-query the underlying plugin session stores.
func (p *SnapshotPublisher) RefreshConversationIndexes(ctx context.Context) error {
	if p == nil || p.Sink == nil {
		return nil
	}
	cfg := p.CurrentConfig()
	p.indexMu.Lock()
	pluginIDs := make([]string, 0, len(p.indexes))
	for id := range p.indexes {
		if pluginruntime.RemoteEnabled(cfg, id) {
			pluginIDs = append(pluginIDs, id)
		}
	}
	p.indexMu.Unlock()

	for _, pluginID := range pluginIDs {
		p.indexMu.Lock()
		plugin, ok := p.indexes[pluginID]
		p.indexMu.Unlock()
		if !ok {
			continue
		}
		plugins := []prismtransport.ConversationIndexPlugin{plugin}
		payload := prismtransport.ConversationIndexSnapshotPayload{
			Plugins:   plugins,
			Signature: ConversationIndexSnapshotSignature(plugins),
		}
		if err := p.publish("index:"+pluginID, prismtransport.MsgConversationIndexSnapshot, payload, false); err != nil {
			return err
		}
	}
	return nil
}

// ConversationIndexSummary exposes the complete local index projection for
// startup/reconnect diagnostics. A successful PublishIndex call can otherwise
// be ambiguous: it returns nil both after sending index scopes and when no
// remote-authorized adapter produced one.
func (p *SnapshotPublisher) ConversationIndexSummary() (pluginScopes, conversations int) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, index := range p.indexes {
		pluginScopes++
		conversations += len(index.Conversations)
	}
	return pluginScopes, conversations
}

// InvalidateConversationIndex drops a cached complete index when a Plugin
// runtime is replaced. Runtime modes can expose disjoint native session
// stores, so retaining the previous mode's index would offer conversations
// that the new runtime cannot attach or resume.
func (p *SnapshotPublisher) InvalidateConversationIndex(pluginID string) {
	if p == nil {
		return
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return
	}
	p.indexMu.Lock()
	delete(p.indexes, pluginID)
	p.indexMu.Unlock()
	p.mu.Lock()
	delete(p.fingerprints, "index:"+pluginID)
	if p.lastPublished != nil {
		delete(p.lastPublished, "index:"+pluginID)
	}
	p.mu.Unlock()
}

// InvalidatePluginRuntime clears detail state that belongs to the runtime being
// replaced. The unavailable event removes the old strict-live projection from
// remote clients before the new runtime publishes its own complete detail.
func (p *SnapshotPublisher) InvalidatePluginRuntime(pluginID string) error {
	if p == nil {
		return nil
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return nil
	}
	var invalidateErr error
	if p.DetailSnapshots != nil {
		invalidateErr = p.PublishTerminalUnavailable(pluginID)
		if conversationID := p.DetailSnapshots.ForegroundConversationID(pluginID); conversationID != "" {
			if err := p.PublishDetailUnavailable(pluginID, conversationID, "plugin_runtime_replaced"); err != nil && invalidateErr == nil {
				invalidateErr = err
			}
		}
		p.DetailSnapshots.ResetPlugin(pluginID)
	}
	p.mu.Lock()
	for key := range p.fingerprints {
		if strings.HasPrefix(key, "detail:"+pluginID+":") || strings.HasPrefix(key, "detail-unavailable:"+pluginID+":") {
			delete(p.fingerprints, key)
			if p.lastPublished != nil {
				delete(p.lastPublished, key)
			}
		}
	}
	p.mu.Unlock()
	return invalidateErr
}

// PublishIndexHint applies one watcher-observed session mutation to a complete
// in-memory index scope. Mobile still receives the whole scope, but a model or
// detail event no longer re-reads every native session from the Plugin.
func (p *SnapshotPublisher) PublishIndexHint(pluginID string, hint pluginruntime.NativeSessionHint, record bridgedb.SessionRecord, deleted bool) error {
	if p == nil || p.Sink == nil {
		return nil
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return fmt.Errorf("conversation index hint requires plugin_id")
	}
	p.indexMu.Lock()
	plugin, ok := p.indexes[pluginID]
	if !ok {
		p.indexMu.Unlock()
		return fmt.Errorf("conversation index is not initialized for plugin %s", pluginID)
	}
	conversationID := strings.TrimSpace(record.PrismConversationID)
	if conversationID == "" {
		p.indexMu.Unlock()
		return fmt.Errorf("conversation index hint requires canonical conversation")
	}
	updated := make([]prismtransport.ConversationIndexItem, 0, len(plugin.Conversations)+1)
	found := false
	for _, item := range plugin.Conversations {
		if item.ID != conversationID {
			updated = append(updated, item)
			continue
		}
		found = true
		if !deleted {
			if replacement, valid := conversationIndexItemFromHint(hint, record); valid {
				updated = append(updated, replacement)
			}
		}
	}
	if !deleted && !found {
		if replacement, valid := conversationIndexItemFromHint(hint, record); valid {
			updated = append(updated, replacement)
		}
	}
	plugin.Conversations = updated
	plugin.UpdatedAt = newestConversationUpdatedAt(updated)
	plugin.Signature = conversationIndexAgentSignature(plugin)
	p.indexes[pluginID] = plugin
	p.indexMu.Unlock()
	plugins := []prismtransport.ConversationIndexPlugin{plugin}
	payload := prismtransport.ConversationIndexSnapshotPayload{
		Plugins:   plugins,
		Signature: ConversationIndexSnapshotSignature(plugins),
	}
	return p.publish("index:"+pluginID, prismtransport.MsgConversationIndexSnapshot, payload, false)
}

func (p *SnapshotPublisher) storeConversationIndex(plugin prismtransport.ConversationIndexPlugin) {
	if p == nil || strings.TrimSpace(plugin.PluginID) == "" {
		return
	}
	p.indexMu.Lock()
	defer p.indexMu.Unlock()
	if p.indexes == nil {
		p.indexes = map[string]prismtransport.ConversationIndexPlugin{}
	}
	p.indexes[plugin.PluginID] = plugin
}

func (p *SnapshotPublisher) PublishForegroundDetails(force bool) error {
	if p == nil || p.DetailSnapshots == nil {
		return nil
	}
	for _, snapshot := range p.DetailSnapshots.ForegroundSnapshots() {
		pluginID := strings.TrimSpace(stringFromAny(snapshot["plugin_id"]))
		if pluginID == "" || !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) {
			continue
		}
		if err := p.publishDetail(snapshot, force); err != nil {
			return err
		}
	}
	return nil
}

// PublishLiveDetail materializes and publishes one foreground watcher result.
// A watcher transition is also responsible for making the previously foreground
// conversation explicitly stale before the new one becomes visible remotely.
func (p *SnapshotPublisher) PublishLiveDetail(ctx context.Context, conversationID, pluginID string, session pluginruntime.NativeSession, cwd string, details map[string]any) error {
	if p == nil || len(details) == 0 {
		return nil
	}
	conversationID = strings.TrimSpace(conversationID)
	pluginID = strings.TrimSpace(pluginID)
	if conversationID == "" || pluginID == "" {
		return fmt.Errorf("live detail requires plugin_id and conversation_id")
	}
	if !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) {
		return nil
	}
	previousForeground := ""
	if p.DetailSnapshots != nil {
		previousForeground = p.DetailSnapshots.ForegroundConversationID(pluginID)
	}
	payload, err := buildLiveConversationDetailSnapshot(
		ctx,
		p.DB,
		p.Service,
		p.DetailSnapshots,
		conversationID,
		pluginID,
		session,
		cwd,
		details,
	)
	if err != nil {
		return err
	}
	if previousForeground != "" && previousForeground != conversationID && firstNonEmptyBool(payload["desktop_foreground"]) && p.DetailSnapshots != nil {
		if stale, ok := p.DetailSnapshots.GetForRuntime(pluginID, previousForeground); ok {
			if err := p.publishDetail(stale, false); err != nil {
				return err
			}
		}
	}
	if previousForeground != conversationID && firstNonEmptyBool(payload["desktop_foreground"]) {
		p.acknowledgeDesktopRead(pluginID, conversationID)
	}
	return p.publishDetail(payload, false)
}

func (p *SnapshotPublisher) acknowledgeDesktopRead(pluginID, conversationID string) {
	if p == nil {
		return
	}
	cfg := p.CurrentConfig()
	if cfg == nil || strings.TrimSpace(cfg.Prism.APIURL()) == "" || strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
		return
	}
	go func(apiURL string, account bridgeconfig.Account, hostID, pluginID, conversationID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := prismapi.MarkUserConversationRead(ctx, apiURL, account, hostID, pluginID, conversationID); err != nil {
			p.warn("mark desktop conversation read failed", "plugin_id", pluginID, "conversation_id", conversationID, "err", err)
		}
	}(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, pluginID, conversationID)
}

// PublishDetailUnavailable clears one remote strict-live projection while the
// local store keeps an explicitly stale copy for the next select-and-recover.
func (p *SnapshotPublisher) PublishDetailUnavailable(pluginID, conversationID, code string) error {
	if p == nil {
		return nil
	}
	pluginID = strings.TrimSpace(pluginID)
	conversationID = strings.TrimSpace(conversationID)
	if pluginID == "" || conversationID == "" {
		return fmt.Errorf("detail unavailable requires plugin_id and conversation_id")
	}
	if p.DetailSnapshots != nil {
		p.DetailSnapshots.MarkForegroundWatcherUnavailable(pluginID)
	}
	if !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) || p.Sink == nil {
		return nil
	}
	payload := prismtransport.ConversationDetailUnavailablePayload{
		PluginID:       pluginID,
		ConversationID: conversationID,
		Code:           firstNonEmpty(strings.TrimSpace(code), "foreground_watcher_unavailable"),
	}
	return p.publish("detail-unavailable:"+pluginID+":"+conversationID, prismtransport.MsgConversationDetailUnavailable, payload, true)
}

// PublishTerminalUnavailable revokes previously advertised native terminal
// controls when its event subscription ends. It sends a complete detail so a
// connected Panel cannot keep an old can_send=true projection.
func (p *SnapshotPublisher) PublishTerminalUnavailable(pluginID string) error {
	return p.publishTerminalUnavailable(pluginID, "")
}

func (p *SnapshotPublisher) PublishTerminalUnavailableForConversation(pluginID, conversationID string) error {
	return p.publishTerminalUnavailable(pluginID, conversationID)
}

func (p *SnapshotPublisher) publishTerminalUnavailable(pluginID, conversationID string) error {
	if p == nil || p.DetailSnapshots == nil {
		return nil
	}
	snapshots := p.DetailSnapshots.MarkTerminalUnavailable(pluginID, conversationID)
	if !pluginruntime.RemoteEnabled(p.CurrentConfig(), pluginID) || p.Sink == nil {
		return nil
	}
	var firstErr error
	for _, snapshot := range snapshots {
		if err := p.publishDetail(snapshot, false); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *SnapshotPublisher) publishDetail(snapshot map[string]any, force bool) error {
	if p == nil || p.Sink == nil || len(snapshot) == 0 {
		return nil
	}
	pluginID := strings.TrimSpace(stringFromAny(snapshot["plugin_id"]))
	conversationID := strings.TrimSpace(stringFromAny(snapshot["conversation_id"]))
	if pluginID == "" || conversationID == "" {
		return fmt.Errorf("detail snapshot requires plugin_id and conversation_id")
	}
	return p.publish("detail:"+pluginID+":"+conversationID, prismtransport.MsgConversationDetailSnapshot, snapshot, force)
}

func (p *SnapshotPublisher) publish(key string, messageType prismtransport.MessageType, payload any, force bool) error {
	if p == nil || p.Sink == nil {
		return nil
	}
	fingerprint, err := snapshotFingerprint(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fingerprints == nil {
		p.fingerprints = map[string]string{}
	}
	if p.lastPublished == nil {
		p.lastPublished = map[string]time.Time{}
	}
	lastTime := p.lastPublished[key]
	if !force && p.fingerprints[key] == fingerprint && !lastTime.IsZero() && time.Since(lastTime) < maxSnapshotLeaseDuration {
		return nil
	}
	if err := p.Sink.SendControl(prismtransport.ControlOutbound{MessageType: messageType, Payload: payload}); err != nil {
		return err
	}
	p.fingerprints[key] = fingerprint
	p.lastPublished[key] = time.Now()
	return nil
}

func snapshotFingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return "", err
	}
	canonical := omitSnapshotObservationTimes(decoded)
	encoded, err = json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func omitSnapshotObservationTimes(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nested := range typed {
			if key == "updated_at" {
				continue
			}
			out[key] = omitSnapshotObservationTimes(nested)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, nested := range typed {
			out = append(out, omitSnapshotObservationTimes(nested))
		}
		return out
	default:
		return value
	}
}

func normalizedPluginIDs(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (p *SnapshotPublisher) warn(msg string, args ...any) {
	if p != nil && p.Logger != nil {
		p.Logger.Warn(msg, args...)
	}
}
