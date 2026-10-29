package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	prismapi "rokid-prism-hub/internal/api"
	"rokid-prism-hub/internal/bridge"
	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
	"rokid-prism-hub/internal/host"
	"rokid-prism-hub/internal/observability"
	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginruntime"
	prismtransport "rokid-prism-hub/internal/realtime"
)

type conversationHistoryRelay struct {
	conversationID string
	pluginID       string
	cancel         context.CancelFunc
}

type cancelledConversationHistoryRelay struct {
	conversationID string
	pluginID       string
	expiresAt      time.Time
}

const cancelledConversationHistoryRelayTTL = time.Minute

const (
	bridgeEventRetention     = 30 * 24 * time.Hour
	bridgeEventPruneInterval = 24 * time.Hour
)

var activeConversationHistoryRelays = struct {
	sync.Mutex
	items     map[string]*conversationHistoryRelay
	cancelled map[string]cancelledConversationHistoryRelay
}{
	items:     make(map[string]*conversationHistoryRelay),
	cancelled: make(map[string]cancelledConversationHistoryRelay),
}

// dashboardRuntimePlugins points only at the active Hub-owned service. The
// Dashboard uses it for local-only actions such as managed terminal launch;
// it must never construct a second ProcessAdapter to service an HTTP click.
var dashboardRuntimePlugins = struct {
	sync.RWMutex
	service *pluginruntime.Service
}{}

// dashboardRuntimePluginReloader exposes the single Hub-owned reloader to the
// local Dashboard installer. It is never used to create a second runtime.
var dashboardRuntimePluginReloader = struct {
	sync.RWMutex
	reloader *pluginpkg.Reloader
}{}

func setDashboardRuntimePlugins(service *pluginruntime.Service) {
	dashboardRuntimePlugins.Lock()
	dashboardRuntimePlugins.service = service
	dashboardRuntimePlugins.Unlock()
}

func currentDashboardRuntimePlugins() *pluginruntime.Service {
	dashboardRuntimePlugins.RLock()
	defer dashboardRuntimePlugins.RUnlock()
	return dashboardRuntimePlugins.service
}

func setDashboardRuntimePluginReloader(reloader *pluginpkg.Reloader) {
	dashboardRuntimePluginReloader.Lock()
	dashboardRuntimePluginReloader.reloader = reloader
	dashboardRuntimePluginReloader.Unlock()
}

func currentDashboardRuntimePluginReloader() *pluginpkg.Reloader {
	dashboardRuntimePluginReloader.RLock()
	defer dashboardRuntimePluginReloader.RUnlock()
	return dashboardRuntimePluginReloader.reloader
}

func registerConversationHistoryRelay(bodyStreamID, conversationID, pluginID string, cancel context.CancelFunc) (*conversationHistoryRelay, bool) {
	relay := &conversationHistoryRelay{conversationID: conversationID, pluginID: pluginID, cancel: cancel}
	activeConversationHistoryRelays.Lock()
	pruneCancelledConversationHistoryRelaysLocked(time.Now())
	if cancelled, ok := activeConversationHistoryRelays.cancelled[bodyStreamID]; ok &&
		cancelled.conversationID == conversationID && cancelled.pluginID == pluginID {
		activeConversationHistoryRelays.Unlock()
		return nil, true
	}
	previous := activeConversationHistoryRelays.items[bodyStreamID]
	activeConversationHistoryRelays.items[bodyStreamID] = relay
	activeConversationHistoryRelays.Unlock()
	if previous != nil {
		previous.cancel()
	}
	return relay, false
}

func unregisterConversationHistoryRelay(bodyStreamID string, relay *conversationHistoryRelay) {
	activeConversationHistoryRelays.Lock()
	if activeConversationHistoryRelays.items[bodyStreamID] == relay {
		delete(activeConversationHistoryRelays.items, bodyStreamID)
	}
	activeConversationHistoryRelays.Unlock()
}

func cancelConversationHistoryRelay(bodyStreamID, conversationID, pluginID string) bool {
	activeConversationHistoryRelays.Lock()
	pruneCancelledConversationHistoryRelaysLocked(time.Now())
	relay := activeConversationHistoryRelays.items[bodyStreamID]
	if relay != nil && (relay.conversationID != conversationID || relay.pluginID != pluginID) {
		activeConversationHistoryRelays.Unlock()
		return false
	}
	activeConversationHistoryRelays.cancelled[bodyStreamID] = cancelledConversationHistoryRelay{
		conversationID: conversationID,
		pluginID:       pluginID,
		expiresAt:      time.Now().Add(cancelledConversationHistoryRelayTTL),
	}
	if relay != nil {
		delete(activeConversationHistoryRelays.items, bodyStreamID)
	}
	activeConversationHistoryRelays.Unlock()
	if relay == nil {
		return false
	}
	relay.cancel()
	return true
}

func pruneCancelledConversationHistoryRelaysLocked(now time.Time) {
	for bodyStreamID, cancelled := range activeConversationHistoryRelays.cancelled {
		if !cancelled.expiresAt.After(now) {
			delete(activeConversationHistoryRelays.cancelled, bodyStreamID)
		}
	}
}

// runBridgeRuntime is the Hub-internal Realtime/Plugin runtime. Its lifecycle
// is always owned by the Hub service context.
func runBridgeRuntime(parent context.Context, cfg *bridgeconfig.Config, db *prismdb.DB, apiURL, realtimeURL, configPath, dbPath string, debug bool, stdout, stderr io.Writer) int {
	if cfg == nil {
		fmt.Fprintln(stderr, "Hub runtime config unavailable")
		return 1
	}
	if !cfg.Bridge.Enabled {
		fmt.Fprintln(stderr, "Hub runtime disabled by config")
		return 1
	}
	if strings.TrimSpace(cfg.Prism.HostToken) == "" {
		fmt.Fprintln(stderr, "Prism host token required; run login/bind first or pass -host-token")
		return 1
	}
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logWriter, logCloser, err := observability.NewServeLogWriter(stderr)
	if err != nil {
		logWriter = stderr
	}
	if logCloser != nil {
		defer logCloser.Close()
	}
	logger := slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: level}))
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	statePath := daemonRuntimeStatePath()
	var runtimeStateMu sync.Mutex
	updateRuntimeState := func(update func(*daemonRuntimeState)) {
		runtimeStateMu.Lock()
		defer runtimeStateMu.Unlock()
		current, _ := readDaemonRuntimeState(statePath)
		state := daemonRuntimeState{DaemonPID: os.Getpid()}
		if current != nil {
			state = *current
			state.DaemonPID = os.Getpid()
		}
		state.UpdatedAt = ""
		update(&state)
		_ = writeDaemonRuntimeState(statePath, state)
	}
	var lastConnectionState atomic.Value
	lastConnectionState.Store("starting")
	updateRuntimeState(func(state *daemonRuntimeState) {
		state.DaemonPID = os.Getpid()
		state.ConnectionState = "connecting"
		state.ConnectionDetail = "正在启动 Prism Realtime 连接。"
	})
	defer func() {
		lastState, _ := lastConnectionState.Load().(string)
		switch strings.ToLower(strings.TrimSpace(lastState)) {
		case "auth_expired", "unauthorized":
			return
		default:
			updateRuntimeState(func(state *daemonRuntimeState) {
				state.ConnectionState = "stopped"
				state.ConnectionDetail = "Prism Hub 已停止连接。"
			})
		}
	}()
	var keepAwake *host.KeepAwakeHandle
	if cfg.Bridge.KeepAwake {
		handle, err := host.StartKeepAwake(ctx, logger)
		if err != nil {
			logger.Warn("keep-awake unavailable", "err", err)
		} else {
			keepAwake = handle
			logger.Info("keep-awake enabled", "method", handle.Method())
		}
	}
	if keepAwake != nil {
		defer keepAwake.Stop()
	}
	hostInfoHeader, authErr := prismapi.FetchHostInfoHeader(apiURL, cfg.Prism.HostToken)
	if authErr != nil {
		logger.Warn("resolve Prism host info failed; realtime gateway may inject it", "err", authErr)
	}
	// Build the router before the transport client so the OnConnect closure
	// can capture it. The Sink is wired after the client exists.
	runTracker := bridge.NewRunTracker()
	detailSnapshots := bridge.NewDetailSnapshotStore()
	service := newBridgeServiceFromConfig(cfg)
	setDashboardRuntimePlugins(service)
	defer func() {
		if currentDashboardRuntimePlugins() == service {
			setDashboardRuntimePlugins(nil)
		}
	}()
	hostPlugins := newHostPluginOverlay()
	var (
		snapshotPublisherMu sync.RWMutex
		snapshotPublisher   *bridge.SnapshotPublisher
	)
	refreshHostPlugins := startHostPluginSyncWithStatus(cfg, configPath, service, hostPlugins, logger, func() {
		snapshotPublisherMu.RLock()
		pub := snapshotPublisher
		snapshotPublisherMu.RUnlock()
		if pub != nil {
			// Bound the republish so a stuck transport cannot stall the sync
			// loop that triggered it.
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer publishCancel()
			pub.PublishAll(publishCtx)
		}
	}, func(syncStatus hostPluginSyncStatus) {
		updateRuntimeState(func(state *daemonRuntimeState) {
			state.HostPluginSyncState = syncStatus.State
			state.HostPluginSyncDetail = syncStatus.Detail
			state.HostPluginSyncUpdated = syncStatus.Updated.UTC().Format(time.RFC3339)
			state.HostPluginCount = syncStatus.Count
		})
	})
	router := &bridge.Router{
		Service:         service,
		DB:              db,
		Config:          cfg,
		ConfigProvider:  currentBridgeConfigProvider(cfg, configPath, service, hostPlugins),
		Logger:          logger,
		Runs:            runTracker,
		DetailSnapshots: detailSnapshots,
	}
	client := prismtransport.New(prismtransport.Options{
		URL:            realtimeURL,
		Token:          cfg.Prism.HostToken,
		HostInfoHeader: hostInfoHeader,
		Logger:         logger,
		OnStateChange: func(status, detail string) {
			lastConnectionState.Store(strings.TrimSpace(status))
			updateRuntimeState(func(state *daemonRuntimeState) {
				state.ConnectionState = status
				state.ConnectionDetail = detail
			})
		},
		OnConnect: func(connectCtx context.Context, _ *prismtransport.Client) {
			snapshotPublisherMu.RLock()
			pub := snapshotPublisher
			snapshotPublisherMu.RUnlock()
			if pub != nil {
				// A reconnect commonly follows a lock-screen network pause. Republish
				// the last complete local mirror before performing any cloud read so
				// Mobile can recover its home view immediately.
				pub.PublishAll(connectCtx)
			}
			if refreshHostPlugins() {
				snapshotPublisherMu.RLock()
				pub := snapshotPublisher
				snapshotPublisherMu.RUnlock()
				if pub != nil {
					// Refresh may have changed the cloud-authorized Plugin set.
					pub.PublishAll(connectCtx)
				}
			}
			snapshotPublisherMu.RLock()
			pub = snapshotPublisher
			snapshotPublisherMu.RUnlock()
			if pub != nil {
				// Process adapters can need a short bootstrap window before their
				// first ListSessions succeeds. PublishAll deliberately does not turn
				// that transient failure into an empty index; retry only the still
				// missing scopes after the adapter has had time to become ready.
				go func() {
					timer := time.NewTimer(time.Second)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return
					case <-timer.C:
					}
					retryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
					defer cancel()
					if err := pub.EnsureConversationIndexes(retryCtx); err != nil {
						logger.Warn("retry initial conversation index snapshot failed", "err", err)
						return
					}
					scopes, conversations := pub.ConversationIndexSummary()
					logger.Info("initial conversation index retry complete",
						"index_plugin_scopes", scopes,
						"indexed_conversations", conversations)
				}()
			}
		},
	})
	router.Sink = client
	createdPublisher := bridge.NewSnapshotPublisher(
		router.Service,
		router.DB,
		router.Config,
		router.ConfigProvider,
		client,
		detailSnapshots,
		logger,
	)
	snapshotPublisherMu.Lock()
	snapshotPublisher = createdPublisher
	snapshotPublisherMu.Unlock()
	router.SnapshotPublisher = createdPublisher
	reverseSyncer := &bridge.ReverseSyncer{
		Service:           router.Service,
		Config:            cfg,
		ConfigProvider:    router.ConfigProvider,
		DB:                db,
		Sink:              client,
		Logger:            logger,
		Runs:              runTracker,
		DetailSnapshots:   detailSnapshots,
		SnapshotPublisher: createdPublisher,
		Drafts:            router,
	}
	pluginReloader := newPluginReloader(cfg, configPath, router.Service, hostPlugins, logger)
	setDashboardRuntimePluginReloader(pluginReloader)
	defer func() {
		if currentDashboardRuntimePluginReloader() == pluginReloader {
			setDashboardRuntimePluginReloader(nil)
		}
		// Do not rely on the asynchronous reloader goroutine during process exit.
		// Process adapters own separate process groups and must be reaped before
		// the Desktop sidecar returns control to its supervisor.
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if pluginReloader != nil {
			pluginReloader.Close(shutdownCtx)
		}
		if err := router.Service.Close(shutdownCtx); err != nil {
			logger.Warn("plugin runtime shutdown incomplete", "err", err)
		}
	}()
	if pluginReloader != nil {
		pluginReloader.OnChange = func(pluginIDs []string) {
			for _, pluginID := range pluginIDs {
				if err := snapshotPublisher.InvalidatePluginRuntime(pluginID); err != nil {
					logger.Warn("invalidate reloaded plugin detail failed", "plugin_id", pluginID, "err", err)
				}
				snapshotPublisher.InvalidateConversationIndex(pluginID)
			}
			snapshotPublisher.PublishAll(ctx)
			reverseSyncer.TriggerSync(context.Background())
			// A hot-reloaded manifest may add capabilities or change its runtime
			// version. Refresh only already-authorized cloud records in the
			// background; do not hold up the reloader or expose newly discovered
			// local plugins remotely.
			go func() {
				if refreshHostPlugins() {
					snapshotPublisher.PublishAll(context.Background())
				}
			}()
		}
		go pluginReloader.Run(ctx)
	}
	go pruneBridgeEvents(ctx, db, logger)
	go reverseSyncer.Run(ctx)
	go func() {
		if err := client.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("Prism Realtime stopped", "err", err)
			cancel()
		}
	}()
	fmt.Fprintf(stdout, "prism runtime running: config=%s db=%s realtime=%s\n",
		configPath, dbPath, realtimeURL)
	queryService := &bridge.QueryService{
		DB:              db,
		History:         &bridge.HistoryService{DB: db},
		Service:         router.Service,
		ConfigProvider:  router.ConfigProvider,
		DetailSnapshots: detailSnapshots,
		Logger:          logger,
	}
	for {
		select {
		case <-ctx.Done():
			return 0
		case in, ok := <-client.Inbound():
			if !ok {
				return 0
			}
			go func(in prismtransport.Inbound) {
				switch in.MessageType {
				case prismtransport.MsgHostPluginRefresh:
					refreshHostPlugins()
					snapshotPublisher.PublishAll(ctx)
				case prismtransport.MsgPluginRuntimeSwitch:
					logger.Info("plugin runtime switch received", "msg_id", in.MessageID,
						"plugin_id", inboundPluginID(in), "runtime_mode", inboundRuntimeMode(in))
					routeCtx, routeCancel := context.WithTimeout(ctx, 30*time.Second)
					defer routeCancel()
					if err := handlePluginRuntimeSwitch(routeCtx, client, configPath, pluginReloader, snapshotPublisher, in); err != nil {
						logger.Warn("plugin runtime switch failed", "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgDesktopLaunch:
					logger.Info("desktop launch received", "msg_id", in.MessageID,
						"plugin_id", inboundPluginID(in))
					routeCtx, routeCancel := context.WithTimeout(ctx, 90*time.Second)
					defer routeCancel()
					if err := handleDesktopLaunch(routeCtx, client, configPath, snapshotPublisher, in); err != nil {
						logger.Warn("desktop launch failed", "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgConversationStart:
					routeCtx, routeCancel := context.WithTimeout(ctx, 2*time.Minute)
					defer routeCancel()
					if err := handleConversationStart(routeCtx, client, router, in); err != nil {
						logger.Warn("conversation start failed", "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgConversationDraftOpen, prismtransport.MsgConversationDraftControl, prismtransport.MsgConversationDraftReopen, prismtransport.MsgConversationDraftStart, prismtransport.MsgConversationDraftWatch, prismtransport.MsgConversationDraftUnwatch:
					routeCtx, routeCancel := context.WithTimeout(ctx, 2*time.Minute)
					defer routeCancel()
					if err := handleConversationDraft(routeCtx, client, router, in); err != nil {
						logger.Warn("conversation draft command failed", "type", in.MessageType, "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgConversationMenuSessionDescribe, prismtransport.MsgConversationMenuSessionApply,
					prismtransport.MsgConversationDraftMenuSessionDescribe, prismtransport.MsgConversationDraftMenuSessionApply:
					routeCtx, routeCancel := context.WithTimeout(ctx, 45*time.Second)
					defer routeCancel()
					if err := handleConversationMenuSession(routeCtx, client, router, in, logger); err != nil {
						logger.Warn("conversation menu session command failed", "type", in.MessageType, "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgChatSend:
					routeCtx, routeCancel := context.WithTimeout(ctx, 2*time.Minute)
					defer routeCancel()
					if err := handleChatSend(routeCtx, client, router, in); err != nil {
						logger.Warn("chat send failed", "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgTaskInterrupt, prismtransport.MsgApprovalResolve, prismtransport.MsgSessionControl:
					routeCtx, routeCancel := context.WithTimeout(ctx, 2*time.Minute)
					defer routeCancel()
					if _, err := router.HandleInbound(routeCtx, in); err != nil {
						logger.Warn("bridge route failed", "msg_id", in.MessageID, "err", err)
					}
				case prismtransport.MsgQueryRequest:
					queryCtx, queryCancel := context.WithTimeout(ctx, 15*time.Second)
					defer queryCancel()
					queryStartedAt := time.Now()
					if in.Payload != nil {
						logger.Info("bridge query received",
							"msg_id", in.MessageID,
							"request_id", in.Payload.RequestID,
							"query_type", in.Payload.QueryType,
							"conversation_id", in.Payload.ConversationID,
							"plugin_id", in.Payload.PluginID,
							"body_stream_id", in.Payload.BodyStreamID,
							"page_id", in.Payload.PageID,
							"live", in.Payload.Live)
					} else {
						logger.Info("bridge query received", "msg_id", in.MessageID)
					}
					if err := handleBridgeQuery(queryCtx, client, queryService, in); err != nil {
						logger.Warn("bridge query failed", "msg_id", in.MessageID, "cost_ms", time.Since(queryStartedAt).Milliseconds(), "err", err)
					} else if in.Payload != nil {
						logger.Info("bridge query response sent",
							"msg_id", in.MessageID,
							"request_id", in.Payload.RequestID,
							"query_type", in.Payload.QueryType,
							"conversation_id", in.Payload.ConversationID,
							"plugin_id", in.Payload.PluginID,
							"cost_ms", time.Since(queryStartedAt).Milliseconds())
					} else {
						logger.Info("bridge query response sent", "msg_id", in.MessageID, "cost_ms", time.Since(queryStartedAt).Milliseconds())
					}
				}
			}(in)
		}
	}
}

func pruneBridgeEvents(ctx context.Context, db *prismdb.DB, logger *slog.Logger) {
	if db == nil {
		return
	}
	prune := func() {
		pruneCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		deleted, err := db.PruneEvents(pruneCtx, time.Now().UTC().Add(-bridgeEventRetention), prismdb.DefaultEventRetentionLimit)
		if err != nil {
			if logger != nil {
				logger.Warn("prune bridge events failed", "err", err)
			}
			return
		}
		if deleted > 0 && logger != nil {
			logger.Info("pruned bridge events", "count", deleted)
		}
	}
	prune()
	ticker := time.NewTicker(bridgeEventPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prune()
		}
	}
}

func inboundPluginID(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.PluginID)
}

func inboundRuntimeMode(in prismtransport.Inbound) string {
	if in.Payload == nil {
		return ""
	}
	return strings.TrimSpace(in.Payload.RuntimeMode)
}

// handleDesktopLaunch answers a remote managed launch request. The action
// stays explicit end to end: the cloud may only relay a user-confirmed launch,
// and this handler re-validates the manifest-declared launch targets and
// platform gates before touching anything. Launch targets are addressed by the
// plugin; kind/app/start semantics live in the plugin manifest
// (managed_launch_specs) — the hub never recognizes plugin IDs here.
func handleDesktopLaunch(ctx context.Context, client *prismtransport.Client, configPath string, publisher *bridge.SnapshotPublisher, in prismtransport.Inbound) error {
	response := prismtransport.DesktopLaunchResultPayload{RequestID: strings.TrimSpace(in.MessageID)}
	launchKind := ""
	if in.Payload != nil {
		response.RequestID = firstNonEmpty(strings.TrimSpace(in.Payload.RequestID), response.RequestID)
		response.PluginID = strings.ToLower(strings.TrimSpace(in.Payload.PluginID))
		launchKind = strings.ToLower(strings.TrimSpace(in.Payload.LaunchKind))
	}
	if client == nil {
		response.Code = "desktop_launch_unavailable"
		response.Message = "Hub realtime client is unavailable"
	} else if response.PluginID == "" {
		response.Code = "invalid_desktop_launch"
		response.Message = "plugin_id is required"
	} else if cfg, err := bridgeconfig.Load(configPath); err != nil {
		response.Code = "desktop_launch_failed"
		response.Message = err.Error()
	} else if discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg)); err != nil {
		response.Code = "desktop_launch_failed"
		response.Message = err.Error()
	} else {
		var manifest *pluginpkg.Manifest
		for index := range discovered {
			if strings.EqualFold(discovered[index].Manifest.ID, response.PluginID) {
				manifest = &discovered[index].Manifest
				break
			}
		}
		if manifest == nil {
			response.Code = "plugin_not_found"
			response.Message = "local plugin not found"
		} else {
			configuredMode := ""
			if pluginCfg, ok := cfg.Bridge.Plugins[manifest.ID]; ok {
				configuredMode = pluginCfg.Mode
			}
			spec, specErr := selectLaunchSpec(*manifest, configuredMode, launchKind)
			if specErr != nil {
				response.Code = "desktop_launch_failed"
				response.Message = specErr.Error()
			} else if spec.Kind == pluginpkg.LaunchKindDesktopApp {
				appSpec, ok := dashboardDesktopAppSpecByID(spec.App)
				if !ok {
					response.Code = "desktop_launch_failed"
					response.Message = fmt.Sprintf("hub has no desktop app spec for %q (declared by plugin %s)", spec.App, manifest.ID)
				} else {
					result, err := dashboardStartManagedDesktopApp(cfg, appSpec)
					if err != nil {
						response.Code = "desktop_launch_failed"
						response.Message = err.Error()
					} else {
						response.Accepted = true
						message := strings.TrimSpace(fmt.Sprint(result["message"]))
						if message == "" || message == "<nil>" {
							message = "desktop app launched"
						}
						response.Message = message
						if port := strings.TrimSpace(fmt.Sprint(result["cdp_port"])); port != "" && port != "<nil>" {
							response.CDPPort = port
						}
					}
				}
			} else if err := startManagedService(ctx, *manifest, spec, publisher); err != nil {
				response.Code = "desktop_launch_failed"
				response.Message = err.Error()
			} else {
				response.Accepted = true
				response.Message = fmt.Sprintf("%s service started", manifest.ID)
			}
			if response.Accepted && publisher != nil {
				// The freshly launched target changes plugin availability;
				// republish so remote clients see it without waiting for the
				// next periodic snapshot.
				publisher.PublishAll(ctx)
			}
		}
	}
	if client != nil {
		if err := client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgDesktopLaunchResult, Payload: response}); err != nil {
			return err
		}
	}
	if !response.Accepted {
		return fmt.Errorf("%s: %s", firstNonEmpty(response.Code, "desktop_launch_failed"), response.Message)
	}
	return nil
}

// selectLaunchSpec picks the manifest launch spec for a remote request: an
// explicit kind wins, otherwise specs already filtered to the plugin's active
// runtime mode are searched probe-first (a probe-linked target is the one that
// restores remote availability).
func selectLaunchSpec(manifest pluginpkg.Manifest, configuredMode, launchKind string) (pluginpkg.ManagedLaunchSpec, error) {
	specs := manifest.LaunchSpecsForMode(configuredMode)
	if len(specs) == 0 {
		return pluginpkg.ManagedLaunchSpec{}, fmt.Errorf("plugin %s declares no managed launch target for runtime mode %q",
			manifest.ID, manifest.RuntimeMode(configuredMode))
	}
	if launchKind != "" {
		for _, spec := range specs {
			if spec.Kind == launchKind {
				return spec, nil
			}
		}
		return pluginpkg.ManagedLaunchSpec{}, fmt.Errorf("plugin %s declares no %q launch target for runtime mode %q",
			manifest.ID, launchKind, manifest.RuntimeMode(configuredMode))
	}
	for _, spec := range specs {
		if spec.Ready == pluginpkg.LaunchReadyProbe {
			return spec, nil
		}
	}
	return specs[0], nil
}

// startManagedService executes a manifest-declared service launch target:
// the start argv runs directly (PATH-resolved, never a shell) with the spec's
// ready policy as success contract — exit_ok accepts a zero exit, probe polls
// the plugin's live availability until the ready timeout.
func startManagedService(ctx context.Context, manifest pluginpkg.Manifest, spec pluginpkg.ManagedLaunchSpec, publisher *bridge.SnapshotPublisher) error {
	bin, err := exec.LookPath(spec.Start[0])
	if err != nil {
		return fmt.Errorf("launch command %q not found in PATH", spec.Start[0])
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(spec.ReadyTimeoutMs)*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(runCtx, bin, spec.Start[1:]...)
	output, runErr := cmd.CombinedOutput()
	if runErr != nil {
		detail := strings.TrimSpace(string(output))
		if detail == "" {
			detail = "no output"
		}
		if len(detail) > 400 {
			detail = detail[len(detail)-400:]
		}
		if errors.Is(runErr, context.DeadlineExceeded) {
			return fmt.Errorf("launch command did not finish within %dms: %s", spec.ReadyTimeoutMs, detail)
		}
		return fmt.Errorf("launch command failed: %s", detail)
	}
	if spec.Ready == pluginpkg.LaunchReadyExitOK {
		return nil
	}
	// Ready policy "probe": the launch only counts when the plugin itself
	// reports Available (that is what restores remote control).
	if publisher == nil || publisher.Service == nil {
		return nil
	}
	deadline := time.Now().Add(time.Duration(spec.ReadyTimeoutMs) * time.Millisecond)
	adapter := publisher.Service.Adapter(manifest.ID)
	for {
		if adapter != nil {
			if status := publisher.Service.AdapterStatus(ctx, manifest.ID, adapter); status.Available {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service started, but plugin %s did not become available within %dms", manifest.ID, spec.ReadyTimeoutMs)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func handlePluginRuntimeSwitch(ctx context.Context, client *prismtransport.Client, configPath string, reloader *pluginpkg.Reloader, publisher *bridge.SnapshotPublisher, in prismtransport.Inbound) error {
	response := prismtransport.PluginRuntimeSwitchResultPayload{RequestID: strings.TrimSpace(in.MessageID)}
	if in.Payload != nil {
		response.RequestID = firstNonEmpty(strings.TrimSpace(in.Payload.RequestID), response.RequestID)
		response.PluginID = strings.ToLower(strings.TrimSpace(in.Payload.PluginID))
		response.RuntimeMode = strings.ToLower(strings.TrimSpace(in.Payload.RuntimeMode))
	}
	if client == nil || reloader == nil {
		response.Code = "plugin_runtime_unavailable"
		response.Message = "Hub plugin runtime is unavailable"
	} else if response.PluginID == "" || response.RuntimeMode == "" {
		response.Code = "invalid_plugin_runtime_switch"
		response.Message = "plugin_id and runtime_mode are required"
	} else {
		cfg, err := bridgeconfig.Load(configPath)
		if err != nil {
			response.Code = "plugin_runtime_config_failed"
			response.Message = err.Error()
		} else if discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg)); err != nil {
			response.Code = "plugin_runtime_manifest_failed"
			response.Message = err.Error()
		} else {
			var manifest *pluginpkg.Manifest
			for index := range discovered {
				if strings.EqualFold(discovered[index].Manifest.ID, response.PluginID) {
					manifest = &discovered[index].Manifest
					break
				}
			}
			if manifest == nil {
				response.Code = "plugin_not_found"
				response.Message = "local plugin not found"
			} else if !manifest.AllowsRuntimeMode(response.RuntimeMode) {
				response.Code = "runtime_mode_unsupported"
				response.Message = "plugin does not declare requested runtime mode"
			} else {
				previousConfig, existed := cfg.Bridge.Plugins[manifest.ID]
				config := previousConfig
				config.Mode = response.RuntimeMode
				cfg.Bridge.Plugins[manifest.ID] = config
				if err := cfg.Save(configPath); err != nil {
					response.Code = "plugin_runtime_config_failed"
					response.Message = err.Error()
				} else {
					var invalidateErr error
					err := reloader.ReloadPluginWithTransition(ctx, manifest.ID, func() {
						if publisher != nil {
							invalidateErr = publisher.InvalidatePluginRuntime(manifest.ID)
						}
					})
					if err != nil {
						// The reloader reads the persisted config to construct its new
						// adapter. Restore the prior value if replacement failed so the
						// next normal reload retains the currently working runtime.
						if existed {
							cfg.Bridge.Plugins[manifest.ID] = previousConfig
						} else {
							delete(cfg.Bridge.Plugins, manifest.ID)
						}
						if rollbackErr := cfg.Save(configPath); rollbackErr != nil {
							response.Message = fmt.Sprintf("%v; restore previous runtime mode: %v", err, rollbackErr)
						}
						response.Code = "plugin_runtime_reload_failed"
						if response.Message == "" {
							response.Message = err.Error()
						}
					} else {
						response.PluginID = manifest.ID
						response.Accepted = true
						response.Message = "plugin runtime replaced; wait for catalog availability"
						if invalidateErr != nil {
							response.Message = fmt.Sprintf("%s; invalidate previous detail: %v", response.Message, invalidateErr)
						}
						if publisher != nil {
							// A runtime mode may address a different native session store.
							// Rebuild this plugin's complete index before it is republished,
							// so remote clients cannot open stale mode-specific bindings.
							publisher.InvalidateConversationIndex(manifest.ID)
							publisher.PublishAll(ctx)
						}
					}
				}
			}
		}
	}
	if client != nil {
		if err := client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgPluginRuntimeSwitchResult, Payload: response}); err != nil {
			return err
		}
	}
	if !response.Accepted {
		return fmt.Errorf("%s: %s", firstNonEmpty(response.Code, "plugin_runtime_switch_failed"), response.Message)
	}
	return nil
}

func handleChatSend(ctx context.Context, client *prismtransport.Client, router *bridge.Router, in prismtransport.Inbound) error {
	if client == nil || router == nil {
		return fmt.Errorf("chat.send dependencies unavailable")
	}
	payload := in.Payload
	response := prismtransport.ChatSendResultPayload{RequestID: strings.TrimSpace(in.MessageID), Accepted: false}
	if payload != nil {
		response.RequestID = firstNonEmpty(strings.TrimSpace(payload.RequestID), response.RequestID)
		response.PluginID = strings.TrimSpace(payload.PluginID)
		response.ConversationID = strings.TrimSpace(payload.ConversationID)
		response.MessageID = firstNonEmpty(strings.TrimSpace(payload.ClientMessageID), response.RequestID)
	}
	result, routeErr := router.HandleInbound(ctx, in)
	response.PluginID = firstNonEmpty(response.PluginID, result.PluginID)
	response.ConversationID = firstNonEmpty(response.ConversationID, result.ConversationID)
	if routeErr != nil {
		response.Code = "send_failed"
		response.Message = routeErr.Error()
	} else {
		response.Accepted = result.Receipt.Accepted
		response.Visible = result.Receipt.Visible
		response.PendingConfirmation = result.Receipt.PendingConfirmation
		response.QueuePending = result.Receipt.QueuePending
		response.Message = result.Receipt.Detail
	}
	if sendErr := client.SendCriticalControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgChatSendResult,
		Payload:     response,
	}); sendErr != nil {
		return sendErr
	}
	return routeErr
}

func handleConversationStart(ctx context.Context, client *prismtransport.Client, router *bridge.Router, in prismtransport.Inbound) error {
	if client == nil || router == nil {
		return fmt.Errorf("conversation.start dependencies unavailable")
	}
	requestID := strings.TrimSpace(in.MessageID)
	payload := in.Payload
	pluginID := ""
	messageID := ""
	if payload != nil {
		requestID = firstNonEmpty(strings.TrimSpace(payload.RequestID), requestID)
		pluginID = strings.TrimSpace(payload.PluginID)
		messageID = strings.TrimSpace(payload.ClientMessageID)
	}
	response := prismtransport.ConversationStartResultPayload{
		RequestID: requestID,
		PluginID:  pluginID,
		MessageID: messageID,
		Accepted:  false,
	}
	result, err := router.HandleConversationStart(ctx, in)
	if err != nil {
		response.Code = conversationStartFailureCode(err)
		response.Message = err.Error()
	} else {
		response.PluginID = firstNonEmpty(response.PluginID, result.PluginID)
		response.ConversationID = result.ConversationID
		response.Accepted = true
	}
	if sendErr := client.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgConversationStartResult,
		Payload:     response,
	}); sendErr != nil {
		return sendErr
	}
	return err
}

func conversationStartFailureCode(err error) string {
	var coded *pluginruntime.AdapterError
	if errors.As(err, &coded) {
		switch coded.Code {
		case "workspace_trust_required", "session_start_timeout":
			return coded.Code
		}
	}
	return "conversation_start_failed"
}

func handleConversationDraft(ctx context.Context, client *prismtransport.Client, router *bridge.Router, in prismtransport.Inbound) error {
	if client == nil || router == nil {
		return fmt.Errorf("conversation draft dependencies unavailable")
	}
	payload := in.Payload
	response := prismtransport.ConversationDraftResultPayload{Accepted: false}
	if payload != nil {
		response.RequestID = firstNonEmpty(strings.TrimSpace(payload.RequestID), strings.TrimSpace(in.MessageID))
		response.PluginID = strings.TrimSpace(payload.PluginID)
		response.DraftID = strings.TrimSpace(payload.DraftID)
		response.DraftStreamID = strings.TrimSpace(payload.DraftStreamID)
		response.Cwd = strings.TrimSpace(payload.Cwd)
		response.MessageID = strings.TrimSpace(payload.ClientMessageID)
	} else {
		response.RequestID = strings.TrimSpace(in.MessageID)
	}
	var result bridge.RouteResult
	var err error
	switch in.MessageType {
	case prismtransport.MsgConversationDraftOpen:
		result, err = router.HandleConversationDraftOpen(ctx, in)
	case prismtransport.MsgConversationDraftControl:
		result, err = router.HandleConversationDraftControl(ctx, in)
	case prismtransport.MsgConversationDraftReopen:
		result, err = router.HandleConversationDraftReopen(ctx, in)
	case prismtransport.MsgConversationDraftStart:
		result, err = router.HandleConversationDraftStart(ctx, in)
	case prismtransport.MsgConversationDraftWatch:
		result, err = router.HandleConversationDraftWatch(ctx, in)
	case prismtransport.MsgConversationDraftUnwatch:
		result, err = router.HandleConversationDraftUnwatch(ctx, in)
	default:
		err = fmt.Errorf("unsupported draft command: %s", in.MessageType)
	}
	if err != nil {
		response.Code = "conversation_draft_failed"
		if in.MessageType == prismtransport.MsgConversationDraftStart {
			if code := conversationStartFailureCode(err); code != "conversation_start_failed" {
				response.Code = code
			}
		}
		if strings.Contains(err.Error(), "draft_stale") {
			response.Code = "draft_stale"
			response.DraftState = "draft_stale"
		}
		response.Message = err.Error()
	} else {
		response.PluginID = firstNonEmpty(response.PluginID, result.PluginID)
		response.DraftID = firstNonEmpty(result.DraftID, response.DraftID)
		response.DraftState = firstNonEmpty(result.DraftState, "open")
		response.Cwd = firstNonEmpty(result.Cwd, response.Cwd)
		response.Controls = result.Controls
		response.ConversationID = result.ConversationID
		response.Accepted = true
	}
	resultType := prismtransport.MsgConversationDraftOpenResult
	if in.MessageType == prismtransport.MsgConversationDraftControl {
		resultType = prismtransport.MsgConversationDraftControlResult
	} else if in.MessageType == prismtransport.MsgConversationDraftStart {
		resultType = prismtransport.MsgConversationDraftStartResult
	} else if in.MessageType == prismtransport.MsgConversationDraftReopen {
		resultType = prismtransport.MsgConversationDraftOpenResult
	} else if in.MessageType == prismtransport.MsgConversationDraftWatch {
		resultType = prismtransport.MsgConversationDraftWatchResult
	} else if in.MessageType == prismtransport.MsgConversationDraftUnwatch {
		resultType = prismtransport.MsgConversationDraftUnwatchResult
	}
	if sendErr := client.SendControl(prismtransport.ControlOutbound{MessageType: resultType, Payload: response}); sendErr != nil {
		return sendErr
	}
	return err
}

func handleConversationMenuSession(ctx context.Context, client *prismtransport.Client, router *bridge.Router, in prismtransport.Inbound, logger *slog.Logger) error {
	if client == nil || router == nil {
		return fmt.Errorf("conversation menu session dependencies unavailable")
	}
	payload := in.Payload
	response := prismtransport.MenuSessionResultPayload{Accepted: false, RequestID: strings.TrimSpace(in.MessageID)}
	if payload != nil {
		response.PluginID = strings.TrimSpace(payload.PluginID)
		response.ConversationID = strings.TrimSpace(payload.ConversationID)
		response.DraftID = strings.TrimSpace(payload.DraftID)
	}
	var result bridge.RouteResult
	var err error
	switch in.MessageType {
	case prismtransport.MsgConversationMenuSessionDescribe:
		result, err = router.HandleConversationMenuSessionDescribe(ctx, in)
	case prismtransport.MsgConversationMenuSessionApply:
		result, err = router.HandleConversationMenuSessionApply(ctx, in)
	case prismtransport.MsgConversationDraftMenuSessionDescribe:
		result, err = router.HandleConversationDraftMenuSessionDescribe(ctx, in)
	case prismtransport.MsgConversationDraftMenuSessionApply:
		result, err = router.HandleConversationDraftMenuSessionApply(ctx, in)
	default:
		err = fmt.Errorf("unsupported menu session command: %s", in.MessageType)
	}
	if err != nil {
		response.Message = err.Error()
		if in.MessageType == prismtransport.MsgConversationDraftMenuSessionDescribe || in.MessageType == prismtransport.MsgConversationDraftMenuSessionApply {
			response.Code = "draft_stale"
		} else {
			response.Code = "control_target_stale"
		}
	} else {
		response.Accepted = true
		response.PluginID = firstNonEmpty(response.PluginID, result.PluginID)
		response.ConversationID = firstNonEmpty(response.ConversationID, result.ConversationID)
		response.DraftID = firstNonEmpty(response.DraftID, result.DraftID)
		if result.MenuSession != nil && (in.MessageType == prismtransport.MsgConversationMenuSessionDescribe || in.MessageType == prismtransport.MsgConversationDraftMenuSessionDescribe) {
			response.MenuSessionID = result.MenuSession.ID
			response.Owner = result.MenuSession.Owner
			response.ExpiresAt = result.MenuSession.ExpiresAt.UnixMilli()
			response.Entries = make([]prismtransport.MenuSessionEntry, 0, len(result.MenuSession.Entries))
			for _, entry := range result.MenuSession.Entries {
				response.Entries = append(response.Entries, prismtransport.MenuSessionEntry{
					EntryID: entry.EntryID, Label: entry.Label, Available: entry.Available, Current: entry.Current,
				})
			}
			response.Groups = make([]prismtransport.MenuSessionGroup, 0, len(result.MenuSession.Groups))
			for _, group := range result.MenuSession.Groups {
				out := prismtransport.MenuSessionGroup{
					GroupID: group.GroupID,
					Label:   group.Label,
					Entries: make([]prismtransport.MenuSessionEntry, 0, len(group.Entries)),
				}
				for _, entry := range group.Entries {
					out.Entries = append(out.Entries, prismtransport.MenuSessionEntry{
						EntryID: entry.EntryID, Label: entry.Label, Available: entry.Available, Current: entry.Current,
					})
				}
				response.Groups = append(response.Groups, out)
			}
			if logger == nil {
				logger = slog.Default()
			}
			logger.Info("conversation menu session described",
				"request_id", response.RequestID,
				"plugin_id", response.PluginID,
				"conversation_id", response.ConversationID,
				"draft_id", response.DraftID,
				"owner_kind", strings.TrimSpace(fmt.Sprint(result.MenuSession.Owner["kind"])),
				"entry_count", len(response.Entries),
				"group_count", len(response.Groups),
			)
		}
		if len(result.Controls) > 0 {
			response.Controls = result.Controls
		}
		response.Message = "桌面菜单操作已提交，等待状态同步。"
	}
	resultType := prismtransport.MsgConversationMenuSessionDescribeResult
	switch in.MessageType {
	case prismtransport.MsgConversationMenuSessionApply:
		resultType = prismtransport.MsgConversationMenuSessionApplyResult
	case prismtransport.MsgConversationDraftMenuSessionDescribe:
		resultType = prismtransport.MsgConversationDraftMenuSessionDescribeResult
	case prismtransport.MsgConversationDraftMenuSessionApply:
		resultType = prismtransport.MsgConversationDraftMenuSessionApplyResult
	}
	if sendErr := client.SendControl(prismtransport.ControlOutbound{MessageType: resultType, Payload: response}); sendErr != nil {
		return sendErr
	}
	return err
}

func handleBridgeQuery(ctx context.Context, client *prismtransport.Client, service *bridge.QueryService, in prismtransport.Inbound) error {
	if client == nil {
		return fmt.Errorf("realtime client unavailable")
	}
	requestID := strings.TrimSpace(in.MessageID)
	payload := in.Payload
	if payload != nil && strings.TrimSpace(payload.RequestID) != "" {
		requestID = strings.TrimSpace(payload.RequestID)
	}
	queryType := ""
	conversationID := ""
	pluginID := ""
	if payload != nil {
		queryType = strings.TrimSpace(payload.QueryType)
		conversationID = strings.TrimSpace(payload.ConversationID)
		pluginID = strings.TrimSpace(payload.PluginID)
	}
	if queryType == "conversation.history.stream" {
		return handleConversationHistoryStream(ctx, client, service, requestID, conversationID, pluginID, payload)
	}
	if queryType == "conversation.history.cancel" {
		return handleConversationHistoryCancel(client, requestID, conversationID, pluginID, payload)
	}
	responsePayload := prismtransport.QueryResponsePayload{
		RequestID:      requestID,
		QueryType:      queryType,
		ConversationID: conversationID,
		PluginID:       pluginID,
		Success:        false,
	}
	result, err := service.Execute(ctx, payload)
	if err != nil {
		responsePayload.Error = err.Error()
	} else {
		responsePayload.Success = true
		responsePayload.Result = result
	}
	if sendErr := client.SendControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgQueryResponse,
		Payload:     responsePayload,
	}); sendErr != nil {
		return sendErr
	}
	return err
}

func handleConversationHistoryStream(_ context.Context, client *prismtransport.Client, service *bridge.QueryService, requestID, conversationID, pluginID string, payload *prismtransport.InboundPayload) error {
	if payload == nil {
		return fmt.Errorf("query payload required")
	}
	bodyStreamID := strings.TrimSpace(payload.BodyStreamID)
	if bodyStreamID == "" {
		bodyStreamID = uuid.NewString()
	}
	pageID := strings.TrimSpace(payload.PageID)
	if pageID == "" {
		pageID = uuid.NewString()
	}
	// The query request has a short deadline, but the plugin's asynchronous
	// stream must outlive that acknowledgement until it reaches its end frame.
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := service.OpenConversationHistoryStream(streamCtx, conversationID, pluginID, pluginruntime.HistoryStreamRequest{
		StreamID: bodyStreamID,
		Limit:    boundedHistoryTurnLimit(payload.Limit),
		Live:     true,
	})
	if err != nil {
		cancel()
		response := prismtransport.QueryResponsePayload{
			RequestID: requestID, QueryType: "conversation.history.stream", ConversationID: conversationID, PluginID: pluginID, Error: err.Error(),
		}
		return client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgQueryResponse, Payload: response})
	}
	relay, cancelled := registerConversationHistoryRelay(bodyStreamID, stream.ConversationID, stream.PluginID, cancel)
	if cancelled {
		cancel()
		response := prismtransport.QueryResponsePayload{
			RequestID: requestID, QueryType: "conversation.history.stream", ConversationID: stream.ConversationID, PluginID: stream.PluginID, Success: true,
			Result: map[string]any{"body_stream_id": bodyStreamID, "page_id": pageID, "cancelled": true},
		}
		return client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgQueryResponse, Payload: response})
	}
	response := prismtransport.QueryResponsePayload{
		RequestID: requestID, QueryType: "conversation.history.stream", ConversationID: stream.ConversationID, PluginID: stream.PluginID, Success: true,
		Result: map[string]any{"body_stream_id": bodyStreamID, "page_id": pageID},
	}
	if err := client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgQueryResponse, Payload: response}); err != nil {
		unregisterConversationHistoryRelay(bodyStreamID, relay)
		cancel()
		return err
	}
	go relayConversationHistoryStream(streamCtx, cancel, client, requestID, bodyStreamID, pageID, stream, relay)
	return nil
}

func boundedHistoryTurnLimit(limit int) int {
	if limit < 1 {
		return 20 // Compatibility with API versions published before the configured window.
	}
	if limit > 20 {
		return 20
	}
	return limit
}

func handleConversationHistoryCancel(client *prismtransport.Client, requestID, conversationID, pluginID string, payload *prismtransport.InboundPayload) error {
	bodyStreamID := ""
	if payload != nil {
		bodyStreamID = strings.TrimSpace(payload.BodyStreamID)
	}
	if bodyStreamID == "" || conversationID == "" || pluginID == "" {
		return client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgQueryResponse, Payload: prismtransport.QueryResponsePayload{
			RequestID: requestID, QueryType: "conversation.history.cancel", ConversationID: conversationID, PluginID: pluginID,
			Error: "body_stream_id, conversation_id and plugin_id required",
		}})
	}
	cancelled := cancelConversationHistoryRelay(bodyStreamID, conversationID, pluginID)
	return client.SendCriticalControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgQueryResponse, Payload: prismtransport.QueryResponsePayload{
		RequestID: requestID, QueryType: "conversation.history.cancel", ConversationID: conversationID, PluginID: pluginID,
		Success: true, Result: map[string]any{"body_stream_id": bodyStreamID, "cancelled": cancelled},
	}})
}

func relayConversationHistoryStream(ctx context.Context, cancel context.CancelFunc, client *prismtransport.Client, requestID, bodyStreamID, pageID string, stream bridge.ConversationHistoryStream, relay *conversationHistoryRelay) {
	defer func() {
		unregisterConversationHistoryRelay(bodyStreamID, relay)
		cancel()
	}()
	sent := map[string]int64{}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-stream.Events:
			if !ok {
				slog.Default().Info("conversation body stream closed",
					"request_id", requestID, "body_stream_id", bodyStreamID,
					"conversation_id", stream.ConversationID, "plugin_id", stream.PluginID)
				publishHistoryRelayFailure(client, requestID, bodyStreamID, pageID, stream, "history stream closed before terminal event")
				return
			}
			publicMessages := []map[string]any(nil)
			if event.Type == "turn" && event.Turn != nil {
				publicMessages = bridge.PublicHistoryMessages(event.Turn.Messages)
			}
			slog.Default().Info("conversation body event received",
				"request_id", requestID, "body_stream_id", bodyStreamID, "page_id", pageID,
				"conversation_id", stream.ConversationID, "plugin_id", stream.PluginID,
				"event_type", event.Type, "source", event.Source, "operation", event.Operation,
				"turn_message_count", len(publicMessages), "attachment_count", publicHistoryAttachmentCount(publicMessages))
			payload := prismtransport.ConversationBodyPayload{
				RequestID: requestID, BodyStreamID: bodyStreamID, PageID: pageID,
				ConversationID: stream.ConversationID, PluginID: stream.PluginID,
				EventType: event.Type,
			}
			switch event.Type {
			case "turn":
				if event.Turn == nil || event.Turn.TurnID == "" {
					continue
				}
				if revision, seen := sent[event.Turn.TurnID]; seen && revision >= event.Turn.Revision {
					continue
				}
				sent[event.Turn.TurnID] = event.Turn.Revision
				payload.Source = firstNonEmpty(event.Source, "initial")
				payload.Operation = firstNonEmpty(event.Operation, "append")
				payload.Turn = map[string]any{
					"turn_id": event.Turn.TurnID, "order_key": event.Turn.OrderKey,
					"revision": event.Turn.Revision, "messages": publicMessages,
				}
			case "page_end", "end":
				payload.Source = firstNonEmpty(event.Source, "initial")
				payload.Operation = firstNonEmpty(event.Operation, "append")
			case "error":
				payload.Source = firstNonEmpty(event.Source, "initial")
				payload.Operation = firstNonEmpty(event.Operation, "append")
				payload.Error = event.Error
				payload.Retryable = event.Retryable
			default:
				continue
			}
			if err := client.SendControl(prismtransport.ControlOutbound{MessageType: prismtransport.MsgConversationBody, Payload: payload}); err != nil {
				slog.Default().Warn("conversation body event send failed",
					"request_id", requestID, "body_stream_id", bodyStreamID,
					"conversation_id", stream.ConversationID, "plugin_id", stream.PluginID, "err", err)
				publishHistoryRelayFailure(client, requestID, bodyStreamID, pageID, stream, "history relay delivery interrupted")
				return
			}
			if event.Type == "end" || event.Type == "error" {
				return
			}
		}
	}
}

func publishHistoryRelayFailure(client *prismtransport.Client, requestID, bodyStreamID, pageID string, stream bridge.ConversationHistoryStream, reason string) {
	if client == nil {
		return
	}
	err := client.SendCriticalControl(prismtransport.ControlOutbound{
		MessageType: prismtransport.MsgConversationBody,
		Payload: prismtransport.ConversationBodyPayload{
			RequestID: requestID, BodyStreamID: bodyStreamID, PageID: pageID,
			ConversationID: stream.ConversationID, PluginID: stream.PluginID,
			Source: "relay", Operation: "replace", EventType: "error",
			Error: reason, Retryable: true,
		},
	})
	if err != nil {
		slog.Default().Warn("conversation body retryable failure send failed",
			"request_id", requestID, "body_stream_id", bodyStreamID,
			"conversation_id", stream.ConversationID, "plugin_id", stream.PluginID, "err", err)
	}
}

// publicHistoryAttachmentCount is diagnostic-only. It deliberately counts
// descriptors after public redaction and never logs attachment names or URLs.
func publicHistoryAttachmentCount(messages []map[string]any) int {
	count := 0
	for _, message := range messages {
		metadata, _ := message["metadata"].(map[string]any)
		attachments, _ := metadata["attachments"].([]map[string]string)
		count += len(attachments)
	}
	return count
}

// The daemon only enters snapshot state through SnapshotPublisher. These
// helpers remain small test/diagnostic entry points; they do not bypass it.
func emitBridgeCatalog(ctx context.Context, router *bridge.Router, logger *slog.Logger) {
	publisher := bridgeSnapshotPublisher(router, logger)
	if publisher == nil {
		return
	}
	if err := publisher.PublishCatalog(ctx, false); err != nil && logger != nil {
		logger.Debug("bridge catalog snapshot send failed", "err", err)
	}
}

func emitBridgeConversationIndex(ctx context.Context, router *bridge.Router, logger *slog.Logger) {
	publisher := bridgeSnapshotPublisher(router, logger)
	if publisher == nil {
		return
	}
	if err := publisher.PublishIndex(ctx, false); err != nil && logger != nil {
		logger.Debug("bridge conversation index snapshot send failed", "err", err)
	}
}

func emitBridgeForegroundDetails(router *bridge.Router, logger *slog.Logger) {
	publisher := bridgeSnapshotPublisher(router, logger)
	if publisher == nil {
		return
	}
	if err := publisher.PublishForegroundDetails(false); err != nil && logger != nil {
		logger.Debug("bridge foreground detail snapshot send failed", "err", err)
	}
}

func bridgeSnapshotPublisher(router *bridge.Router, logger *slog.Logger) *bridge.SnapshotPublisher {
	if router == nil || router.Sink == nil {
		return nil
	}
	if router.SnapshotPublisher == nil {
		router.SnapshotPublisher = bridge.NewSnapshotPublisher(
			router.Service,
			router.DB,
			router.Config,
			router.ConfigProvider,
			router.Sink,
			router.DetailSnapshots,
			logger,
		)
	}
	return router.SnapshotPublisher
}

func runTestCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pluginID := fs.String("plugin", "", "plugin id")
	configPath := fs.String("config", envOr("PRISM_CONFIG", bridgeconfig.DefaultPath()), "path to config.yaml")
	conversationID := fs.String("conversation", "bridge-test", "Prism conversation id used for this daemon test")
	message := fs.String("message", "", "optional test message; default uses an auto-generated marker")
	timeout := fs.Duration("timeout", 90*time.Second, "test timeout")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	payload, code := daemonTestPayload(ctx, *configPath, *pluginID, *conversationID, *message, stderr)
	if *jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(payload); err != nil {
			fmt.Fprintf(stderr, "encode %s test failed: %v\n", cliName, err)
			return 1
		}
		return code
	}
	fmt.Fprintf(stdout, "%s test: plugin_id=%s session=%s accepted=%v visible=%v\n",
		cliName,
		payload["plugin_id"], payload["native_session_id"], payload["accepted"], payload["visible"])
	if detail := strings.TrimSpace(fmt.Sprint(payload["detail"])); detail != "" {
		fmt.Fprintf(stdout, "detail: %s\n", detail)
	}
	if evidence := strings.TrimSpace(fmt.Sprint(payload["evidence"])); evidence != "" {
		fmt.Fprintf(stdout, "evidence: %s\n", evidence)
	}
	if failure := strings.TrimSpace(fmt.Sprint(payload["failure_reason"])); failure != "" {
		fmt.Fprintf(stdout, "failure: %s\n", failure)
	}
	if runStatus := strings.TrimSpace(fmt.Sprint(payload["run_status"])); runStatus != "" {
		fmt.Fprintf(stdout, "run_status: %s\n", runStatus)
	}
	if runSummary := strings.TrimSpace(fmt.Sprint(payload["run_summary"])); runSummary != "" {
		fmt.Fprintf(stdout, "run_summary: %s\n", runSummary)
	}
	return code
}

func daemonTestPayload(ctx context.Context, configPath, pluginID, conversationID, message string, stderr io.Writer) (map[string]any, int) {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load prism config failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	db, err := prismdb.Open(envOr("PRISM_DB", prismdb.DefaultPath()))
	if err != nil {
		fmt.Fprintf(stderr, "open db failed: %v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 1
	}
	defer db.Close()
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		err := fmt.Errorf("plugin id required; pass --plugin")
		fmt.Fprintf(stderr, "%v\n", err)
		return map[string]any{"ok": false, "error": err.Error()}, 2
	}
	svc := newBridgeServiceFromConfig(cfg)
	adapter := svc.Adapter(pluginID)
	if adapter == nil {
		err := fmt.Errorf("plugin adapter not found: %s", pluginID)
		fmt.Fprintf(stderr, "%v\n", err)
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error()}, 1
	}
	marker := strings.TrimSpace(message)
	if marker == "" {
		marker = fmt.Sprintf("PRISM_BRIDGE_TEST_%s", strings.ReplaceAll(time.Now().UTC().Format("20060102T150405.000000000Z"), ".", "_"))
	}
	starter, ok := adapter.(pluginruntime.SessionStarter)
	if !ok {
		err := fmt.Errorf("plugin does not implement startSessionWithMessage: %s", pluginID)
		fmt.Fprintf(stderr, "%v\n", err)
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error(), "marker": marker}, 1
	}
	messageID := "bridge-test-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	operation, err := db.BeginStartOperation(ctx, pluginID, messageID, messageID)
	if err != nil {
		fmt.Fprintf(stderr, "begin start operation failed: %v\n", err)
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error(), "marker": marker}, 1
	}
	started, err := starter.StartSessionWithMessage(ctx, pluginruntime.StartSessionWithMessageRequest{
		PluginID: pluginID,
		Message: pluginruntime.InboundMessage{
			PrismMessageID: messageID,
			Text:           marker,
			SourceDevice:   "bridge-test",
			Timestamp:      time.Now().UTC(),
		},
		SourceDevice: "bridge-test",
	})
	if err != nil {
		fmt.Fprintf(stderr, "start native session failed: %v\n", err)
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error(), "marker": marker}, 1
	}
	session := started.Session
	receipt := started.Receipt
	visibility := started.Visibility
	if strings.TrimSpace(session.NativeSessionID) == "" && strings.TrimSpace(session.NativeThreadID) == "" {
		err := fmt.Errorf("start native session returned no native identity")
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error(), "marker": marker}, 1
	}
	if !receipt.Accepted || !visibility.Visible {
		err := fmt.Errorf("start native session did not produce an accepted visible first message")
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": err.Error(), "marker": marker}, 1
	}
	_, sessionRecord, dbErr := db.CompleteStartOperation(ctx, operation, prismdb.SessionRecord{
		PluginID:           pluginID,
		NativeSessionID:    session.NativeSessionID,
		NativeThreadID:     session.NativeThreadID,
		Surface:            session.Surface,
		Endpoint:           session.Endpoint,
		Cwd:                session.Cwd,
		VisibilityVerified: true,
		VisibilityEvidence: visibility.Evidence,
	}, receipt.NativeMessageID)
	if dbErr != nil {
		fmt.Fprintf(stderr, "persist start binding failed: %v\n", dbErr)
		return map[string]any{"ok": false, "plugin_id": pluginID, "error": dbErr.Error(), "marker": marker}, 1
	}
	conversationID = sessionRecord.PrismConversationID
	_, _ = db.InsertEvent(ctx, prismdb.EventRecord{
		SessionID:           sessionRecord.ID,
		PrismConversationID: conversationID,
		PluginID:            pluginID,
		NativeEventID:       receipt.NativeMessageID,
		EventType:           "message.sent",
		Status:              "accepted",
		Summary:             "bridge test marker accepted by OpenClaw",
		Payload: map[string]any{
			"marker": marker,
			"detail": receipt.Detail,
		},
	})
	_, _ = db.InsertVisibilityCheck(ctx, prismdb.VisibilityCheckRecord{
		PluginID:      pluginID,
		SessionID:     sessionRecord.ID,
		Marker:        marker,
		Visible:       visibility.Visible,
		Evidence:      visibility.Evidence,
		FailureReason: visibility.FailureReason,
		CheckedAt:     visibility.CheckedAt,
	})
	payload := map[string]any{
		"ok":                receipt.Accepted && visibility.Visible,
		"plugin_id":         pluginID,
		"bridge_session_id": sessionRecord.ID,
		"native_session_id": session.NativeSessionID,
		"native_thread_id":  session.NativeThreadID,
		"native_message_id": receipt.NativeMessageID,
		"marker":            marker,
		"accepted":          receipt.Accepted,
		"visible":           visibility.Visible,
		"detail":            receipt.Detail,
		"evidence":          visibility.Evidence,
		"failure_reason":    visibility.FailureReason,
	}
	if waiter, ok := adapter.(pluginruntime.RunWaiter); ok && receipt.NativeMessageID != "" {
		runEvent, waitErr := waiter.WaitForRun(ctx, session, receipt.NativeMessageID)
		if waitErr != nil {
			payload["run_wait_error"] = waitErr.Error()
		} else {
			payload["run_status"] = runEvent.Status
			payload["run_summary"] = runEvent.Summary
			_, _ = db.InsertEvent(ctx, prismdb.EventRecord{
				SessionID:           sessionRecord.ID,
				PrismConversationID: conversationID,
				PluginID:            pluginID,
				NativeEventID:       receipt.NativeMessageID,
				EventType:           runEvent.Type,
				Status:              runEvent.Status,
				Summary:             runEvent.Summary,
				Payload:             runEvent.Payload,
			})
		}
	}
	if err != nil {
		payload["error"] = err.Error()
		return payload, 1
	}
	if !visibility.Visible {
		return payload, 1
	}
	return payload, 0
}

func newBridgeServiceFromConfig(cfg *bridgeconfig.Config) *pluginruntime.Service {
	var adapters []pluginruntime.PluginAdapter
	if cfg != nil {
		if discovered, err := pluginpkg.ScanDirs(pluginDirs(cfg)); err == nil {
			for _, item := range discovered {
				if !bridgePluginAllowed(cfg, item.Manifest.ID) {
					continue
				}
				adapters = append(adapters, pluginpkg.NewProcessAdapterWithEnv(item, bridgePluginEnv(cfg, item)))
			}
		}
	}
	return pluginruntime.New(adapters...)
}

// bridgePluginAllowed only answers whether a discovered plugin should be
// wired into the local managed runtime. It intentionally does not consult pluginruntime.RemoteEnabled(); remote
// enablement is enforced later at catalog/index/router layers so a locally
// managed plugin can stay available for probing and future re-enable.
func bridgePluginAllowed(cfg *bridgeconfig.Config, pluginID string) bool {
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if cfg == nil || pluginID == "" {
		return true
	}
	item, ok := cfg.Bridge.Plugins[pluginID]
	if !ok {
		return true
	}
	if item.Uninstalling {
		return false
	}
	if item.Source != "plugin" {
		return false
	}
	return true
}

func newPluginReloader(cfg *bridgeconfig.Config, configPath string, svc *pluginruntime.Service, overlay *hostPluginOverlay, logger *slog.Logger) *pluginpkg.Reloader {
	if cfg == nil || svc == nil {
		return nil
	}
	configProvider := currentBridgeConfigProvider(cfg, configPath, svc, overlay)
	return &pluginpkg.Reloader{
		DirsProvider: func() []string {
			return pluginDirs(configProvider())
		},
		Service:  svc,
		Reserved: bridgeReservedPluginIDs(cfg),
		Logger:   logger,
		Include: func(item pluginpkg.Discovered) bool {
			current := configProvider()
			return bridgePluginAllowed(current, item.Manifest.ID)
		},
		NewAdapter: func(item pluginpkg.Discovered) pluginruntime.PluginAdapter {
			return pluginpkg.NewProcessAdapterWithEnv(item, bridgePluginEnv(configProvider(), item))
		},
		SignatureExtra: func(item pluginpkg.Discovered) string {
			current := configProvider()
			env := bridgePluginEnv(current, item)
			allowed := bridgePluginAllowed(current, item.Manifest.ID)
			payload := map[string]any{
				"allowed": allowed,
				"env":     env,
			}
			data, err := json.Marshal(payload)
			if err != nil {
				return fmt.Sprint(payload)
			}
			return string(data)
		},
	}
}

func currentBridgeConfigProvider(fallback *bridgeconfig.Config, configPath string, svc *pluginruntime.Service, overlay *hostPluginOverlay) func() *bridgeconfig.Config {
	return func() *bridgeconfig.Config {
		return latestBridgeConfig(fallback, configPath, svc, overlay)
	}
}

func latestBridgeConfig(fallback *bridgeconfig.Config, configPath string, svc *pluginruntime.Service, overlay *hostPluginOverlay) *bridgeconfig.Config {
	cfg := latestBridgeConfigRaw(fallback, configPath)
	return applyHostPluginOverlay(cfg, svc, overlay)
}

func latestBridgeConfigRaw(fallback *bridgeconfig.Config, configPath string) *bridgeconfig.Config {
	path := strings.TrimSpace(configPath)
	if path == "" {
		return fallback
	}
	cfg, err := bridgeconfig.Load(path)
	if err != nil {
		return fallback
	}
	cfg.Normalize()
	return cfg
}

func bridgePluginEnv(cfg *bridgeconfig.Config, item pluginpkg.Discovered) map[string]string {
	if cfg == nil {
		return nil
	}
	out := mergePluginEnv(nil, cfg.Bridge.PluginEnv)
	pluginID := strings.ToLower(strings.TrimSpace(item.Manifest.ID))
	if pluginID != "" {
		if pluginConfig, ok := cfg.Bridge.Plugins[pluginID]; ok {
			out = mergePluginEnv(out, pluginConfig.Env)
		}
		// Runtime selection is manifest-driven. Any Plugin that declares
		// runtime_modes receives the selected value through one Hub-owned
		// environment key; PluginConfig.Env cannot override it.
		if mode := item.Manifest.RuntimeMode(cfg.Bridge.Plugins[pluginID].Mode); mode != "" {
			// NormalizePluginEnv deliberately rejects PRISM_PLUGIN_* keys from
			// configuration. Add this one only after user-provided values have
			// been filtered: it is a Hub-to-Plugin runtime contract, not config.
			if out == nil {
				out = map[string]string{}
			}
			out["PRISM_PLUGIN_MODE"] = mode
		}
	}
	// Desktop-app plugins receive the Hub-detected app path and user data
	// directory through the environment contract so a fresh Hub install works
	// without any manual PRISM_*_APP_PATH configuration. Explicit values from
	// the configuration always win.
	if desktopEnv := autoDetectedDesktopAppEnv(item.Manifest); len(desktopEnv) > 0 {
		if out == nil {
			out = map[string]string{}
		}
		for key, value := range desktopEnv {
			if strings.TrimSpace(out[key]) == "" {
				out[key] = value
			}
		}
	}
	// Desktop-automation plugins shell out to the sqlite3 CLI to read local
	// app state. Resolve it from PATH so they work without a manual
	// PRISM_SQLITE3_BIN configuration.
	if strings.TrimSpace(out["PRISM_SQLITE3_BIN"]) == "" {
		if sqlite3Path, err := exec.LookPath("sqlite3"); err == nil && sqlite3Path != "" {
			out["PRISM_SQLITE3_BIN"] = sqlite3Path
		}
	}
	return out
}

func mergePluginEnv(dst map[string]string, src map[string]string) map[string]string {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]string{}
	}
	for key, value := range bridgeconfig.NormalizePluginEnv(src) {
		dst[key] = value
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

func bridgeReservedPluginIDs(cfg *bridgeconfig.Config) map[string]bool {
	return map[string]bool{}
}

func pluginDirs(cfg *bridgeconfig.Config) []string {
	if cfg == nil || len(cfg.Bridge.PluginDirs) == 0 {
		return []string{bridgeconfig.DefaultPluginDir()}
	}
	out := make([]string, 0, len(cfg.Bridge.PluginDirs))
	for _, dir := range cfg.Bridge.PluginDirs {
		dir = strings.TrimSpace(dir)
		if dir != "" {
			out = append(out, dir)
		}
	}
	if len(out) == 0 {
		return []string{bridgeconfig.DefaultPluginDir()}
	}
	return out
}
