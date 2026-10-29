package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"rokid-prism-hub/internal/pluginruntime"
)

type StdioServer struct {
	Adapter pluginruntime.PluginAdapter
	In      io.Reader
	Out     io.Writer

	cancel  context.CancelFunc
	subs    map[string]context.CancelFunc
	history map[string]context.CancelFunc
}

type lockedEncoder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (e *lockedEncoder) Encode(value any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(value)
}

func (s *StdioServer) Run(ctx context.Context) error {
	if s == nil || s.Adapter == nil {
		return fmt.Errorf("pluginbridge plugin stdio server adapter required")
	}
	in := s.In
	if in == nil {
		return fmt.Errorf("pluginbridge plugin stdio server input required")
	}
	out := s.Out
	if out == nil {
		return fmt.Errorf("pluginbridge plugin stdio server output required")
	}
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.subs = map[string]context.CancelFunc{}
	s.history = map[string]context.CancelFunc{}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	enc := &lockedEncoder{enc: json.NewEncoder(out)}
	for scanner.Scan() {
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		payload, err := s.handle(ctx, req.Method, req.Params, enc)
		if err != nil {
			code := "adapter_error"
			var coded *pluginruntime.AdapterError
			if errors.As(err, &coded) && strings.TrimSpace(coded.Code) != "" {
				code = strings.TrimSpace(coded.Code)
			}
			_ = enc.Encode(rpcResponse{
				ID: req.ID,
				OK: false,
				Error: &rpcError{
					Code:    code,
					Message: err.Error(),
				},
			})
			continue
		}
		raw, err := marshalWire(payload)
		if err != nil {
			_ = enc.Encode(rpcResponse{
				ID: req.ID,
				OK: false,
				Error: &rpcError{
					Code:    "encode_error",
					Message: err.Error(),
				},
			})
			continue
		}
		_ = enc.Encode(rpcResponse{ID: req.ID, OK: true, Payload: raw})
	}
	cancel()
	for _, cancel := range s.subs {
		cancel()
	}
	for _, cancel := range s.history {
		cancel()
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (s *StdioServer) handle(ctx context.Context, method string, params any, enc *lockedEncoder) (any, error) {
	switch method {
	case "adapter.probe":
		return s.Adapter.Probe(ctx)
	case "adapter.discover":
		return s.Adapter.Discover(ctx)
	case "adapter.startSessionWithMessage":
		starter, ok := s.Adapter.(pluginruntime.SessionStarter)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement startSessionWithMessage")
		}
		var req pluginruntime.StartSessionWithMessageRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return starter.StartSessionWithMessage(ctx, req)
	case "adapter.openDraft":
		opener, ok := s.Adapter.(pluginruntime.DraftOpener)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement openDraft")
		}
		var req pluginruntime.DraftOpenRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return opener.OpenDraft(ctx, req)
	case "adapter.controlDraft":
		controller, ok := s.Adapter.(pluginruntime.DraftController)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement controlDraft")
		}
		var req pluginruntime.DraftControlRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return controller.ControlDraft(ctx, req)
	case "adapter.startDraftWithMessage":
		starter, ok := s.Adapter.(pluginruntime.DraftStarter)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement startDraftWithMessage")
		}
		var req pluginruntime.StartDraftWithMessageRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return starter.StartDraftWithMessage(ctx, req)
	case "adapter.listSessions":
		lister, ok := s.Adapter.(pluginruntime.SessionLister)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement listSessions")
		}
		return lister.ListSessions(ctx)
	case "adapter.attachSession":
		attacher, ok := s.Adapter.(pluginruntime.SessionAttacher)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement attachSession")
		}
		var req pluginruntime.AttachSessionRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return attacher.AttachSession(ctx, req)
	case "adapter.readHistory":
		reader, ok := s.Adapter.(pluginruntime.HistoryReader)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement readHistory")
		}
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
			Limit   int                         `json:"limit"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return reader.ReadHistory(ctx, req.Session, req.Limit)
	case "adapter.readDetail":
		reader, ok := s.Adapter.(pluginruntime.DetailReader)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement readDetail")
		}
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return reader.ReadDetail(ctx, req.Session)
	case "adapter.readHistoryStream":
		streamer, ok := s.Adapter.(pluginruntime.HistoryStreamer)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement readHistoryStream")
		}
		var req struct {
			Session pluginruntime.NativeSession        `json:"session"`
			Request pluginruntime.HistoryStreamRequest `json:"request"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		if req.Request.StreamID == "" {
			req.Request.StreamID = uuid.NewString()
		}
		streamCtx, cancel := context.WithCancel(ctx)
		events, err := streamer.ReadHistoryStream(streamCtx, req.Session, req.Request)
		if err != nil {
			cancel()
			return nil, err
		}
		s.history[req.Request.StreamID] = cancel
		go forwardHistoryEvents(streamCtx, enc, req.Request.StreamID, events)
		return map[string]any{"stream_id": req.Request.StreamID}, nil
	case "adapter.cancelHistoryStream":
		var req struct {
			StreamID string `json:"stream_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		if cancel := s.history[req.StreamID]; cancel != nil {
			cancel()
			delete(s.history, req.StreamID)
		}
		return map[string]any{"ok": true}, nil
	case "adapter.readStatus":
		reader, ok := s.Adapter.(pluginruntime.StatusReader)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement readStatus")
		}
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
			RunID   string                      `json:"run_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return reader.ReadStatus(ctx, req.Session, req.RunID)
	case "adapter.controlSession":
		controller, ok := s.Adapter.(pluginruntime.SessionController)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement controlSession")
		}
		var req pluginruntime.SessionControlRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return controller.ControlSession(ctx, req)
	case "adapter.resolveApproval":
		resolver, ok := s.Adapter.(pluginruntime.ApprovalResolver)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement resolveApproval")
		}
		var req pluginruntime.ApprovalResolutionRequest
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, resolver.ResolveApproval(ctx, req)
	case "adapter.send":
		var req struct {
			Session pluginruntime.NativeSession  `json:"session"`
			Message pluginruntime.InboundMessage `json:"message"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return s.Adapter.Send(ctx, req.Session, req.Message)
	case "adapter.subscribe":
		var req struct {
			SubscriptionID string                      `json:"subscription_id"`
			Session        pluginruntime.NativeSession `json:"session"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		if req.SubscriptionID == "" {
			req.SubscriptionID = uuid.NewString()
		}
		subCtx, cancel := context.WithCancel(ctx)
		events, err := s.Adapter.Subscribe(subCtx, req.Session)
		if err != nil {
			cancel()
			return nil, err
		}
		s.subs[req.SubscriptionID] = cancel
		go forwardPluginEvents(subCtx, enc, req.SubscriptionID, events)
		return map[string]any{"subscribed": true, "subscription_id": req.SubscriptionID}, nil
	case "adapter.subscribePlugin":
		var req struct {
			SubscriptionID string `json:"subscription_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		watcher, ok := s.Adapter.(pluginruntime.PluginWideSubscriber)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement subscribePlugin")
		}
		if req.SubscriptionID == "" {
			req.SubscriptionID = uuid.NewString()
		}
		subCtx, cancel := context.WithCancel(ctx)
		events, err := watcher.SubscribePlugin(subCtx)
		if err != nil {
			cancel()
			return nil, err
		}
		s.subs[req.SubscriptionID] = cancel
		go forwardPluginEvents(subCtx, enc, req.SubscriptionID, events)
		return map[string]any{"subscribed": true, "subscription_id": req.SubscriptionID}, nil
	case "adapter.readAgentUsage":
		reader, ok := s.Adapter.(pluginruntime.AgentUsageReader)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement readAgentUsage")
		}
		return reader.ReadAgentUsage(ctx)
	case "adapter.unsubscribe":
		var req struct {
			SubscriptionID string `json:"subscription_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		if cancel := s.subs[req.SubscriptionID]; cancel != nil {
			cancel()
			delete(s.subs, req.SubscriptionID)
		}
		return map[string]any{"ok": true}, nil
	case "adapter.interrupt":
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
			TaskID  string                      `json:"task_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, s.Adapter.Interrupt(ctx, req.Session, req.TaskID)
	case "adapter.verifyVisibility":
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
			Marker  string                      `json:"marker"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return s.Adapter.VerifyVisibility(ctx, req.Session, req.Marker)
	case "adapter.waitForRun":
		waiter, ok := s.Adapter.(pluginruntime.RunWaiter)
		if !ok {
			return nil, fmt.Errorf("adapter does not implement waitForRun")
		}
		var req struct {
			Session pluginruntime.NativeSession `json:"session"`
			RunID   string                      `json:"run_id"`
		}
		if err := decodeParams(params, &req); err != nil {
			return nil, err
		}
		return waiter.WaitForRun(ctx, req.Session, req.RunID)
	case "adapter.close":
		if s.cancel != nil {
			s.cancel()
		}
		return map[string]any{"ok": true}, s.Adapter.Close(ctx)
	default:
		return nil, fmt.Errorf("unknown adapter method %s", method)
	}
}

func decodeParams(params any, out any) error {
	b, err := json.Marshal(params)
	if err != nil {
		return err
	}
	if len(b) == 0 || string(b) == "null" {
		b = []byte(`{}`)
	}
	return unmarshalWire(b, out)
}

func forwardPluginEvents(ctx context.Context, enc *lockedEncoder, subID string, events <-chan pluginruntime.PluginEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.CreatedAt.IsZero() {
				event.CreatedAt = time.Now().UTC()
			}
			_ = enc.Encode(rpcEvent{
				Event: "plugin.event",
				Payload: mustJSONRaw(map[string]any{
					"subscription_id": subID,
					"event":           event,
				}),
			})
		}
	}
}

func forwardHistoryEvents(ctx context.Context, enc *lockedEncoder, streamID string, events <-chan pluginruntime.HistoryStreamEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			event.StreamID = streamID
			_ = enc.Encode(rpcEvent{Event: "history.stream", Payload: mustJSONRaw(event)})
			if event.Type == "end" || event.Type == "error" {
				return
			}
		}
	}
}

func mustJSONRaw(value any) json.RawMessage {
	b, _ := marshalWire(value)
	return b
}
