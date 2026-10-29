package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	heartbeatInterval = 10 * time.Second
	// The Realtime service does not guarantee a JSON pong for every client
	// heartbeat. Do not make a healthy but otherwise idle connection flap just
	// because its ping acknowledgement is delayed or omitted.
	heartbeatTimeout  = 60 * time.Second
	heartbeatCheck    = 5 * time.Second
	handshakeTimeout  = 10 * time.Second
	writeTimeout      = 30 * time.Second
	outboundBufferCap = 128
	// A session that survives two heartbeat intervals is healthy enough that a
	// later network interruption should start from the shortest retry delay.
	// This is especially important after macOS wakes from sleep.
	healthySessionDuration = 20 * time.Second
	criticalBufferCap      = 32
)

var (
	ErrHostTokenExpired = errors.New("realtime: host token expired")
	ErrUnauthorized     = errors.New("realtime: unauthorized")
	ErrGatewayRejected  = errors.New("realtime: gateway rejected")
	ErrControlQueueFull = errors.New("realtime: control queue full")
)

// reconnectDelays mirrors openclaw-rokid: 10s, 20s, 30s, 60s (max).
var reconnectDelays = []time.Duration{
	10 * time.Second, 20 * time.Second, 30 * time.Second, 60 * time.Second,
}

var reconnectJitter = func(delay time.Duration) time.Duration {
	// 在基础延迟上增加最多 20% 抖动，避免大量客户端同时重连。
	if delay <= 0 {
		return delay
	}
	return delay + time.Duration(rand.Int63n(int64(delay/5)+1))
}

// Options configures a Client.
type Options struct {
	URL       string
	Token     string
	HostID    string
	AccountID string
	// HostInfoHeader is the Base64URL UTF-8 JSON X-Host-Info value resolved
	// by Prism API or injected by the deployment gateway.
	HostInfoHeader string
	Logger         *slog.Logger
	OnConnect      func(context.Context, *Client)
	OnStateChange  func(status, detail string)
}

// Client maintains a single long-lived WebSocket connection to Prism Realtime with
// automatic heartbeat and reconnect. It is safe for concurrent use.
type Client struct {
	opts Options

	inbound           chan Inbound
	outbound          chan Outbound
	controlOutbound   chan ControlOutbound
	criticalOutbound  chan ControlOutbound
	confirmedOutbound chan confirmedControlOutbound
	ackMu             sync.Mutex
	pendingAcks       map[string]chan struct{}

	done               chan struct{}
	closed             atomic.Bool
	connected          atomic.Bool
	lastServerActivity atomic.Int64 // unix milli

	stateMu          sync.RWMutex
	connectionStatus string
	connectionDetail string
}

type confirmedControlOutbound struct {
	message ControlOutbound
	result  chan error
}

// New creates a Client. Call Run to actually connect.
func New(opts Options) *Client {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Client{
		opts:              opts,
		inbound:           make(chan Inbound, 64),
		outbound:          make(chan Outbound, outboundBufferCap),
		controlOutbound:   make(chan ControlOutbound, outboundBufferCap),
		criticalOutbound:  make(chan ControlOutbound, criticalBufferCap),
		confirmedOutbound: make(chan confirmedControlOutbound, outboundBufferCap),
		pendingAcks:       make(map[string]chan struct{}),
		done:              make(chan struct{}),
		connectionStatus:  "connecting",
	}
}

// Inbound returns the channel of chat messages received from Prism Realtime.
func (c *Client) Inbound() <-chan Inbound { return c.inbound }

// Send enqueues msg for delivery. MessageID / Timestamp are auto-filled.
// Blocks only if the outbound buffer is full and the client is still alive.
func (c *Client) Send(msg Outbound) error {
	if c.closed.Load() {
		return errors.New("transport: client closed")
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}
	if msg.MessageID == "" {
		msg.MessageID = uuid.NewString()
	}
	select {
	case c.outbound <- msg:
		return nil
	case <-c.done:
		return errors.New("transport: client closed")
	}
}

// SendControl enqueues a control-plane message for delivery. It never blocks:
// callers must preserve their own latest snapshot or report a retryable relay
// failure when the bounded queue is saturated.
func (c *Client) SendControl(msg ControlOutbound) error {
	return c.enqueueControl(c.controlOutbound, msg)
}

// SendControlConfirmed returns only after the frame has been written to the
// current WebSocket. A successful write is not a server receipt.
func (c *Client) SendControlConfirmed(ctx context.Context, msg ControlOutbound) error {
	if c.closed.Load() || !c.connected.Load() {
		return errors.New("transport: not connected")
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}
	if msg.MessageID == "" {
		msg.MessageID = uuid.NewString()
	}
	item := confirmedControlOutbound{message: msg, result: make(chan error, 1)}
	select {
	case c.confirmedOutbound <- item:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("transport: client closed")
	}
	select {
	case err := <-item.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("transport: client closed")
	}
}

// SendControlAcknowledged waits for Realtime to process the event and return
// its matching receipt. A lost receipt is retryable with the same message ID.
func (c *Client) SendControlAcknowledged(ctx context.Context, msg ControlOutbound) error {
	if msg.MessageType != MsgConversationEvent || strings.TrimSpace(msg.MessageID) == "" {
		return errors.New("transport: acknowledged send requires a conversation event with message ID")
	}
	ack := make(chan struct{})
	c.ackMu.Lock()
	if _, exists := c.pendingAcks[msg.MessageID]; exists {
		c.ackMu.Unlock()
		return errors.New("transport: acknowledgement already pending")
	}
	c.pendingAcks[msg.MessageID] = ack
	c.ackMu.Unlock()
	defer func() {
		c.ackMu.Lock()
		delete(c.pendingAcks, msg.MessageID)
		c.ackMu.Unlock()
	}()
	if err := c.SendControlConfirmed(ctx, msg); err != nil {
		return err
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("transport: client closed")
	}
}

func (c *Client) acknowledgeConversationEvent(messageID string) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	if ack := c.pendingAcks[messageID]; ack != nil {
		close(ack)
		delete(c.pendingAcks, messageID)
	}
}

// SendCriticalControl reserves capacity for terminal replies that let the API
// recover a state stream, such as a history relay interruption.
func (c *Client) SendCriticalControl(msg ControlOutbound) error {
	return c.enqueueControl(c.criticalOutbound, msg)
}

func (c *Client) enqueueControl(queue chan<- ControlOutbound, msg ControlOutbound) error {
	if c.closed.Load() {
		return errors.New("transport: client closed")
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}
	if msg.MessageID == "" {
		msg.MessageID = uuid.NewString()
	}
	select {
	case queue <- msg:
		return nil
	case <-c.done:
		return errors.New("transport: client closed")
	default:
		return ErrControlQueueFull
	}
}

// IsConnected reports whether a WebSocket session is currently active.
func (c *Client) IsConnected() bool {
	if c == nil {
		return false
	}
	return c.connected.Load()
}

func (c *Client) ConnectionStatus() string {
	if c == nil {
		return "disconnected"
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if strings.TrimSpace(c.connectionStatus) == "" {
		return "connecting"
	}
	return c.connectionStatus
}

func (c *Client) ConnectionDetail() string {
	if c == nil {
		return ""
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.connectionDetail
}

func (c *Client) setConnectionState(status, detail string) {
	status = strings.TrimSpace(status)
	detail = strings.TrimSpace(detail)
	c.stateMu.Lock()
	c.connectionStatus = status
	c.connectionDetail = detail
	c.stateMu.Unlock()
	if c.opts.OnStateChange != nil {
		c.opts.OnStateChange(status, detail)
	}
}

// Run blocks until ctx is cancelled, maintaining the connection with
// auto-reconnect. On every session end the loop waits reconnectDelays[attempt]
// before retrying. It never returns nil unless ctx is cancelled.
func (c *Client) Run(ctx context.Context) error {
	defer func() {
		c.closed.Store(true)
		c.connected.Store(false)
		if ctx.Err() != nil {
			c.setConnectionState("closed", "Realtime 已关闭。")
		}
		close(c.done)
	}()

	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt == 0 {
			c.setConnectionState("connecting", "正在连接 Realtime。")
		} else {
			c.setConnectionState("reconnecting", fmt.Sprintf("正在重连 Realtime（第 %d 次重试）。", attempt))
		}

		sessionDuration, err := c.dialAndServe(ctx)
		if err != nil {
			c.opts.Logger.Warn("transport session ended", "err", err)
			state, detail, terminal := classifyConnectionError(err)
			c.setConnectionState(state, detail)
			if terminal {
				return err
			}
		}
		attempt = reconnectAttemptAfterSession(attempt, sessionDuration)

		if err := ctx.Err(); err != nil {
			return err
		}

		idx := attempt
		if idx >= len(reconnectDelays) {
			idx = len(reconnectDelays) - 1
		}
		delay := reconnectJitter(reconnectDelays[idx])
		attempt++
		c.setConnectionState("reconnecting", fmt.Sprintf("Realtime 已断开，%s 后重连。", delay))
		c.opts.Logger.Info("reconnect scheduled", "attempt", attempt, "delay", delay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func reconnectAttemptAfterSession(attempt int, sessionDuration time.Duration) int {
	if sessionDuration >= healthySessionDuration {
		return 0
	}
	return attempt
}

func (c *Client) dialAndServe(ctx context.Context) (time.Duration, error) {
	startedAt := time.Now()
	header := http.Header{}
	header.Set("X-Host-Token", c.opts.Token)
	if hostInfo := strings.TrimSpace(c.opts.HostInfoHeader); hostInfo != "" {
		header.Set("X-Host-Info", hostInfo)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout:  handshakeTimeout,
		EnableCompression: true,
	}

	dialCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	conn, resp, err := dialer.DialContext(dialCtx, c.opts.URL, header)
	if err != nil {
		info := readHandshakeResponse(resp)
		if classified := classifyHandshakeError(err, info); classified != nil {
			return 0, classified
		}
		return 0, fmt.Errorf("dial: %w%s", err, formatHandshakeInfo(info))
	}
	defer conn.Close()
	conn.EnableWriteCompression(true)

	// Keep-alive handlers for transport-level RFC 6455 Ping/Pong frames.
	// Gorilla WebSocket consumes control frames internally in advanceFrame / ReadMessage.
	// Registering handlers ensures c.lastServerActivity is updated when Netty or intermediate
	// proxies send standard WebSocket ping or pong frames.
	conn.SetPingHandler(func(appData string) error {
		c.lastServerActivity.Store(time.Now().UnixMilli())
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Timeout() {
			return nil
		}
		return err
	})
	conn.SetPongHandler(func(string) error {
		c.lastServerActivity.Store(time.Now().UnixMilli())
		return nil
	})

	c.opts.Logger.Info("ws connected", "url", c.opts.URL)
	c.connected.Store(true)
	c.setConnectionState("connected", "")
	defer c.connected.Store(false)
	c.lastServerActivity.Store(time.Now().UnixMilli())

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()
	doneClosingConn := make(chan struct{})
	go func() {
		select {
		case <-sessCtx.Done():
			_ = conn.Close()
		case <-doneClosingConn:
		}
	}()
	defer close(doneClosingConn)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); c.readLoop(sessCtx, sessCancel, conn) }()
	go func() { defer wg.Done(); c.writeLoop(sessCtx, sessCancel, conn) }()
	go func() { defer wg.Done(); c.heartbeatLoop(sessCtx, sessCancel, conn) }()
	if c.opts.OnConnect != nil {
		go c.opts.OnConnect(sessCtx, c)
	}
	wg.Wait()

	return time.Since(startedAt), nil
}

func formatHandshakeResponse(resp *http.Response) string {
	return formatHandshakeInfo(readHandshakeResponse(resp))
}

func formatHandshakeInfo(info *handshakeResponseInfo) string {
	if info == nil {
		return ""
	}
	var details []string
	if info.Status != "" {
		details = append(details, "status="+info.Status)
	}
	if info.Server != "" {
		details = append(details, "server="+info.Server)
	}
	if info.RequestID != "" {
		details = append(details, "request_id="+info.RequestID)
	}
	if info.Body != "" {
		details = append(details, "body="+info.Body)
	}
	if len(details) == 0 {
		return ""
	}
	return " (" + strings.Join(details, ", ") + ")"
}

type handshakeResponseInfo struct {
	Status     string
	StatusCode int
	Server     string
	RequestID  string
	Body       string
	Code       string
	Message    string
}

func readHandshakeResponse(resp *http.Response) *handshakeResponseInfo {
	if resp == nil {
		return nil
	}
	defer resp.Body.Close()
	info := &handshakeResponseInfo{
		Status:     strings.TrimSpace(resp.Status),
		StatusCode: resp.StatusCode,
		Server:     strings.TrimSpace(resp.Header.Get("Server")),
		RequestID:  strings.TrimSpace(resp.Header.Get("X-Request-Id")),
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	info.Body = strings.TrimSpace(string(body))
	if info.Body != "" {
		var payload struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(body, &payload); err == nil {
			info.Code = strings.TrimSpace(payload.Code)
			info.Message = strings.TrimSpace(payload.Message)
		}
	}
	return info
}

func classifyHandshakeError(err error, info *handshakeResponseInfo) error {
	if info == nil {
		return nil
	}
	detail := formatHandshakeDetail(info)
	if info.StatusCode == http.StatusUnauthorized {
		switch strings.ToLower(info.Code) {
		case "host_token_expired":
			return fmt.Errorf("%w: %s", ErrHostTokenExpired, detail)
		default:
			return fmt.Errorf("%w: %s", ErrUnauthorized, detail)
		}
	}
	if info.StatusCode == http.StatusForbidden {
		if info.RequestID != "" {
			detail = strings.TrimSpace(detail + " (request_id=" + info.RequestID + ")")
		}
		return fmt.Errorf("%w: %s", ErrGatewayRejected, detail)
	}
	if detail == "" {
		return nil
	}
	return fmt.Errorf("dial: %w (%s)", err, detail)
}

func formatHandshakeDetail(info *handshakeResponseInfo) string {
	if info == nil {
		return ""
	}
	switch {
	case info.Message != "":
		return info.Message
	case info.Body != "":
		return info.Body
	case info.Status != "":
		return info.Status
	default:
		return ""
	}
}

func classifyConnectionError(err error) (status, detail string, terminal bool) {
	switch {
	case err == nil:
		return "disconnected", "", false
	case errors.Is(err, ErrHostTokenExpired):
		return "auth_expired", "Host token 已过期，请重新登录。", true
	case errors.Is(err, ErrUnauthorized):
		return "unauthorized", "Realtime 鉴权失败，请重新登录。", true
	case errors.Is(err, ErrGatewayRejected):
		return "gateway_rejected", "Realtime 网关拒绝连接（HTTP 403）；Hub 将自动重试。", false
	default:
		return "disconnected", strings.TrimSpace(err.Error()), false
	}
}

func (c *Client) readLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	for {
		if ctx.Err() != nil {
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.opts.Logger.Warn("ws read error", "err", err)
			return
		}
		var head struct {
			MessageType MessageType `json:"message_type"`
		}
		if err := json.Unmarshal(data, &head); err != nil {
			c.opts.Logger.Warn("ws bad json", "err", err)
			continue
		}
		// Any well-formed frame proves the server-to-Hub path remains alive. A
		// JSON pong is only one of several valid liveness signals on this wire.
		c.lastServerActivity.Store(time.Now().UnixMilli())
		switch head.MessageType.Canonical() {
		case MsgPong:
			c.opts.Logger.Debug("pong received")
		case MsgConversationEventAck:
			var receipt struct {
				MessageID string `json:"message_id"`
				Payload   struct {
					Accepted bool `json:"accepted"`
				} `json:"payload"`
			}
			if err := json.Unmarshal(data, &receipt); err == nil && receipt.Payload.Accepted && receipt.MessageID != "" {
				c.acknowledgeConversationEvent(receipt.MessageID)
			}
		case MsgChatSend, MsgConversationStart,
			MsgConversationDraftOpen, MsgConversationDraftControl, MsgConversationDraftReopen, MsgConversationDraftStart, MsgConversationDraftWatch, MsgConversationDraftUnwatch,
			MsgConversationMenuSessionDescribe, MsgConversationMenuSessionApply,
			MsgConversationDraftMenuSessionDescribe, MsgConversationDraftMenuSessionApply,
			MsgTaskInterrupt, MsgApprovalResolve, MsgSessionControl, MsgPluginRuntimeSwitch, MsgDesktopLaunch, MsgHostPluginRefresh, MsgQueryRequest:
			var in Inbound
			if err := json.Unmarshal(data, &in); err != nil {
				c.opts.Logger.Warn("ws bad chat", "err", err)
				continue
			}
			in.MessageType = in.MessageType.Canonical()
			select {
			case c.inbound <- in:
			case <-ctx.Done():
				return
			}
		default:
			c.opts.Logger.Debug("ws ignored", "type", head.MessageType.Canonical())
		}
	}
}

func (c *Client) writeLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	writeConfirmed := func(item confirmedControlOutbound) bool {
		err := c.writeControlMessage(conn, item.message)
		item.result <- err
		if err != nil {
			c.opts.Logger.Warn("ws write error", "kind", "confirmed_control", "err", err)
			return false
		}
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.criticalOutbound:
			if err := c.writeControlMessage(conn, msg); err != nil {
				c.opts.Logger.Warn("ws write error", "kind", "critical", "err", err)
				return
			}
			continue
		case item := <-c.confirmedOutbound:
			if !writeConfirmed(item) {
				return
			}
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case msg := <-c.criticalOutbound:
			if err := c.writeControlMessage(conn, msg); err != nil {
				c.opts.Logger.Warn("ws write error", "kind", "critical", "err", err)
				return
			}
		case item := <-c.confirmedOutbound:
			if !writeConfirmed(item) {
				return
			}
		case msg := <-c.outbound:
			if err := c.writeOutboundMessage(conn, msg); err != nil {
				c.opts.Logger.Warn("ws write error", "kind", "chat", "err", err)
				return
			}
		case msg := <-c.controlOutbound:
			if err := c.writeControlMessage(conn, msg); err != nil {
				c.opts.Logger.Warn("ws write error", "kind", "control", "err", err)
				return
			}
		}
	}
}

func (c *Client) writeOutboundMessage(conn *websocket.Conn, msg Outbound) error {
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return conn.WriteJSON(msg.wireCompatible())
}

func (c *Client) writeControlMessage(conn *websocket.Conn, msg ControlOutbound) error {
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	wire := msg.wireCompatible()
	if wire.MessageType == MsgQueryResponse || wire.MessageType == MsgCatalogSnapshot || wire.MessageType == MsgConversationIndexSnapshot || wire.MessageType == MsgConversationDetailSnapshot || wire.MessageType == MsgConversationDetailUnavailable || wire.MessageType == MsgSessionControl || wire.MessageType == MsgPluginRuntimeSwitchResult || wire.MessageType == MsgDesktopLaunchResult {
		c.opts.Logger.Debug("ws control write start", "type", wire.MessageType, "msg_id", wire.MessageID)
	}
	err := conn.WriteJSON(wire)
	if wire.MessageType == MsgQueryResponse || wire.MessageType == MsgCatalogSnapshot || wire.MessageType == MsgConversationIndexSnapshot || wire.MessageType == MsgConversationDetailSnapshot || wire.MessageType == MsgConversationDetailUnavailable || wire.MessageType == MsgSessionControl || wire.MessageType == MsgPluginRuntimeSwitchResult || wire.MessageType == MsgDesktopLaunchResult {
		if err != nil {
			c.opts.Logger.Warn("ws control write failed", "type", wire.MessageType, "msg_id", wire.MessageID, "err", err)
		} else {
			c.opts.Logger.Debug("ws control write completed", "type", wire.MessageType, "msg_id", wire.MessageID)
		}
	}
	return err
}

func (c *Client) heartbeatLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()

	pingTicker := time.NewTicker(heartbeatInterval)
	defer pingTicker.Stop()
	checkTicker := time.NewTicker(heartbeatCheck)
	defer checkTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			// Send transport-level Ping control frame to keep proxies/gateways active
			// and provoke a standard Pong frame back to refresh c.lastServerActivity.
			if conn != nil {
				_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeTimeout))
			}

			// Send protocol-level MsgPing to renew Host presence & Redis projections in Realtime.
			ping := ControlOutbound{
				MessageID:   "ping-" + uuid.NewString(),
				MessageType: MsgPing,
				Timestamp:   time.Now().UnixMilli(),
			}
			select {
			case c.controlOutbound <- ping:
			default:
				c.opts.Logger.Warn("heartbeat dropped (control outbound full)")
			}
		case <-checkTicker.C:
			last := time.UnixMilli(c.lastServerActivity.Load())
			if time.Since(last) > heartbeatTimeout {
				c.opts.Logger.Warn("heartbeat timeout, resetting session", "since", time.Since(last))
				return
			}
		}
	}
}
