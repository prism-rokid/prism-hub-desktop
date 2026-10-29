package realtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSendControlConfirmedWritesFrame(t *testing.T) {
	frames := make(chan ControlOutbound, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var frame ControlOutbound
		if err := conn.ReadJSON(&frame); err == nil {
			frames <- frame
		}
	}))
	defer server.Close()
	connected := make(chan struct{}, 1)
	client := New(Options{URL: "ws" + strings.TrimPrefix(server.URL, "http"), OnConnect: func(context.Context, *Client) { connected <- struct{}{} }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- client.Run(ctx) }()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket did not connect")
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer writeCancel()
	const messageID = "stable-hook-event-1"
	if err := client.SendControlConfirmed(writeCtx, ControlOutbound{MessageID: messageID, MessageType: MsgConversationEvent}); err != nil {
		t.Fatalf("confirmed write: %v", err)
	}
	select {
	case frame := <-frames:
		if frame.MessageID != messageID || frame.MessageType != MsgConversationEvent {
			t.Fatalf("unexpected WebSocket frame: %+v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive confirmed frame")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not stop")
	}
}

func TestSendControlConfirmedRejectsDisconnectedClient(t *testing.T) {
	client := New(Options{})
	if err := client.SendControlConfirmed(context.Background(), ControlOutbound{MessageType: MsgConversationEvent}); err == nil {
		t.Fatal("disconnected client must not confirm a frame")
	}
	if got := len(client.confirmedOutbound); got != 0 {
		t.Fatalf("disconnected frame must not enter queue, got %d", got)
	}
}

func TestSendControlAcknowledgedWaitsForServerReceipt(t *testing.T) {
	received := make(chan struct{}, 1)
	allowReceipt := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var frame ControlOutbound
		if err := conn.ReadJSON(&frame); err != nil {
			return
		}
		received <- struct{}{}
		<-allowReceipt
		_ = conn.WriteJSON(map[string]any{
			"message_id": frame.MessageID, "message_type": MsgConversationEventAck,
			"payload": map[string]any{"accepted": true},
		})
	}))
	defer server.Close()
	connected := make(chan struct{}, 1)
	client := New(Options{URL: "ws" + strings.TrimPrefix(server.URL, "http"), OnConnect: func(context.Context, *Client) { connected <- struct{}{} }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- client.Run(ctx) }()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket did not connect")
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer writeCancel()
	result := make(chan error, 1)
	go func() {
		result <- client.SendControlAcknowledged(writeCtx, ControlOutbound{MessageID: "event-receipt-1", MessageType: MsgConversationEvent})
	}()
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive event")
	}
	select {
	case err := <-result:
		t.Fatalf("send completed before server receipt: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(allowReceipt)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("server receipt not accepted: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server receipt did not complete send")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not stop")
	}
}

func TestSendControlAcknowledgedRetainsEventWithoutReceipt(t *testing.T) {
	client := New(Options{})
	client.connected.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := client.SendControlAcknowledged(ctx, ControlOutbound{MessageID: "event-no-receipt", MessageType: MsgConversationEvent})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing receipt must remain retryable, got %v", err)
	}
	client.ackMu.Lock()
	pending := len(client.pendingAcks)
	client.ackMu.Unlock()
	if pending != 0 {
		t.Fatalf("timed out receipt waiter leaked: %d", pending)
	}
}

func TestClassifyHandshakeErrorHostTokenExpired(t *testing.T) {
	info := &handshakeResponseInfo{
		StatusCode: http.StatusUnauthorized,
		Code:       "host_token_expired",
		Message:    "Host token 已过期",
	}
	err := classifyHandshakeError(errors.New("bad handshake"), info)
	if !errors.Is(err, ErrHostTokenExpired) {
		t.Fatalf("expected host token expired error, got %v", err)
	}
	if !strings.Contains(err.Error(), "Host token 已过期") {
		t.Fatalf("unexpected error detail: %v", err)
	}
}

func TestClassifyConnectionErrorHostTokenExpired(t *testing.T) {
	state, detail, terminal := classifyConnectionError(ErrHostTokenExpired)
	if state != "auth_expired" {
		t.Fatalf("unexpected state: %q", state)
	}
	if !terminal {
		t.Fatal("expected auth expired to be terminal")
	}
	if !strings.Contains(detail, "重新登录") {
		t.Fatalf("unexpected detail: %q", detail)
	}
}

func TestClassifyHandshakeErrorGatewayRejectedIsRetryable(t *testing.T) {
	err := classifyHandshakeError(errors.New("bad handshake"), &handshakeResponseInfo{
		StatusCode: http.StatusForbidden,
		RequestID:  "gateway-request-1",
		Message:    "gateway rejected host connection",
	})
	if !errors.Is(err, ErrGatewayRejected) {
		t.Fatalf("expected gateway rejected error, got %v", err)
	}
	state, detail, terminal := classifyConnectionError(err)
	if state != "gateway_rejected" || terminal {
		t.Fatalf("unexpected classification: state=%q terminal=%v", state, terminal)
	}
	if !strings.Contains(detail, "403") {
		t.Fatalf("missing 403 detail: %q", detail)
	}
	if !strings.Contains(err.Error(), "gateway-request-1") {
		t.Fatalf("missing gateway request id: %v", err)
	}
}

func TestSendControlReturnsQueueFullWithoutBlocking(t *testing.T) {
	client := New(Options{})
	for i := 0; i < cap(client.controlOutbound); i++ {
		if err := client.SendControl(ControlOutbound{MessageType: MsgConversationDetailSnapshot}); err != nil {
			t.Fatalf("fill control queue: %v", err)
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- client.SendControl(ControlOutbound{MessageType: MsgConversationDetailSnapshot})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrControlQueueFull) {
			t.Fatalf("expected ErrControlQueueFull, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SendControl blocked behind a saturated queue")
	}
	if err := client.SendCriticalControl(ControlOutbound{MessageType: MsgConversationBody}); err != nil {
		t.Fatalf("critical queue must remain available during normal queue saturation: %v", err)
	}
}

func TestReconnectAttemptResetsAfterHealthySession(t *testing.T) {
	if got := reconnectAttemptAfterSession(7, healthySessionDuration); got != 0 {
		t.Fatalf("expected healthy session to reset retries, got %d", got)
	}
	if got := reconnectAttemptAfterSession(7, healthySessionDuration-time.Millisecond); got != 7 {
		t.Fatalf("expected short session to retain retry count, got %d", got)
	}
}

func TestReconnectJitterStaysWithinTwentyPercent(t *testing.T) {
	for _, base := range reconnectDelays {
		got := reconnectJitter(base)
		if got < base || got > base+base/5 {
			t.Fatalf("jittered delay %s outside range [%s,%s], got %s", base, base, base+base/5, got)
		}
	}
}

func TestHeartbeatActivityTracking(t *testing.T) {
	client := New(Options{})
	before := time.Now().Add(-10 * time.Second).UnixMilli()
	client.lastServerActivity.Store(before)

	if client.lastServerActivity.Load() != before {
		t.Fatalf("expected initial activity %d, got %d", before, client.lastServerActivity.Load())
	}

	// Simulate Pong frame updating lastServerActivity
	now := time.Now().UnixMilli()
	client.lastServerActivity.Store(now)

	if since := time.Since(time.UnixMilli(client.lastServerActivity.Load())); since > 100*time.Millisecond {
		t.Fatalf("expected recent activity timestamp, got since=%v", since)
	}
}
