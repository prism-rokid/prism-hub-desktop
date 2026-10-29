package realtime

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestVersionLessThan(t *testing.T) {
	if !VersionLessThan("2.0", "2.1") {
		t.Fatal("expected 2.0 < 2.1")
	}
	if VersionLessThan("2.1", "2.1") {
		t.Fatal("expected 2.1 !< 2.1")
	}
	if VersionLessThan("2.2", "2.1") {
		t.Fatal("expected 2.2 !< 2.1")
	}
}

func TestCheckVersionCompatibility(t *testing.T) {
	ok, downgrade, msg := CheckVersionCompatibility("3.0", "3.0")
	if !ok || downgrade || msg != "" {
		t.Fatalf("unexpected exact compatibility result: ok=%v downgrade=%v msg=%q", ok, downgrade, msg)
	}

	ok, downgrade, msg = CheckVersionCompatibility("2.1", "2.0")
	if ok || downgrade || !strings.Contains(msg, "below Prism Hub minimum") {
		t.Fatalf("expected v2 incompatibility, got ok=%v downgrade=%v msg=%q", ok, downgrade, msg)
	}

	ok, downgrade, msg = CheckVersionCompatibility("", "2.0")
	if ok || downgrade || !strings.Contains(msg, "legacy client") {
		t.Fatalf("expected legacy incompatibility, got ok=%v downgrade=%v msg=%q", ok, downgrade, msg)
	}
}

func TestFormatHandshakeResponseIncludesGatewayStatus(t *testing.T) {
	resp := &http.Response{
		Status: "403 Forbidden",
		Header: http.Header{
			"Server":       []string{"istio-envoy"},
			"X-Request-Id": []string{"req-123"},
		},
		Body: io.NopCloser(strings.NewReader("forbidden")),
	}

	got := formatHandshakeResponse(resp)
	for _, want := range []string{"status=403 Forbidden", "server=istio-envoy", "request_id=req-123", "body=forbidden"} {
		if !strings.Contains(got, want) {
			t.Fatalf("formatHandshakeResponse() = %q, want %q", got, want)
		}
	}
}

func TestMessageTypeCanonicalAndWireCompatibility(t *testing.T) {
	if got := MsgConversationEvent.Canonical(); got != MsgConversationEvent {
		t.Fatalf("MsgConversationEvent.Canonical() = %q, want %q", got, MsgConversationEvent)
	}
	if got := MsgConversationEvent.WireCompatible(); got != MsgConversationEvent {
		t.Fatalf("MsgConversationEvent.WireCompatible() = %q, want %q", got, MsgConversationEvent)
	}
	if !MsgConversationEvent.IsConversationEvent() {
		t.Fatal("conversation.event must be recognized")
	}
	if MsgTaskEvent.IsConversationEvent() {
		t.Fatal("task.event must not be treated as conversation.event")
	}
}

func TestPluginRuntimeSwitchProtocolUsesPluginScopedFields(t *testing.T) {
	var inbound Inbound
	if err := json.Unmarshal([]byte(`{"message_id":"request-1","message_type":"plugin.runtime.switch","payload":{"request_id":"client-request-1","plugin_id":"hermes","runtime_mode":"acp"}}`), &inbound); err != nil {
		t.Fatalf("Unmarshal plugin.runtime.switch: %v", err)
	}
	if inbound.MessageType != MsgPluginRuntimeSwitch || inbound.Payload == nil || inbound.Payload.PluginID != "hermes" || inbound.Payload.RuntimeMode != "acp" || inbound.Payload.ConversationID != "" {
		t.Fatalf("unexpected plugin runtime switch frame: %+v", inbound)
	}
	encoded, err := json.Marshal(ControlOutbound{MessageType: MsgPluginRuntimeSwitchResult, Payload: PluginRuntimeSwitchResultPayload{RequestID: "client-request-1", PluginID: "hermes", RuntimeMode: "acp", Accepted: true}})
	if err != nil {
		t.Fatalf("Marshal plugin.runtime.switch.result: %v", err)
	}
	if strings.Contains(string(encoded), "conversation_id") || strings.Contains(string(encoded), "native_session") {
		t.Fatalf("plugin runtime switch must stay outside conversation identity: %s", encoded)
	}
}

func TestConversationStartProtocolUsesOnlyPublicIdentity(t *testing.T) {
	var inbound Inbound
	if err := json.Unmarshal([]byte(`{
        "message_id":"request-1",
        "message_type":"conversation.start",
        "payload":{
            "request_id":"request-1",
            "plugin_id":"codex",
            "message_id":"mobile-message-1",
            "text":"start a conversation"
        }
    }`), &inbound); err != nil {
		t.Fatalf("Unmarshal conversation.start: %v", err)
	}
	if inbound.MessageType != MsgConversationStart || inbound.Payload == nil || inbound.Payload.ClientMessageID != "mobile-message-1" {
		t.Fatalf("unexpected conversation.start frame: %+v", inbound)
	}

	encoded, err := json.Marshal(ControlOutbound{
		MessageType: MsgConversationStartResult,
		Payload: ConversationStartResultPayload{
			RequestID:      "request-1",
			PluginID:       "codex",
			ConversationID: "conv_019f",
			MessageID:      "mobile-message-1",
			Accepted:       true,
		},
	})
	if err != nil {
		t.Fatalf("Marshal conversation.start.result: %v", err)
	}
	if strings.Contains(string(encoded), "native_session_id") || strings.Contains(string(encoded), "native_thread_id") {
		t.Fatalf("conversation.start.result leaked native identity: %s", encoded)
	}

	encoded, err = json.Marshal(ControlOutbound{
		MessageType: MsgChatSendResult,
		Payload: ChatSendResultPayload{
			RequestID: "request-1", PluginID: "codex", ConversationID: "conv_019f", MessageID: "mobile-message-1",
			Accepted: true, QueuePending: true,
		},
	})
	if err != nil {
		t.Fatalf("Marshal chat.send.result: %v", err)
	}
	if !strings.Contains(string(encoded), "queue_pending") || strings.Contains(string(encoded), "native_session_id") {
		t.Fatalf("unexpected chat.send.result wire payload: %s", encoded)
	}
}
