package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"rokid-prism-hub/internal/config"
)

func TestListUserHostPluginsPreservesHostForbiddenCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"code":"host_forbidden","message":"host does not belong to current account"}`))
	}))
	defer server.Close()

	_, err := ListUserHostPlugins(server.URL, testAccount(), "host-1")
	var responseError *ResponseError
	if !errors.As(err, &responseError) || responseError.Code != "host_forbidden" {
		t.Fatalf("expected structured host_forbidden error, got %v", err)
	}
}

func testAccount() config.Account {
	return config.Account{AccountID: "account-1", UserID: "user-1", Name: "Prism Tester", AccessToken: "account-token-1"}
}

func TestListUserHostPluginsUsesAccountTokenAndEnvelopeData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/user/hosts/host-1/plugins" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get(AccountTokenHeader); got != "account-token-1" {
			t.Fatalf("unexpected account token: %q", got)
		}
		if got, _ := url.QueryUnescape(r.Header.Get(AccountInfoHeader)); got != `{"accountId":"account-1","uid":"user-1","userName":"Prism Tester"}` {
			t.Fatalf("unexpected account context: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"host_id":"host-1","items":[{"plugin_id":"codex","display_name":"Codex","icon_url":"https://example.test/codex.webp","capabilities":{},"created_at":1,"updated_at":2}]}}`))
	}))
	defer server.Close()

	plugins, err := ListUserHostPlugins(server.URL, testAccount(), "host-1")
	if err != nil {
		t.Fatalf("ListUserHostPlugins: %v", err)
	}
	if plugins.HostID != "host-1" || len(plugins.Items) != 1 || plugins.Items[0].PluginID != "codex" {
		t.Fatalf("unexpected host plugins: %+v", plugins)
	}
	if plugins.Items[0].IconURL != "https://example.test/codex.webp" {
		t.Fatalf("unexpected icon url: %+v", plugins.Items[0])
	}
}

func TestMarkUserConversationReadUsesAccountTokenAndRequiresOKEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/user/hosts/host-1/plugins/codex/conversations/conv-1:read" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get(AccountTokenHeader); got != "account-token-1" {
			t.Fatalf("unexpected account token: %q", got)
		}
		if got, _ := url.QueryUnescape(r.Header.Get(AccountInfoHeader)); got == "" {
			t.Fatal("missing account context")
		}
		_, _ = w.Write([]byte(`{"success":true,"code":"ok","message":"","data":{"unread":false}}`))
	}))
	defer server.Close()

	if err := MarkUserConversationRead(context.Background(), server.URL, testAccount(), "host-1", "codex", "conv-1"); err != nil {
		t.Fatalf("MarkUserConversationRead: %v", err)
	}
}

func TestListUserHostPluginsAcceptsCommonSuccessCodeWithoutSuccessField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"0","data":{"host_id":"host-1","items":[]}}`))
	}))
	defer server.Close()

	plugins, err := ListUserHostPlugins(server.URL, testAccount(), "host-1")
	if err != nil {
		t.Fatalf("ListUserHostPlugins: %v", err)
	}
	if plugins.HostID != "host-1" || len(plugins.Items) != 0 {
		t.Fatalf("unexpected host plugins: %+v", plugins)
	}
}

func TestListUserHostPluginsRejectsExplicitSuccessFalse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":false,"code":"ok","data":{"host_id":"host-1","items":[]}}`))
	}))
	defer server.Close()

	if _, err := ListUserHostPlugins(server.URL, testAccount(), "host-1"); err == nil {
		t.Fatal("expected explicit success=false response to be rejected")
	}
}

func TestListUserHostPluginsRejectsBareJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"host_id":"host-1","items":[]}`))
	}))
	defer server.Close()

	if _, err := ListUserHostPlugins(server.URL, testAccount(), "host-1"); err == nil {
		t.Fatal("expected bare JSON response to be rejected")
	}
}

func TestIsNotFoundErrorRecognizesIdempotentDeleteFailures(t *testing.T) {
	for _, err := range []error{
		&ResponseError{Status: "404 Not Found"},
		&ResponseError{Status: "400 Bad Request", Code: "plugin_not_found"},
	} {
		if !IsNotFoundError(err) {
			t.Fatalf("expected not-found error, got %v", err)
		}
	}
	if IsNotFoundError(&ResponseError{Status: "403 Forbidden", Code: "host_forbidden"}) {
		t.Fatal("authorization failures must not be treated as idempotent deletes")
	}
}
