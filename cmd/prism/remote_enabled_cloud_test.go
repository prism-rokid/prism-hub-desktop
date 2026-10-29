package main

import (
	"errors"
	"net"
	"testing"

	prismapi "rokid-prism-hub/internal/api"
)

func TestHostPluginOverlayLifecycle(t *testing.T) {
	overlay := newHostPluginOverlay()
	if overlay.IsReady() {
		t.Fatal("expected new overlay to not be ready")
	}
	snap, ready := overlay.Snapshot()
	if ready || snap != nil {
		t.Fatalf("expected unready snapshot, got ready=%v, snap=%v", ready, snap)
	}

	// First replace sets values and ready = true, returning changed = true
	changed := overlay.Replace(map[string]bool{"demo": true, "codex": true})
	if !changed {
		t.Fatal("expected first replace to return changed=true")
	}
	if !overlay.IsReady() {
		t.Fatal("expected overlay to be ready after replace")
	}
	snap, ready = overlay.Snapshot()
	if !ready || len(snap) != 2 || !snap["demo"] || !snap["codex"] {
		t.Fatalf("unexpected snapshot: %v", snap)
	}

	// Identical replace should return changed = false
	changed = overlay.Replace(map[string]bool{"demo": true, "codex": true})
	if changed {
		t.Fatal("expected identical replace to return changed=false")
	}

	// Updated replace should return changed = true
	changed = overlay.Replace(map[string]bool{"demo": true})
	if !changed {
		t.Fatal("expected modified replace to return changed=true")
	}
	snap, ready = overlay.Snapshot()
	if len(snap) != 1 || !snap["demo"] {
		t.Fatalf("unexpected snapshot after update: %v", snap)
	}

	// Invalidate should clear values and return changed = true
	changed = overlay.Invalidate()
	if !changed {
		t.Fatal("expected invalidate to return changed=true")
	}
	if overlay.IsReady() {
		t.Fatal("expected overlay to not be ready after invalidate")
	}

	// Second invalidate should return changed = false
	changed = overlay.Invalidate()
	if changed {
		t.Fatal("expected second invalidate to return changed=false")
	}
}

func TestIsPermanentAuthError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
	}{
		{
			name:      "nil error",
			err:       nil,
			permanent: false,
		},
		{
			name:      "transient network timeout",
			err:       &net.OpError{Op: "dial", Err: errors.New("i/o timeout")},
			permanent: false,
		},
		{
			name:      "bad gateway 502",
			err:       &prismapi.ResponseError{Status: "502 Bad Gateway", Code: "bad_gateway"},
			permanent: false,
		},
		{
			name:      "gateway timeout 504",
			err:       &prismapi.ResponseError{Status: "504 Gateway Timeout"},
			permanent: false,
		},
		{
			name:      "401 unauthorized status",
			err:       &prismapi.ResponseError{Status: "401 Unauthorized", Code: "unauthorized"},
			permanent: true,
		},
		{
			name:      "403 forbidden status",
			err:       &prismapi.ResponseError{Status: "403 Forbidden", Code: "forbidden"},
			permanent: true,
		},
		{
			name:      "host_forbidden code",
			err:       &prismapi.ResponseError{Status: "400 Bad Request", Code: "host_forbidden"},
			permanent: true,
		},
		{
			name:      "host_token_expired code",
			err:       &prismapi.ResponseError{Status: "400 Bad Request", Code: "host_token_expired"},
			permanent: true,
		},
		{
			name:      "plain string 401 unauthorized",
			err:       errors.New("request failed with 401 unauthorized"),
			permanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isPermanentAuthError(tc.err)
			if got != tc.permanent {
				t.Fatalf("expected permanent=%v, got=%v for error %v", tc.permanent, got, tc.err)
			}
		})
	}
}
