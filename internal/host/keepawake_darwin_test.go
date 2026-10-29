//go:build darwin && cgo

package host

import "testing"

func TestDetectKeepAwakeCapabilityPrefersNativeAssertion(t *testing.T) {
	capability, handled := detectPlatformKeepAwakeCapability()
	if !handled || !capability.Supported || capability.Method != "iopm_assertion" {
		t.Fatalf("expected native assertion, got handled=%v capability=%+v", handled, capability)
	}
	resolved := DetectKeepAwakeCapability()
	if !resolved.Supported || resolved.Method != "iopm_assertion" {
		t.Fatalf("expected native keep-awake capability, got %+v", resolved)
	}
}
