package pluginruntime

import "testing"

func TestRemoteContinuationSupport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		caps   Capability
		reason string
	}{
		{
			name: "existing conversations only",
			caps: Capability{CanForwardSync: true, CanListSessions: true, CanReadHistory: true, CanWaitRun: true, CanAttachSession: true},
		},
		{
			name:   "missing history",
			caps:   Capability{CanForwardSync: true, CanListSessions: true, CanWaitRun: true, CanAttachSession: true, CanStartSessionWithMessage: true},
			reason: "missing_read_history",
		},
		{
			name:   "missing session attachment",
			caps:   Capability{CanForwardSync: true, CanListSessions: true, CanReadHistory: true, CanWaitRun: true, CanStartSessionWithMessage: true},
			reason: "missing_attach_session",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ok, reason := RemoteContinuationSupport(tc.caps); ok != (tc.reason == "") || reason != tc.reason {
				t.Fatalf("got supported=%v reason=%q, want reason=%q", ok, reason, tc.reason)
			}
		})
	}
}
