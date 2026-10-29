package pluginruntime

import (
	"context"
	"testing"
)

func TestServiceStatusUsesProbeAndDiscovery(t *testing.T) {
	svc := New(StaticPlugin{
		PluginID: "OpenClaw",
		Capability: Capability{
			Available:          true,
			NativeVisibleInput: true,
			VisibilitySurface:  "gateway",
		},
		Discovery: DiscoveryResult{
			Endpoint: "ws://127.0.0.1:1234",
		},
	})

	statuses := svc.Status(context.Background())
	if len(statuses) != 1 {
		t.Fatalf("expected one status, got %+v", statuses)
	}
	got := statuses[0]
	if got.ID != "openclaw" || !got.Available || !got.Visible || got.Surface != "gateway" || got.Endpoint == "" {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestServiceReplaceAndUnregister(t *testing.T) {
	first := &closingAdapter{PluginID: "Demo"}
	second := &closingAdapter{PluginID: "demo"}
	svc := New(first)
	if previous := svc.Replace(second); previous != first {
		t.Fatalf("expected previous adapter, got %#v", previous)
	}
	if got := svc.Adapter("DEMO"); got != second {
		t.Fatalf("expected replacement adapter, got %#v", got)
	}
	if removed := svc.Unregister("demo"); removed != second {
		t.Fatalf("expected removed adapter, got %#v", removed)
	}
	if got := svc.Adapter("demo"); got != nil {
		t.Fatalf("expected adapter to be removed, got %#v", got)
	}
}

func TestServiceCloseStopsAndDetachesEveryAdapter(t *testing.T) {
	first := &closingAdapter{PluginID: "first"}
	second := &closingAdapter{PluginID: "second"}
	svc := New(first, second)

	if err := svc.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if first.Closed != 1 || second.Closed != 1 || len(svc.AdapterIDs()) != 0 {
		t.Fatalf("service did not close all adapters: first=%d second=%d ids=%v",
			first.Closed, second.Closed, svc.AdapterIDs())
	}
}

type closingAdapter struct {
	PluginID string
	Closed   int
}

func (a *closingAdapter) ID() string { return a.PluginID }

func (a *closingAdapter) Probe(context.Context) (Capability, error) {
	return Capability{PluginID: a.PluginID, Available: true, NativeVisibleInput: true}, nil
}

func (a *closingAdapter) Discover(context.Context) (DiscoveryResult, error) {
	return DiscoveryResult{PluginID: a.PluginID, Verified: true}, nil
}

func (a *closingAdapter) Send(context.Context, NativeSession, InboundMessage) (SendReceipt, error) {
	return SendReceipt{}, nil
}

func (a *closingAdapter) Subscribe(context.Context, NativeSession) (<-chan PluginEvent, error) {
	return nil, nil
}

func (a *closingAdapter) Interrupt(context.Context, NativeSession, string) error { return nil }

func (a *closingAdapter) VerifyVisibility(context.Context, NativeSession, string) (VisibilityResult, error) {
	return VisibilityResult{}, nil
}

func (a *closingAdapter) Close(context.Context) error {
	a.Closed++
	return nil
}
