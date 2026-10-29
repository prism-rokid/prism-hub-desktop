package pluginruntime

import (
	"context"
	"fmt"
)

type StaticPlugin struct {
	PluginID   string
	Capability Capability
	Discovery  DiscoveryResult
	Err        error
}

func (a StaticPlugin) ID() string {
	return a.PluginID
}

func (a StaticPlugin) Probe(context.Context) (Capability, error) {
	if a.Err != nil {
		return Capability{PluginID: a.PluginID}, a.Err
	}
	capability := a.Capability
	if capability.PluginID == "" {
		capability.PluginID = a.PluginID
	}
	return capability, nil
}

func (a StaticPlugin) Discover(context.Context) (DiscoveryResult, error) {
	if a.Err != nil {
		return DiscoveryResult{PluginID: a.PluginID}, a.Err
	}
	discovery := a.Discovery
	if discovery.PluginID == "" {
		discovery.PluginID = a.PluginID
	}
	return discovery, nil
}

func (a StaticPlugin) Send(context.Context, NativeSession, InboundMessage) (SendReceipt, error) {
	return SendReceipt{}, fmt.Errorf("pluginbridge: static plugin cannot send")
}

func (a StaticPlugin) Subscribe(context.Context, NativeSession) (<-chan PluginEvent, error) {
	return nil, fmt.Errorf("pluginbridge: static plugin cannot subscribe")
}

func (a StaticPlugin) Interrupt(context.Context, NativeSession, string) error {
	return fmt.Errorf("pluginbridge: static plugin cannot interrupt")
}

func (a StaticPlugin) VerifyVisibility(context.Context, NativeSession, string) (VisibilityResult, error) {
	return VisibilityResult{}, fmt.Errorf("pluginbridge: static plugin cannot verify visibility")
}

func (a StaticPlugin) Close(context.Context) error {
	return nil
}
