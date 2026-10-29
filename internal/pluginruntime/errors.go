package pluginruntime

import "strings"

// AdapterError preserves a PluginBridge RPC error code across the stdio
// boundary. Consumers must allowlist codes before exposing them to clients.
type AdapterError struct {
	Code    string
	Message string
}

func (e *AdapterError) Error() string {
	if e == nil {
		return "plugin adapter error"
	}
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = "request failed"
	}
	return message
}
