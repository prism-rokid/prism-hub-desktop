package scaffold

import (
	"fmt"
	"path/filepath"
	"strings"
)

// manifestYAML renders the pluginbridge-plugin.yaml for the given options. The
// command points at the template-specific entry binary/script so the plugin is
// runnable after a single build/install step.
func manifestYAML(opts Options) string {
	cmd := manifestCommand(opts.Template, opts)
	mode := "protocol"
	if strings.TrimSpace(opts.IntegrationMode) == "desktop-automation" {
		mode = "desktop"
	}
	runtimeDependencies := ""
	if opts.Template == NodeTemplate {
		runtimeDependencies = "runtime_dependencies:\n  node: \">=22.0.0 <23.0.0\"\n"
	}
	return fmt.Sprintf(`id: %s
display_name: %s
version: 0.1.0
schema_version: 1
plugin_protocol_version: "4"
icon_url: https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/codex.webp
icon_svg_url: https://registry.npmmirror.com/@lobehub/icons-static-svg/latest/files/icons/codex.svg
integration_mode: %s
transport: process
mode: %s
runtime_modes: [%s]
default_runtime_mode: %s
runtime_controls:
  %s: {}
runtime_operations:
  %s:
    rename: unsupported
    pin: unsupported
    archive: unsupported
    delete: unsupported
%scommand: %s
capabilities:
  native_visible_input: true
  native_visible_output: true
  can_start_session_with_message: true
  can_list_sessions: false
  can_read_history: false
  can_interrupt: true
  can_forward_sync: true
  can_reverse_sync: false
  can_wait_run: false
`, opts.PluginID, opts.DisplayName, opts.IntegrationMode, mode, mode, mode, mode, mode, runtimeDependencies, cmd)
}

// manifestCommand returns the YAML list literal for the launch command. The Go
// adapter is a compiled binary; node/python run via interpreter on the entry.
func manifestCommand(t Template, opts Options) string {
	switch t {
	case GoTemplate:
		return fmt.Sprintf(`["./bin/%s"]`, goBinaryName(opts))
	case NodeTemplate:
		return `["${runtime.node}", "index.js"]`
	case PythonTemplate:
		return `["python", "adapter.py"]`
	}
	return `["./adapter"]`
}

// ---- Go template ----

func writeGoTemplate(dir string, opts Options) ([]string, error) {
	files := make(map[string]string)
	files["go.mod"] = goModContent(opts)
	files[filepath.Join("cmd", opts.PluginID+"-adapter", "main.go")] = goMainContent(opts)
	files["README.md"] = goReadme(opts)
	return persist(dir, files)
}

func goModContent(opts Options) string {
	return fmt.Sprintf(`module %s

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/Rokid-Prism/prism-plugin-sdk v0.1.0
)
`, opts.PluginID)
}

func goMainContent(opts Options) string {
	return fmt.Sprintf(`// %s — a scaffolded PluginBridge plugin (Go SDK).
//
// Build: go build -o ./bin/%[5]s ./cmd/%[3]s-adapter
// Test:  prism plugin test --path .
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

type echoAdapter struct{}

func (a *echoAdapter) ID() string { return "%[2]s" }

func (a *echoAdapter) Probe(context.Context) (pluginbridge.Capability, error) {
	return pluginbridge.Capability{
		PluginID:            "%[2]s",
		Available:          true,
		NativeVisibleInput: true,
		CanStartSessionWithMessage: true,
		CanListSessions:    false,
		CanReadHistory:     false,
		CanForwardSync:     true,
		IntegrationMode:    pluginbridge.%[4]s,
		VisibilitySurface:  "%[2]s",
	}, nil
}

func (a *echoAdapter) Discover(context.Context) (pluginbridge.DiscoveryResult, error) {
	return pluginbridge.DiscoveryResult{PluginID: "%[2]s", Surface: "%[2]s", Verified: true}, nil
}

func (a *echoAdapter) StartSessionWithMessage(_ context.Context, req pluginbridge.StartSessionWithMessageRequest) (pluginbridge.StartSessionWithMessageResult, error) {
	if req.Message.PrismMessageID == "" || req.Message.Text == "" {
		return pluginbridge.StartSessionWithMessageResult{}, fmt.Errorf("first message id and text required")
	}
	session := pluginbridge.NativeSession{PluginID: "%[2]s", NativeSessionID: "%[2]s:" + req.Message.PrismMessageID, NativeThreadID: "%[2]s:" + req.Message.PrismMessageID, Surface: "%[2]s", Cwd: req.Cwd, Visible: true}
	return pluginbridge.StartSessionWithMessageResult{Session: session, Receipt: pluginbridge.SendReceipt{NativeMessageID: req.Message.PrismMessageID, Accepted: true, Visible: true}, Visibility: pluginbridge.VisibilityResult{Visible: true, Marker: req.Message.Text, Evidence: "echo adapter", CheckedAt: time.Now().UTC()}}, nil
}

func (a *echoAdapter) Send(_ context.Context, _ pluginbridge.NativeSession, msg pluginbridge.InboundMessage) (pluginbridge.SendReceipt, error) {
	return pluginbridge.SendReceipt{
		NativeMessageID: uuid.NewString(),
		Accepted:        true,
		Visible:         true,
		Detail:          fmt.Sprintf("echoed %%d chars", len(msg.Text)),
	}, nil
}

func (a *echoAdapter) Subscribe(ctx context.Context, _ pluginbridge.NativeSession) (<-chan pluginbridge.PluginEvent, error) {
	ch := make(chan pluginbridge.PluginEvent, 1)
	go func() {
		select {
		case <-ctx.Done():
		case ch <- pluginbridge.PluginEvent{
			ID:        uuid.NewString(),
			Type:      "completed",
			Status:    "completed",
			Summary:   "echo done",
			CreatedAt: time.Now().UTC(),
		}:
		}
		close(ch)
	}()
	return ch, nil
}

func (a *echoAdapter) Interrupt(context.Context, pluginbridge.NativeSession, string) error { return nil }

func (a *echoAdapter) VerifyVisibility(_ context.Context, _ pluginbridge.NativeSession, marker string) (pluginbridge.VisibilityResult, error) {
	return pluginbridge.VisibilityResult{
		Visible:   true,
		Marker:    marker,
		Evidence:  "echo adapter always visible",
		CheckedAt: time.Now().UTC(),
	}, nil
}

func (a *echoAdapter) Close(context.Context) error { return nil }

func main() {
	server := &pluginbridge.StdioServer{
		Adapter: &echoAdapter{},
		In:      os.Stdin,
		Out:     os.Stdout,
	}
	if err := server.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
`, opts.PluginID, opts.PluginID, opts.PluginID, goIntegrationModeConst(opts), goBinaryName(opts))
}

func goReadme(opts Options) string {
	return fmt.Sprintf("# %s\n\nScaffolded PluginBridge plugin (Go SDK).\n\n## Build\n\n```sh\ngo build -o ./bin/%s ./cmd/%s-adapter\n```\n\n## Test\n\n```sh\nprism plugin test --path .\n```\n", opts.PluginID, goBinaryName(opts), opts.PluginID)
}

func goBinaryName(opts Options) string {
	name := strings.TrimSpace(opts.PluginID)
	if name == "" {
		name = strings.TrimSpace(opts.PluginID)
	}
	if name == "" {
		name = "adapter"
	}
	return name + "-adapter"
}

// ---- Node template ----

func writeNodeTemplateFiles(dir string, opts Options) ([]string, error) {
	files := make(map[string]string)
	files["index.js"] = nodeIndexContent(opts)
	files["package.json"] = nodePackageContent(opts)
	files["README.md"] = nodeReadme(opts)
	return persist(dir, files)
}

func nodeIndexContent(opts Options) string {
	return fmt.Sprintf(`// %s — scaffolded PluginBridge plugin (Node SDK).
//
// Run:  node index.js
// Test: prism plugin test --path .
//
// Requires the PluginBridge Node SDK. Adjust the import path to match your
// install (@rokid/pluginbridge-plugin-sdk) or local path.
const { randomUUID } = require("crypto");
const {
  serve,
} = require("@rokid/pluginbridge-plugin-sdk");

class EchoAdapter {
  id() { return "%[2]s"; }

  probe() {
    return {
      PluginID: "%[2]s",
      Available: true,
      NativeVisibleInput: true,
      NativeVisibleOutput: true,
      CanStartSessionWithMessage: true,
      CanListSessions: false,
      CanReadHistory: false,
      CanInterrupt: true,
      CanForwardSync: true,
      IntegrationMode: "%[3]s",
      VisibilitySurface: "%[2]s",
    };
  }

  discover() {
    return { PluginID: "%[2]s", Surface: "%[2]s", Verified: true };
  }

  startSessionWithMessage(req) {
    const message = req && req.Message ? req.Message : {};
    if (!message.PrismMessageID || !message.Text) throw new Error("first message id and text required");
    const id = "%[2]s:" + message.PrismMessageID;
    return { Session: { PluginID: "%[2]s", NativeSessionID: id, NativeThreadID: id, Surface: "%[2]s", Cwd: req.Cwd || "", Visible: true }, Receipt: { NativeMessageID: message.PrismMessageID, Accepted: true, Visible: true, Detail: "echoed first message" }, Visibility: { Visible: true, Marker: message.Text, Evidence: "echo adapter", CheckedAt: new Date().toISOString(), FailureReason: "" } };
  }

  send(_session, msg) {
    return {
      NativeMessageID: randomUUID(),
      Accepted: true,
      Visible: true,
      Detail: `+"`echoed ${msg.Text.length} chars`"+`,
    };
  }

  async *subscribe(_session) {
    yield {
      ID: randomUUID(),
      Type: "completed",
      Status: "completed",
      Summary: "echo done",
    };
  }

  interrupt() { return Promise.resolve(); }
  verifyVisibility(_session, marker) {
    return { Visible: true, Marker: marker, Evidence: "echo adapter always visible" };
  }
  close() { return Promise.resolve(); }
}

serve(new EchoAdapter()).catch((err) => {
  console.error(err);
  process.exit(1);
});
`, opts.PluginID, opts.PluginID, opts.IntegrationMode)
}

func nodePackageContent(opts Options) string {
	return fmt.Sprintf(`{
  "name": "%s",
  "version": "0.1.0",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "@rokid/pluginbridge-plugin-sdk": "^0.1.0"
  }
}
`, opts.PluginID)
}

func nodeReadme(opts Options) string {
	return fmt.Sprintf("# %s\n\nScaffolded PluginBridge plugin (Node SDK).\n\n## Run\n\n```sh\nnpm install\nnode index.js\n```\n\n## Test\n\n```sh\nprism plugin test --path .\n```\n", opts.PluginID)
}

// ---- Python template ----

func writePythonTemplate(dir string, opts Options) ([]string, error) {
	files := make(map[string]string)
	files["adapter.py"] = pythonAdapterContent(opts)
	files["requirements.txt"] = "rokid-pluginbridge-plugin-sdk>=0.1.0\n"
	files["README.md"] = pythonReadme(opts)
	return persist(dir, files)
}

func pythonAdapterContent(opts Options) string {
	return fmt.Sprintf(`#!/usr/bin/env python3
"""%s — scaffolded PluginBridge plugin (Python SDK).

Run:   python adapter.py
Test:  prism plugin test --path .
"""
import uuid
from datetime import datetime, timezone

from pluginbridge import (
    PluginEvent,
    Capability,
    DiscoveryResult,
    NativeSession,
    SendReceipt,
    StartSessionWithMessageRequest,
    StartSessionWithMessageResult,
    VisibilityResult,
    serve,
)


class EchoAdapter:
    def id(self) -> str:
        return "%[2]s"

    def probe(self) -> Capability:
        return Capability(
            PluginID="%[2]s",
            Available=True,
            NativeVisibleInput=True,
            NativeVisibleOutput=True,
            CanStartSessionWithMessage=True,
            CanListSessions=False,
            CanReadHistory=False,
            CanInterrupt=True,
            CanForwardSync=True,
            IntegrationMode="%[3]s",
            VisibilitySurface="%[2]s",
        )

    def discover(self) -> DiscoveryResult:
        return DiscoveryResult(PluginID="%[2]s", Surface="%[2]s", Verified=True)

    def start_session_with_message(self, req: StartSessionWithMessageRequest) -> StartSessionWithMessageResult:
        if not req.Message.PrismMessageID or not req.Message.Text:
            raise ValueError("first message id and text required")
        session_id = "%[2]s:" + req.Message.PrismMessageID
        return StartSessionWithMessageResult(
            Session=NativeSession(PluginID="%[2]s", NativeSessionID=session_id, NativeThreadID=session_id, Surface="%[2]s", Cwd=req.Cwd, Visible=True),
            Receipt=SendReceipt(NativeMessageID=req.Message.PrismMessageID, Accepted=True, Visible=True, Detail="echoed first message"),
            Visibility=VisibilityResult(Visible=True, Marker=req.Message.Text, Evidence="echo adapter", CheckedAt=datetime.now(timezone.utc).isoformat()),
        )

    def send(self, _session, msg) -> SendReceipt:
        return SendReceipt(
            NativeMessageID=str(uuid.uuid4()),
            Accepted=True,
            Visible=True,
            Detail=f"echoed {len(msg.Text)} chars",
        )

    def subscribe(self, _session):
        yield PluginEvent(
            ID=str(uuid.uuid4()),
            Type="completed",
            Status="completed",
            Summary="echo done",
            CreatedAt=datetime.now(timezone.utc),
        )

    def interrupt(self, _session, _correlation_id) -> None:
        return None

    def verify_visibility(self, _session, marker) -> VisibilityResult:
        return VisibilityResult(
            Visible=True,
            Marker=marker,
            Evidence="echo adapter always visible",
        )

    def close(self) -> None:
        return None


if __name__ == "__main__":
    serve(EchoAdapter())
`, opts.PluginID, opts.PluginID, opts.IntegrationMode)
}

func goIntegrationModeConst(opts Options) string {
	if strings.TrimSpace(opts.IntegrationMode) == "desktop-automation" {
		return "IntegrationModeDesktopAutomation"
	}
	return "IntegrationModeProtocolNative"
}

func pythonReadme(opts Options) string {
	return fmt.Sprintf("# %s\n\nScaffolded PluginBridge plugin (Python SDK).\n\n## Run\n\n```sh\npip install -r requirements.txt\npython adapter.py\n```\n\n## Test\n\n```sh\nprism plugin test --path .\n```\n", opts.PluginID)
}

// persist writes each (relPath -> content) into dir, preserving existing files.
func persist(dir string, files map[string]string) ([]string, error) {
	var written []string
	for rel, content := range files {
		abs := filepath.Join(dir, rel)
		if err := writeFilePreserving(abs, content); err != nil {
			return nil, fmt.Errorf("pluginscaffold: write %s: %w", rel, err)
		}
		written = append(written, abs)
	}
	return written, nil
}

// guard against unused import warnings if strings becomes unused later
var _ = strings.TrimSpace
