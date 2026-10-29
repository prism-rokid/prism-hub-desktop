// Package plugin loads PluginBridge adapter plugin manifests.
package plugin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"rokid-prism-hub/internal/pluginruntime"
	"rokid-prism-hub/internal/runtimes"
)

const (
	ManifestYAML = "pluginbridge-plugin.yaml"
	ManifestJSON = "pluginbridge-plugin.json"

	// Legacy manifest filenames from the pre-rename agentbridge era. Kept so
	// plugins installed before the rename are still discovered.
	LegacyManifestYAML = "agentbridge-plugin.yaml"
	LegacyManifestJSON = "agentbridge-plugin.json"

	// DefaultIconURL is deliberately stable. It is used only after a manifest
	// image has been omitted or rejected; callers still render the Lucide bot
	// glyph if this image cannot be loaded by the browser.
	DefaultIconURL = "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/codex.webp"
)

type Manifest struct {
	ID          string `json:"id" yaml:"id"`
	DisplayName string `json:"display_name" yaml:"display_name"`
	Description string `json:"description" yaml:"description"`
	IconURL     string `json:"icon_url" yaml:"icon_url"`
	// IconSVGURL is an optional SVG counterpart for local catalog clients. It
	// is never persisted by the cloud Host Plugin API and must be rendered as an
	// img URL (never fetched and inlined by Hub).
	IconSVGURL          string              `json:"icon_svg_url,omitempty" yaml:"icon_svg_url,omitempty"`
	Version             string              `json:"version" yaml:"version"`
	SchemaVersion       int                 `json:"schema_version,omitempty" yaml:"schema_version,omitempty"`
	ProtocolVersion     string              `json:"plugin_protocol_version,omitempty" yaml:"plugin_protocol_version,omitempty"`
	MinHubVersion       string              `json:"min_hub_version,omitempty" yaml:"min_hub_version,omitempty"`
	RuntimeDependencies RuntimeDependencies `json:"runtime_dependencies,omitempty" yaml:"runtime_dependencies,omitempty"`
	IntegrationMode     string              `json:"integration_mode" yaml:"integration_mode"`
	Transport           string              `json:"transport" yaml:"transport"`
	Command             []string            `json:"command" yaml:"command"`
	SocketPath          string              `json:"socket_path" yaml:"socket_path"`
	Endpoint            string              `json:"endpoint" yaml:"endpoint"`
	Mode                string              `json:"mode" yaml:"mode"`
	// RuntimeModes are Hub-selectable, mutually exclusive Plugin runtime
	// implementations. Every Plugin exposes at least its legacy Mode as a
	// single runtime, so bridge.plugins.<plugin_id>.mode has one uniform
	// meaning for every Plugin.
	RuntimeModes       []string                                         `json:"runtime_modes,omitempty" yaml:"runtime_modes,omitempty"`
	DefaultRuntimeMode string                                           `json:"default_runtime_mode,omitempty" yaml:"default_runtime_mode,omitempty"`
	RuntimeControls    map[string]pluginruntime.RuntimeControlProfile   `json:"runtime_controls,omitempty" yaml:"runtime_controls,omitempty"`
	RuntimeOperations  map[string]pluginruntime.RuntimeOperationProfile `json:"runtime_operations,omitempty" yaml:"runtime_operations,omitempty"`
	// ManagedTerminalModes declares the runtime modes that can expose a local
	// TUI sharing the Plugin-owned transport. Non-empty is the capability; the
	// legacy capabilities.can_open_managed_terminal bool is parse-only.
	ManagedTerminalModes []string `json:"managed_terminal_modes,omitempty" yaml:"managed_terminal_modes,omitempty"`
	// ManagedLaunch keeps its original meaning: the plugin process is under
	// Hub lifecycle management (PRISM_PLUGIN_MANAGED_LAUNCH env contract).
	// It is NOT the "can be launched" signal — see ManagedLaunchSpecs.
	ManagedLaunch bool `json:"managed_launch,omitempty" yaml:"managed_launch,omitempty"`
	// ManagedLaunchSpecs declares externally launchable targets. Presence is
	// the sole launchability signal for remote clients and hub dispatch; the
	// env-contract bool above deliberately does not imply it.
	ManagedLaunchSpecs []ManagedLaunchSpec `json:"managed_launch_specs,omitempty" yaml:"managed_launch_specs,omitempty"`
	// RuntimeIntegration declares the integration kind per runtime mode for
	// plugins whose modes integrate differently (e.g. a CDP-driven desktop
	// mode alongside a WS-gateway native mode). Modes not listed fall back to
	// the top-level integration_mode.
	RuntimeIntegration map[string]string  `json:"runtime_integration,omitempty" yaml:"runtime_integration,omitempty"`
	Capabilities       ManifestCapability `json:"capabilities" yaml:"capabilities"`
}

// Launch target kinds and readiness policies. Keep the string values stable:
// they cross the wire to remote clients.
const (
	LaunchKindDesktopApp = "desktop-app"
	LaunchKindService    = "service"

	LaunchReadyProbe  = "probe"
	LaunchReadyExitOK = "exit_ok"

	defaultLaunchReadyTimeoutMs = 30_000
	minLaunchReadyTimeoutMs     = 5_000
	maxLaunchReadyTimeoutMs     = 120_000
)

// ManagedLaunchSpec declares one externally launchable target. Service specs
// carry their own start/stop argv (exec'd directly, resolved via PATH — never
// a shell); desktop-app specs reference the hub's desktop app registry by app
// id (default: the plugin id), which owns the machine-level launch knowledge.
type ManagedLaunchSpec struct {
	Kind           string   `json:"kind" yaml:"kind"`
	App            string   `json:"app,omitempty" yaml:"app,omitempty"`
	Start          []string `json:"start,omitempty" yaml:"start,omitempty"`
	Stop           []string `json:"stop,omitempty" yaml:"stop,omitempty"`
	Ready          string   `json:"ready,omitempty" yaml:"ready,omitempty"`
	RuntimeModes   []string `json:"runtime_modes,omitempty" yaml:"runtime_modes,omitempty"`
	ReadyTimeoutMs int      `json:"ready_timeout_ms,omitempty" yaml:"ready_timeout_ms,omitempty"`
}

// RuntimeDependencies lists a runtime the Hub must resolve itself.  Empty
// fields retain compatibility with existing self-contained plugins.
type RuntimeDependencies struct {
	Node string `json:"node,omitempty" yaml:"node,omitempty"`
}

type ManifestCapability struct {
	NativeVisibleInput         bool `json:"native_visible_input" yaml:"native_visible_input"`
	NativeVisibleOutput        bool `json:"native_visible_output" yaml:"native_visible_output"`
	CanAttachSession           bool `json:"can_attach_session" yaml:"can_attach_session"`
	CanStartSessionWithMessage bool `json:"can_start_session_with_message" yaml:"can_start_session_with_message"`
	CanOpenDraft               bool `json:"can_open_draft" yaml:"can_open_draft"`
	CanListSessions            bool `json:"can_list_sessions" yaml:"can_list_sessions"`
	CanReadHistory             bool `json:"can_read_history" yaml:"can_read_history"`
	CanInterrupt               bool `json:"can_interrupt" yaml:"can_interrupt"`
	CanApproval                bool `json:"can_approval" yaml:"can_approval"`
	CanForwardSync             bool `json:"can_forward_sync" yaml:"can_forward_sync"`
	CanReverseSync             bool `json:"can_reverse_sync" yaml:"can_reverse_sync"`
	CanPluginWideWatch         bool `json:"can_plugin_wide_watch" yaml:"can_plugin_wide_watch"`
	CanWaitRun                 bool `json:"can_wait_run" yaml:"can_wait_run"`
	CanReadStatus              bool `json:"can_read_status" yaml:"can_read_status"`
	CanControlSession          bool `json:"can_control_session" yaml:"can_control_session"`
	CanOpenManagedTerminal     bool `json:"can_open_managed_terminal" yaml:"can_open_managed_terminal"`
}

type Discovered struct {
	Path     string
	Root     string
	Manifest Manifest
	// SkipRuntimeLease marks short-lived verification processes (release
	// staging probes). They must not touch the per-plugin runtime lease: the
	// lease is owned by the active long-lived runtime, and a probe taking over
	// the file makes each side terminate the other.
	SkipRuntimeLease bool
}

func LoadManifest(path string) (Manifest, error) {
	var manifest Manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return manifest, fmt.Errorf("pluginbridge plugin: read %s: %w", path, err)
	}
	raw, err := decodeManifestRaw(b, strings.ToLower(filepath.Ext(path)))
	if err != nil {
		return manifest, fmt.Errorf("pluginbridge plugin: parse %s: %w", path, err)
	}
	raw = normalizeLegacyManifest(raw)
	encoded, err := encodeManifestRaw(raw, strings.ToLower(filepath.Ext(path)))
	if err != nil {
		return manifest, fmt.Errorf("pluginbridge plugin: normalize %s: %w", path, err)
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&manifest)
	default:
		decoder := yaml.NewDecoder(bytes.NewReader(encoded))
		decoder.KnownFields(true)
		err = decoder.Decode(&manifest)
	}
	if err != nil {
		return manifest, fmt.Errorf("pluginbridge plugin: parse %s: %w", path, err)
	}
	manifest.Normalize()
	if err := manifest.Validate(); err != nil {
		return manifest, fmt.Errorf("pluginbridge plugin: invalid %s: %w", path, err)
	}
	return manifest, nil
}

func ScanDirs(dirs []string) ([]Discovered, error) {
	var out []Discovered
	var errs []string
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		items, err := scanDir(dir)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		out = append(out, items...)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Manifest.ID < out[j].Manifest.ID
	})
	if len(errs) > 0 {
		return out, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, nil
}

func (m *Manifest) Normalize() {
	if m == nil {
		return
	}
	m.ID = strings.ToLower(strings.TrimSpace(m.ID))
	m.DisplayName = strings.TrimSpace(m.DisplayName)
	m.Description = strings.TrimSpace(m.Description)
	m.IconURL = strings.TrimSpace(m.IconURL)
	m.IconSVGURL = strings.TrimSpace(m.IconSVGURL)
	m.Version = strings.TrimSpace(m.Version)
	if m.SchemaVersion < 0 {
		m.SchemaVersion = 0
	}
	m.ProtocolVersion = strings.TrimSpace(m.ProtocolVersion)
	m.MinHubVersion = strings.TrimSpace(m.MinHubVersion)
	m.RuntimeDependencies.Node = strings.TrimSpace(m.RuntimeDependencies.Node)
	m.Transport = strings.ToLower(strings.TrimSpace(m.Transport))
	m.IntegrationMode = normalizeIntegrationMode(m.IntegrationMode, m.Mode)
	m.SocketPath = strings.TrimSpace(m.SocketPath)
	m.Endpoint = strings.TrimRight(strings.TrimSpace(m.Endpoint), "/")
	m.Mode = strings.ToLower(strings.TrimSpace(m.Mode))
	if m.Mode == "" {
		m.Mode = m.Transport
	}
	m.RuntimeModes = normalizeRuntimeModes(m.RuntimeModes)
	if len(m.RuntimeModes) == 0 {
		m.RuntimeModes = []string{m.Mode}
	}
	m.DefaultRuntimeMode = strings.ToLower(strings.TrimSpace(m.DefaultRuntimeMode))
	if m.DefaultRuntimeMode == "" && len(m.RuntimeModes) > 0 {
		m.DefaultRuntimeMode = m.RuntimeModes[0]
	}
	m.RuntimeControls = normalizeRuntimeControls(m.RuntimeControls)
	m.RuntimeOperations = normalizeRuntimeOperations(m.RuntimeOperations)
	m.ManagedTerminalModes = normalizeRuntimeModes(m.ManagedTerminalModes)
	m.RuntimeIntegration = normalizeRuntimeIntegration(m.RuntimeIntegration)
	for i := range m.ManagedLaunchSpecs {
		spec := &m.ManagedLaunchSpecs[i]
		spec.Kind = strings.ToLower(strings.TrimSpace(spec.Kind))
		spec.App = strings.ToLower(strings.TrimSpace(spec.App))
		if spec.App == "" && spec.Kind == LaunchKindDesktopApp {
			spec.App = m.ID
		}
		spec.Ready = strings.ToLower(strings.TrimSpace(spec.Ready))
		if spec.Ready == "" {
			spec.Ready = LaunchReadyProbe
		}
		spec.RuntimeModes = normalizeRuntimeModes(spec.RuntimeModes)
		spec.Start = trimEmpty(spec.Start)
		spec.Stop = trimEmpty(spec.Stop)
		if spec.ReadyTimeoutMs == 0 {
			spec.ReadyTimeoutMs = defaultLaunchReadyTimeoutMs
		}
	}
	m.ManagedLaunchSpecs = normalizeLaunchSpecs(m.ManagedLaunchSpecs)
	for i, item := range m.Command {
		m.Command[i] = strings.TrimSpace(item)
	}
}

func (m Manifest) Validate() error {
	if m.ID == "" {
		return fmt.Errorf("id required")
	}
	switch m.Transport {
	case "process", "stdio":
	case "":
		return fmt.Errorf("transport required")
	default:
		return fmt.Errorf("unsupported transport %q", m.Transport)
	}
	switch m.IntegrationMode {
	case string(pluginruntime.IntegrationModeProtocolNative), string(pluginruntime.IntegrationModeDesktopAutomation):
	case "":
		return fmt.Errorf("integration_mode required")
	default:
		return fmt.Errorf("unsupported integration_mode %q", m.IntegrationMode)
	}
	if len(trimEmpty(m.Command)) == 0 {
		return fmt.Errorf("command required for %s transport", m.Transport)
	}
	if m.IconURL != "" {
		if err := ValidateHTTPSURL(m.IconURL); err != nil {
			return fmt.Errorf("icon_url: %w", err)
		}
	}
	if m.SchemaVersion != 0 && m.SchemaVersion != 1 {
		return fmt.Errorf("unsupported schema_version %d", m.SchemaVersion)
	}
	if m.ProtocolVersion != "" && m.ProtocolVersion != "4" {
		return fmt.Errorf("unsupported plugin_protocol_version %q", m.ProtocolVersion)
	}
	if m.RuntimeDependencies.Node != "" {
		if !runtimes.RequiresNode(m.RuntimeDependencies.Node) {
			return fmt.Errorf("unsupported node runtime range %q", m.RuntimeDependencies.Node)
		}
		if len(m.Command) == 0 || m.Command[0] != "${runtime.node}" {
			return fmt.Errorf("node runtime dependency requires command to start with ${runtime.node}")
		}
	} else {
		for _, argument := range m.Command {
			if strings.Contains(argument, "${runtime.node}") {
				return fmt.Errorf("${runtime.node} requires runtime_dependencies.node")
			}
		}
	}
	if m.IconSVGURL != "" {
		if err := ValidateHTTPSURL(m.IconSVGURL); err != nil {
			return fmt.Errorf("icon_svg_url: %w", err)
		}
	}
	if len(m.RuntimeModes) == 0 || !containsString(m.RuntimeModes, m.DefaultRuntimeMode) {
		return fmt.Errorf("default_runtime_mode %q must appear in runtime_modes", m.DefaultRuntimeMode)
	}
	for mode, profile := range m.RuntimeControls {
		if !containsString(m.RuntimeModes, mode) {
			return fmt.Errorf("runtime_controls mode %q must appear in runtime_modes", mode)
		}
		for kind, implementation := range map[string]pluginruntime.RuntimeControlImplementation{
			"model": profile.Model, "reasoning": profile.Reasoning, "permission": profile.Permission,
		} {
			if implementation != "" && implementation != pluginruntime.RuntimeControlStable && implementation != pluginruntime.RuntimeControlInteractive {
				return fmt.Errorf("runtime_controls.%s.%s has unsupported implementation %q", mode, kind, implementation)
			}
		}
	}
	for mode, profile := range m.RuntimeOperations {
		if !containsString(m.RuntimeModes, mode) {
			return fmt.Errorf("runtime_operations mode %q must appear in runtime_modes", mode)
		}
		for operation, implementation := range map[string]pluginruntime.RuntimeOperationImplementation{
			"rename": profile.Rename, "pin": profile.Pin, "archive": profile.Archive, "delete": profile.Delete,
		} {
			if implementation != "" && implementation != pluginruntime.RuntimeOperationDirect && implementation != pluginruntime.RuntimeOperationMenu && implementation != pluginruntime.RuntimeOperationUnsupported {
				return fmt.Errorf("runtime_operations.%s.%s has unsupported implementation %q", mode, operation, implementation)
			}
		}
	}
	for _, mode := range m.ManagedTerminalModes {
		if !containsString(m.RuntimeModes, mode) {
			return fmt.Errorf("managed_terminal_modes mode %q must appear in runtime_modes", mode)
		}
	}
	// can_plugin_wide_watch only describes the shape of the reverse-sync
	// watcher; without reverse sync the declaration is meaningless.
	if !m.Capabilities.CanReverseSync && m.Capabilities.CanPluginWideWatch {
		return fmt.Errorf("can_plugin_wide_watch requires can_reverse_sync")
	}
	// Forward capabilities are exercised over the forward path; declaring
	// them without the forward path contradicts the capability contract.
	if !m.Capabilities.CanForwardSync {
		for name, declared := range map[string]bool{
			"can_attach_session":             m.Capabilities.CanAttachSession,
			"can_start_session_with_message": m.Capabilities.CanStartSessionWithMessage,
			"can_open_draft":                 m.Capabilities.CanOpenDraft,
			"can_interrupt":                  m.Capabilities.CanInterrupt,
			"can_approval":                   m.Capabilities.CanApproval,
		} {
			if declared {
				return fmt.Errorf("%s requires can_forward_sync", name)
			}
		}
	}
	for mode, kind := range m.RuntimeIntegration {
		if !containsString(m.RuntimeModes, mode) {
			return fmt.Errorf("runtime_integration mode %q must appear in runtime_modes", mode)
		}
		switch kind {
		case string(pluginruntime.IntegrationModeProtocolNative), string(pluginruntime.IntegrationModeDesktopAutomation):
		default:
			return fmt.Errorf("runtime_integration mode %q has unsupported kind %q", mode, kind)
		}
	}
	if err := validateLaunchSpecs(m); err != nil {
		return err
	}
	return nil
}

func validateLaunchSpecs(m Manifest) error {
	seen := make(map[string]bool)
	for _, spec := range m.ManagedLaunchSpecs {
		switch spec.Kind {
		case LaunchKindDesktopApp:
			if len(spec.Start) != 0 || len(spec.Stop) != 0 {
				return fmt.Errorf("managed_launch_specs kind %q must not declare start/stop (the hub desktop app registry owns launch commands)", spec.Kind)
			}
			if spec.App == "" {
				return fmt.Errorf("managed_launch_specs kind %q requires app", spec.Kind)
			}
		case LaunchKindService:
			if len(spec.Start) == 0 {
				return fmt.Errorf("managed_launch_specs kind %q requires start argv", spec.Kind)
			}
			for _, arg := range append(append([]string{}, spec.Start...), spec.Stop...) {
				if strings.ContainsAny(arg, ";|&`$><\n") {
					return fmt.Errorf("managed_launch_specs start/stop must be plain argv without shell metacharacters: %q", arg)
				}
			}
		default:
			return fmt.Errorf("managed_launch_specs kind %q unsupported (want %q or %q)", spec.Kind, LaunchKindDesktopApp, LaunchKindService)
		}
		switch spec.Ready {
		case LaunchReadyProbe, LaunchReadyExitOK:
		default:
			return fmt.Errorf("managed_launch_specs ready %q unsupported (want %q or %q)", spec.Ready, LaunchReadyProbe, LaunchReadyExitOK)
		}
		if spec.ReadyTimeoutMs < minLaunchReadyTimeoutMs || spec.ReadyTimeoutMs > maxLaunchReadyTimeoutMs {
			return fmt.Errorf("managed_launch_specs ready_timeout_ms %d outside [%d, %d]", spec.ReadyTimeoutMs, minLaunchReadyTimeoutMs, maxLaunchReadyTimeoutMs)
		}
		for _, mode := range spec.RuntimeModes {
			if !containsString(m.RuntimeModes, mode) {
				return fmt.Errorf("managed_launch_specs runtime_modes mode %q must appear in runtime_modes", mode)
			}
		}
		if seen[spec.Kind] && spec.Kind == LaunchKindService {
			return fmt.Errorf("managed_launch_specs declares more than one %q target", spec.Kind)
		}
		seen[spec.Kind] = true
	}
	return nil
}

func normalizeRuntimeIntegration(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for mode, kind := range values {
		mode = strings.ToLower(strings.TrimSpace(mode))
		kind = strings.ToLower(strings.TrimSpace(kind))
		if mode == "" || kind == "" {
			continue
		}
		out[mode] = kind
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeLaunchSpecs(specs []ManagedLaunchSpec) []ManagedLaunchSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]ManagedLaunchSpec, 0, len(specs))
	for _, spec := range specs {
		if strings.TrimSpace(spec.Kind) == "" {
			continue
		}
		out = append(out, spec)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ValidateHTTPSURL rejects all URL forms that can access a local resource or
// carry credentials. It purposefully does not fetch or inspect image content.
func ValidateHTTPSURL(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("required")
	}
	if len(value) > 1024 {
		return fmt.Errorf("must be at most 1024 characters")
	}
	u, err := url.ParseRequestURI(value)
	if err != nil || u == nil {
		return fmt.Errorf("invalid absolute HTTPS URL")
	}
	if !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.User != nil {
		return fmt.Errorf("must be an absolute HTTPS URL without credentials")
	}
	return nil
}

// ResolvedIconURLs chooses the common display fallback chain. userIcon is the
// cloud-saved override and therefore always wins when it is safe.  The second
// value is the bitmap fallback for an SVG primary; a UI should finally render
// its existing bot icon when both URLs fail to load.
func (m Manifest) ResolvedIconURLs(userIcon string) (primary, fallback string) {
	if ValidateHTTPSURL(userIcon) == nil {
		if ValidateHTTPSURL(m.IconSVGURL) == nil {
			return strings.TrimSpace(userIcon), m.IconSVGURL
		}
		if ValidateHTTPSURL(m.IconURL) == nil {
			return strings.TrimSpace(userIcon), m.IconURL
		}
		return strings.TrimSpace(userIcon), DefaultIconURL
	}
	if ValidateHTTPSURL(m.IconSVGURL) == nil {
		primary = m.IconSVGURL
		if ValidateHTTPSURL(m.IconURL) == nil {
			return primary, m.IconURL
		}
		return primary, DefaultIconURL
	}
	if ValidateHTTPSURL(m.IconURL) == nil {
		return m.IconURL, DefaultIconURL
	}
	return DefaultIconURL, ""
}

func (m Manifest) AllowsManagedTerminal(mode string) bool {
	// The modes list is the capability; the legacy
	// capabilities.can_open_managed_terminal bool is parse-only.
	return containsString(m.ManagedTerminalModes, strings.ToLower(strings.TrimSpace(mode)))
}

// IntegrationModeFor resolves the integration kind for the active runtime
// mode: a per-mode runtime_integration entry wins over the top-level
// integration_mode. Used for catalog reporting so dual-mode plugins surface
// the mode-accurate value.
func (m Manifest) IntegrationModeFor(configured string) string {
	mode := m.RuntimeMode(configured)
	if kind := strings.TrimSpace(m.RuntimeIntegration[mode]); kind != "" {
		return kind
	}
	return m.IntegrationMode
}

// LaunchSpecsForMode returns the launch specs applicable to the given runtime
// mode (specs without a runtime_modes filter always apply).
func (m Manifest) LaunchSpecsForMode(configured string) []ManagedLaunchSpec {
	mode := m.RuntimeMode(configured)
	if len(m.ManagedLaunchSpecs) == 0 {
		return nil
	}
	out := make([]ManagedLaunchSpec, 0, len(m.ManagedLaunchSpecs))
	for _, spec := range m.ManagedLaunchSpecs {
		if len(spec.RuntimeModes) > 0 && !containsString(spec.RuntimeModes, mode) {
			continue
		}
		out = append(out, spec)
	}
	return out
}

func normalizeRuntimeControls(values map[string]pluginruntime.RuntimeControlProfile) map[string]pluginruntime.RuntimeControlProfile {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]pluginruntime.RuntimeControlProfile, len(values))
	for mode, profile := range values {
		mode = strings.ToLower(strings.TrimSpace(mode))
		if mode == "" {
			continue
		}
		profile.Model = pluginruntime.RuntimeControlImplementation(strings.ToLower(strings.TrimSpace(string(profile.Model))))
		profile.Reasoning = pluginruntime.RuntimeControlImplementation(strings.ToLower(strings.TrimSpace(string(profile.Reasoning))))
		profile.Permission = pluginruntime.RuntimeControlImplementation(strings.ToLower(strings.TrimSpace(string(profile.Permission))))
		out[mode] = profile
	}
	return out
}

func normalizeRuntimeOperations(values map[string]pluginruntime.RuntimeOperationProfile) map[string]pluginruntime.RuntimeOperationProfile {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]pluginruntime.RuntimeOperationProfile, len(values))
	for mode, profile := range values {
		mode = strings.ToLower(strings.TrimSpace(mode))
		if mode == "" {
			continue
		}
		profile.Rename = pluginruntime.RuntimeOperationImplementation(strings.ToLower(strings.TrimSpace(string(profile.Rename))))
		profile.Pin = pluginruntime.RuntimeOperationImplementation(strings.ToLower(strings.TrimSpace(string(profile.Pin))))
		profile.Archive = pluginruntime.RuntimeOperationImplementation(strings.ToLower(strings.TrimSpace(string(profile.Archive))))
		profile.Delete = pluginruntime.RuntimeOperationImplementation(strings.ToLower(strings.TrimSpace(string(profile.Delete))))
		out[mode] = profile
	}
	return out
}

func normalizeRuntimeModes(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// RuntimeMode resolves the configured mode only when the manifest declares
// selectable runtimes. Legacy PluginConfig.Mode values remain untouched.
func (m Manifest) RuntimeMode(configured string) string {
	modes := normalizeRuntimeModes(m.RuntimeModes)
	if len(modes) == 0 {
		return ""
	}
	configured = strings.ToLower(strings.TrimSpace(configured))
	if containsString(modes, configured) {
		return configured
	}
	defaultMode := strings.ToLower(strings.TrimSpace(m.DefaultRuntimeMode))
	if containsString(modes, defaultMode) {
		return defaultMode
	}
	return modes[0]
}

// AllowsRuntimeMode reports whether value is explicitly declared by this
// Plugin. It intentionally does not apply a fallback, so control requests
// cannot silently select a default on a misspelled or stale value.
func (m Manifest) AllowsRuntimeMode(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value != "" && containsString(normalizeRuntimeModes(m.RuntimeModes), value)
}

// RuntimeControlProfile returns the semantic-control ownership declaration for
// the resolved runtime. Missing profiles are retained for legacy manifests and
// mean that Hub must not apply representation filtering.
func (m Manifest) RuntimeControlProfile(configured string) pluginruntime.RuntimeControlProfile {
	mode := m.RuntimeMode(configured)
	if mode == "" || len(m.RuntimeControls) == 0 {
		return pluginruntime.RuntimeControlProfile{}
	}
	return m.RuntimeControls[mode]
}

func (m Manifest) DeclaresRuntimeControls(configured string) bool {
	mode := m.RuntimeMode(configured)
	if mode == "" || len(m.RuntimeControls) == 0 {
		return false
	}
	_, ok := m.RuntimeControls[mode]
	return ok
}

// RuntimeOperationProfile returns the standard conversation-operation owner
// declaration for the resolved runtime.
func (m Manifest) RuntimeOperationProfile(configured string) pluginruntime.RuntimeOperationProfile {
	mode := m.RuntimeMode(configured)
	if mode == "" || len(m.RuntimeOperations) == 0 {
		return pluginruntime.RuntimeOperationProfile{}
	}
	return m.RuntimeOperations[mode]
}

func (m Manifest) DeclaresRuntimeOperations(configured string) bool {
	mode := m.RuntimeMode(configured)
	if mode == "" || len(m.RuntimeOperations) == 0 {
		return false
	}
	_, ok := m.RuntimeOperations[mode]
	return ok
}

func (m Manifest) Capability() pluginruntime.Capability {
	return pluginruntime.Capability{
		PluginID:                   m.ID,
		Available:                  false,
		NativeVisibleInput:         m.Capabilities.NativeVisibleInput,
		NativeVisibleOutput:        m.Capabilities.NativeVisibleOutput,
		CanAttachSession:           m.Capabilities.CanAttachSession,
		CanStartSessionWithMessage: m.Capabilities.CanStartSessionWithMessage,
		CanOpenDraft:               m.Capabilities.CanOpenDraft,
		CanListSessions:            m.Capabilities.CanListSessions,
		CanReadHistory:             m.Capabilities.CanReadHistory,
		CanInterrupt:               m.Capabilities.CanInterrupt,
		CanApproval:                m.Capabilities.CanApproval,
		CanForwardSync:             m.Capabilities.CanForwardSync,
		CanReverseSync:             m.Capabilities.CanReverseSync,
		CanPluginWideWatch:         m.Capabilities.CanPluginWideWatch,
		CanWaitRun:                 m.Capabilities.CanWaitRun,
		CanReadStatus:              m.Capabilities.CanReadStatus,
		CanControlSession:          m.Capabilities.CanControlSession,
		CanOpenManagedTerminal:     m.Capabilities.CanOpenManagedTerminal,
		IntegrationMode:            pluginruntime.IntegrationMode(m.IntegrationMode),
		VisibilitySurface:          m.Mode,
		UnavailableReason:          "plugin_runtime_not_started",
	}
}

func normalizeIntegrationMode(integrationMode, legacyMode string) string {
	value := strings.ToLower(strings.TrimSpace(integrationMode))
	if value != "" {
		return value
	}
	switch strings.ToLower(strings.TrimSpace(legacyMode)) {
	case "desktop", "desktop-ui", "desktop-automation", "automation", "cdp", "accessibility":
		return string(pluginruntime.IntegrationModeDesktopAutomation)
	default:
		return string(pluginruntime.IntegrationModeProtocolNative)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func scanDir(dir string) ([]Discovered, error) {
	var out []Discovered
	if path, manifestRoot := manifestPathInRoot(dir); path != "" {
		manifest, err := LoadManifest(path)
		if err != nil {
			return out, err
		}
		out = append(out, Discovered{Path: path, Root: manifestRoot, Manifest: manifest})
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan plugin dir %s: %w", dir, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		root := filepath.Join(dir, entry.Name())
		path, manifestRoot := manifestPathInRoot(root)
		if path == "" {
			continue
		}
		manifest, err := LoadManifest(path)
		if err != nil {
			return out, err
		}
		out = append(out, Discovered{Path: path, Root: manifestRoot, Manifest: manifest})
	}
	return out, nil
}

func manifestPathInRoot(root string) (string, string) {
	// Official releases use state.json + versions/<active>; read it before the
	// legacy current link so an interrupted migration cannot select stale code.
	if b, err := os.ReadFile(filepath.Join(root, "state.json")); err == nil {
		var state struct {
			ActiveVersion string `json:"active_version"`
		}
		decodeErr := json.Unmarshal(b, &state)
		activeVersion := strings.TrimSpace(state.ActiveVersion)
		if decodeErr == nil && activeVersion != "" && activeVersion == filepath.Base(activeVersion) && activeVersion != "." && activeVersion != ".." {
			versionRoot := filepath.Join(root, "versions", activeVersion)
			for _, name := range []string{ManifestYAML, ManifestJSON, LegacyManifestYAML, LegacyManifestJSON} {
				candidate := filepath.Join(versionRoot, name)
				if _, err := os.Stat(candidate); err == nil {
					return candidate, versionRoot
				}
			}
		}
	}
	candidates := []string{
		filepath.Join(root, ManifestYAML),
		filepath.Join(root, ManifestJSON),
		filepath.Join(root, "current", ManifestYAML),
		filepath.Join(root, "current", ManifestJSON),
		filepath.Join(root, LegacyManifestYAML),
		filepath.Join(root, LegacyManifestJSON),
		filepath.Join(root, "current", LegacyManifestYAML),
		filepath.Join(root, "current", LegacyManifestJSON),
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, filepath.Dir(candidate)
		}
	}
	return "", ""
}

// decodeManifestRaw parses manifest bytes leniently into a generic map so the
// loader can apply legacy field migrations before the strict decode.
func decodeManifestRaw(b []byte, ext string) (map[string]any, error) {
	out := map[string]any{}
	switch ext {
	case ".json":
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	default:
		if err := yaml.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// encodeManifestRaw re-serializes a normalized manifest map back to its
// original format so the strict decoder can validate field names.
func encodeManifestRaw(raw map[string]any, ext string) ([]byte, error) {
	switch ext {
	case ".json":
		return json.Marshal(raw)
	default:
		return yaml.Marshal(raw)
	}
}

// normalizeLegacyManifest applies the field renames / removals needed to
// accept manifests written before the agentbridge -> pluginbridge rename.
//
// Specifically:
//   - top-level `agent_id` is folded into `id` (id takes precedence if both
//     are present), then the legacy `agent_id` key is removed
//   - capability keys that were removed in the current schema
//     (`can_create_session`, `can_resume_session`) are dropped
func normalizeLegacyManifest(raw map[string]any) map[string]any {
	if raw == nil {
		return raw
	}
	if legacyID, ok := raw["agent_id"]; ok {
		if current, present := raw["id"]; !present || isEmptyStringish(current) {
			raw["id"] = legacyID
		}
		delete(raw, "agent_id")
	}
	if caps, ok := raw["capabilities"].(map[string]any); ok {
		delete(caps, "can_create_session")
		delete(caps, "can_resume_session")
		raw["capabilities"] = caps
	}
	return raw
}

func isEmptyStringish(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(x) == ""
	}
	return false
}
