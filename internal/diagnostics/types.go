// Package diagnostics 定义 Hub 与插件共用的运行状态分类模型。
package diagnostics

import (
	"fmt"
	"strings"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

type State string

const (
	StateChecking           State = "checking"
	StateNeedsConfiguration State = "needs_configuration"
	StateNotInstalled       State = "not_installed"
	StateNotRunning         State = "not_running"
	StateConnecting         State = "connecting"
	StateIncompatible       State = "incompatible"
	StateUnavailable        State = "unavailable"
	StateReady              State = "ready"
	StateUnknown            State = "unknown"
)

type Facts struct {
	Installed          *bool  `json:"installed,omitempty"`
	Running            *bool  `json:"running,omitempty"`
	Transport          string `json:"transport,omitempty"`
	TransportReachable *bool  `json:"transport_reachable,omitempty"`
}

type Status struct {
	State           State    `json:"state"`
	Code            string   `json:"code"`
	Available       bool     `json:"available"`
	Known           bool     `json:"known"`
	Summary         string   `json:"summary,omitempty"`
	Detail          string   `json:"detail,omitempty"`
	Component       string   `json:"component,omitempty"`
	ActualVersion   string   `json:"actual_version,omitempty"`
	RequiredVersion string   `json:"required_version,omitempty"`
	Facts           Facts    `json:"facts"`
	Actions         []string `json:"actions,omitempty"`
	CheckedAt       string   `json:"checked_at"`
	Revision        uint64   `json:"revision"`
	DiagnosticID    string   `json:"diagnostic_id,omitempty"`
	Stack           string   `json:"-"`
}

type Input struct {
	PluginID           string
	Component          string
	Available          bool
	ProbeKnown         bool
	ProbeError         string
	LegacyReason       string
	LocationError      string
	ManualLocation     bool
	Installed          *bool
	Running            *bool
	Transport          string
	TransportReachable *bool
	Plugin             *pluginruntime.Diagnostic
	CheckedAt          time.Time
	Revision           uint64
}

func boolPtr(value bool) *bool { return &value }

func Classify(input Input) Status {
	checkedAt := input.CheckedAt
	if checkedAt.IsZero() {
		checkedAt = time.Now().UTC()
	}
	status := Status{
		State:        StateUnknown,
		Code:         "runtime_unknown",
		Available:    input.Available,
		Known:        input.ProbeKnown,
		Component:    strings.TrimSpace(input.Component),
		Facts:        Facts{Installed: input.Installed, Running: input.Running, Transport: strings.TrimSpace(input.Transport), TransportReachable: input.TransportReachable},
		CheckedAt:    checkedAt.Format(time.RFC3339Nano),
		Revision:     input.Revision,
		DiagnosticID: diagnosticID(input.PluginID, input.Revision),
	}
	if input.Available {
		status.State = StateReady
		status.Code = "ready"
		status.Summary = "Agent 已连接"
		status.Known = true
		status.Actions = nil
	}
	if input.Plugin != nil {
		applyPluginDiagnostic(&status, input.Plugin)
	}
	if status.State == StateReady {
		return status
	}
	if status.Code != "runtime_unknown" || status.Summary != "" {
		return status
	}
	if input.LocationError != "" {
		status.State = StateNotInstalled
		status.Code = "location_invalid"
		if !input.ManualLocation {
			status.Code = "program_not_found"
		}
		status.Summary = "未找到 Agent"
		status.Detail = input.LocationError
		status.Actions = []string{"locate_agent", "recheck"}
		return status
	}
	if input.Installed != nil && !*input.Installed {
		status.State = StateNotInstalled
		status.Code = "app_not_installed"
		status.Summary = "未安装桌面端"
		status.Actions = []string{"open_download", "locate_agent", "recheck"}
		return status
	}
	if input.Running != nil && !*input.Running {
		status.State = StateNotRunning
		status.Code = "not_running"
		status.Summary = "已安装，尚未启动"
		status.Actions = []string{"managed_relaunch", "recheck"}
		return status
	}
	if input.TransportReachable != nil && !*input.TransportReachable {
		status.State = StateUnavailable
		status.Code = "control_channel_unreachable"
		status.Summary = "应用已运行，但控制通道未连通"
		status.Actions = []string{"managed_relaunch", "recheck"}
		return status
	}
	if input.ProbeError != "" {
		status.State = StateUnavailable
		status.Code = "runtime_unavailable"
		status.Summary = "Agent 暂不可用"
		status.Detail = input.ProbeError
		status.Actions = []string{"recheck"}
		return status
	}
	lowerReason := strings.ToLower(strings.TrimSpace(input.LegacyReason))
	if strings.Contains(lowerReason, "auth") || strings.Contains(lowerReason, "token") || strings.Contains(lowerReason, "鉴权") || strings.Contains(lowerReason, "登录") {
		status.State = StateUnavailable
		status.Code = "authentication_failed"
		status.Summary = "Agent 验证失败"
		status.Detail = input.LegacyReason
		status.Actions = []string{"recheck"}
		return status
	}
	if strings.Contains(lowerReason, "state_db") || strings.Contains(lowerReason, "session data") || strings.Contains(lowerReason, "会话数据") || strings.Contains(lowerReason, "codex_home") {
		status.State = StateUnavailable
		status.Code = "data_unavailable"
		status.Summary = "Agent 数据不可用"
		status.Detail = input.LegacyReason
		status.Actions = []string{"recheck"}
		return status
	}
	if input.ProbeKnown {
		status.State = StateConnecting
		status.Code = "runtime_connecting"
		status.Summary = "正在连接 Agent"
		status.Actions = []string{"recheck"}
	}
	return status
}

func applyPluginDiagnostic(status *Status, diagnostic *pluginruntime.Diagnostic) {
	if diagnostic == nil {
		return
	}
	if value := strings.TrimSpace(diagnostic.State); value != "" {
		status.State = State(value)
	}
	if value := strings.TrimSpace(diagnostic.Code); value != "" {
		status.Code = value
	}
	if value := strings.TrimSpace(diagnostic.Summary); value != "" {
		status.Summary = value
	}
	if value := strings.TrimSpace(diagnostic.Detail); value != "" {
		status.Detail = value
	}
	if value := strings.TrimSpace(diagnostic.Component); value != "" {
		status.Component = value
	}
	status.ActualVersion = strings.TrimSpace(diagnostic.ActualVersion)
	status.RequiredVersion = strings.TrimSpace(diagnostic.RequiredVersion)
	status.Actions = append([]string(nil), diagnostic.Actions...)
	status.Stack = diagnostic.Stack
	if status.Summary == "" && status.Code == "dependency_version_mismatch" {
		status.Summary = "依赖版本不满足要求"
	}
	if status.Detail == "" && status.ActualVersion != "" && status.RequiredVersion != "" {
		status.Detail = fmt.Sprintf("检测到 %s，要求 %s 或更新版本。", status.ActualVersion, status.RequiredVersion)
	}
	if status.State == "" {
		status.State = StateUnavailable
	}
}

func diagnosticID(pluginID string, revision uint64) string {
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" || revision == 0 {
		return ""
	}
	return fmt.Sprintf("diag-%s-%d", pluginID, revision)
}
