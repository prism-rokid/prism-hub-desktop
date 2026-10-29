package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"rokid-prism-hub/internal/host"
)

type daemonRuntimeState struct {
	UpdatedAt             string `json:"updated_at"`
	DaemonPID             int    `json:"daemon_pid,omitempty"`
	ConnectionState       string `json:"connection_state"`
	ConnectionLabel       string `json:"connection_label,omitempty"`
	ConnectionDetail      string `json:"connection_detail,omitempty"`
	HostPluginSyncState   string `json:"host_plugin_sync_state,omitempty"`
	HostPluginSyncDetail  string `json:"host_plugin_sync_detail,omitempty"`
	HostPluginSyncUpdated string `json:"host_plugin_sync_updated_at,omitempty"`
	HostPluginCount       int    `json:"host_plugin_count,omitempty"`
}

var daemonRuntimeStatePath = func() string {
	return host.DefaultDataPath("daemon-runtime-state.json")
}

const daemonRuntimeStateTempMaxAge = 10 * time.Minute

func daemonRuntimeStateLabel(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "connected":
		return "已连接"
	case "connecting":
		return "连接中"
	case "reconnecting":
		return "重连中"
	case "auth_expired":
		return "登录已过期"
	case "unauthorized":
		return "鉴权失败"
	case "gateway_rejected":
		return "网关拒绝连接"
	case "disconnected":
		return "未连接"
	case "stopped", "closed":
		return "已断开"
	default:
		return ""
	}
}

func writeDaemonRuntimeState(path string, state daemonRuntimeState) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	state.ConnectionState = strings.TrimSpace(state.ConnectionState)
	state.ConnectionDetail = strings.TrimSpace(state.ConnectionDetail)
	state.HostPluginSyncState = strings.TrimSpace(state.HostPluginSyncState)
	state.HostPluginSyncDetail = strings.TrimSpace(state.HostPluginSyncDetail)
	// Connection callbacks update the state while retaining the previous record.
	// Derive known labels from the current state so a reconnect cannot carry an
	// old "disconnected" label into an otherwise connected record.
	if label := daemonRuntimeStateLabel(state.ConnectionState); label != "" {
		state.ConnectionLabel = label
	}
	if strings.TrimSpace(state.UpdatedAt) == "" {
		state.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	cleanupDaemonRuntimeStateTemps(path, time.Now())
	tmp, err := os.CreateTemp(filepath.Dir(path), "daemon-runtime-state-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(state); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// A forced process exit can bypass the deferred temporary-file removal. Keep
// recent files for an in-flight writer, but clear stale files on later writes.
func cleanupDaemonRuntimeStateTemps(path string, now time.Time) {
	dir := filepath.Dir(path)
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	pattern := filepath.Join(dir, base+"-*.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	for _, candidate := range paths {
		info, err := os.Stat(candidate)
		if err != nil || now.Sub(info.ModTime()) < daemonRuntimeStateTempMaxAge {
			continue
		}
		_ = os.Remove(candidate)
	}
}

func readDaemonRuntimeState(path string) (*daemonRuntimeState, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var state daemonRuntimeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	state.ConnectionState = strings.TrimSpace(state.ConnectionState)
	state.ConnectionLabel = strings.TrimSpace(state.ConnectionLabel)
	state.ConnectionDetail = strings.TrimSpace(state.ConnectionDetail)
	// Also repair records written by older Hub versions when reading them.
	if label := daemonRuntimeStateLabel(state.ConnectionState); label != "" {
		state.ConnectionLabel = label
	}
	return &state, nil
}

func removeDaemonRuntimeState(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
