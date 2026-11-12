package plugin

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"rokid-prism-hub/internal/host"
)

// runtimeLease survives an ungraceful Hub exit. It identifies only the local
// process group that Prism owns, allowing the next Hub start to reap it before
// creating another runtime for the same plugin.
type runtimeLease struct {
	path         string
	pluginID     string
	sessionToken string
	pid          int
}

type runtimeLeaseRecord struct {
	PID             int    `json:"pid"`
	PluginID        string `json:"plugin_id"`
	SessionToken    string `json:"session_token"`
	StartIdentifier string `json:"start_identifier,omitempty"`
	ProcessGroupID  int    `json:"process_group_id,omitempty"`
}

type runtimeLeaseProcess struct {
	Alive           bool
	StartIdentifier string
	Command         string
	ProcessGroupID  int
}

var (
	inspectRuntimeLeaseProcess   = runtimeLeaseProcessInspection
	terminateRuntimeLeaseProcess = terminatePluginProcessPID
)

func prepareRuntimeLease(pluginID, sessionToken string) (*runtimeLease, error) {
	pluginID = strings.TrimSpace(pluginID)
	sessionToken = strings.TrimSpace(sessionToken)
	if pluginID == "" || sessionToken == "" {
		return nil, fmt.Errorf("plugin runtime lease identity required")
	}
	path := host.DefaultDataPath(filepath.Join("runtime", "plugins", pluginProcessLabel(pluginID)+".json"))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if previous, ok := readRuntimeLease(path); ok {
		removable, shouldTerminate, err := runtimeLeaseRecoveryDecision(previous)
		if err != nil {
			return nil, fmt.Errorf("recover plugin runtime lease %s: %w", pluginID, err)
		}
		if shouldTerminate {
			if err := terminateRuntimeLeaseProcess(previous.PID); err != nil {
				return nil, fmt.Errorf("terminate stale plugin runtime pid %d: %w", previous.PID, err)
			}
		}
		if removable {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return nil, err
			}
		}
	}
	return &runtimeLease{path: path, pluginID: pluginID, sessionToken: sessionToken}, nil
}

func (l *runtimeLease) claim(pid int) error {
	if l == nil || l.path == "" || pid <= 0 {
		return fmt.Errorf("invalid plugin runtime lease")
	}
	l.pid = pid
	process, err := inspectRuntimeLeaseProcess(pid)
	if err != nil {
		return fmt.Errorf("inspect plugin runtime process %d: %w", pid, err)
	}
	if process.StartIdentifier == "" {
		return fmt.Errorf("plugin runtime process %d has no stable start identity", pid)
	}
	payload, err := json.Marshal(runtimeLeaseRecord{
		PID:             pid,
		PluginID:        l.pluginID,
		SessionToken:    l.sessionToken,
		StartIdentifier: process.StartIdentifier,
		ProcessGroupID:  process.ProcessGroupID,
	})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(l.path), ".runtime-lease-*.json")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, l.path)
}

func (l *runtimeLease) release() {
	if l == nil || l.path == "" {
		return
	}
	current, ok := readRuntimeLease(l.path)
	if !ok || current.PID != l.pid || current.SessionToken != l.sessionToken {
		return
	}
	_ = os.Remove(l.path)
}

func readRuntimeLease(path string) (runtimeLeaseRecord, bool) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return runtimeLeaseRecord{}, false
	}
	var record runtimeLeaseRecord
	if err := json.Unmarshal(payload, &record); err != nil || record.PID <= 0 || record.SessionToken == "" {
		return runtimeLeaseRecord{}, false
	}
	return record, true
}

// runtimeLeaseRecoveryDecision 只在能够确认旧记录对应的进程身份时终止进程。
// PID 被操作系统复用时，删除旧记录并继续启动；身份不明时保留记录而不是冒险终止。
func runtimeLeaseRecoveryDecision(record runtimeLeaseRecord) (removable, shouldTerminate bool, err error) {
	process, inspectErr := inspectRuntimeLeaseProcess(record.PID)
	if inspectErr != nil {
		return false, false, fmt.Errorf("inspect pid %d: %w", record.PID, inspectErr)
	}
	if !process.Alive {
		return true, false, nil
	}
	if record.StartIdentifier != "" {
		if process.StartIdentifier != record.StartIdentifier {
			return true, false, nil
		}
		if record.ProcessGroupID != record.PID || process.ProcessGroupID != record.ProcessGroupID {
			return false, false, fmt.Errorf("pid %d matches the lease identity but is not its isolated process group", record.PID)
		}
		return true, true, nil
	}

	// 旧版本未保存启动标识。仅当命令行仍能确认其属于同一个插件时才迁移回收。
	matched, matchErr := runtimeLeaseCommandMatchesPlugin(process.Command, record.PluginID)
	if matchErr != nil {
		return false, false, fmt.Errorf("legacy pid %d cannot verify plugin identity: %w", record.PID, matchErr)
	}
	if !matched {
		return true, false, nil
	}
	if process.ProcessGroupID != record.PID {
		return false, false, fmt.Errorf("legacy pid %d matches the plugin command but is not its isolated process group", record.PID)
	}
	return true, true, nil
}

func runtimeLeaseCommandMatchesPlugin(command, pluginID string) (bool, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return false, fmt.Errorf("process command is unavailable")
	}
	label := pluginProcessLabel(pluginID)
	entrypoint := filepath.Base(strings.Fields(command)[0])
	if entrypoint == label || strings.HasPrefix(entrypoint, label+"-") {
		return true, nil
	}
	return false, nil
}

// RuntimeLeaseInfo describes one persisted plugin runtime lease file.
type RuntimeLeaseInfo struct {
	Path            string
	PluginID        string
	PID             int
	SessionToken    string
	StartIdentifier string
	ProcessGroupID  int
}

// RuntimeLeaseMatches re-reads the lease file and reports whether it still
// holds the recorded identity. Removers use it immediately before deleting
// so a freshly claimed lease can never be swept away.
func RuntimeLeaseMatches(info RuntimeLeaseInfo) bool {
	record, ok := readRuntimeLease(info.Path)
	return ok && record.PID == info.PID && record.PluginID == info.PluginID &&
		record.SessionToken == info.SessionToken && record.StartIdentifier == info.StartIdentifier &&
		record.ProcessGroupID == info.ProcessGroupID
}

// StaleRuntimeLeases 返回可安全移除的租约：进程已退出、PID 已复用，或可确认的旧格式记录。
// 身份无法确认的活跃旧租约会被保留，避免误杀其他进程或启动重复实例。
func StaleRuntimeLeases(dir string) []RuntimeLeaseInfo {
	paths, err := filepath.Glob(filepath.Join(dir, "prism-plugin-*.json"))
	if err != nil {
		return nil
	}
	var stale []RuntimeLeaseInfo
	for _, path := range paths {
		record, ok := readRuntimeLease(path)
		if !ok || record.PID <= 0 {
			continue
		}
		removable, shouldTerminate, decisionErr := runtimeLeaseRecoveryDecision(record)
		if decisionErr != nil || shouldTerminate || !removable {
			continue
		}
		stale = append(stale, RuntimeLeaseInfo{
			Path:            path,
			PluginID:        record.PluginID,
			PID:             record.PID,
			SessionToken:    record.SessionToken,
			StartIdentifier: record.StartIdentifier,
			ProcessGroupID:  record.ProcessGroupID,
		})
	}
	return stale
}

// CleanupStaleRuntimeLeases removes lease files whose recorded PID is no longer alive
// and returns the cleaned lease records.
func CleanupStaleRuntimeLeases(dir string) []RuntimeLeaseInfo {
	stale := StaleRuntimeLeases(dir)
	for _, item := range stale {
		_ = os.Remove(item.Path)
	}
	return stale
}
