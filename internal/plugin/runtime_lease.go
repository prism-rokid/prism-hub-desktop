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
	PID          int    `json:"pid"`
	PluginID     string `json:"plugin_id"`
	SessionToken string `json:"session_token"`
}

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
		if previous.PID > 0 && runtimeLeaseProcessAlive(previous.PID) {
			if err := terminatePluginProcessPID(previous.PID); err != nil {
				return nil, fmt.Errorf("terminate stale plugin runtime pid %d: %w", previous.PID, err)
			}
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	return &runtimeLease{path: path, pluginID: pluginID, sessionToken: sessionToken}, nil
}

func (l *runtimeLease) claim(pid int) error {
	if l == nil || l.path == "" || pid <= 0 {
		return fmt.Errorf("invalid plugin runtime lease")
	}
	l.pid = pid
	payload, err := json.Marshal(runtimeLeaseRecord{PID: pid, PluginID: l.pluginID, SessionToken: l.sessionToken})
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

// RuntimeLeaseInfo describes one persisted plugin runtime lease file.
type RuntimeLeaseInfo struct {
	Path         string
	PluginID     string
	PID          int
	SessionToken string
}

// RuntimeLeaseMatches re-reads the lease file and reports whether it still
// holds the recorded identity. Removers use it immediately before deleting
// so a freshly claimed lease can never be swept away.
func RuntimeLeaseMatches(info RuntimeLeaseInfo) bool {
	record, ok := readRuntimeLease(info.Path)
	return ok && record.PID == info.PID && record.PluginID == info.PluginID &&
		record.SessionToken == info.SessionToken
}

// StaleRuntimeLeases returns lease records under dir whose recorded PID is no
// longer alive. It only reads files; callers decide whether to clean up.
func StaleRuntimeLeases(dir string) []RuntimeLeaseInfo {
	paths, err := filepath.Glob(filepath.Join(dir, "prism-plugin-*.json"))
	if err != nil {
		return nil
	}
	var stale []RuntimeLeaseInfo
	for _, path := range paths {
		record, ok := readRuntimeLease(path)
		if !ok || record.PID <= 0 || runtimeLeaseProcessAlive(record.PID) {
			continue
		}
		stale = append(stale, RuntimeLeaseInfo{Path: path, PluginID: record.PluginID, PID: record.PID, SessionToken: record.SessionToken})
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
