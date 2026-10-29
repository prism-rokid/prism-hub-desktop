package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteDaemonRuntimeStateRemovesStaleTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "daemon-runtime-state.json")
	stalePath := filepath.Join(dir, "daemon-runtime-state-stale.json")
	freshPath := filepath.Join(dir, "daemon-runtime-state-fresh.json")
	for _, path := range []string{stalePath, freshPath} {
		if err := os.WriteFile(path, []byte("temporary"), 0o600); err != nil {
			t.Fatalf("write temporary state %s: %v", path, err)
		}
	}
	now := time.Now()
	if err := os.Chtimes(stalePath, now.Add(-daemonRuntimeStateTempMaxAge-time.Minute), now.Add(-daemonRuntimeStateTempMaxAge-time.Minute)); err != nil {
		t.Fatalf("age stale temporary state: %v", err)
	}

	if err := writeDaemonRuntimeState(statePath, daemonRuntimeState{ConnectionState: "connected"}); err != nil {
		t.Fatalf("write runtime state: %v", err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale temporary state remains: %v", err)
	}
	if _, err := os.Stat(freshPath); err != nil {
		t.Fatalf("fresh temporary state was removed: %v", err)
	}
	state, err := readDaemonRuntimeState(statePath)
	if err != nil {
		t.Fatalf("read runtime state: %v", err)
	}
	if state == nil || state.ConnectionState != "connected" {
		t.Fatalf("unexpected runtime state: %+v", state)
	}
}

func TestDaemonRuntimeStatePersistsHostPluginSyncStatus(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	expected := daemonRuntimeState{
		ConnectionState:       "connected",
		HostPluginSyncState:   "synced",
		HostPluginSyncDetail:  "Host Plugin 授权集已同步。",
		HostPluginSyncUpdated: "2026-09-23T00:00:00Z",
		HostPluginCount:       2,
	}
	if err := writeDaemonRuntimeState(statePath, expected); err != nil {
		t.Fatalf("写入运行时状态失败: %v", err)
	}

	state, err := readDaemonRuntimeState(statePath)
	if err != nil {
		t.Fatalf("读取运行时状态失败: %v", err)
	}
	if state == nil {
		t.Fatal("运行时状态为空")
	}
	if state.HostPluginSyncState != expected.HostPluginSyncState ||
		state.HostPluginSyncDetail != expected.HostPluginSyncDetail ||
		state.HostPluginSyncUpdated != expected.HostPluginSyncUpdated ||
		state.HostPluginCount != expected.HostPluginCount {
		t.Fatalf("Host Plugin 同步状态未正确持久化: %+v", state)
	}
}

func TestDaemonRuntimeStateRepairsStaleConnectionLabel(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "daemon-runtime-state.json")
	// Older versions retained the disconnected label after reconnecting.
	if err := os.WriteFile(statePath, []byte(`{"connection_state":"connected","connection_label":"未连接"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	state, err := readDaemonRuntimeState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.ConnectionLabel != "已连接" {
		t.Fatalf("connected record retained stale label: %+v", state)
	}
	// The next transition must update the persisted label, too.
	state.ConnectionState = "reconnecting"
	if err := writeDaemonRuntimeState(statePath, *state); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted daemonRuntimeState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.ConnectionLabel != "重连中" {
		t.Fatalf("reconnecting record retained stale label: %+v", persisted)
	}
}
