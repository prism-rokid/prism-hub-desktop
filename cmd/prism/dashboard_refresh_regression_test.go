package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestDashboardCacheMergesQueuedRefreshes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var samples atomic.Int32
	cache := &dashboardStatusCache{collect: func() dashboardStatus {
		count := samples.Add(1)
		if count == 1 {
			close(started)
			<-release
		}
		return dashboardStatus{OK: true, ConnectionState: "connected"}
	}}

	refreshDashboardStatusCacheSoon(cache)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued refresh did not start")
	}
	refreshDashboardStatusCacheSoon(cache)
	refreshDashboardStatusCacheSoon(cache)
	close(release)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && samples.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	if got := samples.Load(); got != 2 {
		t.Fatalf("queued refreshes should coalesce into two samples, got %d", got)
	}
	time.Sleep(20 * time.Millisecond)
	if got := samples.Load(); got != 2 {
		t.Fatalf("coalesced refresh started an unexpected third sample: %d", got)
	}
}

func TestDashboardCacheExpiresWithoutBackgroundSampling(t *testing.T) {
	cache := &dashboardStatusCache{initialized: true, refreshedAt: time.Now(), value: dashboardStatus{ConnectionState: "disconnected"}}
	if _, fresh := cache.Snapshot(); !fresh {
		t.Fatal("fresh snapshot rejected")
	}
	cache.refreshedAt = time.Now().Add(-dashboardStatusRefreshInterval - time.Second)
	if _, fresh := cache.Snapshot(); fresh {
		t.Fatal("stale snapshot accepted")
	}
	if cache.value.ConnectionState != "disconnected" {
		t.Fatal("reading expired cache performed a sample")
	}
}

func TestDashboardConnectionOverlaysCachedStatus(t *testing.T) {
	status := dashboardStatus{HubRunning: true, HostTokenSet: true, LoggedIn: true, BridgeEnabled: true, ConnectionState: "disconnected", ConnectionLabel: "未连接"}
	applyDashboardRuntimeConnection(&status, &daemonRuntimeState{ConnectionState: "connected", ConnectionLabel: "已连接", HostPluginSyncState: "synced", HostPluginCount: 2})
	if status.ConnectionState != "connected" || status.ConnectionLabel != "已连接" || status.HostPluginCount != 2 || !status.RemoteControlReady {
		t.Fatalf("stale cached connection: %+v", status)
	}
	applyDashboardRuntimeConnection(&status, &daemonRuntimeState{ConnectionState: "auth_expired", ConnectionLabel: "登录已过期"})
	if status.RemoteControlReady || status.RecommendedAction != "login" {
		t.Fatalf("expired authentication still actionable: %+v", status)
	}
}
