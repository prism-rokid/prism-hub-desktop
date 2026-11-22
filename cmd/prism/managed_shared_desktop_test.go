package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	pluginpkg "rokid-prism-hub/internal/plugin"
)

func TestSharedDesktopSelectsExistingSharedWorkspace(t *testing.T) {
	const origin = "http://127.0.0.1:9444"
	target := func(id, route string) desktopHostTarget {
		return desktopHostTarget{ID: id, Type: "page", URL: "app://-/index.html" + route, WS: "ws://127.0.0.1:9444/devtools/page/" + id}
	}
	codex := target("codex", "")
	dedicated := target("legacy-dot", "?initialRoute=%2F")
	anotherMain := target("another", "")
	overlay := target("overlay", "?initialRoute=%2Favatar-overlay")
	for _, tc := range []struct {
		name      string
		targets   []desktopHostTarget
		previous  sharedDesktopLease
		want      desktopHostTarget
		ambiguous bool
	}{
		{name: "one existing workspace", targets: []desktopHostTarget{overlay, codex}, want: codex},
		{name: "old dedicated Dot ignored", targets: []desktopHostTarget{overlay, dedicated, codex}, previous: sharedDesktopLease{TargetID: dedicated.ID, CDPOrigin: origin}, want: codex},
		{name: "prefer the workspace Codex uses", targets: []desktopHostTarget{dedicated, codex}, want: codex},
		{name: "two unknown main pages", targets: []desktopHostTarget{anotherMain, codex}, ambiguous: true},
		{name: "known shared page", targets: []desktopHostTarget{anotherMain, codex}, previous: sharedDesktopLease{TargetID: codex.ID, CDPOrigin: origin, SharedWindow: true}, want: codex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedChatGPTWorkspaceTarget(tc.targets, origin, tc.previous)
			if got != tc.want || (err != nil) != tc.ambiguous {
				t.Fatalf("got target %+v and error %v", got, err)
			}
		})
	}
}

func TestSharedDesktopFindsHostByExecutablePath(t *testing.T) {
	root := t.TempDir()
	makeExecutable := func(dir string) string {
		t.Helper()
		executable := filepath.Join(root, dir, "Contents", "MacOS", "ChatGPT")
		if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(executable, nil, 0o700); err != nil {
			t.Fatal(err)
		}
		path, err := filepath.EvalSymlinks(executable)
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	installed := makeExecutable("Managed ChatGPT.app")
	other := makeExecutable("Other ChatGPT.app")
	processes := "11 " + other + "\n12 " + installed + "\n13 ChatGPT Helper\n14 ChatGPT\ninvalid ChatGPT\n"
	resolved := map[int]string{11: other, 12: installed}
	processPath := func(pid int) string { return resolved[pid] }
	if pid, err := selectDesktopHostPID(processes, installed, processPath); err != nil || pid != 12 {
		t.Fatalf("expected the actual managed executable, got PID %d and %v", pid, err)
	}
	resolved[14] = installed
	if _, err := selectDesktopHostPID(processes, installed, processPath); err == nil {
		t.Fatal("expected ambiguity when two live processes resolve to the same executable")
	}
}

func TestSharedDesktopHostLockBlocksConcurrentWindowOperations(t *testing.T) {
	dir := t.TempDir()
	release, err := lockChatGPTDesktopHost(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	_, err = lockChatGPTDesktopHost(ctx, dir)
	release()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second lock should remain busy until its deadline: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err = lockChatGPTDesktopHost(ctx, dir)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	release()
}

func TestSharedDesktopBindsNewlyInstalledPlugin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS shared window contract")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("requires Node.js")
	}
	root, leases := t.TempDir(), t.TempDir()
	target := desktopHostTarget{ID: "native-workspace", Type: "page", URL: "app://-/index.html"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/json/list" {
			http.NotFound(w, r)
			return
		}
		item := target
		item.WS = "ws://" + r.Host + "/devtools/page/" + item.ID
		_ = json.NewEncoder(w).Encode([]desktopHostTarget{item})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	host := sharedDesktopLease{SharedWindow: true, TargetID: target.ID, CDPOrigin: server.URL, HostPID: os.Getpid(), HostStart: desktopHostProcessStart(ctx, os.Getpid()), Phase: "opening"}
	// Both IDs are assigned after this Hub test binary has been built.
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("sdk-agent-%d-%d", os.Getpid(), i)
		writeDashboardTestPlugin(t, root, id, id)
		dir := filepath.Join(root, id)
		manifest := filepath.Join(dir, pluginpkg.ManifestYAML)
		f, err := os.OpenFile(manifest, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("\nmanaged_launch: true\nmanaged_launch_specs:\n  - kind: desktop-app\n    shared_window: chatgpt-single-window-v2\n")
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		script := `require("node:readline").createInterface({input:process.stdin}).on("line", line => {
     const {id,method,params} = JSON.parse(line);
     const action=params?.action;
     const ok=method==="adapter.controlSession" && action==="desktop.bind" &&
       params.session.plugin_id===process.env.PRISM_PLUGIN_ID && !params.session.native_thread_id &&
       params.target.target_id==="native-workspace";
     console.log(JSON.stringify({id,ok,payload:{ok,action,thread_id:process.env.PRISM_PLUGIN_ID+"/room"}}));
   });`
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(script), 0600); err != nil {
			t.Fatal(err)
		}
		items, err := pluginpkg.ScanDirs([]string{root})
		if err != nil {
			t.Fatal(err)
		}
		var bound bool
		for _, item := range items {
			if item.Manifest.ID != id {
				continue
			}
			spec, ok := dashboardManifestDesktopApp(item.Manifest, item.Manifest.ID)
			if !ok || spec.SharedDesktop == nil {
				t.Fatal("new manifest was not discovered")
			}
			item.SkipRuntimeLease = true
			adapter := pluginpkg.NewProcessAdapter(item)
			defer adapter.Close(context.Background())
			lease, err := spec.SharedDesktop.finishBinding(ctx, leases, host, adapter)
			if err != nil || lease.Binding != id+"/room" || spec.SharedDesktop.readLease(leases) != lease {
				t.Fatalf("bind: %+v, %v", lease, err)
			}
			conflict := lease
			conflict.Binding = "unrelated"
			if _, err := spec.SharedDesktop.finishBinding(ctx, leases, conflict, adapter); err == nil || spec.SharedDesktop.readLease(leases) != lease {
				t.Fatal("conflicting proof replaced verified lease")
			}
			bound = true
		}
		if !bound || len(sharedDesktopAgents([]string{root})) != i+1 {
			t.Fatal("installed participant missing")
		}
	}
}
