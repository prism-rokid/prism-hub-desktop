package pluginregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type fakeFetcher struct {
	calls int
	fail  bool
}

func (f *fakeFetcher) Fetch(_ context.Context, e Entry, etag string) (*Release, string, bool, error) {
	f.calls++
	if f.fail {
		return nil, "", false, errors.New("offline")
	}
	return &Release{Repository: e.Repository, Tag: "v1.2.3", Version: "1.2.3", Metadata: ReleaseMetadata{PluginID: e.ID, Version: "1.2.3", ProtocolVersion: "4", MinHubVersion: "0.1.0", SupportedPlatforms: []string{runtime.GOOS + "-" + runtime.GOARCH}}}, "etag-" + e.ID, false, nil
}
func TestManagerCachesAndPreservesOfflineSnapshot(t *testing.T) {
	f := &fakeFetcher{}
	m := New(filepath.Join(t.TempDir(), "catalog.json"), "0.2.0", "4", f)
	s := m.Refresh(context.Background())
	if len(s.Items) != 5 || !s.Items[0].Compatible || !s.Items[0].Supported {
		t.Fatalf("unexpected catalog %#v", s)
	}
	if err := m.Load(); err != nil {
		t.Fatal(err)
	}
	f.fail = true
	s = m.Refresh(context.Background())
	if s.Items[0].LatestVersion != "1.2.3" || s.Items[0].Error == "" {
		t.Fatalf("offline cache not retained %#v", s.Items[0])
	}
}

type unchangedFetcher struct{ calls int }

func (f *unchangedFetcher) Fetch(_ context.Context, e Entry, etag string) (*Release, string, bool, error) {
	f.calls++
	return nil, etag, true, nil
}

// 老版本写入的缓存可能缺少显示名与图标：Load 必须从内置 catalog 回填，
// 304 命中时 Refresh 也必须用内置 Entry 刷新元数据，缺失不能永久化。
func TestManagerBackfillsStaleCacheEntryMetadata(t *testing.T) {
	stale := Snapshot{CheckedAt: time.Now().Add(-24 * time.Hour), Items: []Item{{
		Entry:         Entry{ID: "codex", Repository: "legacy-org/prism-plugin-codex", Channel: "stable"},
		Release:       &Release{Repository: "legacy-org/prism-plugin-codex", Tag: "v1.0.0", Version: "1.0.0"},
		LatestVersion: "1.0.0",
		Compatible:    true,
		Supported:     true,
	}}}
	b, err := json.Marshal(cached{Snapshot: stale, ETags: map[string]string{"codex": "etag-codex"}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	m := New(path, "0.2.0", "4", &unchangedFetcher{})
	if err := m.Load(); err != nil {
		t.Fatal(err)
	}
	loaded := m.Snapshot().Items[0]
	if loaded.Repository != "prism-rokid/prism-plugin-codex" || loaded.DisplayName == "" || loaded.IconURL == "" || loaded.IconSVGURL == "" {
		t.Fatalf("load must backfill stale entry metadata: %#v", loaded.Entry)
	}
	if loaded.LatestVersion != "1.0.0" || loaded.Release == nil || loaded.Release.Version != "1.0.0" {
		t.Fatalf("load must retain cached release: %#v", loaded)
	}

	// 内存中只放旧条目，单独验证 304 分支会刷新 Entry 且保留已缓存的 Release。
	m2 := New(filepath.Join(t.TempDir(), "catalog.json"), "0.2.0", "4", &unchangedFetcher{})
	m2.mu.Lock()
	m2.snapshot = stale
	m2.etags = map[string]string{"codex": "etag-codex"}
	m2.mu.Unlock()
	s := m2.Refresh(context.Background())
	var item *Item
	for i := range s.Items {
		if s.Items[i].ID == "codex" {
			item = &s.Items[i]
			break
		}
	}
	if item == nil {
		t.Fatal("codex missing after refresh")
	}
	if item.Repository != "prism-rokid/prism-plugin-codex" || item.DisplayName == "" || item.IconURL == "" || item.IconSVGURL == "" {
		t.Fatalf("304 refresh must refresh entry metadata: %#v", item.Entry)
	}
	if item.LatestVersion != "1.0.0" || item.Release == nil || item.Release.Version != "1.0.0" {
		t.Fatalf("304 refresh must retain cached release: %#v", item)
	}
	if !item.LastCheckedAt.After(stale.CheckedAt) {
		t.Fatalf("304 refresh must bump last checked time: %#v", item)
	}
}

// 远端 Registry 可能尚未录入展示元数据：官方白名单插件的名称与图标必须
// 以内置 catalog 回填，目录页不能退化成默认 logo。
func TestRemoteCatalogBackfillsOfficialEntryMetadata(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/public/plugins" {
			http.NotFound(w, r)
			return
		}
		payload := fmt.Sprintf(`{"success":true,"data":{"items":[{"plugin_id":"codex","repository":"prism-rokid/prism-plugin-codex","display_name":"codex","description":"","icon_url":"","channel":"stable","version":"1.2.3","release":{"repository":"prism-rokid/prism-plugin-codex","tag":"v1.2.3","version":"1.2.3","assets":[{"name":"plugin-release.json","url":%q}],"metadata":{"plugin_id":"codex","version":"1.2.3","plugin_protocol_version":"4"}}}]}}`, server.URL+"/plugin-release.json")
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()

	m := New(filepath.Join(t.TempDir(), "catalog.json"), "0.2.0", "4", &fakeFetcher{})
	m.catalogURL = server.URL + "/public/plugins"
	s := m.Refresh(context.Background())
	if len(s.Items) != 1 {
		t.Fatalf("unexpected remote catalog size: %d", len(s.Items))
	}
	item := s.Items[0]
	if item.DisplayName != "Codex" || item.IconURL == "" || item.IconSVGURL == "" {
		t.Fatalf("remote catalog must inherit official metadata: %#v", item.Entry)
	}
	if item.LatestVersion != "1.2.3" || item.Release == nil {
		t.Fatalf("remote release data must be retained: %#v", item)
	}
}

func TestSemVerAndCompatibility(t *testing.T) {
	if !ValidSemVer("v1.2.3") || ValidSemVer("1.2") || ValidSemVer("1.2.3-rc.1") || CompareSemVer("0.2.0", "0.1.9") <= 0 || CompareSemVer("1.0.0", "1.0.0-rc.1") <= 0 {
		t.Fatal("semver validation")
	}
	if Compatible("0.1.0", "4", ReleaseMetadata{MinHubVersion: "0.2.0"}) {
		t.Fatal("too-old hub compatible")
	}
}

func TestGitHubFetcherUsesETagAndHandlesNotModified(t *testing.T) {
	var latestCalls int
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/Rokid-Prism/prism-plugin-demo/releases/latest":
			latestCalls++
			if latestCalls == 2 {
				if got := r.Header.Get("If-None-Match"); got != `"demo-etag"` {
					t.Errorf("If-None-Match=%q", got)
				}
				w.Header().Set("ETag", `"demo-etag"`)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `"demo-etag"`)
			_, _ = fmt.Fprintf(w, `{"id":7,"tag_name":"v1.2.3","assets":[{"name":"plugin-release.json","browser_download_url":%q}]}`, server.URL+"/plugin-release.json")
		case "/plugin-release.json":
			_, _ = w.Write([]byte(`{"plugin_id":"demo","version":"1.2.3","plugin_protocol_version":"4"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	fetcher := GitHubFetcher{Client: server.Client(), APIBase: server.URL}
	entry := Entry{ID: "demo", Repository: "Rokid-Prism/prism-plugin-demo"}
	release, etag, unchanged, err := fetcher.Fetch(context.Background(), entry, "")
	if err != nil || unchanged || release == nil || etag != `"demo-etag"` {
		t.Fatalf("first fetch release=%#v etag=%q unchanged=%v err=%v", release, etag, unchanged, err)
	}
	release, etag, unchanged, err = fetcher.Fetch(context.Background(), entry, etag)
	if err != nil || !unchanged || release != nil || etag != `"demo-etag"` {
		t.Fatalf("304 fetch release=%#v etag=%q unchanged=%v err=%v", release, etag, unchanged, err)
	}
}

func TestCheckIntervalIsSixHours(t *testing.T) {
	if CheckInterval != 6*time.Hour {
		t.Fatalf("CheckInterval=%s", CheckInterval)
	}
}
