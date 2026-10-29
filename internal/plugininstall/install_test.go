package plugininstall

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginregistry"
)

type probe struct{}

func (probe) Probe(_ context.Context, _ string, m plugin.Manifest) (ProbeResult, error) {
	return ProbeResult{PluginID: m.ID, ProtocolVersion: "4", Available: false}, nil
}
func tarball(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	for n, v := range files {
		h := &tar.Header{Name: n, Mode: 0o644, Size: int64(len(v)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func platform() string {
	os := runtime.GOOS
	if os == "darwin" {
		os = "macos"
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	return os + "-" + arch
}
func TestInstallWritesAtomicStateAndRollsBack(t *testing.T) {
	archive := tarball(t, map[string]string{plugin.ManifestYAML: `id: demo
version: 1.0.0
plugin_protocol_version: '4'
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}
`})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	release := &pluginregistry.Release{Repository: "Rokid-Prism/demo", Tag: "v1.0.0", Version: "1.0.0", Metadata: pluginregistry.ReleaseMetadata{PluginID: "demo", Version: "1.0.0", ProtocolVersion: "4", SupportedPlatforms: []string{platform()}}}
	req := Request{Entry: pluginregistry.Entry{ID: "demo", Repository: "Rokid-Prism/demo"}, Release: release, Asset: pluginregistry.Asset{Name: "demo-" + platform() + ".tar.gz", URL: server.URL}, ExpectedProtocol: "4", Prober: probe{}}
	if err := ins.Install(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	state, err := ins.State("demo")
	if err != nil || state.ActiveVersion != "1.0.0" {
		t.Fatalf("state %#v %v", state, err)
	}
	release.Version = "1.0.1"
	release.Tag = "v1.0.1"
	release.Metadata.Version = "1.0.1"
	archive = tarball(t, map[string]string{plugin.ManifestYAML: `id: demo
version: 1.0.1
plugin_protocol_version: '4'
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}
`})
	if err := ins.Install(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if err := ins.Rollback("demo"); err != nil {
		t.Fatal(err)
	}
	state, _ = ins.State("demo")
	if state.ActiveVersion != "1.0.0" {
		t.Fatalf("rollback state %#v", state)
	}
}
func TestArchiveRejectsPathTraversal(t *testing.T) {
	b := tarball(t, map[string]string{"../escape": "bad"})
	p := t.TempDir() + "/bad.tar.gz"
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := extract(p, t.TempDir(), MaxExtracted); err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestDownloadReportsActualByteProgress(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 128*1024)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	var updates [][2]int64
	destination := filepath.Join(t.TempDir(), "plugin.tar.gz")
	if err := ins.download(context.Background(), server.URL, destination, int64(len(payload)), func(downloaded, total int64) {
		updates = append(updates, [2]int64{downloaded, total})
	}); err != nil {
		t.Fatal(err)
	}
	if len(updates) < 2 {
		t.Fatalf("expected initial and completed progress updates, got %#v", updates)
	}
	if updates[0] != [2]int64{0, int64(len(payload))} {
		t.Fatalf("unexpected initial progress %#v", updates[0])
	}
	if final := updates[len(updates)-1]; final != [2]int64{int64(len(payload)), int64(len(payload))} {
		t.Fatalf("unexpected final progress %#v", final)
	}
}

func TestDownloadAssetFallsBackToTrustedCDNAndChecksSHA256(t *testing.T) {
	payload := []byte("verified backup artifact")
	digest := sha256.Sum256(payload)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/github" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/backup" {
			_, _ = w.Write(payload)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(strings.TrimPrefix(server.URL, "https://"), "http://")
	if index := strings.Index(host, ":"); index >= 0 {
		host = host[:index]
	}
	t.Setenv("PRISM_PLUGIN_BACKUP_CDN_HOSTS", host)
	ins := New(t.TempDir())
	ins.Client = server.Client()
	destination := filepath.Join(t.TempDir(), "plugin.tar.gz")
	asset := pluginregistry.Asset{
		Name: "demo.tar.gz", URL: server.URL + "/github", FallbackURL: server.URL + "/backup",
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)),
	}
	if err := ins.downloadAsset(context.Background(), asset, destination, nil); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("fallback content=%q err=%v", actual, err)
	}
}

func TestTrustedBackupURLAllowsDefaultRokidCDN(t *testing.T) {
	t.Setenv("PRISM_PLUGIN_BACKUP_CDN_HOSTS", "")
	if !trustedBackupURL("https://rokid-xr.rokidcdn.com/test/prism/plugins/codex/0.1.42/codex.tar.gz") {
		t.Fatal("默认 CDN 域名未被信任")
	}
}

func TestDownloadAssetFallsBackWhenGitHubFirstResponseTimesOut(t *testing.T) {
	payload := []byte("verified backup artifact")
	digest := sha256.Sum256(payload)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/github" {
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/backup" {
			_, _ = w.Write(payload)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(strings.TrimPrefix(server.URL, "https://"), "http://")
	if index := strings.Index(host, ":"); index >= 0 {
		host = host[:index]
	}
	t.Setenv("PRISM_PLUGIN_BACKUP_CDN_HOSTS", host)
	ins := New(t.TempDir())
	ins.Client = server.Client()
	destination := filepath.Join(t.TempDir(), "plugin.tar.gz")
	asset := pluginregistry.Asset{
		Name: "demo.tar.gz", URL: server.URL + "/github", FallbackURL: server.URL + "/backup",
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)),
	}
	timeouts := downloadTimeouts{firstResponse: 25 * time.Millisecond, idleRead: time.Second}
	if err := ins.downloadAssetWithTimeouts(context.Background(), asset, destination, nil, timeouts, timeouts); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("fallback content=%q err=%v", actual, err)
	}
}

func TestDownloadAssetFallsBackWhenGitHubDownloadStalls(t *testing.T) {
	payload := []byte("verified backup artifact")
	digest := sha256.Sum256(payload)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/github" {
			_, _ = w.Write(payload[:4])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/backup" {
			_, _ = w.Write(payload)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(strings.TrimPrefix(server.URL, "https://"), "http://")
	if index := strings.Index(host, ":"); index >= 0 {
		host = host[:index]
	}
	t.Setenv("PRISM_PLUGIN_BACKUP_CDN_HOSTS", host)
	ins := New(t.TempDir())
	ins.Client = server.Client()
	destination := filepath.Join(t.TempDir(), "plugin.tar.gz")
	asset := pluginregistry.Asset{
		Name: "demo.tar.gz", URL: server.URL + "/github", FallbackURL: server.URL + "/backup",
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)),
	}
	timeouts := downloadTimeouts{firstResponse: time.Second, idleRead: 25 * time.Millisecond}
	if err := ins.downloadAssetWithTimeouts(context.Background(), asset, destination, nil, timeouts, timeouts); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("fallback content=%q err=%v", actual, err)
	}
}

func TestArchiveAllowsHarmlessTarRootDirectory(t *testing.T) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "./", Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
		t.Fatal(err)
	}
	contents := []byte("id: demo\n")
	if err := tw.WriteHeader(&tar.Header{Name: plugin.ManifestYAML, Mode: 0o644, Size: int64(len(contents)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(contents); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plugin.tar.gz")
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if err := extract(path, dest, MaxExtracted); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dest, plugin.ManifestYAML)); err != nil {
		t.Fatalf("manifest not extracted: %v", err)
	}
}

func TestExtractZIPUsesArchiveExtension(t *testing.T) {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	f, err := w.Create(plugin.ManifestYAML)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("id: demo")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/plugin.zip"
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if err := extract(path, dest, MaxExtracted); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest + "/" + plugin.ManifestYAML); err != nil {
		t.Fatalf("zip not extracted: %v", err)
	}
}

type activePathProbe struct{ calls []string }

func (p *activePathProbe) Probe(_ context.Context, dir string, m plugin.Manifest) (ProbeResult, error) {
	p.calls = append(p.calls, dir)
	if strings.Contains(filepath.ToSlash(dir), "/versions/") {
		return ProbeResult{}, errors.New("cannot start from active path")
	}
	return ProbeResult{PluginID: m.ID, ProtocolVersion: "4"}, nil
}

func TestActivePathProbeFailureRemovesFirstInstallState(t *testing.T) {
	archive := tarball(t, map[string]string{plugin.ManifestYAML: `id: demo
version: 1.0.0
plugin_protocol_version: '4'
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}
`})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive) }))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	p := &activePathProbe{}
	release := &pluginregistry.Release{Repository: "Rokid-Prism/demo", Tag: "v1.0.0", Version: "1.0.0", Metadata: pluginregistry.ReleaseMetadata{PluginID: "demo", Version: "1.0.0", ProtocolVersion: "4", SupportedPlatforms: []string{platform()}}}
	req := Request{Entry: pluginregistry.Entry{ID: "demo", Repository: "Rokid-Prism/demo"}, Release: release, Asset: pluginregistry.Asset{Name: "demo-" + platform() + ".tar.gz", URL: server.URL}, ExpectedProtocol: "4", Prober: p}
	if err := ins.Install(context.Background(), req, nil); err == nil || !strings.Contains(err.Error(), "active probe") {
		t.Fatalf("expected active probe failure, got %v", err)
	}
	state, err := ins.State("demo")
	if err != nil || state.ActiveVersion != "" {
		t.Fatalf("failed first install left active state: %#v %v", state, err)
	}
	if len(p.calls) != 2 {
		t.Fatalf("expected staging and active probes, got %v", p.calls)
	}
}

func TestActivationCallbackFailureReloadsPreviousVersion(t *testing.T) {
	archives := map[string][]byte{
		"1.0.0": tarball(t, map[string]string{plugin.ManifestYAML: "id: demo\nversion: 1.0.0\nplugin_protocol_version: '4'\nintegration_mode: protocol-native\ntransport: process\ncommand: [demo]\ncapabilities: {}\n"}),
		"1.0.1": tarball(t, map[string]string{plugin.ManifestYAML: "id: demo\nversion: 1.0.1\nplugin_protocol_version: '4'\nintegration_mode: protocol-native\ntransport: process\ncommand: [demo]\ncapabilities: {}\n"}),
	}
	current := "1.0.0"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archives[current]) }))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	release := &pluginregistry.Release{Repository: "Rokid-Prism/demo", Tag: "v1.0.0", Version: "1.0.0", Metadata: pluginregistry.ReleaseMetadata{PluginID: "demo", Version: "1.0.0", ProtocolVersion: "4", SupportedPlatforms: []string{platform()}}}
	req := Request{Entry: pluginregistry.Entry{ID: "demo", Repository: "Rokid-Prism/demo"}, Release: release, Asset: pluginregistry.Asset{Name: "demo-" + platform() + ".tar.gz", URL: server.URL}, ExpectedProtocol: "4", Prober: probe{}}
	if err := ins.Install(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	current = "1.0.1"
	release.Tag, release.Version, release.Metadata.Version = "v1.0.1", "1.0.1", "1.0.1"
	callbackCalls := 0
	ins.OnActivated = func(context.Context, string) error {
		callbackCalls++
		if callbackCalls == 1 {
			return errors.New("new runtime failed")
		}
		return nil
	}
	if err := ins.Install(context.Background(), req, nil); err == nil {
		t.Fatal("expected activation callback failure")
	}
	state, err := ins.State("demo")
	if err != nil || state.ActiveVersion != "1.0.0" || callbackCalls != 2 {
		t.Fatalf("rollback state=%#v callbackCalls=%d err=%v", state, callbackCalls, err)
	}
}

func TestExclusiveSerializesSamePluginLifecycle(t *testing.T) {
	ins := New(t.TempDir())
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- ins.Exclusive("demo", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	var secondEntered atomic.Bool
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- ins.Exclusive("demo", func() error {
			secondEntered.Store(true)
			return nil
		})
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second lifecycle operation bypassed lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if secondEntered.Load() {
		t.Fatal("second lifecycle operation entered while first held the lock")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil || !secondEntered.Load() {
		t.Fatalf("second lifecycle operation did not run after release: %v", err)
	}
}

func tarballWithSymlink(t *testing.T, linkName, linkTarget string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	files := map[string]string{
		plugin.ManifestYAML: `id: demo
version: 1.0.0
plugin_protocol_version: '4'
integration_mode: protocol-native
transport: process
command: [demo]
capabilities: {}
`,
		"bin/demo": "#!/bin/sh\n",
	}
	for n, v := range files {
		h := &tar.Header{Name: n, Mode: 0o644, Size: int64(len(v)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.WriteHeader(&tar.Header{Name: linkName, Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: linkTarget}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestInstallAcceptsPackageRelativeSymlinks(t *testing.T) {
	archive := tarballWithSymlink(t, "./node_modules/.bin/demo", "../bin/demo")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	release := &pluginregistry.Release{Repository: "Rokid-Prism/demo", Tag: "v1.0.0", Version: "1.0.0", Metadata: pluginregistry.ReleaseMetadata{PluginID: "demo", Version: "1.0.0", ProtocolVersion: "4", SupportedPlatforms: []string{platform()}}}
	req := Request{Entry: pluginregistry.Entry{ID: "demo", Repository: "Rokid-Prism/demo"}, Release: release, Asset: pluginregistry.Asset{Name: "demo.tar.gz", URL: server.URL}, ExpectedProtocol: "4", Prober: probe{}}
	if err := ins.Install(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	state, err := ins.State("demo")
	if err != nil || state.ActiveVersion != "1.0.0" {
		t.Fatalf("state %#v %v", state, err)
	}
	link := filepath.Join(ins.Root, "demo", "versions", "1.0.0", "node_modules", ".bin", "demo")
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("symlink not restored: %v", err)
	}
	if got != "../bin/demo" {
		t.Fatalf("symlink target = %q, want ../bin/demo", got)
	}
}

func TestInstallRejectsEscapingSymlinks(t *testing.T) {
	archive := tarballWithSymlink(t, "./node_modules/.bin/evil", "../../../../etc/passwd")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer server.Close()
	ins := New(t.TempDir())
	ins.Client = server.Client()
	release := &pluginregistry.Release{Repository: "Rokid-Prism/demo", Tag: "v1.0.0", Version: "1.0.0", Metadata: pluginregistry.ReleaseMetadata{PluginID: "demo", Version: "1.0.0", ProtocolVersion: "4", SupportedPlatforms: []string{platform()}}}
	req := Request{Entry: pluginregistry.Entry{ID: "demo", Repository: "Rokid-Prism/demo"}, Release: release, Asset: pluginregistry.Asset{Name: "demo.tar.gz", URL: server.URL}, ExpectedProtocol: "4", Prober: probe{}}
	if err := ins.Install(context.Background(), req, nil); err == nil || !strings.Contains(err.Error(), "unsafe archive symlink") {
		t.Fatalf("err = %v, want unsafe archive symlink rejection", err)
	}
}
