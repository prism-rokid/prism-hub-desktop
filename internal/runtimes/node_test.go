package runtimes

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// archiveBinaryPath returns the platform-correct runtime binary path inside a
// node-v<version> archive: node.exe at the archive root on Windows, bin/node
// on Unix.
func archiveBinaryPath() string {
	if runtime.GOOS == "windows" {
		return "node.exe"
	}
	return "bin/node"
}

func TestExpandCommandOnlyAllowsExactToken(t *testing.T) {
	got, err := ExpandCommand([]string{"${runtime.node}", "dist/index.js"}, "/node/bin/node")
	if err != nil || got[0] != "/node/bin/node" {
		t.Fatalf("expand: %v %#v", err, got)
	}
	if _, err := ExpandCommand([]string{"x${runtime.node}"}, "node"); err == nil {
		t.Fatal("partial token accepted")
	}
	if !RequiresNode(">=22.0.0 <23.0.0") {
		t.Fatal("node range unrecognized")
	}
}
func TestDefaultSourcesHavePinnedDigest(t *testing.T) {
	for _, s := range DefaultSources("darwin", "arm64") {
		if len(s.SHA256) != 64 {
			t.Fatalf("source not pinned: %#v", s)
		}
	}
	for _, s := range DefaultSources("windows", "amd64") {
		if len(s.SHA256) != 64 {
			t.Fatalf("windows source not pinned: %#v", s)
		}
		if !strings.HasSuffix(s.URL, "node-v22.23.1-win-x64.zip") {
			t.Fatalf("windows source must use the win-x64 distribution name: %s", s.URL)
		}
	}
}

func TestEnsureNodeDownloadsOnceAndSharesRuntime(t *testing.T) {
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	payload := []byte("node")
	if err := tw.WriteHeader(&tar.Header{Name: "node-v22.23.1-test/" + archiveBinaryPath(), Mode: 0o755, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive.Bytes())
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; _, _ = w.Write(archive.Bytes()) }))
	defer server.Close()
	m := NewNodeManager(t.TempDir(), []Source{{URL: server.URL + "/node.tar.gz", SHA256: hex.EncodeToString(sum[:])}})
	m.Client = server.Client()
	first, err := m.Ensure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Ensure(context.Background())
	if err != nil || first != second || calls != 1 {
		t.Fatalf("runtime should be shared: %q %q calls=%d err=%v", first, second, calls, err)
	}
}

func TestEnsureNodeRejectsOversizedDownloadFromContentLength(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(maxRuntimeDownload+1))
		_, _ = w.Write([]byte("oversized"))
	}))
	defer server.Close()
	m := NewNodeManager(t.TempDir(), []Source{{URL: server.URL + "/node.tar.gz", SHA256: strings.Repeat("0", 64)}})
	m.Client = server.Client()
	if _, err := m.Ensure(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("expected size limit error, got %v", err)
	}
}

func TestEnsureNodeExtractsZIPRuntime(t *testing.T) {
	var archive bytes.Buffer
	zw := zip.NewWriter(&archive)
	header := &zip.FileHeader{Name: "node-v22.23.1-test/" + archiveBinaryPath()}
	header.SetMode(0o755)
	file, err := zw.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("node")); err != nil {
		t.Fatal(err)
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive.Bytes())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive.Bytes()) }))
	defer server.Close()
	m := NewNodeManager(t.TempDir(), []Source{{URL: server.URL + "/node.zip", SHA256: hex.EncodeToString(sum[:])}})
	m.Client = server.Client()
	binary, err := m.Ensure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(binary); err != nil {
		t.Fatalf("ZIP runtime binary missing: %v", err)
	}
}

func TestEnsureNodeExtractsInternalRuntimeSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Unprivileged symlink creation requires Developer Mode on Windows,
		// and the Windows Node distribution contains no symlinks.
		t.Skip("internal runtime symlinks are a Unix distribution feature")
	}
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	payload := []byte("node")
	if err := tw.WriteHeader(&tar.Header{Name: "node-v22.23.1-test/bin/node", Mode: 0o755, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "node-v22.23.1-test/bin/corepack", Linkname: "../lib/node_modules/corepack/dist/corepack.js", Typeflag: tar.TypeSymlink}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive.Bytes())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive.Bytes()) }))
	defer server.Close()
	m := NewNodeManager(t.TempDir(), []Source{{URL: server.URL + "/node.tar.gz", SHA256: hex.EncodeToString(sum[:])}})
	m.Client = server.Client()
	if _, err := m.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	link, err := os.Readlink(filepath.Join(m.Dir(), "bin", "corepack"))
	if err != nil || link != "../lib/node_modules/corepack/dist/corepack.js" {
		t.Fatalf("runtime symlink = %q, %v", link, err)
	}
}

func TestRuntimeSymlinkRejectsEscapingTarget(t *testing.T) {
	root := t.TempDir()
	if _, err := safeRuntimeSymlink(root, filepath.Join(root, "bin", "corepack"), "../../escape"); err == nil {
		t.Fatal("escaping symlink accepted")
	}
}
