// Package runtimes owns shared, verified interpreters used by released plugins.
package runtimes

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const NodeVersion = "22.23.1"
const runtimeToken = "${runtime.node}"
const maxRuntimeDownload = 512 << 20

var nodeInstallMu sync.Mutex

type Source struct{ URL, SHA256 string }
type NodeManager struct {
	Root, Version string
	Sources       []Source
	Client        *http.Client
}

func DefaultRoot() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".prism", "pluginbridge-runtimes")
	}
	return filepath.Join(home, ".prism", "pluginbridge-runtimes")
}
func DefaultSources(goos, goarch string) []Source {
	arch := goarch
	if arch == "amd64" {
		arch = "x64"
	}
	ext := "tar.gz"
	osName := goos
	if goos == "windows" {
		// The official Node.js distributions use "win" in their artifact
		// name (node-v<version>-win-x64.zip), not "windows".
		ext = "zip"
		osName = "win"
	}
	name := fmt.Sprintf("node-v%s-%s-%s.%s", NodeVersion, osName, arch, ext)
	checksums := map[string]string{
		"node-v22.23.1-darwin-arm64.tar.gz": "ef28d8fab2c0e4314522d4bb1b7173270aa3937e93b92cb7de79c112ac1fa953",
		"node-v22.23.1-darwin-x64.tar.gz":   "b8da981b8a0b1241b70249204916da76c63573ddf5814dbd2d1e41069105cb81",
		"node-v22.23.1-linux-x64.tar.gz":    "7a8cb04b4a1df4eaf432125324b81b29a088e73570a23259a8de1c65d07fc129",
		"node-v22.23.1-win-x64.zip":         "7df0bc9375723f4a86b3aa1b7cc73342423d9677a8df4538aca31a049e309c29",
	}
	sha := checksums[name]
	// The mirror comes first, but both downloads are checked against the same
	// Hub-bundled digest before anything can be activated.
	return []Source{{URL: "https://mirrors.aliyun.com/nodejs-release/v" + NodeVersion + "/" + name, SHA256: sha}, {URL: "https://nodejs.org/dist/v" + NodeVersion + "/" + name, SHA256: sha}}
}
func NewNodeManager(root string, sources []Source) *NodeManager {
	if root == "" {
		root = DefaultRoot()
	}
	if len(sources) == 0 {
		sources = DefaultSources(runtime.GOOS, runtime.GOARCH)
	}
	return &NodeManager{Root: root, Version: NodeVersion, Sources: sources, Client: &http.Client{Timeout: 2 * time.Minute}}
}
func (m *NodeManager) Dir() string {
	version := m.Version
	if version == "" {
		version = NodeVersion
	}
	return filepath.Join(m.Root, "node", version)
}
func (m *NodeManager) Binary() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(m.Dir(), "node.exe")
	}
	return filepath.Join(m.Dir(), "bin", "node")
}
func (m *NodeManager) Ensure(ctx context.Context) (string, error) {
	nodeInstallMu.Lock()
	defer nodeInstallMu.Unlock()
	if _, err := os.Stat(m.Binary()); err == nil {
		return m.Binary(), nil
	}
	if len(m.Sources) == 0 {
		return "", errors.New("node runtime has no verified sources")
	}
	var errs []string
	for _, source := range m.Sources {
		if strings.TrimSpace(source.SHA256) == "" {
			errs = append(errs, "checksum not configured for "+source.URL)
			continue
		}
		if err := m.install(ctx, source); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		return m.Binary(), nil
	}
	return "", fmt.Errorf("install node runtime %s: %s", m.Version, strings.Join(errs, "; "))
}
func (m *NodeManager) install(ctx context.Context, source Source) error {
	client := m.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", source.URL, resp.Status)
	}
	if resp.ContentLength > maxRuntimeDownload {
		return errors.New("node runtime download exceeds limit")
	}
	if err := os.MkdirAll(filepath.Dir(m.Dir()), 0o700); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(m.Dir()), "node-staging-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	archive := filepath.Join(staging, "runtime"+archiveExtension(source.URL, ""))
	f, err := os.OpenFile(archive, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, maxRuntimeDownload+1))
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if n > maxRuntimeDownload {
		return errors.New("node runtime download exceeds limit")
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), strings.TrimSpace(source.SHA256)) {
		return fmt.Errorf("node runtime checksum mismatch")
	}
	dest := filepath.Join(staging, "extract")
	if err := extract(archive, dest, 1<<30); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.Dir()), 0o700); err != nil {
		return err
	}
	_ = os.RemoveAll(m.Dir() + ".new")
	if err := os.Rename(dest, m.Dir()+".new"); err != nil {
		return err
	}
	newBinary := filepath.Join(m.Dir()+".new", filepath.Base(m.Binary()))
	if runtime.GOOS != "windows" {
		// Unix tarballs keep the binary under bin/; the Windows distribution
		// puts node.exe at the archive root and has no bin/ directory.
		newBinary = filepath.Join(m.Dir()+".new", "bin", filepath.Base(m.Binary()))
	}
	if _, err := os.Stat(newBinary); err != nil {
		_ = os.RemoveAll(m.Dir() + ".new")
		return fmt.Errorf("node archive missing binary: %w", err)
	}
	if err := os.RemoveAll(m.Dir()); err != nil {
		_ = os.RemoveAll(m.Dir() + ".new")
		return err
	}
	if err := os.Rename(m.Dir()+".new", m.Dir()); err != nil {
		if os.IsExist(err) {
			_ = os.RemoveAll(m.Dir() + ".new")
			return nil
		}
		return err
	}
	return nil
}
func archiveExtension(raw, name string) string {
	value := strings.ToLower(name)
	if value == "" {
		if u, err := url.Parse(raw); err == nil {
			value = strings.ToLower(u.Path)
		}
	}
	if strings.HasSuffix(value, ".zip") {
		return ".zip"
	}
	return ".tar.gz"
}
func ExpandCommand(command []string, nodeBinary string) ([]string, error) {
	out := make([]string, len(command))
	for i, v := range command {
		if strings.Contains(v, runtimeToken) && v != runtimeToken {
			return nil, fmt.Errorf("runtime token must occupy a complete argument")
		}
		if v == runtimeToken {
			if nodeBinary == "" {
				return nil, errors.New("node runtime unavailable")
			}
			out[i] = nodeBinary
		} else {
			out[i] = v
		}
	}
	return out, nil
}
func RequiresNode(rangeExpr string) bool {
	rangeExpr = strings.ReplaceAll(strings.TrimSpace(rangeExpr), " ", "")
	return strings.Contains(rangeExpr, ">=22") && strings.Contains(rangeExpr, "<23")
}

// GC removes unused versions older than maxAge. Callers determine references
// from all active and previous plugin states before invoking it.
func (m *NodeManager) GC(referenced map[string]bool, now time.Time, maxAge time.Duration) error {
	base := filepath.Join(m.Root, "node")
	entries, err := os.ReadDir(base)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || referenced[e.Name()] {
			continue
		}
		info, err := e.Info()
		if err == nil && now.Sub(info.ModTime()) > maxAge {
			if err := os.RemoveAll(filepath.Join(base, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func extract(archive, dest string, max int64) error {
	if strings.HasSuffix(archive, ".zip") {
		return extractZIP(archive, dest, max)
	}
	return extractTar(archive, dest, max)
}
func safePath(root, name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	first := strings.SplitN(name, "/", 2)[0]
	if strings.HasPrefix(name, "/") || strings.Contains(first, ":") {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	clean := filepath.Clean(name)
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	parts := strings.Split(clean, string(filepath.Separator))
	if len(parts) > 1 {
		clean = filepath.Join(parts[1:]...)
	}
	if clean == "" || clean == "." {
		return "", nil
	}
	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	return target, nil
}

func safeRuntimeSymlink(root, linkPath, linkName string) (string, error) {
	linkName = strings.ReplaceAll(linkName, "\\", "/")
	if linkName == "" || strings.HasPrefix(linkName, "/") || strings.Contains(strings.SplitN(linkName, "/", 2)[0], ":") {
		return "", fmt.Errorf("unsafe runtime symlink %q", linkName)
	}
	target := filepath.Clean(filepath.Join(filepath.Dir(linkPath), filepath.FromSlash(linkName)))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe runtime symlink %q", linkName)
	}
	return linkName, nil
}
func extractTar(archive, dest string, max int64) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var total int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target, err := safePath(dest, h.Name)
		if err != nil {
			return err
		}
		if target == "" {
			continue
		}
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeSymlink:
			linkName, linkErr := safeRuntimeSymlink(dest, target, h.Linkname)
			if linkErr != nil {
				return linkErr
			}
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				err = os.Symlink(linkName, target)
			}
		case tar.TypeReg, tar.TypeRegA:
			total += h.Size
			if total > max {
				return errors.New("archive exceeds limit")
			}
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				out, e := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, h.FileInfo().Mode().Perm())
				if e != nil {
					return e
				}
				_, e = io.Copy(out, tr)
				ce := out.Close()
				if e != nil {
					err = e
				} else {
					err = ce
				}
			}
		default:
			return fmt.Errorf("unsupported archive entry %q", h.Name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func extractZIP(archive, dest string, max int64) error {
	r, err := zip.OpenReader(archive)
	if err != nil {
		return err
	}
	defer r.Close()
	var total int64
	for _, z := range r.File {
		target, err := safePath(dest, z.Name)
		if err != nil {
			return err
		}
		if target == "" {
			continue
		}
		if z.FileInfo().IsDir() {
			if err = os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if z.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsupported archive symlink %q", z.Name)
		}
		if !z.Mode().IsRegular() {
			return fmt.Errorf("unsupported archive special file %q", z.Name)
		}
		total += int64(z.UncompressedSize64)
		if total > max {
			return errors.New("archive exceeds limit")
		}
		if err = os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		in, err := z.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, z.Mode().Perm())
		if err == nil {
			_, err = io.Copy(out, in)
			cerr := out.Close()
			if err == nil {
				err = cerr
			}
		}
		in.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
