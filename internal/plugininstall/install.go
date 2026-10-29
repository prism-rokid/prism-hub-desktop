// Package plugininstall securely stages and activates official plugin releases.
package plugininstall

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"rokid-prism-hub/internal/host"
	"rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginregistry"
)

const MaxDownload = 512 << 20
const MaxExtracted = 1 << 30

var githubDownloadTimeouts = downloadTimeouts{
	firstResponse: 5 * time.Second,
	idleRead:      15 * time.Second,
}

var backupDownloadTimeouts = downloadTimeouts{
	firstResponse: 10 * time.Second,
	idleRead:      30 * time.Second,
}

type downloadTimeouts struct {
	firstResponse time.Duration
	idleRead      time.Duration
}

type State struct {
	ActiveVersion   string    `json:"active_version"`
	PreviousVersion string    `json:"previous_version,omitempty"`
	LegacyOfficial  bool      `json:"legacy_official,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}
type OperationState string

const (
	Queued      OperationState = "queued"
	Downloading OperationState = "downloading"
	Installing  OperationState = "installing"
	Probing     OperationState = "probing"
	Activating  OperationState = "activating"
	Completed   OperationState = "completed"
	Failed      OperationState = "failed"
)

type Operation struct {
	ID                 string         `json:"operation_id"`
	PluginID           string         `json:"plugin_id"`
	Version            string         `json:"version"`
	State              OperationState `json:"state"`
	DownloadedBytes    int64          `json:"downloaded_bytes,omitempty"`
	DownloadTotalBytes int64          `json:"download_total_bytes,omitempty"`
	Error              string         `json:"error,omitempty"`
	StartedAt          time.Time      `json:"started_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}
type ProbeResult struct {
	PluginID, ProtocolVersion string
	Available                 bool
}
type Prober interface {
	Probe(context.Context, string, plugin.Manifest) (ProbeResult, error)
}
type Request struct {
	Entry            pluginregistry.Entry
	Release          *pluginregistry.Release
	Asset            pluginregistry.Asset
	ExpectedProtocol string
	Prober           Prober
}
type Installer struct {
	Root        string
	Client      *http.Client
	now         func() time.Time
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
	operations  map[string]*Operation
	OnActivated func(context.Context, string) error
}

func DefaultRoot() string {
	// Keep the installer root aligned with the configured default plugin dir:
	// both must follow PRISM_DATA_DIR so isolated runs never touch the real
	// ~/.prism tree (and doctor's install checks observe the same layout).
	return host.DefaultDataPath("pluginbridge-plugins")
}
func New(root string) *Installer {
	if root == "" {
		root = DefaultRoot()
	}
	return &Installer{Root: root, Client: http.DefaultClient, now: time.Now, locks: map[string]*sync.Mutex{}, operations: map[string]*Operation{}}
}
func (i *Installer) pluginDir(id string) (string, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" || id != filepath.Base(id) || strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return "", fmt.Errorf("invalid plugin id %q", id)
	}
	return filepath.Join(i.Root, id), nil
}
func (i *Installer) State(id string) (State, error) {
	root, err := i.pluginDir(id)
	if err != nil {
		return State{}, err
	}
	b, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err == nil {
		var s State
		err = json.Unmarshal(b, &s)
		if err == nil && (!safeVersionName(s.ActiveVersion) || (s.PreviousVersion != "" && !safeVersionName(s.PreviousVersion))) {
			err = errors.New("invalid plugin state version")
		}
		return s, err
	}
	if !os.IsNotExist(err) {
		return State{}, err
	}
	if _, err = os.Stat(filepath.Join(root, "current")); err == nil {
		return State{ActiveVersion: "current", LegacyOfficial: isOfficialID(id)}, nil
	}
	if os.IsNotExist(err) {
		return State{}, nil
	}
	return State{}, err
}

func safeVersionName(version string) bool {
	version = strings.TrimSpace(version)
	return version != "" && version != "." && version != ".." && version == filepath.Base(version)
}
func isOfficialID(id string) bool {
	for _, e := range pluginregistry.OfficialCatalog() {
		if e.ID == id {
			return true
		}
	}
	return false
}
func (i *Installer) Operation(id string) (Operation, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	op, ok := i.operations[id]
	if !ok {
		return Operation{}, false
	}
	return *op, true
}

// GetOperation is the dashboard-facing name for Operation.
func (i *Installer) GetOperation(id string) (Operation, bool) { return i.Operation(id) }
func (i *Installer) InstallAsync(ctx context.Context, req Request) (Operation, error) {
	if req.Release == nil {
		return Operation{}, errors.New("release required")
	}
	op := Operation{ID: uuid.NewString(), PluginID: req.Entry.ID, Version: req.Release.Version, State: Queued, StartedAt: i.now(), UpdatedAt: i.now()}
	i.mu.Lock()
	i.operations[op.ID] = &op
	i.mu.Unlock()
	go func() {
		err := i.install(ctx, req,
			func(s OperationState) { i.transition(op.ID, s, "") },
			func(downloaded, total int64) { i.transitionDownload(op.ID, downloaded, total) },
		)
		if err != nil {
			i.transition(op.ID, Failed, err.Error())
		} else {
			i.transition(op.ID, Completed, "")
		}
	}()
	return op, nil
}

// StartOperation queues an install or update and returns immediately. Its
// state progresses queued → downloading → installing → probing → activating.
func (i *Installer) StartOperation(ctx context.Context, req Request) (Operation, error) {
	return i.InstallAsync(ctx, req)
}
func (i *Installer) transition(id string, state OperationState, msg string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if o := i.operations[id]; o != nil {
		o.State = state
		o.Error = msg
		o.UpdatedAt = i.now()
	}
}

func (i *Installer) transitionDownload(id string, downloaded, total int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if o := i.operations[id]; o != nil {
		o.State = Downloading
		o.DownloadedBytes = downloaded
		o.DownloadTotalBytes = total
		o.UpdatedAt = i.now()
	}
}

func (i *Installer) Install(ctx context.Context, req Request, progress func(OperationState)) error {
	return i.install(ctx, req, progress, nil)
}

func (i *Installer) install(ctx context.Context, req Request, progress func(OperationState), downloadProgress func(downloaded, total int64)) error {
	if req.Release == nil {
		return errors.New("release required")
	}
	if req.Entry.ID == "" || req.Release.Metadata.PluginID != req.Entry.ID || req.Release.Metadata.Version != req.Release.Version {
		return errors.New("release metadata identity mismatch")
	}
	if !pluginregistry.ValidSemVer(req.Release.Version) {
		return errors.New("release version must be stable semver")
	}
	if req.Release.Repository != req.Entry.Repository || req.Release.Tag != "v"+req.Release.Version {
		return errors.New("release repository or tag mismatch")
	}
	if req.ExpectedProtocol != "" && req.Release.Metadata.ProtocolVersion != req.ExpectedProtocol {
		return fmt.Errorf("release protocol %s incompatible with %s", req.Release.Metadata.ProtocolVersion, req.ExpectedProtocol)
	}
	if !req.Release.Metadata.Supports(runtime.GOOS, runtime.GOARCH) {
		return fmt.Errorf("plugin %s does not support %s", req.Entry.ID, runtimePlatform())
	}
	if req.Asset.URL == "" {
		var ok bool
		req.Asset, ok = SelectAsset(req.Release, runtime.GOOS, runtime.GOARCH)
		if !ok {
			return fmt.Errorf("no release asset for %s", runtimePlatform())
		}
	}
	if isOfficialID(req.Entry.ID) {
		assetURL, parseErr := url.Parse(req.Asset.URL)
		expectedPrefix := "/" + req.Entry.Repository + "/releases/download/" + req.Release.Tag + "/"
		// GitHub 的仓库路径大小写不敏感，但 API 返回的下载地址可能会将组织名标准化为小写。
		if parseErr != nil || !strings.EqualFold(assetURL.Hostname(), "github.com") || !strings.HasPrefix(strings.ToLower(assetURL.EscapedPath()), strings.ToLower(expectedPrefix)) {
			return errors.New("official release asset URL does not match repository and tag")
		}
		if req.Asset.FallbackURL != "" && !trustedBackupURL(req.Asset.FallbackURL) {
			return errors.New("official release fallback URL is not a trusted HTTPS CDN")
		}
	}
	root, err := i.pluginDir(req.Entry.ID)
	if err != nil {
		return err
	}
	lock := i.lock(req.Entry.ID)
	lock.Lock()
	defer lock.Unlock()
	if err := os.MkdirAll(i.Root, 0o700); err != nil {
		return err
	}
	if progress != nil {
		progress(Downloading)
	}
	staging, err := os.MkdirTemp(i.Root, "."+req.Entry.ID+"-staging-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	archive := filepath.Join(staging, "plugin"+archiveExtension(req.Asset.URL, req.Asset.Name))
	if err = i.downloadAsset(ctx, req.Asset, archive, downloadProgress); err != nil {
		return err
	}
	if progress != nil {
		progress(Installing)
	}
	versionDir := filepath.Join(staging, "version")
	if err = extract(archive, versionDir, MaxExtracted); err != nil {
		return err
	}
	manifestPath := filepath.Join(versionDir, plugin.ManifestYAML)
	if _, err = os.Stat(manifestPath); os.IsNotExist(err) {
		manifestPath = filepath.Join(versionDir, plugin.ManifestJSON)
	}
	m, err := plugin.LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.ID != req.Entry.ID || m.Version != req.Release.Version {
		return fmt.Errorf("manifest identity mismatch: %s %s", m.ID, m.Version)
	}
	if isOfficialID(req.Entry.ID) && (m.SchemaVersion != 1 || m.IconURL == "" || m.IconSVGURL == "") {
		return errors.New("official plugin manifest requires schema_version 1 and both icon URLs")
	}
	if req.ExpectedProtocol != "" && m.ProtocolVersion != "" && m.ProtocolVersion != req.ExpectedProtocol {
		return fmt.Errorf("manifest protocol %s incompatible with %s", m.ProtocolVersion, req.ExpectedProtocol)
	}
	if progress != nil {
		progress(Probing)
	}
	if req.Prober != nil {
		result, e := req.Prober.Probe(ctx, versionDir, m)
		if e != nil {
			return fmt.Errorf("probe: %w", e)
		}
		if result.PluginID != m.ID {
			return fmt.Errorf("probe plugin_id mismatch: %s", result.PluginID)
		}
		if req.ExpectedProtocol != "" && result.ProtocolVersion != "" && result.ProtocolVersion != req.ExpectedProtocol {
			return fmt.Errorf("probe protocol mismatch: %s", result.ProtocolVersion)
		}
	}
	if progress != nil {
		progress(Activating)
	}
	if err := i.activate(root, staging, req.Release.Version); err != nil {
		return err
	}
	// Probe once more from the final active path. This catches plugins that
	// accidentally depend on their staging location and ensures a process can
	// complete the protocol handshake after the atomic move.
	if req.Prober != nil {
		activeDir := filepath.Join(root, "versions", req.Release.Version)
		result, probeErr := req.Prober.Probe(ctx, activeDir, m)
		if probeErr == nil && result.PluginID != m.ID {
			probeErr = fmt.Errorf("plugin_id mismatch: %s", result.PluginID)
		}
		if probeErr == nil && req.ExpectedProtocol != "" && result.ProtocolVersion != "" && result.ProtocolVersion != req.ExpectedProtocol {
			probeErr = fmt.Errorf("protocol mismatch: %s", result.ProtocolVersion)
		}
		if probeErr != nil {
			i.restoreAfterFailedActivation(ctx, root, req.Entry.ID)
			return fmt.Errorf("active probe: %w", probeErr)
		}
	}
	if i.OnActivated != nil {
		if err := i.OnActivated(ctx, req.Entry.ID); err != nil {
			restoreErr := i.restoreAfterFailedActivation(ctx, root, req.Entry.ID)
			if restoreErr != nil {
				return fmt.Errorf("activate reloader: %w (restore previous runtime: %v)", err, restoreErr)
			}
			return fmt.Errorf("activate reloader: %w", err)
		}
	}
	return nil
}

func (i *Installer) restoreAfterFailedActivation(ctx context.Context, root, id string) error {
	state, stateErr := i.State(id)
	hasPrevious := stateErr == nil && state.PreviousVersion != ""
	rollbackErr := i.rollbackRoot(root, id)
	if rollbackErr != nil {
		if !strings.Contains(rollbackErr.Error(), "no previous version") {
			return rollbackErr
		}
		if err := os.Remove(filepath.Join(root, "state.json")); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if hasPrevious && i.OnActivated != nil {
		if err := i.OnActivated(ctx, id); err != nil {
			return fmt.Errorf("reload rolled-back version: %w", err)
		}
	}
	return nil
}
func (i *Installer) lock(id string) *sync.Mutex {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.locks[id] == nil {
		i.locks[id] = &sync.Mutex{}
	}
	return i.locks[id]
}

// Exclusive serializes uninstall or other lifecycle work with installs and
// updates for the same plugin ID.
func (i *Installer) Exclusive(id string, fn func() error) error {
	if _, err := i.pluginDir(id); err != nil {
		return err
	}
	lock := i.lock(strings.ToLower(strings.TrimSpace(id)))
	lock.Lock()
	defer lock.Unlock()
	if fn == nil {
		return nil
	}
	return fn()
}

type downloadProgressWriter struct {
	writer     io.Writer
	written    int64
	total      int64
	lastReport time.Time
	onProgress func(downloaded, total int64)
}

func (w *downloadProgressWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n <= 0 {
		return n, err
	}
	w.written += int64(n)
	now := time.Now()
	if w.onProgress != nil && (w.written == w.total || now.Sub(w.lastReport) >= 150*time.Millisecond) {
		w.lastReport = now
		w.onProgress(w.written, w.total)
	}
	return n, err
}

func (w *downloadProgressWriter) finish() {
	if w.onProgress != nil {
		w.onProgress(w.written, w.total)
	}
}

func (i *Installer) download(ctx context.Context, raw, dest string, expectedSize int64, progress func(downloaded, total int64)) error {
	return i.downloadVerified(ctx, raw, dest, expectedSize, "", progress)
}

// downloadAsset 始终先从 GitHub Release 下载，只有主地址失败时才使用已审核的 CDN 备份。
func (i *Installer) downloadAsset(ctx context.Context, asset pluginregistry.Asset, dest string, progress func(downloaded, total int64)) error {
	return i.downloadAssetWithTimeouts(ctx, asset, dest, progress, githubDownloadTimeouts, backupDownloadTimeouts)
}

func (i *Installer) downloadAssetWithTimeouts(ctx context.Context, asset pluginregistry.Asset, dest string,
	progress func(downloaded, total int64), primaryTimeouts, fallbackTimeouts downloadTimeouts) error {
	primaryErr := i.downloadVerifiedWithTimeouts(ctx, asset.URL, dest, asset.Size, asset.SHA256, progress, primaryTimeouts)
	if primaryErr == nil || strings.TrimSpace(asset.FallbackURL) == "" {
		return primaryErr
	}
	if !trustedBackupURL(asset.FallbackURL) {
		return fmt.Errorf("GitHub 下载失败：%v；备用地址不受信任", primaryErr)
	}
	if fallbackErr := i.downloadVerifiedWithTimeouts(ctx, asset.FallbackURL, dest, asset.Size, asset.SHA256, progress, fallbackTimeouts); fallbackErr != nil {
		return fmt.Errorf("GitHub 下载失败：%v；OSS 备份下载失败：%w", primaryErr, fallbackErr)
	}
	return nil
}

func (i *Installer) downloadVerified(ctx context.Context, raw, dest string, expectedSize int64, expectedSHA256 string, progress func(downloaded, total int64)) (resultErr error) {
	return i.downloadVerifiedWithTimeouts(ctx, raw, dest, expectedSize, expectedSHA256, progress, downloadTimeouts{})
}

func (i *Installer) downloadVerifiedWithTimeouts(ctx context.Context, raw, dest string, expectedSize int64,
	expectedSHA256 string, progress func(downloaded, total int64), timeouts downloadTimeouts) (resultErr error) {
	if err := plugin.ValidateHTTPSURL(raw); err != nil {
		return fmt.Errorf("asset URL: %w", err)
	}
	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var firstResponseTimedOut atomic.Bool
	var firstResponseTimer *time.Timer
	if timeouts.firstResponse > 0 {
		firstResponseTimer = time.AfterFunc(timeouts.firstResponse, func() {
			firstResponseTimedOut.Store(true)
			cancel()
		})
		defer firstResponseTimer.Stop()
	}
	traceCtx := httptrace.WithClientTrace(downloadCtx, &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			if firstResponseTimer != nil {
				firstResponseTimer.Stop()
			}
		},
	})
	req, err := http.NewRequestWithContext(traceCtx, http.MethodGet, raw, nil)
	if err != nil {
		return err
	}
	client := i.downloadClient(timeouts.firstResponse)
	resp, err := client.Do(req)
	if err != nil {
		if firstResponseTimedOut.Load() {
			return fmt.Errorf("下载首个响应超时：%s", timeouts.firstResponse)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: %s", resp.Status)
	}
	if resp.ContentLength > MaxDownload {
		return errors.New("download exceeds limit")
	}
	total := resp.ContentLength
	if total <= 0 && expectedSize > 0 {
		total = expectedSize
	}
	if total > MaxDownload {
		return errors.New("download exceeds limit")
	}
	if progress != nil {
		progress(0, total)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	writer := &downloadProgressWriter{writer: io.MultiWriter(f, hash), total: total, onProgress: progress}
	body := io.Reader(resp.Body)
	if timeouts.idleRead > 0 {
		body = &idleTimeoutReader{reader: body, timeout: timeouts.idleRead, cancel: cancel}
	}
	n, copyErr := io.Copy(writer, io.LimitReader(body, MaxDownload+1))
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if n > MaxDownload {
		return errors.New("download exceeds limit")
	}
	if expected := strings.ToLower(strings.TrimSpace(expectedSHA256)); expected != "" {
		actual := hex.EncodeToString(hash.Sum(nil))
		if actual != expected {
			_ = os.Remove(dest)
			return errors.New("download SHA-256 mismatch")
		}
	}
	writer.finish()
	return nil
}

// downloadClient 为默认下载客户端设置连接、TLS 与响应头超时。
// 注入的客户端通常用于测试或受调用方控制，因此仍由请求上下文兜底首响应超时。
func (i *Installer) downloadClient(firstResponseTimeout time.Duration) *http.Client {
	if i.Client != nil || firstResponseTimeout <= 0 {
		if i.Client != nil {
			return i.Client
		}
		return http.DefaultClient
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: firstResponseTimeout, KeepAlive: 30 * time.Second}
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = firstResponseTimeout
	transport.ResponseHeaderTimeout = firstResponseTimeout
	return &http.Client{Transport: transport}
}

// idleTimeoutReader 在下载连接持续无数据时取消请求，使调用方可切换到备用地址。
type idleTimeoutReader struct {
	reader   io.Reader
	timeout  time.Duration
	cancel   context.CancelFunc
	timedOut atomic.Bool
}

func (r *idleTimeoutReader) Read(p []byte) (int, error) {
	timer := time.AfterFunc(r.timeout, func() {
		r.timedOut.Store(true)
		r.cancel()
	})
	n, err := r.reader.Read(p)
	if !timer.Stop() || r.timedOut.Load() {
		return n, fmt.Errorf("下载数据停滞超过 %s", r.timeout)
	}
	return n, err
}

func trustedBackupURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	allowed := os.Getenv("PRISM_PLUGIN_BACKUP_CDN_HOSTS")
	if strings.TrimSpace(allowed) == "" {
		allowed = "rokid-xr.rokidcdn.com,static.rokidcdn.com,rokid-xr.oss-cn-hangzhou.aliyuncs.com"
	}
	for _, value := range strings.Split(allowed, ",") {
		if host == strings.ToLower(strings.TrimSpace(value)) {
			return true
		}
	}
	return false
}
func (i *Installer) activate(root, staging, version string) error {
	source := filepath.Join(staging, "version")
	versions := filepath.Join(root, "versions")
	if err := os.MkdirAll(versions, 0o700); err != nil {
		return err
	}
	target := filepath.Join(versions, version)
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	if err := os.Rename(source, target); err != nil {
		return err
	}
	old, _ := i.State(filepath.Base(root))
	s := State{ActiveVersion: version, PreviousVersion: old.ActiveVersion, LegacyOfficial: false, UpdatedAt: i.now()}
	if s.PreviousVersion == version {
		s.PreviousVersion = ""
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := filepath.Join(root, "state.json.tmp")
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(root, "state.json")); err != nil {
		return err
	}
	return i.prune(root, s)
}
func (i *Installer) prune(root string, s State) error {
	entries, err := os.ReadDir(filepath.Join(root, "versions"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == s.ActiveVersion || e.Name() == s.PreviousVersion {
			continue
		}
		if err = os.RemoveAll(filepath.Join(root, "versions", e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Rollback restores the previous version when a newly activated process fails
// at startup. Call it only after the process-level health check has failed.
func (i *Installer) Rollback(id string) error {
	root, err := i.pluginDir(id)
	if err != nil {
		return err
	}
	lock := i.lock(id)
	lock.Lock()
	defer lock.Unlock()
	return i.rollbackRoot(root, id)
}
func (i *Installer) rollbackRoot(root, id string) error {
	s, err := i.State(id)
	if err != nil {
		return err
	}
	if s.PreviousVersion == "" {
		return errors.New("no previous version")
	}
	s.ActiveVersion, s.PreviousVersion = s.PreviousVersion, s.ActiveVersion
	s.UpdatedAt = i.now()
	b, _ := json.Marshal(s)
	tmp := filepath.Join(root, "state.json.tmp")
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(root, "state.json"))
}
func ActivePath(root, id string) (string, error) {
	i := New(root)
	s, err := i.State(id)
	if err != nil {
		return "", err
	}
	base, err := i.pluginDir(id)
	if err != nil {
		return "", err
	}
	if s.ActiveVersion == "" {
		return "", os.ErrNotExist
	}
	if s.ActiveVersion == "current" {
		return filepath.Join(base, "current"), nil
	}
	return filepath.Join(base, "versions", s.ActiveVersion), nil
}
func runtimePlatform() string {
	osName := runtime.GOOS
	if osName == "darwin" {
		osName = "macos"
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	return osName + "-" + arch
}
func SelectAsset(r *pluginregistry.Release, goos, goarch string) (pluginregistry.Asset, bool) {
	osName := goos
	if osName == "darwin" {
		osName = "macos"
	}
	arch := goarch
	if arch == "amd64" {
		arch = "x64"
	}
	needle := osName + "-" + arch
	for _, a := range r.Assets {
		name := strings.ToLower(a.Name)
		if strings.Contains(name, needle) && (strings.HasSuffix(name, ".zip") || strings.HasSuffix(name, ".tar.gz") || strings.HasSuffix(name, ".tgz")) {
			return a, true
		}
	}
	return pluginregistry.Asset{}, false
}

func extract(path, dest string, max int64) error {
	if strings.HasSuffix(strings.ToLower(path), ".zip") {
		return unzip(path, dest, max)
	}
	return untar(path, dest, max)
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
func safe(root, name string) (string, error) {
	if strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	clean := filepath.Clean(name)
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	return target, nil
}
func untar(path, dest string, max int64) error {
	f, err := os.Open(path)
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
			return nil
		}
		if err != nil {
			return err
		}
		// GNU tar commonly emits a harmless root-directory entry ("./") before
		// files. It has no extraction target and must not be treated as a file.
		if h.Typeflag == tar.TypeDir && (h.Name == "." || h.Name == "./") {
			continue
		}
		target, err := safe(dest, h.Name)
		if err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
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
		case tar.TypeSymlink:
			// npm stages node_modules/.bin shims as symlinks, so plugin
			// tarballs legitimately carry them. Only links that resolve back
			// inside the archive root are accepted.
			link := h.Linkname
			if link == "" || strings.HasPrefix(link, "/") || strings.Contains(link, "\\") {
				return fmt.Errorf("unsafe archive symlink %q -> %q", h.Name, link)
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(filepath.Clean(h.Name)), link))
			if resolved == "." || strings.HasPrefix(resolved, "..") || filepath.IsAbs(resolved) {
				return fmt.Errorf("unsafe archive symlink %q -> %q", h.Name, link)
			}
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				_ = os.Remove(target)
				err = os.Symlink(link, target)
			}
		case tar.TypeLink:
			source, serr := safe(dest, h.Linkname)
			if serr != nil {
				return serr
			}
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				_ = os.Remove(target)
				err = os.Link(source, target)
			}
		default:
			return fmt.Errorf("unsafe archive entry %q", h.Name)
		}
		if err != nil {
			return err
		}
	}
}
func unzip(path, dest string, max int64) error {
	r, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer r.Close()
	var total int64
	for _, z := range r.File {
		target, err := safe(dest, z.Name)
		if err != nil {
			return err
		}
		if z.FileInfo().IsDir() {
			if err = os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if z.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe archive symlink %q", z.Name)
		}
		if !z.Mode().IsRegular() {
			return fmt.Errorf("unsafe archive special file %q", z.Name)
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
			ce := out.Close()
			if err == nil {
				err = ce
			}
		}
		in.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
