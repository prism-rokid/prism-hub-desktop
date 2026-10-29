// pluginregistry 负责维护本地官方 PluginBridge 目录。
package pluginregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/buildinfo"
	"rokid-prism-hub/internal/host"
)

const CheckInterval = 6 * time.Hour
const DefaultProtocolVersion = "4"
const defaultHTTPTimeout = 30 * time.Second

type Entry struct {
	ID          string `json:"plugin_id"`
	Repository  string `json:"repository"`
	DisplayName string `json:"display_name"`
	Description string `json:"description,omitempty"`
	IconURL     string `json:"icon_url,omitempty"`
	IconSVGURL  string `json:"icon_svg_url,omitempty"`
	Channel     string `json:"channel"`
}

// OfficialCatalog 明确使用封闭白名单，目录远端响应不能推导仓库所有者或仓库名。
func OfficialCatalog() []Entry {
	basePNG := "https://registry.npmmirror.com/@lobehub/icons-static-avatar/latest/files/avatars/"
	baseSVG := "https://registry.npmmirror.com/@lobehub/icons-static-svg/latest/files/icons/"
	return []Entry{
		// GitHub 当前组织名为 prism-rokid，必须与 Release 资产 URL 使用同一仓库路径。
		{ID: "codex", Repository: "prism-rokid/prism-plugin-codex", DisplayName: "Codex", IconURL: basePNG + "codex.webp", IconSVGURL: baseSVG + "codex.svg", Channel: "stable"},
		{ID: "claudecode", Repository: "prism-rokid/prism-plugin-claude-code", DisplayName: "Claude Code", IconURL: basePNG + "claudecode.webp", IconSVGURL: baseSVG + "claudecode.svg", Channel: "stable"},
		{ID: "hermes", Repository: "prism-rokid/prism-plugin-hermes", DisplayName: "Hermes", IconURL: basePNG + "hermesagent.webp", IconSVGURL: baseSVG + "hermesagent.svg", Channel: "stable"},
		{ID: "openclaw", Repository: "prism-rokid/prism-plugin-openclaw", DisplayName: "OpenClaw", IconURL: basePNG + "openclaw.webp", IconSVGURL: baseSVG + "openclaw.svg", Channel: "stable"},
		{ID: "opencode", Repository: "prism-rokid/prism-plugin-opencode", DisplayName: "OpenCode", IconURL: basePNG + "opencode.webp", IconSVGURL: baseSVG + "opencode.svg", Channel: "stable"},
	}
}

type Asset struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	FallbackURL string `json:"fallback_url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	Size        int64  `json:"size,omitempty"`
}
type Release struct {
	ID         string          `json:"id,omitempty"`
	Repository string          `json:"repository"`
	Tag        string          `json:"tag"`
	Version    string          `json:"version"`
	Assets     []Asset         `json:"assets,omitempty"`
	Metadata   ReleaseMetadata `json:"metadata"`
}
type ReleaseMetadata struct {
	PluginID           string   `json:"plugin_id"`
	Version            string   `json:"version"`
	ProtocolVersion    string   `json:"plugin_protocol_version,omitempty"`
	MinHubVersion      string   `json:"min_hub_version,omitempty"`
	SupportedPlatforms []string `json:"supported_platforms,omitempty"`
}
type Item struct {
	Entry
	Release       *Release  `json:"release,omitempty"`
	LatestVersion string    `json:"latest_version,omitempty"`
	Compatible    bool      `json:"compatible"`
	Supported     bool      `json:"supported"`
	LastCheckedAt time.Time `json:"last_checked_at,omitempty"`
	Error         string    `json:"error,omitempty"`
}
type Snapshot struct {
	Items     []Item    `json:"items"`
	CheckedAt time.Time `json:"checked_at"`
}

type Fetcher interface {
	Fetch(context.Context, Entry, string) (release *Release, etag string, notModified bool, err error)
}

type GitHubFetcher struct {
	Client  *http.Client
	APIBase string
	// Token 可选；为空时使用匿名请求。
	Token string
}

func (g GitHubFetcher) Fetch(ctx context.Context, entry Entry, etag string) (*Release, string, bool, error) {
	client := g.Client
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	base := strings.TrimRight(g.APIBase, "/")
	if base == "" {
		base = "https://api.github.com"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/repos/"+entry.Repository+"/releases/latest", nil)
	if err != nil {
		return nil, "", false, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	// GitHub 匿名 API 按 IP 限制为每小时 60 次，共享或代理出口容易耗尽额度并返回 403。
	// 配置 Token 后可使用认证请求额度，但开发者不需要提供该 Token。
	if token := strings.TrimSpace(g.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return nil, resp.Header.Get("ETag"), true, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.Header.Get("ETag"), false, fmt.Errorf("github release %s: %s", entry.ID, resp.Status)
	}
	var raw struct {
		ID                int64  `json:"id"`
		TagName           string `json:"tag_name"`
		Draft, Prerelease bool
		Assets            []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
			Size               int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&raw); err != nil {
		return nil, "", false, err
	}
	if raw.Draft || raw.Prerelease {
		return nil, resp.Header.Get("ETag"), false, fmt.Errorf("release %s is not stable", raw.TagName)
	}
	version := strings.TrimPrefix(strings.TrimSpace(raw.TagName), "v")
	if !ValidSemVer(version) {
		return nil, resp.Header.Get("ETag"), false, fmt.Errorf("release tag %q is not a stable semver", raw.TagName)
	}
	r := &Release{ID: fmt.Sprint(raw.ID), Repository: entry.Repository, Tag: raw.TagName, Version: version}
	for _, a := range raw.Assets {
		r.Assets = append(r.Assets, Asset{Name: a.Name, URL: a.BrowserDownloadURL, Size: a.Size})
	}
	for _, a := range r.Assets {
		if a.Name != "plugin-release.json" {
			continue
		}
		metaReq, err := http.NewRequestWithContext(ctx, http.MethodGet, a.URL, nil)
		if err != nil {
			return nil, resp.Header.Get("ETag"), false, err
		}
		metaResp, err := client.Do(metaReq)
		if err != nil {
			return nil, resp.Header.Get("ETag"), false, err
		}
		err = json.NewDecoder(io.LimitReader(metaResp.Body, 1<<20)).Decode(&r.Metadata)
		metaResp.Body.Close()
		if metaResp.StatusCode != http.StatusOK || err != nil {
			return nil, resp.Header.Get("ETag"), false, fmt.Errorf("release metadata %s: %v", entry.ID, err)
		}
		break
	}
	if r.Metadata.PluginID != entry.ID || r.Metadata.Version != version {
		return nil, resp.Header.Get("ETag"), false, fmt.Errorf("release metadata does not match %s %s", entry.ID, version)
	}
	return r, resp.Header.Get("ETag"), false, nil
}

type cached struct {
	Snapshot Snapshot          `json:"snapshot"`
	ETags    map[string]string `json:"etags"`
}
type Manager struct {
	mu                                sync.RWMutex
	refreshMu                         sync.Mutex
	path, hubVersion, protocolVersion string
	catalogURL                        string
	fetcher                           Fetcher
	now                               func() time.Time
	snapshot                          Snapshot
	etags                             map[string]string
}

func New(path, hubVersion, protocolVersion string, fetcher Fetcher) *Manager {
	if fetcher == nil {
		fetcher = GitHubFetcher{
			Client: &http.Client{Timeout: defaultHTTPTimeout},
			Token:  os.Getenv("PRISM_GITHUB_TOKEN"),
		}
	}
	return &Manager{path: path, hubVersion: hubVersion, protocolVersion: protocolVersion, fetcher: fetcher, now: time.Now, etags: map[string]string{}}
}

// NewDefault 构造生产目录管理器。命令入口可先调用 Load 再调用 Start，
// 这样缓存卡片可以立即展示，不必等待 GitHub 请求完成。
func NewDefault() *Manager {
	return NewDefaultForRegion(strings.TrimSpace(os.Getenv("PRISM_REGION")))
}

// NewDefaultForRegion 使用 Hub 当前配置的区域构造生产目录管理器。
func NewDefaultForRegion(region string) *Manager {
	manager := New(DefaultCachePath(), buildinfo.Version(), DefaultProtocolVersion, nil)
	manager.catalogURL = defaultCatalogURL(region)
	return manager
}

func defaultCatalogURL(region string) string {
	if value := strings.TrimSpace(os.Getenv("PRISM_PLUGIN_REGISTRY_URL")); value != "" {
		return strings.TrimRight(value, "/")
	}
	apiURL, _ := buildinfo.Endpoints(region)
	if apiURL == "" {
		return ""
	}
	return strings.TrimRight(apiURL, "/") + "/public/plugins?channel=stable"
}
func DefaultCachePath() string {
	return host.DefaultDataPath("plugin-catalog-cache.json")
}
func (m *Manager) Load() error {
	b, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var c cached
	if err = json.Unmarshal(b, &c); err != nil {
		return err
	}
	// 组织迁移后，旧缓存仍可能保存历史 repository；加载时按官方 catalog 修正，避免离线安装被旧地址阻断。
	// 老版本写入的缓存还可能缺少 DisplayName / 图标等展示元数据，同样从内置 catalog 回填，
	// 否则 304 命中时这些字段会一直为空，Dashboard 只能显示默认 logo。
	entries := make(map[string]Entry, len(OfficialCatalog()))
	for _, entry := range OfficialCatalog() {
		entries[entry.ID] = entry
	}
	for index := range c.Snapshot.Items {
		item := &c.Snapshot.Items[index]
		entry, official := entries[item.ID]
		if !official {
			continue
		}
		if entry.Repository != "" {
			item.Repository = entry.Repository
			if item.Release != nil {
				item.Release.Repository = entry.Repository
			}
		}
		if item.DisplayName == "" {
			item.DisplayName = entry.DisplayName
		}
		if item.IconURL == "" {
			item.IconURL = entry.IconURL
		}
		if item.IconSVGURL == "" {
			item.IconSVGURL = entry.IconSVGURL
		}
		if item.Channel == "" {
			item.Channel = entry.Channel
		}
	}
	m.mu.Lock()
	m.snapshot = c.Snapshot
	m.etags = c.ETags
	if m.etags == nil {
		m.etags = map[string]string{}
	}
	m.mu.Unlock()
	return nil
}
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneSnapshot(m.snapshot)
}
func (m *Manager) Refresh(ctx context.Context) Snapshot {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	if strings.TrimSpace(m.catalogURL) != "" {
		if snapshot, err := m.fetchRemoteCatalog(ctx); err == nil {
			if len(snapshot.Items) == 0 {
				// 迁移期 Registry 尚未导入旧官方插件时继续走旧目录，避免空目录覆盖可用安装源。
				m.mu.RLock()
				previous := cloneSnapshot(m.snapshot)
				m.mu.RUnlock()
				if len(previous.Items) > 0 {
					return previous
				}
			} else {
				m.mu.Lock()
				m.snapshot = snapshot
				m.mu.Unlock()
				_ = m.save()
				return snapshot
			}
		} else {
			// 远程 Registry 暂时不可用时继续使用兼容的 GitHub fallback，避免旧版本安装能力中断。
			m.mu.Lock()
			previous := cloneSnapshot(m.snapshot)
			m.mu.Unlock()
			if len(previous.Items) > 0 {
				return previous
			}
		}
	}
	m.mu.RLock()
	etags := make(map[string]string, len(m.etags))
	for k, v := range m.etags {
		etags[k] = v
	}
	previous := cloneSnapshot(m.snapshot)
	m.mu.RUnlock()
	old := map[string]Item{}
	for _, x := range previous.Items {
		old[x.ID] = x
	}
	items := make([]Item, 0, 4)
	for _, entry := range OfficialCatalog() {
		// Release 尚未解析时保留可安装的展示状态并透出检查错误；实际安装仍必须有
		// 已验证的 Release 和当前平台资产。
		item := Item{Entry: entry, Compatible: true, Supported: true, LastCheckedAt: m.now()}
		release, tag, unchanged, err := m.fetcher.Fetch(ctx, entry, etags[entry.ID])
		if tag != "" {
			etags[entry.ID] = tag
		}
		if unchanged {
			if previousItem, exists := old[entry.ID]; exists {
				item = previousItem
			}
			// 304 只说明 Release 未变化；Entry 元数据（显示名、图标）仍要用
			// 内置 catalog 刷新，老版本缓存缺失的字段才能被补齐。
			item.Entry = entry
			item.LastCheckedAt = m.now()
		} else if err != nil {
			if previousItem, exists := old[entry.ID]; exists {
				item = previousItem
			}
			item.Entry = entry
			item.Error = err.Error()
			item.LastCheckedAt = m.now()
		} else {
			item.Release = release
			item.LatestVersion = release.Version
			item.Compatible = Compatible(m.hubVersion, m.protocolVersion, release.Metadata)
			item.Supported = release.Metadata.Supports(runtime.GOOS, runtime.GOARCH)
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	s := Snapshot{Items: items, CheckedAt: m.now()}
	m.mu.Lock()
	m.snapshot = s
	m.etags = etags
	m.mu.Unlock()
	// 同时持久化成功元数据、ETag 和检查时间；离线或请求受限时保留上一次成功的 Release。
	_ = m.save()
	return s
}

type remoteCatalogResponse struct {
	Success bool `json:"success"`
	Data    struct {
		Items []remoteCatalogItem `json:"items"`
	} `json:"data"`
}

type remoteCatalogItem struct {
	PluginID    string         `json:"plugin_id"`
	Repository  string         `json:"repository"`
	DisplayName string         `json:"display_name"`
	Description string         `json:"description"`
	IconURL     string         `json:"icon_url"`
	Channel     string         `json:"channel"`
	Featured    bool           `json:"featured"`
	Version     string         `json:"version"`
	Release     *remoteRelease `json:"release"`
}

type remoteRelease struct {
	Repository string          `json:"repository"`
	Tag        string          `json:"tag"`
	Version    string          `json:"version"`
	Assets     []Asset         `json:"assets"`
	Metadata   ReleaseMetadata `json:"metadata"`
}

func (m *Manager) fetchRemoteCatalog(ctx context.Context) (Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.catalogURL, nil)
	if err != nil {
		return Snapshot{}, err
	}
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: defaultHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return Snapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return Snapshot{}, fmt.Errorf("plugin registry returned %s", resp.Status)
	}
	var payload remoteCatalogResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&payload); err != nil {
		return Snapshot{}, err
	}
	if !payload.Success {
		return Snapshot{}, fmt.Errorf("plugin registry rejected catalog")
	}
	items := make([]Item, 0, len(payload.Data.Items))
	official := make(map[string]Entry, len(OfficialCatalog()))
	for _, entry := range OfficialCatalog() {
		official[entry.ID] = entry
	}
	for _, remote := range payload.Data.Items {
		if strings.TrimSpace(remote.PluginID) == "" || remote.Release == nil {
			continue
		}
		metadata := remote.Release.Metadata
		entry := Entry{
			ID: remote.PluginID, Repository: remote.Repository, DisplayName: remote.DisplayName,
			Description: remote.Description, IconURL: remote.IconURL, Channel: remote.Channel,
		}
		// Registry 迁移期可能还没录入展示元数据；官方白名单插件的名称与图标
		// 以内置 catalog 为准，避免目录页退化成默认 logo。
		if builtin, ok := official[entry.ID]; ok {
			entry.DisplayName = builtin.DisplayName
			entry.IconURL = builtin.IconURL
			entry.IconSVGURL = builtin.IconSVGURL
		}
		items = append(items, Item{
			Entry: entry,
			Release: &Release{
				Repository: remote.Release.Repository, Tag: remote.Release.Tag,
				Version: remote.Release.Version, Assets: remote.Release.Assets, Metadata: metadata,
			},
			LatestVersion: remote.Version,
			Compatible:    Compatible(m.hubVersion, m.protocolVersion, metadata),
			Supported:     metadata.Supports(runtime.GOOS, runtime.GOARCH),
			LastCheckedAt: m.now(),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return Snapshot{Items: items, CheckedAt: m.now()}, nil
}
func (m *Manager) Start(ctx context.Context) {
	go func() {
		m.Refresh(ctx)
		ticker := time.NewTicker(CheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.Refresh(ctx)
			}
		}
	}()
}
func (m *Manager) save() error {
	m.mu.RLock()
	b, err := json.Marshal(cached{Snapshot: m.snapshot, ETags: m.etags})
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(m.path), 0o700); err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
func cloneSnapshot(s Snapshot) Snapshot {
	b, _ := json.Marshal(s)
	var c Snapshot
	_ = json.Unmarshal(b, &c)
	return c
}

func (r ReleaseMetadata) Supports(goos, goarch string) bool {
	raw := goos + "-" + goarch
	if goos == "darwin" {
		goos = "macos"
	}
	if goarch == "amd64" {
		goarch = "x64"
	}
	want := goos + "-" + goarch
	for _, v := range r.SupportedPlatforms {
		if strings.EqualFold(strings.TrimSpace(v), want) || strings.EqualFold(strings.TrimSpace(v), raw) {
			return true
		}
	}
	return false
}
func Compatible(hub, protocol string, m ReleaseMetadata) bool {
	return (m.MinHubVersion == "" || CompareSemVer(hub, m.MinHubVersion) >= 0) && (m.ProtocolVersion == "" || protocol == "" || strings.TrimSpace(m.ProtocolVersion) == strings.TrimSpace(protocol))
}
func ValidSemVer(v string) bool {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	p := strings.SplitN(v, "+", 2)[0]
	if strings.Contains(p, "-") {
		return false
	}
	a := strings.Split(p, ".")
	if len(a) != 3 {
		return false
	}
	for _, x := range a {
		if x == "" || (len(x) > 1 && x[0] == '0') {
			return false
		}
		for _, r := range x {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}
func CompareSemVer(a, b string) int {
	type parsedVersion struct {
		core [3]int
		pre  []string
	}
	parse := func(s string) parsedVersion {
		var parsed parsedVersion
		s = strings.TrimPrefix(strings.TrimSpace(s), "v")
		s = strings.SplitN(s, "+", 2)[0]
		parts := strings.SplitN(s, "-", 2)
		fmt.Sscanf(parts[0], "%d.%d.%d", &parsed.core[0], &parsed.core[1], &parsed.core[2])
		if len(parts) == 2 {
			parsed.pre = strings.Split(parts[1], ".")
		}
		return parsed
	}
	x, y := parse(a), parse(b)
	for i := 0; i < 3; i++ {
		if x.core[i] < y.core[i] {
			return -1
		}
		if x.core[i] > y.core[i] {
			return 1
		}
	}
	if len(x.pre) == 0 && len(y.pre) == 0 {
		return 0
	}
	if len(x.pre) == 0 {
		return 1
	}
	if len(y.pre) == 0 {
		return -1
	}
	for index := 0; index < len(x.pre) && index < len(y.pre); index++ {
		if x.pre[index] == y.pre[index] {
			continue
		}
		var left, right int
		leftNumeric := false
		rightNumeric := false
		if _, err := fmt.Sscanf(x.pre[index], "%d", &left); err == nil && fmt.Sprint(left) == x.pre[index] {
			leftNumeric = true
		}
		if _, err := fmt.Sscanf(y.pre[index], "%d", &right); err == nil && fmt.Sprint(right) == y.pre[index] {
			rightNumeric = true
		}
		if leftNumeric && rightNumeric {
			if left < right {
				return -1
			}
			return 1
		}
		if leftNumeric {
			return -1
		}
		if rightNumeric {
			return 1
		}
		if x.pre[index] < y.pre[index] {
			return -1
		}
		return 1
	}
	if len(x.pre) < len(y.pre) {
		return -1
	}
	if len(x.pre) > len(y.pre) {
		return 1
	}
	return 0
}
