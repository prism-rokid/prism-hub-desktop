package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

type ServiceRegistry interface {
	Adapter(id string) pluginruntime.PluginAdapter
	Replace(adapter pluginruntime.PluginAdapter) pluginruntime.PluginAdapter
	Unregister(id string) pluginruntime.PluginAdapter
}

type Reloader struct {
	Dirs         []string
	DirsProvider func() []string
	Service      ServiceRegistry
	Reserved     map[string]bool
	Interval     time.Duration
	// DeepScanInterval 控制插件根目录以下变更的兜底扫描周期。
	// 低成本的清单和入口检查仍按 Interval 执行。
	DeepScanInterval time.Duration
	CloseGrace       time.Duration
	Logger           *slog.Logger
	NewAdapter       func(Discovered) pluginruntime.PluginAdapter
	// Include lets callers dynamically suppress a discovered plugin from the
	// managed runtime (for example when config switches a plugin away from a
	// plugin source). Excluded plugins are treated as absent and will be
	// unregistered on the next reload.
	Include func(Discovered) bool
	// OnChange, when set, is invoked with the normalized IDs of adapters that
	// were added, replaced, or removed. It lets the daemon invalidate only the
	// runtime-owned state affected by a reload before rebuilding snapshots.
	OnChange func(pluginIDs []string)
	// SignatureExtra lets callers include non-manifest inputs (for example
	// config-driven plugin env) in the reload signature.
	SignatureExtra func(Discovered) string

	mu      sync.Mutex
	managed map[string]managedPlugin
}

type managedPlugin struct {
	signature        string
	shallowSignature string
	adapter          pluginruntime.PluginAdapter
}

func (r *Reloader) Reload(ctx context.Context) error {
	return r.reload(ctx, true)
}

func (r *Reloader) reload(ctx context.Context, deep bool) error {
	if r == nil || r.Service == nil {
		return nil
	}
	discovered, err := ScanDirs(r.currentDirs())
	if err != nil {
		r.logWarn("scan pluginbridge plugins failed", "err", err)
	}
	r.mu.Lock()
	if r.managed == nil {
		r.managed = map[string]managedPlugin{}
	}
	changedIDs := map[string]struct{}{}
	seen := map[string]bool{}
	var errs []string
	for _, item := range discovered {
		if !r.include(item) {
			continue
		}
		id := strings.ToLower(strings.TrimSpace(item.Manifest.ID))
		if id == "" || r.isReserved(id) {
			continue
		}
		seen[id] = true
		shallowSignature := r.shallowPluginSignature(item)
		if current, ok := r.managed[id]; ok {
			if !deep && current.shallowSignature == shallowSignature {
				continue
			}
			if deep {
				signature := r.pluginSignature(item)
				if current.signature == signature {
					current.shallowSignature = shallowSignature
					r.managed[id] = current
					continue
				}
			}
		}
		// 浅层签名变化很少出现且已经需要替换运行时，仅在此时计算完整签名。
		signature := r.pluginSignature(item)
		// The daemon builds its initial service from the configured plugins before
		// starting the reloader. Adopt that adapter on the first scan instead of
		// replacing it and leaving a second process alive through CloseGrace.
		if _, managed := r.managed[id]; !managed {
			if existing := r.Service.Adapter(id); existing != nil {
				r.managed[id] = managedPlugin{signature: signature, shallowSignature: shallowSignature, adapter: existing}
				r.logInfo("pluginbridge plugin adopted", "plugin_id", id, "manifest_plugin_id", item.Manifest.ID, "path", item.Path)
				continue
			}
		}
		adapter := r.newAdapter(item)
		if adapter == nil {
			errs = append(errs, fmt.Sprintf("%s: adapter factory returned nil", id))
			continue
		}
		previous := r.Service.Replace(adapter)
		if current, ok := r.managed[id]; ok && current.adapter != nil && current.adapter != previous {
			r.retire(ctx, current.adapter)
		} else if previous != nil && previous != adapter {
			r.retire(ctx, previous)
		}
		r.managed[id] = managedPlugin{signature: signature, shallowSignature: shallowSignature, adapter: adapter}
		changedIDs[id] = struct{}{}
		r.logInfo("pluginbridge plugin registered", "plugin_id", id, "manifest_plugin_id", item.Manifest.ID, "path", item.Path)
	}
	for id, current := range r.managed {
		if seen[id] {
			continue
		}
		removed := r.Service.Unregister(id)
		if current.adapter != nil && current.adapter != removed {
			r.retire(ctx, current.adapter)
		}
		if removed != nil {
			r.retire(ctx, removed)
		}
		delete(r.managed, id)
		changedIDs[id] = struct{}{}
		r.logInfo("pluginbridge plugin unregistered", "plugin_id", id)
	}
	r.mu.Unlock()
	if len(changedIDs) > 0 && r.OnChange != nil {
		r.OnChange(sortedPluginIDs(changedIDs))
	}
	if len(errs) > 0 {
		if err != nil {
			errs = append([]string{err.Error()}, errs...)
		}
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return err
}

// ReloadPlugin replaces only one Plugin runtime immediately. It is used by a
// plugin.runtime.switch request after Hub persisted a manifest-validated mode;
// unlike Reload it must not turn unrelated filesystem/config changes into
// additional process replacements.
func (r *Reloader) ReloadPlugin(ctx context.Context, pluginID string) error {
	return r.ReloadPluginWithTransition(ctx, pluginID, nil)
}

// ReloadPluginWithTransition replaces one runtime and invokes transition after
// the new adapter owns the service slot but before OnChange starts new watchers.
// Callers use this boundary to invalidate state owned by the previous runtime
// without racing the first event from the replacement runtime.
func (r *Reloader) ReloadPluginWithTransition(ctx context.Context, pluginID string, transition func()) error {
	if r == nil || r.Service == nil {
		return fmt.Errorf("plugin reloader unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return fmt.Errorf("plugin_id required")
	}
	discovered, err := ScanDirs(r.currentDirs())
	if err != nil {
		return fmt.Errorf("scan pluginbridge plugins: %w", err)
	}
	var item *Discovered
	for index := range discovered {
		candidate := &discovered[index]
		if strings.EqualFold(candidate.Manifest.ID, pluginID) {
			item = candidate
			break
		}
	}
	if item == nil {
		return fmt.Errorf("local plugin not found: %s", pluginID)
	}
	if r.isReserved(pluginID) || !r.include(*item) {
		return fmt.Errorf("plugin runtime is not managed: %s", pluginID)
	}
	adapter := r.newAdapter(*item)
	if adapter == nil {
		return fmt.Errorf("plugin adapter factory returned nil: %s", pluginID)
	}
	signature := r.pluginSignature(*item)
	shallowSignature := r.shallowPluginSignature(*item)
	r.mu.Lock()
	if r.managed == nil {
		r.managed = map[string]managedPlugin{}
	}
	previous := r.Service.Replace(adapter)
	current := r.managed[pluginID]
	r.managed[pluginID] = managedPlugin{signature: signature, shallowSignature: shallowSignature, adapter: adapter}
	r.mu.Unlock()
	if current.adapter != nil && current.adapter != previous {
		r.retire(ctx, current.adapter)
	}
	if previous != nil && previous != adapter {
		r.retire(ctx, previous)
	}
	r.logInfo("pluginbridge plugin runtime switched", "plugin_id", pluginID, "manifest_plugin_id", item.Manifest.ID)
	if transition != nil {
		transition()
	}
	if r.OnChange != nil {
		r.OnChange([]string{pluginID})
	}
	return nil
}

// RemovePlugin immediately detaches one managed runtime and closes its
// adapter. Dashboard uninstall uses this before deleting files so platforms
// that lock running binaries can remove the plugin tree. If file removal
// fails, the next scan may discover the plugin again for a retry.
func (r *Reloader) RemovePlugin(ctx context.Context, pluginID string) error {
	if r == nil || r.Service == nil {
		return nil
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return fmt.Errorf("plugin_id required")
	}
	r.mu.Lock()
	if r.managed == nil {
		r.managed = map[string]managedPlugin{}
	}
	current, managed := r.managed[pluginID]
	removed := r.Service.Unregister(pluginID)
	if managed {
		delete(r.managed, pluginID)
	}
	r.mu.Unlock()

	var closeErr error
	if removed != nil {
		closeErr = removed.Close(ctx)
	}
	if current.adapter != nil && current.adapter != removed {
		if err := current.adapter.Close(ctx); closeErr == nil {
			closeErr = err
		}
	}
	if removed != nil || current.adapter != nil {
		if r.OnChange != nil {
			r.OnChange([]string{pluginID})
		}
	}
	return closeErr
}

func sortedPluginIDs(ids map[string]struct{}) []string {
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func (r *Reloader) Run(ctx context.Context) {
	if r == nil {
		return
	}
	_ = r.Reload(ctx)
	interval := r.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	deepScanInterval := r.DeepScanInterval
	if deepScanInterval <= 0 {
		deepScanInterval = time.Minute
	}
	nextDeepScan := time.Now().Add(deepScanInterval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.Close(context.Background())
			return
		case <-ticker.C:
			deep := !time.Now().Before(nextDeepScan)
			if deep {
				nextDeepScan = time.Now().Add(deepScanInterval)
			}
			if err := r.reload(ctx, deep); err != nil {
				r.logWarn("reload pluginbridge plugins failed", "err", err)
			}
		}
	}
}

func (r *Reloader) Close(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, current := range r.managed {
		removed := r.Service.Unregister(id)
		if current.adapter != nil && current.adapter != removed {
			_ = current.adapter.Close(ctx)
		}
		if removed != nil {
			_ = removed.Close(ctx)
		}
		delete(r.managed, id)
	}
}

func (r *Reloader) retire(ctx context.Context, adapter pluginruntime.PluginAdapter) {
	if adapter == nil {
		return
	}
	grace := r.CloseGrace
	// A replacement adapter has no runtime until its first request. Keeping the
	// previous process alive creates duplicate runtimes and leaves stale desktop
	// automation workers behind when the Hub is restarted.
	if grace <= 0 {
		_ = adapter.Close(ctx)
		return
	}
	go func() {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			_ = adapter.Close(context.Background())
		case <-timer.C:
			_ = adapter.Close(context.Background())
		}
	}()
}

func (r *Reloader) isReserved(id string) bool {
	if r == nil || r.Reserved == nil {
		return false
	}
	return r.Reserved[strings.ToLower(strings.TrimSpace(id))]
}

func (r *Reloader) newAdapter(item Discovered) pluginruntime.PluginAdapter {
	if r != nil && r.NewAdapter != nil {
		return r.NewAdapter(item)
	}
	return NewProcessAdapter(item)
}

func (r *Reloader) currentDirs() []string {
	if r == nil {
		return nil
	}
	dirs := r.Dirs
	if r.DirsProvider != nil {
		dirs = r.DirsProvider()
	}
	return trimEmpty(dirs)
}

func (r *Reloader) include(item Discovered) bool {
	if r == nil || r.Include == nil {
		return true
	}
	return r.Include(item)
}

func (r *Reloader) pluginSignature(item Discovered) string {
	signature := pluginSignature(item)
	if r != nil && r.SignatureExtra != nil {
		extra := strings.TrimSpace(r.SignatureExtra(item))
		if extra != "" {
			signature += "\x1fextra:" + extra
		}
	}
	return signature
}

func (r *Reloader) shallowPluginSignature(item Discovered) string {
	parts := []string{
		item.Path,
		item.Root,
		item.Manifest.ID,
		item.Manifest.Version,
		item.Manifest.Transport,
		item.Manifest.Mode,
		item.Manifest.Endpoint,
		item.Manifest.SocketPath,
		strings.Join(item.Manifest.Command, "\x00"),
		fileSignature(item.Path),
		fileSignature(item.Root),
	}
	command := trimEmpty(item.Manifest.Command)
	if len(command) > 0 {
		name := command[0]
		if !filepath.IsAbs(name) && strings.Contains(name, string(filepath.Separator)) {
			name = filepath.Join(item.Root, name)
		}
		if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
			name += ".exe"
		}
		parts = append(parts, fileSignature(name))
	}
	if r != nil && r.SignatureExtra != nil {
		if extra := strings.TrimSpace(r.SignatureExtra(item)); extra != "" {
			parts = append(parts, "extra:"+extra)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x1f")
}

func (r *Reloader) logInfo(msg string, args ...any) {
	if r != nil && r.Logger != nil {
		r.Logger.Info(msg, args...)
	}
}

func (r *Reloader) logWarn(msg string, args ...any) {
	if r != nil && r.Logger != nil {
		r.Logger.Warn(msg, args...)
	}
}

func pluginSignature(item Discovered) string {
	parts := []string{
		item.Path,
		item.Root,
		item.Manifest.ID,
		item.Manifest.ID,
		item.Manifest.Version,
		item.Manifest.Transport,
		item.Manifest.Mode,
		item.Manifest.Endpoint,
		item.Manifest.SocketPath,
		strings.Join(item.Manifest.Command, "\x00"),
		fileSignature(item.Path),
		dirSignature(item.Root),
	}
	command := trimEmpty(item.Manifest.Command)
	if len(command) > 0 {
		name := command[0]
		if !filepath.IsAbs(name) && strings.Contains(name, string(filepath.Separator)) {
			name = filepath.Join(item.Root, name)
		}
		if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
			name += ".exe"
		}
		parts = append(parts, fileSignature(name))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x1f")
}

func dirSignature(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	// Installed plugins expose their active version through a `current` symlink.
	// WalkDir does not follow a symlink used as its root, so resolve it before
	// calculating the runtime-file signature.
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	var parts []string
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			parts = append(parts, path+":error")
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			switch name {
			case ".git", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}
		parts = append(parts, rel+"="+fileSignature(path))
		return nil
	})
	sort.Strings(parts)
	return strings.Join(parts, "\x1e")
}

func fileSignature(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return path + ":missing"
	}
	return fmt.Sprintf("%s:%d:%d", path, info.Size(), info.ModTime().UnixNano())
}
