package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	prismapi "rokid-prism-hub/internal/api"
	bridgeconfig "rokid-prism-hub/internal/config"
	"rokid-prism-hub/internal/plugininstall"
)

const (
	dashboardPluginUninstallStateRemotePending = "remote_pending"
	dashboardPluginUninstallStateRemoteDeleted = "remote_deleted"
)

var dashboardPluginUninstallMu sync.Mutex

// dashboardPluginUninstallOperation 记录跨云端与本地的卸载阶段。
// 只有云端确认删除后才允许进入本地清理，避免留下云端已注册而本地不存在的状态。
type dashboardPluginUninstallOperation struct {
	PluginID  string `json:"plugin_id"`
	Root      string `json:"root"`
	State     string `json:"state"`
	UpdatedAt string `json:"updated_at"`
}

type dashboardPluginUninstallStore struct {
	Operations map[string]dashboardPluginUninstallOperation `json:"operations"`
}

type dashboardPluginUninstallRemote interface {
	Delete(cfg *bridgeconfig.Config, pluginID string) error
	Registered(cfg *bridgeconfig.Config, pluginID string) (bool, error)
}

type dashboardPluginUninstallRemoteFuncs struct {
	delete     func(*bridgeconfig.Config, string) error
	registered func(*bridgeconfig.Config, string) (bool, error)
}

func (f dashboardPluginUninstallRemoteFuncs) Delete(cfg *bridgeconfig.Config, pluginID string) error {
	return f.delete(cfg, pluginID)
}

func (f dashboardPluginUninstallRemoteFuncs) Registered(cfg *bridgeconfig.Config, pluginID string) (bool, error) {
	return f.registered(cfg, pluginID)
}

func dashboardPluginUninstallStatePath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), "plugin-uninstall-transactions.json")
}

func loadDashboardPluginUninstallStore(configPath string) (dashboardPluginUninstallStore, error) {
	store := dashboardPluginUninstallStore{Operations: map[string]dashboardPluginUninstallOperation{}}
	data, err := os.ReadFile(dashboardPluginUninstallStatePath(configPath))
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return store, fmt.Errorf("读取插件卸载事务失败: %w", err)
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return store, fmt.Errorf("解析插件卸载事务失败: %w", err)
	}
	if store.Operations == nil {
		store.Operations = map[string]dashboardPluginUninstallOperation{}
	}
	return store, nil
}

func saveDashboardPluginUninstallStore(configPath string, store dashboardPluginUninstallStore) error {
	path := dashboardPluginUninstallStatePath(configPath)
	if len(store.Operations) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("删除已完成的插件卸载事务失败: %w", err)
		}
		return nil
	}
	data, err := json.Marshal(store)
	if err != nil {
		return fmt.Errorf("序列化插件卸载事务失败: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("创建插件卸载事务目录失败: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".plugin-uninstall-*.json")
	if err != nil {
		return fmt.Errorf("创建插件卸载事务临时文件失败: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("设置插件卸载事务权限失败: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("写入插件卸载事务失败: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("关闭插件卸载事务失败: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("提交插件卸载事务失败: %w", err)
	}
	return nil
}

func dashboardPluginUninstallRemoteAPI() dashboardPluginUninstallRemote {
	return dashboardPluginUninstallRemoteFuncs{
		delete: func(cfg *bridgeconfig.Config, pluginID string) error {
			_, err := prismapi.DeleteUserHostPlugin(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID, pluginID)
			return err
		},
		registered: func(cfg *bridgeconfig.Config, pluginID string) (bool, error) {
			plugins, err := prismapi.ListUserHostPlugins(cfg.Prism.APIURL(), cfg.Account, cfg.Account.HostID)
			if err != nil {
				return false, err
			}
			for _, plugin := range plugins.Items {
				if strings.EqualFold(strings.TrimSpace(plugin.PluginID), pluginID) {
					return true, nil
				}
			}
			return false, nil
		},
	}
}

func dashboardPluginUninstallRoot(cfg *bridgeconfig.Config, requestedRoot, pluginID string, installer *plugininstall.Installer) (string, error) {
	root := strings.TrimSpace(requestedRoot)
	if root == "" && installer != nil {
		if state, err := installer.State(pluginID); err == nil && state.ActiveVersion != "" {
			root = installer.Root
		}
	}
	if root != "" {
		target, err := resolvePluginRemovalTarget(cfg, root, pluginID)
		if err != nil {
			return "", err
		}
		return target.Root, nil
	}
	target, err := resolvePluginRemovalTarget(cfg, "", pluginID)
	if err != nil {
		return "", err
	}
	return target.Root, nil
}

func dashboardDeleteLocalPlugin(configPath string, operation dashboardPluginUninstallOperation, installer *plugininstall.Installer) (map[string]any, error) {
	var payload map[string]any
	remove := func() error {
		var code int
		payload, code = removeInstalledPlugin(configPath, operation.Root, operation.PluginID, io.Discard)
		if code != 0 || payload["ok"] != true {
			return fmt.Errorf("%v", payload["error"])
		}
		return nil
	}
	if installer != nil {
		if err := installer.Exclusive(operation.PluginID, remove); err != nil {
			return nil, err
		}
	} else if err := remove(); err != nil {
		return nil, err
	}
	return payload, nil
}

// dashboardMarkPluginUninstalling 会让运行时停止采纳该插件，直到本地清理完成。
func dashboardMarkPluginUninstalling(configPath, pluginID string) error {
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("加载配置以标记插件卸载状态失败: %w", err)
	}
	if cfg.Bridge.Plugins == nil {
		cfg.Bridge.Plugins = map[string]bridgeconfig.PluginConfig{}
	}
	item := cfg.Bridge.Plugins[pluginID]
	if item.Uninstalling {
		return nil
	}
	item.Uninstalling = true
	cfg.Bridge.Plugins[pluginID] = item
	if err := cfg.Save(configPath); err != nil {
		return fmt.Errorf("保存插件卸载状态失败: %w", err)
	}
	return nil
}

func dashboardUninstallPluginStrict(configPath, requestedRoot, pluginID string, installer *plugininstall.Installer, remote dashboardPluginUninstallRemote) (map[string]any, error) {
	dashboardPluginUninstallMu.Lock()
	defer dashboardPluginUninstallMu.Unlock()

	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return nil, fmt.Errorf("plugin_id required")
	}
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("加载配置失败: %w", err)
	}
	if strings.TrimSpace(cfg.Account.AccessToken) == "" || strings.TrimSpace(cfg.Account.HostID) == "" {
		return nil, fmt.Errorf("缺少账号会话或主机绑定；为保持云端和本地一致，不能开始卸载")
	}
	store, err := loadDashboardPluginUninstallStore(configPath)
	if err != nil {
		return nil, err
	}
	operation, exists := store.Operations[pluginID]
	if !exists {
		root, rootErr := dashboardPluginUninstallRoot(cfg, requestedRoot, pluginID, installer)
		if rootErr != nil {
			return nil, rootErr
		}
		operation = dashboardPluginUninstallOperation{PluginID: pluginID, Root: root, State: dashboardPluginUninstallStateRemotePending, UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
		store.Operations[pluginID] = operation
		if err := saveDashboardPluginUninstallStore(configPath, store); err != nil {
			return nil, err
		}
	}
	if operation.State != dashboardPluginUninstallStateRemoteDeleted {
		deleteErr := remote.Delete(cfg, pluginID)
		registered, readErr := remote.Registered(cfg, pluginID)
		if readErr != nil {
			return nil, fmt.Errorf("云端注销结果未确认，本地插件保持不变: %w", readErr)
		}
		if registered {
			if deleteErr != nil {
				return nil, fmt.Errorf("云端注销失败，本地插件保持不变: %w", deleteErr)
			}
			return nil, fmt.Errorf("云端仍保留该插件，本地插件保持不变")
		}
		operation.State = dashboardPluginUninstallStateRemoteDeleted
		operation.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		store.Operations[pluginID] = operation
		if err := saveDashboardPluginUninstallStore(configPath, store); err != nil {
			return nil, err
		}
	}
	if err := dashboardMarkPluginUninstalling(configPath, operation.PluginID); err != nil {
		return nil, err
	}
	payload, err := dashboardDeleteLocalPlugin(configPath, operation, installer)
	if err != nil {
		return nil, fmt.Errorf("云端已注销，本地清理未完成，将在下次启动或重试时恢复: %w", err)
	}
	delete(store.Operations, pluginID)
	if err := saveDashboardPluginUninstallStore(configPath, store); err != nil {
		return nil, err
	}
	payload["remote_cleanup"] = "confirmed"
	payload["transaction_state"] = "completed"
	payload["hub_apply"] = dashboardPluginReloaderApply("plugin_removed")
	return payload, nil
}

// dashboardRecoverPluginUninstalls 只恢复已确认云端删除的本地清理，绝不在启动时猜测云端状态。
func dashboardRecoverPluginUninstalls(configPath string, installer *plugininstall.Installer) error {
	dashboardPluginUninstallMu.Lock()
	defer dashboardPluginUninstallMu.Unlock()
	store, err := loadDashboardPluginUninstallStore(configPath)
	if err != nil {
		return err
	}
	for pluginID, operation := range store.Operations {
		if operation.State != dashboardPluginUninstallStateRemoteDeleted {
			continue
		}
		if err := dashboardMarkPluginUninstalling(configPath, operation.PluginID); err != nil {
			return fmt.Errorf("恢复插件 %s 的卸载禁用状态失败: %w", pluginID, err)
		}
		if _, err := dashboardDeleteLocalPlugin(configPath, operation, installer); err != nil {
			return fmt.Errorf("恢复插件 %s 的本地卸载失败: %w", pluginID, err)
		}
		delete(store.Operations, pluginID)
	}
	return saveDashboardPluginUninstallStore(configPath, store)
}
