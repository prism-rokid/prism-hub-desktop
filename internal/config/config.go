// Package config loads the headless bridge runtime configuration.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"rokid-prism-hub/internal/buildinfo"
	"rokid-prism-hub/internal/host"
)

const (
	PrismRegionCN     = "cn"
	PrismRegionGlobal = "global"

	DefaultPrismRegion = PrismRegionCN
)

type Config struct {
	Prism   Prism        `yaml:"prism"`
	Account Account      `yaml:"account"`
	Bridge  BridgeConfig `yaml:"bridge"`
	Doctor  DoctorConfig `yaml:"doctor"`

	activeIdentityKey string
}

// DoctorConfig gates the local self-check engine. Both switches default to
// enabled; the pointers distinguish "unset" from an explicit false.
type DoctorConfig struct {
	Enabled  *bool `yaml:"enabled,omitempty"`
	AutoHeal *bool `yaml:"auto_heal,omitempty"`
}

func (d DoctorConfig) IsEnabled() bool {
	return d.Enabled == nil || *d.Enabled
}

func (d DoctorConfig) IsAutoHealEnabled() bool {
	return d.AutoHeal == nil || *d.AutoHeal
}

type Prism struct {
	Region     string                   `yaml:"region,omitempty"`
	HostToken  string                   `yaml:"host_token,omitempty"` // Legacy input and active runtime identity.
	Identities map[string]CloudIdentity `yaml:"identities,omitempty"`
}

// CloudIdentity is scoped to a packaged deployment environment and account
// region. Local plugins and SQLite state deliberately remain outside it.
type CloudIdentity struct {
	HostToken string  `yaml:"host_token,omitempty"`
	Account   Account `yaml:"account,omitempty"`
}

type PrismEndpoints struct {
	APIURL      string
	RealtimeURL string
}

func NormalizeRegion(region string) string {
	switch strings.ToLower(strings.TrimSpace(region)) {
	case PrismRegionGlobal:
		return PrismRegionGlobal
	case PrismRegionCN, "":
		return PrismRegionCN
	default:
		return DefaultPrismRegion
	}
}

func ResolvePrismEndpoints(region string) PrismEndpoints {
	apiURL, realtimeURL := buildinfo.Endpoints(NormalizeRegion(region))
	return PrismEndpoints{APIURL: apiURL, RealtimeURL: realtimeURL}
}

func (p Prism) APIURL() string {
	return ResolvePrismEndpoints(p.Region).APIURL
}

func (p Prism) RealtimeURL() string {
	return ResolvePrismEndpoints(p.Region).RealtimeURL
}

// Account captures identity info returned by Prism API device binding.
type Account struct {
	UserID             string `yaml:"user_id,omitempty"`
	AccountID          string `yaml:"account_id,omitempty"`
	Name               string `yaml:"name,omitempty"`
	AccessToken        string `yaml:"access_token,omitempty"`
	AgentInfo          string `yaml:"agent_info,omitempty"`
	HostID             string `yaml:"host_id,omitempty"`
	HostAlias          string `yaml:"host_alias,omitempty"`
	InstallID          string `yaml:"install_id,omitempty"`
	MachineFingerprint string `yaml:"machine_fingerprint,omitempty"`
	LoginAt            string `yaml:"login_at,omitempty"`
	BoundAt            string `yaml:"bound_at,omitempty"`
}

func ActiveDeploymentEnvironment() string {
	return buildinfo.Environment()
}

func IdentityKey(environment, region string) string {
	environment = strings.ToLower(strings.TrimSpace(environment))
	if environment == "" {
		environment = "dev"
	}
	return environment + "." + NormalizeRegion(region)
}

type BridgeConfig struct {
	Enabled                  bool                    `yaml:"enabled"`
	KeepAwake                bool                    `yaml:"keep_awake,omitempty"`
	Mode                     string                  `yaml:"mode,omitempty"`
	Summary                  SummaryConfig           `yaml:"summary,omitempty"`
	PluginDirs               []string                `yaml:"plugin_dirs,omitempty"`
	PluginEnv                map[string]string       `yaml:"plugin_env,omitempty"`
	Plugins                  map[string]PluginConfig `yaml:"plugins,omitempty"`
	RemoteAuthorizations     map[string]bool         `yaml:"-"`
	RemoteAuthorizationReady bool                    `yaml:"-"`
}

type SummaryConfig struct {
	StreamProgress bool `yaml:"stream_progress"`
	FinalMaxChars  int  `yaml:"final_max_chars,omitempty"`
}

type PluginConfig struct {
	Mode               string            `yaml:"mode,omitempty"`
	ReverseSync        string            `yaml:"reverse_sync,omitempty"`
	Source             string            `yaml:"source,omitempty"`
	GatewayURL         string            `yaml:"gateway_url,omitempty"`
	Discovery          string            `yaml:"discovery,omitempty"`
	ServerURL          string            `yaml:"server_url,omitempty"`
	Command            string            `yaml:"command,omitempty"`
	SocketPath         string            `yaml:"socket_path,omitempty"`
	UseTUIEndpoint     bool              `yaml:"use_tui_endpoint,omitempty"`
	ChannelName        string            `yaml:"channel_name,omitempty"`
	Beta               bool              `yaml:"beta,omitempty"`
	VisibilityRequired bool              `yaml:"visibility_required"`
	Uninstalling       bool              `yaml:"uninstalling,omitempty"`
	Env                map[string]string `yaml:"env,omitempty"`
}

func Default() *Config {
	return &Config{
		Prism: Prism{
			Region: DefaultPrismRegion,
		},
		Bridge: BridgeConfig{
			Enabled:   true,
			KeepAwake: true,
			Mode:      "native_visible_only",
			Summary: SummaryConfig{
				StreamProgress: true,
				FinalMaxChars:  600,
			},
			Plugins: map[string]PluginConfig{},
		},
	}
}

func DefaultPath() string {
	return host.DefaultDataPath("config.yaml")
}

func DefaultPluginDir() string {
	return host.DefaultDataPath("pluginbridge-plugins")
}

func Load(path string) (*Config, error) {
	cfg := Default()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		cfg.Normalize()
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(b, &document); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	removeLegacyPluginEnabled(&document)
	removeLegacyDefaultPlugin(&document)
	normalized, err := yaml.Marshal(&document)
	if err != nil {
		return nil, fmt.Errorf("config: normalize %s: %w", path, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(normalized))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.Normalize()
	return cfg, nil
}

// Repairable reports whether the file at path has valid YAML syntax. Strict
// decoding failures (unknown fields) are repairable; syntax damage is not,
// because nothing can be salvaged from an unparseable document.
func Repairable(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var document yaml.Node
	return yaml.Unmarshal(b, &document) == nil
}

// RepairFile rebuilds a configuration file whose strict decode fails on
// unknown keys: it decodes leniently (unknown keys dropped), marshals the
// schema-shaped result back, and only then replaces the original — after
// copying it to "<path>.doctor-bak-<timestamp>" first. It refuses to touch a
// file whose YAML syntax itself is broken; manual repair is the only honest
// option there. Callers still need a Hub restart or config reload to apply.
func RepairFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("config: read %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(b, &document); err != nil {
		return "", fmt.Errorf("config: repair %s: syntax damage is not auto-repairable: %w", path, err)
	}
	removeLegacyPluginEnabled(&document)
	normalized, err := yaml.Marshal(&document)
	if err != nil {
		return "", fmt.Errorf("config: normalize %s: %w", path, err)
	}
	// Decode over Default() exactly like Load: a lenient rebuild must not
	// turn fields the broken file simply omitted (bridge.enabled et al.)
	// into explicit zero values that would disable the bridge.
	repaired := Default()
	decoder := yaml.NewDecoder(bytes.NewReader(normalized))
	if err := decoder.Decode(&repaired); err != nil {
		return "", fmt.Errorf("config: repair %s: %w", path, err)
	}
	repaired.Normalize()
	out, err := yaml.Marshal(&repaired)
	if err != nil {
		return "", fmt.Errorf("config: marshal %s: %w", path, err)
	}
	backup := fmt.Sprintf("%s.doctor-bak-%d", path, time.Now().Unix())
	if err := os.WriteFile(backup, b, 0o600); err != nil {
		return "", fmt.Errorf("config: backup %s: %w", backup, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("config: mkdir %s: %w", filepath.Dir(path), err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".config-repair-*")
	if err != nil {
		return "", fmt.Errorf("config: temp %s: %w", path, err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(out); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("config: write %s: %w", path, err)
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("config: chmod %s: %w", path, err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("config: close %s: %w", path, err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return "", fmt.Errorf("config: replace %s: %w", path, err)
	}
	return backup, nil
}

// removeLegacyPluginEnabled makes pre-cloud-authorization configs readable
// without retaining a local per-plugin remote-access switch. Save never emits
// that field again, so the migration completes on the next config write.
func removeLegacyPluginEnabled(document *yaml.Node) {
	if document == nil || len(document.Content) == 0 {
		return
	}
	root := document.Content[0]
	bridge := mappingValue(root, "bridge")
	plugins := mappingValue(bridge, "plugins")
	if plugins == nil || plugins.Kind != yaml.MappingNode {
		return
	}
	for index := 1; index < len(plugins.Content); index += 2 {
		removeMappingKey(plugins.Content[index], "enabled")
	}
}

// removeLegacyDefaultPlugin makes configs written before the default-plugin
// concept was removed readable. The field no longer has any runtime meaning;
// Save never emits it again, so the migration completes on the next write.
func removeLegacyDefaultPlugin(document *yaml.Node) {
	if document == nil || len(document.Content) == 0 {
		return
	}
	root := document.Content[0]
	bridge := mappingValue(root, "bridge")
	if bridge == nil || bridge.Kind != yaml.MappingNode {
		return
	}
	removeMappingKey(bridge, "default_plugin")
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return node.Content[index+1]
		}
	}
	return nil
}

func removeMappingKey(node *yaml.Node, key string) {
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			node.Content = append(node.Content[:index], node.Content[index+2:]...)
			return
		}
	}
}

func (c *Config) Save(path string) error {
	if c == nil {
		return fmt.Errorf("config: nil config")
	}
	c.Normalize()
	c.persistActiveIdentity()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("config: mkdir %s: %w", filepath.Dir(path), err)
	}
	// account and host_token used to be top-level fields. Persist only the
	// scoped identity map, while Load remains backward compatible with them.
	disk := struct {
		Prism  Prism        `yaml:"prism"`
		Bridge BridgeConfig `yaml:"bridge"`
		Doctor DoctorConfig `yaml:"doctor"`
	}{
		Prism:  c.Prism,
		Bridge: c.Bridge,
		Doctor: c.Doctor,
	}
	disk.Prism.HostToken = ""
	b, err := yaml.Marshal(disk)
	if err != nil {
		return fmt.Errorf("config: marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return fmt.Errorf("config: write %s: %w", path, err)
	}
	return nil
}

func (c *Config) Normalize() {
	if c == nil {
		return
	}
	c.Prism.Region = NormalizeRegion(c.Prism.Region)
	c.Prism.HostToken = strings.TrimSpace(c.Prism.HostToken)
	c.Account = normalizeAccount(c.Account)
	c.Prism.Identities = normalizeIdentities(c.Prism.Identities)
	c.activateIdentity()
	c.Bridge.Mode = strings.TrimSpace(c.Bridge.Mode)
	c.Bridge.KeepAwake = true
	if c.Bridge.Mode == "" {
		c.Bridge.Mode = "native_visible_only"
	}
	if c.Bridge.Summary.FinalMaxChars <= 0 {
		c.Bridge.Summary.FinalMaxChars = 600
	}
	if len(c.Bridge.PluginDirs) > 0 {
		dirs := make([]string, 0, len(c.Bridge.PluginDirs))
		for _, dir := range c.Bridge.PluginDirs {
			dir = strings.TrimSpace(dir)
			if dir != "" {
				dirs = append(dirs, dir)
			}
		}
		c.Bridge.PluginDirs = dirs
	}
	c.Bridge.PluginEnv = NormalizePluginEnv(c.Bridge.PluginEnv)
	if c.Bridge.Plugins == nil {
		c.Bridge.Plugins = map[string]PluginConfig{}
	}
	normalized := make(map[string]PluginConfig, len(c.Bridge.Plugins))
	for id, plugin := range c.Bridge.Plugins {
		key := strings.ToLower(strings.TrimSpace(id))
		if key == "" {
			continue
		}
		plugin.Source = strings.ToLower(strings.TrimSpace(plugin.Source))
		if plugin.Source == "" {
			plugin.Source = "plugin"
		}
		plugin.Mode = strings.TrimSpace(plugin.Mode)
		plugin.ReverseSync = strings.ToLower(strings.TrimSpace(plugin.ReverseSync))
		plugin.GatewayURL = strings.TrimSpace(plugin.GatewayURL)
		plugin.Discovery = strings.TrimSpace(plugin.Discovery)
		plugin.ServerURL = strings.TrimSpace(plugin.ServerURL)
		plugin.Command = strings.TrimSpace(plugin.Command)
		plugin.SocketPath = strings.TrimSpace(plugin.SocketPath)
		plugin.ChannelName = strings.TrimSpace(plugin.ChannelName)
		plugin.Env = NormalizePluginEnv(plugin.Env)
		normalized[key] = plugin
	}
	c.Bridge.Plugins = normalized
}

// SelectRegion keeps the prior identity before replacing the active runtime
// credentials with those for the newly selected region.
func (c *Config) SelectRegion(region string) {
	if c == nil {
		return
	}
	c.Prism.Region = NormalizeRegion(region)
	c.activateIdentity()
}

// ClearActiveIdentity logs out only from the active packaged environment and
// region. Identities for other environments remain available to their builds.
func (c *Config) ClearActiveIdentity() {
	if c == nil {
		return
	}
	if c.Prism.Identities != nil {
		delete(c.Prism.Identities, c.currentIdentityKey())
	}
	c.Prism.HostToken = ""
	c.Account = Account{}
	c.activeIdentityKey = c.currentIdentityKey()
}

func (c *Config) currentIdentityKey() string {
	return IdentityKey(ActiveDeploymentEnvironment(), c.Prism.Region)
}

func (c *Config) activateIdentity() {
	if c == nil {
		return
	}
	key := c.currentIdentityKey()
	if c.activeIdentityKey == key {
		return
	}
	if c.activeIdentityKey != "" {
		c.storeIdentity(c.activeIdentityKey, CloudIdentity{HostToken: c.Prism.HostToken, Account: c.Account})
	}
	if identity, ok := c.Prism.Identities[key]; ok {
		c.Prism.HostToken = identity.HostToken
		c.Account = identity.Account
	} else if c.activeIdentityKey == "" && identityPresent(CloudIdentity{HostToken: c.Prism.HostToken, Account: c.Account}) {
		// One-time in-memory migration from legacy top-level fields. Save writes
		// only identities, so the next load has no duplicate credential copy.
		c.storeIdentity(key, CloudIdentity{HostToken: c.Prism.HostToken, Account: c.Account})
	} else {
		c.Prism.HostToken = ""
		c.Account = Account{}
	}
	c.activeIdentityKey = key
}

func (c *Config) persistActiveIdentity() {
	if c == nil {
		return
	}
	c.storeIdentity(c.currentIdentityKey(), CloudIdentity{HostToken: c.Prism.HostToken, Account: c.Account})
	c.activeIdentityKey = c.currentIdentityKey()
}

func (c *Config) storeIdentity(key string, identity CloudIdentity) {
	if c.Prism.Identities == nil {
		c.Prism.Identities = map[string]CloudIdentity{}
	}
	identity.HostToken = strings.TrimSpace(identity.HostToken)
	identity.Account = normalizeAccount(identity.Account)
	if !identityPresent(identity) {
		delete(c.Prism.Identities, key)
		return
	}
	c.Prism.Identities[key] = identity
}

func normalizeIdentities(input map[string]CloudIdentity) map[string]CloudIdentity {
	if len(input) == 0 {
		return map[string]CloudIdentity{}
	}
	output := make(map[string]CloudIdentity, len(input))
	for key, identity := range input {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(key)), ".")
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		normalizedKey := IdentityKey(parts[0], parts[1])
		identity.HostToken = strings.TrimSpace(identity.HostToken)
		identity.Account = normalizeAccount(identity.Account)
		if identityPresent(identity) {
			output[normalizedKey] = identity
		}
	}
	return output
}

func identityPresent(identity CloudIdentity) bool {
	return identity.HostToken != "" || identity.Account.AccessToken != "" || identity.Account.AccountID != "" || identity.Account.HostID != ""
}

func normalizeAccount(account Account) Account {
	account.UserID = strings.TrimSpace(account.UserID)
	account.AccountID = strings.TrimSpace(account.AccountID)
	account.Name = strings.TrimSpace(account.Name)
	account.AccessToken = strings.TrimSpace(account.AccessToken)
	account.AgentInfo = strings.TrimSpace(account.AgentInfo)
	account.HostID = strings.TrimSpace(account.HostID)
	account.HostAlias = strings.TrimSpace(account.HostAlias)
	account.InstallID = strings.TrimSpace(account.InstallID)
	account.MachineFingerprint = strings.TrimSpace(account.MachineFingerprint)
	account.LoginAt = strings.TrimSpace(account.LoginAt)
	account.BoundAt = strings.TrimSpace(account.BoundAt)
	return account
}

func NormalizePluginEnv(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if !IsPluginEnvKeyAllowed(key) {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func IsPluginEnvKeyAllowed(key string) bool {
	key = strings.TrimSpace(key)
	if !validEnvKey(key) {
		return false
	}
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, "AGENTBRIDGE_") || strings.HasPrefix(upper, "PRISM_PLUGIN_") {
		return false
	}
	switch upper {
	case "PRISM_HOST_TOKEN", "PRISM_CONFIG", "PRISM_DB", "PRISM_ACCOUNT_TOKEN", "PRISM_ACCESS_TOKEN":
		return false
	default:
		return true
	}
}

func validEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	first := key[0]
	return first == '_' || first >= 'A' && first <= 'Z' || first >= 'a' && first <= 'z'
}
