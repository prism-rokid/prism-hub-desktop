package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	pluginpkg "rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/plugininstall"
	"rokid-prism-hub/internal/pluginregistry"
)

// TestReleaseChainFromGitHub exercises the full production release chain
// against a real GitHub release: discovery via the GitHub API, metadata
// validation, the official asset-URL policy, tarball download, extraction,
// manifest identity checks, a REAL process probe from staging and from the
// active path (node runtime expansion included), activation, the
// previous-version update transition, and rollback.
//
// It is opt-in because it needs network access and a published release:
//
//	PRISM_CHAIN_REPO=Rokid-Prism/prism-plugin-claude-code \
//	PRISM_CHAIN_VERSION=0.2.0 \
//	go test ./cmd/prism -run TestReleaseChainFromGitHub -v
func TestReleaseChainFromGitHub(t *testing.T) {
	repo := os.Getenv("PRISM_CHAIN_REPO")
	wantVersion := os.Getenv("PRISM_CHAIN_VERSION")
	if repo == "" || wantVersion == "" {
		t.Skip("set PRISM_CHAIN_REPO=<owner/repo> and PRISM_CHAIN_VERSION=<x.y.z> to run the release chain test")
	}
	token := os.Getenv("PRISM_GITHUB_TOKEN")
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// 1. Discovery: the production fetcher must see the release, its assets,
	//    and the checked plugin-release.json metadata.
	entry := pluginregistry.Entry{ID: "claudecode", Repository: repo, DisplayName: "Claude Code"}
	fetcher := pluginregistry.GitHubFetcher{Token: token}
	release, _, _, err := fetcher.Fetch(ctx, entry, "")
	if err != nil {
		t.Fatalf("fetch releases/latest for %s: %v", repo, err)
	}
	if release.Tag != "v"+wantVersion || release.Version != wantVersion {
		t.Fatalf("latest release = %s (%s), want v%s", release.Tag, release.Version, wantVersion)
	}
	if release.Metadata.PluginID != entry.ID || release.Metadata.ProtocolVersion != pluginregistry.DefaultProtocolVersion {
		t.Fatalf("release metadata mismatch: %+v", release.Metadata)
	}
	platform := testPlatform()
	if !release.Metadata.Supports(platform.goos, platform.goarch) {
		t.Fatalf("release does not declare support for %s-%s", platform.goos, platform.goarch)
	}
	assetNames := make([]string, 0, len(release.Assets))
	for _, asset := range release.Assets {
		assetNames = append(assetNames, asset.Name)
	}
	if len(release.Assets) < 5 { // four platforms + plugin-release.json
		t.Fatalf("expected at least 5 assets, got %d: %v", len(release.Assets), assetNames)
	}
	t.Logf("discovered %s with assets %v (protocol %s, min hub %s)", release.Tag, assetNames, release.Metadata.ProtocolVersion, release.Metadata.MinHubVersion)

	// 2. Install with the production dashboard prober: this spawns the real
	//    plugin process (node runtime + dist/index.js + adapter.probe) from
	//    staging and again from the activated path.
	root := t.TempDir()
	installer := plugininstall.New(root)
	req := plugininstall.Request{
		Entry:            entry,
		Release:          release,
		ExpectedProtocol: pluginregistry.DefaultProtocolVersion,
		Prober:           dashboardReleaseProber{},
	}
	progress := []string{}
	if err := installer.Install(ctx, req, func(state plugininstall.OperationState) { progress = append(progress, string(state)) }); err != nil {
		t.Fatalf("install: %v (progress: %v)", err, progress)
	}
	state, err := installer.State(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveVersion != wantVersion || state.PreviousVersion != "" {
		t.Fatalf("fresh install state = %+v, want active %s with no previous", state, wantVersion)
	}
	t.Logf("installed %s through %v", state.ActiveVersion, progress)

	// 3. The activated copy must start on its own from ActivePath.
	active, err := plugininstall.ActivePath(root, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if probeFrom(ctx, t, active) != wantVersion {
		t.Fatalf("active path probe did not report version %s", wantVersion)
	}
	t.Logf("active copy at %s probes over the real PluginBridge handshake", active)

	// 4. Update transition: stage a fabricated 0.1.2 as the previously active
	//    version, then reinstall the release — the installer must record the
	//    previous version and prune it only after a later successful update.
	fabricated := "0.1.2"
	if err := stageFabricatedVersion(root, entry.ID, fabricated, wantVersion); err != nil {
		t.Fatalf("stage fabricated %s: %v", fabricated, err)
	}
	if err := installer.Install(ctx, req, nil); err != nil {
		t.Fatalf("update install: %v", err)
	}
	state, err = installer.State(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveVersion != wantVersion || state.PreviousVersion != fabricated {
		t.Fatalf("update state = %+v, want active %s previous %s", state, wantVersion, fabricated)
	}
	if _, err := os.Stat(filepath.Join(root, entry.ID, "versions", fabricated)); err != nil {
		t.Fatalf("previous version dir must survive the update: %v", err)
	}
	t.Logf("update transition ok: active %s, previous %s retained", state.ActiveVersion, state.PreviousVersion)

	// 5. Rollback must restore the previous version.
	if err := installer.Rollback(entry.ID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	state, _ = installer.State(entry.ID)
	if state.ActiveVersion != fabricated {
		t.Fatalf("after rollback active = %q, want %q", state.ActiveVersion, fabricated)
	}
	rolledBack, err := plugininstall.ActivePath(root, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rollback ok: active version now %s at %s", state.ActiveVersion, rolledBack)

	// 6. Re-update to the release; the rolled-back pair (active 0.1.2,
	//    previous 0.2.0) means the first reinstall keeps 0.1.2 as the new
	//    previous version. One more install from a steady state (active 0.2.0)
	//    clears the previous and prunes the fabricated old version.
	if err := installer.Install(ctx, req, nil); err != nil {
		t.Fatalf("update-after-rollback install: %v", err)
	}
	state, _ = installer.State(entry.ID)
	if state.ActiveVersion != wantVersion || state.PreviousVersion != fabricated {
		t.Fatalf("post-rollback update state = %+v, want active %s previous %s", state, wantVersion, fabricated)
	}
	if err := installer.Install(ctx, req, nil); err != nil {
		t.Fatalf("final steady-state install: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, entry.ID, "versions", fabricated)); !os.IsNotExist(err) {
		t.Fatalf("fabricated old version should be pruned, stat err = %v", err)
	}
	state, _ = installer.State(entry.ID)
	if state.ActiveVersion != wantVersion || state.PreviousVersion != "" {
		t.Fatalf("final state = %+v, want active %s with no previous", state, wantVersion)
	}
	t.Logf("final state: active %s, previous %q, old versions pruned", state.ActiveVersion, state.PreviousVersion)
}

func probeFrom(ctx context.Context, t *testing.T, dir string) string {
	t.Helper()
	manifestPath, err := manifestInDir(dir)
	if err != nil {
		t.Fatalf("manifest in %s: %v", dir, err)
	}
	manifest, err := pluginpkg.LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load manifest %s: %v", manifestPath, err)
	}
	adapter := pluginpkg.NewProcessAdapter(pluginpkg.Discovered{Path: manifestPath, Root: dir, Manifest: manifest})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adapter.Close(closeCtx)
	}()
	capability, err := adapter.ProbeStrict(ctx)
	if err != nil {
		t.Fatalf("probe strict %s: %v", dir, err)
	}
	if !capability.Available {
		t.Fatalf("probe %s reports unavailable: %s", dir, capability.UnavailableReason)
	}
	return manifest.Version
}

// stageFabricatedVersion writes a self-consistent older version directory and
// state.json so a subsequent install takes the real update path.
func stageFabricatedVersion(root, id, version, fromVersion string) error {
	pluginRoot := filepath.Join(root, id)
	source := filepath.Join(pluginRoot, "versions", fromVersion)
	target := filepath.Join(pluginRoot, "versions", version)
	if err := os.MkdirAll(target, 0o700); err != nil {
		return err
	}
	if err := copyDir(source, target); err != nil {
		return err
	}
	manifestPath := filepath.Join(target, pluginpkg.ManifestYAML)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	patched := strings.Replace(string(raw), "version: "+fromVersion, "version: "+version, 1)
	if err := os.WriteFile(manifestPath, []byte(patched), 0o600); err != nil {
		return err
	}
	state := plugininstall.State{ActiveVersion: version, UpdatedAt: time.Now().UTC()}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(pluginRoot, "state.json"), b, 0o600)
}

func testPlatform() (p struct {
	goos, goarch, needle string
}) {
	p.goos = runtime.GOOS
	if p.goos == "darwin" {
		p.goos = "macos"
	}
	p.goarch = runtime.GOARCH
	if p.goarch == "amd64" {
		p.goarch = "x64"
	}
	p.needle = p.goos + "-" + p.goarch
	return p
}
