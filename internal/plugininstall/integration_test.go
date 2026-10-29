package plugininstall

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"rokid-prism-hub/internal/plugin"
	"rokid-prism-hub/internal/pluginregistry"
)

type acceptingProber struct{}

func (acceptingProber) Probe(_ context.Context, _ string, manifest plugin.Manifest) (ProbeResult, error) {
	return ProbeResult{Available: true, PluginID: manifest.ID, ProtocolVersion: manifest.ProtocolVersion}, nil
}

func itPlatform() string {
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

// TestIntegrationInstallFromGitHubRelease exercises the production fetch and
// install path against a real GitHub Release: metadata validation, tarball
// download, staging, activation and rollback. It is opt-in because it needs
// network access and a published release:
//
//	PRISM_IT_REPO=Eis4TY/prism-plugin-claude-code go test ./internal/plugininstall -run Integration -v
func TestIntegrationInstallFromGitHubRelease(t *testing.T) {
	repo := os.Getenv("PRISM_IT_REPO")
	if repo == "" {
		t.Skip("set PRISM_IT_REPO=<owner/repo> to run the GitHub release integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	entry := pluginregistry.Entry{ID: "claudecode", Repository: repo, DisplayName: "Claude Code"}
	fetcher := pluginregistry.GitHubFetcher{Token: os.Getenv("PRISM_GITHUB_TOKEN")}
	release, _, _, err := fetcher.Fetch(ctx, entry, "")
	if err != nil {
		t.Fatalf("fetch releases/latest for %s: %v", repo, err)
	}
	if release.Tag == "" || release.Version == "" {
		t.Fatalf("release metadata incomplete: %+v", release)
	}
	if release.Metadata.PluginID == "" || release.Metadata.ProtocolVersion == "" {
		t.Fatalf("plugin-release.json metadata missing: %+v", release.Metadata)
	}
	t.Logf("fetched %s %s (protocol %s, min hub %s)", release.Tag, release.Version, release.Metadata.ProtocolVersion, release.Metadata.MinHubVersion)

	suffix := fmt.Sprintf("-%s.tar.gz", itPlatform())
	var asset *pluginregistry.Asset
	for i := range release.Assets {
		if strings.HasSuffix(release.Assets[i].Name, suffix) {
			asset = &release.Assets[i]
			break
		}
	}
	if asset == nil {
		t.Fatalf("no asset matching %q in %v", suffix, release.Assets)
	}
	t.Logf("installing asset %s (%d bytes)", asset.Name, asset.Size)

	ins := New(t.TempDir())
	req := Request{
		Entry:            entry,
		Release:          release,
		Asset:            *asset,
		ExpectedProtocol: release.Metadata.ProtocolVersion,
		Prober:           acceptingProber{},
	}
	if err := ins.Install(ctx, req, nil); err != nil {
		t.Fatalf("install: %v", err)
	}

	state, err := ins.State(release.Metadata.PluginID)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveVersion != release.Version {
		t.Fatalf("active version = %q, want %q", state.ActiveVersion, release.Version)
	}
	t.Logf("installed and activated %s at %s", release.Metadata.PluginID, state.ActiveVersion)

	// Rollback is only exercisable when an earlier version exists.
	if state.PreviousVersion == "" {
		t.Log("no previous version staged; skipping rollback leg")
		return
	}
	if err := ins.Rollback(release.Metadata.PluginID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	state, err = ins.State(release.Metadata.PluginID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rollback ok, active version now %q", state.ActiveVersion)
}
