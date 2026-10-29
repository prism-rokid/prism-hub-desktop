package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	prismapi "rokid-prism-hub/internal/api"
	bridgeconfig "rokid-prism-hub/internal/config"
	prismdb "rokid-prism-hub/internal/db"
)

func tryHandleSubcommand(args []string, stdout, stderr io.Writer) (bool, int) {
	if len(args) == 0 {
		return false, 0
	}
	switch strings.TrimSpace(args[0]) {
	case "sidecar":
		return true, runDesktopSidecarCommand(args[1:], stdout, stderr)
	default:
		return false, 0
	}
}

// runDesktopSidecarCommand is the Desktop application's private process
// protocol. It is intentionally absent from the public CLI help surface.
func runDesktopSidecarCommand(args []string, stdout, stderr io.Writer) int {
	if strings.TrimSpace(os.Getenv("PRISM_DESKTOP_MANAGED")) != "1" {
		fmt.Fprintln(stderr, "Prism sidecar is managed by the Prism Desktop application")
		return 2
	}
	fs := flag.NewFlagSet("sidecar", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", envOr("PRISM_CONFIG", bridgeconfig.DefaultPath()), "path to config.yaml")
	dbPath := fs.String("db", envOr("PRISM_DB", prismdb.DefaultPath()), "path to pluginbridge.sqlite")
	addr := fs.String("addr", hubDefaultAddr(), "hub listen address")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return runHubServeCommand(*configPath, *dbPath, *addr, true, stdout, stderr)
}

func performAccountPasswordLogin(configPath, explicitRegion, explicitAPIURL, username, password, hostAlias string) (*bridgeconfig.Config, error) {
	if username == "" || password == "" {
		return nil, fmt.Errorf("account login requires both -username and -password")
	}
	cfg, err := bridgeconfig.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}
	if strings.TrimSpace(explicitRegion) != "" {
		cfg.SelectRegion(explicitRegion)
	}
	apiURL := firstNonEmpty(strings.TrimSpace(explicitAPIURL), strings.TrimSpace(cfg.Prism.APIURL()))
	if apiURL == "" {
		return nil, fmt.Errorf("account login requires configured prism region")
	}
	hostname, _ := os.Hostname()
	result, err := prismapi.DirectBindLogin(apiURL, hostname, username, password, hostAlias)
	if err != nil {
		return nil, fmt.Errorf("prism bind login failed: %w", err)
	}
	prismapi.ApplyApprovedDeviceLogin(cfg, result)
	if err := cfg.Save(configPath); err != nil {
		return nil, fmt.Errorf("save config failed: %w", err)
	}
	return cfg, nil
}

func openBrowser(rawURL string) error {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", rawURL)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", rawURL)
	default:
		cmd = exec.Command("xdg-open", rawURL)
	}
	return cmd.Start()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func emptyDash(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "-"
	}
	return v
}
