package main

import (
	"fmt"
	"os"
	"strings"
)

const cliName = "prism"

// main is the entry point. After the legacy local runtime retirement
// (serve / default interactive CLI / harness removed), a subcommand is always required - the
// binary no longer has a default chat mode.
func main() {
	if handled, code := tryHandleSubcommand(os.Args[1:], os.Stdout, os.Stderr); handled {
		os.Exit(code)
	}
	fmt.Fprintln(os.Stderr, "Prism sidecar is managed by Prism Desktop.")
	os.Exit(2)
}

// envOr returns the named env var or def when unset/blank. Shared by several
// subcommands (bridge, plugin, login, …).
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
