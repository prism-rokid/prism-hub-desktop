package main

import (
	"bytes"
	"testing"

	prismapi "rokid-prism-hub/internal/api"
)

func TestTryHandleSubcommandRejectsPublicLifecycleCommands(t *testing.T) {
	for _, args := range [][]string{
		{"hub"},
		{"dashboard"},
		{"login"},
		{"logout"},
		{"test"},
		{"daemon"},
		{"plugin"},
	} {
		if handled, _ := tryHandleSubcommand(args, &bytes.Buffer{}, &bytes.Buffer{}); handled {
			t.Fatalf("expected legacy command to be unsupported: %v", args)
		}
	}
}

func TestDesktopSidecarRejectsDirectInvocation(t *testing.T) {
	t.Setenv("PRISM_DESKTOP_MANAGED", "")
	var stdout, stderr bytes.Buffer
	handled, code := tryHandleSubcommand([]string{"sidecar"}, &stdout, &stderr)
	if !handled || code != 2 {
		t.Fatalf("expected managed sidecar rejection, handled=%v code=%d", handled, code)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("managed by the Prism Desktop")) {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

func TestPrismAPIEndpointPreservesMountedBasePath(t *testing.T) {
	got := prismapi.Endpoint("https://ai-cloud-dev.rokid.com/prism/api/", "/public/bind/start")
	want := "https://ai-cloud-dev.rokid.com/prism/api/public/bind/start"
	if got != want {
		t.Fatalf("unexpected endpoint: got %q want %q", got, want)
	}
}
