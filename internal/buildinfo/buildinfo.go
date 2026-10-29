// Package buildinfo holds values injected into distributable binaries.
package buildinfo

import "strings"

var (
	DeploymentEnvironment = ""
	APIURLCN              = ""
	RealtimeURLCN         = ""
	APIURLGlobal          = ""
	RealtimeURLGlobal     = ""
	// HubVersion is injected with -ldflags for distributable builds. A stable
	// development fallback keeps compatibility checks deterministic locally.
	HubVersion = ""
)

func Version() string {
	if value := strings.TrimSpace(HubVersion); value != "" {
		return strings.TrimPrefix(value, "v")
	}
	return "0.0.0-dev"
}

func Environment() string {
	switch strings.ToLower(strings.TrimSpace(DeploymentEnvironment)) {
	case "dev", "test", "prod":
		return strings.ToLower(strings.TrimSpace(DeploymentEnvironment))
	default:
		return "dev"
	}
}

func Endpoints(region string) (apiURL, realtimeURL string) {
	if strings.EqualFold(strings.TrimSpace(region), "global") {
		return strings.TrimSpace(APIURLGlobal), strings.TrimSpace(RealtimeURLGlobal)
	}
	return strings.TrimSpace(APIURLCN), strings.TrimSpace(RealtimeURLCN)
}

// AccountWebURL follows the same deployment environment as the API.
func AccountWebURL(path string) string {
	origin := "https://account-web-test.rokid.com"
	if Environment() == "prod" {
		origin = "https://account-web.rokid.com"
	}
	return origin + "/" + strings.TrimLeft(path, "/")
}
