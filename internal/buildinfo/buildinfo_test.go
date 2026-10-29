package buildinfo

import "testing"

func TestAccountWebURLDeployment(t *testing.T) {
	previous := DeploymentEnvironment
	t.Cleanup(func() { DeploymentEnvironment = previous })
	for _, environment := range []string{"prod", "test", "dev", ""} {
		DeploymentEnvironment = environment
		origin := "https://account-web-test.rokid.com"
		if environment == "prod" {
			origin = "https://account-web.rokid.com"
		}
		for _, path := range []string{"register", "forget"} {
			if got := AccountWebURL(path); got != origin+"/"+path {
				t.Fatalf("environment %q: got %q", environment, got)
			}
		}
	}
}
