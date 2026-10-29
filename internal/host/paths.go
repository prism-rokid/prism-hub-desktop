package host

import (
	"os"
	"path/filepath"
)

const (
	PrismDataDir         = ".prism"
	PrismDataDirEnv      = "PRISM_DATA_DIR"
	DefaultProjectSubdir = "projects/default"
)

// DefaultDataPath returns the preferred Prism data path under ~/.prism.
func DefaultDataPath(name string) string {
	if base := filepath.Clean(os.Getenv(PrismDataDirEnv)); base != "" && base != "." {
		return filepath.Join(base, name)
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return name
	}
	return filepath.Join(home, PrismDataDir, name)
}

// DefaultProjectPath returns the shared default project directory for requests
// that do not specify a cwd.
func DefaultProjectPath() string {
	return DefaultDataPath(DefaultProjectSubdir)
}
