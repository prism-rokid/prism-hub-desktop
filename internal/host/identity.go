package host

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const installIDFileName = "install-id"

type Identity struct {
	InstallID          string
	MachineFingerprint string
}

func Current() (Identity, error) {
	installID, err := ensureInstallID()
	if err != nil {
		return Identity{}, err
	}
	return Identity{
		InstallID:          installID,
		MachineFingerprint: machineFingerprint(),
	}, nil
}

func ensureInstallID() (string, error) {
	path := DefaultDataPath(installIDFileName)
	if raw, err := os.ReadFile(path); err == nil {
		if value := strings.TrimSpace(string(raw)); value != "" {
			return value, nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("mkdir install identity dir: %w", err)
	}
	value, err := newInstallID()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write install identity: %w", err)
	}
	return value, nil
}

func machineFingerprint() string {
	hostname, _ := os.Hostname()
	raw := strings.Join([]string{
		strings.TrimSpace(runtime.GOOS),
		strings.TrimSpace(runtime.GOARCH),
		strings.TrimSpace(hostname),
		strings.TrimSpace(readMachineID()),
	}, "|")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func readMachineID() string {
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if raw, err := os.ReadFile(path); err == nil {
			if value := strings.TrimSpace(string(raw)); value != "" {
				return value
			}
		}
	}
	return ""
}

func newInstallID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate install identity: %w", err)
	}
	encoded := hex.EncodeToString(buf)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16], nil
}
