//go:build (!darwin || !cgo) && !windows

package host

type platformKeepAwake struct{}

func detectPlatformKeepAwakeCapability() (KeepAwakeCapability, bool) {
	return KeepAwakeCapability{}, false
}

func startPlatformKeepAwake() (*platformKeepAwake, bool, error) {
	return nil, false, nil
}

func (h *platformKeepAwake) Stop() error {
	return nil
}

func platformKeepAwakeArgs(method string) []string {
	return nil
}
