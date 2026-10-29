//go:build !darwin && !linux

package plugin

// Windows process-tree cleanup is owned by the Desktop Job Object. The lease
// remains useful for diagnostics and is replaced on the next managed start.
func runtimeLeaseProcessAlive(int) bool { return false }

func terminatePluginProcessPID(int) error { return nil }
