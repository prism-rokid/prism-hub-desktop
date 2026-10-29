//go:build windows

package main

import "syscall"

// desktopOwnerAlive reports whether the desktop shell process that started
// this sidecar is still running. The Hub lock is released by defer on a
// graceful shutdown, so an orphaned sidecar must detect a vanished owner and
// exit cleanly instead of leaving a stale hub.lock behind.
func desktopOwnerAlive(pid int) bool {
	const (
		processQueryLimitedInformation = 0x1000
		stillActive                    = 259 // STILL_ACTIVE
	)
	if pid <= 0 {
		return false
	}
	handle, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle)
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(handle, &exitCode); err != nil {
		return false
	}
	return exitCode == stillActive
}
