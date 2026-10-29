package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"
)

const desktopOwnerPIDEnv = "PRISM_DESKTOP_OWNER_PID"

func desktopManagedOwnerPID() int {
	value := strings.TrimSpace(os.Getenv(desktopOwnerPIDEnv))
	pid, err := strconv.Atoi(value)
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// watchDesktopOwner closes an orphaned Desktop-owned Hub. Normal desktop quit
// still sends SIGTERM; this is the fallback for force quit or a crash, where
// macOS otherwise reparents the sidecar and its plugin process groups.
func watchDesktopOwner(ctx context.Context, cancel context.CancelFunc, ownerPID int) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !desktopOwnerAlive(ownerPID) {
				cancel()
				return
			}
		}
	}
}
