//go:build darwin && !cgo

package plugin

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func runtimeLeaseProcessInspection(pid int) (runtimeLeaseProcess, error) {
	if pid <= 0 {
		return runtimeLeaseProcess{}, nil
	}
	startOutput, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=").Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return runtimeLeaseProcess{}, nil
		}
		return runtimeLeaseProcess{}, err
	}
	startIdentifier := strings.Join(strings.Fields(string(startOutput)), " ")
	if startIdentifier == "" {
		return runtimeLeaseProcess{}, nil
	}
	groupID, err := syscall.Getpgid(pid)
	if err != nil {
		return runtimeLeaseProcess{}, fmt.Errorf("read process group: %w", err)
	}
	commandOutput, err := exec.Command("ps", "-ww", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return runtimeLeaseProcess{}, fmt.Errorf("read process command: %w", err)
	}
	return runtimeLeaseProcess{
		Alive:           true,
		StartIdentifier: "darwin-fallback:" + startIdentifier,
		Command:         strings.TrimSpace(string(commandOutput)),
		ProcessGroupID:  groupID,
	}, nil
}
