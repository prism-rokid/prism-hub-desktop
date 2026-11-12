//go:build linux

package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func runtimeLeaseProcessInspection(pid int) (runtimeLeaseProcess, error) {
	if pid <= 0 {
		return runtimeLeaseProcess{}, nil
	}
	statPath := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	stat, err := os.ReadFile(statPath)
	if os.IsNotExist(err) {
		return runtimeLeaseProcess{}, nil
	}
	if err != nil {
		return runtimeLeaseProcess{}, err
	}
	closing := strings.LastIndex(string(stat), ")")
	if closing < 0 {
		return runtimeLeaseProcess{}, fmt.Errorf("parse %s", statPath)
	}
	fields := strings.Fields(string(stat)[closing+1:])
	if len(fields) <= 19 {
		return runtimeLeaseProcess{}, fmt.Errorf("parse %s", statPath)
	}
	groupID, err := strconv.Atoi(fields[2])
	if err != nil {
		return runtimeLeaseProcess{}, fmt.Errorf("parse process group: %w", err)
	}
	command, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if os.IsNotExist(err) {
		return runtimeLeaseProcess{}, nil
	}
	if err != nil {
		return runtimeLeaseProcess{}, err
	}
	return runtimeLeaseProcess{
		Alive:           true,
		StartIdentifier: "linux:" + fields[19],
		Command:         strings.TrimSpace(strings.ReplaceAll(string(command), "\x00", " ")),
		ProcessGroupID:  groupID,
	}, nil
}
