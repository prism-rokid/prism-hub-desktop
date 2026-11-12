//go:build darwin && cgo

package plugin

/*
#cgo LDFLAGS: -lproc
#include <libproc.h>
#include <sys/proc_info.h>
#include <stdint.h>

static int prism_runtime_process_info(int pid, uint64_t *seconds, uint64_t *microseconds, uint32_t *process_group) {
	struct proc_bsdinfo info;
	int size = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, sizeof(info));
	if (size != sizeof(info)) return 0;
	*seconds = info.pbi_start_tvsec;
	*microseconds = info.pbi_start_tvusec;
	*process_group = info.pbi_pgid;
	return 1;
}
*/
import "C"

import (
	"os/exec"
	"strconv"
	"strings"
)

func runtimeLeaseProcessInspection(pid int) (runtimeLeaseProcess, error) {
	if pid <= 0 {
		return runtimeLeaseProcess{}, nil
	}
	var seconds, microseconds C.uint64_t
	var groupID C.uint32_t
	if C.prism_runtime_process_info(C.int(pid), &seconds, &microseconds, &groupID) == 0 {
		return runtimeLeaseProcess{}, nil
	}
	command := ""
	if output, err := exec.Command("ps", "-ww", "-p", strconv.Itoa(pid), "-o", "command=").Output(); err == nil {
		command = strings.TrimSpace(string(output))
	}
	return runtimeLeaseProcess{
		Alive:           true,
		StartIdentifier: "darwin:" + strconv.FormatUint(uint64(seconds), 10) + ":" + strconv.FormatUint(uint64(microseconds), 10),
		Command:         command,
		ProcessGroupID:  int(groupID),
	}, nil
}
