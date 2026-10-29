//go:build darwin || linux

package doctor

import (
	"context"
	"strconv"
	"syscall"
)

func (c diskCheck) Run(ctx context.Context) ([]Finding, error) {
	if c.deps.DataPath == nil {
		return nil, nil
	}
	// Any concrete file under the data directory anchors the volume that
	// matters; the lock file is always a plain sibling of the config.
	var stat syscall.Statfs_t
	if err := syscall.Statfs(c.deps.DataPath("hub.lock"), &stat); err != nil {
		return nil, nil
	}
	free := int64(stat.Bavail) * int64(stat.Bsize)
	if free >= c.deps.minFreeDiskBytes() {
		return nil, nil
	}
	return []Finding{finding(CheckDisk, "disk", SeverityWarning, "doctor.finding_disk",
		map[string]string{"free_mb": strconv.FormatInt(free>>20, 10)}, "").
		withHint("doctor.hint_disk", nil)}, nil
}
