package observability

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotatingFileWriterRotatesBySize(t *testing.T) {
	root := t.TempDir()
	writer := &RotatingFileWriter{
		dir:           root,
		prefix:        "test",
		maxBytes:      16,
		maxBackups:    7,
		maxTotalBytes: 1024,
		retentionDays: 30,
	}
	if err := writer.rotateIfNeededLocked(time.Now(), 0); err != nil {
		t.Fatalf("init writer: %v", err)
	}
	defer writer.Close()

	if _, err := writer.Write([]byte("1234567890\n")); err != nil {
		t.Fatalf("write first chunk: %v", err)
	}
	if _, err := writer.Write([]byte("abcdefghij\n")); err != nil {
		t.Fatalf("write second chunk: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(root, "test-*.log"))
	if err != nil {
		t.Fatalf("glob logs: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("expected rotation to create at least 2 files, got %d", len(files))
	}
}

func TestRotatingFileWriterCleansUpOldBackups(t *testing.T) {
	root := t.TempDir()
	writer := &RotatingFileWriter{
		dir:           root,
		prefix:        "cleanup",
		maxBackups:    2,
		maxTotalBytes: 1024,
		retentionDays: 30,
	}

	base := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 4; i++ {
		path := filepath.Join(root, fmt.Sprintf("cleanup-2026-06-06-%02d.log", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("x%d", i)), 0o600); err != nil {
			t.Fatalf("seed log file %d: %v", i, err)
		}
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatalf("chtimes log file %d: %v", i, err)
		}
	}

	writer.cleanupOldFilesLocked()

	files, err := filepath.Glob(filepath.Join(root, "cleanup-*.log"))
	if err != nil {
		t.Fatalf("glob logs: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 log files after cleanup, got %d", len(files))
	}
}

func TestRotatingFileWriterCleansUpExpiredRetentionDays(t *testing.T) {
	root := t.TempDir()
	writer := &RotatingFileWriter{
		dir:           root,
		prefix:        "retention",
		maxBackups:    10,
		maxTotalBytes: 1024 * 1024,
		retentionDays: 3,
	}

	now := time.Now().UTC()
	oldPath := filepath.Join(root, "retention-2026-06-01-00.log")
	newPath := filepath.Join(root, "retention-2026-06-10-00.log")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("seed old log: %v", err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o600); err != nil {
		t.Fatalf("seed new log: %v", err)
	}
	if err := os.Chtimes(oldPath, now.AddDate(0, 0, -10), now.AddDate(0, 0, -10)); err != nil {
		t.Fatalf("chtimes old log: %v", err)
	}
	if err := os.Chtimes(newPath, now, now); err != nil {
		t.Fatalf("chtimes new log: %v", err)
	}

	writer.cleanupOldFilesLocked()

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected expired log removed, stat err=%v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("expected recent log to remain: %v", err)
	}
}

func TestRotatingFileWriterCleansUpByTotalSize(t *testing.T) {
	root := t.TempDir()
	writer := &RotatingFileWriter{
		dir:           root,
		prefix:        "sizecap",
		maxBackups:    10,
		maxTotalBytes: 5,
		retentionDays: 30,
	}

	base := time.Now().UTC().Truncate(time.Minute)
	paths := []string{
		filepath.Join(root, "sizecap-2026-06-06-00.log"),
		filepath.Join(root, "sizecap-2026-06-06-01.log"),
		filepath.Join(root, "sizecap-2026-06-06-02.log"),
	}
	contents := []string{"111", "22", "3"}
	for i, path := range paths {
		if err := os.WriteFile(path, []byte(contents[i]), 0o600); err != nil {
			t.Fatalf("seed log %d: %v", i, err)
		}
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatalf("chtimes log %d: %v", i, err)
		}
	}

	writer.cleanupOldFilesLocked()

	if _, err := os.Stat(paths[0]); !os.IsNotExist(err) {
		t.Fatalf("expected oldest oversized log removed, stat err=%v", err)
	}
	if _, err := os.Stat(paths[1]); err != nil {
		t.Fatalf("expected middle log kept: %v", err)
	}
	if _, err := os.Stat(paths[2]); err != nil {
		t.Fatalf("expected newest log kept: %v", err)
	}
}
