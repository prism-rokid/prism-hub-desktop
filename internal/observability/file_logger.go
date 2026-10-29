package observability

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"rokid-prism-hub/internal/host"
)

const (
	defaultLogMaxBytes      int64 = 10 * 1024 * 1024
	defaultLogMaxBackups          = 7
	defaultLogMaxTotalBytes       = 50 * 1024 * 1024
	defaultLogRetentionDays       = 7
)

type RotatingFileWriter struct {
	mu            sync.Mutex
	dir           string
	prefix        string
	maxBytes      int64
	maxBackups    int
	maxTotalBytes int64
	retentionDays int
	currentDay    string
	currentIdx    int
	file          *os.File
	size          int64
}

func NewRotatingFileWriter(prefix string) (*RotatingFileWriter, error) {
	dir := host.DefaultDataPath("logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	w := &RotatingFileWriter{
		dir:           dir,
		prefix:        prefix,
		maxBytes:      defaultLogMaxBytes,
		maxBackups:    defaultLogMaxBackups,
		maxTotalBytes: defaultLogMaxTotalBytes,
		retentionDays: defaultLogRetentionDays,
	}
	if err := w.rotateIfNeededLocked(time.Now(), 0); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *RotatingFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateIfNeededLocked(time.Now(), int64(len(p))); err != nil {
		return 0, err
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *RotatingFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *RotatingFileWriter) rotateIfNeededLocked(now time.Time, incoming int64) error {
	day := now.Format("2006-01-02")
	needsRotate := w.file == nil || w.currentDay != day || (w.maxBytes > 0 && w.size+incoming > w.maxBytes)
	if !needsRotate {
		return nil
	}
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.currentDay != day {
		w.currentIdx = 0
	} else {
		w.currentIdx++
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%s-%s-%02d.log", w.prefix, day, w.currentIdx))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat log file: %w", err)
	}
	w.file = file
	w.currentDay = day
	w.size = info.Size()
	w.cleanupOldFilesLocked()
	return nil
}

func (w *RotatingFileWriter) cleanupOldFilesLocked() {
	pattern := filepath.Join(w.dir, fmt.Sprintf("%s-*.log", w.prefix))
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return
	}
	type fileInfo struct {
		path string
		mod  time.Time
		size int64
	}
	files := make([]fileInfo, 0, len(matches))
	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		files = append(files, fileInfo{path: path, mod: info.ModTime(), size: info.Size()})
	}
	if len(files) == 0 {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].mod.Before(files[j].mod)
	})

	if w.retentionDays > 0 {
		cutoff := time.Now().AddDate(0, 0, -w.retentionDays)
		kept := files[:0]
		for _, file := range files {
			if file.mod.Before(cutoff) {
				_ = os.Remove(file.path)
				continue
			}
			kept = append(kept, file)
		}
		files = kept
	}

	if w.maxBackups > 0 && len(files) > w.maxBackups {
		drop := len(files) - w.maxBackups
		for i := 0; i < drop; i++ {
			_ = os.Remove(files[i].path)
		}
		files = files[drop:]
	}

	if w.maxTotalBytes > 0 {
		var total int64
		for _, file := range files {
			total += file.size
		}
		for total > w.maxTotalBytes && len(files) > 1 {
			total -= files[0].size
			_ = os.Remove(files[0].path)
			files = files[1:]
		}
	}
}

func NewServeLogWriter(stderr io.Writer) (io.Writer, io.Closer, error) {
	fileWriter, err := NewRotatingFileWriter("hub")
	if err != nil {
		return stderr, io.NopCloser(nil), err
	}
	return io.MultiWriter(stderr, fileWriter), fileWriter, nil
}
