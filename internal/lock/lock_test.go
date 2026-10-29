package lock

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAcquirePreventsSecondInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire first: %v", err)
	}
	defer first.Release()
	second, err := Acquire(path)
	if !errors.Is(err, ErrLocked) {
		if second != nil {
			_ = second.Release()
		}
		t.Fatalf("expected ErrLocked for second acquire, got lock=%+v err=%v", second, err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release first: %v", err)
	}
	third, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	if err := third.Release(); err != nil {
		t.Fatalf("Release third: %v", err)
	}
}
