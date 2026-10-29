//go:build windows

package doctor

import "context"

// Free-space probing has no portable syscall here yet; the check reports
// nothing instead of guessing.
func (c diskCheck) Run(ctx context.Context) ([]Finding, error) {
	return nil, nil
}
