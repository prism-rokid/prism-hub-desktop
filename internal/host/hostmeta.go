package host

import "runtime"

func CurrentArch() string {
	return runtime.GOARCH
}
