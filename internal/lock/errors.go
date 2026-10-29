package lock

import "errors"

var ErrLocked = errors.New("pluginbridge daemon already running")
