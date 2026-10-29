//go:build darwin && cgo

package host

/*
#cgo LDFLAGS: -framework IOKit -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/pwr_mgt/IOPMLib.h>

static IOReturn prism_create_keep_awake(IOPMAssertionID *display, IOPMAssertionID *system) {
    IOReturn result = IOPMAssertionCreateWithName(
        kIOPMAssertionTypePreventUserIdleDisplaySleep,
        kIOPMAssertionLevelOn,
        CFSTR("Prism remote control"),
        display
    );
    if (result != kIOReturnSuccess) return result;
    result = IOPMAssertionCreateWithName(
        kIOPMAssertionTypePreventUserIdleSystemSleep,
        kIOPMAssertionLevelOn,
        CFSTR("Prism remote control"),
        system
    );
    if (result != kIOReturnSuccess) IOPMAssertionRelease(*display);
    return result;
}

static IOReturn prism_release_keep_awake(IOPMAssertionID display, IOPMAssertionID system) {
    IOReturn first = IOPMAssertionRelease(display);
    IOReturn second = IOPMAssertionRelease(system);
    return first != kIOReturnSuccess ? first : second;
}
*/
import "C"

import "fmt"

type platformKeepAwake struct {
	display C.IOPMAssertionID
	system  C.IOPMAssertionID
}

func detectPlatformKeepAwakeCapability() (KeepAwakeCapability, bool) {
	// Native user-idle assertions remain effective on battery power. caffeinate
	// -s only prevents system sleep on AC power, so it cannot be the primary
	// implementation for a locked portable Mac.
	return KeepAwakeCapability{Supported: true, Method: "iopm_assertion"}, true
}

func startPlatformKeepAwake() (*platformKeepAwake, bool, error) {
	handle := &platformKeepAwake{}
	if result := C.prism_create_keep_awake(&handle.display, &handle.system); result != C.kIOReturnSuccess {
		return nil, true, fmt.Errorf("create macOS power assertions failed: %d", result)
	}
	return handle, true, nil
}

func platformKeepAwakeArgs(method string) []string {
	return nil
}

func (h *platformKeepAwake) Stop() error {
	if h == nil {
		return nil
	}
	if result := C.prism_release_keep_awake(h.display, h.system); result != C.kIOReturnSuccess {
		return fmt.Errorf("release macOS power assertions failed: %d", result)
	}
	return nil
}
