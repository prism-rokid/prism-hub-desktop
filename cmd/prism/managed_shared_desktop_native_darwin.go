//go:build darwin && cgo

package main

/*
#cgo LDFLAGS: -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
#include <libproc.h>
#include <stdbool.h>

static bool dotValidBundle(const char *path, long length) {
  CFURLRef url = CFURLCreateFromFileSystemRepresentation(NULL, (const UInt8 *)path, length, true);
  if (url == NULL) return false;
  CFBundleRef bundle = CFBundleCreate(NULL, url);
  CFRelease(url);
  if (bundle == NULL) return false;
  CFStringRef identity = CFBundleGetIdentifier(bundle);
  bool matches = identity != NULL && CFEqual(identity, CFSTR("com.openai.codex"));
  CFRelease(bundle);
  return matches;
}
*/
import "C"

import "unsafe"

func desktopHostNativeValidBundle(path string) bool {
	data := []byte(path)
	return len(data) > 0 && bool(C.dotValidBundle((*C.char)(unsafe.Pointer(&data[0])), C.long(len(data))))
}

func desktopHostNativeProcessPath(pid int) string {
	var path [4096]byte
	data := (*C.char)(unsafe.Pointer(&path[0]))
	if length := C.proc_pidpath(C.int(pid), unsafe.Pointer(data), C.uint32_t(len(path))); length <= 0 {
		return ""
	}
	return C.GoString(data)
}
