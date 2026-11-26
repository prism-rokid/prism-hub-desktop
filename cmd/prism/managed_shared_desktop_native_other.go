//go:build !darwin || !cgo

package main

func desktopHostNativeProcessPath(int) string { return "" }

func desktopHostNativeValidBundle(string) bool { return false }
