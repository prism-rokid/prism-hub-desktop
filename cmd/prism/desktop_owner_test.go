package main

import "testing"

func TestDesktopManagedOwnerPID(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
	}{
		{value: "", want: 0},
		{value: "bad", want: 0},
		{value: "0", want: 0},
		{value: " 123 ", want: 123},
	} {
		t.Setenv(desktopOwnerPIDEnv, test.value)
		if got := desktopManagedOwnerPID(); got != test.want {
			t.Fatalf("desktopManagedOwnerPID(%q) = %d, want %d", test.value, got, test.want)
		}
	}
}
