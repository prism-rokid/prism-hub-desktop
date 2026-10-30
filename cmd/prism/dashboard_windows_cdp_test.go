package main

import (
	"reflect"
	"runtime"
	"testing"
)

func TestDashboardWindowsSnapshotExcludesChromiumHelpers(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows process discovery")
	}
	previous := dashboardWindowsProcessSnapshot
	dashboardWindowsProcessSnapshot = func() ([]byte, error) {
		return []byte(`[
   {"ProcessId":10,"Name":"ChatGPT.exe","CommandLine":"ChatGPT.exe --remote-debugging-port=0"},
   {"ProcessId":11,"Name":"ChatGPT.exe","CommandLine":"ChatGPT.exe --type=renderer"},
   {"ProcessId":12,"Name":"ChatGPT.exe","CommandLine":"ChatGPT.exe --type=gpu-process"},
   {"ProcessId":13,"Name":"ChatGPT.exe","CommandLine":"ChatGPT.exe --type=utility"},
   {"ProcessId":14,"Name":"ChatGPT.exe","CommandLine":"ChatGPT.exe --type=crashpad-handler"},
   {"ProcessId":15,"Name":"Other.exe","CommandLine":"Other.exe"},
   {"ProcessId":16,"Name":"Hermes.exe","CommandLine":"Hermes.exe --type=renderer"},
   {"ProcessId":17,"Name":"Hermes.exe","CommandLine":"Hermes.exe"}
  ]`), nil
	}
	t.Cleanup(func() { dashboardWindowsProcessSnapshot = previous })
	for _, tc := range []struct {
		app string
		pid int
	}{{"codex", 10}, {"hermes", 17}} {
		t.Run(tc.app, func(t *testing.T) {
			spec, _ := dashboardDesktopAppSpecByID(tc.app)
			rows, err := dashboardDesktopProcessesWithError(spec)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0]["pid"] != tc.pid {
				t.Fatalf("expected only main process %d, got %v", tc.pid, rows)
			}
		})
	}
}

func TestDashboardWindowsListeningPortsFromJSON(t *testing.T) {
	cases := []struct {
		name, input string
		want        []string
		wantErr     bool
	}{
		{"empty", "", nil, false},
		{"whitespace", " \r\n", nil, false},
		{"single", "11437\r\n", []string{"11437"}, false},
		{"multiple", "[11437,9222]", []string{"11437", "9222"}, false},
		{"empty array", "[]", []string{}, false},
		{"malformed", "not json", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dashboardWindowsListeningPortsFromJSON([]byte(tc.input))
			if (err != nil) != tc.wantErr || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, %v; want %v (error=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
