//go:build windows

package agentlocation

import (
	"context"
	"strings"
	"testing"
)

func TestBatchLauncherQuoting(t *testing.T) {
	cmd, err := Command(context.Background(), `D:\工具 Apps\openclaw.cmd`, "gateway", "start")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.SysProcAttr == nil || !strings.Contains(cmd.SysProcAttr.CmdLine, `/d /v:off /s /c ""D:\工具 Apps\openclaw.cmd" "gateway" "start""`) {
		t.Fatalf("incorrect batch command: %+v", cmd)
	}
	for _, arg := range []string{"%PATH%", "!EXPAND!", "quoted\"value", "one\ntwo"} {
		if _, err := Command(context.Background(), `D:\Tools\openclaw.cmd`, arg); err == nil {
			t.Fatalf("accepted expansion: %q", arg)
		}
	}
}
