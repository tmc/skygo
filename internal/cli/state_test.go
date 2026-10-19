package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestStateWorkflow runs the shipped executable, a real server, a completed
// helper workload and offline snapshot commands. Restore switches the server
// to a fresh state directory; the original stays stopped throughout.
func TestStateWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the server executable")
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	args := []string{"build", "-o", exe}
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/...")
	}
	build := exec.Command("go", append(args, "../../cmd/skygo")...)
	build.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	var env []string
	if dir := coverDir(t); dir != "" {
		env = []string{"GOCOVERDIR=" + dir}
	}
	data := t.TempDir()
	address := freeAddress(t)
	srv := &serverProcess{exe: exe, address: address, data: data, env: env}
	t.Cleanup(srv.cleanup)
	e := executableScriptEngine(exe, nil, env)
	e.Cmds["server"] = srv.command()
	e.Cmds["taskfile"] = coverageTaskfile()
	e.Cmds["job-id"] = jobID()
	e.Cmds["use-restored"] = script.Command(script.CmdUsage{Summary: "switch stopped server to restored state", Args: "directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		srv.data = filepath.Join(s.Getwd(), args[0])
		return nil, nil
	})
	file := filepath.Join(t.TempDir(), "state.txt")
	if err := os.WriteFile(file, []byte(stateScript), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=http://" + address, "SKYGO_TOKEN=coverage-token", "DATA=" + data}, file)
}

const stateScript = `
server start
taskfile task.json success
skygo --json run -f task.json --request-id original --wait
stdout '"state": *"JOB_STATE_SUCCEEDED"'
job-id ORIGINAL
! skygo state backup --data-dir $DATA --output snapshot.db
stderr 'stop the server first'
! exists snapshot.db
server stop
skygo state backup --data-dir $DATA --output snapshot.db
stdout '^snapshot.db\n$'
! stderr .
! skygo state backup --data-dir $DATA --output snapshot.db
stderr 'publish snapshot'
skygo --json state restore --input snapshot.db --data-dir restored
stdout '^\{"path":"restored"\}\n$'
! stderr .
! skygo state restore --input snapshot.db --data-dir restored
stderr 'create restore directory'
use-restored restored
server start
skygo --json jobs get $ORIGINAL
stdout '"state": *"JOB_STATE_SUCCEEDED"'
skygo --json run -f task.json --request-id original --wait
stdout '"id": *"'$ORIGINAL'"'
stdout '"state": *"JOB_STATE_SUCCEEDED"'
skygo --json jobs list
stdout '"id": *"'$ORIGINAL'"'
server stop
`

func TestStateRequiredFiles(t *testing.T) {
	for _, command := range []string{"backup", "restore"} {
		out, _, err := execute(context.Background(), "state", command)
		if err == nil || out != "" || !strings.Contains(err.Error(), "required flag") {
			t.Fatalf("%s: output %q, error %v", command, out, err)
		}
	}
}
