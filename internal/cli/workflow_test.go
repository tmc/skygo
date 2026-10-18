package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestWorkloadHelper is the executable submitted by the workflow scripts. It
// avoids depending on shell utilities or programs installed on the test host.
func TestWorkloadHelper(t *testing.T) {
	switch os.Getenv("SKYGO_TEST_WORKLOAD") {
	case "success":
		fmt.Println("hello from the workload")
		os.Exit(0)
	case "failure":
		fmt.Println("workload failed")
		os.Exit(7)
	case "hold":
		fmt.Println("workload is running")
		if err := os.WriteFile(os.Getenv("SKYGO_TEST_READY"), nil, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		time.Sleep(time.Minute)
		os.Exit(0)
	}
}

func TestLocalWorkflow(t *testing.T) {
	runLocalWorkflow(t, scriptEngine(t), "testdata/workflow.txt")
}

// TestCLIParity runs source-derived upstream workflow contracts against the
// production CLI executable and the real local control plane. Each script gets
// an independent database and executor; it cannot inherit another script's jobs.
func TestCLIParity(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "skygo")
	build := exec.Command("go", "build", "-o", exe, "../../cmd/skygo")
	build.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	files, err := filepath.Glob("testdata/parity/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("parity scripts: %v (%d files)", err, len(files))
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			runLocalWorkflow(t, executableScriptEngine(exe, nil, nil), file)
		})
	}
}

func runLocalWorkflow(t *testing.T, e *script.Engine, pattern string) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	c, err := control.New(s, []backend.Target{{Name: "local", Kind: "local", Backend: b}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	srv := httptest.NewServer(rpc.NewHandler(c, "workflow-token"))
	t.Cleanup(srv.Close)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	e.Cmds["taskfile"] = script.Command(script.CmdUsage{Summary: "write a task using the Go workload helper", Args: "file mode"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, errors.New("usage: taskfile file mode")
		}
		task := map[string]any{
			"argv": []string{exe, "-test.run=^TestWorkloadHelper$"},
			"env":  map[string]string{"SKYGO_TEST_WORKLOAD": args[1], "SKYGO_TEST_READY": s.Path("ready")},
		}
		data, err := json.Marshal(task)
		if err == nil {
			err = os.WriteFile(s.Path(args[0]), data, 0600)
		}
		return nil, err
	})
	e.Cmds["job-id"] = script.Command(script.CmdUsage{Summary: "save the previous JSON job ID in an environment variable", Args: "name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, errors.New("usage: job-id name")
		}
		var job struct{ ID string }
		if err := json.Unmarshal([]byte(s.Stdout()), &job); err != nil {
			return nil, err
		}
		if job.ID == "" {
			return nil, errors.New("job ID is empty")
		}
		return nil, s.Setenv(args[0], job.ID)
	})
	e.Cmds["page-token"] = script.Command(script.CmdUsage{Summary: "save the previous JSON continuation token", Args: "name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, errors.New("usage: page-token name")
		}
		var page struct {
			Token string `json:"next_page_token"`
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &page); err != nil {
			return nil, err
		}
		if page.Token == "" {
			return nil, errors.New("continuation token is empty")
		}
		return nil, s.Setenv(args[0], page.Token)
	})
	e.Cmds["ready"] = script.Command(script.CmdUsage{Summary: "wait until the workload has started"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, errors.New("usage: ready")
		}
		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			if _, err := os.Stat(s.Path("ready")); err == nil {
				return nil, nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick.C:
			}
		}
	})
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=workflow-token"}, pattern)
}
