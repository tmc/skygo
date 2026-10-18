package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	e := scriptEngine(t)
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
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=workflow-token"}, "testdata/workflow.txt")
}
