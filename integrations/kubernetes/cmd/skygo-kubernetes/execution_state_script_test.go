//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/server"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestExecutionStateScript runs testdata/execution_state.txt. Its
// skygo-kubernetes command runs the production command in process, under the
// script's context, with a server callback that no case may reach: a call
// prints "server run called", fails the command and fails this test.
func TestExecutionStateScript(t *testing.T) {
	var calls atomic.Int32
	t.Cleanup(func() {
		if n := calls.Load(); n != 0 {
			t.Errorf("server run called %d times", n)
		}
	})
	// The script runs in a parallel subtest after this function returns,
	// so the context is canceled by Cleanup, not by defer.
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	t.Cleanup(cancel)

	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo-kubernetes"] = script.Command(script.CmdUsage{Summary: "run the skygo-kubernetes command in process", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var out, diag bytes.Buffer
		cmd := command(func(context.Context, server.Config) error {
			calls.Add(1)
			fmt.Fprintln(&out, "server run called")
			return errors.New("server run called")
		})
		cmd.SetArgs(args)
		cmd.SetIn(strings.NewReader(""))
		cmd.SetOut(&out)
		cmd.SetErr(&diag)
		err := cmd.ExecuteContext(s.Context())
		if err != nil {
			fmt.Fprintln(&diag, err)
		}
		return func(*script.State) (string, string, error) {
			return out.String(), diag.String(), err
		}, nil
	})
	scripttest.Test(t, ctx, e, nil, "testdata/execution_state.txt")
}
