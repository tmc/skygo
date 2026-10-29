//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"golang.org/x/tools/txtar"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// Actual runJob and private LoadConfig reject malformed prebuilt selection
// before constructing an EC2 client, opening SSH, or reading task/workdir/state.
func TestPrebuiltConfigScripts(t *testing.T) {
	dir := t.TempDir()
	vm := filepath.Join(dir, "vm.json")
	cfg := filepath.Join(dir, "job.json")
	if err := os.WriteFile(vm, []byte(testConfig), 0600); err != nil {
		t.Fatal(err)
	}
	provider, opens := 0, 0
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["config"] = script.Command(script.CmdUsage{Summary: "write a private job config with these agent fields", Args: "agent-json"}, func(_ *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		var agent map[string]any
		if err := json.Unmarshal([]byte(args[0]), &agent); err != nil {
			return nil, err
		}
		agent["identity_dir"], agent["cache_dir"], agent["target"] = "/unused/identity", "/unused/cache", "ec2vm"
		in := map[string]any{"version": 1, "vm": map[string]any{"instance_id": "i-000000000000a0001", "image_id": "ami-0123456789abcdef0", "instance_type": "t4g.small", "architecture": "arm64", "address": "private", "ip": "10.0.0.5", "cpu_millis": 2000, "memory_bytes": 2147483648, "safety_deadline": "2099-01-01T00:00:00Z", "cleanup_reserve_seconds": 600}, "ssh": map[string]any{"port": 22, "user": "ec2-user", "known_hosts_file": "/unused/known_hosts", "key_file": "/unused/key"}, "agent": agent, "installation": map[string]any{"data_dir": "/unused/data", "auth_file": "/unused/auth.json", "artifact_dir": "/unused/artifacts", "scoped_token_file": "/unused/token"}, "task": map[string]any{"shell": "/bin/sh", "max_timeout_seconds": 60}}
		data, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(cfg, data, 0600)
	})
	e.Cmds["skygo-ec2"] = script.Command(script.CmdUsage{Summary: "run the actual job CLI with effect counters", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var stdout, stderr bytes.Buffer
		env := jobEnv{stdout: &stdout, stderr: &stderr, loadConfig: ec2job.LoadConfig, newClient: func(*ec2api.Profile, aws.CredentialsProvider) (*ec2api.Client, error) {
			provider++
			return nil, errors.New("unexpected EC2 client")
		}, open: func(context.Context, sshagent.Config) (session, error) {
			opens++
			return nil, errors.New("unexpected SSH")
		}, settled: func(error) bool { return true }}
		full := append([]string{"-config", vm, "-job-config", cfg, "-credentials", filepath.Join(dir, "absent-credentials"), "-state", filepath.Join(dir, "state")}, args...)
		c := runJob(s.Context(), "run", full, env)
		if c.code != 1 {
			t.Errorf("CLI exit status %d, want 1", c.code)
		}
		return func(*script.State) (string, string, error) {
			if c.code != 1 {
				return stdout.String(), stderr.String(), fmt.Errorf("exit status %d, want 1", c.code)
			}
			return stdout.String(), stderr.String(), fmt.Errorf("exit status %d", c.code)
		}, nil
	})
	e.Cmds["effects"] = script.Command(script.CmdUsage{Summary: "assert no provider/session/state/snapshot effects"}, func(_ *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		if provider != 0 || opens != 0 {
			return nil, fmt.Errorf("EC2 factories/SSH opens=%d/%d", provider, opens)
		}
		for _, p := range []string{"job", "state"} {
			if _, err := os.Lstat(filepath.Join(dir, p)); !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("unexpected %s: %v", p, err)
			}
		}
		return nil, nil
	})
	file := "testdata/config/prebuilt.txt"
	a, err := txtar.ParseFile(file)
	if err != nil {
		t.Fatal(err)
	}
	s, err := script.NewState(context.Background(), dir, []string{"WORK=" + dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ExtractFiles(a); err != nil {
		s.CloseAndWait(io.Discard)
		t.Fatal(err)
	}
	scripttest.Run(t, e, s, file, bytes.NewReader(a.Comment))
}
