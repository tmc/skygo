package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2test"
)

// These tests run the command's run function with real files, the real
// ec2ctl caller and an ec2api client whose endpoints are the ec2test TLS
// model. Only the client constructor differs from main's.

var testCreds = aws.Credentials{
	AccessKeyID:     "AKIDFIXTURE00000",
	SecretAccessKey: "fixture/secret/never-printed",
	SessionToken:    "fixture-session-never-printed",
}

const testConfig = `{
	"version": 1,
	"profile": {
		"account": "123456789012",
		"region": "us-west-2",
		"subnet_id": "subnet-0123456789abcdef0",
		"security_group_ids": ["sg-0aaaaaaaaaaaaaaaa"],
		"image_id": "ami-0123456789abcdef0",
		"instance_type": "t3.micro",
		"root_device": "/dev/xvda",
		"root_volume_gib": 16,
		"root_volume_type": "gp3",
		"encrypt_root": true
	},
	"identity": {"owner": "install-7f3a", "environment": "env-0123456789abcdef", "generation": 1},
	"user_data": "#cloud-config\n"
}`

type harness struct {
	t                    *testing.T
	srv                  *ec2test.Server
	config, creds, state string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	h := &harness{
		t:      t,
		srv:    ec2test.NewServer(t, ec2test.Options{Account: "123456789012", Region: "us-west-2", Creds: testCreds}),
		config: filepath.Join(dir, "config.json"),
		creds:  filepath.Join(dir, "credentials.json"),
		state:  filepath.Join(dir, "state"),
	}
	creds, err := json.Marshal(map[string]string{
		"access_key_id":     testCreds.AccessKeyID,
		"secret_access_key": testCreds.SecretAccessKey,
		"session_token":     testCreds.SessionToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.write(h.config, testConfig)
	h.write(h.creds, string(creds))
	return h
}

func (h *harness) write(name, text string) {
	h.t.Helper()
	if err := os.WriteFile(name, []byte(text), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// run runs a command with the harness's files and no observation wait.
func (h *harness) run(cmd string, flags ...string) (code int, stdout, stderr string) {
	args := append([]string{cmd, "-config", h.config, "-state", h.state, "-credentials", h.creds, "-interval", "0"}, flags...)
	return h.runArgs(args...)
}

func (h *harness) runArgs(args ...string) (code int, stdout, stderr string) {
	h.t.Helper()
	var out, errb bytes.Buffer
	code = run(context.Background(), args, env{&out, &errb, h.srv.NewClient})
	h.private(out.String(), errb.String())
	return code, out.String(), errb.String()
}

// private fails if any text holds a credential or provider message.
func (h *harness) private(texts ...string) {
	h.t.Helper()
	for _, s := range texts {
		for _, secret := range []string{testCreds.AccessKeyID, testCreds.SecretAccessKey, testCreds.SessionToken, ec2test.ProviderMessage} {
			if strings.Contains(s, secret) {
				h.t.Fatalf("output contains %q:\n%s", secret, s)
			}
		}
	}
}

func (h *harness) report(flags ...string) report {
	h.t.Helper()
	code, out, errs := h.run("status", append([]string{"-json"}, flags...)...)
	if code != 0 {
		h.t.Fatalf("status exit %d: %s", code, errs)
	}
	var r report
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		h.t.Fatalf("status -json: %v\n%s", err, out)
	}
	return r
}

// field returns the value of key in text status output.
func field(t *testing.T, out, key string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if len(line) > 18 && strings.TrimSpace(line[:17]) == key {
			return line[18:]
		}
	}
	t.Fatalf("no %q in output:\n%s", key, out)
	return ""
}

func TestUsage(t *testing.T) {
	h := newHarness(t)
	common := []string{"-config", h.config, "-state", h.state, "-credentials", h.creds}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"no command", nil, "usage:"},
		{"unknown command", []string{"launch"}, `unknown command "launch"`},
		{"no config", []string{"start", "-state", h.state, "-credentials", h.creds}, "-config is required"},
		{"no state", []string{"start", "-config", h.config, "-credentials", h.creds}, "-state is required"},
		{"no credentials", []string{"stop", "-config", h.config, "-state", h.state}, "-credentials is required"},
		{"refresh without credentials", []string{"status", "-config", h.config, "-state", h.state, "-refresh"}, "-credentials is required"},
		{"refresh with start", append([]string{"start", "-refresh"}, common...), "-refresh is only for status"},
		{"argument", append([]string{"stop"}, append(common, "now")...), `unexpected argument "now"`},
		{"steps", append([]string{"start", "-steps", "0"}, common...), "-steps must be"},
		{"timeout", append([]string{"start", "-timeout", "3h"}, common...), "-timeout must be"},
		{"interval", append([]string{"start", "-interval", "-1s"}, common...), "-interval must be"},
		{"unknown flag", append([]string{"start", "-endpoint", "https://example.com"}, common...), "flag provided but not defined: -endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errs := h.runArgs(tt.args...)
			if code != 2 || out != "" || !strings.Contains(errs, tt.want) || !strings.Contains(errs, "usage:") {
				t.Fatalf("exit %d, stdout %q, stderr:\n%s\nwant exit 2 and %q", code, out, errs, tt.want)
			}
		})
	}
	if code, _, errs := h.runArgs("start", "-h"); code != 0 || !strings.Contains(errs, "usage:") {
		t.Fatalf("-h: exit %d, stderr %q", code, errs)
	}
	if h.srv.Total() != 0 {
		t.Fatal("a usage error called EC2")
	}
	if _, err := os.Lstat(h.state); err == nil {
		t.Fatal("a usage error created the state directory")
	}
}

func TestLifecycleCommands(t *testing.T) {
	h := newHarness(t)

	code, out, errs := h.run("start")
	if code != 0 {
		t.Fatalf("start: exit %d\n%s", code, errs)
	}
	if field(t, out, "phase") != "running" || field(t, out, "goal reached") != "true" || field(t, out, "observed") != "not refreshed; the fields above are stored evidence" {
		t.Fatalf("start output:\n%s", out)
	}
	id := field(t, out, "instance")

	// status reads stored evidence without credentials or calls.
	total := h.srv.Total()
	code, out, errs = h.runArgs("status", "-config", h.config, "-state", h.state)
	if code != 0 || field(t, out, "instance") != id || h.srv.Total() != total {
		t.Fatalf("status: exit %d, %d calls\n%s%s", code, h.srv.Total()-total, out, errs)
	}
	r := h.report()
	if !r.Durable || !r.GoalReached || r.Record.Phase != "running" || r.Record.InstanceID != id || r.Observed.Refreshed || r.Record.ClientToken == "" {
		t.Fatalf("status -json: %+v", r)
	}

	// A change made outside skygo shows only in a refresh.
	h.srv.SetState(id, "stopped")
	r = h.report("-refresh")
	if r.Record.Phase != "running" || !r.Observed.Refreshed || r.Observed.State != "stopped" || r.Observed.InstanceID != id {
		t.Fatalf("status -refresh: %+v %+v", r.Record, r.Observed)
	}
	h.srv.SetState(id, "running")

	for _, step := range []struct{ cmd, phase string }{
		{"stop", "stopped"},
		{"start", "running"},
		{"restart", "running"},
		{"down", "terminated"},
	} {
		code, out, errs := h.run(step.cmd)
		if code != 0 || field(t, out, "phase") != step.phase || field(t, out, "instance") != id {
			t.Fatalf("%s: exit %d\n%s%s", step.cmd, code, out, errs)
		}
	}
	if h.srv.Count("StopInstances") != 2 || h.srv.Count("StartInstances") != 2 || h.srv.Count("TerminateInstances") != 1 {
		t.Fatalf("stops %d, starts %d, terminates %d", h.srv.Count("StopInstances"), h.srv.Count("StartInstances"), h.srv.Count("TerminateInstances"))
	}

	total = h.srv.Total()
	for _, cmd := range []string{"start", "stop", "restart"} {
		if code, _, errs := h.run(cmd); code != 1 || !strings.Contains(errs, "latched") {
			t.Fatalf("%s after down: exit %d, stderr %q", cmd, code, errs)
		}
	}
	if code, _, errs := h.run("down"); code != 0 {
		t.Fatalf("down again: exit %d\n%s", code, errs)
	}
	if h.srv.Total() != total {
		t.Fatal("a latched VM was called again")
	}
	if h.srv.Count("RunInstances") != 1 || len(h.srv.Instances()) != 1 {
		t.Fatalf("%d creates, %d instances", h.srv.Count("RunInstances"), len(h.srv.Instances()))
	}

	data, err := os.ReadFile(filepath.Join(h.state, "vm.json"))
	if err != nil {
		t.Fatal(err)
	}
	h.private(string(data))
}

func TestStartStepBound(t *testing.T) {
	h := newHarness(t)
	code, out, errs := h.run("start", "-steps", "1")
	if code != 3 || !strings.Contains(errs, "did not reach") || field(t, out, "phase") != "creating" || field(t, out, "goal reached") != "false" {
		t.Fatalf("bounded start: exit %d\n%s%s", code, out, errs)
	}
	if code, _, errs := h.run("start"); code != 0 {
		t.Fatalf("start again: exit %d\n%s", code, errs)
	}
	if n := h.srv.Count("RunInstances"); n != 1 {
		t.Fatalf("%d creates", n)
	}
}

func TestHeldExitsThree(t *testing.T) {
	h := newHarness(t)
	h.srv.SetAccount("210987654321")
	code, out, errs := h.run("start")
	if code != 3 || field(t, out, "phase") != "held" || field(t, out, "hold") != "conflict" || !strings.Contains(errs, "different account") {
		t.Fatalf("start with another account: exit %d\n%s%s", code, out, errs)
	}
	total := h.srv.Total()
	if code, _, _ := h.run("start"); code != 3 || h.srv.Total() != total {
		t.Fatalf("held start again: exit %d, %d calls", code, h.srv.Total()-total)
	}
	if n := h.srv.Count("RunInstances"); n != 0 {
		t.Fatalf("%d creates", n)
	}
}

func TestExpiredTimeoutSendsNothing(t *testing.T) {
	h := newHarness(t)
	code, out, errs := h.run("start", "-timeout", "1ns")
	if code != 1 || !strings.Contains(errs, "deadline exceeded") || field(t, out, "create sent") != "no" {
		t.Fatalf("start with an expired deadline: exit %d\n%s%s", code, out, errs)
	}
	if h.srv.Total() != 0 {
		t.Fatal("an expired deadline called EC2")
	}
}

func TestBusyStateRefused(t *testing.T) {
	h := newHarness(t)
	if code, _, errs := h.run("start"); code != 0 {
		t.Fatalf("start: exit %d\n%s", code, errs)
	}
	f, err := os.Open(h.config)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ec2ctl.ParseConfig(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	m, err := ec2ctl.Open(h.state, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	total := h.srv.Total()
	if code, _, errs := h.run("stop"); code != 1 || !strings.Contains(errs, "locked") {
		t.Fatalf("stop while locked: exit %d, stderr %q", code, errs)
	}
	if h.srv.Total() != total {
		t.Fatal("a refused writer called EC2")
	}
}

func TestAuthFailureRedacted(t *testing.T) {
	h := newHarness(t)
	h.srv.AddRule(ec2test.Rule{Action: "GetCallerIdentity", Code: "AuthFailure", Status: 403})
	code, _, errs := h.run("start")
	if code != 1 || !strings.Contains(errs, "unauthorized") {
		t.Fatalf("start: exit %d, stderr %q", code, errs)
	}
	r := h.report()
	if r.Record.CreateSent || r.Record.LastDiagnostic == nil || r.Record.LastDiagnostic.Call != "GetCallerIdentity" || r.Record.LastDiagnostic.Class != "unauthorized" {
		t.Fatalf("stored record: %+v", r.Record)
	}
	if h.srv.Count("RunInstances") != 0 {
		t.Fatal("create sent without an account check")
	}
}

func TestConfigChangeRefused(t *testing.T) {
	h := newHarness(t)
	if code, _, errs := h.run("start"); code != 0 {
		t.Fatalf("start: exit %d\n%s", code, errs)
	}
	h.write(h.config, strings.Replace(testConfig, `"t3.micro"`, `"t3.small"`, 1))
	total := h.srv.Total()
	for _, cmd := range []string{"stop", "status"} {
		if code, _, errs := h.run(cmd); code != 1 || !strings.Contains(errs, "does not match") {
			t.Fatalf("%s with a changed config: exit %d, stderr %q", cmd, code, errs)
		}
	}
	if h.srv.Total() != total {
		t.Fatal("a mismatched config called EC2")
	}
}

func TestNoState(t *testing.T) {
	h := newHarness(t)
	for _, cmd := range []string{"status", "stop", "down"} {
		if code, _, errs := h.run(cmd); code != 1 || !strings.Contains(errs, "no state") {
			t.Fatalf("%s without state: exit %d, stderr %q", cmd, code, errs)
		}
	}
	if h.srv.Total() != 0 {
		t.Fatal("a command without state called EC2")
	}
}
