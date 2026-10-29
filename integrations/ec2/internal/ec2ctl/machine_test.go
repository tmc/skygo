package ec2ctl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2test"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

// These tests compose the real pieces: a Machine storing to real files
// under t.TempDir with fsync and flock, ec2vm.Step, and an ec2api.Client
// sending signed HTTPS requests to the ec2test model. Faults in the store
// are injected only through store.fault, between real writes, and through
// the parent sync function passed to createMachine and openMachine,
// between a real Mkdir and the real open and lock.

var testCreds = aws.Credentials{
	AccessKeyID:     "AKIDFIXTURE00000",
	SecretAccessKey: "fixture/secret/never-printed",
	SessionToken:    "fixture-session-never-printed",
}

var errFault = errors.New("injected fault")

func testConfig() *Config {
	return &Config{
		Profile: ec2api.Profile{
			Account:          "123456789012",
			Region:           "us-west-2",
			SubnetID:         "subnet-0123456789abcdef0",
			SecurityGroupIDs: []string{"sg-0bbbbbbbbbbbbbbbb", "sg-0aaaaaaaaaaaaaaaa"},
			ImageID:          "ami-0123456789abcdef0",
			InstanceType:     "t3.micro",
			RootDevice:       "/dev/xvda",
			RootVolumeGiB:    16,
			RootVolumeType:   "gp3",
			EncryptRoot:      true,
		},
		Identity: ec2api.Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1},
		UserData: []byte("#cloud-config\n"),
	}
}

var fast = Limits{Steps: 20}

type env struct {
	t   *testing.T
	srv *ec2test.Server
	c   *ec2api.Client
	dir string
	cfg *Config
}

func newEnv(t *testing.T) *env {
	t.Helper()
	srv := ec2test.NewServer(t, ec2test.Options{Account: "123456789012", Region: "us-west-2", Creds: testCreds})
	cfg := testConfig()
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return testCreds, nil })
	c, err := srv.NewClient(&cfg.Profile, creds)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, srv: srv, c: c, dir: filepath.Join(t.TempDir(), "vm"), cfg: cfg}
}

func (e *env) create() *Machine {
	e.t.Helper()
	m, err := Create(e.dir, e.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { m.Close() })
	return m
}

func (e *env) open() *Machine {
	e.t.Helper()
	m, err := Open(e.dir, e.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { m.Close() })
	return m
}

// drive sets goal g and drives m to it.
func (e *env) drive(m *Machine, g Goal) Status {
	e.t.Helper()
	if err := m.SetGoal(g); err != nil {
		e.t.Fatalf("SetGoal(%v): %v", g, err)
	}
	st, err := m.Drive(context.Background(), e.c, fast)
	if err != nil {
		e.t.Fatalf("Drive to goal %v: %v", g, err)
	}
	return st
}

func (e *env) stored() Status {
	e.t.Helper()
	st, err := ReadStatus(e.dir, e.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *env) count(action string, want int) {
	e.t.Helper()
	if n := e.srv.Count(action); n != want {
		e.t.Fatalf("%d %s requests, want %d", n, action, want)
	}
}

// failFrom returns a fault that fails stage from its nth call on.
func failFrom(stage string, n int) func(string) error {
	calls := 0
	return func(s string) error {
		if s != stage {
			return nil
		}
		if calls++; calls >= n {
			return errFault
		}
		return nil
	}
}

// sameRecord reports whether a and b are equal, comparing times by
// instant: a stored time is UTC.
func sameRecord(a, b ec2vm.Record) bool {
	if !a.CreateSentAt.Equal(b.CreateSentAt) {
		return false
	}
	a.CreateSentAt, b.CreateSentAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}

func TestStartRunsOneInstance(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	if e.srv.Total() != 0 {
		t.Fatal("Create called EC2")
	}
	// The record of the new VM is durable before any call.
	if st := e.stored(); st.Record.CreateSent || st.Next != ec2vm.ActionCreate || st.Record.Token != e.cfg.Identity.ClientToken() {
		t.Fatalf("stored before Drive: sent %v, next %v", st.Record.CreateSent, st.Next)
	}

	st := e.drive(m, GoalRunning)
	if !st.Reached() || st.Record.Phase != ec2vm.PhaseRunning || st.Pending {
		t.Fatalf("after start: phase %v, reached %v", st.Record.Phase, st.Reached())
	}
	insts := e.srv.Instances()
	if len(insts) != 1 || insts[0].ID != st.Record.InstanceID || insts[0].ClientToken != e.cfg.Identity.ClientToken() {
		t.Fatalf("instances %+v, record instance %q", insts, st.Record.InstanceID)
	}
	e.count("RunInstances", 1)

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if got := e.open().Status().Record; !sameRecord(got, st.Record) {
		t.Fatalf("reopened record differs:\n got %+v\nwant %+v", got, st.Record)
	}
	for name, want := range map[string]fs.FileMode{".": 0o700, lockName: 0o600, stateName: 0o600} {
		fi, err := os.Lstat(filepath.Join(e.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o077 != 0 || fi.Mode().Perm()&want != want {
			t.Errorf("%s mode %v, want %v", name, fi.Mode().Perm(), want)
		}
	}
	if _, err := os.Lstat(filepath.Join(e.dir, tempName)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left behind: %v", err)
	}
}

// TestCommitFailureSendsNothing fails the save that commits the create as
// sent. RunInstances is never sent, the stored record never claims a
// create, and nothing is sent until the state is stored.
func TestCommitFailureSendsNothing(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	m.s.fault = failFrom("rename", 1)
	_, err := m.Drive(context.Background(), e.c, fast)
	if !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Drive error %v, want ErrUnpersisted", err)
	}
	if n := e.srv.Total(); n != 0 {
		t.Fatalf("%d requests sent after a failed commit", n)
	}
	if st := e.stored(); st.Record.CreateSent {
		t.Fatal("stored record claims a create")
	}
	if st := m.Status(); !st.Pending || st.Record.CreateSent {
		t.Fatalf("in-memory record: pending %v, sent %v", st.Pending, st.Record.CreateSent)
	}
	if _, err := os.Lstat(filepath.Join(e.dir, tempName)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("failed save left its temporary file: %v", err)
	}
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnpersisted) || e.srv.Total() != 0 {
		t.Fatalf("second Drive: %v, %d requests", err, e.srv.Total())
	}

	m.s.fault = nil
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || !st.Reached() {
		t.Fatalf("Drive after the store recovered: %v", err)
	}
	e.count("RunInstances", 1)
}

// TestCreateReplyLost drops the connection after EC2 applied the create.
// The stored record keeps the create as sent; a new process recovers the
// one instance by its ClientToken, following empty search pages, and
// sends no second create.
func TestCreateReplyLost(t *testing.T) {
	e := newEnv(t)
	e.srv.AddRule(ec2test.Rule{Action: "RunInstances", Nth: 1, Drop: true})
	m := e.create()
	_, err := m.Drive(context.Background(), e.c, fast)
	var ae *ec2api.Error
	if !errors.As(err, &ae) || ae.Action != "RunInstances" || !ae.Sent {
		t.Fatalf("Drive error %v, want a sent RunInstances failure", err)
	}
	st := e.stored()
	r := st.Record
	if !r.CreateSent || r.InstanceID != "" || st.Next != ec2vm.ActionRecover || r.LastDiagnostic.Call != "RunInstances" || !r.LastDiagnostic.Sent {
		t.Fatalf("stored after lost reply: %+v, next %v", r, st.Next)
	}
	insts := e.srv.Instances()
	if len(insts) != 1 {
		t.Fatalf("%d instances after one create", len(insts))
	}
	// A fresh owned search sees it; the record does not change.
	obs, err := Observe(context.Background(), e.c, r)
	if err != nil || obs.InstanceID != insts[0].ID {
		t.Fatalf("Observe: %v, %+v", err, obs)
	}
	m.Close()

	e.srv.SetPaging(1, 2)
	m = e.open()
	st, err = m.Drive(context.Background(), e.c, fast)
	if err != nil || !st.Reached() {
		t.Fatalf("Drive after restart: %v", err)
	}
	if st.Record.InstanceID != insts[0].ID || st.Record.Token != e.cfg.Identity.ClientToken() {
		t.Fatalf("recovered %q with token %q", st.Record.InstanceID, st.Record.Token)
	}
	e.count("RunInstances", 1)
	if n := len(e.srv.Instances()); n != 1 {
		t.Fatalf("%d instances", n)
	}
}

// TestCreateResultUnpersisted stores the create as sent, then fails to
// store the RunInstances result. The Machine holds the result and makes
// no call until it is stored.
func TestCreateResultUnpersisted(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	m.s.fault = failFrom("rename", 2) // the commit succeeds; the result does not
	_, err := m.Drive(context.Background(), e.c, fast)
	if !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Drive error %v, want ErrUnpersisted", err)
	}
	st := m.Status()
	if !st.Pending || st.Record.InstanceID == "" {
		t.Fatalf("held result: pending %v, instance %q", st.Pending, st.Record.InstanceID)
	}
	if d := e.stored().Record; !d.CreateSent || d.InstanceID != "" {
		t.Fatalf("stored record: sent %v, instance %q; want the committed create", d.CreateSent, d.InstanceID)
	}
	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("second Drive error %v", err)
	}
	if err := m.SetGoal(GoalStopped); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("SetGoal error %v", err)
	}
	if e.srv.Total() != total {
		t.Fatal("a call was made while the result was not stored")
	}

	m.s.fault = nil
	got, err := m.Drive(context.Background(), e.c, fast)
	if err != nil || !got.Reached() || got.Record.InstanceID != st.Record.InstanceID {
		t.Fatalf("Drive after the store recovered: %v, instance %q", err, got.Record.InstanceID)
	}
	e.count("RunInstances", 1)
}

// TestCreateResultLostInCrash loses the unstored create result to a crash.
// The new process reads the committed create and recovers the same
// instance by ClientToken.
func TestCreateResultLostInCrash(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	m.s.fault = failFrom("rename", 2)
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Drive error %v, want ErrUnpersisted", err)
	}
	id := m.Status().Record.InstanceID
	m.Close() // the process exits with the result unstored

	m = e.open()
	if st := m.Status(); !st.Record.CreateSent || st.Record.InstanceID != "" || st.Next != ec2vm.ActionRecover {
		t.Fatalf("after crash: sent %v, instance %q, next %v", st.Record.CreateSent, st.Record.InstanceID, st.Next)
	}
	st, err := m.Drive(context.Background(), e.c, fast)
	if err != nil || st.Record.InstanceID != id || !st.Reached() {
		t.Fatalf("Drive after crash: %v, instance %q, want %q", err, st.Record.InstanceID, id)
	}
	e.count("RunInstances", 1)
}

// TestDirectorySyncFailureStoresAgain renames the result into place but
// fails the directory sync, so the result may not survive a crash. The
// Machine stores it again before its next call.
func TestDirectorySyncFailureStoresAgain(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	m.s.fault = failFrom("sync directory", 2)
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Drive error %v, want ErrUnpersisted", err)
	}
	if !m.Status().Pending {
		t.Fatal("a result whose directory sync failed is not pending")
	}
	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnpersisted) || e.srv.Total() != total {
		t.Fatalf("second Drive: %v", err)
	}
	m.s.fault = nil
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || !st.Reached() || st.Pending {
		t.Fatalf("Drive after the store recovered: %v", err)
	}
	e.count("RunInstances", 1)
}

func TestAccountMismatchHolds(t *testing.T) {
	e := newEnv(t)
	e.srv.SetAccount("210987654321")
	m := e.create()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ec2api.ErrAccountMismatch) {
		t.Fatalf("Drive error %v, want ErrAccountMismatch", err)
	}
	if r := e.stored().Record; r.Phase != ec2vm.PhaseHeld || r.Hold != ec2vm.HoldConflict {
		t.Fatalf("stored phase %v hold %v", r.Phase, r.Hold)
	}
	e.count("RunInstances", 0)
	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrNotReached) || e.srv.Total() != total {
		t.Fatalf("held Drive: %v, %d new requests", err, e.srv.Total()-total)
	}
}

// TestUnauthorizedSendsNoCreate refuses the account check. The create was
// not sent, so the stored record says so, with a diagnostic that holds no
// provider text, and a later Drive creates once.
func TestUnauthorizedSendsNoCreate(t *testing.T) {
	e := newEnv(t)
	e.srv.AddRule(ec2test.Rule{Action: "GetCallerIdentity", Nth: 1, Code: "AuthFailure", Status: 403})
	m := e.create()
	_, err := m.Drive(context.Background(), e.c, fast)
	var ae *ec2api.Error
	if !errors.As(err, &ae) || ae.Class != ec2api.ClassUnauthorized {
		t.Fatalf("Drive error %v, want unauthorized", err)
	}
	if strings.Contains(err.Error(), ec2test.ProviderMessage) {
		t.Fatalf("error carries the provider message: %v", err)
	}
	r := e.stored().Record
	if r.CreateSent || r.LastDiagnostic.Call != "GetCallerIdentity" || r.LastDiagnostic.Class != ec2api.ClassUnauthorized {
		t.Fatalf("stored record: sent %v, diagnostic %+v", r.CreateSent, r.LastDiagnostic)
	}
	e.count("RunInstances", 0)
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || !st.Reached() {
		t.Fatalf("Drive after authorization: %v", err)
	}
	e.count("RunInstances", 1)
}

func TestConfigMismatch(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"account", func(c *Config) { c.Profile.Account = "210987654321" }},
		{"instance type", func(c *Config) { c.Profile.InstanceType = "t3.small" }},
		{"generation", func(c *Config) { c.Identity.Generation = 2 }},
		{"lease", func(c *Config) { c.Identity.Lease = "lease-1" }},
		{"user data", func(c *Config) { c.UserData = []byte("#cloud-config\nruncmd: []\n") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.create().Close()
			cfg := testConfig()
			tt.change(cfg)
			if _, err := Open(e.dir, cfg); !errors.Is(err, ErrMismatch) {
				t.Fatalf("Open error %v, want ErrMismatch", err)
			}
			if _, err := Create(e.dir, cfg); !errors.Is(err, ErrMismatch) {
				t.Fatalf("Create error %v, want ErrMismatch", err)
			}
			if _, err := ReadStatus(e.dir, cfg); !errors.Is(err, ErrMismatch) {
				t.Fatalf("ReadStatus error %v, want ErrMismatch", err)
			}
		})
	}
}

func TestConcurrentWriterRefused(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	if _, err := Open(e.dir, e.cfg); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Open error %v, want ErrBusy", err)
	}
	if _, err := Create(e.dir, e.cfg); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Create error %v, want ErrBusy", err)
	}
	if _, err := ReadStatus(e.dir, e.cfg); err != nil {
		t.Fatalf("ReadStatus while locked: %v", err)
	}
	m.Close()
	e.open()
}

func TestUnsafeState(t *testing.T) {
	tests := []struct {
		name   string
		reader bool // ReadStatus refuses too
		mutate func(t *testing.T, dir string) string
	}{
		{"directory symlink", true, func(t *testing.T, dir string) string {
			link := dir + "-link"
			must(t, os.Symlink(dir, link))
			return link
		}},
		{"directory mode", true, func(t *testing.T, dir string) string {
			must(t, os.Chmod(dir, 0o755))
			return dir
		}},
		{"state mode", true, func(t *testing.T, dir string) string {
			must(t, os.Chmod(filepath.Join(dir, stateName), 0o644))
			return dir
		}},
		{"state symlink", true, func(t *testing.T, dir string) string {
			other := filepath.Join(dir, "other.json")
			must(t, os.Rename(filepath.Join(dir, stateName), other))
			must(t, os.Symlink(other, filepath.Join(dir, stateName)))
			return dir
		}},
		{"state hard link", true, func(t *testing.T, dir string) string {
			must(t, os.Link(filepath.Join(dir, stateName), filepath.Join(filepath.Dir(dir), "copy.json")))
			return dir
		}},
		{"lock symlink", false, func(t *testing.T, dir string) string {
			target := filepath.Join(filepath.Dir(dir), "elsewhere")
			must(t, os.WriteFile(target, nil, 0o600))
			must(t, os.Remove(filepath.Join(dir, lockName)))
			must(t, os.Symlink(target, filepath.Join(dir, lockName)))
			return dir
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.create().Close()
			dir := tt.mutate(t, e.dir)
			if _, err := Open(dir, e.cfg); err == nil || errors.Is(err, ErrNoState) {
				t.Fatalf("Open error %v, want a refusal", err)
			}
			if _, err := ReadStatus(dir, e.cfg); tt.reader && err == nil {
				t.Fatal("ReadStatus accepted it")
			}
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCorruptState(t *testing.T) {
	tests := []struct {
		name        string
		edit        func([]byte) []byte
		unsupported bool
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)/2] }, false},
		{"trailing data", func(b []byte) []byte { return append(b, "{}\n"...) }, false},
		{"not canonical", func(b []byte) []byte { return append(b, '\n') }, false},
		{"unknown field", replace("{\n", "{\n\t\"extra\": 1,\n"), false},
		{"version 2", replace(`"version": 1,`, `"version": 2,`), true},
		{"other format", replace(`"format": "skygo-ec2-vm-state"`, `"format": "other"`), true},
		{"inconsistent record", replace(`"credits_verified": false`, `"credits_verified": true`), false},
		{"invalid diagnostic", replace(`"reason": ""`, `"reason": "\u0007"`), false},
		{"oversize", func([]byte) []byte { return bytes.Repeat([]byte(" "), maxState+1) }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.create().Close()
			name := filepath.Join(e.dir, stateName)
			data, err := os.ReadFile(name)
			must(t, err)
			bad := tt.edit(bytes.Clone(data))
			if bytes.Equal(bad, data) {
				t.Fatal("test bug: edit changed nothing")
			}
			must(t, os.WriteFile(name, bad, 0o600))
			_, err = Open(e.dir, e.cfg)
			if err == nil || errors.Is(err, ErrMismatch) || errors.Is(err, ErrNoState) {
				t.Fatalf("Open error %v, want corruption", err)
			}
			if errors.Is(err, errUnsupportedState) != tt.unsupported {
				t.Fatalf("Open error %v, unsupported %v", err, tt.unsupported)
			}
			// The refused state is left as it was.
			after, err := os.ReadFile(name)
			must(t, err)
			if !bytes.Equal(after, bad) {
				t.Fatal("refused state was rewritten")
			}
		})
	}
}

func replace(old, new string) func([]byte) []byte {
	return func(b []byte) []byte { return bytes.Replace(b, []byte(old), []byte(new), 1) }
}

func TestStaleTemporaryRemoved(t *testing.T) {
	e := newEnv(t)
	e.create().Close()
	tmp := filepath.Join(e.dir, tempName)
	must(t, os.WriteFile(tmp, []byte("partial"), 0o600))
	e.open()
	if _, err := os.Lstat(tmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale temporary file: %v", err)
	}
}

func TestCanceledBeforeCreate(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Drive(ctx, e.c, fast); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drive error %v, want context.Canceled", err)
	}
	if e.srv.Total() != 0 || e.stored().Record.CreateSent {
		t.Fatalf("%d requests; stored sent %v", e.srv.Total(), e.stored().Record.CreateSent)
	}
}

// TestCancelInFlightCreate cancels while EC2 holds an applied create
// without answering. The record keeps the create as sent and unresolved;
// a later Drive recovers the one instance.
func TestCancelInFlightCreate(t *testing.T) {
	e := newEnv(t)
	hit, done := make(chan struct{}), make(chan struct{})
	e.srv.AddRule(ec2test.Rule{Action: "RunInstances", Nth: 1, Hang: true, Hit: hit, Done: done})
	m := e.create()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-hit
		cancel()
	}()
	_, err := m.Drive(ctx, e.c, fast)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Drive error %v, want context.Canceled", err)
	}
	<-done // the handler that applied the create has returned

	r := e.stored().Record
	if !r.CreateSent || r.InstanceID != "" || r.Phase != ec2vm.PhaseCreating || !r.LastDiagnostic.Sent || r.LastDiagnostic.Call != "RunInstances" {
		t.Fatalf("stored after cancel: %+v", r)
	}
	if len(e.srv.Instances()) != 1 {
		t.Fatal("the hung create did not take effect")
	}
	st, err := m.Drive(context.Background(), e.c, fast)
	if err != nil || !st.Reached() || st.Record.InstanceID != e.srv.Instances()[0].ID {
		t.Fatalf("Drive after cancel: %v", err)
	}
	e.count("RunInstances", 1)
}

func TestThrottledObservationRepeats(t *testing.T) {
	e := newEnv(t)
	// Request 1 verifies credits; request 2 is the first observation.
	e.srv.AddRule(ec2test.Rule{Action: "DescribeInstances", Nth: 2, Code: "RequestLimitExceeded"})
	st := e.drive(e.create(), GoalRunning)
	if st.Record.Phase != ec2vm.PhaseRunning || st.Record.LastDiagnostic != (ec2vm.Diagnostic{}) {
		t.Fatalf("phase %v, diagnostic %+v", st.Record.Phase, st.Record.LastDiagnostic)
	}
	e.count("DescribeInstances", 3)
	e.count("RunInstances", 1)
}

func TestNotOwnedHolds(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	id := e.drive(m, GoalRunning).Record.InstanceID
	e.srv.AddTag(id, "intruder", "1")
	must(t, m.SetGoal(GoalStopped))
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ec2api.ErrNotOwned) {
		t.Fatalf("Drive error %v, want ErrNotOwned", err)
	}
	if r := e.stored().Record; r.Phase != ec2vm.PhaseHeld || r.Hold != ec2vm.HoldConflict {
		t.Fatalf("stored phase %v hold %v", r.Phase, r.Hold)
	}
	e.count("StopInstances", 0)
	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrNotReached) || e.srv.Total() != total {
		t.Fatalf("held Drive: %v", err)
	}
}

func TestLifecycle(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	id := e.drive(m, GoalRunning).Record.InstanceID

	if st := e.drive(m, GoalStopped); st.Record.Phase != ec2vm.PhaseStopped {
		t.Fatalf("after stop: %v", st.Record.Phase)
	}
	e.count("StopInstances", 1)
	if st := e.drive(m, GoalRunning); st.Record.Phase != ec2vm.PhaseRunning {
		t.Fatalf("after start: %v", st.Record.Phase)
	}
	e.count("StartInstances", 1)
	if st := e.drive(m, GoalRestarted); st.Record.Phase != ec2vm.PhaseRunning || st.Restart {
		t.Fatalf("after restart: %v, restart %v", st.Record.Phase, st.Restart)
	}
	e.count("StopInstances", 2)
	e.count("StartInstances", 2)
	st := e.drive(m, GoalAbsent)
	if st.Record.Phase != ec2vm.PhaseTerminated || st.Record.InstanceID != id {
		t.Fatalf("after down: %v, instance %q", st.Record.Phase, st.Record.InstanceID)
	}
	e.count("TerminateInstances", 1)
	if insts := e.srv.Instances(); len(insts) != 1 || insts[0].State != "terminated" {
		t.Fatalf("instances %+v", insts)
	}

	total := e.srv.Total()
	for _, g := range []Goal{GoalRunning, GoalStopped, GoalRestarted} {
		if err := m.SetGoal(g); !errors.Is(err, ErrLatched) {
			t.Fatalf("SetGoal(%v) after down: %v, want ErrLatched", g, err)
		}
	}
	if st := e.drive(m, GoalAbsent); !st.Reached() {
		t.Fatal("down again did not stay reached")
	}
	if e.srv.Total() != total {
		t.Fatal("a latched VM was called again")
	}
	e.count("RunInstances", 1)
}

// TestRestartResumesAfterCrash stores the restart before acting, stops,
// and exits. A new process resumes the stored restart: one stop, one
// start.
func TestRestartResumesAfterCrash(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	e.drive(m, GoalRunning)
	must(t, m.SetGoal(GoalRestarted))
	if st := e.stored(); !st.Restart || st.Record.Desired != ec2vm.DesiredStopped || e.srv.Count("StopInstances") != 0 {
		t.Fatalf("stored restart %v, desired %v", st.Restart, st.Record.Desired)
	}
	if _, err := m.Drive(context.Background(), e.c, Limits{Steps: 1}); !errors.Is(err, ErrNotReached) {
		t.Fatalf("bounded Drive error %v, want ErrNotReached", err)
	}
	e.count("StopInstances", 1)
	m.Close()

	m = e.open()
	must(t, m.SetGoal(GoalRestarted)) // resumes; does not restart again
	st, err := m.Drive(context.Background(), e.c, fast)
	if err != nil || st.Record.Phase != ec2vm.PhaseRunning || st.Restart {
		t.Fatalf("resumed restart: %v, phase %v", err, st.Record.Phase)
	}
	e.count("StopInstances", 1)
	e.count("StartInstances", 1)
}

// TestRestartStartFails fails the start half of a restart. The stored
// record wants present again and is not stopped a second time.
func TestRestartStartFails(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	e.drive(m, GoalRunning)
	e.srv.AddRule(ec2test.Rule{Action: "StartInstances", Nth: 1, Code: "InsufficientInstanceCapacity"})
	must(t, m.SetGoal(GoalRestarted))
	if _, err := m.Drive(context.Background(), e.c, fast); err == nil {
		t.Fatal("failed start reported no error")
	}
	st := e.stored()
	if st.Restart || st.Record.Desired != ec2vm.DesiredPresent || st.Record.Phase != ec2vm.PhaseStopped || st.Record.LastDiagnostic.Call != "StartInstances" {
		t.Fatalf("stored after failed start: restart %v, %+v", st.Restart, st.Record)
	}
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || st.Record.Phase != ec2vm.PhaseRunning {
		t.Fatalf("Drive again: %v", err)
	}
	e.count("StopInstances", 1)
	e.count("StartInstances", 2)
}

// TestStopFailureNotRetried applies a stop and answers a server error.
// Drive does not resend it; the stored record keeps the unknown effect.
func TestStopFailureNotRetried(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	e.drive(m, GoalRunning)
	e.srv.AddRule(ec2test.Rule{Action: "StopInstances", Nth: 1, Apply: true, Code: "InternalError", Status: 500})
	must(t, m.SetGoal(GoalStopped))
	_, err := m.Drive(context.Background(), e.c, fast)
	var ae *ec2api.Error
	if !errors.As(err, &ae) || ae.Class != ec2api.ClassServerError {
		t.Fatalf("Drive error %v", err)
	}
	r := e.stored().Record
	if r.Desired != ec2vm.DesiredStopped || r.Phase != ec2vm.PhaseRunning || !r.LastDiagnostic.Sent || r.LastDiagnostic.Reason != "call failed; effect unknown" {
		t.Fatalf("stored after failed stop: %+v", r)
	}
	e.count("StopInstances", 1)

	// Running the command again is the operator's decision.
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || st.Record.Phase != ec2vm.PhaseStopped {
		t.Fatalf("Drive again: %v", err)
	}
	e.count("StopInstances", 2)
}

func TestTerminateReplyLost(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	e.drive(m, GoalRunning)
	e.srv.AddRule(ec2test.Rule{Action: "TerminateInstances", Nth: 1, Drop: true})
	must(t, m.SetGoal(GoalAbsent))
	if _, err := m.Drive(context.Background(), e.c, fast); err == nil {
		t.Fatal("lost terminate reply reported no error")
	}
	if r := e.stored().Record; r.Desired != ec2vm.DesiredAbsent || r.Phase == ec2vm.PhaseTerminated {
		t.Fatalf("stored after lost terminate: desired %v, phase %v", r.Desired, r.Phase)
	}
	if err := m.SetGoal(GoalRunning); !errors.Is(err, ErrLatched) {
		t.Fatalf("SetGoal after down: %v", err)
	}
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || st.Record.Phase != ec2vm.PhaseTerminated {
		t.Fatalf("Drive again: %v", err)
	}
	e.count("TerminateInstances", 2)
}

// TestDeletedStateNotRecreated deletes the state of a terminated VM.
// Neither Create nor Open starts a new record in the directory.
func TestDeletedStateNotRecreated(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	e.drive(m, GoalRunning)
	e.drive(m, GoalAbsent)
	m.Close()
	must(t, os.Remove(filepath.Join(e.dir, stateName)))
	if _, err := Create(e.dir, e.cfg); !errors.Is(err, ErrNoState) {
		t.Fatalf("Create error %v, want ErrNoState", err)
	}
	if _, err := Open(e.dir, e.cfg); !errors.Is(err, ErrNoState) {
		t.Fatalf("Open error %v, want ErrNoState", err)
	}
	e.count("RunInstances", 1)
}

func TestDownBeforeCreate(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	must(t, m.SetGoal(GoalAbsent))
	st, err := m.Drive(context.Background(), nil, fast)
	if err != nil || !st.Reached() {
		t.Fatalf("Drive: %v", err)
	}
	if e.srv.Total() != 0 {
		t.Fatal("down before create called EC2")
	}
	if err := m.SetGoal(GoalRunning); !errors.Is(err, ErrLatched) {
		t.Fatalf("SetGoal error %v, want ErrLatched", err)
	}
}

func TestStopBeforeCreateRefused(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	if err := m.SetGoal(GoalStopped); err == nil {
		t.Fatal("stop before create accepted")
	}
	if err := m.SetGoal(GoalRestarted); err == nil {
		t.Fatal("restart before create accepted")
	}
	if d := e.stored().Record.Desired; d != ec2vm.DesiredPresent {
		t.Fatalf("stored desired %v", d)
	}
}

// TestObserveDoesNotStore stops the instance outside skygo. The stored
// record still says running, a Drive makes no call, and only Observe
// reports the fresh state, without storing it.
func TestObserveDoesNotStore(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	id := e.drive(m, GoalRunning).Record.InstanceID
	e.srv.SetState(id, "stopped")
	before, err := os.ReadFile(filepath.Join(e.dir, stateName))
	must(t, err)

	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); err != nil || e.srv.Total() != total {
		t.Fatalf("Drive of a running record: %v, %d requests", err, e.srv.Total()-total)
	}
	obs, err := Observe(context.Background(), e.c, e.stored().Record)
	if err != nil || obs.State != "stopped" || obs.InstanceID != id || obs.Call != "DescribeInstances by ID" {
		t.Fatalf("Observe: %v, %+v", err, obs)
	}
	after, err := os.ReadFile(filepath.Join(e.dir, stateName))
	must(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("Observe changed the stored state")
	}
	if p := e.stored().Record.Phase; p != ec2vm.PhaseRunning {
		t.Fatalf("stored phase %v", p)
	}
}

// TestParentSyncedFirst records each sync of the state directory's parent.
// The syncs are real; the recorder only observes. Create syncs the parent
// after making the directory and before storing a state in it or making
// any call, and a reopen syncs it again before taking the lock.
func TestParentSyncedFirst(t *testing.T) {
	e := newEnv(t)
	var syncs []string
	record := func(f *os.File) error {
		if f.Name() != filepath.Dir(e.dir) {
			t.Errorf("synced %s, want the parent %s", f.Name(), filepath.Dir(e.dir))
		}
		if _, err := os.Lstat(e.dir); err != nil {
			t.Errorf("parent synced before the directory was made: %v", err)
		}
		_, err := os.Lstat(filepath.Join(e.dir, stateName))
		syncs = append(syncs, fmt.Sprintf("state %v, %d requests", err == nil, e.srv.Total()))
		return syncDir(f)
	}
	m, err := createMachine(e.dir, e.cfg, record)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"state false, 0 requests"}; !reflect.DeepEqual(syncs, want) {
		t.Fatalf("parent syncs %q, want %q", syncs, want)
	}
	if st := e.stored(); st.Record.CreateSent || st.Next != ec2vm.ActionCreate {
		t.Fatalf("stored after Create: sent %v, next %v", st.Record.CreateSent, st.Next)
	}
	if st, err := m.Drive(context.Background(), e.c, fast); err != nil || !st.Reached() {
		t.Fatalf("Drive: %v", err)
	}
	e.count("RunInstances", 1)
	total := e.srv.Total()
	must(t, m.Close())

	m, err = openMachine(e.dir, e.cfg, record)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	want := []string{"state false, 0 requests", fmt.Sprintf("state true, %d requests", total)}
	if !reflect.DeepEqual(syncs, want) {
		t.Fatalf("parent syncs %q, want %q", syncs, want)
	}
}

// TestParentSyncFailure fails the parent sync, between a real Mkdir and
// the real open, lock and save that would follow it. No Machine is
// returned, nothing is stored or removed and no request is sent. The
// directory left behind holds no state and is refused, not deleted.
func TestParentSyncFailure(t *testing.T) {
	fail := func(*os.File) error { return errFault }
	check := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, errFault) || errors.Is(err, ErrNoState) || !strings.Contains(err.Error(), "sync parent directory") {
			t.Fatalf("error %v, want the parent sync failure", err)
		}
	}

	t.Run("new", func(t *testing.T) {
		e := newEnv(t)
		m, err := createMachine(e.dir, e.cfg, fail)
		check(t, err)
		if m != nil {
			t.Fatal("Create returned a Machine")
		}
		names, err := os.ReadDir(e.dir)
		if err != nil || len(names) != 0 {
			t.Fatalf("directory after the failure: %v, %v; want it empty", names, err)
		}
		if _, err := Create(e.dir, e.cfg); !errors.Is(err, ErrNoState) {
			t.Fatalf("Create again: %v, want ErrNoState", err)
		}
		if _, err := Open(e.dir, e.cfg); !errors.Is(err, ErrNoState) {
			t.Fatalf("Open: %v, want ErrNoState", err)
		}
		if _, err := os.Lstat(filepath.Join(e.dir, stateName)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("state after refusal: %v", err)
		}
		if e.srv.Total() != 0 {
			t.Fatalf("%d requests", e.srv.Total())
		}
	})

	t.Run("existing", func(t *testing.T) {
		e := newEnv(t)
		e.create().Close()
		before, err := os.ReadFile(filepath.Join(e.dir, stateName))
		must(t, err)
		tmp := filepath.Join(e.dir, tempName)
		must(t, os.WriteFile(tmp, []byte("partial"), 0o600))
		_, err = openMachine(e.dir, e.cfg, fail)
		check(t, err)
		_, err = createMachine(e.dir, e.cfg, fail)
		check(t, err)
		// The lock was not taken: the stale temporary file is still there.
		if _, err := os.Lstat(tmp); err != nil {
			t.Fatalf("temporary file: %v", err)
		}
		after, err := os.ReadFile(filepath.Join(e.dir, stateName))
		must(t, err)
		if !bytes.Equal(before, after) {
			t.Fatal("state changed")
		}
		if e.srv.Total() != 0 {
			t.Fatalf("%d requests", e.srv.Total())
		}
	})
}

// TestInvalidConfig passes configurations that ec2api.NewCreateRequest
// refuses. Each is refused with no panic, no directory made, nothing in an
// existing directory removed and no request sent. A valid configuration
// is not changed by use.
func TestInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config) // nil for a nil *Config
	}{
		{"nil", nil},
		{"zero", func(c *Config) { *c = Config{} }},
		{"instance type", func(c *Config) { c.Profile.InstanceType = "t3 micro" }},
		{"generation", func(c *Config) { c.Identity.Generation = 0 }},
		{"lease", func(c *Config) { c.Identity.Lease = "Lease_1" }},
		{"user data", func(c *Config) { c.UserData = bytes.Repeat([]byte("x"), ec2api.MaxUserData+1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bad *Config
			if tt.change != nil {
				bad = testConfig()
				tt.change(bad)
			}
			check := func(op string, err error) {
				t.Helper()
				if err == nil || errors.Is(err, ErrMismatch) || errors.Is(err, ErrNoState) || !strings.Contains(err.Error(), "configuration") {
					t.Fatalf("%s error %v, want a configuration error", op, err)
				}
			}
			e := newEnv(t)
			_, err := Create(e.dir, bad)
			check("Create", err)
			if _, err := os.Lstat(e.dir); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("Create made the directory: %v", err)
			}

			e.create().Close()
			tmp := filepath.Join(e.dir, tempName)
			must(t, os.WriteFile(tmp, []byte("partial"), 0o600))
			_, err = Create(e.dir, bad)
			check("Create existing", err)
			_, err = Open(e.dir, bad)
			check("Open", err)
			_, err = ReadStatus(e.dir, bad)
			check("ReadStatus", err)
			if _, err := os.Lstat(tmp); err != nil {
				t.Fatalf("temporary file: %v", err)
			}
			if e.srv.Total() != 0 {
				t.Fatalf("%d requests", e.srv.Total())
			}
		})
	}

	e := newEnv(t)
	e.drive(e.create(), GoalRunning)
	e.stored()
	if !reflect.DeepEqual(e.cfg, testConfig()) {
		t.Fatalf("configuration changed by use: %+v", e.cfg)
	}
}

// TestUnobservedCreateHeld fails a create that had no effect, so a later
// search by ClientToken completes and finds nothing. The record wanting
// present is held: no further call, no new token and no second create, in
// this process or the next. Down searches again by the stored token, and
// once the instance launched with that token is visible, terminates it.
func TestUnobservedCreateHeld(t *testing.T) {
	e := newEnv(t)
	token := e.cfg.Identity.ClientToken()
	e.srv.AddRule(ec2test.Rule{Action: "RunInstances", Nth: 1, Code: "InternalError", Status: 500})
	m := e.create()
	var ae *ec2api.Error
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.As(err, &ae) || ae.Action != "RunInstances" || !ae.Sent {
		t.Fatalf("Drive error %v, want a sent RunInstances failure", err)
	}
	if len(e.srv.Instances()) != 0 {
		t.Fatal("the failed create took effect")
	}

	total := e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrNotReached) {
		t.Fatalf("Drive error %v, want ErrNotReached", err)
	}
	if n := e.srv.Total() - total; n != 1 {
		t.Fatalf("%d requests, want one search", n)
	}
	held := func(desired ec2vm.Desired, next ec2vm.Action) {
		t.Helper()
		st := e.stored()
		r := st.Record
		if r.Phase != ec2vm.PhaseHeld || r.Hold != ec2vm.HoldNotObserved || r.Desired != desired || st.Next != next ||
			!r.CreateSent || r.InstanceID != "" || r.Token != token || r.LastDiagnostic.Reason != "no instance observed" {
			t.Fatalf("stored: %+v, next %v", r, st.Next)
		}
	}
	held(ec2vm.DesiredPresent, ec2vm.ActionNone)

	total = e.srv.Total()
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrNotReached) {
		t.Fatalf("held Drive error %v", err)
	}
	m.Close()
	m = e.open()
	if err := m.SetGoal(GoalRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrNotReached) {
		t.Fatalf("held Drive after reopen: %v", err)
	}
	if e.srv.Total() != total {
		t.Fatalf("%d requests for a held record", e.srv.Total()-total)
	}
	held(ec2vm.DesiredPresent, ec2vm.ActionNone)
	e.count("RunInstances", 1)

	// Down searches by the stored token, again and again, and sends no
	// create.
	must(t, m.SetGoal(GoalAbsent))
	if _, err := m.Drive(context.Background(), e.c, Limits{Steps: 3}); !errors.Is(err, ErrNotReached) {
		t.Fatalf("down Drive error %v, want ErrNotReached", err)
	}
	if n := e.srv.Total() - total; n != 3 {
		t.Fatalf("%d requests, want three searches", n)
	}
	held(ec2vm.DesiredAbsent, ec2vm.ActionRecover)
	e.count("RunInstances", 1)

	// The model has no delayed visibility, so the test makes an instance
	// with the stored token appear by sending the identical request itself,
	// outside the Machine.
	req, err := ec2api.NewCreateRequest(&e.cfg.Profile, e.cfg.Identity, e.cfg.UserData)
	must(t, err)
	if _, err := e.c.RunInstances(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	st, err := m.Drive(context.Background(), e.c, fast)
	if err != nil || !st.Reached() || st.Record.Phase != ec2vm.PhaseTerminated || st.Record.Token != token {
		t.Fatalf("down Drive: %v, %+v", err, st.Record)
	}
	insts := e.srv.Instances()
	if len(insts) != 1 || insts[0].ClientToken != token || insts[0].State != "terminated" || insts[0].ID != st.Record.InstanceID {
		t.Fatalf("instances %+v", insts)
	}
	e.count("RunInstances", 2) // one from the Machine, one from the test
	e.count("TerminateInstances", 1)
}

func TestDriveLimits(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	for _, lim := range []Limits{{}, {Steps: -1}, {Steps: 1, Interval: -time.Second}} {
		if _, err := m.Drive(context.Background(), e.c, lim); err == nil {
			t.Errorf("Drive accepted %+v", lim)
		}
	}
	if e.srv.Total() != 0 {
		t.Fatal("invalid limits made a call")
	}
}

// TestObserveAddress observes the address facts of the owned instance as
// the ec2test model describes it: a private address and no public one.
// Observe stores nothing and sends no mutation, and Address never puts
// one kind of address in place of the other.
func TestObserveAddress(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	id := e.drive(m, GoalRunning).Record.InstanceID
	before, err := os.ReadFile(filepath.Join(e.dir, stateName))
	must(t, err)
	mutations := e.srv.Count("RunInstances") + e.srv.Count("StartInstances") + e.srv.Count("StopInstances") + e.srv.Count("TerminateInstances")

	obs, err := Observe(context.Background(), e.c, e.stored().Record)
	if err != nil {
		t.Fatal(err)
	}
	if obs.InstanceID != id || obs.PrivateIP != netip.MustParseAddr("10.0.0.5") || obs.PublicIP.IsValid() {
		t.Fatalf("Observe = %+v, want %s at private 10.0.0.5 with no public address", obs, id)
	}
	if a, err := obs.Address(false); err != nil || a != netip.MustParseAddr("10.0.0.5") {
		t.Fatalf("Address(private) = %v, %v", a, err)
	}
	if a, err := obs.Address(true); err == nil || a.IsValid() {
		t.Fatalf("Address(public) = %v, %v; want a refusal, not the private address", a, err)
	}
	after, err := os.ReadFile(filepath.Join(e.dir, stateName))
	must(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("Observe changed the stored state")
	}
	if n := e.srv.Count("RunInstances") + e.srv.Count("StartInstances") + e.srv.Count("StopInstances") + e.srv.Count("TerminateInstances"); n != mutations {
		t.Fatalf("Observe sent %d mutations", n-mutations)
	}
}

func TestObservationAddress(t *testing.T) {
	for _, tt := range []struct {
		name            string
		public, private string
		wantPublic      bool
		wantPrivate     bool
	}{
		{name: "both", public: "203.0.113.7", private: "10.0.0.5", wantPublic: true, wantPrivate: true},
		{name: "private only", private: "10.0.0.5", wantPrivate: true},
		{name: "public in private range", public: "10.1.2.3", private: "10.0.0.5", wantPrivate: true},
		{name: "ipv6", public: "2001:db8::7", private: "fd00::5", wantPublic: true, wantPrivate: true},
		{name: "neither"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			obs := &Observation{InstanceID: "i-0123456789abcdef0"}
			if tt.public != "" {
				obs.PublicIP = netip.MustParseAddr(tt.public)
			}
			if tt.private != "" {
				obs.PrivateIP = netip.MustParseAddr(tt.private)
			}
			if a, err := obs.Address(true); (err == nil) != tt.wantPublic || err == nil && a != obs.PublicIP {
				t.Errorf("Address(public) = %v, %v", a, err)
			}
			if a, err := obs.Address(false); (err == nil) != tt.wantPrivate || err == nil && a != obs.PrivateIP {
				t.Errorf("Address(private) = %v, %v", a, err)
			}
		})
	}
	var nilObs *Observation
	if _, err := nilObs.Address(false); err == nil {
		t.Error("nil Observation returned an address")
	}
}

func TestParseAddress(t *testing.T) {
	for _, tt := range []struct {
		in string
		ok bool
	}{
		{"", true},
		{"10.0.0.5", true},
		{"203.0.113.7", true},
		{"2001:db8::1", true},
		{"fd00::5", true},
		{"2001:DB8::1", false},     // not canonical
		{"010.0.0.5", false},       // not canonical
		{"::ffff:10.0.0.5", false}, // IPv4-mapped
		{"fe80::1%eth0", false},    // zone
		{"127.0.0.1", false},       // loopback
		{"169.254.1.1", false},     // link-local
		{"0.0.0.0", false},         // unspecified
		{"224.0.0.1", false},       // multicast
		{"999.0.0.1", false},
		{"10.0.0.5:22", false},
	} {
		a, err := parseAddress("private", tt.in)
		if (err == nil) != tt.ok {
			t.Errorf("parseAddress(%q) = %v, %v; want ok=%v", tt.in, a, err, tt.ok)
			continue
		}
		if err == nil && tt.in != "" && a.String() != tt.in {
			t.Errorf("parseAddress(%q) = %v", tt.in, a)
		}
		if err != nil && strings.Contains(err.Error(), tt.in) && tt.in != "" {
			t.Errorf("parseAddress(%q) error quotes the reported text: %v", tt.in, err)
		}
	}
}
