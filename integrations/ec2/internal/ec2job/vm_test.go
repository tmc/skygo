//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2test"
)

// These tests verify VMs with the real ec2ctl lifecycle state and an
// ec2api client sending signed HTTPS requests to the ec2test model. The
// model is not EC2.

var testCreds = aws.Credentials{
	AccessKeyID:     "AKIDFIXTURE00000",
	SecretAccessKey: "fixture/secret/never-printed",
	SessionToken:    "fixture-session-never-printed",
}

func testVMConfig() *ec2ctl.Config {
	return &ec2ctl.Config{
		Profile: ec2api.Profile{
			Account:          "123456789012",
			Region:           "us-west-2",
			SubnetID:         "subnet-0123456789abcdef0",
			SecurityGroupIDs: []string{"sg-0aaaaaaaaaaaaaaaa"},
			ImageID:          "ami-0123456789abcdef0",
			InstanceType:     "t4g.small",
			RootDevice:       "/dev/xvda",
			RootVolumeGiB:    16,
			RootVolumeType:   "gp3",
			EncryptRoot:      true,
		},
		Identity: ec2api.Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1},
		UserData: []byte("#cloud-config\n"),
	}
}

// A vmEnv is one owned VM, running in the ec2test model, with its real
// lifecycle state directory.
type vmEnv struct {
	t     *testing.T
	srv   *ec2test.Server
	c     *ec2api.Client
	vmCfg *ec2ctl.Config
	state string
	id    string
}

// newVMEnv starts the VM with the lifecycle caller, as skygo-ec2 start
// does. Only that start mutates the model.
func newVMEnv(t *testing.T, dir string) *vmEnv {
	t.Helper()
	srv := ec2test.NewServer(t, ec2test.Options{Account: "123456789012", Region: "us-west-2", Creds: testCreds})
	vmCfg := testVMConfig()
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return testCreds, nil })
	c, err := srv.NewClient(&vmCfg.Profile, creds)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "vm")
	m, err := ec2ctl.Create(state, vmCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(ec2ctl.GoalRunning); err != nil {
		t.Fatal(err)
	}
	st, err := m.Drive(context.Background(), c, ec2ctl.Limits{Steps: 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return &vmEnv{t: t, srv: srv, c: c, vmCfg: vmCfg, state: state, id: st.Record.InstanceID}
}

// mutations returns the number of mutating requests the model received.
func (e *vmEnv) mutations() int {
	n := 0
	for _, a := range []string{"RunInstances", "StartInstances", "StopInstances", "TerminateInstances"} {
		n += e.srv.Count(a)
	}
	return n
}

func TestVerifyVM(t *testing.T) {
	e := newVMEnv(t, t.TempDir())
	cfg := testJobConfig(t, "/d", e.id)
	before := e.mutations()
	vm, err := VerifyVM(context.Background(), e.state, e.vmCfg, e.c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if vm.InstanceID != e.id || vm.Address != cfg.IP || vm.Architecture != "arm64" || vm.ImageID != cfg.ImageID || vm.ClientToken == "" || vm.check() != nil {
		t.Fatalf("VerifyVM = %+v", vm)
	}
	if e.mutations() != before {
		t.Fatal("VerifyVM sent a mutation")
	}
	// The lifecycle lock was released: a lifecycle writer can open it.
	m, err := ec2ctl.Open(e.state, e.vmCfg)
	if err != nil {
		t.Fatalf("lifecycle state still locked: %v", err)
	}
	m.Close()
}

// TestVerifyVMRefusals changes one fact at a time. Each is refused, none
// mutates the VM, and the ones the configuration alone decides make no
// EC2 call at all.
func TestVerifyVMRefusals(t *testing.T) {
	for _, tt := range []struct {
		name     string
		change   func(e *vmEnv, w *wireConfig, vmCfg *ec2ctl.Config)
		noCall   bool
		mismatch bool
	}{
		{name: "address changed", change: func(e *vmEnv, w *wireConfig, _ *ec2ctl.Config) { w.VM.IP = "10.0.0.6" }, mismatch: true},
		{name: "public address absent", change: func(e *vmEnv, w *wireConfig, _ *ec2ctl.Config) { w.VM.Address, w.VM.IP = "public", "203.0.113.7" }},
		{name: "instance changed", change: func(e *vmEnv, w *wireConfig, _ *ec2ctl.Config) { w.VM.InstanceID = "i-000000000000a0002" }, noCall: true, mismatch: true},
		{name: "image not bound", change: func(e *vmEnv, w *wireConfig, _ *ec2ctl.Config) { w.VM.ImageID = "ami-0fedcba9876543210" }, noCall: true, mismatch: true},
		{name: "type not bound", change: func(e *vmEnv, w *wireConfig, _ *ec2ctl.Config) { w.VM.InstanceType = "t4g.large" }, noCall: true, mismatch: true},
		{name: "stopped outside skygo", change: func(e *vmEnv, _ *wireConfig, _ *ec2ctl.Config) { e.srv.SetState(e.id, "stopped") }},
		{name: "tag added", change: func(e *vmEnv, _ *wireConfig, _ *ec2ctl.Config) { e.srv.AddTag(e.id, "intruder", "x") }},
		{name: "owner changed", change: func(e *vmEnv, _ *wireConfig, vmCfg *ec2ctl.Config) { vmCfg.Identity.Generation = 2 }, noCall: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newVMEnv(t, t.TempDir())
			w := testWire("/d", e.id)
			vmCfg := testVMConfig()
			tt.change(e, &w, vmCfg)
			cfg, err := ParseConfig(bytes.NewReader(encodeWire(t, w)))
			if err != nil {
				t.Fatal(err)
			}
			total, mutations := e.srv.Total(), e.mutations()
			_, err = VerifyVM(context.Background(), e.state, vmCfg, e.c, cfg)
			if err == nil {
				t.Fatal("VerifyVM accepted a changed VM")
			}
			if tt.mismatch && !errors.Is(err, ErrMismatch) {
				t.Errorf("err = %v, want ErrMismatch", err)
			}
			if tt.noCall && e.srv.Total() != total {
				t.Errorf("a refusal the configuration decides made %d EC2 calls", e.srv.Total()-total)
			}
			if e.mutations() != mutations {
				t.Error("a refusal mutated the VM")
			}
		})
	}
}

// TestVerifyVMNeverStarts: a VM wanting stopped, or never created, is
// refused without any mutation; job commands never start or create one.
func TestVerifyVMNeverStarts(t *testing.T) {
	e := newVMEnv(t, t.TempDir())
	m, err := ec2ctl.Open(e.state, e.vmCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(ec2ctl.GoalStopped); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Drive(context.Background(), e.c, ec2ctl.Limits{Steps: 20}); err != nil {
		t.Fatal(err)
	}
	m.Close()
	cfg := testJobConfig(t, "/d", e.id)
	mutations := e.mutations()
	if _, err := VerifyVM(context.Background(), e.state, e.vmCfg, e.c, cfg); err == nil {
		t.Fatal("VerifyVM accepted a stopped VM")
	}
	if e.mutations() != mutations {
		t.Fatal("VerifyVM mutated a stopped VM")
	}

	fresh := filepath.Join(t.TempDir(), "vm")
	m, err = ec2ctl.Create(fresh, e.vmCfg)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	total := e.srv.Total()
	if _, err := VerifyVM(context.Background(), fresh, e.vmCfg, e.c, cfg); err == nil {
		t.Fatal("VerifyVM accepted a VM never created")
	}
	if e.srv.Total() != total {
		t.Fatal("VerifyVM called EC2 for a VM never created")
	}
}

// TestVerifyVMBusy: a lifecycle command holding the state lock refuses a
// job command's verification instead of racing it.
func TestVerifyVMBusy(t *testing.T) {
	e := newVMEnv(t, t.TempDir())
	m, err := ec2ctl.Open(e.state, e.vmCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	cfg := testJobConfig(t, "/d", e.id)
	total := e.srv.Total()
	if _, err := VerifyVM(context.Background(), e.state, e.vmCfg, e.c, cfg); !errors.Is(err, ec2ctl.ErrBusy) {
		t.Fatalf("VerifyVM = %v, want ec2ctl.ErrBusy", err)
	}
	if e.srv.Total() != total {
		t.Fatal("a busy state made an EC2 call")
	}
}
