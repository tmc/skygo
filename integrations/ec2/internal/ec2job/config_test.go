package ec2job

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
)

// testInstanceID is the first instance the ec2test model launches.
const testInstanceID = "i-000000000000a0001"

// testDeadline is far enough ahead for every fixture task.
var testDeadline = time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)

// testWire returns a valid configuration whose local paths are below
// dir, for instance id at private address 10.0.0.5, as the ec2test model
// describes it.
func testWire(dir, id string) wireConfig {
	var w wireConfig
	w.Version = 1
	w.VM.InstanceID = id
	w.VM.ImageID = "ami-0123456789abcdef0"
	w.VM.InstanceType = "t4g.small"
	w.VM.Architecture = "arm64"
	w.VM.Address = "private"
	w.VM.IP = "10.0.0.5"
	w.VM.CPUMillis = 2000
	w.VM.MemoryBytes = 2 << 30
	w.VM.SafetyDeadline = testDeadline.Format(time.RFC3339)
	w.VM.CleanupReserveSeconds = 600
	w.SSH.Port = 22
	w.SSH.User = "ec2-user"
	w.SSH.KnownHostsFile = dir + "/known_hosts"
	w.SSH.KeyFile = dir + "/id_ed25519"
	w.Agent.Source = dir + "/src"
	w.Agent.IdentityDir = dir + "/identity"
	w.Agent.CacheDir = dir + "/cache"
	w.Agent.Target = "ec2vm"
	w.Installation.DataDir = dir + "/installation/data"
	w.Installation.AuthFile = dir + "/installation/auth.json"
	w.Installation.ArtifactDir = dir + "/installation/artifacts"
	w.Installation.ScopedTokenFile = dir + "/bob.token"
	w.Task.Shell = "/bin/sh"
	w.Task.Infra = "aws/us-west-2"
	w.Task.MaxTimeoutSeconds = 3600
	return w
}

func encodeWire(t *testing.T, w wireConfig) []byte {
	t.Helper()
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func testJobConfig(t *testing.T, dir, id string) *Config {
	t.Helper()
	c, err := ParseConfig(bytes.NewReader(encodeWire(t, testWire(dir, id))))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// testVM is the VM the ec2test model describes for the ec2ctl fixture
// configuration in vm_test.go, as VerifyVM reports it.
func testVM(c *Config) VM {
	return VM{
		Account:      "123456789012",
		Region:       "us-west-2",
		ProfileHash:  strings.Repeat("a", 64),
		Owner:        "install-7f3a",
		Environment:  "env-0123456789abcdef",
		Generation:   1,
		ClientToken:  "token",
		InstanceID:   c.InstanceID,
		ImageID:      c.ImageID,
		InstanceType: c.InstanceType,
		Architecture: c.Architecture,
		Public:       c.Public,
		Address:      c.IP,
	}
}

func TestParseConfig(t *testing.T) {
	const dir = "/home/u/ec2"
	for _, tt := range []struct {
		name   string
		mutate func(*wireConfig)
		ok     bool
	}{
		{"valid", func(*wireConfig) {}, true},
		{"agent socket", func(w *wireConfig) {
			w.SSH.KeyFile = ""
			w.SSH.AgentSocket = "/run/agent.sock"
			w.SSH.Fingerprint = "SHA256:" + strings.Repeat("A", 43)
		}, true},
		{"public", func(w *wireConfig) { w.VM.Address, w.VM.IP = "public", "203.0.113.7" }, true},
		{"version", func(w *wireConfig) { w.Version = 2 }, false},
		{"amd64", func(w *wireConfig) { w.VM.Architecture = "amd64" }, false},
		{"no architecture", func(w *wireConfig) { w.VM.Architecture = "" }, false},
		{"address kind", func(w *wireConfig) { w.VM.Address = "elastic" }, false},
		{"public in private range", func(w *wireConfig) { w.VM.Address = "public" }, false},
		{"noncanonical ip", func(w *wireConfig) { w.VM.IP = "010.0.0.5" }, false},
		{"loopback ip", func(w *wireConfig) { w.VM.IP = "127.0.0.1" }, false},
		{"instance", func(w *wireConfig) { w.VM.InstanceID = "i-xyz" }, false},
		{"image", func(w *wireConfig) { w.VM.ImageID = "ami-XYZ" }, false},
		{"instance type", func(w *wireConfig) { w.VM.InstanceType = "t4g" }, false},
		{"no capacity", func(w *wireConfig) { w.VM.CPUMillis = 0 }, false},
		{"deadline zone", func(w *wireConfig) {
			w.VM.SafetyDeadline = testDeadline.In(time.FixedZone("x", 3600)).Format(time.RFC3339)
		}, false},
		{"deadline fraction", func(w *wireConfig) { w.VM.SafetyDeadline = testDeadline.Format(time.RFC3339Nano)[:19] + ".5Z" }, false},
		{"reserve", func(w *wireConfig) { w.VM.CleanupReserveSeconds = 59 }, false},
		{"port", func(w *wireConfig) { w.SSH.Port = 0 }, false},
		{"user", func(w *wireConfig) { w.SSH.User = "" }, false},
		{"relative path", func(w *wireConfig) { w.SSH.KnownHostsFile = "known_hosts" }, false},
		{"unclean path", func(w *wireConfig) { w.Agent.Source = dir + "/src/" }, false},
		{"target", func(w *wireConfig) { w.Agent.Target = "EC2" }, false},
		{"no installation", func(w *wireConfig) {
			w.Installation.DataDir, w.Installation.AuthFile, w.Installation.ArtifactDir = "", "", ""
		}, false},
		{"no scoped token", func(w *wireConfig) { w.Installation.ScopedTokenFile = "" }, false},
		{"relative shell", func(w *wireConfig) { w.Task.Shell = "sh" }, false},
		{"infra space", func(w *wireConfig) { w.Task.Infra = " aws" }, false},
		{"no max timeout", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = 0 }, false},
		{"huge max timeout", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = 8 * 24 * 3600 }, false},
		{"max timeout at the bound", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = 7 * 24 * 3600 }, true},
		{"max timeout one past the bound", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = 7*24*3600 + 1 }, false},
		{"max timeout max int64", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = math.MaxInt64 }, false},
		// -18446744072 seconds times 1e9 wraps, modulo 2^64, to
		// 1709551616 nanoseconds: a plausible 1.7 second maximum.
		{"max timeout negative overflow", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = -18446744072 }, false},
		{"max timeout negative", func(w *wireConfig) { w.Task.MaxTimeoutSeconds = -1 }, false},
		{"reserve at the bound", func(w *wireConfig) { w.VM.CleanupReserveSeconds = 7 * 24 * 3600 }, true},
		{"reserve one past the bound", func(w *wireConfig) { w.VM.CleanupReserveSeconds = 7*24*3600 + 1 }, false},
		{"reserve max int64", func(w *wireConfig) { w.VM.CleanupReserveSeconds = math.MaxInt64 }, false},
		// -18446744013 seconds times 1e9 wraps, modulo 2^64, to
		// 60709551616 nanoseconds: past the 60 second minimum reserve.
		{"reserve negative overflow", func(w *wireConfig) { w.VM.CleanupReserveSeconds = -18446744013 }, false},
		{"reserve negative", func(w *wireConfig) { w.VM.CleanupReserveSeconds = -1 }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := testWire(dir, testInstanceID)
			tt.mutate(&w)
			data := encodeWire(t, w)
			c, err := ParseConfig(bytes.NewReader(data))
			if (err == nil) != tt.ok {
				t.Fatalf("ParseConfig = %v, want ok=%v", err, tt.ok)
			}
			if err != nil {
				for _, secret := range []string{dir, w.VM.IP, w.VM.InstanceID, w.Task.Infra} {
					if secret != "" && strings.Contains(err.Error(), secret) {
						t.Errorf("error quotes the configuration: %v", err)
					}
				}
				return
			}
			if c.Architecture != Architecture || len(c.Digest) != 64 {
				t.Fatalf("config %+v", c)
			}
		})
	}
}

func TestParseConfigStrict(t *testing.T) {
	data := encodeWire(t, testWire("/d", testInstanceID))
	for name, in := range map[string][]byte{
		"unknown field": append(append([]byte{}, data[:len(data)-1]...), []byte(`,"token":"x"}`)...),
		"trailing data": append(append([]byte{}, data...), []byte(` {}`)...),
		"too large":     append(append([]byte{}, data...), bytes.Repeat([]byte(" "), maxConfig)...),
		"not json":      []byte("version: 1"),
	} {
		if _, err := ParseConfig(bytes.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	a, err := ParseConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseConfig(bytes.NewReader(append(append([]byte{}, data...), '\n')))
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest == b.Digest {
		t.Fatal("the digest does not bind the exact configuration bytes")
	}
}

func TestSession(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	for _, tt := range []struct{ addr, want string }{
		{"10.0.0.5", "10.0.0.5:22"},
		{"2001:db8::7", "[2001:db8::7]:22"},
	} {
		s := c.Session(netip.MustParseAddr(tt.addr), nil)
		if s.Address != tt.want || s.KnownHostsFile != "/d/known_hosts" || s.Target != "ec2vm" || s.Installation != c.Installation || s.ScopedTokenFile != "/d/bob.token" {
			t.Errorf("Session(%s) = %+v", tt.addr, s)
		}
	}
}

const (
	testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testSize   = 10240
)

func TestTask(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	now := time.Now()
	const run = "workdir: .\nrun: echo hi\n"
	for _, tt := range []struct {
		name string
		in   TaskInput
		ok   bool
	}{
		{"json", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"setup":["/bin/sh","-c","make"],"outputs":["out"],"timeoutSeconds":"60"}`)}, true},
		{"json target", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"target":"ec2vm","timeoutSeconds":60}`)}, true},
		{"json cpu within capacity", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"cpuMillis":"2000","timeoutSeconds":60}`)}, true},
		{"json other target", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"target":"other","timeoutSeconds":60}`)}, false},
		{"json workdir", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"workdir":{"id":"` + placeholderID + `","sha256":"` + testDigest + `","size":"1"},"timeoutSeconds":60}`)}, false},
		{"json unknown field", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"command":"x","timeoutSeconds":60}`)}, false},
		{"json no timeout", TaskInput{JSON: []byte(`{"argv":["/bin/true"]}`)}, false},
		{"json timeout over max", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"timeoutSeconds":7200}`)}, false},
		{"json accelerator", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"accelerator":"nvidia-h100","acceleratorCount":1,"timeoutSeconds":60}`)}, false},
		{"json image", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"image":"busybox","timeoutSeconds":60}`)}, false},
		{"json cpu over capacity", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"cpuMillis":"4000","timeoutSeconds":60}`)}, false},
		{"json memory over capacity", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"memoryBytes":"4294967296","timeoutSeconds":60}`)}, false},
		{"json limits", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"cpuLimitMillis":"100","timeoutSeconds":60}`)}, false},
		{"json selector", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"nodeSelector":{"a":"b"},"timeoutSeconds":60}`)}, false},
		{"json retry", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"retry":{"maxAttempts":2},"timeoutSeconds":60}`)}, false},
		{"json data mount", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"dataMounts":[{"reference":"r","destination":"d"}],"timeoutSeconds":60}`)}, false},
		{"json directory", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"directory":"/tmp","timeoutSeconds":60}`)}, false},
		{"json empty argv", TaskInput{JSON: []byte(`{"timeoutSeconds":60}`)}, false},
		{"json with yaml outputs", TaskInput{JSON: []byte(`{"argv":["/bin/true"],"timeoutSeconds":60}`), Outputs: []string{"out"}}, false},
		{"neither", TaskInput{}, false},
		{"yaml", TaskInput{YAML: []byte(run + "setup: make\n"), Outputs: []string{"out"}, TimeoutSeconds: 60}, true},
		{"yaml infra binding", TaskInput{YAML: []byte(run + "resources:\n  infra: aws/us-west-2\n  cpus: 2\n"), TimeoutSeconds: 60}, true},
		{"yaml other infra", TaskInput{YAML: []byte(run + "resources:\n  infra: gcp\n"), TimeoutSeconds: 60}, false},
		{"yaml cloud", TaskInput{YAML: []byte(run + "resources:\n  cloud: aws\n"), TimeoutSeconds: 60}, false},
		{"yaml instance type", TaskInput{YAML: []byte(run + "resources:\n  instance_type: t4g.small\n"), TimeoutSeconds: 60}, false},
		{"yaml gpu", TaskInput{YAML: []byte(run + "resources:\n  accelerators: H100:1\n"), TimeoutSeconds: 60}, false},
		{"yaml cpus over capacity", TaskInput{YAML: []byte(run + "resources:\n  cpus: 4\n"), TimeoutSeconds: 60}, false},
		{"yaml nodes", TaskInput{YAML: []byte(run + "num_nodes: 2\n"), TimeoutSeconds: 60}, false},
		{"yaml spot", TaskInput{YAML: []byte(run + "resources:\n  use_spot: true\n"), TimeoutSeconds: 60}, false},
		{"yaml autostop", TaskInput{YAML: []byte(run + "resources:\n  autostop: 10m\n"), TimeoutSeconds: 60}, false},
		{"yaml no workdir", TaskInput{YAML: []byte("run: echo hi\n"), TimeoutSeconds: 60}, false},
		{"yaml no timeout", TaskInput{YAML: []byte(run)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task, err := c.Task(tt.in, testDigest, testSize, now)
			if (err == nil) != tt.ok {
				t.Fatalf("Task = %v, want ok=%v", err, tt.ok)
			}
			if err != nil {
				return
			}
			if task.Workdir != nil || task.Target != "ec2vm" || task.TimeoutSeconds != 60 {
				t.Fatalf("task %v", task)
			}
		})
	}
}

// TestTaskYAMLLowered checks that a YAML task is the existing lowerer's
// output: run and setup through the configured shell, byte exact.
func TestTaskYAMLLowered(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	task, err := c.Task(TaskInput{YAML: []byte("workdir: .\nsetup: |\n  make  all\nrun: ./a 'b c'\nenvs:\n  K: v\n"), Outputs: []string{"out"}, TimeoutSeconds: 60}, testDigest, testSize, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	want := &v1.Task{Argv: []string{"/bin/sh", "-c", "./a 'b c'"}, Setup: []string{"/bin/sh", "-c", "make  all\n"}, Env: map[string]string{"K": "v", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_NODES": "1", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}, Outputs: []string{"out"}, Target: "ec2vm", TimeoutSeconds: 60}
	if !sameTask(task, want) {
		t.Fatalf("lowered %v, want %v", task, want)
	}
	_, err = c.Task(TaskInput{YAML: []byte("workdir: .\nrun: x\nresources:\n  cloud: aws\n"), TimeoutSeconds: 60}, testDigest, testSize, time.Now())
	var ye *taskyaml.Error
	if !errors.As(err, &ye) || ye.Path != "resources.cloud" {
		t.Fatalf("cloud refusal = %v, want a taskyaml.Error at resources.cloud", err)
	}
}

func TestCheckTimeout(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	limit := c.SafetyDeadline.Add(-c.CleanupReserve)
	for _, tt := range []struct {
		name    string
		now     time.Time
		seconds int64
		ok      bool
	}{
		{"fits", limit.Add(-time.Hour), 3600, true},
		{"ends at the limit", limit.Add(-time.Minute), 60, true},
		{"ends past the limit", limit.Add(-time.Minute), 61, false},
		{"after the deadline", c.SafetyDeadline, 1, false},
		{"zero", limit.Add(-time.Hour), 0, false},
		{"negative", limit.Add(-time.Hour), -1, false},
		{"at the maximum", limit.Add(-48 * time.Hour), 3600, true},
		{"over the maximum", limit.Add(-48 * time.Hour), 3601, false},
		// MaxInt64 seconds times 1e9 wraps, modulo 2^64, to -1e9
		// nanoseconds, which would end before any deadline.
		{"max int64", limit.Add(-48 * time.Hour), math.MaxInt64, false},
		{"first count that overflows", limit.Add(-48 * time.Hour), math.MaxInt64/int64(time.Second) + 1, false},
		{"min int64", limit.Add(-48 * time.Hour), math.MinInt64, false},
	} {
		if err := c.checkTimeout(tt.now, tt.seconds); (err == nil) != tt.ok {
			t.Errorf("%s: checkTimeout = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

// TestCheckTimeoutDeadline: a timeout ending exactly at the safety
// deadline less the reserve fits; one second more does not, even with a
// maximum that allows it.
func TestCheckTimeoutDeadline(t *testing.T) {
	w := testWire("/d", testInstanceID)
	w.Task.MaxTimeoutSeconds = 7 * 24 * 3600
	c, err := ParseConfig(bytes.NewReader(encodeWire(t, w)))
	if err != nil {
		t.Fatal(err)
	}
	limit := c.SafetyDeadline.Add(-c.CleanupReserve)
	now := limit.Add(-time.Hour)
	if err := c.checkTimeout(now, 3600); err != nil {
		t.Errorf("ending at the limit: %v", err)
	}
	if err := c.checkTimeout(now, 3601); err == nil {
		t.Error("ending a second past the limit was accepted")
	}
	if err := c.checkTimeout(limit, 1); err == nil {
		t.Error("starting at the limit was accepted")
	}
}

// TestTaskTimeoutOverflow: Config.Task, which runs before any EC2 call,
// refuses a JSON timeout whose Duration would wrap, and accepts the
// largest the configuration allows.
func TestTaskTimeoutOverflow(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	now := time.Now()
	for _, tt := range []struct {
		seconds string
		ok      bool
	}{
		{"3600", true},
		{"3601", false},
		{"9223372036854775807", false},
		{"9223372037", false},
		{"-9223372036854775808", false},
	} {
		in := TaskInput{JSON: []byte(`{"argv":["/bin/true"],"timeoutSeconds":"` + tt.seconds + `"}`)}
		if _, err := c.Task(in, testDigest, testSize, now); (err == nil) != tt.ok {
			t.Errorf("timeout %s: Task = %v, want ok=%v", tt.seconds, err, tt.ok)
		}
	}
}

// TestSameTask: the accepted task must equal the submitted one in every
// field. Each one-field change is refused; an identical copy is accepted.
func TestSameTask(t *testing.T) {
	base := &v1.Task{
		Argv: []string{"/bin/sh", "-c", "echo hi"}, Setup: []string{"/bin/true"}, Outputs: []string{"out"},
		Env: map[string]string{"K": "v"}, Target: "ec2vm", TimeoutSeconds: 60, CpuMillis: 1000, MemoryBytes: 1 << 30,
		Workdir: &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: testDigest, Size: testSize},
	}
	if !sameTask(proto.Clone(base).(*v1.Task), base) {
		t.Fatal("an identical task was refused")
	}
	if sameTask(nil, base) {
		t.Fatal("a missing task was accepted")
	}
	for name, change := range map[string]func(*v1.Task){
		"argv":              func(t *v1.Task) { t.Argv[2] = "echo ho" },
		"setup":             func(t *v1.Task) { t.Setup = nil },
		"outputs":           func(t *v1.Task) { t.Outputs = append(t.Outputs, "more") },
		"env":               func(t *v1.Task) { t.Env["K"] = "w" },
		"target":            func(t *v1.Task) { t.Target = "other" },
		"timeout":           func(t *v1.Task) { t.TimeoutSeconds++ },
		"cpu":               func(t *v1.Task) { t.CpuMillis++ },
		"memory":            func(t *v1.Task) { t.MemoryBytes++ },
		"workdir":           func(t *v1.Task) { t.Workdir.Id = strings.Repeat("b", 32) },
		"directory":         func(t *v1.Task) { t.Directory = "/tmp" },
		"image":             func(t *v1.Task) { t.Image = "busybox" },
		"accelerator":       func(t *v1.Task) { t.Accelerator = "H100" },
		"accelerator count": func(t *v1.Task) { t.AcceleratorCount = 1 },
		"cpu limit":         func(t *v1.Task) { t.CpuLimitMillis = 100 },
		"memory limit":      func(t *v1.Task) { t.MemoryLimitBytes = 1 << 20 },
		"node selector":     func(t *v1.Task) { t.NodeSelector = map[string]string{"a": "b"} },
		"target labels":     func(t *v1.Task) { t.TargetLabels = map[string]string{"a": "b"} },
		"retry":             func(t *v1.Task) { t.Retry = &v1.RetryPolicy{MaxAttempts: 2} },
		"data mounts":       func(t *v1.Task) { t.DataMounts = []*v1.DataMount{{}} },
		"unknown field":     func(t *v1.Task) { t.ProtoReflect().SetUnknown([]byte{0xc0, 0x3e, 0x01}) },
	} {
		got := proto.Clone(base).(*v1.Task)
		change(got)
		if sameTask(got, base) {
			t.Errorf("%s: a changed accepted task was taken as the submitted one", name)
		}
	}
}

func prebuiltConfigWire() wireConfig {
	w := testWire("/d", testInstanceID)
	w.Agent.Source = ""
	w.Agent.Prebuilt = json.RawMessage(`{"path":"/missing/skygo-ssh","commit":"1111111111111111111111111111111111111111","sha256":"2222222222222222222222222222222222222222222222222222222222222222"}`)
	return w
}

// Parsing checks only configuration shape. None of these named files exists;
// actual executable/hash verification belongs to sshagent.Open.
func TestPrebuiltConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*wireConfig)
		ok     bool
	}{
		{"valid", func(*wireConfig) {}, true},
		{"source remains supported", func(w *wireConfig) { w.Agent.Prebuilt = nil; w.Agent.Source = "/src"; w.Agent.AllowDirty = true }, true},
		{"missing selection", func(w *wireConfig) { w.Agent.Prebuilt = nil }, false},
		{"null", func(w *wireConfig) { w.Agent.Prebuilt = json.RawMessage(`null`) }, false},
		{"empty", func(w *wireConfig) { w.Agent.Prebuilt = json.RawMessage(`{}`) }, false},
		{"array", func(w *wireConfig) { w.Agent.Prebuilt = json.RawMessage(`[]`) }, false},
		{"partial", func(w *wireConfig) { w.Agent.Prebuilt = json.RawMessage(`{"path":"/missing/skygo-ssh"}`) }, false},
		{"unknown", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(`"path":`), []byte(`"secret":"do-not-echo","path":`), 1)
		}, false},
		{"mixed source", func(w *wireConfig) { w.Agent.Source = "/src" }, false},
		{"mixed dirty", func(w *wireConfig) { w.Agent.AllowDirty = true }, false},
		{"relative path", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(`/missing/skygo-ssh`), []byte(`relative`), 1)
		}, false},
		{"unclean path", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(`/missing/skygo-ssh`), []byte(`/missing/../skygo-ssh`), 1)
		}, false},
		{"short commit", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(strings.Repeat("1", 40)), []byte("abcdef"), 1)
		}, false},
		{"uppercase commit", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(strings.Repeat("1", 40)), []byte(strings.Repeat("A", 40)), 1)
		}, false},
		{"sha256 commit", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(strings.Repeat("1", 40)), []byte(strings.Repeat("a", 64)), 1)
		}, true},
		{"short digest", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(strings.Repeat("2", 64)), []byte("abc"), 1)
		}, false},
		{"uppercase digest", func(w *wireConfig) {
			w.Agent.Prebuilt = bytes.Replace(w.Agent.Prebuilt, []byte(strings.Repeat("2", 64)), []byte(strings.Repeat("A", 64)), 1)
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := prebuiltConfigWire()
			tt.change(&w)
			c, err := ParseConfig(bytes.NewReader(encodeWire(t, w)))
			if (err == nil) != tt.ok {
				t.Fatalf("ParseConfig = %v, want success %v", err, tt.ok)
			}
			if err != nil {
				if strings.Contains(err.Error(), "do-not-echo") {
					t.Fatal("error echoed input")
				}
				return
			}
			s := c.Session(c.IP, nil)
			if s.Prebuilt != c.Prebuilt || s.Source != c.Source || s.AllowDirty != c.AllowDirty {
				t.Fatalf("Session lost selection: %+v", s)
			}
			if len(w.Agent.Prebuilt) != 0 && (c.Prebuilt.Path != "/missing/skygo-ssh" || c.Prebuilt.Commit == "" || c.Prebuilt.Digest != strings.Repeat("2", 64) || s.Source != "" || s.AllowDirty) {
				t.Fatal("prebuilt selection changed")
			}
		})
	}
}

func TestPrebuiltConfigDigestBinding(t *testing.T) {
	w := prebuiltConfigWire()
	c, err := ParseConfig(bytes.NewReader(encodeWire(t, w)))
	if err != nil {
		t.Fatal(err)
	}
	archive, digest, err := Snapshot(newWorkdir(t, map[string]string{"input.txt": "input\n"}))
	if err != nil {
		t.Fatal(err)
	}
	task, err := c.Task(TaskInput{JSON: []byte(testTaskJSON)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir() + "/job"
	j, err := Create(dir, c, testVM(c), task, archive)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Bind(c, testVM(c)); err != nil {
		t.Fatal(err)
	}
	before := readJournal(t, dir)
	for _, field := range []string{"path", "commit", "sha256"} {
		t.Run(field, func(t *testing.T) {
			var p map[string]string
			if err := json.Unmarshal(w.Agent.Prebuilt, &p); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "path":
				p[field] = "/another/binary"
			case "commit":
				p[field] = strings.Repeat("3", 40)
			case "sha256":
				p[field] = strings.Repeat("4", 64)
			}
			changed := w
			changed.Agent.Prebuilt, _ = json.Marshal(p)
			other, err := ParseConfig(bytes.NewReader(encodeWire(t, changed)))
			if err != nil {
				t.Fatal(err)
			}
			if other.Digest == c.Digest {
				t.Fatal("digest omitted prebuilt identity")
			}
			if err := j.Bind(other, testVM(other)); !errors.Is(err, ErrMismatch) {
				t.Fatalf("Bind = %v, want mismatch", err)
			}
			if !bytes.Equal(before, readJournal(t, dir)) {
				t.Fatal("refused rebind changed durable state")
			}
		})
	}
}
