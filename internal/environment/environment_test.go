package environment_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/environment"
)

func spec() environment.Spec {
	return environment.Spec{Name: "analysis", Target: "kube", Image: "registry.example/task@sha256:" + strings.Repeat("a", 64), CPUMillis: 1000, MemoryBytes: 1 << 30}
}

func TestCanonicalVectors(t *testing.T) {
	full := spec()
	full.CPULimitMillis = 2000
	full.MemoryLimitBytes = 2 << 30
	full.Accelerator = "nvidia-l4"
	full.AcceleratorCount = 1
	full.NodeSelector = map[string]string{"zone": "b", "pool": "gpu"}
	full.StorageAlias = "scratch"
	full.Port = environment.Port
	full.IdleTimeoutSeconds = 3600
	empty := spec()
	empty.NodeSelector = map[string]string{}
	image := `"image":"registry.example/task@sha256:` + strings.Repeat("a", 64) + `"`
	for _, test := range []struct {
		name string
		s    environment.Spec
		want string
	}{
		{"minimal", spec(), `{"version":1,"name":"analysis","target":"kube",` + image + `,"cpu_millis":1000,"memory_bytes":1073741824}`},
		{"empty selector", empty, `{"version":1,"name":"analysis","target":"kube",` + image + `,"cpu_millis":1000,"memory_bytes":1073741824}`},
		{"full", full, `{"version":1,"name":"analysis","target":"kube",` + image + `,"cpu_millis":1000,"memory_bytes":1073741824,"cpu_limit_millis":2000,"memory_limit_bytes":2147483648,"accelerator":"nvidia-l4","accelerator_count":1,"node_selector":{"pool":"gpu","zone":"b"},"storage_alias":"scratch","port":8080,"idle_timeout_seconds":3600}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := environment.Canonical(test.s)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != test.want {
				t.Fatalf("Canonical = %s, want %s", data, test.want)
			}
			got, err := environment.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			again, err := environment.Canonical(got)
			if err != nil || !bytes.Equal(again, data) {
				t.Fatalf("round trip = %s, %v", again, err)
			}
			if len(test.s.NodeSelector) == 0 && got.NodeSelector != nil {
				t.Fatal("Parse kept an empty selector")
			}
		})
	}
}

func TestValidate(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*environment.Spec)
		ok   bool
	}{
		{"valid", func(*environment.Spec) {}, true},
		{"zero", func(s *environment.Spec) { *s = environment.Spec{} }, false},
		{"empty name", func(s *environment.Spec) { s.Name = "" }, false},
		{"name digit first", func(s *environment.Spec) { s.Name = "1a" }, false},
		{"name trailing dash", func(s *environment.Spec) { s.Name = "a-" }, false},
		{"name upper", func(s *environment.Spec) { s.Name = "A" }, false},
		{"name 63", func(s *environment.Spec) { s.Name = "a" + strings.Repeat("b", 62) }, true},
		{"name 64", func(s *environment.Spec) { s.Name = "a" + strings.Repeat("b", 63) }, false},
		{"no target", func(s *environment.Spec) { s.Target = "" }, false},
		{"target 129", func(s *environment.Spec) { s.Target = strings.Repeat("t", 129) }, false},
		{"target control", func(s *environment.Spec) { s.Target = "a\tb" }, false},
		{"no image", func(s *environment.Spec) { s.Image = "" }, false},
		{"image space", func(s *environment.Spec) { s.Image = "a b" }, false},
		{"image 513", func(s *environment.Spec) { s.Image = strings.Repeat("i", 513) }, false},
		{"no cpu", func(s *environment.Spec) { s.CPUMillis = 0 }, false},
		{"no memory", func(s *environment.Spec) { s.MemoryBytes = 0 }, false},
		{"negative memory", func(s *environment.Spec) { s.MemoryBytes = -1 }, false},
		{"cpu limit below request", func(s *environment.Spec) { s.CPULimitMillis = 999 }, false},
		{"cpu limit equal", func(s *environment.Spec) { s.CPULimitMillis = 1000 }, true},
		{"negative cpu limit", func(s *environment.Spec) { s.CPULimitMillis = -1 }, false},
		{"memory limit below request", func(s *environment.Spec) { s.MemoryLimitBytes = 1 }, false},
		{"accelerator without count", func(s *environment.Spec) { s.Accelerator = "gpu" }, false},
		{"count without accelerator", func(s *environment.Spec) { s.AcceleratorCount = 1 }, false},
		{"negative count", func(s *environment.Spec) { s.Accelerator, s.AcceleratorCount = "gpu", -1 }, false},
		{"accelerator", func(s *environment.Spec) { s.Accelerator, s.AcceleratorCount = "gpu", 2 }, true},
		{"selector 16", func(s *environment.Spec) { s.NodeSelector = selector(16) }, true},
		{"selector 17", func(s *environment.Spec) { s.NodeSelector = selector(17) }, false},
		{"selector empty key", func(s *environment.Spec) { s.NodeSelector = map[string]string{"": "v"} }, false},
		{"selector empty value", func(s *environment.Spec) { s.NodeSelector = map[string]string{"k": ""} }, true},
		{"selector NUL", func(s *environment.Spec) { s.NodeSelector = map[string]string{"k": "\x00"} }, false},
		{"selector invalid UTF8", func(s *environment.Spec) { s.NodeSelector = map[string]string{"k": "\xff"} }, false},
		{"alias", func(s *environment.Spec) { s.StorageAlias = "0data" }, true},
		{"alias leading dash", func(s *environment.Spec) { s.StorageAlias = "-a" }, false},
		{"alias upper", func(s *environment.Spec) { s.StorageAlias = "Data" }, false},
		{"port 8080", func(s *environment.Spec) { s.Port = 8080 }, true},
		{"port 80", func(s *environment.Spec) { s.Port = 80 }, false},
		{"idle minimum", func(s *environment.Spec) { s.IdleTimeoutSeconds = 300 }, true},
		{"idle maximum", func(s *environment.Spec) { s.IdleTimeoutSeconds = 604800 }, true},
		{"idle short", func(s *environment.Spec) { s.IdleTimeoutSeconds = 299 }, false},
		{"idle long", func(s *environment.Spec) { s.IdleTimeoutSeconds = 604801 }, false},
		{"idle negative", func(s *environment.Spec) { s.IdleTimeoutSeconds = -1 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := spec()
			test.edit(&s)
			err := environment.Validate(s)
			if (err == nil) != test.ok {
				t.Fatalf("Validate = %v, want ok %v", err, test.ok)
			}
			if _, err := environment.Normalize(s); (err == nil) != test.ok {
				t.Fatalf("Normalize = %v, want ok %v", err, test.ok)
			}
		})
	}
}

func selector(n int) map[string]string {
	m := map[string]string{}
	for i := range n {
		m[strings.Repeat("k", i+1)] = "v"
	}
	return m
}

func TestParseRefuses(t *testing.T) {
	data, err := environment.Canonical(spec())
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	for _, test := range []struct {
		name, data string
	}{
		{"empty", ""},
		{"null", "null"},
		{"space", " " + s},
		{"trailing", s + "{}"},
		{"newline", s + "\n"},
		{"no version", strings.Replace(s, `"version":1,`, ``, 1)},
		{"version 2", strings.Replace(s, `"version":1`, `"version":2`, 1)},
		{"unknown field", strings.Replace(s, `"version":1`, `"version":1,"argv":["sh"]`, 1)},
		{"reordered", strings.Replace(s, `"name":"analysis","target":"kube"`, `"target":"kube","name":"analysis"`, 1)},
		{"explicit zero", strings.Replace(s, `"memory_bytes":1073741824`, `"memory_bytes":1073741824,"port":0`, 1)},
		{"invalid spec", strings.Replace(s, `"cpu_millis":1000`, `"cpu_millis":0`, 1)},
		{"oversize", s + strings.Repeat(" ", environment.MaxSpecBytes)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := environment.Parse([]byte(test.data)); err == nil {
				t.Fatalf("Parse(%.80q) succeeded", test.data)
			}
		})
	}
}

func TestNormalizeOwnsMemory(t *testing.T) {
	s := spec()
	s.NodeSelector = map[string]string{"zone": "a"}
	n, err := environment.Normalize(s)
	if err != nil {
		t.Fatal(err)
	}
	s.NodeSelector["zone"] = "changed"
	if n.NodeSelector["zone"] != "a" {
		t.Fatal("Normalize shares the caller's selector")
	}
	c := n.Clone()
	c.NodeSelector["zone"] = "clone"
	if n.NodeSelector["zone"] != "a" {
		t.Fatal("Clone shares the selector")
	}
	if !reflect.DeepEqual(environment.Spec{}.Clone(), environment.Spec{}) {
		t.Fatal("Clone changed the zero Spec")
	}
}

func TestStateNext(t *testing.T) {
	const (
		P = environment.Present
		A = environment.Absent
	)
	phases := []environment.Phase{environment.Creating, environment.Ready, environment.Deleting, environment.Deleted, environment.Unknown}
	allowed := map[environment.Phase][]environment.Phase{
		environment.Creating: {environment.Creating, environment.Ready, environment.Deleting, environment.Unknown},
		environment.Ready:    {environment.Ready, environment.Deleting, environment.Unknown},
		environment.Unknown:  {environment.Ready, environment.Deleting, environment.Unknown},
		environment.Deleting: {environment.Deleting, environment.Deleted, environment.Unknown},
		environment.Deleted:  {environment.Deleted},
	}
	for _, d1 := range []environment.Desired{P, A} {
		for _, d2 := range []environment.Desired{P, A} {
			for _, p1 := range phases {
				for _, p2 := range phases {
					from, to := environment.State{Desired: d1, Phase: p1}, environment.State{Desired: d2, Phase: p2}
					want := from.Valid() && to.Valid() && !(d1 == A && d2 == P)
					if want {
						want = false
						for _, p := range allowed[p1] {
							want = want || p == p2
						}
					}
					if err := from.Next(to); (err == nil) != want {
						t.Errorf("%v/%v -> %v/%v: %v, want ok %v", d1, p1, d2, p2, err, want)
					}
				}
			}
		}
	}
}

func TestStateValid(t *testing.T) {
	for _, test := range []struct {
		s    environment.State
		want bool
	}{
		{environment.State{}, false},
		{environment.State{Desired: environment.Present}, false},
		{environment.State{Phase: environment.Creating}, false},
		{environment.State{Desired: 3, Phase: environment.Creating}, false},
		{environment.State{Desired: environment.Present, Phase: 6}, false},
		{environment.State{Desired: environment.Present, Phase: environment.Creating}, true},
		{environment.State{Desired: environment.Present, Phase: environment.Ready}, true},
		{environment.State{Desired: environment.Present, Phase: environment.Unknown}, true},
		{environment.State{Desired: environment.Present, Phase: environment.Deleting}, false},
		{environment.State{Desired: environment.Present, Phase: environment.Deleted}, false},
		{environment.State{Desired: environment.Absent, Phase: environment.Ready}, true},
		{environment.State{Desired: environment.Absent, Phase: environment.Deleted}, true},
	} {
		if got := test.s.Valid(); got != test.want {
			t.Errorf("%v/%v Valid = %v, want %v", test.s.Desired, test.s.Phase, got, test.want)
		}
	}
}

func TestIdentity(t *testing.T) {
	id := strings.Repeat("a", 32)
	for _, test := range []struct {
		i    environment.Identity
		want bool
	}{
		{environment.Identity{Installation: id, ID: id, Generation: 1}, true},
		{environment.Identity{Installation: id, ID: id}, false},
		{environment.Identity{Installation: id, ID: id, Generation: 2}, false},
		{environment.Identity{Installation: id, ID: strings.Repeat("A", 32), Generation: 1}, false},
		{environment.Identity{Installation: "legacy-owner", ID: id, Generation: 1}, true},
		{environment.Identity{Installation: strings.Repeat("o", 129), ID: id, Generation: 1}, false},
		{environment.Identity{Installation: "a\x00", ID: id, Generation: 1}, false},
		{environment.Identity{Installation: id, ID: id[:31], Generation: 1}, false},
		{environment.Identity{ID: id, Generation: 1}, false},
	} {
		if got := test.i.Valid(); got != test.want {
			t.Errorf("%+v Valid = %v, want %v", test.i, got, test.want)
		}
	}
}

func TestCheckpoint(t *testing.T) {
	for _, test := range []struct {
		name string
		c    environment.Checkpoint
		ok   bool
	}{
		{"valid", environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: []byte("{}")}, true},
		{"maximum", environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: make([]byte, environment.MaxCheckpointBytes)}, true},
		{"oversize", environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: make([]byte, environment.MaxCheckpointBytes+1)}, false},
		{"empty", environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: []byte{}}, false},
		{"nil", environment.Checkpoint{Kind: "ec2-vm", Version: 1}, false},
		{"no version", environment.Checkpoint{Kind: "ec2-vm", Data: []byte("x")}, false},
		{"no kind", environment.Checkpoint{Version: 1, Data: []byte("x")}, false},
		{"kind upper", environment.Checkpoint{Kind: "EC2", Version: 1, Data: []byte("x")}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.c.Validate(); (err == nil) != test.ok {
				t.Fatalf("Validate = %v, want ok %v", err, test.ok)
			}
		})
	}
	c := environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: []byte("abc")}
	d := c.Clone()
	d.Data[0] = 'x'
	if string(c.Data) != "abc" {
		t.Fatal("Clone shares Data")
	}
}
