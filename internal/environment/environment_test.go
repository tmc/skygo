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

// TestValidNameTarget checks ValidName and ValidTarget at their boundaries and
// that a complete, otherwise valid Spec is accepted exactly when they are.
func TestValidNameTarget(t *testing.T) {
	for _, test := range []struct {
		name string
		s    string
		want bool
	}{
		{"min", "a", true},
		{"digits and dash", "a1-b2", true},
		{"63", "a" + strings.Repeat("b", 62), true},
		{"64", "a" + strings.Repeat("b", 63), false},
		{"empty", "", false},
		{"upper first", "Analysis", false},
		{"upper inner", "anAlysis", false},
		{"slash", "a/b", false},
		{"leading digit", "1a", false},
		{"leading dash", "-a", false},
		{"trailing dash", "a-", false},
		{"underscore", "a_b", false},
		{"space", "a b", false},
	} {
		t.Run("name "+test.name, func(t *testing.T) {
			if got := environment.ValidName(test.s); got != test.want {
				t.Fatalf("ValidName(%q) = %v, want %v", test.s, got, test.want)
			}
			s := spec()
			s.Name = test.s
			if err := environment.Validate(s); (err == nil) != test.want {
				t.Fatalf("Validate with name %q = %v, want ok %v", test.s, err, test.want)
			}
		})
	}
	for _, test := range []struct {
		name string
		s    string
		want bool
	}{
		{"min", "k", true},
		{"128", strings.Repeat("t", 128), true},
		{"129", strings.Repeat("t", 129), false},
		{"empty", "", false},
		{"space", "kube east", true},
		{"only space", " ", true},
		{"printable ends", "!~", true},
		{"upper and slash", "Kube/East", true},
		{"NUL", "kube\x00", false},
		{"newline", "kube\n", false},
		{"tab", "a\tb", false},
		{"unit separator", "kube\x1f", false},
		{"DEL", "kube\x7f", false},
		{"non-ASCII", "kubé", false},
		{"high byte", "kube\x80", false},
	} {
		t.Run("target "+test.name, func(t *testing.T) {
			if got := environment.ValidTarget(test.s); got != test.want {
				t.Fatalf("ValidTarget(%q) = %v, want %v", test.s, got, test.want)
			}
			s := spec()
			s.Target = test.s
			if err := environment.Validate(s); (err == nil) != test.want {
				t.Fatalf("Validate with target %q = %v, want ok %v", test.s, err, test.want)
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

func artifactMount(path string) environment.ArtifactMount {
	return environment.ArtifactMount{ID: strings.Repeat("1", 32), SHA256: strings.Repeat("2", 64), Size: 3, MountPath: path}
}

func artifactSpec() environment.Spec {
	s := spec()
	s.Artifacts = []environment.ArtifactMount{artifactMount("/skygo/artifacts/data")}
	return s
}

func TestCanonicalArtifacts(t *testing.T) {
	image := `"image":"registry.example/task@sha256:` + strings.Repeat("a", 64) + `"`
	want := `{"version":2,"name":"analysis","target":"kube",` + image + `,"cpu_millis":1000,"memory_bytes":1073741824,"artifacts":[{"id":"` + strings.Repeat("1", 32) + `","sha256":"` + strings.Repeat("2", 64) + `","size":3,"mount_path":"/skygo/artifacts/data"}]}`
	data, err := environment.Canonical(artifactSpec())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("Canonical = %s, want %s", data, want)
	}
	got, err := environment.Parse(data)
	if err != nil || !reflect.DeepEqual(got, artifactSpec()) {
		t.Fatalf("Parse = %+v, %v", got, err)
	}
	plain, err := environment.Canonical(spec())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, data string
	}{
		{"version 1 with artifacts", strings.Replace(want, `"version":2`, `"version":1`, 1)},
		{"version 3", strings.Replace(want, `"version":2`, `"version":3`, 1)},
		{"version 2 empty artifacts", strings.Replace(strings.Replace(string(plain), `"version":1`, `"version":2`, 1), `}`, `,"artifacts":[]}`, 1)},
		{"version 1 empty artifacts", strings.Replace(string(plain), `}`, `,"artifacts":[]}`, 1)},
		{"unknown mount field", strings.Replace(want, `"size":3`, `"size":3,"mode":"rw"`, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := environment.Parse([]byte(test.data)); err == nil {
				t.Fatalf("Parse(%.80q) succeeded", test.data)
			}
		})
	}
}

func TestValidateArtifacts(t *testing.T) {
	eight := make([]environment.ArtifactMount, environment.MaxArtifacts)
	for i := range eight {
		eight[i] = artifactMount("/skygo/artifacts/a" + string(rune('0'+i)))
	}
	for _, test := range []struct {
		name string
		edit func(*environment.Spec)
		ok   bool
	}{
		{"one", func(*environment.Spec) {}, true},
		{"eight", func(s *environment.Spec) { s.Artifacts = eight }, true},
		{"nine", func(s *environment.Spec) {
			s.Artifacts = append(append([]environment.ArtifactMount(nil), eight...), artifactMount("/skygo/artifacts/a8"))
		}, false},
		{"one artifact twice", func(s *environment.Spec) {
			s.Artifacts = append(s.Artifacts, artifactMount("/skygo/artifacts/again"))
		}, true},
		{"sibling with shared prefix", func(s *environment.Spec) {
			s.Artifacts = append(s.Artifacts, artifactMount("/skygo/artifacts/database"))
		}, true},
		{"duplicate path", func(s *environment.Spec) {
			s.Artifacts = append(s.Artifacts, artifactMount("/skygo/artifacts/data"))
		}, false},
		{"nested path", func(s *environment.Spec) {
			s.Artifacts = append(s.Artifacts, artifactMount("/skygo/artifacts/data/inner"))
		}, false},
		{"relative", func(s *environment.Spec) { s.Artifacts[0].MountPath = "skygo/artifacts/data" }, false},
		{"parent", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/../etc" }, false},
		{"inner parent", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/a/../b" }, false},
		{"prefix", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/" }, false},
		{"prefix directory", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts" }, false},
		{"outside", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/data" }, false},
		{"trailing slash", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/data/" }, false},
		{"space", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/a b" }, false},
		{"long path", func(s *environment.Spec) { s.Artifacts[0].MountPath = "/skygo/artifacts/" + strings.Repeat("p", 240) }, false},
		{"upper id", func(s *environment.Spec) { s.Artifacts[0].ID = strings.Repeat("A", 32) }, false},
		{"short sha", func(s *environment.Spec) { s.Artifacts[0].SHA256 = strings.Repeat("2", 63) }, false},
		{"zero size", func(s *environment.Spec) { s.Artifacts[0].Size = 0 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := artifactSpec()
			test.edit(&s)
			if err := environment.Validate(s); (err == nil) != test.ok {
				t.Fatalf("Validate = %v, want ok %v", err, test.ok)
			}
		})
	}
}

func TestCloneOwnsArtifacts(t *testing.T) {
	s := artifactSpec()
	n, err := environment.Normalize(s)
	if err != nil {
		t.Fatal(err)
	}
	s.Artifacts[0].MountPath = "/skygo/artifacts/changed"
	if n.Artifacts[0].MountPath != "/skygo/artifacts/data" {
		t.Fatal("Normalize shares the caller's artifacts")
	}
	c := n.Clone()
	c.Artifacts[0].MountPath = "/skygo/artifacts/clone"
	if n.Artifacts[0].MountPath != "/skygo/artifacts/data" {
		t.Fatal("Clone shares the artifacts")
	}
	empty := spec()
	empty.Artifacts = []environment.ArtifactMount{}
	if n, err := environment.Normalize(empty); err != nil || n.Artifacts != nil {
		t.Fatalf("Normalize kept an empty artifact list: %v", err)
	}
}
