package serving

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

func testSpec() Spec {
	return Spec{
		Image:        "registry.example.com/echo@sha256:" + strings.Repeat("a", 64),
		Argv:         []string{"/echo", "-listen", ":8080"},
		Env:          []EnvVar{{"MODE", "v1"}, {"COLOR", "blue"}},
		Port:         8080,
		Readiness:    Readiness{Path: "/healthz"},
		Resources:    Resources{CPUMillis: 250, MemoryBytes: 64 << 20},
		NodeSelector: []Label{{"pool", "general"}},
		Mounts:       []Mount{{"models", "/mnt/skygo/models"}},
		Scaling:      Scaling{Replicas: 2},
	}
}

func testRef() *artifact.Ref {
	return &artifact.Ref{ID: strings.Repeat("1", 32), SHA256: strings.Repeat("2", 64), Size: 1024}
}

func mustDigest(t *testing.T, s Spec) string {
	t.Helper()
	d, err := s.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestNormalizeDefaults(t *testing.T) {
	in := testSpec()
	n, err := in.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if want := (Readiness{"/healthz", 10, 15, 3, 0}); n.Readiness != want {
		t.Errorf("Readiness = %+v, want %+v", n.Readiness, want)
	}
	if want := (RolloutPolicy{600, 30}); n.Rollout != want {
		t.Errorf("Rollout = %+v, want %+v", n.Rollout, want)
	}
	if want := []EnvVar{{"COLOR", "blue"}, {"MODE", "v1"}}; !slices.Equal(n.Env, want) {
		t.Errorf("Env = %v, want %v", n.Env, want)
	}
	if in.Env[0].Name != "MODE" {
		t.Errorf("Normalize reordered its input")
	}
	in.Argv[0] = "/changed"
	in.Env[0].Value = "changed"
	if n.Argv[0] != "/echo" || n.Env[1].Value != "v1" {
		t.Errorf("Normalize result shares memory with its input")
	}
	explicit := testSpec()
	explicit.Readiness = Readiness{"/healthz", 11, 16, 4, 5}
	explicit.Rollout = RolloutPolicy{601, 31}
	n, err = explicit.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if n.Readiness != explicit.Readiness || n.Rollout != explicit.Rollout {
		t.Errorf("Normalize rewrote explicit values: %+v %+v", n.Readiness, n.Rollout)
	}
}

func TestNormalizeAccepts(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"minimal", func(s *Spec) { s.Env, s.NodeSelector, s.Mounts = nil, nil, nil }},
		{"localhost registry", func(s *Spec) { s.Image = "localhost:5000/a/b@sha256:" + strings.Repeat("0", 64) }},
		{"bounds high", func(s *Spec) {
			s.Port, s.Scaling.Replicas = 65535, 8
			s.Readiness = Readiness{"/" + strings.Repeat("a", 255), 60, 60, 10, 1200}
			s.Rollout = RolloutPolicy{3600, 120}
		}},
		{"bounds low", func(s *Spec) {
			s.Port, s.Scaling.Replicas = 1, 1
			s.Readiness = Readiness{"/", 1, 1, 1, 0}
			s.Rollout = RolloutPolicy{1, 1}
			s.Resources = Resources{CPUMillis: 1, MemoryBytes: 1}
		}},
		{"limits equal requests", func(s *Spec) { s.Resources.CPULimitMillis, s.Resources.MemoryLimitBytes = 250, 64<<20 }},
		{"max quantities", func(s *Spec) { s.Resources = Resources{maxCPUMillis, maxMemoryBytes, maxCPUMillis, maxMemoryBytes} }},
		{"prefixed label", func(s *Spec) { s.NodeSelector = []Label{{"kubernetes.io/arch", "amd64"}, {"zone", ""}} }},
		{"sibling mounts", func(s *Spec) { s.Mounts = []Mount{{"a", "/mnt/skygo/ab"}, {"b", "/mnt/skygo/a"}} }},
		{"workdir", func(s *Spec) { s.Workdir = testRef() }},
		{"workdir and setup", func(s *Spec) { s.Workdir, s.Setup = testRef(), []string{"/bin/prepare"} }},
		{"max args", func(s *Spec) { s.Argv = slices.Repeat([]string{strings.Repeat("x", 256)}, 64) }},
		{"probe query", func(s *Spec) { s.Readiness.Path = "/healthz?full=1" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testSpec()
			tt.edit(&s)
			if _, err := s.Normalize(); err != nil {
				t.Fatalf("Normalize: %v", err)
			}
		})
	}
}

func TestNormalizeRejects(t *testing.T) {
	long := strings.Repeat("x", 257)
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"tagged image", func(s *Spec) { s.Image = "registry.example.com/echo:latest" }},
		{"image without registry", func(s *Spec) { s.Image = "echo@sha256:" + strings.Repeat("a", 64) }},
		{"image uppercase digest", func(s *Spec) { s.Image = "registry.example.com/echo@sha256:" + strings.Repeat("A", 64) }},
		{"image short digest", func(s *Spec) { s.Image = "registry.example.com/echo@sha256:abc" }},
		{"image bad port", func(s *Spec) { s.Image = "registry.example.com:99999/echo@sha256:" + strings.Repeat("a", 64) }},
		{"no argv", func(s *Spec) { s.Argv = nil }},
		{"empty program", func(s *Spec) { s.Argv = []string{"", "x"} }},
		{"nul argument", func(s *Spec) { s.Argv = []string{"/echo", "a\x00b"} }},
		{"invalid utf8 argument", func(s *Spec) { s.Argv = []string{"/echo", "\xff"} }},
		{"too many arguments", func(s *Spec) { s.Argv = slices.Repeat([]string{"a"}, 65) }},
		{"argument bytes", func(s *Spec) { s.Argv = []string{"/echo", strings.Repeat("a", maxArgBytes)} }},
		{"env digit name", func(s *Spec) { s.Env = []EnvVar{{"1X", ""}} }},
		{"env dash name", func(s *Spec) { s.Env = []EnvVar{{"A-B", ""}} }},
		{"env empty name", func(s *Spec) { s.Env = []EnvVar{{"", "x"}} }},
		{"env duplicate", func(s *Spec) { s.Env = []EnvVar{{"A", "1"}, {"B", "2"}, {"A", "1"}} }},
		{"env nul value", func(s *Spec) { s.Env = []EnvVar{{"A", "\x00"}} }},
		{"env count", func(s *Spec) {
			s.Env = nil
			for i := range 65 {
				s.Env = append(s.Env, EnvVar{fmt.Sprintf("V%d", i), ""})
			}
		}},
		{"env bytes", func(s *Spec) { s.Env = []EnvVar{{"A", strings.Repeat("a", maxEnvBytes)}} }},
		{"port zero", func(s *Spec) { s.Port = 0 }},
		{"port negative", func(s *Spec) { s.Port = -1 }},
		{"port high", func(s *Spec) { s.Port = 65536 }},
		{"probe no path", func(s *Spec) { s.Readiness.Path = "" }},
		{"probe relative", func(s *Spec) { s.Readiness.Path = "healthz" }},
		{"probe space", func(s *Spec) { s.Readiness.Path = "/a b" }},
		{"probe fragment", func(s *Spec) { s.Readiness.Path = "/a#b" }},
		{"probe non ascii", func(s *Spec) { s.Readiness.Path = "/é" }},
		{"probe long", func(s *Spec) { s.Readiness.Path = "/" + long[:256] }},
		{"probe period", func(s *Spec) { s.Readiness.PeriodSeconds = 61 }},
		{"probe negative period", func(s *Spec) { s.Readiness.PeriodSeconds = -1 }},
		{"probe timeout", func(s *Spec) { s.Readiness.TimeoutSeconds = 61 }},
		{"probe failures", func(s *Spec) { s.Readiness.FailureThreshold = 11 }},
		{"probe negative delay", func(s *Spec) { s.Readiness.InitialDelaySeconds = -1 }},
		{"probe delay", func(s *Spec) { s.Readiness.InitialDelaySeconds = 1201 }},
		{"no cpu request", func(s *Spec) { s.Resources.CPUMillis = 0 }},
		{"no memory request", func(s *Spec) { s.Resources.MemoryBytes = 0 }},
		{"negative cpu", func(s *Spec) { s.Resources.CPUMillis = -1 }},
		{"cpu unbounded", func(s *Spec) { s.Resources.CPUMillis = maxCPUMillis + 1 }},
		{"memory unbounded", func(s *Spec) { s.Resources.MemoryBytes = maxMemoryBytes + 1 }},
		{"cpu limit below request", func(s *Spec) { s.Resources.CPULimitMillis = 249 }},
		{"memory limit below request", func(s *Spec) { s.Resources.MemoryLimitBytes = 1 }},
		{"negative cpu limit", func(s *Spec) { s.Resources.CPULimitMillis = -1 }},
		{"cpu limit unbounded", func(s *Spec) { s.Resources.CPULimitMillis = maxCPUMillis + 1 }},
		{"memory limit unbounded", func(s *Spec) { s.Resources.MemoryLimitBytes = maxMemoryBytes + 1 }},
		{"label key", func(s *Spec) { s.NodeSelector = []Label{{"-pool", "a"}} }},
		{"label empty key", func(s *Spec) { s.NodeSelector = []Label{{"", "a"}} }},
		{"label uppercase prefix", func(s *Spec) { s.NodeSelector = []Label{{"Example.com/pool", "a"}} }},
		{"label empty name", func(s *Spec) { s.NodeSelector = []Label{{"example.com/", "a"}} }},
		{"label value", func(s *Spec) { s.NodeSelector = []Label{{"pool", "a b"}} }},
		{"label value long", func(s *Spec) { s.NodeSelector = []Label{{"pool", long[:64]}} }},
		{"label duplicate", func(s *Spec) { s.NodeSelector = []Label{{"pool", "a"}, {"pool", "b"}} }},
		{"label count", func(s *Spec) {
			s.NodeSelector = nil
			for i := range 17 {
				s.NodeSelector = append(s.NodeSelector, Label{fmt.Sprintf("k%d", i), ""})
			}
		}},
		{"mount alias", func(s *Spec) { s.Mounts = []Mount{{"Models", "/mnt/skygo/models"}} }},
		{"mount empty alias", func(s *Spec) { s.Mounts = []Mount{{"", "/mnt/skygo/models"}} }},
		{"mount outside root", func(s *Spec) { s.Mounts = []Mount{{"models", "/data"}} }},
		{"mount root itself", func(s *Spec) { s.Mounts = []Mount{{"models", "/mnt/skygo"}} }},
		{"mount dot dot", func(s *Spec) { s.Mounts = []Mount{{"models", "/mnt/skygo/../etc"}} }},
		{"mount trailing slash", func(s *Spec) { s.Mounts = []Mount{{"models", "/mnt/skygo/models/"}} }},
		{"mount glob", func(s *Spec) { s.Mounts = []Mount{{"models", "/mnt/skygo/m*"}} }},
		{"mount nested", func(s *Spec) { s.Mounts = []Mount{{"a", "/mnt/skygo/a/b"}, {"b", "/mnt/skygo/a"}} }},
		{"mount same", func(s *Spec) { s.Mounts = []Mount{{"a", "/mnt/skygo/a"}, {"b", "/mnt/skygo/a"}} }},
		{"mount count", func(s *Spec) {
			s.Mounts = nil
			for i := range 17 {
				s.Mounts = append(s.Mounts, Mount{"a", fmt.Sprintf("/mnt/skygo/m%d", i)})
			}
		}},
		{"progress deadline", func(s *Spec) { s.Rollout.ProgressDeadlineSeconds = 3601 }},
		{"negative progress deadline", func(s *Spec) { s.Rollout.ProgressDeadlineSeconds = -1 }},
		{"grace", func(s *Spec) { s.Rollout.TerminationGraceSeconds = 121 }},
		{"negative grace", func(s *Spec) { s.Rollout.TerminationGraceSeconds = -1 }},
		{"no replicas", func(s *Spec) { s.Scaling.Replicas = 0 }},
		{"negative replicas", func(s *Spec) { s.Scaling.Replicas = -1 }},
		{"too many replicas", func(s *Spec) { s.Scaling.Replicas = 9 }},
		{"invalid workdir", func(s *Spec) { s.Workdir = &artifact.Ref{ID: "x"} }},
		{"setup without workdir", func(s *Spec) { s.Setup = []string{"/bin/prepare"} }},
		{"setup empty program", func(s *Spec) { s.Workdir, s.Setup = testRef(), []string{""} }},
		{"setup nul", func(s *Spec) { s.Workdir, s.Setup = testRef(), []string{"/bin/prepare", "\x00"} }},
		{"setup count", func(s *Spec) { s.Workdir, s.Setup = testRef(), slices.Repeat([]string{"a"}, 65) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testSpec()
			tt.edit(&s)
			if n, err := s.Normalize(); err == nil {
				t.Fatalf("Normalize accepted %+v", n)
			}
			if _, err := s.Digest(); err == nil {
				t.Fatalf("Digest accepted invalid spec")
			}
		})
	}
}

func TestNormalizeSize(t *testing.T) {
	s := testSpec()
	s.Env = nil
	for i := range 64 {
		s.Env = append(s.Env, EnvVar{fmt.Sprintf("V%02d", i), strings.Repeat("v", 509)})
	}
	s.Argv = slices.Repeat([]string{strings.Repeat("a", 256)}, 64)
	s.Workdir = testRef()
	s.Setup = slices.Repeat([]string{strings.Repeat("s", 256)}, 64)
	_, err := s.Normalize()
	if err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Fatalf("Normalize = %v, want encoded size error", err)
	}
	s.Setup = s.Setup[:32]
	if _, err := s.Normalize(); err != nil {
		t.Fatalf("Normalize below limit: %v", err)
	}
}

// TestDigestGolden pins the canonical encoding to digests computed by an
// independent encoder of the same byte layout.
func TestDigestGolden(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Spec)
		want string
	}{
		{"base", func(*Spec) {}, "f5b1af0a776c20b08b1a3d6bc2b029e7c363c686637f85003df3b90c6405269b"},
		{"workdir", func(s *Spec) { s.Workdir, s.Setup = testRef(), []string{"/bin/prepare", "--fast"} }, "5d1ebadae269df6f0567f461d9ea99dc229492c53fa504e23b4210c730d0ead3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testSpec()
			tt.edit(&s)
			if got := mustDigest(t, s); got != tt.want {
				t.Errorf("Digest = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDigestEquivalent(t *testing.T) {
	want := mustDigest(t, testSpec())
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"env order", func(s *Spec) { s.Env = []EnvVar{{"COLOR", "blue"}, {"MODE", "v1"}} }},
		{"explicit defaults", func(s *Spec) {
			s.Readiness = Readiness{"/healthz", 10, 15, 3, 0}
			s.Rollout = RolloutPolicy{600, 30}
		}},
		{"empty slices", func(s *Spec) { s.Setup = []string{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testSpec()
			tt.edit(&s)
			if got := mustDigest(t, s); got != want {
				t.Errorf("Digest = %s, want %s", got, want)
			}
		})
	}
	a, b := testSpec(), testSpec()
	a.NodeSelector = []Label{{"a", "1"}, {"b", "2"}}
	b.NodeSelector = []Label{{"b", "2"}, {"a", "1"}}
	a.Mounts = []Mount{{"x", "/mnt/skygo/x"}, {"y", "/mnt/skygo/y"}}
	b.Mounts = []Mount{{"y", "/mnt/skygo/y"}, {"x", "/mnt/skygo/x"}}
	if mustDigest(t, a) != mustDigest(t, b) {
		t.Errorf("label or mount order changed the digest")
	}
}

func TestDigestDistinct(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"base", func(*Spec) {}},
		{"image", func(s *Spec) { s.Image = "registry.example.com/echo@sha256:" + strings.Repeat("b", 64) }},
		{"argv added", func(s *Spec) { s.Argv = append(s.Argv, "-v") }},
		{"argv order", func(s *Spec) { s.Argv = []string{"/echo", ":8080", "-listen"} }},
		{"argv split", func(s *Spec) { s.Argv = []string{"/echo", "-listen:", "8080"} }},
		{"env value", func(s *Spec) { s.Env[0].Value = "v2" }},
		{"env name", func(s *Spec) { s.Env[0].Name = "MODE2" }},
		{"env shift", func(s *Spec) { s.Env = []EnvVar{{"MODE", "v1C"}, {"OLOR", "blue"}} }},
		{"env removed", func(s *Spec) { s.Env = s.Env[:1] }},
		{"port", func(s *Spec) { s.Port = 8081 }},
		{"probe path", func(s *Spec) { s.Readiness.Path = "/ready" }},
		{"probe period", func(s *Spec) { s.Readiness.PeriodSeconds = 15 }},
		{"probe timeout", func(s *Spec) { s.Readiness.TimeoutSeconds = 10 }},
		{"probe failures", func(s *Spec) { s.Readiness.FailureThreshold = 4 }},
		{"probe delay", func(s *Spec) { s.Readiness.InitialDelaySeconds = 3 }},
		{"cpu", func(s *Spec) { s.Resources.CPUMillis = 251 }},
		{"memory", func(s *Spec) { s.Resources.MemoryBytes++ }},
		{"cpu limit", func(s *Spec) { s.Resources.CPULimitMillis = 500 }},
		{"memory limit", func(s *Spec) { s.Resources.MemoryLimitBytes = 128 << 20 }},
		{"label value", func(s *Spec) { s.NodeSelector[0].Value = "gpu" }},
		{"label key", func(s *Spec) { s.NodeSelector[0].Key = "tier" }},
		{"label added", func(s *Spec) { s.NodeSelector = append(s.NodeSelector, Label{"zone", "a"}) }},
		{"labels removed", func(s *Spec) { s.NodeSelector = nil }},
		{"mount alias", func(s *Spec) { s.Mounts[0].Reference = "weights" }},
		{"mount destination", func(s *Spec) { s.Mounts[0].Destination = "/mnt/skygo/weights" }},
		{"mounts removed", func(s *Spec) { s.Mounts = nil }},
		{"progress deadline", func(s *Spec) { s.Rollout.ProgressDeadlineSeconds = 601 }},
		{"grace", func(s *Spec) { s.Rollout.TerminationGraceSeconds = 31 }},
		{"replicas", func(s *Spec) { s.Scaling.Replicas = 3 }},
		{"workdir", func(s *Spec) { s.Workdir = testRef() }},
		{"workdir size", func(s *Spec) { s.Workdir = testRef(); s.Workdir.Size++ }},
		{"setup", func(s *Spec) { s.Workdir, s.Setup = testRef(), []string{"/bin/prepare"} }},
	}
	seen := make(map[string]string)
	for _, tt := range tests {
		s := testSpec()
		tt.edit(&s)
		d := mustDigest(t, s)
		if prev, ok := seen[d]; ok {
			t.Errorf("%s and %s have equal digests", prev, tt.name)
		}
		seen[d] = tt.name
	}
}

// settled returns h after an observation of its current generation, ready
// or not, with two ready endpoints.
func settled(t *testing.T, h History, ready bool) History {
	t.Helper()
	g := h.Current().Generation
	o := Observation{ObservedGeneration: g, DesiredReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2}
	if ready {
		o.ReadyGeneration, o.UpdatedReplicas = g, 2
	}
	h, err := h.Settle(o)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func version(i int) Spec {
	s := testSpec()
	s.Env = []EnvVar{{"MODE", fmt.Sprintf("v%d", i)}}
	return s
}

// updates returns a new history of version(0), with generation 1 complete
// when complete is set, followed by n updates.
func updates(t *testing.T, n int, complete bool) History {
	t.Helper()
	h, err := NewHistory(version(0))
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		h = settled(t, h, true)
	}
	for i := 1; i <= n; i++ {
		if h, err = h.Update(version(i)); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func numbers(revs []Revision) []uint64 {
	var out []uint64
	for _, r := range revs {
		out = append(out, r.Number)
	}
	return out
}

func generations(rs []Rollout) []uint64 {
	var out []uint64
	for _, r := range rs {
		out = append(out, r.Generation)
	}
	return out
}

func TestNewHistory(t *testing.T) {
	h, err := NewHistory(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(h.Revisions) != 1 || h.Revisions[0].Number != 1 || h.Revisions[0].SHA256 != mustDigest(t, testSpec()) {
		t.Errorf("Revisions = %+v", h.Revisions)
	}
	if want := []Rollout{{1, 1, Create, Pending}}; !slices.Equal(h.Rollouts, want) {
		t.Errorf("Rollouts = %+v, want %+v", h.Rollouts, want)
	}
	if _, ok := h.LastComplete(); ok {
		t.Errorf("new history has a complete rollout")
	}
	if p := PhaseOf(Present, h, nil); p != Creating {
		t.Errorf("PhaseOf = %v, want creating", p)
	}
	if _, err := NewHistory(Spec{}); err == nil {
		t.Errorf("NewHistory accepted an invalid spec")
	}
}

func TestUpdate(t *testing.T) {
	h0 := updates(t, 0, false)
	h1, err := h0.Update(version(1))
	if err != nil {
		t.Fatal(err)
	}
	if want := []Rollout{{1, 1, Create, Superseded}, {2, 2, Update, Pending}}; !slices.Equal(h1.Rollouts, want) {
		t.Errorf("Rollouts = %+v, want %+v", h1.Rollouts, want)
	}
	if got := numbers(h1.Revisions); !slices.Equal(got, []uint64{1, 2}) {
		t.Errorf("revisions = %v", got)
	}
	if h0.Rollouts[0].Outcome != Pending || len(h0.Revisions) != 1 {
		t.Errorf("Update modified its receiver: %+v", h0)
	}
	if _, err := h1.Update(version(1)); err == nil {
		t.Errorf("Update accepted the current spec")
	}
	if _, err := h1.Update(Spec{}); err == nil {
		t.Errorf("Update accepted an invalid spec")
	}
	if _, err := (History{}).Update(version(1)); err == nil {
		t.Errorf("Update accepted an invalid history")
	}

	// An update to an older spec is a new revision with an equal digest.
	h2, err := h1.Update(version(0))
	if err != nil {
		t.Fatal(err)
	}
	if r := h2.Revisions[2]; r.Number != 3 || r.SHA256 != h2.Revisions[0].SHA256 {
		t.Errorf("revision = %+v", r)
	}

	// A complete rollout stays complete.
	c := settled(t, h0, true)
	c, err = c.Update(version(1))
	if err != nil {
		t.Fatal(err)
	}
	if c.Rollouts[0].Outcome != Complete {
		t.Errorf("Update changed a complete rollout to %v", c.Rollouts[0].Outcome)
	}
}

func TestRollback(t *testing.T) {
	h := updates(t, 1, true)
	if _, err := h.Rollback(2); err == nil {
		t.Errorf("Rollback accepted the current revision")
	}
	if _, err := h.Rollback(9); err == nil {
		t.Errorf("Rollback accepted an unknown revision")
	}
	r, err := h.Rollback(1)
	if err != nil {
		t.Fatal(err)
	}
	want := []Rollout{{1, 1, Create, Complete}, {2, 2, Update, Superseded}, {3, 1, Rollback, Pending}}
	if !slices.Equal(r.Rollouts, want) {
		t.Errorf("Rollouts = %+v, want %+v", r.Rollouts, want)
	}
	if got := numbers(r.Revisions); !slices.Equal(got, []uint64{1, 2}) {
		t.Errorf("Rollback changed revisions: %v", got)
	}
	if h.Current().Generation != 2 {
		t.Errorf("Rollback modified its receiver")
	}
	if _, err := r.Rollback(1); err == nil {
		t.Errorf("Rollback accepted the current revision")
	}
	r, err = r.Rollback(2)
	if err != nil {
		t.Fatal(err)
	}
	if c := r.Current(); c != (Rollout{4, 2, Rollback, Pending}) {
		t.Errorf("Current = %+v", c)
	}
}

func TestSettle(t *testing.T) {
	ready := Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2}
	tests := []struct {
		name    string
		from    Outcome
		o       Observation
		want    Outcome
		wantErr bool
	}{
		{"not yet observed", Pending, Observation{}, Pending, false},
		{"observed", Pending, Observation{ObservedGeneration: 1}, Rolling, false},
		{"ready", Pending, ready, Complete, false},
		{"ready from rolling", Rolling, ready, Complete, false},
		{"ready without endpoints", Pending, Observation{ObservedGeneration: 1, ReadyGeneration: 1}, Complete, false},
		{"replicas ready without generation", Pending, Observation{ObservedGeneration: 1, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2}, Rolling, false},
		{"image pull", Rolling, Observation{ObservedGeneration: 1, Reason: ImagePull}, Failed, false},
		{"deadline", Pending, Observation{ObservedGeneration: 1, Reason: ProgressDeadline}, Failed, false},
		{"failure before observation", Pending, Observation{Reason: CrashLoop}, Pending, false},
		{"ready despite reason", Pending, Observation{ObservedGeneration: 1, ReadyGeneration: 1, Reason: CrashLoop}, Complete, false},
		{"conflict", Pending, Observation{ObservedGeneration: 1, ReadyGeneration: 1, Reason: IdentityConflict}, Pending, false},
		{"absent", Rolling, Observation{Absent: true}, Rolling, false},
		{"complete stays", Complete, Observation{ObservedGeneration: 1, Reason: CrashLoop}, Complete, false},
		{"failed stays", Failed, ready, Failed, false},
		{"ahead", Pending, Observation{ObservedGeneration: 2}, 0, true},
		{"invalid", Pending, Observation{ReadyGeneration: 1}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := updates(t, 0, false)
			h.Rollouts[0].Outcome = tt.from
			got, err := h.Settle(tt.o)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Settle accepted %+v", tt.o)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o := got.Current().Outcome; o != tt.want {
				t.Errorf("Outcome = %v, want %v", o, tt.want)
			}
			if h.Rollouts[0].Outcome != tt.from {
				t.Errorf("Settle modified its receiver")
			}
		})
	}

	// An observation of a superseded generation does not settle the current one.
	h := updates(t, 1, false)
	h, err := h.Settle(ready)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Rollout{{1, 1, Create, Superseded}, {2, 2, Update, Pending}}; !slices.Equal(h.Rollouts, want) {
		t.Errorf("Rollouts = %+v, want %+v", h.Rollouts, want)
	}
}

func TestPrune(t *testing.T) {
	tests := []struct {
		name        string
		history     func(*testing.T) History
		kept        []uint64
		removed     []uint64
		generations int
	}{
		{"small", func(t *testing.T) History { return updates(t, 3, true) }, []uint64{1, 2, 3, 4}, nil, 4},
		{"oldest first", func(t *testing.T) History { return updates(t, 10, false) }, []uint64{4, 5, 6, 7, 8, 9, 10, 11}, []uint64{1, 2, 3}, 11},
		{"last complete kept", func(t *testing.T) History { return updates(t, 10, true) }, []uint64{1, 5, 6, 7, 8, 9, 10, 11}, []uint64{2, 3, 4}, 11},
		{"current kept", func(t *testing.T) History {
			h, err := updates(t, 10, true).Rollback(3)
			if err != nil {
				t.Fatal(err)
			}
			return h
		}, []uint64{1, 3, 6, 7, 8, 9, 10, 11}, []uint64{2, 4, 5}, 12},
		{"current pending after complete", func(t *testing.T) History {
			h, err := settled(t, updates(t, 10, false), true).Update(version(0))
			if err != nil {
				t.Fatal(err)
			}
			return h
		}, []uint64{5, 6, 7, 8, 9, 10, 11, 12}, []uint64{1, 2, 3, 4}, 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.history(t)
			before := len(h.Revisions)
			got, removed, err := h.Prune()
			if err != nil {
				t.Fatal(err)
			}
			if err := got.Validate(); err != nil {
				t.Fatal(err)
			}
			if n := numbers(got.Revisions); !slices.Equal(n, tt.kept) {
				t.Errorf("kept %v, want %v", n, tt.kept)
			}
			if n := numbers(removed); !slices.Equal(n, tt.removed) {
				t.Errorf("removed %v, want %v", n, tt.removed)
			}
			if len(got.Rollouts) != tt.generations {
				t.Errorf("kept %d rollouts, want %d", len(got.Rollouts), tt.generations)
			}
			if len(h.Revisions) != before {
				t.Errorf("Prune modified its receiver")
			}
		})
	}
}

// TestPruneProtected checks pruning when the current and last complete
// revisions are both older than every other one, and that numbering continues.
func TestPruneProtected(t *testing.T) {
	h := updates(t, 10, true)
	h, err := h.Rollback(2)
	if err != nil {
		t.Fatal(err)
	}
	h, _, err = h.Prune()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := numbers(h.Revisions), []uint64{1, 2, 6, 7, 8, 9, 10, 11}; !slices.Equal(got, want) {
		t.Errorf("kept %v, want %v", got, want)
	}
	h, err = h.Update(version(20))
	if err != nil {
		t.Fatal(err)
	}
	if c := h.Current(); c.Revision != 12 || c.Generation != 13 {
		t.Errorf("Update after Prune = %+v, want revision 12 generation 13", c)
	}
}

func TestPruneRollouts(t *testing.T) {
	h := updates(t, 40, true)
	got, removed, err := h.Prune()
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	want := append([]uint64{1}, generationsFrom(11, 41)...)
	if g := generations(got.Rollouts); !slices.Equal(g, want) {
		t.Errorf("generations = %v, want %v", g, want)
	}
	if n := numbers(got.Revisions); !slices.Equal(n, []uint64{1, 35, 36, 37, 38, 39, 40, 41}) {
		t.Errorf("revisions = %v", n)
	}
	if len(removed) != 33 {
		t.Errorf("removed %d revisions, want 33", len(removed))
	}
	if c, ok := got.LastComplete(); !ok || c.Generation != 1 {
		t.Errorf("LastComplete = %+v, %v", c, ok)
	}
}

func generationsFrom(lo, hi uint64) []uint64 {
	var out []uint64
	for g := lo; g <= hi; g++ {
		out = append(out, g)
	}
	return out
}

func TestHistoryValidate(t *testing.T) {
	tests := []struct {
		name string
		edit func(*History)
	}{
		{"no revisions", func(h *History) { h.Revisions = nil }},
		{"no rollouts", func(h *History) { h.Rollouts = nil }},
		{"revision zero", func(h *History) { h.Revisions[0].Number = 0 }},
		{"revisions descending", func(h *History) { h.Revisions[0].Number, h.Revisions[1].Number = 2, 1 }},
		{"revisions repeated", func(h *History) { h.Revisions[1].Number = 1 }},
		{"invalid spec", func(h *History) { h.Revisions[0].Spec.Port = 0 }},
		{"spec not normalized", func(h *History) { h.Revisions[0].Spec.Readiness.PeriodSeconds = 0 }},
		{"spec unsorted", func(h *History) {
			s := &h.Revisions[0].Spec
			s.Env = []EnvVar{{"Z", "1"}, {"A", "2"}}
			n, _ := s.Normalize()
			h.Revisions[0].SHA256 = n.digest()
		}},
		{"digest mismatch", func(h *History) { h.Revisions[0].SHA256 = h.Revisions[1].SHA256 }},
		{"generation zero", func(h *History) { h.Rollouts[0].Generation = 0 }},
		{"generations repeated", func(h *History) { h.Rollouts[1].Generation = 1 }},
		{"rollout revision zero", func(h *History) { h.Rollouts[0].Revision = 0 }},
		{"rollout revision unknown", func(h *History) { h.Rollouts[1].Revision = 3 }},
		{"create after first", func(h *History) { h.Rollouts[1].Kind = Create }},
		{"first not create", func(h *History) { h.Rollouts[0].Kind = Update }},
		{"kind zero", func(h *History) { h.Rollouts[1].Kind = 0 }},
		{"kind high", func(h *History) { h.Rollouts[1].Kind = Rollback + 1 }},
		{"outcome zero", func(h *History) { h.Rollouts[1].Outcome = 0 }},
		{"outcome high", func(h *History) { h.Rollouts[1].Outcome = Superseded + 1 }},
		{"pending not current", func(h *History) { h.Rollouts[0].Outcome = Pending }},
		{"rolling not current", func(h *History) { h.Rollouts[0].Outcome = Rolling }},
		{"current superseded", func(h *History) { h.Rollouts[1].Outcome = Superseded }},
		{"last complete not retained", func(h *History) { h.Revisions = h.Revisions[1:] }},
	}
	if err := updates(t, 1, true).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := updates(t, 1, true)
			tt.edit(&h)
			if err := h.Validate(); err == nil {
				t.Fatalf("Validate accepted %+v", h)
			}
		})
	}
}

func TestHistoryCurrentNotRetained(t *testing.T) {
	h := updates(t, 2, true)
	h.Revisions = append(h.Revisions[:1:1], h.Revisions[2])
	h.Rollouts = []Rollout{h.Rollouts[0], {2, 2, Update, Pending}}
	if err := h.Validate(); err == nil || !strings.Contains(err.Error(), "current revision") {
		t.Fatalf("Validate = %v, want current revision error", err)
	}
}

func TestHistoryJSON(t *testing.T) {
	h := settled(t, updates(t, 2, true), false)
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	var got History
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Rollouts, h.Rollouts) || !slices.Equal(numbers(got.Revisions), numbers(h.Revisions)) {
		t.Errorf("round trip = %+v, want %+v", got, h)
	}
}

func TestRevisionCopy(t *testing.T) {
	h := updates(t, 0, false)
	r, ok := h.Revision(1)
	if !ok {
		t.Fatal("revision 1 missing")
	}
	r.Spec.Argv[0] = "/changed"
	if h.Revisions[0].Spec.Argv[0] != "/echo" {
		t.Errorf("Revision shares memory with the history")
	}
	u, err := h.Update(version(1))
	if err != nil {
		t.Fatal(err)
	}
	u.Revisions[0].Spec.Argv[0] = "/changed"
	if h.Revisions[0].Spec.Argv[0] != "/echo" {
		t.Errorf("Update result shares memory with its receiver")
	}
	if _, ok := h.Revision(2); ok {
		t.Errorf("Revision found an unknown number")
	}
}

func TestObservationValidate(t *testing.T) {
	tests := []struct {
		name string
		o    Observation
		ok   bool
	}{
		{"zero", Observation{}, true},
		{"absent", Observation{Absent: true}, true},
		{"ready", Observation{ObservedGeneration: 2, ReadyGeneration: 2, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 3, AvailableReplicas: 3, ReadyEndpoints: 3}, true},
		{"conflict", Observation{Reason: IdentityConflict}, true},
		{"ready ahead", Observation{ObservedGeneration: 1, ReadyGeneration: 2}, false},
		{"absent with state", Observation{Absent: true, ReadyEndpoints: 1}, false},
		{"absent with reason", Observation{Absent: true, Reason: CrashLoop}, false},
		{"negative desired", Observation{DesiredReplicas: -1}, false},
		{"negative updated", Observation{UpdatedReplicas: -1}, false},
		{"negative ready", Observation{ReadyReplicas: -1}, false},
		{"negative available", Observation{AvailableReplicas: -1, ReadyReplicas: -2}, false},
		{"negative endpoints", Observation{ReadyEndpoints: -1}, false},
		{"available above ready", Observation{ReadyReplicas: 1, AvailableReplicas: 2}, false},
		{"invalid reason", Observation{Reason: IdentityConflict + 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.o.Validate(); (err == nil) != tt.ok {
				t.Errorf("Validate = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

func TestRefValidate(t *testing.T) {
	hex := strings.Repeat("0123456789abcdef", 2)
	valid := Ref{Service: hex, Owner: hex, Nonce: hex, Binding: "sha256:" + strings.Repeat("c", 64)}
	tests := []struct {
		name string
		edit func(*Ref)
		ok   bool
	}{
		{"valid", func(*Ref) {}, true},
		{"native", func(r *Ref) { r.Native = Native(strings.Repeat("n", 4<<10)) }, true},
		{"native too long", func(r *Ref) { r.Native = Native(strings.Repeat("n", 4<<10+1)) }, false},
		{"service short", func(r *Ref) { r.Service = hex[:31] }, false},
		{"service uppercase", func(r *Ref) { r.Service = strings.ToUpper(hex) }, false},
		{"owner", func(r *Ref) { r.Owner = "" }, false},
		{"nonce", func(r *Ref) { r.Nonce = hex[:30] + "zz" }, false},
		{"no binding", func(r *Ref) { r.Binding = "" }, false},
		{"long binding", func(r *Ref) { r.Binding = strings.Repeat("b", 257) }, false},
		{"control binding", func(r *Ref) { r.Binding = "a\nb" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := valid
			tt.edit(&r)
			if err := r.Validate(); (err == nil) != tt.ok {
				t.Errorf("Validate = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

func TestPhaseOf(t *testing.T) {
	fresh := updates(t, 0, false)
	complete := updates(t, 0, true)
	updating := updates(t, 1, true)
	failed, err := updating.Settle(Observation{ObservedGeneration: 2, ReadyGeneration: 1, Reason: ImagePull})
	if err != nil {
		t.Fatal(err)
	}
	ready := func(g uint64) *Observation {
		return &Observation{ObservedGeneration: g, ReadyGeneration: g, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2}
	}
	tests := []struct {
		name string
		d    Desired
		h    History
		o    *Observation
		want Phase
	}{
		{"deleting unobserved", Absent, complete, nil, Deleting},
		{"deleting present", Absent, complete, ready(1), Deleting},
		{"deleted", Absent, complete, &Observation{Absent: true}, Deleted},
		{"deleted before first observation", Absent, fresh, &Observation{Absent: true}, Deleted},
		{"deleting not yet observed", Absent, complete, &Observation{ObservedGeneration: 1}, Deleting},
		{"deleting conflict and absence", Absent, complete, &Observation{Absent: true, Reason: IdentityConflict}, Held},
		{"deleting absence with endpoints", Absent, complete, &Observation{Absent: true, ReadyEndpoints: 1}, Held},
		{"deleting absence with generation", Absent, complete, &Observation{Absent: true, ObservedGeneration: 1}, Held},
		{"deleting absence with failure", Absent, complete, &Observation{Absent: true, Reason: CrashLoop}, Held},
		{"deleting absence with future reason", Absent, complete, &Observation{Absent: true, Reason: IdentityConflict + 1}, Held},
		{"deleting conflict", Absent, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, ReadyEndpoints: 2, Reason: IdentityConflict}, Held},
		{"deleting bare conflict", Absent, fresh, &Observation{Reason: IdentityConflict}, Held},
		{"deleting future reason", Absent, complete, &Observation{Reason: IdentityConflict + 1}, Held},
		{"deleting negative count", Absent, complete, &Observation{ReadyReplicas: -1}, Held},
		{"deleting ready ahead", Absent, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 2}, Held},
		{"deleting available above ready", Absent, complete, &Observation{ReadyReplicas: 1, AvailableReplicas: 2}, Held},
		{"invalid intent unobserved", 0, complete, nil, Held},
		{"invalid intent", 0, complete, ready(1), Held},
		{"created not observed", Present, fresh, nil, Creating},
		{"spec only observation", Present, fresh, &Observation{}, Creating},
		{"observed not ready", Present, fresh, &Observation{ObservedGeneration: 1}, Creating},
		{"first ready", Present, fresh, ready(1), Ready},
		{"ready", Present, complete, ready(1), Ready},
		{"unobserved after complete", Present, complete, nil, Held},
		{"missing", Present, complete, &Observation{Absent: true}, Held},
		{"conflict", Present, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, ReadyEndpoints: 2, Reason: IdentityConflict}, Held},
		{"ahead", Present, complete, ready(2), Held},
		{"invalid observation", Present, complete, &Observation{ReadyGeneration: 1}, Held},
		{"negative count", Present, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, ReadyEndpoints: -1}, Held},
		{"future reason", Present, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, ReadyEndpoints: 2, Reason: IdentityConflict + 1}, Held},
		{"absence with endpoints", Present, complete, &Observation{Absent: true, ReadyEndpoints: 1}, Held},
		{"bare conflict", Present, fresh, &Observation{Reason: IdentityConflict}, Held},
		{"degraded", Present, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 2, ReadyReplicas: 1, AvailableReplicas: 1, ReadyEndpoints: 1}, Degraded},
		{"no endpoints", Present, complete, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}, Unavailable},
		{"old revision serving", Present, updating, &Observation{ObservedGeneration: 2, ReadyGeneration: 1, DesiredReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2}, Progressing},
		{"update not observed", Present, updating, ready(1), Progressing},
		{"update unavailable", Present, updating, &Observation{ObservedGeneration: 2, ReadyGeneration: 1}, Unavailable},
		{"update failing", Present, updating, &Observation{ObservedGeneration: 2, ReadyGeneration: 1, ReadyEndpoints: 2, Reason: CrashLoop}, RolloutFailed},
		{"stale failure", Present, updating, &Observation{ObservedGeneration: 1, ReadyGeneration: 1, ReadyEndpoints: 2, Reason: CrashLoop}, Progressing},
		{"update ready", Present, updating, ready(2), Ready},
		{"failed", Present, failed, &Observation{ObservedGeneration: 2, ReadyGeneration: 1, ReadyEndpoints: 2}, RolloutFailed},
		{"failed then ready", Present, failed, ready(2), RolloutFailed},
		{"first failing", Present, fresh, &Observation{ObservedGeneration: 1, Reason: InvalidImage}, RolloutFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseOf(tt.d, tt.h, tt.o); got != tt.want {
				t.Errorf("PhaseOf = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStrings(t *testing.T) {
	tests := []struct {
		v    fmt.Stringer
		want string
	}{
		{Create, "create"},
		{Rollback, "rollback"},
		{Kind(0), "invalid"},
		{Rollback + 1, "invalid"},
		{Pending, "pending"},
		{Superseded, "superseded"},
		{Outcome(0), "invalid"},
		{Creating, "creating"},
		{RolloutFailed, "rollout-failed"},
		{Held, "held"},
		{Held + 1, "invalid"},
		{NoReason, "none"},
		{ProgressDeadline, "progress-deadline"},
		{IdentityConflict, "identity-conflict"},
		{IdentityConflict + 1, "invalid"},
	}
	for _, tt := range tests {
		if got := tt.v.String(); got != tt.want {
			t.Errorf("%#v.String() = %q, want %q", tt.v, got, tt.want)
		}
	}
}
