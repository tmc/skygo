package environmentcontrol_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

// noEnvironments checks that who has no environment.
func (f *fixture) noEnvironments(t *testing.T, who string) {
	t.Helper()
	p, err := f.c.List(f.actors[who], 0, 0)
	if err != nil || len(p.Environments) != 0 {
		t.Fatalf("environments = %v, %v; want none", p.Environments, err)
	}
}

// TestPlan checks that a reader's plan is the runtime's answer for the
// normalized spec and the digest Create records, and that it accepts nothing.
func TestPlan(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("env")
	spec.NodeSelector = map[string]string{}
	p, err := f.c.Plan(f.actors["reader"], spec)
	if err != nil {
		t.Fatal(err)
	}
	if p.Support != environmentcontrol.Supported || p.Artifacts != environmentcontrol.FactNotApplicable {
		t.Fatalf("plan = %v %v, want Supported, FactNotApplicable", p.Support, p.Artifacts)
	}
	if p.Spec.NodeSelector != nil {
		t.Fatal("plan spec is not normalized")
	}
	if plans, _, _, _ := f.runtime.counts(); plans != 1 {
		t.Fatalf("runtime plans = %d, want 1", plans)
	}
	f.noEnvironments(t, "reader")

	v := f.create(t, "alice", "env", spec)
	e, err := f.store.Environment(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.SpecDigest != e.SpecDigest {
		t.Fatal("plan digest differs from the one Create recorded")
	}
	raw, err := environment.Canonical(p.Spec)
	if err != nil || sha256.Sum256(raw) != p.SpecDigest {
		t.Fatalf("plan digest is not its spec's: %v", err)
	}
}

// TestPlanRefusals checks the answers that are not Supported. None reaches
// acceptance, and an Unsupported plan carries no runtime text.
func TestPlanRefusals(t *testing.T) {
	failure := errors.New("injected plan failure naming profile p-secret")
	invalid := testSpec("env")
	invalid.CPUMillis = 0
	tests := []struct {
		name    string
		spec    environment.Spec
		planErr error
		support environmentcontrol.Support
		wantErr error
		plans   int
	}{
		{"invalid", invalid, nil, 0, environmentcontrol.ErrInvalid, 0},
		{"unsupported", testSpec("env"), fmt.Errorf("%w: no profile p-secret", environmentcontrol.ErrUnsupported), environmentcontrol.Unsupported, nil, 1},
		{"runtime error", testSpec("env"), failure, 0, failure, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.runtime.mu.Lock()
			f.runtime.planErr = tt.planErr
			f.runtime.mu.Unlock()
			p, err := f.c.Plan(f.actors["alice"], tt.spec)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("plan = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil || p.Support != tt.support {
				t.Fatalf("plan = %v, %v; want %v", p.Support, err, tt.support)
			}
			if strings.Contains(fmt.Sprint(p), "p-secret") || strings.Contains(fmt.Sprint(p), "fake-binding") {
				t.Fatal("plan carries runtime text")
			}
			if plans, _, _, _ := f.runtime.counts(); plans != tt.plans {
				t.Fatalf("runtime plans = %d, want %d", plans, tt.plans)
			}
			f.noEnvironments(t, "alice")
		})
	}
}

// TestPlanUnauthenticated checks that Plan needs an authenticated actor.
func TestPlanUnauthenticated(t *testing.T) {
	f := newFixture(t)
	if _, err := f.c.Plan(auth.Actor{}, testSpec("env")); err == nil {
		t.Fatal("plan without an actor = nil, want an error")
	}
	if plans, _, _, _ := f.runtime.counts(); plans != 0 {
		t.Fatalf("runtime plans = %d, want 0", plans)
	}
}

// TestPlanArtifacts checks a spec with artifacts. Without materialization it
// is Unsupported before the runtime is asked; with it, it is the runtime's
// answer. Either way the artifacts are unknown and nothing is read, opened or
// pinned.
func TestPlanArtifacts(t *testing.T) {
	t.Run("plain runtime", func(t *testing.T) {
		f := newFixture(t)
		spec := testSpec("env")
		spec.Artifacts = []environment.ArtifactMount{{ID: strings.Repeat("c", 32), SHA256: strings.Repeat("d", 64), Size: 1, MountPath: "/skygo/artifacts/in"}}
		p, err := f.c.Plan(f.actors["alice"], spec)
		if err != nil || p.Support != environmentcontrol.Unsupported || p.Artifacts != environmentcontrol.FactUnknown {
			t.Fatalf("plan = %v %v, %v; want Unsupported, FactUnknown", p.Support, p.Artifacts, err)
		}
		if plans, _, _, _ := f.runtime.counts(); plans != 0 {
			t.Fatalf("runtime plans = %d, want 0", plans)
		}
	})
	t.Run("materializer", func(t *testing.T) {
		f := newArtifactFixture(t, knownProducer)
		p, err := f.c.Plan(f.actors["alice"], f.spec("env", "/skygo/artifacts/a"))
		if err != nil || p.Support != environmentcontrol.Supported || p.Artifacts != environmentcontrol.FactUnknown {
			t.Fatalf("plan = %v %v, %v; want Supported, FactUnknown", p.Support, p.Artifacts, err)
		}
		if n := f.faults.counts(); len(n.begins) != 0 || n.dropped != 0 {
			t.Fatal("plan began an artifact read")
		}
		if opened, _ := f.opener.counts(); opened != 0 {
			t.Fatalf("opened = %d, want 0", opened)
		}
		f.noEnvironments(t, "alice")
	})
}
