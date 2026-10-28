package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

// MODELED. These controls run the environment runtime against environmentAPIModel. They
// check that a spec with artifacts is refused, with no Pod write, by a
// runtime that cannot materialize them on the target.

func envGuardSpec() environment.Spec {
	s := envRTSpec()
	sum := sha256.Sum256([]byte("one"))
	s.Artifacts = []environment.ArtifactMount{{ID: "11111111111111111111111111111111", SHA256: hex.EncodeToString(sum[:]),
		Size: 3, MountPath: "/skygo/artifacts/a.conf"}}
	return s
}

func TestEnvironmentRuntimePlanRefusesArtifacts(t *testing.T) {
	e := newEnvRT(t, newFakeAPI(t, execProtocol).config())
	spec := envGuardSpec()
	binding, native, err := e.rt.Plan("alice", spec, envRTDigest(t, spec))
	if !errors.Is(err, environmentcontrol.ErrUnsupported) || binding != "" || native != nil {
		t.Fatalf("Plan = %q, %q, %v; want ErrUnsupported", binding, native, err)
	}
	// The same spec without artifacts is planned.
	if _, _, err := e.rt.Plan("alice", envRTSpec(), envRTDigest(t, envRTSpec())); err != nil {
		t.Fatal(err)
	}
}

// TestEnvironmentRuntimeEnsureRefusesArtifacts gives Ensure a Ref whose
// spec has artifacts but whose native state was planned without them, as
// from an older Plan. Ensure declares and sends nothing.
func TestEnvironmentRuntimeEnsureRefusesArtifacts(t *testing.T) {
	e := newEnvRT(t, newFakeAPI(t, execProtocol).config())
	e.ref.Spec = envGuardSpec()
	e.ref.SpecDigest = envRTDigest(t, e.ref.Spec)
	for range 2 {
		_, err := e.rt.Ensure(context.Background(), e.ref)
		if err == nil || errors.Is(err, environmentcontrol.ErrWritePending) {
			t.Fatalf("Ensure = %v; want a refusal", err)
		}
	}
	if c := e.calls("create", "pods"); len(c) != 0 {
		t.Fatalf("%d pod creates", len(c))
	}
	if e.pod() != nil {
		t.Fatal("pod created")
	}
}
