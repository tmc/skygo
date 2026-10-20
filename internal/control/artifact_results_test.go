package control

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// These DTO tests validate presentation/copy rules, never manufacture stored
// results, byte grants, native closure or a workspace producer receipt.
func TestArtifactResultProjectionCopiesAndRefusesContradictions(t *testing.T) {
	id := strings.Repeat("a", 32)
	ref := store.ArtifactRef{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1}
	input := store.ArtifactAttemptResult{State: "available", AttemptID: id, Reference: &ref}
	got, err := copyArtifactResult(input, id)
	if err != nil {
		t.Fatal(err)
	}
	got.Reference.SHA256 = strings.Repeat("d", 64)
	if input.Reference.SHA256 != strings.Repeat("c", 64) {
		t.Fatal("result Ref aliased caller snapshot")
	}
	for _, state := range []string{"absent", "expired", "held"} {
		result, err := copyArtifactResult(store.ArtifactAttemptResult{State: state, AttemptID: id}, id)
		if err != nil || result.Reference != nil {
			t.Fatalf("%s=%+v %v", state, result, err)
		}
		if _, err = copyArtifactResult(store.ArtifactAttemptResult{State: state, AttemptID: id, Reference: &ref}, id); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("%s carried reference: %v", state, err)
		}
	}
	for _, bad := range []store.ArtifactAttemptResult{
		{State: "available", AttemptID: id},
		{State: "invented", AttemptID: id},
		{State: "absent", AttemptID: strings.Repeat("e", 32)},
		{State: "available", AttemptID: id, Reference: &store.ArtifactRef{ID: ref.ID, SHA256: ref.SHA256, Size: math.MaxUint64}},
	} {
		if _, err := copyArtifactResult(bad, id); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("invalid DTO accepted: %+v %v", bad, err)
		}
	}
}

func TestArtifactResultCursorCanonicalBound(t *testing.T) {
	cursor := artifactPruneCursor{Version: 1, Kind: "artifact-prune", Owner: strings.Repeat("a", 32), Root: strings.Repeat("b", 32), Principal: strings.Repeat("p", 64), Global: true, Scoped: true, Lineage: strings.Repeat("c", 32), Attempt: math.MaxUint32, Before: "9999-12-31T23:59:59.999999999Z", Execute: true, After: math.MaxUint64 - 1, Max: math.MaxUint64}
	token, err := encodeCursor(cursor, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) > 683 {
		t.Fatal("encoded cursor exceeds bound")
	}
	var decoded artifactPruneCursor
	if err := decodeCursor(token, &decoded, 512); err != nil || decoded != cursor {
		t.Fatalf("cursor round trip: %+v %v", decoded, err)
	}
}

func TestArtifactResultAuthBeforeStore(t *testing.T) {
	engine := auth.NewLegacy("secret")
	c := &Control{engine: engine}
	id := strings.Repeat("a", 32)
	if _, err := c.ArtifactResultFor(auth.Actor{}, id, id); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("unauthenticated: %v", err)
	}
	actor, err := engine.Authenticate("Bearer secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.ArtifactResultFor(actor, id, id); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("absent service: %v", err)
	}
	// Nil Store is a tripwire; neither failed admission may reach it.
}
