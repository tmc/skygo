package store

import (
	"fmt"
	"math"
	"testing"
)

// TestArtifactRequiredSize checks the pure counter on in-memory states. The
// states are MODELED: they are not validated, and an overflowing size cannot
// pass ordinary artifact admission. Environment staging keeps its own grant,
// growth-only bound and prepared-change gates; see environment_artifacts_test.
func TestArtifactRequiredSize(t *testing.T) {
	ref := func(id string, size uint64) ArtifactRef { return ArtifactRef{ID: id, SHA256: id, Size: size} }
	objects := func(refs ...ArtifactRef) map[string]artifactObject {
		m := make(map[string]artifactObject)
		for _, r := range refs {
			m[r.ID] = artifactObject{Ref: r, State: "complete"}
		}
		return m
	}
	a, b := ref("a", 10), ref("b", 20)
	many := &artifactState{Objects: map[string]artifactObject{}, EnvironmentPins: map[string]artifactEnvironmentPin{}}
	for i := range maxRequiredObjects + 1 {
		r := ref(fmt.Sprintf("o%03d", i), 5<<20)
		many.Objects[r.ID] = artifactObject{Ref: r, State: "complete"}
		many.EnvironmentPins[r.ID] = artifactEnvironmentPin{Ref: r}
	}
	for _, tt := range []struct {
		name          string
		st            *artifactState
		objects, size uint64
		corrupt       bool
	}{
		{"empty", &artifactState{}, 0, 0, false},
		{"unrequired object", &artifactState{Objects: objects(a)}, 0, 0, false},
		{"nil workflow ref", &artifactState{Objects: objects(a), WorkflowInputs: map[string]workflowInput{"edge": {}}}, 0, 0, false},
		{"environment only", &artifactState{Objects: objects(a), EnvironmentPins: map[string]artifactEnvironmentPin{"e": {Ref: a}}}, 1, 10, false},
		{"aliases", &artifactState{
			Objects:         objects(a, b),
			JobInputs:       map[string]artifactPin{"j": {Ref: a}},
			ManualInputs:    map[string]artifactPin{"m": {Ref: a}},
			Results:         map[string]artifactResult{"r": {Ref: a}},
			WorkflowInputs:  map[string]workflowInput{"w": {Ref: &a}, "nil": {}},
			EnvironmentPins: map[string]artifactEnvironmentPin{"e1": {Ref: a}, "e2": {Ref: a}, "e3": {Ref: b}},
		}, 2, 30, false},
		// The retained helper checks presence only; completeness and origin
		// belong to the state's own validation.
		{"incomplete object", &artifactState{Objects: map[string]artifactObject{"a": {Ref: a, State: "uploading"}}, JobInputs: map[string]artifactPin{"j": {Ref: a}}}, 1, 10, false},
		// No absolute 128-object or 512MiB bound is applied inside the helper.
		{"over absolute bounds", many, maxRequiredObjects + 1, (maxRequiredObjects + 1) * 5 << 20, false},
		{"missing object", &artifactState{Objects: objects(a), EnvironmentPins: map[string]artifactEnvironmentPin{"e": {Ref: b}}}, 0, 0, true},
		{"missing result object", &artifactState{Results: map[string]artifactResult{"r": {Ref: a}}}, 0, 0, true},
		{"overflow", &artifactState{
			Objects:   objects(ref("max", math.MaxUint64), ref("one", 1)),
			JobInputs: map[string]artifactPin{"max": {Ref: ref("max", math.MaxUint64)}, "one": {Ref: ref("one", 1)}},
		}, 0, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objects, size, err := artifactRequiredSize(tt.st)
			if tt.corrupt {
				if err == nil || err.Error() != artifactCorrupt().Error() {
					t.Fatalf("got %d %d %v, want corruption", objects, size, err)
				}
				return
			}
			if err != nil || objects != tt.objects || size != tt.size {
				t.Fatalf("got %d %d %v, want %d %d", objects, size, err, tt.objects, tt.size)
			}
		})
	}
}
