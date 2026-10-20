package rpc

import (
	"errors"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// These are converter vectors, not actual publication, expiry or writer proofs.
func TestArtifactResultWireStates(t *testing.T) {
	id := strings.Repeat("a", 32)
	ref := store.ArtifactRef{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1024}
	for _, tt := range []struct {
		state     string
		reference *store.ArtifactRef
		want      v1.ArtifactResultState
	}{
		{"absent", nil, v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT},
		{"held", nil, v1.ArtifactResultState_ARTIFACT_RESULT_STATE_HELD},
		{"expired", nil, v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED},
		{"available", &ref, v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE},
	} {
		t.Run(tt.state, func(t *testing.T) {
			out, err := wireArtifactResult(store.ArtifactAttemptResult{State: tt.state, AttemptID: id, Reference: tt.reference}, id)
			if err != nil || out.State != tt.want || out.AttemptId != id {
				t.Fatalf("result=%+v error=%v", out, err)
			}
			if tt.reference == nil && out.Reference != nil {
				t.Fatal("unreadable state carried a reference")
			}
			if tt.reference != nil {
				if out.Reference == nil || out.Reference.Id != ref.ID || out.Reference.Size != int64(ref.Size) {
					t.Fatal(out)
				}
				out.Reference.Id = "changed"
				if ref.ID != strings.Repeat("b", 32) {
					t.Fatal("converter aliases input")
				}
			}
		})
	}
	for _, in := range []store.ArtifactAttemptResult{
		{State: "available", AttemptID: id},
		{State: "held", AttemptID: id, Reference: &ref},
		{State: "expired", AttemptID: id, Reference: &ref},
		{State: "absent", AttemptID: id, Reference: &ref},
		{State: "unknown", AttemptID: id},
		{State: "absent", AttemptID: strings.Repeat("d", 32)},
	} {
		if out, err := wireArtifactResult(in, id); out != nil || !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("invalid result=%+v error=%v", out, err)
		}
	}
	for _, size := range []uint64{0, (32 << 20) + 1, ^uint64(0)} {
		bad := ref
		bad.Size = size
		if out, err := wireArtifactResult(store.ArtifactAttemptResult{State: "available", AttemptID: id, Reference: &bad}, id); out != nil || !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("invalid size %d: %+v %v", size, out, err)
		}
	}
}
