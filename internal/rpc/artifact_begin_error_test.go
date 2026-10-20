package rpc

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// Conversion vectors only: these are not actual post-row fault injection or
// evidence that any descriptor closed. Real cold refusal/HTTP tests are separate.
func TestArtifactBeginAdviceRequiresPrivatePreAdmissionCause(t *testing.T) {
	for _, err := range []error{
		artifact.ErrFilesBusy,
		artifact.ErrFilesClosed,
		artifact.ErrFileConflict,
		artifact.ErrFileCloseUncertain,
		store.ErrArtifactConflict,
		&store.ArtifactCapacityError{Domain: "pending uploads", Used: 4, Limit: 4},
		auth.ErrPermissionDenied,
		context.Canceled,
		control.ErrArtifactBusy,
		errors.New("artifact operation busy"),
	} {
		for _, converted := range []error{artifactBeginError(err), artifactError(err)} {
			var wire *connect.Error
			if !errors.As(converted, &wire) {
				t.Fatalf("not a Connect error: %v", converted)
			}
			if len(wire.Meta().Values("Skygo-Artifact-Begin-Not-Started")) != 0 {
				t.Fatalf("unclassified error acquired non-admission advice: %v", err)
			}
		}
	}
}
