package control

import (
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

// These tests verify Control projection of the concrete Files classification.
// The sentinel here is synthetic, not an actual descriptor-closure receipt.
// Actual Files close-fault tests belong to the frozen Files dependency; neither
// source layer claims an end-to-end HTTP/Store/filesystem fault execution.
func TestArtifactUncertainCloseRetainsOriginalCall(t *testing.T) {
	for _, order := range []string{"http first", "io first"} {
		t.Run(order, func(t *testing.T) {
			c := artifactCallControl()
			call, err := c.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			end, err := c.beginArtifactCall(call, "upload")
			if err != nil {
				t.Fatal(err)
			}
			actual := errors.Join(errors.New("original close failure"), artifact.ErrFileCloseUncertain)
			if got := c.artifactFileError(call, actual); got != actual {
				t.Fatal("original error identity lost")
			}
			if order == "http first" {
				call.Done()
				end()
			} else {
				end()
				call.Done()
			}
			call.Done()
			if call.ioActive {
				t.Fatal("actual synchronous return misreported as still executing")
			}
			if call.fileError != actual || c.artifacts.uploads["upload"] != call || len(c.artifacts.calls) != 1 {
				t.Fatal("original uncertain owner released")
			}
			next, err := c.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.beginArtifactCall(next, "upload"); !errors.Is(err, ErrArtifactBusy) {
				t.Fatalf("replacement upload admitted: %v", err)
			}
			next.Done()
			if _, err := c.beginArtifactCall(call, "upload"); !errors.Is(err, ErrInvalid) {
				t.Fatalf("original call reused: %v", err)
			}
		})
	}
}

func TestArtifactFourUncertainOriginalOwners(t *testing.T) {
	c := artifactCallControl()
	for _, id := range []string{"one", "two", "three", "four"} {
		call, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		end, err := c.beginArtifactCall(call, id)
		if err != nil {
			t.Fatal(err)
		}
		c.artifactFileError(call, artifact.ErrFileCloseUncertain)
		call.Done()
		end()
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("fifth original owner: %v", err)
	}
	if len(c.artifacts.uploads) != 4 || len(c.artifacts.calls) != 4 {
		t.Fatal("HTTP completion recycled uncertain owner")
	}
}

func TestArtifactPositivelyClosedErrorsRemainRecoverable(t *testing.T) {
	for _, failure := range []error{nil, errors.New("validation failed after successful close"), errors.New("write failed after successful close"), errors.New("sync failed after successful close"), artifact.ErrFilesBusy} {
		c := artifactCallControl()
		call, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		end, err := c.beginArtifactCall(call, "upload")
		if err != nil {
			t.Fatal(err)
		}
		if got := c.artifactFileError(call, failure); got != failure {
			t.Fatal("ordinary error replaced")
		}
		end()
		call.Done()
		if len(c.artifacts.calls) != 0 || len(c.artifacts.uploads) != 0 {
			t.Fatalf("ordinary error permanently held: %v", failure)
		}
		next, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		done, err := c.beginArtifactCall(next, "upload")
		if err != nil {
			t.Fatal(err)
		}
		done()
		next.Done()
	}
}

func TestArtifactSuccessfulFileThenCASFailureDoesNotHold(t *testing.T) {
	c := artifactCallControl()
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(call, "upload")
	if err != nil {
		t.Fatal(err)
	}
	// Real call sites classify only the Files result. Later auth/Store errors do
	// not relabel a nil Files result as uncertain descriptor ownership.
	if err := c.artifactFileError(call, nil); err != nil {
		t.Fatal(err)
	}
	end()
	call.Done()
	if call.fileError != nil || len(c.artifacts.uploads) != 0 {
		t.Fatal("successful Files join poisoned by later CAS outcome")
	}
}

func TestArtifactCloseErrorCannotMutateForeignOrReleasedCall(t *testing.T) {
	c := artifactCallControl()
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(call, "upload")
	if err != nil {
		t.Fatal(err)
	}
	artifactCallControl().artifactFileError(call, artifact.ErrFileCloseUncertain)
	if call.fileError != nil {
		t.Fatal("foreign service mutated original owner")
	}
	end()
	call.Done()
	c.artifactFileError(call, artifact.ErrFileCloseUncertain)
	if call.fileError != nil || len(c.artifacts.calls) != 0 {
		t.Fatal("released call resurrected")
	}
}
