package control

import (
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

func artifactTerminalClosed(c *Control) bool {
	select {
	case <-c.artifacts.terminal:
		return true
	default:
		return false
	}
}

func TestArtifactCombinedFourOriginalOwners(t *testing.T) {
	c := artifactCallControl()
	first, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	one, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	two, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("fifth HTTP owner: %v", err)
	}
	if _, err := c.tryArtifactIO(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("fifth internal owner: %v", err)
	}
	one.finish(nil)
	one.finish(nil)
	replacement, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	c.StopArtifactAdmission()
	if c.StorageReady() {
		t.Fatal("stopped artifact admission reported ready")
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("HTTP admission after stop: %v", err)
	}
	if _, err := c.tryArtifactIO(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("internal admission after stop: %v", err)
	}
	if artifactTerminalClosed(c) {
		t.Fatal("stop fabricated owner joins")
	}
	first.Done()
	second.Done()
	replacement.Done()
	two.finish(nil)
	if err := c.DrainArtifacts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !artifactTerminalClosed(c) {
		t.Fatal("positive original joins did not close terminal")
	}
}

func TestArtifactDrainBothCompletionOrders(t *testing.T) {
	for _, order := range []string{"HTTP first", "I/O first"} {
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
			c.StopArtifactAdmission()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := c.DrainArtifacts(ctx); !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.Canceled) {
				t.Fatalf("unjoined drain: %v", err)
			}
			if len(c.artifacts.calls) != 1 || c.artifacts.uploads["upload"] != call || artifactTerminalClosed(c) {
				t.Fatal("cancellation erased original owner")
			}
			if order == "HTTP first" {
				call.Done()
			} else {
				end()
			}
			if artifactTerminalClosed(c) {
				t.Fatal("partial completion reported terminal")
			}
			if order == "HTTP first" {
				end()
			} else {
				call.Done()
			}
			if err := c.DrainArtifacts(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.StopArtifactAdmission()
			call.Done()
			if !artifactTerminalClosed(c) {
				t.Fatal("joined terminal not stable")
			}
		})
	}
}

func TestArtifactInternalUncertainJoinCannotBeReleasedAgain(t *testing.T) {
	c := artifactCallControl()
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	original := errors.Join(errors.New("original internal close failure"), artifact.ErrFileCloseUncertain)
	owner.finish(original)
	owner.finish(nil)
	if owner.fileError != original || len(c.artifacts.workspaceIO) != 1 {
		t.Fatal("uncertain internal owner replaced or released")
	}
	if err := c.DrainArtifacts(context.Background()); !errors.Is(err, ErrUnavailable) || !errors.Is(err, artifact.ErrFileCloseUncertain) {
		t.Fatalf("unknown drain: %v", err)
	}
	if artifactTerminalClosed(c) {
		t.Fatal("uncertain original descriptor declared joined")
	}
}

func TestArtifactOriginalHTTPCompletionStillRequiredForDrain(t *testing.T) {
	c := artifactCallControl()
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		t.Fatal(err)
	}
	end()
	// This models an ordinary response that has not finished yet. The later
	// Done below represents its positive completion, not recovery from a failed
	// body Close. Actual sticky HTTP-error ownership is the separate RPC gate.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.DrainArtifacts(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if len(c.artifacts.calls) != 1 || artifactTerminalClosed(c) {
		t.Fatal("missing HTTP join discarded")
	}
	call.Done()
	if err := c.DrainArtifacts(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactUnknownDownloadBlocksDrainWithoutStoreCall(t *testing.T) {
	c := artifactCallControl()
	original := errors.New("original descriptor uncertainty")
	d := &ArtifactDownload{control: c, uncertain: true, closeError: original}
	c.artifacts.downloads[d] = struct{}{}
	// Nil Store is a tripwire: an unknown original cannot enter a cleanup txn.
	if err := c.DrainArtifacts(context.Background()); !errors.Is(err, ErrUnavailable) || !errors.Is(err, original) {
		t.Fatalf("unknown descriptor: %v", err)
	}
	if len(c.artifacts.downloads) != 1 || d.finished || artifactTerminalClosed(c) {
		t.Fatal("unknown descriptor credited or discarded")
	}
}

func TestArtifactStopBetweenAdmissionAndBegin(t *testing.T) {
	c := artifactCallControl()
	original, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	c.StopArtifactAdmission()
	if artifactTerminalClosed(c) {
		t.Fatal("predecode owner discarded at stop")
	}
	end, err := c.beginArtifactCall(original, "")
	if err != nil {
		t.Fatalf("admitted original refused: %v", err)
	}
	d, err := c.reserveArtifactDownload("owner", artifact.Ref{}, original)
	if err != nil {
		t.Fatalf("admitted descriptor reservation refused: %v", err)
	}
	if _, err := c.reserveArtifactDownload("owner", artifact.Ref{}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unadmitted descriptor: %v", err)
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("new call after stop: %v", err)
	}
	if _, err := c.beginArtifactCall(original, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("admitted call reused: %v", err)
	}
	end()
	original.Done()
	// This reservation deliberately never acquired a physical descriptor or a
	// Store lease. Keep it unknown: no fabricated close/finish is used by the test.
	d.uncertain = true
	if err := c.DrainArtifacts(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unknown reservation: %v", err)
	}
	if artifactTerminalClosed(c) {
		t.Fatal("unknown reserved owner erased")
	}
}
