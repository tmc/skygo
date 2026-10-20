package control

import (
	"errors"
	"github.com/tmc/skygo/internal/artifact"
	"testing"
)

// These tests exercise ownership bookkeeping only, not filesystem/Store proof.
func TestArtifactDownloadHTTPAndIOOwners(t *testing.T) {
	c := artifactCallControl()
	d := &ArtifactDownload{control: c}
	c.artifacts.downloads = map[*ArtifactDownload]struct{}{d: {}}
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		t.Fatal(err)
	}
	c.artifacts.mu.Lock()
	d.owners++
	call.download = d
	c.artifacts.mu.Unlock()
	call.Done()
	if d.owners != 1 {
		t.Fatal("early HTTP completion released live read")
	}
	end()
	if d.owners != 0 {
		t.Fatal("actual I/O completion did not join HTTP owner")
	}
	call.Done()
	if d.owners != 0 {
		t.Fatal("duplicate HTTP completion released twice")
	}
}

func TestArtifactDownloadRetainsResponseOwner(t *testing.T) {
	c := artifactCallControl()
	d := &ArtifactDownload{control: c}
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		t.Fatal(err)
	}
	c.artifacts.mu.Lock()
	d.owners++
	call.download = d
	c.artifacts.mu.Unlock()
	end()
	if d.owners != 1 {
		t.Fatal("I/O return released outstanding response")
	}
	if _, err = c.beginArtifactCall(call, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("reused call: %v", err)
	}
	call.Done()
	if d.owners != 0 {
		t.Fatal("completed response retained joined owner")
	}
}

func TestArtifactDownloadDescriptorBoundIndependentOfCalls(t *testing.T) {
	c := artifactCallControl()
	reserve := func() (*ArtifactDownload, error) {
		call, err := c.TryArtifactCall()
		if err != nil {
			return nil, err
		}
		end, err := c.beginArtifactCall(call, "")
		if err != nil {
			call.Done()
			return nil, err
		}
		d, err := c.reserveArtifactDownload("owner", artifact.Ref{}, call)
		end()
		call.Done()
		return d, err
	}
	held := make([]*ArtifactDownload, 32)
	for i := range held {
		var err error
		held[i], err = reserve()
		if err != nil {
			t.Fatalf("descriptor %d: %v", i, err)
		}
	}
	if _, err := reserve(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("33rd descriptor: %v", err)
	}
	// HTTP completion without an attached descriptor cannot release another owner.
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	call.Done()
	if len(c.artifacts.downloads) != 32 {
		t.Fatal("unrelated response released descriptor")
	}
	c.artifacts.mu.Lock()
	delete(c.artifacts.downloads, held[0])
	c.artifacts.mu.Unlock()
	if _, err = reserve(); err != nil {
		t.Fatal(err)
	}
}
