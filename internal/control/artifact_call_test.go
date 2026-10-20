package control

import (
	"errors"
	"testing"
)

func artifactCallControl() *Control {
	return &Control{shutdown: newShutdownState(), artifacts: &artifactService{calls: make(map[*ArtifactCall]struct{}), uploads: make(map[string]*ArtifactCall), downloads: make(map[*ArtifactDownload]struct{}), workspaceIO: make(map[*artifactIO]struct{}), changed: make(chan struct{}), terminal: make(chan struct{})}}
}

func TestArtifactCallRetainsBothOwners(t *testing.T) {
	c := artifactCallControl()
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
	}
	finish, err := c.beginArtifactCall(calls[0], "upload")
	if err != nil {
		t.Fatal(err)
	}
	calls[0].Done()
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("early HTTP completion released real I/O: %v", err)
	}
	if _, err := c.beginArtifactCall(calls[0], "other"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("completed HTTP call reused: %v", err)
	}
	finish()
	replacement, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		call.Done()
	}
	replacement.Done()
	if len(c.artifacts.calls) != 0 || len(c.artifacts.uploads) != 0 {
		t.Fatal("settled ownership remained")
	}
}

func TestArtifactCallRetainsHTTPUpload(t *testing.T) {
	c := artifactCallControl()
	first, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	finish, err := c.beginArtifactCall(first, "upload")
	if err != nil {
		t.Fatal(err)
	}
	finish()
	second, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.beginArtifactCall(second, "upload"); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("HTTP-pending upload admitted concurrent call: %v", err)
	}
	second.Done()
	first.Done()
	third, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	end, err := c.beginArtifactCall(third, "upload")
	if err != nil {
		t.Fatal(err)
	}
	end()
	third.Done()
}

func TestArtifactCallRejectsReuseAndForeignControl(t *testing.T) {
	c := artifactCallControl()
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := artifactCallControl().beginArtifactCall(call, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("foreign service admitted: %v", err)
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.beginArtifactCall(call, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("concurrent call reuse: %v", err)
	}
	end()
	if _, err := c.beginArtifactCall(call, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("post-I/O call reuse: %v", err)
	}
	call.Done()
	call.Done()
	if _, err := c.beginArtifactCall(call, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("released call reuse: %v", err)
	}
	var zero ArtifactCall
	zero.Done()
	if _, err := c.beginArtifactCall(&zero, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero call admitted: %v", err)
	}
}
