package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// MODELED ownership/tuple vectors only. No Files method may run with these
// invented inode values; actual unlink/counter tests live in store_test.
func artifactDeleteVector() (*Control, store.ArtifactDeleteWork) {
	c := artifactCallControl()
	c.artifacts.root = artifact.RootFacts{ID: strings.Repeat("a", 32), Volume: 7}
	w := store.ArtifactDeleteWork{ID: strings.Repeat("b", 32), Kind: "object", Owner: "legacy", RootID: c.artifacts.root.ID, Revision: 1,
		Ref: store.ArtifactRef{ID: strings.Repeat("d", 32), SHA256: strings.Repeat("e", 64), Size: 1024}, File: store.ArtifactFile{Device: 7, Inode: 9}}
	raw, _ := json.Marshal(w)
	w.Digest = sha256.Sum256(raw)
	return c, w
}

func TestArtifactDeletePrincipalIsNotAJobID(t *testing.T) {
	for _, principal := range []string{"legacy", "alice"} {
		t.Run(principal, func(t *testing.T) {
			c, w := artifactDeleteVector()
			w.Owner = principal
			w.Digest = [32]byte{}
			raw, err := json.Marshal(w)
			if err != nil {
				t.Fatal(err)
			}
			w.Digest = sha256.Sum256(raw)
			owner, err := c.tryArtifactDeleteIO(w)
			if err != nil {
				t.Fatal("valid principal refused as a job ID", err)
			}
			owner.finish(nil)
			if len(c.artifacts.workspaceIO) != 0 {
				t.Fatal("principal vector did not settle original permit")
			}
		})
	}
}

func TestArtifactDeleteRejectsUnknownIdentityBeforePermit(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*store.ArtifactDeleteWork)
	}{
		{"File0", func(w *store.ArtifactDeleteWork) { w.File = store.ArtifactFile{} }},
		{"root", func(w *store.ArtifactDeleteWork) { w.RootID = strings.Repeat("f", 32) }},
		{"volume", func(w *store.ArtifactDeleteWork) { w.File.Device++ }},
		{"kind", func(w *store.ArtifactDeleteWork) { w.Kind = "workspace" }},
		{"revision", func(w *store.ArtifactDeleteWork) { w.Revision++ }},
		{"digest", func(w *store.ArtifactDeleteWork) { w.Digest = [32]byte{1} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			c, w := artifactDeleteVector()
			change.apply(&w)
			if owner, err := c.tryArtifactDeleteIO(w); !errors.Is(err, ErrInvalid) || owner != nil || len(c.artifacts.workspaceIO) != 0 {
				t.Fatalf("unknown tuple admitted: %v %v", owner, err)
			}
		})
	}
}

func TestArtifactDeleteOriginalOwnerMirrorsAndCommonFour(t *testing.T) {
	for _, kind := range []string{"upload", "transition", "download", "four", "close uncertainty", "stopped"} {
		t.Run(kind, func(t *testing.T) {
			c, w := artifactDeleteVector()
			want := ErrArtifactBusy
			switch kind {
			case "upload":
				c.artifacts.uploads[w.Ref.ID] = &ArtifactCall{service: c.artifacts}
			case "transition":
				c.artifacts.calls[&ArtifactCall{service: c.artifacts, uploadTransition: true}] = struct{}{}
			case "download":
				c.artifacts.downloads[&ArtifactDownload{control: c, ref: artifact.Ref{ID: w.Ref.ID}}] = struct{}{}
			case "four":
				for i := 0; i < 4; i++ {
					if _, err := c.TryArtifactCall(); err != nil {
						t.Fatal(err)
					}
				}
			case "close uncertainty":
				owner, err := c.tryArtifactIO()
				if err != nil {
					t.Fatal(err)
				}
				owner.finish(artifact.ErrFileCloseUncertain)
				want = artifact.ErrFileCloseUncertain
			case "stopped":
				c.StopArtifactAdmission()
				want = ErrUnavailable
			}
			before := len(c.artifacts.workspaceIO)
			if owner, err := c.tryArtifactDeleteIO(w); !errors.Is(err, want) || owner != nil || len(c.artifacts.workspaceIO) != before {
				t.Fatalf("original owner bypassed: %v %v", owner, err)
			}
		})
	}
	c, w := artifactDeleteVector()
	owner, err := c.tryArtifactDeleteIO(w)
	if err != nil {
		t.Fatal(err)
	}
	if c.workspace != nil || len(c.artifacts.workspaceIO) != 1 {
		t.Fatal("artifact-only admission acquired local dependency or wrong permit count")
	}
	for i := 0; i < 3; i++ {
		if _, err := c.TryArtifactCall(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatal("delete permit was outside common4", err)
	}
	owner.finish(nil)
	if _, err := c.TryArtifactCall(); err != nil {
		t.Fatal("positive original return did not release common permit", err)
	}
}

func TestArtifactDeleteCancellationNeverInvokesFiles(t *testing.T) {
	c, w := artifactDeleteVector()
	owner, err := c.tryArtifactDeleteIO(w)
	if err != nil {
		t.Fatal(err)
	}
	r := maintenanceRequest{sequence: 1, artifactDelete: &artifactDeleteRequest{control: c, work: w, owner: owner}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := runArtifactDelete(ctx, r) // Files is nil: entry must not occur.
	if !errors.Is(err, context.Canceled) || result == nil || result.invoked || !owner.finished || len(c.artifacts.workspaceIO) != 0 {
		t.Fatal("cancellation fabricated invocation or lost original permit", result, err)
	}
	if physicalRequestShape(r) || workspaceRetirementShape(r) {
		t.Fatal("delete request accepted by another union arm")
	}
}

func TestArtifactDeleteMatchingAndUncertainErrorsRetainOriginalPair(t *testing.T) {
	for _, kind := range []string{"wrong request", "missing result", "not invoked", "uncertain close", "ordinary error"} {
		t.Run(kind, func(t *testing.T) {
			c, w := artifactDeleteVector()
			owner, err := c.tryArtifactDeleteIO(w)
			if err != nil {
				t.Fatal(err)
			}
			request := &maintenanceRequest{sequence: 1, artifactDelete: &artifactDeleteRequest{control: c, work: w, owner: owner}}
			r := maintenanceResult{request: *request, deletion: &artifactDeleteResult{}}
			if kind == "wrong request" {
				copied := *request.artifactDelete
				r.request.artifactDelete = &copied
			}
			if kind == "missing result" {
				r.deletion = nil
			}
			if kind == "uncertain close" {
				r.err = artifact.ErrFileCloseUncertain
			}
			if kind == "ordinary error" {
				r.err = errors.New("modeled parent sync failure")
			}
			owner.finish(r.err)
			pair := &receivedMaintenance{request: request, result: r}
			m := &maintenanceState{pending: request, received: pair}
			err = c.integrateArtifactDelete(m, r)
			if kind == "ordinary error" {
				if err != nil || m.pending != nil {
					t.Fatal("ordinary error did not allow fair charged reselection", err)
				}
			} else if err == nil || m.pending != request || m.received != pair {
				t.Fatal("invalid/uncertain result lost original pair", err)
			}
			if kind == "uncertain close" {
				if _, ok := c.artifacts.workspaceIO[owner]; !ok || owner.fileError != r.err {
					t.Fatal("uncertain original permit/cause lost")
				}
			}
		})
	}
}

var heldDeletePolicyStores struct {
	sync.Mutex
	frames []any
}

func TestArtifactDeleteFinishErrorKeepsOriginalReceivedPair(t *testing.T) {
	c, w := artifactDeleteVector()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261021-delete-policy")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.store = s
	var m *maintenanceState
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			heldDeletePolicyStores.Lock()
			heldDeletePolicyStores.frames = append(heldDeletePolicyStores.frames, struct {
				store       *store.Store
				control     *Control
				maintenance *maintenanceState
				cause       error
			}{s, c, m, err})
			heldDeletePolicyStores.Unlock()
			t.Error(err)
		}
	})
	owner, err := c.tryArtifactDeleteIO(w)
	if err != nil {
		t.Fatal(err)
	}
	owner.finish(nil)
	request := &maintenanceRequest{sequence: 1, artifactDelete: &artifactDeleteRequest{control: c, work: w, owner: owner}}
	// MODELED physical success, intentionally NOT an actual unlink receipt. An
	// ordinary real Store without installed artifacts must refuse Finish; this
	// tests fail-stop custody only, never grants or claims filesystem credit.
	r := maintenanceResult{request: *request, deletion: &artifactDeleteResult{invoked: true}}
	pair := &receivedMaintenance{request: request, result: r}
	m = &maintenanceState{pending: request, received: pair}
	if err := c.integrateArtifactDelete(m, r); err == nil || m.pending != request || m.received != pair || m.received.result.deletion != r.deletion {
		t.Fatal("Finish refusal became success or dropped original pair", err)
	}
}
