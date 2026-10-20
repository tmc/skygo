//go:build darwin || linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// This fixture uses the actual Files parser and Store publication. It never
// manufactures a native writer/result receipt or starts a workspace process.
func workspaceUploadedInput(t *testing.T, c *Control) artifact.Ref {
	t.Helper()
	var encoded bytes.Buffer
	writer := tar.NewWriter(&encoded)
	payload := []byte("workspace input")
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	digest := sha256.Sum256(data)
	call := func() *ArtifactCall {
		a, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(a.Done)
		return a
	}
	uploadGrowthSample(t, c)
	a := call()
	u, err := c.BeginArtifactUpload(context.Background(), testActor(), a, hex.EncodeToString(digest[:]), int64(len(data)))
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	uploadGrowthSample(t, c)
	a = call()
	u, err = c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, 0, data)
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	return uploadGrowthFinalize(t, c, u.Reference.ID)
}

func TestWorkspaceReplayBeforeCurrentPlacementAndAdmission(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			c, _ := artifactResultFixture(t, retained)
			input := workspaceUploadedInput(t, c)
			task := backend.Task{Argv: []string{"unused"}, Workdir: &input}
			var domain *store.RequestDomain
			if retained {
				info, err := c.store.RetentionInfo(testActor())
				if err != nil {
					t.Fatal(err)
				}
				domain = &info.Domain
			}
			// Accept with the real public Store seam to test replay independently of
			// today's absent local route. This publishes no execution/writer evidence.
			j, err := c.store.SubmitWorkspace(testActor(), domain, "workspace-replay", task, "old-local", "old-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			c.StopArtifactAdmission()
			c.failed.Store(true)
			var got backend.Job
			if domain == nil {
				got, err = c.SubmitWorkspace(testActor(), "workspace-replay", task)
			} else {
				got, err = c.SubmitWorkspaceInDomain(testActor(), *domain, "workspace-replay", task)
			}
			if err != nil || got.ID != j.ID || got.Target != "old-local" {
				t.Fatalf("original replay=%+v %v", got, err)
			}
			changed := backend.CloneTask(task)
			changed.Argv = []string{"different"}
			if domain == nil {
				_, err = c.SubmitWorkspace(testActor(), "workspace-replay", changed)
			} else {
				_, err = c.SubmitWorkspaceInDomain(testActor(), *domain, "workspace-replay", changed)
			}
			if err == nil || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
				t.Fatalf("request conflict hidden by current placement/pressure: %v", err)
			}
			if domain == nil {
				_, err = c.SubmitWorkspace(testActor(), "new-request", task)
			} else {
				_, err = c.SubmitWorkspaceInDomain(testActor(), *domain, "new-request", task)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("new acceptance bypassed stop: %v", err)
			}
		})
	}
}

func TestOldSubmitEndpointsRefuseWorkspaceBeforeLookup(t *testing.T) {
	c, _ := artifactResultFixture(t, false)
	ref := artifact.Ref{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Size: 1024}
	task := backend.Task{Argv: []string{"unused"}, Workdir: &ref}
	before, err := c.store.LastSequenceFor(testActor())
	if err != nil {
		t.Fatal(err)
	}
	for _, submit := range []func() (backend.Job, error){
		func() (backend.Job, error) { return c.Submit(testActor(), "old-endpoint", task) },
		func() (backend.Job, error) {
			return c.SubmitRetained(testActor(), store.RequestDomain{}, "old-endpoint", task)
		},
	} {
		if _, err := submit(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("old endpoint workspace=%v", err)
		}
	}
	after, err := c.store.LastSequenceFor(testActor())
	if err != nil || before != after {
		t.Fatalf("old endpoint mutated sequence %d→%d %v", before, after, err)
	}
}
