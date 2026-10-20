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
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// This fixture uses real stopped installation and root facts. It publishes no
// workspace/native result and never fabricates a writer completion receipt.
func artifactResultFixture(t *testing.T, retained bool) (*Control, []backend.Job) {
	t.Helper()
	// Reuse the original concrete resource frame. Its cleanup retains the whole
	// namespace on failed Drain/scan Close before any borrowed resource removal.
	resources := newUploadGrowthResources(t)
	dir := resources.store.Directory()
	var domain store.RequestDomain
	if retained {
		if err := resources.store.Close(); err != nil {
			resources.held = err
			t.Fatal(err)
		}
		resources.store = nil
		resources.control = nil // The never-used constructor owns no operation or worker.
		policy := store.DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		enabled, err := store.EnableRetention(dir, policy)
		if err != nil {
			resources.held = err
			t.Fatal(err)
		}
		domain = enabled.Domain
		s, err := store.Open(dir)
		resources.store = s
		if err != nil {
			resources.held = err
			t.Fatal(err)
		}
	}
	s := resources.store
	c, err := NewWithConfig(s, []backend.Target{retainedTarget(&routeBackend{})}, testEngine, Config{Artifacts: resources.files})
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	resources.control = c
	var jobs []backend.Job
	for i := 0; i < 2; i++ {
		task := backend.Task{Argv: []string{"unused"}}
		var job backend.Job
		if retained {
			job, err = s.SubmitRetained(testActor(), domain, fmt.Sprint(i), task, c.targets[0].Name, c.targets[0].Binding, backend.RemoteOutput, 0)
		} else {
			job, err = s.Submit(testActor(), fmt.Sprint(i), task, c.targets[0].Name, c.targets[0].Binding)
		}
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	return c, jobs
}

func TestArtifactResultsAndCursorBothRetentionModes(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			c, jobs := artifactResultFixture(t, retained)
			result, err := c.ArtifactResultFor(testActor(), jobs[0].ID, jobs[0].ID)
			if err != nil || result.State != "absent" || result.Reference != nil || result.AttemptID != jobs[0].ID {
				t.Fatalf("default result=%+v %v", result, err)
			}
			if _, err = c.ArtifactResultFor(testActor(), jobs[0].ID, jobs[1].ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("foreign execution member: %v", err)
			}
			selection := store.PruneSelection{Mode: store.ResultOnly, Attempt: 1, Before: time.Now().UTC().Add(time.Hour), Limit: 1}
			page, token, err := c.Prune(context.Background(), testActor(), selection, false, "")
			if err != nil || page.Inspected != 1 || token == "" {
				t.Fatalf("first page=%+v token=%q %v", page, token, err)
			}
			var cursor artifactPruneCursor
			if err = decodeCursor(token, &cursor, 512); err != nil {
				t.Fatal(err)
			}
			if cursor.Kind != "artifact-prune" || cursor.Root != c.artifacts.root.ID || cursor.Attempt != 1 || cursor.Before != selection.Before.Format(time.RFC3339Nano) {
				t.Fatalf("scope=%+v", cursor)
			}
			if (cursor.Lineage != "") != retained {
				t.Fatalf("invented/missing lineage: %+v", cursor)
			}
			if _, _, err = c.Prune(context.Background(), testActor(), selection, true, token); !errors.Is(err, ErrInvalid) {
				t.Fatalf("dry-run cursor reused for execute: %v", err)
			}
			other := selection
			other.Attempt = 2
			if _, _, err = c.Prune(context.Background(), testActor(), other, false, token); !errors.Is(err, ErrInvalid) {
				t.Fatalf("ordinal changed: %v", err)
			}
			for _, change := range []func(*artifactPruneCursor){
				func(cur *artifactPruneCursor) { cur.Kind = "prune" },
				func(cur *artifactPruneCursor) { cur.Version = 2 },
				func(cur *artifactPruneCursor) { cur.Owner = differentResultScopeID(cur.Owner) },
				func(cur *artifactPruneCursor) { cur.Principal = "different-admin" },
				func(cur *artifactPruneCursor) { cur.Scoped = !cur.Scoped },
				func(cur *artifactPruneCursor) { cur.Lineage = differentResultScopeID(cur.Lineage) },
				func(cur *artifactPruneCursor) { cur.Max++ },
			} {
				changed := cursor
				change(&changed)
				bad, err := encodeCursor(changed, 512)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = c.Prune(context.Background(), testActor(), selection, false, bad); !errors.Is(err, ErrInvalid) {
					t.Fatalf("changed scope accepted: %+v %v", changed, err)
				}
			}
			changed := cursor
			changed.Root = differentResultScopeID(changed.Root)
			bad, err := encodeCursor(changed, 512)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.Prune(context.Background(), testActor(), selection, false, bad); !errors.Is(err, ErrInvalid) {
				t.Fatalf("root changed: %v", err)
			}
			page, next, err := c.Prune(context.Background(), testActor(), selection, false, token)
			if err != nil || page.Inspected != 1 || next != "" {
				t.Fatalf("continuation=%+v next=%q %v", page, next, err)
			}
			if _, _, err = c.Prune(context.Background(), testActor(), store.PruneSelection{Mode: store.ResultOnly, Before: selection.Before}, false, ""); !errors.Is(err, ErrInvalid) {
				t.Fatalf("implicit ordinal accepted: %v", err)
			}
		})
	}
}

func TestArtifactDrainRealOriginalDownloadAfterStop(t *testing.T) {
	c, _ := artifactResultFixture(t, false)
	call := func() *ArtifactCall {
		a, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(a.Done)
		return a
	}
	var encoded bytes.Buffer
	tarWriter := tar.NewWriter(&encoded)
	payload := []byte("actual upload bytes")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	digest := sha256.Sum256(data)
	ctx := context.Background()
	uploadGrowthSample(t, c)
	begin := call()
	upload, err := c.BeginArtifactUpload(ctx, testActor(), begin, hex.EncodeToString(digest[:]), int64(len(data)))
	begin.Done()
	if err != nil {
		t.Fatal(err)
	}
	uploadGrowthSample(t, c)
	write := call()
	upload, err = c.WriteArtifactChunk(ctx, testActor(), write, upload.Reference.ID, 0, data)
	write.Done()
	if err != nil {
		t.Fatal(err)
	}
	ref := uploadGrowthFinalize(t, c, upload.Reference.ID)
	original := call()
	c.StopArtifactAdmission()
	download, err := c.BeginArtifactDownload(ctx, testActor(), original, ref)
	if err != nil {
		original.Done()
		t.Fatal(err)
	}
	if download.reader == nil || len(c.artifacts.downloads) != 1 {
		original.Done()
		t.Fatal("original descriptor not held")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.DrainArtifacts(canceled); !errors.Is(err, ErrUnavailable) {
		original.Done()
		t.Fatalf("pending response: %v", err)
	}
	if download.finished || artifactTerminalClosed(c) {
		original.Done()
		t.Fatal("pending response fabricated descriptor join")
	}
	original.Done()
	if err := c.DrainArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	if !download.finished || len(c.artifacts.downloads) != 0 || !artifactTerminalClosed(c) {
		t.Fatal("actual original close not settled")
	}
	if _, err := c.artifacts.files.RootFacts(); err != nil {
		t.Fatalf("borrowed Files closed by drain: %v", err)
	}
	if _, err := c.store.GetArtifactUpload(testActor(), ref.ID); err != nil {
		t.Fatalf("Store closed by drain: %v", err)
	}
	if _, err := c.TryArtifactCall(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("admission reopened after terminal: %v", err)
	}
}

func differentResultScopeID(id string) string {
	first := strings.Repeat("a", 32)
	if id == first {
		return strings.Repeat("b", 32)
	}
	return first
}
