//go:build linux || darwin

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// All tests are UNRUN. These helpers use actual small Store/Files fixtures and
// the finite sampler engine, not the Run/HTTP adapter or native writer proof.
func uploadGrowthSample(t *testing.T, c *Control) {
	t.Helper()
	for range 32 {
		q := c.physicalQuantum(context.Background())
		if q.Err != nil {
			t.Fatal(q.Err)
		}
		if q.Complete {
			return
		}
	}
	t.Fatal("small original upload root did not produce a complete interval")
}

func uploadGrowthCall(t *testing.T, c *Control) *ArtifactCall {
	t.Helper()
	a, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Done)
	return a
}

func uploadGrowthArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	body := bytes.Repeat([]byte("u"), 70000)
	if err := w.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := b.Bytes()
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}

func uploadGrowthBegin(t *testing.T, c *Control, data []byte, digest string) store.ArtifactUpload {
	t.Helper()
	uploadGrowthSample(t, c)
	a := uploadGrowthCall(t, c)
	u, err := c.BeginArtifactUpload(context.Background(), testActor(), a, digest, int64(len(data)))
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	uploadGrowthSample(t, c)
	return u
}

func TestUploadBeginGuardIsBeforeReservation(t *testing.T) {
	c, root := uploadGrowthPhysicalFixture(t)
	c.engine = testEngine
	before, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	names, err := os.ReadDir(root.Name())
	if err != nil {
		t.Fatal(err)
	}
	a := uploadGrowthCall(t, c)
	u, err := c.BeginArtifactUpload(context.Background(), testActor(), a, strings.Repeat("a", 64), 1024)
	a.Done()
	if !IsArtifactBeginNotStarted(err) || !errors.Is(err, artifact.ErrFilesBusy) || u.Reference.ID != "" {
		t.Fatalf("cold Begin=%+v %v", u, err)
	}
	after, readErr := c.store.ArtifactReservations()
	if readErr != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("pre-admission refusal changed reservations: %v", readErr)
	}
	got, err := os.ReadDir(root.Name())
	if err != nil || len(got) != len(names) {
		t.Fatalf("pre-admission refusal created a name: %v", err)
	}
	for i := range got {
		if got[i].Name() != names[i].Name() {
			t.Fatal("root names changed")
		}
	}
	if IsArtifactBeginNotStarted(errors.New("artifact inventory unavailable")) || IsArtifactBeginNotStarted(artifact.ErrFilesBusy) {
		t.Fatal("ordinary error acquired non-admission authority")
	}
}

func TestUploadQuotaRefusalHasNoBeginMarker(t *testing.T) {
	c, _ := uploadGrowthPhysicalFixture(t)
	c.engine = testEngine
	for range 4 {
		uploadGrowthBegin(t, c, []byte("x"), strings.Repeat("a", 64))
	}
	a := uploadGrowthCall(t, c)
	_, err := c.BeginArtifactUpload(context.Background(), testActor(), a, strings.Repeat("a", 64), 1)
	a.Done()
	var capacity *store.ArtifactCapacityError
	if !errors.As(err, &capacity) || IsArtifactBeginNotStarted(err) {
		t.Fatalf("Store refusal acquired pre-Begin marker: %v", err)
	}
}

func TestUploadSameInodeChunksDoNotRestartInterval(t *testing.T) {
	c, _ := uploadGrowthPhysicalFixture(t)
	c.engine = testEngine
	data, digest := uploadGrowthArchive(t)
	u := uploadGrowthBegin(t, c, data, digest)
	before, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	original := p.cached
	p.mu.Unlock()
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		a := uploadGrowthCall(t, c)
		u, err = c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, int64(offset), data[offset:end])
		a.Done()
		if err != nil || u.Acknowledged != uint64(end) {
			t.Fatalf("chunk=%+v %v", u, err)
		}
		p.mu.Lock()
		unchanged := p.cached == original
		p.mu.Unlock()
		if !unchanged {
			t.Fatal("settled same-inode append invalidated operational interval")
		}
		after, err := c.store.ArtifactReservations()
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("ack changed prepaid cohort: %v", err)
		}
		offset = end
	}
	if c.retention.enabled {
		t.Fatal("fixture unexpectedly enabled retention")
	}
}

func TestUploadPressurePermitsActualEarlierByteComparison(t *testing.T) {
	resources := newUploadGrowthResources(t)
	c, root := resources.control, resources.root
	c.engine = testEngine
	data, digest := uploadGrowthArchive(t)
	u := uploadGrowthBegin(t, c, data, digest)
	a := uploadGrowthCall(t, c)
	u, err := c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, 0, data[:1024])
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	// Explicit cache policy vector, not a spontaneous disk-pressure observation.
	p := &c.artifacts.physical
	p.mu.Lock()
	original := p.cached
	p.points.artifactFree = physicalUint(0)
	p.mu.Unlock()
	a = uploadGrowthCall(t, c)
	_, err = c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, 1024, data[1024:2048])
	a.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("pressure admitted new append: %v", err)
	}
	// Test owns this exact temporary fixture. Add a real crash suffix without
	// advancing metadata; production RecoverPart must truncate it on retransmit.
	name := u.Reference.ID + ".part"
	f, err := root.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteAt([]byte("unacknowledged"), 1024)
	closeErr := f.Close()
	if closeErr != nil {
		resources.held = closeErr
	}
	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}
	a = uploadGrowthCall(t, c)
	again, err := c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, 8, data[8:24])
	a.Done()
	if err != nil || again.Acknowledged != u.Acknowledged || again.Revision != u.Revision {
		t.Fatalf("earlier retry=%+v %v", again, err)
	}
	opened, err := root.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(opened)
	if closeErr := opened.Close(); closeErr != nil {
		resources.held = closeErr
		t.Fatal(closeErr)
	}
	if err != nil || !bytes.Equal(actual, data[:1024]) {
		t.Fatalf("actual suffix not repaired: %v", err)
	}
	a = uploadGrowthCall(t, c)
	_, err = c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, 8, bytes.Repeat([]byte("x"), 16))
	a.Done()
	if !errors.Is(err, artifact.ErrFileConflict) {
		t.Fatalf("conflicting earlier bytes accepted: %v", err)
	}
	// A real conflicting observation does invalidate, unlike settled IO noise.
	p.mu.Lock()
	invalidated := p.cached == nil
	p.mu.Unlock()
	if !invalidated || original == nil {
		t.Fatal("conflicting byte observation did not invalidate")
	}
}

func TestUploadFinalizeRequiresRenewedSampleAndCompleteReplay(t *testing.T) {
	c, _ := uploadGrowthPhysicalFixture(t)
	c.engine = testEngine
	data, digest := uploadGrowthArchive(t)
	u := uploadGrowthBegin(t, c, data, digest)
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		a := uploadGrowthCall(t, c)
		var err error
		u, err = c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, int64(offset), data[offset:end])
		a.Done()
		if err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	a := uploadGrowthCall(t, c)
	_, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, u.Reference.ID)
	a.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("first finalize=%v", err)
	}
	pending, err := c.store.GetArtifactUpload(testActor(), u.Reference.ID)
	if err != nil || pending.State != "finalizing" || pending.Acknowledged != u.Acknowledged {
		t.Fatalf("lost publishing reservation: %+v %v", pending, err)
	}
	uploadGrowthSample(t, c)
	a = uploadGrowthCall(t, c)
	ref, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, u.Reference.ID)
	a.Done()
	if err != nil || ref.ID != u.Reference.ID || ref.SHA256 != digest {
		t.Fatalf("same-ID finalize=%+v %v", ref, err)
	}
	// Structural completion invalidated cache. Exact complete replay still works.
	a = uploadGrowthCall(t, c)
	again, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, u.Reference.ID)
	a.Done()
	if err != nil || again != ref {
		t.Fatalf("complete replay under denied growth=%+v %v", again, err)
	}
}

func TestUploadStructuralMirrorAndLateCacheLock(t *testing.T) {
	c, _ := uploadGrowthPhysicalFixture(t)
	uploadGrowthSample(t, c)
	p := &c.artifacts.physical
	// Explicit frame/cache-lock vector: no descriptor or native scan is fabricated.
	frame := &physicalFrame{serial: 17}
	p.mu.Lock()
	p.frame = frame
	old := p.cached
	p.mu.Unlock()
	if err := c.cachedLinkGrowthCheck(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	observational := p.cached == old && !frame.invalidated
	p.mu.Unlock()
	if !observational {
		t.Fatal("ordinary readiness invalidated its cache")
	}
	c.artifacts.mu.Lock()
	err := c.uploadLinkGrowthCheck() // Must not reenter service.mu under Files.mu.
	c.artifacts.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	invalidated := p.frame == frame && frame.invalidated && p.cached == nil
	p.frame = nil
	p.mu.Unlock()
	if !invalidated {
		t.Fatal("late permission failed to invalidate original frame")
	}
	a := uploadGrowthCall(t, c)
	end, err := c.beginArtifactCall(a, "")
	if err != nil {
		t.Fatal(err)
	}
	finish := c.beginUploadStructure(a)
	if _, err := c.physicalCohort(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("transition promoted: %v", err)
	}
	finish()
	end()
	a.Done()
	if _, err := c.physicalCohort(); err != nil {
		t.Fatalf("stable original mirror did not resume: %v", err)
	}
}

// Existing fixtures call this only after their real complete part is uploaded.
// First publishing CAS must return Busy; the next call uses a new actual census.
func uploadGrowthFinalize(t *testing.T, c *Control, id string) artifact.Ref {
	t.Helper()
	uploadGrowthSample(t, c)
	a := uploadGrowthCall(t, c)
	_, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, id)
	a.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("first finalizing transition: %v", err)
	}
	uploadGrowthSample(t, c)
	a = uploadGrowthCall(t, c)
	ref, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, id)
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
