//go:build linux || darwin

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// trimScratch leaves the exact part at size, as a crash between the grow and
// shrink of an allocation trim does. The test owns this temporary fixture.
func trimScratch(t *testing.T, root *os.Root, name string, size int64) {
	t.Helper()
	file, err := root.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Truncate(size), file.Close()); err != nil {
		t.Fatal(err)
	}
}

type trimPart struct {
	inode  uint64
	links  uint64
	size   int64
	digest string
}

func trimObservePart(t *testing.T, root *os.Root, name string) trimPart {
	t.Helper()
	info, err := root.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("inode unavailable")
	}
	file, err := root.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, err = file.WriteTo(h)
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	return trimPart{inode: uint64(s.Ino), links: uint64(s.Nlink), size: info.Size(), digest: hex.EncodeToString(h.Sum(nil))}
}

func trimDigest(p []byte) string {
	h := sha256.Sum256(p)
	return hex.EncodeToString(h[:])
}

// trimHold replaces the complete cached observation with the same one showing
// one byte more allocation than its cohort: an explicit cache vector for the
// unchanged accounting-hold predicate, not a filesystem observation.
func trimHold(t *testing.T, c *Control) *physicalObservation {
	t.Helper()
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached == nil {
		t.Fatal("no complete observation to hold")
	}
	held := *p.cached
	held.sample.AllocatedBytes = physicalUint(held.cohort.allocated + 1)
	p.cached = &held
	return &held
}

func trimCached(c *Control) *physicalObservation {
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cached
}

func trimAccountingHold(err error) bool {
	return errors.Is(err, artifact.ErrFilesBusy) && strings.Contains(err.Error(), "artifact_accounting_hold")
}

// TestUploadTrimRepairPrecedesHeldGuard holds the unchanged growth predicate
// and shows that the exact owned part is still repaired, that the repair is no
// release credit, and that only a new sample admits growth.
func TestUploadTrimRepairPrecedesHeldGuard(t *testing.T) {
	c, root := uploadGrowthPhysicalFixture(t)
	data, digest := uploadGrowthArchive(t)
	u := uploadGrowthBegin(t, c, data, digest)
	id := u.Reference.ID
	name := id + ".part"
	a := uploadGrowthCall(t, c)
	u, err := c.WriteArtifactChunk(context.Background(), testActor(), a, id, 0, data[:64<<10])
	a.Done()
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	original := trimObservePart(t, root, name)
	ack := int64(u.Acknowledged)

	trimScratch(t, root, name, ack+1)
	uploadGrowthSample(t, c)
	held := trimHold(t, c)
	for range 2 {
		a = uploadGrowthCall(t, c)
		_, err = c.WriteArtifactChunk(context.Background(), testActor(), a, id, ack, data[ack:])
		a.Done()
		if !trimAccountingHold(err) {
			t.Fatalf("held append=%v", err)
		}
		if got := trimObservePart(t, root, name); got.inode != original.inode || got.size != ack || got.digest != trimDigest(data[:ack]) {
			t.Fatalf("held part=%+v, want repaired prefix %d", got, ack)
		}
		if trimCached(c) != held {
			t.Fatal("repair replaced the held observation")
		}
		latest, err := c.store.GetArtifactUpload(testActor(), id)
		if err != nil || latest.Acknowledged != u.Acknowledged || latest.Revision != u.Revision || latest.State != "writable" {
			t.Fatalf("held upload=%+v %v", latest, err)
		}
	}
	// A new actual observation, never the truncate, admits the append.
	uploadGrowthSample(t, c)
	a = uploadGrowthCall(t, c)
	u, err = c.WriteArtifactChunk(context.Background(), testActor(), a, id, ack, data[ack:])
	a.Done()
	if err != nil || u.Acknowledged != uint64(len(data)) {
		t.Fatalf("renewed append=%+v %v", u, err)
	}
	if got, err := c.store.ArtifactReservations(); err != nil || !reflect.DeepEqual(got, reservations) {
		t.Fatalf("ack-only repair changed reservations: %v", err)
	}

	// The same boundary at the full ack is one byte past the reference.
	trimScratch(t, root, name, int64(len(data))+1)
	uploadGrowthSample(t, c)
	held = trimHold(t, c)
	a = uploadGrowthCall(t, c)
	_, err = c.FinalizeArtifactUpload(context.Background(), testActor(), a, id)
	a.Done()
	if !trimAccountingHold(err) {
		t.Fatalf("held finalize=%v", err)
	}
	if got := trimObservePart(t, root, name); got.inode != original.inode || got.size != int64(len(data)) || got.digest != digest {
		t.Fatalf("held full part=%+v", got)
	}
	if trimCached(c) != held {
		t.Fatal("finalize repair replaced the held observation")
	}
	if latest, err := c.store.GetArtifactUpload(testActor(), id); err != nil || latest.State != "writable" || latest.Revision != u.Revision {
		t.Fatalf("held finalize changed upload=%+v %v", latest, err)
	}
	if got, err := c.store.ArtifactReservations(); err != nil || !reflect.DeepEqual(got, reservations) {
		t.Fatalf("held finalize changed reservations: %v", err)
	}
	uploadGrowthSample(t, c)
	a = uploadGrowthCall(t, c)
	_, err = c.FinalizeArtifactUpload(context.Background(), testActor(), a, id)
	a.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("structural finalize=%v", err)
	}
	// A crash inside the finalizing seal's own trim.
	trimScratch(t, root, name, int64(len(data))+1)
	uploadGrowthSample(t, c)
	a = uploadGrowthCall(t, c)
	ref, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, id)
	a.Done()
	if err != nil || ref.ID != id || ref.SHA256 != digest || ref.Size != int64(len(data)) {
		t.Fatalf("finalize=%+v %v", ref, err)
	}
	if got := trimObservePart(t, root, id+".tar"); got.inode != original.inode || got.links != 1 || got.size != ref.Size || got.digest != digest {
		t.Fatalf("object=%+v", got)
	}
}

// trimFullArchive is a valid archive of exactly the 32 MiB encoded cap.
func trimFullArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	body := artifact.MaxSize - 3*512
	if err := w.WriteHeader(&tar.Header{Name: "full", Mode: 0600, Size: body, Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("f"), int(body))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if int64(b.Len()) != artifact.MaxSize {
		t.Fatalf("archive=%d", b.Len())
	}
	return b.Bytes(), trimDigest(b.Bytes())
}

// trimWrite appends one chunk, renewing the actual sample once if a stale
// interval refuses it.
func trimWrite(t *testing.T, f *finalizationCrashResources, id string, offset int64, p []byte) store.ArtifactUpload {
	t.Helper()
	for attempt := 0; ; attempt++ {
		call := f.call(t)
		u, err := f.control.WriteArtifactChunk(context.Background(), f.actor, call, id, offset, p)
		call.Done()
		if err == nil {
			return u
		}
		if !errors.Is(err, artifact.ErrFilesBusy) || attempt == 1 {
			f.fail(t, err)
		}
		f.sample(t)
	}
}

type trimDurable struct {
	upload       store.ArtifactUpload
	reservations store.ArtifactReservations
}

func trimSnapshot(t *testing.T, f *finalizationCrashResources, id string) trimDurable {
	t.Helper()
	u, err := f.database.GetArtifactUpload(f.actor, id)
	if err != nil {
		f.fail(t, err)
	}
	r, err := f.database.ArtifactReservations()
	if err != nil {
		f.fail(t, err)
	}
	return trimDurable{u, r}
}

// TestUploadTrimRestartAtFullCap restarts the same Store and root after a
// crash between grow and shrink, first at the final append to MaxSize+1 and
// then at the full acknowledgement. Owners are positively closed between.
func TestUploadTrimRestartAtFullCap(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 32 MiB archive")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261022-apfs-trim-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	})
	data, digest := trimFullArchive(t)
	f := finalizationCrashOpen(t, dir, true)
	f.sample(t)
	call := f.call(t)
	u, err := f.control.BeginArtifactUpload(context.Background(), f.actor, call, digest, int64(len(data)))
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	id := u.Reference.ID
	name := id + ".part"
	f.sample(t)
	last := artifact.MaxSize - 64<<10
	for offset := int64(0); offset < last; offset += 64 << 10 {
		u = trimWrite(t, f, id, offset, data[offset:offset+64<<10])
	}
	original := trimObservePart(t, f.root, name)
	// The final append wrote its bytes and grew to MaxSize+1; it never returned.
	file, err := f.root.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		f.fail(t, err)
	}
	_, writeErr := file.WriteAt(data[last:], last)
	if err := errors.Join(writeErr, file.Truncate(artifact.MaxSize+1), file.Close()); err != nil {
		f.fail(t, err)
	}
	before := trimSnapshot(t, f, id)
	if before.upload.Acknowledged != uint64(last) {
		t.Fatalf("ack=%d, want %d", before.upload.Acknowledged, last)
	}
	f.retire(t)

	f = finalizationCrashOpen(t, dir, false)
	if after := trimSnapshot(t, f, id); !reflect.DeepEqual(after, before) {
		t.Fatalf("restart changed durable upload or reservations:\n%+v\n%+v", before, after)
	}
	if got := trimObservePart(t, f.root, name); got.inode != original.inode || got.size != artifact.MaxSize+1 {
		t.Fatalf("restarted part=%+v", got)
	}
	f.sample(t)
	u = trimWrite(t, f, id, last, data[last:])
	if u.Acknowledged != uint64(artifact.MaxSize) {
		t.Fatalf("final ack=%d", u.Acknowledged)
	}
	if got := trimObservePart(t, f.root, name); got.inode != original.inode || got.size != artifact.MaxSize || got.digest != digest {
		t.Fatalf("full part=%+v", got)
	}
	if after := trimSnapshot(t, f, id); !reflect.DeepEqual(after.reservations, before.reservations) {
		t.Fatal("final append changed reservations")
	}
	// A crash inside the full-ack repair, then restart before Finalize.
	trimScratch(t, f.root, name, artifact.MaxSize+1)
	full := trimSnapshot(t, f, id)
	f.retire(t)

	f = finalizationCrashOpen(t, dir, false)
	if after := trimSnapshot(t, f, id); !reflect.DeepEqual(after, full) {
		t.Fatal("restart changed durable full upload or reservations")
	}
	f.sample(t)
	call = f.call(t)
	_, err = f.control.FinalizeArtifactUpload(context.Background(), f.actor, call, id)
	call.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		f.fail(t, err)
	}
	if got := trimObservePart(t, f.root, name); got.inode != original.inode || got.size != artifact.MaxSize || got.digest != digest {
		t.Fatalf("repaired full part=%+v", got)
	}
	f.sample(t)
	call = f.call(t)
	ref, err := f.control.FinalizeArtifactUpload(context.Background(), f.actor, call, id)
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	if ref.ID != id || ref.SHA256 != digest || ref.Size != artifact.MaxSize {
		t.Fatalf("ref=%+v", ref)
	}
	if got := trimObservePart(t, f.root, id+".tar"); got.inode != original.inode || got.links != 1 || got.size != artifact.MaxSize || got.digest != digest {
		t.Fatalf("object=%+v", got)
	}
}
