//go:build darwin || linux

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
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// environmentLease returns a real lease the Store admitted for f's uploaded
// input, under f's Control's producer, and registers its Mark and Finish as
// cleanup. An ordinary download lease stands in for an environment
// materialization read: the opener checks the same provenance fields of both.
func environmentLease(t *testing.T, f *dataControlFixture) (store.ArtifactLease, []byte) {
	t.Helper()
	ref, data := f.input(t)
	return leaseOf(t, f, ref), data
}

// leaseOf returns a real lease of ref, as environmentLease does.
func leaseOf(t *testing.T, f *dataControlFixture, ref artifact.Ref) store.ArtifactLease {
	t.Helper()
	stored, err := artifactStoreRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.store.BeginArtifactDownload(f.actor, stored, f.control.ArtifactProducer())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closure, err := f.store.MarkArtifactDownloadClosing(store.ArtifactDownloadClosure{Owner: f.actor.Principal(), LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer})
		if err == nil {
			err = f.store.FinishArtifactDownload(closure, time.Now().UTC())
		}
		if err != nil {
			f.hold(err)
			t.Error(err)
		}
	})
	return lease
}

// uploadArchive publishes data, a valid archive, as f's actor through the
// same actual Store and Files steps as f.input, in chunks of at most 64 KiB.
// The shared fixture's own input is left unchanged.
func uploadArchive(t *testing.T, f *dataControlFixture, data []byte) artifact.Ref {
	t.Helper()
	digest := sha256.Sum256(data)
	u, err := f.store.BeginArtifactUpload(f.actor, hex.EncodeToString(digest[:]), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.files.CreatePart(ref)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	storedFile := store.ArtifactFile{Device: file.Device, Inode: file.Inode}
	u, err = f.store.ArtifactPartCreated(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, storedFile)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(data); {
		end := min(off+64<<10, len(data))
		chunk, err := f.files.WritePart(ref, file, int64(off), int64(off), data[off:end])
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		u, err = f.store.ArtifactChunkSynced(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, storedFile, uint64(chunk.Offset), uint64(chunk.Size), chunk.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		off = end
	}
	u, err = f.store.BeginArtifactFinalize(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	f.control.physicalChanged()
	uploadGrowthSample(t, f.control)
	sealed, err := f.files.SealPart(ref, file, f.control.uploadLinkGrowthCheck)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	manifest := store.ArtifactManifest{Reference: u.Reference, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}
	if _, err = f.store.CompleteArtifactUpload(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, u.Reference, store.ArtifactFile{Device: sealed.File.Device, Inode: sealed.File.Inode}, manifest); err != nil {
		t.Fatal(err)
	}
	f.control.physicalChanged()
	return ref
}

// largeArchive returns a valid archive of one file of 200,000 patterned
// bytes, more than three times 64 KiB.
func largeArchive(t *testing.T) []byte {
	t.Helper()
	payload := make([]byte, 200000)
	for i := range payload {
		payload[i] = byte(i * 7 % 251)
	}
	var archive bytes.Buffer
	w := tar.NewWriter(&archive)
	if err := w.WriteHeader(&tar.Header{Name: "large.bin", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(archive.Bytes())
}

// heldPermits reports how many of the four artifact I/O permits are held, by
// acquiring the rest as calls and releasing them.
func heldPermits(t *testing.T, c *Control) int {
	t.Helper()
	var calls []*ArtifactCall
	for {
		call, err := c.TryArtifactCall()
		if errors.Is(err, ErrArtifactBusy) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	for _, call := range calls {
		call.Done()
	}
	return 4 - len(calls)
}

func TestEnvironmentArtifactRead(t *testing.T) {
	f := newDataControlFixture(t)
	lease, data := environmentLease(t, f)
	body, err := f.control.OpenEnvironmentArtifact(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if n := heldPermits(t, f.control); n != 1 {
		t.Fatalf("held permits with an open body = %d, want 1", n)
	}
	// The open body is an internal I/O owner: a drain waits for it.
	drain, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := f.control.DrainArtifacts(drain); !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain with an open body = %v, want held until its deadline", err)
	}
	var got []byte
	p := make([]byte, 1<<20)
	for {
		n, err := body.Read(p)
		if n > 64<<10 {
			t.Fatalf("read %d bytes, want at most 64 KiB", n)
		}
		got = append(got, p[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want the %d uploaded bytes", len(got), len(data))
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Read(p); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after close = %v, want os.ErrClosed", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("second close = %v, want the first result", err)
	}
	// The first drain stopped admission, so new calls cannot count permits.
	// Closing the original body was its last owner: a fresh drain, within the
	// fixture's ordinary retirement bound, now finds the service terminal.
	terminal, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := f.control.DrainArtifacts(terminal); err != nil {
		t.Fatalf("drain after close = %v, want nil", err)
	}
	if call, err := f.control.TryArtifactCall(); call != nil || !errors.Is(err, ErrUnavailable) {
		call.Done()
		t.Fatalf("call after drain = %v, %v; want nil and ErrUnavailable", call, err)
	}
	if again, err := f.control.OpenEnvironmentArtifact(context.Background(), lease); again != nil || !errors.Is(err, ErrUnavailable) {
		if again != nil {
			if err := again.Close(); err != nil {
				f.hold(err)
				t.Error(err)
			}
		}
		t.Fatalf("open after drain = %v, %v; want nil and ErrUnavailable", again, err)
	}
}

// TestEnvironmentArtifactReadLarge reads a valid archive of more than 64 KiB
// with a 1 MiB buffer. Every read but the last returns exactly 64 KiB, the
// last returns the rest, the bytes arrive in order, and EOF follows the last
// byte with no bytes, again on a repeated Read.
func TestEnvironmentArtifactReadLarge(t *testing.T) {
	f := newDataControlFixture(t)
	data := largeArchive(t)
	lease := leaseOf(t, f, uploadArchive(t, f, data))
	body, err := f.control.OpenEnvironmentArtifact(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	p := make([]byte, 1<<20)
	reads := 0
	for {
		n, err := body.Read(p)
		if err == io.EOF {
			if n != 0 {
				t.Fatalf("EOF read returned %d bytes, want 0", n)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if want := min(64<<10, len(data)-len(got)); n != want {
			t.Fatalf("read %d at offset %d returned %d bytes, want %d", reads, len(got), n, want)
		}
		got = append(got, p[:n]...)
		reads++
	}
	if want := (len(data) + 64<<10 - 1) / (64 << 10); reads != want || reads < 2 {
		t.Fatalf("%d reads, want %d", reads, want)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want the %d uploaded bytes in order", len(got), len(data))
	}
	if n, err := body.Read(p); n != 0 || err != io.EOF {
		t.Fatalf("read after EOF = %d, %v; want 0 and io.EOF", n, err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if n := heldPermits(t, f.control); n != 0 {
		t.Fatalf("held permits after close = %d, want 0", n)
	}
}

func TestEnvironmentArtifactRefusals(t *testing.T) {
	f := newDataControlFixture(t)
	lease, _ := environmentLease(t, f)
	for _, tt := range []struct {
		name   string
		change func(*store.ArtifactLease)
		want   error
	}{
		{"foreign producer", func(l *store.ArtifactLease) { l.Producer.StartTicks++ }, ErrUnavailable},
		{"closing", func(l *store.ArtifactLease) { l.Closing = true }, ErrUnavailable},
		{"other root", func(l *store.ArtifactLease) { l.RootID = strings.Repeat("b", 32) }, ErrUnavailable},
		{"other device", func(l *store.ArtifactLease) { l.File.Device++ }, ErrUnavailable},
		{"no inode", func(l *store.ArtifactLease) { l.File.Inode = 0 }, ErrUnavailable},
		{"bad lease ID", func(l *store.ArtifactLease) { l.ID = "lease" }, ErrInvalid},
		{"bad reference", func(l *store.ArtifactLease) { l.Reference.Size = 0 }, ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := lease
			tt.change(&l)
			body, err := f.control.OpenEnvironmentArtifact(context.Background(), l)
			if body != nil || !errors.Is(err, tt.want) {
				t.Fatalf("open = %v, %v; want nil and %v", body, err, tt.want)
			}
			if n := heldPermits(t, f.control); n != 0 {
				t.Fatalf("held permits after a refusal = %d, want 0", n)
			}
		})
	}
}

// TestEnvironmentArtifactWrongInode refuses a lease naming another inode of
// the same root. Files proves nothing was opened: the error is ordinary, not
// uncertain, and the permit is released.
func TestEnvironmentArtifactWrongInode(t *testing.T) {
	f := newDataControlFixture(t)
	lease, _ := environmentLease(t, f)
	lease.File.Inode++
	body, err := f.control.OpenEnvironmentArtifact(context.Background(), lease)
	if body != nil || err == nil || errors.Is(err, artifact.ErrFileCloseUncertain) {
		t.Fatalf("open of another inode = %v, %v; want nil and an ordinary error", body, err)
	}
	if n := heldPermits(t, f.control); n != 0 {
		t.Fatalf("held permits after an ordinary failure = %d, want 0", n)
	}
}

func TestEnvironmentArtifactWaitsForPermit(t *testing.T) {
	f := newDataControlFixture(t)
	lease, _ := environmentLease(t, f)
	var calls []*ArtifactCall
	for range 4 {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if body, err := f.control.OpenEnvironmentArtifact(ctx, lease); body != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open with every permit held = %v, %v; want its context's error", body, err)
	}
	calls[0].Done()
	body, err := f.control.OpenEnvironmentArtifact(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls[1:] {
		call.Done()
	}
}

func TestEnvironmentArtifactStoppedAdmission(t *testing.T) {
	f := newDataControlFixture(t)
	lease, _ := environmentLease(t, f)
	f.control.StopArtifactAdmission()
	if body, err := f.control.OpenEnvironmentArtifact(context.Background(), lease); body != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("open after stopped admission = %v, %v; want nil and ErrUnavailable", body, err)
	}
}

// TestEnvironmentArtifactUncertainOpenKeepsPermit projects Files'
// classification only. The sentinel is synthetic, not an actual descriptor
// closure failure; the actual Files fault belongs to the Files tests. A nil
// reader with an uncertain error keeps the permit and its error, and a drain
// reports the owner held.
func TestEnvironmentArtifactUncertainOpenKeepsPermit(t *testing.T) {
	c := artifactCallControl()
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.Join(errors.New("validation failed"), artifact.ErrFileCloseUncertain)
	body, err := c.openedEnvironmentArtifact(owner, nil, 1, failure)
	if body != nil || err != failure {
		t.Fatalf("uncertain open = %v, %v; want nil and the original error", body, err)
	}
	if _, ok := c.artifacts.workspaceIO[owner]; !ok || owner.fileError != failure {
		t.Fatal("uncertain open released its permit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.DrainArtifacts(ctx); !errors.Is(err, failure) {
		t.Fatalf("drain = %v, want the retained owner's error", err)
	}

	ordinary := artifactCallControl()
	owner, err = ordinary.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.openedEnvironmentArtifact(owner, nil, 1, artifact.ErrFileConflict); !errors.Is(err, artifact.ErrFileConflict) {
		t.Fatalf("ordinary open failure = %v, want ErrFileConflict", err)
	}
	if len(ordinary.artifacts.workspaceIO) != 0 {
		t.Fatal("ordinary open failure kept its permit")
	}
}
