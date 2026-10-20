//go:build linux || darwin

package store_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// The generic route is deliberately not a Local backend or workspace route.
// No job is submitted: these methods cannot supply native or cleanup evidence.
type deleteIdleBackend struct{}

func (deleteIdleBackend) Check(backend.Task) error { return nil }
func (deleteIdleBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected job start")
}
func (deleteIdleBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected job observation")
}
func (deleteIdleBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected job cancellation")
}
func (deleteIdleBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected log request")
}

type deleteCallOwner struct {
	call *control.ArtifactCall
	done bool
}
type deleteControlFixture struct {
	path, objects                                    string
	database                                         *store.Store
	root                                             *os.Root
	files                                            *artifact.Files
	control                                          *control.Control
	actor                                            auth.Actor
	clock                                            atomic.Int64
	initial                                          time.Time
	baseline                                         store.ArtifactReservations
	calls                                            []*deleteCallOwner
	download                                         *control.ArtifactDownload
	cancel                                           context.CancelFunc
	done                                             chan struct{}
	runErr                                           error
	started, stopped                                 bool
	filesAttempted, rootAttempted, databaseAttempted bool
	constructorErr, held                             error
	keeping                                          bool
}

func (f *deleteControlFixture) hold(err error) {
	f.held = errors.Join(f.held, err)
	if !f.keeping {
		f.keeping = true
		wait := make(chan struct{})
		go func() { <-wait; runtime.KeepAlive(f) }()
	}
}
func (f *deleteControlFixture) fail(t *testing.T, err error) {
	t.Helper()
	f.hold(err)
	t.Fatalf("retaining original deletion fixture %s: %v", f.path, err)
}
func (f *deleteControlFixture) check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		f.fail(t, err)
	}
}
func (f *deleteControlFixture) call(t *testing.T) *deleteCallOwner {
	t.Helper()
	call, err := f.control.TryArtifactCall()
	f.check(t, err)
	owner := &deleteCallOwner{call: call}
	f.calls = append(f.calls, owner)
	return owner
}
func (owner *deleteCallOwner) finish() {
	if !owner.done {
		owner.done = true
		owner.call.Done()
	}
}
func (f *deleteControlFixture) start() {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel, f.done, f.started = cancel, make(chan struct{}), true
	go func() { f.runErr = f.control.Run(ctx); close(f.done) }()
}
func (f *deleteControlFixture) closeOriginal(ctx context.Context) error {
	if f.held != nil {
		return f.held
	}
	for _, call := range f.calls {
		call.finish()
	}
	if f.started && !f.stopped {
		if err := f.control.BeginShutdown(); err != nil {
			return err
		}
		if err := f.control.WaitShutdownPrepared(ctx); err != nil {
			return err
		}
		if err := f.control.DrainArtifacts(ctx); err != nil {
			return err
		}
		f.cancel()
		select {
		case <-f.done:
			if f.runErr != nil && !errors.Is(f.runErr, context.Canceled) {
				return f.runErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := f.control.Join(ctx); err != nil {
			return err
		}
		f.stopped = true
	}
	if f.files != nil && !f.filesAttempted {
		f.filesAttempted = true
		if err := f.files.Close(); err != nil {
			return err
		}
	}
	if f.constructorErr != nil {
		return f.constructorErr
	}
	if f.root != nil && !f.rootAttempted {
		f.rootAttempted = true
		if err := f.root.Close(); err != nil {
			return err
		}
	}
	if f.database != nil && !f.databaseAttempted {
		f.databaseAttempted = true
		if err := f.database.Close(); err != nil {
			return err
		}
	}
	return nil
}

func newDeleteControlFixture(t *testing.T, initial time.Time) *deleteControlFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-artifact-delete-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	f := &deleteControlFixture{path: path, objects: filepath.Join(path, "objects"), initial: initial}
	t.Cleanup(func() {
		if f.held != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.closeOriginal(ctx); err != nil {
			f.hold(err)
			t.Errorf("HOLD original deletion fixture %s: %v", f.path, err)
			return
		}
		if err := os.RemoveAll(f.path); err != nil {
			t.Error(err)
		}
	})
	f.check(t, os.Mkdir(f.objects, 0700))
	f.database, err = store.Open(filepath.Join(path, "state"))
	f.check(t, err)
	// This existing test seam is installed BEFORE Control or any goroutine sees
	// Store. Atomic advances model expiry/grace policy, never native elapsed time.
	f.clock.Store(f.initial.UnixNano())
	store.SetClockForTest(f.database, func() time.Time { return time.Unix(0, f.clock.Load()).UTC() })
	f.root, err = os.OpenRoot(f.objects)
	f.check(t, err)
	info, err := f.root.Stat(".")
	f.check(t, err)
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.fail(t, errors.New("actual root identity unavailable"))
	}
	identity := artifact.FileID{Device: uint64(st.Dev), Inode: uint64(st.Ino)}
	facts, err := artifact.Initialize(f.root, identity, strings.Repeat("a", 32))
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.constructorErr = err
	}
	f.check(t, err)
	f.files, err = artifact.Open(f.root, identity)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.constructorErr = err
	}
	f.check(t, err)
	_, err = f.database.InstallArtifacts(store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume})
	f.check(t, err)
	engine := auth.NewLegacy("deletion-tests")
	f.actor, err = engine.Authenticate("Bearer deletion-tests")
	f.check(t, err)
	if f.actor.Principal() != "legacy" {
		f.fail(t, errors.New("fixture replaced the real nonhex principal"))
	}
	// No retention activation, local backend, local output callbacks or local
	// workspace configuration. Deletion must use only the installed service.
	f.control, err = control.NewWithConfig(f.database, []backend.Target{{Name: "generic", Kind: "external", Binding: "generic-binding", Backend: deleteIdleBackend{}}}, engine, control.Config{Artifacts: f.files})
	f.check(t, err)
	_, enabled, err := f.database.RetentionPolicy()
	f.check(t, err)
	if enabled {
		f.fail(t, errors.New("fixture unexpectedly enabled retention"))
	}
	f.baseline, err = f.database.ArtifactReservations()
	f.check(t, err)
	return f
}

type deleteUpload struct {
	upload store.ArtifactUpload
	ref    artifact.Ref
	file   artifact.FileID
	data   []byte
}

func (f *deleteControlFixture) upload(t *testing.T, state string) deleteUpload {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	body := []byte("actual deletion-worker payload\n")
	f.check(t, tw.WriteHeader(&tar.Header{Name: "input", Mode: 0600, Size: int64(len(body)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR, ModTime: time.Unix(0, 0).UTC()}))
	_, writeErr := tw.Write(body)
	f.check(t, errors.Join(writeErr, tw.Close()))
	data := archive.Bytes()
	hash := sha256.Sum256(data)
	u, err := f.database.BeginArtifactUpload(f.actor, hex.EncodeToString(hash[:]), uint64(len(data)))
	f.check(t, err)
	ref := artifact.Ref{ID: u.Reference.ID, SHA256: u.Reference.SHA256, Size: int64(u.Reference.Size)}
	file, err := f.files.CreatePart(ref)
	f.check(t, err)
	storedFile := store.ArtifactFile{Device: file.Device, Inode: file.Inode}
	u, err = f.database.ArtifactPartCreated(f.actor, store.ArtifactCursor{ID: ref.ID, Revision: u.Revision}, storedFile)
	f.check(t, err)
	_, err = f.files.WritePart(ref, file, 0, 0, data)
	f.check(t, err)
	u, err = f.database.ArtifactChunkSynced(f.actor, store.ArtifactCursor{ID: ref.ID, Revision: u.Revision}, storedFile, 0, uint64(len(data)), ref.SHA256)
	f.check(t, err)
	out := deleteUpload{upload: u, ref: ref, file: file, data: data}
	if state == "writable" {
		return out
	}
	u, err = f.database.BeginArtifactFinalize(f.actor, store.ArtifactCursor{ID: ref.ID, Revision: u.Revision})
	f.check(t, err)
	out.upload = u
	if state == "finalizing-part" || state == "finalizing-both" {
		if state == "finalizing-both" {
			// A real second name for the SAME actual part inode models the
			// interrupted link-before-part-unlink publication boundary. This is
			// not a claimed native crash or a fabricated seal/completion receipt.
			f.check(t, os.Link(filepath.Join(f.objects, ref.ID+".part"), filepath.Join(f.objects, ref.ID+".tar")))
			part, err := f.root.Lstat(ref.ID + ".part")
			f.check(t, err)
			object, err := f.root.Lstat(ref.ID + ".tar")
			f.check(t, err)
			if !os.SameFile(part, object) {
				f.fail(t, errors.New("finalizing fixture does not have two actual names for its original inode"))
			}
		}
		return out
	}
	receipt, err := f.files.SealPart(ref, file, func() error {
		var stats syscall.Statfs_t
		if err := syscall.Statfs(f.objects, &stats); err != nil {
			return err
		}
		if uint64(stats.Bavail)*uint64(stats.Bsize) < 2<<30 {
			return errors.New("insufficient fixture publication space")
		}
		return nil
	})
	f.check(t, err)
	if receipt.File != file {
		f.fail(t, errors.New("actual seal changed original part identity"))
	}
	if state == "finalizing-published" {
		return out
	}
	_, err = f.database.CompleteArtifactUpload(f.actor, store.ArtifactCursor{ID: ref.ID, Revision: u.Revision}, u.Reference, store.ArtifactFile{Device: receipt.File.Device, Inode: receipt.File.Inode}, store.ArtifactManifest{Reference: u.Reference, ExpandedBytes: receipt.Manifest.ExpandedBytes, RegularFiles: receipt.Manifest.RegularFiles, Directories: receipt.Manifest.Directories})
	f.check(t, err)
	return out
}
func (f *deleteControlFixture) mark(t *testing.T, upload deleteUpload, complete bool) {
	t.Helper()
	if complete {
		f.clock.Store(f.initial.Add(25 * time.Hour).UnixNano())
		f.check(t, f.control.RemoveArtifact(context.Background(), f.actor, upload.ref))
	} else {
		_, err := f.database.CancelArtifactUpload(f.actor, store.ArtifactCursor{ID: upload.ref.ID, Revision: upload.upload.Revision})
		f.check(t, err)
	}
	work, found, err := f.database.NextArtifactCleanup("")
	f.check(t, err)
	wantKind := "upload"
	if complete {
		wantKind = "object"
	} else if upload.upload.State == "finalizing" {
		wantKind = "finalizing"
	}
	if !found || work.Delete == nil || work.Delete.Kind != wantKind || work.Delete.Owner != f.actor.Principal() || work.Delete.Ref.ID != upload.ref.ID || work.Delete.File != (store.ArtifactFile{Device: upload.file.Device, Inode: upload.file.Inode}) {
		f.fail(t, errors.New("public mark did not commit the exact actual-file deletion intent"))
	}
}
func (f *deleteControlFixture) projection(t *testing.T) store.ArtifactReservations {
	t.Helper()
	p, err := f.database.ArtifactReservations()
	f.check(t, err)
	return p
}
func (f *deleteControlFixture) awaitRetired(t *testing.T, upload deleteUpload) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		p := f.projection(t)
		_, found, err := f.database.NextArtifactCleanup("")
		f.check(t, err)
		if !found && reflect.DeepEqual(p, f.baseline) {
			for _, suffix := range []string{".part", ".tar"} {
				_, err := f.root.Lstat(upload.ref.ID + suffix)
				if !errors.Is(err, os.ErrNotExist) {
					f.fail(t, fmt.Errorf("retired charge without physical unlink of %s: %v", suffix, err))
				}
			}
			return
		}
		select {
		case <-f.done:
			f.fail(t, fmt.Errorf("original Run returned before retirement: %w", f.runErr))
		case <-timer.C:
			f.fail(t, errors.New("original maintenance worker did not unlink and release canonical reservations"))
		case <-tick.C:
		}
	}
}
func (f *deleteControlFixture) assertBlocked(t *testing.T, expected store.ArtifactReservations, path string, original os.FileInfo) {
	t.Helper()
	// This is a bounded no-progress observation, not proof of a native timeout,
	// death, scheduling attempt or lifetime beyond this test interval.
	until := time.NewTimer(600 * time.Millisecond)
	defer until.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if !reflect.DeepEqual(f.projection(t), expected) {
			f.fail(t, errors.New("blocked deletion released reservations"))
		}
		now, err := os.Lstat(path)
		f.check(t, err)
		if !os.SameFile(original, now) {
			f.fail(t, errors.New("blocked deletion changed original name identity"))
		}
		select {
		case <-until.C:
			return
		case <-f.done:
			f.fail(t, fmt.Errorf("original Run failed while work should remain charged: %w", f.runErr))
		case <-tick.C:
		}
	}
}

func TestArtifactDeleteControlWithoutLocalWorkspace(t *testing.T) {
	for _, state := range []string{"complete", "writable", "finalizing-part", "finalizing-published", "finalizing-both"} {
		t.Run(state, func(t *testing.T) {
			f := newDeleteControlFixture(t, time.Now().UTC())
			u := f.upload(t, state)
			f.mark(t, u, state == "complete")
			if reflect.DeepEqual(f.projection(t), f.baseline) {
				f.fail(t, errors.New("mark released charges before original worker completion"))
			}
			f.start()
			f.awaitRetired(t, u)
		})
	}
}

func TestArtifactDeleteControlSharedFourOwners(t *testing.T) {
	f := newDeleteControlFixture(t, time.Now().UTC())
	u := f.upload(t, "complete")
	for i := 0; i < 4; i++ {
		f.call(t)
	}
	f.mark(t, u, true)
	charged := f.projection(t)
	path := filepath.Join(f.objects, u.ref.ID+".tar")
	original, err := os.Lstat(path)
	f.check(t, err)
	f.start()
	f.assertBlocked(t, charged, path, original)
	for _, call := range f.calls {
		call.finish()
	}
	f.awaitRetired(t, u)
}

func TestArtifactDeleteControlWrongInode(t *testing.T) {
	f := newDeleteControlFixture(t, time.Now().UTC())
	u := f.upload(t, "complete")
	f.mark(t, u, true)
	charged := f.projection(t)
	path := filepath.Join(f.objects, u.ref.ID+".tar")
	f.check(t, os.Rename(path, filepath.Join(f.path, "original-object")))
	f.check(t, os.WriteFile(path, u.data, 0600))
	replacement, err := os.Lstat(path)
	f.check(t, err)
	f.start()
	f.assertBlocked(t, charged, path, replacement)
	data, err := os.ReadFile(path)
	f.check(t, err)
	if !bytes.Equal(data, u.data) {
		f.fail(t, errors.New("wrong-inode payload changed"))
	}
	// No pathname repair, manual unlink or manufactured receipt forces success.
	// Positive original shutdown/joins precede eventual throwaway-tree cleanup.
}

func (f *deleteControlFixture) openDownload(t *testing.T, ref artifact.Ref) {
	t.Helper()
	call := f.call(t)
	var err error
	f.download, err = f.control.BeginArtifactDownload(context.Background(), f.actor, call.call, ref)
	f.check(t, err)
	call.finish() // Actual synchronous call returned; its response owner ends now.
}
func (f *deleteControlFixture) closeDownload(t *testing.T) string {
	t.Helper()
	id := f.download.ID()
	call := f.call(t)
	err := f.download.Close(call.call)
	// Preserve original download + call on any failed or uncertain Close.
	f.check(t, err)
	call.finish()
	f.download = nil
	return id
}

func TestArtifactDeleteControlLiveOriginalDownload(t *testing.T) {
	// Backdate only this policy fixture BEFORE any Store metadata is created.
	// The actual cached reader is opened, never reconstructed after expiry.
	f := newDeleteControlFixture(t, time.Now().UTC().Add(-25*time.Hour))
	u := f.upload(t, "complete")
	// Keep the actual Begin response owner while asserting that this original
	// reader blocks retirement; an idle expired reader may now close normally.
	heldCall := f.call(t)
	var err error
	f.download, err = f.control.BeginArtifactDownload(context.Background(), f.actor, heldCall.call, u.ref)
	f.check(t, err)
	f.clock.Store(time.Now().UTC().UnixNano())
	err = f.control.RemoveArtifact(context.Background(), f.actor, u.ref)
	if !errors.Is(err, store.ErrArtifactConflict) {
		f.fail(t, fmt.Errorf("modeled expiry bypassed original reader: got %v, want %v", err, store.ErrArtifactConflict))
	}
	_, found, err := f.database.NextArtifactCleanup("")
	f.check(t, err)
	if found {
		f.fail(t, errors.New("live original reader permitted a committed deletion intent"))
	}
	charged := f.projection(t)
	path := filepath.Join(f.objects, u.ref.ID+".tar")
	original, err := os.Lstat(path)
	f.check(t, err)
	f.start()
	f.assertBlocked(t, charged, path, original)
	beforeClose := time.Now().UTC()
	heldCall.finish()
	// Expiry may now own settling. Retry only ordinary Busy against the SAME
	// original cached d; each actual call has its own observed response Done.
	// Any typed/Store/other error retains the fixture, never retries raw Close.
	download := f.download
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		call := f.call(t)
		err := download.Close(call.call)
		call.finish()
		if err == nil {
			f.download = nil // Includes idempotent nil after original expiry Finish.
			break
		}
		if err != control.ErrArtifactBusy {
			f.fail(t, err)
		}
		select {
		case <-f.done:
			f.fail(t, fmt.Errorf("original Run returned while settling reader: %w", f.runErr))
		case <-timer.C:
			f.fail(t, errors.New("same original download remained busy before close observation"))
		case <-tick.C:
		}
	}
	afterClose := time.Now().UTC()
	// Control supplies REAL time to FinishArtifactDownload. Bracket that
	// original positive Close, then align only Store's modeled policy clock.
	// This timestamp is not descriptor/native closure authority.
	if afterClose.Before(beforeClose) || !afterClose.Before(beforeClose.Add(time.Hour)) {
		f.fail(t, errors.New("actual close interval exceeded the grace-test observation bound"))
	}
	f.clock.Store(afterClose.UnixNano())
	if err := f.control.RemoveArtifact(context.Background(), f.actor, u.ref); !errors.Is(err, store.ErrArtifactConflict) {
		f.fail(t, fmt.Errorf("last-reader grace was bypassed: got %v, want %v", err, store.ErrArtifactConflict))
	}
	f.clock.Store(afterClose.Add(time.Hour + time.Second).UnixNano())
	// Run is already active, so do not require a read-only selection to win a
	// race with the worker's legitimate completion after this public mark.
	f.check(t, f.control.RemoveArtifact(context.Background(), f.actor, u.ref))
	f.awaitRetired(t, u)
}

func TestArtifactDeleteControlClosedReaderTail(t *testing.T) {
	f := newDeleteControlFixture(t, time.Now().UTC())
	u := f.upload(t, "complete")
	ids := make([]string, 0, 65)
	for i := 0; i < 65; i++ {
		f.openDownload(t, u.ref)
		ids = append(ids, f.closeDownload(t))
	}
	f.mark(t, u, true)
	f.start()
	f.awaitRetired(t, u)
	for _, id := range ids {
		_, err := f.database.GetArtifactDownload(f.actor, id)
		if !errors.Is(err, store.ErrNotFound) {
			f.fail(t, fmt.Errorf("closed reader tail survived final retirement: %w", err))
		}
	}
	// This crosses Store's bounded64 tail and requires worker reselection with
	// real missing-name+parent-sync calls; it does not observe each private tick.
}
