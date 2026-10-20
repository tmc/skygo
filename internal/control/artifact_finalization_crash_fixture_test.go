//go:build linux || darwin

package control

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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

type finalizationCrashBoundary struct {
	Stage        string
	Reference    artifact.Ref
	File         artifact.FileID
	Acknowledged uint64
}

// These are idle, positively returned API checkpoints, not interruption inside
// HTTP, a syscall or production Finalize. No Local.Start, Control.Run or worker
// goroutine is used. Parent owns the original subprocess and its positive Wait.
type finalizationCrashResources struct {
	directory string
	root      *os.Root
	files     *artifact.Files
	database  *store.Store
	idle      *local.Backend
	control   *Control
	actor     auth.Actor
	reader    *artifact.Reader
	held      error
	keepOnce  sync.Once
}

// Purpose-only test custody: preparation must keep the SAME open Store/Files
// alive through READY and the driver's later process interruption. Failed
// recovery likewise retains exact owners/errors, not just a pathname or DTO.
var finalizationCrashHolds struct {
	sync.Mutex
	frames []*finalizationCrashResources
}

func (f *finalizationCrashResources) keep() {
	f.keepOnce.Do(func() {
		finalizationCrashHolds.Lock()
		finalizationCrashHolds.frames = append(finalizationCrashHolds.frames, f)
		finalizationCrashHolds.Unlock()
	})
}

func (f *finalizationCrashResources) fail(t *testing.T, err error) {
	t.Helper()
	f.held = errors.Join(f.held, err)
	f.keep()
	t.Fatal(f.held)
}

func finalizationCrashArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	var buffer bytes.Buffer
	body := bytes.Repeat([]byte("finalization boundary payload\n"), 128)
	writer := tar.NewWriter(&buffer)
	if err := writer.WriteHeader(&tar.Header{Name: "payload.txt", Mode: 0600, Size: int64(len(body)), Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	if len(data) >= 64<<10 {
		t.Fatal("deterministic archive exceeds one actual bounded reader quantum")
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}

func finalizationCrashOpen(t *testing.T, directory string, initialize bool) *finalizationCrashResources {
	t.Helper()
	f := &finalizationCrashResources{directory: directory}
	if initialize {
		f.keep()
	} else {
		t.Cleanup(func() { f.retire(t) })
	}
	rootName := filepath.Join(directory, "artifacts")
	if initialize {
		if err := os.Mkdir(rootName, 0700); err != nil {
			f.fail(t, err)
		}
	}
	var err error
	f.root, err = os.OpenRoot(rootName)
	if err != nil {
		f.fail(t, err)
	}
	info, err := f.root.Lstat(".")
	if err != nil {
		f.fail(t, err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.fail(t, errors.New("actual root identity unavailable"))
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	var facts artifact.RootFacts
	if initialize {
		facts, err = artifact.Initialize(f.root, identity, strings.Repeat("b", 32))
		if err != nil {
			f.fail(t, err)
		}
	}
	f.files, err = artifact.Open(f.root, identity)
	if err != nil {
		f.fail(t, err)
	}
	state := filepath.Join(directory, "state")
	if initialize {
		_, err = store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume})
		if err != nil {
			f.fail(t, err)
		}
	}
	f.database, err = store.Open(state)
	if err != nil {
		f.fail(t, err)
	}
	f.idle, err = local.New(filepath.Join(directory, "idle-local"))
	if err != nil {
		f.fail(t, err)
	}
	engine := auth.NewLegacy("")
	f.actor, err = engine.Authenticate("")
	if err != nil {
		f.fail(t, err)
	}
	binding, err := backend.LocalBinding(f.database.Owner())
	if err != nil {
		f.fail(t, err)
	}
	target := backend.Target{Name: "idle-local", Kind: "local", Binding: binding, Backend: f.idle}
	f.control, err = NewWithConfig(f.database, []backend.Target{target}, engine, Config{Artifacts: f.files, LocalBackend: f.idle})
	if err != nil {
		f.fail(t, err)
	}
	return f
}

func (f *finalizationCrashResources) sample(t *testing.T) {
	t.Helper()
	for range 32 {
		q := f.control.physicalQuantum(context.Background())
		if q.Err != nil {
			f.fail(t, q.Err)
		}
		if q.Complete {
			if q.Registered || q.Held || f.control.physicalRegistered() {
				f.fail(t, errors.New("complete interval retained an original cursor"))
			}
			return
		}
	}
	f.fail(t, errors.New("bounded synchronous sampler did not finish the small explained root"))
}

func (f *finalizationCrashResources) call(t *testing.T) *ArtifactCall {
	t.Helper()
	call, err := f.control.TryArtifactCall()
	if err != nil {
		f.fail(t, err)
	}
	return call
}

func (f *finalizationCrashResources) idleBoundary(t *testing.T) {
	t.Helper()
	c := f.control
	c.requestPhysicalClose()
	for i := 0; c.physicalRegistered(); i++ {
		if i == 32 {
			f.fail(t, errors.New("original finite cursor did not positively close"))
		}
		q := c.physicalQuantum(context.Background())
		if q.Err != nil || q.Held {
			f.fail(t, fmt.Errorf("original finite scan close: %w", errors.Join(q.Err, ErrUnavailable)))
		}
	}
	c.artifacts.mu.Lock()
	active := len(c.artifacts.calls) + len(c.artifacts.workspaceIO) + len(c.artifacts.downloads) + len(c.artifacts.uploads)
	pending := c.artifacts.physicalWorkPending || c.artifacts.physicalFrameRegistered || c.artifacts.physicalHeld != nil
	c.artifacts.mu.Unlock()
	if active != 0 || pending {
		f.fail(t, errors.New("idle checkpoint retained active original owners"))
	}
	if err := f.idle.CheckShutdown(); err != nil {
		f.fail(t, err)
	}
}

func (f *finalizationCrashResources) projection(t *testing.T, b finalizationCrashBoundary, complete bool) {
	t.Helper()
	snapshot, err := f.database.ArtifactReservations()
	if err != nil {
		f.fail(t, err)
	}
	if len(snapshot.Subjects) != 2 {
		f.fail(t, fmt.Errorf("durable projection has %d subjects, want one upload plus object", len(snapshot.Subjects)))
	}
	var uploads, objects int
	for _, subject := range snapshot.Subjects {
		if subject.ID != b.Reference.ID {
			f.fail(t, errors.New("projection allocated a different durable ID"))
		}
		switch subject.Kind {
		case "upload":
			uploads++
			if subject.File.Device != b.File.Device || subject.File.Inode != b.File.Inode {
				f.fail(t, errors.New("upload projection changed original inode"))
			}
		case "object":
			objects++
			expected := store.ArtifactFile{}
			if complete {
				expected = store.ArtifactFile{Device: b.File.Device, Inode: b.File.Inode}
			}
			if subject.File != expected {
				f.fail(t, errors.New("object projection differs from actual publication state"))
			}
		default:
			f.fail(t, errors.New("unexpected durable projection kind"))
		}
	}
	if uploads != 1 || objects != 1 {
		f.fail(t, errors.New("durable projection does not contain exactly one upload and object"))
	}
}

func (f *finalizationCrashResources) names(t *testing.T, b finalizationCrashBoundary, published bool) {
	t.Helper()
	name, absent := b.Reference.ID+".part", b.Reference.ID+".tar"
	if published {
		name, absent = absent, name
	}
	info, err := f.root.Lstat(name)
	if err != nil {
		f.fail(t, err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() != b.Reference.Size || uint64(physical.Dev) != b.File.Device || uint64(physical.Ino) != b.File.Inode || uint64(physical.Nlink) != 1 {
		f.fail(t, errors.New("actual published/part identity or link count differs"))
	}
	if _, err := f.root.Lstat(absent); !errors.Is(err, os.ErrNotExist) {
		f.fail(t, fmt.Errorf("unexpected opposite publication name: %w", errors.Join(err, artifact.ErrFileConflict)))
	}
}

func finalizationCrashPrepare(t *testing.T, directory, stage string) finalizationCrashBoundary {
	t.Helper()
	if stage != "before-publication" && stage != "after-publication" {
		t.Fatal("invalid finalization checkpoint")
	}
	f := finalizationCrashOpen(t, directory, true)
	data, digest := finalizationCrashArchive(t)
	f.sample(t)
	call := f.call(t)
	upload, err := f.control.BeginArtifactUpload(context.Background(), f.actor, call, digest, int64(len(data)))
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	f.sample(t)
	call = f.call(t)
	upload, err = f.control.WriteArtifactChunk(context.Background(), f.actor, call, upload.Reference.ID, 0, data)
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	f.sample(t)
	call = f.call(t)
	_, err = f.control.FinalizeArtifactUpload(context.Background(), f.actor, call, upload.Reference.ID)
	call.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		f.fail(t, fmt.Errorf("first structural Finalize: %w", errors.Join(err, ErrUnavailable)))
	}
	upload, err = f.database.GetArtifactUpload(f.actor, upload.Reference.ID)
	if err != nil {
		f.fail(t, err)
	}
	if upload.State != "finalizing" || upload.Acknowledged != uint64(len(data)) {
		f.fail(t, errors.New("actual structural CAS did not retain finalizing upload"))
	}
	_, file, err := f.database.GetArtifactUploadFile(f.actor, store.ArtifactCursor{ID: upload.Reference.ID, Revision: upload.Revision})
	if err != nil {
		f.fail(t, err)
	}
	boundary := finalizationCrashBoundary{
		Stage:        stage,
		Reference:    artifact.Ref{ID: upload.Reference.ID, SHA256: digest, Size: int64(len(data))},
		File:         artifact.FileID{Device: file.Device, Inode: file.Inode},
		Acknowledged: upload.Acknowledged,
	}
	f.projection(t, boundary, false)
	f.names(t, boundary, false)
	if stage == "after-publication" {
		f.sample(t)
		call = f.call(t)
		_, current, identity, end, err := f.control.uploadForIO(context.Background(), f.actor, call, boundary.Reference.ID)
		if err != nil {
			call.Done()
			f.fail(t, err)
		}
		if current.State != "finalizing" || identity != file {
			end()
			call.Done()
			f.fail(t, errors.New("seal owner no longer matches actual finalizing upload"))
		}
		sealed, sealErr := f.files.SealPart(boundary.Reference, boundary.File, f.control.uploadLinkGrowthCheck)
		if sealErr != nil {
			sealErr = f.control.artifactFileError(call, sealErr)
		}
		end()
		call.Done()
		if sealErr != nil {
			f.fail(t, sealErr)
		}
		f.control.physicalChanged()
		if sealed.File != boundary.File || sealed.Manifest.Reference != boundary.Reference {
			f.fail(t, errors.New("actual Seal receipt differs from original upload"))
		}
		// Deliberately stop BETWEEN actual completed APIs. No Store Complete/grant
		// or claim of interruption inside the production Finalize handler occurs.
		f.names(t, boundary, true)
		f.projection(t, boundary, false)
		retained, err := f.database.GetArtifactUpload(f.actor, boundary.Reference.ID)
		if err != nil {
			f.fail(t, err)
		}
		if retained.State != "finalizing" || retained.Revision != upload.Revision || retained.Acknowledged != boundary.Acknowledged {
			f.fail(t, errors.New("physical publication changed the durable finalizing prefix"))
		}
	}
	f.idleBoundary(t)
	// f remains strongly reachable with its original Store/Files/root open. The
	// process driver, not a test cleanup, interrupts it AFTER the idle handshake.
	return boundary
}

func finalizationCrashRecover(t *testing.T, directory string, b finalizationCrashBoundary) {
	t.Helper()
	if b.Stage != "before-publication" && b.Stage != "after-publication" {
		t.Fatal("invalid retained checkpoint stage")
	}
	if err := b.Reference.Validate(); err != nil {
		t.Fatal(err)
	}
	data, digest := finalizationCrashArchive(t)
	if b.Reference.Size != int64(len(data)) || b.Reference.SHA256 != digest || b.Acknowledged != uint64(len(data)) || b.File.Device == 0 || b.File.Inode == 0 {
		t.Fatal("retained checkpoint differs from deterministic archive")
	}
	f := finalizationCrashOpen(t, directory, false)
	upload, err := f.database.GetArtifactUpload(f.actor, b.Reference.ID)
	if err != nil {
		f.fail(t, err)
	}
	if upload.State != "finalizing" || upload.Acknowledged != b.Acknowledged {
		f.fail(t, errors.New("reopened Store lost actual durable finalizing prefix"))
	}
	f.projection(t, b, false)
	f.names(t, b, b.Stage == "after-publication")
	f.sample(t)
	call := f.call(t)
	recovered, err := f.control.FinalizeArtifactUpload(context.Background(), f.actor, call, b.Reference.ID)
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	if recovered != b.Reference {
		f.fail(t, errors.New("recovery returned a different durable reference"))
	}
	call = f.call(t)
	replayed, err := f.control.FinalizeArtifactUpload(context.Background(), f.actor, call, b.Reference.ID)
	call.Done()
	if err != nil {
		f.fail(t, err)
	}
	if replayed != b.Reference {
		f.fail(t, errors.New("complete replay changed original reference"))
	}
	final, err := f.database.GetArtifactUpload(f.actor, b.Reference.ID)
	if err != nil {
		f.fail(t, err)
	}
	if final.State != "complete" || final.Acknowledged != b.Acknowledged {
		f.fail(t, errors.New("same-ID recovery did not complete exact upload"))
	}
	f.projection(t, b, true)
	f.names(t, b, true)
	f.reader, err = f.files.OpenObject(b.Reference, b.File)
	if err != nil {
		f.fail(t, err)
	}
	actual := make([]byte, len(data))
	n, readErr := f.reader.ReadAt(actual, 0)
	closeErr := f.reader.Close()
	if closeErr != nil {
		f.fail(t, errors.Join(readErr, closeErr))
	}
	f.reader = nil
	if readErr != nil || n != len(data) || !bytes.Equal(actual, data) {
		f.fail(t, fmt.Errorf("recovered actual archive differs: %w", errors.Join(readErr, artifact.ErrFileConflict)))
	}
	actualDigest := sha256.Sum256(actual)
	if hex.EncodeToString(actualDigest[:]) != b.Reference.SHA256 {
		f.fail(t, errors.New("recovered actual digest differs"))
	}
	f.idleBoundary(t)
}

func (f *finalizationCrashResources) retire(t *testing.T) {
	t.Helper()
	if f.held != nil {
		f.keep()
		t.Errorf("retaining original recovery frame at %s: %v", f.directory, f.held)
		return
	}
	fail := func(err error) {
		f.held = errors.Join(f.held, err)
		f.keep()
		t.Errorf("retaining original recovery frame at %s: %v", f.directory, f.held)
	}
	if f.control != nil {
		f.idleBoundary(t)
		// This bounds the service wait, not synchronous mutex or kernel I/O.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := f.control.DrainArtifacts(ctx)
		cancel()
		if err != nil {
			fail(err)
			return
		}
	}
	if f.reader != nil {
		fail(errors.New("original object reader remains owned"))
		return
	}
	if f.idle != nil {
		if err := f.idle.Close(); err != nil {
			fail(err)
			return
		}
	}
	if f.files != nil {
		if err := f.files.Close(); err != nil {
			fail(err)
			return
		}
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			fail(err)
			return
		}
	}
	if f.database != nil {
		if err := f.database.Close(); err != nil {
			fail(err)
			return
		}
	}
	// Namespace evidence stays private/persistent even after positive cleanup;
	// parent must never infer successful recovery from mere process termination.
	t.Logf("positively closed recovery owners; preserving checkpoint evidence: %s", f.directory)
}
