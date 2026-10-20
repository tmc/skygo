//go:build darwin || linux

package control_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// This uses real installed metadata and Files, never private artifact schema
// writes. Calls model explicit response ownership; it does not test real HTTP.
// All execution remains held; these source tests have never been run.
func TestArtifactCoordinatorRealLifecycle(t *testing.T) {
	resources := &coordinatorArtifactResources{directory: coordinatorArtifactDirectory(t)}
	t.Cleanup(func() { resources.retire(t) })
	rootName := filepath.Join(resources.directory, "root")
	if err := os.Mkdir(rootName, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(rootName, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootName)
	resources.root = root
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	info, err := root.Stat(".")
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		resources.held = errors.New("missing original root identity")
		t.Fatal(resources.held)
	}
	identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	files, err := artifact.Open(root, identity)
	resources.files = files
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	directory := filepath.Join(resources.directory, "state")
	if _, err := store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		resources.held = err
		t.Fatal(err)
	}
	s, err := store.Open(directory)
	resources.store = s
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	c, err := control.NewWithConfig(s, []backend.Target{{Name: "remote", Binding: "binding", Backend: newExecutor()}}, engine, control.Config{Artifacts: files})
	resources.control = c
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	// The original Run/maintenance worker and all dependencies live in the SAME
	// retained frame; failed retirement cannot fall through to Close/RemoveAll.
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	resources.stop, resources.done = stopRun, runDone
	go func() { runDone <- c.Run(runCtx) }()
	otherName := filepath.Join(resources.directory, "other")
	if err := os.Mkdir(otherName, 0700); err != nil {
		t.Fatal(err)
	}
	otherRoot, err := os.OpenRoot(otherName)
	resources.otherRoot = otherRoot
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	otherInfo, err := otherRoot.Stat(".")
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	otherStat, ok := otherInfo.Sys().(*syscall.Stat_t)
	if !ok {
		resources.held = errors.New("missing other root identity")
		t.Fatal(resources.held)
	}
	otherIdentity := artifact.FileID{Device: uint64(otherStat.Dev), Inode: uint64(otherStat.Ino)}
	if _, err := artifact.Initialize(otherRoot, otherIdentity, strings.Repeat("c", 32)); err != nil {
		resources.held = err
		t.Fatal(err)
	}
	otherFiles, err := artifact.Open(otherRoot, otherIdentity)
	resources.otherFiles = otherFiles
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	if _, err := control.NewWithConfig(s, []backend.Target{{Name: "remote", Binding: "binding", Backend: newExecutor()}}, engine, control.Config{Artifacts: otherFiles}); err == nil {
		t.Fatal("different actual root accepted")
	} else if errors.Is(err, artifact.ErrFileCloseUncertain) {
		// RootFacts can fail while closing its own original marker descriptor.
		// That is not the expected positively observed identity mismatch.
		resources.held = err
		t.Fatal(err)
	}
	call := func() *control.ArtifactCall {
		a, err := c.TryArtifactCall()
		if err != nil {
			resources.held = err
			t.Fatal(err)
		}
		t.Cleanup(a.Done)
		return a
	}
	ctx, cancelUpload := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancelUpload()
	retry := func(fn func() error, allowed func(error) bool) error {
		for range 50 {
			err := fn()
			if err == nil || !allowed(err) {
				return err
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		return errors.New("upload source fixture retry bound reached")
	}
	busy := func(err error) bool {
		return errors.Is(err, artifact.ErrFilesBusy) || errors.Is(err, control.ErrArtifactBusy)
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	payload := []byte(strings.Repeat("payload", 1024))
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := archive.Bytes()
	digest := sha256.Sum256(data)
	var upload store.ArtifactUpload
	err = retry(func() error {
		begin := call()
		var e error
		upload, e = c.BeginArtifactUpload(ctx, actor, begin, hex.EncodeToString(digest[:]), int64(len(data)))
		begin.Done()
		return e
	}, control.IsArtifactBeginNotStarted)
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	err = retry(func() error {
		first := call()
		next, e := c.WriteArtifactChunk(ctx, actor, first, upload.Reference.ID, 0, data[:64])
		first.Done()
		if e == nil {
			upload = next
		}
		return e
	}, busy)
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	revision := upload.Revision
	replay := call()
	upload, err = c.WriteArtifactChunk(ctx, actor, replay, upload.Reference.ID, 8, data[8:24])
	replay.Done()
	if err != nil || upload.Revision != revision || upload.Acknowledged != 64 {
		t.Fatalf("earlier replay=%+v %v", upload, err)
	}
	wrong := call()
	_, err = c.WriteArtifactChunk(ctx, actor, wrong, upload.Reference.ID, 8, []byte("different bytes!"))
	wrong.Done()
	if err == nil {
		t.Fatal("conflicting acknowledged bytes accepted")
	}
	err = retry(func() error {
		rest := call()
		next, e := c.WriteArtifactChunk(ctx, actor, rest, upload.Reference.ID, 64, data[64:])
		rest.Done()
		if e == nil {
			upload = next
		}
		return e
	}, busy)
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	var ref artifact.Ref
	err = retry(func() error {
		final := call()
		var e error
		ref, e = c.FinalizeArtifactUpload(ctx, actor, final, upload.Reference.ID)
		final.Done()
		return e
	}, busy)
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	again := call()
	replayRef, err := c.FinalizeArtifactUpload(ctx, actor, again, upload.Reference.ID)
	again.Done()
	if err != nil || replayRef != ref {
		t.Fatalf("finalize replay=%+v %v", replayRef, err)
	}
	open := call()
	download, err := c.BeginArtifactDownload(ctx, actor, open, ref)
	open.Done()
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
	// Always settle actual original response owners before closing this handle.
	defer func() {
		closeCall, err := c.TryArtifactCall()
		if err != nil {
			t.Error(err)
			return
		}
		defer closeCall.Done()
		if err := download.Close(closeCall); err != nil {
			t.Error(err)
		}
	}()
	read := call()
	got, complete, err := c.ReadArtifactChunk(ctx, actor, read, download.ID(), 0, 64<<10)
	if err != nil || !complete || !bytes.Equal(got, data) {
		read.Done()
		t.Fatalf("download=%d complete=%v %v", len(got), complete, err)
	}
	closePending := call()
	err = download.Close(closePending)
	closePending.Done()
	if !errors.Is(err, control.ErrArtifactBusy) {
		read.Done()
		t.Fatalf("pending response released descriptor: %v", err)
	}
	read.Done()
	closeJoined := call()
	err = download.Close(closeJoined)
	closeJoined.Done()
	if err != nil {
		resources.held = err
		t.Fatal(err)
	}
}
