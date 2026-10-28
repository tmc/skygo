//go:build linux || darwin

package server

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// effectIntegration is a MODELED integration, not native materialization
// evidence. Its one effect holds a Control artifact I/O permit through the
// actual opener, as an environment materialization does, until StopEffects
// stops it; with ignoreStop it keeps holding. Close joins the effect, then
// marks and finishes the effect's Store read.
type effectIntegration struct {
	*testIntegration
	ignoreStop bool

	engine   *auth.Engine
	control  *control.Control
	runDone  <-chan struct{} // the server's Run, for setup only
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	lease    store.ArtifactLease
	closeErr error // the body's Close
}

func newEffectIntegration() *effectIntegration {
	in := &effectIntegration{testIntegration: newTestIntegration(), stop: make(chan struct{})}
	in.targets = nil
	return in
}

func (in *effectIntegration) install(s *store.Store, e *auth.Engine) (Integration, error) {
	if _, err := in.testIntegration.install(s, e); err != nil {
		return nil, err
	}
	in.engine = e
	return in, nil
}

func (in *effectIntegration) BindControl(c *control.Control) error {
	in.record("bind")
	in.control = c
	return nil
}

func (in *effectIntegration) StopEffects() {
	in.record("stop effects")
	if !in.ignoreStop {
		in.stopOnce.Do(func() { close(in.stop) })
	}
}

func (in *effectIntegration) Close(ctx context.Context) error {
	if in.done != nil {
		select {
		case <-in.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if in.closeErr != nil {
		return in.closeErr
	}
	if in.done != nil {
		closure, err := in.store.MarkArtifactDownloadClosing(store.ArtifactDownloadClosure{Owner: in.actor().Principal(), LeaseID: in.lease.ID, Revision: in.lease.Revision, Ref: in.lease.Reference, File: in.lease.File, RootID: in.lease.RootID, Producer: in.lease.Producer})
		if err == nil {
			err = in.store.FinishArtifactDownload(closure, time.Now().UTC())
		}
		if err != nil {
			return err
		}
		in.record("finished")
	}
	return in.testIntegration.Close(ctx)
}

func (in *effectIntegration) actor() auth.Actor {
	a, err := in.engine.Authenticate("")
	if err != nil {
		panic(err)
	}
	return a
}

// effectUploadBusy reports whether err is only Files' ordinary Busy: the
// sentinel itself, the growth guard's Busy with its reason, or a Files join
// of exactly one of those. Busy joined with anything else, such as an
// uncertain close, is not ordinary; nor is an authorization, expiry or
// conflict error, or the upload's own ErrArtifactBusy.
func effectUploadBusy(err error) bool {
	if err == nil {
		return false
	}
	if err == artifact.ErrFilesBusy {
		return true
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		errs := j.Unwrap()
		return len(errs) == 1 && effectUploadBusy(errs[0])
	}
	return errors.Unwrap(err) == artifact.ErrFilesBusy && strings.HasPrefix(err.Error(), artifact.ErrFilesBusy.Error()+": ")
}

// invoke calls op on a and then Done, even if op fails the test or panics,
// so the call is never held past op.
func invoke(a *control.ArtifactCall, op func(*control.ArtifactCall) error) error {
	defer a.Done()
	return op(a)
}

// attempt runs op, one actual operation, on a new ArtifactCall and calls
// Done as soon as op returns. While no call is free, or op fails with an
// error retry allows, it waits for the Control to observe storage ready
// again, then calls op on another new call. It returns op's first other
// result. It fails after 50 attempts, if the server's Run returns, or once
// ctx is done: before a new attempt, after an operation's Done, before its
// result is accepted or retried, and while waiting.
//
// ctx bounds only this orchestration and the late acceptance of a result;
// op receives ctx through its own calls, but an operation already in a
// system call returns when that call does, not at ctx's deadline.
func (in *effectIntegration) attempt(t *testing.T, ctx context.Context, name string, retry func(error) bool, op func(*control.ArtifactCall) error) error {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var last error
	check := func() {
		t.Helper()
		select {
		case <-in.runDone:
			t.Fatalf("%s: server Run returned; last attempt: %v", name, last)
		default:
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("%s: setup deadline: %v; last attempt: %v", name, err, last)
		}
	}
	for n := 1; n <= 50; n++ {
		check()
		a, err := in.control.TryArtifactCall()
		if err == nil {
			err = invoke(a, op)
			last = err
			check()
			if err == nil || !retry(err) {
				return err
			}
		} else if err != control.ErrArtifactBusy {
			return err
		}
		last = err
		t.Logf("%s attempt %d: %v", name, n, err)
		for ready := false; !ready; ready = in.control.StorageReady() {
			select {
			case <-in.runDone:
				t.Fatalf("%s: server Run returned; last attempt: %v", name, last)
			case <-ctx.Done():
				t.Fatalf("%s: storage not ready by the setup deadline; last attempt: %v", name, last)
			case <-tick.C:
			}
		}
	}
	check()
	t.Fatalf("%s: attempt bound reached: %v", name, last)
	return nil
}

// begin uploads data through the server's Control, admits a read of it in
// the server's Store and opens it through the opener. The effect holds the
// body until StopEffects.
//
// Every structural upload step invalidates the Control's physical census,
// so the upload follows the protocol, within the fixture's startup budget.
// Begin is repeated only when its guard refused it before the Store, so no
// admitted upload is begun again. A chunk is written again only after an
// ordinary Busy, with the same upload, offset and bytes, and the offset
// advances only on success. The first Finalize of a writable upload begins
// finalizing and reports Busy; the same upload is finalized again.
func (in *effectIntegration) begin(t *testing.T, data []byte) {
	t.Helper()
	actor := in.actor()
	// ctx bounds the upload. The body outlives begin, so it is opened
	// without ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	digest := sha256.Sum256(data)
	sum := hex.EncodeToString(digest[:])
	var id string
	err := in.attempt(t, ctx, "begin upload", control.IsArtifactBeginNotStarted, func(a *control.ArtifactCall) error {
		u, err := in.control.BeginArtifactUpload(ctx, actor, a, sum, int64(len(data)))
		if err == nil {
			id = u.Reference.ID
		}
		return err
	})
	if err != nil {
		t.Fatalf("begin upload: %v", err)
	}
	for off := 0; off < len(data); {
		end := min(off+64<<10, len(data))
		err := in.attempt(t, ctx, "write upload", effectUploadBusy, func(a *control.ArtifactCall) error {
			u, err := in.control.WriteArtifactChunk(ctx, actor, a, id, int64(off), data[off:end])
			if err == nil && u.Acknowledged != uint64(end) {
				t.Fatalf("write upload %s at %d acknowledged %d, want %d", id, off, u.Acknowledged, end)
			}
			return err
		})
		if err != nil {
			t.Fatalf("write upload %s at %d: %v", id, off, err)
		}
		off = end
	}
	// After a Busy the same upload is writable, if the guard refused it, or
	// finalizing, once its first Finalize committed; never writable again.
	finalizing := false
	busy := func(err error) bool {
		if !effectUploadBusy(err) {
			return false
		}
		u, err := in.store.GetArtifactUpload(actor, id)
		if err != nil {
			t.Fatalf("upload %s after Busy finalize: %v", id, err)
		}
		switch {
		case u.State == "finalizing":
			finalizing = true
		case u.State != "writable" || finalizing:
			t.Fatalf("upload %s after Busy finalize is %q, finalizing seen %v", id, u.State, finalizing)
		}
		return true
	}
	var ref artifact.Ref
	err = in.attempt(t, ctx, "finalize upload", busy, func(a *control.ArtifactCall) error {
		var err error
		ref, err = in.control.FinalizeArtifactUpload(ctx, actor, a, id)
		return err
	})
	if err != nil {
		t.Fatalf("finalize upload %s: %v", id, err)
	}
	if ref.ID != id || ref.SHA256 != sum || ref.Size != int64(len(data)) {
		t.Fatalf("finalized %+v, want upload %s of %d bytes with SHA-256 %s", ref, id, len(data), sum)
	}
	lease, err := in.store.BeginArtifactDownload(actor, store.ArtifactRef{ID: ref.ID, SHA256: ref.SHA256, Size: uint64(ref.Size)}, in.control.ArtifactProducer())
	if err != nil {
		t.Fatal(err)
	}
	in.lease = lease
	body, err := in.control.OpenEnvironmentArtifact(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	in.record("open")
	in.done = make(chan struct{})
	go func() {
		defer close(in.done)
		<-in.stop
		// Recorded before Close: Close releases the permit the drain
		// waits for.
		in.record("body closing")
		in.closeErr = body.Close()
	}()
}

// effectArchive returns a small tar archive, the only artifact Files accepts.
func effectArchive(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	payload := []byte(strings.Repeat("artifact", 1000))
	if err := w.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// before fails unless each event occurs in events, in the order given.
func before(t *testing.T, events []string, order ...string) {
	t.Helper()
	at := -1
	for _, e := range order {
		i := slices.Index(events, e)
		if i <= at {
			t.Fatalf("events = %q, want %q in that order", events, order)
		}
		at = i
	}
}

func effectFixture(t *testing.T, in *effectIntegration) *lifetimeServerFixture {
	t.Helper()
	f := lifetimeFixture(t)
	root := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(f.data, root); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	cfg := lifetimeRemoteConfig(&lifetimeRemote{})
	cfg.ArtifactDir = root
	cfg.Install = in.install
	f.start(t, cfg)
	in.runDone = f.done
	return f
}

// TestServerStopsEffectsBeforeArtifactDrain checks the early stop: the
// effect's body is closed only after StopEffects, and before Control's Run
// is canceled, which follows the drain. Shutdown is ordinary, and Close then
// joins the effect and finishes its read while the Store is open.
func TestServerStopsEffectsBeforeArtifactDrain(t *testing.T) {
	in := newEffectIntegration()
	f := effectFixture(t, in)
	in.begin(t, effectArchive(t))
	f.stopped(t)
	events := in.recorded()
	before(t, events, "install", "targets", "bind", "run", "run canceled")
	before(t, events, "open", "stop effects", "body closing", "run canceled", "finished", "close")
	if in.closeErr != nil {
		t.Fatalf("body close = %v", in.closeErr)
	}
	if in.readable != nil {
		t.Fatalf("Store read at integration Close = %v, want the Store still open", in.readable)
	}
}

// TestServerHeldEffectHoldsDrain checks an effect that does not stop: the
// drain cannot complete, so the server returns a RunError at the drain and
// keeps the integration unclosed, its Store open and authentication serving.
func TestServerHeldEffectHoldsDrain(t *testing.T) {
	in := newEffectIntegration()
	in.ignoreStop = true
	f := effectFixture(t, in)
	in.begin(t, effectArchive(t))
	f.cancel()
	integrationReceive(t, f.done, "server Run with a held effect")
	var held *RunError
	if !errors.As(f.result, &held) || held.stage != "original artifact drain remains held" {
		t.Fatalf("Run = %v, want a RunError at the artifact drain", f.result)
	}
	frame := held.frame
	if frame.integration != Integration(in) {
		t.Fatal("held frame lost the original integration")
	}
	if _, _, err := frame.store.RetentionPolicy(); err != nil {
		t.Fatalf("Store read in the held frame = %v, want the Store still open", err)
	}
	if _, err := frame.engine.Check(in.actor(), auth.Read); err != nil {
		t.Fatalf("authentication in the held frame = %v, want it still serving", err)
	}
	events := in.recorded()
	before(t, events, "open", "stop effects")
	if slices.Contains(events, "run canceled") || slices.Contains(events, "close") {
		t.Fatalf("events = %q, want Run neither canceled nor the integration closed", events)
	}
	f.expectedHeld = context.DeadlineExceeded
	f.hold(t, f.result)
}
