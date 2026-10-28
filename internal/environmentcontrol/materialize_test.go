package environmentcontrol_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

// These tests drive real Store artifact reads of a real uploaded artifact,
// through real Files and a real Control upload, and real actors and
// delegations. The runtime's copies and the descriptor opener are models:
// the opener stands for Control's OpenEnvironmentArtifact, and each copy
// reads the whole body and settles when the test lets it. Lost results are
// injected around the actual Store calls.

// knownProducer is a server lifetime the Store can replay reads of.
var knownProducer = store.OriginalServerLifetime{
	Host:            strings.Repeat("1", 32),
	Boot:            strings.Repeat("2", 32),
	NamespaceDevice: 1,
	NamespaceInode:  1,
	PID:             1,
	StartTicks:      1,
}

type artifactFixture struct {
	*fixture
	control *control.Control
	files   *artifact.Files
	root    *os.Root
	data    []byte
	ref     artifact.Ref
	mat     *fakeMaterializer
	opener  *fakeOpener
	faults  *artifactFaults
	// held records that a test left a frame holding a descriptor, so
	// cleanup keeps the Store and Files open.
	held bool
	// stopRun cancels the control's one original Run. That Run sets runErr
	// and then closes runDone; runErr is read only after runDone is closed.
	stopRun context.CancelFunc
	runDone chan struct{}
	runErr  error
}

// newArtifactFixture builds a Store with installed artifacts, a Control that
// owns its Files, an artifact alice uploaded, and a Controller whose runtime
// is a Materializer, with producer.
func newArtifactFixture(t *testing.T, producer store.OriginalServerLifetime) *artifactFixture {
	t.Helper()
	// The credential parent must be private; TempDir follows the umask.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "artifacts")
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("artifact root identity unavailable")
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "store")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		t.Fatal(err)
	}
	retention, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	f := &artifactFixture{
		fixture: &fixture{dir: dir, store: s, domain: retention.Domain, tokens: map[string]string{}, actors: map[string]auth.Actor{}},
		files:   files,
		root:    root,
		opener:  &fakeOpener{producer: producer, data: map[string][]byte{}},
		faults:  &artifactFaults{s: s},
	}
	f.config = filepath.Join(dir, "auth.json")
	f.tokens["legacy"] = filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(f.config, f.tokens["legacy"], s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(f.config, "alice", auth.Operator); err != nil {
		t.Fatal(err)
	}
	f.tokens["alice"] = filepath.Join(dir, "alice.token")
	if _, err := auth.Issue(f.config, "alice", f.tokens["alice"], time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if f.engine, err = auth.NewFile(f.config, s.Owner()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	f.stop = func() { cancel(); <-done }
	t.Cleanup(func() { f.cleanup(t) })
	for id, path := range f.tokens {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if f.actors[id], err = f.engine.Authenticate("Bearer " + string(bytes.TrimSpace(raw))); err != nil {
			t.Fatal(err)
		}
	}
	target := backend.Target{Name: "test", Kind: "test", Binding: "test-binding", Backend: idleBackend{}}
	if f.control, err = control.NewWithConfig(s, []backend.Target{target}, f.engine, control.Config{Artifacts: files}); err != nil {
		t.Fatal(err)
	}
	// The Control's own Run samples the physical points every upload needs.
	// Cleanup, already registered, stops and joins this one original Run.
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	f.stopRun, f.runDone = stopRun, runDone
	go func() {
		defer close(runDone)
		f.runErr = f.control.Run(runCtx)
	}()
	ready, cancelReady := context.WithTimeout(context.Background(), 10*time.Second)
	err = f.storageReady(ready, false)
	cancelReady()
	if err != nil {
		t.Fatal(err)
	}
	f.data = artifactTar(t)
	f.ref = f.upload(t, "alice", f.data)
	f.opener.data[f.ref.ID] = f.data

	f.mat = &fakeMaterializer{fakeRuntime: newFakeRuntime()}
	f.runtime = f.mat.fakeRuntime
	f.clock = &clock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	f.c, err = environmentcontrol.New(environmentcontrol.Config{Store: s, Auth: f.engine, Runtime: f.mat, Now: f.clock.now})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.BindArtifacts(f.opener); err != nil {
		t.Fatal(err)
	}
	environmentcontrol.SetMaterializeRetry(f.c, time.Millisecond)
	environmentcontrol.SetMaterializeFaults(f.c, f.faults.begin, f.faults.mark, f.faults.finish)
	return f
}

// cleanup lets every held copy and Finish proceed and closes the Controller.
// It then retires the Control while its original Run and the auth Engine are
// alive, and only if the Controller reports nothing held and the Control
// retired closes Files, root and Store. The auth Engine stops last, and not at
// all while an environment Controller or original Control owner remains held.
func (f *artifactFixture) cleanup(t *testing.T) {
	f.faults.clear()
	if f.mat != nil {
		f.mat.releaseAll()
	}
	held := false
	if f.c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := f.c.Close(ctx)
		cancel()
		if err != nil {
			if !f.held {
				t.Errorf("close controller: %v", err)
			}
			held = true
		}
	}
	joined := true
	if f.control != nil {
		var err error
		if joined, err = f.retireControl(); err != nil {
			t.Errorf("retire control: %v", err)
			held = true
		}
	}
	if held {
		return
	}
	if joined && f.stop != nil {
		defer f.stop()
	}
	if err := f.files.Close(); err != nil {
		t.Errorf("close files: %v", err)
		return
	}
	if err := f.root.Close(); err != nil {
		t.Errorf("close root: %v", err)
	}
	if err := f.store.Close(); err != nil {
		t.Errorf("close store: %v", err)
	}
}

// retireControl drains the Control's artifacts while its original Run is
// alive, then cancels that Run, receives its result and joins it. It reports
// whether the original Run and its workers were joined. A timeout is an
// error, never a join; Context.Canceled is accepted only from the Run that
// cleanup canceled.
func (f *artifactFixture) retireControl() (joined bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	drainErr := f.control.DrainArtifacts(ctx)
	if drainErr != nil {
		drainErr = fmt.Errorf("drain artifacts: %w", drainErr)
	}
	if f.runDone == nil {
		return true, drainErr
	}
	var runErr error
	select {
	case <-f.runDone:
		runErr = fmt.Errorf("control run returned before cleanup: %v", f.runErr)
	default:
	}
	f.stopRun()
	select {
	case <-f.runDone:
	case <-ctx.Done():
		return false, errors.Join(drainErr, fmt.Errorf("original control run remains held: %w", ctx.Err()))
	}
	if runErr == nil && f.runErr != nil && !errors.Is(f.runErr, context.Canceled) {
		runErr = fmt.Errorf("control run: %w", f.runErr)
	}
	if err := f.control.Join(ctx); err != nil {
		return false, errors.Join(drainErr, runErr, fmt.Errorf("join control: %w", err))
	}
	return true, errors.Join(drainErr, runErr)
}

// storageReady waits until the Control reports its recent storage
// observations ready, checking every 10ms, and first waiting 10ms if pace is
// set. It fails if the original Run returned or ctx ends. Readiness is an
// observation, not a lease: the next upload may still be Busy.
func (f *artifactFixture) storageReady(ctx context.Context, pace bool) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if pace {
			select {
			case <-f.runDone:
				return fmt.Errorf("control run returned: %v", f.runErr)
			case <-ctx.Done():
				return fmt.Errorf("control storage not ready: %w", ctx.Err())
			case <-tick.C:
			}
		}
		pace = true
		select {
		case <-f.runDone:
			return fmt.Errorf("control run returned: %v", f.runErr)
		default:
		}
		if f.control.StorageReady() {
			return nil
		}
	}
}

// uploadBusy reports whether err is only a documented upload Busy refusal:
// ErrArtifactBusy, ErrFilesBusy, the growth guard's ErrFilesBusy with its
// reason, or a Files join of exactly one of those. An error that wraps Busy
// alongside anything else, such as an uncertain close, is not Busy.
func uploadBusy(err error) bool {
	switch err {
	case nil:
		return false
	case control.ErrArtifactBusy, artifact.ErrFilesBusy:
		return true
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		errs := j.Unwrap()
		return len(errs) == 1 && uploadBusy(errs[0])
	}
	return errors.Unwrap(err) == artifact.ErrFilesBusy && strings.HasPrefix(err.Error(), artifact.ErrFilesBusy.Error()+": ")
}

// call runs op, one actual operation, on a new ArtifactCall, which it
// releases as soon as op returns. While op fails with an error retry allows,
// or no call is free, it waits for storage to be ready again and calls op on
// another new call, up to 50 times. It fails if the original Run returns or
// ctx ends.
func (f *artifactFixture) call(ctx context.Context, retry func(error) bool, op func(*control.ArtifactCall) error) error {
	var last error
	for range 50 {
		a, err := f.control.TryArtifactCall()
		if err == nil {
			err = op(a)
			a.Done()
			if err == nil || !retry(err) {
				return err
			}
		} else if err != control.ErrArtifactBusy {
			return err
		}
		last = err
		if err := f.storageReady(ctx, true); err != nil {
			return fmt.Errorf("%w; last attempt: %v", err, last)
		}
	}
	return fmt.Errorf("retry bound reached: %w", last)
}

// upload uploads data as who through Control and returns its reference.
// Begin is repeated only when its guard refused it before the Store, so no
// admitted upload is begun again. Each chunk is written again only after a
// Busy refusal, with the same upload, offset and bytes, and the offset
// advances only on success. The first Finalize of a writable upload begins
// finalizing and reports Busy; the same upload is finalized again once the
// Control has observed storage afresh.
func (f *artifactFixture) upload(t *testing.T, who string, data []byte) artifact.Ref {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	digest := sha256.Sum256(data)
	var id string
	err := f.call(ctx, control.IsArtifactBeginNotStarted, func(a *control.ArtifactCall) error {
		u, err := f.control.BeginArtifactUpload(ctx, f.actors[who], a, hex.EncodeToString(digest[:]), int64(len(data)))
		id = u.Reference.ID
		return err
	})
	if err != nil {
		t.Fatalf("begin upload: %v", err)
	}
	for off := 0; off < len(data); {
		end := min(off+64<<10, len(data))
		err := f.call(ctx, uploadBusy, func(a *control.ArtifactCall) error {
			_, err := f.control.WriteArtifactChunk(ctx, f.actors[who], a, id, int64(off), data[off:end])
			return err
		})
		if err != nil {
			t.Fatalf("write upload %s at %d: %v", id, off, err)
		}
		off = end
	}
	var ref artifact.Ref
	err = f.call(ctx, uploadBusy, func(a *control.ArtifactCall) error {
		var err error
		ref, err = f.control.FinalizeArtifactUpload(ctx, f.actors[who], a, id)
		return err
	})
	if err != nil {
		t.Fatalf("finalize upload %s: %v", id, err)
	}
	return ref
}

// uploadOther uploads a second artifact as alice, which the opener serves,
// and returns its reference.
func (f *artifactFixture) uploadOther(t *testing.T) artifact.Ref {
	t.Helper()
	data := artifactTar(t, "other")
	ref := f.upload(t, "alice", data)
	f.opener.mu.Lock()
	f.opener.data[ref.ID] = data
	f.opener.mu.Unlock()
	return ref
}

// artifactTar returns an archive of one file of "artifact", or of content[0],
// repeated.
func artifactTar(t *testing.T, content ...string) []byte {
	t.Helper()
	var archive bytes.Buffer
	w := tar.NewWriter(&archive)
	payload := bytes.Repeat([]byte("artifact"), 10000)
	if len(content) > 0 {
		payload = bytes.Repeat([]byte(content[0]), 10000)
	}
	if err := w.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

// spec returns a spec named name that mounts the fixture's artifact at each
// of paths.
func (f *artifactFixture) spec(name string, paths ...string) environment.Spec {
	s := testSpec(name)
	for _, p := range paths {
		s.Artifacts = append(s.Artifacts, mount(f.ref, p))
	}
	return s
}

func mount(ref artifact.Ref, path string) environment.ArtifactMount {
	return environment.ArtifactMount{ID: ref.ID, SHA256: ref.SHA256, Size: uint64(ref.Size), MountPath: path}
}

// instance makes Ensure observe incarnation inc, Ready if ready is set and
// Creating otherwise.
func (f *artifactFixture) instance(inc string, ready bool) {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		phase := environment.Creating
		if ready {
			phase = environment.Ready
		}
		return environmentcontrol.Observation{Phase: phase, Native: []byte("uid-" + inc), Incarnation: inc}, nil
	}
}

// row reads environment id from the Store.
func (f *artifactFixture) row(t *testing.T, id string) store.Environment {
	t.Helper()
	e, err := f.store.Environment(id)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// wantIncarnation checks environment id's committed incarnation.
func (f *artifactFixture) wantIncarnation(t *testing.T, id, inc string, seq uint64) {
	t.Helper()
	if e := f.row(t, id); e.Incarnation != inc || e.IncarnationSeq != seq {
		t.Fatalf("incarnation = %q seq %d, want %q seq %d", e.Incarnation, e.IncarnationSeq, inc, seq)
	}
}

// probe calls the Store's Begin for the committed tuple seq, inc of id with
// producer, in a state where it writes nothing: a replay, a held read, a
// closed read or a fence refusal.
func (f *artifactFixture) probe(id string, seq uint64, inc string, producer store.OriginalServerLifetime) (store.ArtifactLease, error) {
	ref := store.ArtifactRef{ID: f.ref.ID, SHA256: f.ref.SHA256, Size: uint64(f.ref.Size)}
	return f.store.BeginEnvironmentMaterialization(id, 0, seq, inc, ref, producer, f.clock.now())
}

func (f *artifactFixture) closeController(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.c.Close(ctx)
}

// materializeEventually waits up to ten seconds for cond. A timeout is a
// failure, never a completion.
func materializeEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// settle waits until the Controller holds nothing of environment id's
// materialization: its frame has returned, after any Bootstrap, and released
// every read.
func (f *artifactFixture) settle(t *testing.T, id string) {
	t.Helper()
	materializeEventually(t, "the frame to settle", func() bool { return !environmentcontrol.HoldsArtifacts(f.c, id) })
}

// artifactFaults wraps the Store's materialization calls of one Controller.
// The next Begin commits, calls afterLose if it is set, and reports errFault
// while loseBegin is set, or calls dropBegin instead of the Store and reports
// errFault while it is set;
// every Begin reports errFault without calling the Store while dropAll is
// set. The next Finish commits, then waits for loseFinish to close and
// reports errFault, while loseFinish is set. Every Finish reports errFault
// without calling the Store while failFinish is set.
type artifactFaults struct {
	s *store.Store

	mu         sync.Mutex
	loseBegin  bool
	afterLose  func()
	dropBegin  func()
	dropAll    bool
	loseFinish chan struct{}
	failFinish bool
	begins     []error // each Begin's actual Store result
	leases     []store.ArtifactLease
	dropped    int
	marks      int // marks the Store applied
	finishes   int // finishes the Store applied
}

func (a *artifactFaults) begin(id string, revision, seq uint64, inc string, ref store.ArtifactRef, producer store.OriginalServerLifetime, now time.Time) (store.ArtifactLease, error) {
	a.mu.Lock()
	lose, after, drop, all := a.loseBegin, a.afterLose, a.dropBegin, a.dropAll
	a.loseBegin, a.dropBegin = false, nil
	if lose {
		a.afterLose = nil
	}
	if drop != nil || all {
		a.dropped++
	}
	a.mu.Unlock()
	if all {
		return store.ArtifactLease{}, errFault
	}
	if drop != nil {
		drop()
		return store.ArtifactLease{}, errFault
	}
	lease, err := a.s.BeginEnvironmentMaterialization(id, revision, seq, inc, ref, producer, now)
	a.mu.Lock()
	a.begins = append(a.begins, err)
	if err == nil {
		a.leases = append(a.leases, lease)
	}
	a.mu.Unlock()
	if lose && err == nil {
		if after != nil {
			after()
		}
		return store.ArtifactLease{}, errFault
	}
	return lease, err
}

func (a *artifactFaults) mark(c store.ArtifactDownloadClosure) (store.ArtifactDownloadClosure, error) {
	out, err := a.s.MarkArtifactDownloadClosing(c)
	if err == nil {
		a.mu.Lock()
		a.marks++
		a.mu.Unlock()
	}
	return out, err
}

func (a *artifactFaults) finish(c store.ArtifactDownloadClosure, now time.Time) error {
	a.mu.Lock()
	fail, lose := a.failFinish, a.loseFinish
	a.loseFinish = nil
	a.mu.Unlock()
	if fail {
		return errFault
	}
	err := a.s.FinishArtifactDownload(c, now)
	if err == nil {
		a.mu.Lock()
		a.finishes++
		a.mu.Unlock()
	}
	if lose != nil && err == nil {
		<-lose
		return errFault
	}
	return err
}

func (a *artifactFaults) arm(f func(*artifactFaults)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f(a)
}

func (a *artifactFaults) clear() {
	a.arm(func(a *artifactFaults) { a.failFinish, a.loseBegin, a.dropBegin, a.dropAll = false, false, nil, false })
}

type faultCounts struct {
	begins          []error
	leases          []store.ArtifactLease
	dropped         int
	marks, finishes int
}

func (a *artifactFaults) counts() faultCounts {
	a.mu.Lock()
	defer a.mu.Unlock()
	return faultCounts{slices.Clone(a.begins), slices.Clone(a.leases), a.dropped, a.marks, a.finishes}
}

// fakeOpener models Control's ArtifactOpener for producer: each Open first
// takes the next step of script, and otherwise returns a body that yields the
// lease's artifact; each body's Close reports closeErr. With through set, each
// Open opens the lease with through instead, and its body's Close is
// through's. Each Open calls before, if set, once it was called and before it
// opens anything, and after, if set, once it opened and before it returns.
type fakeOpener struct {
	producer store.OriginalServerLifetime

	mu       sync.Mutex
	data     map[string][]byte // artifact ID: bytes
	script   []openStep
	closeErr error
	through  environmentcontrol.ArtifactOpener
	before   func()
	after    func()
	opened   []store.ArtifactLease
	returned []io.ReadCloser // bodies of through, as returned
	closed   []io.ReadCloser // bodies of through, as closed
	closes   int
}

// An openStep is one scripted Open result: err, with a body if body is set.
type openStep struct {
	err  error
	body bool
}

func (o *fakeOpener) ArtifactProducer() store.OriginalServerLifetime { return o.producer }

func (o *fakeOpener) OpenEnvironmentArtifact(ctx context.Context, lease store.ArtifactLease) (io.ReadCloser, error) {
	o.mu.Lock()
	before, after, through := o.before, o.after, o.through
	o.mu.Unlock()
	if before != nil {
		before()
	}
	if after != nil {
		defer after()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opened = append(o.opened, lease)
	if through != nil {
		body, err := through.OpenEnvironmentArtifact(ctx, lease)
		if body == nil {
			return nil, err
		}
		b := &throughBody{ReadCloser: body, o: o}
		o.returned = append(o.returned, b)
		return b, err
	}
	body := &fakeBody{Reader: bytes.NewReader(o.data[lease.Reference.ID]), o: o}
	if len(o.script) > 0 {
		step := o.script[0]
		o.script = o.script[1:]
		if !step.body {
			return nil, step.err
		}
		return body, step.err
	}
	return body, nil
}

func (o *fakeOpener) setScript(steps ...openStep) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.script = steps
}

func (o *fakeOpener) counts() (opened, closes int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.opened), o.closes
}

type fakeBody struct {
	*bytes.Reader
	o *fakeOpener
}

func (b *fakeBody) Close() error {
	b.o.mu.Lock()
	defer b.o.mu.Unlock()
	b.o.closes++
	return b.o.closeErr
}

// throughBody is a body of a fakeOpener's through.
type throughBody struct {
	io.ReadCloser
	o *fakeOpener
}

func (b *throughBody) Close() error {
	err := b.ReadCloser.Close()
	b.o.mu.Lock()
	defer b.o.mu.Unlock()
	b.o.closes++
	b.o.closed = append(b.o.closed, b)
	return err
}

func (o *fakeOpener) set(f func(*fakeOpener)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f(o)
}

// sameLease reports whether a and b are the same lease, field by field.
func sameLease(a, b store.ArtifactLease) bool {
	return a.ID == b.ID && a.Reference == b.Reference && a.Revision == b.Revision && a.ExpiresAt.Equal(b.ExpiresAt) &&
		a.Closing == b.Closing && a.File == b.File && a.RootID == b.RootID && a.Producer == b.Producer
}

// fakeMaterializer is fakeRuntime with Materialize. Each copy reads its whole
// body, or with perMount reads it once for each mount, as a runtime that
// expects to read the bytes again. A copy fails with fail, or if a read did
// not get the artifact's Size bytes; it is done once the test releases it, at
// once unless hold is set. Cancel does not release a copy, as a runtime that
// ignores cancellation. While refuse is set, Materialize starts nothing and
// returns it.
//
// Each Bootstrap first takes the next step of bootScript. By default it
// starts a frame that reads nothing and is done at once with a nil Err: the
// instance's acknowledgement. A step may refuse, starting nothing, may
// settle with an error, an unknown result, or may hold the frame until the
// test releases it; as for a copy, Cancel does not release it.
type fakeMaterializer struct {
	*fakeRuntime

	mu         sync.Mutex
	hold       bool
	fail       error
	refuse     error
	perMount   bool
	refused    int
	copies     []*fakeCopy
	bootScript []bootStep
	bootCalls  int
	boots      []*fakeCopy
}

// A bootStep is one scripted Bootstrap: refused with refuse, or started and
// settled with err, once released if hold is set.
type bootStep struct {
	refuse error
	err    error
	hold   bool
}

func (m *fakeMaterializer) Bootstrap(r environmentcontrol.Ref, inc string) (environmentcontrol.Materialization, error) {
	m.mu.Lock()
	m.bootCalls++
	var step bootStep
	if len(m.bootScript) > 0 {
		step = m.bootScript[0]
		m.bootScript = m.bootScript[1:]
	}
	if step.refuse != nil {
		m.mu.Unlock()
		return nil, step.refuse
	}
	x := &fakeCopy{incarnation: inc, release: make(chan struct{}), done: make(chan struct{}), canceled: make(chan struct{})}
	m.boots = append(m.boots, x)
	m.mu.Unlock()
	if !step.hold {
		x.free()
	}
	go func() {
		defer close(x.done)
		<-x.release
		x.err = step.err
	}()
	return x, nil
}

func (m *fakeMaterializer) setBoot(steps ...bootStep) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bootScript = steps
}

// bootstraps returns the started Bootstrap frames and the number of calls,
// refused ones included.
func (m *fakeMaterializer) bootstraps() ([]*fakeCopy, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.boots), m.bootCalls
}

func (m *fakeMaterializer) Materialize(r environmentcontrol.Ref, inc string, mounts []environment.ArtifactMount, body io.Reader) (environmentcontrol.Materialization, error) {
	x := &fakeCopy{incarnation: inc, mounts: mounts, release: make(chan struct{}), done: make(chan struct{}), canceled: make(chan struct{})}
	m.mu.Lock()
	hold, fail, refuse, perMount := m.hold, m.fail, m.refuse, m.perMount
	if refuse != nil {
		m.refused++
		m.mu.Unlock()
		return nil, refuse
	}
	m.copies = append(m.copies, x)
	m.mu.Unlock()
	if !hold {
		x.free()
	}
	go func() {
		defer close(x.done)
		reads := 1
		if perMount {
			reads = len(mounts)
		}
		errs := []error{fail}
		for range reads {
			data, err := io.ReadAll(body)
			x.reads = append(x.reads, data)
			if uint64(len(data)) != mounts[0].Size {
				err = errors.Join(err, errShort)
			}
			errs = append(errs, err)
		}
		<-x.release
		x.data, x.err = x.reads[0], errors.Join(errs...)
	}()
	return x, nil
}

var errShort = errors.New("short artifact read")

func (m *fakeMaterializer) set(f func(*fakeMaterializer)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f(m)
}

func (m *fakeMaterializer) setHold(hold bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hold = hold
}

func (m *fakeMaterializer) started() []*fakeCopy {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.copies)
}

func (m *fakeMaterializer) releaseAll() {
	boots, _ := m.bootstraps()
	for _, x := range append(m.started(), boots...) {
		x.free()
	}
}

type fakeCopy struct {
	incarnation string
	mounts      []environment.ArtifactMount
	release     chan struct{}
	freed       sync.Once
	done        chan struct{}
	canceled    chan struct{}
	cancel      sync.Once
	data        []byte   // valid once done is closed
	reads       [][]byte // each read of the body; valid once done is closed
	err         error
}

func (x *fakeCopy) free()                 { x.freed.Do(func() { close(x.release) }) }
func (x *fakeCopy) Done() <-chan struct{} { return x.done }
func (x *fakeCopy) Cancel()               { x.cancel.Do(func() { close(x.canceled) }) }

func (x *fakeCopy) Result() environmentcontrol.MaterializationResult {
	select {
	case <-x.done:
		return environmentcontrol.MaterializationResult{Settled: true, Err: x.err}
	default:
		return environmentcontrol.MaterializationResult{}
	}
}

// idleBackend is the backend of the artifact Control's one target, which
// runs no job in these tests.
type idleBackend struct{}

var errNoJobs = errors.New("no jobs in this test")

func (idleBackend) Check(backend.Task) error                          { return errNoJobs }
func (idleBackend) Start(context.Context, string, backend.Task) error { return errNoJobs }
func (idleBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errNoJobs
}
func (idleBackend) Cancel(context.Context, backend.Reference) error { return errNoJobs }
func (idleBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errNoJobs
}

// TestMaterializeUnsupported checks that a spec with artifacts is refused
// before any runtime call or Store write, by a runtime that is not a
// Materializer and by a Materializer without an opener.
func TestMaterializeUnsupported(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("art")
	spec.Artifacts = []environment.ArtifactMount{{ID: strings.Repeat("c", 32), SHA256: strings.Repeat("d", 64), Size: 1, MountPath: "/skygo/artifacts/in"}}
	if _, err := f.c.Create(f.actors["alice"], f.domain, "art", spec); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("create with a plain runtime = %v, want ErrUnsupported", err)
	}
	m := &fakeMaterializer{fakeRuntime: newFakeRuntime()}
	c, err := environmentcontrol.New(environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: m, Now: f.clock.now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(f.actors["alice"], f.domain, "art", spec); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("create without an opener = %v, want ErrUnsupported", err)
	}
	if plans, _, _, _ := f.runtime.counts(); plans != 0 {
		t.Fatalf("plans = %d, want 0", plans)
	}
	if plans, _, _, _ := m.counts(); plans != 0 {
		t.Fatalf("materializer plans = %d, want 0", plans)
	}
	p, err := f.c.List(f.actors["alice"], 0, 0)
	if err != nil || len(p.Environments) != 0 {
		t.Fatalf("environments = %v, %v; want none", p.Environments, err)
	}
}

// TestMaterializeReady checks the ordinary path: the committed incarnation's
// one read serves both mounts of the artifact, Ready waits for that read to
// close even when the runtime reports Ready, and the closed read is the
// incarnation's only one.
func TestMaterializeReady(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.setHold(true)
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a", "/skygo/artifacts/b"))
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)
	materializeEventually(t, "the copy to start", func() bool { return len(f.mat.started()) == 1 })

	// The runtime reports Ready before the read closes.
	f.instance("pod-1", true)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Creating)

	f.mat.releaseAll()
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	f.settle(t, v.ID)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Ready)
	if boots, calls := f.mat.bootstraps(); calls != 1 || len(boots) != 1 || boots[0].incarnation != "pod-1" {
		t.Fatalf("bootstraps = %d, want one of pod-1", calls)
	}

	copies := f.mat.started()
	if len(copies) != 1 {
		t.Fatalf("copies = %d, want 1", len(copies))
	}
	x := copies[0]
	if x.incarnation != "pod-1" || len(x.mounts) != 2 || x.mounts[0].MountPath != "/skygo/artifacts/a" || x.mounts[1].MountPath != "/skygo/artifacts/b" {
		t.Fatalf("copy = %q %v, want pod-1 at both mounts", x.incarnation, x.mounts)
	}
	if !bytes.Equal(x.data, f.data) {
		t.Fatal("copy read other bytes")
	}
	if opened, closes := f.opener.counts(); opened != 1 || closes != 1 {
		t.Fatalf("opened %d, closed %d; want 1 and 1", opened, closes)
	}
	if _, err := f.probe(v.ID, 1, "pod-1", knownProducer); !errors.Is(err, store.ErrEnvironmentMaterialized) {
		t.Fatalf("probe = %v, want ErrEnvironmentMaterialized", err)
	}
	if err := f.closeController(time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestMaterializeLostBegin (C-LB, and C-UNK-K) loses the result of an
// admitted Begin under a known producer. The retry replays the same lease;
// a new incarnation waits until that one read is released; no other reader
// is opened, and nothing is held.
func TestMaterializeLostBegin(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.setHold(true)
	f.faults.arm(func(a *artifactFaults) { a.loseBegin = true })
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the copy to start", func() bool { return len(f.mat.started()) == 1 })
	n := f.faults.counts()
	if len(n.leases) != 2 || n.leases[0].ID != n.leases[1].ID {
		t.Fatalf("leases = %v, want the lost one replayed", n.leases)
	}

	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("a replayed read is held")
	}

	f.mat.releaseAll()
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	f.settle(t, v.ID)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-2", 2)
	materializeEventually(t, "the new incarnation's copy", func() bool { return len(f.mat.started()) == 2 })
	if opened, _ := f.opener.counts(); opened != 2 {
		t.Fatalf("opened = %d, want one per incarnation", opened)
	}
	if x := f.mat.started()[1]; x.incarnation != "pod-2" {
		t.Fatalf("second copy into %q, want pod-2", x.incarnation)
	}
}

// TestMaterializeDroppedBegin (C-LB-N) reports a Begin that never reached the
// Store as failed, and commits the next incarnation before the retry. The
// retry is refused by the sequence fence, and nothing of the old incarnation
// is read or held.
func TestMaterializeDroppedBegin(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	var commitErr error
	f.faults.arm(func(a *artifactFaults) {
		a.dropBegin = func() {
			e, err := f.store.Environment(v.ID)
			if err == nil {
				_, err = f.store.CommitEnvironment(v.ID, e.Revision, store.EnvironmentCommit{State: e.State, Checkpoint: e.Checkpoint, Incarnation: "pod-2"})
			}
			commitErr = err
		}
	})
	f.reconcile(t)
	materializeEventually(t, "the retry", func() bool { return len(f.faults.counts().begins) >= 1 })
	if commitErr != nil {
		t.Fatalf("commit the next incarnation: %v", commitErr)
	}
	n := f.faults.counts()
	if n.dropped != 1 || !errors.Is(n.begins[0], store.ErrEnvironmentIncarnation) {
		t.Fatalf("dropped %d, retry %v; want 1 and ErrEnvironmentIncarnation", n.dropped, n.begins[0])
	}
	if opened, _ := f.opener.counts(); opened != 0 {
		t.Fatalf("opened = %d, want 0", opened)
	}
	if _, err := f.probe(v.ID, 1, "pod-1", knownProducer); !errors.Is(err, store.ErrEnvironmentIncarnation) {
		t.Fatalf("probe of the old tuple = %v, want ErrEnvironmentIncarnation", err)
	}
	if err := f.closeController(time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestMaterializeLostFinish (C-LF-alive) loses the result of an applied
// Finish. Until the retried Finish succeeds the frame holds the read's
// closure, so a new incarnation is not committed, although the Store would
// allow it; then it is, and the next incarnation is read once.
func TestMaterializeLostFinish(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	lost := make(chan struct{})
	f.faults.arm(func(a *artifactFaults) { a.loseFinish = lost })
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the lost Finish", func() bool { return f.faults.counts().finishes == 1 })

	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)

	close(lost)
	materializeEventually(t, "the retried Finish", func() bool { return f.faults.counts().finishes == 2 })
	f.settle(t, v.ID)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-2", 2)
	materializeEventually(t, "the new incarnation's read", func() bool { return f.faults.counts().finishes == 3 })
	if opened, _ := f.opener.counts(); opened != 2 {
		t.Fatalf("opened = %d, want one per incarnation", opened)
	}
}

// TestMaterializeUnknownProducer (C-UNK) loses an admitted Begin's result
// under the unknown producer. The retry is Held, which settles the frame as
// held (S4): no new incarnation and no Deleted is committed, Close reports
// nothing held, and the read stays open in the Store.
func TestMaterializeUnknownProducer(t *testing.T) {
	f := newArtifactFixture(t, store.OriginalServerLifetime{})
	f.faults.arm(func(a *artifactFaults) { a.loseBegin = true })
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the held retry", func() bool {
		n := f.faults.counts()
		return len(n.begins) == 2 && errors.Is(n.begins[1], store.ErrEnvironmentMaterializationHeld)
	})
	materializeEventually(t, "the hold", func() bool {
		v, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && v.Held
	})
	if opened, _ := f.opener.counts(); opened != 0 {
		t.Fatalf("opened = %d, want 0", opened)
	}

	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)

	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleting)

	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close with a held read = %v, want nil", err)
	}
	if _, err := f.probe(v.ID, 1, "pod-1", store.OriginalServerLifetime{}); !errors.Is(err, store.ErrEnvironmentMaterializationHeld) {
		t.Fatalf("probe = %v, want ErrEnvironmentMaterializationHeld", err)
	}
}

// TestMaterializeUnknownProducerLease (C-UNK-L) returns the lease under the
// unknown producer: the read is released normally and the next incarnation
// commits.
func TestMaterializeUnknownProducerLease(t *testing.T) {
	f := newArtifactFixture(t, store.OriginalServerLifetime{})
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("a released read is held")
	}
	f.settle(t, v.ID)
	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-2", 2)
}

// TestMaterializeHeldLease (NEG-LEASE) keeps a returned lease under the
// unknown producer while the copy never finishes, even after Cancel. That is
// never the S4 hold: the frame holds the lease, the environment is not
// reported held, and Close reports it.
func TestMaterializeHeldLease(t *testing.T) {
	f := newArtifactFixture(t, store.OriginalServerLifetime{})
	f.mat.setHold(true)
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the copy to start", func() bool { return len(f.mat.started()) == 1 })
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("a held lease is reported as the S4 hold")
	}
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with a running copy = nil, want an error")
	}
	canceled := f.mat.started()[0].canceled
	materializeEventually(t, "the copy to be canceled", func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
	if n := f.faults.counts(); n.marks != 0 || n.finishes != 0 {
		t.Fatalf("marks %d, finishes %d; want none while the copy runs", n.marks, n.finishes)
	}
}

// TestMaterializeCloseError (NEG-CLOSE) fails the body's Close after the copy
// is done. The descriptor may be open, so the lease stays held: it is never
// marked or finished, the environment is not reported held, and Close
// reports it.
func TestMaterializeCloseError(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.held = true
	f.opener.closeErr = errors.New("injected close failure")
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the failed close", func() bool { _, closes := f.opener.counts(); return closes == 1 })
	if err := f.closeController(time.Second); err == nil {
		t.Fatal("close with an unclosed descriptor = nil, want an error")
	}
	if n := f.faults.counts(); n.marks != 0 || n.finishes != 0 {
		t.Fatalf("marks %d, finishes %d; want none", n.marks, n.finishes)
	}
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("an unclosed descriptor is reported as the S4 hold")
	}
	if _, err := f.probe(v.ID, 1, "pod-1", knownProducer); err != nil {
		t.Fatalf("probe = %v, want the open read replayed", err)
	}
}

// TestMaterializeFinishFails (NEG-FINISH) refuses every Finish. The frame
// keeps retrying and holds the closure: no new incarnation commits and Close
// reports it.
func TestMaterializeFinishFails(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.faults.arm(func(a *artifactFaults) { a.failFinish = true })
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the mark", func() bool { return f.faults.counts().marks >= 1 })
	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with an unfinished read = nil, want an error")
	}
	if n := f.faults.counts(); n.finishes != 0 {
		t.Fatalf("finishes = %d, want 0", n.finishes)
	}
}

// TestMaterializeUnknownBegin (NEG-UNKNOWN) never learns a Begin's result
// under the unknown producer: no Begin reaches the Store, and Close stops the
// retries before any is Held. An unknown result settles as the S4 hold only
// after a Held, so the frame keeps it, the environment is not reported held,
// and Close reports it.
func TestMaterializeUnknownBegin(t *testing.T) {
	f := newArtifactFixture(t, store.OriginalServerLifetime{})
	f.held = true
	f.faults.arm(func(a *artifactFaults) { a.dropAll = true })
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "a retried Begin", func() bool { return f.faults.counts().dropped >= 2 })
	if err := f.closeController(time.Second); err == nil {
		t.Fatal("close with an unknown Begin result = nil, want an error")
	}
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("an unknown Begin result without Held is reported as the S4 hold")
	}
	if n := f.faults.counts(); len(n.begins) != 0 {
		t.Fatalf("Store Begins = %d, want 0", len(n.begins))
	}
}

// TestBindArtifacts checks that an opener is bound once, only before Run.
func TestBindArtifacts(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	if err := f.c.BindArtifacts(&fakeOpener{}); err == nil {
		t.Fatal("second bind = nil, want an error")
	}
	if err := (environmentcontrol.Integration{Controller: f.c}).BindControl(nil); err == nil {
		t.Fatal("bind a nil control = nil, want an error")
	}
	c, err := environmentcontrol.New(environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: &fakeMaterializer{fakeRuntime: newFakeRuntime()}, Now: f.clock.now})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Run(ctx)
	if err := c.BindArtifacts(f.opener); err == nil {
		t.Fatal("bind after Run = nil, want an error")
	}
	if _, err := c.Create(f.actors["alice"], f.domain, "art", f.spec("art", "/skygo/artifacts/a")); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("create after a refused bind = %v, want ErrUnsupported", err)
	}
}

// TestMaterializeGroups mounts one artifact at two paths around another
// artifact. Each distinct artifact is opened and staged once, with all of its
// mounts in spec order, and Ready waits for both.
func TestMaterializeGroups(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	other := f.uploadOther(t)
	spec := testSpec("env")
	spec.Artifacts = []environment.ArtifactMount{mount(f.ref, "/skygo/artifacts/a"), mount(other, "/skygo/artifacts/b"), mount(f.ref, "/skygo/artifacts/c")}
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", spec)
	f.reconcile(t)
	materializeEventually(t, "both reads to finish", func() bool { return f.faults.counts().finishes == 2 })
	f.settle(t, v.ID)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Ready)

	copies := f.mat.started()
	if len(copies) != 2 {
		t.Fatalf("copies = %d, want one per artifact", len(copies))
	}
	paths := func(x *fakeCopy) (out []string) {
		for _, m := range x.mounts {
			out = append(out, m.ID+":"+m.MountPath)
		}
		return out
	}
	if got, want := paths(copies[0]), []string{f.ref.ID + ":/skygo/artifacts/a", f.ref.ID + ":/skygo/artifacts/c"}; !slices.Equal(got, want) {
		t.Fatalf("first copy mounts = %v, want %v", got, want)
	}
	if got, want := paths(copies[1]), []string{other.ID + ":/skygo/artifacts/b"}; !slices.Equal(got, want) {
		t.Fatalf("second copy mounts = %v, want %v", got, want)
	}
	if !bytes.Equal(copies[0].data, f.data) || !bytes.Equal(copies[1].data, f.opener.data[other.ID]) {
		t.Fatal("a copy read another artifact's bytes")
	}
	if opened, closes := f.opener.counts(); opened != 2 || closes != 2 {
		t.Fatalf("opened %d, closed %d; want 2 and 2", opened, closes)
	}
}

// TestMaterializeReadOnce has a runtime read the body once per mount. The body
// is not read again: the second read is empty, the copy fails, and the
// Controller neither reopens the artifact nor commits Ready.
func TestMaterializeReadOnce(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.set(func(m *fakeMaterializer) { m.perMount = true })
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a", "/skygo/artifacts/b"))
	f.reconcile(t)
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
		t.Fatal("an incarnation with an unstaged artifact is not held")
	}
	x := f.mat.started()[0]
	if len(x.reads) != 2 || !bytes.Equal(x.reads[0], f.data) || len(x.reads[1]) != 0 {
		t.Fatalf("reads = %d, want the bytes once and then none", len(x.reads))
	}
	if opened, _ := f.opener.counts(); opened != 1 {
		t.Fatalf("opened = %d, want 1", opened)
	}
}

// TestMaterializeStageFails (N6) fails a copy of an artifact mounted at two
// paths. Its read is released but the artifact is not staged, so the
// incarnation is held and never Ready, and the Store keeps it closed. A
// replacement incarnation is staged afresh and becomes Ready.
func TestMaterializeStageFails(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.set(func(m *fakeMaterializer) { m.fail = errors.New("injected second mount failure") })
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a", "/skygo/artifacts/b"))
	f.reconcile(t)
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
		t.Fatal("an incarnation with a failed copy is not held")
	}
	if _, err := f.probe(v.ID, 1, "pod-1", knownProducer); !errors.Is(err, store.ErrEnvironmentMaterialized) {
		t.Fatalf("probe = %v, want ErrEnvironmentMaterialized", err)
	}
	// N-B10: a copy settled with an error stages nothing, so no Bootstrap
	// of pod-1 is ever called.
	f.settle(t, v.ID)
	if _, calls := f.mat.bootstraps(); calls != 0 {
		t.Fatalf("bootstraps after a failed copy = %d, want 0", calls)
	}

	f.mat.set(func(m *fakeMaterializer) { m.fail = nil })
	f.instance("pod-2", true)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-2", 2)
	materializeEventually(t, "the replacement's read", func() bool { return f.faults.counts().finishes == 2 })
	f.settle(t, v.ID)
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Ready); v.Held {
		t.Fatal("a staged replacement is held")
	}
	if x := f.mat.started()[1]; x.incarnation != "pod-2" {
		t.Fatalf("second copy into %q, want pod-2", x.incarnation)
	}
	if boots, calls := f.mat.bootstraps(); calls != 1 || boots[0].incarnation != "pod-2" {
		t.Fatalf("bootstraps = %d, want one of pod-2", calls)
	}
}

// TestMaterializeRefused (N5) has Materialize refuse the incarnation before
// any effect. The body is closed and the read released, not staged.
func TestMaterializeRefused(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.set(func(m *fakeMaterializer) {
		m.refuse = fmt.Errorf("pod-1 bootstrapped: %w", environmentcontrol.ErrIncarnation)
	})
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
		t.Fatal("a refused incarnation is not held")
	}
	if opened, closes := f.opener.counts(); opened != 1 || closes != 1 {
		t.Fatalf("opened %d, closed %d; want 1 and 1", opened, closes)
	}
	f.settle(t, v.ID)
	if _, calls := f.mat.bootstraps(); calls != 0 {
		t.Fatalf("bootstraps after a refused copy = %d, want 0", calls)
	}
}

// TestMaterializeOpen checks each Open result that leaves nothing open: busy
// (N2) is opened again, and a failure without (N3) or with (N4) a body is
// released, not staged.
func TestMaterializeOpen(t *testing.T) {
	failure := errors.New("injected open failure")
	tests := []struct {
		name   string
		script []openStep
		opened int
		closes int
		ready  bool
	}{
		{"busy", []openStep{{err: artifact.ErrFilesBusy}, {err: artifact.ErrFilesBusy}}, 3, 1, true},
		{"failure", []openStep{{err: failure}}, 1, 0, false},
		{"failure with body", []openStep{{err: failure, body: true}}, 1, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newArtifactFixture(t, knownProducer)
			f.opener.setScript(tt.script...)
			f.instance("pod-1", true)
			v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
			f.reconcile(t)
			materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
			f.settle(t, v.ID)
			f.reconcile(t)
			want := environment.Creating
			if tt.ready {
				want = environment.Ready
			}
			if v := f.get(t, "alice", v.ID, want); v.Held == tt.ready {
				t.Fatalf("held = %v, want %v", v.Held, !tt.ready)
			}
			if opened, closes := f.opener.counts(); opened != tt.opened || closes != tt.closes {
				t.Fatalf("opened %d, closed %d; want %d and %d", opened, closes, tt.opened, tt.closes)
			}
			if copies := len(f.mat.started()); tt.ready != (copies == 1) {
				t.Fatalf("copies = %d", copies)
			}
		})
	}
}

// TestMaterializeUncertainOpen (N1) has Open report that a file may still be
// open, without a body. The lease is kept: it is never marked or finished,
// the environment is not reported held, no new incarnation commits, and Close
// reports it.
func TestMaterializeUncertainOpen(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.held = true
	f.opener.setScript(openStep{err: fmt.Errorf("verify: %w", artifact.ErrFileCloseUncertain)})
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the open", func() bool { opened, _ := f.opener.counts(); return opened == 1 })
	f.instance("pod-2", false)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)
	if err := f.closeController(time.Second); err == nil {
		t.Fatal("close with an uncertain file = nil, want an error")
	}
	if n := f.faults.counts(); n.marks != 0 || n.finishes != 0 {
		t.Fatalf("marks %d, finishes %d; want none", n.marks, n.finishes)
	}
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("an uncertain file is reported as the S4 hold")
	}
	if len(f.mat.started()) != 0 {
		t.Fatal("an uncertain file was staged")
	}
}

// TestMaterializeStopAtLostBegin stops effects after an admitted Begin's
// result is lost. The same-tuple retry continues: under a known producer it
// recovers the lease, which is only released, never opened; under the
// unknown producer it is Held, the S4 hold. Either way Close then reports
// nothing held.
func TestMaterializeStopAtLostBegin(t *testing.T) {
	for _, known := range []bool{true, false} {
		name, producer := "known", knownProducer
		if !known {
			name, producer = "unknown", store.OriginalServerLifetime{}
		}
		t.Run(name, func(t *testing.T) {
			f := newArtifactFixture(t, producer)
			f.faults.arm(func(a *artifactFaults) { a.loseBegin, a.afterLose = true, f.c.StopMaterializing })
			f.instance("pod-1", true)
			v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
			f.reconcile(t)
			materializeEventually(t, "the retried Begin", func() bool { return len(f.faults.counts().begins) == 2 })
			if err := f.closeController(time.Second); err != nil {
				t.Fatalf("close = %v, want nil", err)
			}
			n := f.faults.counts()
			if known {
				if n.leases[0].ID != n.leases[1].ID || n.marks != 1 || n.finishes != 1 {
					t.Fatalf("leases %v, marks %d, finishes %d; want the lost lease replayed and released", n.leases, n.marks, n.finishes)
				}
			} else if !errors.Is(n.begins[1], store.ErrEnvironmentMaterializationHeld) {
				t.Fatalf("retry = %v, want ErrEnvironmentMaterializationHeld", n.begins[1])
			}
			if opened, _ := f.opener.counts(); opened != 0 {
				t.Fatalf("opened = %d after the stop, want 0", opened)
			}
			f.reconcile(t)
			if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
				t.Fatal("an unstaged incarnation is not held")
			}
		})
	}
}

// TestMaterializeFinishUnderClose loses an applied Finish's result and closes
// the Controller while the retry waits. Close reports the read held; the retry
// continues after Close returns, and a later Close reports nothing held.
func TestMaterializeFinishUnderClose(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	lost := make(chan struct{})
	f.faults.arm(func(a *artifactFaults) { a.loseFinish = lost })
	f.instance("pod-1", false)
	f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the lost Finish", func() bool { return f.faults.counts().finishes == 1 })
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with a lost Finish = nil, want an error")
	}
	close(lost)
	materializeEventually(t, "the retried Finish", func() bool { return f.faults.counts().finishes == 2 })
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close after the retry = %v, want nil", err)
	}
}

// TestMaterializeStopJoins stops effects during a copy that ignores Cancel.
// The copy is canceled, and the body is closed and the read released only
// after the copy is done; nothing else is opened.
func TestMaterializeStopJoins(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.setHold(true)
	f.instance("pod-1", false)
	f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the copy to start", func() bool { return len(f.mat.started()) == 1 })
	environmentcontrol.Integration{Controller: f.c}.StopEffects()
	x := f.mat.started()[0]
	materializeEventually(t, "the copy to be canceled", func() bool {
		select {
		case <-x.canceled:
			return true
		default:
			return false
		}
	})
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with a running copy = nil, want an error")
	}
	if _, closes := f.opener.counts(); closes != 0 {
		t.Fatal("the body was closed before the copy was done")
	}
	if n := f.faults.counts(); n.marks != 0 {
		t.Fatal("the read was marked before the copy was done")
	}
	x.free()
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close after the copy = %v, want nil", err)
	}
	if opened, closes := f.opener.counts(); opened != 1 || closes != 1 {
		t.Fatalf("opened %d, closed %d; want 1 and 1", opened, closes)
	}
}

// TestMaterializeKeptLease (F1) checks each path that leaves a file possibly
// open: an uncertain Open with or without a body, a failed Open whose body's
// Close fails, and a staged body whose Close fails. The frame keeps the exact
// lease the Store returned, and the read is never marked or finished.
func TestMaterializeKeptLease(t *testing.T) {
	uncertain := fmt.Errorf("verify: %w", artifact.ErrFileCloseUncertain)
	failure := errors.New("injected open failure")
	closeErr := errors.New("injected close failure")
	tests := []struct {
		name     string
		script   []openStep
		closeErr error
	}{
		{"uncertain open", []openStep{{err: uncertain}}, nil},
		{"uncertain open with body", []openStep{{err: uncertain, body: true}}, nil},
		{"failed open close", []openStep{{err: failure, body: true}}, closeErr},
		{"staged close", nil, closeErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newArtifactFixture(t, knownProducer)
			f.held = true
			f.opener.setScript(tt.script...)
			f.opener.set(func(o *fakeOpener) { o.closeErr = tt.closeErr })
			f.instance("pod-1", false)
			v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
			f.reconcile(t)
			materializeEventually(t, "the kept lease", func() bool { return len(environmentcontrol.KeptLeases(f.c, v.ID)) != 0 })
			n := f.faults.counts()
			kept := environmentcontrol.KeptLeases(f.c, v.ID)
			if len(n.leases) != 1 || len(kept) != 1 || !sameLease(kept[0], n.leases[0]) {
				t.Fatalf("kept %+v, want only the Store's lease %+v", kept, n.leases)
			}
			if err := f.closeController(50 * time.Millisecond); err == nil {
				t.Fatal("close with a kept lease = nil, want an error")
			}
			if n := f.faults.counts(); n.marks != 0 || n.finishes != 0 {
				t.Fatalf("marks %d, finishes %d; want none", n.marks, n.finishes)
			}
		})
	}
}

// TestMaterializeStopAfterOpen (F2) stops effects while an Open already
// called is still running. StopEffects returns without it; the read stays
// held, unmarked, until that Open returns. Its body is then closed, never
// staged, and its lease released.
func TestMaterializeStopAfterOpen(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.opener.set(func(o *fakeOpener) {
		o.before = func() {
			once.Do(func() { close(entered) })
			<-resume
		}
	})
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the Open was not called")
	}
	stopped := make(chan struct{})
	go func() {
		environmentcontrol.Integration{Controller: f.c}.StopEffects()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		close(resume)
		t.Fatal("StopEffects waited for an Open already called")
	}
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with an Open running = nil, want an error")
	}
	if n := f.faults.counts(); n.marks != 0 || n.finishes != 0 {
		t.Fatalf("marks %d, finishes %d while the Open runs; want none", n.marks, n.finishes)
	}
	close(resume)
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close after the Open = %v, want nil", err)
	}
	if opened, closes := f.opener.counts(); opened != 1 || closes != 1 {
		t.Fatalf("opened %d, closed %d; want 1 and 1", opened, closes)
	}
	if len(f.mat.started()) != 0 {
		t.Fatal("Materialize was called after StopEffects")
	}
	n := f.faults.counts()
	f.opener.mu.Lock()
	opened := slices.Clone(f.opener.opened)
	f.opener.mu.Unlock()
	if len(n.leases) != 1 || !sameLease(opened[0], n.leases[0]) || n.marks != 1 {
		t.Fatalf("leases %v, opened %v, marks %d; want the one lease opened and released", n.leases, opened, n.marks)
	}
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
		t.Fatal("an unstaged incarnation is not held")
	}
}

// TestMaterializeRevokedAfterOpen opens through the real Control, and revokes
// the principal's token after the Open verified the file and before it
// returns. No Materialize is called; the Controller closes the very body the
// Open returned and releases the very lease it opened.
func TestMaterializeRevokedAfterOpen(t *testing.T) {
	f := newArtifactFixture(t, store.OriginalServerLifetime{})
	if f.control.ArtifactProducer() != (store.OriginalServerLifetime{}) {
		t.Fatal("the Control's producer is not the Controller's")
	}
	tokens, err := auth.Tokens(f.config)
	if err != nil {
		t.Fatal(err)
	}
	var token string
	for _, info := range tokens {
		if info.Principal == "alice" {
			token = info.ID
		}
	}
	if token == "" {
		t.Fatal("alice has no token")
	}
	f.instance("pod-1", false)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	authority := f.row(t, v.ID).Authority
	revoked := make(chan error, 1)
	var once sync.Once
	f.opener.set(func(o *fakeOpener) {
		o.through = f.control
		o.after = func() {
			once.Do(func() {
				err := auth.Revoke(f.config, token)
				deadline := time.Now().Add(10 * time.Second)
				for err == nil && f.engine.CheckDelegation(authority) == nil {
					if time.Now().After(deadline) {
						err = errors.New("revocation not observed")
					}
					time.Sleep(time.Millisecond)
				}
				revoked <- err
			})
		}
	})
	f.reconcile(t)
	select {
	case err := <-revoked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the Open was not called")
	}
	materializeEventually(t, "the read to finish", func() bool { return f.faults.counts().finishes == 1 })
	if len(f.mat.started()) != 0 {
		t.Fatal("Materialize was called after authority was lost")
	}
	f.opener.mu.Lock()
	opened, returned, closed := slices.Clone(f.opener.opened), slices.Clone(f.opener.returned), slices.Clone(f.opener.closed)
	f.opener.mu.Unlock()
	if len(returned) != 1 || len(closed) != 1 || closed[0] != returned[0] {
		t.Fatalf("returned %d, closed %d bodies; want the one returned body closed once", len(returned), len(closed))
	}
	n := f.faults.counts()
	if len(n.leases) != 1 || len(opened) != 1 || !sameLease(opened[0], n.leases[0]) || n.marks != 1 {
		t.Fatalf("leases %v, opened %v, marks %d; want the one lease opened and released", n.leases, opened, n.marks)
	}
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close = %v, want nil", err)
	}
}

// TestMaterializeBootstrapJoin holds a Bootstrap whose Cancel returns at once
// but whose Done stays open, after the frame released its only read. The
// read closed in the Store does not settle the frame: Ready is not
// committed, a new incarnation is neither committed nor, once committed
// behind the Controller, allowed to replace the frame, Deleted is not
// committed, and Close reports the frame, also with an ended context. Only
// after the original Bootstrap is done does the frame settle and Close
// return nil.
func TestMaterializeBootstrapJoin(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.setBoot(bootStep{hold: true})
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the Bootstrap to start", func() bool { boots, _ := f.mat.bootstraps(); return len(boots) == 1 })
	boots, _ := f.mat.bootstraps()
	boot := boots[0]
	if boot.incarnation != "pod-1" {
		t.Fatalf("Bootstrap of %q, want pod-1", boot.incarnation)
	}

	// The read is marked and finished; the Store closed it.
	if n := f.faults.counts(); n.marks != 1 || n.finishes != 1 {
		t.Fatalf("marks %d, finishes %d; want the read released", n.marks, n.finishes)
	}
	if _, err := f.probe(v.ID, 1, "pod-1", knownProducer); !errors.Is(err, store.ErrEnvironmentMaterialized) {
		t.Fatalf("probe = %v, want ErrEnvironmentMaterialized", err)
	}
	if !environmentcontrol.HoldsArtifacts(f.c, v.ID) {
		t.Fatal("a frame whose Bootstrap runs holds nothing")
	}
	f.reconcile(t)
	if v := f.get(t, "alice", v.ID, environment.Creating); v.Held {
		t.Fatal("a running Bootstrap is reported held")
	}

	// A new incarnation the runtime observes is not committed.
	f.instance("pod-2", true)
	f.reconcile(t)
	f.wantIncarnation(t, v.ID, "pod-1", 1)

	// One committed behind the Controller does not replace the frame.
	e := f.row(t, v.ID)
	if _, err := f.store.CommitEnvironment(v.ID, e.Revision, store.EnvironmentCommit{State: e.State, Checkpoint: e.Checkpoint, Incarnation: "pod-2"}); err != nil {
		t.Fatalf("commit the next incarnation: %v", err)
	}
	f.reconcile(t)
	if inc, seq, ok := environmentcontrol.FrameIncarnation(f.c, v.ID); !ok || inc != "pod-1" || seq != 1 {
		t.Fatalf("frame = %q seq %d (%v), want the original pod-1 seq 1", inc, seq, ok)
	}
	if n := f.faults.counts(); len(n.begins) != 1 {
		t.Fatalf("Begins = %d, want only the original", len(n.begins))
	}
	if _, calls := f.mat.bootstraps(); calls != 1 || len(f.mat.started()) != 1 {
		t.Fatalf("bootstraps %d, copies %d; want 1 and 1", calls, len(f.mat.started()))
	}

	// Deleted is not committed.
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleting)

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.c.Close(ended); err == nil {
		t.Fatal("close with an ended context and a running Bootstrap = nil, want an error")
	}
	if err := f.closeController(50 * time.Millisecond); err == nil {
		t.Fatal("close with a running Bootstrap = nil, want an error")
	}
	materializeEventually(t, "the Bootstrap to be canceled", func() bool {
		select {
		case <-boot.canceled:
			return true
		default:
			return false
		}
	})
	select {
	case <-boot.done:
		t.Fatal("the held Bootstrap is done")
	default:
	}
	if !environmentcontrol.HoldsArtifacts(f.c, v.ID) {
		t.Fatal("a canceled Bootstrap that is not done holds nothing")
	}

	boot.free()
	f.settle(t, v.ID)
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close after the Bootstrap = %v, want nil", err)
	}
	if inc, seq, ok := environmentcontrol.FrameIncarnation(f.c, v.ID); !ok || inc != "pod-1" || seq != 1 {
		t.Fatalf("frame = %q seq %d (%v), want the original pod-1 seq 1", inc, seq, ok)
	}
	if _, calls := f.mat.bootstraps(); calls != 1 {
		t.Fatalf("bootstraps = %d, want 1", calls)
	}
}

// TestMaterializeBootstrapUnknownRefused (N-B9) settles the first Bootstrap
// with an error, an unknown result: the release may be recorded. The retry
// is refused with ErrIncarnation, which does not mean that it was not. The
// incarnation is held and never Ready, after exactly two calls, and the
// frame holds nothing.
func TestMaterializeBootstrapUnknownRefused(t *testing.T) {
	f := newArtifactFixture(t, knownProducer)
	f.mat.setBoot(
		bootStep{err: errors.New("injected lost acknowledgement")},
		bootStep{refuse: fmt.Errorf("pod-1 bootstrapped: %w", environmentcontrol.ErrIncarnation)},
	)
	f.instance("pod-1", true)
	v := f.create(t, "alice", "env", f.spec("env", "/skygo/artifacts/a"))
	f.reconcile(t)
	materializeEventually(t, "the refused retry", func() bool { _, calls := f.mat.bootstraps(); return calls == 2 })
	f.settle(t, v.ID)
	for range 2 {
		f.reconcile(t)
		if v := f.get(t, "alice", v.ID, environment.Creating); !v.Held {
			t.Fatal("an incarnation whose Bootstrap was refused after an unknown result is not held")
		}
	}
	if boots, calls := f.mat.bootstraps(); calls != 2 || len(boots) != 1 {
		t.Fatalf("bootstraps %d, started %d; want 2 and 1", calls, len(boots))
	}
	if err := f.closeController(time.Second); err != nil {
		t.Fatalf("close = %v, want nil", err)
	}
}
