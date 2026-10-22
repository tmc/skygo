//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// No Run is started. These counters detect accidental admission/start work;
// these methods produce no native creation, observation or removal receipt.
type dataAdmissionBackend struct {
	logBackend
	checks atomic.Int32
	starts atomic.Int32
}

func (b *dataAdmissionBackend) Check(backend.Task) error { b.checks.Add(1); return nil }
func (b *dataAdmissionBackend) Start(context.Context, string, backend.Task) error {
	b.starts.Add(1)
	return errors.New("unexpected ordinary start in metadata-only fixture")
}
func (b *dataAdmissionBackend) StartData(context.Context, backend.DataStart) (backend.DataCreated, error) {
	b.starts.Add(1)
	return backend.DataCreated{}, errors.New("unexpected data start in metadata-only fixture")
}
func (*dataAdmissionBackend) ObserveData(context.Context, backend.Reference) (backend.DataObservation, error) {
	return backend.DataObservation{}, errors.New("unexpected native observation")
}
func (*dataAdmissionBackend) RemoveData(context.Context, backend.DataResources) (backend.DataRemoval, error) {
	return backend.DataRemoval{}, errors.New("unexpected native removal")
}

func dataAdmissionTarget(b *dataAdmissionBackend) backend.Target {
	policy := backend.DefaultDataPolicy()
	return backend.Target{Name: "test", Binding: "test-binding", Kind: "kubernetes", Backend: b, OutputStorage: backend.RemoteOutput,
		DataPolicy: &policy, SupportsRemoteData: true, SupportsDataMounts: true}
}

// Retention disabled: this is the actual fresh HTTP data acceptance. It needs
// no request domain and no storage sample, so the unsupported-route refusals
// and binding's new acceptance below run through real HTTP admission.
func TestSubmitDataHTTPMetadataAcceptanceAcrossDiscoveryDrift(t *testing.T) {
	for _, drift := range []string{"capability", "policy", "binding", "absent"} {
		t.Run(drift, func(t *testing.T) { dataSubmitAcrossDrift(t, false, drift) })
	}
}

// Retention enabled: the initial queued row is Store fixture setup, NOT an
// HTTP retained-admission PASS. A retained new acceptance requires the
// Control's actual storage sample, which only Run produces, and Run would
// dispatch the queued job. That boundary is covered at the sampled Control
// oracle internal/control TestDataSubmissionAcceptsWithoutStartingNativeWork/true.
// Here the first HTTP request is exact replay of that real Store.SubmitData row.
func TestSubmitDataHTTPQueuedReplayAcrossDiscoveryDrift(t *testing.T) {
	for _, drift := range []string{"capability", "policy", "binding", "absent"} {
		t.Run(drift, func(t *testing.T) { dataSubmitAcrossDrift(t, true, drift) })
	}
}

func dataSubmitAcrossDrift(t *testing.T, retained bool, drift string) {
	b := new(dataAdmissionBackend)
	target := dataAdmissionTarget(b)
	f := artifactRPCTargetOptions(t, nil, false, retained, func(value *backend.Target) { *value = target })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	message := &v1.SubmitDataJobRequest{RequestId: "data-stable", Task: &v1.Task{Argv: []string{"never-run"}, Target: "test", Directory: "/work", DataMounts: []*v1.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/data", ReadOnly: true}}}}
	if retained {
		info, err := f.client.GetRetention(ctx, authorized(f.tokens["alice"], &v1.GetRetentionRequest{}))
		if err != nil || info.Msg.Domain == nil {
			t.Fatalf("domain: %v %v", info, err)
		}
		message.Domain = proto.Clone(info.Msg.Domain).(*v1.RequestDomain)
	}
	planBefore := dataAdmissionDB(t, f)
	planned, err := f.client.PlanTask(ctx, authorized(f.tokens["alice"], &v1.PlanTaskRequest{Task: proto.Clone(message.Task).(*v1.Task)}))
	if err != nil || planned.Msg.Target != "test" {
		t.Fatalf("actual data plan: %v %v", planned, err)
	}
	// Planning opens no Bolt Update, so the file bytes are exact.
	if !bytes.Equal(planBefore, dataAdmissionDB(t, f)) {
		t.Fatal("data plan changed database bytes")
	}
	if got := b.starts.Load(); got != 0 {
		t.Fatalf("data plan: native start calls = %d, want 0", got)
	}
	planChecks := b.checks.Load()
	var id string
	if retained {
		id = seedRetainedData(t, f, message, target)
		before := dataAdmissionState(t, f)
		replay, err := f.client.SubmitDataJob(ctx, authorized(f.tokens["alice"], proto.Clone(message).(*v1.SubmitDataJobRequest)))
		if err != nil || replay.Msg.Job == nil || replay.Msg.Job.Id != id || replay.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED {
			t.Fatalf("exact HTTP replay of retained Store row: %v %v", replay, err)
		}
		f.awaitReturned(t, skyv1connect.ControlServiceSubmitDataJobProcedure, 1)
		dataAdmissionCounts(t, b, "exact HTTP replay", planChecks)
		before.unchanged(t, f, retained, "exact HTTP replay")
	} else {
		accepted, err := f.client.SubmitDataJob(ctx, authorized(f.tokens["alice"], message))
		if err != nil {
			t.Fatal(err)
		}
		if accepted.Msg.Job == nil || accepted.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED {
			t.Fatalf("not a metadata-only accepted queue: %v", accepted)
		}
		dataAdmissionCounts(t, b, "metadata-only acceptance", planChecks+1)
		f.awaitReturned(t, skyv1connect.ControlServiceSubmitDataJobProcedure, 1)
		id = accepted.Msg.Job.Id
	}
	switch drift {
	case "capability":
		target.SupportsRemoteData, target.SupportsDataMounts = false, false
	case "policy":
		target.DataPolicy, target.SupportsRemoteData, target.SupportsDataMounts = nil, false, false
	case "binding":
		target.Binding = "changed-binding"
	case "absent":
		target.Name, target.Binding = "other", "other-binding"
	}
	g := reopenDataAdmission(t, f, []backend.Target{target})
	before := dataAdmissionState(t, g)
	checks := b.checks.Load()
	replay, err := g.client.SubmitDataJob(ctx, authorized(g.tokens["alice"], proto.Clone(message).(*v1.SubmitDataJobRequest)))
	if err != nil || replay.Msg.Job.Id != id {
		t.Fatalf("exact replay after %s: %v %v", drift, replay, err)
	}
	g.awaitReturned(t, skyv1connect.ControlServiceSubmitDataJobProcedure, 1)
	dataAdmissionCounts(t, b, "exact replay after "+drift, checks)
	before.unchanged(t, g, retained, "exact replay after "+drift)
	changed := proto.Clone(message).(*v1.SubmitDataJobRequest)
	changed.Task.Argv = []string{"changed-never-run"}
	_, err = g.client.SubmitDataJob(ctx, authorized(g.tokens["alice"], changed))
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("changed semantic replay: %v", err)
	}
	before.unchanged(t, g, retained, "semantic conflict")
	fresh := proto.Clone(message).(*v1.SubmitDataJobRequest)
	fresh.RequestId = "data-new"
	freshAccepted, err := g.client.SubmitDataJob(ctx, authorized(g.tokens["alice"], fresh))
	switch {
	case retained:
		// The reopened Control has no storage sample and no Run is started, so
		// the real admission guard refuses before placement. Route refusal and
		// binding's new acceptance are the metadata test's HTTP coverage.
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("unsampled retained new admission: %v", err)
		}
		dataAdmissionCounts(t, b, "unsampled retained new admission", checks)
	case drift != "binding":
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("new admission with unsupported %s: %v", drift, err)
		}
	default:
		// Binding drift remains a currently qualified independent target. A
		// new ID is allowed there; replay still used the old binding.
		if err != nil || freshAccepted.Msg.Job.Id == id {
			t.Fatalf("qualified new binding admission: %v", err)
		}
		dataAdmissionCounts(t, b, "qualified new binding admission", checks+1)
	}
	if retained || drift != "binding" {
		before.unchanged(t, g, retained, "refused new ID")
	}
	changedAuth := g.engine.Changed()
	if err := auth.Revoke(g.authFile, g.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changedAuth:
	case <-ctx.Done():
		t.Fatal("original auth observer did not load revocation")
	}
	_, err = g.client.SubmitDataJob(ctx, authorized(g.tokens["alice"], message))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("revoked actor replay: %v", err)
	}
	if b.starts.Load() != 0 {
		t.Fatal("metadata-only fixture invoked native start")
	}
}

// seedRetainedData accepts message's task through the real Store.SubmitData
// with alice's actual file Delegation, request domain, target route and data
// policy. It is fixture setup, not HTTP admission: no Backend.Check runs.
func seedRetainedData(t *testing.T, f *artifactRPCFixture, message *v1.SubmitDataJobRequest, target backend.Target) string {
	t.Helper()
	actor, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
	if err != nil {
		t.Fatal(err)
	}
	if actor, err = f.engine.Check(actor, auth.Mutate); err != nil {
		t.Fatal(err)
	}
	authority, err := f.engine.Delegate(actor)
	if err != nil {
		t.Fatal(err)
	}
	// The same backend.Task the HTTP handler derives from message.Task.
	task := backend.Task{Argv: []string{"never-run"}, Target: "test", Directory: "/work", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/data", ReadOnly: true}}}
	if err := control.Validate(task); err != nil {
		t.Fatal(err)
	}
	if err := backend.ValidateDataTask(task, nil); err != nil {
		t.Fatal(err)
	}
	domain := store.RequestDomain{Lineage: message.Domain.Lineage, Epoch: message.Domain.Epoch}
	job, err := f.store.SubmitData(actor, &domain, message.RequestId, task, target.Name, target.Binding, authority, *target.DataPolicy, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if job.Status.State != backend.Queued || job.Target != target.Name || job.TargetBinding != target.Binding {
		t.Fatalf("seeded retained row: %#v", job)
	}
	return job.ID
}

// Reopen only after the SAME old HTTP/Control owners and resources positively
// join. The new fixture takes namespace custody; no live Control is replaced.
func reopenDataAdmission(t *testing.T, old *artifactRPCFixture, targets []backend.Target) *artifactRPCFixture {
	t.Helper()
	if !old.settle(t) {
		old.retain(t)
		t.Fatal(old.held)
	}
	if err := old.retire(); err != nil {
		old.held = err
		old.retain(t)
		t.Fatal(err)
	}
	old.retired = true
	f := &artifactRPCFixture{directory: old.directory, rootName: old.rootName, tokens: old.tokens, ids: old.ids, authFile: old.authFile, returned: make(chan string, 128), remove: os.RemoveAll}
	t.Cleanup(func() { f.cleanup(t) })
	f.retire = func() error {
		if f.authCancel != nil {
			f.authCancel()
			<-f.authDone
		}
		if f.files != nil {
			if err := f.files.Close(); err != nil {
				return err
			}
		}
		if f.root != nil {
			if err := f.root.Close(); err != nil {
				return err
			}
		}
		if f.store != nil {
			return f.store.Close()
		}
		return nil
	}
	fail := func(err error) { f.held = err; t.Fatal(err) }
	var err error
	f.root, err = os.OpenRoot(f.rootName)
	if err != nil {
		fail(err)
	}
	info, err := f.root.Stat(".")
	if err != nil {
		fail(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		fail(errors.New("root identity unavailable"))
	}
	f.files, err = artifact.Open(f.root, artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)})
	if err != nil {
		fail(err)
	}
	f.store, err = store.Open(filepath.Join(f.directory, "state"))
	if err != nil {
		fail(err)
	}
	f.engine, err = auth.NewFile(f.authFile, f.store.Owner())
	if err != nil {
		fail(err)
	}
	authCtx, cancel := context.WithCancel(context.Background())
	f.authCancel, f.authDone = cancel, make(chan struct{})
	go func() { defer close(f.authDone); f.engine.Run(authCtx) }()
	f.control, err = control.NewWithConfig(f.store, targets, f.engine, control.Config{Artifacts: f.files})
	if err != nil {
		fail(err)
	}
	f.httpHandler = rpc.NewHandler(f.control, f.engine)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.httpHandler.ServeHTTP(w, r); f.returned <- r.URL.Path }))
	f.client = skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL)
	return f
}

type dataAdmissionCloseError struct {
	original *os.File
	cause    error
}

func (e *dataAdmissionCloseError) Error() string { return "original metadata snapshot Close failed" }
func (e *dataAdmissionCloseError) Unwrap() error { return e.cause }

func dataAdmissionDB(t *testing.T, f *artifactRPCFixture) []byte {
	t.Helper()
	original, err := os.Open(filepath.Join(f.store.Directory(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(original)
	closeErr := original.Close()
	if closeErr != nil {
		f.held = &dataAdmissionCloseError{original, errors.Join(readErr, closeErr)}
		f.retain(t)
		t.Fatal(f.held)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	return data
}

// dataAdmissionCounts reports placement and native start separately: a
// replay or refusal must not run Backend.Check again or start anything.
func dataAdmissionCounts(t *testing.T, b *dataAdmissionBackend, what string, checks int32) {
	t.Helper()
	if got := b.checks.Load(); got != checks {
		t.Fatalf("%s: Backend.Check calls = %d, want %d", what, got, checks)
	}
	if got := b.starts.Load(); got != 0 {
		t.Fatalf("%s: native start calls = %d, want 0", what, got)
	}
}

// dataAdmissionSnapshot is the store database as file bytes and as the
// logical rows they decode to.
type dataAdmissionSnapshot struct {
	raw  []byte
	rows map[string]string
}

func dataAdmissionState(t *testing.T, f *artifactRPCFixture) dataAdmissionSnapshot {
	t.Helper()
	raw := dataAdmissionDB(t, f)
	return dataAdmissionSnapshot{raw: raw, rows: dataAdmissionRows(t, f, raw)}
}

// unchanged fails if a request changed the database since s.
//
// Without retention, replay and refusal only read in a Bolt View, so the
// file bytes must be identical. With retention, Store.LookupRetained runs in
// a Bolt Update even when it writes nothing, and every commit advances the
// transaction ID and rewrites the meta and freelist pages. There, every
// logical row and bucket sequence must be identical instead: request rows,
// authority, grants, reservations, artifact rows, the job, its task and
// route, audit, and the retention state itself. No phase falls due inside
// the default 30-day window, so no retention phase or clock delta is allowed.
func (s dataAdmissionSnapshot) unchanged(t *testing.T, f *artifactRPCFixture, retained bool, what string) {
	t.Helper()
	after := dataAdmissionState(t, f)
	if diff := dataAdmissionDiff(s.rows, after.rows); len(diff) != 0 {
		t.Fatalf("%s changed database rows %q", what, diff)
	}
	if !retained && !bytes.Equal(s.raw, after.raw) {
		t.Fatalf("%s changed database bytes", what)
	}
}

// dataAdmissionCopy owns one private directory and every original file and
// Bolt handle opened in it. Each handle is closed exactly once and its result
// kept. The directory is removed only after every Close positively
// succeeded; otherwise the copy, its original handles, the exact error and
// the directory stay held. A later Close of the same Bolt handle would not
// prove the first one: bbolt marks the handle closed before its fallible
// munmap and file Close.
type dataAdmissionCopy struct {
	dir     string
	handles []*dataAdmissionHandle
	// closeHook, if set, replaces the direct call of each original Close. It
	// must call that Close exactly once; tests use it to add a known result.
	closeHook func(*dataAdmissionHandle) error
	held      error
	removed   bool
}

type dataAdmissionHandle struct {
	original any // *os.File or *bolt.DB
	close    func() error
	closed   bool
	err      error
}

// dataAdmissionCopyError links a held copy into its owner's hold.
type dataAdmissionCopyError struct {
	owner *dataAdmissionCopy
	cause error
}

func (e *dataAdmissionCopyError) Error() string {
	return "private database copy held at " + e.owner.dir + ": " + e.cause.Error()
}
func (e *dataAdmissionCopyError) Unwrap() error { return e.cause }

// newDataAdmissionCopy makes a private 0700 directory under parent, which
// must be a namespace whose removal the caller already owns.
func newDataAdmissionCopy(parent string) (*dataAdmissionCopy, error) {
	dir, err := os.MkdirTemp(parent, "rows-")
	if err != nil {
		return nil, err
	}
	return &dataAdmissionCopy{dir: dir}, nil
}

func (c *dataAdmissionCopy) own(original any, close func() error) *dataAdmissionHandle {
	h := &dataAdmissionHandle{original: original, close: close}
	c.handles = append(c.handles, h)
	return h
}

// closeOnce closes h if it has not been closed and returns the one recorded
// result. It never closes a handle twice.
func (c *dataAdmissionCopy) closeOnce(h *dataAdmissionHandle) error {
	if !h.closed {
		h.closed = true
		if c.closeHook != nil {
			h.err = c.closeHook(h)
		} else {
			h.err = h.close()
		}
	}
	return h.err
}

// release closes any handle still open, once, then removes the directory if
// every Close succeeded. Otherwise it holds the copy. A held copy stays held.
func (c *dataAdmissionCopy) release() error {
	if c.held != nil {
		return c.held
	}
	var failed []error
	for _, h := range c.handles {
		if err := c.closeOnce(h); err != nil {
			failed = append(failed, err)
		}
	}
	if len(failed) != 0 {
		c.held = &dataAdmissionCopyError{c, errors.Join(failed...)}
		return c.held
	}
	if err := os.RemoveAll(c.dir); err != nil {
		c.held = &dataAdmissionCopyError{c, err}
		return c.held
	}
	c.removed = true
	return nil
}

func (c *dataAdmissionCopy) write(name string, data []byte) error {
	file, err := os.OpenFile(filepath.Join(c.dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	h := c.own(file, file.Close)
	_, err = file.Write(data)
	return errors.Join(err, c.closeOnce(h))
}

func (c *dataAdmissionCopy) read(name string) ([]byte, error) {
	file, err := os.Open(filepath.Join(c.dir, name))
	if err != nil {
		return nil, err
	}
	h := c.own(file, file.Close)
	data, err := io.ReadAll(file)
	return data, errors.Join(err, c.closeOnce(h))
}

func (c *dataAdmissionCopy) open(name string, readOnly bool) (*bolt.DB, *dataAdmissionHandle, error) {
	db, err := bolt.Open(filepath.Join(c.dir, name), 0600, &bolt.Options{ReadOnly: readOnly, Timeout: time.Second})
	if err != nil {
		return nil, nil, err
	}
	return db, c.own(db, db.Close), nil
}

// project writes raw to a private copy and decodes it read-only, so the live,
// exclusively locked database is never opened twice. Each bucket, nested ones
// included, contributes its sequence and every row, keyed by its quoted
// bucket path. Meta pages, the transaction ID and the freelist are physical
// and do not appear. The caller must release c.
func (c *dataAdmissionCopy) project(raw []byte) (map[string]string, error) {
	if err := c.write("jobs.db", raw); err != nil {
		return nil, err
	}
	db, h, err := c.open("jobs.db", true)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]string)
	var walk func(string, *bolt.Bucket) error
	walk = func(path string, b *bolt.Bucket) error {
		rows[path+" seq"] = strconv.FormatUint(b.Sequence(), 10)
		return b.ForEach(func(k, v []byte) error {
			key := path + "/" + strconv.Quote(string(k))
			if v == nil {
				if child := b.Bucket(k); child != nil {
					return walk(key, child)
				}
			}
			rows[key] = string(v)
			return nil
		})
	}
	viewErr := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(bucket []byte, b *bolt.Bucket) error { return walk(strconv.Quote(string(bucket)), b) })
	})
	if err := errors.Join(viewErr, c.closeOnce(h)); err != nil {
		return nil, err
	}
	return rows, nil
}

// dataAdmissionProject projects raw through a copy inside f's namespace. If
// the copy cannot be released, only the hold joins f's hold, so f's cleanup
// retains the namespace; the returned error keeps the projection's own write,
// view or Close error joined with that hold.
func dataAdmissionProject(f *artifactRPCFixture, raw []byte, hook func(*dataAdmissionHandle) error) (map[string]string, *dataAdmissionCopy, error) {
	c, err := newDataAdmissionCopy(f.directory)
	if err != nil {
		return nil, nil, err
	}
	c.closeHook = hook
	rows, err := c.project(raw)
	if held := c.release(); held != nil {
		f.held = errors.Join(f.held, held)
		return nil, c, errors.Join(err, held)
	}
	return rows, c, err
}

func dataAdmissionRows(t *testing.T, f *artifactRPCFixture, raw []byte) map[string]string {
	t.Helper()
	rows, _, err := dataAdmissionProject(f, raw, nil)
	if f.held != nil {
		f.retain(t)
		t.Fatal(f.held)
	}
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// dataAdmissionDiff names, without values, each row that differs.
func dataAdmissionDiff(before, after map[string]string) []string {
	var diff []string
	for k, v := range before {
		if w, ok := after[k]; !ok || w != v {
			diff = append(diff, k)
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			diff = append(diff, k)
		}
	}
	slices.Sort(diff)
	return diff
}

// TestDataAdmissionRowsIgnorePhysicalCommit checks the oracle itself: an
// empty Bolt commit changes the file but no projected row, and a changed
// nested row and sequence are each reported by path. Its private namespace
// is removed only after every original handle closed positively.
func TestDataAdmissionRowsIgnorePhysicalCommit(t *testing.T) {
	c := &dataAdmissionCopy{dir: artifactFixtureDirectory(t)}
	t.Cleanup(func() {
		if err := c.release(); err != nil {
			t.Errorf("retaining private database copy: %v", err)
			keep := make(chan struct{})
			go func() { <-keep; runtime.KeepAlive(c) }()
		}
	})
	update := func(fn func(*bolt.Tx) error) []byte {
		t.Helper()
		db, h, err := c.open("jobs.db", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(db.Update(fn), c.closeOnce(h)); err != nil {
			t.Fatal(err)
		}
		raw, err := c.read("jobs.db")
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	rowsOf := func(raw []byte) map[string]string {
		t.Helper()
		p, err := newDataAdmissionCopy(c.dir)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := p.project(raw)
		if held := p.release(); held != nil {
			c.held = held
			t.Fatal(held)
		}
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	initial := update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket([]byte("retention"))
		if err != nil {
			return err
		}
		requests, err := root.CreateBucket([]byte("requests"))
		if err != nil {
			return err
		}
		if _, err := requests.NextSequence(); err != nil {
			return err
		}
		return requests.Put([]byte{0, 0xff}, []byte(`{"job_id":"a"}`))
	})
	empty := update(func(*bolt.Tx) error { return nil })
	if bytes.Equal(initial, empty) {
		t.Fatal("empty Bolt commit left the file unchanged; the physical control is vacuous")
	}
	if diff := dataAdmissionDiff(rowsOf(initial), rowsOf(empty)); len(diff) != 0 {
		t.Fatalf("physical commit changed rows %q", diff)
	}
	changed := update(func(tx *bolt.Tx) error {
		requests := tx.Bucket([]byte("retention")).Bucket([]byte("requests"))
		if _, err := requests.NextSequence(); err != nil {
			return err
		}
		return requests.Put([]byte{0, 0xff}, []byte(`{"job_id":"b"}`))
	})
	want := []string{`"retention"/"requests" seq`, `"retention"/"requests"/"\x00\xff"`}
	if diff := dataAdmissionDiff(rowsOf(empty), rowsOf(changed)); !slices.Equal(diff, want) {
		t.Fatalf("changed rows = %q, want %q", diff, want)
	}
}

// TestDataAdmissionCopyCloseCustody checks the copy owner on the actual
// fixture. The hook calls each original Close exactly once; for the failure
// case it adds an injected unknown result to the Bolt Close only, and
// simulates no kernel failure. Positive release closes each handle once and
// removes the copy; a held copy keeps its original handle, exact result and
// directory, joins the fixture hold, and survives fixture cleanup.
func TestDataAdmissionCopyCloseCustody(t *testing.T) {
	f := artifactRPC(t, nil)
	raw := dataAdmissionDB(t, f)
	calls := make(map[*dataAdmissionHandle]int)
	var injected, actual error
	hook := func(h *dataAdmissionHandle) error {
		calls[h]++
		err := h.close()
		if _, ok := h.original.(*bolt.DB); ok && injected != nil {
			actual = err
			return errors.Join(injected, err)
		}
		return err
	}
	closedOnce := func(c *dataAdmissionCopy, err error) {
		t.Helper()
		if len(c.handles) != 2 {
			t.Fatalf("copy owned %d handles, want the write file and the Bolt handle; projection: %v", len(c.handles), err)
		}
		if _, ok := c.handles[0].original.(*os.File); !ok {
			t.Fatalf("first handle is %T, want the original write file", c.handles[0].original)
		}
		if _, ok := c.handles[1].original.(*bolt.DB); !ok {
			t.Fatalf("second handle is %T, want the original Bolt handle", c.handles[1].original)
		}
		for _, h := range c.handles {
			if !h.closed || calls[h] != 1 {
				t.Fatalf("%T Close calls = %d, want exactly 1", h.original, calls[h])
			}
		}
	}

	rows, c, err := dataAdmissionProject(f, raw, hook)
	if c == nil {
		t.Fatal(err)
	}
	// Close counts first: a repeated Close also fails the release, and the
	// count is the direct witness.
	closedOnce(c, err)
	if err != nil || len(rows) == 0 || f.held != nil {
		t.Fatalf("positive projection: %d rows, %v, held %v", len(rows), err, f.held)
	}
	if _, err := os.Stat(c.dir); !c.removed || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("positively closed copy not removed: removed=%t %v", c.removed, err)
	}

	injected = errors.New("injected unknown Bolt Close result")
	rows, c, err = dataAdmissionProject(f, raw, hook)
	if c == nil {
		t.Fatal(err)
	}
	closedOnce(c, err)
	var held *dataAdmissionCopyError
	if rows != nil || !errors.Is(err, injected) || !errors.As(f.held, &held) || held.owner != c || c.held != held {
		t.Fatalf("failed Close not held: %d rows, %v, fixture held %v", len(rows), err, f.held)
	}
	// The projection's own Close error travels beside the hold rather than
	// being replaced by it: exactly the projection error, then the hold.
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 {
		t.Fatalf("projection error dropped beside the hold: %v", err)
	}
	var inner *dataAdmissionCopyError
	if es := joined.Unwrap(); es[1] != held || errors.As(es[0], &inner) || !errors.Is(es[0], injected) {
		t.Fatalf("projection error and hold not both kept: %v", err)
	}
	if !errors.Is(c.handles[1].err, injected) {
		t.Fatalf("lost the exact Bolt Close result: %v", c.handles[1].err)
	}
	if _, err := os.Stat(c.dir); c.removed || err != nil {
		t.Fatalf("held copy removed: removed=%t %v", c.removed, err)
	}
	if err := c.release(); err != held || calls[c.handles[1]] != 1 {
		t.Fatal("repeated release closed again or replaced the hold")
	}
	saved := f.held
	if err := f.retireJoined(t); err != saved || f.held != saved || f.retired {
		t.Fatalf("fixture cleanup released a held copy: %v", err)
	}
	if _, err := os.Stat(c.dir); err != nil {
		t.Fatalf("fixture cleanup removed the held copy: %v", err)
	}
	if actual != nil {
		// The real Close failed too; the fixture cleanup keeps all of it.
		t.Fatalf("actual Bolt Close: %v", actual)
	}
	// The real Close succeeded; only the injected sentinel was unknown.
	// Release that known sentinel so positive fixture cleanup removes the
	// namespace. Real held owner errors are never reset by fixture code.
	f.held = nil
	f.cleanup(t)
	if !f.retired {
		t.Fatal("positively settled fixture not retired")
	}
	if _, err := os.Stat(f.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("positively settled namespace not removed: %v", err)
	}
}
