package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

func TestWorkerSpecWire(t *testing.T) {
	tests := []struct {
		name string
		in   *v1.WorkerSpec
		want backend.WorkerSpec
		ok   bool
	}{
		{"nil", nil, backend.WorkerSpec{}, false},
		{"independent", &v1.WorkerSpec{Count: 3, Mode: v1.WorkerMode_WORKER_MODE_INDEPENDENT}, backend.WorkerSpec{Count: 3, Mode: backend.WorkerIndependent}, true},
		{"tcp", &v1.WorkerSpec{Count: 2, Mode: v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS}, backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}, true},
		// Narrowing 257 to uint8 would give 1.
		{"count 257", &v1.WorkerSpec{Count: 257, Mode: v1.WorkerMode_WORKER_MODE_INDEPENDENT}, backend.WorkerSpec{}, false},
		{"count 17", &v1.WorkerSpec{Count: 17, Mode: v1.WorkerMode_WORKER_MODE_INDEPENDENT}, backend.WorkerSpec{}, false},
		{"no mode", &v1.WorkerSpec{Count: 2}, backend.WorkerSpec{}, false},
		{"unknown mode", &v1.WorkerSpec{Count: 2, Mode: 9}, backend.WorkerSpec{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := workerSpec(tt.in)
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("workerSpec = %+v, %v; want %+v ok %v", got, err, tt.want, tt.ok)
			}
			if err != nil && !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("error %v is not invalid argument", err)
			}
			if tt.ok {
				if back := wireWorkerSpec(got); back.Count != tt.in.Count || back.Mode != tt.in.Mode {
					t.Fatalf("round trip = %+v", back)
				}
			}
		})
	}
}

func TestWireWorkerRank(t *testing.T) {
	code := int32(3)
	tests := []struct {
		in   control.WorkerRank
		want v1.WorkerRankState
	}{
		{control.WorkerRank{State: control.WorkerWaiting}, v1.WorkerRankState_WORKER_RANK_STATE_WAITING},
		{control.WorkerRank{State: control.WorkerStarting}, v1.WorkerRankState_WORKER_RANK_STATE_STARTING},
		{control.WorkerRank{State: control.WorkerRunning}, v1.WorkerRankState_WORKER_RANK_STATE_RUNNING},
		{control.WorkerRank{State: control.WorkerSucceeded}, v1.WorkerRankState_WORKER_RANK_STATE_SUCCEEDED},
		{control.WorkerRank{State: control.WorkerFailed, ExitCode: &code}, v1.WorkerRankState_WORKER_RANK_STATE_FAILED},
		{control.WorkerRank{State: control.WorkerCanceled}, v1.WorkerRankState_WORKER_RANK_STATE_CANCELED},
		{control.WorkerRank{State: control.WorkerUnknown}, v1.WorkerRankState_WORKER_RANK_STATE_UNKNOWN},
		// A state with no wire value never reports progress.
		{control.WorkerRank{}, v1.WorkerRankState_WORKER_RANK_STATE_UNKNOWN},
	}
	for _, tt := range tests {
		got := wireWorkerRank(tt.in)
		if got.State != tt.want {
			t.Fatalf("wireWorkerRank(%+v).State = %v, want %v", tt.in, got.State, tt.want)
		}
		if (got.ExitCode == nil) != (tt.in.ExitCode == nil) || got.ExitCode != nil && *got.ExitCode != *tt.in.ExitCode {
			t.Fatalf("exit code = %v, want %v", got.ExitCode, tt.in.ExitCode)
		}
	}
}

// mountedWorkerBackend is MODELED. It records exact resources and returns
// prepared=false, so these HTTP tests cannot accidentally claim main execution.
type mountedWorkerBackend struct {
	stopping atomic.Bool
	removes  atomic.Int32
	reads    atomic.Int32
	checks   atomic.Int32
	read     func(context.Context, backend.WorkerReference, int64) (backend.LogChunk, error)
}

func (b *mountedWorkerBackend) Check(backend.Task) error { b.checks.Add(1); return nil }
func (*mountedWorkerBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected ordinary start")
}
func (*mountedWorkerBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected ordinary observe")
}
func (*mountedWorkerBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected ordinary cancel")
}
func (*mountedWorkerBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected ordinary logs")
}
func (*mountedWorkerBackend) CreateWorker(_ context.Context, c backend.WorkerCreate) (backend.WorkerCreated, error) {
	got := backend.WorkerCreated{Pod: backend.WorkerResource{Name: fmt.Sprintf("worker-%d", c.Rank), UID: fmt.Sprintf("pod-%s-%d", c.Group.AttemptID, c.Rank)}}
	if c.Rank == 0 && c.Group.Spec.Mode == backend.WorkerTCP {
		got.Service = backend.WorkerResource{Name: "rendezvous", UID: "service-" + c.Group.AttemptID}
	}
	return got, nil
}
func (*mountedWorkerBackend) ReleaseWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerReleased, error) {
	return backend.WorkerReleased{Pod: r.Pod, Acknowledged: true, Revision: "7"}, nil
}
func (*mountedWorkerBackend) ObserveWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerObservation, error) {
	return backend.WorkerObservation{Pod: r.Pod, Status: backend.Status{State: backend.Starting}}, nil
}
func (b *mountedWorkerBackend) CancelWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerCanceled, error) {
	b.stopping.Store(true)
	return backend.WorkerCanceled{Pod: r.Pod, Acknowledged: true}, nil
}
func (b *mountedWorkerBackend) WorkerLogs(ctx context.Context, r backend.WorkerReference, offset int64, _ int) (backend.LogChunk, error) {
	b.reads.Add(1)
	return b.read(ctx, r, offset)
}
func (b *mountedWorkerBackend) RemoveWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerRemoval, error) {
	b.removes.Add(1)
	got := backend.WorkerRemoval{Pod: r.Pod, PodRemoved: r.Pod.UID != ""}
	if r.Rank == 0 && r.Group.Spec.Mode == backend.WorkerTCP {
		got.Service = r.Service
		got.ServiceRemoved = r.Service.UID != ""
	}
	return got, nil
}
func (b *mountedWorkerBackend) ObserveBarrier(_ context.Context, r backend.WorkerReference) (backend.WorkerBarrier, error) {
	got := backend.WorkerBarrier{Pod: r.Pod, Revision: "8", Node: "modeled-node"}
	if b.stopping.Load() {
		got.Stop = backend.WorkerTerminated
		got.Deleting = true
	}
	return got, nil
}
func (*mountedWorkerBackend) StartWorker(context.Context, backend.WorkerReference, [32]byte) (backend.WorkerStarted, error) {
	return backend.WorkerStarted{}, errors.New("unprepared member started")
}
func (*mountedWorkerBackend) ResolveWorker(context.Context, backend.WorkerCreate) (backend.WorkerCreated, error) {
	return backend.WorkerCreated{}, errors.New("unexpected resolve")
}
func (*mountedWorkerBackend) RecoverWorkerRemoval(context.Context, backend.WorkerRecovery) (backend.WorkerRemoval, error) {
	return backend.WorkerRemoval{}, errors.New("unexpected recover")
}

var _ backend.WorkerBarrierBackend = (*mountedWorkerBackend)(nil)

type mountedWorkerFixture struct {
	store   *store.Store
	engine  *auth.Engine
	control *control.Control
	workers *control.Workers
	backend *mountedWorkerBackend
	server  *httptest.Server
	client  skyv1connect.WorkerGroupServiceClient
	tokens  map[string]string
	domain  *v1.RequestDomain
}

func mountedWorkers(t *testing.T, read func(context.Context, backend.WorkerReference, int64) (backend.LogChunk, error)) *mountedWorkerFixture {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	f := &mountedWorkerFixture{store: s, tokens: make(map[string]string), backend: &mountedWorkerBackend{read: read}, domain: &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}}
	config := filepath.Join(dir, "auth.json")
	if _, err := auth.Initialize(config, filepath.Join(dir, "legacy.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alice", "bob"} {
		if err := auth.AddPrincipal(config, name, auth.Operator); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name+".token")
		if _, err := auth.Issue(config, name, path, time.Hour); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[name] = strings.TrimSpace(string(data))
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	f.engine, err = auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	f.control, err = control.New(s, []backend.Target{{Name: "gang", Kind: "MODELED", Binding: "gang-binding", Backend: f.backend, SupportsResourceLimits: true, OutputStorage: backend.RemoteOutput}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	// Run the real maintenance sampler instead of manufacturing physical facts.
	runCtx, stopRun := context.WithCancel(context.Background())
	var runErr error
	runDone := make(chan struct{})
	go func() { defer close(runDone); runErr = f.control.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		select {
		case <-runDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Error(runErr)
			}
		case <-time.After(15 * time.Second):
			t.Error("Control.Run did not join")
		}
	})
	readyDeadline := time.NewTimer(10 * time.Second)
	defer readyDeadline.Stop()
	readyTick := time.NewTicker(10 * time.Millisecond)
	defer readyTick.Stop()
	for !f.control.StorageReady() {
		select {
		case <-runDone:
			t.Fatalf("Control.Run returned before sampler ready: %v", runErr)
		case <-readyDeadline.C:
			t.Fatal("actual storage sampler not ready")
		case <-readyTick.C:
		}
	}
	f.workers, err = control.NewWorkers(f.control, control.WorkerConfig{Budgets: map[string]workeradmission.Budget{"gang": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30}}})
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(NewWorkerHandler(f.control, f.engine, f.workers))
	t.Cleanup(f.server.Close)
	f.client = skyv1connect.NewWorkerGroupServiceClient(f.server.Client(), f.server.URL)
	return f
}
func workerRequest[T any](f *mountedWorkerFixture, name string, m *T) *connect.Request[T] {
	r := connect.NewRequest(m)
	r.Header().Set("Authorization", "Bearer "+f.tokens[name])
	return r
}
func (f *mountedWorkerFixture) submit(t *testing.T, id string) *v1.Job {
	t.Helper()
	m := &v1.SubmitWorkerJobRequest{Task: &v1.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: "gang", CpuMillis: 1000, MemoryBytes: 1 << 30}, RequestId: id, Domain: f.domain, Workers: &v1.WorkerSpec{Count: 2, Mode: v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS}}
	got, err := f.client.SubmitWorkerJob(context.Background(), workerRequest(f, "alice", m))
	if err != nil {
		t.Fatal(err)
	}
	return got.Msg.Job
}
func mountedRankLog(_ context.Context, r backend.WorkerReference, offset int64) (backend.LogChunk, error) {
	if r.Service.UID == "" {
		return backend.LogChunk{}, errors.New("canonical rank-zero service omitted")
	}
	data := []byte(fmt.Sprintf("rank %d\n", r.Rank))
	if offset > int64(len(data)) {
		return backend.LogChunk{}, errors.New("offset past log")
	}
	return backend.LogChunk{Data: data[offset:], NextOffset: int64(len(data)), Complete: true}, nil
}

func TestWorkerMountedReplayAndSelectors(t *testing.T) {
	f := mountedWorkers(t, mountedRankLog)
	job := f.submit(t, "stable")
	checks := f.backend.checks.Load()
	replay := f.submit(t, "stable")
	if replay.Id != job.Id || f.backend.checks.Load() != checks {
		t.Fatalf("replay changed ID or replanned: %s %s checks %d/%d", job.Id, replay.Id, checks, f.backend.checks.Load())
	}
	for range 3 {
		if err := f.workers.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		name, principal, attempt string
		rank                     uint32
		want                     connect.Code
	}{
		{"foreign", "bob", job.Id, 0, connect.CodeNotFound},
		{"other attempt", "alice", strings.Repeat("f", 32), 0, connect.CodeNotFound},
		{"other rank", "alice", job.Id, 2, connect.CodeInvalidArgument},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := f.backend.reads.Load()
			stream, err := f.client.StreamWorkerLogs(context.Background(), workerRequest(f, tt.principal, &v1.StreamWorkerLogsRequest{Id: job.Id, AttemptId: tt.attempt, Rank: tt.rank, Limit: 1024}))
			if err == nil {
				defer stream.Close()
				for stream.Receive() {
				}
				err = stream.Err()
			}
			if connect.CodeOf(err) != tt.want || f.backend.reads.Load() != before {
				t.Fatalf("refusal=%v reads %d/%d", err, before, f.backend.reads.Load())
			}
		})
	}
	stream, err := f.client.StreamWorkerLogs(context.Background(), workerRequest(f, "alice", &v1.StreamWorkerLogsRequest{Id: job.Id, AttemptId: job.Id, Rank: 1, Limit: 1024}))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Receive() {
		t.Fatal(stream.Err())
	}
	m := stream.Msg()
	if string(m.Data) != "rank 1\n" || m.Rank != 1 || m.AttemptId != job.Id || m.Complete {
		t.Fatalf("rank projection: %v", m)
	}
	for stream.Receive() {
		m := stream.Msg()
		if len(m.Data) != 0 || m.NextOffset != 7 || m.AttemptId != job.Id || m.Rank != 1 {
			t.Fatalf("trailing rank projection: %v", m)
		}
	}
	if stream.Err() != nil {
		t.Fatalf("stream termination: %v", stream.Err())
	}
}

func TestWorkerMountDoesNotAdvertise(t *testing.T) {
	f := mountedWorkers(t, mountedRankLog)
	c := skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL)
	info, err := c.GetServerInfo(context.Background(), workerRequest(f, "alice", &v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, cap := range info.Msg.Capabilities {
		if cap == v1.Capability_CAPABILITY_INDEXED_WORKERS_V1 || cap == v1.Capability_CAPABILITY_TCP_RENDEZVOUS_V1 {
			t.Fatalf("unqualified capability: %v", cap)
		}
	}
	old := httptest.NewServer(NewHandler(f.control, f.engine))
	defer old.Close()
	_, err = skyv1connect.NewWorkerGroupServiceClient(old.Client(), old.URL).GetWorkerRanks(context.Background(), workerRequest(f, "alice", &v1.GetWorkerRanksRequest{Id: strings.Repeat("a", 32), AttemptId: strings.Repeat("a", 32)}))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("ordinary handler mounts workers: %v", err)
	}
}

// The original real handler commits and completes before the proxy deliberately
// loses the response. A canonical retry must recover that same Store request;
// neither the proxy nor this test emulates persistence or runs a workload.
func TestWorkerMountedLostResponse(t *testing.T) {
	f := mountedWorkers(t, mountedRankLog)
	handler := NewWorkerHandler(f.control, f.engine, f.workers)
	returned := make(chan struct{}, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { returned <- struct{}{} }()
		handler.ServeHTTP(httptest.NewRecorder(), r)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	}))
	defer proxy.Close()
	client := skyv1connect.NewWorkerGroupServiceClient(proxy.Client(), proxy.URL)
	request := &v1.SubmitWorkerJobRequest{Task: &v1.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: "gang", CpuMillis: 1000, MemoryBytes: 1 << 30}, RequestId: "lost", Domain: f.domain, Workers: &v1.WorkerSpec{Count: 2, Mode: v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.SubmitWorkerJob(ctx, workerRequest(f, "alice", request)); err == nil {
		t.Fatal("lost response unexpectedly delivered")
	}
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("original handler did not return")
	}
	checks := f.backend.checks.Load()
	recovered := f.submit(t, "lost")
	if f.backend.checks.Load() != checks {
		t.Fatal("canonical retry replanned")
	}
	actor, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
	if err != nil {
		t.Fatal(err)
	}
	jobs, _, err := f.control.List(actor, 100, "")
	if err != nil || len(jobs) != 1 || jobs[0].ID != recovered.Id {
		t.Fatalf("lost response accepted duplicate: %+v %v", jobs, err)
	}
}

// mountedTrailerWriter only models a blocked network write. It surrounds the
// actual mounted handler and leaves authentication, Connect and response.finish
// intact. No replacement RPC enclosure or Store lifecycle is used.
type mountedTrailerWriter struct {
	http.ResponseWriter
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *mountedTrailerWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *mountedTrailerWriter) Flush()                      { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w *mountedTrailerWriter) Write(p []byte) (int, error) {
	if len(p) == 5 && p[0]&2 != 0 {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return w.ResponseWriter.Write(p)
}

func TestWorkerMountedLogsOwnFinalTrailers(t *testing.T) {
	f := mountedWorkers(t, mountedRankLog)
	job := f.submit(t, "trailers")
	for range 3 {
		if err := f.workers.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	release := make(chan struct{})
	var once sync.Once
	entered, returned := make(chan struct{}), make(chan struct{})
	handler := NewWorkerHandler(f.control, f.engine, f.workers)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		handler.ServeHTTP(&mountedTrailerWriter{ResponseWriter: w, entered: entered, release: release}, r)
	}))
	defer func() { once.Do(func() { close(release) }); server.Close() }()
	client := skyv1connect.NewWorkerGroupServiceClient(server.Client(), server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	receiveDone := make(chan error, 1)
	go func() {
		stream, err := client.StreamWorkerLogs(ctx, workerRequest(f, "alice", &v1.StreamWorkerLogsRequest{Id: job.Id, AttemptId: job.Id, Rank: 0, Limit: 1024}))
		if err == nil {
			defer stream.Close()
			for stream.Receive() {
			}
			err = stream.Err()
		}
		receiveDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Connect did not reach final trailers")
	}
	if f.backend.reads.Load() != 2 {
		t.Fatalf("original log calls=%d, want data and trailing empty read", f.backend.reads.Load())
	}
	actor, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.control.Cancel(actor, job.Id); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if err := f.workers.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if f.backend.removes.Load() != 0 {
		t.Fatal("removal overtook final response owner")
	}
	before := f.backend.reads.Load()
	if _, err := f.workers.BeginLogs(actor, job.Id, job.Id, 0); err == nil {
		t.Fatal("stopping group admitted new log owner")
	}
	if f.backend.reads.Load() != before {
		t.Fatal("refused admission read backend")
	}
	once.Do(func() { close(release) })
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("actual response did not finish")
	}
	select {
	case err := <-receiveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("client did not finish")
	}
	for range 6 {
		if err := f.workers.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if f.backend.removes.Load() == 0 {
		t.Fatal("positive stopped resources not removed after response finished")
	}
}
