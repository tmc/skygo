package control_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

type observation struct {
	id    string
	reply chan observationResult
}
type observationResult struct {
	status backend.Status
	err    error
}
type executor struct {
	starts   atomic.Int32
	cancels  atomic.Int32
	checkErr error
	startErr error
	logsErr  error
	observed chan observation
}

func newExecutor() *executor                 { return &executor{observed: make(chan observation)} }
func (e *executor) Check(backend.Task) error { return e.checkErr }
func (e *executor) Start(context.Context, string, backend.Task) error {
	e.starts.Add(1)
	return e.startErr
}
func (e *executor) Cancel(context.Context, backend.Reference) error { e.cancels.Add(1); return nil }
func (e *executor) Observe(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	call := observation{id: ref.ID, reply: make(chan observationResult, 1)}
	select {
	case e.observed <- call:
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
	select {
	case r := <-call.reply:
		return r.status, r.err
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
}
func (e *executor) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, e.logsErr
}
func nextObservation(t *testing.T, e *executor) observation {
	t.Helper()
	select {
	case call := <-e.observed:
		return call
	case <-time.After(10 * time.Second):
		t.Fatal("reconciler did not observe execution")
		return observation{}
	}
}
func answer(call observation, state backend.State, err error) {
	call.reply <- observationResult{status: backend.Status{State: state}, err: err}
}
func newControl(t *testing.T, s *store.Store, e *executor) *control.Control {
	t.Helper()
	c, err := control.New(s, []backend.Target{{Name: "local", Binding: "test-binding", Kind: "local", Backend: e}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func run(t *testing.T, c *control.Control) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("reconciler did not stop")
		}
	})
}
func submit(t *testing.T, c *control.Control, id string) backend.Job {
	t.Helper()
	j, err := c.Submit(testActor(), id, backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestConcurrentDedupAndChangedConfiguration(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := newExecutor()
	c := newControl(t, s, e)
	task := backend.Task{Argv: []string{"work"}}
	const n = 24
	jobs := make(chan backend.Job, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); j, err := c.Submit(testActor(), "same", task); jobs <- j; errs <- err }()
	}
	wg.Wait()
	close(jobs)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	for j := range jobs {
		if id == "" {
			id = j.ID
		}
		if j.ID != id {
			t.Fatal("same submission produced different jobs")
		}
	}
	if _, err := c.Submit(testActor(), "same", backend.Task{Argv: []string{"different"}}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("conflicting submission: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	unavailable := newExecutor()
	unavailable.checkErr = errors.New("capability removed")
	c, err = control.New(s, []backend.Target{{Name: "replacement", Binding: "test-binding", Kind: "local", Backend: unavailable}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	task.Env = map[string]string{}
	j, err := c.Submit(testActor(), "same", task)
	if err != nil || j.ID != id || j.Target != "local" {
		t.Fatalf("durable retry after target removal: %+v %v", j, err)
	}
	if _, err := c.Submit(testActor(), "new", task); !errors.Is(err, control.ErrUnsupported) {
		t.Fatalf("new unsupported submission: %v", err)
	}
}

func TestCancelQueuedNeverStarts(t *testing.T) {
	s := openStore(t)
	e := newExecutor()
	c := newControl(t, s, e)
	j := submit(t, c, "canceled")
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	next := submit(t, c, "next")
	run(t, c)
	call := nextObservation(t, e)
	if call.id != next.ID {
		t.Fatalf("observed canceled job %s", call.id)
	}
	got, err := c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Canceled {
		t.Fatalf("canceled job: %+v %v", got, err)
	}
	if e.starts.Load() != 1 {
		t.Fatalf("starts = %d, want only next job", e.starts.Load())
	}
	answer(call, backend.Running, nil)
}

func TestUncertainStartNotRetriedAndOutagePreservesState(t *testing.T) {
	s := openStore(t)
	e := newExecutor()
	e.startErr = context.DeadlineExceeded
	c := newControl(t, s, e)
	j := submit(t, c, "one")
	run(t, c)
	first := nextObservation(t, e)
	answer(first, backend.Running, nil)
	second := nextObservation(t, e)
	got, err := c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Running {
		t.Fatalf("after uncertain start: %+v %v", got, err)
	}
	answer(second, "", errors.New("API outage"))
	third := nextObservation(t, e)
	got, err = c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Running {
		t.Fatalf("outage replaced observed state: %+v %v", got, err)
	}
	if e.starts.Load() != 1 {
		t.Fatalf("uncertain startup repeated %d times", e.starts.Load())
	}
	answer(third, backend.Running, nil)
}

func TestRecoveredStartingNeverStarts(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := newExecutor()
	c := newControl(t, s, e)
	j := submit(t, c, "recovered")
	if _, err := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Starting; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e = newExecutor()
	c = newControl(t, s, e)
	sentinel := submit(t, c, "sentinel")
	run(t, c)
	call := nextObservation(t, e)
	if call.id != j.ID {
		t.Fatalf("unexpected recovered observation %s", call.id)
	}
	if e.starts.Load() != 0 {
		t.Fatal("recovery relaunched uncertain execution")
	}
	answer(call, backend.Unknown, nil)
	call = nextObservation(t, e)
	if call.id != sentinel.ID {
		t.Fatalf("unexpected sentinel %s", call.id)
	}
	got, err := c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Unknown {
		t.Fatalf("unknown recovery: %+v %v", got, err)
	}
	answer(call, backend.Running, nil)
}

func TestCancellationPreservesObservedCompletion(t *testing.T) {
	s := openStore(t)
	e := newExecutor()
	c := newControl(t, s, e)
	j := submit(t, c, "finished")
	sentinel := submit(t, c, "sentinel")
	run(t, c)
	call := nextObservation(t, e)
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	answer(call, backend.Succeeded, nil)
	call = nextObservation(t, e)
	if call.id != sentinel.ID {
		t.Fatalf("unexpected next observation %s", call.id)
	}
	got, err := c.Cancel(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Succeeded {
		t.Fatalf("late cancellation changed completion: %+v %v", got, err)
	}
	answer(call, backend.Running, nil)
}

func TestPaginationSnapshotAndMalformedTokens(t *testing.T) {
	s := openStore(t)
	c := newControl(t, s, newExecutor())
	for i := 0; i < 3; i++ {
		submit(t, c, fmt.Sprint(i))
	}
	jobs, token, err := c.List(testActor(), 1, "")
	if err != nil || len(jobs) != 1 || token == "" {
		t.Fatalf("first page: %+v %q %v", jobs, token, err)
	}
	submit(t, c, "later")
	jobs, next, err := c.List(testActor(), 2, token)
	if err != nil || len(jobs) != 2 || next != "" || jobs[1].Sequence != 3 {
		t.Fatalf("snapshot: %+v %q %v", jobs, next, err)
	}
	tokens := []string{"!", base64.RawURLEncoding.EncodeToString([]byte(`{}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"a":1,"m":100}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"a":2,"m":2}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"a":1,"m":3,"extra":true}`))}
	for _, token := range tokens {
		if _, _, err := c.List(testActor(), 1, token); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("token %q: %v", token, err)
		}
	}
	for _, size := range []int{-1, 201} {
		if _, _, err := c.List(testActor(), size, ""); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("size %d: %v", size, err)
		}
	}
}

func TestTaskValidation(t *testing.T) {
	tests := []struct {
		name string
		task backend.Task
	}{
		{"empty", backend.Task{}}, {"empty program", backend.Task{Argv: []string{""}}},
		{"nul argument", backend.Task{Argv: []string{"work", "\x00"}}},
		{"environment key", backend.Task{Argv: []string{"work"}, Env: map[string]string{"BAD=KEY": "x"}}},
		{"environment value", backend.Task{Argv: []string{"work"}, Env: map[string]string{"KEY": "\x00"}}},
		{"negative CPU", backend.Task{Argv: []string{"work"}, CPUMillis: -1}},
		{"negative memory", backend.Task{Argv: []string{"work"}, MemoryBytes: -1}},
		{"missing accelerator count", backend.Task{Argv: []string{"work"}, Accelerator: "gpu"}},
		{"missing accelerator name", backend.Task{Argv: []string{"work"}, AcceleratorCount: 1}},
		{"too long timeout", backend.Task{Argv: []string{"work"}, TimeoutSeconds: 604801}},
		{"too big", backend.Task{Argv: []string{"work", strings.Repeat("x", 256<<10)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := control.Validate(tt.task); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
	if err := control.Validate(backend.Task{Argv: []string{"work", ""}, Env: map[string]string{"KEY": ""}}); err != nil {
		t.Fatalf("valid empty argument or env: %v", err)
	}
}

func TestPlanDeterministic(t *testing.T) {
	s := openStore(t)
	bad := newExecutor()
	bad.checkErr = errors.New("unsupported image")
	c, err := control.New(s, []backend.Target{{Name: "z", Binding: "test-binding", Backend: newExecutor()}, {Name: "b", Binding: "test-binding", Backend: bad}, {Name: "a", Binding: "test-binding", Backend: newExecutor()}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	target, rejected, err := c.Plan(testActor(), backend.Task{Argv: []string{"work"}})
	if err != nil || target != "a" || len(rejected) != 1 || rejected[0].Target != "b" {
		t.Fatalf("plan: %q %+v %v", target, rejected, err)
	}
}

func TestCancelQueuedAfterTargetRemoved(t *testing.T) {
	s := openStore(t)
	c := newControl(t, s, newExecutor())
	j := submit(t, c, "removed-target")
	replacement := newExecutor()
	c, err := control.New(s, []backend.Target{{Name: "replacement", Binding: "test-binding", Kind: "local", Backend: replacement}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	// No backend event should occur: a queued cancellation needs no executor.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	got, err := c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Canceled {
		t.Fatalf("queued cancellation after target removal: %+v %v", got, err)
	}
	if replacement.starts.Load() != 0 {
		t.Fatal("replacement target executed canceled job")
	}
}

func TestActiveExecutionLimit(t *testing.T) {
	s := openStore(t)
	e := newExecutor()
	c := newControl(t, s, e)
	var jobs []backend.Job
	for i := 0; i < 35; i++ {
		jobs = append(jobs, submit(t, c, fmt.Sprint(i)))
	}
	run(t, c)
	for i := 0; i < 32; i++ {
		call := nextObservation(t, e)
		if call.id != jobs[i].ID {
			t.Fatalf("start order: %s", call.id)
		}
		answer(call, backend.Running, nil)
	}
	// Observing the first job again proves the first reconciliation pass finished.
	call := nextObservation(t, e)
	if call.id != jobs[0].ID || e.starts.Load() != 32 {
		t.Fatalf("admission exceeded: next=%s starts=%d", call.id, e.starts.Load())
	}
	for _, j := range jobs[32:] {
		got, err := c.Get(testActor(), j.ID)
		if err != nil || got.Status.State != backend.Queued {
			t.Fatalf("overflow job: %+v %v", got, err)
		}
	}
	answer(call, backend.Succeeded, nil)
	// Completing one job frees one slot on the next pass.
	for i := 1; i < 32; i++ {
		answer(nextObservation(t, e), backend.Running, nil)
	}
	for i := 1; i < 32; i++ {
		answer(nextObservation(t, e), backend.Running, nil)
	}
	call = nextObservation(t, e)
	if call.id != jobs[32].ID || e.starts.Load() != 33 {
		t.Fatalf("slot not reused: next=%s starts=%d", call.id, e.starts.Load())
	}
	answer(call, backend.Running, nil)
}

func TestLogErrorClassification(t *testing.T) {
	for _, tt := range []struct {
		name        string
		cause, want error
	}{
		{"temporary", errors.New("API unavailable private detail"), control.ErrUnavailable},
		{"missing", fmt.Errorf("retained file: %w", fs.ErrNotExist), control.ErrLogsUnavailable},
		{"invalid", fs.ErrInvalid, fs.ErrInvalid},
		{"canceled", context.Canceled, context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := openStore(t)
			e := newExecutor()
			e.logsErr = tt.cause
			c := newControl(t, s, e)
			j := submit(t, c, "logs")
			if _, err := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
				t.Fatal(err)
			}
			_, err := c.Logs(context.Background(), testActor(), j.ID, 0)
			if !errors.Is(err, tt.want) {
				t.Fatalf("log error: %v, want %v", err, tt.want)
			}
			if strings.Contains(err.Error(), "private detail") {
				t.Fatalf("private backend detail leaked: %v", err)
			}
		})
	}
}

func TestPlanIncludesRejections(t *testing.T) {
	e := newExecutor()
	e.checkErr = errors.New("images unsupported")
	c := newControl(t, openStore(t), e)
	_, _, err := c.Plan(testActor(), backend.Task{Argv: []string{"work"}, Image: "image"})
	if !errors.Is(err, control.ErrUnsupported) || !strings.Contains(err.Error(), "local: images unsupported") {
		t.Fatalf("plan rejection: %v", err)
	}
}
