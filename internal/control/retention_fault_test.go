package control

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func closedRetainedJob(t *testing.T, c *Control, s *store.Store, actor auth.Actor, domain store.RequestDomain, reserve uint64) backend.Job {
	t.Helper()
	j, err := c.SubmitRetained(actor, domain, "closed", retryTask(1))
	if err != nil {
		t.Fatal(err)
	}
	j, began, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), reserve, time.Now().UTC())
	if err != nil || !began {
		t.Fatal("begin", err)
	}
	j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), failedStatus(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Cancel(actor, j.ID); err != nil {
		t.Fatal(err)
	}
	j, began, err = s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, time.Now().UTC())
	if err != nil || began || j.Status.State != backend.Canceled {
		t.Fatal("cancel backoff", err)
	}
	return j
}
func outputTarget(t *testing.T, s *store.Store, b backend.Backend, closed func(context.Context, backend.OutputReference) (backend.OutputReceipt, error), remove func(context.Context, backend.OutputReference) error) backend.Target {
	t.Helper()
	policy, _, err := s.RetentionPolicy()
	if err != nil {
		t.Fatal(err)
	}
	target := retryTarget(b)
	target.OutputStorage = backend.LocalOutput
	target.OutputReservation = uint64(backend.MaxLogBytes) + 4*policy.OutputUnit
	target.Retention = &backend.Retention{CheckOutputClosed: closed, RemoveOutput: remove}
	return target
}
func outputControl(t *testing.T, s *store.Store, target backend.Target, engine *auth.Engine, usage func(context.Context) (uint64, uint64, error)) *Control {
	t.Helper()
	c, err := NewWithLocalOutput(s, []backend.Target{target}, engine, LocalOutputConfig{Target: target.Name, Binding: target.Binding, BindOutput: func(backend.OutputReference) error { return nil }, OutputUsage: usage})
	if err != nil {
		t.Fatal(err)
	}
	primeStorage(c)
	return c
}

func TestBlockedObservationKeepsOneOwnerAndCleanupGetsNextSlot(t *testing.T) {
	s, info := retainedStore(t)
	b := &routeBackend{}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var samples, removals atomic.Int32
	target := outputTarget(t, s, b, func(_ context.Context, r backend.OutputReference) (backend.OutputReceipt, error) {
		return backend.OutputReceipt{ClosureID: "closed-" + r.AttemptID, PayloadReleased: true}, nil
	}, func(context.Context, backend.OutputReference) error { removals.Add(1); return nil })
	c := outputControl(t, s, target, testEngine, func(context.Context) (uint64, uint64, error) {
		samples.Add(1)
		close(entered)
		<-release
		return 123, 2, nil
	})
	closedRetainedJob(t, c, s, testActor(), info.Domain, target.OutputReservation)
	page, _, err := c.Prune(context.Background(), testActor(), store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.OutputOnly}, true, "")
	if err != nil || page.Marked != 1 {
		t.Fatal(page, err)
	}
	c.retention.sample = nil
	ctx, cancel := context.WithCancel(context.Background())
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
	exited := make(chan struct{})
	go func() { defer close(exited); runMaintenance(ctx, m.input, m.results) }()
	defer func() { unblock(); cancel(); waitValue(t, exited) }()
	if _, err = c.RetentionInfo(testActor(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if err = c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	waitValue(t, entered)
	for i := 0; i < 100; i++ {
		got, err := c.RetentionInfo(testActor(), 0, "")
		if err != nil || got.Admin == nil || got.Admin.DatabaseFileBytes != nil {
			t.Fatal("held sample reported invented metrics", err)
		}
		m.due = time.Time{}
		if err = c.maintain(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if samples.Load() != 1 || removals.Load() != 0 {
		t.Fatal("blocked sample was replaced or cleanup overlapped")
	}
	unblock()
	result := waitValue(t, m.results)
	if err = c.integrateMaintenance(m, result); err != nil {
		t.Fatal(err)
	}
	m.due = time.Time{}
	if err = c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	result = waitValue(t, m.results)
	if result.request.record == nil || result.request.record.Operation != store.OutputCleanup {
		t.Fatal("sample starved ready cleanup")
	}
	if err = c.integrateMaintenance(m, result); err != nil {
		t.Fatal(err)
	}
	if samples.Load() != 1 || removals.Load() != 1 {
		t.Fatal("unexpected callback count")
	}
}

func scopedRetention(t *testing.T) (*store.Store, store.RetentionInfo, *auth.Engine, auth.Actor, string) {
	t.Helper()
	s, info := retainedStore(t)
	config := filepath.Join(t.TempDir(), "auth.json")
	initial := filepath.Join(filepath.Dir(config), "admin.token")
	if _, err := auth.Initialize(config, initial, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(config, "backup", auth.Admin); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Issue(config, "backup", filepath.Join(filepath.Dir(config), "backup.token"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(initial)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go engine.Run(ctx)
	return s, info, engine, actor, config
}

func TestPruneRevokedDuringFirstClosureHasZeroWrites(t *testing.T) {
	s, info, engine, actor, config := scopedRetention(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var queries atomic.Int32
	target := outputTarget(t, s, &routeBackend{}, func(_ context.Context, r backend.OutputReference) (backend.OutputReceipt, error) {
		queries.Add(1)
		close(entered)
		<-release
		return backend.OutputReceipt{ClosureID: "closed-" + r.AttemptID, PayloadReleased: true}, nil
	}, func(context.Context, backend.OutputReference) error { t.Error("prune dispatched mutation"); return nil })
	c := outputControl(t, s, target, engine, func(context.Context) (uint64, uint64, error) { return 0, 0, nil })
	closedRetainedJob(t, c, s, actor, info.Domain, target.OutputReservation)
	path := filepath.Join(s.Directory(), "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	selection := store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.OutputOnly}
	done := make(chan error, 1)
	go func() { _, _, err := c.Prune(context.Background(), actor, selection, true, ""); done <- err }()
	waitValue(t, entered)
	if _, _, err = c.Prune(context.Background(), actor, selection, false, ""); !errors.Is(err, ErrUnavailable) {
		t.Fatal("blocked closure acquired replacement", err)
	}
	if err = auth.Revoke(config, actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err = engine.Check(actor, auth.Read); errors.Is(err, auth.ErrUnauthenticated) {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("revocation not observed")
		case <-tick.C:
		}
	}
	unblock()
	if err = waitValue(t, done); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("stale query authorized prune", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || len(c.access) != 0 || queries.Load() != 1 {
		t.Fatal("revoked query wrote proof/mark/audit or was replaced", err)
	}
}

func TestLocalBinderFailureNeverStartsOrReleasesReservation(t *testing.T) {
	s, info := retainedStore(t)
	local := outputTarget(t, s, &routeBackend{}, func(context.Context, backend.OutputReference) (backend.OutputReceipt, error) {
		return backend.OutputReceipt{}, errors.New("not closed")
	}, func(context.Context, backend.OutputReference) error { return nil })
	local.Name = "local"
	local.Binding = "binding-local"
	remote := newFairBackend()
	remoteTarget := targetFor("remote", remote)
	remoteTarget.OutputStorage = backend.RemoteOutput
	entered, release := make(chan backend.OutputReference, 1), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	c, err := NewWithLocalOutput(s, []backend.Target{local, remoteTarget}, testEngine, LocalOutputConfig{Target: local.Name, Binding: local.Binding, BindOutput: func(r backend.OutputReference) error {
		entered <- r
		<-release
		return errors.New("injected binder failure")
	}, OutputUsage: func(context.Context) (uint64, uint64, error) { return 0, 0, nil }})
	if err != nil {
		t.Fatal(err)
	}
	primeStorage(c)
	task := backend.Task{Argv: []string{"work"}, Target: "local"}
	job, err := c.SubmitRetained(testActor(), info.Domain, "local", task)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := c.SubmitRetained(testActor(), info.Domain, "remote", backend.Task{Argv: []string{"work"}, Target: "remote"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := c.newLoop(ctx)
	if err = c.schedule(ctx, loop); err != nil {
		t.Fatal(err)
	}
	ref := waitValue(t, entered)
	if ref.Owner != s.Owner() || ref.JobID != job.ID || ref.AttemptID != job.ID || ref.Binding != job.TargetBinding || ref.Generation != 1 || ref.ClosureID != "" || ref.PayloadReleased {
		t.Fatal("binder received fabricated output authority", ref)
	}
	if id := waitValue(t, remote.starts); id != peer.ID {
		t.Fatal("blocked binder starved another target")
	}
	if _, err = c.Cancel(testActor(), job.ID); err != nil {
		t.Fatal(err)
	}
	unblock()
	for i := 0; i < 2; i++ {
		if err = c.integrate(loop, waitValue(t, loop.results)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(job.ID)
	if err != nil || got.Status.State != backend.Unknown || got.Status.Termination != backend.UnknownTermination || !got.CancelRequested || local.Backend.(*routeBackend).starts != 0 {
		t.Fatal("binder failure started/retried or forged termination", got, err)
	}
	_, err = s.OutputReference(job.ID, job.ID)
	if err != nil {
		t.Fatal("failed binder released committed output reservation", err)
	}
}

func TestRetentionDiscoveryRejectsReaderPagination(t *testing.T) {
	s, _, _, _, config := scopedRetention(t)
	if err := auth.AddPrincipal(config, "reader", auth.Reader); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(filepath.Dir(config), "reader.token")
	if _, err := auth.Issue(config, "reader", tokenPath, time.Hour); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(s, []backend.Target{retainedTarget(&routeBackend{})}, engine)
	if err != nil {
		t.Fatal(err)
	}
	c.retention.wantSample.Store(false)
	view, err := c.RetentionInfo(actor, 0, "")
	if err != nil || view.Admin != nil || !view.Info.Enabled {
		t.Fatal("reader discovery", view, err)
	}
	for _, tt := range []struct {
		size  int
		token string
	}{{1, ""}, {0, "token"}} {
		if _, err := c.RetentionInfo(actor, tt.size, tt.token); !errors.Is(err, auth.ErrPermissionDenied) {
			t.Fatal("reader pagination accepted", tt, err)
		}
	}
	if c.retention.wantSample.Load() {
		t.Fatal("reader discovery requested privileged diagnostics")
	}
}

func TestCanceledBinderCannotLaunchAfterItsActualReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	b := &routeBackend{}
	input, results := make(chan operation, 1), make(chan operationResult, 1)
	go func() {
		defer close(exited)
		runWorkerWithOutput(ctx, b, func(backend.OutputReference) error { close(entered); <-release; return nil }, input, results)
	}()
	defer func() { unblock(); cancel(); waitValue(t, exited) }()
	job := backend.Job{ID: "11111111111111111111111111111111", Task: backend.Task{Argv: []string{"work"}}, Status: backend.Status{State: backend.Starting}}
	input <- operation{kind: startOperation, job: job, output: &backend.OutputReference{AttemptID: job.ID}}
	waitValue(t, entered)
	cancel()
	unblock()
	waitValue(t, exited)
	if b.starts != 0 {
		t.Fatal("binder return after context cancellation launched execution")
	}
}

func TestEnabledCursorRejectedAcrossScopedActivation(t *testing.T) {
	s, info := retainedStore(t)
	target := retainedTarget(&routeBackend{})
	c := routeControl(t, s, target)
	for _, request := range []string{"first", "second"} {
		if _, err := c.SubmitRetained(testActor(), info.Domain, request, backend.Task{Argv: []string{"work"}}); err != nil {
			t.Fatal(err)
		}
	}
	_, token, err := c.List(testActor(), 1, "")
	if err != nil || token == "" {
		t.Fatal(token, err)
	}
	old, err := decodeRetentionCursor(token)
	if err != nil || old.Scoped {
		t.Fatal(old, err)
	}
	config := filepath.Join(t.TempDir(), "auth.json")
	tokenPath := filepath.Join(filepath.Dir(config), "admin.token")
	if _, err = auth.Initialize(config, tokenPath, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := New(s, []backend.Target{target}, engine)
	if err != nil {
		t.Fatal(err)
	}
	if !scoped.scopeCursor(actor, "list", info.Domain.Lineage).Scoped {
		t.Fatal("cursor omitted actual scoped-auth mode")
	}
	if _, _, err = scoped.List(actor, 1, token); !errors.Is(err, ErrInvalid) {
		t.Fatal("enabled legacy-auth cursor accepted after activation", err)
	}
}

type heldCheckBackend struct {
	*routeBackend
	entered chan struct{}
	release chan struct{}
}

func (b *heldCheckBackend) Check(backend.Task) error { close(b.entered); <-b.release; return nil }
func TestAcceptanceRechecksStorageAfterBlockingPlacement(t *testing.T) {
	s, info := retainedStore(t)
	b := &heldCheckBackend{routeBackend: &routeBackend{}, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(b.release) }) }
	defer unblock()
	c := routeControl(t, s, retainedTarget(b))
	done := make(chan error, 1)
	go func() {
		_, err := c.SubmitRetained(testActor(), info.Domain, "held-check", backend.Task{Argv: []string{"work"}})
		done <- err
	}()
	waitValue(t, b.entered)
	c.retention.mu.Lock()
	c.retention.sample.ObservedAt = time.Now().Add(-6 * time.Second)
	c.retention.mu.Unlock()
	unblock()
	if err := waitValue(t, done); !errors.Is(err, ErrUnavailable) {
		t.Fatal("stale pre-placement observation admitted task", err)
	}
	max, err := s.LastSequence()
	if err != nil || max != 0 || b.starts != 0 {
		t.Fatal("held placement accepted job or execution", max, err)
	}
}
