package control_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

type scopedControl struct {
	c      *control.Control
	s      *store.Store
	e      *auth.Engine
	actors map[string]auth.Actor
	config string
}

func newScopedControl(t *testing.T) scopedControl {
	t.Helper()
	s := openStore(t)
	dir := t.TempDir()
	config := filepath.Join(dir, "auth.json")
	initial := filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(config, initial, s.Owner()); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{"legacy": initial}
	for id, role := range map[string]auth.Role{"alice": auth.Operator, "bob": auth.Operator, "reader": auth.Reader} {
		if err := auth.AddPrincipal(config, id, role); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, id+".token")
		if _, err := auth.Issue(config, id, path, time.Hour); err != nil {
			t.Fatal(err)
		}
		paths[id] = path
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	e, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.Run(ctx)
	actors := map[string]auth.Actor{}
	for id, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		a, err := e.Authenticate("Bearer " + string(bytes.TrimSpace(raw)))
		if err != nil {
			t.Fatal(err)
		}
		actors[id] = a
	}
	c, err := control.New(s, []backend.Target{{Name: "local", Binding: "test-binding", Kind: "local", Backend: newExecutor()}}, e)
	if err != nil {
		t.Fatal(err)
	}
	return scopedControl{c, s, e, actors, config}
}

func TestScopedControlRejectsForgedAndForeign(t *testing.T) {
	f := newScopedControl(t)
	task := backend.Task{Argv: []string{"work"}}
	b, err := f.c.Submit(f.actors["bob"], "same", task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Get(auth.Actor{}, b.ID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("zero get: %v", err)
	}
	if _, err := f.c.Submit(testActor(), "forged", task); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("foreign engine: %v", err)
	}
	if _, err := f.c.Get(f.actors["alice"], b.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign get: %v", err)
	}
	if _, err := f.c.Cancel(f.actors["alice"], b.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign cancel: %v", err)
	}
	if _, err := f.c.Logs(context.Background(), f.actors["alice"], b.ID, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign logs: %v", err)
	}
	for _, call := range []func() error{
		func() error { _, err := f.c.Submit(f.actors["reader"], "read-only", task); return err },
		func() error { _, err := f.c.Cancel(f.actors["reader"], b.ID); return err },
	} {
		if err := call(); !errors.Is(err, auth.ErrPermissionDenied) {
			t.Fatalf("reader mutation: %v", err)
		}
	}
	if _, _, err := f.c.Plan(f.actors["reader"], task); err != nil {
		t.Fatalf("reader plan: %v", err)
	}
	got, err := f.c.Cancel(f.actors["legacy"], b.ID)
	if err != nil || !got.CancelRequested {
		t.Fatalf("admin cancel: %+v %v", got, err)
	}
}

func TestScopedCursorCannotInferOtherAcceptance(t *testing.T) {
	f := newScopedControl(t)
	task := backend.Task{Argv: []string{"work"}}
	alice, bob, admin := f.actors["alice"], f.actors["bob"], f.actors["legacy"]
	first, err := f.c.Submit(alice, "first", task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Submit(bob, "other", task); err != nil {
		t.Fatal(err)
	}
	second, err := f.c.Submit(alice, "second", task)
	if err != nil {
		t.Fatal(err)
	}
	jobs, cursor, err := f.c.List(alice, 1, "")
	if err != nil || len(jobs) != 1 || jobs[0].ID != first.ID || cursor == "" {
		t.Fatalf("first page: %+v %q %v", jobs, cursor, err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["m"] != float64(2) || fields["a"] != float64(1) || fields["p"] != "alice" {
		t.Fatalf("global ordering leaked: %s", raw)
	}
	jobs, next, err := f.c.List(alice, 1, cursor)
	if err != nil || len(jobs) != 1 || jobs[0].ID != second.ID || next != "" {
		t.Fatalf("second page: %+v %q %v", jobs, next, err)
	}
	for _, actor := range []auth.Actor{bob, admin} {
		if _, _, err := f.c.List(actor, 1, cursor); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("cross-scope replay: %v", err)
		}
	}
	old := base64.RawURLEncoding.EncodeToString([]byte(`{"a":1,"m":2}`))
	if _, _, err := f.c.List(alice, 1, old); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("legacy cursor survived activation: %v", err)
	}
}

func TestControlRefreshesActorAfterDemotion(t *testing.T) {
	f := newScopedControl(t)
	actor := f.actors["alice"]
	task := backend.Task{Argv: []string{"work"}}
	if _, err := f.c.Submit(actor, "before", task); err != nil {
		t.Fatal(err)
	}
	if err := auth.SetRole(f.config, "alice", auth.Reader); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		fresh, err := f.e.Check(actor, auth.Read)
		if err == nil && fresh.Role() == auth.Reader {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("role refresh did not arrive")
		case <-tick.C:
		}
	}
	if _, err := f.c.Submit(actor, "after", task); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("stale actor retained mutation: %v", err)
	}
	if _, err := f.c.Targets(actor); err != nil {
		t.Fatalf("reader targets: %v", err)
	}
}

func TestAuditFailureDoesNotChangeAdmission(t *testing.T) {
	f := newScopedControl(t)
	actor := f.actors["alice"]
	if err := f.c.RecordAccess(actor, "invalid-category", "", "allowed"); err == nil {
		t.Fatal("invalid category accepted")
	}
	if f.c.AuditHealthy() || !f.c.Healthy() {
		t.Fatal("audit fault changed wrong health")
	}
	if _, _, err := f.c.Plan(actor, backend.Task{Argv: []string{"work"}}); err != nil {
		t.Fatalf("audit fault changed admission: %v", err)
	}
	if err := f.c.RecordAccess(actor, "plan", "", "allowed"); err != nil {
		t.Fatal(err)
	}
	if f.c.AuditHealthy() {
		t.Fatal("enqueue cleared a fault before persistence")
	}
}

func TestZeroActorDeniedAtEveryControlEntrypoint(t *testing.T) {
	f := newScopedControl(t)
	zero := auth.Actor{}
	task := backend.Task{Argv: []string{"work"}}
	calls := map[string]func() error{
		"targets": func() error { _, err := f.c.Targets(zero); return err },
		"plan":    func() error { _, _, err := f.c.Plan(zero, task); return err },
		"submit":  func() error { _, err := f.c.Submit(zero, "zero", task); return err },
		"get":     func() error { _, err := f.c.Get(zero, "00000000000000000000000000000000"); return err },
		"list":    func() error { _, _, err := f.c.List(zero, 1, ""); return err },
		"cancel":  func() error { _, err := f.c.Cancel(zero, "00000000000000000000000000000000"); return err },
		"logs": func() error {
			_, err := f.c.Logs(context.Background(), zero, "00000000000000000000000000000000", 0)
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, auth.ErrUnauthenticated) {
				t.Fatalf("zero actor: %v", err)
			}
		})
	}
}
