package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

type scopeFixture struct {
	s      *Store
	engine *auth.Engine
	actors map[string]auth.Actor
}

func newScopeFixture(t *testing.T) scopeFixture {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	config, initial := filepath.Join(dir, "auth.json"), filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(config, initial, s.Owner()); err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{"legacy": initial}
	for id, role := range map[string]auth.Role{"alice": auth.Operator, "bob": auth.Operator, "reader": auth.Reader} {
		if err := auth.AddPrincipal(config, id, role); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, id+".token")
		if _, err := auth.Issue(config, id, path, time.Hour); err != nil {
			t.Fatal(err)
		}
		tokens[id] = path
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
	for id, path := range tokens {
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
	return scopeFixture{s, e, actors}
}

func TestScopedNamespacesAndDenseIndex(t *testing.T) {
	f := newScopeFixture(t)
	a, b, admin := f.actors["alice"], f.actors["bob"], f.actors["legacy"]
	task := backend.Task{Argv: []string{"a"}}
	first, err := f.s.Submit(a, "same", task, "local")
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.s.Submit(b, "same", backend.Task{Argv: []string{"b"}}, "local")
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.s.Submit(a, "second", task, "local")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == other.ID {
		t.Fatal("request crossed principal namespace")
	}
	again, err := f.s.Submit(a, "same", task, "new-target")
	if err != nil || again.ID != first.ID || again.Target != "local" {
		t.Fatalf("retry: %+v %v", again, err)
	}
	if _, err := f.s.Submit(a, "same", backend.Task{Argv: []string{"different"}}, "local"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	n, err := f.s.LastSequenceFor(a)
	if err != nil || n != 2 {
		t.Fatalf("own highwater: %d %v", n, err)
	}
	own, err := f.s.ListFor(a, 0, n, 10)
	if err != nil || len(own) != 2 || own[0].ID != first.ID || own[1].ID != second.ID {
		t.Fatalf("own list: %+v %v", own, err)
	}
	all, err := f.s.ListFor(admin, 0, 3, 10)
	if err != nil || len(all) != 3 {
		t.Fatalf("admin list: %+v %v", all, err)
	}
	if _, err := f.s.GetFor(a, other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign: %v", err)
	}
	if _, err := f.s.GetFor(a, "ffffffffffffffffffffffffffffffff"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	called := false
	if _, err := f.s.UpdateFor(a, other.ID, func(j *backend.Job) error { called = true; j.CancelRequested = true; return nil }); !errors.Is(err, ErrNotFound) || called {
		t.Fatalf("foreign mutation called=%v: %v", called, err)
	}
	if _, err := f.s.UpdateFor(f.actors["reader"], first.ID, func(j *backend.Job) error { called = true; return nil }); !errors.Is(err, auth.ErrPermissionDenied) || called {
		t.Fatalf("reader mutation: %v", err)
	}
	if _, err := f.s.Submit(auth.Actor{}, "zero", task, "local"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("zero actor: %v", err)
	}
	adminJob, err := f.s.Submit(admin, "same", task, "local")
	if err != nil || adminJob.ID == first.ID {
		t.Fatalf("admin namespace: %+v %v", adminJob, err)
	}
}

func TestScopedActivationPreservesLegacyAndSnapshots(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	original, err := s.Submit(testActor(), "legacy-request", backend.Task{Argv: []string{"old"}}, "local")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Submit(testActor(), "second-legacy", backend.Task{Argv: []string{"second"}}, "local")
	if err != nil {
		t.Fatal(err)
	}
	var before []byte
	s.db.View(func(tx *bolt.Tx) error {
		before = append(before, tx.Bucket(jobsBucket).Get([]byte(original.ID))...)
		return nil
	})
	if s.Scoped() {
		t.Fatal("Open activated scoped mode")
	}
	owner := s.Owner()
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if !s.Scoped() {
		t.Fatal("activation missing")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if !bytes.Equal(before, tx.Bucket(jobsBucket).Get([]byte(original.ID))) {
			t.Error("activation rewrote raw row")
		}
		if string(tx.Bucket(jobPrincipalsBucket).Get([]byte(original.ID))) != "legacy" {
			t.Error("historical owner guessed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	snapshot := filepath.Join(base, "backup.db")
	if err := Backup(dir, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(base, "restored")
	if err := Restore(snapshot, restored); err != nil {
		t.Fatal(err)
	}
	s, err = Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.Scoped() || s.Owner() != owner {
		t.Fatal("restore lost mode or installation")
	}
	restoredSecond, err := s.Submit(testActor(), "second-legacy", second.Task, "replacement")
	if err != nil || restoredSecond.ID != second.ID || restoredSecond.Sequence != 2 {
		t.Fatalf("second legacy binding changed: %+v %v", restoredSecond, err)
	}
	again, err := s.Submit(testActor(), "legacy-request", original.Task, "replacement")
	if err != nil || again.ID != original.ID {
		t.Fatalf("restored binding: %+v %v", again, err)
	}
	entries, err := os.ReadDir(restored)
	if err != nil || len(entries) != 1 || entries[0].Name() != "jobs.db" {
		t.Fatalf("snapshot bundled credential: %+v %v", entries, err)
	}
}

func TestScopedIntegrityRefusesWithoutWrites(t *testing.T) {
	for _, kind := range []string{"owner", "request", "index", "mode", "audit"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			j, err := s.Submit(testActor(), "old", backend.Task{Argv: []string{"work"}}, "local")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.ActivateScoped(); err != nil {
				t.Fatal(err)
			}
			err = s.db.Update(func(tx *bolt.Tx) error {
				switch kind {
				case "owner":
					return tx.Bucket(jobPrincipalsBucket).Delete([]byte(j.ID))
				case "request":
					return tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")).Delete([]byte("old"))
				case "index":
					return tx.Bucket(principalIndexBucket).Bucket([]byte("legacy")).SetSequence(100)
				case "mode":
					return tx.Bucket(metadataBucket).Delete(scopedKey)
				default:
					return tx.DeleteBucket(auditBucket)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			before, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if reopened, err := Open(dir); err == nil {
				reopened.Close()
				t.Fatal("corrupt scoped state accepted")
			}
			after, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if !bytes.Equal(before, after) {
				t.Fatal("refusal changed scoped state")
			}
		})
	}
}

func TestBoundedMetadataAudit(t *testing.T) {
	f := newScopeFixture(t)
	actor := f.actors["alice"]
	// Seed a full valid retained ring in one fixture transaction, then exercise
	// production writes across the pruning boundary without thousands of fsyncs.
	data, err := json.Marshal(auditEntry{Time: time.Now().UTC(), Principal: actor.Principal(), TokenID: actor.TokenID(), Operation: "list", Result: "allowed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(auditBucket)
		for n := uint64(1); n <= maxAuditEntries; n++ {
			if err := b.Put(sequenceKey(n), data); err != nil {
				return err
			}
		}
		return b.SetSequence(maxAuditEntries)
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := f.s.RecordAccess(actor, "list", "", "allowed"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.s.RecordAccess(auth.Actor{}, "authenticate", "", "denied"); err != nil {
		t.Fatal(err)
	}
	if err := f.s.RecordAccess(actor, "secret-token", "", "allowed"); err == nil {
		t.Fatal("arbitrary category accepted")
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(auditBucket)
		if b.Stats().KeyN != maxAuditEntries {
			t.Error("unbounded audit")
		}
		return b.ForEach(func(_, raw []byte) error {
			var e auditEntry
			if err := json.Unmarshal(raw, &e); err != nil {
				return err
			}
			if e.Resource != "" || e.Principal != "" && e.Principal != "alice" {
				t.Error("unexpected audit metadata")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestScopedActivationTransactionRollback(t *testing.T) {
	s := openTest(t)
	accepted, err := s.Submit(testActor(), "old", backend.Task{Argv: []string{"old"}}, "local")
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected activation failure")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := activateScoped(tx); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("activation rollback: %v", err)
	}
	if s.Scoped() {
		t.Fatal("failed activation changed mode")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if isScoped(tx) || tx.Bucket(jobPrincipalsBucket) != nil || tx.Bucket(principalRequestsBucket) != nil || tx.Bucket(principalIndexBucket) != nil || tx.Bucket(auditBucket) != nil {
			t.Error("partial scoped relations committed")
		}
		if !bytes.Equal(tx.Bucket(metadataBucket).Get(schemaKey), sequenceKey(1)) {
			t.Error("failed activation bumped schema")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	again, err := s.Submit(testActor(), "old", accepted.Task, "changed")
	if err != nil || again.ID != accepted.ID {
		t.Fatalf("activation changed binding: %+v %v", again, err)
	}
}

func TestZeroActorDeniedAtEveryScopedStoreEntrypoint(t *testing.T) {
	f := newScopeFixture(t)
	zero := auth.Actor{}
	task := backend.Task{Argv: []string{"work"}}
	calls := map[string]func() error{
		"lookup": func() error { _, _, err := f.s.Lookup(zero, "zero", task); return err },
		"submit": func() error { _, err := f.s.Submit(zero, "zero", task, "local"); return err },
		"get":    func() error { _, err := f.s.GetFor(zero, "missing"); return err },
		"last":   func() error { _, err := f.s.LastSequenceFor(zero); return err },
		"list":   func() error { _, err := f.s.ListFor(zero, 0, 1, 1); return err },
		"update": func() error {
			_, err := f.s.UpdateFor(zero, "missing", func(*backend.Job) error { t.Fatal("zero actor callback"); return nil })
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
