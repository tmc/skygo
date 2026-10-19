package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSubmitConcurrent(t *testing.T) {
	s := openTest(t)
	task := backend.Task{Argv: []string{"work"}}
	const count = 32
	jobs := make(chan backend.Job, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); j, e := s.Submit(testActor(), "same", task, "local"); jobs <- j; errs <- e }()
	}
	wg.Wait()
	close(jobs)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var id string
	for j := range jobs {
		if id == "" {
			id = j.ID
		}
		if j.ID != id || j.Sequence != 1 {
			t.Fatalf("duplicate job: %+v", j)
		}
	}
	n, err := s.LastSequence()
	if err != nil || n != 1 {
		t.Fatalf("last sequence %d, %v", n, err)
	}
	task.Argv[0] = "changed"
	if _, err := s.Submit(testActor(), "same", task, "local"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	j, err := s.Get(id)
	if err != nil || j.Task.Argv[0] != "work" {
		t.Fatalf("mutated durable task: %+v, %v", j, err)
	}
}

func TestReopenAndWriterLock(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}}
	j, err := s.Submit(testActor(), "request", task, "original")
	if err != nil {
		t.Fatal(err)
	}
	owner := s.Owner()
	if other, e := Open(dir); e == nil {
		other.Close()
		t.Fatal("second writer accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Owner() != owner || len(owner) != 32 {
		t.Fatalf("owner changed: %q", s.Owner())
	}
	task.Env = map[string]string{}
	again, found, err := s.Lookup(testActor(), "request", task)
	if err != nil || !found || again.ID != j.ID {
		t.Fatalf("lookup after reopen: %+v %v %v", again, found, err)
	}
	again, err = s.Submit(testActor(), "request", task, "new-target")
	if err != nil || again.Target != "original" {
		t.Fatalf("retry changed target: %+v %v", again, err)
	}
}

func TestSnapshotAndUpdate(t *testing.T) {
	s := openTest(t)
	task := backend.Task{Argv: []string{"work"}, Env: map[string]string{"K": "V"}}
	var ids []string
	for i := 0; i < 3; i++ {
		j, e := s.Submit(testActor(), fmt.Sprint(i), task, "local")
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, j.ID)
	}
	first, err := s.List(0, 2, 1)
	if err != nil || len(first) != 1 || first[0].Sequence != 1 {
		t.Fatalf("first page: %+v %v", first, err)
	}
	second, err := s.List(1, 2, 2)
	if err != nil || len(second) != 1 || second[0].Sequence != 2 {
		t.Fatalf("snapshot page: %+v %v", second, err)
	}
	if _, err := s.Update(ids[0], func(j *backend.Job) error { j.Task.Env["K"] = "changed"; return nil }); err == nil {
		t.Fatal("immutable mutation accepted")
	}
	j, err := s.Get(ids[0])
	if err != nil || j.Task.Env["K"] != "V" {
		t.Fatalf("rollback: %+v %v", j, err)
	}
	wantErr := errors.New("callback failure")
	if _, err := s.Update(ids[0], func(j *backend.Job) error { j.CancelRequested = true; return wantErr }); !errors.Is(err, wantErr) {
		t.Fatal(err)
	}
	j, _ = s.Get(ids[0])
	if j.CancelRequested {
		t.Fatal("failed callback persisted")
	}
	if _, err := s.Update(ids[0], func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	active, err := s.Active()
	if err != nil || len(active) != 2 || active[0].ID != ids[1] {
		t.Fatalf("active: %+v %v", active, err)
	}
	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestFullRetainsDedup(t *testing.T) {
	s := openTest(t)
	task := backend.Task{Argv: []string{"work"}}
	first, err := s.Submit(testActor(), "first", task, "local")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(sequenceBucket).SetSequence(maxJobs) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(testActor(), "new", task, "local"); !errors.Is(err, ErrFull) {
		t.Fatalf("full: %v", err)
	}
	again, err := s.Submit(testActor(), "first", task, "local")
	if err != nil || again.ID != first.ID {
		t.Fatalf("retry at limit: %+v %v", again, err)
	}
}

func TestTaskPayloadLimitAndMigration(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}}
	first, err := s.Submit(testActor(), "first", task, "local")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(maxTaskBytes)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(testActor(), "new", task, "local"); !errors.Is(err, ErrFull) {
		t.Fatalf("payload cap: %v", err)
	}
	again, err := s.Submit(testActor(), "first", task, "local")
	if err != nil || again.ID != first.ID {
		t.Fatalf("retry at byte cap: %+v %v", again, err)
	}
	// Simulate an existing store created before aggregate accounting existed.
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(taskBytesKey) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	encoded, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		n := binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey))
		if n != uint64(len(encoded)) {
			t.Fatalf("migrated bytes=%d want %d", n, len(encoded))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(testActor(), "new", task, "local"); err != nil {
		t.Fatalf("admission after migration: %v", err)
	}
}
