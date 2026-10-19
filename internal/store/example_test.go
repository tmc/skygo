package store_test

import (
	"fmt"
	"github.com/tmc/skygo/internal/auth"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

func ExampleStore_BindLegacyLocal() {
	dir, err := os.MkdirTemp("", "skygo-local-migration-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		panic(err)
	}
	defer s.Close()
	// Only actual built-in executor construction permits this migration.
	executor, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		panic(err)
	}
	defer executor.Close()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		panic(err)
	}
	fmt.Println(s.BindLegacyLocal(binding))
	// Output: <nil>
}

// exampleStore gives each runnable example a private, disposable database.
func exampleStore() (*store.Store, func()) {
	dir, err := os.MkdirTemp("", "skygo-store-example-")
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	return s, func() {
		if err := s.Close(); err != nil {
			panic(err)
		}
		os.RemoveAll(dir)
	}
}

func Example() {
	s, close := exampleStore()
	defer close()
	task := backend.Task{Argv: []string{"trainer"}}
	job, err := s.Submit(testActor(), "training-1", task, "local", "test-binding")
	same, found, lookupErr := s.Lookup(testActor(), "training-1", task)
	fmt.Println(job.Status.State, err, found, lookupErr, same.ID == job.ID)
	// Output: queued <nil> true <nil> true
}

func ExampleStore_Get() {
	s, close := exampleStore()
	defer close()
	accepted, err := s.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding")
	if err != nil {
		panic(err)
	}
	job, err := s.Get(accepted.ID)
	fmt.Println(job.Target, job.Task.Argv[0], err)
	// Output: local trainer <nil>
}

func ExampleStore_List() {
	s, close := exampleStore()
	defer close()
	if _, err := s.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding"); err != nil {
		panic(err)
	}
	last, err := s.LastSequence()
	if err != nil {
		panic(err)
	}
	jobs, err := s.List(0, last, 10)
	fmt.Println(len(jobs), err)
	// Output: 1 <nil>
}

func ExampleStore_Update() {
	s, close := exampleStore()
	defer close()
	job, err := s.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding")
	if err != nil {
		panic(err)
	}
	job, err = s.Update(job.ID, func(job *backend.Job) error { job.CancelRequested = true; return nil })
	fmt.Println(job.CancelRequested, err)
	// Output: true <nil>
}

func ExampleStore_Active() {
	s, close := exampleStore()
	defer close()
	jobs, err := s.Active()
	fmt.Println(len(jobs), err)
	// The owner remains stable for the lifetime of this data directory.
	fmt.Println(s.Owner() != "")
	// Output:
	// 0 <nil>
	// true
}

func ExampleBackup() {
	dir, err := os.MkdirTemp("", "skygo-backup-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	source := filepath.Join(dir, "source")
	s, err := store.Open(source)
	if err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	err = store.Backup(source, filepath.Join(dir, "snapshot.db"))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleRestore() {
	dir, err := os.MkdirTemp("", "skygo-restore-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	source := filepath.Join(dir, "source")
	s, err := store.Open(source)
	if err != nil {
		panic(err)
	}
	owner := s.Owner()
	if err := s.Close(); err != nil {
		panic(err)
	}
	snapshot := filepath.Join(dir, "snapshot.db")
	if err := store.Backup(source, snapshot); err != nil {
		panic(err)
	}
	restored := filepath.Join(dir, "restored")
	if err := store.Restore(snapshot, restored); err != nil {
		panic(err)
	}
	s, err = store.Open(restored)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	fmt.Println(s.Owner() == owner)
	// Output: true
}

func exampleScopedStore() (*store.Store, auth.Actor, func()) {
	s, closeStore := exampleStore()
	dir, err := os.MkdirTemp("", "skygo-auth-example-")
	if err != nil {
		panic(err)
	}
	config, token := filepath.Join(dir, "auth.json"), filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(config, token, s.Owner()); err != nil {
		panic(err)
	}
	if err := s.ActivateScoped(); err != nil {
		panic(err)
	}
	e, err := auth.NewFile(config, s.Owner())
	if err != nil {
		panic(err)
	}
	raw, err := os.ReadFile(token)
	if err != nil {
		panic(err)
	}
	actor, err := e.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		panic(err)
	}
	return s, actor, func() { closeStore(); os.RemoveAll(dir) }
}

func ExampleStore_ActivateScoped() {
	s, _, close := exampleScopedStore()
	defer close()
	fmt.Println(s.Scoped())
	// Output: true
}

func ExampleStore_GetFor() {
	s, actor, close := exampleScopedStore()
	defer close()
	accepted, err := s.Submit(actor, "training", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding")
	if err != nil {
		panic(err)
	}
	job, err := s.GetFor(actor, accepted.ID)
	fmt.Println(job.Task.Argv[0], err)
	// Output: trainer <nil>
}

func ExampleStore_LastSequenceFor() {
	s, actor, close := exampleScopedStore()
	defer close()
	n, err := s.LastSequenceFor(actor)
	fmt.Println(n, err)
	// Output: 0 <nil>
}

func ExampleStore_ListFor() {
	s, actor, close := exampleScopedStore()
	defer close()
	jobs, err := s.ListFor(actor, 0, 10, 10)
	fmt.Println(len(jobs), err)
	// Output: 0 <nil>
}

func ExampleStore_UpdateFor() {
	s, actor, close := exampleScopedStore()
	defer close()
	accepted, err := s.Submit(actor, "training", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding")
	if err != nil {
		panic(err)
	}
	job, err := s.UpdateFor(actor, accepted.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
	fmt.Println(job.CancelRequested, err)
	// Output: true <nil>
}

func ExampleStore_RecordAccess() {
	s, actor, close := exampleScopedStore()
	defer close()
	fmt.Println(s.RecordAccess(actor, "list", "", "allowed"))
	// Output: <nil>
}

func ExampleStore_Scoped() {
	s, close := exampleStore()
	defer close()
	fmt.Println(s.Scoped())
	// Output: false
}

func ExampleStore_ValidateAccess() {
	s, close := exampleStore()
	defer close()
	fmt.Println(s.ValidateAccess(testActor(), "list", "", "allowed"))
	// Output: <nil>
}
