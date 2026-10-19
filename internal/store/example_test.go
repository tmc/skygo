package store_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

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
	job, err := s.Submit("training-1", task, "local")
	same, found, lookupErr := s.Lookup("training-1", task)
	fmt.Println(job.Status.State, err, found, lookupErr, same.ID == job.ID)
	// Output: queued <nil> true <nil> true
}

func ExampleStore_Get() {
	s, close := exampleStore()
	defer close()
	accepted, err := s.Submit("training-1", backend.Task{Argv: []string{"trainer"}}, "local")
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
	if _, err := s.Submit("training-1", backend.Task{Argv: []string{"trainer"}}, "local"); err != nil {
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
	job, err := s.Submit("training-1", backend.Task{Argv: []string{"trainer"}}, "local")
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
