package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func reusableStore(t *testing.T) (*Store, func() error) {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	closeStore := sync.OnceValue(s.Close)
	t.Cleanup(func() {
		if err := closeStore(); err != nil {
			t.Error(err)
		}
	})
	return s, closeStore
}

func TestReusableBytesAfterClose(t *testing.T) {
	s, closeStore := reusableStore(t)
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if got, err := s.ReusableBytes(); !errors.Is(err, bolt.ErrDatabaseNotOpen) || got != 0 {
			t.Fatalf("closed database: got %d, %v", got, err)
		}
	}
}

func TestReusableBytesReadOnly(t *testing.T) {
	s, _ := reusableStore(t)
	name := filepath.Join(s.Directory(), "jobs.db")
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var want uint64
	if err = s.db.View(func(*bolt.Tx) error { want = uint64(s.db.Stats().FreePageN) * uint64(s.db.Info().PageSize); return nil }); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		got, err := s.ReusableBytes()
		if err != nil || got != want {
			t.Fatalf("got %d, %v; want %d", got, err, want)
		}
	}
	after, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("sampling changed database bytes, schema or usage")
	}
}

func TestReusableBytesConcurrentClose(t *testing.T) {
	const readers = 8
	for range 32 {
		s, closeStore := reusableStore(t)
		if _, err := s.ReusableBytes(); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, readers)
		var workers sync.WaitGroup
		for range readers {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for range 64 {
					n, err := s.ReusableBytes()
					if errors.Is(err, bolt.ErrDatabaseNotOpen) {
						if n != 0 {
							results <- errors.New("closed database returned usable bytes")
							return
						}
						results <- nil
						return
					}
					if err != nil {
						results <- err
						return
					}
				}
				results <- nil
			}()
		}
		close(start)
		closeErr := closeStore()
		workers.Wait() // Join every original sampling goroutine before assertions.
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		for range readers {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.ReusableBytes(); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
			t.Fatal("post-join sampling did not report closed database", err)
		}
	}
}
