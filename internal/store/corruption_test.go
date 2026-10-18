package store

import (
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestMalformedTaskByteCounter(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"short", make([]byte, 7)},
		{"long", make([]byte, 9)},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(metadataBucket).Put(taskBytesKey, test.data)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Submit("new", backend.Task{Argv: []string{"work"}}, "local"); err == nil || !strings.Contains(err.Error(), "invalid task byte counter") {
				t.Fatalf("Submit with corrupt counter: %v", err)
			}
			if n, err := s.LastSequence(); err != nil || n != 0 {
				t.Fatalf("failed submission changed sequence: %d, %v", n, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			other, err := Open(dir)
			if err == nil {
				other.Close()
				t.Fatal("Open accepted corrupt counter")
			}
			if !strings.Contains(err.Error(), "invalid task byte counter") {
				t.Fatalf("Open with corrupt counter: %v", err)
			}
		})
	}
}

func TestMalformedSequenceKey(t *testing.T) {
	for _, test := range []struct {
		name string
		key  []byte
	}{
		{"short", []byte{1}},
		{"long", append(sequenceKey(1), 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			j, err := s.Submit("one", backend.Task{Argv: []string{"work"}}, "local")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(sequenceBucket)
				if err := b.Delete(sequenceKey(j.Sequence)); err != nil {
					return err
				}
				return b.Put(test.key, []byte(j.ID))
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.List(0, j.Sequence, 10); err == nil || !strings.Contains(err.Error(), "invalid job sequence key") {
				t.Fatalf("List with corrupt key: %v", err)
			}
		})
	}
}
