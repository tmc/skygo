package store

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// workerBarrierStripModel makes an explicitly MODELED predecessor only when
// every worker row is legacy. It never discards a versioned row or sequence.
func workerBarrierStripModel(tx *bolt.Tx) error {
	if b := tx.Bucket(workerGroupsBucket); b != nil {
		if err := b.ForEach(func(k, v []byte) error {
			switch {
			case bytes.HasPrefix(k, []byte("g/")):
				g, err := decodeWorkerGroupRecord(v)
				if err != nil {
					return err
				}
				if g.Barrier != nil {
					return errors.New("model cannot strip populated worker barrier")
				}
			case bytes.HasPrefix(k, []byte("m/")):
				m, err := decodeWorkerMemberRecord(v)
				if err != nil {
					return err
				}
				if m.Barrier != nil {
					return errors.New("model cannot strip populated worker barrier")
				}
			default:
				return errors.New("model cannot strip unknown worker row")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	m := tx.Bucket(metadataBucket)
	if hasKey(m, workerBarrierStorageFormatKey) && (m.Bucket(workerBarrierStorageFormatKey) != nil || string(m.Get(workerBarrierStorageFormatKey)) != workerBarrierStorageFormat) {
		return errors.New("model cannot strip unknown worker barrier format")
	}
	return m.Delete(workerBarrierStorageFormatKey)
}

func TestWorkerBarrierSchemaInstalled(t *testing.T) {
	for _, companion := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "companion"}[companion], func(t *testing.T) {
			s := openTest(t)
			if companion {
				artifactTestInstall(t, s)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if schemaNumber(tx) != workerBarrierSchema {
					t.Fatalf("schema=%d", schemaNumber(tx))
				}
				if string(tx.Bucket(metadataBucket).Get(workerBarrierStorageFormatKey)) != workerBarrierStorageFormat {
					t.Fatal("worker barrier tag missing")
				}
				if err := checkStorageFormat(tx, workerBarrierSchema); err != nil {
					return err
				}
				if companion {
					_, floor, err := artifactTablesFor(workerBarrierSchema)
					if err != nil {
						return err
					}
					if floor != artifactFormat {
						t.Fatalf("artifact floor=%d", floor)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkerBarrierMigrationModel(t *testing.T) {
	f := newWorkerFixture(t)
	// The fixture is deliberately a MODEL: legacy rows are authored by current
	// APIs, then only the new empty tag and marker are projected to 14.
	r := f.request("alice", "legacy-model", 2)
	if _, err := f.s.SubmitWorkerGroup(f.actors["alice"], r); err != nil {
		t.Fatal(err)
	}
	dir := f.dir
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := workerBarrierStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(artifactBeginRequestSchema))
	})
	before := fixtureState(t, dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	after := workflowDatabase(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	marker := string(metadataBucket) + "/" + string(schemaKey)
	tag := string(metadataBucket) + "/" + string(workerBarrierStorageFormatKey)
	if before[marker] != string(sequenceKey(14)) || after[marker] != string(sequenceKey(15)) || after[tag] != workerBarrierStorageFormat {
		t.Fatal("wrong migration facts")
	}
	delete(before, marker)
	delete(after, marker)
	delete(after, tag)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("migration changed legacy rows, sequences, authority or accounting")
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerBarrierFormatRefusals(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*bolt.Tx) error
	}{
		{"missing tag", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(workerBarrierStorageFormatKey) }},
		{"unknown tag", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(workerBarrierStorageFormatKey, []byte("unknown"))
		}},
		{"tag bucket", func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(workerBarrierStorageFormatKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(workerBarrierStorageFormatKey)
			return err
		}},
		{"early tag", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(14)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			editFixture(t, dir, false, tt.edit)
			before := fixtureState(t, dir)
			s, err = Open(dir)
			if s != nil {
				s.Close()
				t.Fatal("opened invalid format")
			}
			if !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(before, fixtureState(t, dir)) {
				t.Fatal("refusal wrote state")
			}
		})
	}
}
