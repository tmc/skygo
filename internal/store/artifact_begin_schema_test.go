package store

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// artifactBeginStripModel projects an empty current request family to the
// predecessor layout. It is only a MODEL helper, never a genuine13 writer.
func artifactBeginStripModel(tx *bolt.Tx) error {
	if err := workerBarrierStripModel(tx); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	if hasKey(m, artifactBeginStorageFormatKey) && (m.Bucket(artifactBeginStorageFormatKey) != nil || string(m.Get(artifactBeginStorageFormatKey)) != artifactBeginStorageFormat) {
		return errors.New("model cannot strip unknown artifact Begin format")
	}
	if a := tx.Bucket(artifactsBucket); a != nil {
		if b := a.Bucket(artifactBeginRequestsTable); b != nil {
			if k, _ := b.Cursor().First(); k != nil || b.Sequence() != 0 {
				return errors.New("model cannot strip populated artifact Begin requests")
			}
			if err := a.DeleteBucket(artifactBeginRequestsTable); err != nil {
				return err
			}
		}
	}
	return m.Delete(artifactBeginStorageFormatKey)
}

func TestArtifactBeginSchemaLayout(t *testing.T) {
	for _, companion := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "companion"}[companion], func(t *testing.T) {
			s := openTest(t)
			if companion {
				artifactTestInstall(t, s)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if schemaNumber(tx) != schemaVersion || string(tx.Bucket(metadataBucket).Get(artifactBeginStorageFormatKey)) != artifactBeginStorageFormat {
					t.Fatal("new installation lacks current marker/artifact14 format")
				}
				if err := checkStorageFormat(tx, schemaVersion); err != nil {
					return err
				}
				if companion {
					a, err := artifactLoad(tx)
					if err != nil {
						return err
					}
					if a.Root.SchemaFloor != artifactFormat || len(a.BeginRequests) != 0 || tx.Bucket(artifactsBucket).Bucket(artifactBeginRequestsTable) == nil {
						t.Fatal("wrong artifact root/family")
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, tc := range []struct {
		schema uint64
		count  int
		floor  uint64
	}{
		{6, 12, 6}, {7, 13, 7}, {8, 14, 8}, {9, 14, 8}, {10, 14, 8},
		{11, 15, 8}, {12, 15, 8}, {13, 15, 8}, {14, 16, 8},
	} {
		tables, floor, err := artifactTablesFor(tc.schema)
		if err != nil || len(tables) != tc.count || floor != tc.floor {
			t.Fatalf("schema%d tables=%d floor=%d err=%v", tc.schema, len(tables), floor, err)
		}
	}
}

func TestArtifactBeginSchemaFormatRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*bolt.Tx) error
	}{
		{"missing tag", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(artifactBeginStorageFormatKey) }},
		{"wrong tag", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(artifactBeginStorageFormatKey, []byte("skygo-artifact-begin-requests-v2"))
		}},
		{"missing table", func(tx *bolt.Tx) error { return tx.Bucket(artifactsBucket).DeleteBucket(artifactBeginRequestsTable) }},
		{"scalar table", func(tx *bolt.Tx) error {
			a := tx.Bucket(artifactsBucket)
			if err := a.DeleteBucket(artifactBeginRequestsTable); err != nil {
				return err
			}
			return a.Put(artifactBeginRequestsTable, []byte("not a table"))
		}},
		{"premature tag", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(servingSchema)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTest(t)
			artifactTestInstall(t, s)
			if err := s.db.Update(tc.edit); err != nil {
				t.Fatal(err)
			}
			before := artifactDatabaseBytes(t, s)
			if err := s.db.View(func(tx *bolt.Tx) error { return inspect(tx) }); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("format admitted: %v", err)
			}
			if after := artifactDatabaseBytes(t, s); !bytes.Equal(after, before) {
				t.Fatal("refusal changed database")
			}
		})
	}
}

func TestArtifactBeginRequestRows(t *testing.T) {
	now := time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC)
	id := strings.Repeat("a", 32)
	row := artifactBeginRequest{Owner: "alice", Domain: RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}, RequestID: "upload-one", Ref: ArtifactRef{ID: id, SHA256: strings.Repeat("c", 64), Size: 1024}, Created: now, RetryUntil: now.Add(time.Hour)}
	for _, tc := range []struct {
		name string
		edit func(*artifactBeginRequest)
	}{
		{"valid retired memo", func(*artifactBeginRequest) {}},
		{"wrong Ref ID", func(r *artifactBeginRequest) { r.Ref.ID = strings.Repeat("d", 32) }},
		{"bad owner", func(r *artifactBeginRequest) { r.Owner = "alice\x00" }},
		{"bad domain", func(r *artifactBeginRequest) { r.Domain.Epoch = 0 }},
		{"bad request", func(r *artifactBeginRequest) { r.RequestID = "bad request" }},
		{"bad digest", func(r *artifactBeginRequest) { r.Ref.SHA256 = "bad" }},
		{"bad size", func(r *artifactBeginRequest) { r.Ref.Size = 0 }},
		{"bad time", func(r *artifactBeginRequest) { r.RetryUntil = r.Created }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := row
			tc.edit(&r)
			s := &artifactState{BeginRequests: map[string]artifactBeginRequest{id: r}}
			err := validateArtifactBeginRequests(s)
			if (err == nil) != (tc.name == "valid retired memo") {
				t.Fatalf("row error=%v", err)
			}
		})
	}
	s := &artifactState{BeginRequests: map[string]artifactBeginRequest{id: row}, Uploads: map[string]artifactUploadRecord{id: {Owner: row.Owner, Ref: row.Ref, Created: row.Created}}}
	if err := validateArtifactBeginRequests(s); err != nil {
		t.Fatal(err)
	}
	u := s.Uploads[id]
	u.Owner = "bob"
	s.Uploads[id] = u
	if err := validateArtifactBeginRequests(s); err == nil {
		t.Fatal("foreign upload matches witness")
	}
	clone := s.clone()
	r := clone.BeginRequests[id]
	r.RequestID = "changed"
	clone.BeginRequests[id] = r
	if s.BeginRequests[id].RequestID != "upload-one" {
		t.Fatal("clone aliases request map")
	}

	// Distinct upload IDs may not encode the same scoped request twice.
	r2 := row
	r2.Ref.ID = strings.Repeat("d", 32)
	s = &artifactState{BeginRequests: map[string]artifactBeginRequest{id: row, r2.Ref.ID: r2}}
	if err := validateArtifactBeginRequests(s); err == nil {
		t.Fatal("duplicate scoped request admitted")
	}
}

func TestArtifactBeginRequestCanonicalEncoding(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	rollback := errors.New("rollback modeled invalid encoding")
	err := s.db.Update(func(tx *bolt.Tx) error {
		row := artifactBeginRequest{Owner: "alice", Domain: RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}, RequestID: "request", Ref: ArtifactRef{ID: strings.Repeat("c", 32), SHA256: strings.Repeat("d", 64), Size: 1024}, Created: s.now(), RetryUntil: s.now().Add(time.Hour)}
		if _, err := artifactChange(tx, 1<<20, func(a *artifactState) error {
			a.BeginRequests[row.Ref.ID] = row
			return nil
		}); err != nil {
			return err
		}
		a, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		rows, usage, owners, err := a.encode()
		if err != nil {
			return err
		}
		encoded := rows[string(artifactBeginRequestsTable)][row.Ref.ID]
		var decoded artifactBeginRequest
		if err := artifactDecode(encoded, &decoded); err != nil {
			return err
		}
		if decoded != row || len(encoded) > 1024 || owners[row.Owner][aMetadataBytes] != 1024+32+80+uint64(len(row.Owner)) || owners[row.Owner][aMetadataRecords] != 2 {
			t.Fatal("wrong row charge/encoding")
		}
		if usage[aMetadataBytes] < owners[row.Owner][aMetadataBytes] {
			t.Fatal("owner charge omitted globally")
		}
		var duplicate bytes.Buffer
		duplicate.Write(encoded[:len(encoded)-1])
		duplicate.WriteString(`,"RequestID":"request"}`)
		if err := tx.Bucket(artifactsBucket).Bucket(artifactBeginRequestsTable).Put([]byte(row.Ref.ID), duplicate.Bytes()); err != nil {
			return err
		}
		// All counters were committed through actual artifactChange first.
		// Only alternate JSON bytes differ, so refusal tests canonical encoding.
		if _, err := artifactLoad(tx); err == nil {
			t.Fatal("duplicate JSON accepted")
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("modeled transaction = %v, want rollback after actual canonical check", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error { _, err := artifactLoad(tx); return err }); err != nil {
		t.Fatal(err)
	}
}
