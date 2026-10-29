package store

import (
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

var artifactBeginRequestsTable = []byte("artifact_begin_requests")
var artifactBeginStorageFormatKey = []byte("artifact_begin_request_storage_format")

const artifactBeginStorageFormat = "skygo-artifact-begin-requests-v1"

// checkArtifactBeginRequestFormat reads only fixed keys. The format tag is mandatory
// at schema14 even while artifacts are disabled; the table is required exactly
// when a schema14 installation has an artifact companion.
func checkArtifactBeginRequestFormat(tx *bolt.Tx, n uint64) error {
	m := tx.Bucket(metadataBucket)
	tagged := m != nil && hasKey(m, artifactBeginStorageFormatKey)
	a := tx.Bucket(artifactsBucket)
	table := a != nil && hasKey(a, artifactBeginRequestsTable)
	if n < artifactBeginRequestSchema {
		if tagged || table {
			return foreignFormat("artifact Begin requests before schema 14")
		}
		return nil
	}
	switch {
	case !tagged:
		return foreignFormat("missing artifact Begin request storage format tag")
	case m.Bucket(artifactBeginStorageFormatKey) != nil:
		return foreignFormat("artifact Begin request storage format tag is a bucket")
	case string(m.Get(artifactBeginStorageFormatKey)) != artifactBeginStorageFormat:
		return foreignFormat("unknown artifact Begin request storage format tag")
	case a != nil && a.Bucket(artifactBeginRequestsTable) == nil:
		return foreignFormat("missing artifact Begin request table")
	}
	return nil
}

// migrateArtifactBeginRequests validates a genuine schema13 installation before the
// caller's transaction adds only an empty request table, its format scalar and
// marker14. It changes neither serving v1 nor the artifact root floor or rows.
func migrateArtifactBeginRequests(tx *bolt.Tx) error {
	if schemaNumber(tx) != servingSchema {
		return errors.New("artifact Begin requests require schema 13")
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if a := tx.Bucket(artifactsBucket); a != nil {
		if _, err := a.CreateBucket(artifactBeginRequestsTable); err != nil {
			return err
		}
	}
	m := tx.Bucket(metadataBucket)
	if err := m.Put(artifactBeginStorageFormatKey, []byte(artifactBeginStorageFormat)); err != nil {
		return err
	}
	return m.Put(schemaKey, sequenceKey(artifactBeginRequestSchema))
}

func artifactBeginTime(t time.Time) bool {
	return !t.IsZero() && t.Location() == time.UTC && t.Year() >= 1 && t.Year() <= 9999
}

// validateArtifactBeginRequests is independent of the current retention epoch:
// a witness must remain valid after epoch retirement or restore changes the
// lineage, until bounded maintenance removes it. Missing uploads are tombstones.
func validateArtifactBeginRequests(s *artifactState) error {
	if len(s.BeginRequests) > 4096 {
		return artifactCorrupt()
	}
	seen := make(map[string]bool, len(s.BeginRequests))
	for id, r := range s.BeginRequests {
		if !artifactHex(id, 32) || r.Ref.ID != id || !artifactRefValid(r.Ref) || !validPrincipal([]byte(r.Owner)) || !validRequestDomain(r.Domain) || !validRetainedRequestID(r.RequestID) || !artifactBeginTime(r.Created) || !artifactBeginTime(r.RetryUntil) || !r.RetryUntil.After(r.Created) {
			return artifactCorrupt()
		}
		// The full64hex request key is only an in-memory uniqueness index.
		// Persisted row keys remain the original32hex upload ID.
		key := string(requestKey(r.Owner, r.Domain, r.RequestID))
		if seen[key] {
			return artifactCorrupt()
		}
		seen[key] = true
		if u, ok := s.Uploads[id]; ok && (u.Owner != r.Owner || u.Ref != r.Ref || !u.Created.Equal(r.Created) || u.RemoteAttemptID != "") {
			return artifactCorrupt()
		}
		if o, ok := s.Objects[id]; ok && (o.Owner != r.Owner || o.Ref != r.Ref || o.Origin != "upload") {
			return artifactCorrupt()
		}
	}
	return nil
}
