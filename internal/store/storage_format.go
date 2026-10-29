package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// Schema ordinals after the environments bucket of schema9.
const (
	// workerGroupSchema adds the empty worker_groups bucket.
	workerGroupSchema = 10
	// environmentArtifactSchema adds environment artifact pins and
	// incarnations, and the storage format tag.
	environmentArtifactSchema = 11
	// environmentRetirementSchema adds request witnesses, tombstones and
	// reclaimed counts.
	environmentRetirementSchema = 12
	// servingSchema adds the empty serving_services bucket and the serving
	// storage format tag.
	servingSchema = 13
)

// servingStorageFormatKey names the metadata scalar that the schema13
// migration writes in the same transaction as the serving_services bucket and
// marker13. Other storage lines reused ordinal 13 for different contents, so
// at schema13 the ordinal alone does not identify this format: the tag must
// be present with exactly servingStorageFormat, as well as the environment
// tag. Below schema13 it must be absent. A database at 13 without it is never
// tagged after the fact.
var servingStorageFormatKey = []byte("serving_storage_format")

const servingStorageFormat = "skygo-serving-store-v1"

// environmentStorageFormatKey names the metadata scalar that the schema11
// migration writes in the same transaction as marker11. Another storage line
// reused ordinals 11 and 12 for different contents, so from schema11 the
// ordinal alone does not identify this format: the tag must be present with
// exactly environmentStorageFormat. Below schema11 it must be absent. A
// database at 11 or 12 without the tag is never tagged after the fact.
var environmentStorageFormatKey = []byte("environment_storage_format")

const environmentStorageFormat = "skygo-environment-store-v1"

// foreignFamilies are top-level buckets of another storage line. No schema
// of this format has them, even empty.
var foreignFamilies = [][]byte{[]byte("services"), []byte("compat_requests")}

// foreignArtifactTable is an artifact table of another storage line, refused
// whether it is a table or a scalar.
var foreignArtifactTable = []byte("service_pins")

// foreignReadFields are artifact read fields of another storage line. They
// are matched as encoding/json matches field names, ignoring case, so that
// no case variant can reach a decoder.
var foreignReadFields = []string{"Service", "ServiceRevision", "ServicePod"}

// Read rows are bounded as artifactLoad bounds them: at most 4096 rows of
// at most 1024 bytes.
const (
	formatReadRows     = 4096
	formatReadRowBytes = 1024
)

func foreignFormat(what string) error {
	return fmt.Errorf("%w: foreign storage format: %s", ErrUnsupportedSchema, what)
}

// hasKey reports whether b has key, as a scalar or a nested bucket.
// Bucket.Get returns nil for a nested bucket, so it cannot answer this.
func hasKey(b *bolt.Bucket, key []byte) bool {
	k, _ := b.Cursor().Seek(key)
	return k != nil && bytes.Equal(k, key)
}

// checkStorageFormat refuses a database of another storage line at schema n.
// It reads only fixed keys and the bounded artifact read rows, decodes no
// record, and writes nothing. Every Store path that opens, initializes,
// migrates, snapshots, backs up or restores a database reaches it through
// inspect before any other decode or effect.
func checkStorageFormat(tx *bolt.Tx, n uint64) error {
	if err := checkFixedStorageFormat(tx, n); err != nil {
		return err
	}
	return checkReadPurposes(tx, nil)
}

// checkFixedStorageFormat checks the format facts held at fixed keys: the
// foreign families, the storage format tag and the foreign artifact table.
func checkFixedStorageFormat(tx *bolt.Tx, n uint64) error {
	for _, name := range foreignFamilies {
		if tx.Bucket(name) != nil {
			return foreignFormat(string(name) + " bucket")
		}
	}
	if m := tx.Bucket(metadataBucket); m != nil {
		present := hasKey(m, environmentStorageFormatKey)
		switch {
		case n < environmentArtifactSchema:
			if present {
				return foreignFormat("storage format tag before schema 11")
			}
		case !present:
			return foreignFormat("missing storage format tag")
		case m.Bucket(environmentStorageFormatKey) != nil:
			return foreignFormat("storage format tag is a bucket")
		case string(m.Get(environmentStorageFormatKey)) != environmentStorageFormat:
			return foreignFormat("unknown storage format tag")
		}
	} else if n >= environmentArtifactSchema {
		return foreignFormat("missing storage format tag")
	}
	if err := checkServingFormat(tx, n); err != nil {
		return err
	}
	// These are the actual family ordinals: published schema9 already
	// requires environments, while worker_groups first appears at schema10.
	// Ordinal zero is an empty, not-yet-initialized database, so neither
	// family is required there. Presence before the family's ordinal is
	// still refused. Cursor presence also catches a scalar in place of a
	// required bucket; Bucket alone cannot distinguish absence from scalar.
	for _, family := range []struct {
		name  []byte
		label string
		first uint64
	}{
		{environmentsBucket, "environment", 9},
		{workerGroupsBucket, "worker group", workerGroupSchema},
	} {
		k, _ := tx.Cursor().Seek(family.name)
		present := bytes.Equal(k, family.name)
		if n < family.first {
			if present {
				return fmt.Errorf("%s metadata requires schema %d", family.label, family.first)
			}
		} else if tx.Bucket(family.name) == nil {
			return fmt.Errorf("missing %s metadata", family.label)
		}
	}
	if a := tx.Bucket(artifactsBucket); a != nil {
		if hasKey(a, foreignArtifactTable) {
			return foreignFormat("artifacts/service_pins")
		}
		pins := []byte("environment_pins")
		if n < environmentArtifactSchema && hasKey(a, pins) {
			return foreignFormat("environment pins before schema 11")
		}
		if n >= environmentArtifactSchema && a.Bucket(pins) == nil {
			return foreignFormat("missing environment pin table")
		}
		reads := []byte("reads")
		if hasKey(a, reads) && a.Bucket(reads) == nil {
			return artifactCorrupt()
		}
	}
	return nil
}

// checkReadPurposes refuses an artifact read row that has a foreign read
// field, whatever its value. charge, if not nil, is called with each row's
// size before the row is examined; its error is returned unchanged. A row
// past the read bounds, or a nested bucket, is corrupt.
func checkReadPurposes(tx *bolt.Tx, charge func(uint64) error) error {
	reads := artifactReadsTable(tx)
	if reads == nil {
		return nil
	}
	rows := 0
	c := reads.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if charge != nil {
			if err := charge(uint64(len(k)) + uint64(len(v))); err != nil {
				return err
			}
		}
		rows++
		if v == nil || len(v) > formatReadRowBytes || rows > formatReadRows {
			return artifactCorrupt()
		}
		foreign, err := foreignRead(v)
		if foreign {
			return foreignFormat("service read purpose")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// artifactReadsTable returns the artifacts/reads table, or nil if there is
// none. checkFixedStorageFormat refuses a reads scalar first.
func artifactReadsTable(tx *bolt.Tx) *bolt.Bucket {
	a := tx.Bucket(artifactsBucket)
	if a == nil {
		return nil
	}
	return a.Bucket([]byte("reads"))
}

// foreignRead reports whether the JSON object raw has a top-level key that
// names a foreign read field. Malformed JSON cannot establish this format.
// Record validation remains the responsibility of artifactLoad.
func foreignRead(raw []byte) (bool, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	if t, err := d.Token(); err != nil || t != json.Delim('{') {
		return false, artifactCorrupt()
	}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return false, artifactCorrupt()
		}
		key, ok := t.(string)
		if !ok {
			return false, artifactCorrupt()
		}
		for _, f := range foreignReadFields {
			if strings.EqualFold(key, f) {
				return true, nil
			}
		}
		var skip json.RawMessage
		if d.Decode(&skip) != nil {
			return false, artifactCorrupt()
		}
	}
	if t, err := d.Token(); err != nil || t != json.Delim('}') {
		return false, artifactCorrupt()
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return false, artifactCorrupt()
	}
	return false, nil
}
