package store

import (
	"errors"
	bolt "go.etcd.io/bbolt"
)

var workerBarrierStorageFormatKey = []byte("worker_barrier_storage_format")

const workerBarrierStorageFormat = "skygo-worker-barrier-store-v1"

// The tag fences optional versioned rows without introducing another bucket.
func checkWorkerBarrierFormat(tx *bolt.Tx, n uint64) error {
	m := tx.Bucket(metadataBucket)
	present := m != nil && hasKey(m, workerBarrierStorageFormatKey)
	if n < workerBarrierSchema {
		if present {
			return foreignFormat("worker barrier before schema 15")
		}
		return nil
	}
	switch {
	case !present:
		return foreignFormat("missing worker barrier storage format tag")
	case m.Bucket(workerBarrierStorageFormatKey) != nil:
		return foreignFormat("worker barrier storage format tag is a bucket")
	case string(m.Get(workerBarrierStorageFormatKey)) != workerBarrierStorageFormat:
		return foreignFormat("unknown worker barrier storage format tag")
	}
	return nil
}

// Migration changes exactly the marker and tag; old rows and charges stay put.
func migrateWorkerBarrier(tx *bolt.Tx) error {
	if schemaNumber(tx) != artifactBeginRequestSchema {
		return errors.New("worker barrier requires schema 14")
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	if err := m.Put(workerBarrierStorageFormatKey, []byte(workerBarrierStorageFormat)); err != nil {
		return err
	}
	return m.Put(schemaKey, sequenceKey(workerBarrierSchema))
}
