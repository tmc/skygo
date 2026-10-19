package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/auth"
	bolt "go.etcd.io/bbolt"
)

var auditBucket = []byte("access_audit")

const maxAuditEntries = 4096

type auditEntry struct {
	Time      time.Time `json:"time"`
	Principal string    `json:"principal,omitempty"`
	TokenID   string    `json:"token_id,omitempty"`
	Operation string    `json:"operation"`
	Resource  string    `json:"resource,omitempty"`
	Result    string    `json:"result"`
}

func validAccess(operation, resource, result string) bool {
	switch operation {
	case "authenticate", "info", "targets", "plan", "submit", "get", "list", "cancel", "logs", "prune":
	default:
		return false
	}
	switch result {
	case "allowed", "denied", "error", "started", "ended":
	default:
		return false
	}
	if resource != "" {
		if len(resource) != 32 {
			return false
		}
		for _, r := range resource {
			if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
				return false
			}
		}
		if _, err := hex.DecodeString(resource); err != nil {
			return false
		}
	}
	return true
}

// ValidateAccess validates only diagnostic metadata and basic opaque actor
// shape. It performs no I/O and does not grant access or revalidate credentials.
func (s *Store) ValidateAccess(actor auth.Actor, operation, resource, result string) error {
	if !validAccess(operation, resource, result) {
		return errors.New("invalid audit category")
	}
	if actor.Principal() == "" {
		if operation != "authenticate" || result != "denied" || resource != "" {
			return auth.ErrUnauthenticated
		}
	} else if err := s.authorize(actor, auth.Read); err != nil {
		return err
	}
	return nil
}

// RecordAccess records bounded, metadata-only access diagnostics in scoped
// mode. Time records the persistence attempt, not an access-occurrence time.
// resource is empty unless the caller resolved an authorized job ID.
// A zero actor is allowed only for a generic authenticate/denied event.
func (s *Store) RecordAccess(actor auth.Actor, operation, resource, result string) error {
	if err := s.ValidateAccess(actor, operation, resource, result); err != nil {
		return err
	}
	if !s.scoped {
		return nil
	}
	data, err := json.Marshal(auditEntry{Time: time.Now().UTC(), Principal: actor.Principal(), TokenID: actor.TokenID(), Operation: operation, Resource: resource, Result: result})
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(auditBucket)
		if b == nil {
			return errors.New("missing access audit bucket")
		}
		n, err := b.NextSequence()
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("audit sequence exhausted")
		}
		var removed uint64
		if n > maxAuditEntries {
			key := sequenceKey(n - maxAuditEntries)
			if old := b.Get(key); old != nil {
				removed = uint64(8 + len(old))
			}
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		if retentionRoot(tx) != nil {
			if err := adjust(tx, MetadataBytesQuota, uint64(8+len(data)), removed); err != nil {
				return err
			}
		}
		if err := b.Put(sequenceKey(n), data); err != nil {
			return err
		}

		return nil
	})
}

func validateAudit(tx *bolt.Tx) error {
	b := tx.Bucket(auditBucket)
	if b == nil {
		return errors.New("missing access audit bucket")
	}
	count := uint64(0)
	first := uint64(1)
	if b.Sequence() > maxAuditEntries {
		first = b.Sequence() - maxAuditEntries + 1
	}
	err := b.ForEach(func(k, v []byte) error {
		var entry auditEntry
		if len(k) != 8 || !bytes.Equal(k, sequenceKey(first+count)) || count >= maxAuditEntries || json.Unmarshal(v, &entry) != nil || !validAccess(entry.Operation, entry.Resource, entry.Result) {
			return errors.New("invalid access audit entry")
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	expected := b.Sequence()
	if expected > maxAuditEntries {
		expected = maxAuditEntries
	}
	if count != expected {
		return errors.New("invalid access audit retention")
	}
	return nil
}
