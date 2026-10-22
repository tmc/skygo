package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// migrateEnvironmentFence validates the genuine schema8 installation before
// installing the empty environment bucket and marker9. Artifact, workflow and
// remote rows are not rewritten. The caller owns the transaction.
func migrateEnvironmentFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != 8 || tx.Bucket(environmentsBucket) != nil {
		return ErrArtifactConflict
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if _, err := tx.CreateBucket(environmentsBucket); err != nil {
		return err
	}
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(9))
}

// validateEnvironments checks every environment row against its spec,
// request binding, reservations, indexes and bounds. Schema9 requires the
// bucket; an earlier schema must not contain it.
func validateEnvironments(tx *bolt.Tx) error {
	b := tx.Bucket(environmentsBucket)
	if schemaNumber(tx) < 9 {
		if b != nil {
			return errors.New("environment metadata requires schema 9")
		}
		return nil
	}
	if b == nil {
		return errors.New("missing environment metadata")
	}
	records := map[string]environmentRecord{}
	specs := map[string]environment.Spec{}
	requests := map[string]string{}
	names := map[string]string{}
	aliases := map[string]string{}
	index := map[uint64]string{}
	scopes := map[string]map[uint64]string{}
	highs := map[string]uint64{}
	// pair splits "<a>/<b>" after a two-byte prefix; a has no '/'.
	pair := func(k []byte) (string, string, bool) {
		a, rest, ok := strings.Cut(string(k[2:]), "/")
		return a, rest, ok && a != "" && rest != ""
	}
	var usage uint64
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected environment nested bucket")
		}
		var e error
		if usage, e = addChecked(usage, uint64(len(k)+len(v))); e != nil {
			return e
		}
		key := string(k)
		switch {
		case strings.HasPrefix(key, "e/"):
			r, e := decodeEnvironmentRecord(v)
			if e != nil || r.Identity.ID != key[2:] {
				return errors.New("invalid environment record")
			}
			records[key[2:]] = r
		case strings.HasPrefix(key, "s/"):
			s, e := environment.Parse(v)
			if e != nil || !environment.ValidID(key[2:]) {
				return errors.New("invalid environment spec")
			}
			specs[key[2:]] = s
		case strings.HasPrefix(key, "r/"):
			p, r, ok := pair(k)
			if !ok || !validPrincipal([]byte(p)) || !validRetainedRequestID(r) || !environment.ValidID(string(v)) {
				return errors.New("invalid environment request binding")
			}
			requests[key[2:]] = string(v)
		case strings.HasPrefix(key, "n/"):
			p, _, ok := pair(k)
			if !ok || !validPrincipal([]byte(p)) || !environment.ValidID(string(v)) {
				return errors.New("invalid environment name reservation")
			}
			names[key[2:]] = string(v)
		case strings.HasPrefix(key, "a/"):
			if _, _, ok := pair(k); !ok || !environment.ValidID(string(v)) {
				return errors.New("invalid environment alias reservation")
			}
			aliases[key[2:]] = string(v)
		case strings.HasPrefix(key, "i/"):
			if len(k) != 10 || !environment.ValidID(string(v)) {
				return errors.New("invalid environment index")
			}
			n := binary.BigEndian.Uint64(k[2:])
			if n == 0 || n > b.Sequence() {
				return errors.New("invalid environment index ordinal")
			}
			index[n] = string(v)
		case strings.HasPrefix(key, "h/"):
			if !validPrincipal(k[2:]) || len(v) != 8 || binary.BigEndian.Uint64(v) == 0 {
				return errors.New("invalid environment scope high-water")
			}
			highs[key[2:]] = binary.BigEndian.Uint64(v)
		case strings.HasPrefix(key, "p/"):
			if len(k) < 11 || k[len(k)-9] != '/' || !validPrincipal(k[2:len(k)-9]) || !environment.ValidID(string(v)) {
				return errors.New("invalid environment scope index")
			}
			p := key[2 : len(k)-9]
			if scopes[p] == nil {
				scopes[p] = map[uint64]string{}
			}
			scopes[p][binary.BigEndian.Uint64(k[len(k)-8:])] = string(v)
		default:
			return errors.New("unknown environment row")
		}
		return nil
	}); err != nil {
		return err
	}
	if usage > maxEnvironmentMetadataBytes {
		return errors.New("environment metadata exceeds limit")
	}
	if len(records) > maxEnvironments || b.Sequence() != uint64(len(records)) {
		return errors.New("environment count mismatch")
	}
	owner := string(tx.Bucket(metadataBucket).Get([]byte("owner")))
	withAlias := 0
	for id, r := range records {
		s, ok := specs[id]
		raw := b.Get(environmentSpecKey(id))
		digest := sha256.Sum256(raw)
		if !ok || hex.EncodeToString(digest[:]) != r.Spec {
			return errors.New("environment spec mismatch")
		}
		if r.Identity.Installation != owner {
			return errors.New("environment installation mismatch")
		}
		if requests[r.Principal+"/"+r.RequestID] != id || names[r.Principal+"/"+s.Name] != id {
			return errors.New("environment reservation mismatch")
		}
		if s.StorageAlias != "" {
			if aliases[s.StorageAlias+"/"+s.Target] != id {
				return errors.New("environment alias reservation mismatch")
			}
			withAlias++
		}
		if index[r.Ordinal] != id || scopes[r.Principal][r.Scope] != id || r.Scope > highs[r.Principal] {
			return errors.New("environment index mismatch")
		}
	}
	if len(specs) != len(records) || len(requests) != len(records) || len(names) != len(records) || len(aliases) != withAlias || len(index) != len(records) {
		return errors.New("orphan environment row")
	}
	n := 0
	for p, m := range scopes {
		if highs[p] != uint64(len(m)) {
			return errors.New("environment scope high-water mismatch")
		}
		n += len(m)
	}
	if n != len(records) || len(highs) != len(scopes) {
		return errors.New("orphan environment scope index")
	}
	return nil
}
