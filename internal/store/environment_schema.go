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

// migrateEnvironmentArtifactFence validates the genuine schema10 worker
// installation before installing, in the same transaction, the empty
// environment pin table when artifacts are installed, the environment storage
// format tag and marker11. No row is rewritten. The caller owns the
// transaction.
func migrateEnvironmentArtifactFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != workerGroupSchema {
		return ErrArtifactConflict
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if b := tx.Bucket(artifactsBucket); b != nil {
		var root artifactRoot
		if err := artifactDecode(b.Get([]byte("state")), &root); err != nil {
			return err
		}
		if root.SchemaFloor != artifactFormat || b.Bucket([]byte("environment_pins")) != nil {
			return artifactCorrupt()
		}
		if _, err := b.CreateBucket([]byte("environment_pins")); err != nil {
			return err
		}
	}
	m := tx.Bucket(metadataBucket)
	if err := m.Put(environmentStorageFormatKey, []byte(environmentStorageFormat)); err != nil {
		return err
	}
	return m.Put(schemaKey, sequenceKey(environmentArtifactSchema))
}

// validateEnvironments checks every environment row against its spec,
// request binding or witness, reservations, indexes and bounds. Schema9
// requires the bucket; an earlier schema must not contain it. Before
// schema11 no spec has artifacts and no record an incarnation; a spec with
// artifacts requires installed artifacts, whose pins artifactLoad checks.
// Before schema12 there are no request domains, witnesses, tombstones or
// reclaimed counts, so the sequence and every high-water equal the live
// counts. From schema12 they equal the live counts plus the reclaimed
// counts, every witness names a record or tombstone of its own request, and
// only a pre-12 record has an r/ binding.
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
	retire := schemaNumber(tx) >= environmentRetirementSchema
	tombstones := map[string]environmentTombstone{}
	reclaimed := map[string]uint64{}
	type witness struct {
		principal string
		domain    RequestDomain
		request   string
		w         environmentWitness
	}
	var witnesses []witness
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
		case retire && strings.HasPrefix(key, "w/"):
			p, d, request, ok := parseEnvironmentWitnessKey(k)
			if !ok {
				return errors.New("invalid environment request witness")
			}
			w, e := decodeEnvironmentWitness(v)
			if e != nil {
				return e
			}
			witnesses = append(witnesses, witness{p, d, request, w})
		case retire && strings.HasPrefix(key, "t/"):
			t, e := decodeEnvironmentTombstone(v)
			if e != nil || t.Identity.ID != key[2:] {
				return errors.New("invalid environment tombstone")
			}
			tombstones[key[2:]] = t
		case retire && strings.HasPrefix(key, "x/"):
			if !validPrincipal(k[2:]) || len(v) != 8 || binary.BigEndian.Uint64(v) == 0 {
				return errors.New("invalid environment reclaimed count")
			}
			reclaimed[key[2:]] = binary.BigEndian.Uint64(v)
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
	var total uint64
	for _, n := range reclaimed {
		var e error
		if total, e = addChecked(total, n); e != nil {
			return e
		}
	}
	if len(records) > maxEnvironments || b.Sequence() < total || b.Sequence()-total != uint64(len(records)) {
		return errors.New("environment count mismatch")
	}
	if len(tombstones) > maxEnvironmentTombstones {
		return errors.New("environment tombstones exceed limit")
	}
	owner := string(tx.Bucket(metadataBucket).Get([]byte("owner")))
	withAlias, legacy := 0, 0
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
		if schemaNumber(tx) < environmentArtifactSchema && (len(s.Artifacts) != 0 || r.IncarnationSeq != 0) || len(s.Artifacts) != 0 && tx.Bucket(artifactsBucket) == nil {
			return errors.New("unsupported environment artifacts or incarnation")
		}
		if r.Domain != nil && !retire {
			return errors.New("environment request domain requires schema 12")
		}
		if _, ok := tombstones[id]; ok {
			return errors.New("reclaimed environment has a record")
		}
		if r.Domain == nil {
			legacy++
			if requests[r.Principal+"/"+r.RequestID] != id {
				return errors.New("environment reservation mismatch")
			}
		}
		if names[r.Principal+"/"+s.Name] != id {
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
	if len(specs) != len(records) || len(requests) != legacy || len(names) != len(records) || len(aliases) != withAlias || len(index) != len(records) {
		return errors.New("orphan environment row")
	}
	n := 0
	for _, m := range scopes {
		if len(m) > maxPrincipalEnvironments {
			return errors.New("environment principal count exceeds limit")
		}
		n += len(m)
	}
	for p, h := range highs {
		if h < reclaimed[p] || h-reclaimed[p] != uint64(len(scopes[p])) {
			return errors.New("environment scope high-water mismatch")
		}
	}
	for p := range scopes {
		if _, ok := highs[p]; !ok {
			return errors.New("orphan environment scope index")
		}
	}
	for p := range reclaimed {
		if _, ok := highs[p]; !ok {
			return errors.New("orphan environment reclaimed count")
		}
	}
	if n != len(records) {
		return errors.New("orphan environment scope index")
	}
	perPrincipal := map[string]uint64{}
	for _, t := range tombstones {
		if t.Identity.Installation != owner {
			return errors.New("environment installation mismatch")
		}
		perPrincipal[t.Principal]++
		if perPrincipal[t.Principal] > reclaimed[t.Principal] {
			return errors.New("orphan environment tombstone")
		}
	}
	witnessed := map[string]bool{}
	for _, x := range witnesses {
		if witnessed[x.w.Environment] {
			return errors.New("duplicate environment request witness")
		}
		witnessed[x.w.Environment] = true
		var domain *RequestDomain
		var principal, request, spec string
		if r, ok := records[x.w.Environment]; ok {
			domain, principal, request, spec = r.Domain, r.Principal, r.RequestID, r.Spec
		} else if t, ok := tombstones[x.w.Environment]; ok {
			domain, principal, request, spec = &t.Domain, t.Principal, t.RequestID, t.Spec
		} else {
			return errors.New("environment request witness names no environment")
		}
		if domain == nil || *domain != x.domain || principal != x.principal || request != x.request || spec != x.w.Spec {
			return errors.New("environment request witness mismatch")
		}
	}
	for id, r := range records {
		if r.Domain != nil && !witnessed[id] {
			return errors.New("environment request witness missing")
		}
	}
	return nil
}
