package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// PhysicalInspection reports observations of one existing database. Nil fields
// are unknown. ProbeComplete covers only fixed metadata and physical observations;
// GeometryStatus describes the separately requested binding scan. Neither is a
// certificate of migration safety, accepted work, or available disk reservation.
// ObservedAt is zero until a post-lock physical sample has been obtained.
type PhysicalInspection struct {
	ObservedAt                     time.Time `json:"observed_at"`
	FileBytes                      *uint64   `json:"file_bytes,omitempty"`
	AllocatedBytes                 *uint64   `json:"allocated_bytes,omitempty"`
	FilesystemFreeBytes            *uint64   `json:"filesystem_free_bytes,omitempty"`
	Device                         *uint64   `json:"device,omitempty"`
	Inode                          *uint64   `json:"inode,omitempty"`
	SchemaVersion                  *uint64   `json:"schema_version,omitempty"`
	SchemaSupported                *bool     `json:"schema_supported,omitempty"`
	Scoped                         *bool     `json:"scoped,omitempty"`
	RetentionEnabled               *bool     `json:"retention_enabled,omitempty"`
	OwnerPresent                   *bool     `json:"owner_present,omitempty"`
	OwnerSHA256                    string    `json:"owner_sha256,omitempty"`
	ProbeComplete                  bool      `json:"probe_complete"`
	GeometryStatus                 string    `json:"geometry_status"`
	Reason                         string    `json:"reason,omitempty"`
	ExaminedEntries                uint64    `json:"examined_entries"`
	AccountedInputBytes            uint64    `json:"accounted_input_bytes"`
	Jobs                           *uint64   `json:"jobs,omitempty"`
	RootRequestBindings            *uint64   `json:"root_request_bindings,omitempty"`
	ScopedRequestBindings          *uint64   `json:"scoped_request_bindings,omitempty"`
	RawRequestKeyBytes             *uint64   `json:"raw_request_key_bytes,omitempty"`
	RawRequestValueBytes           *uint64   `json:"raw_request_value_bytes,omitempty"`
	DistinctLegacyBindings         *uint64   `json:"distinct_legacy_bindings,omitempty"`
	ExistingLivePayloadBytes       *uint64   `json:"existing_live_payload_bytes,omitempty"`
	AddedLegacyIDPayloadLowerBound *uint64   `json:"added_legacy_id_payload_lower_bound,omitempty"`
	ScopedActivationCopyPayload    *uint64   `json:"scoped_activation_copy_payload,omitempty"`
	MigratedLivePayloadLowerBound  *uint64   `json:"migrated_live_payload_lower_bound,omitempty"`
	Conditions                     []string  `json:"conditions,omitempty"`
}

// ProbePhysicalState reads only fixed, bounded metadata and descriptor facts.
// It never scans bindings or decodes Jobs. Its 100 ms cooperative budget and
// 25 ms lock wait do not interrupt filesystem calls blocked in the kernel.
func ProbePhysicalState(ctx context.Context, dir string) (PhysicalInspection, error) {
	return inspectPhysicalState(ctx, dir, false, physicalLimits{duration: 100 * time.Millisecond, lock: 25 * time.Millisecond})
}

// InspectPhysicalState requests bounded schema and binding geometry without
// migration or writes. It never decodes canonical Job or Task JSON. Complete
// geometry does not validate work, routes, proofs, counters, or migration safety.
// Filesystem access is its authority; no server credentials are consulted.
func InspectPhysicalState(ctx context.Context, dir string) (PhysicalInspection, error) {
	return inspectPhysicalState(ctx, dir, true, physicalLimits{duration: 5 * time.Second, lock: time.Second, entries: 131072, input: 2 << 30, ids: 16384, clones: 16 << 20, depth: 8})
}

type physicalLimits struct {
	duration, lock              time.Duration
	entries, input, ids, clones uint64
	depth                       int
}

type physicalBudget struct {
	ctx    context.Context
	limit  physicalLimits
	out    *PhysicalInspection
	cloned uint64
}

var errPhysicalInspection = errors.New("physical inspection unavailable or incomplete")

func (b *physicalBudget) fail(reason string) error {
	b.out.Reason = reason
	return errPhysicalInspection
}
func (b *physicalBudget) check() error {
	if err := b.ctx.Err(); err != nil {
		return errors.Join(b.fail("canceled_or_deadline"), err)
	}
	if deadline, ok := b.ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return errors.Join(b.fail("canceled_or_deadline"), context.DeadlineExceeded)
	}
	return nil
}
func (b *physicalBudget) account(n uint64) error {
	if err := b.check(); err != nil {
		return err
	}
	cap := b.limit.input
	if cap == 0 {
		cap = 128 << 10
	}
	if b.out.AccountedInputBytes > cap || n > cap-b.out.AccountedInputBytes {
		return b.fail("input_budget")
	}
	b.out.AccountedInputBytes += n
	return nil
}
func (b *physicalBudget) entry(n uint64) error {
	if err := b.check(); err != nil {
		return err
	}
	if b.out.ExaminedEntries >= b.limit.entries {
		return b.fail("entry_budget")
	}
	b.out.ExaminedEntries++
	return b.account(n)
}

func inspectPhysicalState(ctx context.Context, dir string, geometry bool, limits physicalLimits) (out PhysicalInspection, err error) {
	out.GeometryStatus = "not_requested"
	if geometry {
		out.GeometryStatus = "incomplete"
	}
	ctx, cancel := context.WithTimeout(ctx, limits.duration)
	defer cancel()
	budget := &physicalBudget{ctx: ctx, limit: limits, out: &out}
	if err = budget.check(); err != nil {
		return
	}
	deadline, _ := ctx.Deadline()
	wait := min(limits.lock, time.Until(deadline))
	if wait <= 0 {
		err = budget.fail("canceled_or_deadline")
		return
	}
	if dir == "" {
		err = budget.fail("invalid_directory")
		return
	}
	path := filepath.Join(dir, "jobs.db")
	db, file, e := openPhysicalSnapshot(path, wait)
	if e != nil {
		if err = budget.check(); err != nil {
			return
		}
		reason := "open_failed"
		if errors.Is(e, os.ErrNotExist) {
			reason = "missing"
		}
		if errors.Is(e, bolt.ErrTimeout) {
			reason = "locked"
		}
		err = budget.fail(reason)
		return
	}
	// file is borrowed from db after successful transfer. Never close it here.
	defer func() {
		if e := db.Close(); e != nil {
			out.ProbeComplete = false
			if geometry {
				out.GeometryStatus = "incomplete"
				clearPhysicalGeometry(&out)
			}
			err = budget.fail("close_failed")
		}
	}()
	if err = budget.check(); err != nil {
		return
	}
	info, e := physicalDescriptor(ctx, file, &out)
	if e != nil {
		if err = budget.check(); err != nil {
			return
		}
		err = budget.fail("physical_unavailable")
		return
	}
	out.ObservedAt = time.Now().UTC()
	if err = budget.check(); err != nil {
		return
	}
	err = db.View(func(tx *bolt.Tx) error {
		if e := readPhysicalScalars(tx, budget); e != nil {
			return e
		}
		out.ProbeComplete = true
		if !geometry {
			return nil
		}
		return readPhysicalGeometry(tx, budget)
	})
	if e := budget.check(); e != nil {
		err = e
	}
	if err == nil {
		last, e := os.Lstat(path)
		if e != nil || !last.Mode().IsRegular() || !os.SameFile(info, last) {
			err = budget.fail("source_path_changed")
			out.ProbeComplete = false
		}
		if e := budget.check(); e != nil {
			err = e
		}
	}
	if err != nil && geometry {
		out.GeometryStatus = "incomplete"
		clearPhysicalGeometry(&out)
	}
	if err == nil {
		if geometry {
			out.GeometryStatus = "complete"
		}
		if *out.FileBytes >= 320<<20 {
			out.Conditions = append(out.Conditions, "observed_db_at_maintenance_watermark")
		} else if *out.FileBytes >= 256<<20 {
			out.Conditions = append(out.Conditions, "observed_db_at_admission_watermark")
		}
		if geometry && out.MigratedLivePayloadLowerBound != nil && *out.MigratedLivePayloadLowerBound >= 320<<20 {
			out.Conditions = append(out.Conditions, "projected_live_payload_above_maintenance")
		}
		if *out.FilesystemFreeBytes < 1<<30 {
			out.Conditions = append(out.Conditions, "critical_free_space")
		} else if *out.FilesystemFreeBytes < 2<<30 {
			out.Conditions = append(out.Conditions, "low_free_space")
		}
	}
	return
}

// A hook owns its file until f,nil is returned. Thereafter bbolt alone closes it,
// including Open errors. On error the borrowed pointer is never returned or used.
func openPhysicalSnapshot(path string, wait time.Duration) (*bolt.DB, *os.File, error) {
	var borrowed *os.File
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: wait, OpenFile: func(name string, _ int, _ os.FileMode) (*os.File, error) {
		f, e := openPhysicalFile(name)
		if e != nil {
			return nil, e
		}
		info, e := f.Stat()
		if e != nil || !info.Mode().IsRegular() || info.Size() < 2048 {
			return nil, errors.Join(errors.New("physical inspection requires an existing regular database"), f.Close())
		}
		borrowed = f
		return f, nil
	}})
	if err != nil {
		return nil, nil, err
	}
	return db, borrowed, nil
}

func readPhysicalScalars(tx *bolt.Tx, b *physicalBudget) error {
	// Fixed lookups only. Do not call inspect: its missing-owner path uses Stats.
	if err := b.check(); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	jobs, requests, sequence := tx.Bucket(jobsBucket), tx.Bucket(requestsBucket), tx.Bucket(sequenceBucket)
	if m == nil || jobs == nil || requests == nil || sequence == nil {
		return b.fail("no_installation_or_layout")
	}
	if err := b.check(); err != nil {
		return err
	}
	version := uint64(0)
	if m.Bucket(schemaKey) != nil {
		return b.fail("invalid_schema_scalar")
	}
	marker := m.Get(schemaKey)
	if raw := marker; raw != nil {
		if len(raw) != 8 {
			return b.fail("invalid_schema_scalar")
		}
		if err := b.account(8); err != nil {
			return err
		}
		version = binary.BigEndian.Uint64(raw)
	}
	supported := marker == nil || version >= 1 && version <= 5
	b.out.SchemaVersion, b.out.SchemaSupported = &version, &supported
	if !supported {
		return b.fail("unsupported_schema")
	}
	if err := b.check(); err != nil {
		return err
	}
	if m.Bucket([]byte("owner")) != nil {
		return b.fail("invalid_owner_scalar")
	}
	owner := m.Get([]byte("owner"))
	if len(owner) > 65536 {
		return b.fail("owner_scalar_budget")
	}
	if err := b.account(uint64(len(owner))); err != nil {
		return err
	}
	present := len(owner) != 0
	b.out.OwnerPresent = &present
	if present {
		digest := sha256.Sum256(owner)
		b.out.OwnerSHA256 = hex.EncodeToString(digest[:])
	}
	if err := b.check(); err != nil {
		return err
	}
	if m.Bucket(scopedKey) != nil {
		return b.fail("invalid_scoped_scalar")
	}
	raw := m.Get(scopedKey)
	if len(raw) > 1 || raw != nil && !bytes.Equal(raw, []byte{1}) {
		return b.fail("invalid_scoped_scalar")
	}
	if err := b.account(uint64(len(raw))); err != nil {
		return err
	}
	scoped := len(raw) == 1
	b.out.Scoped = &scoped
	if version == 2 && !scoped || scoped && version < 2 {
		return b.fail("schema_scope_conflict")
	}
	if err := b.check(); err != nil {
		return err
	}
	root := tx.Bucket(retentionBucket)
	if root != nil {
		if version < 5 {
			return b.fail("retention_schema_conflict")
		}
		if root.Bucket(retentionStateKey) != nil {
			return b.fail("invalid_retention_scalar")
		}
		raw := root.Get(retentionStateKey)
		if len(raw) == 0 || len(raw) > 2048 {
			return b.fail("retention_scalar_budget")
		}
		if err := b.account(uint64(len(raw))); err != nil {
			return err
		}
		var state struct {
			Enabled *bool `json:"enabled"`
		}
		if json.Unmarshal(raw, &state) != nil || state.Enabled == nil {
			return b.fail("invalid_retention_scalar")
		}
		b.out.RetentionEnabled = state.Enabled
	} else if version < 5 {
		enabled := false
		b.out.RetentionEnabled = &enabled
	} else {
		return b.fail("retention_state_missing")
	}
	return b.check()
}

type physicalGeometry struct {
	jobs, root, scoped, keyBytes, valueBytes, bindings, live, base64 uint64
}

func readPhysicalGeometry(tx *bolt.Tx, b *physicalBudget) error {
	var g physicalGeometry
	add := func(p *uint64, n uint64) error {
		if ^uint64(0)-*p < n {
			return b.fail("arithmetic_overflow")
		}
		*p += n
		return nil
	}
	var walk func(*bolt.Bucket, int) error
	walk = func(bucket *bolt.Bucket, depth int) error {
		if depth > b.limit.depth {
			return b.fail("depth_budget")
		}
		if bucket == nil {
			return b.fail("invalid_bucket_layout")
		}
		if e := b.check(); e != nil {
			return e
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if e := b.entry(uint64(len(k)) + uint64(len(v))); e != nil {
				return e
			}
			if e := add(&g.live, uint64(len(k))+uint64(len(v))); e != nil {
				return e
			}
			if v == nil {
				if e := walk(bucket.Bucket(k), depth+1); e != nil {
					return e
				}
			}
			if e := b.check(); e != nil {
				return e
			}
		}
		return nil
	}
	if e := b.check(); e != nil {
		return e
	}
	root := tx.Cursor()
	for k, _ := root.First(); k != nil; k, _ = root.Next() {
		if e := b.entry(uint64(len(k))); e != nil {
			return e
		}
		if e := add(&g.live, uint64(len(k))); e != nil {
			return e
		}
		if e := walk(tx.Bucket(k), 1); e != nil {
			return e
		}
		if e := b.check(); e != nil {
			return e
		}
	}
	if e := b.check(); e != nil {
		return e
	}
	jobs := map[string]bool{}
	c := tx.Bucket(jobsBucket).Cursor()
	for id, raw := c.First(); id != nil; id, raw = c.Next() {
		if e := b.entry(uint64(len(id)) + uint64(len(raw))); e != nil {
			return e
		}
		if raw == nil {
			return b.fail("invalid_job_key")
		}
		if uint64(len(jobs)) >= b.limit.ids || uint64(len(id)) > b.limit.clones-b.cloned {
			return b.fail("job_identity_budget")
		}
		b.cloned += uint64(len(id))
		jobs[string(id)] = false
		g.jobs++
		if e := b.check(); e != nil {
			return e
		}
	}
	if g.jobs != 0 && !*b.out.OwnerPresent {
		return b.fail("missing_owner")
	}
	binding := func(k, id []byte, principal []byte, canonical bool) error {
		if e := b.entry(uint64(len(k)) + uint64(len(id))); e != nil {
			return e
		}
		if len(k) == 0 || id == nil {
			return b.fail("invalid_binding")
		}
		if uint64(len(id)) > b.limit.clones {
			return b.fail("job_identity_budget")
		}
		seen, exists := jobs[string(id)]
		if !exists {
			return b.fail("binding_job_missing")
		}
		if *b.out.Scoped && canonical {
			owners := tx.Bucket(jobPrincipalsBucket)
			if owners == nil {
				return b.fail("missing_scope_owner")
			}
			if e := b.check(); e != nil {
				return e
			}
			owner := owners.Get(id)
			if e := b.account(uint64(len(owner))); e != nil {
				return e
			}
			if !bytes.Equal(owner, principal) {
				return b.fail("binding_owner_conflict")
			}
		}
		if canonical {
			if seen {
				return b.fail("multiple_job_identities")
			}
			jobs[string(id)] = true
			g.bindings++
			n := uint64(len(k))
			if n > ^uint64(0)-2 {
				return b.fail("arithmetic_overflow")
			}
			n = (n + 2) / 3
			if n > ^uint64(0)/4 {
				return b.fail("arithmetic_overflow")
			}
			if e := add(&g.base64, 4*n); e != nil {
				return e
			}
		}
		if e := add(&g.keyBytes, uint64(len(k))); e != nil {
			return e
		}
		return add(&g.valueBytes, uint64(len(id)))
	}
	if e := b.check(); e != nil {
		return e
	}
	rc := tx.Bucket(requestsBucket).Cursor()
	for k, id := rc.First(); k != nil; k, id = rc.Next() {
		if e := binding(k, id, []byte("legacy"), !*b.out.Scoped); e != nil {
			return e
		}
		g.root++
		if *b.out.Scoped {
			p := tx.Bucket(principalRequestsBucket)
			if p == nil || p.Bucket([]byte("legacy")) == nil {
				return b.fail("missing_legacy_mirror")
			}
			if e := b.check(); e != nil {
				return e
			}
			mirror := p.Bucket([]byte("legacy")).Get(k)
			if e := b.account(uint64(len(mirror))); e != nil {
				return e
			}
			if !bytes.Equal(mirror, id) {
				return b.fail("legacy_mirror_conflict")
			}
		}
		if e := b.check(); e != nil {
			return e
		}
	}
	if *b.out.Scoped {
		p := tx.Bucket(principalRequestsBucket)
		if p == nil {
			return b.fail("missing_scoped_bindings")
		}
		if e := b.check(); e != nil {
			return e
		}
		pc := p.Cursor()
		for principal, v := pc.First(); principal != nil; principal, v = pc.Next() {
			if e := b.entry(uint64(len(principal)) + uint64(len(v))); e != nil {
				return e
			}
			if v != nil || !validPrincipal(principal) {
				return b.fail("invalid_namespace")
			}
			if e := b.check(); e != nil {
				return e
			}
			bc := p.Bucket(principal).Cursor()
			for k, id := bc.First(); k != nil; k, id = bc.Next() {
				if e := binding(k, id, principal, true); e != nil {
					return e
				}
				g.scoped++
				if e := b.check(); e != nil {
					return e
				}
			}
			if e := b.check(); e != nil {
				return e
			}
		}
	}
	if *b.out.SchemaVersion < 5 {
		for _, seen := range jobs {
			if !seen {
				return b.fail("missing_legacy_binding")
			}
		}
	}
	added := uint64(0)
	if *b.out.SchemaVersion < 5 {
		if g.base64 > ^uint64(0)/2 {
			return b.fail("arithmetic_overflow")
		}
		added = 2 * g.base64
	}
	projected := g.live
	if e := add(&projected, added); e != nil {
		return e
	}
	scoping := uint64(0)
	if !*b.out.Scoped {
		scoping = g.keyBytes
		if e := add(&scoping, g.valueBytes); e != nil {
			return e
		}
	}
	b.out.Jobs = &g.jobs
	b.out.RootRequestBindings = &g.root
	b.out.ScopedRequestBindings = &g.scoped
	b.out.RawRequestKeyBytes = &g.keyBytes
	b.out.RawRequestValueBytes = &g.valueBytes
	b.out.DistinctLegacyBindings = &g.bindings
	b.out.ExistingLivePayloadBytes = &g.live
	b.out.AddedLegacyIDPayloadLowerBound = &added
	b.out.ScopedActivationCopyPayload = &scoping
	b.out.MigratedLivePayloadLowerBound = &projected
	return b.check()
}

func clearPhysicalGeometry(out *PhysicalInspection) {
	out.Jobs = nil
	out.RootRequestBindings = nil
	out.ScopedRequestBindings = nil
	out.RawRequestKeyBytes = nil
	out.RawRequestValueBytes = nil
	out.DistinctLegacyBindings = nil
	out.ExistingLivePayloadBytes = nil
	out.AddedLegacyIDPayloadLowerBound = nil
	out.ScopedActivationCopyPayload = nil
	out.MigratedLivePayloadLowerBound = nil
}
