package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// RevalidateLocalOutput records stopped-state local filesystem facts after the
// operator has stopped all local writers. It preserves identities, closures,
// native receipts and accepted rows. Missing known output is never adopted,
// classified as closed or deleted. The caller must obtain explicit operator
// acknowledgement before calling this filesystem-authorized offline operation.
func RevalidateLocalOutput(dir string) (out RetentionInfo, err error) {
	// Open the existing file exclusively without initialization/migration or
	// freelist publication. Refusals and no-ops never begin a write transaction.
	db, e := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{
		Timeout: time.Second, NoFreelistSync: true,
		OpenFile: func(name string, flag int, mode os.FileMode) (*os.File, error) {
			f, e := os.OpenFile(name, flag&^os.O_CREATE, mode)
			if e != nil {
				return nil, e
			}
			info, e := f.Stat()
			if e != nil {
				return nil, errors.Join(e, f.Close())
			}
			// bbolt initializes size0 files in Open before any View. Validate
			// the actual opened descriptor; its smallest supported meta page is1KiB.
			if !info.Mode().IsRegular() || info.Size() < 2048 {
				return nil, errors.Join(errors.New("output revalidation requires an existing nonempty database"), f.Close())
			}
			return f, nil
		},
	})
	if e != nil {
		return out, e
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	var st retentionState
	if e = db.View(func(tx *bolt.Tx) error {
		if e := validateSnapshot(tx); e != nil {
			return e
		}
		if schemaNumber(tx) < 5 {
			return ErrRetentionDisabled
		}
		var e error
		st, e = loadRetention(tx)
		if e != nil {
			return e
		}
		if !st.Enabled || !st.Policy.LocalOutputEnabled {
			return errors.New("local output retention is not enabled")
		}
		return nil
	}); e != nil {
		return out, e
	}
	unit, volume, inventory, e := inspectOutputRoot(filepath.Join(dir, "logs"))
	if e != nil {
		return out, e
	}
	var plan outputRevalidation
	if e = db.View(func(tx *bolt.Tx) error {
		var e error
		plan, e = planOutputRevalidation(tx, unit, volume, inventory)
		return e
	}); e != nil {
		return out, e
	}
	if len(plan.writes) == 0 && unit == st.Policy.OutputUnit && volume == st.Policy.Volume {
		e = db.View(func(tx *bolt.Tx) error { var e error; out, e = offlineRetentionInfo(tx); return e })
		return out, e
	}
	// Operational headroom is sampled outside every database transaction. It is
	// a conservative admission check, not a reservation or hard physical DB cap.
	free, e := filesystemFree(dir)
	if e != nil {
		return out, e
	}
	need, e := revalidationSpace(plan.changedBytes)
	if e != nil {
		return out, e
	}
	if free < need {
		return out, errors.New("insufficient output revalidation space")
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if e := applyOutputRevalidation(tx, plan); e != nil {
			return e
		}
		var e error
		out, e = offlineRetentionInfo(tx)
		return e
	})
	return
}

type outputRevalidationWrite struct{ table, key, value []byte }
type outputRevalidation struct {
	state        retentionState
	used         retentionUsage
	writes       []outputRevalidationWrite
	changedBytes uint64
}

// The offline corpus is bounded by10000 job side rows and256 output domains.
// Each job side value is at most45KiB even for a32768-byte raw legacy identity;
// each output reservation is below2KiB. A1GiB changed-value cap independently
// checks actual encodings. Tasks/accepted rows/closure and cleanup payloads are
// never copied. This is one exclusive offline reconciliation transaction.
func planOutputRevalidation(tx *bolt.Tx, unit, volume uint64, inventory map[string]uint64) (p outputRevalidation, err error) {
	if err = validateSnapshot(tx); err != nil {
		return p, err
	}
	p.state, err = loadRetention(tx)
	if err != nil {
		return p, err
	}
	if !p.state.Enabled || !p.state.Policy.LocalOutputEnabled || unit < 4096 || unit > 65536 || unit&(unit-1) != 0 {
		return p, errors.New("invalid output revalidation policy")
	}
	if retentionTable(tx, retainedJobsBucket).Stats().KeyN > 10000 || retentionTable(tx, retainedOutputsBucket).Stats().KeyN > 256 {
		return p, errors.New("output revalidation corpus exceeds limit")
	}
	for id := range inventory {
		if retentionTable(tx, retainedOutputsBucket).Get([]byte(id)) == nil {
			return p, errors.New("output inventory has no durable reservation")
		}
	}
	p.used, err = usage(tx)
	if err != nil {
		return p, err
	}
	reserve, err := addChecked(uint64(backend.MaxLogBytes), 4*unit)
	if err != nil {
		return p, err
	}
	add := func(table, key, old, data []byte) error {
		if bytes.Equal(old, data) {
			return nil
		}
		var e error
		for _, n := range []uint64{uint64(len(key)), uint64(len(old)), uint64(len(data))} {
			p.changedBytes, e = addChecked(p.changedBytes, n)
			if e != nil {
				return e
			}
		}
		if p.changedBytes > 1<<30 {
			return errors.New("output revalidation change budget exceeded")
		}
		before, after := recordCharge(table, key, old), recordCharge(table, key, data)
		if p.used[MetadataBytesQuota] < before {
			return errors.New("output revalidation metadata underflow")
		}
		p.used[MetadataBytesQuota], e = addChecked(p.used[MetadataBytesQuota]-before, after)
		if e != nil {
			return e
		}
		p.writes = append(p.writes, outputRevalidationWrite{table, append([]byte(nil), key...), data})
		return nil
	}
	if err = retentionTable(tx, retainedJobsBucket).ForEach(func(k, v []byte) error {
		var m retainedJob
		if e := json.Unmarshal(v, &m); e != nil {
			return e
		}
		if m.Storage != backend.LocalOutput {
			return nil
		}
		m.OutputReserve = reserve
		data, e := json.Marshal(m)
		if e != nil {
			return e
		}
		return add(retainedJobsBucket, k, v, data)
	}); err != nil {
		return p, err
	}
	var total uint64
	if err = retentionTable(tx, retainedOutputsBucket).ForEach(func(k, v []byte) error {
		var r outputReservation
		if e := json.Unmarshal(v, &r); e != nil {
			return e
		}
		var e error
		total, e = addChecked(total, reserve)
		if e != nil {
			return e
		}
		r.Bytes = reserve
		data, e := json.Marshal(r)
		if e != nil {
			return e
		}
		return add(retainedOutputsBucket, k, v, data)
	}); err != nil {
		return p, err
	}
	p.used[OutputBytesQuota] = total
	p.state.Policy.OutputUnit = unit
	p.state.Policy.Volume = volume
	for _, x := range []struct {
		key   []byte
		value any
	}{{retentionStateKey, p.state}, {retentionUsageKey, p.used}} {
		data, e := json.Marshal(x.value)
		if e != nil {
			return p, e
		}
		old := retentionRoot(tx).Get(x.key)
		if !bytes.Equal(old, data) {
			for _, n := range []uint64{uint64(len(x.key)), uint64(len(old)), uint64(len(data))} {
				p.changedBytes, e = addChecked(p.changedBytes, n)
				if e != nil {
					return p, e
				}
			}
		}
	}
	if p.changedBytes > 1<<30 {
		return p, errors.New("output revalidation change budget exceeded")
	}
	return p, nil
}
func revalidationSpace(changed uint64) (uint64, error) {
	n, e := addChecked(changed, changed)
	if e != nil {
		return 0, e
	}
	n, e = addChecked(n, 64<<20)
	if e != nil {
		return 0, e
	}
	return max(n, 1<<30), nil
}
func offlineRetentionInfo(tx *bolt.Tx) (out RetentionInfo, err error) {
	st, e := loadRetention(tx)
	if e != nil {
		return out, e
	}
	u, e := usage(tx)
	if e != nil {
		return out, e
	}
	out = retentionInfo(st)
	out.PendingCleanupRecords = uint64(retentionTable(tx, cleanupOperationsBucket).Stats().KeyN)
	l := limits(st.Policy)
	for q := JobsQuota; q <= PrincipalScopesQuota; q++ {
		out.Usage = append(out.Usage, Usage{q, u[q], l[q]})
	}
	return out, nil
}

func applyOutputRevalidation(tx *bolt.Tx, plan outputRevalidation) error {
	for _, w := range plan.writes {
		if e := retentionTable(tx, w.table).Put(w.key, w.value); e != nil {
			return e
		}
	}
	if e := putJSON(retentionRoot(tx), retentionStateKey, plan.state); e != nil {
		return e
	}
	if e := putJSON(retentionRoot(tx), retentionUsageKey, plan.used); e != nil {
		return e
	}
	return validateSnapshot(tx)
}
