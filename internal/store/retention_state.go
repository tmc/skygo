package store

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// EnableRetention explicitly activates bounded epochs while holding the stopped
// installation's exclusive writer lock. It never starts or signals workloads.
func EnableRetention(dir string, p RetentionPolicy) (out RetentionInfo, err error) {
	if !policyValid(p) {
		return out, errors.New("invalid retention policy")
	}
	s, e := Open(dir)
	if e != nil {
		return out, e
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	lineage, e := newID()
	if e != nil {
		return out, e
	}
	logs := filepath.Join(dir, "logs")
	var inventory map[string]uint64
	if p.LocalOutputEnabled {
		p.OutputUnit, p.Volume, inventory, e = inspectOutputRoot(logs)
		if e != nil {
			return out, e
		}
	} else {
		p.OutputUnit = 0
		p.Volume = 0
		inventory = map[string]uint64{}
		if e = emptyOutputRoot(logs); e != nil {
			return out, e
		}
	}
	var localBinding string
	if len(inventory) != 0 {
		localBinding, e = backend.LocalBinding(s.owner)
		if e != nil {
			return out, e
		}
	}
	now := s.now()
	err = s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if st.Enabled {
			return errors.New("retention is already enabled")
		}
		// Populate before enabling limit enforcement: valid preexisting over-quota
		// state remains inspectable and collectible, but subsequent growth is refused.
		st.Policy = p
		if st.LineageSerial == ^uint64(0) {
			return errors.New("lineage issuance exhausted")
		}
		st.LineageSerial++
		st.Lineage = lineage
		st.Current = 1
		st.Epochs = []requestEpoch{{Number: 1, AcceptUntil: now.Add(p.Window), RetryUntil: now.Add(2 * p.Window)}}
		st.LegacyReplayOpen = true
		st.LegacyCutoff = now.Add(p.Window)
		b := retentionTable(tx, retainedRequestsBucket)
		var rows []requestRecord
		if e = b.ForEach(func(_, v []byte) error {
			var r requestRecord
			if e := json.Unmarshal(v, &r); e != nil {
				return e
			}
			rows = append(rows, r)
			return nil
		}); e != nil {
			return e
		}
		for _, r := range rows {
			if e = deleteRetentionRecord(tx, retainedRequestsBucket, requestKey(r.Principal, r.Domain, r.requestID())); e != nil {
				return e
			}
			r.Domain.Lineage = lineage
			if e = putRetentionRecord(tx, retainedRequestsBucket, requestKey(r.Principal, r.Domain, r.requestID()), r); e != nil {
				return e
			}
			if r.WorkflowID != "" {
				if r.Expired {
					continue
				}
				ws, e := loadWorkflowState(tx, r.WorkflowID)
				if e != nil {
					return e
				}
				ws.Domain = r.Domain
				if e = putWorkflowState(tx, r.WorkflowID, ws); e != nil {
					return e
				}
				continue
			}
			var m retainedJob
			if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(r.JobID), &m); e != nil {
				return e
			}
			if m.LegacyRequestRetired {
				return errors.New("disabled activation has retired legacy identity")
			}
			m.Domain = r.Domain
			if e = putRetentionRecord(tx, retainedJobsBucket, []byte(r.JobID), m); e != nil {
				return e
			}
		}
		for aid := range inventory {
			jobID := aid
			if owner := tx.Bucket(attemptJobsBucket).Get([]byte(aid)); owner != nil {
				jobID = string(owner)
			}
			j, e := get(tx, jobID)
			if e != nil {
				return errors.New("output root contains an orphan execution")
			}
			if j.TargetBinding != localBinding {
				return errors.New("output binding does not identify built-in local target")
			}
			var m retainedJob
			if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(jobID), &m); e != nil {
				return e
			}
			m.Storage = backend.LocalOutput
			m.OutputReserve = uint64(backend.MaxLogBytes) + 4*p.OutputUnit
			m.Started = true
			if e = putRetentionRecord(tx, retainedJobsBucket, []byte(jobID), m); e != nil {
				return e
			}
			r := outputReservation{JobID: jobID, AttemptID: aid, Binding: j.TargetBinding, Generation: 1, Bytes: m.OutputReserve}
			for _, x := range []struct {
				q QuotaDomain
				n uint64
			}{{OutputBytesQuota, r.Bytes}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
				if e = adjust(tx, x.q, x.n, 0); e != nil {
					return e
				}
			}
			if e = putRetentionRecord(tx, retainedOutputsBucket, []byte(aid), r); e != nil {
				return e
			}
			ref := backend.OutputReference{Owner: s.owner, JobID: jobID, AttemptID: aid, Binding: j.TargetBinding, Generation: 1, ClosureID: "offline-closed-" + lineage, PayloadReleased: true}
			if e = putRetentionRecord(tx, retainedClosuresBucket, []byte(aid), outputClosure{Reference: ref, SubjectDigest: rowDigest(tx.Bucket(jobsBucket).Get([]byte(jobID)))}); e != nil {
				return e
			}
		}
		u, e := usage(tx)
		if e != nil {
			return e
		}
		graphs, _, e := workflowUsage(tx)
		if e != nil {
			return e
		}
		if e = tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(u[TaskBytesQuota]-graphs)); e != nil {
			return e
		}
		st.TaskCounterOffset = 0
		st.TaskCounterDeficit = 0
		st.Enabled = true
		if e = putJSON(retentionRoot(tx), retentionStateKey, st); e != nil {
			return e
		}
		if e = validateRetention(tx); e != nil {
			return e
		}
		out, e = offlineRetentionInfo(tx)
		return e
	})
	return
}
func inspectOutputRoot(dir string) (unit, volume uint64, files map[string]uint64, err error) {
	files = map[string]uint64{}
	info, e := os.Lstat(dir)
	if errors.Is(e, os.ErrNotExist) {
		return outputVolume(filepath.Dir(dir), files)
	}
	if e != nil {
		return 0, 0, nil, e
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, 0, nil, errors.New("output root is not a directory")
	}
	unit, volume, _, err = outputVolume(dir, files)
	if err != nil {
		return
	}
	f, e := os.Open(dir)
	if e != nil {
		err = e
		return
	}
	defer f.Close()
	entries, e := f.ReadDir(513)
	if e != nil && !errors.Is(e, io.EOF) {
		err = e
		return
	}
	if len(entries) > 512 {
		err = errors.New("output inventory exceeds file limit")
		return
	}
	logs := map[string]bool{}
	for _, de := range entries {
		name := de.Name()
		id := strings.TrimSuffix(name, ".log")
		marker := false
		if id == name {
			id = strings.TrimSuffix(name, ".truncated")
			marker = true
		}
		if !validAttemptID(id) || id == name {
			err = errors.New("unrecognized output entry")
			return
		}
		i, e := os.Lstat(filepath.Join(dir, name))
		if e != nil {
			err = e
			return
		}
		if !i.Mode().IsRegular() || i.Mode()&os.ModeSymlink != 0 || i.Size() < 0 || i.Size() > backend.MaxLogBytes || marker && i.Size() > 64 {
			err = errors.New("invalid retained output file")
			return
		}
		if !marker {
			logs[id] = true
		}
		files[id] += uint64(i.Size())
	}
	if len(files) > 256 {
		err = errors.New("output inventory exceeds domain limit")
		return
	}
	for id := range files {
		if !logs[id] {
			err = errors.New("output marker has no log file")
			return
		}
	}
	return
}

// BindOutputStorage fills only pre-activation unknown placement facts for an
// exact configured route. Call before serving, from actual target constructors.
func (s *Store) BindOutputStorage(target, binding string, storage backend.OutputStorage, reserve uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if !st.Enabled {
			return nil
		}
		if storage == backend.LocalOutput && !st.Policy.LocalOutputEnabled {
			return errors.New("local output retention is disabled")
		}
		if storage != backend.LocalOutput && storage != backend.RemoteOutput || storage == backend.RemoteOutput && reserve != 0 || storage == backend.LocalOutput && reserve != uint64(backend.MaxLogBytes)+4*st.Policy.OutputUnit {
			return errors.New("invalid output storage binding")
		}
		var ids []string
		if e = tx.Bucket(jobsBucket).ForEach(func(k, v []byte) error {
			var j backend.Job
			if e := decodeStoredJob(v, &j); e != nil {
				return e
			}
			if j.Target == target && j.TargetBinding == binding {
				ids = append(ids, string(k))
			}
			return nil
		}); e != nil {
			return e
		}
		for _, id := range ids {
			var m retainedJob
			if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(id), &m); e != nil {
				return e
			}
			if m.Storage != backend.UnspecifiedOutput {
				if m.Storage != storage || m.OutputReserve != reserve {
					return errors.New("accepted output storage changed")
				}
				continue
			}
			m.Storage = storage
			m.OutputReserve = reserve
			if e = putRetentionRecord(tx, retainedJobsBucket, []byte(id), m); e != nil {
				return e
			}
		}
		return nil
	})
}

// Compact writes a validated compact database into a new output file. The
// original installation must be stopped and is never replaced or removed.
func Compact(dir, output string) (err error) {
	if dir == "" || output == "" {
		return errors.New("compact requires directory and output")
	}
	source := filepath.Join(filepath.Clean(dir), "jobs.db")
	if filepath.Clean(source) == filepath.Clean(output) {
		return errors.New("compact output is the source")
	}
	db, e := openSnapshot(source)
	if e != nil {
		return e
	}
	defer db.Close()
	if e = db.View(validateSnapshot); e != nil {
		return e
	}
	info, e := os.Stat(source)
	if e != nil {
		return e
	}
	free, e := filesystemFree(filepath.Dir(output))
	if e != nil {
		return e
	}
	if uint64(info.Size()) > ^uint64(0)-(64<<20) || free < uint64(info.Size())+(64<<20) {
		return errors.New("insufficient compaction space")
	}
	f, e := os.CreateTemp(filepath.Dir(output), ".skygo-compact-*")
	if e != nil {
		return e
	}
	name := f.Name()
	if e = f.Close(); e != nil {
		os.Remove(name)
		return e
	}
	defer os.Remove(name)
	dest, e := bolt.Open(name, 0600, nil)
	if e != nil {
		return e
	}
	e = bolt.Compact(dest, db, maxChangeBytes)
	closeErr := dest.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	compact, e := openSnapshot(name)
	if e != nil {
		return e
	}
	e = compact.View(validateSnapshot)
	closeErr = compact.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	i, e := os.Stat(name)
	if e != nil {
		return e
	}
	if i.Size() > info.Size() {
		return errors.New("compact result exceeds source length")
	}
	handle, e := os.OpenFile(name, os.O_RDWR, 0)
	if e != nil {
		return e
	}
	e = handle.Sync()
	closeErr = handle.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Link(name, output); e != nil {
		return e
	}
	return syncDirectory(filepath.Dir(output))
}
func freshenRestored(path string) error {
	lineage, e := newID()
	if e != nil {
		return e
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true})
	if e != nil {
		return e
	}
	defer db.Close()
	if e = db.View(validateSnapshot); e != nil {
		return e
	}
	now := time.Now().UTC()
	return db.Update(func(tx *bolt.Tx) error {
		return freshenRestoredTx(tx, lineage, now)
	})
}

func freshenRestoredTx(tx *bolt.Tx, lineage string, now time.Time) error {
	if schemaNumber(tx) < 5 {
		return nil
	}
	st, e := loadRetention(tx)
	if e != nil {
		return e
	}
	if !st.Enabled {
		return nil
	}
	if st.LineageSerial == ^uint64(0) {
		return errors.New("lineage issuance exhausted")
	}
	st.LineageSerial++
	st.Lineage = lineage
	st.Current = 1
	st.RetiredThrough = 0
	st.LegacyReplayOpen = false
	st.LegacyCutoff = now
	st.Restored = true
	st.Epochs = []requestEpoch{{Number: 1, AcceptUntil: now.Add(st.Policy.Window), RetryUntil: now.Add(2 * st.Policy.Window)}}
	if e = putJSON(retentionRoot(tx), retentionStateKey, st); e != nil {
		return e
	}
	return validateRetention(tx)
}

func syncDirectory(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

// Directory returns the private state directory for trusted local sampling.
// It is not safe diagnostic output and must not be exposed by RPC handlers.
func (s *Store) Directory() string { return filepath.Dir(s.db.Path()) }

// ReusableBytes reports committed free database pages. Pending pages are not
// included because a live read transaction can still prevent their reuse.
func (s *Store) ReusableBytes() (uint64, error) {
	stats := s.db.Stats()
	size := s.db.Info().PageSize
	if stats.FreePageN < 0 || size <= 0 {
		return 0, errors.New("invalid database free-page statistics")
	}
	n := uint64(stats.FreePageN)
	if n > ^uint64(0)/uint64(size) {
		return 0, errors.New("database free-page statistics overflow")
	}
	return n * uint64(size), nil
}

// InspectRetention reads stopped-state retention facts without migration or any
// database writes. Like Backup, filesystem access is its authority; it is not an
// authenticated server API. The database's exclusive lock must be available.
func InspectRetention(dir string) (out RetentionInfo, err error) {
	db, e := openSnapshot(filepath.Join(dir, "jobs.db"))
	if e != nil {
		return out, e
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	err = db.View(func(tx *bolt.Tx) error {
		if e := validateSnapshot(tx); e != nil {
			return e
		}
		if schemaNumber(tx) < 5 {
			return nil
		}
		var e error
		out, e = offlineRetentionInfo(tx)
		return e
	})
	return
}

// RetentionPolicy reads trusted constructor configuration without inventing an
// authenticated caller or advancing request phases.
func (s *Store) RetentionPolicy() (out RetentionPolicy, enabled bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error { st, e := loadRetention(tx); out = st.Policy; enabled = st.Enabled; return e })
	return
}

func emptyOutputRoot(dir string) error {
	info, e := os.Lstat(dir)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("output root is not a directory")
	}
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	entries, e := f.ReadDir(1)
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if len(entries) != 0 {
		return errors.New("remote-only retention requires empty local output")
	}
	return nil
}

// OutputReservations reports committed local-output charges for trusted sampling.
// It includes manual/pending cleanup domains and performs no filesystem I/O.
func (s *Store) OutputReservations() (bytes, files uint64, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		u, e := usage(tx)
		if e != nil {
			return e
		}
		bytes = u[OutputBytesQuota]
		files = u[OutputFilesQuota]
		return nil
	})
	return
}
