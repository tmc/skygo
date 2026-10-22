package store

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotFound = errors.New("job not found")
	ErrConflict = errors.New("request ID belongs to a different task")
	ErrFull     = errors.New("job retention limit reached")
)

const maxJobs = 10000
const maxTaskBytes = 16 << 20

var taskBytesKey = []byte("task_bytes")

var jobsBucket = []byte("jobs")
var requestsBucket = []byte("requests")
var sequenceBucket = []byte("sequence")
var metadataBucket = []byte("metadata")

// Store owns the data directory's exclusive database lock.
type Store struct {
	db     *bolt.DB
	owner  string
	scoped bool
	now    func() time.Time
	wf     workflowCursor
}

// Open opens or initializes a database in dir. Another writer is refused after
// one second. The persistent owner identifies this installation to backends.
// On initialization failure it returns no Store and preserves any error from
// the single database Close. A close error identifies neither a retained lock
// nor a handle for another Close attempt; bbolt may have discarded its handle.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		return nil, fmt.Errorf("open job database: %w", err)
	}
	s := &Store{db: db, now: func() time.Time { return time.Now().UTC() }}
	// bbolt may flush an unsynced freelist during Open. Defer that write until
	// the schema has been accepted while holding the exclusive writer lock.
	err = db.View(validateExisting)
	var owner string
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error {
			if m := tx.Bucket(metadataBucket); m != nil {
				owner = string(m.Get([]byte("owner")))
			}
			return nil
		})
	}
	if err == nil && owner == "" {
		owner, err = newID()
	}
	if err == nil {
		db.NoFreelistSync = false
		err = db.Update(func(tx *bolt.Tx) error { return initialize(tx, owner) })
	}
	s.owner = owner
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error { s.scoped = isScoped(tx); return nil })
	}
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close job database after initialization failure: %w", closeErr))
		}
		return nil, fmt.Errorf("initialize job database: %w", err)
	}
	return s, nil
}

// Close releases the database and writer lock.
func (s *Store) Close() error { return s.db.Close() }

// Owner returns the persistent installation identity.
func (s *Store) Owner() string { return s.owner }

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func sequenceKey(n uint64) []byte { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); return b[:] }
func get(tx *bolt.Tx, id string) (backend.Job, error) {
	var j backend.Job
	v := tx.Bucket(jobsBucket).Get([]byte(id))
	if v == nil {
		return j, ErrNotFound
	}
	if err := json.Unmarshal(v, &j); err != nil {
		return j, fmt.Errorf("decode job %s: %w", id, err)
	}
	return j, nil
}
func lookup(tx *bolt.Tx, actor auth.Actor, requestID string, task backend.Task) (backend.Job, bool, error) {
	bindings := requestNamespace(tx, actor.Principal())
	if bindings == nil {
		return backend.Job{}, false, nil
	}
	id := bindings.Get([]byte(requestID))
	if id == nil {
		return backend.Job{}, false, nil
	}
	if isScoped(tx) && string(tx.Bucket(jobPrincipalsBucket).Get(id)) != actor.Principal() {
		return backend.Job{}, true, ErrNotFound
	}
	j, err := get(tx, string(id))
	if err != nil {
		return j, true, err
	}
	want, err := backend.TaskBytes(task)
	if err != nil {
		return j, true, err
	}
	have, err := backend.TaskBytes(j.Task)
	if err != nil {
		return j, true, err
	}
	if !bytes.Equal(have, want) {
		return backend.Job{}, true, ErrConflict
	}
	return j, true, nil
}

// Lookup returns a binding in the authenticated principal's namespace. Task equality uses its canonical
// JSON representation, treating nil and empty environment maps equally.
func (s *Store) Lookup(actor auth.Actor, requestID string, task backend.Task) (j backend.Job, found bool, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	task.Retry, err = backend.NormalizeRetry(task.Retry)
	if err != nil {
		return
	}
	var enabled bool
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		enabled = st.Enabled
		if enabled {
			return nil
		}
		j, found, e = lookup(tx, actor, requestID, task)
		return e
	})
	if err != nil || !enabled {
		return
	}
	var result error
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		j, found, result, e = legacyLookup(tx, actor, requestID, task, s.now())
		return e
	})
	if err == nil {
		err = result
	}
	return
}

// Submit atomically binds requestID within the actor's namespace to a job. Repeating an
// identical request returns the original job, including its target and binding,
// before validating the newly proposed route.
func (s *Store) Submit(actor auth.Actor, requestID string, task backend.Task, target, binding string) (j backend.Job, err error) {
	return s.submit(actor, requestID, task, target, binding, submissionOrdinary, nil)
}

func (s *Store) submit(actor auth.Actor, requestID string, task backend.Task, target, binding string, kind submissionKind, remote *remoteSubmission) (j backend.Job, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if err = validateSubmitWorkspace(task, kind); err != nil {
		return
	}
	task.Retry, err = backend.NormalizeRetry(task.Retry)
	if err != nil {
		return
	}
	existing, found, err := s.Lookup(actor, requestID, task)
	if err != nil || found {
		return existing, err
	}
	if !validRetainedRequestID(requestID) {
		return backend.Job{}, errors.New("invalid request ID")
	}
	id, err := newID()
	if err != nil {
		return backend.Job{}, err
	}
	now := time.Now().UTC()
	if kind != submissionOrdinary {
		now = s.now().UTC()
		if remote != nil {
			now = remote.now
		}
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if st.Enabled {
			return &ExpiryError{Reason: RequestDomainRequired}
		}
		existing, found, e := lookup(tx, actor, requestID, task)
		if e != nil {
			return e
		}
		if found {
			j = existing
			return nil
		}
		accept := func(art *artifactState) error {
			if kind == submissionRemote {
				if e := remote.validate(actor.Principal(), task); e != nil {
					return e
				}
			}
			if art != nil && task.Workdir != nil {
				ref, e := artifactTaskRef(task)
				if e != nil {
					return e
				}
				if e := artifactGranted(art, actor.Principal(), ref, now); e != nil {
					return e
				}
			}
			if target == "" || !validBinding(binding) {
				return errors.New("new job requires a target and valid binding")
			}
			encodedTask, e := backend.TaskBytes(task)
			if e != nil {
				return e
			}
			if kind != submissionOrdinary && len(encodedTask) > 256<<10 {
				return errors.New("task exceeds limit")
			}
			metadata := tx.Bucket(metadataBucket)
			data := metadata.Get(taskBytesKey)
			if len(data) != 8 {
				return errors.New("invalid task byte counter")
			}
			taskBytes := binary.BigEndian.Uint64(data)
			if taskBytes >= maxTaskBytes || uint64(len(encodedTask)) > maxTaskBytes-taskBytes {
				return ErrFull
			}
			jobs := tx.Bucket(jobsBucket)
			seq := tx.Bucket(sequenceBucket)
			if seq.Sequence() >= maxJobs {
				return ErrFull
			}
			if jobs.Get([]byte(id)) != nil || tx.Bucket(attemptJobsBucket).Get([]byte(id)) != nil {
				return errors.New("random job identity collision")
			}
			n, e := seq.NextSequence()
			if e != nil {
				return e
			}
			accepted := backend.Job{ID: id, Task: task, Target: target, TargetBinding: binding, Status: backend.Status{State: backend.Queued}, CreatedAt: now, UpdatedAt: now, Sequence: n}
			if e = acceptAttempts(tx, &accepted); e != nil {
				return e
			}
			data, e = json.Marshal(accepted)
			if e != nil {
				return e
			}
			if e = jobs.Put([]byte(id), data); e != nil {
				return e
			}
			if e = seq.Put(sequenceKey(n), []byte(id)); e != nil {
				return e
			}
			if e = bindRequest(tx, actor.Principal(), requestID, id); e != nil {
				return e
			}
			if isScoped(tx) {
				if e = tx.Bucket(jobPrincipalsBucket).Put([]byte(id), []byte(actor.Principal())); e != nil {
					return e
				}
				index, e := tx.Bucket(principalIndexBucket).CreateBucketIfNotExists([]byte(actor.Principal()))
				if e != nil {
					return e
				}
				ordinal, e := index.NextSequence()
				if e != nil {
					return e
				}
				if e = index.Put(sequenceKey(ordinal), []byte(id)); e != nil {
					return e
				}
			}
			if e = metadata.Put(taskBytesKey, sequenceKey(taskBytes+uint64(len(encodedTask)))); e != nil {
				return e
			}
			// Decode a fresh value so callers cannot mutate task maps owned by others.
			if e = trackLegacyAccepted(tx, accepted, actor.Principal(), requestID); e != nil {
				return e
			}
			if art != nil && task.Workdir != nil {
				ref, e := artifactTaskRef(task)
				if e != nil {
					return e
				}
				if e := artifactAcceptInput(tx, art, actor.Principal(), accepted.ID, ref, now); e != nil {
					return e
				}
			}
			if kind == submissionRemote {
				if e := remoteAccept(art, accepted.ID, actor.Principal(), remote); e != nil {
					return e
				}
			}
			return json.Unmarshal(data, &j)
		}
		if kind != submissionOrdinary {
			core := artifactAcceptLedger(tx, id, actor.Principal(), requestID, RequestDomain{}, true)
			_, e := artifactCoreChange(tx, core, maxChangeBytes, 100, accept)
			return e
		}
		return accept(nil)
	})
	return
}

// Get reads one accepted job for trusted reconciliation and integrity code.
// Request handlers must use GetFor.
func (s *Store) Get(id string) (j backend.Job, err error) {
	err = s.db.View(func(tx *bolt.Tx) error { var e error; j, e = get(tx, id); return e })
	return
}

// LastSequence returns the global sequence for trusted integrity code.
// Request handlers must use LastSequenceFor.
func (s *Store) LastSequence() (n uint64, err error) {
	err = s.db.View(func(tx *bolt.Tx) error { n = tx.Bucket(sequenceBucket).Sequence(); return nil })
	return
}

// List returns global jobs for trusted integrity code, in acceptance order.
// Request handlers must use ListFor.
func (s *Store) List(after, max uint64, limit int) (out []backend.Job, err error) {
	if limit <= 0 {
		return nil, errors.New("list limit must be positive")
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if after >= max {
			return nil
		}
		c := tx.Bucket(sequenceBucket).Cursor()
		for k, v := c.Seek(sequenceKey(after + 1)); k != nil && len(out) < limit; k, v = c.Next() {
			if len(k) != 8 {
				return errors.New("invalid job sequence key")
			}
			if binary.BigEndian.Uint64(k) > max {
				break
			}
			j, e := get(tx, string(v))
			if e != nil {
				return e
			}
			out = append(out, j)
		}
		return nil
	})
	return
}

// Update changes state for trusted reconciliation. Request handlers use UpdateFor.
// The callback must not perform
// external I/O or call Store methods. Identity, task and target cannot change.
func (s *Store) Update(id string, fn func(*backend.Job) error) (j backend.Job, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error { var e error; j, e = update(tx, id, fn); return e })
	return
}

func update(tx *bolt.Tx, id string, fn func(*backend.Job) error) (j backend.Job, result error) {
	before, e := get(tx, id)
	if e != nil {
		return j, e
	}
	originalTask, e := backend.TaskBytes(before.Task)
	if e != nil {
		return j, e
	}
	j, e = get(tx, id)
	if e != nil {
		return j, e
	}
	if e = fn(&j); e != nil {
		return j, e
	}
	changedTask, e := backend.TaskBytes(j.Task)
	if e != nil {
		return j, e
	}
	if j.ID != before.ID || j.Target != before.Target || j.TargetBinding != before.TargetBinding || j.Sequence != before.Sequence || !j.CreatedAt.Equal(before.CreatedAt) || !bytes.Equal(originalTask, changedTask) {
		return j, errors.New("cannot change immutable job fields")
	}
	if remoteRoute(tx, id) && !reflect.DeepEqual(before.Status, j.Status) {
		return j, ErrRemoteAttemptRequired
	}
	if e = validateUpdate(before, j); e != nil {
		return j, e
	}
	j.UpdatedAt = time.Now().UTC()
	if j.Status.Reason != boundedReason(j.Status.Reason) {
		j.Status.Reason = boundedReason(j.Status.Reason)
	}
	if e = putTrackedJob(tx, j); e != nil {
		return j, e
	}
	return get(tx, id)
}

// Active reads all nonterminal jobs in acceptance order.
func (s *Store) Active() (out []backend.Job, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(sequenceBucket).ForEach(func(_, id []byte) error {
			j, e := get(tx, string(id))
			if e != nil {
				return e
			}
			if !j.Status.State.Terminal() {
				out = append(out, j)
			}
			return nil
		})
	})
	return
}
