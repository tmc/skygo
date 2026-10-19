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
}

// Open opens or initializes a database in dir. Another writer is refused after
// one second. The persistent owner identifies this installation to backends.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		return nil, fmt.Errorf("open job database: %w", err)
	}
	s := &Store{db: db}
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
		db.Close()
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
	want, err := json.Marshal(task)
	if err != nil {
		return j, true, err
	}
	have, err := json.Marshal(j.Task)
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
	err = s.db.View(func(tx *bolt.Tx) error { var e error; j, found, e = lookup(tx, actor, requestID, task); return e })
	return
}

// Submit atomically binds requestID within the actor's namespace to a job. Repeating an
// identical request returns the original job, including its original target.
func (s *Store) Submit(actor auth.Actor, requestID string, task backend.Task, target string) (j backend.Job, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	existing, found, err := s.Lookup(actor, requestID, task)
	if err != nil || found {
		return existing, err
	}
	id, err := newID()
	if err != nil {
		return backend.Job{}, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		existing, found, e := lookup(tx, actor, requestID, task)
		if e != nil {
			return e
		}
		if found {
			j = existing
			return nil
		}
		encodedTask, e := json.Marshal(task)
		if e != nil {
			return e
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
		if jobs.Get([]byte(id)) != nil {
			return errors.New("random job identity collision")
		}
		n, e := seq.NextSequence()
		if e != nil {
			return e
		}
		now := time.Now().UTC()
		accepted := backend.Job{ID: id, Task: task, Target: target, Status: backend.Status{State: backend.Queued}, CreatedAt: now, UpdatedAt: now, Sequence: n}
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
		return json.Unmarshal(data, &j)
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
	originalTask, e := json.Marshal(before.Task)
	if e != nil {
		return j, e
	}
	j = before
	if e = fn(&j); e != nil {
		return j, e
	}
	changedTask, e := json.Marshal(j.Task)
	if e != nil {
		return j, e
	}
	if j.ID != before.ID || j.Target != before.Target || j.Sequence != before.Sequence || !j.CreatedAt.Equal(before.CreatedAt) || !bytes.Equal(originalTask, changedTask) {
		return j, errors.New("cannot change immutable job fields")
	}
	j.UpdatedAt = time.Now().UTC()
	data, e := json.Marshal(j)
	if e != nil {
		return j, e
	}
	return j, tx.Bucket(jobsBucket).Put([]byte(id), data)
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
