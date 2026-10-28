package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func legacyFixture(t *testing.T, dir string, unsynced bool) backend.Job {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: unsynced})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	j := backend.Job{ID: "0123456789abcdef0123456789abcdef", Task: backend.Task{Argv: []string{"work"}}, Target: "local", Status: backend.Status{State: backend.Succeeded}, Sequence: 1, CreatedAt: time.Unix(100, 0).UTC(), UpdatedAt: time.Unix(200, 0).UTC()}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{jobsBucket, requestsBucket, sequenceBucket, metadataBucket} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		data, err := json.Marshal(j)
		if err != nil {
			return err
		}
		if err := tx.Bucket(jobsBucket).Put([]byte(j.ID), data); err != nil {
			return err
		}
		if err := tx.Bucket(requestsBucket).Put([]byte("original-request"), []byte(j.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).Put(sequenceKey(1), []byte(j.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).SetSequence(1); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put([]byte("owner"), []byte("legacy-owner"))
	})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func editFixture(t *testing.T, dir string, unsynced bool, fn func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: unsynced})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMigrationPreservesRecordsAndIdentity(t *testing.T) {
	dir := t.TempDir()
	first := legacyFixture(t, dir, false)
	second := first
	second.ID = "fedcba9876543210fedcba9876543210"
	second.Task = backend.Task{Argv: []string{"second"}, Env: map[string]string{"K": "V"}}
	second.Sequence = 2
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		raw, err := json.Marshal(second)
		if err != nil {
			return err
		}
		if err := tx.Bucket(jobsBucket).Put([]byte(second.ID), raw); err != nil {
			return err
		}
		if err := tx.Bucket(requestsBucket).Put([]byte("second-request"), []byte(second.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).Put(sequenceKey(2), []byte(second.ID)); err != nil {
			return err
		}
		return tx.Bucket(sequenceBucket).SetSequence(2)
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Owner() != "legacy-owner" {
		t.Fatal("owner changed")
	}
	for _, binding := range []struct {
		request string
		job     backend.Job
	}{{"original-request", first}, {"second-request", second}} {
		task := binding.job.Task
		if len(task.Env) == 0 {
			task.Env = map[string]string{}
		}
		got, err := s.Submit(testActor(), binding.request, task, "replacement", "test-binding")
		if err != nil || got.ID != binding.job.ID || got.Target != binding.job.Target || got.Sequence != binding.job.Sequence {
			t.Fatalf("retry: %+v %v", got, err)
		}
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		var total uint64
		for _, j := range []backend.Job{first, second} {
			raw, _ := json.Marshal(j)
			if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(j.ID)), raw) {
				t.Error("migration rewrote accepted record")
			}
			taskRaw, _ := json.Marshal(j.Task)
			total += uint64(len(taskRaw))
		}
		m := tx.Bucket(metadataBucket)
		if !bytes.Equal(m.Get(schemaKey), sequenceKey(schemaVersion)) {
			t.Error("missing schema fence")
		}
		if !bytes.Equal(m.Get(taskBytesKey), sequenceKey(total)) {
			t.Error("aggregate counter not derived")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(0, 2, 10)
	if err != nil || len(jobs) != 2 || jobs[0].ID != first.ID || jobs[1].ID != second.ID {
		t.Fatalf("pagination changed: %+v %v", jobs, err)
	}
	if n, err := s.LastSequence(); err != nil || n != 2 {
		t.Fatalf("sequence changed: %d %v", n, err)
	}
}

func TestSchemaRefusedWithoutWrites(t *testing.T) {
	for _, unsynced := range []bool{false, true} {
		for _, marker := range []struct {
			name   string
			value  []byte
			future bool
		}{{"future", sequenceKey(schemaVersion + 1), true}, {"malformed", []byte{1}, false}} {
			t.Run(marker.name+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
				dir := t.TempDir()
				legacyFixture(t, dir, unsynced)
				editFixture(t, dir, unsynced, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, marker.value) })
				path := filepath.Join(dir, "jobs.db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				s, err := Open(dir)
				if err == nil {
					s.Close()
					t.Fatal("unsupported schema opened")
				}
				if s != nil {
					t.Fatal("initialization refusal returned a Store")
				}
				if !strings.HasPrefix(err.Error(), "initialize job database: ") {
					t.Fatalf("missing initialization context: %v", err)
				}
				if marker.future && !errors.Is(err, ErrUnsupportedSchema) {
					t.Fatalf("future error: %v", err)
				}
				after, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !bytes.Equal(before, after) {
					t.Fatalf("refused open changed database: %x -> %x", sha256.Sum256(before), sha256.Sum256(after))
				}
			})
		}
	}
}

func TestMigrationRollback(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).Put([]byte("zz-invalid-last-record"), []byte("not JSON"))
	})
	db, err := openSnapshot(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := db.View(func(tx *bolt.Tx) error {
		before = append(before, tx.Bucket(jobsBucket).Get([]byte("zz-invalid-last-record"))...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("invalid migration succeeded")
	}
	db, err = openSnapshot(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m.Get(schemaKey) != nil || m.Get(taskBytesKey) != nil || string(m.Get([]byte("owner"))) != "legacy-owner" {
			t.Error("failed migration partially committed")
		}
		if !bytes.Equal(before, tx.Bucket(jobsBucket).Get([]byte("zz-invalid-last-record"))) {
			t.Error("failed migration rewrote record")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyCanonicalTaskV1(t *testing.T) {
	for _, test := range []struct {
		name string
		task backend.Task
		want string
	}{
		{"zero", backend.Task{}, `{"argv":null}`},
		{"nil-env", backend.Task{Argv: []string{"work"}}, `{"argv":["work"]}`},
		{"empty-env", backend.Task{Argv: []string{"work"}, Env: map[string]string{}}, `{"argv":["work"]}`},
		{"sorted-env", backend.Task{Argv: []string{"work"}, Env: map[string]string{"Z": "2", "A": "1"}, CPUMillis: 1, TimeoutSeconds: 2}, `{"argv":["work"],"env":{"A":"1","Z":"2"},"cpu_millis":1,"timeout_seconds":2}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := backend.TaskBytes(test.task)
			if err != nil || string(raw) != test.want {
				t.Fatalf("canonical hash input changed: %s %v", raw, err)
			}
			if sha256.Sum256(raw) != sha256.Sum256([]byte(test.want)) {
				t.Fatal("hash changed")
			}
		})
	}
}

func TestLegacyMissingBucketRefused(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error { return tx.DeleteBucket(requestsBucket) })
	before, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("partial store accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("partial store mutated")
	}
}

func TestLegacyCorruptionRefusedWithExistingCounter(t *testing.T) {
	for _, kind := range []string{"job", "sequence", "request"} {
		for _, unsynced := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
				dir := t.TempDir()
				j := legacyFixture(t, dir, unsynced)
				editFixture(t, dir, unsynced, func(tx *bolt.Tx) error {
					raw, _ := json.Marshal(j.Task)
					if err := tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(uint64(len(raw)))); err != nil {
						return err
					}
					switch kind {
					case "job":
						return tx.Bucket(jobsBucket).Put([]byte(j.ID), []byte("invalid JSON"))
					case "sequence":
						return tx.Bucket(sequenceBucket).Put(sequenceKey(2), []byte("orphan"))
					default:
						return tx.Bucket(requestsBucket).Put([]byte("orphan"), []byte("missing"))
					}
				})
				path := filepath.Join(dir, "jobs.db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if s, err := Open(dir); err == nil {
					s.Close()
					t.Fatal("corrupt legacy store migrated")
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("refused legacy store changed bytes")
				}
			})
		}
	}
}

func TestMigrationTransactionRollback(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("injected after migration writes")
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused-candidate"); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("migration failure: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m.Get(schemaKey) != nil || m.Get(taskBytesKey) != nil || string(m.Get([]byte("owner"))) != "legacy-owner" {
			t.Error("failed migration committed partial metadata")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// schemaSixFixture is a MODEL of version6: it initializes the current schema
// and removes every later addition. It keeps no remote table, rootfloor6,
// and the legacy job/task/request rows unchanged. Genuine predecessor bytes
// come only from the path+SHA fixtures in storage_format_test.go.
func schemaSixFixture(t *testing.T, dir string) backend.Job {
	t.Helper()
	j := legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused"); err != nil {
			return err
		}
		root := artifactRoot{Layout: 1, SchemaFloor: artifactFormat, Policy: "default-v1", ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
		if err := initializeArtifacts(tx, root); err != nil {
			return err
		}
		if err := tx.DeleteBucket(workflowsBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(environmentsBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
			return err
		}
		if err := environmentArtifactStripModel(tx); err != nil {
			return err
		}
		b := tx.Bucket(artifactsBucket)
		if err := b.DeleteBucket([]byte("workflow_inputs")); err != nil {
			return err
		}
		if err := b.DeleteBucket([]byte("remote_attempts")); err != nil {
			return err
		}
		root.SchemaFloor = 6
		raw, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("state"), raw); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(6)); err != nil {
			return err
		}
		return validateSnapshot(tx)
	})
	return j
}

func TestRemoteSchemaSixMigration(t *testing.T) {
	dir := t.TempDir()
	j := schemaSixFixture(t, dir)
	wantJob, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	wantTask, err := backend.TaskBytes(j.Task)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != schemaVersion {
			t.Error("schema marker not migrated")
		}
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if st.Root.SchemaFloor != artifactFormat || st.Root.Layout != 1 || len(st.Remote) != 0 {
			t.Error("incorrect remote migration")
		}
		if tx.Bucket(artifactsBucket).Bucket([]byte("remote_attempts")) == nil {
			t.Error("missing remote table")
		}
		for _, b := range []*bolt.Bucket{tx.Bucket(workflowsBucket), tx.Bucket(artifactsBucket).Bucket([]byte("workflow_inputs"))} {
			if b == nil {
				t.Fatal("missing empty workflow metadata")
			}
			if k, _ := b.Cursor().First(); k != nil || b.Sequence() != 0 {
				t.Fatal("migration invented workflow contents")
			}
		}
		if b := tx.Bucket(environmentsBucket); b == nil || b.Sequence() != 0 || b.Stats().KeyN != 0 {
			t.Fatal("migration omitted or filled environment metadata")
		}
		if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(j.ID)), wantJob) {
			t.Error("accepted job rewritten")
		}
		got, err := get(tx, j.ID)
		if err != nil {
			return err
		}
		task, err := backend.TaskBytes(got.Task)
		if err != nil {
			return err
		}
		if !bytes.Equal(task, wantTask) || sha256.Sum256(task) != sha256.Sum256(wantTask) {
			t.Error("task bytes or digest changed")
		}
		if string(tx.Bucket(requestsBucket).Get([]byte("original-request"))) != j.ID {
			t.Error("request binding changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteSchemaSixRefusesCorruptionBeforeMigration(t *testing.T) {
	for _, kind := range []string{"rootfloor", "extra-remote-table", "missing-old-table", "usage", "job"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			j := schemaSixFixture(t, dir)
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				b := tx.Bucket(artifactsBucket)
				switch kind {
				case "rootfloor":
					var root artifactRoot
					if err := json.Unmarshal(b.Get([]byte("state")), &root); err != nil {
						return err
					}
					root.SchemaFloor = 7
					raw, err := json.Marshal(root)
					if err != nil {
						return err
					}
					return b.Put([]byte("state"), raw)
				case "extra-remote-table":
					_, err := b.CreateBucket([]byte("remote_attempts"))
					return err
				case "missing-old-table":
					return b.DeleteBucket([]byte("reads"))
				case "usage":
					return b.Put([]byte("usage"), []byte("corrupt"))
				default:
					return tx.Bucket(jobsBucket).Put([]byte(j.ID), []byte("invalid JSON"))
				}
			})
			path := filepath.Join(dir, "jobs.db")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if s, err := Open(dir); err == nil {
				s.Close()
				t.Fatal("corrupt version6 migrated")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("refused migration changed database")
			}
		})
	}
}

func TestRemoteSchemaSixMigrationRollback(t *testing.T) {
	dir := t.TempDir()
	schemaSixFixture(t, dir)
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("rollback remote migration")
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused"); err != nil {
			return err
		}
		if schemaNumber(tx) != schemaVersion {
			t.Error("migration did not run")
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("migration: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != 6 {
			t.Error("marker escaped rollback")
		}
		if tx.Bucket(artifactsBucket).Bucket([]byte("remote_attempts")) != nil {
			t.Error("table escaped rollback")
		}
		if tx.Bucket(workflowsBucket) != nil || tx.Bucket(artifactsBucket).Bucket([]byte("workflow_inputs")) != nil {
			t.Error("workflow metadata escaped rollback")
		}
		if tx.Bucket(environmentsBucket) != nil {
			t.Error("environment metadata escaped rollback")
		}
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if st.Root.SchemaFloor != 6 {
			t.Error("rootfloor escaped rollback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteFutureFenceBeforeAnyRecordDecode(t *testing.T) {
	dir := t.TempDir()
	j := schemaSixFixture(t, dir)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1)); err != nil {
			return err
		}
		if err := tx.Bucket(jobsBucket).Put([]byte(j.ID), []byte("invalid job JSON")); err != nil {
			return err
		}
		return tx.Bucket(artifactsBucket).Put([]byte("state"), []byte("invalid root JSON"))
	})
	path := filepath.Join(dir, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if s, err := Open(dir); !errors.Is(err, ErrUnsupportedSchema) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("future schema did not take precedence: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("future refusal wrote database")
	}
}

// Both remote-row and shared completion limits are1024. This checks the
// aggregate encoding boundary, not mutation isolation of either guard.
func TestRemoteRowsAggregateCountBound(t *testing.T) {
	st := &artifactState{Remote: make(map[string]remoteRecord)}
	for i := 0; i < 1024; i++ {
		id := fmt.Sprintf("%032x", i)
		st.Remote[id] = remoteRecord{Job: id, Revision: 1, State: 1, Authority: &remoteAuthority{Principal: "owner", TokenID: strings.Repeat("b", 32), Digest: [32]byte{1}, Policy: [5]int64{600, 600, 86400, 604800, 120}}}
	}
	_, usage, _, err := st.encode()
	if err != nil {
		t.Fatalf("valid1024 rows: %v", err)
	}
	// Encoding is not admission: fixed charges exceed the independent global
	// metadata limit before the theoretical aggregate row boundary.
	if err := artifactAdmission(artifactUsage{}, usage, false); err == nil {
		t.Fatal("global metadata budget ignored")
	}
	id := fmt.Sprintf("%032x", 1024)
	r := st.Remote[fmt.Sprintf("%032x", 0)]
	r.Job = id
	st.Remote[id] = r
	if _, _, _, err := st.encode(); err == nil {
		t.Fatal("valid1025 rows accepted")
	}
}

func TestRemoteMetadataChargeAndSharedWorkspaceAdmission(t *testing.T) {
	st := &artifactState{Root: artifactRoot{Unit: 4096}, Remote: make(map[string]remoteRecord)}
	id := strings.Repeat("a", 32)
	accepted := remoteRecord{Job: id, Revision: 1, State: 1, Authority: &remoteAuthority{Principal: "owner", TokenID: strings.Repeat("b", 32), Digest: [32]byte{1}, Policy: [5]int64{600, 600, 86400, 604800, 120}}}
	st.Remote[id] = accepted
	rows, usage, owners, err := st.encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows["remote_attempts"][id]) >= 4096 {
		t.Fatal("small accepted fixture unexpectedly fills row")
	}
	want := uint64(4096 + 32 + 80 + len("owner"))
	if owners["owner"][aMetadataBytes] != want || usage[aMetadataBytes] != artifactRootUsage(st.Root)[aMetadataBytes]+want {
		t.Fatal("remote metadata not charged at fixed row bound")
	}
	if owners["owner"][aMetadataRecords] != 2 || usage[aWorkspaces] != 1 {
		t.Fatal("remote admission charge missing")
	}
	if usage[aAllocatedBytes] != 2*st.Root.Unit || usage[aFiles] != 2 {
		t.Fatal("remote Pod charged local filesystem allocation")
	}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("%032x", i)
		r := accepted
		r.Job = key
		st.Remote[key] = r
	}
	_, after, _, err := st.encode()
	if err != nil {
		t.Fatal(err)
	}
	if after[aWorkspaces] != 65 {
		t.Fatal("remote accepted rows not charged")
	}
	if err := artifactAdmission(usage, after, false); err == nil {
		t.Fatal("shared workspace capacity ignored")
	}
	// The same domain includes local workspace charges without a remote-only
	// exemption. Retention does not participate in this admission arithmetic.
	mixed := usage
	mixed[aWorkspaces] = 65
	if err := artifactAdmission(artifactUsage{}, mixed, false); err == nil {
		t.Fatal("mixed workspace capacity ignored")
	}
}

func TestArtifactCloneDetachesRemoteProof(t *testing.T) {
	id := strings.Repeat("a", 32)
	when := time.Unix(100, 0).UTC()
	r := remoteRecord{Authority: &remoteAuthority{Principal: "owner"}, Resources: remoteResources{Pod: &remoteResource{Name: "pod", UID: "uid"}}, Active: &remoteActive{Input: &remoteLease{Epoch: 1}, TaskDeadline: &when, Stop: &remoteTaskStop{Stage: &remoteExit{ID: "stage"}}}, Done: &remoteDecision{Collector: &remoteExit{ID: "collector"}}}
	before := &artifactState{Remote: map[string]remoteRecord{id: r}}
	after := before.clone()
	v := after.Remote[id]
	v.Authority.Principal = "changed"
	v.Resources.Pod.UID = "changed"
	v.Active.Input.Epoch = 2
	*v.Active.TaskDeadline = when.Add(time.Second)
	v.Active.Stop.Stage.ID = "changed"
	v.Done.Collector.ID = "changed"
	got := before.Remote[id]
	if got.Authority.Principal != "owner" || got.Resources.Pod.UID != "uid" || got.Active.Input.Epoch != 1 || !got.Active.TaskDeadline.Equal(when) || got.Active.Stop.Stage.ID != "stage" || got.Done.Collector.ID != "collector" {
		t.Fatal("remote clone aliases nested proof")
	}
}
