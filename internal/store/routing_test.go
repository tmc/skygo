package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These text fixtures preserve exact logical buckets emitted by the signed
// predecessor's Store APIs. They are not native workload execution receipts.
type fixtureEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
type fixtureBucket struct {
	Name     []byte          `json:"name"`
	Sequence uint64          `json:"sequence"`
	Entries  []fixtureEntry  `json:"entries"`
	Buckets  []fixtureBucket `json:"buckets"`
}

func routingFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil || len(data) > 1<<20 {
		t.Fatalf("read bounded legacy fixture: %v", err)
	}
	var buckets []fixtureBucket
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&buckets); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: true})
	if err != nil {
		t.Fatal(err)
	}
	var fill func(*bolt.Bucket, fixtureBucket, int) error
	fill = func(b *bolt.Bucket, raw fixtureBucket, depth int) error {
		if depth > 8 || len(raw.Entries) > 100 || len(raw.Buckets) > 32 {
			return errors.New("legacy fixture exceeds bounds")
		}
		if err := b.SetSequence(raw.Sequence); err != nil {
			return err
		}
		for _, e := range raw.Entries {
			if err := b.Put(e.Key, e.Value); err != nil {
				return err
			}
		}
		for _, child := range raw.Buckets {
			c, err := b.CreateBucket(child.Name)
			if err != nil {
				return err
			}
			if err := fill(c, child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if len(buckets) > 32 {
			return errors.New("too many legacy fixture buckets")
		}
		for _, raw := range buckets {
			b, err := tx.CreateBucket(raw.Name)
			if err != nil {
				return err
			}
			if err := fill(b, raw, 0); err != nil {
				return err
			}
		}
		return nil
	})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("restore logical fixture: %v %v", err, closeErr)
	}
	return dir
}

// routingState preserves bucket entries and sequence counters byte-for-byte,
// ignoring only the format marker whose upgrade is asserted separately.
func routingState(t *testing.T, db *bolt.DB) []byte {
	t.Helper()
	var export func([]byte, *bolt.Bucket) fixtureBucket
	export = func(name []byte, b *bolt.Bucket) fixtureBucket {
		out := fixtureBucket{Name: bytes.Clone(name), Sequence: b.Sequence()}
		if err := b.ForEach(func(k, v []byte) error {
			if bytes.Equal(name, metadataBucket) && bytes.Equal(k, schemaKey) {
				return nil
			}
			if v == nil {
				out.Buckets = append(out.Buckets, export(k, b.Bucket(k)))
			} else {
				out.Entries = append(out.Entries, fixtureEntry{bytes.Clone(k), bytes.Clone(v)})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	var out []fixtureBucket
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(k []byte, b *bolt.Bucket) error { out = append(out, export(k, b)); return nil })
	}); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRoutingUpgradePreservesModeAndAllBuckets(t *testing.T) {
	for _, name := range []string{"schema1", "schema2"} {
		t.Run(name, func(t *testing.T) {
			dir := routingFixture(t, name)
			editFixture(t, dir, true, func(tx *bolt.Tx) error {
				if err := tx.Bucket(sequenceBucket).SetSequence(maxJobs); err != nil {
					return err
				}
				return tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(maxTaskBytes))
			})
			db, err := openSnapshot(filepath.Join(dir, "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			before := routingState(t, db)
			db.Close()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.Scoped() != (name == "schema2") || !bytes.Equal(before, routingState(t, s.db)) {
				t.Fatal("format upgrade changed mode or non-format bucket contents")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if !bytes.Equal(tx.Bucket(metadataBucket).Get(schemaKey), sequenceKey(3)) {
					t.Error("missing format-3 fence")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			original, err := s.List(0, 3, 3)
			if err != nil || len(original) != 3 {
				t.Fatal("legacy fixture rows missing", err)
			}
			job, found, err := s.Lookup(testActor(), "queued-local", original[0].Task)
			if err != nil || !found || job.TargetBinding != "" {
				t.Fatalf("legacy replay: found=%v err=%v", found, err)
			}
			again, err := s.Submit(testActor(), "queued-local", job.Task, "", "")
			if err != nil || again.ID != job.ID || again.Target != "local" || again.TargetBinding != "" {
				t.Fatal("replay validated today's route or rebound old work", err)
			}
			if _, err := s.Submit(testActor(), "new", job.Task, "local", "new-binding"); !errors.Is(err, ErrFull) {
				t.Fatalf("upgrade reset quota: %v", err)
			}
		})
	}
}

func TestRoutingRejectsMalformedStateWithoutWrites(t *testing.T) {
	for _, kind := range []string{"counter-missing", "mode-empty", "mode-zero", "mode-bucket", "mode-relations", "binding-long", "binding-nul", "old-binding", "old-limits", "old-selectors", "old-labels"} {
		t.Run(kind, func(t *testing.T) {
			dir := routingFixture(t, "schema1")
			editFixture(t, dir, true, func(tx *bolt.Tx) error {
				m := tx.Bucket(metadataBucket)
				if !strings.HasPrefix(kind, "old-") {
					if err := m.Put(schemaKey, sequenceKey(3)); err != nil {
						return err
					}
				}
				switch kind {
				case "counter-missing":
					return m.Delete(taskBytesKey)
				case "mode-empty":
					return m.Put(scopedKey, []byte{})
				case "mode-zero":
					return m.Put(scopedKey, []byte{0})
				case "mode-bucket":
					_, err := m.CreateBucket(scopedKey)
					return err
				case "mode-relations":
					_, err := tx.CreateBucket(jobPrincipalsBucket)
					return err
				}
				_, raw := tx.Bucket(jobsBucket).Cursor().First()
				var job backend.Job
				if err := json.Unmarshal(raw, &job); err != nil {
					return err
				}
				switch kind {
				case "binding-long":
					job.TargetBinding = strings.Repeat("b", 257)
				case "binding-nul":
					job.TargetBinding = "bad\x00binding"
				case "old-binding":
					job.TargetBinding = "unsupported-binding"
				case "old-limits":
					job.Task.CPULimitMillis = 1
				case "old-selectors":
					job.Task.NodeSelector = map[string]string{"key": "value"}
				case "old-labels":
					job.Task.TargetLabels = map[string]string{"key": "value"}
				}
				data, err := json.Marshal(job)
				if err != nil {
					return err
				}
				return tx.Bucket(jobsBucket).Put([]byte(job.ID), data)
			})
			path := filepath.Join(dir, "jobs.db")
			before, _ := os.ReadFile(path)
			if s, err := Open(dir); err == nil {
				s.Close()
				t.Fatal("corrupt routing state opened")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("refused routing state changed bytes")
			}
		})
	}
}

func TestRoutingBindingAdmissionAndImmutability(t *testing.T) {
	f := newScopeFixture(t)
	a := f.actors["alice"]
	task := backend.Task{Argv: []string{"work"}, CPUMillis: 1, CPULimitMillis: 2, TargetLabels: map[string]string{"location": "lab"}, NodeSelector: map[string]string{"pool": "cpu"}}
	first, err := f.s.Submit(a, "one", task, "cpu", "accepted-binding")
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range []string{"", strings.Repeat("b", 257), "b\x00", string([]byte{0xff})} {
		before := routingState(t, f.s.db)
		if _, err := f.s.Submit(a, "new", task, "cpu", binding); err == nil {
			t.Fatal("invalid new binding accepted")
		}
		again, err := f.s.Submit(a, "one", task, "", binding)
		if err != nil || again.ID != first.ID || again.TargetBinding != "accepted-binding" {
			t.Fatal("retry consulted current route", err)
		}
		if !bytes.Equal(before, routingState(t, f.s.db)) {
			t.Fatal("rejected admission or replay changed state")
		}
	}
	for _, update := range []func(string, func(*backend.Job) error) (backend.Job, error){f.s.Update, func(id string, fn func(*backend.Job) error) (backend.Job, error) { return f.s.UpdateFor(a, id, fn) }} {
		if _, err := update(first.ID, func(j *backend.Job) error { j.TargetBinding = "replacement"; return nil }); err == nil {
			t.Fatal("update changed immutable binding")
		}
	}
	changed := task
	changed.MemoryBytes, changed.MemoryLimitBytes = 1, 2
	if _, err := f.s.Submit(a, "one", changed, "cpu", "accepted-binding"); !errors.Is(err, ErrConflict) {
		t.Fatalf("extension absent from request equality: %v", err)
	}
}

func TestBindLegacyLocal(t *testing.T) {
	dir := routingFixture(t, "schema1")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before, err := s.List(0, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindLegacyLocal("wrong-binding"); err == nil {
		t.Fatal("migration accepted an unrelated installation binding")
	}
	original := routingState(t, s.db)
	injected := errors.New("injected migration failure")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := bindLegacyLocal(tx, binding); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) || !bytes.Equal(original, routingState(t, s.db)) {
		t.Fatal("failed local migration partially committed", err)
	}
	for i := 0; i < 2; i++ {
		if err := s.BindLegacyLocal(binding); err != nil {
			t.Fatal(err)
		}
		for _, old := range before {
			got, err := s.Get(old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if old.Target == "local" {
				old.TargetBinding = binding
			}
			want, _ := json.Marshal(old)
			raw, _ := json.Marshal(got)
			if !bytes.Equal(want, raw) {
				t.Fatal("local migration changed fields beyond the proven binding")
			}
		}
	}
}

func TestRoutingScopedActivationAndSnapshot(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "scoped"}[scoped], func(t *testing.T) {
			dir := routingFixture(t, "schema1")
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if scoped {
				if err := s.ActivateScoped(); err != nil {
					t.Fatal(err)
				}
			}
			binding, err := backend.LocalBinding(s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			if err := s.BindLegacyLocal(binding); err != nil {
				t.Fatal(err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if !bytes.Equal(tx.Bucket(metadataBucket).Get(schemaKey), sequenceKey(3)) {
					t.Error("scoped activation downgraded format")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before, owner := routingState(t, s.db), s.Owner()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			base := t.TempDir()
			backup, restored := filepath.Join(base, "snapshot.db"), filepath.Join(base, "restored")
			if err := Backup(dir, backup); err != nil {
				t.Fatal(err)
			}
			if err := Restore(backup, restored); err != nil {
				t.Fatal(err)
			}
			s, err = Open(restored)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.Owner() != owner || s.Scoped() != scoped || !bytes.Equal(before, routingState(t, s.db)) {
				t.Fatal("snapshot changed format3 mode, identity or bucket bytes")
			}
		})
	}
}

func TestRoutingPriorBinaryRefusal(t *testing.T) {
	exe := os.Getenv("SKYGO_S02_BINARY")
	if exe == "" {
		t.Skip("requires exact archived signed S02 binary")
	}
	want := os.Getenv("SKYGO_S02_SHA256")
	binary, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binary)
	if len(want) != 64 || hex.EncodeToString(sum[:]) != want {
		t.Fatal("prior binary hash mismatch")
	}
	for _, name := range []string{"schema1", "schema2"} {
		t.Run(name, func(t *testing.T) {
			dir := routingFixture(t, name)
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "server", "--listen", "127.0.0.1:0", "--data-dir", dir)
			cmd.Env = append(os.Environ(), "SKYGO_TOKEN=")
			out, runErr := cmd.CombinedOutput()
			if runErr == nil || ctx.Err() != nil || !bytes.Contains(out, []byte("unsupported database schema")) {
				t.Fatal("prior binary did not promptly refuse actual format3 state", runErr)
			}
			after, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("prior binary refusal modified format3 state", err)
			}
			t.Logf("prior SHA256 %s refused format3 %s state SHA256 %x unchanged", want, name, sha256.Sum256(before))
		})
	}
}
