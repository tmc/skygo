package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions on a scoped, retention-enabled
// store. The legacy record in TestEnvironmentRetirementMigration is a MODEL:
// a domain record rewritten by hand into the pre-retirement form at marker 11
// (environmentArtifactSchema), keeping the storage format tag.

func (f *workerFixture) acceptEnvironment(t *testing.T, who, request string, spec environment.Spec) Environment {
	t.Helper()
	e, err := f.s.AcceptEnvironment(f.actors[who], f.domain, request, spec, "test-binding", f.authority[who], environmentCheckpoint("created"))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (f *workerFixture) deleteEnvironment(t *testing.T, e Environment) Environment {
	t.Helper()
	for _, p := range []environment.Phase{environment.Deleting, environment.Deleted} {
		var err error
		e, err = f.s.CommitEnvironment(e.Identity.ID, e.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: p}, Checkpoint: e.Checkpoint})
		if err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func (f *workerFixture) valid(t *testing.T) {
	t.Helper()
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

// environmentRows returns the environment rows with prefix.
func environmentRows(t *testing.T, s *Store, prefix string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(environmentsBucket).Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, v = c.Next() {
			out[string(k)] = string(v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEnvironmentRetirementMigration(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("legacy")
	e := f.acceptEnvironment(t, "alice", "old-1", spec)
	// MODEL: rewrite the record as a pre-retirement one and the marker as 11.
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		r, err := loadEnvironmentRecord(b, e.Identity.ID)
		if err != nil {
			return err
		}
		r.Domain = nil
		data, err := encodeEnvironmentRecord(r)
		if err != nil {
			return err
		}
		if err := b.Put(environmentRecordKey(r.Identity.ID), data); err != nil {
			return err
		}
		if err := b.Delete(environmentWitnessKey("alice", f.domain, "old-1")); err != nil {
			return err
		}
		if err := b.Put(environmentRequestKey("alice", "old-1"), []byte(r.Identity.ID)); err != nil {
			return err
		}
		if err := servingStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentArtifactSchema))
	}); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	f.reopen(t)
	f.valid(t)
	after := workflowDatabase(t, f.s)
	// Schema13 adds the serving tag and empty serving family; schema14 adds
	// the artifact Begin tag and, when installed, an empty request table.
	// Schema15 adds only the worker barrier tag to this modeled predecessor.
	if after[string(metadataBucket)+"/"+string(workerBarrierStorageFormatKey)] != workerBarrierStorageFormat {
		t.Error("migration omitted the worker barrier storage format tag")
	}
	delete(after, string(metadataBucket)+"/"+string(workerBarrierStorageFormatKey))
	if after[string(metadataBucket)+"/"+string(artifactBeginStorageFormatKey)] != artifactBeginStorageFormat {
		t.Error("migration omitted the artifact Begin storage format tag")
	}
	delete(after, string(metadataBucket)+"/"+string(artifactBeginStorageFormatKey))
	table := string(artifactsBucket) + "/" + string(artifactBeginRequestsTable) + "\x00sequence"
	if _, installed := before[string(artifactsBucket)+"\x00sequence"]; installed {
		if after[table] != "0" {
			t.Error("migration omitted the empty artifact Begin request table")
		}
		delete(after, table)
	} else if _, installed := after[table]; installed {
		t.Error("migration installed an artifact Begin table without artifacts")
	}
	if after[string(metadataBucket)+"/"+string(servingStorageFormatKey)] != servingStorageFormat {
		t.Error("migration omitted the serving storage format tag")
	}
	delete(after, string(metadataBucket)+"/"+string(servingStorageFormatKey))
	if after[string(servingServicesBucket)+"\x00sequence"] != "0" {
		t.Error("migration omitted the empty serving family")
	}
	delete(after, string(servingServicesBucket)+"\x00sequence")
	for k, v := range before {
		if k != string(metadataBucket)+"/"+string(schemaKey) && after[k] != v {
			t.Errorf("migration changed %q", k)
		}
	}
	if len(after) != len(before) {
		t.Errorf("migration wrote %d rows, had %d", len(after), len(before))
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != schemaVersion {
			return fmt.Errorf("marker %d", schemaNumber(tx))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"w/", "t/", "x/"} {
		if rows := environmentRows(t, f.s, p); len(rows) != 0 {
			t.Errorf("migration wrote %s rows %v", p, rows)
		}
	}
	got, err := f.s.EnvironmentFor(f.actors["alice"], e.Identity.ID)
	if err != nil || got.Domain != nil {
		t.Fatalf("legacy view %+v %v", got, err)
	}
	// A domain request never matches the legacy row; the name still does.
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "old-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("c")); !errors.Is(err, ErrEnvironmentReserved) {
		t.Fatalf("same name in a domain: %v", err)
	}
	if _, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "old-1", spec); found || err != nil {
		t.Fatalf("lookup matched the legacy row: %v %v", found, err)
	}
	other := f.acceptEnvironment(t, "alice", "old-1", environmentSpec("new"))
	if other.Identity.ID == e.Identity.ID {
		t.Fatal("domain request replayed the legacy row")
	}
	gone := f.deleteEnvironment(t, got)
	before = workflowDatabase(t, f.s)
	if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision); !errors.Is(err, ErrEnvironmentLegacy) {
		t.Fatalf("legacy reclaim: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	f.reopen(t)
	f.valid(t)
}

func TestEnvironmentDomainReplay(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	e := f.acceptEnvironment(t, "alice", "create-1", spec)
	if e.Domain == nil || *e.Domain != f.domain {
		t.Fatalf("domain %+v", e.Domain)
	}
	if rows := environmentRows(t, f.s, "r/"); len(rows) != 0 {
		t.Fatalf("domain record has a legacy binding: %v", rows)
	}
	changed := spec
	changed.Image = "task:v2"
	for range 2 {
		before := workflowDatabase(t, f.s)
		again := f.acceptEnvironment(t, "alice", "create-1", spec)
		if again.Identity != e.Identity || again.Revision != e.Revision {
			t.Fatalf("replay %+v", again)
		}
		got, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec)
		if err != nil || !found || got.Identity != e.Identity {
			t.Fatalf("lookup %+v %v %v", got, found, err)
		}
		if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", changed, "test-binding", f.authority["alice"], environmentCheckpoint("c")); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed spec: %v", err)
		}
		if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", changed); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed lookup: %v", err)
		}
		if _, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-2", spec); found || err != nil {
			t.Fatalf("unknown lookup: %v %v", found, err)
		}
		// Another principal's request ID is its own.
		if _, found, err := f.s.EnvironmentRequest(f.actors["bob"], f.domain, "create-1", spec); found || err != nil {
			t.Fatalf("bob lookup: %v %v", found, err)
		}
		workflowUnchanged(t, f.s, before)
		f.reopen(t)
		f.valid(t)
	}
}

func TestEnvironmentDomainRefusals(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	for _, tt := range []struct {
		name string
		d    RequestDomain
		ok   func(error) bool
	}{
		{"epoch zero", RequestDomain{f.domain.Lineage, 0}, func(err error) bool { return errors.Is(err, ErrInvalidRequestDomain) }},
		{"lineage form", RequestDomain{"x", f.domain.Epoch}, func(err error) bool { return errors.Is(err, ErrInvalidRequestDomain) }},
		{"other lineage", RequestDomain{fmt.Sprintf("%032x", 7), f.domain.Epoch}, func(err error) bool {
			var x *ExpiryError
			return errors.As(err, &x) && x.Reason == RequestLineageChanged
		}},
		{"unknown epoch", RequestDomain{f.domain.Lineage, f.domain.Epoch + 1}, func(err error) bool { return errors.Is(err, ErrUnknownRequestEpoch) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := workflowDatabase(t, f.s)
			if _, err := f.s.AcceptEnvironment(f.actors["alice"], tt.d, "r", spec, "test-binding", f.authority["alice"], environmentCheckpoint("c")); !tt.ok(err) {
				t.Errorf("Accept: %v", err)
			}
			if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], tt.d, "r", spec); !tt.ok(err) {
				t.Errorf("lookup: %v", err)
			}
			workflowUnchanged(t, f.s, before)
		})
	}
}

func TestEnvironmentEpochClosedAndRetired(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	e := f.acceptEnvironment(t, "alice", "create-1", spec)
	info, err := f.s.RetentionInfo(f.actors["alice"])
	if err != nil {
		t.Fatal(err)
	}
	expired := func(err error) bool {
		var x *ExpiryError
		return errors.As(err, &x) && x.Reason == RequestEpochExpired
	}
	// Closed to new requests: an exact replay still replays.
	f.s.now = func() time.Time { return info.AcceptUntil }
	before := workflowDatabase(t, f.s)
	if got, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec); err != nil || !found || got.Identity != e.Identity {
		t.Fatalf("closed lookup %v %v", found, err)
	}
	if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-2", environmentSpec("other")); !expired(err) {
		t.Fatalf("closed new lookup: %v", err)
	}
	// The lookup evaluates the phase change without storing it.
	workflowUnchanged(t, f.s, before)
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-2", environmentSpec("other"), "test-binding", f.authority["alice"], environmentCheckpoint("c")); !expired(err) {
		t.Fatalf("closed new accept: %v", err)
	}
	// The refusal rolls back the phase change with everything else.
	workflowUnchanged(t, f.s, before)
	again := f.acceptEnvironment(t, "alice", "create-1", spec)
	if again.Identity != e.Identity {
		t.Fatal("closed replay")
	}
	// Retired: even an exact replay is refused.
	f.s.now = func() time.Time { return info.RetryUntil }
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("created")); !expired(err) {
		t.Fatalf("retired replay: %v", err)
	}
	if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec); !expired(err) {
		t.Fatalf("retired lookup: %v", err)
	}
}

func TestEnvironmentReclaim(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	spec.StorageAlias = "scratch"
	e := f.acceptEnvironment(t, "alice", "create-1", spec)
	before := workflowDatabase(t, f.s)
	if _, err := f.s.ReclaimEnvironment(e.Identity.ID, e.Revision); err == nil {
		t.Fatal("present environment reclaimed")
	}
	workflowUnchanged(t, f.s, before)
	gone := f.deleteEnvironment(t, e)
	before = workflowDatabase(t, f.s)
	if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision-1); !errors.Is(err, ErrEnvironmentStale) {
		t.Fatalf("stale reclaim: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	retired, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if !retired.Retired || retired.Identity != e.Identity || retired.Revision != gone.Revision+1 || retired.State != (environment.State{Desired: environment.Absent, Phase: environment.Deleted}) || retired.SpecDigest != e.SpecDigest || *retired.Domain != f.domain {
		t.Fatalf("retired view %+v", retired)
	}
	environmentRetiredSpec(t, retired, e)
	if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, retired.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second reclaim: %v", err)
	}
	f.reopen(t)
	f.valid(t)
	if _, err := f.s.EnvironmentFor(f.actors["alice"], e.Identity.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reclaimed read: %v", err)
	}
	// The request replays the retired view, never new work.
	before = workflowDatabase(t, f.s)
	replay := f.acceptEnvironment(t, "alice", "create-1", spec)
	if !replay.Retired || replay.Identity != e.Identity || replay.Revision != retired.Revision {
		t.Fatalf("replay %+v", replay)
	}
	environmentRetiredSpec(t, replay, e)
	got, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec)
	if err != nil || !found || !got.Retired {
		t.Fatalf("lookup %+v %v %v", got, found, err)
	}
	environmentRetiredSpec(t, got, e)
	changed := spec
	changed.Image = "task:v2"
	if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed retired lookup: %v", err)
	}
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", changed, "test-binding", f.authority["alice"], environmentCheckpoint("c")); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed retired replay: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// The Store keeps the digest, never the spec bytes.
	if rows := environmentRows(t, f.s, "s/"); len(rows) != 0 {
		t.Fatalf("spec rows %v", rows)
	}
	for _, v := range environmentRows(t, f.s, "t/") {
		if strings.Contains(v, spec.Image) {
			t.Fatalf("tombstone holds spec bytes: %s", v)
		}
	}
	// The name and storage alias are reusable.
	reused := f.acceptEnvironment(t, "alice", "create-2", spec)
	if reused.Identity.ID == e.Identity.ID || reused.Retired {
		t.Fatalf("reuse %+v", reused)
	}
	if got := environmentRows(t, f.s, "x/"); got["x/alice"] != string(sequenceKey(1)) {
		t.Fatalf("reclaimed count %q", got)
	}
	f.reopen(t)
	f.valid(t)
}

// environmentRetiredSpec checks that retired view v carries a valid public
// spec equal to original's, rebuilt from the replay spec, whose canonical
// bytes hash exactly to the stored digest.
func environmentRetiredSpec(t *testing.T, v, original Environment) {
	t.Helper()
	if !reflect.DeepEqual(v.Spec, original.Spec) {
		t.Fatalf("retired spec %+v, want %+v", v.Spec, original.Spec)
	}
	raw, err := environment.Canonical(v.Spec)
	if err != nil {
		t.Fatalf("retired spec invalid: %v", err)
	}
	if sha256.Sum256(raw) != v.SpecDigest || v.SpecDigest != original.SpecDigest {
		t.Fatal("retired spec digest mismatch")
	}
	if v.Authority != (auth.Delegation{}) || v.Checkpoint.Kind != "" || v.Checkpoint.Data != nil || v.Incarnation != "" || v.IncarnationSeq != 0 {
		t.Fatalf("retired view leaks server state %+v", v)
	}
}

// TestEnvironmentTombstoneSpecBound shows that replay checks the replayed
// spec against the tombstone, not only against the witness: a tombstone
// whose name no longer matches the spec refuses replay as corrupt.
func TestEnvironmentTombstoneSpecBound(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	e := f.deleteEnvironment(t, f.acceptEnvironment(t, "alice", "create-1", spec))
	if _, err := f.s.ReclaimEnvironment(e.Identity.ID, e.Revision); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		tomb, err := decodeEnvironmentTombstone(b.Get(environmentTombstoneKey(e.Identity.ID)))
		if err != nil {
			return err
		}
		tomb.Name = "other"
		raw, err := encodeEnvironmentTombstone(tomb)
		if err != nil {
			return err
		}
		return b.Put(environmentTombstoneKey(e.Identity.ID), raw)
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec); err == nil || errors.Is(err, ErrConflict) {
		t.Fatalf("lookup %v %v, want corruption", found, err)
	}
	if v, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("c")); err == nil || errors.Is(err, ErrConflict) {
		t.Fatalf("replay %+v %v, want corruption", v, err)
	}
}

// TestEnvironmentTombstoneFieldShapes writes, bypassing the encoder, a
// reclaimed tombstone whose name or target Spec would refuse, as the raw
// json.Marshal of the mutated typed row. The encoder, the decoder, the
// snapshot validator and replay all refuse it.
func TestEnvironmentTombstoneFieldShapes(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*environmentTombstone)
	}{
		{"name 64", func(tomb *environmentTombstone) { tomb.Name = "a" + strings.Repeat("b", 63) }},
		{"name upper", func(tomb *environmentTombstone) { tomb.Name = "Gone" }},
		{"name slash", func(tomb *environmentTombstone) { tomb.Name = "a/b" }},
		{"name leading digit", func(tomb *environmentTombstone) { tomb.Name = "1gone" }},
		{"name trailing dash", func(tomb *environmentTombstone) { tomb.Name = "gone-" }},
		{"name empty", func(tomb *environmentTombstone) { tomb.Name = "" }},
		{"target 129", func(tomb *environmentTombstone) { tomb.Target = strings.Repeat("t", 129) }},
		{"target NUL", func(tomb *environmentTombstone) { tomb.Target = "kube\x00" }},
		{"target newline", func(tomb *environmentTombstone) { tomb.Target = "kube\n" }},
		{"target DEL", func(tomb *environmentTombstone) { tomb.Target = "kube\x7f" }},
		{"target non-ASCII", func(tomb *environmentTombstone) { tomb.Target = "kubé" }},
		{"target empty", func(tomb *environmentTombstone) { tomb.Target = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWorkerFixture(t)
			spec := environmentSpec("gone")
			gone := f.deleteEnvironment(t, f.acceptEnvironment(t, "alice", "a-1", spec))
			if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision); err != nil {
				t.Fatal(err)
			}
			f.valid(t)
			key := environmentTombstoneKey(gone.Identity.ID)
			var raw []byte
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(environmentsBucket)
				tomb, err := decodeEnvironmentTombstone(b.Get(key))
				if err != nil {
					return err
				}
				tt.edit(&tomb)
				if _, err := encodeEnvironmentTombstone(tomb); err == nil {
					return errors.New("encoder accepted the malformed tombstone")
				}
				if raw, err = json.Marshal(tomb); err != nil {
					return err
				}
				return b.Put(key, raw)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := decodeEnvironmentTombstone(raw); err == nil {
				t.Fatalf("decoder accepted %s", raw)
			}
			if err := f.s.db.View(validateSnapshot); err == nil {
				t.Fatal("snapshot accepted the malformed tombstone")
			}
			if _, _, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "a-1", spec); err == nil || errors.Is(err, ErrConflict) {
				t.Fatalf("lookup %v, want corruption", err)
			}
			if v, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "a-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("c")); err == nil || errors.Is(err, ErrConflict) {
				t.Fatalf("replay %+v %v, want corruption", v, err)
			}
		})
	}
}

// TestEnvironmentTombstoneBoundaryFields reclaims an environment whose name
// and target are at their maximum lengths. Its tombstone is exactly the
// json.Marshal of the typed row, stays valid across reopen, and replays the
// retired view with the detached spec.
func TestEnvironmentTombstoneBoundaryFields(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("a" + strings.Repeat("b", 62))
	spec.Target = "a ~" + strings.Repeat("t", 125)
	if !environment.ValidName(spec.Name) || !environment.ValidTarget(spec.Target) || len(spec.Name) != 63 || len(spec.Target) != 128 {
		t.Fatalf("boundary spec %q %q", spec.Name, spec.Target)
	}
	e := f.acceptEnvironment(t, "alice", "create-1", spec)
	gone := f.deleteEnvironment(t, e)
	retired, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision)
	if err != nil {
		t.Fatal(err)
	}
	environmentRetiredSpec(t, retired, e)
	stored := environmentRows(t, f.s, "t/")[string(environmentTombstoneKey(e.Identity.ID))]
	tomb, err := decodeEnvironmentTombstone([]byte(stored))
	if err != nil {
		t.Fatal(err)
	}
	if tomb.Name != spec.Name || tomb.Target != spec.Target {
		t.Fatalf("tombstone name %q target %q", tomb.Name, tomb.Target)
	}
	enc, err := encodeEnvironmentTombstone(tomb)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(tomb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(enc, want) || string(enc) != stored {
		t.Fatalf("encoding %s, want %s, stored %s", enc, want, stored)
	}
	if !strings.Contains(stored, `,"name":"`+spec.Name+`","target":"`+spec.Target+`","binding":`) {
		t.Fatalf("tombstone field order %s", stored)
	}
	f.valid(t)
	f.reopen(t)
	f.valid(t)
	got, found, err := f.s.EnvironmentRequest(f.actors["alice"], f.domain, "create-1", spec)
	if err != nil || !found || !got.Retired {
		t.Fatalf("lookup %+v %v %v", got, found, err)
	}
	environmentRetiredSpec(t, got, e)
	replay := f.acceptEnvironment(t, "alice", "create-1", spec)
	if !replay.Retired || replay.Identity != e.Identity || replay.Revision != retired.Revision {
		t.Fatalf("replay %+v", replay)
	}
	environmentRetiredSpec(t, replay, e)
}

func TestEnvironmentLiveLimit(t *testing.T) {
	f := newWorkerFixture(t)
	var first Environment
	for i := range maxPrincipalEnvironments {
		e := f.acceptEnvironment(t, "alice", fmt.Sprintf("r%d", i), environmentSpec(fmt.Sprintf("e%d", i)))
		if i == 0 {
			first = e
		}
	}
	over := func() error {
		_, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "over", environmentSpec("over"), "test-binding", f.authority["alice"], environmentCheckpoint("c"))
		return err
	}
	if err := over(); !errors.Is(err, ErrEnvironmentLimit) {
		t.Fatalf("fifth: %v", err)
	}
	// Deletion alone frees nothing.
	gone := f.deleteEnvironment(t, first)
	if err := over(); !errors.Is(err, ErrEnvironmentLimit) {
		t.Fatalf("fifth after delete: %v", err)
	}
	if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision); err != nil {
		t.Fatal(err)
	}
	if err := over(); err != nil {
		t.Fatalf("fifth after reclaim: %v", err)
	}
	if err := over(); err != nil {
		t.Fatalf("replay after reclaim: %v", err)
	}
	f.reopen(t)
	f.valid(t)
}

func TestEnvironmentTombstoneBound(t *testing.T) {
	f := newWorkerFixture(t)
	reclaim := func(i int) error {
		e := f.acceptEnvironment(t, "alice", fmt.Sprintf("r%d", i), environmentSpec("analysis"))
		e = f.deleteEnvironment(t, e)
		_, err := f.s.ReclaimEnvironment(e.Identity.ID, e.Revision)
		return err
	}
	for i := range maxEnvironmentTombstones {
		if err := reclaim(i); err != nil {
			t.Fatalf("reclaim %d: %v", i, err)
		}
	}
	e := f.acceptEnvironment(t, "alice", "last", environmentSpec("analysis"))
	e = f.deleteEnvironment(t, e)
	before := workflowDatabase(t, f.s)
	if _, err := f.s.ReclaimEnvironment(e.Identity.ID, e.Revision); !errors.Is(err, ErrEnvironmentLimit) {
		t.Fatalf("reclaim past the bound: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Witnesses of the current epoch are kept, so their tombstones are too.
	if n, err := f.s.PruneEnvironmentRequests(100); err != nil || n != 0 {
		t.Fatalf("prune %d %v", n, err)
	}
	f.reopen(t)
	f.valid(t)
}

func TestEnvironmentPruneRetiredLineage(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("analysis")
	e := f.deleteEnvironment(t, f.acceptEnvironment(t, "alice", "create-1", spec))
	if _, err := f.s.ReclaimEnvironment(e.Identity.ID, e.Revision); err != nil {
		t.Fatal(err)
	}
	live := f.acceptEnvironment(t, "alice", "create-2", environmentSpec("kept"))
	// A restored copy has a fresh lineage: the old witnesses can no longer
	// replay, so the reclaimed one and its tombstone are pruned. The live
	// record keeps its witness, and the reclaimed count stays.
	r := restoreEnvironments(t, f)
	defer r.Close()
	if _, err := r.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("created")); err == nil {
		t.Fatal("old lineage request accepted")
	}
	if n, err := r.PruneEnvironmentRequests(10); err != nil || n != 2 {
		t.Fatalf("prune %d %v", n, err)
	}
	if rows := environmentRows(t, r, "t/"); len(rows) != 0 {
		t.Errorf("t/ rows left: %v", rows)
	}
	kept := string(environmentWitnessKey("alice", f.domain, "create-2"))
	if rows := environmentRows(t, r, "w/"); len(rows) != 1 || rows[kept] == "" {
		t.Errorf("w/ rows left: %v, want only %q", rows, kept)
	}
	if rows := environmentRows(t, r, "x/"); rows["x/alice"] != string(sequenceKey(1)) {
		t.Errorf("reclaimed count %v", rows)
	}
	if got, err := r.EnvironmentFor(f.actors["alice"], live.Identity.ID); err != nil || got.Identity != live.Identity {
		t.Fatalf("live record %+v %v", got, err)
	}
	if err := r.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	f.open(t)
}

// restoreEnvironments closes f's store and opens a restored copy, which has
// a fresh request lineage. The caller closes the copy and reopens f.
func restoreEnvironments(t *testing.T, f *workerFixture) *Store {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshot, dir); err != nil {
		t.Fatal(err)
	}
	r, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestEnvironmentPruneLiveWitness follows a live record whose request domain
// can no longer replay: pruning keeps its witness, so the snapshot stays
// valid, until the record is reclaimed. Then the witness and the tombstone
// are pruned, at most limit rows at a time.
func TestEnvironmentPruneLiveWitness(t *testing.T) {
	f := newWorkerFixture(t)
	spec := environmentSpec("kept")
	live := f.acceptEnvironment(t, "alice", "create-1", spec)
	r := restoreEnvironments(t, f)
	defer r.Close()
	valid := func() {
		t.Helper()
		if err := r.db.View(validateSnapshot); err != nil {
			t.Fatal(err)
		}
	}
	prune := func(limit, want int) {
		t.Helper()
		if n, err := r.PruneEnvironmentRequests(limit); err != nil || n != want {
			t.Fatalf("prune(%d) %d %v, want %d", limit, n, err, want)
		}
		valid()
	}
	if _, err := r.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("created")); err == nil {
		t.Fatal("old lineage request accepted")
	}
	key := string(environmentWitnessKey("alice", f.domain, "create-1"))
	prune(10, 0)
	if rows := environmentRows(t, r, "w/"); len(rows) != 1 || rows[key] == "" {
		t.Fatalf("w/ rows %v, want only %q", rows, key)
	}
	e, err := r.Environment(live.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []environment.Phase{environment.Deleting, environment.Deleted} {
		if e, err = r.CommitEnvironment(e.Identity.ID, e.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: p}, Checkpoint: e.Checkpoint}); err != nil {
			t.Fatal(err)
		}
	}
	prune(10, 0)
	if _, err := r.ReclaimEnvironment(e.Identity.ID, e.Revision); err != nil {
		t.Fatal(err)
	}
	valid()
	// The witness goes first; the tombstone it named is then unnamed.
	prune(1, 1)
	if rows := environmentRows(t, r, "w/"); len(rows) != 0 {
		t.Fatalf("w/ rows left: %v", rows)
	}
	if rows := environmentRows(t, r, "t/"); len(rows) != 1 {
		t.Fatalf("t/ rows %v, want the tombstone", rows)
	}
	prune(1, 1)
	prune(10, 0)
	if rows := environmentRows(t, r, "t/"); len(rows) != 0 {
		t.Fatalf("t/ rows left: %v", rows)
	}
	if rows := environmentRows(t, r, "x/"); rows["x/alice"] != string(sequenceKey(1)) {
		t.Fatalf("reclaimed count %v", rows)
	}
	f.open(t)
}

func TestEnvironmentRetirementCorruption(t *testing.T) {
	setup := func(t *testing.T) (*workerFixture, Environment, Environment) {
		f := newWorkerFixture(t)
		gone := f.deleteEnvironment(t, f.acceptEnvironment(t, "alice", "a-1", environmentSpec("gone")))
		if _, err := f.s.ReclaimEnvironment(gone.Identity.ID, gone.Revision); err != nil {
			t.Fatal(err)
		}
		live := f.acceptEnvironment(t, "alice", "a-2", environmentSpec("live"))
		f.valid(t)
		return f, gone, live
	}
	for _, tt := range []struct {
		name string
		edit func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error
	}{
		{"crossed witness", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Put(environmentWitnessKey("alice", d, "a-2"), b.Get(environmentWitnessKey("alice", d, "a-1")))
		}},
		{"witness names nothing", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Delete(environmentTombstoneKey(gone.Identity.ID))
		}},
		{"tombstone with record", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Put(environmentTombstoneKey(live.Identity.ID), b.Get(environmentTombstoneKey(gone.Identity.ID)))
		}},
		{"reclaimed count", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Put(environmentReclaimedKey("alice"), sequenceKey(2))
		}},
		{"missing reclaimed count", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Delete(environmentReclaimedKey("alice"))
		}},
		{"orphan reclaimed count", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Put(environmentReclaimedKey("carol"), sequenceKey(1))
		}},
		{"domain record with binding", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.Put(environmentRequestKey("alice", "a-2"), []byte(live.Identity.ID))
		}},
		{"sequence", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			return b.SetSequence(b.Sequence() + 1)
		}},
		// MODEL: a pre-retirement marker under retirement rows.
		{"marker 11 with witness", func(b *bolt.Bucket, d RequestDomain, gone, live Environment) error {
			if err := servingStripModel(b.Tx()); err != nil {
				return err
			}
			return b.Tx().Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentArtifactSchema))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, gone, live := setup(t)
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				return tt.edit(tx.Bucket(environmentsBucket), f.domain, gone, live)
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.s.db.View(validateSnapshot); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}
