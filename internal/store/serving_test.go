package store

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	bolt "go.etcd.io/bbolt"
)

// Source-only; NOT_RUN. These tests drive real Store transactions with real
// file-authenticated actors and delegations. Runtime observations are
// MODELED inputs to CommitService; no native target is involved.

const servingTestImage = "registry.example.com/team/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type servingFixture struct {
	s         *Store
	actors    map[string]auth.Actor
	authority map[string]auth.Delegation
	domain    RequestDomain
	clock     time.Time
}

func newServingFixture(t *testing.T) *servingFixture {
	t.Helper()
	sf := newScopeFixture(t)
	f := &servingFixture{actors: sf.actors, authority: map[string]auth.Delegation{}}
	for _, id := range []string{"alice", "bob", "legacy"} {
		d, err := sf.engine.Delegate(sf.actors[id])
		if err != nil {
			t.Fatal(err)
		}
		f.authority[id] = d
	}
	dir := filepath.Dir(sf.s.db.Path())
	if err := sf.s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := EnableRetention(dir, DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.domain = info.Domain
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
	f.clock = s.now().UTC()
	s.now = func() time.Time { return f.clock }
	return f
}

// tick advances the store clock.
func (f *servingFixture) tick() time.Time {
	f.clock = f.clock.Add(time.Second)
	return f.clock
}

func servingTestSpec(argv ...string) serving.Spec {
	return serving.Spec{
		Image:     servingTestImage,
		Argv:      append([]string{"/bin/web"}, argv...),
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   serving.Scaling{Replicas: 1},
	}
}

var servingTestLimits = ServiceLimits{CPUMillis: 4000, MemoryBytes: 8 << 30}

func (f *servingFixture) accept(t *testing.T, who, request, name string, spec serving.Spec) Service {
	t.Helper()
	svc, err := f.s.AcceptService(f.actors[who], f.request(who, request, name, spec), servingTestLimits)
	if err != nil {
		t.Fatalf("accept %s: %v", request, err)
	}
	return svc
}

func (f *servingFixture) request(who, request, name string, spec serving.Spec) ServiceRequest {
	return ServiceRequest{Domain: f.domain, RequestID: request, Name: name, Target: "kube", Binding: "test-binding", Authority: f.authority[who], Spec: spec}
}

// servingRows returns every row of the serving family.
func servingRows(t *testing.T, s *Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(servingServicesBucket).ForEach(func(k, v []byte) error {
			out[string(k)] = string(v)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameRows(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestServingSchemaInstalled(t *testing.T) {
	f := newServingFixture(t)
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		switch {
		case schemaNumber(tx) != schemaVersion:
			t.Errorf("marker %d, schema %d", schemaNumber(tx), schemaVersion)
		case string(m.Get(servingStorageFormatKey)) != "skygo-serving-store-v1":
			t.Errorf("serving tag %q", m.Get(servingStorageFormatKey))
		case string(m.Get(environmentStorageFormatKey)) != environmentStorageFormat:
			t.Errorf("environment tag %q", m.Get(environmentStorageFormatKey))
		case tx.Bucket(servingServicesBucket) == nil:
			t.Error("no serving_services bucket")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

// TestServingMigrationFromSchema12 strips a current installation to the
// schema12 shape (a MODEL, not a genuine signed9f4 writer) and checks that
// Open adds exactly the empty serving family, its v1 tag, the artifact Begin
// format tag and the current marker. A populated or tagged schema12 database
// is refused unchanged.
func TestServingMigrationFromSchema12(t *testing.T) {
	dir, _, _ := environmentPopulated(t)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := servingStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentRetirementSchema))
	})
	before := fixtureState(t, dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	after := workflowDatabase(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	marker := string(metadataBucket) + "/" + string(schemaKey)
	added := map[string]string{
		string(metadataBucket) + "/" + string(servingStorageFormatKey):       servingStorageFormat,
		string(servingServicesBucket) + "\x00sequence":                       "0",
		string(metadataBucket) + "/" + string(artifactBeginStorageFormatKey): artifactBeginStorageFormat,
	}
	if before[marker] != string(sequenceKey(environmentRetirementSchema)) {
		t.Fatal("model does not have the schema12 marker")
	}
	for k, v := range before {
		if k == marker {
			continue
		}
		if got, ok := after[k]; !ok || got != v {
			t.Errorf("migration changed %q", k)
		}
	}
	if after[marker] != string(sequenceKey(schemaVersion)) {
		t.Error("migration did not install the current marker")
	}
	for k, v := range added {
		if _, ok := before[k]; ok {
			t.Errorf("schema12 model already has %q", k)
		}
		if got, ok := after[k]; !ok || got != v {
			t.Errorf("migration did not add exactly %q", k)
		}
	}
	if len(after) != len(before)+len(added) {
		t.Fatalf("migration added other rows: %d -> %d", len(before), len(after))
	}

	for _, tt := range []struct {
		name      string
		artifacts bool // populated canonical artifact MODEL before the one foreign pin
		edit      func(*bolt.Tx) error
	}{
		{"tag at twelve", false, func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(servingStorageFormatKey, []byte(servingStorageFormat))
		}},
		{"family at twelve", false, func(tx *bolt.Tx) error { _, err := tx.CreateBucket(servingServicesBucket); return err }},
		{"services foreign", false, func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("services")); return err }},
		{"compat foreign", false, func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("compat_requests")); return err }},
		// The foreign pin family is artifacts/service_pins, as a table or
		// a scalar; the Store keeps no whitelist of top-level buckets.
		{"pins foreign table", true, func(tx *bolt.Tx) error {
			_, err := tx.Bucket(artifactsBucket).CreateBucket(foreignArtifactTable)
			return err
		}},
		{"pins foreign scalar", true, func(tx *bolt.Tx) error {
			return tx.Bucket(artifactsBucket).Put(foreignArtifactTable, []byte("{}"))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, _, _ := environmentPopulated(t)
			if tt.artifacts {
				dir = formatArtifactPopulated(t)
			}
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				if err := servingStripModel(tx); err != nil {
					return err
				}
				if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentRetirementSchema)); err != nil {
					return err
				}
				return tt.edit(tx)
			})
			before := fixtureState(t, dir)
			s, err := Open(dir)
			if err == nil {
				s.Close()
				t.Fatal("foreign schema12 database opened")
			}
			if !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("open: %v", err)
			}
			if after := fixtureState(t, dir); !sameRows(before, after) {
				t.Fatal("refused open changed the database")
			}
		})
	}
}

// fixtureState returns every row of the closed database in dir.
func fixtureState(t *testing.T, dir string) map[string]string {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	out := map[string]string{}
	var walk func(path string, b *bolt.Bucket) error
	walk = func(path string, b *bolt.Bucket) error {
		out[path+"\x00sequence"] = fmt.Sprint(b.Sequence())
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return walk(path+"/"+string(k), b.Bucket(k))
			}
			out[path+"/"+string(k)] = string(v)
			return nil
		})
	}
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error { return walk(string(name), b) })
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestServiceAcceptReplay(t *testing.T) {
	f := newServingFixture(t)
	svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	if svc.Revision != 1 || svc.Desired != serving.Present || svc.Operation == nil || svc.Operation.Kind != ServiceApply ||
		svc.Operation.Generation != 1 || svc.Reserved != (ServiceReservation{Replicas: 1, CPUMillis: 250, MemoryBytes: 256 << 20}) {
		t.Fatalf("accepted %+v", svc)
	}
	rows := servingRows(t, f.s)

	// An exact replay returns the service and writes nothing.
	again := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	if again.ID != svc.ID || !sameRows(rows, servingRows(t, f.s)) {
		t.Fatal("replay wrote or returned another service")
	}
	// Another request under the key is a conflict; refusals write nothing.
	for _, tt := range []struct {
		name string
		r    ServiceRequest
		want error
	}{
		{"other spec", f.request("alice", "create-1", "web", servingTestSpec("--x")), ErrConflict},
		{"other name", f.request("alice", "create-1", "web2", servingTestSpec()), ErrConflict},
		{"name held", f.request("alice", "create-2", "web", servingTestSpec()), ErrServiceNameInUse},
	} {
		if _, err := f.s.AcceptService(f.actors["alice"], tt.r, servingTestLimits); !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", tt.name, err, tt.want)
		}
	}
	if _, err := f.s.AcceptService(f.actors["reader"], f.request("alice", "create-3", "other", servingTestSpec()), servingTestLimits); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Errorf("reader accept: %v", err)
	}
	bad := f.request("alice", "create-4", "other", servingTestSpec())
	bad.Authority = f.authority["bob"]
	if _, err := f.s.AcceptService(f.actors["alice"], bad, servingTestLimits); err == nil {
		t.Error("accepted another principal's delegation")
	}
	if !sameRows(rows, servingRows(t, f.s)) {
		t.Fatal("refusal wrote rows")
	}
	// Names are per principal.
	f.accept(t, "bob", "create-1", "web", servingTestSpec())
}

func TestServiceVisibility(t *testing.T) {
	f := newServingFixture(t)
	svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	if _, err := f.s.ServiceFor(f.actors["bob"], svc.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob sees alice's service: %v", err)
	}
	for _, who := range []string{"alice", "legacy"} {
		if got, err := f.s.ServiceFor(f.actors[who], svc.ID); err != nil || got.ID != svc.ID {
			t.Errorf("%s: %v", who, err)
		}
	}
	page, err := f.s.ServicesFor(f.actors["bob"], 0, 10)
	if err != nil || len(page.Services) != 0 {
		t.Errorf("bob lists %+v %v", page, err)
	}
	m := ServiceMutation{Domain: f.domain, RequestID: "delete-1", ID: svc.ID, Revision: svc.Revision}
	if _, err := f.s.DeleteService(f.actors["bob"], m); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob deletes alice's service: %v", err)
	}
	if _, err := f.s.DeleteService(f.actors["reader"], m); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Errorf("reader deletes: %v", err)
	}
	if got, err := f.s.DeleteService(f.actors["legacy"], m); err != nil || got.Desired != serving.Absent {
		t.Errorf("admin delete: %+v %v", got, err)
	}
}

func TestServiceQuotas(t *testing.T) {
	f := newServingFixture(t)
	limits := ServiceLimits{PrincipalServices: 1, CPUMillis: 400, MemoryBytes: 8 << 30}
	if _, err := f.s.AcceptService(f.actors["alice"], f.request("alice", "c-1", "a", servingTestSpec()), limits); err != nil {
		t.Fatal(err)
	}
	var ce *ServiceCapacityError
	_, err := f.s.AcceptService(f.actors["alice"], f.request("alice", "c-2", "b", servingTestSpec()), limits)
	if !errors.As(err, &ce) || ce.Domain != ServicePrincipalQuota || ce.Used != 1 || ce.Limit != 1 {
		t.Fatalf("principal quota: %v %+v", err, ce)
	}
	// Bob is under his own principal quota but the binding's CPU is reserved.
	_, err = f.s.AcceptService(f.actors["bob"], f.request("bob", "c-1", "a", servingTestSpec()), limits)
	if !errors.As(err, &ce) || ce.Domain != ServiceCPUQuota || ce.Used != 250 || ce.Requested != 250 || ce.Limit != 400 {
		t.Fatalf("cpu quota: %v %+v", err, ce)
	}
}

func TestServiceDeleteRevision(t *testing.T) {
	f := newServingFixture(t)
	svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	stale := ServiceMutation{Domain: f.domain, RequestID: "delete-0", ID: svc.ID, Revision: svc.Revision + 1}
	if _, err := f.s.DeleteService(f.actors["alice"], stale); !errors.Is(err, ErrServiceStale) {
		t.Fatalf("stale delete: %v", err)
	}
	m := ServiceMutation{Domain: f.domain, RequestID: "delete-1", ID: svc.ID, Revision: svc.Revision}
	d, err := f.s.DeleteService(f.actors["alice"], m)
	if err != nil {
		t.Fatal(err)
	}
	// Deletion keeps the operation in flight, the Native and the charge.
	if d.Desired != serving.Absent || d.Revision != 2 || d.Operation == nil || d.Reserved != svc.Reserved || d.Phase() == serving.Deleted {
		t.Fatalf("deleted %+v", d)
	}
	// The exact replay, against the old revision, returns the current view.
	if again, err := f.s.DeleteService(f.actors["alice"], m); err != nil || again.Revision != d.Revision {
		t.Fatalf("replay: %+v %v", again, err)
	}
	m2 := ServiceMutation{Domain: f.domain, RequestID: "delete-2", ID: svc.ID, Revision: d.Revision}
	if _, err := f.s.DeleteService(f.actors["alice"], m2); !errors.Is(err, ErrServiceDeleted) {
		t.Fatalf("second delete: %v", err)
	}
}

// TestServiceCommitGuards checks CommitService's custody rules.
func TestServiceCommitGuards(t *testing.T) {
	f := newServingFixture(t)
	svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	commit := func(next ServiceCommit) (Service, error) {
		return f.s.CommitService(svc.ID, svc.Revision, next)
	}
	refuse := func(name string, next ServiceCommit) {
		t.Helper()
		rows := servingRows(t, f.s)
		if _, err := commit(next); err == nil {
			t.Errorf("%s accepted", name)
		}
		if !sameRows(rows, servingRows(t, f.s)) {
			t.Errorf("%s wrote rows", name)
		}
	}
	present := &serving.Observation{DesiredReplicas: 1}

	c := svc.Commit()
	c.Operation.ID = "00000000000000000000000000000000"
	refuse("changed operation", c)

	c = svc.Commit()
	c.Operation = nil
	refuse("clear without observation", c)

	c = svc.Commit()
	c.Operation, c.Native, c.Observation, c.ObservedAt = nil, "declared", present, f.tick()
	refuse("clear with uncommitted native", c)

	if _, err := f.s.CommitService(svc.ID, svc.Revision+1, svc.Commit()); !errors.Is(err, ErrServiceStale) {
		t.Errorf("stale commit: %v", err)
	}

	// A changed Native is committed with the operation unchanged.
	c = svc.Commit()
	c.Native = "declared"
	var err error
	if svc, err = commit(c); err != nil {
		t.Fatal(err)
	}
	if svc.Operation == nil || svc.Native != "declared" {
		t.Fatalf("native commit %+v", svc)
	}
	// Clearing integrates a fresh observation of the committed Native.
	c = svc.Commit()
	c.Operation, c.Observation, c.ObservedAt = nil, present, f.tick()
	if svc, err = commit(c); err != nil {
		t.Fatal(err)
	}
	c = svc.Commit()
	c.Observation = nil
	refuse("observation removed", c)

	c = svc.Commit()
	c.Operation = &ServiceOperation{ID: "11111111111111111111111111111111", Kind: ServiceDelete, Started: f.clock}
	refuse("delete operation while present", c)
}

// TestServiceReleaseTwins checks that only a fresh absence of the committed
// Native, with no operation in flight, releases the charge; a recorded
// absence that predates the clear does not.
func TestServiceReleaseTwins(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		f := newServingFixture(t)
		svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
		d, err := f.s.DeleteService(f.actors["alice"], ServiceMutation{Domain: f.domain, RequestID: "delete-1", ID: svc.ID, Revision: svc.Revision})
		if err != nil {
			t.Fatal(err)
		}
		// Absence observed while the original Apply is still in flight.
		c := d.Commit()
		c.Observation, c.ObservedAt = &serving.Observation{Absent: true}, f.tick()
		if d, err = f.s.CommitService(d.ID, d.Revision, c); err != nil {
			t.Fatal(err)
		}
		if d.Reserved == (ServiceReservation{}) || d.Phase() == serving.Deleted {
			t.Fatalf("absence with an operation in flight released: %+v", d)
		}
		c = d.Commit()
		c.Operation = nil
		if fresh {
			c.ObservedAt = f.tick()
		}
		got, err := f.s.CommitService(d.ID, d.Revision, c)
		if !fresh {
			if err == nil {
				t.Fatal("stale absence cleared the operation")
			}
			if _, err := f.s.RetireService(d.ID, d.Revision); !errors.Is(err, ErrServiceHeld) {
				t.Fatalf("retire held: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if got.Reserved != (ServiceReservation{}) || got.Phase() != serving.Deleted {
			t.Fatalf("fresh absence did not release: %+v", got)
		}
	}
}

func TestServiceRetireAndPrune(t *testing.T) {
	f := newServingFixture(t)
	svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	if _, err := f.s.RetireService(svc.ID, svc.Revision); !errors.Is(err, ErrServiceHeld) {
		t.Fatalf("retire present: %v", err)
	}
	d, err := f.s.DeleteService(f.actors["alice"], ServiceMutation{Domain: f.domain, RequestID: "delete-1", ID: svc.ID, Revision: svc.Revision})
	if err != nil {
		t.Fatal(err)
	}
	c := d.Commit()
	c.Operation, c.Observation, c.ObservedAt = nil, &serving.Observation{Absent: true}, f.tick()
	if d, err = f.s.CommitService(d.ID, d.Revision, c); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.RetireService(d.ID, d.Revision+1); !errors.Is(err, ErrServiceStale) {
		t.Fatalf("stale retire: %v", err)
	}
	r, err := f.s.RetireService(d.ID, d.Revision)
	if err != nil || !r.Retired || r.Desired != serving.Absent || r.Phase() != serving.Deleted {
		t.Fatalf("retired %+v %v", r, err)
	}
	if _, err := f.s.ServiceFor(f.actors["alice"], svc.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get retired: %v", err)
	}
	// Exact replays of the create and delete return the retired view.
	again := f.accept(t, "alice", "create-1", "web", servingTestSpec())
	if again.ID != svc.ID || !again.Retired {
		t.Fatalf("create replay %+v", again)
	}
	if got, err := f.s.DeleteService(f.actors["alice"], ServiceMutation{Domain: f.domain, RequestID: "delete-1", ID: svc.ID, Revision: svc.Revision}); err != nil || !got.Retired {
		t.Fatalf("delete replay %+v %v", got, err)
	}
	// The name is free again.
	if next := f.accept(t, "alice", "create-2", "web", servingTestSpec()); next.ID == svc.ID {
		t.Fatal("reused service ID")
	}
	// The current epoch's witnesses can still replay, so nothing is pruned.
	if n, err := f.s.PruneServiceRequests(64); err != nil || n != 0 {
		t.Fatalf("prune current: %d %v", n, err)
	}
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestServiceValidateRefusesCorruption(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(b *bolt.Bucket, id string) error
	}{
		{"orphan name", func(b *bolt.Bucket, id string) error { return b.Put([]byte("n/alice/other"), []byte(id)) }},
		{"unknown row", func(b *bolt.Bucket, id string) error { return b.Put([]byte("z/x"), []byte("1")) }},
		{"missing witness", func(b *bolt.Bucket, id string) error {
			c := b.Cursor()
			for k, _ := c.Seek([]byte("w/")); k != nil && bytes.HasPrefix(k, []byte("w/")); k, _ = c.Next() {
				return b.Delete(k)
			}
			return errors.New("no witness")
		}},
		{"nested bucket", func(b *bolt.Bucket, id string) error { _, err := b.CreateBucket([]byte("e/x")); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newServingFixture(t)
			svc := f.accept(t, "alice", "create-1", "web", servingTestSpec())
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				return tt.edit(tx.Bucket(servingServicesBucket), svc.ID)
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.s.db.View(validateSnapshot); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}
