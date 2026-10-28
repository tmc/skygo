package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions with real file-authenticated
// actors and delegations. Checkpoints are opaque modeled bytes; no provider,
// Control path or native resource is involved.
type environmentFixture struct {
	scopeFixture
	dir       string
	authority map[string]auth.Delegation
	domain    RequestDomain
}

// newEnvironmentFixture returns a scoped, retention-enabled store: every
// acceptance names a request domain, as newWorkerFixture's do.
func newEnvironmentFixture(t *testing.T) *environmentFixture {
	t.Helper()
	sf := newScopeFixture(t)
	f := &environmentFixture{scopeFixture: sf, dir: filepath.Dir(sf.s.db.Path()), authority: map[string]auth.Delegation{}}
	for _, id := range []string{"alice", "bob"} {
		d, err := sf.engine.Delegate(sf.actors[id])
		if err != nil {
			t.Fatal(err)
		}
		f.authority[id] = d
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := EnableRetention(f.dir, workerPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.domain = info.Domain
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
	return f
}

func (f *environmentFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
}

func (f *environmentFixture) accept(t *testing.T, who, request string, spec environment.Spec) Environment {
	t.Helper()
	e, err := f.s.AcceptEnvironment(f.actors[who], f.domain, request, spec, "test-binding", f.authority[who], environmentCheckpoint("created"))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (f *environmentFixture) commit(t *testing.T, e Environment, d environment.Desired, p environment.Phase) Environment {
	t.Helper()
	e, err := f.s.CommitEnvironment(e.Identity.ID, e.Revision, EnvironmentCommit{State: environment.State{Desired: d, Phase: p}, Checkpoint: e.Checkpoint})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func environmentSpec(name string) environment.Spec {
	return environment.Spec{Name: name, Target: "kube", Image: "task:v1", CPUMillis: 100, MemoryBytes: 1 << 20}
}

func environmentCheckpoint(data string) environment.Checkpoint {
	return environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: []byte(data)}
}

// environmentDelegation is a well-formed delegation for a principal without
// credentials, for transaction-level quota tests.
func environmentDelegation(principal string) auth.Delegation {
	return auth.Delegation{Principal: principal, TokenID: strings.Repeat("0", 32), Digest: [32]byte{1}}
}

func TestEnvironmentAcceptAndReplay(t *testing.T) {
	f := newEnvironmentFixture(t)
	spec := environmentSpec("analysis")
	spec.StorageAlias = "scratch"
	e := f.accept(t, "alice", "create-1", spec)
	raw, err := environment.Canonical(spec)
	if err != nil {
		t.Fatal(err)
	}
	want := environment.Identity{Installation: f.s.Owner(), ID: e.Identity.ID, Generation: 1}
	if e.Identity != want || !environment.ValidID(e.Identity.ID) || e.Principal != "alice" || e.RequestID != "create-1" || e.Binding != "test-binding" || e.Authority != f.authority["alice"] {
		t.Fatalf("identity: %+v", e)
	}
	if e.Revision != 1 || e.State != (environment.State{Desired: environment.Present, Phase: environment.Creating}) || e.SpecDigest != sha256.Sum256(raw) || string(e.Checkpoint.Data) != "created" || e.CreatedAt.IsZero() || !e.UpdatedAt.Equal(e.CreatedAt) {
		t.Fatalf("initial record: %+v", e)
	}
	before := workflowDatabase(t, f.s)
	// Exact replay precedes binding, authority, checkpoint and capacity.
	again, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "other-binding", auth.Delegation{}, environment.Checkpoint{})
	if err != nil || again.Identity != e.Identity || again.Binding != "test-binding" || again.Revision != 1 {
		t.Fatalf("replay: %+v %v", again, err)
	}
	changed := spec
	changed.CPUMillis++
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", changed, "test-binding", f.authority["alice"], environmentCheckpoint("x")); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	f.restart(t)
	again, err = f.s.AcceptEnvironment(f.actors["alice"], f.domain, "create-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("created"))
	if err != nil || again.Identity != e.Identity {
		t.Fatalf("replay after reopen: %+v %v", again, err)
	}
	workflowUnchanged(t, f.s, before)
}

func TestEnvironmentAcceptRefusals(t *testing.T) {
	f := newEnvironmentFixture(t)
	alice := f.actors["alice"]
	for _, test := range []struct {
		name      string
		actor     auth.Actor
		request   string
		spec      environment.Spec
		binding   string
		authority auth.Delegation
		cp        environment.Checkpoint
		want      error
	}{
		{"reader", f.actors["reader"], "r", environmentSpec("a"), "b", f.authority["alice"], environmentCheckpoint("c"), auth.ErrPermissionDenied},
		{"request", alice, "bad/request", environmentSpec("a"), "b", f.authority["alice"], environmentCheckpoint("c"), nil},
		{"spec", alice, "r", environment.Spec{}, "b", f.authority["alice"], environmentCheckpoint("c"), nil},
		{"foreign authority", alice, "r", environmentSpec("a"), "b", f.authority["bob"], environmentCheckpoint("c"), auth.ErrUnauthenticated},
		{"no authority", alice, "r", environmentSpec("a"), "b", auth.Delegation{}, environmentCheckpoint("c"), auth.ErrUnauthenticated},
		{"binding", alice, "r", environmentSpec("a"), "", f.authority["alice"], environmentCheckpoint("c"), nil},
		{"checkpoint", alice, "r", environmentSpec("a"), "b", f.authority["alice"], environment.Checkpoint{Kind: "ec2-vm", Version: 1}, nil},
		{"checkpoint size", alice, "r", environmentSpec("a"), "b", f.authority["alice"], environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: make([]byte, environment.MaxCheckpointBytes+1)}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := workflowDatabase(t, f.s)
			_, err := f.s.AcceptEnvironment(test.actor, f.domain, test.request, test.spec, test.binding, test.authority, test.cp)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("accept: %v, want %v", err, test.want)
			}
			workflowUnchanged(t, f.s, before)
		})
	}
}

func TestEnvironmentRefusesUnscopedInstallation(t *testing.T) {
	// A retention-enabled store that was never scoped issues a real domain;
	// the legacy actor passes authorization and the domain gate, and the
	// transaction refuses the unscoped installation.
	s, info := enableTest(t, DefaultRetentionPolicy())
	actor := testActor()
	before := workflowDatabase(t, s)
	if _, err := s.AcceptEnvironment(actor, RequestDomain{}, "r", environmentSpec("a"), "b", environmentDelegation(actor.Principal()), environmentCheckpoint("c")); !errors.Is(err, ErrInvalidRequestDomain) {
		t.Fatalf("zero domain accept: %v", err)
	}
	workflowUnchanged(t, s, before)
	if _, err := s.AcceptEnvironment(actor, info.Domain, "r", environmentSpec("a"), "b", environmentDelegation(actor.Principal()), environmentCheckpoint("c")); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("unscoped accept: %v", err)
	}
	workflowUnchanged(t, s, before)
}

func TestEnvironmentConcurrentReplay(t *testing.T) {
	f := newEnvironmentFixture(t)
	var wg sync.WaitGroup
	ids := make([]string, 8)
	errs := make([]error, 8)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "same", environmentSpec("a"), "test-binding", f.authority["alice"], environmentCheckpoint("c"))
			ids[i], errs[i] = e.Identity.ID, err
		}()
	}
	wg.Wait()
	for i := range ids {
		if errs[i] != nil || ids[i] != ids[0] {
			t.Fatalf("concurrent accept %d: %s %v", i, ids[i], errs[i])
		}
	}
	page, err := f.s.Environments(0, 10)
	if err != nil || len(page.Environments) != 1 {
		t.Fatalf("rows: %d %v", len(page.Environments), err)
	}
}

func TestEnvironmentReservations(t *testing.T) {
	f := newEnvironmentFixture(t)
	spec := environmentSpec("analysis")
	spec.StorageAlias = "scratch"
	e := f.accept(t, "alice", "first", spec)
	other := spec
	other.StorageAlias = ""
	for _, test := range []struct {
		name, who, request string
		spec               environment.Spec
	}{
		{"same name", "alice", "second", other},
		{"same alias", "bob", "second", spec},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := workflowDatabase(t, f.s)
			if _, err := f.s.AcceptEnvironment(f.actors[test.who], f.domain, test.request, test.spec, "test-binding", f.authority[test.who], environmentCheckpoint("c")); !errors.Is(err, ErrEnvironmentReserved) {
				t.Fatalf("accept: %v", err)
			}
			workflowUnchanged(t, f.s, before)
		})
	}
	// The name is the principal's; the alias is the target's.
	f.accept(t, "bob", "bob-1", other)
	moved := spec
	moved.Name, moved.Target = "moved", "other-target"
	f.accept(t, "bob", "bob-2", moved)
	// Neither deletion nor a Deleted phase releases a reservation.
	e = f.commit(t, e, environment.Absent, environment.Deleting)
	f.commit(t, e, environment.Absent, environment.Deleted)
	renamed := spec
	renamed.Name = "renamed"
	for _, test := range []struct {
		who  string
		spec environment.Spec
	}{{"alice", other}, {"bob", renamed}} {
		if _, err := f.s.AcceptEnvironment(f.actors[test.who], f.domain, "after-delete", test.spec, "test-binding", f.authority[test.who], environmentCheckpoint("c")); !errors.Is(err, ErrEnvironmentReserved) {
			t.Fatalf("%s after delete: %v", test.who, err)
		}
	}
}

func TestEnvironmentPrincipalLimitCountsDeleted(t *testing.T) {
	f := newEnvironmentFixture(t)
	for i := range maxPrincipalEnvironments {
		e := f.accept(t, "alice", fmt.Sprintf("r%d", i), environmentSpec(fmt.Sprintf("e%d", i)))
		if i == 0 {
			e := f.commit(t, e, environment.Absent, environment.Deleting)
			f.commit(t, e, environment.Absent, environment.Deleted)
		}
		if i == 1 {
			f.commit(t, e, environment.Present, environment.Unknown)
		}
	}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "over", environmentSpec("over"), "test-binding", f.authority["alice"], environmentCheckpoint("c")); !errors.Is(err, ErrEnvironmentLimit) {
		t.Fatalf("fifth environment: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Another principal is unaffected, and an accepted request still replays.
	f.accept(t, "bob", "r0", environmentSpec("e0"))
	f.accept(t, "alice", "r3", environmentSpec("e3"))
}

func TestEnvironmentGlobalLimit(t *testing.T) {
	f := newEnvironmentFixture(t)
	accept := func(principal, name string) error {
		id, err := newID()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := environment.Canonical(environmentSpec(name))
		if err != nil {
			t.Fatal(err)
		}
		return f.s.db.Update(func(tx *bolt.Tx) error {
			_, e := acceptEnvironment(tx, principal, f.domain, "r-"+name, raw, "test-binding", environmentDelegation(principal), environmentCheckpoint("c"), id, f.s.now().UTC())
			return e
		})
	}
	for i := range maxEnvironments {
		if err := accept(fmt.Sprintf("p%d", i/maxPrincipalEnvironments), fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	if err := accept("fresh", "over"); !errors.Is(err, ErrEnvironmentLimit) {
		t.Fatalf("thirty-third environment: %v", err)
	}
	workflowUnchanged(t, f.s, before)
}

func TestEnvironmentCommitCompareAndSwap(t *testing.T) {
	f := newEnvironmentFixture(t)
	e := f.accept(t, "alice", "r", environmentSpec("a"))
	ready := f.commit(t, e, environment.Present, environment.Ready)
	if ready.Revision != 2 || ready.State.Phase != environment.Ready || ready.Identity != e.Identity || ready.SpecDigest != e.SpecDigest || ready.Binding != e.Binding || ready.Authority != e.Authority || !ready.CreatedAt.Equal(e.CreatedAt) {
		t.Fatalf("commit: %+v", ready)
	}
	unknown := environment.State{Desired: environment.Present, Phase: environment.Unknown}
	for _, test := range []struct {
		name     string
		revision uint64
		next     EnvironmentCommit
		want     error
	}{
		{"stale", 1, EnvironmentCommit{State: unknown, Checkpoint: e.Checkpoint}, ErrEnvironmentStale},
		{"future", 3, EnvironmentCommit{State: unknown, Checkpoint: e.Checkpoint}, ErrEnvironmentStale},
		{"creating again", 2, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Creating}, Checkpoint: e.Checkpoint}, nil},
		{"deleted without deleting", 2, EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: environment.Deleted}, Checkpoint: e.Checkpoint}, nil},
		{"deleting while present", 2, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Deleting}, Checkpoint: e.Checkpoint}, nil},
		{"zero state", 2, EnvironmentCommit{State: environment.State{}, Checkpoint: e.Checkpoint}, nil},
		{"kind", 2, EnvironmentCommit{State: unknown, Checkpoint: environment.Checkpoint{Kind: "kube-pod", Version: 1, Data: []byte("c")}}, nil},
		{"version", 2, EnvironmentCommit{State: unknown, Checkpoint: environment.Checkpoint{Kind: "ec2-vm", Version: 2, Data: []byte("c")}}, nil},
		{"empty checkpoint", 2, EnvironmentCommit{State: unknown, Checkpoint: environment.Checkpoint{Kind: "ec2-vm", Version: 1}}, nil},
		{"oversize checkpoint", 2, EnvironmentCommit{State: unknown, Checkpoint: environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: make([]byte, environment.MaxCheckpointBytes+1)}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := workflowDatabase(t, f.s)
			_, err := f.s.CommitEnvironment(e.Identity.ID, test.revision, test.next)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("commit: %v, want %v", err, test.want)
			}
			workflowUnchanged(t, f.s, before)
		})
	}
	// The stale check precedes the transition check.
	if _, err := f.s.CommitEnvironment(e.Identity.ID, 1, EnvironmentCommit{State: environment.State{}, Checkpoint: environment.Checkpoint{}}); !errors.Is(err, ErrEnvironmentStale) {
		t.Fatalf("stale invalid commit: %v", err)
	}
	if _, err := f.s.CommitEnvironment(strings.Repeat("f", 32), 1, EnvironmentCommit{State: unknown, Checkpoint: e.Checkpoint}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing commit: %v", err)
	}
	maximal := environment.Checkpoint{Kind: "ec2-vm", Version: 1, Data: make([]byte, environment.MaxCheckpointBytes)}
	got, err := f.s.CommitEnvironment(e.Identity.ID, 2, EnvironmentCommit{State: unknown, Checkpoint: maximal})
	if err != nil || got.Revision != 3 || len(got.Checkpoint.Data) != environment.MaxCheckpointBytes {
		t.Fatalf("maximal checkpoint: %v", err)
	}
	f.restart(t)
	if got, err = f.s.Environment(e.Identity.ID); err != nil || got.Revision != 3 || got.State != unknown {
		t.Fatalf("after reopen: %+v %v", got.State, err)
	}
}

func TestEnvironmentAbsentLatches(t *testing.T) {
	f := newEnvironmentFixture(t)
	e := f.accept(t, "alice", "r", environmentSpec("a"))
	e = f.commit(t, e, environment.Present, environment.Ready)
	// Deletion is a compare-and-swap commit of Absent intent.
	d := f.commit(t, e, environment.Absent, environment.Ready)
	if d.Revision != e.Revision+1 || string(d.Checkpoint.Data) != string(e.Checkpoint.Data) {
		t.Fatalf("delete: %+v", d)
	}
	before := workflowDatabase(t, f.s)
	// A writer still holding the old revision is stale, and Absent latches.
	if _, err := f.s.CommitEnvironment(e.Identity.ID, e.Revision, EnvironmentCommit{State: e.State, Checkpoint: e.Checkpoint}); !errors.Is(err, ErrEnvironmentStale) {
		t.Fatalf("stale adapter commit: %v", err)
	}
	if _, err := f.s.CommitEnvironment(d.Identity.ID, d.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Ready}, Checkpoint: d.Checkpoint}); err == nil {
		t.Fatal("absent environment became present")
	}
	workflowUnchanged(t, f.s, before)
	deleting := f.commit(t, d, environment.Absent, environment.Deleting)
	deleted := f.commit(t, deleting, environment.Absent, environment.Deleted)
	for _, p := range []environment.Phase{environment.Ready, environment.Unknown, environment.Deleting} {
		if _, err := f.s.CommitEnvironment(deleted.Identity.ID, deleted.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: p}, Checkpoint: deleted.Checkpoint}); err == nil {
			t.Fatalf("deleted environment moved to %v", p)
		}
	}
	// Deleted rows remain readable and listed.
	page, err := f.s.EnvironmentsFor(f.actors["alice"], 0, 10)
	if err != nil || len(page.Environments) != 1 || page.Environments[0].State.Phase != environment.Deleted {
		t.Fatalf("list after delete: %+v %v", page, err)
	}
}

func TestEnvironmentVisibilityAndPaging(t *testing.T) {
	f := newEnvironmentFixture(t)
	var mine []string
	for i := range 3 {
		mine = append(mine, f.accept(t, "alice", fmt.Sprintf("a%d", i), environmentSpec(fmt.Sprintf("a%d", i))).Identity.ID)
		f.accept(t, "bob", fmt.Sprintf("b%d", i), environmentSpec(fmt.Sprintf("b%d", i)))
	}
	if _, err := f.s.EnvironmentFor(f.actors["bob"], mine[0]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign read: %v", err)
	}
	if _, err := f.s.EnvironmentFor(f.actors["bob"], "not-an-id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("malformed read: %v", err)
	}
	for _, who := range []string{"alice", "legacy"} {
		if e, err := f.s.EnvironmentFor(f.actors[who], mine[0]); err != nil || e.Identity.ID != mine[0] {
			t.Fatalf("%s read: %v", who, err)
		}
	}
	var got []string
	var after uint64
	for {
		page, err := f.s.EnvironmentsFor(f.actors["alice"], after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Environments {
			got = append(got, e.Identity.ID)
		}
		after = page.After
		if !page.More {
			break
		}
	}
	if strings.Join(got, ",") != strings.Join(mine, ",") || after != 3 {
		t.Fatalf("alice pages: %v after %d", got, after)
	}
	all, err := f.s.EnvironmentsFor(f.actors["legacy"], 0, 200)
	if err != nil || len(all.Environments) != 6 || all.After != 6 || all.More {
		t.Fatalf("admin list: %d %v", len(all.Environments), err)
	}
	for _, limit := range []int{0, 201} {
		if _, err := f.s.EnvironmentsFor(f.actors["alice"], 0, limit); err == nil {
			t.Fatalf("limit %d accepted", limit)
		}
	}
}

func TestEnvironmentValuesAreCopies(t *testing.T) {
	f := newEnvironmentFixture(t)
	spec := environmentSpec("a")
	spec.NodeSelector = map[string]string{"zone": "a"}
	cp := environmentCheckpoint("abc")
	e, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "r", spec, "test-binding", f.authority["alice"], cp)
	if err != nil {
		t.Fatal(err)
	}
	cp.Data[0] = 'x'
	spec.NodeSelector["zone"] = "b"
	e.Checkpoint.Data[1] = 'x'
	e.Spec.NodeSelector["zone"] = "c"
	next := environmentCheckpoint("def")
	got, err := f.s.CommitEnvironment(e.Identity.ID, 1, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Ready}, Checkpoint: next})
	if err != nil {
		t.Fatal(err)
	}
	next.Data[0] = 'x'
	got.Checkpoint.Data[1] = 'x'
	got, err = f.s.Environment(e.Identity.ID)
	if err != nil || string(got.Checkpoint.Data) != "def" || got.Spec.NodeSelector["zone"] != "a" {
		t.Fatalf("stored values changed: %q %v %v", got.Checkpoint.Data, got.Spec.NodeSelector, err)
	}
}

func TestEnvironmentCommitErrorWritesNothing(t *testing.T) {
	f := newEnvironmentFixture(t)
	e := f.accept(t, "alice", "r", environmentSpec("a"))
	// A closed database refuses the update before any commit. The returned
	// error is the caller's only signal: it must not cause a provider effect.
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.CommitEnvironment(e.Identity.ID, 1, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Ready}, Checkpoint: e.Checkpoint}); err == nil {
		t.Fatal("commit on a closed store succeeded")
	}
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if got, err := s.Environment(e.Identity.ID); err != nil || got.Revision != 1 || got.State.Phase != environment.Creating {
		t.Fatalf("after failed commit: %+v %v", got.State, err)
	}
}
