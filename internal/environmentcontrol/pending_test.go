package environmentcontrol_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

var errFault = errors.New("injected store fault")

// storeFaults wraps the fixture's Store for one Controller. A commit whose
// checkpoint records native is refused unwritten while refuse is set, or
// lands with its response lost while lose is set; either fault then fails
// the next after reads, starting with the readback. A read armed by
// pauseRead waits before it reads. onCommit, if set, runs before every
// commit. sessionCommits counts the commits whose ledger records a session.
type storeFaults struct {
	f      *fixture
	native []byte

	mu     sync.Mutex
	refuse bool
	lose   bool
	after  int
	outage int
	paused chan struct{}
	resume chan struct{}

	onCommit       func()
	sessionCommits int
}

func (f *fixture) faults(native string) *storeFaults {
	s := &storeFaults{f: f, native: []byte(native)}
	environmentcontrol.SetStoreFaults(f.c, s.read, s.commit)
	return s
}

// sessions returns the number of commits so far whose ledger records a
// session.
func (s *storeFaults) sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionCommits
}

func (s *storeFaults) set(refuse, lose bool, after int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuse, s.lose, s.after, s.outage = refuse, lose, after, 0
}

func (s *storeFaults) read(id string) (store.Environment, error) {
	s.mu.Lock()
	fail := s.outage > 0
	if fail {
		s.outage--
	}
	paused, resume := s.paused, s.resume
	s.paused, s.resume = nil, nil
	s.mu.Unlock()
	if paused != nil {
		close(paused)
		<-resume
	}
	if fail {
		return store.Environment{}, errFault
	}
	return s.f.store.Environment(id)
}

func (s *storeFaults) commit(id string, revision uint64, next store.EnvironmentCommit) (store.Environment, error) {
	// The ledger encodes Native in base64.
	records := bytes.Contains(next.Checkpoint.Data, []byte(base64.StdEncoding.EncodeToString(s.native)))
	s.mu.Lock()
	if bytes.Contains(next.Checkpoint.Data, []byte(`"sessions":[`)) {
		s.sessionCommits++
	}
	onCommit := s.onCommit
	s.mu.Unlock()
	if onCommit != nil {
		onCommit()
	}
	s.mu.Lock()
	refuse, lose := records && s.refuse, records && s.lose
	if refuse || lose {
		s.outage = s.after
	}
	s.mu.Unlock()
	if refuse {
		return store.Environment{}, errFault
	}
	out, err := s.f.store.CommitEnvironment(id, revision, next)
	if err == nil && lose {
		return store.Environment{}, errFault
	}
	return out, err
}

// ensureNatives returns the Native of every Ensure call so far.
func (r *fakeRuntime) ensureNatives() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, ref := range r.ensures {
		out = append(out, string(ref.Native))
	}
	return out
}

func (f *fixture) reconcileFails(t *testing.T, n int) {
	t.Helper()
	for i := range n {
		if err := f.c.Reconcile(context.Background()); err == nil {
			t.Fatalf("pass %d succeeded with its result uncommitted", i)
		}
	}
}

func TestPendingDeclarationNotRedispatched(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	const decl = "declared-create-0001"
	f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		if string(r.Native) == decl {
			return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte("uid-0001")}, nil
		}
		return environmentcontrol.Observation{Phase: environment.Creating, Native: []byte(decl)}, environmentcontrol.ErrWritePending
	}
	faults := f.faults(decl)
	// Refused unwritten; the readback and the next pass's read fail.
	faults.set(true, false, 2)
	f.reconcileFails(t, 3)
	if got := f.runtime.ensureNatives(); len(got) != 1 {
		t.Fatalf("ensures %q: the runtime was called again before the declaration was committed", got)
	}
	faults.set(false, false, 0)
	f.reconcile(t)
	if got := f.runtime.ensureNatives(); len(got) != 2 || got[1] != decl {
		t.Fatalf("ensures %q, want the declaration dispatched once, after its commit", got)
	}
	f.get(t, "alice", v.ID, environment.Ready)
}

func TestPendingResultLostResponseKeepsLateUID(t *testing.T) {
	f := newFixture(t)
	f.create(t, "alice", "create-1", testSpec("analysis"))
	const uid = "late-uid-0002"
	calls := 0
	f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		calls++
		if calls == 1 {
			// The caller gave up; the late identity still came back.
			return environmentcontrol.Observation{Native: []byte(uid)}, context.Canceled
		}
		return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte(uid)}, nil
	}
	faults := f.faults(uid)
	// The commit lands but its response is lost; the readback and the
	// next two passes' reads fail.
	faults.set(false, true, 3)
	f.reconcileFails(t, 3)
	if got := f.runtime.ensureNatives(); len(got) != 1 {
		t.Fatalf("ensures %q before the result was known committed", got)
	}
	faults.set(false, false, 0)
	f.reconcile(t)
	if got := f.runtime.ensureNatives(); len(got) != 2 || got[1] != uid {
		t.Fatalf("ensures %q, want the next call to carry the late identity", got)
	}
}

func TestPendingResultHeldAgainstAbsentAndSessions(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	const uid = "replaced-uid-0003"
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte(uid)}, nil
	}
	faults := f.faults(uid)
	faults.set(true, false, 0)
	f.reconcileFails(t, 1)

	// No session is admitted on the old identity.
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}}); err == nil {
		t.Fatal("exec admitted with a result uncommitted")
	}
	w := httptest.NewRecorder()
	if _, err := f.c.Forward(w, httptest.NewRequest("GET", "/", nil), f.actors["alice"], v.ID); err == nil || w.Body.Len() != 0 {
		t.Fatalf("forward admitted with a result uncommitted: %v", err)
	}
	f.runtime.mu.Lock()
	execs, forwards := len(f.runtime.execRef), f.runtime.forwards
	f.runtime.mu.Unlock()
	if execs != 0 || forwards != 0 {
		t.Fatalf("%d execs and %d forwards reached the runtime", execs, forwards)
	}

	// A durable Absent lands meanwhile; Delete still waits for the result.
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	f.reconcileFails(t, 2)
	if _, ensures, deletes, _ := f.runtime.counts(); ensures != 2 || deletes != 0 {
		t.Fatalf("%d ensures and %d deletes while the result was uncommitted", ensures, deletes)
	}
	faults.set(false, false, 0)
	f.runtime.remove = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: environment.Deleting}, nil
	}
	f.reconcile(t)
	f.runtime.mu.Lock()
	deletes := append([]environmentcontrol.Ref(nil), f.runtime.deletes...)
	f.runtime.mu.Unlock()
	if len(deletes) != 1 || string(deletes[0].Native) != uid {
		t.Fatalf("deletes %+v, want one Delete of the merged identity", deletes)
	}
	if g := f.get(t, "alice", v.ID, environment.Deleting); g.State.Desired != environment.Absent {
		t.Fatalf("merge undid the deletion: %+v", g.State)
	}
}
