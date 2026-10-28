package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions with real file-authenticated
// actors. Producers are modeled OriginalServerLifetime values: no process,
// liveness or native observation occurs, and a stopped settlement here is a
// structural Store positive, never stop proof. A lost result is modeled by
// discarding the return of a call that committed.

// environmentArtifactClock is artifactTestInstall's clock.
var environmentArtifactClock = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

type environmentArtifactFixture struct {
	*environmentFixture
	refs []ArtifactRef
}

// newEnvironmentArtifactFixture installs artifacts and completes n of
// alice's uploads.
func newEnvironmentArtifactFixture(t *testing.T, n int) *environmentArtifactFixture {
	t.Helper()
	f := &environmentArtifactFixture{environmentFixture: newEnvironmentFixture(t)}
	artifactTestInstall(t, f.s)
	for i := range n {
		f.refs = append(f.refs, environmentTestObject(t, f.s, f.actors["alice"], uint64(42+i)))
	}
	return f
}

// environmentTestObject completes one of actor's uploads, as
// artifactTestComplete does, without its single-object usage checks.
func environmentTestObject(t *testing.T, s *Store, actor auth.Actor, inode uint64) ArtifactRef {
	t.Helper()
	u, err := s.BeginArtifactUpload(actor, strings.Repeat("b", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	file := ArtifactFile{Device: 17, Inode: inode}
	if u, err = s.ArtifactPartCreated(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file); err != nil {
		t.Fatal(err)
	}
	if u, err = s.ArtifactChunkSynced(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file, 0, 1, strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if u, err = s.BeginArtifactFinalize(actor, ArtifactCursor{u.Reference.ID, u.Revision}); err != nil {
		t.Fatal(err)
	}
	ref, err := s.CompleteArtifactUpload(actor, ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, file, ArtifactManifest{Reference: u.Reference, ExpandedBytes: 1, RegularFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// reopen restarts the Store and restores the fixture clock.
func (f *environmentArtifactFixture) reopen(t *testing.T) {
	t.Helper()
	f.restart(t)
	f.s.now = func() time.Time { return environmentArtifactClock }
}

func environmentArtifactSpec(name string, refs ...ArtifactRef) environment.Spec {
	s := environmentSpec(name)
	for i, ref := range refs {
		s.Artifacts = append(s.Artifacts, environment.ArtifactMount{ID: ref.ID, SHA256: ref.SHA256, Size: ref.Size, MountPath: "/skygo/artifacts/in" + string(rune('0'+i))})
	}
	return s
}

// move commits incarnation inc of Present environment e.
func (f *environmentArtifactFixture) move(e Environment, inc string) (Environment, error) {
	return f.s.CommitEnvironment(e.Identity.ID, e.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Present, Phase: environment.Ready}, Checkpoint: e.Checkpoint, Incarnation: inc})
}

func (f *environmentArtifactFixture) incarnate(t *testing.T, e Environment, inc string) Environment {
	t.Helper()
	e, err := f.move(e, inc)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// begin begins the read of ref for e's committed incarnation.
func (f *environmentArtifactFixture) begin(e Environment, ref ArtifactRef, p OriginalServerLifetime) (ArtifactLease, error) {
	return f.s.BeginEnvironmentMaterialization(e.Identity.ID, e.Revision, e.IncarnationSeq, e.Incarnation, ref, p, environmentArtifactClock)
}

func environmentClosure(e Environment, l ArtifactLease) ArtifactDownloadClosure {
	return ArtifactDownloadClosure{Owner: e.Principal, LeaseID: l.ID, Revision: l.Revision, Ref: l.Reference, File: l.File, RootID: l.RootID, Producer: l.Producer}
}

// settle marks and finishes lease l of e, as its original owner does after
// joining every holder, and returns the finished closure.
func (f *environmentArtifactFixture) settle(t *testing.T, e Environment, l ArtifactLease) ArtifactDownloadClosure {
	t.Helper()
	c, err := f.s.MarkArtifactDownloadClosing(environmentClosure(e, l))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.FinishArtifactDownload(c, environmentArtifactClock); err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *environmentArtifactFixture) state(t *testing.T) *artifactState {
	t.Helper()
	var st *artifactState
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		var err error
		st, err = artifactLoad(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return st
}

// witness returns the pin of e's ref and its witness read, if any.
func (f *environmentArtifactFixture) witness(t *testing.T, e Environment, ref ArtifactRef) (artifactEnvironmentPin, artifactRead, bool) {
	t.Helper()
	st := f.state(t)
	p, ok := st.EnvironmentPins[environmentPinKey(e.Identity.ID, ref)]
	if !ok {
		t.Fatal("missing environment pin")
	}
	r, ok := st.Reads[p.Read]
	return p, r, ok
}

func materializationRefused(t *testing.T, name string, l ArtifactLease, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || l != (ArtifactLease{}) {
		t.Fatalf("%s: lease %+v, %v; want %v", name, l, err, want)
	}
}

func sameLease(a, b ArtifactLease) bool {
	return a.ID == b.ID && a.Reference == b.Reference && a.Revision == b.Revision && a.ExpiresAt.Equal(b.ExpiresAt) && a.Closing == b.Closing && a.File == b.File && a.RootID == b.RootID && a.Producer == b.Producer
}

// environmentArtifactDowngrade is a MODEL: it removes the empty environment
// pin table that schema 11 (environmentArtifactSchema) added, so that a test
// can write an earlier schema marker. It refuses a non-empty table and keeps
// the storage format tag; environmentArtifactStripModel removes both.
func environmentArtifactDowngrade(tx *bolt.Tx) error {
	a := tx.Bucket(artifactsBucket)
	if a == nil {
		return nil
	}
	p := a.Bucket([]byte("environment_pins"))
	if p == nil {
		return nil
	}
	if p.Stats().KeyN != 0 {
		return errors.New("environment pins are not empty")
	}
	return a.DeleteBucket([]byte("environment_pins"))
}

func TestEnvironmentArtifactAccept(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 1)
	ref := f.refs[0]
	before := workflowDatabase(t, f.s)
	if _, err := f.s.AcceptEnvironment(f.actors["bob"], f.domain, "b-1", environmentArtifactSpec("b", ref), "test-binding", f.authority["bob"], environmentCheckpoint("c")); !errors.Is(err, ErrEnvironmentArtifact) {
		t.Fatalf("foreign artifact: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Two mounts of one artifact share its pin.
	spec := environmentArtifactSpec("analysis", ref)
	spec.Artifacts = append(spec.Artifacts, environment.ArtifactMount{ID: ref.ID, SHA256: ref.SHA256, Size: ref.Size, MountPath: "/skygo/artifacts/again"})
	e := f.accept(t, "alice", "a-1", spec)
	if len(e.Spec.Artifacts) != 2 || e.Incarnation != "" || e.IncarnationSeq != 0 {
		t.Fatalf("accepted %+v", e)
	}
	st := f.state(t)
	p, ok := st.EnvironmentPins[environmentPinKey(e.Identity.ID, ref)]
	if len(st.EnvironmentPins) != 1 || !ok || p != (artifactEnvironmentPin{Owner: e.Principal, Environment: e.Identity.ID, Ref: ref}) || !st.pinned(ref.ID) {
		t.Fatalf("pins %+v", st.EnvironmentPins)
	}
	if _, err := f.s.RemoveArtifact(f.actors["alice"], ref); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("remove pinned: %v", err)
	}
	// A version 1 environment touches no artifact row.
	before = workflowDatabase(t, f.s)
	f.accept(t, "alice", "a-2", environmentSpec("plain"))
	for k, v := range workflowDatabase(t, f.s) {
		if strings.HasPrefix(k, "artifacts") && before[k] != v {
			t.Errorf("version 1 acceptance changed %q", k)
		}
	}
	f.reopen(t)
	if st := f.state(t); len(st.EnvironmentPins) != 1 {
		t.Fatal("pin lost on reopen")
	}
}

func TestEnvironmentArtifactsDisabled(t *testing.T) {
	f := newEnvironmentFixture(t)
	spec := environmentSpec("analysis")
	spec.Artifacts = []environment.ArtifactMount{{ID: strings.Repeat("1", 32), SHA256: strings.Repeat("2", 64), Size: 1, MountPath: "/skygo/artifacts/in"}}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.AcceptEnvironment(f.actors["alice"], f.domain, "a-1", spec, "test-binding", f.authority["alice"], environmentCheckpoint("c")); !errors.Is(err, ErrArtifactsDisabled) {
		t.Fatalf("accept without artifacts: %v", err)
	}
	workflowUnchanged(t, f.s, before)
}

// The expected sequence is checked before any witness: a delayed call of
// incarnation A at sequence 1 never reaches the read of A at sequence 3.
func TestEnvironmentMaterializationSequenceFence(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 1)
	ref, p := f.refs[0], artifactKnownProducer()
	e := f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", ref))
	l, err := f.begin(e, ref, p)
	materializationRefused(t, "no incarnation", l, err, ErrEnvironmentIncarnation)
	a1 := f.incarnate(t, e, "A")
	if l, err = f.begin(a1, ref, p); err != nil {
		t.Fatal(err)
	}
	f.settle(t, a1, l)
	b2 := f.incarnate(t, a1, "B")
	if l, err = f.begin(b2, ref, p); err != nil {
		t.Fatal(err)
	}
	f.settle(t, b2, l)
	a3 := f.incarnate(t, b2, "A")
	if a1.IncarnationSeq != 1 || b2.IncarnationSeq != 2 || a3.IncarnationSeq != 3 || a3.Incarnation != "A" {
		t.Fatalf("sequences %d %d %d", a1.IncarnationSeq, b2.IncarnationSeq, a3.IncarnationSeq)
	}
	before := workflowDatabase(t, f.s)
	l, err = f.s.BeginEnvironmentMaterialization(a3.Identity.ID, a1.Revision, 3, "A", ref, p, environmentArtifactClock)
	materializationRefused(t, "fresh at a stale revision", l, err, ErrEnvironmentStale)
	workflowUnchanged(t, f.s, before)
	r3, err := f.begin(a3, ref, p)
	if err != nil {
		t.Fatal(err)
	}
	old := func(state string) {
		t.Helper()
		before := workflowDatabase(t, f.s)
		l, err := f.s.BeginEnvironmentMaterialization(a1.Identity.ID, a1.Revision, 1, "A", ref, p, environmentArtifactClock)
		materializationRefused(t, "sequence 1 call, "+state, l, err, ErrEnvironmentIncarnation)
		workflowUnchanged(t, f.s, before)
	}
	// The current sequence replays at a stale revision.
	current := func() (ArtifactLease, error) {
		return f.s.BeginEnvironmentMaterialization(a3.Identity.ID, a1.Revision, 3, "A", ref, p, environmentArtifactClock)
	}
	old("open")
	if got, err := current(); err != nil || !sameLease(got, r3) {
		t.Fatalf("open replay %+v %v, want %+v", got, err, r3)
	}
	c, err := f.s.MarkArtifactDownloadClosing(environmentClosure(a3, r3))
	if err != nil {
		t.Fatal(err)
	}
	old("closing")
	r3.Closing, r3.Revision = true, c.Revision
	if got, err := current(); err != nil || !sameLease(got, r3) {
		t.Fatalf("closing replay %+v %v, want %+v", got, err, r3)
	}
	if err := f.s.FinishArtifactDownload(c, environmentArtifactClock); err != nil {
		t.Fatal(err)
	}
	old("closed")
	before = workflowDatabase(t, f.s)
	l, err = current()
	materializationRefused(t, "closed replay", l, err, ErrEnvironmentMaterialized)
	for _, tt := range []struct {
		name        string
		seq         uint64
		incarnation string
	}{
		{"zero", 0, "A"},
		{"zero and empty", 0, ""},
		{"next", 4, "A"},
		{"other incarnation", 3, "B"},
		{"empty incarnation", 3, ""},
	} {
		l, err := f.s.BeginEnvironmentMaterialization(a3.Identity.ID, a3.Revision, tt.seq, tt.incarnation, ref, p, environmentArtifactClock)
		materializationRefused(t, tt.name, l, err, ErrEnvironmentIncarnation)
	}
	workflowUnchanged(t, f.s, before)
}

// A Begin whose result is lost holds the transition until its original
// owner recovers the lease and settles it.
func TestEnvironmentMaterializationLostBegin(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 2)
	p := artifactKnownProducer()
	a1 := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", f.refs...)), "A")
	if _, err := f.begin(a1, f.refs[0], p); err != nil {
		t.Fatal(err)
	}
	pin, stored, ok := f.witness(t, a1, f.refs[0])
	if !ok || stored.Closed {
		t.Fatal("Begin did not commit an open read")
	}
	before := workflowDatabase(t, f.s)
	if _, err := f.move(a1, "B"); !errors.Is(err, ErrEnvironmentArtifactHeld) {
		t.Fatalf("transition over an open read: %v", err)
	}
	l, err := f.begin(a1, f.refs[0], p)
	if err != nil || l.ID != pin.Read {
		t.Fatalf("recovered lease %+v %v, want %s", l, err, pin.Read)
	}
	workflowUnchanged(t, f.s, before)
	f.settle(t, a1, l)
	b2, err := f.move(a1, "B")
	if err != nil || b2.IncarnationSeq != 2 {
		t.Fatalf("transition after settlement: %+v %v", b2, err)
	}
	n := 0
	for _, r := range f.state(t).Reads {
		if r.Environment == a1.Identity.ID {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d environment reads", n)
	}
	// After the committed transition the old tuple is definitely not
	// admitted: no read of it exists.
	before = workflowDatabase(t, f.s)
	l, err = f.begin(a1, f.refs[1], p)
	materializationRefused(t, "old tuple", l, err, ErrEnvironmentIncarnation)
	workflowUnchanged(t, f.s, before)
}

// A Finish whose result is lost is retried before any transition, while
// its receipt exists. A transition before then loses the receipt.
func TestEnvironmentMaterializationLostFinish(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 2)
	p := artifactKnownProducer()
	a1 := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", f.refs...)), "A")
	l, err := f.begin(a1, f.refs[0], p)
	if err != nil {
		t.Fatal(err)
	}
	c := f.settle(t, a1, l)
	before := workflowDatabase(t, f.s)
	if err := f.s.FinishArtifactDownload(c, environmentArtifactClock); err != nil {
		t.Fatalf("retried Finish: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	b2 := f.incarnate(t, a1, "B")
	l2, err := f.begin(b2, f.refs[0], p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.state(t).Reads[l.ID]; ok {
		t.Fatal("replaced receipt kept")
	}
	f.settle(t, b2, l2)
	// The barrier violated: the Finish result of l3 is not yet observed.
	l3, err := f.begin(b2, f.refs[1], p)
	if err != nil {
		t.Fatal(err)
	}
	c3 := f.settle(t, b2, l3)
	e4 := f.incarnate(t, b2, "C")
	if _, err := f.begin(e4, f.refs[1], p); err != nil {
		t.Fatal(err)
	}
	if err := f.s.FinishArtifactDownload(c3, environmentArtifactClock); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("Finish after receipt replacement: %v", err)
	}
}

// Another server lifetime's open read holds the replay, every transition
// and Deleted until a stopped settlement closes it.
func TestEnvironmentMaterializationRestart(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 1)
	ref, p1 := f.refs[0], artifactKnownProducer()
	p2 := p1
	p2.PID++
	a1 := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", ref)), "A")
	l, err := f.begin(a1, ref, p1)
	if err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	before := workflowDatabase(t, f.s)
	got, err := f.begin(a1, ref, p2)
	materializationRefused(t, "other lifetime", got, err, ErrEnvironmentMaterializationHeld)
	if _, err := f.move(a1, "B"); !errors.Is(err, ErrEnvironmentArtifactHeld) {
		t.Fatalf("transition: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	deleting := f.commit(t, a1, environment.Absent, environment.Deleting)
	deleted := EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: environment.Deleted}, Checkpoint: deleting.Checkpoint}
	before = workflowDatabase(t, f.s)
	if _, err := f.s.CommitEnvironment(deleting.Identity.ID, deleting.Revision, deleted); !errors.Is(err, ErrEnvironmentArtifactHeld) {
		t.Fatalf("Deleted: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Structural only: the caller of SettleStoppedArtifactDownload holds the
	// separately qualified stop proof.
	if err := f.s.SettleStoppedArtifactDownload(environmentClosure(a1, l)); err != nil {
		t.Fatal(err)
	}
	got, err = f.begin(deleting, ref, p2)
	materializationRefused(t, "settled", got, err, ErrEnvironmentMaterialized)
	gone, err := f.s.CommitEnvironment(deleting.Identity.ID, deleting.Revision, deleted)
	if err != nil {
		t.Fatal(err)
	}
	st := f.state(t)
	if len(st.EnvironmentPins) != 0 || st.pinned(ref.ID) || st.Objects[ref.ID].DeleteAfter.Before(environmentArtifactClock.Add(time.Hour)) {
		t.Fatalf("Deleted kept pins %+v or gave no grace", st.EnvironmentPins)
	}
	got, err = f.begin(gone, ref, p1)
	materializationRefused(t, "deleted", got, err, ErrNotFound)
}

// An unsettled read replays only to a fully known, equal producer. A nil,
// zero or partial identity is no proof of the same lifetime.
func TestEnvironmentMaterializationReplayProducer(t *testing.T) {
	known := artifactKnownProducer()
	other, noDevice, noTicks := known, known, known
	other.PID++
	noDevice.NamespaceDevice = 0
	noTicks.StartTicks = 0
	for _, tt := range []struct {
		name            string
		stored, request OriginalServerLifetime
		closing         bool
		replay          bool
	}{
		{"U1 zero", OriginalServerLifetime{}, OriginalServerLifetime{}, false, false},
		{"U1c zero closing", OriginalServerLifetime{}, OriginalServerLifetime{}, true, false},
		{"U2 zero then known", OriginalServerLifetime{}, known, false, false},
		{"U3 no device", noDevice, noDevice, false, false},
		{"U3c no device closing", noDevice, noDevice, true, false},
		{"U4 no ticks", noTicks, noTicks, false, false},
		{"U4c no ticks closing", noTicks, noTicks, true, false},
		{"U5 request no device", known, noDevice, false, false},
		{"U5 request no ticks", known, noTicks, false, false},
		{"K1 known", known, known, false, true},
		{"K1c known closing", known, known, true, true},
		{"K2 other", known, other, false, false},
		{"K2c other closing", known, other, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newEnvironmentArtifactFixture(t, 1)
			ref := f.refs[0]
			e := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", ref)), "A")
			l, err := f.begin(e, ref, tt.stored)
			if err != nil {
				t.Fatal(err)
			}
			if tt.closing {
				c, err := f.s.MarkArtifactDownloadClosing(environmentClosure(e, l))
				if err != nil {
					t.Fatal(err)
				}
				l.Closing, l.Revision = true, c.Revision
			}
			before := workflowDatabase(t, f.s)
			got, err := f.begin(e, ref, tt.request)
			if tt.replay {
				if err != nil || !sameLease(got, l) {
					t.Fatalf("replay %+v %v, want %+v", got, err, l)
				}
			} else {
				materializationRefused(t, "replay", got, err, ErrEnvironmentMaterializationHeld)
			}
			workflowUnchanged(t, f.s, before)
		})
	}
}

// Z1 and L2: a settled read is already materialized for every producer, and
// an original owner that kept its lease settles it whatever its producer.
func TestEnvironmentMaterializationSettledAnyProducer(t *testing.T) {
	partial := artifactKnownProducer()
	partial.StartTicks = 0
	for _, p := range []OriginalServerLifetime{{}, partial, artifactKnownProducer()} {
		f := newEnvironmentArtifactFixture(t, 1)
		ref := f.refs[0]
		e := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", ref)), "A")
		l, err := f.begin(e, ref, p)
		if err != nil {
			t.Fatal(err)
		}
		f.settle(t, e, l)
		if _, r, ok := f.witness(t, e, ref); !ok || !r.Closed {
			t.Fatalf("%+v: read not settled", p)
		}
		before := workflowDatabase(t, f.s)
		got, err := f.begin(e, ref, p)
		materializationRefused(t, "settled", got, err, ErrEnvironmentMaterialized)
		workflowUnchanged(t, f.s, before)
	}
}

// F1: a fresh read admits a nil or partial producer, stored as given.
func TestEnvironmentMaterializationUnknownAdmission(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 2)
	partial := artifactKnownProducer()
	partial.NamespaceDevice = 0
	e := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", f.refs...)), "A")
	for i, p := range []OriginalServerLifetime{{}, partial} {
		l, err := f.begin(e, f.refs[i], p)
		if err != nil || l.Producer != p {
			t.Fatalf("admission %d: %+v %v", i, l, err)
		}
		if _, r, ok := f.witness(t, e, f.refs[i]); !ok || artifactProducerValue(r.Producer) != p || (r.Producer == nil) != (i == 0) {
			t.Fatalf("admission %d stored producer %+v", i, r.Producer)
		}
	}
}

// L1: the lost Begin of an unknown producer is never reconciled. It holds
// the read, every transition and Deleted, and no stopped settlement takes
// its missing identity. This is the accepted HOLD, not a recovery.
func TestEnvironmentMaterializationUnknownLostBegin(t *testing.T) {
	f := newEnvironmentArtifactFixture(t, 1)
	ref := f.refs[0]
	e := f.incarnate(t, f.accept(t, "alice", "a-1", environmentArtifactSpec("analysis", ref)), "A")
	if _, err := f.begin(e, ref, OriginalServerLifetime{}); err != nil {
		t.Fatal(err)
	}
	pin, r, ok := f.witness(t, e, ref)
	if !ok || r.Closed || r.Producer != nil {
		t.Fatalf("lost Begin stored %+v", r)
	}
	before := workflowDatabase(t, f.s)
	got, err := f.begin(e, ref, OriginalServerLifetime{})
	materializationRefused(t, "retry", got, err, ErrEnvironmentMaterializationHeld)
	if _, err := f.move(e, "B"); !errors.Is(err, ErrEnvironmentArtifactHeld) {
		t.Fatalf("transition: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	deleting := f.commit(t, e, environment.Absent, environment.Deleting)
	before = workflowDatabase(t, f.s)
	if _, err := f.s.CommitEnvironment(deleting.Identity.ID, deleting.Revision, EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: environment.Deleted}, Checkpoint: deleting.Checkpoint}); !errors.Is(err, ErrEnvironmentArtifactHeld) {
		t.Fatalf("Deleted: %v", err)
	}
	st := f.state(t)
	c := ArtifactDownloadClosure{Owner: e.Principal, LeaseID: pin.Read, Revision: r.Revision, Ref: ref, File: st.Objects[ref.ID].File, RootID: st.Root.ID}
	if err := f.s.SettleStoppedArtifactDownload(c); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("stopped settlement of an unknown producer: %v", err)
	}
	workflowUnchanged(t, f.s, before)
}
