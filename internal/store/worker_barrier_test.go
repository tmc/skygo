package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// All native receipts below are MODELED. Transactions, credentials, reopen,
// Backup and Restore use the actual Store and filesystem.
func barrierRequest(f *workerFixture, id string, mode backend.WorkerMode) WorkerGroupRequest {
	r := f.request("alice", id, 2)
	r.Task.CPUMillis = 1000
	r.Task.MemoryBytes = 1 << 30
	r.Spec.Mode = mode
	return r
}
func barrierSubmit(t *testing.T, f *workerFixture, id string, mode backend.WorkerMode) WorkerGroup {
	t.Helper()
	g, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], barrierRequest(f, id, mode))
	if e != nil {
		t.Fatal(e)
	}
	return g
}
func barrierIntent(t *testing.T, f *workerFixture, g WorkerGroup, rank uint8, e WorkerEffect) (WorkerToken, WorkerGroup) {
	t.Helper()
	tok, out, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, rank, e, workerNowUTC())
	if err != nil {
		t.Fatalf("intent %d/%d: %v", rank, e, err)
	}
	return tok, out
}
func barrierRecord(t *testing.T, f *workerFixture, tok WorkerToken, r WorkerBarrierReceipt, cause string) WorkerGroup {
	t.Helper()
	g, e := f.s.RecordWorkerBarrierEffect(tok, r, cause, workerNowUTC())
	if e != nil {
		t.Fatalf("record %d/%d: %v", tok.Rank, tok.Effect, e)
	}
	return g
}
func barrierCreated(t *testing.T, f *workerFixture, g WorkerGroup) WorkerGroup {
	t.Helper()
	for rank := range g.Group.Spec.Count {
		tok, next := barrierIntent(t, f, g, rank, WorkerCreate)
		r := backend.WorkerCreated{Pod: workerPod(g.Job.ID, rank)}
		if rank == 0 && g.Group.Spec.Mode == backend.WorkerTCP {
			r.Service = backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
		}
		g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: r}, "")
		_ = next
	}
	return g
}
func barrierReleased(t *testing.T, f *workerFixture, g WorkerGroup) WorkerGroup {
	t.Helper()
	for rank := range g.Group.Spec.Count {
		tok, _ := barrierIntent(t, f, g, rank, WorkerRelease)
		g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Released: backend.WorkerReleased{Pod: g.Members[rank].Pod, Acknowledged: true, Revision: "1"}}, "")
	}
	return g
}
func barrierPrepared(t *testing.T, f *workerFixture, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	o := backend.WorkerBarrier{Pod: g.Members[rank].Pod, Revision: "2", Node: "node-a", Prepared: true}
	if rank == 0 && g.Group.Spec.Mode == backend.WorkerTCP {
		o.Rendezvous = backend.WorkerRendezvous{Service: g.Members[0].Barrier.Service, Host: "rendezvous.ns.svc", Port: 29500, Routed: true}
	}
	out, e := f.s.RecordWorkerBarrier(g.Job.ID, g.Revision, rank, o, workerNowUTC())
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func workerDatabaseBytes(t *testing.T, f *workerFixture) []byte {
	t.Helper()
	b, e := os.ReadFile(f.s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestWorkerBarrierLifecycle(t *testing.T) {
	for _, mode := range []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkerFixture(t)
			g := f.admit(t, barrierSubmit(t, f, "lifecycle", mode))
			g = barrierCreated(t, f, g)
			g = barrierReleased(t, f, g)
			g = barrierPrepared(t, f, g, 0)
			before := workerDatabaseBytes(t, f)
			if _, e := f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierStart, workerNowUTC()); e == nil {
				t.Fatal("start before every prepared member")
			}
			if !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("refused barrier wrote database")
			}
			if g.Members[0].Barrier.StartToken != ([32]byte{}) {
				t.Fatal("token before decision")
			}
			g = barrierPrepared(t, f, g, 1)
			var e error
			g, e = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierStart, workerNowUTC())
			if e != nil {
				t.Fatal(e)
			}
			before = workerDatabaseBytes(t, f)
			same, e := f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierStart, workerNowUTC())
			if e != nil || !reflect.DeepEqual(same, g) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("same decision not read-only", e)
			}
			for rank := range g.Group.Spec.Count {
				if g.Members[rank].Barrier.StartToken == ([32]byte{}) {
					t.Fatal("missing token")
				}
				tok, _ := barrierIntent(t, f, g, rank, WorkerStart)
				g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Started: backend.WorkerStarted{Pod: g.Members[rank].Pod, Acknowledged: true, Revision: "3"}}, "")
			}
			for rank := range g.Group.Spec.Count {
				o := backend.WorkerBarrier{Pod: g.Members[rank].Pod, Revision: "4", Node: "node-a", Started: true, Stop: backend.WorkerTerminated, Exit: &backend.ContainerExit{ContainerID: fmt.Sprintf("container-%d", rank), ExitCode: 0, StartedAt: workerT0, FinishedAt: workerT0.Add(time.Second)}}
				if rank == 0 && mode == backend.WorkerTCP {
					o.Rendezvous = backend.WorkerRendezvous{Service: g.Members[0].Barrier.Service, Host: "rendezvous.ns.svc", Port: 29500}
				}
				g, e = f.s.RecordWorkerBarrier(g.Job.ID, g.Revision, rank, o, workerNowUTC())
				if e != nil {
					t.Fatal(e)
				}
			}
			if g.Job.Status.State != backend.Succeeded || !g.Reserved {
				t.Fatalf("pre-retirement %+v", g)
			}
			for _, rank := range []uint8{1, 0} {
				tok, _ := barrierIntent(t, f, g, rank, WorkerRemove)
				r := backend.WorkerRemoval{Pod: g.Members[rank].Pod, PodRemoved: true}
				if rank == 0 && mode == backend.WorkerTCP {
					r.Service = g.Members[0].Barrier.Service
					r.ServiceRemoved = true
				}
				g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: r}, "")
			}
			if g.Phase != WorkerGroupDone || g.Reserved {
				t.Fatal("whole reservation not released")
			}
			f.durable(t, g.Job.ID)
		})
	}
}

func TestWorkerBarrierReplayAndCAS(t *testing.T) {
	f := newWorkerFixture(t)
	r := barrierRequest(f, "replay", backend.WorkerIndependent)
	g, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r)
	if e != nil {
		t.Fatal(e)
	}
	before := workerDatabaseBytes(t, f)
	got, found, e := f.s.LookupWorkerGroup(f.actors["alice"], r.Domain, r.RequestID, r.Task, r.Spec)
	if e != nil || !found || got.Job.ID != g.Job.ID {
		t.Fatal(got, found, e)
	}
	again, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r)
	if e != nil || again.Job.ID != g.Job.ID || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("canonical replay wrote state", e)
	}
	if _, _, e := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision+1, 0, WorkerCreate, workerNowUTC()); !errors.Is(e, ErrWorkerGroupStale) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("stale CAS wrote state", e)
	}
	r.RequestID = "undercharge"
	r.Demand.CPUMillis--
	if _, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r); e == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("undercharged acceptance", e)
	}
	r = barrierRequest(f, "limits", backend.WorkerIndependent)
	r.Task.CPULimitMillis = 2000
	r.Demand.CPUMillis = 2000
	if _, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r); e != nil {
		t.Fatal(e)
	}
}

func TestWorkerBarrierLateEffectReadback(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "late", backend.WorkerIndependent))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	f.cancel(t, "alice", g.Job.ID)
	r := WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}}
	g = barrierRecord(t, f, tok, r, "original timeout")
	if g.Members[0].Pod != r.Created.Pod || !g.Job.CancelRequested || g.Members[0].Create.Error != "original_timeout" {
		t.Fatal("late original discarded")
	}
	before := workerDatabaseBytes(t, f)
	again, e := f.s.RecordWorkerBarrierEffect(tok, r, "original timeout", workerNowUTC())
	if e != nil || !reflect.DeepEqual(g, again) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("readback not idempotent", e)
	}
	r.Created.Pod.UID = "different"
	if _, e := f.s.RecordWorkerBarrierEffect(tok, r, "original timeout", workerNowUTC()); e == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("differing same-token result accepted", e)
	}
}

func TestWorkerBarrierUnknownRemoveHolds(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "unknown-remove", backend.WorkerIndependent))
	g = barrierCreated(t, f, g)
	g = barrierReleased(t, f, g)
	var e error
	g, e = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierAbort, workerNowUTC())
	if e != nil {
		t.Fatal(e)
	}
	o := backend.WorkerBarrier{Pod: g.Members[0].Pod, Revision: "2", Deleting: true, Stop: backend.WorkerNeverBound}
	g, e = f.s.RecordWorkerBarrier(g.Job.ID, g.Revision, 0, o, workerNowUTC())
	if e != nil {
		t.Fatal(e)
	}
	tok, _ := barrierIntent(t, f, g, 0, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[0].Pod}}, "lost response")
	if g.Members[0].Barrier.PodRemoveSent {
		t.Fatal("intent fabricated dispatch")
	}
	before := workerDatabaseBytes(t, f)
	if _, _, e := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRecover, workerNowUTC()); e == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("unknown removal recovered without dispatch", e)
	}
	if !g.Reserved || g.Phase == WorkerGroupDone {
		t.Fatal("unknown removal refunded group")
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierPartialCreateKindCustody(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "partial", backend.WorkerTCP))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: service}, Refused: true}, "pod admission refused")
	if g.Phase != WorkerGroupHeld || g.Members[0].Pod != (backend.WorkerResource{}) || g.Members[0].Barrier.Service != service {
		t.Fatal("partial kinds conflated")
	}
	tok, _ = barrierIntent(t, f, g, 0, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: service, ServiceRemoved: true}}, "")
	if g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Failed {
		t.Fatal("cleanup-only group failed settlement", g.Phase)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierAuthUnavailableNoRevoke(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "auth", backend.WorkerIndependent))
	before := workerDatabaseBytes(t, f)
	if _, e := f.s.RevokeWorkerGroup(g.Job.ID, g.Revision, auth.ErrUnavailable, workerNowUTC()); e == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("unavailable auth revoked group", e)
	}
	g, e := f.s.RevokeWorkerGroup(g.Job.ID, g.Revision, auth.ErrPermissionDenied, workerNowUTC())
	if e != nil || !g.Revoked || g.Reserved || g.Phase != WorkerGroupDone {
		t.Fatal("true loss not effective", e)
	}
}

func TestWorkerBarrierTransactionBudgetRollback(t *testing.T) {
	f := newWorkerFixture(t)
	g := barrierSubmit(t, f, "budget", backend.WorkerIndependent)
	before := workerDatabaseBytes(t, f)
	err := f.s.db.Update(func(tx *bolt.Tx) error {
		check := workerBarrierBudget(tx, g.Job.ID, g.Principal, g.Domain, g.RequestID)
		if e := tx.Bucket(jobsBucket).Put([]byte(g.Job.ID), bytes.Repeat([]byte("x"), maxChangeBytes+1)); e != nil {
			return e
		}
		return check()
	})
	if err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("oversized transaction not atomically refused", err)
	}
}

// TestWorkerBarrierLostAfterRestart uses an actual second process generation.
// It never replaces workerGen and never launches a native worker.
func TestWorkerBarrierLostAfterRestart(t *testing.T) {
	if dir := os.Getenv("SKYGO_BARRIER_CHILD"); dir != "" {
		s, e := Open(dir)
		if e != nil {
			t.Fatal(e)
		}
		groups, e := s.WorkerGroups(0, 1)
		if e != nil || len(groups) != 1 {
			t.Fatalf("child groups: %v", e)
		}
		_, _, e = s.IntendWorkerBarrier(groups[0].Job.ID, groups[0].Revision, 0, WorkerCreate, workerNowUTC())
		if closeErr := s.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if e != nil {
			t.Fatal(e)
		}
		return
	}
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "child-restart", backend.WorkerIndependent))
	if e := f.s.Close(); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestWorkerBarrierLostAfterRestart$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SKYGO_BARRIER_CHILD="+f.dir)
	output, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("child Wait: %v: %s", e, output)
	}
	t.Logf("actual child pid=%d Wait=%s exited=%v", cmd.Process.Pid, cmd.ProcessState, cmd.ProcessState.Exited())
	f.open(t)
	g = f.group(t, g.Job.ID)
	effect := g.Members[0].Create
	if effect.State != WorkerIntended || effect.Gen == workerGen() {
		t.Fatal("no actual foreign process intent")
	}
	g, e = f.s.RecordWorkerLost(WorkerToken{Job: g.Job.ID, Rank: 0, Effect: WorkerCreate, Seq: effect.Seq, Gen: effect.Gen}, workerNowUTC())
	if e != nil || g.Phase != WorkerGroupHeld || !g.Reserved || g.Members[0].Create.State != WorkerUnknown {
		t.Fatal("foreign intent did not hold reservation", e)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierMaximumTaskTransaction(t *testing.T) {
	f := newWorkerFixture(t)
	r := barrierRequest(f, "max-task", backend.WorkerIndependent)
	r.Spec.Count = 16
	r.Demand.Ranks = 16
	r.Task.Argv = append(r.Task.Argv, "")
	raw, encodeErr := backend.TaskBytes(r.Task)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	r.Task.Argv[len(r.Task.Argv)-1] = strings.Repeat("x", (256<<10)-len(raw))
	raw, encodeErr = backend.TaskBytes(r.Task)
	if encodeErr != nil || len(raw) != 256<<10 {
		t.Fatalf("not maximum accepted task: %d %v", len(raw), encodeErr)
	}
	var before map[string][]byte
	capture := func(tx *bolt.Tx) (map[string][]byte, error) {
		m := map[string][]byte{}
		var visit func(*bolt.Bucket, string) error
		visit = func(b *bolt.Bucket, path string) error {
			m[path+"#sequence"] = sequenceKey(b.Sequence())
			return b.ForEach(func(k, v []byte) error {
				p := path + "/" + string(k)
				if v == nil {
					return visit(b.Bucket(k), p)
				}
				m[p] = bytes.Clone(v)
				return nil
			})
		}
		e := tx.ForEach(func(k []byte, b *bolt.Bucket) error { return visit(b, string(k)) })
		return m, e
	}
	if e := f.s.db.View(func(tx *bolt.Tx) error { var e error; before, e = capture(tx); return e }); e != nil {
		t.Fatal(e)
	}
	g, e := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r)
	if e != nil {
		t.Fatal(e)
	}
	var changed uint64
	if e := f.s.db.View(func(tx *bolt.Tx) error {
		after, e := capture(tx)
		if e != nil {
			return e
		}
		for k, v := range after {
			old := before[k]
			if !bytes.Equal(old, v) {
				changed += uint64(len(k) + len(old) + len(v))
			}
			delete(before, k)
		}
		for k, v := range before {
			changed += uint64(len(k) + len(v))
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	t.Logf("actual all-family changed-cell encoding including paths/sequences=%d; limit=%d", changed, maxChangeBytes)
	if changed > maxChangeBytes {
		t.Fatal("full acceptance transaction exceeds byte bound")
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierRecoverOriginalReference(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "recover-reference", backend.WorkerTCP))
	g = barrierCreated(t, f, g)
	var err error
	g, err = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierAbort, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	for rank := range g.Group.Spec.Count {
		g = barrierNeverBound(t, f, g, rank)
	}
	tok, _ := barrierIntent(t, f, g, 1, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[1].Pod, PodRemoved: true}}, "")
	tok, _ = barrierIntent(t, f, g, 0, WorkerRemove)
	original := WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[0].Pod, Service: g.Members[0].Barrier.Service, PodRemoved: true, PodDispatched: true, ServiceDispatched: true}}
	g = barrierRecord(t, f, tok, original, "service response lost")
	if !g.Members[0].Barrier.RemoveHadPod || !g.Members[0].Removed {
		t.Fatal("original Pod retirement not retained")
	}
	before := workerDatabaseBytes(t, f)
	again, err := f.s.RecordWorkerBarrierEffect(tok, original, "service response lost", workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, again) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("original Remove reference changed after partial retirement", err)
	}
	tok, g = barrierIntent(t, f, g, 0, WorkerRecover)
	if g.Members[0].Barrier.RecoverHadPod {
		t.Fatal("recovery included already retired Pod")
	}
	receipt := WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: g.Members[0].Barrier.Service, ServiceRemoved: true}}
	g = barrierRecord(t, f, tok, receipt, "")
	if g.Phase != WorkerGroupDone || g.Reserved {
		t.Fatal("qualified modeled recovery did not settle")
	}
	before = workerDatabaseBytes(t, f)
	again, err = f.s.RecordWorkerBarrierEffect(tok, receipt, "", workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, again) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("same original Recover readback changed", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierLocalUnsettledLatch(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "local-owner", backend.WorkerIndependent))
	g = barrierCreated(t, f, g)
	var err error
	g, err = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierAbort, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	for rank := range g.Group.Spec.Count {
		g = barrierNeverBound(t, f, g, rank)
	}
	tok, _ := barrierIntent(t, f, g, 1, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[1].Pod, PodRemoved: true}}, "")
	tok, _ = barrierIntent(t, f, g, 0, WorkerRemove)
	r := WorkerBarrierReceipt{Unsettled: true, Removed: backend.WorkerRemoval{Pod: g.Members[0].Pod, PodRemoved: true}}
	g = barrierRecord(t, f, tok, r, "original local owner unsettled")
	if !g.Members[0].Removed || !g.Members[0].Barrier.LocalUnsettled || !g.Reserved || g.Phase == WorkerGroupDone {
		t.Fatal("Store ACK discharged local owner")
	}
	before := workerDatabaseBytes(t, f)
	same, err := f.s.RecordWorkerBarrierEffect(tok, r, "original local owner unsettled", workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, same) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("unsettled original replay changed", err)
	}
	different := r
	different.Unsettled = false
	if _, err := f.s.RecordWorkerBarrierEffect(tok, different, "original local owner unsettled", workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("same-token replay cleared original typed marker", err)
	}
	same, err = f.s.SettleWorkerGroup(g.Job.ID, g.Revision, workerNowUTC())
	if err != nil || !same.Reserved || same.Phase == WorkerGroupDone {
		t.Fatal("settlement ignored local owner", err)
	}
	if _, _, err = f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerCancel, workerNowUTC()); err == nil {
		t.Fatal("new native intent with unresolved local owner")
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierFIFOWholeGroupRefund(t *testing.T) {
	f := newWorkerFixture(t)
	first := barrierSubmit(t, f, "head", backend.WorkerIndependent)
	second := barrierSubmit(t, f, "tail", backend.WorkerIndependent)
	budget := workerBudget
	budget.Ranks = 2
	var ok bool
	var err error
	if _, ok, err = f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC()); err != nil || ok {
		t.Fatal("FIFO bypassed head", err)
	}
	first, ok, err = f.s.AdmitWorkerGroup(first.Job.ID, first.Revision, budget, workerNowUTC())
	if err != nil || !ok {
		t.Fatal("head admission", err)
	}
	if _, ok, err = f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC()); err != nil || ok {
		t.Fatal("whole budget overbooked", err)
	}
	f.cancel(t, "alice", first.Job.ID)
	first = f.group(t, first.Job.ID)
	first, err = f.s.SettleWorkerGroup(first.Job.ID, first.Revision, workerNowUTC())
	if err != nil || first.Reserved || first.Phase != WorkerGroupDone {
		t.Fatal("cancel did not refund whole unused group", err)
	}
	if _, ok, err = f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC()); err != nil || !ok {
		t.Fatal("whole refund not available to head", err)
	}
}

func TestWorkerBarrierReadLatch(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "read-owner", backend.WorkerTCP))
	g = barrierCreated(t, f, g)
	ref := backend.WorkerReference{Group: g.Group, Rank: 1, Pod: g.Members[1].Pod, Service: g.Members[0].Barrier.Service}
	before := workerDatabaseBytes(t, f)
	wrong := ref
	wrong.Service.UID = "foreign-service"
	if _, err := f.s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 1, wrong, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("read owner bound to foreign resource", err)
	}
	var err error
	g, err = f.s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 1, ref, workerNowUTC())
	if err != nil || !g.Reserved || !g.Members[1].Barrier.LocalUnsettled {
		t.Fatal("read owner not latched", err)
	}
	before = workerDatabaseBytes(t, f)
	same, err := f.s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 1, ref, workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, same) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("same read latch not read-only", err)
	}
	if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRelease, workerNowUTC()); err == nil {
		t.Fatal("new native effect crossed read owner fence")
	}
	f.durable(t, g.Job.ID)
}

// This is an encoding/budget proof with MODELED maximal native facts, not a
// lifecycle test. It uses the actual validators, JSON writer, Store transaction
// guard and every retained family cell. The public lifecycle is tested above.
func TestWorkerBarrierMaximumFutureTransaction(t *testing.T) {
	f := newWorkerFixture(t)
	r := barrierRequest(f, "maximum-future", backend.WorkerTCP)
	r.Spec.Count = 16
	r.Demand.Ranks = 16
	r.Task.Argv = append(r.Task.Argv, "")
	raw, err := backend.TaskBytes(r.Task)
	if err != nil {
		t.Fatal(err)
	}
	r.Task.Argv[len(r.Task.Argv)-1] = strings.Repeat("x", (256<<10)-len(raw))
	g, err := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r)
	if err != nil {
		t.Fatal(err)
	}
	g = f.admit(t, g)
	capture := func() map[string][]byte {
		var out map[string][]byte
		err := f.s.db.View(func(tx *bolt.Tx) error { out = workerBarrierTestCells(tx); return nil })
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	update := func(second bool) error {
		return f.s.db.Update(func(tx *bolt.Tx) error {
			w, e := loadWorkerState(tx, g.Job.ID)
			if e != nil {
				return e
			}
			if !second {
				_, template := maximalWorkerBarrierRecords()
				w.g.Phase = WorkerGroupActive
				w.g.Barrier.Decision = WorkerBarrierStart
				w.g.Barrier.DecidedAt = template.Barrier.StopAt
				w.job.Status = backend.Status{State: backend.Failed, Reason: strings.Repeat("r", 8000)}
				for rank := range w.m {
					m := template
					b := *template.Barrier
					m.Barrier = &b
					m.Job = g.Job.ID
					m.Rank = uint8(rank)
					m.Pod.Name = fmt.Sprintf("p%02d", rank) + strings.Repeat("p", 60)
					m.Pod.UID = strings.Repeat("<", 126) + fmt.Sprintf("%02x", rank)
					b.LocalUnsettled = true
					for _, x := range m.effects() {
						x.Unsettled = true
					}
					m.Release.State = WorkerRecorded
					m.ReleaseAck = true
					m.Revision = strings.Repeat("r", workerRevisionLimit)
					if rank != 0 {
						b.Service = workerPodRecord{}
						b.ServiceRemoveSent = false
						b.Observation = &workerBarrierObservationRecord{Revision: b.Observation.Revision, Node: b.Observation.Node, Started: true, Deleting: true, At: b.Observation.At}
					}
					w.m[rank] = m
				}
				for rank := range w.m {
					token, e := backend.WorkerStartToken(w.barrierRef(uint8(rank), false))
					if e != nil {
						return e
					}
					w.m[rank].Barrier.StartToken = fmt.Sprintf("%x", token)
				}
			} else {
				w.job.Status.Reason = strings.Repeat("s", 8000)
				for rank := range w.m {
					m := &w.m[rank]
					for _, x := range m.effects() {
						x.Error = strings.Repeat("f", workerErrorLimit)
					}
					m.Barrier.Observation.Revision = strings.Repeat("s", workerRevisionLimit)
				}
			}
			if e := w.save(tx, workerNowUTC()); e != nil {
				return e
			}
			return validateWorkerGroups(tx)
		})
	}
	if err := update(false); err != nil {
		t.Fatal("maximal first state", err)
	}
	before := capture()
	if err := update(true); err != nil {
		t.Fatal("maximal later mutation", err)
	}
	after := capture()
	var changed uint64
	for k, v := range after {
		old := before[k]
		if !bytes.Equal(old, v) {
			changed += uint64(len(k) + len(old) + len(v))
		}
		delete(before, k)
	}
	for k, v := range before {
		changed += uint64(len(k) + len(v))
	}
	t.Logf("actual maximum accepted Task later all-family mutation bytes=%d limit=%d", changed, maxChangeBytes)
	if changed > maxChangeBytes {
		t.Fatal("accepted task cannot progress")
	}
	f.durable(t, g.Job.ID)
}

func workerBarrierTestCells(tx *bolt.Tx) map[string][]byte {
	out := map[string][]byte{}
	var visit func(*bolt.Bucket, string)
	visit = func(b *bolt.Bucket, path string) {
		out[path+"#sequence"] = sequenceKey(b.Sequence())
		_ = b.ForEach(func(k, v []byte) error {
			p := path + "/" + string(k)
			if v == nil {
				visit(b.Bucket(k), p)
			} else {
				out[p] = bytes.Clone(v)
			}
			return nil
		})
	}
	_ = tx.ForEach(func(k []byte, b *bolt.Bucket) error { visit(b, string(k)); return nil })
	return out
}

func TestWorkerBarrierResolveIsNotAbsence(t *testing.T) {
	for _, class := range []WorkerResolveClass{WorkerResolveClean, WorkerResolveUncertain, WorkerResolveInterrupted, WorkerResolveMismatch} {
		t.Run(fmt.Sprint(class), func(t *testing.T) {
			f := newWorkerFixture(t)
			g := f.admit(t, barrierSubmit(t, f, "no-absence", backend.WorkerTCP))
			tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
			service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
			g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: service}}, "pod response lost")
			var err error
			g, err = f.s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{Service: service}, WorkerResolveOutcome{Class: class}, workerNowUTC())
			if err != nil {
				t.Fatal(err)
			}
			if g.Members[0].Barrier.CreateRefused || g.Members[0].Pod.UID != "" {
				t.Fatal("resolve invented absence or adopted missing Pod")
			}
			before := workerDatabaseBytes(t, f)
			if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRemove, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("service removal crossed unresolved original Pod", err)
			}
			if g.Phase == WorkerGroupDone || !g.Reserved {
				t.Fatal("clean/error/notfound class refunded missing original Pod")
			}
			f.durable(t, g.Job.ID)
		})
	}
}

func TestWorkerBarrierResolveRefusedCannotAddIdentity(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "rejected-lookup", backend.WorkerIndependent))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{}, "original response lost")
	before := workerDatabaseBytes(t, f)
	if _, err := f.s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}, WorkerResolveOutcome{Class: WorkerResolveMismatch, Refused: true}, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("rejected lookup added identity", err)
	}
	var err error
	g, err = f.s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{}, WorkerResolveOutcome{Class: WorkerResolveMismatch, Refused: true, Unsettled: true}, workerNowUTC())
	if err != nil || !g.Members[0].Barrier.LocalUnsettled || !g.Members[0].Barrier.Resolve.Unsettled || g.Members[0].Barrier.CreateRefused || !g.Reserved {
		t.Fatal("rejected lookup changed resource obligation or lost owner", err)
	}
	before = workerDatabaseBytes(t, f)
	if _, err = f.s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{}, WorkerResolveOutcome{Class: WorkerResolveClean}, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("resolve replay cleared original marker", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierDifferentLateOriginalDoesNotClearOwner(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "late-owners", backend.WorkerIndependent))
	first, g := barrierIntent(t, f, g, 0, WorkerCreate)
	second, _ := barrierIntent(t, f, g, 1, WorkerCreate)
	g = barrierRecord(t, f, first, WorkerBarrierReceipt{Unsettled: true, Created: backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}}, "original local owner unsettled")
	g = barrierRecord(t, f, second, WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: workerPod(g.Job.ID, 1)}}, "")
	if !g.Members[0].Create.Unsettled || !g.Members[0].Barrier.LocalUnsettled || g.Members[1].Create.Unsettled || g.Members[1].Pod.UID == "" || !g.Reserved {
		t.Fatal("late original lost marker or positive identity")
	}
	before := workerDatabaseBytes(t, f)
	same, err := f.s.RecordWorkerBarrierEffect(second, WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: workerPod(g.Job.ID, 1)}}, "", workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, same) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("different original marker false not independently replayable", err)
	}
	g, err = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierAbort, workerNowUTC())
	if err != nil || !g.Reserved {
		t.Fatal("abort lost latched original", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierSameTokenStartRedelivery(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "same-start", backend.WorkerIndependent))
	g = barrierCreated(t, f, g)
	g = barrierReleased(t, f, g)
	for rank := range g.Group.Spec.Count {
		g = barrierPrepared(t, f, g, rank)
	}
	var err error
	g, err = f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierStart, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	delivery := g.Members[0].Barrier.StartToken
	first, _ := barrierIntent(t, f, g, 0, WorkerStart)
	g = barrierRecord(t, f, first, WorkerBarrierReceipt{}, "locally settled unknown delivery")
	second, g := barrierIntent(t, f, g, 0, WorkerStart)
	if first.Seq == second.Seq || delivery != g.Members[0].Barrier.StartToken || g.Members[0].Barrier.LocalUnsettled {
		t.Fatal("retry changed immutable token or skipped new durable intent")
	}
	g = barrierRecord(t, f, second, WorkerBarrierReceipt{Started: backend.WorkerStarted{Pod: g.Members[0].Pod, Acknowledged: true, Revision: "3"}}, "")
	if !g.Members[0].Barrier.StartAck || g.Members[0].Barrier.StartToken != delivery {
		t.Fatal("same-token delivery not acknowledged")
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierLegacyReplayUnchanged(t *testing.T) {
	f := newWorkerFixture(t)
	r := f.request("alice", "legacy-replay", 2)
	legacy := f.submit(t, "alice", r)
	before := workerDatabaseBytes(t, f)
	// Existing legacy tasks need not meet NEW barrier resource requirements.
	// Placement/delegation inputs are irrelevant to an existing canonical key.
	r.Target = ""
	r.Binding = ""
	r.Authority = auth.Delegation{}
	r.Demand.CPUMillis = 0
	replay, err := f.s.SubmitWorkerBarrierGroup(f.actors["alice"], r)
	if err != nil || !reflect.DeepEqual(legacy, replay) || replay.Barrier != nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("legacy replay repriced/promoted/required new placement", err)
	}
}

// A completed preparation observation cannot authorize Start after cleanup has
// begun. The current revision is intentional: this is not stale-CAS protection.
func TestWorkerBarrierDecisionAfterCleanupRefused(t *testing.T) {
	for _, kind := range []string{"cancel intended", "cancel recorded", "remove intended", "remove recorded", "recover intended"} {
		t.Run(kind, func(t *testing.T) {
			f := newWorkerFixture(t)
			g := f.admit(t, barrierSubmit(t, f, "decision-cleanup", backend.WorkerIndependent))
			g = barrierCreated(t, f, g)
			g = barrierReleased(t, f, g)
			for rank := range g.Group.Spec.Count {
				g = barrierPrepared(t, f, g, rank)
			}
			var tok WorkerToken
			if strings.HasPrefix(kind, "cancel") {
				tok, g = barrierIntent(t, f, g, 1, WorkerCancel)
				if kind == "cancel recorded" {
					g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Canceled: backend.WorkerCanceled{Pod: g.Members[1].Pod, Acknowledged: true}}, "")
				}
			} else {
				var err error
				g, err = f.s.RecordWorkerBarrier(g.Job.ID, g.Revision, 1, backend.WorkerBarrier{Pod: g.Members[1].Pod, Revision: "3", Node: "node-a", Deleting: true, Stop: backend.WorkerTerminated}, workerNowUTC())
				if err != nil {
					t.Fatal(err)
				}
				tok, g = barrierIntent(t, f, g, 1, WorkerRemove)
				if kind == "remove recorded" {
					g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[1].Pod, PodRemoved: true}}, "")
				}
				if kind == "recover intended" {
					g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[1].Pod, PodDispatched: true}}, "retirement response lost")
					_, g = barrierIntent(t, f, g, 1, WorkerRecover)
				}
			}
			before := workerDatabaseBytes(t, f)
			if _, err := f.s.DecideWorkerBarrier(g.Job.ID, g.Revision, WorkerBarrierStart, workerNowUTC()); err == nil {
				t.Fatal("Start decision crossed cleanup original/history")
			}
			if !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("cleanup decision refusal changed database bytes")
			}
			current := f.group(t, g.Job.ID)
			if current.Barrier.Decision != WorkerBarrierUndecided {
				t.Fatal("cleanup allocated a durable decision")
			}
			for _, m := range current.Members {
				if m.Barrier.StartToken != ([32]byte{}) {
					t.Fatal("cleanup allocated a member start token")
				}
			}
		})
	}
}

// barrierNeverBound records MODELED native evidence from the exact original
// Pod: deletion has begun while it has no assigned node. It does not infer
// stop merely from the absence of a Release intent.
func barrierNeverBound(t *testing.T, f *workerFixture, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	next, err := f.s.RecordWorkerBarrier(g.Job.ID, g.Revision, rank, backend.WorkerBarrier{Pod: g.Members[rank].Pod, Revision: "stopped", Gated: true, Deleting: true, Stop: backend.WorkerNeverBound}, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestWorkerBarrierRemovalRequiresRecordedStop(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "recorded-stop", backend.WorkerIndependent))
	g = barrierCreated(t, f, g)
	if g.Members[0].Release.State != WorkerNone {
		t.Fatal("fixture released unexpectedly")
	}
	before := workerDatabaseBytes(t, f)
	if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRemove, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("no Release was treated as native stop", err)
	}
	var m workerMemberRecord
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		w, err := loadWorkerState(tx, g.Job.ID)
		if err == nil {
			m = w.m[0]
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// MODELED positive UID retirement alone still does not prove stopped.
	m.Counter++
	m.Remove = workerEffectRecord{State: WorkerRecorded, Seq: m.Counter, Gen: workerGenString(workerGen()), At: workerNowUTC()}
	m.Removed = true
	m.Barrier.RemoveHadPod = true
	if workerBarrierMemberSettled(m) {
		t.Fatal("retirement without native stop released a member")
	}
	g = barrierNeverBound(t, f, g, 0)
	tok, _ := barrierIntent(t, f, g, 0, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Pod: g.Members[0].Pod, PodRemoved: true}}, "")
	if !g.Members[0].Removed || g.Members[0].Barrier.Stop != backend.WorkerNeverBound {
		t.Fatal("positive native stop plus UID retirement lost")
	}
	f.durable(t, g.Job.ID)
}

// noPodRecoveryFixture uses real Store transactions and MODELED original
// refusal/dispatch receipts. No absence read or native backend runs here.
func noPodRecoveryFixture(t *testing.T, sent bool) (*workerFixture, WorkerGroup) {
	t.Helper()
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "no-pod-recovery", backend.WorkerTCP))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: backend.WorkerResource{Name: "rendezvous", UID: "original-service"}}, Refused: true}, "original Pod Create refused")
	tok, _ = barrierIntent(t, f, g, 0, WorkerRemove)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: g.Members[0].Barrier.Service, ServiceDispatched: sent}}, "service response lost")
	return f, g
}

func TestWorkerBarrierNoPodServiceRecovery(t *testing.T) {
	f, g := noPodRecoveryFixture(t, true)
	service := g.Members[0].Barrier.Service
	tok, g := barrierIntent(t, f, g, 0, WorkerRecover)
	if g.Members[0].Barrier.Stop != backend.WorkerStopUnproven || g.Members[0].Barrier.RecoverHadPod {
		t.Fatal("ephemeral no-Pod recovery fabricated a native stop or Pod")
	}
	receipt := WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: service, ServiceRemoved: true}}
	g = barrierRecord(t, f, tok, receipt, "")
	if g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Failed || !g.Members[0].Barrier.ServiceRemoved {
		t.Fatal("qualified no-Pod Service recovery did not settle failed group")
	}
	before := workerDatabaseBytes(t, f)
	same, err := f.s.RecordWorkerBarrierEffect(tok, receipt, "", workerNowUTC())
	if err != nil || !reflect.DeepEqual(g, same) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("original retired-Service recovery readback changed", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierNoPodRecoveryRequiresActualSent(t *testing.T) {
	f, g := noPodRecoveryFixture(t, false)
	before := workerDatabaseBytes(t, f)
	if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRecover, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("no-Pod recovery inferred original dispatch", err)
	}
	if !g.Reserved || g.Phase == WorkerGroupDone {
		t.Fatal("missing dispatch refunded group")
	}
	// A different UID cannot turn this original's missing dispatch into proof.
	m := g.Members[0]
	tok := WorkerToken{Job: g.Job.ID, Rank: 0, Effect: WorkerRemove, Seq: m.Remove.Seq, Gen: m.Remove.Gen}
	wrong := m.Barrier.Service
	wrong.UID = "foreign-service"
	if _, err := f.s.RecordWorkerBarrierEffect(tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: wrong, ServiceDispatched: true}}, "service response lost", workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("wrong Service UID supplied dispatch authority", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierNoPodResolveRefusedIsNotCreateRefused(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "only-resolve-refused", backend.WorkerTCP))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: service}}, "original Pod result lost")
	var err error
	g, err = f.s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{Service: service}, WorkerResolveOutcome{Class: WorkerResolveClean, Refused: true}, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	before := workerDatabaseBytes(t, f)
	for _, effect := range []WorkerEffect{WorkerRemove, WorkerRecover} {
		if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, effect, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
			t.Fatal("Resolve.Refused became original missing-Pod refusal", effect, err)
		}
	}
	if g.Members[0].Barrier.CreateRefused || !g.Reserved {
		t.Fatal("lookup rejection changed original resource obligation")
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerBarrierNoPodRecoveryLiveLocalHold(t *testing.T) {
	t.Run("joined original refusal", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.admit(t, barrierSubmit(t, f, "refusal-with-owner", backend.WorkerTCP))
		tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
		g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: backend.WorkerResource{Name: "rendezvous", UID: "original-service"}}, Refused: true, Unsettled: true}, "original refusal and unsettled local owner")
		before := workerDatabaseBytes(t, f)
		for _, effect := range []WorkerEffect{WorkerRemove, WorkerRecover} {
			if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, effect, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("joined refusal crossed live owner", effect, err)
			}
		}
		f.durable(t, g.Job.ID)
	})
	t.Run("read owner after sent removal", func(t *testing.T) {
		f, g := noPodRecoveryFixture(t, true)
		ref := backend.WorkerReference{Group: g.Group, Rank: 0, Service: g.Members[0].Barrier.Service}
		var err error
		g, err = f.s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 0, ref, workerNowUTC())
		if err != nil {
			t.Fatal(err)
		}
		before := workerDatabaseBytes(t, f)
		if _, _, err := f.s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, WorkerRecover, workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
			t.Fatal("historical qualification bypassed current local owner", err)
		}
		f.durable(t, g.Job.ID)
	})
}

func TestWorkerBarrierNoPodRecoveryHistoricalQualification(t *testing.T) {
	for _, ownMarker := range []bool{false, true} {
		t.Run(fmt.Sprint(ownMarker), func(t *testing.T) {
			f, g := noPodRecoveryFixture(t, true)
			service := g.Members[0].Barrier.Service
			tok, g := barrierIntent(t, f, g, 0, WorkerRecover)
			if !ownMarker {
				// MODELED typed read failure AFTER the valid recovery intent. This later
				// live hold cannot invalidate the original's historical qualification.
				var err error
				g, err = f.s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 0, backend.WorkerReference{Group: g.Group, Rank: 0, Service: service}, workerNowUTC())
				if err != nil {
					t.Fatal(err)
				}
			}
			receipt := WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: service, ServiceRemoved: true}, Unsettled: ownMarker}
			cause := ""
			if ownMarker {
				cause = "original recovery has unsettled local owner"
			}
			g = barrierRecord(t, f, tok, receipt, cause)
			if !g.Members[0].Barrier.ServiceRemoved || !g.Members[0].Barrier.LocalUnsettled || g.Phase == WorkerGroupDone || !g.Reserved || g.Members[0].Barrier.Stop != backend.WorkerStopUnproven {
				t.Fatal("late positive recovery facts or local hold lost")
			}
			before := workerDatabaseBytes(t, f)
			same, err := f.s.RecordWorkerBarrierEffect(tok, receipt, cause, workerNowUTC())
			if err != nil || !reflect.DeepEqual(g, same) || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
				t.Fatal("later local/retired flags invalidated original recovery readback", err)
			}
			f.durable(t, g.Job.ID)
		})
	}
}

func TestWorkerBarrierNoPodRecoveryRejectsWrongServiceUID(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "wrong-Service-receipt", backend.WorkerTCP))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: service}, Refused: true}, "original Pod Create refused")
	tok, g = barrierIntent(t, f, g, 0, WorkerRemove)
	before := workerDatabaseBytes(t, f)
	foreign := service
	foreign.UID = "foreign-service"
	// This is the FIRST return for the intended Remove, not a differing replay.
	// Only the exact original UID can supply its dispatch/retirement evidence.
	if _, err := f.s.RecordWorkerBarrierEffect(tok, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: foreign, ServiceDispatched: true}}, "service response lost", workerNowUTC()); err == nil || !bytes.Equal(before, workerDatabaseBytes(t, f)) {
		t.Fatal("foreign first-return Service granted removal authority", err)
	}
	g = f.group(t, g.Job.ID)
	if g.Members[0].Barrier.Service != service || g.Members[0].Barrier.ServiceRemoveSent || !g.Reserved {
		t.Fatal("foreign Service changed original obligation")
	}
	f.durable(t, g.Job.ID)
}

// Versioned Held groups project their stop request; legacy Held behavior is
// separately fixed by the unchanged TestWorkerGroupStoppingSurvivesReopen.
func TestWorkerBarrierHeldStoppingSurvivesReopen(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "held-projection", backend.WorkerIndependent))
	tok, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	g = barrierRecord(t, f, tok, WorkerBarrierReceipt{}, "original Create outcome unknown")
	if g.Barrier == nil || g.Phase != WorkerGroupHeld || g.Stopping || !g.Reserved {
		t.Fatal("initial versioned held projection changed")
	}
	f.cancel(t, "alice", g.Job.ID)
	g = f.durable(t, g.Job.ID)
	if g.Barrier == nil || g.Phase != WorkerGroupHeld || !g.Stopping || !g.Reserved || !g.Job.CancelRequested {
		t.Fatal("versioned Held stop request lost across durability")
	}
}

// The native refusal, dispatch and retirement receipts are MODELED; all state
// transitions and replay checks use the real Store and on-disk database.
func TestWorkerBarrierPartialCreateReplayAfterDone(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.admit(t, barrierSubmit(t, f, "partial-create-replay", backend.WorkerTCP))
	create, _ := barrierIntent(t, f, g, 0, WorkerCreate)
	service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
	original := WorkerBarrierReceipt{Created: backend.WorkerCreated{Service: service}, Refused: true}
	const cause = "original Pod Create refused"
	g = barrierRecord(t, f, create, original, cause)
	if g.Phase != WorkerGroupHeld || !g.Reserved {
		t.Fatal("fresh partial Create did not hold the reservation")
	}
	remove, _ := barrierIntent(t, f, g, 0, WorkerRemove)
	g = barrierRecord(t, f, remove, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: service, ServiceDispatched: true}}, "service response lost")
	recover, _ := barrierIntent(t, f, g, 0, WorkerRecover)
	g = barrierRecord(t, f, recover, WorkerBarrierReceipt{Removed: backend.WorkerRemoval{Service: service, ServiceRemoved: true}}, "")
	if g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Failed {
		t.Fatal("qualified cleanup did not settle the group")
	}
	before := workerDatabaseBytes(t, f)
	rows := plannerStoreSnapshot(t, f.s)
	same, err := f.s.RecordWorkerBarrierEffect(create, original, cause, workerNowUTC())
	current, readErr := f.s.WorkerGroup(g.Job.ID)
	if err != nil || readErr != nil || !reflect.DeepEqual(g, same) || !reflect.DeepEqual(current, same) {
		t.Fatal("historical Create replay changed the current whole view", err, readErr)
	}
	if !bytes.Equal(before, workerDatabaseBytes(t, f)) || !reflect.DeepEqual(rows, plannerStoreSnapshot(t, f.s)) {
		t.Fatal("historical Create replay changed database bytes, rows or sequences")
	}
	for _, name := range []string{"receipt", "token", "cause"} {
		t.Run(name, func(t *testing.T) {
			tok, receipt, why := create, original, cause
			switch name {
			case "receipt":
				receipt.Refused = false
			case "token":
				tok.Seq++
			case "cause":
				why = "another failure"
			}
			if _, err := f.s.RecordWorkerBarrierEffect(tok, receipt, why, workerNowUTC()); err == nil {
				t.Fatal("different historical Create result was accepted")
			}
			if !bytes.Equal(before, workerDatabaseBytes(t, f)) || !reflect.DeepEqual(rows, plannerStoreSnapshot(t, f.s)) {
				t.Fatal("refused historical Create changed database bytes, rows or sequences")
			}
		})
	}
}
