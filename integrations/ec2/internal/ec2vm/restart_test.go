package ec2vm

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

// restart returns the record a new process reads back after a crash: the
// last record Commit stored, copied field by field and checked.
func restart(t *testing.T, f *fake) Record {
	t.Helper()
	if f.committed == nil {
		t.Fatal("nothing was committed")
	}
	c := *f.committed
	stored := Record{
		Identity:        c.Identity,
		ProfileHash:     c.ProfileHash,
		Request:         bytes.Clone(c.Request),
		Token:           c.Token,
		Desired:         c.Desired,
		Phase:           c.Phase,
		Hold:            c.Hold,
		InstanceID:      c.InstanceID,
		Reported:        slices.Clone(c.Reported),
		CreateSent:      c.CreateSent,
		CreateSentAt:    c.CreateSentAt,
		CreditsVerified: c.CreditsVerified,
		LastDiagnostic:  c.LastDiagnostic,
	}
	r, err := FromFields(testProfile(), stored)
	if err != nil {
		t.Fatalf("FromFields after restart: %v", err)
	}
	return r
}

// TestLostReplyReissue loses the reply to the first RunInstances and
// restarts before storing the result. Within the reissue window the new
// process reissues the identical request, and EC2 returns the one
// instance.
func TestLostReplyReissue(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.loseReply = 1
	if _, err := step(t, f, r, f.options()); err == nil {
		t.Fatal("lost reply reported no error")
	}
	f.settle()

	r = restart(t, f)
	if !r.CreateSent || r.InstanceID != "" {
		t.Fatalf("restarted record: sent %v, instance %q", r.CreateSent, r.InstanceID)
	}
	opt := f.options()
	opt.ReissueWindow = time.Hour
	opt.Now = func() time.Time { return testTime.Add(time.Minute) }
	if a := Next(r, opt); a != ActionReissue {
		t.Fatalf("Next = %v, want reissue", a)
	}
	r, err := step(t, f, r, opt)
	if err != nil {
		t.Fatal(err)
	}
	if r.InstanceID != testID || r.Phase != PhaseCreating {
		t.Fatalf("after reissue: instance %q, phase %v", r.InstanceID, r.Phase)
	}
	if !r.CreateSentAt.Equal(testTime) {
		t.Fatalf("reissue moved CreateSentAt to %v, want the first send %v", r.CreateSentAt, testTime)
	}
	if f.count("RunInstances") != 2 || f.created != 1 {
		t.Fatalf("%d creates sent, %d instances; want 2, 1", f.count("RunInstances"), f.created)
	}
}

// TestLostReplyHolds loses the reply with the default reissue window. The
// new process searches by ClientToken instead of reissuing; the search is
// eventually consistent and observes nothing, so the record is held, not
// abandoned or recreated.
func TestLostReplyHolds(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.loseReply = 1
	f.hidden = 1
	if _, err := step(t, f, r, f.options()); err == nil {
		t.Fatal("lost reply reported no error")
	}

	r = restart(t, f)
	r, err := step(t, f, r, f.options())
	if !errors.Is(err, ec2api.ErrNotObserved) {
		t.Fatalf("recover error %v, want ErrNotObserved", err)
	}
	want(t, r, PhaseHeld, HoldNotObserved)
	if !r.CreateSent || r.Token != testIdentity().ClientToken() {
		t.Fatal("held record lost its create")
	}
	for range 3 {
		r = run(t, f, r, ActionNone)
	}
	if f.count("RunInstances") != 1 || f.created != 1 {
		t.Fatalf("%d creates sent, %d instances; want 1, 1", f.count("RunInstances"), f.created)
	}
}

// TestCancelWhileAmbiguous cancels a record whose create reply was lost.
// The record is recovered by ClientToken and its instance terminated; no
// second create is sent.
func TestCancelWhileAmbiguous(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.loseReply = 1
	f.hidden = 1
	if _, err := step(t, f, r, f.options()); err == nil {
		t.Fatal("lost reply reported no error")
	}

	r = desire(t, restart(t, f), DesiredAbsent)
	if _, err := r.WithDesired(DesiredPresent); err == nil {
		t.Fatal("cancellation was not latched")
	}
	r, err := step(t, f, r, f.options())
	if !errors.Is(err, ec2api.ErrNotObserved) {
		t.Fatalf("recover error %v, want ErrNotObserved", err)
	}
	want(t, r, PhaseHeld, HoldNotObserved)
	r = run(t, f, r, ActionRecover)
	if r.InstanceID != testID {
		t.Fatalf("recovered instance %q", r.InstanceID)
	}
	r = run(t, f, r, ActionTerminate)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseTerminated, HoldNone)
	if f.count("RunInstances") != 1 || f.created != 1 {
		t.Fatalf("%d creates sent, %d instances; want 1, 1", f.count("RunInstances"), f.created)
	}
}

// TestCancelBeforeCreate cancels a record before anything was sent:
// there is nothing to recover and Step makes no call.
func TestCancelBeforeCreate(t *testing.T) {
	r := desire(t, newTestRecord(t), DesiredAbsent)
	f := newFake(t, r)
	r = run(t, f, r, ActionNone)
	if len(f.calls) != 0 || f.committed != nil {
		t.Fatalf("calls %v, committed %v", f.calls, f.committed != nil)
	}
}

// TestReissueWindowFromFirstSend loses two create replies. The window is
// measured from the first send, which a reissue does not move: once it has
// passed, Step searches instead of reissuing.
func TestReissueWindowFromFirstSend(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.loseReply = 2
	now := testTime
	opt := f.options()
	opt.ReissueWindow = time.Hour
	opt.Now = func() time.Time { return now }

	r, _ = step(t, f, r, opt)
	now = testTime.Add(30 * time.Minute)
	if a := Next(r, opt); a != ActionReissue {
		t.Fatalf("Next at +30m = %v, want reissue", a)
	}
	r, _ = step(t, f, r, opt)
	if r.InstanceID != "" || !r.CreateSentAt.Equal(testTime) {
		t.Fatalf("after second lost reply: instance %q, first send %v", r.InstanceID, r.CreateSentAt)
	}
	now = testTime.Add(80 * time.Minute)
	if a := Next(r, opt); a != ActionRecover {
		t.Fatalf("Next at +80m = %v, want recover", a)
	}
	r, err := step(t, f, r, opt)
	if err != nil || r.InstanceID != testID {
		t.Fatalf("recover: %v, instance %q", err, r.InstanceID)
	}
	if f.count("RunInstances") != 2 || f.created != 1 {
		t.Fatalf("%d creates sent, %d instances; want 2, 1", f.count("RunInstances"), f.created)
	}
}
