package ec2vm

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

func TestNewRecord(t *testing.T) {
	r := newTestRecord(t)
	req, err := ec2api.ParseCreateRequest(r.Request)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case r.Identity != testIdentity():
		t.Errorf("identity %+v", r.Identity)
	case r.ProfileHash != testProfile().Hash():
		t.Errorf("profile hash %s", r.ProfileHash)
	case r.Token != testIdentity().ClientToken() || req.ClientToken() != r.Token:
		t.Errorf("token %s", r.Token)
	case r.Desired != DesiredPresent || r.Phase != PhaseCreating || r.Hold != HoldNone:
		t.Errorf("desired %v, phase %v, hold %v", r.Desired, r.Phase, r.Hold)
	case r.InstanceID != "" || r.CreateSent || !r.CreateSentAt.IsZero() || r.CreditsVerified:
		t.Errorf("new record has lifecycle facts: %+v", r)
	}
	if _, err := NewRecord(testProfile(), ec2api.Identity{Owner: "o", Environment: "e"}, nil); err == nil {
		t.Error("NewRecord accepted generation 0")
	}
}

func TestWithDesired(t *testing.T) {
	r := newTestRecord(t)
	r, err := r.WithDesired(DesiredStopped)
	if err != nil || r.Desired != DesiredStopped {
		t.Fatalf("WithDesired(stopped) = %v, %v", r.Desired, err)
	}
	if _, err := r.WithDesired(Desired(0)); err == nil {
		t.Fatal("WithDesired accepted an invalid state")
	}
	r, err = r.WithDesired(DesiredAbsent)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []Desired{DesiredPresent, DesiredStopped} {
		got, err := r.WithDesired(d)
		if err == nil || got.Desired != DesiredAbsent {
			t.Fatalf("WithDesired(%v) after absent = %v, %v; want absent and an error", d, got.Desired, err)
		}
	}
	if got, err := r.WithDesired(DesiredAbsent); err != nil || got.Desired != DesiredAbsent {
		t.Fatalf("WithDesired(absent) after absent = %v, %v", got.Desired, err)
	}
}

func TestFromFields(t *testing.T) {
	r := newTestRecord(t)
	r.CreateSent, r.CreateSentAt, r.InstanceID = true, testTime, testID
	r.Reported = []string{"i-0fedcba9876543210"}
	got, err := FromFields(testProfile(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Request, r.Request) || got.Token != r.Token || got.InstanceID != testID {
		t.Fatalf("FromFields changed the record: %+v", got)
	}
	r.Request[0] ^= 1
	r.Reported[0] = testID
	if bytes.Equal(got.Request, r.Request) || got.Reported[0] == testID {
		t.Fatal("FromFields shares the request or reported IDs with its argument")
	}
}

func TestFromFieldsRefuses(t *testing.T) {
	other := testProfile()
	other.InstanceType = "t3.small"
	otherRecord, err := NewRecord(other, testIdentity(), []byte("#cloud-config\n"))
	if err != nil {
		t.Fatal(err)
	}
	gen2 := testIdentity()
	gen2.Generation = 2
	gen2Record, err := NewRecord(testProfile(), gen2, []byte("#cloud-config\n"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		edit   func(r *Record)
		reason string
	}{
		{"request for another profile", func(r *Record) { *r = otherRecord }, "profile hash"},
		{"profile hash", func(r *Record) { r.ProfileHash = other.Hash() }, "profile hash"},
		{"token", func(r *Record) { r.Token = "skygo-" + strings.Repeat("0", 58) }, "token"},
		{"identity", func(r *Record) { r.Identity.Lease = "lease-1" }, "identity"},
		{"request of another generation", func(r *Record) { r.Request = gen2Record.Request }, "token"},
		{"request bytes", func(r *Record) { r.Request = append(bytes.Clone(r.Request), '&') }, "request"},
		{"request not canonical", func(r *Record) { r.Request = bytes.Replace(r.Request, []byte("MinCount=1"), []byte("MinCount=01"), 1) }, "request"},
		{"empty request", func(r *Record) { r.Request = nil }, "request"},
		{"phase", func(r *Record) { r.Phase = 0 }, "phase"},
		{"desired", func(r *Record) { r.Desired = DesiredAbsent + 1 }, "desired"},
		{"hold without held", func(r *Record) { r.Hold = HoldCredits }, "hold"},
		{"held without hold", func(r *Record) { r.Phase = PhaseHeld }, "hold"},
		{"sent without time", func(r *Record) { r.CreateSent = true }, "sent"},
		{"time without sent", func(r *Record) { r.CreateSentAt = testTime }, "sent"},
		{"instance without create", func(r *Record) { r.InstanceID = testID }, "record has an instance ID but sent no create"},
		{"instance ID", func(r *Record) { r.CreateSent, r.CreateSentAt, r.InstanceID = true, testTime, "i-123" }, "instance ID"},
		{"running without instance", func(r *Record) { r.CreateSent, r.CreateSentAt, r.Phase = true, testTime, PhaseRunning }, "instance ID"},
		{"running without credits", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase = true, testTime, testID, PhaseRunning
		}, "record is running without verified credits"},
		{"stopped with credits", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase, r.CreditsVerified = true, testTime, testID, PhaseStopped, true
		}, "record phase stopped has verified credits"},
		{"unknown with credits", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase, r.CreditsVerified = true, testTime, testID, PhaseUnknown, true
		}, "record phase unknown has verified credits"},
		{"terminated with credits", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase, r.CreditsVerified = true, testTime, testID, PhaseTerminated, true
		}, "record phase terminated has verified credits"},
		{"credits without instance", func(r *Record) { r.CreateSent, r.CreateSentAt, r.CreditsVerified = true, testTime, true }, "credits"},
		{"not observed with instance", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase, r.Hold = true, testTime, testID, PhaseHeld, HoldNotObserved
		}, "unobserved"},
		{"not observed without create", func(r *Record) { r.Phase, r.Hold = PhaseHeld, HoldNotObserved }, "unobserved"},
		{"unverified with instance", func(r *Record) {
			r.CreateSent, r.CreateSentAt, r.InstanceID, r.Phase, r.Hold = true, testTime, testID, PhaseHeld, HoldUnverified
		}, "unobserved"},
		{"reported without create", func(r *Record) { r.Reported = []string{testID} }, "reported"},
		{"reported ID", func(r *Record) { r.CreateSent, r.CreateSentAt, r.Reported = true, testTime, []string{"i-1"} }, "reported"},
		{"hold", func(r *Record) { r.Phase, r.Hold = PhaseHeld, HoldUnverified+1 }, "hold"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRecord(t)
			tt.edit(&r)
			_, err := FromFields(testProfile(), r)
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("FromFields error %v, want one about %s", err, tt.reason)
			}
			if tt.name != "request for another profile" { // consistent, but for another profile
				f := newFake(t, newTestRecord(t))
				if _, err := Step(t.Context(), f, r, f.options()); err == nil || len(f.calls) != 0 {
					t.Fatalf("Step error %v, calls %v; want an error and no calls", err, f.calls)
				}
			}
		})
	}
}
