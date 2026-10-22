package ec2vm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

const (
	testAccount = "123456789012"
	testID      = "i-0123456789abcdef0"
)

var testTime = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func testNow() time.Time { return testTime }

func testProfile() *ec2api.Profile {
	return &ec2api.Profile{
		Account:          testAccount,
		Region:           "us-west-2",
		SubnetID:         "subnet-0123456789abcdef0",
		SecurityGroupIDs: []string{"sg-0bbbbbbbbbbbbbbbb", "sg-0aaaaaaaaaaaaaaaa"},
		ImageID:          "ami-0123456789abcdef0",
		InstanceType:     "t3.micro",
		CPUCredits:       "standard",
		RootDevice:       "/dev/xvda",
		RootVolumeGiB:    16,
		RootVolumeType:   "gp3",
		EncryptRoot:      true,
	}
}

func testIdentity() ec2api.Identity {
	return ec2api.Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1}
}

func newTestRecord(t *testing.T) Record {
	t.Helper()
	r, err := NewRecord(testProfile(), testIdentity(), []byte("#cloud-config\n"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A fake is a scripted provider for one instance. It answers with real
// ec2api types and errors, and checks that every create carries the
// recorded request after a record marked CreateSent was committed.
type fake struct {
	t           *testing.T
	body        []byte           // the only RunInstances body it accepts
	inst        *ec2api.Instance // the instance, once created
	created     int              // instances created
	credits     string           // the instance's CPU credit option
	hidden      int              // searches that observe nothing although the instance exists
	loseReply   int              // creates whose reply is lost after the effect
	loseState   int              // state changes whose reply is lost after the effect
	reportState string           // if set, the state the next mutation reports and leaves
	errs        map[string][]error
	calls       []string

	// Errors returned together with a result, after the effect, as
	// *ec2api.Client does when a reply fails a check or does not close.
	runErr    error             // with the reservation
	extra     []ec2api.Instance // added to that reservation
	findErr   error             // with the found instance
	mutateErr error             // with the state change

	committed *Record // last record Commit stored
}

func newFake(t *testing.T, r Record) *fake {
	return &fake{t: t, body: bytes.Clone(r.Request), credits: "standard", errs: map[string][]error{}}
}

func (f *fake) commit(_ context.Context, r Record) error {
	f.committed = &r
	return nil
}

func (f *fake) options() Options {
	return Options{Commit: f.commit, Now: testNow}
}

// fail makes the next call of method fail with err before any effect.
func (f *fake) fail(method string, err error) {
	f.errs[method] = append(f.errs[method], err)
}

func (f *fake) begin(method string) error {
	f.calls = append(f.calls, method)
	q := f.errs[method]
	if len(q) == 0 {
		return nil
	}
	f.errs[method] = q[1:]
	return q[0]
}

func (f *fake) count(method string) int {
	n := 0
	for _, c := range f.calls {
		if c == method {
			n++
		}
	}
	return n
}

// settle completes the instance's transition.
func (f *fake) settle() {
	switch f.inst.State {
	case "pending":
		f.inst.State = "running"
	case "stopping":
		f.inst.State = "stopped"
	case "shutting-down":
		f.inst.State = "terminated"
	}
}

func notFoundErr(action string) error {
	return &ec2api.Error{Action: action, Class: ec2api.ClassInstanceNotFound, Code: "InvalidInstanceID.NotFound", StatusCode: 400, Sent: true}
}

func (f *fake) find(action, id string) error {
	if f.inst == nil || f.inst.ID != id {
		return notFoundErr(action)
	}
	return nil
}

func (f *fake) checkCredits(id string) error {
	if want := testProfile().CPUCredits; f.credits != want {
		return &ec2api.CreditMismatchError{InstanceID: id, Want: want, Got: f.credits}
	}
	return nil
}

func (f *fake) RunInstances(_ context.Context, req *ec2api.CreateRequest) (*ec2api.Reservation, error) {
	if err := f.begin("RunInstances"); err != nil {
		return nil, err
	}
	if f.committed == nil || !f.committed.CreateSent {
		f.t.Error("RunInstances sent before a record marked CreateSent was committed")
	}
	if !bytes.Equal(req.Body(), f.body) || req.ClientToken() != testIdentity().ClientToken() {
		f.t.Error("RunInstances request differs from the recorded request")
	}
	if f.inst == nil {
		f.inst = &ec2api.Instance{ID: testID, State: "pending", ClientToken: req.ClientToken()}
		f.created++
	}
	if f.loseReply > 0 {
		f.loseReply--
		return nil, &ec2api.Error{Action: "RunInstances", Class: ec2api.ClassTransport, Sent: true}
	}
	res := &ec2api.Reservation{ID: "r-0123456789abcdef0", OwnerID: testAccount, Instances: append([]ec2api.Instance{*f.inst}, f.extra...)}
	err := f.runErr
	f.runErr = nil
	return res, err
}

func (f *fake) FindOwned(_ context.Context, _ *ec2api.CreateRequest) (*ec2api.Instance, error) {
	if err := f.begin("FindOwned"); err != nil {
		return nil, err
	}
	if f.inst == nil || f.hidden > 0 {
		if f.hidden > 0 {
			f.hidden--
		}
		return nil, fmt.Errorf("fake: %w", ec2api.ErrNotObserved)
	}
	inst := *f.inst
	err := f.findErr
	f.findErr = nil
	return &inst, err
}

func (f *fake) DescribeOwned(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.Instance, error) {
	if err := f.begin("DescribeOwned"); err != nil {
		return nil, err
	}
	if err := f.find("DescribeInstances", id); err != nil {
		return nil, err
	}
	inst := *f.inst
	return &inst, nil
}

func (f *fake) CheckCPUCredits(_ context.Context, _ *ec2api.CreateRequest, id string) (string, error) {
	if err := f.begin("CheckCPUCredits"); err != nil {
		return "", err
	}
	if err := f.find("DescribeInstances", id); err != nil {
		return "", err
	}
	return f.credits, f.checkCredits(id)
}

func (f *fake) Stop(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return f.mutate("Stop", "StopInstances", id, "stopping", nil)
}

func (f *fake) Start(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return f.mutate("Start", "StartInstances", id, "pending", f.checkCredits)
}

func (f *fake) Terminate(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return f.mutate("Terminate", "TerminateInstances", id, "shutting-down", nil)
}

func (f *fake) mutate(method, action, id, state string, check func(string) error) (*ec2api.StateChange, error) {
	if err := f.begin(method); err != nil {
		return nil, err
	}
	// The ownership description the client sends before the mutation.
	if err := f.find("DescribeInstances", id); err != nil {
		return nil, err
	}
	if check != nil {
		if err := check(id); err != nil {
			return nil, err
		}
	}
	if f.reportState != "" {
		state, f.reportState = f.reportState, ""
	}
	prev := f.inst.State
	f.inst.State = state
	if f.loseState > 0 {
		f.loseState--
		return nil, &ec2api.Error{Action: action, Class: ec2api.ClassTransport, Sent: true}
	}
	err := f.mutateErr
	f.mutateErr = nil
	return &ec2api.StateChange{InstanceID: id, Previous: prev, Current: state}, err
}

// step runs one Step and checks that it made at most one call and
// returned a consistent record.
func step(t *testing.T, f *fake, r Record, opt Options) (Record, error) {
	t.Helper()
	n := len(f.calls)
	r, err := Step(context.Background(), f, r, opt)
	if len(f.calls)-n > 1 {
		t.Fatalf("Step made %d calls: %v", len(f.calls)-n, f.calls[n:])
	}
	if cerr := r.check(); cerr != nil {
		t.Fatalf("Step returned an inconsistent record: %v", cerr)
	}
	return r, err
}

// run checks that Next(r) is a, runs one Step, and requires it to succeed.
func run(t *testing.T, f *fake, r Record, a Action) Record {
	t.Helper()
	opt := f.options()
	if got := Next(r, opt); got != a {
		t.Fatalf("Next = %v, want %v (phase %v, hold %v, desired %v)", got, a, r.Phase, r.Hold, r.Desired)
	}
	r, err := step(t, f, r, opt)
	if err != nil {
		t.Fatalf("%v: %v", a, err)
	}
	return r
}

func want(t *testing.T, r Record, phase Phase, hold Hold) {
	t.Helper()
	if r.Phase != phase || r.Hold != hold {
		t.Fatalf("phase %v, hold %v; want %v, %v", r.Phase, r.Hold, phase, hold)
	}
}

func desire(t *testing.T, r Record, d Desired) Record {
	t.Helper()
	r, err := r.WithDesired(d)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// running returns the record of a created instance observed running.
func running(t *testing.T, f *fake, r Record) Record {
	t.Helper()
	r = run(t, f, r, ActionCreate)
	r = run(t, f, r, ActionVerifyCredits)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseRunning, HoldNone)
	return r
}

func TestLifecycle(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)

	r = run(t, f, r, ActionCreate)
	if r.InstanceID != testID || !r.CreateSent || !r.CreateSentAt.Equal(testTime) {
		t.Fatalf("after create: instance %q, sent %v at %v", r.InstanceID, r.CreateSent, r.CreateSentAt)
	}
	want(t, r, PhaseCreating, HoldNone)
	r = run(t, f, r, ActionVerifyCredits)
	r = run(t, f, r, ActionObserve) // still pending
	want(t, r, PhaseCreating, HoldNone)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseRunning, HoldNone)
	if a := Next(r, f.options()); a != ActionNone {
		t.Fatalf("Next(running) = %v, want none", a)
	}

	r = desire(t, r, DesiredStopped)
	r = run(t, f, r, ActionStop)
	want(t, r, PhaseStopping, HoldNone)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseStopped, HoldNone)

	r = desire(t, r, DesiredPresent)
	r = run(t, f, r, ActionStart)
	want(t, r, PhaseStarting, HoldNone)
	r = run(t, f, r, ActionVerifyCredits)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseRunning, HoldNone)

	r = desire(t, r, DesiredAbsent)
	r = run(t, f, r, ActionTerminate)
	want(t, r, PhaseTerminating, HoldNone)
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseTerminating, HoldNone)
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseTerminated, HoldNone)
	if a := Next(r, f.options()); a != ActionNone {
		t.Fatalf("Next(terminated) = %v, want none", a)
	}

	if r.InstanceID != testID || f.created != 1 {
		t.Fatalf("instance %q, %d created; want %s, 1", r.InstanceID, f.created, testID)
	}
	wantCalls := []string{
		"RunInstances", "CheckCPUCredits", "DescribeOwned", "DescribeOwned",
		"Stop", "DescribeOwned",
		"Start", "CheckCPUCredits", "DescribeOwned",
		"Terminate", "DescribeOwned", "DescribeOwned",
	}
	if !slices.Equal(f.calls, wantCalls) {
		t.Fatalf("calls %v\nwant  %v", f.calls, wantCalls)
	}
}

func TestNext(t *testing.T) {
	base := newTestRecord(t)
	tests := []struct {
		name              string
		phase             Phase
		hold              Hold
		desired           Desired
		sent, id, credits bool
		window, after     time.Duration // after: time since the first create
		want              Action
	}{
		{"new", PhaseCreating, HoldNone, DesiredPresent, false, false, false, 0, 0, ActionCreate},
		{"new absent", PhaseCreating, HoldNone, DesiredAbsent, false, false, false, 0, 0, ActionNone},
		{"ambiguous default window", PhaseCreating, HoldNone, DesiredPresent, true, false, false, 0, 0, ActionRecover},
		{"ambiguous within window", PhaseCreating, HoldNone, DesiredPresent, true, false, false, time.Hour, 10 * time.Minute, ActionReissue},
		{"ambiguous at window end", PhaseCreating, HoldNone, DesiredPresent, true, false, false, time.Hour, time.Hour, ActionRecover},
		{"ambiguous after window", PhaseCreating, HoldNone, DesiredPresent, true, false, false, time.Hour, 2 * time.Hour, ActionRecover},
		{"ambiguous clock behind", PhaseCreating, HoldNone, DesiredPresent, true, false, false, time.Hour, -time.Minute, ActionRecover},
		{"ambiguous absent within window", PhaseCreating, HoldNone, DesiredAbsent, true, false, false, time.Hour, time.Minute, ActionRecover},
		{"not observed present", PhaseHeld, HoldNotObserved, DesiredPresent, true, false, false, 0, 0, ActionNone},
		{"not observed absent", PhaseHeld, HoldNotObserved, DesiredAbsent, true, false, false, 0, 0, ActionRecover},
		{"unverified present", PhaseHeld, HoldUnverified, DesiredPresent, true, false, false, 0, 0, ActionNone},
		{"unverified stopped", PhaseHeld, HoldUnverified, DesiredStopped, true, false, false, 0, 0, ActionNone},
		{"unverified absent", PhaseHeld, HoldUnverified, DesiredAbsent, true, false, false, 0, 0, ActionRecover},
		{"conflict absent", PhaseHeld, HoldConflict, DesiredAbsent, true, true, false, 0, 0, ActionNone},
		{"conflict stopped", PhaseHeld, HoldConflict, DesiredStopped, true, true, false, 0, 0, ActionNone},
		{"conflict without instance absent", PhaseHeld, HoldConflict, DesiredAbsent, true, false, false, 0, 0, ActionNone},
		{"credits held present", PhaseHeld, HoldCredits, DesiredPresent, true, true, false, 0, 0, ActionNone},
		{"credits held stopped", PhaseHeld, HoldCredits, DesiredStopped, true, true, false, 0, 0, ActionStop},
		{"credits held absent", PhaseHeld, HoldCredits, DesiredAbsent, true, true, false, 0, 0, ActionTerminate},
		{"unknown", PhaseUnknown, HoldNone, DesiredAbsent, true, true, false, 0, 0, ActionObserve},
		{"terminated", PhaseTerminated, HoldNone, DesiredAbsent, true, true, false, 0, 0, ActionNone},
		{"created unverified", PhaseCreating, HoldNone, DesiredPresent, true, true, false, 0, 0, ActionVerifyCredits},
		{"created verified", PhaseCreating, HoldNone, DesiredPresent, true, true, true, 0, 0, ActionObserve},
		{"created absent", PhaseCreating, HoldNone, DesiredAbsent, true, true, false, 0, 0, ActionTerminate},
		{"created stopped", PhaseCreating, HoldNone, DesiredStopped, true, true, false, 0, 0, ActionStop},
		{"running", PhaseRunning, HoldNone, DesiredPresent, true, true, true, 0, 0, ActionNone},
		{"running stopped", PhaseRunning, HoldNone, DesiredStopped, true, true, true, 0, 0, ActionStop},
		{"running absent", PhaseRunning, HoldNone, DesiredAbsent, true, true, true, 0, 0, ActionTerminate},
		{"stopping stopped", PhaseStopping, HoldNone, DesiredStopped, true, true, false, 0, 0, ActionObserve},
		{"stopped stopped", PhaseStopped, HoldNone, DesiredStopped, true, true, false, 0, 0, ActionNone},
		{"stopped present", PhaseStopped, HoldNone, DesiredPresent, true, true, false, 0, 0, ActionStart},
		{"stopped absent", PhaseStopped, HoldNone, DesiredAbsent, true, true, false, 0, 0, ActionTerminate},
		{"starting unverified", PhaseStarting, HoldNone, DesiredPresent, true, true, false, 0, 0, ActionVerifyCredits},
		{"terminating absent", PhaseTerminating, HoldNone, DesiredAbsent, true, true, false, 0, 0, ActionObserve},
		{"terminating present", PhaseTerminating, HoldNone, DesiredPresent, true, true, false, 0, 0, ActionObserve},
		{"terminating stopped", PhaseTerminating, HoldNone, DesiredStopped, true, true, false, 0, 0, ActionObserve},
		{"stopping present", PhaseStopping, HoldNone, DesiredPresent, true, true, false, 0, 0, ActionObserve},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := base
			r.Phase, r.Hold, r.Desired, r.CreditsVerified = tt.phase, tt.hold, tt.desired, tt.credits
			if tt.sent {
				r.CreateSent, r.CreateSentAt = true, testTime
			}
			if tt.id {
				r.InstanceID = testID
			}
			if err := r.check(); err != nil {
				t.Fatalf("test record is inconsistent: %v", err)
			}
			opt := Options{ReissueWindow: tt.window, Now: func() time.Time { return testTime.Add(tt.after) }}
			if got := Next(r, opt); got != tt.want {
				t.Fatalf("Next = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateCommitsFirst(t *testing.T) {
	errStore := errors.New("store unavailable")
	tests := []struct {
		name   string
		commit func(context.Context, Record) error
	}{
		{"no commit", nil},
		{"commit fails", func(context.Context, Record) error { return errStore }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRecord(t)
			f := newFake(t, r)
			opt := f.options()
			opt.Commit = tt.commit
			got, err := step(t, f, r, opt)
			if err == nil || len(f.calls) != 0 {
				t.Fatalf("Step error %v, calls %v; want an error and no calls", err, f.calls)
			}
			if tt.commit != nil && !errors.Is(err, errStore) {
				t.Fatalf("Step error %v does not wrap the commit error", err)
			}
			if got.CreateSent || got.Phase != PhaseCreating {
				t.Fatalf("record after refused create: sent %v, phase %v", got.CreateSent, got.Phase)
			}
		})
	}

	t.Run("committed", func(t *testing.T) {
		r := newTestRecord(t)
		f := newFake(t, r)
		r = run(t, f, r, ActionCreate) // the fake checks the commit
		c := f.committed
		if c == nil || !c.CreateSent || !c.CreateSentAt.Equal(testTime) || c.InstanceID != "" || !bytes.Equal(c.Request, r.Request) {
			t.Fatalf("committed record %+v", c)
		}
	})
}

func TestUnsentCreate(t *testing.T) {
	// The client's STS account check precedes RunInstances; a failed
	// check comes back sent, but RunInstances was not.
	unsentErr := &ec2api.Error{Action: "GetCallerIdentity", Class: ec2api.ClassThrottled, Code: "Throttling", StatusCode: 400, Sent: true}

	t.Run("first create", func(t *testing.T) {
		r := newTestRecord(t)
		f := newFake(t, r)
		f.fail("RunInstances", unsentErr)
		r, err := step(t, f, r, f.options())
		if !errors.Is(err, unsentErr) {
			t.Fatalf("Step error %v", err)
		}
		if r.CreateSent || !r.CreateSentAt.IsZero() {
			t.Fatal("an unsent first create left the record marked CreateSent")
		}
		if a := Next(r, f.options()); a != ActionCreate {
			t.Fatalf("Next = %v, want create", a)
		}
	})

	t.Run("reissue", func(t *testing.T) {
		r := newTestRecord(t)
		f := newFake(t, r)
		f.loseReply = 1
		r, _ = step(t, f, r, f.options())
		f.fail("RunInstances", unsentErr)
		opt := f.options()
		opt.ReissueWindow = time.Hour
		if a := Next(r, opt); a != ActionReissue {
			t.Fatalf("Next = %v, want reissue", a)
		}
		r, _ = step(t, f, r, opt)
		if !r.CreateSent || !r.CreateSentAt.Equal(testTime) {
			t.Fatal("an unsent reissue cleared the earlier ambiguous create")
		}
	})
}

func TestCreditMismatchHeld(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.credits = "unlimited"
	r = run(t, f, r, ActionCreate)
	r, err := step(t, f, r, f.options())
	var cm *ec2api.CreditMismatchError
	if !errors.As(err, &cm) {
		t.Fatalf("verify credits error %v, want a CreditMismatchError", err)
	}
	want(t, r, PhaseHeld, HoldCredits)
	f.settle()
	for range 3 {
		r = run(t, f, r, ActionNone)
	}
	want(t, r, PhaseHeld, HoldCredits)

	// Stopping is still allowed; starting again re-checks and holds.
	r = desire(t, r, DesiredStopped)
	r = run(t, f, r, ActionStop)
	want(t, r, PhaseStopping, HoldNone)
	f.settle()
	r = run(t, f, r, ActionObserve)
	r = desire(t, r, DesiredPresent)
	r, err = step(t, f, r, f.options())
	if !errors.As(err, &cm) {
		t.Fatalf("start error %v, want a CreditMismatchError", err)
	}
	want(t, r, PhaseHeld, HoldCredits)

	// Terminating is still allowed.
	r = desire(t, r, DesiredAbsent)
	r = run(t, f, r, ActionTerminate)
	want(t, r, PhaseTerminating, HoldNone)
	if f.count("Terminate") != 1 || f.inst.State != "shutting-down" {
		t.Fatalf("terminate calls %d, state %s", f.count("Terminate"), f.inst.State)
	}
}

func TestRecoverRunningVerifiesCredits(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	f.loseReply = 1
	r, _ = step(t, f, r, f.options())
	f.settle()
	f.credits = "unlimited"
	r = run(t, f, r, ActionRecover)
	if r.InstanceID != testID || r.Phase == PhaseRunning {
		t.Fatalf("recovered running instance %q is %v before its credits were verified", r.InstanceID, r.Phase)
	}
	r, _ = step(t, f, r, f.options())
	want(t, r, PhaseHeld, HoldCredits)
}

func TestConflictHeld(t *testing.T) {
	ipm := &ec2api.Error{Action: "RunInstances", Class: ec2api.ClassIdempotentParameterMismatch, Code: "IdempotentParameterMismatch", StatusCode: 400, Sent: true}
	account := fmt.Errorf("ec2api: %w", ec2api.ErrAccountMismatch)
	notOwned := fmt.Errorf("%w: instance %s tags does not match", ec2api.ErrNotOwned, testID)
	tests := []struct {
		name    string
		setup   func(t *testing.T, f *fake, r Record) Record
		method  string
		err     error
		desired Desired
	}{
		{"idempotent parameter mismatch", nil, "RunInstances", ipm, DesiredPresent},
		{"account mismatch on create", nil, "RunInstances", account, DesiredPresent},
		{"not owned on recover", lostReply, "FindOwned", notOwned, DesiredPresent},
		{"not owned on credits", created, "CheckCPUCredits", notOwned, DesiredPresent},
		{"not owned on observe", running, "DescribeOwned", notOwned, DesiredPresent},
		{"not owned on stop", running, "Stop", notOwned, DesiredStopped},
		{"account mismatch on terminate", running, "Terminate", account, DesiredAbsent},
		{"not owned on start", stopped, "Start", notOwned, DesiredPresent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRecord(t)
			f := newFake(t, r)
			if tt.setup != nil {
				r = tt.setup(t, f, r)
			}
			r = desire(t, r, tt.desired)
			if tt.method == "DescribeOwned" {
				r.Phase, r.CreditsVerified = PhaseUnknown, false
			}
			f.fail(tt.method, tt.err)
			n := len(f.calls)
			r, err := step(t, f, r, f.options())
			if !errors.Is(err, tt.err) || len(f.calls) != n+1 || f.calls[n] != tt.method {
				t.Fatalf("Step error %v, calls %v", err, f.calls[n:])
			}
			want(t, r, PhaseHeld, HoldConflict)
			n = len(f.calls)
			r = desire(t, r, DesiredAbsent)
			for range 3 {
				r = run(t, f, r, ActionNone)
			}
			if len(f.calls) != n {
				t.Fatalf("held record made calls %v", f.calls[n:])
			}
		})
	}
}

// lostReply returns r after a create whose reply was lost.
func lostReply(t *testing.T, f *fake, r Record) Record {
	t.Helper()
	f.loseReply = 1
	r, err := step(t, f, r, f.options())
	if err == nil || r.InstanceID != "" || !r.CreateSent {
		t.Fatalf("lost reply: error %v, instance %q, sent %v", err, r.InstanceID, r.CreateSent)
	}
	return r
}

// created returns r after a successful create.
func created(t *testing.T, f *fake, r Record) Record {
	t.Helper()
	return run(t, f, r, ActionCreate)
}

// stopped returns r with its instance observed stopped.
func stopped(t *testing.T, f *fake, r Record) Record {
	t.Helper()
	r = desire(t, running(t, f, r), DesiredStopped)
	r = run(t, f, r, ActionStop)
	f.settle()
	return run(t, f, r, ActionObserve)
}

func TestNotFoundIsUnknown(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, f *fake, r Record) Record
		method  string
		err     error
		desired Desired
	}{
		{"terminate not found", running, "Terminate", notFoundErr("TerminateInstances"), DesiredAbsent},
		{"stop not found", running, "Stop", notFoundErr("StopInstances"), DesiredStopped},
		{"observe not found", stopped, "DescribeOwned", notFoundErr("DescribeInstances"), DesiredStopped},
		{"observe not observed", stopped, "DescribeOwned", fmt.Errorf("instance %s: %w", testID, ec2api.ErrNotObserved), DesiredStopped},
		{"credits not found", created, "CheckCPUCredits", notFoundErr("DescribeInstances"), DesiredPresent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRecord(t)
			f := newFake(t, r)
			r = desire(t, tt.setup(t, f, r), tt.desired)
			if tt.method == "DescribeOwned" {
				r.Phase = PhaseStopping
			}
			f.fail(tt.method, tt.err)
			r, err := step(t, f, r, f.options())
			if !errors.Is(err, tt.err) {
				t.Fatalf("Step error %v", err)
			}
			want(t, r, PhaseUnknown, HoldNone)
			if r.InstanceID != testID || r.CreditsVerified {
				t.Fatalf("unknown record: instance %q, credits verified %v", r.InstanceID, r.CreditsVerified)
			}
			// Not found again is still not terminated.
			f.fail("DescribeOwned", notFoundErr("DescribeInstances"))
			r, _ = step(t, f, r, f.options())
			want(t, r, PhaseUnknown, HoldNone)
		})
	}
}

func TestTerminatedOnlyObserved(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	r = run(t, f, r, ActionTerminate)
	want(t, r, PhaseTerminating, HoldNone)
	for range 2 {
		r = run(t, f, r, ActionObserve)
		want(t, r, PhaseTerminating, HoldNone)
	}
	f.settle()
	r = run(t, f, r, ActionObserve)
	want(t, r, PhaseTerminated, HoldNone)
}

func TestUnsettledMutationRetries(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	f.fail("Terminate", &ec2api.Error{Action: "TerminateInstances", Class: ec2api.ClassServerError, StatusCode: 503, Sent: true})
	r, err := step(t, f, r, f.options())
	if err == nil {
		t.Fatal("Step hid the failed terminate")
	}
	want(t, r, PhaseRunning, HoldNone)
	d := r.LastDiagnostic
	if d.Action != ActionTerminate || d.Call != "TerminateInstances" || d.Class != ec2api.ClassServerError || !d.Sent || d.Reason != outcomeNames[unsettled] {
		t.Fatalf("diagnostic %+v", d)
	}
	r = run(t, f, r, ActionTerminate)
	want(t, r, PhaseTerminating, HoldNone)
	if r.LastDiagnostic != (Diagnostic{}) {
		t.Fatalf("diagnostic %+v after success", r.LastDiagnostic)
	}
}

// TestCreateEvidenceWithError has RunInstances return its reservation
// together with an error. The reported instances are kept as unverified
// evidence and the record is held: no second create, and no reported
// instance treated as owned until a search by ClientToken verifies it.
func TestCreateEvidenceWithError(t *testing.T) {
	other := ec2api.Instance{ID: "i-0fedcba9876543210", State: "pending"}
	closeErr := &ec2api.Error{Action: "RunInstances", Class: ec2api.ClassTransport, StatusCode: 200, Sent: true}
	malformed := &ec2api.Error{Action: "RunInstances", Class: ec2api.ClassMalformed, StatusCode: 200, Sent: true}
	tests := []struct {
		name     string
		err      error
		extra    []ec2api.Instance
		reported []string
	}{
		{"close error", closeErr, nil, []string{testID}},
		{"two instances", malformed, []ec2api.Instance{other}, []string{testID, other.ID}},
		{"invalid ID", malformed, []ec2api.Instance{{ID: "i-bad"}}, []string{testID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRecord(t)
			f := newFake(t, r)
			f.runErr, f.extra = tt.err, tt.extra
			r, err := step(t, f, r, f.options())
			if !errors.Is(err, tt.err) {
				t.Fatalf("Step error %v", err)
			}
			want(t, r, PhaseHeld, HoldUnverified)
			if r.InstanceID != "" || !r.CreateSent || !slices.Equal(r.Reported, tt.reported) {
				t.Fatalf("instance %q, sent %v, reported %v; want none, true, %v", r.InstanceID, r.CreateSent, r.Reported, tt.reported)
			}
			for range 3 {
				r = run(t, f, r, ActionNone)
			}

			stored, err := FromFields(testProfile(), r)
			if err != nil || !slices.Equal(stored.Reported, tt.reported) {
				t.Fatalf("FromFields: %v, reported %v", err, stored.Reported)
			}
			r = desire(t, stored, DesiredAbsent)
			r = run(t, f, r, ActionRecover)
			r = run(t, f, r, ActionTerminate)
			if r.InstanceID != testID || !slices.Equal(r.Reported, tt.reported) {
				t.Fatalf("instance %q, reported %v", r.InstanceID, r.Reported)
			}
			if f.count("RunInstances") != 1 || f.created != 1 {
				t.Fatalf("%d creates sent, %d instances; want 1, 1", f.count("RunInstances"), f.created)
			}
		})
	}
}

// TestRecoverEvidenceWithError has FindOwned return an instance together
// with an error. The instance is kept as unverified evidence.
func TestRecoverEvidenceWithError(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = lostReply(t, f, r)
	findErr := &ec2api.Error{Action: "DescribeInstances", Class: ec2api.ClassTransport, StatusCode: 200, Sent: true}
	f.findErr = findErr
	r, err := step(t, f, r, f.options())
	if !errors.Is(err, findErr) {
		t.Fatalf("Step error %v", err)
	}
	want(t, r, PhaseHeld, HoldUnverified)
	if r.InstanceID != "" || !slices.Equal(r.Reported, []string{testID}) {
		t.Fatalf("instance %q, reported %v", r.InstanceID, r.Reported)
	}
	r = run(t, f, r, ActionNone)
}

// TestMutationEvidenceWithError has Terminate report its state change
// together with an error. The change is applied, so Step observes rather
// than terminating again.
func TestMutationEvidenceWithError(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	closeErr := &ec2api.Error{Action: "TerminateInstances", Class: ec2api.ClassTransport, StatusCode: 200, Sent: true}
	f.mutateErr = closeErr
	r, err := step(t, f, r, f.options())
	if !errors.Is(err, closeErr) {
		t.Fatalf("Step error %v", err)
	}
	want(t, r, PhaseTerminating, HoldNone)
	if d := r.LastDiagnostic; d.Call != "TerminateInstances" || !d.Sent || d.Class != ec2api.ClassTransport {
		t.Fatalf("diagnostic %+v", d)
	}
	r = run(t, f, r, ActionObserve)
	if f.count("Terminate") != 1 {
		t.Fatalf("terminate sent %d times", f.count("Terminate"))
	}
}

// TestMutationPrecheckNotSent fails the checks the client sends before a
// mutation. The mutation was not sent: the record is unchanged, and the
// next Step sends it.
func TestMutationPrecheckNotSent(t *testing.T) {
	tests := []struct {
		method  string
		call    string // the failed check
		desired Desired
		next    Action
	}{
		{"Stop", "DescribeInstances", DesiredStopped, ActionStop},
		{"Terminate", "GetCallerIdentity", DesiredAbsent, ActionTerminate},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			r := newTestRecord(t)
			f := newFake(t, r)
			r = desire(t, running(t, f, r), tt.desired)
			f.fail(tt.method, &ec2api.Error{Action: tt.call, Class: ec2api.ClassThrottled, StatusCode: 400, Sent: true})
			r, err := step(t, f, r, f.options())
			if err == nil {
				t.Fatal("Step hid the failed check")
			}
			want(t, r, PhaseRunning, HoldNone)
			if d := r.LastDiagnostic; d.Call != tt.call || d.Reason != outcomeNames[unsent] {
				t.Fatalf("diagnostic %+v", d)
			}
			r = run(t, f, r, tt.next)
		})
	}
}

// TestTerminatedWithoutPlacementIsHeld documents current behavior that
// root is reviewing: if EC2 describes a terminated instance without its
// subnet and security groups, ec2api.VerifyOwner refuses it as not owned,
// and the record is held as a conflict instead of reaching Terminated.
func TestTerminatedWithoutPlacementIsHeld(t *testing.T) {
	r := newTestRecord(t)
	req, err := ec2api.ParseCreateRequest(r.Request)
	if err != nil {
		t.Fatal(err)
	}
	p := testProfile()
	inst := ec2api.Instance{
		ID:               testID,
		State:            "terminated",
		ImageID:          p.ImageID,
		InstanceType:     p.InstanceType,
		SubnetID:         p.SubnetID,
		AvailabilityZone: p.Region + "a",
		ClientToken:      r.Token,
		SecurityGroupIDs: []string{"sg-0aaaaaaaaaaaaaaaa", "sg-0bbbbbbbbbbbbbbbb"},
		Tags: []ec2api.Tag{
			{Key: ec2api.TagName, Value: "skygo-env-0123"},
			{Key: ec2api.TagEnvironment, Value: "env-0123456789abcdef"},
			{Key: ec2api.TagGeneration, Value: "1"},
			{Key: ec2api.TagOwner, Value: "install-7f3a"},
			{Key: ec2api.TagProfile, Value: r.ProfileHash},
		},
	}
	verify := func(inst ec2api.Instance) error {
		_, err := ec2api.VerifyOwner(p, req, []ec2api.Reservation{{ID: "r-0123456789abcdef0", OwnerID: testAccount, Instances: []ec2api.Instance{inst}}}, testID)
		return err
	}
	if err := verify(inst); err != nil {
		t.Fatalf("VerifyOwner refused the complete terminated instance: %v", err)
	}
	inst.SubnetID, inst.SecurityGroupIDs = "", nil
	notOwned := verify(inst)
	if !errors.Is(notOwned, ec2api.ErrNotOwned) {
		t.Fatalf("VerifyOwner without placement: %v, want ErrNotOwned", notOwned)
	}

	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	r = run(t, f, r, ActionTerminate)
	f.fail("DescribeOwned", notOwned)
	r, _ = step(t, f, r, f.options())
	want(t, r, PhaseHeld, HoldConflict)
}

// TestAmbiguousTerminateRetried loses the reply to a TerminateInstances
// that took effect. The record is unchanged but for its diagnostic, which
// is stored, and the next Step terminates again through the client's
// ownership check. When the instance has meanwhile aged out of EC2's
// descriptions, the record becomes Unknown, never Terminated.
func TestAmbiguousTerminateRetried(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	f.loseState = 1
	r, err := step(t, f, r, f.options())
	if err == nil {
		t.Fatal("Step hid the lost terminate reply")
	}
	want(t, r, PhaseRunning, HoldNone)
	d := r.LastDiagnostic
	if d.Action != ActionTerminate || d.Call != "TerminateInstances" || !d.Sent || d.Reason != outcomeNames[unsettled] {
		t.Fatalf("diagnostic %+v", d)
	}
	stored, err := FromFields(testProfile(), r)
	if err != nil || stored.LastDiagnostic != d {
		t.Fatalf("FromFields: %v, diagnostic %+v", err, stored.LastDiagnostic)
	}

	f.inst = nil // terminated and no longer described
	if a := Next(stored, f.options()); a != ActionTerminate {
		t.Fatalf("Next = %v, want terminate", a)
	}
	r, err = step(t, f, stored, f.options())
	if err == nil {
		t.Fatal("retry reported no error")
	}
	want(t, r, PhaseUnknown, HoldNone)
	if d := r.LastDiagnostic; d.Call != "DescribeInstances" || d.Reason != outcomeNames[notFound] {
		t.Fatalf("retry diagnostic %+v; want the ownership check to fail", d)
	}
	for range 2 {
		r, _ = step(t, f, r, f.options())
		want(t, r, PhaseUnknown, HoldNone)
	}
	if f.count("Terminate") != 2 {
		t.Fatalf("terminate attempts %d, want 2", f.count("Terminate"))
	}
}

// TestTerminatedEvidenceWithError has TerminateInstances report the
// terminated state together with an error. A reply with an error is not an
// observation of termination: the record is Terminating, keeps its
// diagnostic, and becomes Terminated only when Observe sees it.
func TestTerminatedEvidenceWithError(t *testing.T) {
	r := newTestRecord(t)
	f := newFake(t, r)
	r = desire(t, running(t, f, r), DesiredAbsent)
	closeErr := &ec2api.Error{Action: "TerminateInstances", Class: ec2api.ClassTransport, StatusCode: 200, Sent: true}
	f.reportState, f.mutateErr = "terminated", closeErr
	r, err := step(t, f, r, f.options())
	if !errors.Is(err, closeErr) {
		t.Fatalf("Step error %v", err)
	}
	want(t, r, PhaseTerminating, HoldNone)
	d := r.LastDiagnostic
	if d.Call != "TerminateInstances" || !d.Sent || d.Reason != outcomeNames[unsettled] {
		t.Fatalf("diagnostic %+v", d)
	}
	stored, err := FromFields(testProfile(), r)
	if err != nil || stored.Phase != PhaseTerminating || stored.LastDiagnostic != d {
		t.Fatalf("FromFields: %v, phase %v, diagnostic %+v", err, stored.Phase, stored.LastDiagnostic)
	}
	r = run(t, f, stored, ActionObserve)
	want(t, r, PhaseTerminated, HoldNone)
	if f.count("Terminate") != 1 {
		t.Fatalf("terminate sent %d times", f.count("Terminate"))
	}
}
