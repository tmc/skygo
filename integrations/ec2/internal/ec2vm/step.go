package ec2vm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

// provider is the part of [ec2api.Client] that Step calls.
type provider interface {
	RunInstances(ctx context.Context, req *ec2api.CreateRequest) (*ec2api.Reservation, error)
	FindOwned(ctx context.Context, req *ec2api.CreateRequest) (*ec2api.Instance, error)
	DescribeOwned(ctx context.Context, req *ec2api.CreateRequest, id string) (*ec2api.Instance, error)
	CheckCPUCredits(ctx context.Context, req *ec2api.CreateRequest, id string) (string, error)
	Stop(ctx context.Context, req *ec2api.CreateRequest, id string) (*ec2api.StateChange, error)
	Start(ctx context.Context, req *ec2api.CreateRequest, id string) (*ec2api.StateChange, error)
	Terminate(ctx context.Context, req *ec2api.CreateRequest, id string) (*ec2api.StateChange, error)
}

var _ provider = (*ec2api.Client)(nil)

// An Action is the one call Step makes for a record.
type Action int

const (
	ActionNone          Action = iota // nothing until the record changes
	ActionCreate                      // commit CreateSent, then RunInstances
	ActionReissue                     // RunInstances with the identical request
	ActionRecover                     // FindOwned by ClientToken
	ActionVerifyCredits               // CheckCPUCredits
	ActionObserve                     // DescribeOwned
	ActionStop                        // Stop
	ActionStart                       // Start
	ActionTerminate                   // Terminate
)

var actionNames = [...]string{
	ActionNone:          "none",
	ActionCreate:        "create",
	ActionReissue:       "reissue",
	ActionRecover:       "recover",
	ActionVerifyCredits: "verify credits",
	ActionObserve:       "observe",
	ActionStop:          "stop",
	ActionStart:         "start",
	ActionTerminate:     "terminate",
}

func (a Action) String() string {
	if a >= ActionNone && int(a) < len(actionNames) {
		return actionNames[a]
	}
	return fmt.Sprintf("Action(%d)", int(a))
}

// Options configures Step.
type Options struct {
	// Commit durably stores a record. Step sends a record's first
	// RunInstances only after Commit has stored the record marked
	// CreateSent. Step refuses to create without Commit.
	Commit func(context.Context, Record) error

	// ReissueWindow is how long after the first create was committed Step
	// may reissue the identical request after an ambiguous result. Zero,
	// the default, never reissues: Step searches by ClientToken instead.
	// It must not exceed the period for which EC2 retains a ClientToken.
	ReissueWindow time.Duration

	// Now returns the current time. If nil, Step uses time.Now.
	Now func() time.Time
}

func (o *Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// reissue reports whether r's ambiguous create may be reissued now.
func (o *Options) reissue(r Record) bool {
	if o.ReissueWindow <= 0 {
		return false
	}
	d := o.now().Sub(r.CreateSentAt)
	return d >= 0 && d < o.ReissueWindow
}

// Next returns the action Step takes for r under opt.
func Next(r Record, opt Options) Action {
	switch r.Phase {
	case PhaseTerminated:
		return ActionNone
	case PhaseHeld:
		switch {
		case (r.Hold == HoldNotObserved || r.Hold == HoldUnverified) && r.Desired == DesiredAbsent:
			return ActionRecover
		case r.Hold == HoldCredits && r.Desired == DesiredAbsent:
			return ActionTerminate
		case r.Hold == HoldCredits && r.Desired == DesiredStopped:
			return ActionStop
		}
		return ActionNone
	case PhaseUnknown:
		return ActionObserve
	}
	if r.InstanceID == "" {
		switch {
		case !r.CreateSent && r.Desired == DesiredAbsent:
			return ActionNone // nothing was sent
		case !r.CreateSent:
			return ActionCreate
		case r.Desired != DesiredAbsent && opt.reissue(r):
			return ActionReissue
		}
		return ActionRecover
	}
	switch r.Desired {
	case DesiredAbsent:
		if r.Phase == PhaseTerminating {
			return ActionObserve
		}
		return ActionTerminate
	case DesiredStopped:
		switch r.Phase {
		case PhaseStopped:
			return ActionNone
		case PhaseStopping, PhaseTerminating:
			return ActionObserve
		}
		return ActionStop
	}
	switch r.Phase {
	case PhaseRunning:
		return ActionNone
	case PhaseStopped:
		return ActionStart
	case PhaseCreating, PhaseStarting:
		if !r.CreditsVerified {
			return ActionVerifyCredits
		}
	}
	return ActionObserve
}

// Step makes the call Next(r, opt) names and returns the record to store
// before the next Step, together with the call's error, if any. Step makes
// at most one call to p, and so at most one EC2 mutation.
//
// Persistence boundary: the caller must durably store the record Step
// returns, error or not, before it calls Step again. For ActionCreate,
// Step first passes opt.Commit the record marked CreateSent, and sends
// RunInstances only after Commit returns nil, which must mean the record
// is durably stored. Step refuses to create if opt.Commit is nil, and
// sends nothing and returns r if Commit fails.
//
// When p returns a result together with an error, Step keeps the result:
// a reported reservation is held as unverified evidence (see
// HoldUnverified), and a reported state change is applied, except that a
// reported terminated state goes no further than PhaseTerminating, so that
// the next Step observes it.
func Step(ctx context.Context, p provider, r Record, opt Options) (Record, error) {
	if err := r.check(); err != nil {
		return r, err
	}
	req, err := r.request()
	if err != nil {
		return r, err
	}
	a := Next(r, opt)
	switch a {
	case ActionNone:
		return r, nil
	case ActionCreate:
		if opt.Commit == nil {
			return r, errors.New("ec2vm: no Commit to store the create before sending it")
		}
		sent := r
		sent.CreateSent, sent.CreateSentAt = true, opt.now()
		if err := opt.Commit(ctx, sent); err != nil {
			return r, fmt.Errorf("ec2vm: commit create: %w", err)
		}
		res, err := p.RunInstances(ctx, req)
		return sent.created(a, res, err)
	case ActionReissue:
		res, err := p.RunInstances(ctx, req)
		return r.created(a, res, err)
	case ActionRecover:
		inst, err := p.FindOwned(ctx, req)
		if err != nil {
			r, err = r.failed(a, err)
			if inst != nil {
				r = r.unverified(*inst)
			}
			return r, err
		}
		r.InstanceID = inst.ID
		r.Phase = PhaseCreating
		return r.observed(inst.State), nil
	case ActionVerifyCredits:
		if _, err := p.CheckCPUCredits(ctx, req, r.InstanceID); err != nil {
			return r.failed(a, err)
		}
		r.CreditsVerified = true
		r.LastDiagnostic = Diagnostic{}
		return r, nil
	case ActionObserve:
		inst, err := p.DescribeOwned(ctx, req, r.InstanceID)
		if err != nil {
			// A description that failed verification says nothing
			// about the owned instance's state.
			return r.failed(a, err)
		}
		return r.observed(inst.State), nil
	case ActionStop, ActionStart, ActionTerminate:
		mutate := p.Terminate
		switch a {
		case ActionStop:
			mutate = p.Stop
		case ActionStart:
			mutate = p.Start
		}
		sc, err := mutate(ctx, req, r.InstanceID)
		if sc != nil {
			// Applied even with an error, such as a failure to close
			// the response: the change was reported. A reply that came
			// with an error does not observe termination.
			r = r.observed(sc.Current)
			if err != nil && r.Phase == PhaseTerminated {
				r.Phase = PhaseTerminating
			}
		}
		if err != nil {
			return r.failed(a, err)
		}
		return r, nil
	}
	return r, fmt.Errorf("ec2vm: unknown action %v", a)
}

// created applies the result of a RunInstances request.
// RunInstances verified a successful reservation: one instance, the
// profile's account, the request's token. A reservation returned with an
// error, because it failed one of those checks or its response did not
// close cleanly, is kept as unverified evidence.
func (r Record) created(a Action, res *ec2api.Reservation, err error) (Record, error) {
	if err != nil {
		r, err = r.failed(a, err)
		if res != nil {
			r = r.unverified(res.Instances...)
		}
		return r, err
	}
	r.InstanceID = res.Instances[0].ID
	return r.observed(res.Instances[0].State), nil
}

// unverified holds r with instances a reply reported together with an
// error, adding their IDs to r.Reported. A conflict hold stays.
func (r Record) unverified(insts ...ec2api.Instance) Record {
	ids := slices.Clone(r.Reported)
	for _, inst := range insts {
		if instanceIDRE.MatchString(inst.ID) && !slices.Contains(ids, inst.ID) {
			ids = append(ids, inst.ID)
		}
	}
	r.Reported = ids
	if r.Hold != HoldConflict {
		r.Phase, r.Hold = PhaseHeld, HoldUnverified
	}
	return r
}

// observed applies an observed EC2 instance state.
func (r Record) observed(state string) Record {
	r.Phase, r.Hold = r.phaseOf(state), HoldNone
	switch r.Phase {
	case PhaseStopping, PhaseStopped, PhaseTerminating, PhaseTerminated, PhaseUnknown:
		r.CreditsVerified = false
	}
	r.LastDiagnostic = Diagnostic{}
	return r
}

// phaseOf returns the phase of r's instance in EC2 state.
func (r Record) phaseOf(state string) Phase {
	switch state {
	case "pending":
		if r.Phase == PhaseCreating {
			return PhaseCreating
		}
		return PhaseStarting
	case "running":
		switch {
		case r.CreditsVerified:
			return PhaseRunning
		case r.Phase == PhaseCreating:
			return PhaseCreating
		}
		return PhaseStarting
	case "stopping":
		return PhaseStopping
	case "stopped":
		return PhaseStopped
	case "shutting-down":
		return PhaseTerminating
	case "terminated":
		return PhaseTerminated
	}
	return PhaseUnknown
}

// failed applies the facts a failed call of action a establishes and
// returns err with the record.
func (r Record) failed(a Action, err error) (Record, error) {
	o := classify(a, err)
	r.LastDiagnostic = diagnose(a, o, err)
	switch o {
	case conflict:
		r.Phase, r.Hold = PhaseHeld, HoldConflict
	case creditMismatch:
		r.Phase, r.Hold = PhaseHeld, HoldCredits
	case notObserved, notFound:
		if r.InstanceID == "" {
			r.Phase, r.Hold = PhaseHeld, HoldNotObserved
		} else {
			r.Phase, r.Hold, r.CreditsVerified = PhaseUnknown, HoldNone, false
		}
	case unsent:
		if a == ActionCreate {
			// Nothing was ever sent for this record.
			r.CreateSent, r.CreateSentAt = false, time.Time{}
		}
	}
	return r, err
}

// An outcome is what a failed call establishes.
type outcome int

const (
	unsettled      outcome = iota // the call failed; a mutation may have taken effect
	unsent                        // the request was not sent
	notObserved                   // a search observed no instance
	notFound                      // EC2 reported the instance ID not found
	creditMismatch                // the owned instance's CPU credit option is wrong
	conflict                      // the identity or account conflicts
)

var outcomeNames = [...]string{
	unsettled:      "call failed; effect unknown",
	unsent:         "call not sent",
	notObserved:    "no instance observed",
	notFound:       "instance not found",
	creditMismatch: "CPU credit option mismatch",
	conflict:       "identity or account conflict",
}

// mutationCalls maps each mutating action to the ec2api Action of the
// mutation itself. The client sends checks first, such as the STS account
// check before RunInstances and the ownership description before Stop.
var mutationCalls = map[Action]string{
	ActionCreate:    "RunInstances",
	ActionReissue:   "RunInstances",
	ActionStop:      "StopInstances",
	ActionStart:     "StartInstances",
	ActionTerminate: "TerminateInstances",
}

// classify returns what err, from the call action a makes, establishes.
func classify(a Action, err error) outcome {
	var (
		e  *ec2api.Error
		cm *ec2api.CreditMismatchError
	)
	switch {
	case errors.As(err, &cm):
		return creditMismatch
	case errors.Is(err, ec2api.ErrNotObserved):
		return notObserved
	case !errors.As(err, &e):
		// ErrNotOwned, ErrAccountMismatch, or another refusal before
		// sending, such as a request for another profile.
		return conflict
	case e.Class == ec2api.ClassIdempotentParameterMismatch:
		return conflict
	case e.Class == ec2api.ClassInstanceNotFound:
		return notFound
	case !e.Sent:
		return unsent
	case mutationCalls[a] != "" && e.Action != mutationCalls[a]:
		// A check that precedes the mutation failed, so the mutation
		// itself was not sent.
		return unsent
	}
	return unsettled
}

func diagnose(a Action, o outcome, err error) Diagnostic {
	d := Diagnostic{Action: a, Reason: outcomeNames[o]}
	var e *ec2api.Error
	if errors.As(err, &e) {
		d.Call, d.Class, d.Sent = e.Action, e.Class, e.Sent
	}
	return d
}
