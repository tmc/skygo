package ec2vm

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

// A Desired is the state the owner of a record wants.
type Desired int

const (
	DesiredPresent Desired = iota + 1 // running
	DesiredStopped
	DesiredAbsent // terminated; latched
)

var desiredNames = [...]string{
	DesiredPresent: "present",
	DesiredStopped: "stopped",
	DesiredAbsent:  "absent",
}

func (d Desired) String() string {
	if d >= DesiredPresent && int(d) < len(desiredNames) {
		return desiredNames[d]
	}
	return fmt.Sprintf("Desired(%d)", int(d))
}

// A Phase is the last observed lifecycle phase of a record's instance.
type Phase int

const (
	PhaseCreating    Phase = iota + 1 // no create sent, create ambiguous, or created and not yet verified running
	PhaseRunning                      // observed running with its CPU credit option verified
	PhaseStopping                     // observed stopping
	PhaseStopped                      // observed stopped
	PhaseStarting                     // started and not yet verified running
	PhaseTerminating                  // observed shutting down
	PhaseTerminated                   // observed terminated
	PhaseUnknown                      // the instance was not found before it was observed terminated
	PhaseHeld                         // waiting for the operator; see Hold
)

var phaseNames = [...]string{
	PhaseCreating:    "creating",
	PhaseRunning:     "running",
	PhaseStopping:    "stopping",
	PhaseStopped:     "stopped",
	PhaseStarting:    "starting",
	PhaseTerminating: "terminating",
	PhaseTerminated:  "terminated",
	PhaseUnknown:     "unknown",
	PhaseHeld:        "held",
}

func (p Phase) String() string {
	if p >= PhaseCreating && int(p) < len(phaseNames) {
		return phaseNames[p]
	}
	return fmt.Sprintf("Phase(%d)", int(p))
}

// A Hold is the reason a record is in PhaseHeld.
type Hold int

const (
	HoldNone Hold = iota

	// HoldNotObserved: a create may have been sent, and searching by
	// ClientToken observed no instance. If the record wants Absent, Step
	// keeps searching so that it can terminate what it finds.
	HoldNotObserved

	// HoldConflict: an IdempotentParameterMismatch, an instance that is
	// not owned, credentials of another account, or another refusal
	// before sending. Step makes no further calls.
	HoldConflict

	// HoldCredits: the owned instance's CPU credit option differs from
	// the profile's. Step still stops or terminates it if the record
	// wants Stopped or Absent.
	HoldCredits

	// HoldUnverified: a reply reported instances together with an error,
	// such as a create reply that failed a check or did not close
	// cleanly. Reported keeps their IDs; none is verified as owned. If
	// the record wants Absent, Step searches by ClientToken so that it
	// can terminate what it verifies.
	HoldUnverified
)

var holdNames = [...]string{
	HoldNone:        "none",
	HoldNotObserved: "not observed",
	HoldConflict:    "conflict",
	HoldCredits:     "credit mismatch",
	HoldUnverified:  "unverified evidence",
}

func (h Hold) String() string {
	if h >= HoldNone && int(h) < len(holdNames) {
		return holdNames[h]
	}
	return fmt.Sprintf("Hold(%d)", int(h))
}

// A Diagnostic describes the last failed call of a record. It holds no
// provider text.
type Diagnostic struct {
	Action Action       // the Step action
	Call   string       // the failed ec2api Action, such as "DescribeInstances", if known
	Class  ec2api.Class // the ec2api error class, if Call is set
	Sent   bool         // whether the failed request may have reached EC2
	Reason string       // fixed text from this package
}

// A Record is the durable lifecycle state of one owned EC2 instance.
//
// A Record has no encoding: whoever stores it stores every field and
// passes exactly what it stored to FromFields.
type Record struct {
	Identity    ec2api.Identity
	ProfileHash string
	Request     []byte // exact body of the RunInstances request
	Token       string // ClientToken of Request

	Desired Desired
	Phase   Phase
	Hold    Hold // set only in PhaseHeld

	InstanceID string // empty until a create or a search observes the instance

	// Reported lists instance IDs that a reply reported together with an
	// error. They are evidence for the operator, not verified as owned,
	// and are never removed.
	Reported []string

	// CreateSent reports that a RunInstances request may have reached
	// EC2. CreateSentAt is when Step committed the first one.
	CreateSent   bool
	CreateSentAt time.Time

	// CreditsVerified reports that CheckCPUCredits accepted the instance
	// since it was created or last observed stopping, stopped or gone.
	CreditsVerified bool

	LastDiagnostic Diagnostic
}

// NewRecord returns the record of a new instance for id from profile p,
// wanting it present. Nothing has been sent.
func NewRecord(p *ec2api.Profile, id ec2api.Identity, userData []byte) (Record, error) {
	req, err := ec2api.NewCreateRequest(p, id, userData)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Identity:    id,
		ProfileHash: p.Hash(),
		Request:     req.Body(),
		Token:       req.ClientToken(),
		Desired:     DesiredPresent,
		Phase:       PhaseCreating,
	}, nil
}

// FromFields returns a copy of r, a record read back from storage, after
// checking it against profile p: Request must be a canonical request for
// p carrying exactly r's Token, Identity and ProfileHash, and the
// lifecycle fields must be consistent.
func FromFields(p *ec2api.Profile, r Record) (Record, error) {
	if err := p.Validate(); err != nil {
		return Record{}, err
	}
	if r.ProfileHash != p.Hash() {
		return Record{}, errors.New("record profile hash is not the profile's")
	}
	if _, err := r.request(); err != nil {
		return Record{}, err
	}
	if err := r.check(); err != nil {
		return Record{}, err
	}
	r.Request = bytes.Clone(r.Request)
	r.Reported = slices.Clone(r.Reported)
	return r, nil
}

// WithDesired returns r wanting d. Absent is latched: a record that wants
// Absent keeps wanting it.
func (r Record) WithDesired(d Desired) (Record, error) {
	if d < DesiredPresent || d > DesiredAbsent {
		return r, fmt.Errorf("desired state %v is invalid", d)
	}
	if r.Desired == DesiredAbsent && d != DesiredAbsent {
		return r, errors.New("record already wants absent")
	}
	r.Desired = d
	return r, nil
}

// request parses r's request and checks that it carries r's token,
// identity and profile hash.
func (r *Record) request() (*ec2api.CreateRequest, error) {
	if err := r.Identity.Validate(); err != nil {
		return nil, fmt.Errorf("record %w", err)
	}
	req, err := ec2api.ParseCreateRequest(r.Request)
	if err != nil {
		return nil, fmt.Errorf("record request: %w", err)
	}
	switch {
	case req.ClientToken() != r.Token || r.Token != r.Identity.ClientToken():
		return nil, errors.New("record token does not match its request and identity")
	case req.Identity() != r.Identity:
		return nil, errors.New("record identity does not match its request")
	case req.ProfileHash() != r.ProfileHash:
		return nil, errors.New("record profile hash does not match its request")
	}
	return req, nil
}

var instanceIDRE = regexp.MustCompile(`^i-([0-9a-f]{8}|[0-9a-f]{17})$`)

// check reports whether r's lifecycle fields are consistent.
func (r *Record) check() error {
	switch {
	case r.Desired < DesiredPresent || r.Desired > DesiredAbsent:
		return errors.New("record desired state is invalid")
	case r.Phase < PhaseCreating || r.Phase > PhaseHeld:
		return errors.New("record phase is invalid")
	case r.Hold < HoldNone || r.Hold > HoldUnverified:
		return errors.New("record hold is invalid")
	case (r.Phase == PhaseHeld) != (r.Hold != HoldNone):
		return errors.New("record hold does not match its phase")
	case r.CreateSent == r.CreateSentAt.IsZero():
		return errors.New("record create sent time does not match its create sent flag")
	case r.InstanceID != "" && !instanceIDRE.MatchString(r.InstanceID):
		return errors.New("record instance ID is invalid")
	case r.InstanceID != "" && !r.CreateSent:
		return errors.New("record has an instance ID but sent no create")
	case (r.Hold == HoldNotObserved || r.Hold == HoldUnverified) && (r.InstanceID != "" || !r.CreateSent):
		return errors.New("record holds for an unobserved create it does not have")
	case len(r.Reported) > 0 && !r.CreateSent:
		return errors.New("record has reported instances but sent no create")
	case slices.ContainsFunc(r.Reported, func(id string) bool { return !instanceIDRE.MatchString(id) }):
		return errors.New("record reported instance ID is invalid")
	case r.InstanceID == "" && r.Phase != PhaseCreating && r.Hold != HoldNotObserved && r.Hold != HoldConflict && r.Hold != HoldUnverified:
		return fmt.Errorf("record phase %v requires an instance ID", r.Phase)
	case r.CreditsVerified && r.InstanceID == "":
		return errors.New("record has verified credits but no instance ID")
	case r.Phase == PhaseRunning && !r.CreditsVerified:
		return errors.New("record is running without verified credits")
	case r.CreditsVerified && (r.Phase == PhaseStopping || r.Phase == PhaseStopped || r.Phase == PhaseTerminating || r.Phase == PhaseTerminated || r.Phase == PhaseUnknown):
		return fmt.Errorf("record phase %v has verified credits", r.Phase)
	}
	return nil
}
