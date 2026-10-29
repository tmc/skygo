package gce

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

// A Goal is the desired state of a machine.
type Goal string

const (
	Running   Goal = "running"   // the owned VM exists and runs
	Stopped   Goal = "stopped"   // the owned VM exists and is stopped (TERMINATED)
	Restarted Goal = "restarted" // the owned VM was stopped, then runs again
	Absent    Goal = "absent"    // the owned VM, then the owned disk, are deleted; latched
)

func (g Goal) valid() bool {
	switch g {
	case Running, Stopped, Restarted, Absent:
		return true
	}
	return false
}

// DriveOptions bound one Drive: at most Steps lifecycle steps, 1 to 1000,
// waiting Interval, 1s to 5m, between observations of unfinished work.
type DriveOptions struct {
	Steps    int
	Interval time.Duration
}

// A Status is a detached snapshot of a machine's state. Held is empty or
// one of a fixed set of short reasons the goal is not reached; it never
// holds server text. Reached is true only when the exact owned resources
// were observed in the desired state and no original request is
// unresolved. PendingRequestID and PendingOperation identify an
// unresolved mutation.
//
// InstanceState and DiskState are the last observed Compute states. ABSENT
// means the resource with the pinned id was found gone after this
// machine's own delete of it; REMOVED-EXTERNALLY means it was found gone
// without one.
type Status struct {
	Project, ComputeProjectID, Zone         string
	InstanceName, InstanceID, InstanceState string
	DiskName, DiskID, DiskState             string
	ImageID                                 string
	Desired                                 Goal
	PendingRequestID, PendingOperation      string
	ObservedAt                              time.Time
	Reached                                 bool
	Held                                    string
}

// Held reasons.
const (
	heldPending      = "original-request-pending"
	heldBudget       = "budget-exhausted"
	heldToken        = "token-expiring"
	heldSteps        = "step-bound"
	heldTransient    = "service-unavailable"
	heldUnresolved   = "operation-unresolved"
	heldPrepared     = "operation-prepared"
	heldTruncated    = "operation-list-truncated"
	heldOpMissing    = "operation-evidence-missing"
	heldNameTaken    = "reserved-name-in-use"
	heldDrift        = "identity-or-spec-drift"
	heldImageChanged = "image-changed"
	heldMissing      = "owned-resource-missing"
	heldDiskInUse    = "disk-in-use"
	heldUnknownState = "unpinned-resource-state"
	heldNotCreated   = "not-created"
	heldCreateFailed = "create-not-applied"
	heldConflict     = "request-conflict"
	heldNotApplied   = "goal-not-observed"
	heldUnadmitted   = "not-admitted"
)

// validHeld reports whether h is empty or a held reason.
func validHeld(h string) bool {
	switch h {
	case "", heldPending, heldBudget, heldToken, heldSteps, heldTransient,
		heldUnresolved, heldPrepared, heldTruncated, heldOpMissing, heldNameTaken,
		heldDrift, heldImageChanged, heldMissing, heldDiskInUse, heldUnknownState,
		heldNotCreated, heldCreateFailed, heldConflict, heldNotApplied, heldUnadmitted:
		return true
	}
	return false
}

// A holdError ends a step without error: the goal is not reached and no
// further effect is admitted in this Drive or Refresh.
type holdError string

func (h holdError) Error() string { return "held: " + string(h) }

type outcome int

const (
	stepNext    outcome = iota // continue at once
	stepWait                   // wait Interval, then continue
	stepReached                // goal observed
)

// Drive moves the machine toward its goal with the explicit OAuth 2 bearer
// token tok, which it copies and never refreshes, stores or prints. Each
// step sends at most one mutation; before every request Drive checks that
// ctx is live and tok has at least two minutes of validity. ctx must have a
// deadline.
//
// A held or bounded outcome returns the Status with Reached false and a nil
// error. Invalid input, refused or failed operations, authorization
// failures and state or custody faults return an error with the last
// Status. If ctx ends while an original request is in flight, Drive
// returns with the request still owned by m.
func (m *Machine) Drive(ctx context.Context, tok oauth2.Token, opt DriveOptions) (Status, error) {
	if err := m.usable(); err != nil {
		return Status{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return m.rec.status(), errors.New("gce: drive needs a context deadline")
	}
	if opt.Steps < 1 || opt.Steps > 1000 || opt.Interval < time.Second || opt.Interval > 5*time.Minute {
		return m.rec.status(), errors.New("gce: drive options need 1 to 1000 steps and an interval of 1s to 5m")
	}
	if err := checkToken(&tok); err != nil {
		return m.rec.status(), fmt.Errorf("gce: %w", err)
	}
	for i := 0; i < opt.Steps; i++ {
		out, err := m.step(ctx, &tok)
		var h holdError
		switch {
		case errors.As(err, &h):
			return m.settle(false, string(h))
		case err != nil:
			return m.fail(err)
		case out == stepReached:
			return m.settle(true, "")
		case out == stepWait:
			t := time.NewTimer(opt.Interval)
			select {
			case <-ctx.Done():
				t.Stop()
				return m.settle(false, heldBudget)
			case <-t.C:
			}
		}
	}
	return m.settle(false, heldSteps)
}

// Refresh observes the machine's owned resources with the explicit OAuth 2
// bearer token tok, under the same token and deadline rules as Drive, and
// records what it saw in the state directory. It sends only GET requests:
// it never prepares, sends or retries a mutation, reserves a name, mints a
// request id or changes the goal. An original left pending by an earlier
// call is waited for and recorded, never overlapped. A prepared, lost or
// running operation is only looked up; a resource whose numeric id was
// never pinned is never reported gone. A goal other than Absent is
// reported reached only for an admitted machine whose disk holds exactly
// the pinned image; Refresh never admits. A machine that is closed or
// fenced by a custody or commit fault returns its stored Status and the
// fault without any request.
//
// A held outcome returns the Status with Reached false and a nil error.
func (m *Machine) Refresh(ctx context.Context, tok oauth2.Token) (Status, error) {
	if err := m.usable(); err != nil {
		if errors.Is(err, errZero) {
			return Status{}, err
		}
		return m.rec.status(), err
	}
	if _, ok := ctx.Deadline(); !ok {
		return m.rec.status(), errors.New("gce: refresh needs a context deadline")
	}
	if err := checkToken(&tok); err != nil {
		return m.rec.status(), fmt.Errorf("gce: %w", err)
	}
	reached, err := m.refresh(ctx, &tok)
	var h holdError
	switch {
	case errors.As(err, &h):
		return m.settle(false, string(h))
	case err != nil:
		return m.fail(err)
	}
	return m.settle(reached, "")
}

// checkToken reports whether tok is a finite bearer access token with no
// refresh token.
func checkToken(tok *oauth2.Token) error {
	switch {
	case tok.AccessToken == "":
		return errors.New("token has no access token")
	case tok.Type() != "Bearer":
		return errors.New("token type is not Bearer")
	case tok.RefreshToken != "":
		return errors.New("token carries a refresh token; supply an access token only")
	case tok.Expiry.IsZero():
		return errors.New("token has no expiry")
	}
	return nil
}

// settle commits the end of a Drive or Refresh and returns its Status.
func (m *Machine) settle(reached bool, held string) (Status, error) {
	r := m.rec
	r.Reached, r.Held = reached && r.Op == nil && m.pending == nil, held
	if r.Reached {
		r.CompletedIntent = r.Intent
	}
	if err := m.save(); err != nil {
		return r.status(), err
	}
	return r.status(), nil
}

// fail records err as the last diagnostic and returns it.
func (m *Machine) fail(err error) (Status, error) {
	r := m.rec
	d := err.Error()
	if len(d) > 256 {
		d = d[:256]
	}
	r.Reached, r.Held, r.Diagnostic = false, "", d
	if serr := m.save(); serr != nil {
		return r.status(), fmt.Errorf("gce: %w", errors.Join(err, serr))
	}
	return r.status(), fmt.Errorf("gce: %w", err)
}

// drain waits for and records a pending original, if any.
func (m *Machine) drain(ctx context.Context) error {
	c := m.pending
	if c == nil {
		return nil
	}
	if !wait(ctx, c) {
		return holdError(heldPending)
	}
	replyErr, err := m.absorb()
	if err != nil {
		return err
	}
	return replyErr
}

// step settles any pending original, then advances an unresolved mutation
// or plans the next one.
func (m *Machine) step(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	if err := m.drain(ctx); err != nil {
		return 0, err
	}
	if m.rec.Op != nil {
		return m.advance(ctx, tok, m.rec.Op)
	}
	return m.plan(ctx, tok)
}

// wait reports whether c settled before ctx ended.
func wait(ctx context.Context, c *call) bool {
	select {
	case <-c.done:
		return true
	case <-ctx.Done():
		return false
	}
}

// admit reports whether a new original may be sent now.
func (m *Machine) admit(ctx context.Context, tok *oauth2.Token) error {
	switch {
	case m.pending != nil || m.held != nil:
		return errors.New("an original request is already pending")
	case m.fault != nil:
		return m.fault
	case ctx.Err() != nil:
		return holdError(heldBudget)
	case tok.Expiry.Sub(m.now()) < tokenMargin:
		return holdError(heldToken)
	}
	return nil
}

// get sends one read and waits for it to settle. If ctx ends first, m
// keeps the call pending. If the body's first Close failed, m keeps the
// call and is fenced.
func (m *Machine) get(ctx context.Context, tok *oauth2.Token, what, path, query string, v any) error {
	if err := m.admit(ctx, tok); err != nil {
		return err
	}
	c, err := m.client.start(what, "GET", path, query, nil, tok)
	if err != nil {
		return err
	}
	m.pending = c
	if !wait(ctx, c) {
		return holdError(heldPending)
	}
	m.pending = nil
	if c.closeErr != nil {
		return m.keep(c, c.readResult(v))
	}
	err = c.readResult(v)
	switch {
	case errors.Is(err, errTransient):
		return holdError(heldTransient)
	case errors.Is(err, errAuth):
		return fmt.Errorf("%s: %w", what, err)
	}
	return err
}

// absorb records the reply of the settled pending call. A read's reply is
// dropped; a mutation's is classified into its operation and committed.
// The call is no longer pending afterward: a failed commit fences m with
// the receipt kept in m.rec, and a failed first body Close keeps the call
// in m.held. It returns the reply's error, or, after a custody or commit
// failure, one error whose causes are every original one: the reply's
// read and classification errors, the body's Close failure and the commit
// failure.
func (m *Machine) absorb() (replyErr, err error) {
	c := m.pending
	op := c.op
	if op == nil {
		m.pending = nil
		if c.closeErr != nil {
			return nil, m.keep(c)
		}
		return nil, nil
	}
	if err := m.crash("receipt"); err != nil {
		return nil, err
	}
	replyErr = m.classify(op, c)
	if op.State == opRefused {
		m.finish(op)
	}
	m.pending = nil
	var kept error
	if c.closeErr != nil {
		kept = m.keep(c, replyErr)
	}
	serr := m.save()
	switch {
	case kept != nil && serr != nil:
		return nil, causes("gce: "+c.what+": reply not committed and first close of its response body failed; the machine keeps the response and its lock", c, kept, serr)
	case kept != nil:
		return nil, kept
	case serr != nil:
		return nil, causes("gce: "+c.what+": reply not committed", nil, c.err, replyErr, serr)
	}
	return replyErr, nil
}

// classify applies the reply of op's original to op.
func (m *Machine) classify(op *operation, c *call) error {
	op.HTTPStatus = c.status
	switch {
	case c.err != nil:
		op.State = opLost
		return nil
	case c.status == 200:
		var w operationWire
		if err := decodeStrict(c.body, &w, false); err != nil {
			op.State = opLost
			return wireErrorf(op.Kind, "malformed operation reply: %v", err)
		}
		if err := m.correlate(op, &w); err != nil {
			op.State = opLost
			return err
		}
		op.Name, op.ID, op.State = w.Name, w.ID, opRunning
		return nil
	case c.status == 409:
		op.State = opRefused
		return holdError(heldConflict)
	case c.status >= 400 && c.status < 500 && c.status != 408 && c.status != 429:
		op.State = opRefused
		return fmt.Errorf("%s %w with HTTP status %d", op.Kind, errRefused, c.status)
	default:
		op.State = opLost
		return nil
	}
}

// crash runs the test crash cut at point.
func (m *Machine) crash(point string) error {
	if m.cut == nil {
		return nil
	}
	return m.cut(point)
}

// finish retires op into the bounded history.
func (m *Machine) finish(op *operation) {
	r := m.rec
	r.History = append(r.History, *op)
	if n := len(r.History); n > maxHistory {
		r.History = append([]operation(nil), r.History[n-maxHistory:]...)
	}
	r.Op = nil
}

// lastDone reports whether the most recent retired operation succeeded.
// A power operation only suppresses another send in the same intent;
// an old successful start or stop cannot satisfy a fresh explicit goal.
func (m *Machine) lastDone(kind string) bool {
	h := m.rec.History
	if len(h) == 0 {
		return false
	}
	o := &h[len(h)-1]
	return o.Kind == kind && o.State == opDone &&
		(kind != kindStart && kind != kindStop || o.Intent == m.rec.Intent)
}

// deleted reports whether this machine's own delete of kind finished for
// the resource with id.
func (m *Machine) deleted(kind, id string) bool {
	for _, o := range m.rec.History {
		if o.Kind == kind && o.State == opDone && o.TargetID == id {
			return true
		}
	}
	return false
}

// advance moves the unresolved mutation op one step.
func (m *Machine) advance(ctx context.Context, tok *oauth2.Token, op *operation) (outcome, error) {
	switch op.State {
	case opPrepared:
		if m.rec.Desired == Absent && op.Kind != kindInstanceDelete && op.Kind != kindDiskDelete {
			// Never sent, so withdrawing it has no effect to account for.
			switch op.Kind {
			case kindDiskInsert:
				m.rec.DiskSent = false
			case kindInstanceInsert:
				m.rec.InstanceSent = false
			}
			op.State = opWithdrawn
			m.finish(op)
			return stepNext, m.save()
		}
		if op.Kind != kindInstanceDelete && op.Kind != kindDiskDelete && m.rec.Admission == nil {
			return 0, holdError(heldUnadmitted) // only exact-resource teardown is allowed
		}
		if err := m.admit(ctx, tok); err != nil {
			return 0, err
		}
		op.State, op.SentAt = opSent, m.now().UTC()
		if err := m.save(); err != nil {
			return 0, err
		}
		if err := m.crash("sent"); err != nil {
			return 0, err
		}
		c, err := m.client.start(op.Kind, op.Method, op.Path, op.Query, op.Body, tok)
		if err != nil {
			return 0, err // nothing dispatched; op stays sent and is recovered, never resent
		}
		c.op = op
		m.pending = c
		if !wait(ctx, c) {
			return 0, holdError(heldPending)
		}
		replyErr, err := m.absorb()
		if err != nil {
			return 0, err
		}
		return stepNext, replyErr
	case opSent:
		// A previous process recorded the send but not its reply.
		op.State = opLost
		return stepNext, m.save()
	case opLost:
		return m.recoverOp(ctx, tok, op)
	case opRunning:
		return m.poll(ctx, tok, op)
	}
	return 0, fmt.Errorf("operation in state %q cannot advance", op.State)
}

// recoverOp looks for op's original operation in at most maxListPages
// pages of the configured zone's operations. Only a complete,
// unambiguous, fully correlated match resolves it: an enumeration that
// ends with another page holds even if it found a match, since an
// unvisited page may hold a conflicting original. Nothing is resent.
func (m *Machine) recoverOp(ctx context.Context, tok *oauth2.Token, op *operation) (outcome, error) {
	var match *operationWire
	complete := false
	page := ""
	for i := 0; i < maxListPages; i++ {
		var l operationListWire
		if err := m.get(ctx, tok, "zoneOperations.list", m.zonePath()+"/operations", listQuery(page), &l); err != nil {
			return 0, err
		}
		for j := range l.Items {
			w := l.Items[j]
			if w.ClientOperationID != op.RequestID {
				continue
			}
			if match != nil {
				return 0, errors.New("two operations carry one request id")
			}
			if err := m.correlate(op, &w); err != nil {
				return 0, err
			}
			match = &w
		}
		if l.NextPageToken == "" {
			complete = true
			break
		}
		page = l.NextPageToken
	}
	switch {
	case !complete:
		return 0, holdError(heldTruncated)
	case match == nil:
		return 0, holdError(heldUnresolved)
	}
	op.Name, op.ID, op.State = match.Name, match.ID, opRunning
	return stepNext, m.save()
}

// poll observes op's operation and retires it once DONE.
func (m *Machine) poll(ctx context.Context, tok *oauth2.Token, op *operation) (outcome, error) {
	var w operationWire
	err := m.get(ctx, tok, "zoneOperations.get", m.zonePath()+"/operations/"+op.Name, "", &w)
	if errors.Is(err, errNotFound) {
		return 0, holdError(heldOpMissing)
	}
	if err != nil {
		return 0, err
	}
	if err := m.correlate(op, &w); err != nil {
		return 0, err
	}
	switch w.Status {
	case "PENDING", "RUNNING":
		return stepWait, nil
	case "DONE":
	default:
		return 0, fmt.Errorf("operation status %q is not pinned", w.Status)
	}
	if w.Error != nil || w.HTTPErrorStatusCode >= 400 {
		op.State, op.Errors = opFailed, w.Error.codes()
		if op.Errors == "" {
			op.Errors = "HTTP-" + strconv.Itoa(w.HTTPErrorStatusCode)
		}
		m.finish(op)
		if err := m.save(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("%s operation failed: %s", op.Kind, op.Errors)
	}
	if checkID("target id", w.TargetID) != nil {
		return 0, errors.New("done operation has no target id")
	}
	op.TargetID, op.State = w.TargetID, opDone
	switch op.Kind {
	case kindDiskInsert:
		m.rec.DiskID, m.rec.DiskCreate = w.TargetID, m.rec.witness(op)
	case kindInstanceInsert:
		m.rec.InstanceID, m.rec.InstanceCreate = w.TargetID, m.rec.witness(op)
	}
	m.finish(op)
	return stepNext, m.save()
}

// correlate reports whether w is the operation of op's original request.
func (m *Machine) correlate(op *operation, w *operationWire) error {
	want := map[string]string{
		kindDiskInsert: "insert", kindInstanceInsert: "insert",
		kindStop: "stop", kindStart: "start",
		kindInstanceDelete: "delete", kindDiskDelete: "delete",
	}[op.Kind]
	var known string
	switch op.Kind {
	case kindStop, kindStart, kindInstanceDelete:
		known = m.rec.InstanceID
	case kindDiskDelete:
		known = m.rec.DiskID
	}
	switch {
	case !opNameRE.MatchString(w.Name):
		return errors.New("operation has no valid name")
	case checkID("operation id", w.ID) != nil:
		return errors.New("operation has no valid id")
	case op.Name != "" && w.Name != op.Name, op.ID != "" && w.ID != op.ID:
		return errors.New("operation identity differs from the recorded original")
	case resourcePath(w.Zone) != m.zonePath():
		return errors.New("operation zone differs from the configured zone")
	case w.OperationType != want:
		return fmt.Errorf("operation type %q is not %q", w.OperationType, want)
	case resourcePath(w.TargetLink) != op.Target:
		return errors.New("operation target differs from the original target")
	case w.ClientOperationID != op.RequestID:
		return errors.New("operation client id differs from the original request id")
	case w.TargetID != "" && known != "" && w.TargetID != known:
		return errors.New("operation target id differs from the owned resource id")
	}
	return nil
}

// plan observes the owned resources and returns the next step toward the
// goal, preparing at most one mutation.
func (m *Machine) plan(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	if r.Desired == Absent {
		return m.teardown(ctx, tok)
	}
	if r.Phase != phaseNew && r.Admission == nil {
		// Only pin admits; a record without its witness never qualifies
		// and may only tear down what it owns.
		return 0, holdError(heldUnadmitted)
	}
	switch r.Phase {
	case phaseImageChanged:
		return 0, holdError(heldImageChanged)
	case phaseCreated:
		return m.converge(ctx, tok)
	}
	if r.Desired != Running {
		return 0, holdError(heldNotCreated)
	}
	switch r.Phase {
	case phaseNew:
		return m.pin(ctx, tok)
	case phaseImagePinned:
		return m.createDisk(ctx, tok)
	}
	return m.createInstance(ctx, tok)
}

// pin verifies the project, then admits the exact image and machine type:
// one images.get and one machineTypes.get of exactly the configured
// resources must report their exact identities and architecture ARM64.
// The admission witness is committed with the pinned image before any
// mutation can be prepared; any refusal sends nothing.
func (m *Machine) pin(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	cfg := &r.Config
	var p projectWire
	if err := m.get(ctx, tok, "projects.get", "projects/"+cfg.Project, "", &p); err != nil {
		return 0, notFoundErr(err, "project")
	}
	if p.Name != cfg.Project || !sameID(p.ID, cfg.ComputeProjectID) {
		return 0, errors.New("project name or compute project id differs from the config")
	}
	if v, n := p.CommonInstanceMetadata.value("enable-oslogin"); n > 1 || n == 1 && !strings.EqualFold(v, "false") {
		return 0, errors.New("project enables OS Login; this metadata-key profile refuses it")
	}
	var im imageWire
	if err := m.get(ctx, tok, "images.get", r.imagePath(), "", &im); err != nil {
		return 0, notFoundErr(err, "image")
	}
	switch {
	case im.Name != cfg.Image.Name || !sameID(im.ID, cfg.Image.ID) || resourcePath(im.SelfLink) != r.imagePath():
		return 0, errors.New("image name, id or link differs from the pinned image")
	case im.Status != "READY":
		return 0, errors.New("image status is not READY")
	case im.Architecture != archARM64:
		return 0, errors.New("image architecture is not ARM64")
	}
	var mt machineTypeWire
	if err := m.get(ctx, tok, "machineTypes.get", r.machineTypePath(), "", &mt); err != nil {
		return 0, notFoundErr(err, "machine type")
	}
	switch {
	case mt.Name != r.typeName() || mt.Zone != cfg.Zone || resourcePath(mt.SelfLink) != r.machineTypePath() || checkID("machine type id", mt.ID) != nil:
		return 0, errors.New("machine type name, zone, link or id differs from the configured machine type")
	case mt.Architecture != archARM64:
		return 0, errors.New("machine type architecture is not ARM64")
	}
	r.Admission = &admission{
		Version:    admissionVersion,
		ConfigHash: r.ConfigHash,
		ImageID:    im.ID,
		ImageName:  im.Name,
		ImageLink:  r.imagePath(),
		ImageArch:  im.Architecture,
		TypeID:     mt.ID,
		TypeName:   mt.Name,
		TypeZone:   mt.Zone,
		TypeLink:   r.machineTypePath(),
		TypeArch:   mt.Architecture,
	}
	r.ImageID, r.Phase = cfg.Image.ID, phaseImagePinned
	return stepNext, m.save()
}

func notFoundErr(err error, what string) error {
	if errors.Is(err, errNotFound) {
		return fmt.Errorf("%s not found", what)
	}
	return err
}

// createDisk sends the explicit owned disk insert, then verifies that disk
// holds the pinned image before any VM may reference it.
func (m *Machine) createDisk(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	if !r.DiskSent {
		if err := m.get(ctx, tok, "disks.get", m.diskPath(), "", new(diskWire)); !errors.Is(err, errNotFound) {
			if err == nil {
				return 0, holdError(heldNameTaken)
			}
			return 0, err
		}
		return m.prepare(kindDiskInsert)
	}
	if r.DiskID == "" {
		return 0, holdError(heldCreateFailed)
	}
	d, err := m.observeDisk(ctx, tok)
	if err != nil {
		return 0, err
	}
	if d.SourceImageID != r.ImageID {
		r.Phase = phaseImageChanged
		if err := m.save(); err != nil {
			return 0, err
		}
		return 0, holdError(heldImageChanged)
	}
	switch d.Status {
	case "CREATING", "RESTORING":
		return stepWait, nil
	case "READY":
	default:
		return 0, holdError(heldUnknownState)
	}
	if len(d.Users) != 0 {
		return 0, holdError(heldDrift)
	}
	r.Phase = phaseDiskVerified
	return stepNext, m.save()
}

// createInstance sends the VM insert that attaches only the verified,
// unused owned disk, then verifies the created VM.
func (m *Machine) createInstance(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	if !r.InstanceSent {
		d, err := m.observeDisk(ctx, tok)
		if err != nil {
			return 0, err
		}
		if d.SourceImageID != r.ImageID || d.Status != "READY" || len(d.Users) != 0 {
			return 0, holdError(heldDrift)
		}
		if err := m.get(ctx, tok, "instances.get", m.instancePath(), "", new(instanceWire)); !errors.Is(err, errNotFound) {
			if err == nil {
				return 0, holdError(heldNameTaken)
			}
			return 0, err
		}
		return m.prepare(kindInstanceInsert)
	}
	if r.InstanceID == "" {
		return 0, holdError(heldCreateFailed)
	}
	if _, err := m.observeInstance(ctx, tok); err != nil {
		return 0, err
	}
	r.Phase = phaseCreated
	return stepNext, m.save()
}

// converge drives the created VM to Running, Stopped or a restart.
func (m *Machine) converge(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	vm, err := m.observeInstance(ctx, tok)
	if err != nil {
		return 0, err
	}
	goal := r.Desired
	if goal == Restarted {
		goal = Running
		if r.Restart == "stop" {
			goal = Stopped
		}
	}
	// A fresh observation can require new work after this intent was
	// already completed, including after a guest shutdown. Start a new
	// unfinished intent before lastDone or prepare can consider that work.
	// Unknown and transitional states, and Refresh, do not begin one.
	needsChange := vm.Status == "RUNNING" && goal == Stopped || vm.Status == "TERMINATED" && goal == Running
	if needsChange && r.CompletedIntent == r.Intent {
		if err := m.beginIntent(r.Desired, r.Restart); err != nil {
			return 0, err
		}
	}
	switch vm.Status {
	case "PROVISIONING", "STAGING", "STOPPING":
		return stepWait, nil
	case "RUNNING":
		if goal == Running {
			if r.Desired == Restarted && r.Restart == "start" {
				r.Restart = "done"
				if err := m.save(); err != nil {
					return 0, err
				}
			}
			return stepReached, nil
		}
		if m.lastDone(kindStop) {
			return stepWait, nil // DONE is not the observed state
		}
		return m.prepare(kindStop)
	case "TERMINATED":
		if goal == Stopped {
			if r.Desired == Restarted {
				r.Restart = "start"
				return stepNext, m.save()
			}
			return stepReached, nil
		}
		if m.lastDone(kindStart) {
			return stepWait, nil
		}
		return m.prepare(kindStart)
	}
	return 0, holdError(heldUnknownState)
}

// teardown deletes the owned VM, verifies it gone, then deletes the
// unused owned disk and verifies it gone.
func (m *Machine) teardown(ctx context.Context, tok *oauth2.Token) (outcome, error) {
	r := m.rec
	if r.InstanceSent {
		var vm instanceWire
		err := m.get(ctx, tok, "instances.get", m.instancePath(), "", &vm)
		switch {
		case errors.Is(err, errNotFound):
			s, err := m.absence(kindInstanceDelete, r.InstanceID)
			if err != nil {
				return 0, err
			}
			r.InstanceState, r.ObservedAt = s, m.now().UTC()
		case err != nil:
			return 0, err
		default:
			if r.InstanceID == "" || m.checkInstance(&vm) != nil {
				return 0, holdError(heldDrift)
			}
			r.InstanceState, r.ObservedAt = vm.Status, m.now().UTC()
			if m.lastDone(kindInstanceDelete) {
				return stepWait, nil
			}
			return m.prepare(kindInstanceDelete)
		}
	}
	if r.DiskSent {
		var d diskWire
		err := m.get(ctx, tok, "disks.get", m.diskPath(), "", &d)
		switch {
		case errors.Is(err, errNotFound):
			s, err := m.absence(kindDiskDelete, r.DiskID)
			if err != nil {
				return 0, err
			}
			r.DiskState, r.ObservedAt = s, m.now().UTC()
		case err != nil:
			return 0, err
		default:
			if r.DiskID == "" || m.checkDisk(&d) != nil {
				return 0, holdError(heldDrift)
			}
			r.DiskState, r.ObservedAt = d.Status, m.now().UTC()
			if len(d.Users) != 0 {
				return 0, holdError(heldDiskInUse)
			}
			if m.lastDone(kindDiskDelete) {
				return stepWait, nil
			}
			return m.prepare(kindDiskDelete)
		}
	}
	return stepReached, nil
}

// absence returns the state to record for an owned resource whose fresh
// GET found nothing. It is evidence of absence only for a resource whose
// numeric id was pinned by a settled successful create, as its durable
// create witness records, with no unresolved mutation,
// pending original or custody fault: ABSENT after this machine's own
// delete of that id finished, REMOVED-EXTERNALLY otherwise. A lost,
// failed or unpinned create holds; it is never inferred gone. Treating a
// pinned name's 404 as absence relies on the operator-controlled project
// and zone and reserved names that no other writer reuses; Compute Engine
// offers no id precondition that would prove it.
func (m *Machine) absence(deleteKind, id string) (string, error) {
	w := m.rec.DiskCreate
	if deleteKind == kindInstanceDelete {
		w = m.rec.InstanceCreate
	}
	if id == "" || w == nil || w.TargetID != id || m.rec.Op != nil || m.pending != nil || m.held != nil || m.fault != nil {
		return "", holdError(heldUnknownState)
	}
	if m.deleted(deleteKind, id) {
		return stateAbsent, nil
	}
	return stateRemoved, nil
}

// prepare commits the mutation of kind with its own fresh request id
// before it can be sent; the next step sends it.
func (m *Machine) prepare(kind string) (outcome, error) {
	r := m.rec
	method, path, body, target, ok := r.request(kind)
	if !ok {
		return 0, fmt.Errorf("unknown operation kind %q", kind)
	}
	id, err := newUUID()
	if err != nil {
		return 0, err
	}
	r.Reached = false
	r.Op = &operation{
		Kind:       kind,
		Intent:     r.Intent,
		RequestID:  id,
		Method:     method,
		Path:       path,
		Query:      "requestId=" + id,
		Body:       body,
		Target:     target,
		State:      opPrepared,
		PreparedAt: m.now().UTC(),
	}
	switch kind {
	case kindDiskInsert:
		r.DiskSent = true
	case kindInstanceInsert:
		r.InstanceSent = true
	}
	if err := m.save(); err != nil {
		return 0, err
	}
	return stepNext, m.crash("prepared")
}

// refresh records any pending original and the evidence of an unresolved
// operation, then observes the owned resources. It sends only GETs.
func (m *Machine) refresh(ctx context.Context, tok *oauth2.Token) (bool, error) {
	if err := m.drain(ctx); err != nil {
		return false, err
	}
	r := m.rec
	if op := r.Op; op != nil {
		if op.State == opPrepared {
			return false, holdError(heldPrepared)
		}
		if op.State == opSent {
			// A previous process recorded the send but not its reply.
			op.State = opLost
			if err := m.save(); err != nil {
				return false, err
			}
		}
		if op.State == opLost {
			if _, err := m.recoverOp(ctx, tok, op); err != nil {
				return false, err
			}
		}
		if op.State == opRunning {
			if _, err := m.poll(ctx, tok, op); err != nil {
				return false, err
			}
		}
		if r.Op != nil {
			return false, holdError(heldUnresolved)
		}
	}
	return m.observe(ctx, tok)
}

// observe reads the owned resources, records what it saw and reports
// whether the goal is observed. It prepares nothing.
func (m *Machine) observe(ctx context.Context, tok *oauth2.Token) (bool, error) {
	r := m.rec
	if !r.DiskSent {
		if r.Desired == Absent {
			return true, nil // nothing was ever sent
		}
		return false, holdError(heldNotCreated)
	}
	vmGone := true
	if r.InstanceSent {
		var vm instanceWire
		err := m.get(ctx, tok, "instances.get", m.instancePath(), "", &vm)
		switch {
		case errors.Is(err, errNotFound):
			s, err := m.absence(kindInstanceDelete, r.InstanceID)
			if err != nil {
				return false, err
			}
			r.InstanceState, r.ObservedAt = s, m.now().UTC()
		case err != nil:
			return false, err
		case r.InstanceID == "":
			return false, holdError(heldUnknownState)
		case m.checkInstance(&vm) != nil:
			return false, holdError(heldDrift)
		default:
			vmGone = false
			r.InstanceState, r.ObservedAt = vm.Status, m.now().UTC()
		}
	}
	diskGone := false
	var d diskWire
	err := m.get(ctx, tok, "disks.get", m.diskPath(), "", &d)
	switch {
	case errors.Is(err, errNotFound):
		s, err := m.absence(kindDiskDelete, r.DiskID)
		if err != nil {
			return false, err
		}
		r.DiskState, r.ObservedAt = s, m.now().UTC()
		diskGone = true
	case err != nil:
		return false, err
	case r.DiskID == "":
		return false, holdError(heldUnknownState)
	case m.checkDisk(&d) != nil, vmGone && len(d.Users) != 0, !vmGone && len(d.Users) != 1:
		return false, holdError(heldDrift)
	default:
		r.DiskState, r.ObservedAt = d.Status, m.now().UTC()
	}
	// Absent is observed by identity alone, so an owned disk that holds
	// another image is still seen and torn down. Any other goal qualifies
	// only an admitted record whose disk holds exactly the pinned image.
	var reached bool
	switch {
	case r.Desired == Absent:
		reached = vmGone && diskGone
	case r.Admission == nil:
		return false, holdError(heldUnadmitted)
	case r.Phase == phaseImageChanged, !diskGone && d.SourceImageID != r.ImageID:
		return false, holdError(heldImageChanged)
	case r.Phase != phaseCreated:
		return false, holdError(heldNotCreated)
	case vmGone || diskGone:
		return false, holdError(heldMissing)
	case r.Desired == Running:
		reached = r.InstanceState == "RUNNING"
	case r.Desired == Stopped:
		reached = r.InstanceState == "TERMINATED"
	case r.Desired == Restarted:
		reached = r.Restart == "done" && r.InstanceState == "RUNNING"
	}
	if !reached {
		return false, holdError(heldNotApplied)
	}
	return true, nil
}

// observeDisk reads and verifies the owned disk.
func (m *Machine) observeDisk(ctx context.Context, tok *oauth2.Token) (*diskWire, error) {
	var d diskWire
	if err := m.get(ctx, tok, "disks.get", m.diskPath(), "", &d); err != nil {
		if errors.Is(err, errNotFound) {
			return nil, holdError(heldMissing)
		}
		return nil, err
	}
	if err := m.checkDisk(&d); err != nil {
		return nil, holdError(heldDrift)
	}
	m.rec.DiskState, m.rec.ObservedAt = d.Status, m.now().UTC()
	return &d, nil
}

// checkDisk reports whether d is exactly the owned disk. It checks only
// that d names some image, not which: teardown must still delete an owned
// disk that holds another image. Every qualifying use compares
// SourceImageID with the pinned image itself.
func (m *Machine) checkDisk(d *diskWire) error {
	r, cfg := m.rec, &m.rec.Config
	switch {
	case !sameID(d.ID, r.DiskID):
		return errors.New("disk id differs")
	case d.Name != r.DiskName || resourcePath(d.Zone) != m.zonePath():
		return errors.New("disk name or zone differs")
	case resourcePath(d.Type) != "projects/"+cfg.Project+"/"+cfg.DiskType:
		return errors.New("disk type differs")
	case d.SizeGb != strconv.FormatInt(cfg.DiskGiB, 10):
		return errors.New("disk size differs")
	case d.Labels[ownerLabel] != r.Install:
		return errors.New("disk owner label differs")
	case checkID("source image id", d.SourceImageID) != nil:
		return errors.New("disk has no source image id")
	}
	for _, u := range d.Users {
		if resourcePath(u) != m.instancePath() {
			return errors.New("disk is used by another instance")
		}
	}
	return nil
}

// observeInstance reads and verifies the owned VM and its owned disk.
func (m *Machine) observeInstance(ctx context.Context, tok *oauth2.Token) (*instanceWire, error) {
	var vm instanceWire
	if err := m.get(ctx, tok, "instances.get", m.instancePath(), "", &vm); err != nil {
		if errors.Is(err, errNotFound) {
			return nil, holdError(heldMissing)
		}
		return nil, err
	}
	if err := m.checkInstance(&vm); err != nil {
		return nil, holdError(heldDrift)
	}
	d, err := m.observeDisk(ctx, tok)
	if err != nil {
		return nil, err
	}
	if len(d.Users) != 1 {
		return nil, holdError(heldDrift)
	}
	if d.SourceImageID != m.rec.ImageID {
		return nil, holdError(heldImageChanged)
	}
	m.rec.InstanceState, m.rec.ObservedAt = vm.Status, m.now().UTC()
	return &vm, nil
}

// checkInstance reports whether vm is exactly the owned VM with the
// approved spec: one boot disk that is the owned disk with autoDelete
// false, no service account, no external address and only the approved
// metadata.
func (m *Machine) checkInstance(vm *instanceWire) error {
	r, cfg := m.rec, &m.rec.Config
	switch {
	case !sameID(vm.ID, r.InstanceID):
		return errors.New("instance id differs")
	case vm.Name != r.InstanceName || resourcePath(vm.Zone) != m.zonePath():
		return errors.New("instance name or zone differs")
	case resourcePath(vm.MachineType) != "projects/"+cfg.Project+"/"+cfg.MachineType:
		return errors.New("machine type differs")
	case len(vm.Disks) != 1 || !vm.Disks[0].Boot || vm.Disks[0].AutoDelete || resourcePath(vm.Disks[0].Source) != m.diskPath():
		return errors.New("boot disk differs")
	case len(vm.ServiceAccounts) != 0:
		return errors.New("instance has a service account")
	case len(vm.NetworkInterfaces) != 1 || resourcePath(vm.NetworkInterfaces[0].Subnetwork) != cfg.Subnet || len(vm.NetworkInterfaces[0].AccessConfigs) != 0:
		return errors.New("network interface differs")
	case vm.Labels[ownerLabel] != r.Install:
		return errors.New("instance owner label differs")
	case len(vm.Metadata.Items) != len(r.metadata()):
		return errors.New("instance metadata differs")
	}
	for _, it := range r.metadata() {
		if v, n := vm.Metadata.value(it.Key); n != 1 || v != it.Value {
			return errors.New("instance metadata differs")
		}
	}
	return nil
}

func (m *Machine) zonePath() string     { return m.rec.zonePath() }
func (m *Machine) diskPath() string     { return m.rec.diskPath() }
func (m *Machine) instancePath() string { return m.rec.instancePath() }
