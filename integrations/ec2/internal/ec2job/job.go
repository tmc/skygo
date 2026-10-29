package ec2job

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/proto"
)

// A VM is the verified identity of the job's VM: its profile, owner
// identity, ClientToken and instance, the operator-bound image, type and
// architecture, and the address selected for it. A journal binds it before
// any effect, and every later command must verify the same VM again.
type VM struct {
	Account      string
	Region       string
	ProfileHash  string
	Owner        string
	Environment  string
	Generation   uint64
	Lease        string
	ClientToken  string
	InstanceID   string
	ImageID      string
	InstanceType string
	Architecture string
	Public       bool
	Address      netip.Addr
}

func (v VM) check() error {
	switch {
	case v.Account == "" || v.Region == "" || !hex64.MatchString(v.ProfileHash):
		return errors.New("invalid profile")
	case v.Owner == "" || v.Environment == "" || v.ClientToken == "":
		return errors.New("invalid identity")
	case !instanceIDRE.MatchString(v.InstanceID) || !imageIDRE.MatchString(v.ImageID) || !typeRE.MatchString(v.InstanceType):
		return errors.New("invalid instance")
	case v.Architecture != Architecture:
		return errors.New("invalid architecture")
	case !v.Address.IsValid() || !v.Address.IsGlobalUnicast() || v.Public && v.Address.IsPrivate():
		return errors.New("invalid address")
	}
	return nil
}

// VerifyVM verifies, without changing anything, that the VM the lifecycle
// state directory stateDir holds for vmCfg is the one cfg names and is
// running now, and returns its identity and selected address.
//
// Before any EC2 call it checks that cfg binds vmCfg's image and instance
// type. It then opens the state directory as [ec2ctl.Open] does, holding
// its lock only while it checks the stored record (a sent create, the
// configured instance, wanting present, running, no restart or hold) and
// describes the owned instance afresh with [ec2ctl.Observe], which
// verifies the instance's account, token, tags, image, type, subnet, key
// and groups. The description must report the instance running at
// exactly cfg's address of cfg's kind. It releases the lock before it
// returns; a failed release is returned and nothing may act on the VM.
//
// VerifyVM never creates, starts, stops or replaces a VM, and a stored
// running phase is not taken as fresh evidence.
func VerifyVM(ctx context.Context, stateDir string, vmCfg *ec2ctl.Config, c *ec2api.Client, cfg *Config) (VM, error) {
	switch {
	case vmCfg == nil || cfg == nil || c == nil:
		return VM{}, errors.New("ec2job: VerifyVM needs both configurations and a client")
	case vmCfg.Profile.ImageID != cfg.ImageID:
		return VM{}, fmt.Errorf("%w: the job configuration binds another image", ErrMismatch)
	case vmCfg.Profile.InstanceType != cfg.InstanceType:
		return VM{}, fmt.Errorf("%w: the job configuration binds another instance type", ErrMismatch)
	}
	m, err := ec2ctl.Open(stateDir, vmCfg)
	if err != nil {
		return VM{}, err
	}
	vm, err := verifyRecord(ctx, m.Status(), vmCfg, c, cfg)
	if cerr := m.Close(); cerr != nil {
		return VM{}, errors.Join(err, fmt.Errorf("ec2job: release VM state lock: %w", cerr))
	}
	return vm, err
}

func verifyRecord(ctx context.Context, st ec2ctl.Status, vmCfg *ec2ctl.Config, c *ec2api.Client, cfg *Config) (VM, error) {
	r := st.Record
	switch {
	case st.Pending || st.Restart:
		return VM{}, errors.New("ec2job: the VM record is not settled")
	case !r.CreateSent || r.InstanceID == "":
		return VM{}, errors.New("ec2job: the VM record names no instance; job commands never create one")
	case r.InstanceID != cfg.InstanceID:
		return VM{}, fmt.Errorf("%w: the VM record names another instance", ErrMismatch)
	case r.Desired != ec2vm.DesiredPresent || r.Phase != ec2vm.PhaseRunning:
		return VM{}, fmt.Errorf("ec2job: the VM wants %v and is %v; job commands never start one", r.Desired, r.Phase)
	}
	obs, err := ec2ctl.Observe(ctx, c, r)
	if err != nil {
		return VM{}, err
	}
	if obs.InstanceID != cfg.InstanceID {
		return VM{}, fmt.Errorf("%w: observed another instance", ErrMismatch)
	}
	if obs.State != "running" {
		return VM{}, fmt.Errorf("ec2job: instance %s is %s, not running; job commands never start one", obs.InstanceID, obs.State)
	}
	addr, err := obs.Address(cfg.Public)
	if err != nil {
		return VM{}, err
	}
	if addr != cfg.IP {
		return VM{}, fmt.Errorf("%w: the instance's address is not the configured address", ErrMismatch)
	}
	return VM{
		Account:      vmCfg.Profile.Account,
		Region:       vmCfg.Profile.Region,
		ProfileHash:  r.ProfileHash,
		Owner:        r.Identity.Owner,
		Environment:  r.Identity.Environment,
		Generation:   r.Identity.Generation,
		Lease:        r.Identity.Lease,
		ClientToken:  r.Token,
		InstanceID:   r.InstanceID,
		ImageID:      vmCfg.Profile.ImageID,
		InstanceType: vmCfg.Profile.InstanceType,
		Architecture: cfg.Architecture,
		Public:       cfg.Public,
		Address:      addr,
	}, nil
}

// A Session is what a Job uses of an [sshagent.Session]: its fixed
// authenticated control client and the facts Open verified. A Job never
// closes a Session; its opener does, once, after the Job's last call.
type Session interface {
	Control() skyv1connect.ControlServiceClient
	Binding() sshagent.Binding
}

// A Job is the journal of one finite job on one VM, opened for writing. It
// holds the job directory's lock until Close. Only [Create] and [Open]
// make a usable Job; every method of a nil or zero Job but Status,
// NeedsRemote and Known reports [os.ErrClosed].
//
// Every effect is preceded by a durable save of what it may do, and every
// identity an effect returns is saved before the next effect. If a save
// fails, the Job keeps the state pending and makes no further call until a
// later call saves it. If the Close of a temporary journal file, or of a
// file the Job read from its directory, fails, the Job keeps that file and
// refuses every later call ([ErrUnsettled]). A Job's methods must not be
// called concurrently.
type Job struct {
	dir      string
	s        *store // kept for the Job's life, closed or not
	st       state
	pending  bool
	closed   bool
	closeErr error // the first Close's result

	cfg     *Config // set by Bind
	session Session // set by Attach
	now     func() time.Time

	read *LogPage // the last log copy that ended cleanly, until committed

	// unsent is the download request whose intent this Job tried to store
	// and, because the save failed, never sent. It is never in a journal:
	// it is this Job's own knowledge that no request was sent, and it is
	// cleared just before the request is sent.
	unsent frame
}

// Create creates the job directory dir, which must not exist, and stores
// the frozen workdir snapshot and a journal binding cfg, the verified vm,
// task (from [Config.Task]) and a new request ID, wanting the job run and
// with nothing sent. snapshot is the archive [Snapshot] returned; its
// digest and size must be those task was checked with.
//
// A directory left by a failed Create holds no journal and is refused,
// never deleted: it may hold a frozen snapshot. If a failed Create's own
// Close of a file fails, its error keeps that file, and the directory's
// lock while a temporary file's state is unknown ([ErrUnsettled]).
func Create(dir string, cfg *Config, vm VM, task *v1.Task, snapshot []byte) (*Job, error) {
	if cfg == nil || task == nil {
		return nil, errors.New("ec2job: Create needs a configuration and a task")
	}
	if err := vm.check(); err != nil {
		return nil, fmt.Errorf("ec2job: VM: %w", err)
	}
	if err := bindVM(cfg, vm); err != nil {
		return nil, err
	}
	if task.Workdir != nil {
		return nil, errors.New("ec2job: the task's workdir is the uploaded snapshot")
	}
	size := int64(len(snapshot))
	digest, err := artifact.Validate(bytes.NewReader(snapshot), size)
	if err != nil {
		return nil, fmt.Errorf("ec2job: workdir snapshot: %w", err)
	}
	b, err := encodeTask(task)
	if err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("ec2job: request ID: %w", err)
	}
	st := state{
		Created:      time.Now().UTC(),
		ConfigDigest: cfg.Digest,
		VM:           vm,
		Task:         b,
		RequestID:    hex.EncodeToString(id[:]),
		Snapshot:     artifact.Ref{SHA256: digest, Size: size},
		Desired:      desiredRun,
		Upload:       uploadNone,
		Submit:       submitNone,
		Cancel:       cancelNone,
	}
	if err := st.check(); err != nil {
		return nil, fmt.Errorf("ec2job: %w", err)
	}
	s, err := openStore(dir, true)
	if err != nil {
		return nil, err
	}
	if err := s.freeze(workdirName, snapshot); err != nil {
		return nil, errors.Join(err, s.close())
	}
	j := &Job{dir: s.dir, s: s, now: time.Now}
	if err := j.commit(st); err != nil {
		return nil, errors.Join(err, s.close())
	}
	return j, nil
}

// Open opens the existing job directory dir. It refuses a journal of
// another format or version, or one that is corrupt. A version 1 or 2
// journal is read as written and stored as version 3 at its next save.
//
// Open first reads the journal without the lock and refuses one it
// cannot read before it creates the lock file or removes anything. It
// then takes the lock, reads and checks the journal again, and only then
// removes the temporary files this format's writers leave. The first
// read is not a lock: it assumes that every writer of the directory
// holds its lock, as every writer in this package does.
func Open(dir string) (*Job, error) {
	if err := preflight(dir); err != nil {
		return nil, err
	}
	s, err := openStore(dir, false)
	if err != nil {
		return nil, err
	}
	data, err := s.load()
	if err == nil {
		var st state
		if st, err = decodeState(data); err == nil {
			if err = s.clean(); err == nil {
				return &Job{dir: s.dir, s: s, st: st, now: time.Now}, nil
			}
		}
	}
	return nil, errors.Join(err, s.close())
}

// Close releases the job directory's lock, once. Every call returns the
// first Close's result: a failed Close is never retried, the Job stays
// closed and keeps its original files. A state not durably stored is
// lost. A Job one of whose original files' Close failed keeps the lock
// and returns [ErrUnsettled]; only the process's exit releases it. Close
// of a nil or zero Job returns [os.ErrClosed].
func (j *Job) Close() error {
	if j == nil || j.s == nil {
		return os.ErrClosed
	}
	if !j.closed {
		j.closed = true
		j.closeErr = j.s.close()
	}
	return j.closeErr
}

// usable reports whether j is open and its journal's files settled.
func (j *Job) usable() error {
	if j == nil || j.s == nil || j.closed {
		return os.ErrClosed
	}
	return j.s.settled()
}

// Bind checks that cfg and the freshly verified vm are the ones the
// journal binds. It makes no call and changes nothing.
func (j *Job) Bind(cfg *Config, vm VM) error {
	if err := j.usable(); err != nil {
		return err
	}
	if cfg == nil || cfg.Digest != j.st.ConfigDigest {
		return fmt.Errorf("%w: job configuration changed", ErrMismatch)
	}
	if err := bindVM(cfg, vm); err != nil {
		return err
	}
	if vm != j.st.VM {
		return fmt.Errorf("%w: the verified VM or its address is not the journal's", ErrMismatch)
	}
	j.cfg = cfg
	return nil
}

func bindVM(cfg *Config, vm VM) error {
	if vm.InstanceID != cfg.InstanceID || vm.ImageID != cfg.ImageID || vm.InstanceType != cfg.InstanceType ||
		vm.Architecture != cfg.Architecture || vm.Public != cfg.Public || vm.Address != cfg.IP {
		return fmt.Errorf("%w: the VM is not the configured one", ErrMismatch)
	}
	return nil
}

// Attach checks that s is a session to the bound VM's address with the
// configuration's SSH keys, target and installation, and to the same
// agent source and binary as the journal's first session, and keeps s for
// later calls. The first Attach stores the agent's facts before any
// control request. Attach sends no request.
func (j *Job) Attach(s Session) error {
	if err := j.ready(false); err != nil {
		return err
	}
	if s == nil || s.Control() == nil {
		return errors.New("ec2job: no session")
	}
	b, want := s.Binding(), j.cfg.Session(j.st.VM.Address, nil)
	switch {
	case b.Address != want.Address || b.User != want.User || b.KnownHostsFile != want.KnownHostsFile ||
		b.KeyFile != want.KeyFile || b.Fingerprint != want.Fingerprint:
		return fmt.Errorf("%w: the session is not to the verified address with the configured keys", ErrMismatch)
	case b.Target != want.Target || b.Installation != want.Installation:
		return fmt.Errorf("%w: the session's target or installation is not the configured one", ErrMismatch)
	case b.Dirty && !j.cfg.AllowDirty:
		return fmt.Errorf("%w: the agent was built from uncommitted source", ErrMismatch)
	}
	facts := &AgentFacts{Commit: b.Commit, Digest: b.Digest, Dirty: b.Dirty, AgentDir: b.AgentDir, AgentAddress: b.AgentAddress}
	if j.st.Agent == nil {
		next := j.st
		next.Agent = facts
		if err := next.check(); err != nil {
			return fmt.Errorf("ec2job: session facts: %w", err)
		}
		if err := j.commit(next); err != nil {
			return err
		}
	} else if *j.st.Agent != *facts {
		return fmt.Errorf("%w: the agent's source or binary changed", ErrMismatch)
	}
	j.session = s
	return nil
}

// ready reports whether j may act: it is open, bound, its state is
// stored, and, if session is set, it has a session.
func (j *Job) ready(session bool) error {
	if err := j.usable(); err != nil {
		return err
	}
	switch {
	case j.cfg == nil:
		return errors.New("ec2job: the job is not bound to a verified VM")
	case session && j.session == nil:
		return errors.New("ec2job: the job has no session")
	}
	return j.flush()
}

// commit stores st and makes it the job's state. If the store fails, st
// is kept pending: it may or may not be durable.
func (j *Job) commit(st state) error {
	j.st = st
	data, err := encodeState(st)
	if err == nil {
		err = j.s.save(data)
	}
	j.pending = err != nil
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnpersisted, err)
	}
	return nil
}

func (j *Job) flush() error {
	if !j.pending {
		return nil
	}
	return j.commit(j.st)
}

// hold stores reason as the job's latched hold and returns ErrHeld.
func (j *Job) hold(reason string) error {
	next := j.st
	next.Hold = reason
	if err := j.commit(next); err != nil {
		return errors.Join(fmt.Errorf("%w: %s", ErrHeld, reason), err)
	}
	return fmt.Errorf("%w: %s", ErrHeld, reason)
}

// A Wait bounds how a call waits for the job to end. Poll is the interval
// between GetJob requests; zero returns after the call's own request
// without waiting. The call's context bounds the whole wait; its end
// leaves the job running and its outcome unknown, and never stops it.
type Wait struct {
	Poll time.Duration
}

// ErrNotTerminal reports a wait that ended before the job did. The job
// was not stopped; its outcome is unknown.
var ErrNotTerminal = errors.New("the job has not ended; it was not stopped and its outcome is unknown")

// Start runs the new job: it checks the endpoint and target, records the
// request domain, uploads and finalizes the workdir, submits the task once
// and, with a Wait, observes the job until it ends and collects its
// outputs. Start makes no submit whose outcome is already in doubt; that
// is Resume's, explicitly.
func (j *Job) Start(ctx context.Context, w Wait) (Status, error) {
	if err := j.ready(true); err != nil {
		return j.Status(), err
	}
	if j.st.Retention != "" || j.st.Upload != uploadNone || j.st.Submit != submitNone {
		return j.Status(), errors.New("ec2job: Start runs only a new job; this one has progress: use Resume")
	}
	return j.advance(ctx, false, w)
}

// Resume continues the job from its journal, while its desired state is
// run and it is not held. A known job is only observed, never submitted.
// A submit whose outcome is in doubt is replayed with exactly its stored
// task, request ID, method and request domain, and only while the domain
// is still the server's current lineage and within its retry window; an
// expired or changed domain is refused, never refreshed. A known upload is
// reconciled with GetArtifactUpload; an upload whose ID was lost is held.
func (j *Job) Resume(ctx context.Context, w Wait) (Status, error) {
	if err := j.ready(true); err != nil {
		return j.Status(), err
	}
	return j.advance(ctx, true, w)
}

// errCanceled reports a job whose cancel was requested.
var errCanceled = errors.New("ec2job: cancel was requested; the job is never run again")

// CheckResume reports, from the journal alone, a refusal Resume would
// make whatever the VM, session or server say: a latched hold, a
// requested cancel, an upload or download Begin or a download Close whose
// outcome is unknown (which it latches as a hold; a download request this
// Job stored but never sent is not unknown), or a submit in doubt
// whose request domain's retry window has passed. It needs neither Bind
// nor Attach, so that a caller can refuse before any VM, SSH or job
// request. Apart from storing that hold, or a state still pending, it
// changes nothing.
func (j *Job) CheckResume() error {
	if err := j.usable(); err != nil {
		return err
	}
	if err := j.flush(); err != nil {
		return err
	}
	return j.resumable()
}

func (j *Job) resumable() error {
	switch {
	case j.st.Hold != "":
		return fmt.Errorf("%w: %s", ErrHeld, j.st.Hold)
	case j.st.Desired != desiredRun:
		return errCanceled
	case j.st.Upload == uploadBeginSent:
		return j.hold(HoldUploadBegin)
	case j.st.Download == downloadBegin && !j.unsentFrame():
		return j.hold(HoldDownloadBegin)
	case j.st.Download == downloadClose && !j.unsentFrame():
		return j.hold(HoldDownloadClose)
	case j.st.Submit == submitSent && j.st.Retention == RetentionEnabled && !j.now().Before(j.st.RetryUntil):
		return ErrDomainExpired
	}
	return nil
}

func (j *Job) advance(ctx context.Context, resume bool, w Wait) (Status, error) {
	if err := j.resumable(); err != nil {
		return j.Status(), err
	}
	c := j.session.Control()
	if j.st.JobID == "" {
		if j.st.Retention == "" {
			if err := j.preflight(ctx, c); err != nil {
				return j.Status(), err
			}
			if err := j.recordRetention(ctx, c); err != nil {
				return j.Status(), err
			}
		}
		if err := j.upload(ctx, c); err != nil {
			return j.Status(), err
		}
		if err := j.submit(ctx, c, resume); err != nil {
			return j.Status(), err
		}
	}
	return j.wait(ctx, c, w)
}

// preflight requires the endpoint's artifact service and the configured
// target's finite and workdir snapshot capabilities. It only reads.
func (j *Job) preflight(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	r, err := c.GetServerInfo(ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
	if err != nil {
		return remote("GetServerInfo", err)
	}
	if r == nil || r.Msg == nil {
		return errors.New("ec2job: server returned no server information")
	}
	if !slices.Contains(r.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) {
		return errors.New("ec2job: the agent serves no artifact snapshot service; is the installation initialized?")
	}
	for _, t := range r.Msg.Targets {
		if t.GetName() == j.cfg.Target {
			if !slices.Contains(t.Capabilities, v1.Capability_CAPABILITY_FINITE_JOBS_V1) || !slices.Contains(t.Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1) {
				return errors.New("ec2job: the agent target does not run finite workspace jobs")
			}
			return nil
		}
	}
	return errors.New("ec2job: the agent does not serve the configured target")
}

// recordRetention reads the server's retention policy and durably records
// the mode and, if enabled, the current request domain, once, before the
// first mutation. A recorded domain is never refreshed.
func (j *Job) recordRetention(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	r, err := c.GetRetention(ctx, connect.NewRequest(&v1.GetRetentionRequest{}))
	next := j.st
	switch {
	case err != nil && connect.CodeOf(err) == connect.CodeUnimplemented:
		next.Retention = RetentionUnimplemented
	case err != nil:
		return remote("GetRetention", err)
	case r == nil || r.Msg == nil:
		return errors.New("ec2job: server returned no retention information")
	case !r.Msg.Enabled:
		next.Retention = RetentionDisabled
	default:
		m, now := r.Msg, j.now()
		switch {
		case m.Domain == nil || !hexID32.MatchString(m.Domain.Lineage) || m.Domain.Epoch == 0:
			return errors.New("ec2job: server returned an invalid request domain")
		case m.AcceptUntil == nil || m.AcceptUntil.CheckValid() != nil || !now.Before(m.AcceptUntil.AsTime()):
			return errors.New("ec2job: server returned no accepting request domain")
		case m.RetryUntil == nil || m.RetryUntil.CheckValid() != nil || m.RetryUntil.AsTime().Before(m.AcceptUntil.AsTime()):
			return errors.New("ec2job: server returned an invalid retry window")
		}
		next.Retention, next.Lineage, next.Epoch, next.RetryUntil = RetentionEnabled, m.Domain.Lineage, m.Domain.Epoch, m.RetryUntil.AsTime().UTC()
	}
	return j.commit(next)
}

// domain returns the recorded request domain, nil without retention.
func (j *Job) domain() *v1.RequestDomain {
	if j.st.Retention != RetentionEnabled {
		return nil
	}
	return &v1.RequestDomain{Lineage: j.st.Lineage, Epoch: j.st.Epoch}
}

// ErrDomainExpired reports a submit refused because the recorded request
// domain is past its retry window or no longer the server's lineage. The
// request is never sent with a fresh domain or ID; a new job is needed.
var ErrDomainExpired = errors.New("the recorded request domain can no longer be replayed")

// checkDomain refuses a submit outside the recorded domain's window. For
// a replay it also reads the current policy and refuses a changed mode or
// lineage. It only reads.
func (j *Job) checkDomain(ctx context.Context, c skyv1connect.ControlServiceClient, replay bool) error {
	if j.st.Retention == RetentionEnabled && !j.now().Before(j.st.RetryUntil) {
		return ErrDomainExpired
	}
	if !replay {
		return nil
	}
	r, err := c.GetRetention(ctx, connect.NewRequest(&v1.GetRetentionRequest{}))
	if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
		return remote("GetRetention", err)
	}
	enabled := err == nil && r != nil && r.Msg != nil && r.Msg.Enabled
	switch {
	case err == nil && (r == nil || r.Msg == nil):
		return errors.New("ec2job: server returned no retention information")
	case j.st.Retention == RetentionEnabled && !enabled:
		return fmt.Errorf("%w: retention is no longer enabled", ErrDomainExpired)
	case j.st.Retention == RetentionEnabled && (r.Msg.Domain == nil || r.Msg.Domain.Lineage != j.st.Lineage):
		return fmt.Errorf("%w: request lineage changed", ErrDomainExpired)
	case j.st.Retention != RetentionEnabled && enabled:
		return fmt.Errorf("%w: retention was enabled after the request", ErrDomainExpired)
	}
	return nil
}

// submit sends the stored submit tuple: once for a new job, again only on
// an explicit resume of a submit in doubt.
func (j *Job) submit(ctx context.Context, c skyv1connect.ControlServiceClient, resume bool) error {
	if j.st.Submit == submitSent && !resume {
		return errors.New("ec2job: the submit's outcome is unknown; replay it only with resume")
	}
	task, err := j.task()
	if err != nil {
		return err
	}
	if err := j.cfg.checkTimeout(j.now(), task.TimeoutSeconds); err != nil {
		return fmt.Errorf("ec2job: %w", err)
	}
	if err := j.checkDomain(ctx, c, j.st.Submit == submitSent); err != nil {
		return err
	}
	if j.st.Submit == submitNone {
		next := j.st
		next.Submit = submitSent
		if err := j.commit(next); err != nil {
			return err
		}
	}
	r, err := c.SubmitWorkspaceJob(ctx, connect.NewRequest(&v1.SubmitWorkspaceJobRequest{Task: task, RequestId: j.st.RequestID, Domain: j.domain()}))
	if err != nil {
		return fmt.Errorf("%w (the job may exist; replay only with resume)", remote(Method, err))
	}
	if r == nil || r.Msg == nil || r.Msg.Job == nil || !hexID32.MatchString(r.Msg.Job.Id) {
		return errors.New("ec2job: server returned no valid job; replay only with resume")
	}
	job := r.Msg.Job
	next := j.st
	next.Submit, next.JobID = submitAccepted, job.Id
	reason := ""
	switch {
	case observe(&next, job, j.now()) != nil:
		reason = HoldAcceptedReply
	case job.Target != j.cfg.Target || !sameTask(job.Task, task):
		reason = HoldAcceptedTask
	}
	if reason == "" {
		return j.commit(next)
	}
	// The returned ID is stored, and held, before anything else: the job
	// can still be observed and canceled by that ID, it is never run as if
	// it were this task, and it is never submitted again. If the save
	// fails, the ID stays pending and every later call stores it first.
	next.Hold = reason
	held := fmt.Errorf("%w: %s", ErrHeld, reason)
	if err := j.commit(next); err != nil {
		return errors.Join(held, err)
	}
	return held
}

// task returns the stored task with the finalized workdir.
func (j *Job) task() (*v1.Task, error) {
	t, err := decodeTask(j.st.Task)
	if err != nil {
		return nil, err
	}
	w := j.st.Workdir
	return withWorkdir(t, &v1.ArtifactRef{Id: w.ID, Sha256: w.SHA256, Size: w.Size}), nil
}

// sameTask reports whether the accepted task is exactly the one
// submitted: every field, including fields unknown to this version, is
// equal. An absent and an empty list or map are equal, as they are on
// the wire.
func sameTask(got, want *v1.Task) bool {
	return got != nil && proto.Equal(got, want)
}

// observe records job's state in st. It refuses a job of another ID or an
// unknown state and then changes nothing.
func observe(st *state, job *v1.Job, now time.Time) error {
	if job == nil || job.Id != st.JobID {
		return errors.New("ec2job: server returned another job")
	}
	name := strings.TrimPrefix(job.State.String(), "JOB_STATE_")
	switch job.State {
	case v1.JobState_JOB_STATE_QUEUED, v1.JobState_JOB_STATE_STARTING, v1.JobState_JOB_STATE_RUNNING,
		v1.JobState_JOB_STATE_RETRYING, v1.JobState_JOB_STATE_SUCCEEDED, v1.JobState_JOB_STATE_FAILED,
		v1.JobState_JOB_STATE_CANCELED, v1.JobState_JOB_STATE_UNKNOWN:
	default:
		return errors.New("ec2job: server returned an invalid job state")
	}
	st.Observed, st.ObservedAt = name, now.UTC()
	st.ExitCode = nil
	if job.ExitCode != nil {
		code := *job.ExitCode
		st.ExitCode = &code
	}
	return nil
}

// terminal reports whether the observed state name is final. UNKNOWN is
// final for this package: the server no longer knows the outcome, and
// nothing here retries or replaces the job.
func terminal(name string) bool {
	switch name {
	case "SUCCEEDED", "FAILED", "CANCELED", "UNKNOWN", "NEVER_SUBMITTED":
		return true
	}
	return false
}

// wait observes the job until it ends, or once without a Wait, and then
// collects its outputs. A held job's outputs are never collected: they
// may not be this task's.
func (j *Job) wait(ctx context.Context, c skyv1connect.ControlServiceClient, w Wait) (Status, error) {
	for {
		job, err := j.get(ctx, c)
		if err != nil {
			return j.Status(), err
		}
		if terminal(j.st.Observed) {
			if j.st.Output == outputNone && j.st.Hold == "" {
				if err := j.collect(ctx, c, job); err != nil {
					return j.Status(), err
				}
			}
			return j.Status(), nil
		}
		if w.Poll <= 0 {
			return j.Status(), nil
		}
		t := time.NewTimer(w.Poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return j.Status(), fmt.Errorf("%w: %w", ErrNotTerminal, ctx.Err())
		case <-t.C:
		}
	}
}

// get reads the job with GetJob and stores what it observed if it changed.
func (j *Job) get(ctx context.Context, c skyv1connect.ControlServiceClient) (*v1.Job, error) {
	r, err := c.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: j.st.JobID}))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %w", ErrNotTerminal, remote("GetJob", err))
		}
		return nil, remote("GetJob", err)
	}
	if r == nil || r.Msg == nil {
		return nil, errors.New("ec2job: server returned no job")
	}
	next := j.st
	if err := observe(&next, r.Msg.Job, j.now()); err != nil {
		return nil, err
	}
	if next.Observed != j.st.Observed || !equalCode(next.ExitCode, j.st.ExitCode) {
		if err := j.commit(next); err != nil {
			return nil, err
		}
	}
	return r.Msg.Job, nil
}

func equalCode(a, b *int32) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

// Refresh observes the job with one GetJob request and stores what it
// observed. It never submits, uploads or cancels; a job without a known
// ID is not looked up, because no request can find it without creating
// it.
func (j *Job) Refresh(ctx context.Context) (Status, error) {
	if err := j.ready(true); err != nil {
		return j.Status(), err
	}
	if j.st.JobID == "" {
		return j.Status(), nil
	}
	_, err := j.get(ctx, j.session.Control())
	return j.Status(), err
}

// RequestCancel durably latches the wish to cancel before any request
// acts on it. It needs no session. A job never submitted is then over,
// though a known upload of its workdir may still need cleanup (see
// [Job.Cancel]). A job whose submit is in doubt and whose ID is unknown
// is held: no read-only request can find it, and a submit could create
// it. So is one whose upload Begin is in doubt: its upload ID is unknown,
// and no request can find or clean it up without allocating another. So,
// too, is one whose result download's Begin or Close is in doubt. A known
// download, whose ID is stored and which was never closed, is not in
// doubt: Cancel closes it.
//
// A download Begin or Close this Job stored but, because the save failed,
// never sent is not in doubt either: the Begin is withdrawn, since a
// canceled job's result is not collected, and the Close is Cancel's to
// send. A reopened journal has no such proof and holds them.
//
// Every call reports a latched hold as [ErrHeld], a repeated call or one
// on a reopened journal included, except the holds of a known accepted
// job that did not match ([HoldAcceptedTask], [HoldAcceptedReply]): that
// job is canceled by its ID like any known job.
func (j *Job) RequestCancel() error {
	if err := j.usable(); err != nil {
		return err
	}
	if err := j.flush(); err != nil {
		return err
	}
	next, changed := j.st, j.st.Desired != desiredCancel
	next.Desired = desiredCancel
	if next.Submit == submitNone && next.Observed == "" {
		next.Observed, next.ObservedAt = "NEVER_SUBMITTED", time.Now().UTC()
		next.Output = outputNotUsed
		changed = true
	}
	if next.Download == downloadBegin && j.unsentFrame() {
		next.Download, next.DownloadID, next.DownloadRef, next.DownloadAttempt = downloadNone, "", artifact.Ref{}, ""
		j.unsent = frame{}
		changed = true
	}
	if changed {
		if err := j.commit(next); err != nil {
			return err
		}
	}
	switch {
	case j.st.Hold == HoldAcceptedTask || j.st.Hold == HoldAcceptedReply:
		// The job's ID is known: Cancel acts on it.
	case j.st.Hold != "":
		return fmt.Errorf("%w: %s", ErrHeld, j.st.Hold)
	case j.st.Submit == submitSent && j.st.JobID == "":
		return j.hold(HoldCanceledSubmit)
	case j.st.Upload == uploadBeginSent:
		return j.hold(HoldUploadBegin)
	case j.st.Download == downloadBegin:
		return j.hold(HoldDownloadBegin)
	case j.st.Download == downloadClose && !j.unsentFrame():
		return j.hold(HoldDownloadClose)
	}
	return nil
}

// NeedsRemote reports whether a requested cancel has a known job that has
// not been seen to end, a known upload to clean up, or a known result
// download to close.
func (j *Job) NeedsRemote() bool {
	return j != nil && (j.st.JobID != "" && !terminal(j.st.Observed) || j.needsCleanup() || j.closesDownload())
}

// closesDownload reports whether the job is canceled and has a result
// download to close: one whose ID is known and that was never closed, or
// whose Close this Job stored but never sent.
func (j *Job) closesDownload() bool {
	return j.st.Desired == desiredCancel &&
		(j.st.Download == downloadOpen || j.st.Download == downloadClose && j.unsentFrame())
}

// downloadHeld reports a result download whose Begin or Close is in
// doubt as [ErrHeld], storing the hold if it is not yet latched. A Begin
// or Close this Job stored but never sent is not in doubt. It sends
// nothing. The holds of a known accepted job that did not match are left
// to Cancel, as in [Job.RequestCancel].
func (j *Job) downloadHeld() error {
	switch {
	case j.st.Hold == HoldAcceptedTask || j.st.Hold == HoldAcceptedReply:
		return nil
	case j.st.Hold == HoldDownloadBegin || j.st.Hold == HoldDownloadClose:
		return fmt.Errorf("%w: %s", ErrHeld, j.st.Hold)
	case j.st.Download == downloadBegin && !j.unsentFrame():
		return j.hold(HoldDownloadBegin)
	case j.st.Download == downloadClose && !j.unsentFrame():
		return j.hold(HoldDownloadClose)
	}
	return nil
}

// Known reports whether the job's ID is known: whether there is a job to
// observe, read the log of or cancel.
func (j *Job) Known() bool { return j != nil && j.st.JobID != "" }

// Cancel acts on a cancel RequestCancel latched. For a known job, it asks
// the server to cancel it, once. A nil reply confirms delivery only: with
// a Wait, Cancel then observes the job until it ends, and the job is
// canceled only if it is seen CANCELED. A failed request leaves delivery
// unknown.
//
// For a job never submitted whose upload ID is known, Cancel instead asks
// the server to delete that upload with CancelArtifactUpload, as the
// package documentation's Cleanup section describes.
//
// A known result download of the job, whose ID is stored and which was
// never closed, is first closed by exactly that ID, once, as a download
// is (see [Job.download]); its result is not read, no other download is
// begun and nothing is submitted. A failed Close holds the job
// ([HoldDownloadClose]).
//
// A download whose Begin or Close is in doubt is reported as [ErrHeld]
// by every call, a repeated call or one on a reopened journal included,
// even when the job has ended; no request is sent for it. As with
// [Job.RequestCancel], a known accepted job that did not match is
// canceled by its ID.
func (j *Job) Cancel(ctx context.Context, w Wait) (Status, error) {
	if err := j.ready(true); err != nil {
		return j.Status(), err
	}
	switch {
	case j.st.Desired != desiredCancel:
		return j.Status(), errors.New("ec2job: request the cancel first")
	case j.needsCleanup():
		err := j.cancelUpload(ctx, j.session.Control())
		return j.Status(), err
	case j.st.JobID == "":
		return j.Status(), errors.New("ec2job: no job ID is known; nothing can be canceled")
	}
	c := j.session.Control()
	if j.closesDownload() {
		if err := j.settleDownload(ctx, c); err != nil {
			return j.Status(), err
		}
	}
	if err := j.downloadHeld(); err != nil {
		return j.Status(), err
	}
	if terminal(j.st.Observed) {
		return j.Status(), nil
	}
	if j.st.Cancel == cancelNone {
		next := j.st
		next.Cancel = cancelSent
		if err := j.commit(next); err != nil {
			return j.Status(), err
		}
	}
	r, err := c.CancelJob(ctx, connect.NewRequest(&v1.CancelJobRequest{Id: j.st.JobID}))
	if err != nil {
		return j.Status(), fmt.Errorf("%w (delivery unknown)", remote("CancelJob", err))
	}
	next := j.st
	next.Cancel = cancelDelivered
	if r != nil && r.Msg != nil && r.Msg.Job != nil {
		if err := observe(&next, r.Msg.Job, j.now()); err != nil {
			return j.Status(), err
		}
	}
	if err := j.commit(next); err != nil {
		return j.Status(), err
	}
	return j.wait(ctx, c, w)
}

// Status returns the job's status from its journal.
func (j *Job) Status() Status {
	if j == nil {
		return Status{}
	}
	st := j.st
	s := Status{
		RequestID:      st.RequestID,
		Method:         Method,
		Retention:      st.Retention,
		Desired:        st.Desired,
		Upload:         st.Upload,
		UploadID:       st.UploadID,
		Acknowledged:   st.Acknowledged,
		SnapshotSHA256: st.Snapshot.SHA256,
		SnapshotSize:   st.Snapshot.Size,
		Submit:         st.Submit,
		JobID:          st.JobID,
		Cancel:         st.Cancel,
		State:          st.Observed,
		Output:         st.Output,
		Download:       st.Download,
		DownloadID:     st.DownloadID,
		Cleanup:        st.Cleanup,
		LogOffset:      st.LogOffset,
		Hold:           st.Hold,
		InstanceID:     st.VM.InstanceID,
		Address:        st.VM.Address.String(),
		Durable:        !j.pending,
	}
	if st.ExitCode != nil {
		code := *st.ExitCode
		s.ExitCode = &code
	}
	if st.Retention == RetentionEnabled {
		s.RequestDomain = fmt.Sprintf("%s:%d", st.Lineage, st.Epoch)
	}
	if st.Workdir != (artifact.Ref{}) {
		w := st.Workdir
		s.Workdir = &w
	}
	if !st.ObservedAt.IsZero() {
		at := st.ObservedAt
		s.ObservedAt = &at
	}
	if st.Output == outputSaved {
		r := st.OutputRef
		s.OutputRef = &r
		s.OutputFile = filepath.Join(j.dir, outputName)
	}
	return s
}

// A Status is a job's journal as this package last stored it. It holds
// no credential, task environment or server message.
type Status struct {
	RequestID      string        `json:"request_id"`
	Method         string        `json:"method"`
	Retention      string        `json:"retention"`
	RequestDomain  string        `json:"request_domain,omitempty"`
	Desired        string        `json:"desired"`
	Upload         string        `json:"upload"`
	UploadID       string        `json:"upload_id,omitempty"`
	Acknowledged   int64         `json:"acknowledged"`
	SnapshotSHA256 string        `json:"snapshot_sha256"`
	SnapshotSize   int64         `json:"snapshot_size"`
	Workdir        *artifact.Ref `json:"workdir"`
	Submit         string        `json:"submit"`
	JobID          string        `json:"job_id,omitempty"`
	Cancel         string        `json:"cancel"`
	State          string        `json:"state,omitempty"`
	ExitCode       *int32        `json:"exit_code"`
	ObservedAt     *time.Time    `json:"observed_at,omitempty"`
	Output         string        `json:"output,omitempty"`
	OutputRef      *artifact.Ref `json:"output_ref,omitempty"`
	OutputFile     string        `json:"output_file,omitempty"`
	Download       string        `json:"download,omitempty"`
	DownloadID     string        `json:"download_id,omitempty"`
	Cleanup        string        `json:"cleanup,omitempty"`
	LogOffset      int64         `json:"log_offset"`
	Hold           string        `json:"hold,omitempty"`
	InstanceID     string        `json:"instance_id"`
	Address        string        `json:"address"`
	Durable        bool          `json:"durable"`
}

// Ended reports whether the job was seen to end, or was never submitted.
func (s Status) Ended() bool { return terminal(s.State) }

// Succeeded reports whether the job was seen to succeed and its declared
// outputs, if any, are durably saved.
func (s Status) Succeeded() bool {
	return s.State == "SUCCEEDED" && (s.Output == outputSaved || s.Output == outputNotUsed)
}

// A RemoteError reports a failed control request by method and Connect
// code, and a fixed retention reason if the server gave one. Its text
// never quotes the server's message, which may hold workload or remote
// text; Unwrap returns the original error.
type RemoteError struct {
	Method string
	Code   connect.Code
	Reason string
	err    error
}

func (e *RemoteError) Error() string {
	s := "ec2job: " + e.Method + ": " + e.Code.String()
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	return s
}

func (e *RemoteError) Unwrap() error { return e.err }

func remote(method string, err error) error {
	e := &RemoteError{Method: method, Code: connect.CodeOf(err), err: err}
	var ce *connect.Error
	if errors.As(err, &ce) {
		for _, d := range ce.Details() {
			v, derr := d.Value()
			if derr != nil {
				continue
			}
			if x, ok := v.(*v1.RetentionExpiry); ok {
				switch x.Reason {
				case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED:
					e.Reason = "request epoch expired"
				case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED:
					e.Reason = "request domain required"
				case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED:
					e.Reason = "request lineage changed"
				case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED:
					e.Reason = "result expired"
				case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_LOGS_EXPIRED:
					e.Reason = "logs expired"
				}
			}
		}
	}
	return e
}
