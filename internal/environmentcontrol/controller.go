package environmentcontrol

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

// Limits a Runtime may rely on. The controller checks them before any
// runtime call.
const (
	// MaxNativeBytes bounds a runtime's native state.
	MaxNativeBytes = 16 << 10
	// MaxArgvBytes bounds the total bytes of an exec's argv.
	MaxArgvBytes = 64 << 10
	// MaxStdinBytes bounds an exec's stdin.
	MaxStdinBytes = 64 << 10
)

// ErrUnsupported reports a spec that a runtime's target cannot run. Plan
// returns it before any effect.
var ErrUnsupported = errors.New("unsupported environment feature")

// ErrWritePending reports an Ensure or Delete that declared, in its returned
// Native, a write it has not sent. The runtime sends that write only in a
// later call, by the same process, whose Ref.Native equals the declaring
// Native byte for byte: only after the declaration is durable. The
// Controller commits the Native and calls again; it never calls again while
// that commit is unresolved.
var ErrWritePending = errors.New("environment runtime write pending")

// Runtime is the provider side of a Controller: the concrete resources of
// environments of one checkpoint kind. Its methods are safe for concurrent
// use. The Controller calls Ensure and Delete for one environment from one
// goroutine at a time; Exec and Forward may run concurrently with them and
// with each other.
//
// A Runtime never decides lifecycle. It receives only committed rows: every
// Ref it is given was durably accepted, with its Nonce, before the call, and
// its Native is the last committed native state. The Controller commits every
// Observation an Ensure or Delete returns before it starts any effect that
// depends on it, including the dispatch of a write declared with
// ErrWritePending.
type Runtime interface {
	// Kind returns the checkpoint kind of every environment the runtime
	// accepts: 1..63 lowercase letters, digits or '-'. It never changes.
	Kind() string

	// Plan checks spec, whose canonical encoding has the given SHA-256
	// digest, for principal against the runtime's current configuration.
	// It returns the target binding, 1..256 bytes of UTF-8 without NUL, and
	// the initial native state, at most MaxNativeBytes. It sends no request
	// and has no effect. A spec the target cannot run wraps ErrUnsupported.
	Plan(principal string, spec environment.Spec, digest [32]byte) (binding string, native []byte, err error)

	// Ensure creates the environment's own resources, then observes them.
	//
	// Every mutating request it sends is declared first: the call that
	// decides to send it sends nothing, records the request's exact
	// obligation in the returned Native and returns ErrWritePending, and the
	// request is sent at most once, by that process, in a later call whose
	// r.Native is that declaration. A request is one network send; a client
	// or transport retry is another send of the same declaration, so the
	// runtime disables such retries or declares each of them.
	//
	// A declared creation stays unresolved until positive evidence resolves
	// it: a definitive refusal that proves it was not applied, or an actual
	// identity fence such as the created resource's own response or a
	// resource that carries r.Identity and r.Nonce and that only that
	// declaration could have created. A timeout, a lost response, a process
	// restart, elapsed time, a not-found read or an empty list is not
	// evidence. A declaration made by another process, or before a restart,
	// counts as possibly sent. While any creation is unresolved Ensure
	// declares no other creation, mints no nonce and reports Creating or
	// Unknown, never Ready.
	//
	// It marks every resource it creates with r.Identity and r.Nonce, and it
	// recognizes a resource as its own only by them together with what
	// r.Native records, never by name alone. It never adopts, modifies or
	// deletes a resource it does not own, and it never creates again a
	// resource that r.Native records as created; a recorded resource it
	// cannot observe is Unknown. It verifies, and never creates or deletes,
	// a borrowed resource such as the operator volume that Spec.StorageAlias
	// names.
	//
	// The Observation's Phase is Creating, Ready, Unknown or zero. Ready is a
	// fresh observation in this call that every owned and borrowed resource
	// matches r and that no creation is unresolved. Native, when not nil,
	// records every declaration and every resource identity the call learned,
	// including a late result of an earlier send; it is returned even with an
	// error.
	Ensure(ctx context.Context, r Ref) (Observation, error)

	// Delete removes the environment's owned resources, each only with the
	// identity r.Native records as a precondition, declaring each mutating
	// request as Ensure does. It never deletes a borrowed resource. It may
	// be called while, or after, an Ensure declared a creation; it then
	// keeps that creation's obligation, never sends it, and recovers any
	// resource it made, from every send of it, by r.Identity and r.Nonce.
	//
	// Delete reports Deleted only when every declared creation is resolved,
	// so that none can later materialize, and every owned resource identity
	// recorded or recovered was positively observed gone. An empty read
	// alone never proves either. The Observation's Phase is Deleting,
	// Deleted, Unknown or zero; Native is as for Ensure. Delete may be
	// called again after any result, including Deleted.
	Delete(ctx context.Context, r Ref) (Observation, error)

	// Exec starts one noninteractive command in the environment's own
	// resources, after checking that they are the ones r.Native records.
	// An error means that no request was sent. Otherwise the returned
	// frame is the only record of the call.
	Exec(r Ref, req ExecRequest) (Exec, error)

	// Forward relays one HTTP exchange of req to the environment's declared
	// port, Spec.Port, with req.URL's path and query; it takes no host,
	// port or URL from req. It removes credentials, refuses upgrades and
	// never follows a redirect. It is called from an http.Handler with that
	// handler's w, so that it may set deadlines. It returns once the
	// response is written, or after a bounded join with the frame
	// unsettled. The exchange ends when req's context ends: when the
	// client leaves or the Controller closes the session.
	Forward(w http.ResponseWriter, req *http.Request, r Ref) Forward
}

// Ref is one accepted environment as a Runtime sees it. Every field but
// Native was committed at acceptance, before any runtime call. Nonce is 32
// lowercase hexadecimal digits, fixed at acceptance and never replaced.
// Native is the runtime's last committed state, in the runtime's own
// versioned encoding, which it refuses when it does not know the version.
// The Controller stores it apart from its own session and expiry records and
// never interprets it. The runtime must not retain or modify it.
type Ref struct {
	Identity   environment.Identity
	Principal  string
	Spec       environment.Spec
	SpecDigest [32]byte
	Binding    string
	Nonce      string
	Native     []byte
}

// Observation is the result of one Ensure or Delete. A zero Phase observed
// nothing and keeps the recorded phase. A nil Native keeps the recorded
// native state; any other value, at most MaxNativeBytes, replaces it.
// Incarnation, when not empty, is the runtime identity of the environment's
// current instance that this call positively observed, such as a Pod UID:
// 1..128 bytes of [A-Za-z0-9._:-]. Empty observed none. It is never a name,
// a guess or an identity the runtime did not observe in this call.
type Observation struct {
	Phase       environment.Phase
	Native      []byte
	Incarnation string
}

// ExecRequest is one noninteractive command. Argv is passed as is, without a
// shell; it is not empty, its total size is at most MaxArgvBytes and no
// argument contains NUL. Stdin, at most MaxStdinBytes, is nil for no stdin
// stream.
type ExecRequest struct {
	Argv  []string
	Stdin []byte
}

// Exec is the frame of one started command. Done is closed once every
// goroutine and connection operation the frame owns has returned. Cancel
// closes only this frame's connection and returns at once; it never reruns
// the command or resends stdin. Neither proves that the remote command, or
// anything it started, stopped.
type Exec interface {
	Done() <-chan struct{}
	Cancel()
	Result() ExecResult
}

// ExecResult is a snapshot of an Exec frame. Settled reports that Done is
// closed. Err joins every recorded failure, including cancellation. Exited
// reports that the command's exit status was relayed, with Code. Refused
// reports that the runtime knows no command started: the request was never
// written, or its upgrade was answered with a refusal. Any other outcome is
// unknown, never exit zero. Stdout and Stderr are the retained output;
// Truncated reports that more was discarded.
type ExecResult struct {
	Settled   bool
	Err       error
	Exited    bool
	Refused   bool
	Code      int
	Stdout    []byte
	Stderr    []byte
	Truncated bool
}

// Forward is the frame of one relayed exchange. Done, Cancel and Result are as
// for Exec.
type Forward interface {
	Done() <-chan struct{}
	Cancel()
	Result() ForwardResult
}

// ForwardResult is a snapshot of a Forward frame. Status is the status code
// written to the client, or zero if none was.
type ForwardResult struct {
	Settled bool
	Err     error
	Status  int
}

// Errors of Controller methods. Store errors, such as store.ErrNotFound for a
// missing or foreign environment, and auth errors are returned as they are.
var (
	// ErrInvalid reports a malformed request.
	ErrInvalid = errors.New("invalid environment request")
	// ErrClosed reports an environment whose deletion or expiry is
	// recorded. It admits no session again.
	ErrClosed = errors.New("environment is deleting or expired")
	// ErrNotReady reports an environment whose recorded phase is not
	// Ready.
	ErrNotReady = errors.New("environment is not ready")
	// ErrSessionLimit reports an environment that already records
	// MaxSessions sessions, open or unknown.
	ErrSessionLimit = errors.New("environment session limit reached")
	// ErrNoPort reports a forward to an environment that declares no
	// port.
	ErrNoPort = errors.New("environment declares no port")
)

const (
	// MaxSessions bounds the sessions one environment records, open or
	// unknown.
	MaxSessions = 8
	// MaxExecTime bounds one exec session.
	MaxExecTime = time.Hour

	// checkpointVersion is the version of the ledger encoding that a
	// Controller keeps in each environment's checkpoint.
	checkpointVersion = 1
	// maxAttempts bounds the commits of one change that lose a revision
	// race.
	maxAttempts = 16
	// maxPending bounds the declared runtime writes dispatched for one
	// environment in one Reconcile pass.
	maxPending = 4
	// reconcileInterval is Run's period between passes.
	reconcileInterval = 10 * time.Second
)

// errContended reports a change that lost maxAttempts revision races.
var errContended = errors.New("environment revision contended")

// errSessionsStopped reports a session refused because the Controller's
// sessions were stopped by Integration.Close. Refused at entry, it read,
// wrote and started nothing.
var errSessionsStopped = errors.New("environment sessions stopped")

// Config configures a Controller. Store, Auth and Runtime are required.
type Config struct {
	Store   *store.Store
	Auth    *auth.Engine
	Runtime Runtime
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
}

// A Controller accepts environments of one Runtime, drives their lifecycle
// and admits their sessions. Its zero value is not usable; use New.
type Controller struct {
	store       *store.Store
	auth        *auth.Engine
	runtime     Runtime
	kind        string
	now         func() time.Time
	incarnation string // this process's session owner
	wake        chan struct{}
	// gate, of capacity one, is held by one Reconcile pass or one session
	// admission at a time; see lock.
	gate chan struct{}
	// beforeGate, if set, is called by an admission just before it waits
	// for gate; tests use it to change state while admission waits.
	beforeGate func()

	// read and commit are the Store's Environment and CommitEnvironment;
	// tests replace them to inject faults.
	read   func(id string) (store.Environment, error)
	commit func(id string, revision uint64, next store.EnvironmentCommit) (store.Environment, error)

	// materializer is runtime if it is a Materializer, and nil otherwise.
	materializer Materializer
	retry        time.Duration // materializeRetry; tests shorten it
	copies       chan struct{} // one artifact copy at a time
	// begin, mark and finish are the Store's
	// BeginEnvironmentMaterialization, MarkArtifactDownloadClosing and
	// FinishArtifactDownload; tests replace them to inject faults.
	begin  func(id string, revision, seq uint64, incarnation string, ref store.ArtifactRef, producer store.OriginalServerLifetime, now time.Time) (store.ArtifactLease, error)
	mark   func(store.ArtifactDownloadClosure) (store.ArtifactDownloadClosure, error)
	finish func(store.ArtifactDownloadClosure, time.Time) error

	mu            sync.Mutex                   // guards the fields below
	frames        map[string]map[string]func() // environment, session: cancel
	closed        map[string]bool              // environments whose sessions were closed
	pending       map[string]*pending          // environment: uncommitted runtime result
	materializing map[string]*frame            // environment: its latest artifact frame
	stopping      bool                         // StopMaterializing was called
	running       bool                         // Run has started
	// owners counts session owners: each Exec or Forward from its entry,
	// before any authentication, Store or runtime call, until its last
	// Store operation, including a settlement that outlives the call.
	// sessionsStopped refuses new owners and frames; sessionsIdle is closed
	// once sessionsStopped is set and owners is zero. See stopSessions.
	owners          int
	sessionsStopped bool
	sessionsIdle    chan struct{}
	// artifacts, bound once by BindArtifacts, opens artifact reads of
	// producer.
	artifacts ArtifactOpener
	producer  store.OriginalServerLifetime
}

// New returns a Controller for c.Runtime's environments. Before it returns it
// records every exec session of a previous process as unknown and ends every
// forward session of a previous process; it never infers that a command
// stopped.
func New(c Config) (*Controller, error) {
	if c.Store == nil || c.Auth == nil || c.Runtime == nil {
		return nil, errors.New("environment controller requires a store, an auth engine and a runtime")
	}
	kind := c.Runtime.Kind()
	if err := (environment.Checkpoint{Kind: kind, Version: checkpointVersion, Data: []byte{0}}).Validate(); err != nil {
		return nil, fmt.Errorf("runtime kind: %w", err)
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	incarnation, err := newID()
	if err != nil {
		return nil, err
	}
	ctl := &Controller{
		store:       c.Store,
		auth:        c.Auth,
		runtime:     c.Runtime,
		kind:        kind,
		now:         func() time.Time { return now().UTC() },
		incarnation: incarnation,
		wake:        make(chan struct{}, 1),
		gate:        make(chan struct{}, 1),
		read:        c.Store.Environment,
		commit:      c.Store.CommitEnvironment,
		retry:       materializeRetry,
		copies:      make(chan struct{}, 1),
		begin:       c.Store.BeginEnvironmentMaterialization,
		mark:        c.Store.MarkArtifactDownloadClosing,
		finish:      c.Store.FinishArtifactDownload,
		frames:      map[string]map[string]func(){},
		closed:      map[string]bool{},
		pending:     map[string]*pending{},

		materializing: map[string]*frame{},
		sessionsIdle:  make(chan struct{}),
	}
	if m, ok := c.Runtime.(Materializer); ok {
		ctl.materializer = m
	}
	if err := ctl.recover(); err != nil {
		return nil, err
	}
	return ctl, nil
}

// View is the portable state of one environment. It holds no provider
// checkpoint, native state, nonce, binding or credential.
type View struct {
	ID        string
	Principal string
	Spec      environment.Spec
	State     environment.State
	// Expired reports that idle expiry recorded the deletion.
	Expired bool
	// Sessions counts open sessions; Unknown counts sessions whose
	// outcome is unknown. Both are zero for an environment of another
	// runtime kind.
	Sessions, Unknown int
	// Held reports that the environment's committed incarnation cannot
	// become Ready in this process: an artifact read of it failed to
	// stage, closed without staging, was materialized by another server
	// lifetime, is held by another reader, or is held unknown (S4).
	Held bool
	// IdleSince is the start of the current idle period.
	IdleSince time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ExpiresAt returns the earliest time idle expiry may record the deletion of
// v, and false if it cannot now: no idle timeout, a recorded session, Absent
// intent or a Creating phase. Expiry is latched by a Reconcile pass at or
// after that time.
func (v View) ExpiresAt() (time.Time, bool) {
	return idleDeadline(v.Spec, v.State, v.IdleSince, v.Sessions+v.Unknown)
}

// Page is one listing page. After is the cursor of the next page.
type Page struct {
	Environments []View
	After        uint64
	More         bool
}

// Get returns one environment visible to actor. Missing and foreign
// environments are both store.ErrNotFound.
func (c *Controller) Get(actor auth.Actor, id string) (View, error) {
	actor, err := c.auth.Check(actor, auth.Read)
	if err != nil {
		return View{}, err
	}
	if !environment.ValidID(id) {
		return View{}, fmt.Errorf("%w: invalid environment id", ErrInvalid)
	}
	e, err := c.store.EnvironmentFor(actor, id)
	if err != nil {
		return View{}, err
	}
	return c.view(e)
}

// List lists the environments visible to actor in acceptance order, after
// the cursor after. A limit of zero means 50; at most 200.
func (c *Controller) List(actor auth.Actor, after uint64, limit int) (Page, error) {
	actor, err := c.auth.Check(actor, auth.Read)
	if err != nil {
		return Page{}, err
	}
	if limit == 0 {
		limit = 50
	}
	if limit < 0 || limit > 200 {
		return Page{}, fmt.Errorf("%w: limit outside 0..200", ErrInvalid)
	}
	p, err := c.store.EnvironmentsFor(actor, after, limit)
	if err != nil {
		return Page{}, err
	}
	out := Page{After: p.After, More: p.More}
	for _, e := range p.Environments {
		v, err := c.view(e)
		if err != nil {
			return Page{}, err
		}
		out.Environments = append(out.Environments, v)
	}
	return out, nil
}

func (c *Controller) view(e store.Environment) (View, error) {
	v := View{
		ID:        e.Identity.ID,
		Principal: e.Principal,
		Spec:      e.Spec.Clone(),
		State:     e.State,
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
	}
	if e.Checkpoint.Kind != c.kind {
		return v, nil
	}
	l, err := c.decode(e)
	if err != nil {
		return View{}, err
	}
	v.Expired, v.IdleSince = l.Expired, l.Idle
	v.Held = c.artifactsHeld(e)
	for _, s := range l.Sessions {
		if s.Unknown {
			v.Unknown++
		} else {
			v.Sessions++
		}
	}
	return v, nil
}

// ref returns e as its runtime sees it. Nothing in it shares memory with e
// or l.
func (c *Controller) ref(e store.Environment, l ledger) Ref {
	return Ref{
		Identity:   e.Identity,
		Principal:  e.Principal,
		Spec:       e.Spec.Clone(),
		SpecDigest: e.SpecDigest,
		Binding:    e.Binding,
		Nonce:      l.Nonce,
		Native:     bytes.Clone(l.Native),
	}
}

// encode returns l as a checkpoint of this Controller's kind.
func (c *Controller) encode(l ledger) (environment.Checkpoint, error) {
	data, err := encodeLedger(l)
	if err != nil {
		return environment.Checkpoint{}, err
	}
	return environment.Checkpoint{Kind: c.kind, Version: checkpointVersion, Data: data}, nil
}

// decode returns e's ledger. An environment of another kind or version is
// not this Controller's.
func (c *Controller) decode(e store.Environment) (ledger, error) {
	if e.Checkpoint.Kind != c.kind || e.Checkpoint.Version != checkpointVersion {
		return ledger{}, fmt.Errorf("environment checkpoint %s/%d is not %s/%d", e.Checkpoint.Kind, e.Checkpoint.Version, c.kind, checkpointVersion)
	}
	return decodeLedger(e.Checkpoint.Data)
}

// newID returns 32 random lowercase hexadecimal digits.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
