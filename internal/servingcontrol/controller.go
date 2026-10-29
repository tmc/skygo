package servingcontrol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/store"
)

// Errors of Controller methods. Store errors, such as store.ErrNotFound for
// a missing or foreign service, and auth errors are returned as they are.
var (
	// ErrInvalid reports a malformed request.
	ErrInvalid = errors.New("invalid service request")
	// ErrUnsupported reports a spec feature this controller or its runtime
	// does not run. It is serving.ErrUnsupported.
	ErrUnsupported = serving.ErrUnsupported
	// ErrUnknownTarget reports a target this controller does not serve.
	ErrUnknownTarget = errors.New("unknown service target")
	// ErrNotReady reports a forward to a service whose current generation
	// is not observed ready, or that has an operation in flight.
	ErrNotReady = errors.New("service is not ready")
	// ErrForwardLimit reports a forward refused at MaxForwards.
	ErrForwardLimit = errors.New("service forward limit reached")
	// ErrClosed reports a call refused because Close has begun.
	ErrClosed = errors.New("service controller closed")
)

const (
	// MaxForwards bounds the forwards admitted at once, over every service.
	MaxForwards = 64

	// maxAttempts bounds the commits of one change that lose a record
	// revision race.
	maxAttempts = 16
	// maxApply bounds the Apply calls for one service in one pass; each
	// follows the commit of the previous call's Native. A runtime that
	// declares one create per call needs two calls per object and one to
	// verify.
	maxApply = 8
	// reconcileInterval is Run's period between passes.
	reconcileInterval = 10 * time.Second
	// pruneLimit bounds the request witnesses one pass prunes.
	pruneLimit = 64
)

// errContended reports a change that lost maxAttempts revision races.
var errContended = errors.New("service record revision contended")

// Config configures a Controller. Every field but Now is required.
type Config struct {
	Store *store.Store
	Auth  *auth.Engine
	// Target is the configured public target name and Binding its
	// server-side binding, which the Store charges limits against.
	Target  string
	Binding string
	Runtime serving.Runtime
	Limits  store.ServiceLimits
	// Admit, if not nil, refuses a spec that the operator has not granted to
	// principal, such as an image outside the principal's grant. It is
	// called by Create, without I/O, before acceptance.
	Admit func(principal string, spec serving.Spec) error
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
}

// A Controller accepts and reconciles the services of one target. Its zero
// value is not usable; use New.
type Controller struct {
	store   *store.Store
	auth    *auth.Engine
	target  string
	binding string
	runtime serving.Runtime
	limits  store.ServiceLimits
	admit   func(principal string, spec serving.Spec) error
	now     func() time.Time
	wake    chan struct{}
	// gate, of capacity one, is held by one Reconcile pass at a time.
	gate chan struct{}

	// read and commit are the Store's Service and CommitService; tests
	// replace them to inject faults.
	read   func(id string) (store.Service, error)
	commit func(id string, revision uint64, next store.ServiceCommit) (store.Service, error)

	// effects is canceled by Close; every runtime call runs under it.
	effects context.Context
	cancel  context.CancelFunc

	mu sync.Mutex // guards the fields below
	// pending holds, per service, a runtime result whose commit is not
	// known, with the operation it was returned under; see hold and flush.
	pending map[string]result
	// originals holds, per service, the original runtime calls whose late
	// results a resultDrainer runtime may still offer; see drain. It is
	// apart from pending.
	originals map[string]*original
	// forwards holds the cancel functions of admitted forwards per service.
	forwards map[string]map[*forward]context.CancelFunc
	// active counts admitted calls: Create, Get, List, Delete, Reconcile
	// passes and forwards. closed refuses new ones; idle is closed once
	// closed is set and active is zero.
	active   int
	nforward int
	closed   bool
	idle     chan struct{}
}

// A resultDrainer is a runtime that keeps custody of a call's original
// exchange after the call returns. Drain offers, under a nonzero token,
// the facts of ref's exchanges that arrived since and the Native they make
// of ref's, repeating that offer until Ack acknowledges its token; Ack
// may be called only once that Native is committed. Token zero is no
// offer: a nil error then reports every original returned and every fact
// acknowledged. Neither makes a native call; both are usable after Close.
type resultDrainer interface {
	Drain(ref serving.Ref) (native serving.Native, token uint64, err error)
	Ack(ref serving.Ref, token uint64) error
}

// An original is the custody of service runtime calls admitted under op,
// recorded before the first of them: ref is the service's, and before the
// Native it then had. acked is the last Native of their results known
// committed under op, starting at before.
type original struct {
	op     store.ServiceOperation
	ref    serving.Ref
	before serving.Native
	acked  serving.Native
}

// New returns a Controller for c.
func New(c Config) (*Controller, error) {
	if c.Store == nil || c.Auth == nil || c.Runtime == nil {
		return nil, errors.New("service controller requires a store, an auth engine and a runtime")
	}
	if c.Target == "" || c.Binding == "" {
		return nil, errors.New("service controller requires a target and a binding")
	}
	if err := c.Limits.Validate(); err != nil {
		return nil, err
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	effects, cancel := context.WithCancel(context.Background())
	return &Controller{
		store:     c.Store,
		auth:      c.Auth,
		target:    c.Target,
		binding:   c.Binding,
		runtime:   c.Runtime,
		limits:    c.Limits,
		admit:     c.Admit,
		now:       func() time.Time { return now().UTC() },
		wake:      make(chan struct{}, 1),
		gate:      make(chan struct{}, 1),
		read:      c.Store.Service,
		commit:    c.Store.CommitService,
		effects:   effects,
		cancel:    cancel,
		pending:   map[string]result{},
		originals: map[string]*original{},
		forwards:  map[string]map[*forward]context.CancelFunc{},
		idle:      make(chan struct{}),
	}, nil
}

// Target returns the configured target name.
func (c *Controller) Target() string { return c.target }

// Create durably accepts spec as service name on target under requestID in
// actor's request domain d, or returns the service already bound to that
// request, even a retired one, without a runtime call. The spec must be one
// this endpoint runs: one fixed replica, no mounts, workdir or setup, and
// what the runtime's Check accepts. Create causes no runtime effect; a later
// pass applies the service. After Close has begun it is ErrClosed.
func (c *Controller) Create(actor auth.Actor, d store.RequestDomain, requestID, name, target string, spec serving.Spec) (store.Service, error) {
	if err := c.enter(); err != nil {
		return store.Service{}, err
	}
	defer c.leave()
	actor, err := c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return store.Service{}, err
	}
	if !validDomain(d) {
		return store.Service{}, fmt.Errorf("%w: invalid request domain", ErrInvalid)
	}
	if !store.ValidServiceName(name) {
		return store.Service{}, fmt.Errorf("%w: invalid service name", ErrInvalid)
	}
	if target != c.target {
		return store.Service{}, ErrUnknownTarget
	}
	n, err := spec.Normalize()
	if err != nil {
		return store.Service{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := supported(n); err != nil {
		return store.Service{}, err
	}
	if err := c.runtime.Check(n); err != nil {
		return store.Service{}, err
	}
	if c.admit != nil {
		if err := c.admit(actor.Principal(), n); err != nil {
			return store.Service{}, err
		}
	}
	authority, err := c.auth.Delegate(actor)
	if err != nil {
		return store.Service{}, err
	}
	s, err := c.store.AcceptService(actor, store.ServiceRequest{
		Domain:    d,
		RequestID: requestID,
		Name:      name,
		Target:    c.target,
		Binding:   c.binding,
		Authority: authority,
		Spec:      n,
	}, c.limits)
	if err != nil {
		return store.Service{}, err
	}
	c.signal()
	return s, nil
}

// supported refuses what this endpoint does not run, before acceptance.
func supported(s serving.Spec) error {
	switch {
	case len(s.Mounts) != 0:
		return fmt.Errorf("%w: mounts", ErrUnsupported)
	case s.Workdir != nil:
		return fmt.Errorf("%w: workdir", ErrUnsupported)
	case len(s.Setup) != 0:
		return fmt.Errorf("%w: setup", ErrUnsupported)
	case s.Scaling.Replicas != 1:
		return fmt.Errorf("%w: replicas other than 1", ErrUnsupported)
	}
	return nil
}

// validDomain reports whether d names a lineage, 32 lowercase hexadecimal
// digits, and a nonzero epoch.
func validDomain(d store.RequestDomain) bool {
	return len(d.Lineage) == 32 && strings.Trim(d.Lineage, "0123456789abcdef") == "" && d.Epoch != 0
}

// Get returns service id if actor may read it. After Close has begun it is
// ErrClosed.
func (c *Controller) Get(actor auth.Actor, id string) (store.Service, error) {
	if err := c.enter(); err != nil {
		return store.Service{}, err
	}
	defer c.leave()
	actor, err := c.auth.Check(actor, auth.Read)
	if err != nil {
		return store.Service{}, err
	}
	if !store.ValidServiceID(id) {
		return store.Service{}, store.ErrNotFound
	}
	return c.store.ServiceFor(actor, id)
}

// List returns at most limit services visible to actor after listing
// ordinal after. After Close has begun it is ErrClosed.
func (c *Controller) List(actor auth.Actor, after uint64, limit int) (store.ServicePage, error) {
	if err := c.enter(); err != nil {
		return store.ServicePage{}, err
	}
	defer c.leave()
	actor, err := c.auth.Check(actor, auth.Read)
	if err != nil {
		return store.ServicePage{}, err
	}
	return c.store.ServicesFor(actor, after, limit)
}

// Delete latches service id Absent under requestID in actor's request
// domain d if its record revision is still expected, then ends this
// process's forwards to it. A later pass removes its native objects once
// those forwards have returned. An exact replay returns the service only if
// actor may now read it. After Close has begun it is ErrClosed.
func (c *Controller) Delete(actor auth.Actor, d store.RequestDomain, requestID, id string, expected uint64) (store.Service, error) {
	if err := c.enter(); err != nil {
		return store.Service{}, err
	}
	defer c.leave()
	actor, err := c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return store.Service{}, err
	}
	if !validDomain(d) {
		return store.Service{}, fmt.Errorf("%w: invalid request domain", ErrInvalid)
	}
	s, err := c.store.DeleteService(actor, store.ServiceMutation{Domain: d, RequestID: requestID, ID: id, Revision: expected})
	if err != nil {
		return store.Service{}, err
	}
	c.endForwards(id)
	c.signal()
	return s, nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func (c *Controller) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
