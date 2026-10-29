package kube

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/serving"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// serviceReadTimeout bounds the reads of one runtime call.
const serviceReadTimeout = 30 * time.Second

var (
	// errServiceWritePending reports an Apply that declared, in its
	// returned Native, a create it has not sent. The create is sent only by
	// this process, once, in a later Apply whose Ref.Native equals that
	// Native: after the controller committed it. It is private: the
	// controller relies only on the returned Native differing.
	errServiceWritePending = errors.New("kubernetes service write pending")
	// errServiceCreateUnresolved reports a declared create whose outcome is
	// not known: it may have been sent, by this process before a lost
	// response or by an earlier process, and no object that is exactly its
	// rendering has been read. It is never Absent.
	errServiceCreateUnresolved = errors.New("kubernetes service create unresolved")
	errServiceStopped          = errors.New("kubernetes service runtime stopped")
	// errServiceNotRendered reports an object at a service's name that is
	// not exactly the rendering its Native records.
	errServiceNotRendered = fmt.Errorf("%w: kubernetes service object is not its rendering", serving.ErrIdentityConflict)
)

// serviceRuntime is the serving.Runtime of one Kubernetes target. It reads
// only Backend fields that never change after construction.
type serviceRuntime struct {
	b      *Backend
	target string
	config ServiceConfig
	writer serviceWriter
	// port is the transport newServiceRuntime built from the frozen
	// configuration, and serve its servePort; tests reach its original
	// connections through port alone.
	port  *envTransport
	serve func(http.ResponseWriter, *http.Request, portTarget) *portFrame

	// effects is the parent of every context the runtime's own frames use;
	// stop cancels it. It never deletes an object.
	effects    context.Context
	endEffects context.CancelFunc

	mu       sync.Mutex
	locks    map[string]chan struct{}  // per service, capacity 1
	declared map[string]serving.Native // per service, the Native that declared a create this process has not sent
	resolved map[string]types.UID      // per declaration, the UID of an accepted object carrying it
	// withdrawn holds declarations this process made and will never send.
	withdrawn map[string]bool
	// owners holds, per service, each write's original exchange that had
	// not settled cleanly when the write returned, until a Drain reports it
	// done; see own. It fences that service alone; a failed response body
	// Close for good in this process. Made on first use.
	owners map[string][]*serviceOwner
	// token is the last offer token Drain issued; zero is never one, and
	// none is issued twice. At its maximum no more are.
	token   uint64
	active  int // calls, forward frames and exchanges with originals pending
	stopped bool
	drained chan struct{}
}

var _ serving.Runtime = (*serviceRuntime)(nil)

// newServiceRuntime returns the service runtime of target, built by
// loadTarget from config, which must not change afterwards, under cfg. It
// makes no request.
func newServiceRuntime(target backend.Target, config *rest.Config, cfg ServiceConfig) (*serviceRuntime, error) {
	b, ok := target.Backend.(*Backend)
	if !ok || b == nil || b.binding == "" || b.binding != target.Binding {
		return nil, errors.New("kubernetes service runtime requires a bound kubernetes target")
	}
	if config == nil {
		return nil, errors.New("kubernetes configuration is required")
	}
	if cfg.target != target.Name {
		return nil, errors.New("kubernetes service profiles name another target")
	}
	c := rest.CopyConfig(config)
	host, err := canonicalServer(c.Host)
	if err != nil {
		return nil, err
	}
	c.Host = host
	if c.CAFile != "" || c.Transport != nil || c.Dial != nil || c.WrapTransport != nil {
		return nil, errors.New("kubernetes service runtime requires the target's frozen configuration")
	}
	binding, err := targetBinding(TargetConfig{Namespace: b.namespace, Labels: target.Labels, Resources: b.policy}, c, b.data, b.dataQual)
	if err != nil || binding != b.binding {
		return nil, errors.New("kubernetes service configuration differs from target binding")
	}
	writer, err := newServiceHTTP(c, b.namespace)
	if err != nil {
		return nil, err
	}
	port, err := newEnvTransport(c, b.namespace)
	if err != nil {
		return nil, err
	}
	effects, endEffects := context.WithCancel(context.Background())
	return &serviceRuntime{b: b, target: target.Name, config: cfg, writer: writer, port: port, serve: port.servePort,
		effects: effects, endEffects: endEffects,
		locks: map[string]chan struct{}{}, declared: map[string]serving.Native{}, resolved: map[string]types.UID{},
		withdrawn: map[string]bool{}, drained: make(chan struct{})}, nil
}

// Check refuses what this runtime does not render: more or fewer than one
// replica, mounts, workdir, setup, and requests or limits above the
// profile's ceilings.
func (rt *serviceRuntime) Check(s serving.Spec) error {
	p := rt.config.profile
	switch {
	case s.Scaling.Replicas != 1:
		return fmt.Errorf("%w: replicas other than 1", serving.ErrUnsupported)
	case len(s.Mounts) != 0 || s.Workdir != nil || len(s.Setup) != 0:
		return fmt.Errorf("%w: service data", serving.ErrUnsupported)
	case s.Resources.CPUMillis > p.cpuMillis || s.Resources.CPULimitMillis > p.cpuMillis:
		return fmt.Errorf("%w: cpu above the target's ceiling", serving.ErrUnsupported)
	case s.Resources.MemoryBytes > p.memoryBytes || s.Resources.MemoryLimitBytes > p.memoryBytes:
		return fmt.Errorf("%w: memory above the target's ceiling", serving.ErrUnsupported)
	}
	return nil
}

// Admit refuses an image the profile does not grant to principal.
func (rt *serviceRuntime) Admit(principal string, s serving.Spec) error {
	if !rt.config.granted(principal, s.Image) {
		return auth.ErrPermissionDenied
	}
	return nil
}

// begin admits one call or frame for service id and returns its release.
// It refuses once stop was called.
func (rt *serviceRuntime) begin() (func(), error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stopped {
		return nil, errServiceStopped
	}
	rt.active++
	return func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		rt.active--
		rt.checkDrainedLocked()
	}, nil
}

// lock serializes Apply and Delete of service id.
func (rt *serviceRuntime) lock(ctx context.Context, id string) (func(), error) {
	rt.mu.Lock()
	l := rt.locks[id]
	if l == nil {
		l = make(chan struct{}, 1)
		rt.locks[id] = l
	}
	rt.mu.Unlock()
	select {
	case l <- struct{}{}:
		return func() { <-l }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// call runs f for service id under the runtime's admission, the service's
// lock and a context that also ends when the runtime stops.
func (rt *serviceRuntime) call(ctx context.Context, id string, f func(context.Context) error) error {
	release, err := rt.begin()
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(rt.effects, cancel)
	defer stop()
	unlock, err := rt.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	return f(ctx)
}

// takeDeclared reports whether native is the Native in which this process
// declared a create for service id that it has not sent, and if so forgets
// the declaration: the create is sent at most once.
func (rt *serviceRuntime) takeDeclared(id string, native serving.Native) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if d, ok := rt.declared[id]; ok && d == native {
		delete(rt.declared, id)
		return true
	}
	return false
}

func (rt *serviceRuntime) declare(id string, native serving.Native) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.declared[id] = native
}

func (rt *serviceRuntime) resolve(declaration string, uid types.UID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.resolved[declaration] = uid
}

func (rt *serviceRuntime) resolvedUID(declaration string) types.UID {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.resolved[declaration]
}

// withdraw records that this process declared, and will never send, the
// create of declaration. It resolves the declaration as not applied for
// this process only: after a restart it is unresolved again.
func (rt *serviceRuntime) withdraw(declaration string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.withdrawn[declaration] = true
}

func (rt *serviceRuntime) isWithdrawn(declaration string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.withdrawn[declaration]
}

var (
	// errServiceFactsUnacknowledged reports an owned exchange with facts
	// its write did not report and no Ack has acknowledged.
	errServiceFactsUnacknowledged = errors.New("kubernetes service exchange facts unacknowledged")
	// errServiceCustody reports a Drain or Ack whose Ref is not the one
	// an owned exchange was admitted under, with its accepted Native.
	errServiceCustody = errors.New("kubernetes service exchange custody mismatch")
	// errServiceTokensExhausted reports that no offer token is left. It
	// holds every exchange with facts to offer for good in this process.
	errServiceTokensExhausted = errors.New("kubernetes service offer tokens exhausted")
)

// A serviceOwner is the runtime's custody of one write's original
// exchange, x, made under ref; ref.Native is the Native its write
// returned, or the last offer's that Ack acknowledged. delivered are the
// facts that return or offer reported. offer is the outstanding offer, if
// any. project, for a create whose answer had not returned with its write,
// returns the Native that answer makes and its error.
type serviceOwner struct {
	x         *serviceExchange
	ref       serving.Ref
	delivered exchangeFacts
	offer     *serviceOffer
	project   func(serviceAnswer) (serving.Native, error)
}

// A serviceOffer is one immutable offer of an owned exchange's facts: the
// Native they make and every cause of the exchange as of those facts.
type serviceOffer struct {
	token  uint64
	native serving.Native
	facts  exchangeFacts
	err    error
}

// pins reports whether ref names the service, owner, nonce and binding ow
// was admitted under. Its Native is checked apart.
func (ow *serviceOwner) pins(ref serving.Ref) bool {
	r := ow.ref
	return ref.Service == r.Service && ref.Owner == r.Owner && ref.Nonce == r.Nonce && ref.Binding == r.Binding
}

// held returns every cause for which ow still fences its service, or nil
// once its originals have returned cleanly and each of its facts has been
// acknowledged.
func (ow *serviceOwner) held() error {
	f := ow.x.facts()
	if f.settled() && !ow.x.failed(f) && ow.offer == nil && f == ow.delivered {
		return nil
	}
	err := ow.x.cause(f)
	if ow.offer != nil || f != ow.delivered {
		err = errors.Join(err, errServiceFactsUnacknowledged)
	}
	return err
}

// own takes custody of a, the answer of a write made under ref whose
// Native, as the write returns it, is native, before the write's outcome is
// reported, and returns the causes of a's originals that had not returned,
// and of a failed response body Close, as a reflects them.
//
// An exchange whose originals had all returned, with no failed Close,
// needs no custody. Any other fences ref.Service alone until a Drain
// reports it done: until its originals have returned, each later fact of
// it has been offered and acknowledged, and for good if its Close failed.
// Its originals keep the runtime's admission until they return, so that
// join retains every dependency they may use.
func (rt *serviceRuntime) own(ref serving.Ref, a serviceAnswer, native serving.Native, project func(serviceAnswer) (serving.Native, error)) error {
	x, f := a.x, a.facts
	if x == nil {
		x = answeredExchange(a)
		f = x.facts()
	}
	err := x.pending(f)
	if f.settled() && !x.failed(f) {
		return err
	}
	ref.Native = native
	ow := &serviceOwner{x: x, ref: ref, delivered: f}
	if !f.answered {
		ow.project = project
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.owners == nil {
		rt.owners = map[string][]*serviceOwner{}
	}
	rt.owners[ref.Service] = append(rt.owners[ref.Service], ow)
	if !x.facts().settled() {
		rt.active++
		go func() {
			x.join()
			rt.mu.Lock()
			defer rt.mu.Unlock()
			rt.active--
			rt.checkDrainedLocked()
		}()
	}
	return err
}

// closeFence returns every cause of service id's owned exchanges, or nil
// if it has none still held. Other services are not fenced.
func (rt *serviceRuntime) closeFence(id string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.fenceLocked(id)
}

func (rt *serviceRuntime) fenceLocked(id string) error {
	var errs []error
	for _, ow := range rt.owners[id] {
		if err := ow.held(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("kubernetes service %s: %w", id, err)
	}
	return nil
}

// closeFailures returns the causes of every service's owned exchanges.
func (rt *serviceRuntime) closeFailures() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	ids := make([]string, 0, len(rt.owners))
	for id := range rt.owners {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var errs []error
	for _, id := range ids {
		errs = append(errs, rt.fenceLocked(id))
	}
	return errors.Join(errs...)
}

// Drain reports the facts of ref.Service's owned exchanges that ref, whose
// Native the caller has committed, does not reflect. It makes no request,
// writes nothing and may be called after stop.
//
// A nonzero token is an offer: the Native those facts make of ref's and
// every cause of the exchange as of them. Drain repeats the same offer
// until Ack acknowledges its token. Facts arriving meanwhile are offered
// after it under another token, even if they leave the Native unchanged.
//
// With token zero there is no offer and native is ref's. Then err reports
// originals that have not returned, a failed response body Close, a Ref
// that is not an owned exchange's or exhausted tokens; nil reports that
// every original has returned cleanly and each of its facts has been
// acknowledged, and the custody ends.
func (rt *serviceRuntime) Drain(ref serving.Ref) (native serving.Native, token uint64, err error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var errs []error
	owners := rt.owners[ref.Service]
	for _, ow := range owners {
		o := ow.offer
		if !ow.pins(ref) || ref.Native != ow.ref.Native && (o == nil || ref.Native != o.native) {
			return ref.Native, 0, fmt.Errorf("%w: kubernetes service %s", errServiceCustody, ref.Service)
		}
		if o != nil {
			return o.native, o.token, o.err
		}
		f := ow.x.facts()
		if f == ow.delivered {
			if err := ow.held(); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if rt.token == ^uint64(0) {
			return ref.Native, 0, fmt.Errorf("%w: kubernetes service %s: %w", errServiceTokensExhausted, ref.Service, ow.x.cause(f))
		}
		rt.token++
		o = &serviceOffer{token: rt.token, native: ow.ref.Native, facts: f, err: ow.x.cause(f)}
		if f.answered && !ow.delivered.answered && ow.project != nil {
			n, err := ow.project(ow.x.answer(f))
			o.native, o.err = n, errors.Join(err, o.err)
		}
		ow.offer = o
		return o.native, o.token, o.err
	}
	owners = slices.DeleteFunc(owners, func(ow *serviceOwner) bool { return ow.held() == nil })
	if len(owners) == 0 {
		delete(rt.owners, ref.Service)
	} else {
		rt.owners[ref.Service] = owners
	}
	if err := errors.Join(errs...); err != nil {
		return ref.Native, 0, fmt.Errorf("kubernetes service %s: %w", ref.Service, err)
	}
	return ref.Native, 0, nil
}

// Ack acknowledges the offer Drain made under token, once the caller has
// committed its Native: ref must be the owned exchange's, with the offer's
// Native. It acknowledges that offer's facts only, never later ones, and
// never reports the exchange done; only a later Drain does. Any other
// token, including zero, one already acknowledged and one of an exchange
// whose custody ended, is an error and changes nothing.
func (rt *serviceRuntime) Ack(ref serving.Ref, token uint64) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, ow := range rt.owners[ref.Service] {
		o := ow.offer
		if token == 0 || o == nil || o.token != token {
			continue
		}
		if !ow.pins(ref) || ref.Native != o.native {
			return fmt.Errorf("%w: kubernetes service %s: offer %d", errServiceCustody, ref.Service, token)
		}
		ow.ref.Native, ow.delivered, ow.offer = o.native, o.facts, nil
		return nil
	}
	return fmt.Errorf("kubernetes service %s: no offer %d", ref.Service, token)
}

// serviceKind names one of the two owned objects.
type serviceKind int

const (
	deploymentObject serviceKind = iota
	serviceObjectKind
)

func (k serviceKind) of(n *serviceNative) *serviceObject {
	if k == deploymentObject {
		return &n.Deployment
	}
	return &n.Service
}

// render returns object k of ref as Apply creates it under declaration,
// and the witnesses of its rendering: its spec and, for the Deployment, its
// Pod template.
func (rt *serviceRuntime) render(ref serving.Ref, rev serving.Revision, n serviceNative, k serviceKind, declaration string) (obj any, spec, pod string) {
	ns := rt.b.namespace
	if k == deploymentObject {
		d := serviceDeployment(ns, ref, rev, n.Generation, declaration, rt.config.profile)
		return d, deploymentIdentity(d).witness(), templateIdentity(ns, d.Spec.Template).witness()
	}
	s := serviceService(ns, ref, n.Port, declaration)
	return s, serviceIdentity(s).witness(), ""
}

// Apply makes the native state of ref run rev as rollout generation. Each
// owned object is created by a declared, committed, send-once create: the
// call that decides to create declares it, with the witnesses of its exact
// rendering, in the Native it returns, with errServiceWritePending, and the
// next call with that Native committed sends it. An object is the
// service's only if it is exactly its rendering and has the UID the Native
// records or, for a create whose response was lost, carries its
// declaration; never by name or labels. Apply never re-creates a recorded
// object, and a recorded object that is missing, replaced or changed is
// ErrIdentityConflict.
func (rt *serviceRuntime) Apply(ctx context.Context, ref serving.Ref, rev serving.Revision, generation uint64) (out serving.Native, err error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	if err := rt.Check(rev.Spec); err != nil {
		return "", err
	}
	if generation != 1 {
		return "", fmt.Errorf("%w: rollouts after the first", serving.ErrUnsupported)
	}
	err = rt.call(ctx, ref.Service, func(ctx context.Context) error {
		n, err := decodeServiceNative(ref.Native)
		if err != nil {
			return err
		}
		if n.Generation == 0 {
			n.Generation, n.Port = generation, rev.Spec.Port
		}
		if n.Generation != generation || n.Port != rev.Spec.Port {
			return errors.New("kubernetes service native state records another rollout")
		}
		// An original response Close of this service that has not
		// returned, or that failed, fences it: no declaration, read or
		// send. A refused create is still reported as one.
		if err := rt.closeFence(ref.Service); err != nil {
			if n.Deployment.Refused || n.Service.Refused {
				return errors.Join(fmt.Errorf("%w: kubernetes service create refused", serving.ErrIdentityConflict), err)
			}
			return err
		}
		for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
			changed, err := rt.ensure(ctx, ref, rev, &n, k)
			if changed {
				out = n.encode()
			}
			if err != nil || changed {
				return err
			}
		}
		return rt.verify(ctx, ref, n)
	})
	return out, err
}

// ensure creates object k of ref under its declaration. It reports whether
// n changed; a changed n must be committed before any other call.
func (rt *serviceRuntime) ensure(ctx context.Context, ref serving.Ref, rev serving.Revision, n *serviceNative, k serviceKind) (bool, error) {
	o := k.of(n)
	switch {
	case o.UID != "":
		return false, nil
	case o.Refused:
		return false, fmt.Errorf("%w: kubernetes service create refused", serving.ErrIdentityConflict)
	case !o.declared():
		id, err := declarationID()
		if err != nil {
			return false, err
		}
		_, o.Spec, o.Pod = rt.render(ref, rev, *n, k, id)
		o.Declaration = id
		rt.declare(ref.Service, n.encode())
		return true, errServiceWritePending
	case rt.takeDeclared(ref.Service, ref.Native):
		obj, spec, pod := rt.render(ref, rev, *n, k, o.Declaration)
		if spec != o.Spec || pod != o.Pod {
			// Not possible within one process, whose profile is fixed;
			// the create is never sent.
			rt.withdraw(o.Declaration)
			return false, errServiceNotRendered
		}
		o.Sent = true
		sent := *n
		a := rt.writer.create(ctx, k, obj)
		err := rt.created(o, k, a)
		// An answer arriving after the write returned is only offered, by
		// Drain, as the Native it makes of the one returned now.
		project := func(a serviceAnswer) (serving.Native, error) {
			m := sent
			err := rt.answered(k.of(&m), k, a)
			return m.encode(), err
		}
		return true, errors.Join(err, rt.own(ref, a, n.encode(), project))
	}
	// Declared before, by this process with a lost response or by an
	// earlier one: only an object that is exactly its rendering and carries
	// its declaration resolves it.
	uid, ok, err := rt.carrier(ctx, ref, *o, k)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errServiceCreateUnresolved
	}
	o.UID = uid
	return true, nil
}

// created records in o the outcome of o's one create, answered a.
//
// Only a complete 201 Created whose object is exactly the rendering grants
// o its UID. Every other complete answer — a 201 of another object, a 200, a
// 409, a 429, a 5xx, a redirect or any other refusal — refuses the create
// for good, with the answer's bounded witness: its status and the size and
// SHA-256 of its body. No later read accepts it, not of an object carrying
// its declaration and not of a healthy object at its name, and it is never
// sent again. A UID such an answer reports is never recorded.
//
// A refusal is not proof that nothing was created: the API server may have
// applied the write before it answered. Observe and Delete never count a
// refused create gone, and nothing is deleted on its account; the service
// keeps its name, charges and custody until an exact native proof is
// established outside this runtime.
//
// An answer that did not arrive, or whose body is incomplete, leaves the
// create unresolved: it may have been applied, and only a read of the exact
// object can resolve it. Nothing is resent.
//
// The answer's original response Close, and its other originals, are
// separate facts. Their outcome changes none of the above: an accepted UID
// is kept, and a refusal stays one. The caller's own returns the causes of
// those that have not returned or failed, and holds the service meanwhile.
// An answer that arrives only after the write returned is the same
// exchange's: Drain offers what it makes of the write's Native.
func (rt *serviceRuntime) created(o *serviceObject, k serviceKind, a serviceAnswer) error {
	err := rt.answered(o, k, a)
	if o.UID != "" {
		rt.resolve(o.Declaration, o.UID)
	}
	return err
}

// answered records a's outcome in o, as created describes, and nothing
// else; it takes no lock.
func (rt *serviceRuntime) answered(o *serviceObject, k serviceKind, a serviceAnswer) error {
	switch {
	case a.Status == 0:
		return fmt.Errorf("kubernetes service create sent with no answer: %w", a.Err)
	case !a.Complete:
		return fmt.Errorf("kubernetes service create answer %d incomplete: %w", a.Status, a.Err)
	case a.Status == http.StatusCreated:
		obj, decoded := decodeAnswer(k, a.Body)
		var uid types.UID
		var witness string
		switch got := obj.(type) {
		case *appsv1.Deployment:
			uid, witness = got.UID, deploymentIdentity(got).witness()
		case *corev1.Service:
			uid, witness = got.UID, serviceIdentity(got).witness()
		}
		if !decoded || !o.accepts(uid, witness) {
			o.Refused, o.Refusal = true, a.refusal(decoded)
			return errServiceNotRendered
		}
		o.UID = uid
		return nil
	}
	decoded := statusBody(a.Body)
	if a.Status/100 == 2 {
		_, decoded = decodeAnswer(k, a.Body)
	}
	o.Refused, o.Refusal = true, a.refusal(decoded)
	return fmt.Errorf("%w: kubernetes service create answered %d", serving.ErrIdentityConflict, a.Status)
}

// served is an object read at a service's name.
type served struct {
	uid      types.UID
	deleting bool   // it has a deletion timestamp
	witness  string // of its identity projection
	d        *appsv1.Deployment
}

// get reads object k at ref's name, or returns nil if none is found. A read
// error is not absence.
func (rt *serviceRuntime) get(ctx context.Context, ref serving.Ref, k serviceKind) (*served, error) {
	ctx, cancel := context.WithTimeout(ctx, serviceReadTimeout)
	defer cancel()
	name := serviceObjectName(ref.Service)
	if k == deploymentObject {
		d, err := rt.b.client.AppsV1().Deployments(rt.b.namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &served{uid: d.UID, deleting: d.DeletionTimestamp != nil, witness: deploymentIdentity(d).witness(), d: d}, nil
	}
	s, err := rt.b.client.CoreV1().Services(rt.b.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &served{uid: s.UID, deleting: s.DeletionTimestamp != nil, witness: serviceIdentity(s).witness()}, nil
}

// carrier reads object k at ref's name and reports its UID if it is
// exactly o's rendering, which carries o's declaration and ref's nonce,
// service and owner. A declaration resolved earlier in this process
// returns its accepted UID. A read error is not absence.
func (rt *serviceRuntime) carrier(ctx context.Context, ref serving.Ref, o serviceObject, k serviceKind) (types.UID, bool, error) {
	if uid := rt.resolvedUID(o.Declaration); uid != "" {
		return uid, true, nil
	}
	s, err := rt.get(ctx, ref, k)
	if err != nil || s == nil {
		return "", false, err
	}
	if s.deleting || !o.accepts(s.uid, s.witness) {
		return "", false, nil
	}
	rt.resolve(o.Declaration, s.uid)
	return s.uid, true, nil
}

// verify checks that both recorded objects exist, with their recorded UIDs,
// exactly as rendered and not deleting.
func (rt *serviceRuntime) verify(ctx context.Context, ref serving.Ref, n serviceNative) error {
	for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
		s, err := rt.get(ctx, ref, k)
		if err != nil {
			return err
		}
		if s == nil || s.deleting || !k.of(&n).accepts(s.uid, s.witness) {
			return fmt.Errorf("%w: recorded kubernetes service object missing, replaced or changed", serving.ErrIdentityConflict)
		}
	}
	return nil
}

// Observe reads the native state of ref, counting only objects of its
// recorded identity. A declared create that is unresolved is never Absent;
// a refused create, whatever its answer's status, is IdentityConflict, never
// gone. A recorded object that is gone while another remains, or that was
// replaced or changed, is IdentityConflict.
func (rt *serviceRuntime) Observe(ctx context.Context, ref serving.Ref) (o serving.Observation, err error) {
	if err := ref.Validate(); err != nil {
		return o, err
	}
	release, err := rt.begin()
	if err != nil {
		return o, err
	}
	defer release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(rt.effects, cancel)()
	n, err := decodeServiceNative(ref.Native)
	if err != nil {
		return o, err
	}
	// No observation, which could establish absence, is made while an
	// original response Close of this service has not returned or failed.
	if err := rt.closeFence(ref.Service); err != nil {
		return o, err
	}
	return rt.observe(ctx, ref, n)
}

func (rt *serviceRuntime) observe(ctx context.Context, ref serving.Ref, n serviceNative) (serving.Observation, error) {
	var objs [2]*served
	present, gone, deleting := 0, 0, false
	for i, k := range []serviceKind{deploymentObject, serviceObjectKind} {
		o := k.of(&n)
		if o.Refused {
			// Whatever the refusing answer said, the create may have been
			// applied. Its object is not this service's and is never
			// deleted, so its absence is never established here.
			return serving.Observation{Reason: serving.IdentityConflict}, nil
		}
		uid := o.UID
		if o.unresolved() && !rt.isWithdrawn(o.Declaration) {
			if uid = rt.resolvedUID(o.Declaration); uid == "" {
				return serving.Observation{}, errServiceCreateUnresolved
			}
		}
		if uid == "" {
			gone++
			continue
		}
		s, err := rt.get(ctx, ref, k)
		if err != nil {
			return serving.Observation{}, err
		}
		switch {
		case s == nil:
			gone++
		case s.uid != uid:
			return serving.Observation{Reason: serving.IdentityConflict}, nil
		default:
			present++
			objs[i] = s
			deleting = deleting || s.deleting
		}
	}
	if present == 0 {
		return serving.Observation{Absent: true}, nil
	}
	if gone != 0 || objs[0] == nil {
		return serving.Observation{Reason: serving.IdentityConflict}, nil
	}
	if deleting {
		// Present, being deleted: neither ready nor absent.
		return serving.Observation{}, nil
	}
	for i, k := range []serviceKind{deploymentObject, serviceObjectKind} {
		if !k.of(&n).accepts(objs[i].uid, objs[i].witness) {
			return serving.Observation{Reason: serving.IdentityConflict}, nil
		}
	}
	return rt.readiness(ctx, ref, n, objs[0].d)
}

// owned reads the ReplicaSets, Pods and EndpointSlices of ref and returns
// the Pods of the accepted Deployment d, each of which must be exactly a
// Pod of its template, and the UIDs of those the owned Service lists as
// ready endpoints. A Pod of d that is not is errServiceNotRendered.
func (rt *serviceRuntime) owned(ctx context.Context, ref serving.Ref, n serviceNative, d *appsv1.Deployment) ([]corev1.Pod, map[types.UID]bool, error) {
	ns, name := rt.b.namespace, serviceObjectName(ref.Service)
	selector := labels.SelectorFromSet(serviceSelector(ref)).String()
	sets, err := rt.b.client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, nil, err
	}
	pods, err := rt.b.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, nil, err
	}
	eps, err := rt.b.client.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + name})
	if err != nil {
		return nil, nil, err
	}
	owned := ownedPods(sets.Items, pods.Items, d)
	for i := range owned {
		if podIdentity(&owned[i]).witness() != n.Deployment.Pod {
			return nil, nil, errServiceNotRendered
		}
	}
	return owned, readyEndpoints(eps.Items, ns, name, n.Service.UID, n.Port), nil
}

// readiness reads the accepted Deployment d's rollout, its owned Pods and
// the ready endpoints of the owned Service.
func (rt *serviceRuntime) readiness(ctx context.Context, ref serving.Ref, n serviceNative, d *appsv1.Deployment) (serving.Observation, error) {
	ctx, cancel := context.WithTimeout(ctx, serviceReadTimeout)
	defer cancel()
	owned, endpoints, err := rt.owned(ctx, ref, n, d)
	if errors.Is(err, errServiceNotRendered) {
		return serving.Observation{Reason: serving.IdentityConflict}, nil
	}
	if err != nil {
		return serving.Observation{}, err
	}
	o := serving.Observation{DesiredReplicas: 1}
	if d.Spec.Replicas != nil {
		o.DesiredReplicas = *d.Spec.Replicas
	}
	rollout, err := strconv.ParseUint(d.Annotations[serviceGeneration], 10, 64)
	if err != nil || rollout != n.Generation {
		return serving.Observation{Reason: serving.IdentityConflict}, nil
	}
	if d.Status.ObservedGeneration >= d.Generation {
		o.ObservedGeneration = rollout
	}
	for i := range owned {
		if podReady(&owned[i]) {
			o.ReadyReplicas++
			if endpoints[owned[i].UID] {
				o.ReadyEndpoints++
			}
		}
	}
	o.UpdatedReplicas = min(d.Status.UpdatedReplicas, int32(len(owned)))
	o.AvailableReplicas = min(d.Status.AvailableReplicas, o.ReadyReplicas)
	if o.ObservedGeneration == rollout && d.Status.Replicas == o.DesiredReplicas &&
		o.UpdatedReplicas == o.DesiredReplicas && o.ReadyReplicas == o.DesiredReplicas && o.AvailableReplicas == o.DesiredReplicas {
		o.ReadyGeneration = rollout
	}
	if o.ReadyGeneration == 0 && o.ObservedGeneration == rollout {
		if progressDeadline(d) {
			o.Reason = serving.ProgressDeadline
		} else {
			o.Reason = serviceReason(owned)
		}
	}
	return o, nil
}

// Delete removes the objects of ref's recorded identity, each with its UID
// as a precondition; the Deployment in the foreground, so that it remains
// until its Pods are gone. A delete with a UID precondition cannot remove
// another object, so it is not declared; after a lost answer the same
// delete is sent again, with the same precondition, by a later call. It
// deletes only an object recorded by UID, or one that is exactly the
// rendering of an unresolved declaration; never anything on account of a
// refused create, never with a guessed UID, a forced grace period or a
// finalizer removal, and never the borrowed namespace, service account or
// pull secrets. It reports Absent only once every recorded object, and the
// object of every declared create, is observed gone in a fresh read after
// ref's Native; an unresolved or refused create is never Absent, and no
// delete answer's status is taken as proof of absence.
func (rt *serviceRuntime) Delete(ctx context.Context, ref serving.Ref) (o serving.Observation, err error) {
	if err := ref.Validate(); err != nil {
		return o, err
	}
	err = rt.call(ctx, ref.Service, func(ctx context.Context) error {
		n, err := decodeServiceNative(ref.Native)
		if err != nil {
			return err
		}
		// Nothing is read, resolved or deleted while an original
		// response Close of this service has not returned or has failed.
		if err := rt.closeFence(ref.Service); err != nil {
			return err
		}
		// A create this process declared and has not sent is never sent
		// now; this process knows it was not applied.
		if rt.takeDeclared(ref.Service, ref.Native) {
			for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
				if obj := k.of(&n); obj.unresolved() && !obj.Sent {
					rt.withdraw(obj.Declaration)
				}
			}
		}
		for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
			obj := k.of(&n)
			uid := obj.UID
			if obj.unresolved() && !rt.isWithdrawn(obj.Declaration) {
				var ok bool
				if uid, ok, err = rt.carrier(ctx, ref, *obj, k); err != nil {
					return err
				} else if !ok {
					continue // observe reports it unresolved
				}
			}
			if uid == "" {
				continue
			}
			if err := rt.remove(ctx, ref, k, uid); err != nil {
				return err
			}
		}
		o, err = rt.observe(ctx, ref, n)
		return err
	})
	return o, err
}

// remove deletes object k of uid. A 404, or a 409 precondition conflict
// showing another object holds the name, means the object of uid is gone;
// only a later observation establishes absence. The answer's original
// response Close, if it has not returned or failed, fences the service
// before anything else is sent, and its cause is returned with any other,
// whatever the status.
func (rt *serviceRuntime) remove(ctx context.Context, ref serving.Ref, k serviceKind, uid types.UID) error {
	ctx, cancel := context.WithTimeout(ctx, serviceReadTimeout)
	defer cancel()
	a := rt.writer.delete(ctx, k, serviceObjectName(ref.Service), uid)
	cerr := rt.own(ref, a, ref.Native, nil)
	var err error
	switch {
	case a.Status == 0:
		err = fmt.Errorf("kubernetes service delete sent with no answer: %w", a.Err)
	case !a.Complete:
		err = fmt.Errorf("kubernetes service delete answer %d incomplete: %w", a.Status, a.Err)
	case a.Status == http.StatusOK, a.Status == http.StatusAccepted, a.Status == http.StatusNotFound, a.Status == http.StatusConflict:
	default:
		err = fmt.Errorf("kubernetes service delete answered %d", a.Status)
	}
	return errors.Join(err, cerr)
}

// Forward relays one request to a ready owned Pod of ref that the owned
// Service lists as a ready endpoint, on the port the Native records. It
// takes no destination from r, and sends the Pod only the headers
// serving.ForwardHeader keeps; the gateway writes that request, with a
// fixed Host, inside the operator-authenticated port-forward stream, so
// that the operator's credentials reach only the API server.
//
// The Deployment, Service, Pod and endpoint are read and checked once,
// before the tunnel is opened; the port-forward addresses the Pod by name,
// so a namespace writer that replaces the Pod between that check and the
// tunnel is not excluded: the check is point in time, not an atomic UID
// binding.
//
// Forward returns only once the exchange's frame has settled: every
// original owner — the request body, the response body's Close, the streams
// and the connection's Close — has returned. The gateway's handler may
// return before then; Forward keeps its admission, and the caller its
// forward, however long that takes. Cancellation is not their end.
func (rt *serviceRuntime) Forward(ctx context.Context, ref serving.Ref, w http.ResponseWriter, r *http.Request) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	n, err := decodeServiceNative(ref.Native)
	if err != nil {
		return err
	}
	if n.Deployment.UID == "" || n.Service.UID == "" || n.Port == 0 {
		return errors.New("kubernetes service has no recorded objects")
	}
	release, err := rt.begin()
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(rt.effects, cancel)()
	pod, err := rt.readyPod(ctx, ref, n)
	if err != nil {
		return err
	}
	in := r.Clone(ctx)
	in.Header = serving.ForwardHeader(r.Header)
	f := rt.serve(w, in, portTarget{Pod: pod, Port: n.Port})
	<-f.done
	return f.result().Err
}

// readyPod returns the name of a ready owned Pod of ref, exactly a Pod of
// its template, listed as a ready endpoint of the owned Service, after
// checking that the Deployment and Service are the recorded ones, exactly
// as rendered and not deleting.
func (rt *serviceRuntime) readyPod(ctx context.Context, ref serving.Ref, n serviceNative) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, serviceReadTimeout)
	defer cancel()
	var d *appsv1.Deployment
	for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
		s, err := rt.get(ctx, ref, k)
		if err != nil {
			return "", err
		}
		if s == nil || s.deleting || !k.of(&n).accepts(s.uid, s.witness) {
			return "", errors.New("kubernetes service objects are not the recorded ones")
		}
		if k == deploymentObject {
			d = s.d
		}
	}
	owned, endpoints, err := rt.owned(ctx, ref, n, d)
	if err != nil {
		return "", err
	}
	slices.SortFunc(owned, func(a, b corev1.Pod) int { return cmp.Compare(a.Name, b.Name) })
	for i := range owned {
		if podReady(&owned[i]) && endpoints[owned[i].UID] {
			return owned[i].Name, nil
		}
	}
	return "", errors.New("kubernetes service has no ready endpoint")
}

// verifyBorrowed reads the target's namespace, its default service account
// and each pull secret, and refuses any that differs from the profile. It
// sends only reads and never creates, modifies or deletes them.
func (rt *serviceRuntime) verifyBorrowed(ctx context.Context) error {
	c := rt.b.client.CoreV1()
	p := rt.config.profile
	ns, err := c.Namespaces().Get(ctx, rt.b.namespace, metav1.GetOptions{})
	if err != nil {
		return serviceReadError("namespace", err)
	}
	if ns.UID != p.namespaceUID {
		return fmt.Errorf("%w: kubernetes service namespace differs from profile", backend.ErrIdentityMismatch)
	}
	if ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating {
		return errors.New("kubernetes service namespace is terminating")
	}
	sa, err := c.ServiceAccounts(rt.b.namespace).Get(ctx, environmentServiceAccount, metav1.GetOptions{})
	if err != nil {
		return serviceReadError("service account", err)
	}
	if sa.UID != p.serviceAccountUID || sa.DeletionTimestamp != nil {
		return fmt.Errorf("%w: kubernetes service account differs from profile", backend.ErrIdentityMismatch)
	}
	for _, name := range p.pullSecrets {
		s, err := c.Secrets(rt.b.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return serviceReadError("image pull secret", err)
		}
		if s.Type != corev1.SecretTypeDockerConfigJson || s.DeletionTimestamp != nil {
			return fmt.Errorf("%w: kubernetes service image pull secret differs from profile", backend.ErrIdentityMismatch)
		}
	}
	return nil
}

// serviceReadError classifies a failed startup read of what, without the
// object's name.
func serviceReadError(what string, err error) error {
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("kubernetes service %s not found", what)
	}
	return fmt.Errorf("read kubernetes service %s", what)
}

// stop refuses every new call and frame and cancels those in progress. It
// returns at once; a canceled create may still land, and its declaration
// stays the only authority on whether it did. It is safe to call more than
// once.
func (rt *serviceRuntime) stop() {
	rt.mu.Lock()
	rt.stopped = true
	rt.checkDrainedLocked()
	rt.mu.Unlock()
	rt.endEffects()
}

// join calls stop and waits until every call, frame and owned exchange's
// originals have returned, or ctx ends. ctx ending is not their return. It
// is never clean while an owned exchange is held: an original has not
// returned, a response body Close failed, or a fact is unacknowledged.
func (rt *serviceRuntime) join(ctx context.Context) error {
	rt.stop()
	select {
	case <-rt.drained:
		return rt.closeFailures()
	case <-ctx.Done():
	}
	rt.mu.Lock()
	n := rt.active
	rt.mu.Unlock()
	if n == 0 {
		return rt.closeFailures()
	}
	return errors.Join(fmt.Errorf("%d kubernetes service calls and frames unsettled", n), ctx.Err(), rt.closeFailures())
}

func (rt *serviceRuntime) checkDrainedLocked() {
	if !rt.stopped || rt.active != 0 {
		return
	}
	select {
	case <-rt.drained:
	default:
		close(rt.drained)
	}
}

func declarationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate declaration: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
