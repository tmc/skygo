package kube

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	"k8s.io/client-go/rest"
)

// DRAOptions configures NewDRAIntegration.
type DRAOptions struct {
	// ExecutionState is the private execution state directory of
	// FromTargetWithState; every target of the integration uses it.
	ExecutionState string

	// State is the private DRA state directory: an existing absolute
	// directory without symlinks, owned by the server user with mode 0700,
	// separate from ExecutionState. A fresh one is empty.
	State string

	// LogRetention is how long a witnessed succeeded Pod, and so its logs,
	// is kept before API cleanup deletes it and then its claim. Zero keeps
	// them until the operator acts.
	LogRetention time.Duration
}

// NewDRAIntegration returns the server integration that owns every target of
// targets for installation owner, and the DRA custody and maintenance of
// those with DRA accelerators. It is meant to be called from
// server.Config.Install, in place of a Targets callback.
//
// Every target is constructed once, as FromTargetWithState does with
// opts.ExecutionState; tasks without a DRA accelerator keep that behavior.
// Before any write it checks the DRA state directory read-only and refuses an
// unknown, legacy, foreign, mixed or corrupt one. Then, under ctx, it
// confirms resource.k8s.io/v1 and reads each DRA target's namespace and
// classes, which every later create rechecks; only then does it create state.
// Only these targets advertise their DRA accelerators.
//
// Each construction read is bounded by ctx and by 30 seconds, including the
// Close of its original response body.
//
// The integration's Run maintains every DRA record, including those the core
// no longer observes, until its context ends or Close; Close fences every
// operation of every target first, then joins Run, every original DRA
// operation and the original Close of every native response. A failed
// construction returns a nil Integration, unless the original Close of one
// of its reads is still held or failed: it then returns a partial Integration
// with the error, whose Close joins that original, so that the server keeps
// everything it depends on (server.Config.Install).
func NewDRAIntegration(ctx context.Context, targets Config, owner string, opts DRAOptions) (server.Integration, error) {
	in, err := newDRAIntegration(ctx, targets, owner, opts, nil)
	if in == nil {
		return nil, err // never a typed nil
	}
	return in, err
}

// newDRAIntegration is NewDRAIntegration; a non-nil wrap, used by tests,
// wraps the base transport of every target's receipt client.
func newDRAIntegration(ctx context.Context, targets Config, owner string, opts DRAOptions, wrap func(http.RoundTripper) http.RoundTripper) (*draIntegration, error) {
	if opts.LogRetention < 0 {
		return nil, errors.New("kubernetes dra log retention must be nonnegative")
	}
	if opts.State == "" || !filepath.IsAbs(opts.State) {
		return nil, errors.New("kubernetes dra state directory must be absolute")
	}
	if opts.ExecutionState == "" || !filepath.IsAbs(opts.ExecutionState) {
		return nil, errors.New("kubernetes dra requires an absolute execution state directory")
	}
	configs := targets.Targets()
	if !slices.ContainsFunc(configs, func(t TargetConfig) bool { return t.Resources.hasDRA() }) {
		return nil, errors.New("kubernetes dra integration requires a dra accelerator")
	}
	type built struct {
		target backend.Target
		config *rest.Config
		key    draScopeKey
		dra    bool
	}
	var all []built
	var keys []draScopeKey
	for _, tc := range configs {
		if tc.Data != nil {
			return nil, errors.New("kubernetes execution receipts do not cover data targets")
		}
		target, config, err := loadTarget(ctx, tc, owner)
		if err != nil {
			return nil, err
		}
		b := target.Backend.(*Backend)
		x := built{target: target, config: config, key: draScopeKey{owner, b.namespace, b.binding}, dra: tc.Resources.hasDRA()}
		if x.dra {
			if slices.Contains(keys, x.key) {
				return nil, errors.New("kubernetes dra targets must have distinct bindings")
			}
			keys = append(keys, x.key)
		}
		all = append(all, x)
	}
	// Read-only: nothing is written before the DRA root is accepted.
	if _, err := preflightDRAState(opts.State, opts.ExecutionState, owner, keys); err != nil {
		return nil, err
	}
	in := &draIntegration{interval: draInterval, startup: newDRAOriginals(), gate: new(draGate)}
	for i := range all {
		x := &all[i]
		target, err := withReceipts(x.target, x.config, opts.ExecutionState)
		if err != nil {
			return nil, err
		}
		x.target = target
		b := target.Backend.(*Backend)
		b.gate = in.gate
		if wrap != nil {
			c, err := receiptHTTP(b.client.CoreV1().RESTClient())
			if err != nil {
				return nil, err
			}
			t := c.Client.Transport.(*receiptTransport)
			t.base = wrap(t.base)
		}
	}
	type pinned struct {
		uid     string
		classes map[string]draClass
	}
	pins := make(map[draScopeKey]pinned)
	for _, x := range all {
		if !x.dra {
			continue
		}
		read, cancel := context.WithTimeout(ctx, draReadTimeout)
		read, f := withDRAFrame(read, in.startup, "")
		uid, classes, err := x.target.Backend.(*Backend).draDiscover(read)
		f.end()
		cancel()
		if err != nil {
			// An original still held, or whose Close failed, is owned
			// by the partial integration until its Close joins it.
			if held, failed := in.startup.state(""); held || failed != nil {
				return in, err
			}
			return nil, err
		}
		pins[x.key] = pinned{uid, classes}
	}
	stores, err := openDRAState(opts.State, opts.ExecutionState, owner, keys)
	if err != nil {
		return nil, err
	}
	for _, x := range all {
		if x.dra {
			b := x.target.Backend.(*Backend)
			p := pins[x.key]
			d := &draRuntime{store: stores[x.key], namespaceUID: p.uid, classes: p.classes,
				retention: opts.LogRetention, now: time.Now, orig: newDRAOriginals(), starting: make(map[string]int)}
			d.orig.persist = d.holdClosed
			b.dra = d
			x.target.AcceleratorNames = slices.Sorted(maps.Keys(b.policy.Accelerators))
			in.backends = append(in.backends, b)
		}
		in.targets = append(in.targets, x.target)
	}
	return in, nil
}

// draIntegration owns a set of targets and the DRA custody of some of them.
// Its only goroutine is the caller of Run; the original Close of a native
// response may run on a goroutine counted before it starts (draCall).
type draIntegration struct {
	targets  []backend.Target
	backends []*Backend    // those with DRA
	startup  *draOriginals // of construction reads
	gate     *draGate      // admits operations of every owned Backend
	interval time.Duration

	mu      sync.Mutex
	closed  bool
	runDone chan struct{} // closed when Run returns; nil until Run starts
	stopRun func()        // cancels Run's context; nil until Run starts
	lastErr error         // of the last maintenance pass, for diagnostics
}

// draGate admits the operations of every Backend an integration owns, those
// without DRA included, so that its Close fences them all at once, before it
// joins anything, and a cached target cannot operate after Close.
type draGate struct {
	mu     sync.Mutex
	closed bool
	ops    int
	idle   chan struct{} // closed when ops returns to zero; nil while zero
}

// enter admits one operation; the caller calls leave once it returned. A
// nil gate, of a Backend no integration owns, admits everything.
func (g *draGate) enter() (leave func(), err error) {
	if g == nil {
		return func() {}, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errDRAClosed
	}
	if g.ops == 0 {
		g.idle = make(chan struct{})
	}
	g.ops++
	return sync.OnceFunc(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.ops--; g.ops == 0 {
			close(g.idle)
			g.idle = nil
		}
	}), nil
}

func (g *draGate) fence() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (g *draGate) join(ctx context.Context) error {
	g.mu.Lock()
	idle := g.idle
	g.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kubernetes integration operations remain held: %w", ctx.Err())
	}
}

var _ server.Integration = (*draIntegration)(nil)

// Targets returns the integration's targets.
func (in *draIntegration) Targets() []backend.Target { return slices.Clone(in.targets) }

// Run maintains the DRA records of every DRA target, at once and then every
// interval, until ctx ends or Close cancels it. Each pass is bounded per
// record and canceled with ctx, so Run returns promptly. A pass's errors do
// not end Run: they leave records as they are, for the next pass.
func (in *draIntegration) Run(ctx context.Context) error {
	in.mu.Lock()
	if in.closed || in.runDone != nil {
		in.mu.Unlock()
		return errors.New("kubernetes dra integration cannot run again")
	}
	// Close's cancellation and join are registered before any work.
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	in.runDone, in.stopRun = done, cancel
	in.mu.Unlock()
	defer close(done)
	defer cancel()
	t := time.NewTicker(in.interval)
	defer t.Stop()
	for {
		var errs []error
		for _, b := range in.backends {
			if ctx.Err() != nil {
				break
			}
			errs = append(errs, b.maintainDRA(ctx))
		}
		in.mu.Lock()
		in.lastErr = errors.Join(errs...)
		in.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Close first refuses every further operation of every target and native
// call, and cancels Run; only then does it join Run, if it ran, every
// original operation and the original Close of every native response, until
// ctx ends. It returns an error, and the server then keeps everything the
// integration depends on, if any of them is still held, an original Close
// failed or a DRA log body's Close was uncertain. A Close that returned an
// error never readmits anything. Close never deletes a claim or Pod: their
// records remain for the next process.
func (in *draIntegration) Close(ctx context.Context) error {
	in.mu.Lock()
	in.closed = true
	done, stop := in.runDone, in.stopRun
	in.mu.Unlock()
	in.gate.fence()
	in.startup.fence()
	for _, b := range in.backends {
		b.dra.fence()
	}
	if stop != nil {
		stop()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("kubernetes dra maintenance remains held: %w", ctx.Err())
		}
	}
	errs := []error{in.startup.join(ctx), in.gate.join(ctx)}
	for _, b := range in.backends {
		errs = append(errs, b.dra.close(ctx))
	}
	return errors.Join(errs...)
}
