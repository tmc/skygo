package leaderlease

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// The tests in this file are offline. leaseAPI is a labeled model of the
// Lease API, not a cluster: it keeps one Lease and enforces resourceVersion
// on update as the API server does, which the fake clientset does not.

type leaseAPI struct {
	mu     sync.Mutex
	lease  *coordinationv1.Lease // nil: not found
	rv     int
	err    map[string]error // injected original errors by verb
	writes int              // successful creates and updates
	skew   bool             // store recorded times in the year 2000

	// If hold is set, an update first sends on entered, if it can, and
	// then waits for hold to be closed. The fake clientset serializes
	// requests, so no other request proceeds meanwhile.
	hold    chan struct{}
	entered chan struct{}
}

func newLeaseAPI() (*fake.Clientset, *leaseAPI) {
	c := fake.NewSimpleClientset()
	a := &leaseAPI{err: map[string]error{}}
	c.PrependReactor("*", "leases", a.react)
	return c, a
}

func (a *leaseAPI) react(action ktesting.Action) (bool, runtime.Object, error) {
	if action.GetVerb() == "update" {
		a.mu.Lock()
		hold, entered := a.hold, a.entered
		a.mu.Unlock()
		if hold != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-hold
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	gr := coordinationv1.Resource("leases")
	if err := a.err[action.GetVerb()]; err != nil {
		return true, nil, err
	}
	switch action.GetVerb() {
	case "get":
		name := action.(ktesting.GetAction).GetName()
		if a.lease == nil {
			return true, nil, apierrors.NewNotFound(gr, name)
		}
		return true, a.lease.DeepCopy(), nil
	case "create":
		l := action.(ktesting.CreateAction).GetObject().(*coordinationv1.Lease)
		if a.lease != nil {
			return true, nil, apierrors.NewAlreadyExists(gr, l.Name)
		}
		a.store(l)
	case "update":
		l := action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease)
		if a.lease == nil {
			return true, nil, apierrors.NewNotFound(gr, l.Name)
		}
		if l.ResourceVersion != a.lease.ResourceVersion {
			return true, nil, apierrors.NewConflict(gr, l.Name, errors.New("modeled stale resourceVersion"))
		}
		a.store(l)
	default:
		return false, nil, nil
	}
	return true, a.lease.DeepCopy(), nil
}

func (a *leaseAPI) store(l *coordinationv1.Lease) {
	a.rv++
	a.writes++
	a.lease = l.DeepCopy()
	a.lease.ResourceVersion = strconv.Itoa(a.rv)
	if a.skew {
		old := metav1.NewMicroTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
		a.lease.Spec.RenewTime, a.lease.Spec.AcquireTime = &old, &old
	}
}

func (a *leaseAPI) holder() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lease == nil || a.lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *a.lease.Spec.HolderIdentity
}

func (a *leaseAPI) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.writes
}

func (a *leaseAPI) set(f func(*leaseAPI)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f(a)
}

// takeOver models another process writing the Lease as its holder.
func (a *leaseAPI) takeOver(holder string) {
	a.set(func(a *leaseAPI) {
		l := a.lease.DeepCopy()
		l.Spec.HolderIdentity = &holder
		a.store(l)
	})
}

func testConfig(identity string) Config {
	return Config{Namespace: "skygo", Name: "skygo-server", Identity: identity,
		LeaseDuration: time.Second, RenewDeadline: 300 * time.Millisecond, RetryPeriod: 50 * time.Millisecond}
}

// eventually waits at most d for f to hold. It is a bounded wait for a
// condition, not a sleep used as proof.
func eventually(d time.Duration, f func() bool) bool {
	deadline := time.Now().Add(d)
	for !f() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// closed reports whether c is closed, waiting at most d for it. With d <= 0
// it does not wait: a select between c and an expired timer would choose
// at random.
func closed(c <-chan struct{}, d time.Duration) bool {
	select {
	case <-c:
		return true
	default:
	}
	if d <= 0 {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-c:
		return true
	case <-t.C:
		return false
	}
}

// closed without a wait is exact: the old select against time.After(0)
// reported a closed channel open about half the time, so the repetition
// makes that failure certain in practice.
func TestClosed(t *testing.T) {
	open, done := make(chan struct{}), make(chan struct{})
	close(done)
	for range 100 {
		if !closed(done, 0) || closed(open, 0) {
			t.Fatal("closed without a wait is inexact")
		}
	}
	if !closed(done, time.Second) || closed(open, time.Millisecond) {
		t.Fatal("closed with a wait is inexact")
	}
}

func mustAcquire(t *testing.T, ctx context.Context, client *fake.Clientset, cfg Config) *Held {
	t.Helper()
	h, err := Acquire(ctx, client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	return h
}

func TestConfigValidate(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Config)
		ok   bool
	}{
		{"valid", func(*Config) {}, true},
		{"renew deadline equals duration", func(c *Config) { c.RenewDeadline = c.LeaseDuration }, false},
		{"renew deadline exceeds duration", func(c *Config) { c.RenewDeadline = 2 * c.LeaseDuration }, false},
		{"retry not jittered below deadline", func(c *Config) { c.RetryPeriod = c.RenewDeadline * 5 / 6 }, false},
		{"retry equals deadline", func(c *Config) { c.RetryPeriod = c.RenewDeadline }, false},
		{"max retry", func(c *Config) { c.RetryPeriod = math.MaxInt64 }, false},
		{"zero retry", func(c *Config) { c.RetryPeriod = 0 }, false},
		{"fractional duration", func(c *Config) { c.LeaseDuration = 1500 * time.Millisecond }, false},
		{"max int32 seconds", func(c *Config) { c.LeaseDuration = math.MaxInt32 * time.Second }, true},
		{"max int32 plus one seconds", func(c *Config) { c.LeaseDuration = (math.MaxInt32 + 1) * time.Second }, false},
		{"2^32+1 seconds", func(c *Config) { c.LeaseDuration = (1<<32 + 1) * time.Second }, false},
		{"no identity", func(c *Config) { c.Identity = "" }, false},
		{"identity with space", func(c *Config) { c.Identity = "a b" }, false},
		{"bad namespace", func(c *Config) { c.Namespace = "Skygo" }, false},
		{"bad name", func(c *Config) { c.Name = "" }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig("skygo-0_0123456789abcdef")
			tt.edit(&c)
			if err := c.validate(); (err == nil) != tt.ok {
				t.Fatalf("validate = %v, want ok %v", err, tt.ok)
			}
			client, a := newLeaseAPI()
			if !tt.ok {
				h, err := Acquire(context.Background(), client, c)
				if h != nil {
					h.Stop()
				}
				if err == nil || h != nil || a.count() != 0 || len(client.Actions()) != 0 {
					t.Fatalf("invalid configuration acquired: %v, actions %v", err, client.Actions())
				}
			}
		})
	}
}

// A RetryPeriod beyond the renew deadline is refused by its ordering, before
// the jittered product, whose float conversion would be out of range for
// time.Duration and so architecture-dependent.
func TestConfigValidateRetryOrder(t *testing.T) {
	c := testConfig("skygo-0_1")
	c.LeaseDuration, c.RenewDeadline, c.RetryPeriod = 3*time.Second, 2*time.Second, math.MaxInt64
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "retry period must be less than the renew deadline") {
		t.Fatalf("validate = %v", err)
	}
	client, a := newLeaseAPI()
	h, err := Acquire(context.Background(), client, c)
	if h != nil {
		h.Stop()
	}
	if err == nil || h != nil || a.count() != 0 || len(client.Actions()) != 0 {
		t.Fatalf("acquire = %v, actions %v", err, client.Actions())
	}
}

func TestNewIdentity(t *testing.T) {
	a, err := NewIdentity("skygo-0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewIdentity("skygo-0")
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !strings.HasPrefix(a, "skygo-0_") || len(a) != len("skygo-0_")+16 {
		t.Fatalf("identities %q %q", a, b)
	}
	for _, base := range []string{"", "a b", strings.Repeat("a", 201)} {
		if _, err := NewIdentity(base); err == nil {
			t.Errorf("NewIdentity(%q) accepted", base)
		}
	}
}

func TestAcquireRenewRelease(t *testing.T) {
	client, a := newLeaseAPI()
	cfg := testConfig("skygo-0_1")
	h := mustAcquire(t, context.Background(), client, cfg)
	if a.holder() != cfg.Identity || !h.Valid(time.Now()) || h.Err() != nil {
		t.Fatalf("holder %q valid %v err %v", a.holder(), h.Valid(time.Now()), h.Err())
	}
	n := a.count()
	if !eventually(5*time.Second, func() bool { return a.count() >= n+3 }) {
		t.Fatal("lease not renewed")
	}
	if err := h.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.holder() != "" || !closed(h.Lost(), 0) || !errors.Is(h.Err(), ErrReleased) || h.Valid(time.Now()) || !closed(h.done, 0) {
		t.Fatalf("after release: holder %q err %v", a.holder(), h.Err())
	}
	n = a.count()
	if err := h.Release(context.Background()); err == nil || a.count() != n {
		t.Fatal("second release wrote the lease")
	}
}

// A second process waits while the first renews, even past LeaseDuration,
// and takes the Lease over only after the first stopped renewing.
func TestAcquireWaitsForHolder(t *testing.T) {
	client, a := newLeaseAPI()
	first := mustAcquire(t, context.Background(), client, testConfig("skygo-0_1"))
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if h, err := Acquire(ctx, client, testConfig("skygo-0_2")); err == nil || h != nil || !errors.Is(err, context.DeadlineExceeded) {
		if h != nil {
			h.Stop()
		}
		t.Fatalf("second acquire = %v", err)
	}
	if a.holder() != "skygo-0_1" || !first.Valid(time.Now()) {
		t.Fatalf("holder %q", a.holder())
	}
	first.Stop()
	if a.holder() != "skygo-0_1" || !errors.Is(first.Err(), ErrStopped) || first.Valid(time.Now()) {
		t.Fatal("stop wrote the lease or left it valid")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	second := mustAcquire(t, ctx, client, testConfig("skygo-0_2"))
	var transitions int32
	a.set(func(a *leaseAPI) { transitions = *a.lease.Spec.LeaseTransitions })
	if a.holder() != "skygo-0_2" || transitions != 1 || !second.Valid(time.Now()) {
		t.Fatalf("takeover: holder %q", a.holder())
	}
	if err := first.Release(context.Background()); err == nil || a.holder() != "skygo-0_2" {
		t.Fatal("stopped lease released another holder's lease")
	}
}

// A renewing holder keeps the Lease even when its recorded times, and so
// its whole-second leader election record, never change: a contender
// detects renewal by the Lease's resourceVersion.
func TestAcquireWaitsForUnchangedRecord(t *testing.T) {
	client, a := newLeaseAPI()
	a.skew = true
	first := mustAcquire(t, context.Background(), client, testConfig("skygo-0_1"))
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	if h, err := Acquire(ctx, client, testConfig("skygo-0_2")); err == nil || h != nil || !errors.Is(err, context.DeadlineExceeded) {
		if h != nil {
			h.Stop()
		}
		t.Fatalf("second acquire = %v", err)
	}
	if a.holder() != "skygo-0_1" || !first.Valid(time.Now()) || first.Err() != nil {
		t.Fatalf("holder %q err %v", a.holder(), first.Err())
	}
}

// Failed renewals lose the Lease within the renew deadline, with the
// original API error, and the loss is sticky: no renewal goroutine remains.
func TestRenewFailureLoses(t *testing.T) {
	client, a := newLeaseAPI()
	cfg := testConfig("skygo-0_1")
	h := mustAcquire(t, context.Background(), client, cfg)
	injected := apierrors.NewServiceUnavailable("modeled outage")
	a.set(func(a *leaseAPI) { a.err["update"], a.err["get"] = injected, injected })
	if !closed(h.Lost(), 5*time.Second) {
		t.Fatal("lease not lost")
	}
	if err := h.Err(); !errors.Is(err, ErrLost) || !errors.Is(err, injected) || h.Valid(time.Now()) {
		t.Fatalf("err = %v", err)
	}
	if !closed(h.done, 5*time.Second) {
		t.Fatal("renewal goroutine did not return")
	}
	a.set(func(a *leaseAPI) { clear(a.err) })
	n := a.count()
	if err := h.Release(context.Background()); err == nil || !errors.Is(err, ErrLost) || a.count() != n || a.holder() != cfg.Identity {
		t.Fatalf("release after loss = %v", err)
	}
	if !errors.Is(h.Err(), ErrLost) || h.Valid(time.Now()) {
		t.Fatal("loss was not sticky")
	}
}

// Another holder ends the Lease at once; Release then leaves that holder's
// Lease alone.
func TestTakenOver(t *testing.T) {
	client, a := newLeaseAPI()
	h := mustAcquire(t, context.Background(), client, testConfig("skygo-0_1"))
	a.takeOver("other")
	if !closed(h.Lost(), 5*time.Second) {
		t.Fatal("lease not lost")
	}
	if err := h.Err(); !errors.Is(err, ErrLost) || !strings.Contains(err.Error(), `"other"`) {
		t.Fatalf("err = %v", err)
	}
	n := a.count()
	if err := h.Release(context.Background()); err == nil || a.count() != n || a.holder() != "other" {
		t.Fatalf("release = %v, holder %q", err, a.holder())
	}
}

// Canceling Acquire's context after it returned neither stops the renewal
// nor releases the Lease.
func TestCallerCancelKeepsLease(t *testing.T) {
	client, a := newLeaseAPI()
	ctx, cancel := context.WithCancel(context.Background())
	h := mustAcquire(t, ctx, client, testConfig("skygo-0_1"))
	cancel()
	n := a.count()
	if !eventually(5*time.Second, func() bool { return a.count() >= n+3 }) {
		t.Fatal("renewal stopped with the caller's context")
	}
	if a.holder() != "skygo-0_1" || !h.Valid(time.Now()) || h.Err() != nil {
		t.Fatalf("holder %q err %v", a.holder(), h.Err())
	}
}

// Valid follows this process's send time of the last successful write on
// the monotonic clock, never the acquisition time or the Lease's recorded
// times.
func TestValidMonotonic(t *testing.T) {
	client, a := newLeaseAPI()
	a.skew = true
	acquired := time.Now()
	h := mustAcquire(t, context.Background(), client, testConfig("skygo-0_1"))
	n := a.count()
	if !eventually(5*time.Second, func() bool { return a.count() >= n+2 }) || !h.Valid(time.Now()) {
		t.Fatal("skewed recorded times changed Valid")
	}
	if !eventually(5*time.Second, func() bool { return time.Since(acquired) > 2*h.cfg.RenewDeadline }) {
		t.Fatal("clock did not advance")
	}
	if !h.Valid(time.Now()) || h.Err() != nil {
		t.Fatalf("renewed lease not valid past twice the renew deadline after acquisition: err %v", h.Err())
	}
	start := time.Now()
	held := &Held{cfg: testConfig("x"), renewed: start}
	if !held.Valid(start.Add(299*time.Millisecond)) || held.Valid(start.Add(300*time.Millisecond)) || held.Valid(start.Add(time.Hour)) {
		t.Fatal("Valid does not follow the renew deadline")
	}
	held.err = ErrLost
	if held.Valid(start) {
		t.Fatal("lost lease valid")
	}
}

// Acquire returns the original API error with the context's.
func TestAcquireReturnsAPIError(t *testing.T) {
	client, a := newLeaseAPI()
	forbidden := apierrors.NewForbidden(coordinationv1.Resource("leases"), "skygo-server", errors.New("modeled RBAC"))
	a.set(func(a *leaseAPI) { a.err["get"] = forbidden })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	h, err := Acquire(ctx, client, testConfig("skygo-0_1"))
	if h != nil {
		h.Stop()
	}
	if h != nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, forbidden) || a.count() != 0 {
		t.Fatalf("acquire = %v", err)
	}
}

// The largest LeaseDuration reaches the Lease unnarrowed.
func TestMaxLeaseDuration(t *testing.T) {
	client, a := newLeaseAPI()
	cfg := testConfig("skygo-0_1")
	cfg.LeaseDuration = math.MaxInt32 * time.Second
	mustAcquire(t, context.Background(), client, cfg)
	var seconds int32
	a.set(func(a *leaseAPI) { seconds = *a.lease.Spec.LeaseDurationSeconds })
	if seconds != math.MaxInt32 {
		t.Fatalf("LeaseDurationSeconds = %d", seconds)
	}
}

// Release writes only if the Lease is unchanged since this process wrote
// it, and returns the original conflict otherwise.
func TestReleaseConflict(t *testing.T) {
	client, a := newLeaseAPI()
	h := mustAcquire(t, context.Background(), client, testConfig("skygo-0_1"))
	h.stop()
	<-h.done
	a.set(func(a *leaseAPI) { a.store(a.lease) })
	err := h.Release(context.Background())
	if !apierrors.IsConflict(err) || a.holder() != "skygo-0_1" || !errors.Is(h.Err(), ErrReleased) || h.Valid(time.Now()) {
		t.Fatalf("release = %v, holder %q", err, a.holder())
	}
}

// Release joins the renewal goroutine before it writes: a renewal blocked
// in the API completes first, and Release's conditional write follows it.
func TestReleaseJoinsRenewal(t *testing.T) {
	client, a := newLeaseAPI()
	cfg := testConfig("skygo-0_1")
	cfg.LeaseDuration, cfg.RenewDeadline = 3*time.Second, 2*time.Second
	h := mustAcquire(t, context.Background(), client, cfg)
	hold, entered := make(chan struct{}), make(chan struct{}, 1)
	a.set(func(a *leaseAPI) { a.hold, a.entered = hold, entered })
	unblock := sync.OnceFunc(func() {
		a.set(func(a *leaseAPI) { a.hold, a.entered = nil, nil })
		close(hold)
	})
	t.Cleanup(unblock) // before h.Stop, so that a failure does not hang
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not start")
	}
	released := make(chan error, 1)
	go func() { released <- h.Release(context.Background()) }()
	select {
	case err := <-released:
		t.Fatalf("release returned %v during a renewal", err)
	case <-time.After(200 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-released:
		if err != nil || a.holder() != "" || !errors.Is(h.Err(), ErrReleased) {
			t.Fatalf("release = %v, holder %q, err %v", err, a.holder(), h.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("release did not return")
	}
}

// Release and Stop end with ErrLost, without writing, a Lease whose renew
// deadline passed while the renewal goroutine was stopped before it noticed.
func TestEndAfterDeadline(t *testing.T) {
	for _, release := range []bool{true, false} {
		client, a := newLeaseAPI()
		cfg := testConfig("skygo-0_1")
		h := mustAcquire(t, context.Background(), client, cfg)
		h.stop()
		<-h.done
		h.mu.Lock()
		h.renewed = h.renewed.Add(-cfg.RenewDeadline)
		h.mu.Unlock()
		n := a.count()
		if release {
			if err := h.Release(context.Background()); !errors.Is(err, ErrLost) {
				t.Fatalf("release after deadline = %v", err)
			}
		} else {
			h.Stop()
		}
		if a.count() != n || a.holder() != cfg.Identity || !closed(h.Lost(), 0) || !errors.Is(h.Err(), ErrLost) || h.Valid(time.Now()) {
			t.Fatalf("release %v: writes %d, holder %q, err %v", release, a.count()-n, a.holder(), h.Err())
		}
	}
}
