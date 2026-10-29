//go:build unix

package gce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// Machine methods are synchronous: a held directory sync or file Close
// holds the method itself, and no context bounds it. These tests hold
// one through the fileClose and fsyncDir seams, register the release and
// join of the actual original before any check can fail, and only then
// let the root be retired. NOT_RUN by the author.

// holdSeam makes the seam calls for which match reports true wait for
// release after signaling entered. The returned release is safe to call
// more than once; the seam is restored after the test.
type holdSeam struct {
	entered chan struct{}
	hold    chan struct{}
	once    atomic.Bool
}

func newHoldSeam() *holdSeam {
	return &holdSeam{entered: make(chan struct{}, 1), hold: make(chan struct{})}
}

func (h *holdSeam) wait() {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.hold
}

func (h *holdSeam) release() {
	if h.once.CompareAndSwap(false, true) {
		close(h.hold)
	}
}

// holdSync holds every directory sync while on is true.
func holdSync(t *testing.T, on *bool) *holdSeam {
	h := newHoldSeam()
	t.Cleanup(func() { fsyncDir = syncDir })
	fsyncDir = func(d *os.File) error {
		if *on {
			h.wait()
		}
		return syncDir(d)
	}
	return h
}

// holdClose holds the first Close of files named with suffix, then runs
// the real Close, counting them.
func holdClose(t *testing.T, suffix string, n *atomic.Int32) *holdSeam {
	h := newHoldSeam()
	t.Cleanup(func() { fileClose = (*os.File).Close })
	fileClose = func(f *os.File) error {
		if strings.HasSuffix(f.Name(), suffix) {
			n.Add(1)
			h.wait()
		}
		return f.Close()
	}
	return h
}

// joined reports whether done is closed, without waiting.
func joined(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// SetGoal's commit is held in its directory sync: the call itself does
// not return until the sync does.
func TestHeldSaveHoldsSetGoal(t *testing.T) {
	on := false
	h := holdSync(t, &on)
	m, dir, _ := newStateMachine(t)
	on = true
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = m.SetGoal(Absent)
	}()
	t.Cleanup(func() { h.release(); <-done }) // before any check
	<-h.entered
	if joined(done) {
		t.Fatal("SetGoal returned during its held commit")
	}
	h.release()
	<-done
	on = false
	if err != nil {
		t.Fatalf("SetGoal = %v", err)
	}
	if st, err := ReadStatus(dir); err != nil || st.Desired != Absent {
		t.Fatalf("on disk: %+v, %v", st, err)
	}
}

// Close's lock release is held in the lock file's first Close: Close does
// not return until it does, and no later Close runs it again.
func TestHeldLockCloseHoldsClose(t *testing.T) {
	m, _, _ := newStateMachine(t)
	var n atomic.Int32
	h := holdClose(t, lockName, &n)
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = m.Close()
	}()
	t.Cleanup(func() { h.release(); <-done })
	<-h.entered
	if joined(done) {
		t.Fatal("Close returned during its held lock Close")
	}
	h.release()
	<-done
	if err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if n.Load() != 1 {
		t.Errorf("lock closed %d times, want 1", n.Load())
	}
}

// Close of a machine with an unconfirmed commit retries it in full; that
// retry's directory sync holds Close.
func TestHeldRecommitHoldsClose(t *testing.T) {
	on := false
	errSync := errors.New("injected directory sync failure")
	m, dir, _ := newStateMachine(t)
	injectSync(t, &on, errSync)
	on = true
	if err := m.SetGoal(Absent); !errors.Is(err, errSync) {
		t.Fatalf("SetGoal = %v", err)
	}
	on = false
	held := true
	h := holdSync(t, &held)
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = m.Close()
	}()
	t.Cleanup(func() { h.release(); <-done })
	<-h.entered
	if joined(done) {
		t.Fatal("Close returned during its held recommit")
	}
	h.release()
	<-done
	held = false
	if err != nil {
		t.Fatalf("Close = %v", err)
	}
	if st, err := ReadStatus(dir); err != nil || st.Desired != Absent {
		t.Fatalf("on disk: %+v, %v", st, err)
	}
}

// A constructor held in its parent directory sync is joined before its
// result is registered, and the root is retired only after its Machine
// closed.
func TestHeldConstructor(t *testing.T) {
	on := true
	h := holdSync(t, &on)
	sr := newStateRoot(t)
	done := make(chan struct{})
	var (
		m   *Machine
		err error
	)
	go func() {
		defer close(done)
		m, err = Create(filepath.Join(sr.dir, "state"), testConfig())
	}()
	t.Cleanup(func() { h.release(); <-done }) // runs before the root's
	<-h.entered
	if joined(done) {
		t.Fatal("Create returned during its held parent sync")
	}
	h.release()
	<-done
	on = false
	sr.own(t, nil, m, err)
	if err != nil {
		t.Fatal(err)
	}
}

// Each early exit right after a failed constructor, before any check of
// its result, still retains the actual error and its lock or file and
// the root, past the subtest. Controls: a clean Machine and an ordinary
// failure that keeps no owner let the root go.
func TestConstructorEarlyExit(t *testing.T) {
	errClose := errors.New("injected first close failure")
	edges := []struct {
		name   string
		suffix string // file whose first Close fails on Open
		other  bool   // Open with another config
		kept   bool
	}{
		{"state close", stateName, false, true},
		{"lock close", lockName, true, true},
		{"clean", "", false, false},
		{"ordinary failure", "", true, false},
	}
	for _, e := range edges {
		var sr *stateRoot
		t.Run(e.name, func(t *testing.T) {
			var dir string
			var m *Machine
			m, dir, sr = newStateMachine(t)
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if e.suffix != "" {
				injectClose(t, e.suffix, errClose)
			}
			cfg := testConfig()
			if e.other {
				cfg.DiskGiB = 30
			}
			m2, err := Open(dir, cfg)
			sr.own(t, nil, m2, err)
			t.SkipNow() // as an early t.Fatal: only cleanups run
		})
		if sr == nil {
			t.Fatalf("%s: no root", e.name)
		}
		if e.kept {
			checkRetained(t, sr)
			continue
		}
		if _, err := os.Stat(sr.dir); !os.IsNotExist(err) {
			t.Errorf("%s: root %s not removed after its owners settled", e.name, sr.dir)
		}
	}
}
