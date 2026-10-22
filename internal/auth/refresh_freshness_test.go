package auth

import (
	"testing"
	"time"
)

// stepClock returns t on its first call, after running first if it is set,
// and after on every later call. A refresh that stamps its read start makes
// the first call before reading the credential file.
type stepClock struct {
	t, after time.Time
	first    func()
	calls    int
}

func (c *stepClock) now() time.Time {
	c.calls++
	if c.calls > 1 {
		return c.after
	}
	if c.first != nil {
		c.first()
	}
	return c.t
}

// TestRefreshFreshnessFromReadStart checks that a credential file refresh is
// fresh as of the start of its read. The clock's first reading rewrites the
// file, as an edit made while the read is under way would, and every later
// reading is three seconds on. The published snapshot must contain that edit
// and carry the read start, so a read that outlasts the two-second envelope
// is already stale; a following quick refresh restores readiness.
func TestRefreshFreshnessFromReadStart(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	clock := &stepClock{t: start, after: start}
	e, err := newFile(f.path, installation, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	gen := e.snapshot.Generation
	e.mu.RUnlock()

	clock.calls, clock.after = 0, start.Add(3*time.Second)
	clock.first = func() { writeFixture(t, f.path, func(c *config) { c.Generation++ }) }
	if err := e.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	e.mu.RLock()
	last, got := e.lastRead, e.snapshot.Generation
	e.mu.RUnlock()
	if got != gen+1 {
		t.Fatalf("published generation %d, want %d: the file was read before the freshness stamp", got, gen+1)
	}
	if !last.Equal(start) {
		t.Fatalf("lastRead = %v, want the read start %v", last, start)
	}
	if e.Ready() {
		t.Fatal("Ready after a refresh whose read outlasted the envelope")
	}
	if _, err := e.Authenticate("Bearer " + f.initial); err != ErrUnavailable {
		t.Fatalf("Authenticate after a slow refresh = %v, want ErrUnavailable", err)
	}

	clock.calls, clock.t, clock.first = 0, clock.after, nil
	if err := e.refresh(); err != nil {
		t.Fatalf("quick refresh: %v", err)
	}
	if !e.Ready() {
		t.Fatal("not Ready after a quick refresh")
	}
	if _, err := e.Authenticate("Bearer " + f.initial); err != nil {
		t.Fatalf("Authenticate after a quick refresh: %v", err)
	}
}
