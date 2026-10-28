package environmentcontrol_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

func TestExecRunsAndReleases(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	res, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"echo", "hello"}})
	if err != nil || !res.Settled || !res.Exited || string(res.Stdout) != "hello" {
		t.Fatalf("Exec = %+v, %v", res, err)
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 0 || g.Unknown != 0 {
		t.Fatalf("sessions after exit %+v", g)
	}
}

func TestExecRefusals(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	creating := f.create(t, "alice", "creating", testSpec("pending"))
	for _, test := range []struct {
		name string
		who  string
		id   string
		argv []string
		in   []byte
		want error
	}{
		{"empty argv", "alice", v.ID, nil, nil, environmentcontrol.ErrInvalid},
		{"nul argv", "alice", v.ID, []string{"a\x00b"}, nil, environmentcontrol.ErrInvalid},
		{"large argv", "alice", v.ID, []string{strings.Repeat("a", environmentcontrol.MaxArgvBytes+1)}, nil, environmentcontrol.ErrInvalid},
		{"large stdin", "alice", v.ID, []string{"cat"}, make([]byte, environmentcontrol.MaxStdinBytes+1), environmentcontrol.ErrInvalid},
		{"invalid id", "alice", "x", []string{"true"}, nil, environmentcontrol.ErrInvalid},
		{"foreign", "bob", v.ID, []string{"true"}, nil, store.ErrNotFound},
		{"admin is not owner", "legacy", v.ID, []string{"true"}, nil, store.ErrNotFound},
		{"reader", "reader", v.ID, []string{"true"}, nil, auth.ErrPermissionDenied},
		{"not ready", "alice", creating.ID, []string{"true"}, nil, environmentcontrol.ErrNotReady},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := f.c.Exec(context.Background(), f.actors[test.who], test.id, environmentcontrol.ExecRequest{Argv: test.argv, Stdin: test.in})
			if !errors.Is(err, test.want) {
				t.Fatalf("Exec: %v, want %v", err, test.want)
			}
		})
	}
	if _, _, _, execs := f.runtime.counts(); execs != 0 {
		t.Fatalf("%d refused execs reached the runtime", execs)
	}
}

func TestExecRuntimeErrorReleases(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	refused := errors.New("pod identity differs")
	f.runtime.execErr = refused
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}}); !errors.Is(err, refused) {
		t.Fatalf("Exec: %v", err)
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 0 || g.Unknown != 0 {
		t.Fatalf("sessions after refusal %+v", g)
	}
}

func TestExecOutcomes(t *testing.T) {
	for _, test := range []struct {
		name    string
		res     environmentcontrol.ExecResult
		unknown int
	}{
		{"exited", environmentcontrol.ExecResult{Exited: true, Code: 3}, 0},
		{"refused", environmentcontrol.ExecResult{Refused: true, Err: errors.New("upgrade refused")}, 0},
		{"no status", environmentcontrol.ExecResult{Err: errors.New("status stream truncated")}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			f.runtime.hold = true
			done := make(chan environmentcontrol.ExecResult, 1)
			go func() {
				res, _ := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
				done <- res
			}()
			(<-f.runtime.started).finish(test.res)
			res := <-done
			if !res.Settled || res.Code != test.res.Code {
				t.Fatalf("Exec = %+v", res)
			}
			if g := f.get(t, "alice", v.ID, environment.Ready); g.Unknown != test.unknown || g.Sessions != 0 {
				t.Fatalf("sessions %+v, want %d unknown", g, test.unknown)
			}
		})
	}
}

func TestSessionLimit(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	for range environmentcontrol.MaxSessions {
		go f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
		<-f.runtime.started
	}
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}}); !errors.Is(err, environmentcontrol.ErrSessionLimit) {
		t.Fatalf("Exec beyond limit: %v", err)
	}
}

func TestDisconnectCancelsOnlyItsSession(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(ctx, f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"one"}})
		first <- res
	}()
	x1 := <-f.runtime.started
	second := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"two"}})
		second <- res
	}()
	x2 := <-f.runtime.started
	cancel()
	if res := <-first; res.Settled || res.Exited {
		t.Fatalf("disconnected exec %+v", res)
	}
	if !x1.wasCanceled() || x2.wasCanceled() {
		t.Fatalf("canceled first %v second %v", x1.wasCanceled(), x2.wasCanceled())
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Unknown != 1 || g.Sessions != 1 {
		t.Fatalf("sessions after disconnect %+v", g)
	}
	x2.finish(environmentcontrol.ExecResult{Exited: true})
	if res := <-second; !res.Exited {
		t.Fatalf("second exec %+v", res)
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Unknown != 1 || g.Sessions != 0 {
		t.Fatalf("sessions after second exit %+v", g)
	}
}

func TestLateExitReleasesUnknownSession(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(ctx, f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
		done <- res
	}()
	x := <-f.runtime.started
	cancel()
	<-done
	// The status arrived after the cancel: the command did exit.
	x.finish(environmentcontrol.ExecResult{Exited: true})
	eventually(t, "release", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Unknown == 0 && g.Sessions == 0
	})
}
