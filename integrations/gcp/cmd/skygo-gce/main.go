//go:build unix

// Command skygo-gce creates, stops, restarts and deletes one Compute Engine
// VM and its owned boot disk, keeping their lifecycle in a private state
// directory.
//
// Usage:
//
//	skygo-gce command -config FILE -state DIR [flags]
//
// The commands are start, stop, restart, down and status. skygo-gce manages
// lifecycle only: it runs no skygo job and is not a skygo execution target.
//
// The -timeout flag sets the operation deadline, fixed when the command
// starts: reading the private inputs, opening the state, setting the goal
// and driving or refreshing must end by then. Closing the state must end
// by the cleanup deadline, fixed at the same time 90 seconds later; that
// is longer than one request's fixed 60 second deadline, to let a pending
// original request end.
//
// skygo-gce runs those steps on one Machine strictly one at a time and
// only observes the deadlines: it never stops a step, and no deadline
// bounds the state's file system calls (write, sync, Close). If the
// operation deadline passes first, nothing more is started, and the state
// is closed only after the running step returns. If the cleanup deadline
// passes before that step or the close returns, skygo-gce reports HOLD
// and exits 1. The process still owns that step, its Machine and the
// state lock until the step returns or the process exits; the state
// records any request as possibly sent, and a later run recovers it
// without resending. A step that returns after the HOLD changes nothing.
// Neither a deadline nor process exit is a successful close.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/tmc/skygo/integrations/gcp"
)

const usage = `usage: skygo-gce command -config FILE -state DIR [flags]

Commands:
  start    create the disk and VM if needed and wait until the VM runs
  stop     stop the VM and wait until it is stopped
  restart  stop the running VM, then start it again
  down     delete the VM, then its disk; latched: never started again
  status   print the stored state; calls no service unless -refresh

Flags:
  -config FILE       private operator configuration (JSON)
  -state DIR         private state directory
  -credentials FILE  private OAuth 2 access token (JSON); status needs it
                     only with -refresh
  -refresh           status: observe the resources with GET requests only
                     and record what was seen; never changes them
  -timeout D         operation deadline (default 10m, at most 2h); closing
                     the state must end 90s after it
  -steps N           most lifecycle steps (default 120, at most 1000)
  -interval D        wait between observations (default 5s, 1s to 5m)
  -json              print the status as JSON

Exit status: 0 goal reached or status printed, 1 error (including HOLD:
a step or close still running at the cleanup deadline), 2 usage error,
3 goal not reached (held, or the step bound).
`

const (
	maxTimeout  = 2 * time.Hour
	closeBudget = 90 * time.Second // after the operation deadline; longer than one original's fixed deadline
	closePoll   = 250 * time.Millisecond
)

var goals = map[string]gce.Goal{
	"start":   gce.Running,
	"stop":    gce.Stopped,
	"restart": gce.Restarted,
	"down":    gce.Absent,
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	c := newCommand()
	code := c.run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	c.exit(code)
}

// exit ends the process with code. c stays reachable until the process
// ends, so nothing it owns, a step still running, its Machine, a file or
// the state lock kept by an error, is collected or finalized before.
func (c *command) exit(code int) {
	os.Exit(code)
	runtime.KeepAlive(c)
}

type options struct {
	config, state, credentials string
	timeout, interval          time.Duration
	steps                      int
	json, refresh              bool
}

var errUsage = errors.New("usage")

// steps are the originals a command runs. Tests replace single entries
// to hold or fail one; production runs these.
type steps struct {
	readConfig func(string) (gce.Config, error)
	readToken  func(string) (oauthToken, error)
	readStatus func(string) (gce.Status, error)
	create     func(string, gce.Config) (*gce.Machine, error)
	open       func(string, gce.Config) (*gce.Machine, error)
	setGoal    func(*gce.Machine, gce.Goal) error
	drive      func(*gce.Machine, context.Context, oauthToken, gce.DriveOptions) (gce.Status, error)
	refresh    func(*gce.Machine, context.Context, oauthToken) (gce.Status, error)
	close      func(*gce.Machine) error
}

var realSteps = steps{
	readConfig: readConfig,
	readToken:  readToken,
	readStatus: gce.ReadStatus,
	create:     gce.Create,
	open:       gce.Open,
	setGoal:    (*gce.Machine).SetGoal,
	drive:      (*gce.Machine).Drive,
	refresh:    (*gce.Machine).Refresh,
	close:      (*gce.Machine).Close,
}

// A command is the one owner of a skygo-gce run: its inputs, its Machine,
// every error a step returned (which may keep a file or the state lock)
// and the step still running, if any. It runs each step in a goroutine so
// that it can observe a deadline while the step blocks, but it starts no
// step while another has not returned and never stops one. A field that
// a step sets is read only after that step returned.
type command struct {
	step  steps
	now   func() time.Time
	timer func(deadline time.Time) <-chan time.Time // fires at deadline
	poll  func() <-chan time.Time                   // fires after closePoll

	expiry  time.Time // operation deadline
	cleanup time.Time // expiry plus closeBudget

	cfg    gce.Config
	tok    oauthToken
	m      *gce.Machine
	st     gce.Status
	errs   []error // every step's error, with what it keeps
	active *job    // the step that has not returned, if any
	held   error   // the HOLD, once the cleanup deadline passed first; final
}

// A job is one step. done is closed after the step returned with err.
type job struct {
	what string
	done chan struct{}
	err  error
}

var (
	errExpired = errors.New("deadline passed before the step returned")
	errOverlap = errors.New("a step was started while another runs")
)

func newCommand() *command {
	return &command{
		step:  realSteps,
		now:   time.Now,
		timer: func(d time.Time) <-chan time.Time { return time.After(time.Until(d)) },
		poll:  func() <-chan time.Time { return time.After(closePoll) },
	}
}

// begin fixes both deadlines, once, before any step.
func (c *command) begin(timeout time.Duration) {
	c.expiry = c.now().Add(timeout)
	c.cleanup = c.expiry.Add(closeBudget)
}

// do starts f as c's one step if the time is before start, then waits
// for it until the cleanup deadline. It returns f's error when f joins,
// even after the operation admission deadline, or an error wrapping
// errExpired if the cleanup deadline came first. A step that has not
// returned by the cleanup deadline stays c's active step, owned by c.
func (c *command) do(what string, start time.Time, f func() error) error {
	switch {
	case c.active != nil:
		return errOverlap
	case !c.now().Before(start):
		return fmt.Errorf("the deadline passed before %s could start: %w", what, errExpired)
	}
	j := &job{what: what, done: make(chan struct{})}
	c.active = j
	go func() {
		defer close(j.done)
		j.err = f()
	}()
	select {
	case <-j.done:
	default:
		select {
		case <-j.done:
		case <-c.timer(c.cleanup):
			return fmt.Errorf("the cleanup deadline passed before %s returned: %w", what, errExpired)
		}
	}
	c.active = nil
	if j.err != nil {
		c.errs = append(c.errs, j.err)
	}
	return j.err
}

func (c *command) run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd := args[0]
	if _, ok := goals[cmd]; !ok && cmd != "status" {
		fmt.Fprintf(stderr, "skygo-gce: unknown command %q\n%s", cmd, usage)
		return 2
	}
	o, err := parseFlags(cmd, args[1:], stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		if err != errUsage {
			fmt.Fprintf(stderr, "skygo-gce: %v\n", err)
		}
		fmt.Fprint(stderr, usage)
		return 2
	}
	c.begin(o.timeout)
	code := c.operate(ctx, cmd, o, stdout, stderr)
	if err := c.finish(); err != nil {
		fmt.Fprintf(stderr, "skygo-gce: %s: %v\n", cmd, err)
		code = 1
	}
	return code
}

// operate runs the command's steps by the operation deadline.
func (c *command) operate(ctx context.Context, cmd string, o options, stdout, stderr io.Writer) int {
	err := c.do("read config", c.expiry, func() (err error) {
		c.cfg, err = c.step.readConfig(o.config)
		return err
	})
	if err != nil {
		return fail(stderr, cmd, err)
	}
	if cmd == "status" && !o.refresh {
		err := c.do("read status", c.expiry, func() (err error) {
			c.st, err = c.step.readStatus(o.state)
			return err
		})
		if err == nil && (c.st.Project != c.cfg.Project || c.st.Zone != c.cfg.Zone) {
			err = errors.New("state belongs to another project or zone")
		}
		if err == nil {
			err = printStatus(stdout, o.json, c.st)
		}
		if err != nil {
			return fail(stderr, cmd, err)
		}
		return 0
	}
	err = c.do("read credentials", c.expiry, func() (err error) {
		c.tok, err = c.step.readToken(o.credentials)
		return err
	})
	if err != nil {
		return fail(stderr, cmd, err)
	}
	what, construct := "open state", c.step.open
	if cmd == "start" {
		what, construct = "create state", c.step.create
	}
	err = c.do(what, c.expiry, func() (err error) {
		c.m, err = construct(o.state, c.cfg)
		return err
	})
	if err != nil {
		return fail(stderr, cmd, err)
	}
	ctx, cancel := context.WithDeadline(ctx, c.expiry)
	defer cancel()
	if cmd == "status" {
		err = c.do("refresh", c.expiry, func() (err error) {
			c.st, err = c.step.refresh(c.m, ctx, c.tok)
			return err
		})
	} else {
		err = c.do("set goal", c.expiry, func() error { return c.step.setGoal(c.m, goals[cmd]) })
		if err != nil {
			return fail(stderr, cmd, err)
		}
		err = c.do("drive", c.expiry, func() (err error) {
			c.st, err = c.step.drive(c.m, ctx, c.tok, gce.DriveOptions{Steps: o.steps, Interval: o.interval})
			return err
		})
	}
	if errors.Is(err, errExpired) {
		return fail(stderr, cmd, err)
	}
	if perr := printStatus(stdout, o.json, c.st); perr != nil && err == nil {
		err = perr
	}
	switch {
	case err != nil:
		return fail(stderr, cmd, err)
	case cmd != "status" && !c.st.Reached:
		fmt.Fprintf(stderr, "skygo-gce: %s: goal not reached: %s\n", cmd, c.st.Held)
		return 3
	}
	return 0
}

// finish closes the Machine by the cleanup deadline, calling Close again
// after closePoll while it fails. Close starts only once no step is
// running. If a step or a Close is still running at the cleanup deadline,
// finish returns the final HOLD; c keeps that step and the Machine with
// its lock until the process exits. Only a Close that returned nil is a
// successful close.
func (c *command) finish() error {
	switch {
	case c.held != nil:
		return c.held
	case c.active != nil:
		return c.hold()
	case c.m == nil:
		return nil
	}
	var last error
	for {
		err := c.do("close", c.cleanup, func() error { return c.step.close(c.m) })
		switch {
		case err == nil:
			return nil
		case c.active != nil:
			return c.hold()
		case errors.Is(err, errExpired):
			return fmt.Errorf("the machine and its state lock are still owned and not closed: %w", errors.Join(last, err))
		}
		last = err
		select {
		case <-c.poll():
		case <-c.timer(c.cleanup):
			return fmt.Errorf("the cleanup deadline passed; the original request and state lock are still owned and not closed: %w", err)
		}
	}
}

// hold records and returns the final HOLD for the active step.
func (c *command) hold() error {
	c.held = fmt.Errorf("HOLD: the cleanup deadline passed before %s returned; this process keeps it, the machine and the state lock until it returns or the process exits; nothing was closed", c.active.what)
	return c.held
}

func parseFlags(cmd string, args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {}
	fs.StringVar(&o.config, "config", "", "")
	fs.StringVar(&o.state, "state", "", "")
	fs.StringVar(&o.credentials, "credentials", "", "")
	fs.DurationVar(&o.timeout, "timeout", 10*time.Minute, "")
	fs.IntVar(&o.steps, "steps", 120, "")
	fs.DurationVar(&o.interval, "interval", 5*time.Second, "")
	fs.BoolVar(&o.json, "json", false, "")
	if cmd == "status" {
		fs.BoolVar(&o.refresh, "refresh", false, "")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, usage)
			return o, err
		}
		return o, errUsage
	}
	switch {
	case fs.NArg() != 0:
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	case o.config == "":
		return o, errors.New("-config is required")
	case o.state == "":
		return o, errors.New("-state is required")
	case o.timeout <= 0 || o.timeout > maxTimeout:
		return o, fmt.Errorf("-timeout must be positive and at most %v", maxTimeout)
	case o.steps < 1 || o.steps > 1000:
		return o, errors.New("-steps must be 1 to 1000")
	case o.interval < time.Second || o.interval > 5*time.Minute:
		return o, errors.New("-interval must be 1s to 5m")
	case cmd == "status" && !o.refresh && o.credentials != "":
		return o, errors.New("status reads no credentials without -refresh")
	case (cmd != "status" || o.refresh) && o.credentials == "":
		return o, errors.New("-credentials is required")
	}
	return o, nil
}

func fail(stderr io.Writer, cmd string, err error) int {
	fmt.Fprintf(stderr, "skygo-gce: %s: %v\n", cmd, err)
	return 1
}
