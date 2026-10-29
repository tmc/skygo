// Command skygo-ec2 creates, stops, restarts and terminates one EC2
// virtual machine and keeps its lifecycle in a private state directory,
// and runs finite skygo jobs on that VM once it is running.
//
// Usage:
//
//	skygo-ec2 command -config FILE -state DIR [flags]
//
// The lifecycle commands are start, stop, restart, down and status. The
// job commands run, resume, job-status, logs and cancel use an existing,
// owned, running VM through an SSH agent session and never create, start,
// stop or terminate it. See the README of this module for the
// configuration, credential, state and job formats.
//
// skygo-ec2 is not a skygo execution target: its jobs are not managed by
// a skygo server's configured targets.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

const usage = `usage: skygo-ec2 command -config FILE -state DIR [flags]

Commands:
  start    create the VM if needed and wait until it runs
  stop     stop the VM and wait until it is stopped
  restart  stop the running VM, then start it again
  down     terminate the VM; latched: it is never started again
  status   print the stored state; with -refresh, also describe the VM

Job commands (see skygo-ec2 run -h): run, resume, job-status, logs, cancel

Flags:
  -config FILE       operator configuration (JSON)
  -state DIR         private state directory
  -credentials FILE  private AWS credentials (JSON); needed except by status without -refresh
  -timeout D         bound on the command (default 10m, at most 2h)
  -steps N           most lifecycle steps (default 120, at most 1000)
  -interval D        wait between observations (default 5s, at most 5m)
  -json              print the status as JSON
  -refresh           status: describe the owned VM now

Exit status: 0 goal reached or status printed, 1 error,
2 usage error, 3 goal not reached (held, or the step bound).
`

const (
	maxTimeout  = 2 * time.Hour
	maxSteps    = 1000
	maxInterval = 5 * time.Minute
)

var goals = map[string]ec2ctl.Goal{
	"start":   ec2ctl.GoalRunning,
	"stop":    ec2ctl.GoalStopped,
	"restart": ec2ctl.GoalRestarted,
	"down":    ec2ctl.GoalAbsent,
}

// An env is what run needs from its process.
type env struct {
	stdout, stderr io.Writer
	newClient      func(*ec2api.Profile, aws.CredentialsProvider) (*ec2api.Client, error)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	e := env{os.Stdout, os.Stderr, ec2api.NewClient}
	args := os.Args[1:]
	var (
		c    *jobCommand
		code int
	)
	if len(args) > 0 && jobCommands[args[0]] {
		c = runJob(ctx, args[0], args[1:], jobEnv{e.stdout, e.stderr, e.newClient, openSession, openSettled, ec2job.LoadConfig})
		code = c.code
	} else {
		code = run(ctx, args, e)
	}
	stop()
	// c owns what a job command could not settle: an SSH session or open
	// error, a file whose Close failed and the locked journal. kept, a
	// package variable, keeps it reachable through os.Exit, which closes
	// every descriptor the process holds; nothing closes them before.
	kept = c
	os.Exit(code)
}

// kept is the job command main keeps until the process exits.
var kept *jobCommand

type options struct {
	config, state, credentials string
	timeout, interval          time.Duration
	steps                      int
	json, refresh              bool
}

// run runs a lifecycle command and returns its exit status. A job
// command returns an owner its caller must keep, so it is run by runJob,
// never here.
func run(ctx context.Context, args []string, e env) int {
	if len(args) == 0 {
		fmt.Fprint(e.stderr, usage)
		return 2
	}
	cmd := args[0]
	if jobCommands[cmd] {
		fmt.Fprintf(e.stderr, "skygo-ec2: %s: a job command is run by runJob, which returns its owner\n", cmd)
		return 2
	}
	if _, ok := goals[cmd]; !ok && cmd != "status" {
		fmt.Fprintf(e.stderr, "skygo-ec2: unknown command %q\n%s", cmd, usage)
		return 2
	}
	o, err := parseFlags(cmd, args[1:], e.stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		if err != errUsage {
			fmt.Fprintf(e.stderr, "skygo-ec2: %v\n", err)
		}
		fmt.Fprint(e.stderr, usage)
		return 2
	}

	cfg, err := ec2ctl.LoadConfig(o.config)
	if err != nil {
		return fail(e, cmd, err)
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	if cmd == "status" {
		return status(ctx, e, o, cfg)
	}
	client, err := newClient(e, o, cfg)
	if err != nil {
		return fail(e, cmd, err)
	}
	var m *ec2ctl.Machine
	if cmd == "start" {
		m, err = ec2ctl.Create(o.state, cfg)
	} else {
		m, err = ec2ctl.Open(o.state, cfg)
	}
	if err != nil {
		return fail(e, cmd, err)
	}
	defer m.Close()
	if err := m.SetGoal(goals[cmd]); err != nil {
		return fail(e, cmd, err)
	}
	st, err := m.Drive(ctx, client, ec2ctl.Limits{Steps: o.steps, Interval: o.interval})
	if perr := printStatus(e.stdout, o.json, st, nil); perr != nil && err == nil {
		err = perr
	}
	if err != nil {
		return fail(e, cmd, err)
	}
	return 0
}

var errUsage = errors.New("usage")

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
	fs.BoolVar(&o.refresh, "refresh", false, "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, usage)
			return o, err
		}
		return o, errUsage // flag has printed the error
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
	case o.steps < 1 || o.steps > maxSteps:
		return o, fmt.Errorf("-steps must be 1 to %d", maxSteps)
	case o.interval < 0 || o.interval > maxInterval:
		return o, fmt.Errorf("-interval must be 0 to %v", maxInterval)
	case o.refresh && cmd != "status":
		return o, errors.New("-refresh is only for status")
	case o.credentials == "" && (cmd != "status" || o.refresh):
		return o, errors.New("-credentials is required")
	}
	return o, nil
}

func newClient(e env, o options, cfg *ec2ctl.Config) (*ec2api.Client, error) {
	creds, err := ec2ctl.LoadCredentials(o.credentials)
	if err != nil {
		return nil, err
	}
	return e.newClient(&cfg.Profile, creds)
}

func status(ctx context.Context, e env, o options, cfg *ec2ctl.Config) int {
	st, err := ec2ctl.ReadStatus(o.state, cfg)
	if err != nil {
		return fail(e, "status", err)
	}
	var (
		obs    *observed
		obsErr error
	)
	if o.refresh {
		client, err := newClient(e, o, cfg)
		if err != nil {
			return fail(e, "status", err)
		}
		var got *ec2ctl.Observation
		got, obsErr = ec2ctl.Observe(ctx, client, st.Record)
		obs = observation(got, obsErr)
	}
	if err := printStatus(e.stdout, o.json, st, obs); err != nil {
		return fail(e, "status", err)
	}
	if obsErr != nil {
		return fail(e, "status -refresh", obsErr)
	}
	return 0
}

// fail reports err and returns the exit status for it.
func fail(e env, cmd string, err error) int {
	fmt.Fprintf(e.stderr, "skygo-ec2: %s: %v\n", cmd, err)
	if errors.Is(err, ec2ctl.ErrNotReached) {
		return 3
	}
	return 1
}

// observed is the JSON and text form of a refresh.
type observed struct {
	Refreshed  bool       `json:"refreshed"`
	Call       string     `json:"call,omitempty"`
	InstanceID string     `json:"instance_id,omitempty"`
	State      string     `json:"state,omitempty"`
	At         *time.Time `json:"at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

// observation returns the form of a refresh that returned obs and err.
func observation(obs *ec2ctl.Observation, err error) *observed {
	o := &observed{Refreshed: true}
	if obs != nil {
		at := obs.At
		o.Call, o.InstanceID, o.State, o.At = obs.Call, obs.InstanceID, obs.State, &at
	}
	if err != nil {
		o.Error = err.Error()
	}
	return o
}

type diagnostic struct {
	Action string `json:"action"`
	Call   string `json:"call,omitempty"`
	Class  string `json:"class,omitempty"`
	Sent   bool   `json:"sent"`
	Reason string `json:"reason"`
}

type record struct {
	Owner           string      `json:"owner"`
	Environment     string      `json:"environment"`
	Generation      uint64      `json:"generation"`
	Lease           string      `json:"lease,omitempty"`
	ClientToken     string      `json:"client_token"`
	ProfileHash     string      `json:"profile_hash"`
	Desired         string      `json:"desired"`
	Phase           string      `json:"phase"`
	Hold            string      `json:"hold"`
	InstanceID      string      `json:"instance_id"`
	Reported        []string    `json:"reported"`
	CreateSent      bool        `json:"create_sent"`
	CreateSentAt    *time.Time  `json:"create_sent_at"`
	CreditsVerified bool        `json:"credits_verified"`
	LastDiagnostic  *diagnostic `json:"last_diagnostic"`
}

// report is the JSON form of a status. Everything but Observed is the
// stored record; Durable is false if it may not be stored.
type report struct {
	Durable        bool      `json:"durable"`
	Record         record    `json:"record"`
	RestartPending bool      `json:"restart_pending"`
	NextAction     string    `json:"next_action"`
	GoalReached    bool      `json:"goal_reached"`
	Observed       *observed `json:"observed"`
}

func newReport(st ec2ctl.Status, obs *observed) report {
	r := st.Record
	rec := record{
		Owner:           r.Identity.Owner,
		Environment:     r.Identity.Environment,
		Generation:      r.Identity.Generation,
		Lease:           r.Identity.Lease,
		ClientToken:     r.Token,
		ProfileHash:     r.ProfileHash,
		Desired:         r.Desired.String(),
		Phase:           r.Phase.String(),
		Hold:            r.Hold.String(),
		InstanceID:      r.InstanceID,
		Reported:        append([]string{}, r.Reported...),
		CreateSent:      r.CreateSent,
		CreditsVerified: r.CreditsVerified,
	}
	if r.CreateSent {
		at := r.CreateSentAt.UTC()
		rec.CreateSentAt = &at
	}
	if d := r.LastDiagnostic; d != (ec2vm.Diagnostic{}) {
		rec.LastDiagnostic = &diagnostic{Action: d.Action.String(), Call: d.Call, Sent: d.Sent, Reason: d.Reason}
		if d.Call != "" {
			rec.LastDiagnostic.Class = d.Class.String()
		}
	}
	if obs == nil {
		obs = &observed{}
	}
	return report{
		Durable:        !st.Pending,
		Record:         rec,
		RestartPending: st.Restart,
		NextAction:     st.Next.String(),
		GoalReached:    st.Reached(),
		Observed:       obs,
	}
}

// printStatus prints st and obs, which is nil if there was no refresh.
func printStatus(w io.Writer, asJSON bool, st ec2ctl.Status, obs *observed) error {
	rp := newReport(st, obs)
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rp)
	}
	r := rp.Record
	var b strings.Builder
	line := func(k, v string) { fmt.Fprintf(&b, "%-17s %s\n", k, v) }
	line("owner", r.Owner)
	line("environment", r.Environment)
	line("generation", fmt.Sprint(r.Generation))
	if r.Lease != "" {
		line("lease", r.Lease)
	}
	line("client token", r.ClientToken)
	line("instance", orDash(r.InstanceID))
	line("desired", r.Desired)
	line("phase", r.Phase)
	line("hold", r.Hold)
	line("restart pending", fmt.Sprint(rp.RestartPending))
	if r.CreateSentAt != nil {
		line("create sent", r.CreateSentAt.Format(time.RFC3339))
	} else {
		line("create sent", "no")
	}
	line("credits verified", fmt.Sprint(r.CreditsVerified))
	if len(r.Reported) > 0 {
		line("reported", strings.Join(r.Reported, ", "))
	}
	if d := r.LastDiagnostic; d != nil {
		sent := "not sent"
		if d.Sent {
			sent = "may have been sent"
		}
		line("last failure", fmt.Sprintf("%s: %s %s (%s): %s", d.Action, orDash(d.Call), orDash(d.Class), sent, d.Reason))
	}
	line("next action", rp.NextAction)
	line("goal reached", fmt.Sprint(rp.GoalReached))
	line("durable", fmt.Sprint(rp.Durable))
	switch o := rp.Observed; {
	case !o.Refreshed:
		line("observed", "not refreshed; the fields above are stored evidence")
	case o.Error != "":
		line("observed", "refresh failed: "+o.Error)
	default:
		line("observed", fmt.Sprintf("%s %s at %s (%s)", o.InstanceID, o.State, o.At.Format(time.RFC3339), o.Call))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
