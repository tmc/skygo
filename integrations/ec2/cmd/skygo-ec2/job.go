package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/artifact"
)

const jobUsage = `usage: skygo-ec2 job-command -config FILE -state DIR -credentials FILE -job-config FILE -job DIR [flags]

Job commands run finite workspace jobs on the existing, owned, running VM.
They never create, start, stop or terminate it.

Commands:
  run         freeze the workdir, upload it, submit the task once and wait
  resume      continue a job; replays a submit in doubt with its exact request
  job-status  observe the job once; never submits
  logs        copy the job's log to standard output; never submits
  cancel      latch cancel, deliver it to a known job and wait for the end;
              for a job never submitted, delete its known upload

A command the job directory alone refuses (a held or canceled job for
resume, a job held with an upload, submit or download in doubt for cancel,
a job without a known ID for job-status and logs) makes no EC2 call and
opens no session.

Flags:
  -job-config FILE   private execution configuration (JSON)
  -job DIR           private job directory; run creates it
  -task FILE         run: skygo task (protobuf JSON)
  -yaml FILE         run: SkyPilot task YAML
  -workdir DIR       run: directory to snapshot as the task's workdir
  -outputs A,B       run -yaml: output roots relative to the workdir
  -task-timeout D    run -yaml: the task's timeout, in whole seconds
  -poll D            interval between observations (default 5s); 0 does not wait
  -offset N          logs: start at byte N instead of the log checkpoint
  -json              logs: print the page and data as JSON
  -timeout D         bound on the command (default 10m, at most 24h)

logs starts where the last delivered copy ended (the log checkpoint, kept
in the job directory) unless -offset is given, and moves the checkpoint
to where the copy ends once it is written to standard output. A crash
between the two copies the same bytes again: delivery is at least once.

Every command but logs prints the job's status as JSON.
Exit status: 0 succeeded (status, logs: printed; cancel: canceled),
1 error, 2 usage error, 3 the job has not ended (it was not stopped),
4 the job ended without success, 5 the job is held.
`

const (
	maxJobTimeout = 24 * time.Hour
	maxPoll       = 5 * time.Minute
	maxTaskFile   = 1 << 20
)

var jobCommands = map[string]bool{"run": true, "resume": true, "job-status": true, "logs": true, "cancel": true}

// A session is an sshagent session as a job command uses it.
type session interface {
	ec2job.Session
	Close() error
}

// A jobEnv is what runJob needs from its process. open opens the SSH
// session; main's is sshagent.Open. settled reports whether a failed
// open closed everything it opened; main's is openSettled. loadConfig
// loads the job configuration; main's is ec2job.LoadConfig.
type jobEnv struct {
	stdout, stderr io.Writer
	newClient      func(*ec2api.Profile, aws.CredentialsProvider) (*ec2api.Client, error)
	open           func(context.Context, sshagent.Config) (session, error)
	settled        func(error) bool
	loadConfig     func(string) (*ec2job.Config, error)
}

func openSession(ctx context.Context, cfg sshagent.Config) (session, error) {
	s, err := sshagent.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// openSettled reports whether err, from a failed sshagent.Open, settled:
// sshagent returns an *sshagent.OpenError, which keeps the connections it
// could not settle, and otherwise an ordinary error.
func openSettled(err error) bool {
	var oe *sshagent.OpenError
	return !errors.As(err, &oe)
}

type jobOptions struct {
	config, state, credentials, jobConfig, job string
	task, yaml, workdir, outputs               string
	taskTimeout, poll, timeout                 time.Duration
	offset                                     int64
	offsetSet                                  bool
	json                                       bool
}

func parseJobFlags(cmd string, args []string, stderr io.Writer) (jobOptions, error) {
	var o jobOptions
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {}
	fs.StringVar(&o.config, "config", "", "")
	fs.StringVar(&o.state, "state", "", "")
	fs.StringVar(&o.credentials, "credentials", "", "")
	fs.StringVar(&o.jobConfig, "job-config", "", "")
	fs.StringVar(&o.job, "job", "", "")
	fs.DurationVar(&o.poll, "poll", 5*time.Second, "")
	fs.DurationVar(&o.timeout, "timeout", 10*time.Minute, "")
	if cmd == "run" {
		fs.StringVar(&o.task, "task", "", "")
		fs.StringVar(&o.yaml, "yaml", "", "")
		fs.StringVar(&o.workdir, "workdir", "", "")
		fs.StringVar(&o.outputs, "outputs", "", "")
		fs.DurationVar(&o.taskTimeout, "task-timeout", 0, "")
	}
	if cmd == "logs" {
		fs.Int64Var(&o.offset, "offset", 0, "")
		fs.BoolVar(&o.json, "json", false, "")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, jobUsage)
			return o, err
		}
		return o, errUsage
	}
	fs.Visit(func(f *flag.Flag) { o.offsetSet = o.offsetSet || f.Name == "offset" })
	switch {
	case fs.NArg() != 0:
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	case o.config == "" || o.state == "" || o.credentials == "" || o.jobConfig == "" || o.job == "":
		return o, errors.New("-config, -state, -credentials, -job-config and -job are required")
	case o.timeout <= 0 || o.timeout > maxJobTimeout:
		return o, fmt.Errorf("-timeout must be positive and at most %v", maxJobTimeout)
	case o.poll < 0 || o.poll > maxPoll:
		return o, fmt.Errorf("-poll must be 0 to %v", maxPoll)
	case cmd != "run":
		return o, nil
	case (o.task == "") == (o.yaml == ""):
		return o, errors.New("run needs exactly one of -task and -yaml")
	case o.workdir == "":
		return o, errors.New("run needs -workdir: every job uploads one")
	case o.task != "" && (o.outputs != "" || o.taskTimeout != 0):
		return o, errors.New("-outputs and -task-timeout apply only to -yaml; a JSON task names its own")
	case o.yaml != "" && (o.taskTimeout <= 0 || o.taskTimeout%time.Second != 0):
		return o, errors.New("run -yaml needs a positive -task-timeout in whole seconds")
	}
	return o, nil
}

// A jobCommand is one job command's exit status and the owner of what
// the command could not settle. Its caller keeps it: main until the
// process exits, a test until it settles it itself.
type jobCommand struct {
	code int

	// job is the journal the command opened, if any. While anything
	// below is unsettled it stays open, and the job directory locked, so
	// that no later command in the process opens the job or a session
	// for it.
	job *ec2job.Job

	// unsettled joins every original error the command could not
	// settle: an sshagent.Open whose cleanup did not settle, a session
	// Close that failed, and any error wrapping ec2job.ErrUnsettled (the
	// task file's, the workdir root's, the job configuration's, a journal
	// constructor's or an operation's failed file Close),
	// ec2ctl.ErrUnsettled (the VM configuration's, credentials' or state
	// directory's) or artifact.ErrFileCloseUncertain, each of which keeps
	// its original file. session is the session whose Close failed.
	// Nothing closes any of them again.
	unsettled error
	session   session
}

// keep keeps err, whole, if it wraps ec2job.ErrUnsettled,
// ec2ctl.ErrUnsettled or artifact.ErrFileCloseUncertain: the error holds
// the original file whose Close failed.
func (c *jobCommand) keep(err error) {
	if errors.Is(err, ec2job.ErrUnsettled) || errors.Is(err, ec2ctl.ErrUnsettled) || errors.Is(err, artifact.ErrFileCloseUncertain) {
		c.unsettle(err)
	}
}

// unsettle joins err to what the command could not settle, keeping every
// earlier cause.
func (c *jobCommand) unsettle(err error) {
	if c.unsettled == nil {
		c.unsettled = err
		return
	}
	c.unsettled = errors.Join(c.unsettled, err)
}

// closeSession closes s once, after the operation that returned err. It
// keeps err if it is unsettled and, if the Close fails, s and the Close's
// error, and returns err joined with that error: neither cause replaces
// the other.
func (c *jobCommand) closeSession(s session, err error) error {
	c.keep(err)
	if cerr := s.Close(); cerr != nil {
		cerr = fmt.Errorf("close SSH session: %w", cerr)
		c.session = s
		c.unsettle(cerr)
		err = errors.Join(err, cerr)
	}
	return err
}

// release closes the held journal, once, after its caller has settled
// what the command could not. main never calls it: the process's exit
// closes every descriptor the process holds. A test whose MODEL session
// owns no resource may.
func (c *jobCommand) release() error {
	if c.job == nil {
		return nil
	}
	return c.job.Close()
}

// runJob runs a job command. It closes the job journal it opened, once,
// after everything else, unless the command left something unsettled; a
// failed Close is reported, makes the command fail and is never retried.
func runJob(ctx context.Context, cmd string, args []string, e jobEnv) *jobCommand {
	c := new(jobCommand)
	c.code = jobMain(ctx, cmd, args, e, c)
	switch {
	case c.unsettled != nil:
		if c.job != nil {
			fmt.Fprintf(e.stderr, "skygo-ec2: %s: keeping the job directory locked: cleanup did not settle\n", cmd)
		}
		if c.code == 0 {
			c.code = 1
		}
	case c.job != nil:
		if err := c.job.Close(); err != nil {
			fmt.Fprintf(e.stderr, "skygo-ec2: %s: release job directory: %v\n", cmd, err)
			c.keep(err)
			if c.code == 0 {
				c.code = 1
			}
		}
	}
	return c
}

// jobMain runs a job command, storing in c what it opened and every
// original error it could not settle, on every return path.
func jobMain(ctx context.Context, cmd string, args []string, e jobEnv, c *jobCommand) int {
	o, err := parseJobFlags(cmd, args, e.stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		if err != errUsage {
			fmt.Fprintf(e.stderr, "skygo-ec2: %v\n", err)
		}
		fmt.Fprint(e.stderr, jobUsage)
		return 2
	}
	var j *ec2job.Job
	// Every error is offered to c before it is reported: one wrapping
	// ec2job.ErrUnsettled holds an original file c must keep.
	fail := func(err error) int {
		c.keep(err)
		return jobFail(e, cmd, err)
	}
	finish := func(err error) int {
		c.keep(err)
		return finishJob(e, cmd, o, j, err)
	}
	vmCfg, err := ec2ctl.LoadConfig(o.config)
	if err != nil {
		return fail(err)
	}
	cfg, err := e.loadConfig(o.jobConfig)
	if err != nil {
		return fail(err)
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	var (
		task     *ec2job.TaskInput
		snapshot []byte
		digest   string
	)
	if cmd == "run" {
		// Everything local is checked, and the task refused if the VM
		// cannot run it, before any EC2 call.
		task, err = readTask(o, nil)
		if err == nil {
			snapshot, digest, err = ec2job.Snapshot(o.workdir)
		}
		if err != nil {
			return fail(err)
		}
	} else {
		if j, err = ec2job.Open(o.job); err != nil {
			return fail(err)
		}
		c.job = j
		// What the journal alone decides is decided before any VM, SSH
		// or job request.
		switch cmd {
		case "cancel":
			err := j.RequestCancel()
			if err != nil || !j.NeedsRemote() {
				return finish(err)
			}
		case "resume":
			if err := j.CheckResume(); err != nil {
				return finish(err)
			}
		case "job-status":
			if !j.Known() {
				return finish(nil)
			}
		case "logs":
			if !j.Known() {
				return finishLogs(e, o, c, j, ec2job.LogPage{}, nil, errors.New("no job ID is known; there is no log to read"))
			}
		}
	}
	vt, err := checkRunTask(cmd, cfg, task, digest, int64(len(snapshot)))
	if err != nil {
		return fail(err)
	}
	creds, err := ec2ctl.LoadCredentials(o.credentials)
	if err != nil {
		return fail(err)
	}
	client, err := e.newClient(&vmCfg.Profile, creds)
	if err != nil {
		return fail(err)
	}
	vm, err := ec2job.VerifyVM(ctx, o.state, vmCfg, client, cfg)
	if err != nil {
		return finish(err)
	}
	if cmd == "run" {
		if j, err = ec2job.Create(o.job, cfg, vm, vt, snapshot); err != nil {
			return fail(err)
		}
		c.job = j
	}
	if err := j.Bind(cfg, vm); err != nil {
		return finish(err)
	}
	s, err := e.open(ctx, cfg.Session(vm.Address, e.stderr))
	if err != nil {
		if !e.settled(err) {
			c.unsettle(err)
		}
		return finish(err)
	}
	err = j.Attach(s)
	var (
		page ec2job.LogPage
		data bytes.Buffer
	)
	if err == nil {
		w := ec2job.Wait{Poll: o.poll}
		switch cmd {
		case "run":
			_, err = j.Start(ctx, w)
		case "resume":
			_, err = j.Resume(ctx, w)
		case "job-status":
			_, err = j.Refresh(ctx)
		case "cancel":
			_, err = j.Cancel(ctx, w)
		case "logs":
			out := e.stdout
			if o.json {
				out = &data
			}
			if o.offsetSet {
				page, err = j.Logs(ctx, o.offset, out)
			} else {
				page, err = j.NextLogs(ctx, out)
			}
		}
	}
	// The session is closed once, after its last request. A failed Close
	// is reported and never retried, and the command keeps the session
	// beside any unsettled error of the operation.
	err = c.closeSession(s, err)
	if cmd == "logs" {
		return finishLogs(e, o, c, j, page, data.Bytes(), err)
	}
	return finishJob(e, cmd, o, j, err)
}

// checkRunTask returns run's task, checked against cfg, or nil for
// another command.
func checkRunTask(cmd string, cfg *ec2job.Config, in *ec2job.TaskInput, digest string, size int64) (*v1.Task, error) {
	if cmd != "run" {
		return nil, nil
	}
	return cfg.Task(*in, digest, size, time.Now())
}

// A taskCloseError is the failed first Close of run's task file. It keeps
// the original file reachable while the error is, and wraps
// ec2job.ErrUnsettled so that the command keeps it. Nothing closes the
// file again.
type taskCloseError struct {
	f   *os.File
	err error
}

func (e *taskCloseError) Error() string   { return "task: " + e.err.Error() }
func (e *taskCloseError) Unwrap() []error { return []error{ec2job.ErrUnsettled, e.err} }

// readTask reads run's task file of at most 1 MiB. It closes the file
// once; hook, if set, is called with it first. A failed Close returns no
// task and joins any earlier error with a *taskCloseError.
func readTask(o jobOptions, hook func(*os.File)) (in *ec2job.TaskInput, err error) {
	name := o.task
	if name == "" {
		name = o.yaml
	}
	f, err := openTaskFile(name)
	if err != nil {
		return nil, fmt.Errorf("task: %w", err)
	}
	defer func() {
		if hook != nil {
			hook(f)
		}
		if cerr := f.Close(); cerr != nil {
			in, err = nil, errors.Join(err, &taskCloseError{f: f, err: cerr})
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("task: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("task must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxTaskFile+1))
	if err != nil {
		return nil, fmt.Errorf("task: %w", err)
	}
	if len(data) > maxTaskFile {
		return nil, errors.New("task exceeds 1 MiB")
	}
	if o.task != "" {
		return &ec2job.TaskInput{JSON: data}, nil
	}
	in = &ec2job.TaskInput{YAML: data, TimeoutSeconds: int64(o.taskTimeout / time.Second)}
	if o.outputs != "" {
		in.Outputs = strings.Split(o.outputs, ",")
	}
	return in, nil
}

// A jobReport is the JSON a job command prints.
type jobReport struct {
	Command string `json:"command"`
	JobDir  string `json:"job_dir"`
	ec2job.Status
}

// finishJob prints j's status, reports err and returns the exit status.
func finishJob(e jobEnv, cmd string, o jobOptions, j *ec2job.Job, err error) int {
	if j != nil {
		enc := json.NewEncoder(e.stdout)
		enc.SetIndent("", "  ")
		if perr := enc.Encode(jobReport{Command: cmd, JobDir: o.job, Status: j.Status()}); perr != nil && err == nil {
			err = perr
		}
	}
	if err != nil {
		fmt.Fprintf(e.stderr, "skygo-ec2: %s: %v\n", cmd, err)
	}
	switch {
	case errors.Is(err, ec2job.ErrHeld):
		return 5
	case errors.Is(err, ec2job.ErrNotTerminal):
		return 3
	case err != nil:
		return 1
	case j == nil:
		return 1
	}
	st := j.Status()
	switch cmd {
	case "job-status":
		return 0
	case "cancel":
		switch {
		case st.State == "CANCELED" || st.State == "NEVER_SUBMITTED":
			return 0
		case st.Ended():
			return 4
		}
		return 3
	}
	switch {
	case st.Succeeded():
		return 0
	case st.Ended():
		return 4
	}
	return 3
}

// finishLogs prints a logs page and returns the exit status. Once the
// copy was written to standard output in full, and only if nothing
// failed, it commits the page as the job's log checkpoint. c keeps any
// unsettled error.
func finishLogs(e jobEnv, o jobOptions, c *jobCommand, j *ec2job.Job, page ec2job.LogPage, data []byte, err error) int {
	if o.json {
		b, merr := json.Marshal(struct {
			ec2job.LogPage
			Data []byte `json:"data"`
		}{page, data})
		if merr == nil {
			merr = writeAll(e.stdout, append(b, '\n'))
		}
		if merr != nil && err == nil {
			err = merr
		}
	} else {
		fmt.Fprintf(e.stderr, "log: next-offset %d complete %t truncated %t\n", page.NextOffset, page.Complete, page.Truncated)
	}
	if err == nil {
		if err = j.CommitLogs(page); err != nil {
			c.keep(err)
		}
	}
	if err != nil {
		fmt.Fprintf(e.stderr, "skygo-ec2: logs: %v\n", err)
		return 1
	}
	return 0
}

// writeAll writes b to w and reports a short write as an error.
func writeAll(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

// jobFail reports an error before any journal exists.
func jobFail(e jobEnv, cmd string, err error) int {
	fmt.Fprintf(e.stderr, "skygo-ec2: %s: %v\n", cmd, err)
	return 1
}
