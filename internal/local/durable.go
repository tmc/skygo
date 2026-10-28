package local

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// DurableConfig selects durable execution for plain local jobs.
type DurableConfig struct {
	// State is a private directory for durable job records. It must not be
	// the log directory. It is created with mode 0700 if missing.
	State string

	// Executable is the supervisor binary, normally os.Executable(). Its
	// main must call RunSupervisor when its first argument is
	// SupervisorCommand, before any other initialization.
	Executable string
}

type durableState struct {
	dir  string // absolute State
	logs string // absolute log directory
	exe  string
	jobs map[string]*durableJob
}

// durableJob is this backend's view of one durable record.
type durableJob struct {
	launch durableLaunch
	task   backend.Task
	valid  bool // original intent and allocation were validated
	// starting is set while this backend's Start is launching a supervisor.
	// Start writes exited and prelaunch before clearing it under b.mu; neither
	// may be read by an observer that saw starting set.
	starting bool
	// exited is closed after a supervisor launched by this backend is reaped.
	// It is nil for records found on disk.
	exited chan struct{}
	// prelaunch is set when this backend's Start returned without launching
	// any supervisor, so no workload can ever start for this record.
	prelaunch bool
	// terminal is set once an authoritative terminal status was read.
	terminal bool
}

// EnableDurable runs later plain jobs under detached supervisors and adopts
// existing durable records. A durable job outlives Close and the server
// process: a later backend with the same configuration observes, cancels and
// reads logs for it. Only a supervisor started by Start runs the workload, at
// most once; nothing is ever re-run.
//
// Jobs with a workspace or a retry policy keep their server-owned execution,
// as do backends with output retention, for which EnableDurable fails.
// Requires Linux or macOS process start-time identity.
func (b *Backend) EnableDurable(c DurableConfig) error {
	if _, err := bootIdentity(); err != nil {
		return fmt.Errorf("durable local execution is unsupported: %w", err)
	}
	if _, err := identifyProcess(os.Getpid()); err != nil {
		return fmt.Errorf("durable local execution is unsupported: %w", err)
	}
	if c.Executable == "" || !filepath.IsAbs(c.Executable) {
		return errors.New("durable supervisor executable must be an absolute path")
	}
	dir, err := filepath.Abs(c.State)
	if err != nil {
		return err
	}
	logs, err := filepath.Abs(b.directory)
	if err != nil {
		return err
	}
	if dir == logs {
		return errors.New("durable state must be outside the log directory")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create durable state: %w", err)
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		return fmt.Errorf("durable state %s is not a directory", dir)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("protect durable state: %w", err)
	}
	if err := checkPrivateDir(dir); err != nil {
		return err
	}
	// Records are kept, like logs, for the life of the data directory.
	// Each is a few small files read once here.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	jobs := make(map[string]*durableJob)
	for _, e := range entries {
		if !validID.MatchString(e.Name()) {
			continue
		}
		j := &durableJob{}
		jobDir := filepath.Join(dir, e.Name())
		var in durableIntent
		if err := checkPrivateDir(jobDir); err == nil &&
			readDurable(jobDir, durableIntentFile, maxDurableIntent, &in) == nil &&
			validDurableIntent(in, e.Name(), logs) == nil {
			j.launch = in.Launch
			j.task = backend.CloneTask(in.Task)
			j.valid = true
			if status, found, err := readStatus(jobDir, in); found && err == nil && status.State != backend.Unknown {
				j.terminal = true
			}
		}
		jobs[e.Name()] = j
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retention != nil || b.workspaces != nil {
		return errors.New("durable local execution does not support output retention or workspaces")
	}
	if b.closed || len(b.jobs) != 0 || b.durable != nil {
		return errors.New("durable execution must be enabled before use")
	}
	b.durable = &durableState{dir: dir, logs: logs, exe: c.Executable, jobs: jobs}
	return nil
}

// durableReservation is one durable job's placement, for resource admission.
type durableReservation struct {
	ID     string
	Launch durableLaunch
	Task   backend.Task
	Valid  bool
}

// durableReservationsLocked returns placements of durable jobs that this
// backend has not seen reach an authoritative terminal status, including
// unknown ones. The caller holds b.mu. Close never releases them.
func (b *Backend) durableReservationsLocked() []durableReservation {
	if b.durable == nil {
		return nil
	}
	var r []durableReservation
	for id, j := range b.durable.jobs {
		if !j.terminal {
			r = append(r, durableReservation{ID: id, Launch: j.launch, Task: backend.CloneTask(j.task), Valid: j.valid})
		}
	}
	return r
}

// durableEligible reports whether t runs under a durable supervisor.
func (b *Backend) durableEligible(t backend.Task) bool {
	return b.durable != nil && t.Workdir == nil && (t.Retry == nil || t.Retry.MaxAttempts <= 1)
}

// Detaches reports whether id names a durable execution. Close leaves such
// executions running, and a restarted backend observes them.
func (b *Backend) Detaches(id string) bool {
	if !validID.MatchString(id) {
		return false
	}
	b.mu.Lock()
	d := b.durable
	b.mu.Unlock()
	if d == nil {
		return false
	}
	_, err := os.Lstat(filepath.Join(d.dir, id))
	return err == nil
}

// startDurable reserves the job, records intent and launches its supervisor.
// The context bounds only the wait for the supervisor's readiness; a timeout
// or cancellation leaves the job to Observe, which never launches a second
// copy.
func (b *Backend) startDurable(ctx context.Context, id string, t backend.Task, inv hostInventory) error {
	b.mu.Lock()
	d := b.durable
	if b.closed {
		b.mu.Unlock()
		return errors.New("local backend is closed")
	}
	if _, ok := b.jobs[id]; ok {
		b.mu.Unlock()
		return errors.New("job already started")
	}
	if _, ok := d.jobs[id]; ok {
		b.mu.Unlock()
		return errors.New("job already started")
	}
	launch, err := b.durableLaunchFor(id, t, inv)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	// The exclusive directory fences a repeated start, including after restart.
	dir := filepath.Join(d.dir, id)
	if err := os.Mkdir(dir, 0700); err != nil {
		b.mu.Unlock()
		if errors.Is(err, fs.ErrExist) {
			return errors.New("job already started")
		}
		return fmt.Errorf("reserve durable job: %w", err)
	}
	j := &durableJob{launch: launch, task: backend.CloneTask(t), valid: true, starting: true}
	d.jobs[id] = j
	b.mu.Unlock()

	err = b.launchDurable(ctx, d, dir, id, t, j)
	b.mu.Lock()
	j.prelaunch = j.exited == nil
	j.starting = false
	b.mu.Unlock()
	return err
}

func (b *Backend) launchDurable(ctx context.Context, d *durableState, dir, id string, t backend.Task, j *durableJob) error {
	if err := checkPrivateDir(dir); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(d.logs, id+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL|noFollow, 0600)
	if err != nil {
		return fmt.Errorf("create job output: %w", err)
	}
	defer log.Close()
	instance, err := newDurableInstance()
	if err != nil {
		return fmt.Errorf("create execution identity: %w", err)
	}
	in := durableIntent{Version: durableVersion, ID: id, Instance: instance, Task: backend.CloneTask(t), Launch: j.launch, Logs: d.logs}
	if err := validDurableIntent(in, id, d.logs); err != nil {
		return err
	}
	if err := writeDurable(dir, durableIntentFile, in); err != nil {
		return err
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create readiness pipe: %w", err)
	}
	defer readyR.Close()
	cmd := exec.Command(d.exe, SupervisorCommand, dir)
	cmd.Dir = "/"
	cmd.ExtraFiles = []*os.File{log, readyW}
	detach(cmd)
	err = cmd.Start()
	readyW.Close()
	if err != nil {
		return fmt.Errorf("start durable supervisor: %w", err)
	}
	j.exited = make(chan struct{})
	go func() { cmd.Wait(); close(j.exited) }()
	log.Close()

	if deadline, ok := ctx.Deadline(); ok {
		readyR.SetReadDeadline(deadline)
	}
	// Cancellation interrupts the read on this same pipe. The callback is
	// stopped, or joined if it already ran, before readyR is closed.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		readyR.SetReadDeadline(time.Now())
		close(interrupted)
	})
	line, err := bufio.NewReader(readyR).ReadString('\n')
	if !stop() {
		<-interrupted
	}
	switch {
	case line == "ok\n":
		return nil
	case strings.HasPrefix(line, "fail: "):
		return fmt.Errorf("start program: %s", strings.TrimSpace(strings.TrimPrefix(line, "fail: ")))
	case errors.Is(err, os.ErrDeadlineExceeded):
		// Only ctx sets the read deadline. The supervisor is left running
		// for Observe.
		if err = ctx.Err(); err == nil {
			err = context.DeadlineExceeded
		}
		return fmt.Errorf("await durable supervisor: %w", err)
	case err != nil:
		return fmt.Errorf("await durable supervisor: %w", err)
	}
	return errors.New("durable supervisor sent a malformed readiness message")
}

// observeDurable reports a durable job. ok is false when id has no durable
// record. It never signals any process: a PID, an exit status or a missing
// process alone never establishes success.
func (b *Backend) observeDurable(ref backend.Reference) (s backend.Status, ok bool, err error) {
	b.mu.Lock()
	d := b.durable
	var j *durableJob
	var starting bool
	if d != nil {
		j = d.jobs[ref.ID]
		starting = j != nil && j.starting
	}
	b.mu.Unlock()
	if d == nil {
		return backend.Status{}, false, nil
	}
	dir := filepath.Join(d.dir, ref.ID)
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		return backend.Status{}, false, nil
	}
	s, err = d.observe(dir, ref, j, starting)
	if err == nil && s.State.Terminal() && s.State != backend.Unknown {
		b.mu.Lock()
		if j = d.jobs[ref.ID]; j == nil {
			j = &durableJob{}
			d.jobs[ref.ID] = j
		}
		j.terminal = true
		b.mu.Unlock()
	}
	return s, true, err
}

// observe reads one durable record. starting is j.starting as read under b.mu;
// while it is set, this backend's Start may be publishing the intent and its
// supervisor may not have claimed the job yet.
func (d *durableState) observe(dir string, ref backend.Reference, j *durableJob, starting bool) (backend.Status, error) {
	if err := checkPrivateDir(dir); err != nil {
		return backend.Status{State: backend.Unknown, Reason: "durable record is not private"}, nil
	}
	var in durableIntent
	err := readDurable(dir, durableIntentFile, maxDurableIntent, &in)
	if errors.Is(err, fs.ErrNotExist) {
		return missingIntent(ref, j, starting)
	}
	if err != nil || validDurableIntent(in, ref.ID, d.logs) != nil {
		return backend.Status{State: backend.Unknown, Reason: "durable record is invalid"}, nil
	}
	if err := checkReference(in, ref); err != nil {
		return backend.Status{}, err
	}
	unknown := func(reason string) backend.Status {
		return backend.Status{State: backend.Unknown, Reason: reason, Instance: in.Instance}
	}
	if s, ok, err := readStatus(dir, in); ok || err != nil {
		if err != nil {
			return unknown("durable status is invalid"), nil
		}
		return s, nil
	}
	var c durableClaim
	err = readDurable(dir, durableClaimFile, maxDurableRecord, &c)
	if errors.Is(err, fs.ErrNotExist) {
		if starting {
			// The supervisor this Start is launching has yet to claim.
			return backend.Status{State: backend.Starting, Instance: in.Instance}, nil
		}
		if j != nil && j.exited != nil {
			select {
			case <-j.exited:
			default:
				return backend.Status{State: backend.Starting, Instance: in.Instance}, nil
			}
		}
		// No supervisor holds the claim. Taking it forbids any later launch.
		err = createDurable(dir, durableClaimFile, durableClaim{Version: durableVersion, Instance: in.Instance, Fenced: true})
		if err == nil {
			c = durableClaim{Version: durableVersion, Instance: in.Instance, Fenced: true}
		} else if errors.Is(err, fs.ErrExist) {
			err = readDurable(dir, durableClaimFile, maxDurableRecord, &c)
		}
	}
	if err != nil {
		return backend.Status{}, fmt.Errorf("read durable claim: %w", err)
	}
	if c.Version != durableVersion || c.Instance != in.Instance || c.Fenced == (c.Supervisor != nil) {
		return unknown("durable claim is invalid"), nil
	}
	if c.Fenced {
		var cancel durableCancel
		if readDurable(dir, durableCancelFile, maxDurableRecord, &cancel) == nil && cancel.Version == durableVersion && cancel.Instance == in.Instance {
			return backend.Status{State: backend.Canceled, Reason: "canceled before launch", Instance: in.Instance}, nil
		}
		return backend.Status{State: backend.Failed, Reason: "durable start ended before launch; workload never started", Instance: in.Instance}, nil
	}
	alive, err := c.Supervisor.alive()
	if err != nil {
		return backend.Status{}, fmt.Errorf("verify durable supervisor: %w", err)
	}
	var r durableRunning
	runErr := readDurable(dir, durableRunningFile, maxDurableRecord, &r)
	if runErr == nil && (r.Version != durableVersion || r.Instance != in.Instance || r.Supervisor != *c.Supervisor || r.Child.PID <= 0 || r.Child.Start == 0 || r.Child.Boot == "") {
		return unknown("durable running record is invalid"), nil
	}
	if alive {
		if runErr == nil {
			return backend.Status{State: backend.Running, Instance: in.Instance}, nil
		}
		return backend.Status{State: backend.Starting, Instance: in.Instance}, nil
	}
	// The supervisor is gone. It may have written its status just before.
	if s, ok, err := readStatus(dir, in); ok || err != nil {
		if err != nil {
			return unknown("durable status is invalid"), nil
		}
		return s, nil
	}
	if runErr != nil {
		return unknown("durable supervisor exited during launch; workload state is unknown"), nil
	}
	if childAlive, err := r.Child.alive(); err != nil || childAlive {
		return unknown(fmt.Sprintf("durable supervisor exited; workload pid %d may still be running unobserved", r.Child.PID)), nil
	}
	return unknown("durable supervisor exited without a terminal status"), nil
}

// missingIntent classifies a durable record without an intent. Start writes
// the intent before launching any supervisor, so absence proves nothing ran
// only for a Start of this backend that launched none. Otherwise the record is
// damaged: the error leaves the job, and its allocation, held.
func missingIntent(ref backend.Reference, j *durableJob, starting bool) (backend.Status, error) {
	if j == nil || !j.valid || !starting && !j.prelaunch {
		return backend.Status{}, errors.New("durable intent is missing; execution state is unknown")
	}
	if err := checkOriginalTask(j.task, ref.Task); err != nil {
		return backend.Status{}, err
	}
	if starting {
		if ref.Instance != "" {
			return backend.Status{}, errors.New("durable execution identity is not yet published")
		}
		return backend.Status{State: backend.Starting}, nil
	}
	if ref.Instance != "" {
		return backend.Status{}, backend.ErrIdentityMismatch
	}
	return backend.Status{State: backend.Failed, Reason: "durable start ended before launch; workload never started"}, nil
}

// checkReference binds a reference to the original task and instance.
func checkReference(in durableIntent, ref backend.Reference) error {
	if ref.Instance != "" && ref.Instance != in.Instance {
		return backend.ErrIdentityMismatch
	}
	return checkOriginalTask(in.Task, ref.Task)
}

// checkOriginalTask compares a reference's task with the original canonical task.
func checkOriginalTask(task, ref backend.Task) error {
	want, err := json.Marshal(task)
	if err != nil {
		return err
	}
	got, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return backend.ErrIdentityMismatch
	}
	return nil
}

// validDurableIntent validates persisted launch facts without probing the host
// or recomputing a placement. Invalid allocation facts must remain reserved,
// rather than being interpreted as an execution that consumes no resources.
func validDurableIntent(in durableIntent, id, logs string) error {
	if in.Version != durableVersion || in.ID != id || !validID.MatchString(id) ||
		len(in.Instance) != len("local-")+32 || !strings.HasPrefix(in.Instance, "local-") ||
		!validID.MatchString(strings.TrimPrefix(in.Instance, "local-")) || in.Logs != logs {
		return errors.New("invalid durable intent identity")
	}
	t := in.Task
	if len(t.Argv) == 0 || t.Argv[0] == "" || t.Workdir != nil || t.Image != "" ||
		t.CPUMillis < 0 || t.MemoryBytes < 0 || t.AcceleratorCount < 0 {
		return errors.New("invalid durable task")
	}
	if retry, err := backend.NormalizeRetry(t.Retry); err != nil || retry != nil {
		return errors.New("durable task has a retry policy")
	}
	if err := backend.ValidateWorkspace(t); err != nil {
		return err
	}
	for _, arg := range t.Argv {
		if strings.ContainsRune(arg, 0) {
			return errors.New("durable argv contains NUL")
		}
	}
	if err := validateDurableResourceLaunch(t, in.Launch); err != nil {
		return err
	}
	return nil
}

func readStatus(dir string, in durableIntent) (backend.Status, bool, error) {
	var st durableStatus
	err := readDurable(dir, durableStatusFile, maxDurableRecord, &st)
	if errors.Is(err, fs.ErrNotExist) {
		return backend.Status{}, false, nil
	}
	if err != nil {
		return backend.Status{}, true, err
	}
	if st.Version != durableVersion || st.Instance != in.Instance || !st.State.Terminal() {
		return backend.Status{}, true, errors.New("invalid durable status")
	}
	if st.State != backend.Unknown && !st.GroupClosed {
		return backend.Status{}, true, errors.New("durable status lacks execution group closure")
	}
	if st.State == backend.Succeeded && (!st.OutputComplete || st.ExitCode == nil || *st.ExitCode != 0) {
		return backend.Status{}, true, errors.New("durable success lacks completion")
	}
	return backend.Status{State: st.State, Reason: retryReason(st.Reason), ExitCode: st.ExitCode, Instance: in.Instance}, true, nil
}

// cancelDurable records a cancel request and waits until the supervisor
// writes a terminal status. ok is false when id has no durable record. The
// request names the exact instance: while the original Start has yet to
// publish it, cancelDurable waits for that same Start, bounded by ctx.
func (b *Backend) cancelDurable(ctx context.Context, ref backend.Reference) (ok bool, err error) {
	b.mu.Lock()
	dir := ""
	if b.durable != nil {
		dir = filepath.Join(b.durable.dir, ref.ID)
	}
	b.mu.Unlock()
	requested := false
	tick := time.NewTicker(durablePoll)
	defer tick.Stop()
	for {
		s, ok, err := b.observeDurable(ref)
		if !ok || err != nil {
			return ok, err
		}
		if s.State == backend.Unknown {
			return true, fmt.Errorf("durable execution is unknown: %s", s.Reason)
		}
		if s.State.Terminal() {
			return true, nil
		}
		if !requested && s.Instance != "" {
			if err := requestDurableCancel(dir, s.Instance); err != nil {
				return true, err
			}
			requested = true
			if ref.Instance == "" {
				ref.Instance = s.Instance
			}
		}
		select {
		case <-ctx.Done():
			return true, fmt.Errorf("await durable cancellation: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// requestDurableCancel creates the immutable cancel request for instance. An
// existing request counts only if it names the same instance.
func requestDurableCancel(dir, instance string) error {
	want := durableCancel{Version: durableVersion, Instance: instance}
	err := createDurable(dir, durableCancelFile, want)
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	var got durableCancel
	if err := readDurable(dir, durableCancelFile, maxDurableRecord, &got); err != nil {
		return fmt.Errorf("read durable cancel request: %w", err)
	}
	if got != want {
		return errors.New("existing durable cancel request names another execution")
	}
	return nil
}
