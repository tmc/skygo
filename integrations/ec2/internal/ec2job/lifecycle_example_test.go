//go:build darwin || linux

package ec2job_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
)

// These examples use a private temporary directory and no network: the
// session they attach answers no request, and counts any it is asked to
// send. Each removes its directory only after every original it opened
// there closed; otherwise it keeps the directory and the error. The
// examples of a successful job are in job_example_test.go.

// exampleVM is the identity ec2job.VerifyVM would report for
// exampleConfig's VM.
func exampleVM(c *ec2job.Config) ec2job.VM {
	return ec2job.VM{
		Account:      "123456789012",
		Region:       "us-west-2",
		ProfileHash:  strings.Repeat("a", 64),
		Owner:        "install-7f3a",
		Environment:  "env-0123456789abcdef",
		Generation:   1,
		ClientToken:  "token",
		InstanceID:   c.InstanceID,
		ImageID:      c.ImageID,
		InstanceType: c.InstanceType,
		Architecture: c.Architecture,
		Public:       c.Public,
		Address:      c.IP,
	}
}

// An exampleJob is a job created in a temporary directory.
type exampleJob struct {
	parent string
	dir    string
	cfg    *ec2job.Config
	vm     ec2job.VM
	j      *ec2job.Job
	err    error // a failure that may hold an original below parent
}

// newExampleJob snapshots a one-file workdir and creates a job for it.
// The caller calls close, also when newExampleJob fails.
func newExampleJob() (*exampleJob, error) {
	parent, err := os.MkdirTemp("", "ec2job-example-")
	if err != nil {
		return nil, err
	}
	e := &exampleJob{parent: parent, dir: filepath.Join(parent, "job")}
	e.err = e.create()
	return e, e.err
}

func (e *exampleJob) create() error {
	src := filepath.Join(e.parent, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(src, "input.txt"), []byte("input\n"), 0o600); err != nil {
		return err
	}
	archive, digest, err := ec2job.Snapshot(src)
	if err != nil {
		return err
	}
	if e.cfg, err = ec2job.ParseConfig(strings.NewReader(exampleConfig)); err != nil {
		return err
	}
	task, err := e.cfg.Task(ec2job.TaskInput{JSON: []byte(`{"argv":["/bin/sh","-c","cp input.txt out/"],"outputs":["out"],"timeoutSeconds":"60"}`)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		return err
	}
	e.vm = exampleVM(e.cfg)
	e.j, err = ec2job.Create(e.dir, e.cfg, e.vm, task, archive)
	return err
}

// close closes the job, if any, once, and removes the temporary directory
// only if that Close and everything before it succeeded. Otherwise an
// original below the directory may still be held: it keeps the directory
// and the error and reports both.
func (e *exampleJob) close() {
	if e == nil {
		return
	}
	err := e.err
	if e.j != nil {
		err = errors.Join(err, e.j.Close())
	}
	removeSettled(e.parent, err)
}

// keptErrors keeps every error that may hold an original reachable until
// the process exits.
var keptErrors []error

// removeSettled removes dir if err is nil. Otherwise it keeps dir and
// err, and reports them.
func removeSettled(dir string, err error) {
	if err != nil {
		keptErrors = append(keptErrors, err)
		fmt.Printf("keeping %s: %v\n", dir, err)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		fmt.Println(err)
	}
}

// noRequests is a transport that sends nothing and counts what it is
// asked to send.
type noRequests struct{ n atomic.Int64 }

func (t *noRequests) RoundTrip(*http.Request) (*http.Response, error) {
	t.n.Add(1)
	return nil, errors.New("example: no request is sent")
}

// exampleSession is the session sshagent.Open would return for cfg's
// configuration to addr, whose control client sends nothing.
type exampleSession struct {
	control skyv1connect.ControlServiceClient
	binding sshagent.Binding
}

func (s exampleSession) Control() skyv1connect.ControlServiceClient { return s.control }
func (s exampleSession) Binding() sshagent.Binding                  { return s.binding }

func newExampleSession(cfg *ec2job.Config, addr netip.Addr, t *noRequests) exampleSession {
	c := cfg.Session(addr, nil)
	return exampleSession{
		control: skyv1connect.NewControlServiceClient(&http.Client{Transport: t}, "http://agent.invalid"),
		binding: sshagent.Binding{
			Address: c.Address, User: c.User, KnownHostsFile: c.KnownHostsFile, KeyFile: c.KeyFile,
			Fingerprint: c.Fingerprint, AgentAddress: "127.0.0.1:47080", Target: c.Target,
			Commit: strings.Repeat("1", 40), Digest: strings.Repeat("2", 64),
			AgentDir: "skygo/agents/x", Installation: c.Installation,
		},
	}
}

func ExampleSnapshot() {
	dir, err := os.MkdirTemp("", "ec2job-example-")
	if err != nil {
		fmt.Println(err)
		return
	}
	var kept error
	defer func() { removeSettled(dir, kept) }()
	if err := os.WriteFile(filepath.Join(dir, "input.txt"), []byte("input\n"), 0o600); err != nil {
		fmt.Println(err)
		return
	}
	a1, d1, err := ec2job.Snapshot(dir)
	if err != nil {
		kept = err
		return
	}
	a2, d2, err := ec2job.Snapshot(dir)
	if err != nil {
		kept = err
		return
	}
	fmt.Println(len(d1), d1 == d2, bytes.Equal(a1, a2))
	// Output:
	// 64 true true
}

func ExampleLoadConfig() {
	dir, err := os.MkdirTemp("", "ec2job-example-")
	if err != nil {
		fmt.Println(err)
		return
	}
	var kept error
	defer func() { removeSettled(dir, kept) }()
	name := filepath.Join(dir, "job-config.json")
	if err := os.WriteFile(name, []byte(exampleConfig), 0o600); err != nil {
		fmt.Println(err)
		return
	}
	c, err := ec2job.LoadConfig(name)
	if err != nil {
		kept = err
		return
	}
	fmt.Println(c.InstanceID, c.Target)

	// A configuration others can read is refused. Its Close succeeded, so
	// the refusal is an ordinary error.
	if err := os.Chmod(name, 0o644); err != nil {
		fmt.Println(err)
		return
	}
	_, err = ec2job.LoadConfig(name)
	if errors.Is(err, ec2job.ErrUnsettled) {
		kept = err
	}
	fmt.Println(err != nil, errors.Is(err, ec2job.ErrUnsettled))
	// Output:
	// i-0123456789abcdef0 ec2vm
	// true false
}

func ExampleVerifyVM() {
	c, err := ec2job.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	// Without the lifecycle configuration and an EC2 client, VerifyVM
	// refuses before it reads any state or sends any request.
	_, err = ec2job.VerifyVM(context.Background(), "/nonexistent", nil, nil, c)
	fmt.Println(err)
	// Output:
	// ec2job: VerifyVM needs both configurations and a client
}

func ExampleCreate() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	st := e.j.Status()
	fmt.Println(st.Method, st.Desired, st.Upload, st.Submit, st.Durable)
	fmt.Println(len(st.RequestID), st.InstanceID, st.Address)
	// Output:
	// SubmitWorkspaceJob run none none true
	// 32 i-0123456789abcdef0 10.0.0.5
}

func ExampleOpen() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	id := e.j.Status().RequestID

	// While the job is open, another owner is refused.
	_, err = ec2job.Open(e.dir)
	if !errors.Is(err, ec2job.ErrBusy) {
		e.err = err
	}
	fmt.Println(errors.Is(err, ec2job.ErrBusy))

	if err := e.j.Close(); err != nil {
		return // e.close keeps the Close's error
	}
	e.j, err = ec2job.Open(e.dir)
	if err != nil {
		e.err = err
		return
	}
	fmt.Println(e.j.Status().RequestID == id)
	// Output:
	// true
	// true
}

func ExampleJob_Close() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Every Close returns the first Close's result; the closed Job keeps
	// its status and refuses every other call.
	fmt.Println(e.j.Close(), e.j.Close())
	fmt.Println(errors.Is(e.j.RequestCancel(), os.ErrClosed), e.j.Status().Desired)
	// Output:
	// <nil> <nil>
	// true run
}

func ExampleJob_Bind() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(e.j.Bind(e.cfg, e.vm))

	moved := e.vm
	moved.Address = netip.MustParseAddr("10.0.0.6")
	fmt.Println(errors.Is(e.j.Bind(e.cfg, moved), ec2job.ErrMismatch))
	// Output:
	// <nil>
	// true
}

func ExampleJob_RequestCancel() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	// A job never submitted ends at its cancel; a repeated cancel is the
	// same.
	fmt.Println(e.j.RequestCancel(), e.j.RequestCancel())
	st := e.j.Status()
	fmt.Println(st.Desired, st.State, st.Output)
	fmt.Println(st.Ended(), st.Succeeded(), e.j.NeedsRemote(), e.j.Known())
	// Output:
	// <nil> <nil>
	// cancel NEVER_SUBMITTED not-used
	// true false false false
}

func ExampleJob_CheckResume() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(e.j.CheckResume())
	if err := e.j.RequestCancel(); err != nil {
		fmt.Println(err)
		return
	}
	// The journal alone refuses a resume, before any VM or SSH request.
	fmt.Println(e.j.CheckResume())
	// Output:
	// <nil>
	// ec2job: cancel was requested; the job is never run again
}

func ExampleJob_Attach() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := e.j.Bind(e.cfg, e.vm); err != nil {
		fmt.Println(err)
		return
	}
	var t noRequests
	if err := e.j.Attach(newExampleSession(e.cfg, e.vm.Address, &t)); err != nil {
		fmt.Println(err)
		return
	}
	// A session to another address is refused.
	other := newExampleSession(e.cfg, netip.MustParseAddr("10.0.0.6"), &t)
	fmt.Println(errors.Is(e.j.Attach(other), ec2job.ErrMismatch))

	// A job without a known ID is not looked up.
	st, err := e.j.Refresh(context.Background())
	fmt.Println(st.JobID == "", err, t.n.Load())
	// Output:
	// true
	// true <nil> 0
}

func ExampleJob_NextLogs() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := e.j.Bind(e.cfg, e.vm); err != nil {
		fmt.Println(err)
		return
	}
	var t noRequests
	if err := e.j.Attach(newExampleSession(e.cfg, e.vm.Address, &t)); err != nil {
		fmt.Println(err)
		return
	}
	// A job without a known ID has no log, and nothing is requested.
	var out bytes.Buffer
	page, err := e.j.NextLogs(context.Background(), &out)
	fmt.Println(err)
	fmt.Println(page.NextOffset, out.Len(), t.n.Load())

	// Only the last clean copy can be committed as the checkpoint.
	fmt.Println(e.j.CommitLogs(page))
	// Output:
	// ec2job: no job ID is known; there is no log to read
	// 0 0 0
	// ec2job: the page is not the last clean log copy
}

func ExampleJob_Cancel() {
	e, err := newExampleJob()
	defer e.close()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := e.j.Bind(e.cfg, e.vm); err != nil {
		fmt.Println(err)
		return
	}
	var t noRequests
	if err := e.j.Attach(newExampleSession(e.cfg, e.vm.Address, &t)); err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()
	_, err = e.j.Cancel(ctx, ec2job.Wait{})
	fmt.Println(err)

	// The cancel is latched first. A job never submitted and with no
	// known upload needs no request: there is nothing to cancel, and a
	// resume is refused.
	if err := e.j.RequestCancel(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(e.j.NeedsRemote())
	_, err = e.j.Resume(ctx, ec2job.Wait{})
	fmt.Println(err)
	fmt.Println(t.n.Load())
	// Output:
	// ec2job: request the cancel first
	// false
	// ec2job: cancel was requested; the job is never run again
	// 0
}
