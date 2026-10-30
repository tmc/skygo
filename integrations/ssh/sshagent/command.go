package sshagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/cli"
)

// Command describes one canonical skygo command. Host is an optional
// operator-selected placement label; it defaults to Config.Address. It
// does not select a connection, host key, credential or server target.
// Nil streams use the command's standard input, output and error streams.
type Command struct {
	Args           []string
	Host           string
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

// CheckCommand reports whether args needs a remote agent. It refuses
// endpoint and credential overrides, local-only commands and dashboard.
// Help needs no agent. CheckCommand opens no file or connection.
func CheckCommand(args []string) (bool, error) {
	root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
	for _, a := range args {
		if a == "--" {
			break
		}
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if strings.HasPrefix(a, "-") && (name == "endpoint" || name == "token" || name == "token-file") {
			return false, fmt.Errorf("skygo-ssh: --%s is set by skygo-ssh", name)
		}
		if a == "-h" || a == "--help" {
			return false, nil
		}
	}
	if os.Getenv("SKYGO_TOKEN") != "" {
		return false, errors.New("skygo-ssh: unset SKYGO_TOKEN; skygo-ssh supplies the agent token")
	}
	root.InitDefaultHelpCmd()
	cmd, _, err := root.Find(args)
	if err != nil {
		return false, err
	}
	top := cmd
	for top.Parent() != nil && top.Parent() != root {
		top = top.Parent()
	}
	switch top.Name() {
	case "server", "auth", "state":
		return false, fmt.Errorf("skygo-ssh: %q works on local files; run it on the remote host", top.Name())
	case "dashboard":
		return false, errors.New("skygo-ssh: the dashboard is not supported over SSH")
	case "help":
		return false, nil
	}
	return cmd.Runnable(), nil
}

// RunCommand opens the agent described by cfg and runs one canonical
// skygo command through it. Help does not open an agent. Every request
// uses the session's captured bearer in place of caller credentials.
// The command owns its original transport, Read and first Close frames;
// an unsettled frame fences the session and remains reachable through the
// returned error. Callers retain such an error and do not reopen the host.
// RunCommand never retries a command, stops an agent or cancels its jobs.
// Errors retain canonical CLI identity, including failed waited jobs.
func RunCommand(ctx context.Context, cfg Config, command Command) error {
	needed, err := CheckCommand(command.Args)
	if err != nil {
		return err
	}
	if !needed {
		root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
		command.configure(root)
		return root.ExecuteContext(ctx)
	}
	o, err := prepare(ctx, cfg)
	if err != nil {
		return err
	}
	s, err := openAgent(ctx, o)
	if err != nil {
		return err
	}
	host := command.Host
	if host == "" {
		host = cfg.Address
	}
	return runCommand(ctx, commandAgent{client: s.HTTPClient(), owner: s, endpoint: s.Endpoint(), tokenFile: s.TokenFile(), bearer: s.Bearer(), host: host, target: cfg.Target}, command)
}

// commandAgent is the already verified agent. Tests model its SSH edge;
// production constructs it only after prepare and agentsession.Open.
type commandAgent struct {
	bodyFault func(*call, io.ReadCloser) io.ReadCloser // test-only original body fault; nil in production

	client                                    *http.Client
	owner                                     io.Closer
	endpoint, tokenFile, bearer, host, target string
}

type commandFrame struct {
	root   *cobra.Command
	calls  *ledger
	call   *call
	agent  commandAgent
	result error
}

func (c Command) configure(root *cobra.Command) {
	root.SetArgs(c.Args)
	if c.Stdin != nil {
		root.SetIn(c.Stdin)
	}
	if c.Stdout != nil {
		root.SetOut(c.Stdout)
	}
	if c.Stderr != nil {
		root.SetErr(c.Stderr)
	}
}

func runCommand(ctx context.Context, agent commandAgent, command Command) error {
	frame := &commandFrame{agent: agent, calls: newLedger(agent.owner)}
	c, err := frame.calls.admit(false)
	if err != nil {
		return err
	}
	frame.call = c
	// Command-only admission/reclamation is ratified separately; typed
	// Control calls keep their existing default behavior.
	c.command = true
	commandCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()
	hc := *agent.client
	hc.Transport = &commandTransport{call: c, token: agent.bearer, recorder: recorder{l: frame.calls, next: hc.Transport}, bodyFault: agent.bodyFault}
	root := cli.NewWithClient(nil, &hc, cli.ClientOptions{Infra: "ssh/" + agent.host, Target: agent.target})
	frame.root = root
	command.configure(root)
	frame.calls.mu.Lock()
	c.resp = frame
	frame.calls.mu.Unlock()
	err = root.PersistentFlags().Set("endpoint", agent.endpoint)
	if err == nil {
		err = root.PersistentFlags().Set("token-file", agent.tokenFile)
	}
	if err == nil {
		err = root.ExecuteContext(commandCtx)
	}
	frame.result = err
	_, err = c.settle(nil, err)
	// An error holding the call must also hold the actual canonical root
	// and its workspace owners, not just the signer and HTTP body ledger.
	frame.calls.mu.Lock()
	if _, live := frame.calls.live[c]; live {
		c.resp = frame
	}
	frame.calls.mu.Unlock()
	cerr := frame.calls.close()
	if cerr != nil {
		return errors.Join(err, cerr)
	}
	return err
}

// commandTransport binds every request of this one command, including
// relayed requests with their own contexts, to the admitted original.
// Clone preserves the caller's actual context and body; only the private
// call marker and captured session Authorization are replaced.
type commandTransport struct {
	bodyFault func(*call, io.ReadCloser) io.ReadCloser

	call     *call
	token    string
	recorder recorder
}

func (t *commandTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(context.WithValue(req.Context(), callKey{}, t.call))
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	for k := range req.Header {
		if strings.EqualFold(k, "Authorization") {
			delete(req.Header, k)
		}
	}
	req.Header.Set("Authorization", "Bearer "+t.token)
	if r, ok := req.Body.(*refusedRequest); ok && r.c == t.call {
		t.call.l.mu.Lock()
		if r.request == nil {
			r.request = req
		}
		t.call.l.mu.Unlock()
	}
	return t.recorder.RoundTrip(req)
}

// WrapRequestBody guards the original before ReverseProxy captures Close.
// The server's separate native request-body cleanup remains its own owner.
func (t *commandTransport) WrapRequestBody(original io.ReadCloser) io.ReadCloser {
	if original == nil || original == http.NoBody {
		return original
	}
	if r, ok := original.(*refusedRequest); ok && r.c == t.call {
		return r
	}
	if t.bodyFault != nil {
		original = t.bodyFault(t.call, original)
	}
	return &refusedRequest{c: t.call, original: original, done: make(chan struct{})}
}

// refusedRequest is the SAME command-local request body guard, whether
// dispatched or refused. Its owner count is separate from admitted
// response/error/reservation records. All mutable fields use c.l.mu.
type refusedRequest struct {
	c                                    *call
	request                              *http.Request
	original                             io.ReadCloser
	done                                 chan struct{}
	registered, closing, closed, retired bool
	active                               int
	readErr, err                         error
}

func (c *call) stopCommandLocked() context.CancelFunc {
	if !c.command {
		return nil
	}
	stop := c.cancel
	c.cancel = nil
	return stop
}

func (r *refusedRequest) holdLocked() {
	if r.registered {
		return
	}
	r.registered = true
	r.c.refusals = append(r.c.refusals, r)
	r.c.refusalHighWater = max(r.c.refusalHighWater, len(r.c.refusals))
}

func (r *refusedRequest) retireLocked() {
	if !r.closed || r.err != nil || r.readErr != nil || r.active != 0 {
		return
	}
	for i, v := range r.c.refusals {
		if v == r {
			r.c.refusals = slices.Delete(r.c.refusals, i, i+1)
			break
		}
	}
	r.retired = true
	r.request, r.original = nil, nil
}

func (c *call) refuse(req *http.Request, admission error) error {
	r, ok := req.Body.(*refusedRequest)
	if !ok || r.c != c {
		r = &refusedRequest{c: c, request: req, original: req.Body, done: make(chan struct{})}
	}
	l := c.l
	l.mu.Lock()
	if r.request == nil {
		r.request = req
	}
	r.holdLocked()
	c.commandRefused, l.faulted = true, true
	stop := c.stopCommandLocked()
	l.mu.Unlock()
	// Stop before the potentially blocking SAME original Close. This does
	// not join Close or any handler already accepted by the relay.
	if stop != nil {
		stop()
	}
	if err := r.Close(); err != nil {
		l.mu.Lock()
		owned := c.errorLocked()
		l.mu.Unlock()
		return errors.Join(admission, owned)
	}
	return admission
}

func (r *refusedRequest) Read(p []byte) (int, error) {
	c, l := r.c, r.c.l
	l.mu.Lock()
	if r.closing || r.retired {
		l.mu.Unlock()
		return 0, &CallError{kind: kindRefused, stages: stageStreamClosed, owner: c}
	}
	r.holdLocked()
	r.active++
	c.enterLocked()
	original := r.original
	l.mu.Unlock()
	n, err := original.Read(p)
	l.mu.Lock()
	if err != nil && err != io.EOF && r.readErr == nil {
		r.readErr = err
		l.faulted = true
	}
	r.active--
	r.retireLocked()
	c.exitLocked()
	var stop context.CancelFunc
	if l.faulted {
		stop = c.stopCommandLocked()
	}
	if err != nil && err != io.EOF {
		err = c.errorLocked()
	}
	l.mu.Unlock()
	if stop != nil {
		stop()
	}
	return n, err
}

func (r *refusedRequest) Close() error {
	if r.start() {
		r.run()
	}
	<-r.done
	return r.err
}

// start selects the one first Close without waiting for another actor.
// Settlement can therefore retain an already-running original explicitly.
func (r *refusedRequest) start() bool {
	l := r.c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	if r.closing {
		return false
	}
	r.closing = true
	r.holdLocked()
	r.active++
	r.c.refusalPending++
	r.c.enterLocked()
	return true
}

func (r *refusedRequest) run() {
	var err error
	if r.original != nil {
		err = r.original.Close()
	}
	c, l := r.c, r.c.l
	l.mu.Lock()
	r.closed, r.err = true, err
	r.active--
	c.refusalPending--
	if err != nil {
		l.faulted = true
	}
	r.retireLocked()
	c.exitLocked()
	var stop context.CancelFunc
	if l.faulted {
		stop = c.stopCommandLocked()
	}
	l.mu.Unlock()
	if stop != nil {
		stop()
	}
	close(r.done)
}
