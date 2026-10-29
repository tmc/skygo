package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var workspaceDefaultHTTPClient = http.DefaultClient
var workspaceDefaultHTTPTransport = http.DefaultTransport

type workspaceFlags struct {
	sync          bool
	state, resume string
}

func (f *workspaceFlags) flags(c *cobra.Command) {
	c.Flags().BoolVar(&f.sync, "sync-workdir", false, "Upload the complete trusted, quiescent SkyPilot workdir using a private durable journal")
	c.Flags().StringVar(&f.state, "workspace-state", "", "Private directory for a new or matching workspace intent")
	c.Flags().StringVar(&f.resume, "resume-workspace", "", "Resume only the frozen workspace intent in this private directory")
}

func (f workspaceFlags) changed(c *cobra.Command) bool {
	return c.Flags().Changed("sync-workdir") || c.Flags().Changed("workspace-state") || c.Flags().Changed("resume-workspace")
}

func (f workspaceFlags) validate(c *cobra.Command, file string) error {
	if c.Flags().Changed("resume-workspace") {
		if f.resume == "" || file != "" {
			return errors.New("--resume-workspace requires a directory and no task path")
		}
		for _, name := range []string{"sync-workdir", "workspace-state", "file", "env", "output", "workdir-ref", "shell", "target", "request-id", "request-domain", "remote-data"} {
			if c.Flags().Changed(name) {
				return errors.New("--resume-workspace refuses task and placement overrides")
			}
		}
		return nil
	}
	if !f.sync || file == "" {
		return errors.New("--sync-workdir requires true and a SkyPilot task path")
	}
	if c.Flags().Changed("workspace-state") && f.state == "" {
		return errors.New("--workspace-state requires a private directory")
	}
	if c.Flags().Changed("workdir-ref") || c.Flags().Changed("remote-data") {
		return errors.New("--sync-workdir refuses --workdir-ref and --remote-data")
	}
	return nil
}

// workspaceIdentity is deliberately narrower than the general client: a
// selected opaque or SSH transport cannot establish a durable HTTP identity.
func (o *options) workspaceIdentity() (string, string, error) {
	client := o.httpClient()
	base := workspaceDefaultHTTPClient
	if http.DefaultClient != base || base.Transport != nil || base.CheckRedirect != nil || base.Jar != nil || http.DefaultTransport != workspaceDefaultHTTPTransport {
		return "", "", errors.New("workspace sync requires the plain default HTTP transport")
	}
	if o.workspaceHTTP == nil {
		if client != base {
			return "", "", errors.New("workspace sync refuses an unverified HTTP client")
		}
	} else if client != o.workspaceHTTP || client.Transport != base.Transport || client.Timeout != base.Timeout || client.Jar != nil || client.CheckRedirect == nil {
		return "", "", errors.New("workspace HTTP binding changed")
	}
	u, err := url.Parse(o.endpoint)
	if err != nil || u.User != nil || u.Opaque != "" || u.Host == "" || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Path != "" && u.Path != "/" || u.Scheme != "http" && u.Scheme != "https" {
		return "", "", errors.New("workspace sync requires a canonical HTTP or HTTPS endpoint")
	}
	endpoint := u.Scheme + "://" + strings.ToLower(u.Host)
	digest := sha256.Sum256([]byte(o.token))
	return endpoint, hex.EncodeToString(digest[:]), nil
}

// workspaceOptions shares the verified transport and timeout without mutating
// the command's general client. Redirects return their original response to
// Connect; no workspace request may acquire a different endpoint by redirect.
func (o *options) workspaceOptions() (*options, error) {
	if _, _, err := o.workspaceIdentity(); err != nil {
		return nil, err
	}
	if o.workspaceHTTP != nil {
		return o, nil
	}
	client := *workspaceDefaultHTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	scoped := *o
	scoped.http, scoped.workspaceHTTP = &client, &client
	return &scoped, nil
}

func (o *options) workspaceContinuity(r *workspaceRun) error {
	if r.failed != nil {
		return &workspaceRPCError{cause: r.failed}
	}
	endpoint, credential, err := o.workspaceIdentity()
	if err != nil {
		return err
	}
	if endpoint != r.intent.Endpoint || credential != r.intent.CredentialSHA256 {
		return errors.New("workspace endpoint or credential changed; keep the existing intent")
	}
	return nil
}

func workspaceDocument(input []byte, lower tasklower.Options, in skypilotInputs) (*taskyaml.Document, tasklower.Options, error) {
	sky, err := skypilotTask(input)
	if err != nil {
		return nil, lower, err
	}
	if !sky {
		return nil, lower, errors.New("--sync-workdir requires SkyPilot run YAML")
	}
	lower, env, err := in.options(lower)
	if err != nil {
		return nil, lower, err
	}
	doc, err := taskyaml.Parse(bytes.NewReader(input), taskyaml.Options{EnvOverrides: env})
	if err != nil {
		return nil, lower, err
	}
	if err := tasklower.CheckWorkspace(doc, lower); err != nil {
		return nil, lower, err
	}
	return doc, lower, nil
}

func frozenWorkspaceDocument(intent workspaceIntent) (*taskyaml.Document, tasklower.Options, error) {
	lower := tasklower.Options{Shell: intent.Shell, Target: intent.Target}
	if intent.Infra != "" || intent.BindingTarget != "" {
		lower.InfraBinding = &tasklower.InfraBinding{Infra: intent.Infra, Target: intent.BindingTarget}
	}
	return workspaceDocument(intent.YAML, lower, skypilotInputs{env: intent.Env, outputs: intent.Outputs})
}

func (o *options) runWorkspaceCommand(cmd *cobra.Command, file string, lower tasklower.Options, in skypilotInputs, flags workspaceFlags, id, domain string, wait bool) (err error) {
	if err := flags.validate(cmd, file); err != nil {
		return err
	}
	o, err = o.workspaceOptions()
	if err != nil {
		return err
	}
	endpoint, credential, err := o.workspaceIdentity()
	if err != nil {
		return err
	}
	intent := workspaceIntent{Version: 1, Endpoint: endpoint, CredentialSHA256: credential}
	directory := flags.resume
	if flags.resume == "" {
		if cmd.Flags().Changed("request-id") && !validRequestID(id) {
			return errors.New("request ID requires 1..128 letters, digits, underscores or hyphens")
		}
		if cmd.Flags().Changed("request-domain") {
			if _, err := parseRequestDomain(domain); err != nil {
				return err
			}
		}
		input, err := readTaskBytes(cmd, file)
		if err != nil {
			return err
		}
		doc, selected, err := workspaceDocument(input, lower, in)
		if err != nil {
			return err
		}
		path, err := resolvedWorkspacePath(*doc.Task.Workdir)
		if err != nil {
			return err
		}
		intent.RequestID, intent.YAML, intent.Shell, intent.Target, intent.Workdir = id, input, selected.Shell, selected.Target, path
		intent.RequestedDomain = domain
		intent.Env, intent.Outputs = append([]string(nil), in.env...), append([]string(nil), selected.Outputs...)
		if selected.InfraBinding != nil {
			intent.Infra, intent.BindingTarget = selected.InfraBinding.Infra, selected.InfraBinding.Target
		}
		directory = flags.state
	} else if o.clientOptions.Infra != "" || o.clientOptions.Target != "" {
		return errors.New("workspace resume refuses a transport placement override")
	}
	run, err := openWorkspaceRun(directory, intent)
	if err != nil {
		return err
	}
	o.workspaceOwner = run
	defer func() { err = errors.Join(err, run.Close()) }()
	if err := o.workspaceContinuity(run); err != nil {
		return err
	}
	if len(run.archive) == 0 {
		archive, _, err := snapshotWorkspace(run.intent.Workdir, run.dir)
		if err != nil {
			return err
		}
		if err := run.saveArchive(archive); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "workspace-state: %q\nrequest-id: %s\n", run.dir, run.intent.RequestID); err != nil {
		return fmt.Errorf("write workspace resume hint: %w", err)
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
	defer cancel()
	job, err := o.continueWorkspace(ctx, run, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	if wait {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "job: %s\n", job.Id); err != nil {
			return err
		}
		// Every wait read uses the same immutable resolved credential/endpoint.
		if err := o.workspaceContinuity(run); err != nil {
			return err
		}
		job, err = o.waitState(cmd.Context(), o.client(), job, run)
		if err != nil {
			return err
		}
	}
	if err := o.job(cmd.OutOrStdout(), job); err != nil {
		return err
	}
	if wait && job.State != v1.JobState_JOB_STATE_SUCCEEDED {
		return &jobError{job}
	}
	return nil
}

func (o *options) continueWorkspace(ctx context.Context, run *workspaceRun, diagnostics io.Writer) (*v1.Job, error) {
	if err := o.workspaceContinuity(run); err != nil {
		return nil, err
	}
	if run.state.Phase == "accepted" {
		r, err := o.client().GetJob(ctx, request(o, &v1.GetJobRequest{Id: run.state.JobID}))
		if err != nil {
			return nil, err
		}
		if r == nil || r.Msg == nil || r.Msg.Job == nil || r.Msg.Job.Id != run.state.JobID {
			return nil, errors.New("server returned a different accepted workspace job")
		}
		return r.Msg.Job, nil
	}
	if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1, v1.Capability_CAPABILITY_ARTIFACT_BEGIN_REQUESTS_V1); err != nil {
		return nil, err
	}
	if run.state.Domain == nil {
		if err := o.workspaceContinuity(run); err != nil {
			return nil, err
		}
		r, err := o.client().GetRetention(ctx, request(o, &v1.GetRetentionRequest{}))
		if err != nil {
			return nil, err
		}
		if r == nil || r.Msg == nil || !r.Msg.Enabled || r.Msg.AcceptUntil == nil || r.Msg.AcceptUntil.CheckValid() != nil || !time.Now().Before(r.Msg.AcceptUntil.AsTime()) {
			return nil, errors.New("workspace sync requires an accepting retained request domain")
		}
		text, err := domainText(r.Msg.Domain)
		if err != nil {
			return nil, err
		}
		if run.intent.RequestedDomain != "" && text != run.intent.RequestedDomain {
			return nil, errors.New("requested workspace domain is not the accepting domain")
		}
		next := run.state
		next.Domain, next.Phase = proto.Clone(r.Msg.Domain).(*v1.RequestDomain), "begin"
		if err := run.saveState(next); err != nil {
			return nil, err
		}
	} else if run.intent.RequestedDomain != "" {
		text, err := domainText(run.state.Domain)
		if err != nil || text != run.intent.RequestedDomain {
			return nil, errors.New("workspace request domain differs from the saved intent")
		}
	}
	if len(run.task) == 0 {
		ref, err := o.uploadWorkspaceSnapshot(ctx, run)
		if err != nil {
			return nil, err
		}
		doc, lower, err := frozenWorkspaceDocument(run.intent)
		if err != nil {
			return nil, err
		}
		lower.Workdir = wireArtifactRef(ref)
		task, notes, err := tasklower.Lower(doc, lower)
		if err != nil {
			return nil, err
		}
		if err := writeLowerNotes(diagnostics, notes); err != nil {
			return nil, err
		}
		encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(task)
		if err != nil {
			return nil, err
		}
		if err := run.saveTask(encoded); err != nil {
			return nil, err
		}
	}
	var task v1.Task
	if err := protojson.Unmarshal(run.task, &task); err != nil {
		return nil, errors.New("invalid frozen workspace task")
	}
	if err := o.workspaceContinuity(run); err != nil {
		return nil, err
	}
	next := run.state
	next.Phase = "submit"
	if err := run.saveState(next); err != nil {
		return nil, err
	}
	if err := o.workspaceContinuity(run); err != nil {
		return nil, err
	}
	job, err := o.submitWorkspace(ctx, o.client(), &task, run.intent.RequestID, run.state.Domain)
	if err != nil {
		return nil, err
	}
	if !validHexID(job.Id) {
		return nil, errors.New("server returned an invalid workspace job ID")
	}
	next = run.state
	next.Phase, next.JobID = "accepted", job.Id
	if err := run.saveState(next); err != nil {
		return nil, err
	}
	return job, nil
}

// workspaceClient changes only the two existing generated-client factories.
// It observes bodies returned by Do, not owners kept inside net/http on error.
func (o *options) workspaceClient() (connect.HTTPClient, []connect.ClientOption) {
	if o.workspaceOwner == nil {
		return o.httpClient(), nil
	}
	run := o.workspaceOwner
	interceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			if err := o.workspaceContinuity(run); err != nil {
				return nil, err
			}
			response, err := next(ctx, request)
			if run.failed != nil {
				run.failed = &workspaceRPCError{cause: errors.Join(err, run.failed)}
				return nil, run.failed
			}
			return response, err
		}
	})
	return workspaceHTTPDo{client: o.httpClient(), options: o, run: run}, []connect.ClientOption{connect.WithInterceptors(interceptor)}
}

type workspaceHTTPDo struct {
	client  connect.HTTPClient
	options *options
	run     *workspaceRun
}

func (d workspaceHTTPDo) Do(request *http.Request) (*http.Response, error) {
	if err := d.options.workspaceContinuity(d.run); err != nil {
		return nil, err
	}
	response, err := d.client.Do(request)
	if response != nil && response.Body != nil {
		response.Body = &workspaceResponseBody{original: response.Body, run: d.run}
	}
	return response, err
}

// A generated unary error may discard CloseResponse's error. This concrete
// body still poisons its original workspace owner on the first failed Close.
type workspaceResponseBody struct {
	original io.ReadCloser
	run      *workspaceRun
	once     sync.Once
	closeErr error
}

func (b *workspaceResponseBody) Read(p []byte) (int, error) { return b.original.Read(p) }
func (b *workspaceResponseBody) Close() error {
	b.once.Do(func() {
		if err := b.original.Close(); err != nil {
			b.closeErr = &workspaceResponseCloseError{body: b, cause: err}
			b.run.failed = errors.Join(b.run.failed, b.closeErr)
		}
	})
	return b.closeErr
}

type workspaceResponseCloseError struct {
	body  *workspaceResponseBody
	cause error
}

func (e *workspaceResponseCloseError) Error() string { return "workspace response closure unresolved" }
func (e *workspaceResponseCloseError) Unwrap() error { return e.cause }

type workspaceRPCError struct{ cause error }

func (e *workspaceRPCError) Error() string { return "workspace response closure unresolved" }
func (e *workspaceRPCError) Unwrap() error { return e.cause }
