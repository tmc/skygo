package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/skycompat/tasklower"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ServerOptions configures the server callback.
type ServerOptions struct {
	DataDir, Listen, Token, AuthFile, TargetFacts string
	// OIDCFile and OIDCJWKSURL configure scoped ID-token authentication.
	OIDCFile, OIDCJWKSURL string
	// ArtifactDir opens an initialized artifact directory; empty disables it.
	ArtifactDir  string
	DisableLocal bool
	// RecoverStoppedArtifactReaders requires the restricted historical-owner premise.
	RecoverStoppedArtifactReaders bool
}

type options struct {
	endpoint, token, tokenFile string
	json                       bool
	timeout                    time.Duration
	http                       *http.Client // used by every generated client
	workspaceHTTP              *http.Client // private no-redirect copy for one workspace invocation
	workspaceOwner             *workspaceRun
	clientOptions              ClientOptions
}

// ClientOptions describes placement known by a selected transport. Empty
// fields supply no infrastructure binding. The transport must verify Target
// at Infra before constructing the command; a plain HTTP client supplies none.
type ClientOptions struct {
	Infra  string
	Target string
}

type jobError struct{ job *skyv1.Job }

func (e *jobError) Error() string {
	return fmt.Sprintf("job %s: %s: %s", e.job.Id, state(e.job.State), e.job.Reason)
}

// ExitCode returns 2 for an unsuccessful waited job, 1 for other errors, or 0.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var e *jobError
	if errors.As(err, &e) {
		return 2
	}
	return 1
}

// New builds the sole command tree. The caller supplies server startup and a
// signal-aware context to ExecuteContext, and prints returned errors once.
func New(server func(context.Context, ServerOptions) error) *cobra.Command {
	return NewWithClient(server, http.DefaultClient, ClientOptions{})
}

// NewWithClient is New with the HTTP client that every Control, Workflow
// and artifact RPC uses. A nil client means http.DefaultClient. The
// dashboard keeps its own client. Config records the selected transport's
// optional infrastructure binding; it does not grant server authority.
func NewWithClient(server func(context.Context, ServerOptions) error, client *http.Client, config ClientOptions) *cobra.Command {
	if client == nil {
		client = http.DefaultClient
	}
	o := &options{endpoint: "http://127.0.0.1:8080", timeout: 30 * time.Second, http: client, clientOptions: config}
	if v := os.Getenv("SKYGO_ENDPOINT"); v != "" {
		o.endpoint = v
	}
	root := &cobra.Command{Use: "skygo", Short: "Run finite jobs on local or Kubernetes targets", SilenceUsage: true, SilenceErrors: true}
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentFlags().StringVar(&o.endpoint, "endpoint", o.endpoint, "Connect server URL (SKYGO_ENDPOINT)")
	root.PersistentFlags().StringVar(&o.token, "token", o.token, "Bearer token (deprecated; prefer --token-file or SKYGO_TOKEN)")
	root.PersistentFlags().StringVar(&o.tokenFile, "token-file", "", "Private bearer token file")
	root.PersistentFlags().BoolVar(&o.json, "json", false, "Write protobuf JSON; logs use one JSON object per chunk")
	root.PersistentFlags().DurationVar(&o.timeout, "timeout", o.timeout, "Per-RPC deadline (log follow and wait use the command context)")
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if localCommand(cmd) {
			return nil
		}
		if o.timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		return o.resolveCredentials(cmd)
	}
	s := ServerOptions{}
	srv := &cobra.Command{Use: "server", Short: "Serve the workload API", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		s.Token = o.token
		if server == nil {
			return errors.New("server startup unavailable")
		}
		warnPhysicalState(cmd, s.DataDir)
		return server(cmd.Context(), s)
	}}
	srv.Flags().StringVar(&s.DataDir, "data-dir", "skygo-data", "Durable state directory")
	srv.Flags().StringVar(&s.ArtifactDir, "artifact-dir", "", "Open an already initialized artifact directory (empty disables artifacts)")
	srv.Flags().StringVar(&s.Listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	srv.Flags().StringVar(&s.TargetFacts, "target-facts", "", "Configured advisory quote and capacity facts (Linux and macOS)")
	srv.Flags().StringVar(&s.AuthFile, "auth-file", "", "Private scoped authentication file")
	srv.Flags().StringVar(&s.OIDCFile, "oidc-file", "", "Private OpenID Connect identity mapping (requires --auth-file)")
	srv.Flags().StringVar(&s.OIDCJWKSURL, "oidc-jwks-url", "", "The one allowed HTTPS key set URL (requires --oidc-file)")
	srv.Flags().BoolVar(&s.RecoverStoppedArtifactReaders, "recover-stopped-artifact-readers", false, "Recover stopped readers only when ALL historical producers have no surviving descriptor copy or use and the same-domain kernel is qualified")
	srv.Flags().BoolVar(&s.DisableLocal, "disable-local", false, "Disable execution inside the server process environment")
	root.AddCommand(srv, o.taskCommand(false), o.taskCommand(true), o.jobs(), o.workflowCommand(), o.dashboardCommand(), o.infoCommand(), o.stateCommand(), o.authCommand(), o.artifactsCommand(), o.environmentCommand(), o.serveCommand(), o.oidcCommand())
	targets := &cobra.Command{Use: "targets", Short: "Inspect configured execution targets"}
	targets.AddCommand(&cobra.Command{Use: "list", Short: "List execution targets", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.client().ListTargets(ctx, request(o, &skyv1.ListTargetsRequest{}))
		if err != nil {
			return err
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tKIND")
		for _, t := range r.Msg.Targets {
			fmt.Fprintf(w, "%s\t%s\n", t.Name, t.Kind)
		}
		return w.Flush()
	}})
	root.AddCommand(targets)
	return root
}

func (o *options) client() skyv1connect.ControlServiceClient {
	client, opts := o.workspaceClient()
	return skyv1connect.NewControlServiceClient(client, o.endpoint, opts...)
}

func (o *options) httpClient() *http.Client {
	if o.http != nil {
		return o.http
	}
	return http.DefaultClient
}
func request[T any](o *options, m *T) *connect.Request[T] {
	r := connect.NewRequest(m)
	if o.token != "" {
		r.Header().Set("Authorization", "Bearer "+o.token)
	}
	return r
}
func writeJSON(w io.Writer, m proto.Message) error {
	b, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", b)
	return err
}
func state(s skyv1.JobState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATE_"))
}
func terminal(s skyv1.JobState) bool {
	return s == skyv1.JobState_JOB_STATE_SUCCEEDED || s == skyv1.JobState_JOB_STATE_FAILED || s == skyv1.JobState_JOB_STATE_CANCELED || s == skyv1.JobState_JOB_STATE_UNKNOWN
}
func (o *options) job(w io.Writer, j *skyv1.Job) error {
	if j == nil {
		return errors.New("server returned no job")
	}
	if o.json {
		return writeJSON(w, j)
	}
	_, err := fmt.Fprintf(w, "%s\t%s\t%s\n", j.Id, j.Target, state(j.State))
	return err
}

func (o *options) taskCommand(run bool) *cobra.Command {
	var file, id, domain string
	var wait, remoteData bool
	var quote quoteFlags
	var lower tasklower.Options
	var inputs skypilotInputs
	var workspace workspaceFlags
	if b := o.clientOptions; b.Infra != "" || b.Target != "" {
		lower.InfraBinding = &tasklower.InfraBinding{Infra: b.Infra, Target: b.Target}
	}
	name, short := "plan", "Validate a task and select an eligible target"
	if run {
		name, short = "run", "Submit a task"
	}
	c := &cobra.Command{Use: name + " [task.yaml | -f task.yaml]", Short: short, Args: taskArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if !run {
			if err := quote.validate(cmd); err != nil {
				return err
			}
		}
		if len(args) == 1 {
			file = args[0]
		}
		if run && workspace.changed(cmd) {
			return o.runWorkspaceCommand(cmd, file, lower, inputs, workspace, id, domain, wait)
		}
		task, err := readTaskInputs(cmd, file, lower, inputs)
		if err != nil {
			return err
		}
		if len(task.DataMounts) != 0 && !remoteData {
			return errors.New("data mounts require --remote-data")
		}
		if remoteData {
			if err := validateDataTask(task); err != nil {
				return err
			}
			if !run && quote.objective != "first-fit" {
				return errors.New("remote data does not support rate quoting")
			}
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		client := o.client()
		if !run {
			if quote.objective == "lowest-rate" {
				return o.quoteTask(ctx, cmd, task, quote)
			}
			var r *skyv1.PlanTaskResponse
			if remoteData {
				r, err = o.planDataTask(ctx, client, task)
			} else {
				r, err = o.planTask(ctx, client, task)
			}
			if err == nil && !remoteData && workspaceTask(task) {
				_, err = o.requireCapabilities(ctx, r.Target, skyv1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1)
			}
			if err != nil {
				return err
			}
			if o.json {
				return writeJSON(cmd.OutOrStdout(), r)
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), r.Target); err != nil {
				return err
			}
			for _, r := range r.Rejections {
				fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s\n", r.Target, r.Reason)
			}
			return nil
		}
		j, err := o.submitTask(cmd, ctx, client, task, id, domain, file, cmd.Flags().Changed("request-domain"), cmd.Flags().Changed("request-id"))
		if err != nil {
			return err
		}
		if wait {
			fmt.Fprintf(cmd.ErrOrStderr(), "job: %s\n", j.Id)
			j, err = o.wait(cmd.Context(), client, j)
			if err != nil {
				return err
			}
		}
		if err = o.job(cmd.OutOrStdout(), j); err != nil {
			return err
		}
		if wait && j.State != skyv1.JobState_JOB_STATE_SUCCEEDED {
			return &jobError{j}
		}
		return nil
	}}
	c.Flags().StringVarP(&file, "file", "f", "", "Task YAML path, or - for stdin")
	c.Flags().StringVar(&lower.Shell, "shell", "", "Clean absolute shell that runs SkyPilot run text as SHELL -c TEXT (default "+defaultShell+")")
	c.Flags().StringVar(&lower.Target, "target", "", "Target for a SkyPilot task; skygo task files declare their own target")
	c.Flags().StringVar(&inputs.workdirRef, "workdir-ref", "", "Uploaded workdir reference JSON for a SkyPilot task")
	c.Flags().StringArrayVar(&inputs.outputs, "output", nil, "Workdir output root for a SkyPilot task (repeatable)")
	c.Flags().StringArrayVar(&inputs.env, "env", nil, "Explicit KEY=VALUE for a SkyPilot task (repeatable)")
	c.Flags().BoolVar(&remoteData, "remote-data", false, "Use the explicit target's qualified remote data route")
	if !run {
		quote.flags(c)
	}
	if run {
		workspace.flags(c)
		c.Flags().StringVar(&id, "request-id", "", "Stable request ID to reuse when retrying submission")
		c.Flags().StringVar(&domain, "request-domain", "", "Explicit retained request domain LINEAGE:EPOCH; reuse with the request ID")
		c.Flags().BoolVar(&wait, "wait", false, "Wait for terminal status; unsuccessful jobs exit 2")
	}
	return c
}

// taskArgs accepts at most one task path, given either as an argument or
// with --file.
func taskArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 1 {
		return errors.New("task accepts one path")
	}
	if len(args) == 1 && cmd.Flags().Changed("file") {
		return errors.New("task path given both as an argument and with --file")
	}
	return nil
}

func (o *options) wait(ctx context.Context, c skyv1connect.ControlServiceClient, j *skyv1.Job) (*skyv1.Job, error) {
	return o.waitState(ctx, c, j, nil)
}

func (o *options) waitState(ctx context.Context, c skyv1connect.ControlServiceClient, j *skyv1.Job, run *workspaceRun) (*skyv1.Job, error) {
	for !terminal(j.State) {
		t := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		if run != nil {
			if err := o.workspaceContinuity(run); err != nil {
				return nil, err
			}
		}
		rpcctx, cancel := context.WithTimeout(ctx, o.timeout)
		r, err := c.GetJob(rpcctx, request(o, &skyv1.GetJobRequest{Id: j.Id}))
		cancel()
		if err != nil {
			return nil, retentionDiagnostic(err)
		}
		j = r.Msg.Job
		if j == nil {
			return nil, errors.New("server returned no job")
		}
	}
	return j, nil
}

func (o *options) jobs() *cobra.Command {
	c := &cobra.Command{Use: "jobs", Short: "Inspect and control submitted jobs"}
	var size int32
	var page string
	list := &cobra.Command{Use: "list", Short: "List one page of jobs", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.client().ListJobs(ctx, request(o, &skyv1.ListJobsRequest{PageSize: size, PageToken: page}))
		if err != nil {
			return err
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tTARGET\tSTATE")
		for _, j := range r.Msg.Jobs {
			if err := o.job(w, j); err != nil {
				return err
			}
		}
		if r.Msg.NextPageToken != "" {
			fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", r.Msg.NextPageToken)
		}
		return w.Flush()
	}}
	list.Flags().Int32Var(&size, "page-size", 100, "Maximum jobs per page")
	list.Flags().StringVar(&page, "page-token", "", "Continuation token from the previous page")
	get := &cobra.Command{Use: "get ID", Short: "Show a job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.client().GetJob(ctx, request(o, &skyv1.GetJobRequest{Id: args[0]}))
		if err != nil {
			return retentionDiagnostic(err)
		}
		return o.jobDetails(cmd.OutOrStdout(), r.Msg.Job)
	}}
	var yes bool
	cancelCmd := &cobra.Command{Use: "cancel ID --yes", Short: "Request job cancellation", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !yes {
			return errors.New("cancellation requires --yes")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.client().CancelJob(ctx, request(o, &skyv1.CancelJobRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		return o.job(cmd.OutOrStdout(), r.Msg.Job)
	}}
	cancelCmd.Flags().BoolVarP(&yes, "yes", "y", false, "Confirm cancellation")
	var follow bool
	var offset int64
	var attempt uint32
	var cursor string
	logs := &cobra.Command{Use: "logs ID", Short: "Read retained logs; interrupting does not cancel the job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return o.readLogs(cmd, args[0], offset, attempt, follow, cursor, cmd.Flags().Changed("cursor"))
	}}
	logs.Flags().BoolVarP(&follow, "follow", "f", false, "Follow until terminal status or interruption")
	logs.Flags().Int64Var(&offset, "offset", 0, "Byte offset into retained logs")
	logs.Flags().Uint32Var(&attempt, "attempt", 0, "Read one attempt (one-based; default selects current once)")
	logs.Flags().StringVar(&cursor, "cursor", "", "Read a retained cursor stream; empty starts a new fixed cursor")
	c.AddCommand(list, get, cancelCmd, logs, o.retentionInfoCommand(), o.pruneCommand())
	return c
}

// taskFile deliberately exposes only the portable wire fields.
type taskFile struct {
	Argv             []string             `yaml:"argv"`
	Env              map[string]string    `yaml:"env"`
	Directory        string               `yaml:"directory"`
	Image            string               `yaml:"image"`
	CPUMillis        int64                `yaml:"cpu_millis"`
	MemoryBytes      int64                `yaml:"memory_bytes"`
	Accelerator      string               `yaml:"accelerator"`
	AcceleratorCount int32                `yaml:"accelerator_count"`
	Target           string               `yaml:"target"`
	TimeoutSeconds   int64                `yaml:"timeout_seconds"`
	CPULimitMillis   int64                `yaml:"cpu_limit_millis"`
	MemoryLimitBytes int64                `yaml:"memory_limit_bytes"`
	TargetLabels     map[string]string    `yaml:"target_labels"`
	NodeSelector     map[string]string    `yaml:"node_selector"`
	Retry            *backend.RetryPolicy `yaml:"retry"`
	Workdir          *artifact.Ref        `yaml:"workdir"`
	Setup            []string             `yaml:"setup"`
	Outputs          []string             `yaml:"outputs"`
	DataMounts       []backend.DataMount  `yaml:"data_mounts"`
}

// readTask reads a skygo task file or SkyPilot task YAML from path.
// Lower supplies semantics for SkyPilot tasks only.
func readTask(cmd *cobra.Command, path string, lower tasklower.Options) (*skyv1.Task, error) {
	return readTaskInputs(cmd, path, lower, skypilotInputs{})
}

func readTaskInputs(cmd *cobra.Command, path string, lower tasklower.Options, inputs skypilotInputs) (*skyv1.Task, error) {
	b, err := readTaskBytes(cmd, path)
	if err != nil {
		return nil, err
	}
	sky, err := skypilotTask(b)
	if err != nil {
		return nil, err
	}
	if sky {
		if cmd.Flags().Changed("workdir-ref") && inputs.workdirRef == "" {
			return nil, errors.New("invalid --workdir-ref")
		}
		return inputs.lower(cmd.ErrOrStderr(), b, lower)
	}
	if cmd.Flags().Changed("env") || cmd.Flags().Changed("output") || cmd.Flags().Changed("workdir-ref") {
		return nil, errors.New("--env, --output and --workdir-ref apply only to SkyPilot task YAML")
	}
	if lower.Shell != "" || lower.Target != "" {
		return nil, errors.New("--shell and --target apply only to SkyPilot task YAML")
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	var f taskFile
	if err := d.Decode(&f); err != nil {
		return nil, fmt.Errorf("decode task: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("task must contain exactly one YAML document")
	}
	if len(f.Argv) == 0 {
		return nil, errors.New("task argv must not be empty")
	}
	if _, err := backend.NormalizeRetry(f.Retry); err != nil {
		return nil, fmt.Errorf("retry: %w", err)
	}
	if err := backend.ValidateWorkspace(backend.Task{Directory: f.Directory, Workdir: f.Workdir, Setup: f.Setup, Outputs: f.Outputs}); err != nil {
		return nil, fmt.Errorf("workspace: %w", err)
	}
	if len(f.DataMounts) != 0 {
		if err := backend.ValidateDataTask(backend.Task{Directory: f.Directory, Workdir: f.Workdir, Setup: f.Setup, Outputs: f.Outputs, DataMounts: f.DataMounts}, nil); err != nil {
			return nil, fmt.Errorf("data mounts: %w", err)
		}
	}
	var mounts []*skyv1.DataMount
	for _, m := range f.DataMounts {
		mounts = append(mounts, &skyv1.DataMount{Reference: m.Reference, Destination: m.Destination, ReadOnly: m.ReadOnly})
	}
	var workdir *skyv1.ArtifactRef
	if f.Workdir != nil {
		workdir = &skyv1.ArtifactRef{Id: f.Workdir.ID, Sha256: f.Workdir.SHA256, Size: f.Workdir.Size}
	}
	var retry *skyv1.RetryPolicy
	if f.Retry != nil {
		retry = &skyv1.RetryPolicy{MaxAttempts: f.Retry.MaxAttempts, DelaySeconds: f.Retry.DelaySeconds, ExitCodes: f.Retry.ExitCodes}
	}
	return &skyv1.Task{DataMounts: mounts, Workdir: workdir, Setup: f.Setup, Outputs: f.Outputs, Retry: retry, Argv: f.Argv, Env: f.Env, Directory: f.Directory, Image: f.Image, CpuMillis: f.CPUMillis, MemoryBytes: f.MemoryBytes, Accelerator: f.Accelerator, AcceleratorCount: f.AcceleratorCount, Target: f.Target, TimeoutSeconds: f.TimeoutSeconds, CpuLimitMillis: f.CPULimitMillis, MemoryLimitBytes: f.MemoryLimitBytes, TargetLabels: f.TargetLabels, NodeSelector: f.NodeSelector}, nil
}

// readTaskBytes is the shared bounded input reader for both task forms.
func readTaskBytes(cmd *cobra.Command, path string) (data []byte, err error) {
	if path == "" {
		return nil, errors.New("task requires --file or a path argument")
	}
	r := cmd.InOrStdin()
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				data = nil
				err = errors.Join(err, &taskInputCloseError{file: f, err: closeErr})
			}
		}()
		r = f
	}
	b, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return nil, errors.New("task exceeds 1 MiB")
	}
	return b, nil
}

// taskInputCloseError retains the original input descriptor after a failed
// Close; no bytes from that unresolved read can authorize a later RPC.
type taskInputCloseError struct {
	file *os.File
	err  error
}

func (e *taskInputCloseError) Error() string { return e.err.Error() }
func (e *taskInputCloseError) Unwrap() error { return e.err }
