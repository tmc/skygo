package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ServerOptions configures the server callback.
type ServerOptions struct {
	DataDir, Listen, Token string
	DisableLocal           bool
}

type options struct {
	endpoint, token string
	json            bool
	timeout         time.Duration
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
	o := &options{endpoint: "http://127.0.0.1:8080", token: os.Getenv("SKYGO_TOKEN"), timeout: 30 * time.Second}
	if v := os.Getenv("SKYGO_ENDPOINT"); v != "" {
		o.endpoint = v
	}
	root := &cobra.Command{Use: "skygo", Short: "Run finite jobs on local or Kubernetes targets", SilenceUsage: true, SilenceErrors: true}
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentFlags().StringVar(&o.endpoint, "endpoint", o.endpoint, "Connect server URL (SKYGO_ENDPOINT)")
	root.PersistentFlags().StringVar(&o.token, "token", o.token, "Bearer token (SKYGO_TOKEN)")
	root.PersistentFlags().BoolVar(&o.json, "json", false, "Write protobuf JSON; logs use one JSON object per chunk")
	root.PersistentFlags().DurationVar(&o.timeout, "timeout", o.timeout, "Per-RPC deadline (log follow and wait use the command context)")
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if o.timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		return nil
	}
	s := ServerOptions{}
	srv := &cobra.Command{Use: "server", Short: "Serve the workload API", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		s.Token = o.token
		if server == nil {
			return errors.New("server startup unavailable")
		}
		return server(cmd.Context(), s)
	}}
	srv.Flags().StringVar(&s.DataDir, "data-dir", "skygo-data", "Durable state directory")
	srv.Flags().StringVar(&s.Listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	srv.Flags().BoolVar(&s.DisableLocal, "disable-local", false, "Disable execution inside the server process environment")
	root.AddCommand(srv, o.taskCommand(false), o.taskCommand(true), o.jobs())
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
	return skyv1connect.NewControlServiceClient(http.DefaultClient, o.endpoint)
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
	var file, id string
	var wait bool
	name, short := "plan", "Validate a task and select an eligible target"
	if run {
		name, short = "run", "Submit a task"
	}
	c := &cobra.Command{Use: name + " -f task.yaml", Short: short, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		task, err := readTask(cmd, file)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		client := o.client()
		if !run {
			r, err := client.PlanTask(ctx, request(o, &skyv1.PlanTaskRequest{Task: task}))
			if err != nil {
				return err
			}
			if o.json {
				return writeJSON(cmd.OutOrStdout(), r.Msg)
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), r.Msg.Target); err != nil {
				return err
			}
			for _, r := range r.Msg.Rejections {
				fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s\n", r.Target, r.Reason)
			}
			return nil
		}
		if id == "" {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				return err
			}
			id = hex.EncodeToString(b[:])
		}
		// Print before sending: even a lost response can be retried safely.
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-id: %s\n", id); err != nil {
			return fmt.Errorf("write request id: %w", err)
		}
		r, err := client.SubmitJob(ctx, request(o, &skyv1.SubmitJobRequest{Task: task, RequestId: id}))
		if err != nil {
			return fmt.Errorf("submit (retry with --request-id %s): %w", id, err)
		}
		j := r.Msg.Job
		if j == nil {
			return errors.New("server returned no job")
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
	if run {
		c.Flags().StringVar(&id, "request-id", "", "Stable request ID to reuse when retrying submission")
		c.Flags().BoolVar(&wait, "wait", false, "Wait for terminal status; unsuccessful jobs exit 2")
	}
	return c
}

func (o *options) wait(ctx context.Context, c skyv1connect.ControlServiceClient, j *skyv1.Job) (*skyv1.Job, error) {
	for !terminal(j.State) {
		t := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		rpcctx, cancel := context.WithTimeout(ctx, o.timeout)
		r, err := c.GetJob(rpcctx, request(o, &skyv1.GetJobRequest{Id: j.Id}))
		cancel()
		if err != nil {
			return nil, err
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
			return err
		}
		return o.job(cmd.OutOrStdout(), r.Msg.Job)
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
	logs := &cobra.Command{Use: "logs ID", Short: "Read retained logs; interrupting does not cancel the job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if offset < 0 {
			return errors.New("offset must not be negative")
		}
		ctx := cmd.Context()
		if !follow {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, o.timeout)
			defer cancel()
		}
		s, err := o.client().StreamJobLogs(ctx, request(o, &skyv1.StreamJobLogsRequest{Id: args[0], Offset: offset, Follow: follow}))
		if err != nil {
			return err
		}
		defer s.Close()
		warned := false
		for s.Receive() {
			m := s.Msg()
			if m.Truncated && !warned {
				fmt.Fprintln(cmd.ErrOrStderr(), "logs truncated: retained output limit reached")
				warned = true
			}
			if o.json {
				err = writeJSON(cmd.OutOrStdout(), m)
			} else {
				_, err = cmd.OutOrStdout().Write(m.Data)
			}
			if err != nil {
				return err
			}
		}
		return s.Err()
	}}
	logs.Flags().BoolVarP(&follow, "follow", "f", false, "Follow until terminal status or interruption")
	logs.Flags().Int64Var(&offset, "offset", 0, "Byte offset into retained logs")
	c.AddCommand(list, get, cancelCmd, logs)
	return c
}

// taskFile deliberately exposes only the portable wire fields.
type taskFile struct {
	Argv             []string          `yaml:"argv"`
	Env              map[string]string `yaml:"env"`
	Directory        string            `yaml:"directory"`
	Image            string            `yaml:"image"`
	CPUMillis        int64             `yaml:"cpu_millis"`
	MemoryBytes      int64             `yaml:"memory_bytes"`
	Accelerator      string            `yaml:"accelerator"`
	AcceleratorCount int32             `yaml:"accelerator_count"`
	Target           string            `yaml:"target"`
	TimeoutSeconds   int64             `yaml:"timeout_seconds"`
}

func readTask(cmd *cobra.Command, path string) (*skyv1.Task, error) {
	if path == "" {
		return nil, errors.New("task requires --file")
	}
	r := cmd.InOrStdin()
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	b, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return nil, errors.New("task exceeds 1 MiB")
	}
	d := yaml.NewDecoder(strings.NewReader(string(b)))
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
	return &skyv1.Task{Argv: f.Argv, Env: f.Env, Directory: f.Directory, Image: f.Image, CpuMillis: f.CPUMillis, MemoryBytes: f.MemoryBytes, Accelerator: f.Accelerator, AcceleratorCount: f.AcceleratorCount, Target: f.Target, TimeoutSeconds: f.TimeoutSeconds}, nil
}
