package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	"gopkg.in/yaml.v3"
)

// graphFile is the typed workflow.Graph shape. Each node's task uses the
// task file fields of "skygo run", so a graph has no second task dialect.
type graphFile struct {
	Nodes []graphNode `yaml:"nodes"`
}

type graphNode struct {
	Name      string      `yaml:"name"`
	Task      *taskFile   `yaml:"task"`
	DependsOn []string    `yaml:"depends_on"`
	Input     *graphInput `yaml:"input"`
}

type graphInput struct {
	Node string `yaml:"node"`
	Slot string `yaml:"slot"`
}

func (o *options) workflowClient() skyv1connect.WorkflowServiceClient {
	return skyv1connect.NewWorkflowServiceClient(o.httpClient(), o.endpoint)
}

func (o *options) workflowCommand() *cobra.Command {
	c := &cobra.Command{Use: "workflow", Short: "Submit and inspect static workflows"}
	var file, id, domain string
	var remoteData bool
	run := &cobra.Command{Use: "run -f graph.yaml", Short: "Submit a static workflow without waiting", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		g, err := openGraph(cmd, file)
		if err != nil {
			return err
		}
		// As with "skygo run", data mounts on any node, input templates
		// included, need explicit consent before any request: Control places
		// such nodes on qualified remote data routes.
		if !remoteData && slices.ContainsFunc(g.Nodes, func(n *skyv1.WorkflowNode) bool { return len(n.GetTask().GetDataMounts()) != 0 }) {
			return errors.New("data mounts require --remote-data")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		w, err := o.submitWorkflow(cmd, ctx, g, id, domain, file, cmd.Flags().Changed("request-domain"), cmd.Flags().Changed("request-id"))
		if err != nil {
			return err
		}
		return o.writeWorkflow(cmd.OutOrStdout(), w)
	}}
	run.Flags().StringVarP(&file, "file", "f", "", "Graph YAML path, or - for stdin")
	run.Flags().BoolVar(&remoteData, "remote-data", false, "Allow nodes with data mounts, which use qualified remote data routes")
	run.Flags().StringVar(&id, "request-id", "", "Stable request ID to reuse when retrying submission")
	run.Flags().StringVar(&domain, "request-domain", "", "Explicit retained request domain LINEAGE:EPOCH; reuse with the request ID")
	get := &cobra.Command{Use: "get ID", Short: "Show a workflow and its nodes", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.workflowClient().GetWorkflow(ctx, request(o, &skyv1.GetWorkflowRequest{Id: args[0]}))
		if err != nil {
			return retentionDiagnostic(err)
		}
		return o.writeWorkflow(cmd.OutOrStdout(), r.Msg.GetWorkflow())
	}}
	var size int32
	var page string
	list := &cobra.Command{Use: "list", Short: "List one page of workflows", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.workflowClient().ListWorkflows(ctx, request(o, &skyv1.ListWorkflowsRequest{PageSize: size, PageToken: page}))
		if err != nil {
			return err
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tCREATED\tCANCEL-REQUESTED")
		for _, s := range r.Msg.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%t\n", s.GetId(), s.GetCreatedAt().AsTime().Format(time.RFC3339), s.GetCancelRequested())
		}
		if r.Msg.NextPageToken != "" {
			fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", r.Msg.NextPageToken)
		}
		return w.Flush()
	}}
	list.Flags().Int32Var(&size, "page-size", 50, "Maximum workflows per page")
	list.Flags().StringVar(&page, "page-token", "", "Continuation token from the previous page")
	var yes bool
	cancelCmd := &cobra.Command{Use: "cancel ID --yes", Short: "Request workflow cancellation; bound jobs are not awaited", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !yes {
			return errors.New("cancellation requires --yes")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		r, err := o.workflowClient().CancelWorkflow(ctx, request(o, &skyv1.CancelWorkflowRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		return o.writeWorkflow(cmd.OutOrStdout(), r.Msg.GetWorkflow())
	}}
	cancelCmd.Flags().BoolVarP(&yes, "yes", "y", false, "Confirm cancellation")
	c.AddCommand(run, get, list, cancelCmd)
	return c
}

// writeWorkflow writes w. It reports node dispositions and bound job states
// only, never a derived workflow outcome.
func (o *options) writeWorkflow(out io.Writer, w *skyv1.Workflow) error {
	if w == nil {
		return errors.New("server returned no workflow")
	}
	if o.json {
		return writeJSON(out, w)
	}
	if _, err := fmt.Fprintf(out, "%s\tcancel-requested=%t\n", w.Id, w.CancelRequested); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tDISPOSITION\tJOB\tSTATE")
	for _, n := range w.Nodes {
		job, st := "-", "-"
		if n.GetJobId() != "" {
			job, st = n.GetJobId(), state(n.GetChildState())
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.GetName(), strings.ToLower(strings.TrimPrefix(n.GetDisposition().String(), "WORKFLOW_DISPOSITION_")), job, st)
	}
	return tw.Flush()
}

// submitWorkflow requires positive discovery of the workflow service before
// any submission. A server that does not advertise it, including one that
// predates discovery, receives no workflow and no job submission.
func (o *options) submitWorkflow(cmd *cobra.Command, ctx context.Context, g *skyv1.WorkflowGraph, id, domain, file string, domainSet, suppliedID bool) (*skyv1.Workflow, error) {
	if (suppliedID && id == "") || (id != "" && !validRequestID(id)) {
		return nil, errors.New("request ID requires 1..128 letters, digits, underscores or hyphens")
	}
	var selected *skyv1.RequestDomain
	if domainSet {
		var err error
		selected, err = parseRequestDomain(domain)
		if err != nil {
			return nil, err
		}
	}
	info, err := o.requireCapabilities(ctx, "")
	if err != nil {
		return nil, err
	}
	if !slices.Contains(info.Capabilities, skyv1.Capability_CAPABILITY_STATIC_WORKFLOWS_V1) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("endpoint does not advertise static-workflows-v1"))
	}
	if !domainSet {
		r, err := o.client().GetRetention(ctx, request(o, &skyv1.GetRetentionRequest{}))
		if err != nil {
			return nil, err
		}
		if r == nil || r.Msg == nil {
			return nil, errors.New("server returned no retention information")
		}
		if r.Msg.Enabled && !suppliedID {
			if domain, err = domainText(r.Msg.Domain); err != nil {
				return nil, err
			}
			if r.Msg.AcceptUntil == nil || r.Msg.AcceptUntil.CheckValid() != nil || !time.Now().Before(r.Msg.AcceptUntil.AsTime()) {
				return nil, errors.New("server returned no accepting request domain")
			}
			selected, _ = parseRequestDomain(domain)
		}
	}
	if id == "" {
		var data [16]byte
		if _, err := rand.Read(data[:]); err != nil {
			return nil, err
		}
		id = hex.EncodeToString(data[:])
	}
	retry := "retry with --request-id " + id
	if selected != nil {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-domain: %s\nrequest-id: %s\ngraph-file: %q\n", domain, id, file); err != nil {
			return nil, fmt.Errorf("write request hint: %w", err)
		}
		retry = "--request-domain " + domain + " --request-id " + id
	} else if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-id: %s\n", id); err != nil {
		return nil, fmt.Errorf("write request id: %w", err)
	}
	r, err := o.workflowClient().SubmitWorkflow(ctx, request(o, &skyv1.SubmitWorkflowRequest{Graph: g, RequestId: id, Domain: selected}))
	if err != nil {
		return nil, fmt.Errorf("submit workflow (%s): %w", retry, retentionDiagnostic(err))
	}
	if r == nil || r.Msg == nil || r.Msg.Workflow == nil {
		return nil, errors.New("server returned no workflow")
	}
	return r.Msg.Workflow, nil
}

func openGraph(cmd *cobra.Command, path string) (*skyv1.WorkflowGraph, error) {
	if path == "" {
		return nil, errors.New("graph requires --file")
	}
	if path == "-" {
		return readGraph(cmd.InOrStdin())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readGraph(f)
}

// readGraph decodes one strict graph document and applies the graph's own
// validation before any request. Unknown and duplicate keys are errors, as in
// a task file, and so is a null node, task or input. Anchors, aliases and
// merge keys are refused, so no value or key is hidden from these checks.
func readGraph(r io.Reader) (*skyv1.WorkflowGraph, error) {
	b, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return nil, errors.New("graph exceeds 1 MiB")
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("decode graph: %w", err)
	}
	if err := checkGraphPlain(&doc); err != nil {
		return nil, err
	}
	if err := checkGraphNulls(&doc); err != nil {
		return nil, err
	}
	d := yaml.NewDecoder(strings.NewReader(string(b)))
	d.KnownFields(true)
	var f graphFile
	if err := d.Decode(&f); err != nil {
		return nil, fmt.Errorf("decode graph: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("graph must contain exactly one YAML document")
	}
	g := workflow.Graph{Nodes: make([]workflow.Node, len(f.Nodes))}
	for i, n := range f.Nodes {
		if n.Task == nil {
			return nil, fmt.Errorf("node %d requires a task", i)
		}
		t := backend.Task(*n.Task)
		if len(t.Argv) == 0 {
			return nil, fmt.Errorf("node %q: task argv must not be empty", n.Name)
		}
		g.Nodes[i] = workflow.Node{Name: n.Name, Task: t, DependsOn: n.DependsOn}
		if n.Input != nil {
			g.Nodes[i].Input = &workflow.Input{Node: n.Input.Node, Slot: n.Input.Slot}
		}
		if len(t.DataMounts) != 0 {
			check := func(t backend.Task) error { return backend.ValidateDataTask(t, nil) }
			if n.Input != nil {
				check = backend.ValidateDataTemplate
			}
			if err := check(t); err != nil {
				return nil, fmt.Errorf("node %q: data mounts: %w", n.Name, err)
			}
		}
	}
	if err := workflow.Validate(g); err != nil {
		return nil, fmt.Errorf("graph: %w", err)
	}
	out := &skyv1.WorkflowGraph{Nodes: make([]*skyv1.WorkflowNode, len(f.Nodes))}
	for i, n := range f.Nodes {
		out.Nodes[i] = &skyv1.WorkflowNode{Name: n.Name, Task: wireTaskFile(n.Task), DependsOn: n.DependsOn}
		if n.Input != nil {
			out.Nodes[i].Input = &skyv1.WorkflowInput{Node: n.Input.Node, Slot: n.Input.Slot}
		}
	}
	return out, nil
}

// checkGraphPlain rejects anchors, aliases and merge keys anywhere in the
// document. An alias can stand for a null, and a merge key can supply a
// duplicate or null field, past the checks that read the document as written.
func checkGraphPlain(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("line %d: graph must not use anchors or aliases", n.Line)
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i < len(n.Content); i += 2 {
			if k := n.Content[i]; k.Kind == yaml.ScalarNode && k.ShortTag() == "!!merge" {
				return fmt.Errorf("line %d: graph must not use merge keys", k.Line)
			}
		}
	}
	for _, c := range n.Content {
		if err := checkGraphPlain(c); err != nil {
			return err
		}
	}
	return nil
}

// checkGraphNulls rejects a null in the graph's own shape: the node list, a
// node, a node field or an input field. Task fields keep the task file rules.
func checkGraphNulls(doc *yaml.Node) error {
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return errors.New("graph must be a mapping")
	}
	nodes := mappingValue(doc.Content[0], "nodes")
	if nodes == nil || nodes.Kind != yaml.SequenceNode {
		return errors.New("graph requires a nodes list")
	}
	for i, n := range nodes.Content {
		if n.Kind != yaml.MappingNode {
			return fmt.Errorf("node %d must be a mapping", i)
		}
		if err := checkNullValues(n, fmt.Sprintf("node %d", i)); err != nil {
			return err
		}
		if in := mappingValue(n, "input"); in != nil {
			if err := checkNullValues(in, fmt.Sprintf("node %d input", i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkNullValues(m *yaml.Node, where string) error {
	for i := 1; i < len(m.Content); i += 2 {
		if v := m.Content[i]; v.Kind == yaml.ScalarNode && v.ShortTag() == "!!null" {
			return fmt.Errorf("%s: %s must not be null", where, m.Content[i-1].Value)
		}
	}
	return nil
}

func mappingValue(m *yaml.Node, key string) *yaml.Node {
	if m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// wireTaskFile converts a task file exactly as readTask does.
func wireTaskFile(f *taskFile) *skyv1.Task {
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
	return &skyv1.Task{DataMounts: mounts, Workdir: workdir, Setup: f.Setup, Outputs: f.Outputs, Retry: retry, Argv: f.Argv, Env: f.Env, Directory: f.Directory, Image: f.Image, CpuMillis: f.CPUMillis, MemoryBytes: f.MemoryBytes, Accelerator: f.Accelerator, AcceleratorCount: f.AcceleratorCount, Target: f.Target, TimeoutSeconds: f.TimeoutSeconds, CpuLimitMillis: f.CPULimitMillis, MemoryLimitBytes: f.MemoryLimitBytes, TargetLabels: f.TargetLabels, NodeSelector: f.NodeSelector}
}
