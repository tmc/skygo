package cli

import (
	"bytes"
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
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentwire"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// maxExecStdin bounds the stdin of one command, as the server does.
const maxExecStdin = 64 << 10

// envSpecFile is the environment spec file. Every key is the wire field name.
type envSpecFile struct {
	Name               string            `yaml:"name"`
	Target             string            `yaml:"target"`
	Image              string            `yaml:"image"`
	CPUMillis          int64             `yaml:"cpu_millis"`
	MemoryBytes        int64             `yaml:"memory_bytes"`
	CPULimitMillis     int64             `yaml:"cpu_limit_millis"`
	MemoryLimitBytes   int64             `yaml:"memory_limit_bytes"`
	Accelerator        string            `yaml:"accelerator"`
	AcceleratorCount   int32             `yaml:"accelerator_count"`
	NodeSelector       map[string]string `yaml:"node_selector"`
	StorageAlias       string            `yaml:"storage_alias"`
	Port               int32             `yaml:"port"`
	IdleTimeoutSeconds int64             `yaml:"idle_timeout_seconds"`
	Artifacts          []envArtifactFile `yaml:"artifacts"`
}

type envArtifactFile struct {
	ID        string `yaml:"id"`
	SHA256    string `yaml:"sha256"`
	Size      uint64 `yaml:"size"`
	MountPath string `yaml:"mount_path"`
}

// envRequest is what a retry of CreateEnvironment resends unchanged: the
// request ID and its original request domain.
type envRequest struct {
	id     string
	domain *skyv1.RequestDomain
}

// retry returns the flags that resend r.
func (r envRequest) retry() string {
	return fmt.Sprintf("--request-domain %s:%d --request-id %s", r.domain.Lineage, r.domain.Epoch, r.id)
}

// suppliedEnvRequest returns the retry of an earlier create. Its domain is
// the original one, used as given: the current domain is never substituted.
func suppliedEnvRequest(id, domain string) (envRequest, error) {
	if !validRequestID(id) {
		return envRequest{}, errors.New("request ID requires 1..128 letters, digits, underscores or hyphens")
	}
	d, err := parseRequestDomain(domain)
	if err != nil {
		return envRequest{}, err
	}
	return envRequest{id: id, domain: d}, nil
}

// newEnvRequest returns a new request: the current domain, sampled once,
// and a fresh ID.
func (o *options) newEnvRequest(ctx context.Context) (envRequest, error) {
	r, err := o.client().GetRetention(ctx, request(o, &skyv1.GetRetentionRequest{}))
	if err != nil {
		return envRequest{}, err
	}
	if r == nil || r.Msg == nil {
		return envRequest{}, errors.New("server returned no retention information")
	}
	if !r.Msg.Enabled {
		return envRequest{}, errors.New("server retains no request domain; environment create requires one")
	}
	text, err := domainText(r.Msg.Domain)
	if err != nil {
		return envRequest{}, err
	}
	if r.Msg.AcceptUntil == nil || r.Msg.AcceptUntil.CheckValid() != nil || !time.Now().Before(r.Msg.AcceptUntil.AsTime()) {
		return envRequest{}, errors.New("server returned no accepting request domain")
	}
	d, _ := parseRequestDomain(text)
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return envRequest{}, err
	}
	return envRequest{id: hex.EncodeToString(data[:]), domain: d}, nil
}

// createError reports a failed create. A retired domain refuses even an
// exact replay, so the original request's outcome stays unknown and it is
// not to be resubmitted.
func createError(req envRequest, err error) error {
	var rpc *connect.Error
	if errors.As(err, &rpc) {
		for _, detail := range rpc.Details() {
			v, decodeErr := detail.Value()
			if decodeErr != nil {
				continue
			}
			if e, ok := v.(*skyv1.RetentionExpiry); ok && (e.Reason == skyv1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED || e.Reason == skyv1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED) {
				return fmt.Errorf("create environment outcome unknown; request %s retired, do not resubmit: %w", req.id, retentionDiagnostic(err))
			}
		}
	}
	return fmt.Errorf("create environment (retry with %s): %w", req.retry(), retentionDiagnostic(err))
}

func (o *options) environmentClient() skyv1connect.EnvironmentServiceClient {
	return skyv1connect.NewEnvironmentServiceClient(o.httpClient(), o.endpoint)
}

func (o *options) environmentCommand() *cobra.Command {
	c := &cobra.Command{Use: "env", Short: "Manage persistent environments"}
	var file string
	plan := &cobra.Command{Use: "plan -f spec.yaml", Short: "Check an environment spec without creating anything", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		spec, err := openEnvSpec(cmd, file)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireEnvironments(ctx, spec); err != nil {
			return err
		}
		r, err := o.environmentClient().PlanEnvironment(ctx, request(o, &skyv1.PlanEnvironmentRequest{Spec: environmentwire.SpecToProto(spec)}))
		if err != nil {
			return err
		}
		if err := environmentwire.CheckPlan(r.Msg, spec); err != nil {
			return err
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "spec-sha256: %s\nsupport: %s\nartifacts: %s\n", r.Msg.SpecSha256, envEnum(r.Msg.Support.String(), "ENVIRONMENT_PLAN_SUPPORT_"), planFact(r.Msg.Artifacts))
		return err
	}}
	plan.Flags().StringVarP(&file, "file", "f", "", "Spec YAML path, or - for stdin")
	var id, domain string
	create := &cobra.Command{Use: "create -f spec.yaml", Short: "Create an environment, or replay an earlier request", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		retry := cmd.Flags().Changed("request-id")
		if retry != cmd.Flags().Changed("request-domain") {
			return errors.New("--request-id and --request-domain are given together, both from the original request")
		}
		var req envRequest
		if retry {
			var err error
			if req, err = suppliedEnvRequest(id, domain); err != nil {
				return err
			}
		}
		spec, err := openEnvSpec(cmd, file)
		if err != nil {
			return err
		}
		sum, err := environmentwire.SpecSHA256(spec)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		// A retry of an original pair needs only the endpoint: the server's
		// record of that request decides its replay, even if the target no
		// longer advertises what the original create needed. A new create
		// needs the target's current capabilities.
		admit := spec
		if retry {
			admit = environment.Spec{}
		}
		if err := o.requireEnvironments(ctx, admit); err != nil {
			return err
		}
		if !retry {
			if req, err = o.newEnvRequest(ctx); err != nil {
				return err
			}
		}
		// Both are written before the request, so a lost reply can be
		// replayed with them. A retry with a new ID or domain could create
		// a second environment, so none is made here.
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-domain: %s:%d\nrequest-id: %s\n", req.domain.Lineage, req.domain.Epoch, req.id); err != nil {
			return fmt.Errorf("write request hint: %w", err)
		}
		r, err := o.environmentClient().CreateEnvironment(ctx, request(o, &skyv1.CreateEnvironmentRequest{RequestId: req.id, Spec: environmentwire.SpecToProto(spec), Domain: req.domain}))
		if err != nil {
			return createError(req, err)
		}
		if err := checkResponse(r.Msg); err != nil {
			return fmt.Errorf("create environment (retry with %s): %w", req.retry(), err)
		}
		e := r.Msg.GetEnvironment()
		if err := environmentwire.CheckEnvironment(e); err != nil {
			return err
		}
		if e.SpecSha256 != sum {
			return errors.New("server returned an environment with a different spec")
		}
		return o.writeEnvironments(cmd.OutOrStdout(), e)
	}}
	create.Flags().StringVarP(&file, "file", "f", "", "Spec YAML path, or - for stdin")
	create.Flags().StringVar(&id, "request-id", "", "Request ID of the original create, to retry it")
	create.Flags().StringVar(&domain, "request-domain", "", "Request domain LINEAGE:EPOCH of the original create, to retry it")
	get := &cobra.Command{Use: "get ID", Short: "Show an environment", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireEnvironments(ctx, environment.Spec{}); err != nil {
			return err
		}
		r, err := o.environmentClient().GetEnvironment(ctx, request(o, &skyv1.GetEnvironmentRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		if err := checkResponse(r.Msg); err != nil {
			return err
		}
		if err := environmentwire.CheckEnvironment(r.Msg.GetEnvironment()); err != nil {
			return err
		}
		return o.writeEnvironments(cmd.OutOrStdout(), r.Msg.Environment)
	}}
	var size int32
	var page string
	list := &cobra.Command{Use: "list", Short: "List one page of environments", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireEnvironments(ctx, environment.Spec{}); err != nil {
			return err
		}
		r, err := o.environmentClient().ListEnvironments(ctx, request(o, &skyv1.ListEnvironmentsRequest{PageSize: size, PageToken: page}))
		if err != nil {
			return err
		}
		if err := checkResponse(r.Msg); err != nil {
			return err
		}
		for _, e := range r.Msg.Environments {
			if err := environmentwire.CheckEnvironment(e); err != nil {
				return err
			}
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		if err := o.writeEnvironments(cmd.OutOrStdout(), r.Msg.Environments...); err != nil {
			return err
		}
		if r.Msg.NextPageToken != "" {
			fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", r.Msg.NextPageToken)
		}
		return nil
	}}
	list.Flags().Int32Var(&size, "page-size", 50, "Maximum environments per page")
	list.Flags().StringVar(&page, "page-token", "", "Continuation token from the previous page")
	var yes bool
	del := &cobra.Command{Use: "delete ID --yes", Short: "Latch an environment absent; phase deleted follows observed cleanup", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !yes {
			return errors.New("deletion requires --yes")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireEnvironments(ctx, environment.Spec{}); err != nil {
			return err
		}
		r, err := o.environmentClient().DeleteEnvironment(ctx, request(o, &skyv1.DeleteEnvironmentRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		if err := checkResponse(r.Msg); err != nil {
			return err
		}
		if err := environmentwire.CheckEnvironment(r.Msg.GetEnvironment()); err != nil {
			return err
		}
		return o.writeEnvironments(cmd.OutOrStdout(), r.Msg.Environment)
	}}
	del.Flags().BoolVarP(&yes, "yes", "y", false, "Confirm deletion")
	var stdin bool
	exec := &cobra.Command{Use: "exec ID -- ARGV...", Short: "Run one command in a ready environment; it is never resent", Args: cobra.MinimumNArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.ArgsLenAtDash() != 1 {
			return errors.New("exec requires ID -- ARGV")
		}
		req := &skyv1.ExecEnvironmentCommandRequest{Id: args[0], Argv: args[1:]}
		if stdin {
			b, err := io.ReadAll(io.LimitReader(cmd.InOrStdin(), maxExecStdin+1))
			if err != nil {
				return fmt.Errorf("read stdin: %w", err)
			}
			if len(b) > maxExecStdin {
				return errors.New("stdin exceeds 64 KiB")
			}
			req.Stdin = b
			if req.Stdin == nil {
				req.Stdin = []byte{}
			}
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireEnvironments(ctx, environment.Spec{}); err != nil {
			return err
		}
		r, err := o.environmentClient().ExecEnvironmentCommand(ctx, request(o, req))
		if err != nil {
			// No status proves the command was never dispatched.
			return fmt.Errorf("exec outcome unknown; it was not resent: %w", err)
		}
		if err := checkResponse(r.Msg); err != nil {
			return fmt.Errorf("exec outcome unknown: %w", err)
		}
		// Writing the result, in either mode, is not the command's
		// outcome; the exit status below is.
		if o.json {
			if err := writeJSON(cmd.OutOrStdout(), r.Msg); err != nil {
				return err
			}
		} else {
			if _, err := cmd.OutOrStdout().Write(r.Msg.Stdout); err != nil {
				return err
			}
			if _, err := cmd.ErrOrStderr().Write(r.Msg.Stderr); err != nil {
				return err
			}
			if r.Msg.Truncated {
				fmt.Fprintln(cmd.ErrOrStderr(), "output truncated")
			}
		}
		switch {
		case r.Msg.ExitCode == nil:
			return errors.New("command outcome unknown; it was not resent")
		case *r.Msg.ExitCode != 0:
			return fmt.Errorf("command exited with status %d", *r.Msg.ExitCode)
		}
		return nil
	}}
	exec.Flags().BoolVar(&stdin, "stdin", false, "Send standard input, at most 64 KiB, to the command")
	c.AddCommand(plan, create, get, list, del, exec, o.environmentForwardCommand())
	return c
}

// requireEnvironments requires the endpoint to advertise environments and,
// for a spec, its target to advertise persistent environments, and artifact
// mounts too if the spec has any. It is a sample, not a reservation.
func (o *options) requireEnvironments(ctx context.Context, spec environment.Spec) error {
	info, err := o.requireCapabilities(ctx, "")
	if err != nil {
		return err
	}
	if !slices.Contains(info.Capabilities, skyv1.Capability_CAPABILITY_ENVIRONMENTS_V1) {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("endpoint does not advertise environments-v1"))
	}
	if spec.Target == "" {
		return nil
	}
	i := slices.IndexFunc(info.Targets, func(t *skyv1.Target) bool { return t.GetName() == spec.Target })
	if i < 0 {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("target %q is not advertised by this server", spec.Target))
	}
	t := info.Targets[i]
	if !slices.Contains(t.Capabilities, skyv1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("target %q does not advertise persistent-environments-v1", spec.Target))
	}
	if len(spec.Artifacts) != 0 && !slices.Contains(t.Capabilities, skyv1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("target %q does not advertise environment-artifacts-v1", spec.Target))
	}
	return nil
}

// checkResponse refuses a missing response, or one with unknown fields of
// its own, before its contents are trusted. The caller checks the contents.
func checkResponse(m proto.Message) error {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return errors.New("server returned no response")
	}
	if len(r.GetUnknown()) != 0 {
		return fmt.Errorf("server returned a %s with unknown fields", r.Descriptor().Name())
	}
	return nil
}

// writeEnvironments writes environments that CheckEnvironment accepted.
func (o *options) writeEnvironments(out io.Writer, envs ...*skyv1.Environment) error {
	if o.json && len(envs) == 1 {
		return writeJSON(out, envs[0])
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tTARGET\tDESIRED\tPHASE\tOPEN\tUNKNOWN")
	for _, e := range envs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n", e.Id, e.Spec.Name, e.Spec.Target, envEnum(e.Desired.String(), "ENVIRONMENT_DESIRED_"), envEnum(e.Phase.String(), "ENVIRONMENT_PHASE_"), e.OpenSessions, e.UnknownSessions)
	}
	return w.Flush()
}

func envEnum(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

// planFact names f. UNSPECIFIED means the precondition does not apply; it
// is never read as verified.
func planFact(f skyv1.EnvironmentPlanFact) string {
	if f == skyv1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNSPECIFIED {
		return "not-applicable"
	}
	return envEnum(f.String(), "ENVIRONMENT_PLAN_FACT_")
}

func openEnvSpec(cmd *cobra.Command, path string) (environment.Spec, error) {
	if path == "" {
		return environment.Spec{}, errors.New("environment spec requires --file")
	}
	if path == "-" {
		return readEnvSpec(cmd.InOrStdin())
	}
	f, err := os.Open(path)
	if err != nil {
		return environment.Spec{}, err
	}
	defer f.Close()
	return readEnvSpec(f)
}

// readEnvSpec decodes one strict spec document and normalizes it. Unknown
// and duplicate keys, nulls, anchors, aliases and merge keys are errors.
// environment.Normalize is the only check of the spec's values.
func readEnvSpec(r io.Reader) (environment.Spec, error) {
	b, err := io.ReadAll(io.LimitReader(r, environment.MaxSpecBytes+1))
	if err != nil {
		return environment.Spec{}, err
	}
	if len(b) > environment.MaxSpecBytes {
		return environment.Spec{}, errors.New("environment spec exceeds 256 KiB")
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return environment.Spec{}, fmt.Errorf("decode environment spec: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return environment.Spec{}, errors.New("environment spec must be a mapping")
	}
	if err := checkEnvSpecPlain(&doc); err != nil {
		return environment.Spec{}, err
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	var f envSpecFile
	if err := d.Decode(&f); err != nil {
		return environment.Spec{}, fmt.Errorf("decode environment spec: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return environment.Spec{}, errors.New("environment spec must contain exactly one YAML document")
	}
	s := environment.Spec{
		Name:               f.Name,
		Target:             f.Target,
		Image:              f.Image,
		CPUMillis:          f.CPUMillis,
		MemoryBytes:        f.MemoryBytes,
		CPULimitMillis:     f.CPULimitMillis,
		MemoryLimitBytes:   f.MemoryLimitBytes,
		Accelerator:        f.Accelerator,
		AcceleratorCount:   f.AcceleratorCount,
		NodeSelector:       f.NodeSelector,
		StorageAlias:       f.StorageAlias,
		Port:               f.Port,
		IdleTimeoutSeconds: f.IdleTimeoutSeconds,
	}
	for _, a := range f.Artifacts {
		s.Artifacts = append(s.Artifacts, environment.ArtifactMount{ID: a.ID, SHA256: a.SHA256, Size: a.Size, MountPath: a.MountPath})
	}
	s, err = environment.Normalize(s)
	if err != nil {
		return environment.Spec{}, fmt.Errorf("environment spec: %w", err)
	}
	return s, nil
}

// checkEnvSpecPlain rejects anchors, aliases, merge keys and nulls anywhere
// in the document, so no value or key is hidden from the decoder's checks
// and no null reads as a zero value.
func checkEnvSpecPlain(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("line %d: environment spec must not use anchors or aliases", n.Line)
	}
	if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!null" {
		return fmt.Errorf("line %d: environment spec must not contain null", n.Line)
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i < len(n.Content); i += 2 {
			if k := n.Content[i]; k.Kind == yaml.ScalarNode && k.ShortTag() == "!!merge" {
				return fmt.Errorf("line %d: environment spec must not use merge keys", k.Line)
			}
		}
	}
	for _, c := range n.Content {
		if err := checkEnvSpecPlain(c); err != nil {
			return err
		}
	}
	return nil
}
