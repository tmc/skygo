package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingwire"
	"gopkg.in/yaml.v3"
)

// maxServiceSpecBytes bounds a service spec file, as the server bounds the
// encoded spec.
const maxServiceSpecBytes = 64 << 10

// maxPageToken bounds a list continuation token, as the server does.
const maxPageToken = 512

// serviceSpecFile is the service spec file. Every key is the wire field
// name. Mounts, workdir, setup and CPU scaling are declared only so that
// they are refused by name rather than as unknown keys.
type serviceSpecFile struct {
	Image        string            `yaml:"image"`
	Argv         []string          `yaml:"argv"`
	Env          []serviceEnvFile  `yaml:"env"`
	Port         int32             `yaml:"port"`
	Readiness    serviceReadyFile  `yaml:"readiness"`
	Resources    serviceResFile    `yaml:"resources"`
	NodeSelector map[string]string `yaml:"node_selector"`
	Rollout      serviceRollFile   `yaml:"rollout"`
	Scaling      serviceScaleFile  `yaml:"scaling"`
	Mounts       []yaml.Node       `yaml:"mounts"`
	Workdir      yaml.Node         `yaml:"workdir"`
	Setup        []string          `yaml:"setup"`
}

type serviceEnvFile struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type serviceReadyFile struct {
	Path                string `yaml:"path"`
	PeriodSeconds       int32  `yaml:"period_seconds"`
	TimeoutSeconds      int32  `yaml:"timeout_seconds"`
	FailureThreshold    int32  `yaml:"failure_threshold"`
	InitialDelaySeconds int32  `yaml:"initial_delay_seconds"`
}

type serviceResFile struct {
	CPUMillis        int64 `yaml:"cpu_millis"`
	MemoryBytes      int64 `yaml:"memory_bytes"`
	CPULimitMillis   int64 `yaml:"cpu_limit_millis"`
	MemoryLimitBytes int64 `yaml:"memory_limit_bytes"`
}

type serviceRollFile struct {
	ProgressDeadlineSeconds int32 `yaml:"progress_deadline_seconds"`
	TerminationGraceSeconds int32 `yaml:"termination_grace_seconds"`
}

type serviceScaleFile struct {
	Replicas int32     `yaml:"replicas"`
	CPU      yaml.Node `yaml:"cpu"`
}

func (o *options) servingClient() skyv1connect.ServingServiceClient {
	return skyv1connect.NewServingServiceClient(o.httpClient(), o.endpoint)
}

func (o *options) serveCommand() *cobra.Command {
	c := &cobra.Command{Use: "serve", Short: "Run services with one to eight fixed replicas"}
	var name, target, id, domain string
	up := &cobra.Command{Use: "up FILE --name NAME --target TARGET", Short: "Create a service, or replay an earlier request", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		retry := cmd.Flags().Changed("request-id")
		if retry != cmd.Flags().Changed("request-domain") {
			return errors.New("--request-id and --request-domain are given together, both from the original request")
		}
		if name == "" || target == "" {
			return errors.New("serve up requires --name and --target")
		}
		var req envRequest
		if retry {
			var err error
			if req, err = suppliedEnvRequest(id, domain); err != nil {
				return err
			}
		}
		spec, err := openServiceSpec(cmd, args[0])
		if err != nil {
			return err
		}
		sum, err := spec.Digest()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		// As for environments, a retry needs only the endpoint: the server's
		// record of the original request decides its replay.
		admit := target
		if retry {
			admit = ""
		}
		if err := o.requireServices(ctx, admit); err != nil {
			return err
		}
		if !retry {
			if req, err = o.newEnvRequest(ctx, "service create"); err != nil {
				return err
			}
		}
		// Written before the request, so a lost reply can be replayed.
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-domain: %s:%d\nrequest-id: %s\n", req.domain.Lineage, req.domain.Epoch, req.id); err != nil {
			return fmt.Errorf("write request hint: %w", err)
		}
		r, err := o.servingClient().CreateService(ctx, request(o, &skyv1.CreateServiceRequest{RequestId: req.id, Name: name, Target: target, Spec: servingwire.SpecToProto(spec), Domain: req.domain}))
		if err != nil {
			return createError("service", req, serviceDiagnostic(err))
		}
		if err := checkResponse(r.Msg); err != nil {
			return fmt.Errorf("create service (retry with %s): %w", req.retry(), err)
		}
		s := r.Msg.GetService()
		if err := servingwire.CheckService(s); err != nil {
			return err
		}
		if s.Name != name || s.Target != target || !s.Retired && !hasRevision(s, sum) {
			return errors.New("server returned a service with a different name, target or spec")
		}
		return o.writeServices(cmd.OutOrStdout(), s)
	}}
	up.Flags().StringVar(&name, "name", "", "Service name, a DNS label")
	up.Flags().StringVar(&target, "target", "", "Configured target name")
	up.Flags().StringVar(&id, "request-id", "", "Request ID of the original create, to retry it")
	up.Flags().StringVar(&domain, "request-domain", "", "Request domain LINEAGE:EPOCH of the original create, to retry it")

	var size int32
	var page string
	status := &cobra.Command{Use: "status [ID]", Short: "Show a service, or one page of services", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireServices(ctx, ""); err != nil {
			return err
		}
		if len(args) == 1 {
			if !servingwire.ValidID(args[0]) {
				return errors.New("service ID requires 32 lowercase hexadecimal digits")
			}
			r, err := o.servingClient().GetService(ctx, request(o, &skyv1.GetServiceRequest{Id: args[0]}))
			if err != nil {
				return err
			}
			if err := checkResponse(r.Msg); err != nil {
				return err
			}
			if err := servingwire.CheckService(r.Msg.GetService()); err != nil {
				return err
			}
			return o.writeServices(cmd.OutOrStdout(), r.Msg.Service)
		}
		r, err := o.servingClient().ListServices(ctx, request(o, &skyv1.ListServicesRequest{PageSize: size, PageToken: page}))
		if err != nil {
			return err
		}
		if err := checkResponse(r.Msg); err != nil {
			return err
		}
		// Every listed service is checked before anything is written.
		for _, s := range r.Msg.Services {
			if err := servingwire.CheckSummary(s); err != nil {
				return err
			}
		}
		if len(r.Msg.NextPageToken) > maxPageToken {
			return errors.New("server returned an oversized page token")
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTARGET\tDESIRED\tPHASE\tGENERATION\tREADY")
		for _, s := range r.Msg.Services {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n", s.Id, s.Name, s.Target, envEnum(s.Desired.String(), "SERVICE_DESIRED_"), envEnum(s.Status.Phase.String(), "SERVICE_PHASE_"), s.Status.DesiredGeneration, s.Status.GetObservation().GetReadyGeneration())
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if r.Msg.NextPageToken != "" {
			fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", r.Msg.NextPageToken)
		}
		return nil
	}}
	status.Flags().Int32Var(&size, "page-size", 50, "Maximum services per page")
	status.Flags().StringVar(&page, "page-token", "", "Continuation token from the previous page")

	var downID, downDomain string
	var revision uint64
	down := &cobra.Command{Use: "down ID", Short: "Latch a service absent; phase deleted follows observed removal", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		sid := args[0]
		if !servingwire.ValidID(sid) {
			return errors.New("service ID requires 32 lowercase hexadecimal digits")
		}
		retry := cmd.Flags().Changed("request-id")
		if retry != cmd.Flags().Changed("request-domain") || retry != cmd.Flags().Changed("record-revision") {
			return errors.New("--request-id, --request-domain and --record-revision are given together, all from the original request")
		}
		var req envRequest
		if retry {
			var err error
			if req, err = suppliedEnvRequest(downID, downDomain); err != nil {
				return err
			}
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if err := o.requireServices(ctx, ""); err != nil {
			return err
		}
		if !retry {
			// The revision read here is the one this deletion is decided
			// against; a change since is a stale conflict, never overwritten.
			g, err := o.servingClient().GetService(ctx, request(o, &skyv1.GetServiceRequest{Id: sid}))
			if err != nil {
				return err
			}
			if err := checkResponse(g.Msg); err != nil {
				return err
			}
			if err := servingwire.CheckService(g.Msg.GetService()); err != nil {
				return err
			}
			revision = g.Msg.Service.RecordRevision
			if req, err = o.newEnvRequest(ctx, "service delete"); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-domain: %s:%d\nrequest-id: %s\nrecord-revision: %d\n", req.domain.Lineage, req.domain.Epoch, req.id, revision); err != nil {
			return fmt.Errorf("write request hint: %w", err)
		}
		r, err := o.servingClient().DeleteService(ctx, request(o, &skyv1.DeleteServiceRequest{RequestId: req.id, Id: sid, ExpectedRecordRevision: revision, Domain: req.domain}))
		if err != nil {
			return fmt.Errorf("delete service (retry with %s --record-revision %d): %w", req.retry(), revision, serviceDiagnostic(err))
		}
		if err := checkResponse(r.Msg); err != nil {
			return err
		}
		s := r.Msg.GetService()
		if err := servingwire.CheckService(s); err != nil {
			return err
		}
		if s.Id != sid || s.Desired != skyv1.ServiceDesired_SERVICE_DESIRED_ABSENT {
			return errors.New("server returned a service that is not the deleted one")
		}
		return o.writeServices(cmd.OutOrStdout(), s)
	}}
	down.Flags().StringVar(&downID, "request-id", "", "Request ID of the original deletion, to retry it")
	down.Flags().StringVar(&downDomain, "request-domain", "", "Request domain LINEAGE:EPOCH of the original deletion, to retry it")
	down.Flags().Uint64Var(&revision, "record-revision", 0, "Expected record revision of the original deletion, to retry it")

	listen := "127.0.0.1:0"
	forward := &cobra.Command{Use: "forward ID --listen 127.0.0.1:PORT", Short: "Relay a loopback HTTP listener to a ready service", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		sid := args[0]
		if !servingwire.ValidID(sid) {
			return errors.New("service ID requires 32 lowercase hexadecimal digits")
		}
		if err := checkLoopback(listen); err != nil {
			return err
		}
		u, err := routeURL(o.endpoint, "services", sid)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		err = o.requireServices(ctx, "")
		cancel()
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "forwarding http://%s to service %s\n", ln.Addr(), sid); err != nil {
			ln.Close()
			return err
		}
		return relayForward(cmd.Context(), ln, u, o.token, cmd.ErrOrStderr(), o.httpClient().Transport, forwardDrain)
	}}
	forward.Flags().StringVar(&listen, "listen", listen, "Loopback IP address and port to listen on")

	c.AddCommand(up, status, down, forward)
	return c
}

// requireServices requires the endpoint to advertise services and, for a
// target, that target to advertise fixed services. It is a sample, not a
// reservation.
func (o *options) requireServices(ctx context.Context, target string) error {
	info, err := o.requireCapabilities(ctx, "")
	if err != nil {
		return err
	}
	if !slices.Contains(info.Capabilities, skyv1.Capability_CAPABILITY_SERVICES_V1) {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("endpoint does not advertise services-v1"))
	}
	if target == "" {
		return nil
	}
	i := slices.IndexFunc(info.Targets, func(t *skyv1.Target) bool { return t.GetName() == target })
	if i < 0 {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("target %q is not advertised by this server", target))
	}
	if !slices.Contains(info.Targets[i].Capabilities, skyv1.Capability_CAPABILITY_FIXED_SERVICES_V1) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("target %q does not advertise fixed-services-v1", target))
	}
	return nil
}

// serviceDiagnostic adds the reason of a ServiceConflict or ServiceCapacity
// detail to err.
func serviceDiagnostic(err error) error {
	var rpc *connect.Error
	if !errors.As(err, &rpc) {
		return err
	}
	for _, detail := range rpc.Details() {
		v, decodeErr := detail.Value()
		if decodeErr != nil {
			continue
		}
		switch d := v.(type) {
		case *skyv1.ServiceConflict:
			if d.Reason == skyv1.ServiceConflictReason_SERVICE_CONFLICT_REASON_STALE_RECORD_REVISION {
				return fmt.Errorf("%w (service changed; record revision now %d)", err, d.RecordRevision)
			}
			return fmt.Errorf("%w (%s)", err, envEnum(d.Reason.String(), "SERVICE_CONFLICT_REASON_"))
		case *skyv1.ServiceCapacity:
			return fmt.Errorf("%w (%s: %d used, %d requested, limit %d)", err, envEnum(d.Domain.String(), "SERVICE_QUOTA_DOMAIN_"), d.Used, d.Requested, d.Limit)
		}
	}
	return err
}

// hasRevision reports whether s retains a revision with digest sum.
func hasRevision(s *skyv1.Service, sum string) bool {
	return slices.ContainsFunc(s.GetHistory().GetRevisions(), func(r *skyv1.ServiceRevision) bool { return r.GetSha256() == sum })
}

// writeServices writes services that servingwire.CheckService accepted.
// Conditions are
// listed by code; a code this client does not know is printed as unknown.
func (o *options) writeServices(out io.Writer, services ...*skyv1.Service) error {
	if o.json && len(services) == 1 {
		return writeJSON(out, services[0])
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tTARGET\tREVISION\tDESIRED\tPHASE\tGENERATION\tREADY\tCONDITIONS")
	for _, s := range services {
		var conds []string
		for _, c := range s.Status.Conditions {
			conds = append(conds, envEnum(c.Code.String(), "SERVICE_CONDITION_CODE_"))
		}
		if len(conds) == 0 {
			conds = []string{"-"}
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%d\t%d\t%s\n", s.Id, s.Name, s.Target, s.RecordRevision,
			envEnum(s.Desired.String(), "SERVICE_DESIRED_"), envEnum(s.Status.Phase.String(), "SERVICE_PHASE_"),
			s.Status.DesiredGeneration, s.Status.GetObservation().GetReadyGeneration(), strings.Join(conds, ","))
	}
	return w.Flush()
}

func openServiceSpec(cmd *cobra.Command, path string) (serving.Spec, error) {
	if path == "-" {
		return readServiceSpec(cmd.InOrStdin())
	}
	f, err := os.Open(path)
	if err != nil {
		return serving.Spec{}, err
	}
	defer f.Close()
	return readServiceSpec(f)
}

// readServiceSpec decodes one strict spec document and checks it as the
// server does, through servingwire.SpecFromProto. Unknown and duplicate
// keys, nulls, anchors, aliases and merge keys are errors.
func readServiceSpec(r io.Reader) (serving.Spec, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxServiceSpecBytes+1))
	if err != nil {
		return serving.Spec{}, err
	}
	if len(b) > maxServiceSpecBytes {
		return serving.Spec{}, errors.New("service spec exceeds 64 KiB")
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return serving.Spec{}, fmt.Errorf("decode service spec: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return serving.Spec{}, errors.New("service spec must be a mapping")
	}
	if err := checkServiceSpecPlain(&doc); err != nil {
		return serving.Spec{}, err
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	var f serviceSpecFile
	if err := d.Decode(&f); err != nil {
		return serving.Spec{}, fmt.Errorf("decode service spec: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return serving.Spec{}, errors.New("service spec must contain exactly one YAML document")
	}
	m := &skyv1.ServiceSpec{
		Image: f.Image,
		Argv:  f.Argv,
		Port:  f.Port,
		Readiness: &skyv1.ServiceReadiness{
			Path: f.Readiness.Path, PeriodSeconds: f.Readiness.PeriodSeconds, TimeoutSeconds: f.Readiness.TimeoutSeconds,
			FailureThreshold: f.Readiness.FailureThreshold, InitialDelaySeconds: f.Readiness.InitialDelaySeconds,
		},
		Resources: &skyv1.ServiceResources{
			CpuMillis: f.Resources.CPUMillis, MemoryBytes: f.Resources.MemoryBytes,
			CpuLimitMillis: f.Resources.CPULimitMillis, MemoryLimitBytes: f.Resources.MemoryLimitBytes,
		},
		Rollout: &skyv1.ServiceRolloutPolicy{
			ProgressDeadlineSeconds: f.Rollout.ProgressDeadlineSeconds, TerminationGraceSeconds: f.Rollout.TerminationGraceSeconds,
		},
		Scaling: &skyv1.ServiceScaling{Replicas: f.Scaling.Replicas},
		Setup:   f.Setup,
	}
	for _, e := range f.Env {
		m.Env = append(m.Env, &skyv1.ServiceEnvVar{Name: e.Name, Value: e.Value})
	}
	for _, k := range slices.Sorted(maps.Keys(f.NodeSelector)) {
		m.NodeSelector = append(m.NodeSelector, &skyv1.ServiceLabel{Key: k, Value: f.NodeSelector[k]})
	}
	// The unsupported features are refused by name, before any request.
	switch {
	case len(f.Mounts) != 0:
		m.Mounts = []*skyv1.ServiceMount{{}}
	case f.Workdir.Kind != 0:
		m.Workdir = &skyv1.ArtifactRef{}
	case f.Scaling.CPU.Kind != 0:
		m.Scaling.Cpu = &skyv1.ServiceCpuScaling{}
	}
	s, err := servingwire.SpecFromProto(m)
	if err != nil {
		return serving.Spec{}, fmt.Errorf("service spec: %w", err)
	}
	return s, nil
}

// checkServiceSpecPlain rejects anchors, aliases, merge keys and nulls
// anywhere in the document.
func checkServiceSpecPlain(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("line %d: service spec must not use anchors or aliases", n.Line)
	}
	if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!null" {
		return fmt.Errorf("line %d: service spec must not contain null", n.Line)
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i < len(n.Content); i += 2 {
			if k := n.Content[i]; k.Kind == yaml.ScalarNode && k.ShortTag() == "!!merge" {
				return fmt.Errorf("line %d: service spec must not use merge keys", k.Line)
			}
		}
	}
	for _, c := range n.Content {
		if err := checkServiceSpecPlain(c); err != nil {
			return err
		}
	}
	return nil
}
