package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

func (o *options) infoCommand() *cobra.Command {
	var target string
	var names []string
	cmd := &cobra.Command{Use: "info", Short: "Show capabilities advertised by this server", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		var required []v1.Capability
		for _, name := range names {
			var capability v1.Capability
			switch name {
			case "finite-jobs-v1":
				capability = v1.Capability_CAPABILITY_FINITE_JOBS_V1
			case "resource-limits-v1":
				capability = v1.Capability_CAPABILITY_RESOURCE_LIMITS_V1
			case "node-selector-v1":
				capability = v1.Capability_CAPABILITY_NODE_SELECTOR_V1
			case "target-labels-v1":
				capability = v1.Capability_CAPABILITY_TARGET_LABELS_V1
			default:
				return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown capability %q", name))
			}
			required = append(required, capability)
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		info, err := o.requireCapabilities(ctx, target, required...)
		if err != nil {
			return err
		}
		if target != "" {
			for _, t := range info.Targets {
				if t.GetName() == target {
					info = &v1.GetServerInfoResponse{Targets: []*v1.Target{t}}
					break
				}
			}
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), info)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tKIND\tCAPABILITIES")
		for _, t := range info.Targets {
			var capabilities []string
			for _, capability := range t.GetCapabilities() {
				capabilities = append(capabilities, capabilityName(capability))
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", t.GetName(), t.GetKind(), strings.Join(capabilities, ","))
		}
		return w.Flush()
	}}
	cmd.Flags().StringVar(&target, "target", "", "Select one named target")
	cmd.Flags().StringSliceVar(&names, "require", nil, "Require capabilities on --target (finite-jobs-v1, resource-limits-v1, node-selector-v1, target-labels-v1)")
	return cmd
}

func capabilityName(capability v1.Capability) string {
	switch capability {
	case v1.Capability_CAPABILITY_FINITE_JOBS_V1:
		return "finite-jobs-v1"
	case v1.Capability_CAPABILITY_RESOURCE_LIMITS_V1:
		return "resource-limits-v1"
	case v1.Capability_CAPABILITY_NODE_SELECTOR_V1:
		return "node-selector-v1"
	case v1.Capability_CAPABILITY_TARGET_LABELS_V1:
		return "target-labels-v1"
	}
	return fmt.Sprintf("unknown(%d)", capability)
}

// requireCapabilities checks only this endpoint and the exact named target.
// Discovery does not reserve capacity or replace validation at submission.
func (o *options) requireCapabilities(ctx context.Context, target string, required ...v1.Capability) (*v1.GetServerInfoResponse, error) {
	if len(required) > 0 && target == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("capability requirements need --target"))
	}
	for _, capability := range required {
		switch capability {
		case v1.Capability_CAPABILITY_FINITE_JOBS_V1, v1.Capability_CAPABILITY_RESOURCE_LIMITS_V1, v1.Capability_CAPABILITY_NODE_SELECTOR_V1, v1.Capability_CAPABILITY_TARGET_LABELS_V1:
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown required capability %d", capability))
		}
	}
	r, err := o.client().GetServerInfo(ctx, request(o, &v1.GetServerInfoRequest{}))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeUnimplemented {
			return nil, connect.NewError(connect.CodeUnimplemented, errors.New("server does not support capability discovery; legacy task commands remain available"))
		}
		return nil, fmt.Errorf("get server capabilities: %w", err)
	}
	if target == "" {
		return r.Msg, nil
	}
	var selected *v1.Target
	for _, t := range r.Msg.Targets {
		if t.GetName() == target {
			if selected != nil {
				return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("server advertised duplicate target %q", target))
			}
			selected = t
		}
	}
	if selected == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("target %q is not advertised by this server", target))
	}
	for _, capability := range required {
		found := false
		for _, advertised := range selected.Capabilities {
			if advertised == capability {
				found = true
				break
			}
		}
		if !found {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("target %q does not advertise %s", target, capabilityName(capability)))
		}
	}
	return r.Msg, nil
}

// taskCapabilities returns capabilities required by the task's extensions.
func taskCapabilities(task *v1.Task) []v1.Capability {
	var required []v1.Capability
	if task.CpuLimitMillis != 0 || task.MemoryLimitBytes != 0 {
		required = append(required, v1.Capability_CAPABILITY_RESOURCE_LIMITS_V1)
	}
	if len(task.NodeSelector) != 0 {
		required = append(required, v1.Capability_CAPABILITY_NODE_SELECTOR_V1)
	}
	if len(task.TargetLabels) != 0 {
		required = append(required, v1.Capability_CAPABILITY_TARGET_LABELS_V1)
	}
	return required
}

// planTask checks the chosen target before returning a successful plan.
// Discovery does not reserve capacity or change the original task.
func (o *options) planTask(ctx context.Context, client skyv1connect.ControlServiceClient, task *v1.Task) (*v1.PlanTaskResponse, error) {
	r, err := client.PlanTask(ctx, request(o, &v1.PlanTaskRequest{Task: task}))
	if err != nil {
		return nil, err
	}
	if required := taskCapabilities(task); len(required) != 0 {
		if r.Msg.Target == "" {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("server returned no planned target"))
		}
		if _, err := o.requireCapabilities(ctx, r.Msg.Target, required...); err != nil {
			return nil, err
		}
	}
	return r.Msg, nil
}
