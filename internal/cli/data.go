package cli

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// dataPreflightMismatch marks only a local decision about current discovery.
// Server errors, malformed discovery and transport failures never acquire it.
type dataPreflightMismatch struct{ cause error }

func (e *dataPreflightMismatch) Error() string { return e.cause.Error() }
func (e *dataPreflightMismatch) Unwrap() error { return e.cause }
func dataMismatch(code connect.Code, message string) error {
	return &dataPreflightMismatch{connect.NewError(code, errors.New(message))}
}

func portableDataTask(task *v1.Task) backend.Task {
	t := backend.Task{Directory: task.Directory, Setup: task.Setup, Outputs: task.Outputs}
	if task.Workdir != nil {
		t.Workdir = &artifact.Ref{ID: task.Workdir.Id, SHA256: task.Workdir.Sha256, Size: task.Workdir.Size}
	}
	for _, m := range task.DataMounts {
		if m == nil {
			t.DataMounts = append(t.DataMounts, backend.DataMount{})
			continue
		}
		t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: m.Reference, Destination: m.Destination, ReadOnly: m.ReadOnly})
	}
	return t
}

func validateDataTask(task *v1.Task) error {
	if task == nil {
		return errors.New("remote data requires a task")
	}
	if task.Target == "" {
		return errors.New("remote data requires an explicit task target")
	}
	if err := backend.ValidateDataTask(portableDataTask(task), nil); err != nil {
		return fmt.Errorf("remote data: %w", err)
	}
	if _, err := enabledRetry(task); err != nil {
		return fmt.Errorf("retry: %w", err)
	}
	if _, err := backend.DefaultDataPolicy().Budgets(task.TimeoutSeconds); err != nil {
		return fmt.Errorf("remote data: %w", err)
	}
	return nil
}

func advertisedDataPolicy(target *v1.Target, timeout int64) error {
	p := target.GetDataPolicy()
	if p == nil {
		return errors.New("target advertises no data policy")
	}
	policy := backend.DataPolicy{StageSeconds: p.StageSeconds, CollectionSeconds: p.CollectionSeconds, DefaultTaskSeconds: p.DefaultTaskSeconds, MaxTaskSeconds: p.MaxTaskSeconds, ShutdownSeconds: p.ShutdownSeconds}
	if _, err := policy.Budgets(timeout); err != nil {
		return fmt.Errorf("target data policy: %w", err)
	}
	return nil
}

func (o *options) preflightData(ctx context.Context, task *v1.Task) error {
	if err := validateDataTask(task); err != nil {
		return err
	}
	info, err := o.requireCapabilities(ctx, "")
	if err != nil {
		return err
	}
	requiredEndpoint := []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_TRANSFER_V1}
	if task.Workdir != nil {
		requiredEndpoint = append(requiredEndpoint, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1)
	}
	if err := checkEndpointCapabilities(info, requiredEndpoint...); err != nil {
		return &dataPreflightMismatch{err}
	}
	var selected *v1.Target
	for _, target := range info.Targets {
		if target.Name == task.Target {
			selected = target
			break
		}
	}
	if selected == nil {
		return dataMismatch(connect.CodeNotFound, fmt.Sprintf("target %q is not advertised by this server", task.Target))
	}
	required := append(taskCapabilities(task), v1.Capability_CAPABILITY_REMOTE_DATA_V1)
	if len(task.DataMounts) != 0 {
		required = append(required, v1.Capability_CAPABILITY_DATA_MOUNTS_V1)
	}
	if retrying, _ := enabledRetry(task); retrying {
		required = append(required, v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1)
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
			return dataMismatch(connect.CodeFailedPrecondition, fmt.Sprintf("target %q does not advertise %s", task.Target, capabilityName(capability)))
		}
	}
	if err := advertisedDataPolicy(selected, task.TimeoutSeconds); err != nil {
		return &dataPreflightMismatch{err}
	}
	return nil
}

// Only explicit retained hints and an unchanged, already-referenced task may
// probe current discovery drift. The server still validates every lookup MISS.
func dataReplayMismatch(err error) bool {
	var mismatch *dataPreflightMismatch
	return errors.As(err, &mismatch) || connect.CodeOf(err) == connect.CodeUnimplemented
}

func (o *options) planDataTask(ctx context.Context, client skyv1connect.ControlServiceClient, task *v1.Task) (*v1.PlanTaskResponse, error) {
	if err := o.preflightData(ctx, task); err != nil {
		return nil, err
	}
	r, err := client.PlanTask(ctx, request(o, &v1.PlanTaskRequest{Task: task}))
	if err != nil {
		return nil, err
	}
	if r == nil || r.Msg == nil || r.Msg.Target != task.Target {
		return nil, errors.New("server returned a different or missing data target")
	}
	return r.Msg, nil
}

func (o *options) submitData(ctx context.Context, client skyv1connect.ControlServiceClient, task *v1.Task, id string, domain *v1.RequestDomain) (*v1.Job, error) {
	r, err := client.SubmitDataJob(ctx, request(o, &v1.SubmitDataJobRequest{Task: task, RequestId: id, Domain: domain}))
	if err != nil {
		return nil, fmt.Errorf("submit remote data (retry the same task and request hints): %w", retentionDiagnostic(err))
	}
	if r == nil || r.Msg == nil || r.Msg.Job == nil {
		return nil, errors.New("server returned no job")
	}
	return r.Msg.Job, nil
}
