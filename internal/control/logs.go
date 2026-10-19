package control

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// LogAttempt resolves ordinal once for a log stream after fresh authorization.
// Zero selects the current attempt. Default jobs have one implicit ordinal1.
func (c *Control) LogAttempt(actor auth.Actor, id string, ordinal uint32) (string, error) {
	j, err := c.Get(actor, id)
	if err != nil {
		return "", err
	}
	if len(j.Attempts) == 0 {
		if ordinal > 1 {
			return "", fmt.Errorf("%w: invalid attempt ordinal", ErrInvalid)
		}
		return j.ID, nil
	}
	if ordinal == 0 {
		return j.CurrentAttemptID(), nil
	}
	if ordinal > uint32(len(j.Attempts)) {
		return "", fmt.Errorf("%w: invalid attempt ordinal", ErrInvalid)
	}
	return j.Attempts[ordinal-1].ID, nil
}
func logStatus(j backend.Job, attemptID string) (backend.Status, bool, error) {
	if len(j.Attempts) == 0 {
		if attemptID != j.ID {
			return backend.Status{}, false, fmt.Errorf("%w: invalid attempt selection", ErrInvalid)
		}
		return cloneStatus(j.Status), j.Status.State == backend.Canceled && j.Status.Reason == "canceled before execution", nil
	}
	for _, a := range j.Attempts {
		if a.ID == attemptID {
			return cloneStatus(a.Status), a.Status.State == backend.Canceled && a.StartingAt == nil, nil
		}
	}
	return backend.Status{}, false, fmt.Errorf("%w: invalid attempt selection", ErrInvalid)
}

// readLogs reads the selected immutable attempt after lease admission.
func (c *Control) readLogs(ctx context.Context, actor auth.Actor, id, attemptID string, offset int64) (backend.LogChunk, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if offset < 0 || offset > backend.MaxLogBytes {
		return backend.LogChunk{}, fmt.Errorf("%w: invalid log offset", ErrInvalid)
	}
	j, err := c.Get(actor, id)
	if err != nil {
		return backend.LogChunk{}, err
	}
	status, neverStarted, err := logStatus(j, attemptID)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if neverStarted || status.State == backend.Queued {
		if offset != 0 {
			return backend.LogChunk{}, fmt.Errorf("%w: offset past log end", ErrInvalid)
		}
		return backend.LogChunk{Complete: neverStarted}, nil
	}
	target, reason := c.boundTarget(j)
	if reason != "" {
		return backend.LogChunk{}, ErrUnavailable
	}
	ref := backend.Reference{ID: attemptID, Task: cloneTask(j.Task), Binding: j.TargetBinding, Instance: status.Instance}
	chunk, err := target.Backend.Logs(ctx, ref, offset, backend.MaxChunkBytes)
	if err == nil {
		return chunk, nil
	}
	if status.State == backend.Starting && offset == 0 && errors.Is(err, fs.ErrNotExist) {
		return backend.LogChunk{}, nil
	}
	if errors.Is(err, fs.ErrInvalid) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return chunk, err
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, backend.ErrIdentityMismatch) {
		return chunk, ErrLogsUnavailable
	}
	return chunk, fmt.Errorf("%w: backend log read failed", ErrUnavailable)
}
