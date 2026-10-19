package cli

import (
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
)

// enabledRetry validates a policy without rewriting the original request.
func enabledRetry(t *v1.Task) (bool, error) {
	if t.Retry == nil {
		return false, nil
	}
	p, err := backend.NormalizeRetry(&backend.RetryPolicy{MaxAttempts: t.Retry.MaxAttempts, DelaySeconds: t.Retry.DelaySeconds, ExitCodes: t.Retry.ExitCodes})
	return p != nil, err
}
