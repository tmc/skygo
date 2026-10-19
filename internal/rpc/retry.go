package rpc

import (
	"slices"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func retry(p *v1.RetryPolicy) *backend.RetryPolicy {
	if p == nil {
		return nil
	}
	return &backend.RetryPolicy{MaxAttempts: p.MaxAttempts, DelaySeconds: p.DelaySeconds, ExitCodes: slices.Clone(p.ExitCodes)}
}

func wireRetry(p *backend.RetryPolicy) *v1.RetryPolicy {
	if p == nil {
		return nil
	}
	return &v1.RetryPolicy{MaxAttempts: p.MaxAttempts, DelaySeconds: p.DelaySeconds, ExitCodes: slices.Clone(p.ExitCodes)}
}

func timestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func exitCode(code *int32) *int32 {
	if code == nil {
		return nil
	}
	value := *code
	return &value
}
