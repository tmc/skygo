//go:build !linux && !darwin

package control

import (
	"context"
	"time"
)

func sampleStorage(context.Context, string, uint64, func(context.Context) (uint64, uint64, error)) *RetentionAdmin {
	return &RetentionAdmin{Conditions: []string{"metrics_unavailable"}, ObservedAt: time.Now().UTC()}
}
