package control_test

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

func TestLogsBeforeBackendStart(t *testing.T) {
	for _, tt := range []struct {
		name   string
		state  backend.State
		offset int64
		cause  error
		want   error
	}{
		{"starting", backend.Starting, 0, fs.ErrNotExist, nil},
		{"past pending output", backend.Starting, 1, fs.ErrNotExist, control.ErrLogsUnavailable},
		{"running missing", backend.Running, 0, fs.ErrNotExist, control.ErrLogsUnavailable},
		{"terminal missing", backend.Succeeded, 0, fs.ErrNotExist, control.ErrLogsUnavailable},
		{"identity conflict", backend.Starting, 0, backend.ErrIdentityMismatch, control.ErrLogsUnavailable},
		{"canceled request", backend.Starting, 0, context.Canceled, context.Canceled},
		{"invalid offset", backend.Starting, 0, fs.ErrInvalid, fs.ErrInvalid},
		{"backend failure", backend.Starting, 0, errors.New("unreachable"), control.ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := openStore(t)
			e := newExecutor()
			e.logsErr = tt.cause
			c := newControl(t, s, e)
			j := submit(t, c, "before-start")
			// Reproduce the durable state before Start has created any logs.
			if _, err := s.Update(j.ID, func(j *backend.Job) error {
				j.Status.State = tt.state
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			chunk, err := c.Logs(context.Background(), testActor(), j.ID, j.ID, tt.offset)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Logs error = %v; want %v", err, tt.want)
			}
			if tt.want == nil && (chunk.Complete || len(chunk.Data) != 0 || chunk.NextOffset != 0) {
				t.Fatalf("pending logs = %+v", chunk)
			}
		})
	}
}
