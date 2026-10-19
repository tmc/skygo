package rpc_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// TestBlockedAuditRevocation holds the database writer while the independent
// file authority revokes a real HTTP stream. A transport abort alone is not
// enough: the outer production handler must also finish within the same bound.
func TestBlockedAuditRevocation(t *testing.T) {
	for _, phase := range []string{"started", "ended"} {
		t.Run(phase, func(t *testing.T) {
			first, backendCanceled := make(chan struct{}), make(chan struct{})
			var firstOnce, canceledOnce sync.Once
			f := scoped(t, &logBackend{read: func(ctx context.Context, offset int64, _ int) (backend.LogChunk, error) {
				if offset == 0 {
					return backend.LogChunk{Data: []byte("first\n"), NextOffset: 6}, nil
				}
				<-ctx.Done()
				canceledOnce.Do(func() { close(backendCanceled) })
				return backend.LogChunk{}, ctx.Err()
			}})
			id := f.running(t, "alice")
			entered, handlerDone := make(chan struct{}), make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				defer close(handlerDone)
				f.handler.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			client := skyv1connect.NewControlServiceClient(srv.Client(), srv.URL)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			clientDone := make(chan error, 1)
			start := func() {
				go func() {
					stream, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: id, Follow: true}))
					if err == nil {
						for stream.Receive() {
							if string(stream.Msg().Data) == "first\n" {
								firstOnce.Do(func() { close(first) })
							}
						}
						err = stream.Err()
						_ = stream.Close()
					}
					clientDone <- err
				}()
			}
			if phase == "ended" {
				start()
				waitEvent(t, first)
			}

			locked, release := make(chan struct{}), make(chan struct{})
			writerDone := make(chan error, 1)
			go func() {
				_, err := f.store.Update(id, func(*backend.Job) error {
					close(locked)
					<-release
					return nil
				})
				writerDone <- err
			}()
			t.Cleanup(func() {
				close(release)
				if err := <-writerDone; err != nil {
					t.Error(err)
				}
			})
			waitEvent(t, locked)
			if phase == "started" {
				start()
				waitEvent(t, entered)
			}
			changed := f.engine.Changed()
			if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
				t.Fatal(err)
			}
			expires := time.Now().Add(2 * time.Second)
			deadline := time.NewTimer(time.Until(expires))
			defer deadline.Stop()
			clientFinished, handlerFinished, revoked := false, false, false
			var clientErr error
			done := (<-chan struct{})(handlerDone)
			for !clientFinished || !handlerFinished || !revoked {
				select {
				case err := <-clientDone:
					clientErr, clientFinished = err, true
				case <-done:
					handlerFinished, done = true, nil
				case <-changed:
					_, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
					revoked = errors.Is(err, auth.ErrUnauthenticated)
					changed = nil
				case <-deadline.C:
					backendStopped := false
					select {
					case <-backendCanceled:
						backendStopped = true
					default:
					}
					t.Fatalf("blocked audit exceeded two-second revocation bound: authority_revoked=%t client_done=%t handler_done=%t backend_canceled=%t", revoked, clientFinished, handlerFinished, backendStopped)
				}
			}
			if !time.Now().Before(expires) {
				t.Fatal("blocked audit client/handler completion exceeded the absolute two-second deadline")
			}
			code(t, clientErr, connect.CodeUnauthenticated)
		})
	}
}
