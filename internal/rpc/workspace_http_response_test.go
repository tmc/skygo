//go:build darwin || linux

package rpc_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/control"
)

// These are simulated synchronous response barriers, not blocked sockets or
// kernel deadline evidence. The original NewHandler and Control slots are real.
type workspaceResponseBarrier struct {
	http.ResponseWriter
	mode    string
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *workspaceResponseBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *workspaceResponseBarrier) wait() {
	w.once.Do(func() { close(w.entered); <-w.release })
}
func (w *workspaceResponseBarrier) Write(data []byte) (int, error) {
	if w.mode == "write" {
		w.wait()
	}
	return w.ResponseWriter.Write(data)
}
func (w *workspaceResponseBarrier) FlushError() error {
	if w.mode == "flush" {
		w.wait()
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

// All cases are UNRUN authored oracles. No ioActive or original map entry is
// seeded; an otherwise unused workspace HTTP call must survive synchronous I/O.
func TestWorkspaceHTTPBlockedResponseRetainsOriginalUntilReturn(t *testing.T) {
	for _, mode := range []string{"write", "flush"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			f := artifactRPC(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
						w = &workspaceResponseBarrier{ResponseWriter: w, mode: mode, entered: entered, release: release}
					}
					next.ServeHTTP(w, r)
				})
			})
			defer unblock()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			response := make(chan error, 1)
			go func() {
				_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
				response <- err
			}()
			workspaceOwnerEvent(t, f, entered, "original synchronous "+mode+" entry")
			releaseOthers := workspaceOtherThree(t, f)
			defer releaseOthers()
			waited := make(chan struct{})
			go func() { f.httpHandler.WaitArtifactHTTP(); close(waited) }()
			workspaceWaitStopped(t, f, ctx)
			select {
			case <-waited:
				t.Fatal("same HTTP owner retired before original response returned")
			default:
			}
			releaseOthers()
			unblock()
			select {
			case err := <-response:
				if err == nil {
					t.Fatal("invalid workspace request succeeded")
				}
			case <-ctx.Done():
				f.held = errors.Join(f.held, ctx.Err())
				t.Fatal("original response did not return")
			}
			f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
			workspaceOwnerEvent(t, f, waited, "original returned "+mode+" owner")
			var calls []*control.ArtifactCall
			for i := 0; i < 4; i++ {
				call, err := f.control.TryArtifactCall()
				if err != nil {
					for _, c := range calls {
						c.Done()
					}
					t.Fatal(err)
				}
				calls = append(calls, call)
			}
			for _, call := range calls {
				call.Done()
			}
		})
	}
}

type workspaceCountedRead struct {
	io.ReadCloser
	reads *atomic.Int32
}

func (b *workspaceCountedRead) Read(data []byte) (int, error) {
	b.reads.Add(1)
	return b.ReadCloser.Read(data)
}

func TestWorkspaceHTTPMutationAuthorityPrecedesDecode(t *testing.T) {
	var reads atomic.Int32
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
				r.Body = &workspaceCountedRead{ReadCloser: r.Body, reads: &reads}
			}
			next.ServeHTTP(w, r)
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["reader"], &v1.SubmitWorkspaceJobRequest{}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("reader mutation=%v", err)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	if reads.Load() != 0 {
		t.Fatal("workspace body decoded before mutation authority check")
	}
	var calls []*control.ArtifactCall
	for i := 0; i < 4; i++ {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			for _, c := range calls {
				c.Done()
			}
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	for _, call := range calls {
		call.Done()
	}
}
