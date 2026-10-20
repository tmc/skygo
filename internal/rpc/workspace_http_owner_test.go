//go:build darwin || linux

package rpc_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Simulated admitted replacement body, not interception of net/http's saved
// transport body, socket FD closure or a blocked kernel operation. The real
// generated client/production handler still own the actual request and slot.
type workspaceBodyClose struct {
	io.ReadCloser
	entered chan struct{}
	release <-chan struct{}
	failure error
	calls   atomic.Int32
}

func (b *workspaceBodyClose) Close() error {
	if b.calls.Add(1) == 1 {
		close(b.entered)
	}
	if b.release != nil {
		<-b.release
	}
	return errors.Join(b.ReadCloser.Close(), b.failure)
}

func workspaceOwnerEvent(t *testing.T, f *artifactRPCFixture, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		f.held = errors.Join(f.held, errors.New(what+" did not positively return"))
		t.Fatal(f.held)
	}
}

func workspaceOtherThree(t *testing.T, f *artifactRPCFixture) func() {
	t.Helper()
	var calls []*control.ArtifactCall
	release := func() {
		for _, call := range calls {
			call.Done()
		}
	}
	for i := 0; i < 3; i++ {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			release()
			t.Fatalf("unused workspace HTTP call did not leave three real slots: %v", err)
		}
		calls = append(calls, call)
	}
	if call, err := f.control.TryArtifactCall(); !errors.Is(err, control.ErrArtifactBusy) {
		if call != nil {
			call.Done()
		}
		release()
		t.Fatalf("workspace HTTP owner was not retained: %v", err)
	}
	return release
}

// A real unavailable response proves the original Wait has actually stopped
// admission; a goroutine-start notification alone would not prove that boundary.
func workspaceWaitStopped(t *testing.T, f *artifactRPCFixture, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+skyv1connect.ControlServiceBeginArtifactUploadProcedure, strings.NewReader(`{"sha256":"`+strings.Repeat("b", 64)+`","size":"1"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+f.tokens["alice"])
		request.Header.Set("Content-Type", "application/json")
		response, err := f.server.Client().Do(request)
		if err != nil {
			f.held = errors.Join(f.held, err)
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			f.held = errors.Join(f.held, readErr, closeErr)
			t.Fatal(f.held)
		}
		// Different auth/freshness failures also use503; their response cannot
		// substitute for this actual handler-admission stop observation.
		if response.StatusCode == http.StatusServiceUnavailable && string(body) == "artifact admission unavailable\n" {
			return
		}
		if !time.Now().Before(deadline) {
			f.held = errors.Join(f.held, errors.New("same HTTP handler did not stop admission"))
			t.Fatal(f.held)
		}
		if err := artifactRetryWait(ctx); err != nil {
			f.held = errors.Join(f.held, err)
			t.Fatal(err)
		}
	}
}

// All nine existing artifact methods traverse actual HTTP while the real
// blocked workspace owner and three real Control calls occupy the shared four.
// No flags, owner map, fake ioActive state or response grant is seeded.
func workspaceNineRefused(t *testing.T, f *artifactRPCFixture, ctx context.Context) {
	t.Helper()
	id := strings.Repeat("a", 32)
	ref := &v1.ArtifactRef{Id: id, Sha256: strings.Repeat("b", 64), Size: 1024}
	methods := []struct {
		procedure string
		request   proto.Message
	}{
		{skyv1connect.ControlServiceBeginArtifactUploadProcedure, &v1.BeginArtifactUploadRequest{Sha256: ref.Sha256, Size: ref.Size}},
		{skyv1connect.ControlServiceGetArtifactUploadProcedure, &v1.GetArtifactUploadRequest{Id: id}},
		{skyv1connect.ControlServiceWriteArtifactChunkProcedure, &v1.WriteArtifactChunkRequest{Id: id, Data: []byte("x")}},
		{skyv1connect.ControlServiceFinalizeArtifactUploadProcedure, &v1.FinalizeArtifactUploadRequest{Id: id}},
		{skyv1connect.ControlServiceCancelArtifactUploadProcedure, &v1.CancelArtifactUploadRequest{Id: id}},
		{skyv1connect.ControlServiceBeginArtifactDownloadProcedure, &v1.BeginArtifactDownloadRequest{Reference: ref}},
		{skyv1connect.ControlServiceReadArtifactChunkProcedure, &v1.ReadArtifactChunkRequest{DownloadId: id, Limit: 1}},
		{skyv1connect.ControlServiceCloseArtifactDownloadProcedure, &v1.CloseArtifactDownloadRequest{DownloadId: id}},
		{skyv1connect.ControlServiceRemoveArtifactProcedure, &v1.RemoveArtifactRequest{Reference: ref}},
	}
	for _, method := range methods {
		body, err := protojson.Marshal(method.request)
		if err != nil {
			t.Fatal(err)
		}
		// Admission precedes Connect decoding. Check the actual HTTP refusal
		// with the same typed request rather than interpreting it as Connect.
		workspaceAdmissionRefused(t, f, ctx, method.procedure, string(body), http.StatusTooManyRequests)
	}
	if _, err := f.client.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{})); err != nil {
		t.Fatalf("ordinary ListJobs acquired artifact owner: %v", err)
	}
}

// Check the outer HTTP admission boundary, not a typed Connect method error.
func workspaceAdmissionRefused(t *testing.T, f *artifactRPCFixture, ctx context.Context, procedure, body string, status int) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+procedure, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+f.tokens["alice"])
	request.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		f.held = errors.Join(f.held, readErr, closeErr)
		t.Fatal(f.held)
	}
	if response.StatusCode != status || string(data) != "artifact admission unavailable\n" {
		t.Fatalf("%s admission status=%d body=%q want=%d", procedure, response.StatusCode, data, status)
	}
}

func TestWorkspaceHTTPBlockedBodyCloseRetainsSameHandlerAndUnusedCall(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var body *workspaceBodyClose
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
				body = &workspaceBodyClose{ReadCloser: r.Body, entered: entered, release: release}
				r.Body = body
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
	workspaceOwnerEvent(t, f, entered, "original admitted Close entry")
	releaseOthers := workspaceOtherThree(t, f)
	defer releaseOthers()
	workspaceNineRefused(t, f, ctx)
	waited := make(chan struct{})
	go func() { f.httpHandler.WaitArtifactHTTP(); close(waited) }()
	workspaceWaitStopped(t, f, ctx)
	select {
	case <-waited:
		t.Fatal("same original Wait returned while request Close blocked")
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
		t.Fatal("original workspace client did not return")
	}
	f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	workspaceOwnerEvent(t, f, waited, "same handler positive wait")
	if body.calls.Load() != 1 {
		t.Fatalf("admitted original Close calls=%d", body.calls.Load())
	}
	var calls []*control.ArtifactCall
	for i := 0; i < 4; i++ {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	for _, call := range calls {
		call.Done()
	}
}

func TestWorkspaceHTTPBodyCloseUncertaintyRetainsOriginalMapAndNamespace(t *testing.T) {
	failure := errors.New("simulated admitted workspace Close uncertainty")
	entered := make(chan struct{})
	var body *workspaceBodyClose
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
				body = &workspaceBodyClose{ReadCloser: r.Body, entered: entered, failure: failure}
				r.Body = body
			}
			next.ServeHTTP(w, r)
		})
	})
	// The error lives in the SAME HTTPHandler response map, not call.fileError.
	// Attach it before any failure/cleanup; retain h/body/writer/Control/root/path.
	f.held, f.wantHeld = failure, failure
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
	if err == nil {
		t.Fatal("invalid workspace request succeeded")
	}
	workspaceOwnerEvent(t, f, entered, "original uncertain Close entry")
	f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	releaseOthers := workspaceOtherThree(t, f)
	defer releaseOthers()
	waited := make(chan struct{})
	go func() { f.httpHandler.WaitArtifactHTTP(); close(waited) }()
	workspaceWaitStopped(t, f, ctx)
	select {
	case <-waited:
		t.Fatal("uncertain same original HTTP owner falsely joined")
	default:
	}
	if body.calls.Load() != 1 {
		t.Fatalf("unknown original Close retried: %d", body.calls.Load())
	}
	// This one wait intentionally remains with the retained same concrete frame
	// until process exit. No replacement Close or borrowed namespace removal.
}

// Returned simulated write/flush failures are distinguished from unknown body
// closure. The real handler must release its unused workspace call after its
// actual synchronous response returns, even though the client sees failure.
type workspaceReturnedWriter struct {
	http.ResponseWriter
	writeErr error
	flushErr error
	writes   atomic.Int32
	flushes  atomic.Int32
}

func (w *workspaceReturnedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *workspaceReturnedWriter) Write(data []byte) (int, error) {
	w.writes.Add(1)
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.ResponseWriter.Write(data)
}
func (w *workspaceReturnedWriter) FlushError() error {
	w.flushes.Add(1)
	if w.flushErr != nil {
		return w.flushErr
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func TestWorkspaceHTTPReturnedWriteAndFlushErrorsJoinOriginal(t *testing.T) {
	for _, mode := range []string{"write", "flush"} {
		t.Run(mode, func(t *testing.T) {
			failure := errors.New("simulated returned workspace " + mode + " error")
			var writer *workspaceReturnedWriter
			f := artifactRPC(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
						writer = &workspaceReturnedWriter{ResponseWriter: w}
						if mode == "write" {
							writer.writeErr = failure
						} else {
							writer.flushErr = failure
						}
						next.ServeHTTP(writer, r)
						return
					}
					next.ServeHTTP(w, r)
				})
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
			if err == nil {
				t.Fatal("invalid workspace request succeeded")
			}
			f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
			if mode == "write" && writer.writes.Load() == 0 || mode == "flush" && writer.flushes.Load() == 0 {
				t.Fatal("simulated original response fault was not reached")
			}
			waited := make(chan struct{})
			go func() { f.httpHandler.WaitArtifactHTTP(); close(waited) }()
			workspaceOwnerEvent(t, f, waited, "returned response-error owner")
			var calls []*control.ArtifactCall
			for i := 0; i < 4; i++ {
				call, err := f.control.TryArtifactCall()
				if err != nil {
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

type workspaceReadBarrier struct {
	io.ReadCloser
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *workspaceReadBarrier) Read(data []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.release })
	return b.ReadCloser.Read(data)
}

func TestWorkspaceHTTPFreshRevocationBeforeResponseWrite(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var writer *workspaceReturnedWriter
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
				r.Body = &workspaceReadBarrier{ReadCloser: r.Body, entered: entered, release: release}
				writer = &workspaceReturnedWriter{ResponseWriter: w}
				next.ServeHTTP(writer, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor := actorForRPC(t, f, "alice")
	before, err := f.store.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
		result <- err
	}()
	workspaceOwnerEvent(t, f, entered, "admitted workspace body read")
	releaseOthers := workspaceOtherThree(t, f)
	defer releaseOthers()
	if err := auth.Revoke(f.authFile, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err = f.engine.Check(actor, auth.Mutate)
		if errors.Is(err, auth.ErrUnauthenticated) || errors.Is(err, auth.ErrPermissionDenied) {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("actual auth engine did not observe revocation: %v", err)
		}
		if err := artifactRetryWait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	releaseOthers()
	unblock()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("revoked workspace call succeeded")
		}
	case <-ctx.Done():
		f.held = errors.Join(f.held, ctx.Err())
		t.Fatal("revoked original response did not return")
	}
	f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	if writer.writes.Load() != 0 || writer.flushes.Load() != 0 {
		t.Fatalf("revoked body escaped fresh artifactWriter check: writes=%d flushes=%d", writer.writes.Load(), writer.flushes.Load())
	}
	waited := make(chan struct{})
	go func() { f.httpHandler.WaitArtifactHTTP(); close(waited) }()
	workspaceOwnerEvent(t, f, waited, "revoked original response owner")
	// Trusted test integrity reads the real global acceptance sequence, not a
	// different principal's private index or the revoked actor's authority.
	after, err := f.store.LastSequence()
	if err != nil || after != before {
		t.Fatalf("revoked malformed request mutated jobs: %d -> %d %v", before, after, err)
	}
}
