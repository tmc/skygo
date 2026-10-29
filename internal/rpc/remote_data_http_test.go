//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/control"
)

// These are actual canonical HTTP/admission refusal tests with real Files and
// Store. They launch no workload and do not assert a positive remote INPUT4.
func TestRemoteHTTPAdmissionPrecedesWorkerDecode(t *testing.T) {
	f := artifactRPC(t, nil)
	var held []interface{ Done() }
	for i := 0; i < 4; i++ {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, call)
	}
	defer func() {
		for _, call := range held {
			call.Done()
		}
	}()
	request, err := http.NewRequest(http.MethodPost, f.server.URL+skyv1connect.RemoteDataServiceReadInputProcedure, strings.NewReader("invalid protobuf"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/proto")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("Authorization", "Bearer malformed")
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data := remoteResponseBytes(t, f, response)
	if response.StatusCode != http.StatusTooManyRequests || !bytes.Contains(data, []byte("remote admission unavailable")) {
		t.Fatalf("capacity decoded/authenticated first: %s %q", response.Status, data)
	}
}

func TestRemoteHTTPMalformedCredentialReleasesOriginalAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, credential string
		code             connect.Code
	}{
		{"header", "malformed-secret", connect.CodeUnauthenticated},
		{"token", strings.Repeat("x", 114), connect.CodeUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := artifactRPC(t, nil)
			client := skyv1connect.NewRemoteDataServiceClient(f.server.Client(), f.server.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for i := 1; i <= 5; i++ {
				request := connect.NewRequest(&v1.ReadInputRequest{Limit: 1})
				request.Header().Set("Authorization", "Bearer "+tc.credential)
				_, err := client.ReadInput(ctx, request)
				if connect.CodeOf(err) != tc.code || strings.Contains(err.Error(), tc.credential) {
					t.Fatalf("fixed worker refusal%d: %v", i, err)
				}
				f.awaitReturned(t, skyv1connect.RemoteDataServiceReadInputProcedure, 1)
			}
			var calls []interface{ Done() }
			defer func() {
				for _, call := range calls {
					call.Done()
				}
			}()
			for i := 0; i < 4; i++ {
				call, err := f.control.TryArtifactCall()
				if err != nil {
					t.Fatalf("original refusal owner leaked slot%d: %v", i, err)
				}
				calls = append(calls, call)
			}
		})
	}
}

func TestRemoteHTTPProfileDoesNotRouteThroughUserAuthentication(t *testing.T) {
	f := artifactRPC(t, nil)
	for _, path := range []string{skyv1connect.RemoteDataServiceReadInputProcedure, "/skygo.v1.RemoteDataService/Unknown"} {
		request, err := http.NewRequest(http.MethodPost, f.server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := f.server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = remoteResponseBytes(t, f, response)
		want := http.StatusUnsupportedMediaType
		if strings.HasSuffix(path, "/Unknown") {
			want = http.StatusNotFound
		}
		if response.StatusCode != want {
			t.Fatalf("worker route fell through user auth: %s", response.Status)
		}
	}
}

// Retain the original client response and actual failed Close, never retry it.
type remoteResponseCloseError struct {
	response *http.Response
	cause    error
}

func (e *remoteResponseCloseError) Error() string {
	return "original remote test response Close failed"
}
func (e *remoteResponseCloseError) Unwrap() error { return e.cause }

func remoteResponseBytes(t *testing.T, f *artifactRPCFixture, response *http.Response) []byte {
	t.Helper()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 96<<10))
	closeErr := response.Body.Close()
	if closeErr != nil {
		f.held = &remoteResponseCloseError{response, errors.Join(readErr, closeErr)}
		f.retain(t)
		t.Fatal(f.held)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	return data
}

func TestLegacyRPCMountsRefuseBeforeAcceptance(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value := &v1.Task{Argv: []string{"must-not-run"}, DataMounts: []*v1.DataMount{{Reference: "dataset", Destination: "/data"}}}
	for _, submit := range []func() error{
		func() error {
			_, err := f.client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "mount", Task: value}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetryJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "mount", Task: value}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], &v1.SubmitRetainedJobRequest{RequestId: "mount", Task: value}))
			return err
		},
		func() error {
			_, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{RequestId: "mount", Task: value}))
			return err
		},
		func() error {
			_, err := f.client.PlanTask(ctx, authorized(f.tokens["alice"], &v1.PlanTaskRequest{Task: value}))
			return err
		},
		func() error {
			_, err := f.client.QuoteTask(ctx, authorized(f.tokens["alice"], &v1.QuoteTaskRequest{Task: value}))
			return err
		},
	} {
		err := submit()
		if code := connect.CodeOf(err); code != connect.CodeInvalidArgument && code != connect.CodeFailedPrecondition {
			t.Fatalf("legacy mount route: %v", err)
		}
	}
	out, err := f.client.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{}))
	if err != nil || len(out.Msg.Jobs) != 0 {
		t.Fatalf("legacy mount acceptance had effects: %+v %v", out, err)
	}
}

func TestRemoteHTTPMethodBodyBoundsPreserveWriteRange(t *testing.T) {
	f := artifactRPC(t, nil)
	for _, tc := range []struct {
		path         string
		size, status int
	}{
		{skyv1connect.RemoteDataServiceReadInputProcedure, (16 << 10) + 1, http.StatusRequestEntityTooLarge},
		{skyv1connect.RemoteDataServiceWriteOutputProcedure, (16 << 10) + 1, http.StatusUnauthorized},
		{skyv1connect.RemoteDataServiceWriteOutputProcedure, (96 << 10) + 1, http.StatusRequestEntityTooLarge},
	} {
		request, err := http.NewRequest(http.MethodPost, f.server.URL+tc.path, strings.NewReader(strings.Repeat("x", tc.size)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/proto")
		request.Header.Set("Connect-Protocol-Version", "1")
		request.Header.Set("Authorization", "Bearer malformed")
		response, err := f.server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = remoteResponseBytes(t, f, response)
		if response.StatusCode != tc.status {
			t.Fatalf("%s size=%d status=%s", tc.path, tc.size, response.Status)
		}
		f.awaitReturned(t, tc.path, 1)
	}
}

// The body bytes and enclosing handler are real. Only the replacement body's
// Close error is MODELED: even EOF is uncertainty, not a positive Close receipt.
func TestRemoteHTTPOriginalCloseErrorRetainsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
	}{
		{"eof", io.EOF},
		{"sentinel", errors.New("modeled original remote body closure uncertainty")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var closes atomic.Int32
			f := artifactRPC(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == skyv1connect.RemoteDataServiceReadInputProcedure {
						r.Body = &artifactHTTPBodyCloseError{ReadCloser: r.Body, calls: &closes, err: tc.failure}
					}
					next.ServeHTTP(w, r)
				})
			})
			// This same concrete HTTP owner remains deliberately retained through
			// test-process exit. No cleanup retries its Close or removes its roots.
			f.held, f.wantHeld = tc.failure, tc.failure
			client := skyv1connect.NewRemoteDataServiceClient(f.server.Client(), f.server.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req := connect.NewRequest(&v1.ReadInputRequest{Limit: 1})
			req.Header().Set("Authorization", "Bearer malformed-secret")
			_, err := client.ReadInput(ctx, req)
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("refusal=%v", err)
			}
			f.awaitReturned(t, skyv1connect.RemoteDataServiceReadInputProcedure, 1)
			if n := closes.Load(); n != 1 {
				t.Fatalf("original Close calls=%d, want1", n)
			}
			var calls []interface{ Done() }
			defer func() {
				for _, call := range calls {
					call.Done()
				}
			}()
			for i := 0; i < 3; i++ {
				call, err := f.control.TryArtifactCall()
				if err != nil {
					t.Fatalf("free slot%d: %v", i, err)
				}
				calls = append(calls, call)
			}
			if call, err := f.control.TryArtifactCall(); !errors.Is(err, control.ErrArtifactBusy) {
				if call != nil {
					call.Done()
				}
				t.Fatalf("failed original Close released admission: %v", err)
			}
			if n := closes.Load(); n != 1 {
				t.Fatalf("original Close retried: %d calls", n)
			}
		})
	}
}
