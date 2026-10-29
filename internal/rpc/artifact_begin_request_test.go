//go:build darwin || linux

package rpc_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

func keyedUploadDomain(t *testing.T, f *artifactRPCFixture, ctx context.Context) *v1.RequestDomain {
	t.Helper()
	r, err := f.client.GetRetention(ctx, authorized(f.tokens["alice"], &v1.GetRetentionRequest{}))
	if err != nil || !r.Msg.Enabled || r.Msg.Domain == nil {
		t.Fatalf("retained domain: %v %v", r, err)
	}
	return proto.Clone(r.Msg.Domain).(*v1.RequestDomain)
}

func keyedUploadBegin(t *testing.T, f *artifactRPCFixture, ctx context.Context, principal string, request *v1.BeginArtifactUploadRequest) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	t.Helper()
	f.startSampler(t)
	for attempt := 0; ; attempt++ {
		r, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens[principal], proto.Clone(request).(*v1.BeginArtifactUploadRequest)))
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
		if !artifactBeginNotStarted(err) || attempt == 20 {
			return r, err
		}
		if err := artifactRetryWait(ctx); err != nil {
			return nil, err
		}
	}
}

func TestArtifactBeginRequestReplay(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	domain := keyedUploadDomain(t, f, ctx)
	request := &v1.BeginArtifactUploadRequest{Sha256: strings.Repeat("a", 64), Size: 1024, RequestId: proto.String("once"), Domain: domain}
	first, err := keyedUploadBegin(t, f, ctx, "alice", request)
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.store.ArtifactReservations()
	if err != nil || len(before.Subjects) != 1 || before.Subjects[0].File.Inode == 0 {
		t.Fatalf("actual original upload: %+v %v", before, err)
	}
	again, err := keyedUploadBegin(t, f, ctx, "alice", request)
	if err != nil || !proto.Equal(first.Msg.Upload, again.Msg.Upload) {
		t.Fatalf("replay changed original: %v %v", again, err)
	}
	after, err := f.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("replay changed physical cohort: %+v %v", after, err)
	}
	for _, altered := range []*v1.BeginArtifactUploadRequest{
		{Sha256: strings.Repeat("b", 64), Size: request.Size, RequestId: request.RequestId, Domain: domain},
		{Sha256: request.Sha256, Size: request.Size + 1, RequestId: request.RequestId, Domain: domain},
	} {
		_, err := keyedUploadBegin(t, f, ctx, "alice", altered)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("same-key mismatch: %v", err)
		}
	}
	_, err = f.client.GetArtifactUpload(ctx, authorized(f.tokens["bob"], &v1.GetArtifactUploadRequest{Id: first.Msg.Upload.Id}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("foreign upload disclosed: %v", err)
	}
	// Principal scope matches existing request domains: the same textual key
	// names a distinct Bob request and cannot replay Alice's object.
	bob, err := keyedUploadBegin(t, f, ctx, "bob", request)
	if err != nil || bob.Msg.Upload.Id == first.Msg.Upload.Id {
		t.Fatalf("principal scope lost: %v %v", bob, err)
	}
	info, err := f.client.GetServerInfo(ctx, authorized(f.tokens["alice"], &v1.GetServerInfoRequest{}))
	if err != nil || !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_BEGIN_REQUESTS_V1) {
		t.Fatalf("keyed capability missing: %v %v", info, err)
	}
}

func TestArtifactBeginRequestPairRefusal(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	domain := keyedUploadDomain(t, f, ctx)
	before, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []*v1.BeginArtifactUploadRequest{
		{RequestId: proto.String("key")},
		{RequestId: proto.String("")},
		{Domain: domain},
		{RequestId: proto.String(""), Domain: domain},
		{RequestId: proto.String("bad key"), Domain: domain},
		{RequestId: proto.String("key"), Domain: &v1.RequestDomain{Lineage: domain.Lineage}},
		{RequestId: proto.String("key"), Domain: &v1.RequestDomain{Lineage: domain.Lineage, Epoch: domain.Epoch + 1}},
	} {
		request.Sha256, request.Size = strings.Repeat("a", 64), 1024
		if _, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], request)); err == nil {
			t.Fatalf("partial/invalid pair admitted: %v", request)
		}
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
	}
	after, err := f.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("pair refusal changed actual cohort: %+v %v", after, err)
	}
}

func TestArtifactBeginRequestConcurrentReplay(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r := &v1.BeginArtifactUploadRequest{Sha256: strings.Repeat("a", 64), Size: 1024, RequestId: proto.String("shared"), Domain: keyedUploadDomain(t, f, ctx)}
	first, err := keyedUploadBegin(t, f, ctx, "alice", r)
	if err != nil {
		t.Fatal(err)
	}
	// Match the real HTTP handler's four admitted owner slots; a fifth call
	// is correctly refused before the keyed replay handler is entered.
	const count = 4
	type result struct {
		upload *v1.ArtifactUpload
		err    error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], proto.Clone(r).(*v1.BeginArtifactUploadRequest)))
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{upload: out.Msg.Upload}
		}()
	}
	wg.Wait()
	f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, count)
	close(results)
	for got := range results {
		if got.err != nil || !proto.Equal(first.Msg.Upload, got.upload) {
			t.Fatalf("concurrent replay: %v %v", got.upload, got.err)
		}
	}
	cohort, err := f.store.ArtifactReservations()
	if err != nil || len(cohort.Subjects) != 1 || cohort.Subjects[0].ID != first.Msg.Upload.Id {
		t.Fatalf("replay allocated new subjects: %+v %v", cohort, err)
	}
}

// The proxy reads and positively closes the real upstream response, then
// positively closes the downstream connection without publishing the first
// successful Begin response. The real Store/RPC/Files owner already returned;
// this is a local lost-response test, not native crash or Close-failure proof.
func TestArtifactBeginRequestLostResponse(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	domain := keyedUploadDomain(t, f, ctx)
	upstream, err := url.Parse(f.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var dropped atomic.Bool
	closed := make(chan error, 32)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forward := r.Clone(r.Context())
		forward.URL.Scheme, forward.URL.Host = upstream.Scheme, upstream.Host
		forward.RequestURI = ""
		response, err := http.DefaultTransport.RoundTrip(forward)
		if err != nil {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, (128<<10)+1))
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil || len(body) > 128<<10 {
			if err == nil {
				err = errors.New("bounded upstream response exceeded")
			}
			closed <- &beginResponseOwnerError{body: response.Body, err: err}
			http.Error(w, "upstream response unavailable", http.StatusBadGateway)
			return
		}
		if r.URL.Path == skyv1connect.ControlServiceBeginArtifactUploadProcedure && response.StatusCode == http.StatusOK && dropped.CompareAndSwap(false, true) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				closed <- errors.New("proxy does not support hijacking")
				http.Error(w, "proxy cannot drop response", http.StatusInternalServerError)
				return
			}
			conn, buffer, err := hijacker.Hijack()
			if err == nil {
				if closeErr := conn.Close(); closeErr != nil {
					err = &beginResponseOwnerError{conn: conn, buffer: buffer, err: closeErr}
				}
			}
			closed <- err
			return
		}
		for key, values := range response.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(proxy.Close)
	f.client = skyv1connect.NewControlServiceClient(proxy.Client(), proxy.URL)
	r := &v1.BeginArtifactUploadRequest{Sha256: strings.Repeat("a", 64), Size: 1024, RequestId: proto.String("lost"), Domain: domain}
	if first, err := keyedUploadBegin(t, f, ctx, "alice", r); err == nil || first != nil {
		t.Fatalf("first response was not lost: %v %v", first, err)
	}
	select {
	case err := <-closed:
		if err != nil {
			f.held = err // retain actual failed body/socket with the same fixture
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("proxy original close did not return")
	}
	before, err := f.store.ArtifactReservations()
	if err != nil || len(before.Subjects) != 1 {
		t.Fatalf("lost Begin admitted unexpected subjects: %+v %v", before, err)
	}
	again, err := keyedUploadBegin(t, f, ctx, "alice", r)
	if err != nil || again.Msg.Upload.Id != before.Subjects[0].ID {
		t.Fatalf("lost response changed upload identity: %v %v", again, err)
	}
	after, err := f.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("lost-response replay changed cohort: %+v %v", after, err)
	}
}

type beginResponseOwnerError struct {
	body   io.ReadCloser
	conn   net.Conn
	buffer *bufio.ReadWriter
	err    error
}

func (e *beginResponseOwnerError) Error() string { return e.err.Error() }
func (e *beginResponseOwnerError) Unwrap() error { return e.err }
