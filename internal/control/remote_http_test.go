//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func remoteHTTPRef() artifact.Ref {
	return artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 64 << 10}
}

func remoteHTTPMessage(ref artifact.Ref, data []byte, next int64, complete bool) *skyv1.ReadInputResponse {
	return &skyv1.ReadInputResponse{Data: data, NextOffset: next, Complete: complete, Reference: &skyv1.ArtifactRef{Id: ref.ID, Sha256: ref.SHA256, Size: ref.Size}}
}

func TestRemoteReadPublicationRefusesNominalSuccess(t *testing.T) {
	ref := remoteHTTPRef()
	for _, test := range []struct {
		name       string
		breakFrame func(*remoteHTTP, *workerPermit)
	}{
		{"uninvoked", func(h *remoteHTTP, p *workerPermit) { h.invoked = false }},
		{"not-returned", func(h *remoteHTTP, p *workerPermit) { h.returned = false }},
		{"body-unjoined", func(h *remoteHTTP, p *workerPermit) { h.bodyJoined = false }},
		{"zero-writes", func(h *remoteHTTP, p *workerPermit) { h.written = 0 }},
		{"short-write", func(h *remoteHTTP, p *workerPermit) { h.writeErr = io.ErrShortWrite }},
		{"write-error", func(h *remoteHTTP, p *workerPermit) { h.writeErr = io.ErrClosedPipe }},
		{"flush-error", func(h *remoteHTTP, p *workerPermit) { h.flushErr = io.ErrClosedPipe }},
		{"close-error", func(h *remoteHTTP, p *workerPermit) { h.closeErr = io.ErrClosedPipe }},
		{"error-status", func(h *remoteHTTP, p *workerPermit) { h.status = 500 }},
		{"unarmed", func(h *remoteHTTP, p *workerPermit) { p.readArmed = false }},
		{"negative-offset", func(h *remoteHTTP, p *workerPermit) { p.readOffset = -1 }},
		{"invalid-end", func(h *remoteHTTP, p *workerPermit) { p.readEnd = p.readRef.Size + 1 }},
		{"wrong-reference", func(h *remoteHTTP, p *workerPermit) { p.readRef.ID = "invalid" }},
		{"wrong-profile", func(h *remoteHTTP, p *workerPermit) { h.contentType = "application/grpc" }},
		{"oversize-write", func(h *remoteHTTP, p *workerPermit) { h.written = remoteResponseLimit + 1 }},
		{"compressed", func(h *remoteHTTP, p *workerPermit) { h.contentEncoding = "gzip" }},
		{"different-frame", func(h *remoteHTTP, p *workerPermit) { p.response = &remoteHTTP{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			call := &ArtifactCall{}
			p := &workerPermit{call: call, operation: RemoteReadInput, readArmed: true, readRef: ref, readEnd: 3}
			h := &remoteHTTP{call: call, invoked: true, returned: true, bodyJoined: true, status: 200, written: 100, contentType: "application/proto"}
			p.response = h
			test.breakFrame(h, p)
			if remoteReadPublished(h, p) {
				t.Fatal("nominal or mismatched observations published")
			}
		})
	}
}

func TestRemoteResponseProfileRefusals(t *testing.T) {
	for _, test := range []struct{ name, key, value string }{

		{"stream", "Content-Type", "application/connect+proto"},
		{"grpc", "Content-Type", "application/grpc"},
		{"parameter", "Content-Type", "application/json; charset=utf-8"},
		{"trailer", "Trailer", "Connect-Error-Code"},
		{"grpc-status", "Grpc-Status", "0"},
		{"connect-error", "Connect-Error-Code", "internal"},
		{"short-length", "Content-Length", "9"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := make(http.Header)
			h.Set("Content-Type", "application/proto")
			h.Set(test.key, test.value)
			if remoteResponseType(h, 10) != "" {
				t.Fatal("nonfixed response profile accepted")
			}
		})
	}
}

type remoteHTTPCloseBody struct {
	io.Reader
	mu    sync.Mutex
	calls int
	err   error
}

func (b *remoteHTTPCloseBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	return b.err
}

func TestRemoteBodyRetainsOriginalCloseError(t *testing.T) {
	want := errors.New("original close uncertainty")
	original := &remoteHTTPCloseBody{Reader: strings.NewReader("request"), err: want}
	body := &remoteRequestBody{original: original}
	for i := 0; i < 3; i++ {
		if !errors.Is(body.Close(), want) {
			t.Fatal("original close result changed")
		}
	}
	if original.calls != 1 {
		t.Fatal("original Close retried")
	}
}

func TestServeRemoteHTTPRejectsUnattachedCall(t *testing.T) {
	c := artifactCallControl()
	call, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	defer call.Done()
	invoked := false
	service := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { invoked = true })
	if err := c.ServeRemoteHTTP(call, httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil), service); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unattached call: %v", err)
	}
	if invoked {
		t.Fatal("unattached call invoked handler")
	}
}

type remoteHTTPBlockingBody struct {
	entered chan struct{}
	release chan struct{}
}

func (b *remoteHTTPBlockingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *remoteHTTPBlockingBody) Close() error             { close(b.entered); <-b.release; return nil }

func TestRemoteBodyDoesNotInventJoinWhileCloseBlocked(t *testing.T) {
	original := &remoteHTTPBlockingBody{entered: make(chan struct{}), release: make(chan struct{})}
	body := &remoteRequestBody{original: original}
	defer func() {
		select {
		case <-original.release:
		default:
			close(original.release)
		}
	}()
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- body.Close() }()
	<-original.entered
	go func() { second <- body.Close() }()
	select {
	case <-first:
		t.Fatal("original blocked Close reported joined")
	default:
	}
	select {
	case <-second:
		t.Fatal("cached wrapper invented a joined Close")
	default:
	}
	close(original.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestRemoteResponseIdentityEncodingOnly(t *testing.T) {
	for _, encoding := range []string{"", "identity", "gzip", "br", "gzip, identity"} {
		h := make(http.Header)
		if encoding != "" {
			h.Set("Content-Encoding", encoding)
		}
		_, ok := remoteResponseEncoding(h)
		if ok != (encoding == "") {
			t.Fatalf("encoding %q accepted=%v", encoding, ok)
		}
	}
	h := make(http.Header)
	h.Add("Content-Encoding", "identity")
	h.Add("Content-Encoding", "identity")
	if _, ok := remoteResponseEncoding(h); ok {
		t.Fatal("ambiguous encoding accepted")
	}
}

// The adapter invokes real Control methods. The actual generated constructor,
// client and default codecs own serialization; no fake inner200 produces credit.
type remoteHTTPTestCallKey struct{}
type remoteHTTPControlService struct{ c *Control }

func (s remoteHTTPControlService) call(ctx context.Context) *ArtifactCall {
	v, _ := ctx.Value(remoteHTTPTestCallKey{}).(*ArtifactCall)
	return v
}
func (s remoteHTTPControlService) ReadInput(ctx context.Context, r *connect.Request[skyv1.ReadInputRequest]) (*connect.Response[skyv1.ReadInputResponse], error) {
	ref, data, err := s.c.ReadRemoteInput(ctx, s.call(ctx), r.Msg.Offset, int(r.Msg.Limit))
	if err != nil {
		return nil, err
	}
	next := r.Msg.Offset + int64(len(data))
	return connect.NewResponse(remoteHTTPMessage(ref, data, next, next == ref.Size)), nil
}
func (s remoteHTTPControlService) PollRelease(ctx context.Context, r *connect.Request[skyv1.PollReleaseRequest]) (*connect.Response[skyv1.PollReleaseResponse], error) {
	release, err := s.c.PollRemoteRelease(ctx, s.call(ctx))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&skyv1.PollReleaseResponse{State: skyv1.RemoteReleaseState(release.State), Revision: release.Revision, TaskContainerId: release.TaskContainerID, TaskExitCode: release.TaskExitCode}), nil
}
func (s remoteHTTPControlService) BeginOutput(ctx context.Context, r *connect.Request[skyv1.BeginOutputRequest]) (*connect.Response[skyv1.BeginOutputResponse], error) {
	u, err := s.c.BeginRemoteOutput(ctx, s.call(ctx), r.Msg.Sha256, r.Msg.Size)
	if err != nil {
		return nil, err
	}
	var state skyv1.RemoteOutputState
	switch u.State {
	case "creating", "writable":
		state = skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE
	case "finalizing":
		state = skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALING
	case "complete":
		state = skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALED
	default:
		return nil, ErrInvalid
	}
	return connect.NewResponse(&skyv1.BeginOutputResponse{Acknowledged: int64(u.Acknowledged), State: state}), nil
}
func (s remoteHTTPControlService) WriteOutput(ctx context.Context, r *connect.Request[skyv1.WriteOutputRequest]) (*connect.Response[skyv1.WriteOutputResponse], error) {
	n, err := s.c.WriteRemoteOutput(ctx, s.call(ctx), r.Msg.Offset, r.Msg.Data)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&skyv1.WriteOutputResponse{Acknowledged: int64(n)}), nil
}
func (s remoteHTTPControlService) FinishOutput(ctx context.Context, r *connect.Request[skyv1.FinishOutputRequest]) (*connect.Response[skyv1.FinishOutputResponse], error) {
	u, err := s.c.FinishRemoteOutput(ctx, s.call(ctx))
	if err != nil {
		return nil, err
	}
	var state skyv1.RemoteOutputSealState
	switch u.State {
	case "finalizing":
		state = skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALING
	case "complete":
		state = skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED
	default:
		return nil, ErrInvalid
	}
	return connect.NewResponse(&skyv1.FinishOutputResponse{State: state}), nil
}

func TestRemoteHTTPGeneratedClientActualPartialPublication(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(map[bool]string{false: "proto", true: "json"}[jsonCodec], func(t *testing.T) {
			f := newDataControlFixture(t)
			ref, archive := f.input(t)
			authority, err := f.engine.Delegate(f.actor)
			if err != nil {
				t.Fatal(err)
			}
			policy := backend.DefaultDataPolicy()
			now := time.Now().UTC()
			target := f.target()
			job, err := f.store.SubmitData(f.actor, nil, "actual-generated-read", backend.Task{Argv: []string{"reader"}, Image: "modeled-image", Workdir: &ref}, target.Name, target.Binding, authority, policy, now)
			if err != nil {
				t.Fatal(err)
			}
			secret := bytes.Repeat([]byte{0x91}, 32)
			start, began, err := f.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{Input: remoteTokenHash(store.RemoteInput, secret)}, policy, now)
			if err != nil || !began {
				t.Fatalf("begin: %v %v", began, err)
			}
			key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
			// Native creation receipts here are MODELED. Transport/Files evidence
			// does not qualify a Pod, integration, or original native handoff.
			_, err = f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: backend.DataCreated{Instance: "modeled-pod-uid", Pod: backend.DataResource{Name: "modeled-pod", UID: "modeled-pod-uid"}, InputSecret: backend.DataResource{Name: "modeled-input", UID: "modeled-input-uid"}}}, now)
			if err != nil {
				t.Fatal(err)
			}
			token, err := remoteToken(key, secret)
			if err != nil {
				t.Fatal(err)
			}
			_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
			type observation struct {
				published          bool
				encoding, accepted string
				err                error
			}
			joined := make(chan observation, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call, err := f.control.TryArtifactCall()
				if err != nil {
					joined <- observation{err: err}
					return
				}
				if err = f.control.AuthorizeRemoteCall(r.Context(), call, string(token), RemoteReadInput); err != nil {
					// No worker is attached on this branch. Close only this original
					// incoming body before ordinary Done; any uncertainty is retained.
					if closeErr := r.Body.Close(); closeErr != nil {
						f.hold(closeErr)
						joined <- observation{err: closeErr}
						return
					}
					call.Done()
					joined <- observation{err: err}
					return
				}
				s := call.service
				s.mu.Lock()
				permit := call.worker
				s.mu.Unlock()
				request := r.Clone(context.WithValue(r.Context(), remoteHTTPTestCallKey{}, call))
				err = f.control.ServeRemoteHTTP(call, w, request, generated)
				s.mu.Lock()
				p := permit
				published := p != nil && remoteReadPublished(p.response, p)
				encoding := ""
				if p != nil && p.response != nil {
					encoding = p.response.contentEncoding
				}
				s.mu.Unlock()
				if err != nil {
					f.hold(err)
				} else {
					call.Done()
				}
				joined <- observation{published: published, encoding: encoding, accepted: r.Header.Get("Accept-Encoding"), err: err}
			}))
			defer server.Close()
			options := []connect.ClientOption{}
			if jsonCodec {
				options = append(options, connect.WithProtoJSON())
			}
			client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL, options...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, clientErr := client.ReadInput(ctx, connect.NewRequest(&skyv1.ReadInputRequest{Limit: 128}))
			observed := <-joined
			if clientErr != nil || observed.err != nil {
				t.Fatalf("real generated call: client=%v producer=%v", clientErr, observed.err)
			}
			if !strings.Contains(observed.accepted, "gzip") {
				t.Fatal("actual default generated client did not advertise gzip")
			}
			if !observed.published || observed.encoding != "" {
				t.Fatalf("actual producer profile: %#v", observed)
			}
			if !bytes.Equal(response.Msg.Data, archive[:128]) || response.Msg.NextOffset != 128 || response.Msg.Complete || response.Msg.Reference.Id != ref.ID || response.Msg.Reference.Sha256 != ref.SHA256 || response.Msg.Reference.Size != ref.Size {
				t.Fatal("actual generated response differs from original reader range")
			}
			s := f.control.artifacts
			s.mu.Lock()
			prefix := int64(-1)
			for d := range s.downloads {
				if d.remote != nil && d.remote.key == key {
					prefix = d.remote.published
				}
			}
			s.mu.Unlock()
			if prefix != 128 {
				t.Fatalf("original cached reader publication prefix=%d", prefix)
			}
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if row.Active == nil || row.Active.Input == nil || row.Active.Input.State != 2 {
				t.Fatal("partial response invented INPUT4 or closed the durable input scope")
			}
			// This is a nonfinal range. It leaves only the permitted cached partial
			// original reader for fixture Drain, never an invented INPUT4 closure.
		})
	}
}

type remoteHTTPPanicBody struct{ calls int }

func (b *remoteHTTPPanicBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *remoteHTTPPanicBody) Close() error             { b.calls++; panic("modeled original Close panic") }
func TestRemoteBodyRecoveredPanicCannotBecomeNilClose(t *testing.T) {
	original := &remoteHTTPPanicBody{}
	body := &remoteRequestBody{original: original}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected original panic")
			}
		}()
		_ = body.Close()
	}()
	if body.Close() == nil {
		t.Fatal("recovered original panic became positive Close")
	}
	if original.calls != 1 {
		t.Fatal("panicking original Close retried")
	}
}
