package sshagent

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// The control clients, Connect, its read limit and decompression, and the
// ledger under test below are actual; the agent transport and its
// responses are the MODELS described in calls_model_test.go. A response
// here is a finite valid protobuf message, sized by an unknown field, not
// an agent's artifact chunk.

// An artifactCase is a generated artifact method the session bounds.
type artifactCase struct {
	method    string
	procedure string
	call      func(context.Context, skyv1connect.ControlServiceClient) (proto.Message, error)
}

var artifactCases = []artifactCase{
	{"BeginArtifactUpload", skyv1connect.ControlServiceBeginArtifactUploadProcedure, unaryCall(skyv1connect.ControlServiceClient.BeginArtifactUpload)},
	{"GetArtifactUpload", skyv1connect.ControlServiceGetArtifactUploadProcedure, unaryCall(skyv1connect.ControlServiceClient.GetArtifactUpload)},
	{"WriteArtifactChunk", skyv1connect.ControlServiceWriteArtifactChunkProcedure, unaryCall(skyv1connect.ControlServiceClient.WriteArtifactChunk)},
	{"FinalizeArtifactUpload", skyv1connect.ControlServiceFinalizeArtifactUploadProcedure, unaryCall(skyv1connect.ControlServiceClient.FinalizeArtifactUpload)},
	{"CancelArtifactUpload", skyv1connect.ControlServiceCancelArtifactUploadProcedure, unaryCall(skyv1connect.ControlServiceClient.CancelArtifactUpload)},
	{"BeginArtifactDownload", skyv1connect.ControlServiceBeginArtifactDownloadProcedure, unaryCall(skyv1connect.ControlServiceClient.BeginArtifactDownload)},
	{"ReadArtifactChunk", skyv1connect.ControlServiceReadArtifactChunkProcedure, unaryCall(skyv1connect.ControlServiceClient.ReadArtifactChunk)},
	{"CloseArtifactDownload", skyv1connect.ControlServiceCloseArtifactDownloadProcedure, unaryCall(skyv1connect.ControlServiceClient.CloseArtifactDownload)},
	{"RemoveArtifact", skyv1connect.ControlServiceRemoveArtifactProcedure, unaryCall(skyv1connect.ControlServiceClient.RemoveArtifact)},
}

// unaryCall returns a call of the generated method m with an empty request.
func unaryCall[Req, Res any](m func(skyv1connect.ControlServiceClient, context.Context, *connect.Request[Req]) (*connect.Response[Res], error)) func(context.Context, skyv1connect.ControlServiceClient) (proto.Message, error) {
	return func(ctx context.Context, cc skyv1connect.ControlServiceClient) (proto.Message, error) {
		resp, err := m(cc, ctx, connect.NewRequest(new(Req)))
		if err != nil {
			return nil, err
		}
		return any(resp.Msg).(proto.Message), nil
	}
}

// callByName calls the unary method named name of cc with an empty request.
func callByName(t *testing.T, cc skyv1connect.ControlServiceClient, name string) (proto.Message, error) {
	t.Helper()
	m := reflect.ValueOf(cc).MethodByName(name)
	if !m.IsValid() {
		t.Fatalf("%T has no method %s", cc, name)
	}
	req := reflect.New(m.Type().In(1).Elem())
	msg := req.Elem().FieldByName("Msg")
	msg.Set(reflect.New(msg.Type().Elem()))
	out := m.Call([]reflect.Value{reflect.ValueOf(context.Background()), req})
	if err, _ := out[1].Interface().(error); err != nil {
		return nil, err
	}
	return out[0].Elem().FieldByName("Msg").Interface().(proto.Message), nil
}

func controlMethods() []string {
	methods := v1.File_skygo_v1_control_proto.Services().ByName("ControlService").Methods()
	var names []string
	for i := range methods.Len() {
		names = append(names, string(methods.Get(i).Name()))
	}
	return names
}

// unknownField is a field number no control service response declares.
const unknownField protowire.Number = 1000

// unknownMessage returns a valid protobuf message of exactly n bytes, n
// well above 128, of any control service response type: one unknown
// length-delimited field of zeros.
func unknownMessage(t *testing.T, n int) []byte {
	t.Helper()
	p := n - protowire.SizeTag(unknownField) - protowire.SizeVarint(uint64(n))
	b := protowire.AppendTag(nil, unknownField, protowire.BytesType)
	b = protowire.AppendBytes(b, make([]byte, p))
	if len(b) != n {
		t.Fatalf("unknown message of %d bytes, want %d", len(b), n)
	}
	return b
}

// unaryServed is an uncompressed unary Connect response holding data.
func unaryServed(data []byte) served {
	return served{http.StatusOK, http.Header{"Content-Type": {"application/proto"}}, data}
}

// gzipServed is a unary Connect response holding data compressed with
// gzip, which the session never offers to accept.
func gzipServed(t *testing.T, data []byte) served {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	sv := unaryServed(buf.Bytes())
	sv.header.Set("Content-Encoding", "gzip")
	return sv
}

func chunkRequest() *connect.Request[v1.ReadArtifactChunkRequest] {
	return connect.NewRequest(&v1.ReadArtifactChunkRequest{})
}

// TestControlArtifactInventory checks that the artifact methods the
// session bounds are exactly the generated service's methods named
// Artifact, so that a new one fails here until its bound is reviewed, and
// that unknownField is unknown to every response.
func TestControlArtifactInventory(t *testing.T) {
	var want []string
	for _, tt := range artifactCases {
		want = append(want, tt.method)
		if p := "/" + skyv1connect.ControlServiceName + "/" + tt.method; tt.procedure != p {
			t.Errorf("%s procedure = %q, want %q", tt.method, tt.procedure, p)
		}
	}
	var got []string
	for _, name := range controlMethods() {
		if strings.Contains(name, "Artifact") {
			got = append(got, name)
		}
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("artifact methods %q, bounded %q; review the bound of each new artifact method", got, want)
	}
	methods := v1.File_skygo_v1_control_proto.Services().ByName("ControlService").Methods()
	if n := reflect.TypeFor[skyv1connect.ControlServiceClient]().NumMethod(); n != methods.Len() {
		t.Errorf("client has %d methods, service %d", n, methods.Len())
	}
	for i := range methods.Len() {
		if m := methods.Get(i); m.Output().Fields().ByNumber(unknownField) != nil {
			t.Fatalf("%s declares field %d", m.Output().FullName(), unknownField)
		}
	}
}

// TestControlArtifactRouting checks that each artifact method sends one
// request, to its own procedure, with the session's token and no
// compression offer, and that with no transport it is refused rather than
// sent through http.DefaultTransport.
func TestControlArtifactRouting(t *testing.T) {
	ctx := context.Background()
	for _, tt := range artifactCases {
		t.Run(tt.method, func(t *testing.T) {
			body := newModelBody(nil)
			next := sequence(unaryServed(nil).reply(body))
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			if _, err := tt.call(ctx, s.Control()); err != nil {
				t.Fatalf("%s = %v", tt.method, err)
			}
			if next.count() != 1 || body.closeCount() != 1 {
				t.Fatalf("requests %d, body Closes %d; want 1 and 1", next.count(), body.closeCount())
			}
			out := next.got[0]
			if out.URL.Path != tt.procedure {
				t.Errorf("path = %q, want %q", out.URL.Path, tt.procedure)
			}
			if got := out.Header.Values("Authorization"); len(got) != 1 || got[0] != "Bearer session-token" {
				t.Errorf("Authorization = %q, want the session's token only", got)
			}
			if out.Header.Get("Accept-Encoding") != "" || out.Header.Get("Connect-Accept-Encoding") != "" {
				t.Errorf("sent encodings %q", out.Header)
			}
			if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
				t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
			}
		})
	}

	cc := newControl(newLedger(nil), &http.Client{}, testEndpoint, "session-token")
	for _, tt := range artifactCases {
		if _, err := tt.call(ctx, cc); !errors.Is(err, errNoTransport) {
			t.Errorf("%s with no transport = %v, want %v", tt.method, err, errNoTransport)
		}
	}
}

// TestControlReadLimits sends a valid response message one byte over 128
// KiB to every unary method of the session's client. Each method named
// Artifact refuses it with ResourceExhausted, settling cleanly; every
// other method decodes it unchanged. An unbounded generated client decodes
// the same response for every method, so the wire is valid.
func TestControlReadLimits(t *testing.T) {
	data := unknownMessage(t, artifactReadMaxBytes+1)
	methods := v1.File_skygo_v1_control_proto.Services().ByName("ControlService").Methods()
	for i := range methods.Len() {
		m := methods.Get(i)
		if m.IsStreamingServer() || m.IsStreamingClient() {
			continue
		}
		name := string(m.Name())
		t.Run(name, func(t *testing.T) {
			ref := skyv1connect.NewControlServiceClient(&http.Client{Transport: sequence(unaryServed(data).reply(newModelBody(data)))}, testEndpoint)
			if msg, err := callByName(t, ref, name); err != nil || proto.Size(msg) != len(data) {
				t.Fatalf("unbounded %s = %v; want the message decoded", name, err)
			}

			body := newModelBody(data)
			next := sequence(unaryServed(data).reply(body))
			s := newTestSession(nil, next)
			msg, err := callByName(t, s.Control(), name)
			if strings.Contains(name, "Artifact") {
				if connect.CodeOf(err) != connect.CodeResourceExhausted || errors.As(err, new(*CallError)) {
					t.Errorf("%s = %v; want Connect's ResourceExhausted", name, err)
				}
			} else if err != nil || proto.Size(msg) != len(data) {
				t.Errorf("%s = %v; want the message decoded", name, err)
			}
			if next.count() != 1 || body.closeCount() != 1 {
				t.Errorf("requests %d, body Closes %d; want 1 and 1", next.count(), body.closeCount())
			}
			if err := s.Close(); err != nil || liveCalls(s) != 0 {
				t.Errorf("Close = %v with %d calls held; want nil and 0", err, liveCalls(s))
			}
		})
	}
}

// TestArtifactReadBoundExact checks each artifact method at the bound: a
// response message of 128 KiB less one byte and of 128 KiB decodes, and
// one of 128 KiB and one byte is refused.
func TestArtifactReadBoundExact(t *testing.T) {
	for _, tt := range artifactCases {
		for _, n := range []int{artifactReadMaxBytes - 1, artifactReadMaxBytes, artifactReadMaxBytes + 1} {
			t.Run(fmt.Sprintf("%s/%d", tt.method, n), func(t *testing.T) {
				data := unknownMessage(t, n)
				body := newModelBody(data)
				next := sequence(unaryServed(data).reply(body))
				s := newTestSession(nil, next)
				msg, err := tt.call(context.Background(), s.Control())
				if n > artifactReadMaxBytes {
					if connect.CodeOf(err) != connect.CodeResourceExhausted || errors.As(err, new(*CallError)) {
						t.Errorf("%s = %v; want Connect's ResourceExhausted", tt.method, err)
					}
				} else if err != nil || proto.Size(msg) != n {
					t.Errorf("%s = %v; want the %d-byte message", tt.method, err, n)
				}
				if next.count() != 1 || body.closeCount() != 1 {
					t.Errorf("requests %d, body Closes %d; want 1 and 1", next.count(), body.closeCount())
				}
				if err := s.Close(); err != nil {
					t.Errorf("Close = %v", err)
				}
			})
		}
	}
}

// TestControlLargeJob checks that GetJob and ListJobs, which are not
// artifact methods, decode actual job responses larger than 128 KiB
// unchanged. It shows the client's routing and limit, not what an agent
// admits.
func TestControlLargeJob(t *testing.T) {
	argv := strings.Repeat("x", 3*artifactReadMaxBytes)
	job := &v1.GetJobResponse{Job: &v1.Job{Id: "job-1", Task: &v1.Task{Argv: []string{"echo", argv}}}}
	list := &v1.ListJobsResponse{Jobs: []*v1.Job{job.Job, {Id: "job-2", Task: &v1.Task{Argv: []string{"echo", argv}}}}}
	jobData, err := proto.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	listData, err := proto.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GetJob response %d bytes, ListJobs response %d bytes, bound %d", len(jobData), len(listData), artifactReadMaxBytes)
	if len(jobData) <= artifactReadMaxBytes || len(listData) <= artifactReadMaxBytes {
		t.Fatal("responses within the artifact bound")
	}
	next := sequence(unaryServed(jobData).reply(newModelBody(jobData)), unaryServed(listData).reply(newModelBody(listData)))
	signer := &modelSigner{}
	s := newTestSession(signer, next)
	ctx := context.Background()

	gotJob, err := s.Control().GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: "job-1"}))
	if err != nil || !proto.Equal(gotJob.Msg, job) {
		t.Errorf("GetJob = %v; want the large job unchanged", err)
	}
	gotList, err := s.Control().ListJobs(ctx, connect.NewRequest(&v1.ListJobsRequest{}))
	if err != nil || !proto.Equal(gotList.Msg, list) {
		t.Errorf("ListJobs = %v; want the large jobs unchanged", err)
	}
	if err := s.Close(); err != nil || signer.count() != 1 || next.count() != 2 {
		t.Errorf("Close = %v, signer Closes %d, requests %d; want nil, 1 and 2", err, signer.count(), next.count())
	}
}

// TestArtifactReadBoundCompressed checks that the bound applies to the
// decompressed message. A hostile agent may answer with gzip though the
// session offered none; Connect decodes it. A gzip response under 128 KiB
// whose message is over it is refused by an artifact method and decoded
// by GetJob, and a small compressed artifact response decodes, so the
// refusal is the bound, not the encoding.
func TestArtifactReadBoundCompressed(t *testing.T) {
	big := gzipServed(t, unknownMessage(t, 2*artifactReadMaxBytes))
	chunk := &v1.ReadArtifactChunkResponse{Data: []byte("chunk"), NextOffset: 5, Complete: true}
	chunkData, err := proto.Marshal(chunk)
	if err != nil {
		t.Fatal(err)
	}
	small := gzipServed(t, chunkData)
	t.Logf("compressed response %d bytes, message %d bytes", len(big.body), 2*artifactReadMaxBytes)
	if len(big.body) >= artifactReadMaxBytes {
		t.Fatalf("compressed response of %d bytes, want under %d", len(big.body), artifactReadMaxBytes)
	}

	tests := []struct {
		name      string
		sv        served
		call      func(context.Context, skyv1connect.ControlServiceClient) (proto.Message, error)
		exhausted bool
		want      proto.Message // if not nil, the decoded message
	}{
		{"artifact over the bound", big, unaryCall(skyv1connect.ControlServiceClient.ReadArtifactChunk), true, nil},
		{"artifact within the bound", small, unaryCall(skyv1connect.ControlServiceClient.ReadArtifactChunk), false, chunk},
		{"job over the bound", big, unaryCall(skyv1connect.ControlServiceClient.GetJob), false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := newModelBody(tt.sv.body)
			next := sequence(tt.sv.reply(body))
			s := newTestSession(nil, next)
			msg, err := tt.call(context.Background(), s.Control())
			if tt.exhausted {
				if connect.CodeOf(err) != connect.CodeResourceExhausted || errors.As(err, new(*CallError)) {
					t.Errorf("= %v; want Connect's ResourceExhausted", err)
				}
			} else if err != nil {
				t.Errorf("= %v; want the message decoded", err)
			}
			if tt.want != nil && err == nil && !proto.Equal(msg, tt.want) {
				t.Errorf("= %v, want %v", msg, tt.want)
			}
			out := next.got[0]
			if out.Header.Get("Accept-Encoding") != "" || out.Header.Get("Connect-Accept-Encoding") != "" {
				t.Errorf("sent encodings %q", out.Header)
			}
			if next.count() != 1 || body.closeCount() != 1 {
				t.Errorf("requests %d, body Closes %d; want 1 and 1", next.count(), body.closeCount())
			}
			if err := s.Close(); err != nil {
				t.Errorf("Close = %v", err)
			}
		})
	}
}

// TestArtifactBoundSettles checks an artifact response over the bound
// whose MODEL body's first Close succeeds or fails. A clean Close leaves
// Connect's ResourceExhausted alone and the session usable by the
// ordinary client. A failed one returns a *CallError keeping that result,
// the Close failure and the call with its original body, faults the
// session for both clients, and leaves the signer open.
func TestArtifactBoundSettles(t *testing.T) {
	info := infoServed(t)
	over := unknownMessage(t, artifactReadMaxBytes+1)
	for _, closeFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("closeFails=%v", closeFails), func(t *testing.T) {
			closeErr := &secretError{"modeled artifact body close failure", secret}
			body := newModelBody(over)
			if closeFails {
				body.closeErr = closeErr
			}
			next := sequence(unaryServed(over).reply(body), info.reply(newModelBody(info.body)))
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			cc := s.Control()
			ctx := context.Background()

			_, err := cc.ReadArtifactChunk(ctx, chunkRequest())
			if connect.CodeOf(err) != connect.CodeResourceExhausted || body.closeCount() != 1 {
				t.Fatalf("ReadArtifactChunk = %v after %d body Closes; want ResourceExhausted after 1", err, body.closeCount())
			}
			if !closeFails {
				if errors.As(err, new(*CallError)) {
					t.Errorf("ReadArtifactChunk = %v, want Connect's own result", err)
				}
				if _, err := cc.GetServerInfo(ctx, infoRequest()); err != nil || next.count() != 2 {
					t.Errorf("GetServerInfo = %v after %d requests; want it sent and answered", err, next.count())
				}
				if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
					t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
				}
				return
			}

			ce := asCallError(t, err, "sshagent: request did not settle: close response body failed")
			var got *secretError
			if !errors.As(err, &got) || got != closeErr {
				t.Errorf("ReadArtifactChunk = %v, want the original Close failure", err)
			}
			for _, format := range []string{"%v", "%s", "%q", "%+v", "%#v"} {
				if got := fmt.Sprintf(format, err); strings.Contains(got, secret) {
					t.Errorf("Sprintf(%q) = %q", format, got)
				}
			}
			c, ok := ce.owner.(*call)
			s.calls.mu.Lock()
			kept := ok && len(c.bodies) == 1 && c.bodies[0].rc == io.ReadCloser(body)
			s.calls.mu.Unlock()
			if !kept {
				t.Errorf("CallError owner %T does not keep the original body", ce.owner)
			}

			_, err = cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedFaulted)
			_, err = cc.RemoveArtifact(ctx, connect.NewRequest(&v1.RemoveArtifactRequest{}))
			asCallError(t, err, refusedFaulted)
			for range 2 {
				cerr := s.Close()
				asCallError(t, cerr, "sshagent: session did not close: close response body failed")
				if !errors.Is(cerr, closeErr) {
					t.Errorf("Close = %v, want the Close failure", cerr)
				}
			}
			if next.count() != 1 || body.closeCount() != 1 || signer.count() != 0 || liveCalls(s) != 1 {
				t.Errorf("requests %d, body Closes %d, signer Closes %d, calls held %d; want 1, 1, 0 and 1",
					next.count(), body.closeCount(), signer.count(), liveCalls(s))
			}
		})
	}
}

// TestArtifactHeldClose checks an artifact response over the bound whose
// MODEL body's first Close is held. Session.Close returns at once, closing
// no signer, and both clients refuse. Once the Close returns nil, the call
// returns Connect's ResourceExhausted and a later Session.Close closes the
// signer once; once it fails, the call and the session keep the failure.
func TestArtifactHeldClose(t *testing.T) {
	over := unknownMessage(t, artifactReadMaxBytes+1)
	for _, closeFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("closeFails=%v", closeFails), func(t *testing.T) {
			o := newOwner(t)
			closeErr := &secretError{"modeled held artifact close failure", secret}
			body := newModelBody(over)
			g := o.gate()
			body.gate = g.c
			if closeFails {
				body.closeErr = closeErr
			}
			next := sequence(unaryServed(over).reply(body))
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			cc := s.Control()
			ctx := context.Background()

			var eA error
			r := o.goRun("ReadArtifactChunk", func() { _, eA = cc.ReadArtifactChunk(ctx, chunkRequest()) })
			await(t, body.entered, "the artifact body's first Close")

			asCallError(t, s.Close(), "sshagent: session did not close: request still running")
			if signer.count() != 0 {
				t.Errorf("signer closed while the artifact Close was held")
			}
			_, err := cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedClosed)
			_, err = cc.RemoveArtifact(ctx, connect.NewRequest(&v1.RemoveArtifactRequest{}))
			asCallError(t, err, refusedClosed)

			g.release()
			join(t, r)
			if connect.CodeOf(eA) != connect.CodeResourceExhausted || body.closeCount() != 1 || next.count() != 1 {
				t.Fatalf("ReadArtifactChunk = %v after %d body Closes, %d requests; want ResourceExhausted after 1 and 1",
					eA, body.closeCount(), next.count())
			}
			if !closeFails {
				if errors.As(eA, new(*CallError)) {
					t.Errorf("ReadArtifactChunk = %v, want Connect's own result", eA)
				}
				for range 2 {
					if err := s.Close(); err != nil {
						t.Errorf("Close = %v", err)
					}
				}
				if signer.count() != 1 || liveCalls(s) != 0 {
					t.Errorf("signer Closes %d, calls held %d; want 1 and 0", signer.count(), liveCalls(s))
				}
				return
			}

			asCallError(t, eA, "sshagent: request did not settle: close response body failed")
			if !errors.Is(eA, closeErr) {
				t.Errorf("ReadArtifactChunk = %v, want the held Close failure", eA)
			}
			for range 2 {
				cerr := s.Close()
				asCallError(t, cerr, "sshagent: session did not close: close response body failed")
				if !errors.Is(cerr, closeErr) {
					t.Errorf("Close = %v, want the held Close failure", cerr)
				}
			}
			if signer.count() != 0 || liveCalls(s) != 1 {
				t.Errorf("signer Closes %d, calls held %d; want 0 and 1", signer.count(), liveCalls(s))
			}
		})
	}
}

// TestArtifactStreamFence checks the artifact client against a pending
// stream of the ordinary one. An open stream B holds no artifact request
// back. From the moment stream A's first Close starts over its held MODEL
// body, the session refuses a new artifact request, and one it admitted
// earlier and paused in its cookie jar, each unsent with a refusal not
// matching io.EOF. A clean drain admits artifact requests again unless
// Session.Close was called while A was pending; a failed Close faults the
// session for both clients.
func TestArtifactStreamFence(t *testing.T) {
	logs := logsServed(t)
	empty := unaryServed(nil)
	tests := []struct {
		name         string
		closeFail    bool
		closeSession bool
	}{
		{"clean close", false, false},
		{"clean close, session closed while pending", false, true},
		{"failed close", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOwner(t)
			closeErr := &secretError{"modeled held stream close failure", secret}
			bodyA := newModelBody(logs.body)
			gateA := o.gate()
			bodyA.gate = gateA.c
			if tt.closeFail {
				bodyA.closeErr = closeErr
			}
			jar := newPauseJar(o, skyv1connect.ControlServiceRemoveArtifactProcedure)
			next := sequence(
				logs.reply(newModelBody(logs.body)),
				empty.reply(newModelBody(nil)),
				logs.reply(bodyA),
				empty.reply(newModelBody(nil)),
			)
			signer := &modelSigner{}
			s, _ := newJarSession(signer, next, jar)
			cc := s.Control()
			ctx := context.Background()

			stB, err := cc.StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			for stB.Receive() {
			}
			if err := stB.Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := cc.ReadArtifactChunk(ctx, chunkRequest()); err != nil {
				t.Fatalf("ReadArtifactChunk with stream B open = %v", err)
			}
			if err := stB.Close(); err != nil {
				t.Fatalf("B Close = %v", err)
			}

			stA, err := cc.StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			for stA.Receive() {
			}
			if err := stA.Err(); err != nil {
				t.Fatal(err)
			}
			ours := ownStream(t, stA)

			// C, an artifact request, is admitted while A is open and
			// paused before its send.
			var eC error
			rC := o.goRun("C", func() {
				_, eC = cc.RemoveArtifact(ctx, connect.NewRequest(&v1.RemoveArtifactRequest{}))
			})
			await(t, jar.entered, "C's request to pause")

			var first error
			rA := o.goRun("A first Close", func() { first = stA.Close() })
			await(t, bodyA.entered, "A's first body Close")

			_, err = cc.ReadArtifactChunk(ctx, chunkRequest())
			if errors.Is(err, io.EOF) {
				t.Errorf("ReadArtifactChunk = %v, which matches io.EOF", err)
			}
			asCallError(t, err, refusedPending)
			jar.release.release()
			join(t, rC)
			if errors.Is(eC, io.EOF) {
				t.Errorf("RemoveArtifact = %v, which matches io.EOF", eC)
			}
			asCallError(t, eC, refusedPending)
			if next.count() != 3 {
				t.Errorf("requests %d while A's Close was held, want 3", next.count())
			}
			if tt.closeSession {
				asCallError(t, s.Close(), "sshagent: session did not close: request still running, stream response not closed")
				if signer.count() != 0 {
					t.Errorf("signer closed while A was pending")
				}
			}

			gateA.release()
			join(t, rA)

			if !tt.closeFail {
				if first != nil {
					t.Errorf("A first Close = %v, want nil", first)
				}
				_, err = cc.ReadArtifactChunk(ctx, chunkRequest())
				if tt.closeSession {
					asCallError(t, err, refusedClosed)
					_, err = cc.GetServerInfo(ctx, infoRequest())
					asCallError(t, err, refusedClosed)
				} else if err != nil {
					t.Errorf("ReadArtifactChunk after A drained = %v", err)
				}
				if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
					t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
				}
				if want := map[bool]int{false: 4, true: 3}[tt.closeSession]; next.count() != want || bodyA.closeCount() != 1 {
					t.Errorf("requests %d, A body Closes %d; want %d and 1", next.count(), bodyA.closeCount(), want)
				}
				return
			}

			ce := asCallError(t, first, "sshagent: request did not settle: close response body failed")
			if !errors.Is(first, closeErr) || ce.owner != any(ours.c) {
				t.Errorf("A first Close = %v; want the held Close failure, holding A's call", first)
			}
			_, err = cc.ReadArtifactChunk(ctx, chunkRequest())
			asCallError(t, err, refusedFaulted)
			_, err = cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedFaulted)
			for range 2 {
				cerr := s.Close()
				asCallError(t, cerr, "sshagent: session did not close: close response body failed")
				if !errors.Is(cerr, closeErr) {
					t.Errorf("Close = %v, want the held Close failure", cerr)
				}
			}
			if bodyA.closeCount() != 1 || signer.count() != 0 || next.count() != 3 || liveCalls(s) != 1 {
				t.Errorf("A body Closes %d, signer Closes %d, requests %d, calls held %d; want 1, 0, 3 and 1",
					bodyA.closeCount(), signer.count(), next.count(), liveCalls(s))
			}
		})
	}
}
