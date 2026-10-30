//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// progressPeer is a modeled artifact endpoint, not a replacement for the
// real Store/Control/RPC/local-target fixtures in job_test.go. It records
// actual HTTP requests and emits Busy before applying the selected operation.
type progressPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu                                 sync.Mutex
	upload                             *v1.ArtifactUpload
	beginBusy, chunkBusy, finalizeBusy int
	beginErr                           error
	cancel                             context.CancelFunc
	begins                             []*v1.BeginArtifactUploadRequest
	chunks                             []*v1.WriteArtifactChunkRequest
	finals                             []*v1.FinalizeArtifactUploadRequest
	badAck                             bool
}

func (p *progressPeer) BeginArtifactUpload(_ context.Context, r *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins = append(p.begins, proto.Clone(r.Msg).(*v1.BeginArtifactUploadRequest))
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	if p.beginBusy > 0 {
		p.beginBusy--
		e := connect.NewError(connect.CodeResourceExhausted, errors.New("modeled: no upload admitted"))
		e.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
		if p.cancel != nil {
			p.cancel()
		}
		return nil, e
	}
	return connect.NewResponse(&v1.BeginArtifactUploadResponse{Upload: proto.Clone(p.upload).(*v1.ArtifactUpload)}), nil
}

func (p *progressPeer) WriteArtifactChunk(_ context.Context, r *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chunks = append(p.chunks, proto.Clone(r.Msg).(*v1.WriteArtifactChunkRequest))
	if p.chunkBusy > 0 {
		p.chunkBusy--
		if p.cancel != nil {
			p.cancel()
		}
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("modeled: sample pending"))
	}
	p.upload.Acknowledged = r.Msg.Offset + int64(len(r.Msg.Data))
	out := proto.Clone(p.upload).(*v1.ArtifactUpload)
	if p.badAck {
		out.Acknowledged--
	}
	return connect.NewResponse(&v1.WriteArtifactChunkResponse{Upload: out}), nil
}

func (p *progressPeer) FinalizeArtifactUpload(_ context.Context, r *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.finals = append(p.finals, proto.Clone(r.Msg).(*v1.FinalizeArtifactUploadRequest))
	if p.finalizeBusy > 0 {
		p.finalizeBusy--
		if p.cancel != nil {
			p.cancel()
		}
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("modeled: sample pending"))
	}
	return connect.NewResponse(&v1.FinalizeArtifactUploadResponse{Reference: proto.Clone(p.upload.Reference).(*v1.ArtifactRef)}), nil
}

func newProgressPeer(t *testing.T, j *Job) (*progressPeer, skyv1connect.ControlServiceClient) {
	t.Helper()
	p, server := newProgressServer(t, j)
	return p, skyv1connect.NewControlServiceClient(server.Client(), server.URL)
}

func newProgressServer(t *testing.T, j *Job) (*progressPeer, *httptest.Server) {
	t.Helper()
	next := j.st
	next.Retention = RetentionDisabled
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("c", 32)
	p := &progressPeer{upload: &v1.ArtifactUpload{Id: id, Reference: &v1.ArtifactRef{Id: id, Sha256: j.st.Snapshot.SHA256, Size: j.st.Snapshot.Size}, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}}
	_, h := skyv1connect.NewControlServiceHandler(p)
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return p, server
}

func (p *progressPeer) check(t *testing.T, j *Job, archive []byte, begins, chunks, finals int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.begins) != begins || len(p.chunks) != chunks || len(p.finals) != finals {
		t.Fatalf("requests begin/chunk/finalize = %d/%d/%d, want %d/%d/%d", len(p.begins), len(p.chunks), len(p.finals), begins, chunks, finals)
	}
	for _, r := range p.begins {
		if r.Sha256 != j.st.Snapshot.SHA256 || r.Size != j.st.Snapshot.Size {
			t.Fatal("Begin changed frozen reference")
		}
	}
	for _, r := range p.chunks {
		if r.Id != p.upload.Id || r.Offset != 0 || !bytes.Equal(r.Data, archive) {
			t.Fatal("chunk changed upload ID, range or frozen bytes")
		}
	}
	for _, r := range p.finals {
		if r.Id != p.upload.Id {
			t.Fatal("Finalize changed upload ID")
		}
	}
}

func TestUploadBusyProgress(t *testing.T) {
	j, _, _ := newJob(t)
	archive, err := j.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	p, c := newProgressPeer(t, j)
	p.beginBusy, p.chunkBusy, p.finalizeBusy = 1, 1, 1
	if err := j.upload(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	p.check(t, j, archive, 2, 2, 2)
	if j.st.Upload != uploadDone || j.st.Acknowledged != j.st.Snapshot.Size || j.st.Submit != submitNone || j.st.Hold != "" {
		t.Fatalf("upload status %+v", j.Status())
	}
}

func TestUploadBusySharedBudget(t *testing.T) {
	j, _, _ := newJob(t)
	archive, err := j.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	p, c := newProgressPeer(t, j)
	p.beginBusy, p.chunkBusy, p.finalizeBusy = 7, 7, 7
	err = j.upload(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "retry limit reached") {
		t.Fatalf("upload = %v, want shared retry limit", err)
	}
	p.check(t, j, archive, 8, 8, 7)
	if j.st.Upload != uploadFinalize || j.st.Acknowledged != j.st.Snapshot.Size || j.st.Submit != submitNone || j.st.Hold != "" {
		t.Fatalf("exhausted status %+v", j.Status())
	}
}

func TestUploadBusyCancel(t *testing.T) {
	for _, stage := range []string{"begin", "chunk", "finalize"} {
		t.Run(stage, func(t *testing.T) {
			j, _, _ := newJob(t)
			archive, err := j.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			p, c := newProgressPeer(t, j)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p.cancel = cancel
			b, ch, f := 1, 0, 0
			switch stage {
			case "begin":
				p.beginBusy = 1
			case "chunk":
				p.chunkBusy = 1
				ch = 1
			case "finalize":
				p.finalizeBusy = 1
				ch, f = 1, 1
			}
			if err := j.upload(ctx, c); !errors.Is(err, context.Canceled) {
				t.Fatalf("upload = %v, want canceled", err)
			}
			p.check(t, j, archive, b, ch, f)
			if j.st.Submit != submitNone {
				t.Fatal("canceled upload submitted a job")
			}
		})
	}
}

func TestUploadUnknownBeginNotRetried(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeResourceExhausted, connect.CodeUnavailable} {
		t.Run(code.String(), func(t *testing.T) {
			j, _, _ := newJob(t)
			p, c := newProgressPeer(t, j)
			p.beginErr = connect.NewError(code, errors.New("modeled: unknown outcome"))
			if err := j.upload(context.Background(), c); !errors.Is(err, ErrHeld) {
				t.Fatalf("upload = %v, want held", err)
			}
			if err := j.upload(context.Background(), c); !errors.Is(err, ErrHeld) {
				t.Fatalf("second upload = %v, want held", err)
			}
			p.check(t, j, nil, 1, 0, 0)
			if j.st.Hold != HoldUploadBegin || j.st.Upload != uploadBeginSent || j.st.Submit != submitNone {
				t.Fatalf("unknown Begin status %+v", j.Status())
			}
		})
	}
}

func TestUploadBusyBadAcknowledgment(t *testing.T) {
	j, _, _ := newJob(t)
	archive, err := j.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	p, c := newProgressPeer(t, j)
	p.chunkBusy = 1
	p.badAck = true
	if err := j.upload(context.Background(), c); err == nil {
		t.Fatal("accepted partial acknowledgment")
	}
	p.check(t, j, archive, 1, 2, 0)
	if j.st.Acknowledged != 0 || j.st.Submit != submitNone {
		t.Fatalf("bad acknowledgment advanced status %+v", j.Status())
	}
}

func TestUploadBusyDeadline(t *testing.T) {
	for _, stage := range []string{"begin", "chunk", "finalize"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				j, _, _ := newJob(t)
				archive, err := j.snapshot()
				if err != nil {
					t.Fatal(err)
				}
				p, server := newProgressServer(t, j)
				defer server.Close()
				b, ch, f := 1, 0, 0
				procedure := skyv1connect.ControlServiceBeginArtifactUploadProcedure
				phase, acknowledged := uploadNone, int64(0)
				switch stage {
				case "begin":
					p.beginBusy = 1
				case "chunk":
					p.chunkBusy = 1
					ch = 1
					procedure = skyv1connect.ControlServiceWriteArtifactChunkProcedure
					phase = uploadOpen
				case "finalize":
					p.finalizeBusy = 1
					ch, f = 1, 1
					procedure = skyv1connect.ControlServiceFinalizeArtifactUploadProcedure
					phase, acknowledged = uploadFinalize, j.st.Snapshot.Size
				}
				// MODELED time cannot advance through the real HTTP/disk work.
				// The selected Busy response closes the HTTP owners before the
				// retry wait, so the same standard deadline expires at that phase.
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				deadline, _ := ctx.Deadline()
				httpClient := &deadlineProgressClient{server: server, procedure: procedure, ctx: ctx}
				c := skyv1connect.NewControlServiceClient(httpClient, server.URL)
				request, snapshot := j.st.RequestID, j.st.Snapshot
				if err := j.upload(ctx, c); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("upload = %v, want deadline", err)
				}
				if !httpClient.closed || httpClient.beforeDeadline != nil || ctx.Err() != context.DeadlineExceeded || time.Since(start) != 100*time.Millisecond {
					t.Fatalf("deadline did not follow the selected Busy response: closed=%t before=%v context=%v elapsed=%s", httpClient.closed, httpClient.beforeDeadline, ctx.Err(), time.Since(start))
				}
				if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
					t.Fatal("context deadline changed")
				}
				if err := j.upload(ctx, c); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("upload after deadline = %v", err)
				}
				p.check(t, j, archive, b, ch, f)
				if httpClient.requests != b+ch+f || j.st.RequestID != request || j.st.Snapshot != snapshot || j.st.Upload != phase || j.st.Acknowledged != acknowledged {
					t.Fatalf("deadline changed upload identity or progress: requests=%d status=%+v", httpClient.requests, j.Status())
				}
				if stage != "begin" && (j.st.UploadID != p.upload.Id || j.st.Workdir.ID != p.upload.Reference.Id || j.st.Workdir.SHA256 != snapshot.SHA256 || j.st.Workdir.Size != snapshot.Size) {
					t.Fatal("deadline changed the accepted upload reference")
				}
				if j.st.Submit != submitNone {
					t.Fatal("upload submitted after deadline")
				}
			})
		})
	}
}

func TestUploadMarkedBeginResetFailure(t *testing.T) {
	j, _, dir := newJob(t)
	p, c := newProgressPeer(t, j)
	p.beginBusy = 1
	renames := 0
	j.s.fault = func(stage string) error {
		if stage == "rename" {
			renames++
			if renames == 2 {
				return errFault
			}
		}
		return nil
	}
	if err := j.upload(context.Background(), c); !errors.Is(err, errFault) || !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("upload = %v, want unpersisted reset fault", err)
	}
	p.check(t, j, nil, 1, 0, 0)
	if !j.pending || j.st.Upload != uploadNone || j.st.Submit != submitNone {
		t.Fatalf("pending reset status %+v", j.Status())
	}
	durable, err := decodeState(readJournal(t, dir))
	if err != nil || durable.Upload != uploadBeginSent || durable.Submit != submitNone {
		t.Fatalf("durable reset state %+v, %v", durable, err)
	}
}
