//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
)

const artifactBeginNotStartedHeader = "Skygo-Artifact-Begin-Not-Started"

func artifactBeginNotStarted(err error) bool {
	var failure *connect.Error
	return errors.As(err, &failure) && failure.Code() == connect.CodeResourceExhausted && reflect.DeepEqual(failure.Meta().Values(artifactBeginNotStartedHeader), []string{"v1"})
}

// These helpers use the original Run/worker and real HTTP. They never install a
// positive cache or infer handler completion from a response body. Each retry is
// a new call after the prior enclosing HTTP handler returned.
func (f *artifactRPCFixture) startSampler(t *testing.T) {
	t.Helper()
	if f.held != nil || f.samplerJoined {
		t.Fatal("cannot start a settled or held fixture")
	}
	if f.runCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.runCancel, f.runDone = cancel, make(chan error, 1)
	go func() { f.runDone <- f.control.Run(ctx) }()
}

func (f *artifactRPCFixture) stopSampler(t *testing.T) bool {
	t.Helper()
	if f.held != nil {
		return false
	}
	if f.samplerJoined {
		return true
	}
	if f.control == nil {
		f.samplerJoined = true
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Even an unrun service must positively settle its original HTTP/FD owners.
	// A prior Drain error is retained and cannot become nil-shortcut success.
	drainErr := f.control.DrainArtifacts(ctx)
	var runErr, joinErr error
	if f.runCancel != nil {
		f.runCancel()
		select {
		case err := <-f.runDone:
			if !errors.Is(err, context.Canceled) {
				runErr = err
			}
		case <-ctx.Done():
			runErr = ctx.Err()
		}
		joinErr = f.control.Join(ctx)
	}
	if err := errors.Join(drainErr, runErr, joinErr); err != nil {
		f.held = err
		return false
	}
	// Retain the concrete cancel/frame fields after successful join as well;
	// samplerJoined alone records positive settlement, never a canceled context.
	f.samplerJoined = true
	return true
}

func artifactRetryWait(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *artifactRPCFixture) beginUpload(t *testing.T, ctx context.Context, digest string, size int64) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	t.Helper()
	f.startSampler(t)
	for attempt := 0; ; attempt++ {
		out, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactUploadRequest{Sha256: digest, Size: size}))
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
		if !artifactBeginNotStarted(err) || attempt == 20 {
			return out, err
		}
		if err := artifactRetryWait(ctx); err != nil {
			return nil, err
		}
	}
}

func (f *artifactRPCFixture) writeUpload(t *testing.T, ctx context.Context, id string, offset int64, data []byte) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		out, err := f.client.WriteArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.WriteArtifactChunkRequest{Id: id, Offset: offset, Data: data}))
		f.awaitReturned(t, skyv1connect.ControlServiceWriteArtifactChunkProcedure, 1)
		if connect.CodeOf(err) != connect.CodeResourceExhausted || attempt == 20 {
			return out, err
		}
		if artifactBeginNotStarted(err) {
			t.Fatal("Begin-only marker on Write")
		}
		if err := artifactRetryWait(ctx); err != nil {
			return nil, err
		}
	}
}

func (f *artifactRPCFixture) finalizeUpload(t *testing.T, ctx context.Context, id string) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		out, err := f.client.FinalizeArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.FinalizeArtifactUploadRequest{Id: id}))
		f.awaitReturned(t, skyv1connect.ControlServiceFinalizeArtifactUploadProcedure, 1)
		if connect.CodeOf(err) != connect.CodeResourceExhausted || attempt == 20 {
			return out, err
		}
		if artifactBeginNotStarted(err) {
			t.Fatal("Begin-only marker on Finalize")
		}
		if err := artifactRetryWait(ctx); err != nil {
			return nil, err
		}
	}
}

// This distinct temporary Control borrows the actual installed Store/Files. The
// caller joins its HTTP and sampler owners before using the original unrun
// workspace Control. No second worker is installed in either Control.
func (f *artifactRPCFixture) uploadService(t *testing.T) *artifactRPCFixture {
	t.Helper()
	if f.dependency != nil {
		t.Fatal("fixture already has its one temporary upload dependency")
	}
	c, err := control.NewWithConfig(f.store, []backend.Target{{Name: "upload-only", Binding: "upload-only", Backend: &logBackend{}}}, f.engine, control.Config{Artifacts: f.files})
	out := &artifactRPCFixture{store: f.store, control: c, engine: f.engine, tokens: f.tokens, files: f.files, rootName: f.rootName, directory: f.directory, returned: make(chan string, 128)}
	f.dependency = out
	if err != nil {
		out.held = err
		f.held = err
		t.Fatal(err)
	}
	original := rpc.NewHandler(c, f.engine)
	out.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { original.ServeHTTP(w, r); out.returned <- r.URL.Path }))
	out.client = skyv1connect.NewControlServiceClient(out.server.Client(), out.server.URL)
	return out
}

func artifactRootBytes(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	err := filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		out[rel], err = os.ReadFile(name)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// UNRUN source oracle. The cold refusal is generated by the production handler,
// not a proxy. Both real retention modes must refuse before any durable Begin.
func TestArtifactHTTPColdBeginMarkerAndUnchangedOwnership(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "retention_off"
		if retained {
			name = "retention_on"
		}
		t.Run(name, func(t *testing.T) {
			f := artifactRPCOptions(t, nil, false, retained)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			data, digest := artifactTar(t)
			before, err := f.store.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			rootBefore := artifactRootBytes(t, f.rootName)
			_, err = f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactUploadRequest{Sha256: digest, Size: int64(len(data))}))
			if !artifactBeginNotStarted(err) {
				t.Fatalf("cold Begin lacks exact singleton marker: %v", err)
			}
			f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
			after, err := f.store.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rootBefore, artifactRootBytes(t, f.rootName)) {
				t.Fatal("cold refusal changed actual reservation/root bytes")
			}
			for _, tt := range []struct {
				token string
				size  int64
				code  connect.Code
			}{{f.tokens["reader"], int64(len(data)), connect.CodePermissionDenied}, {f.tokens["alice"], 0, connect.CodeInvalidArgument}} {
				_, err := f.client.BeginArtifactUpload(ctx, authorized(tt.token, &v1.BeginArtifactUploadRequest{Sha256: digest, Size: tt.size}))
				if connect.CodeOf(err) != tt.code {
					t.Fatalf("code=%v want=%v", err, tt.code)
				}
				var failure *connect.Error
				if errors.As(err, &failure) && len(failure.Meta().Values(artifactBeginNotStartedHeader)) != 0 {
					t.Fatal("marker on auth/validation refusal")
				}
				f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
			}
			started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			actual, err := f.store.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			if len(actual.Subjects) != 1 || actual.Subjects[0].Kind != "upload" || actual.Subjects[0].ID != started.Msg.Upload.Id || actual.Subjects[0].File.Inode == 0 {
				t.Fatalf("durable allocation=%+v", actual.Subjects)
			}
		})
	}
}

// Actual two-quantum append and exact replay leave the prepaid cohort unchanged.
// Finalize's first structural transition must durably retain the same ID while
// refusing late publication; a subsequent NEW call uses a fresh worker sample.
func TestArtifactHTTPGrowthIntervalAndFinalizeNewCall(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, digest := artifactTar(t)
	started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	id := started.Msg.Upload.Id
	before, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		written, err := f.writeUpload(t, ctx, id, int64(offset), data[offset:end])
		if err != nil || written.Msg.Upload.Acknowledged != int64(end) {
			t.Fatalf("actual append=%v %v", written, err)
		}
		offset = end
	}
	after, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("settled append changed prepaid identity/cohort")
	}
	replay, err := f.writeUpload(t, ctx, id, 8, data[8:24])
	if err != nil || replay.Msg.Upload.Acknowledged != int64(len(data)) {
		t.Fatalf("actual replay=%v %v", replay, err)
	}
	// Busy before the first durable transition may mean an actual sample aged
	// under load. Only a real observed finalizing row satisfies this boundary.
	for attempt := 0; ; attempt++ {
		_, err = f.client.FinalizeArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.FinalizeArtifactUploadRequest{Id: id}))
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("first structural Finalize=%v", err)
		}
		var failure *connect.Error
		if errors.As(err, &failure) && len(failure.Meta().Values(artifactBeginNotStartedHeader)) != 0 {
			t.Fatal("Begin marker on durable finalizing response")
		}
		f.awaitReturned(t, skyv1connect.ControlServiceFinalizeArtifactUploadProcedure, 1)
		pending, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: id}))
		f.awaitReturned(t, skyv1connect.ControlServiceGetArtifactUploadProcedure, 1)
		if err != nil || pending.Msg.Upload.Acknowledged != int64(len(data)) {
			t.Fatalf("durable finalize state=%v %v", pending, err)
		}
		if pending.Msg.Upload.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING {
			break
		}
		if pending.Msg.Upload.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE || attempt == 20 {
			t.Fatalf("never entered actual durable finalizing: %v", pending)
		}
		if err := artifactRetryWait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	finalized, err := f.finalizeUpload(t, ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	ref := finalized.Msg.Reference
	if ref.Id != id || ref.Sha256 != digest || ref.Size != int64(len(data)) {
		t.Fatalf("published different reference=%v", ref)
	}
	again, err := f.finalizeUpload(t, ctx, id)
	if err != nil || !reflect.DeepEqual(again.Msg.Reference, ref) {
		t.Fatalf("same-ID replay=%v %v", again, err)
	}
}

// Real Files comparison refuses different bytes and partial overlap without
// advancing the Store acknowledgement; these are not simulated policy vectors.
func TestArtifactHTTPReplayRefusesChangedBytesAndPartialOverlap(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, digest := artifactTar(t)
	started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	id := started.Msg.Upload.Id
	written, err := f.writeUpload(t, ctx, id, 0, data[:64<<10])
	if err != nil {
		t.Fatal(err)
	}
	wrong := bytes.Clone(data[8:24])
	wrong[0] ^= 1
	for _, request := range []*v1.WriteArtifactChunkRequest{{Id: id, Offset: 8, Data: wrong}, {Id: id, Offset: (64 << 10) - 8, Data: data[(64<<10)-8 : (64<<10)+8]}} {
		_, err := f.client.WriteArtifactChunk(ctx, authorized(f.tokens["alice"], request))
		if err == nil {
			t.Fatal("changed or partly overlapping range acknowledged")
		}
		f.awaitReturned(t, skyv1connect.ControlServiceWriteArtifactChunkProcedure, 1)
	}
	actual, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: id}))
	if err != nil || actual.Msg.Upload.Acknowledged != written.Msg.Upload.Acknowledged {
		t.Fatalf("refused write changed acknowledgement: %v %v", actual, err)
	}
}

// Four real known-inode parts reach the public per-owner pending-count limit.
// The fifth call waits through genuine pre-Begin cold refusals, then observes
// actual Store quota refusal, which must never carry non-admission authority.
func TestArtifactHTTPStoreQuotaHasNoBeginMarker(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	data, digest := artifactTar(t)
	ids := make(map[string]bool)
	for i := 0; i < 4; i++ {
		started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		if ids[started.Msg.Upload.Id] {
			t.Fatal("Begin reused a durable identity")
		}
		ids[started.Msg.Upload.Id] = true
	}
	before, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Subjects) != 4 {
		t.Fatalf("actual pending cohort=%+v", before.Subjects)
	}
	for _, subject := range before.Subjects {
		if subject.Kind != "upload" || !ids[subject.ID] || subject.File.Inode == 0 {
			t.Fatalf("quota fixture is not actual known-inode upload: %+v", subject)
		}
	}
	rootBefore := artifactRootBytes(t, f.rootName)
	_, err = f.beginUpload(t, ctx, digest, int64(len(data)))
	var failure *connect.Error
	if !errors.As(err, &failure) || failure.Code() != connect.CodeResourceExhausted || failure.Message() != "artifact capacity reached" {
		t.Fatalf("real quota refusal=%v", err)
	}
	if len(failure.Meta().Values(artifactBeginNotStartedHeader)) != 0 {
		t.Fatal("Store quota refusal carried Begin marker")
	}
	after, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rootBefore, artifactRootBytes(t, f.rootName)) {
		t.Fatal("quota refusal changed real reservations/root bytes")
	}
}
