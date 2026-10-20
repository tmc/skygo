//go:build darwin || linux

package rpc_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

type artifactRPCFixture struct {
	store         *store.Store
	server        *httptest.Server
	binding       string
	client        skyv1connect.ControlServiceClient
	control       *control.Control
	httpHandler   *rpc.HTTPHandler
	tokens        map[string]string
	ids           map[string]string
	authFile      string
	engine        *auth.Engine
	returned      chan string
	files         *artifact.Files
	rootName      string
	runCancel     context.CancelFunc
	runDone       chan error
	directory     string
	root          *os.Root
	localBackend  *local.Backend
	authCancel    context.CancelFunc
	authDone      chan struct{}
	dependency    *artifactRPCFixture
	held          error
	wantHeld      error
	httpJoined    bool
	samplerJoined bool
	retired       bool
	retainOnce    sync.Once
	retire        func() error
	remove        func(string) error
}

// This fixture installs real public Store metadata and the actual Files marker.
// No bbolt edits, synthetic closure proof, workload or fake artifact service.
func artifactRPC(t *testing.T, wrap func(http.Handler) http.Handler) *artifactRPCFixture {
	t.Helper()
	return artifactRPCConfigured(t, wrap, false)
}

func artifactRPCConfigured(t *testing.T, wrap func(http.Handler) http.Handler, workspace bool) *artifactRPCFixture {
	t.Helper()
	return artifactRPCOptions(t, wrap, workspace, false)
}

func artifactRPCOptions(t *testing.T, wrap func(http.Handler) http.Handler, workspace, retained bool) *artifactRPCFixture {
	t.Helper()
	dir := artifactFixtureDirectory(t)
	f := &artifactRPCFixture{directory: dir, tokens: make(map[string]string), ids: make(map[string]string), authFile: filepath.Join(dir, "auth.json"), returned: make(chan string, 128), remove: os.RemoveAll}
	// This one cleanup owns both explicit Close and namespace removal; no
	// independent testing.TempDir cleanup can delete a retained namespace.
	t.Cleanup(func() { f.cleanup(t) })
	rootName := filepath.Join(dir, "artifacts")
	if err := os.Mkdir(rootName, 0700); err != nil {
		f.held = err
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootName)
	f.root, f.rootName = root, rootName
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	var files *artifact.Files
	var s *store.Store
	var server *httptest.Server
	var localBackend *local.Backend
	f.retire = func() error {
		if f.authCancel != nil {
			f.authCancel()
			<-f.authDone
		}
		if f.localBackend != nil {
			if err := f.localBackend.Close(); err != nil {
				return err
			}
		}
		if f.files != nil {
			if err := f.files.Close(); err != nil {
				return err
			}
		}
		if f.root != nil {
			if err := f.root.Close(); err != nil {
				return err
			}
		}
		if f.store != nil {
			if err := f.store.Close(); err != nil {
				return err
			}
		}
		return nil
	}

	info, err := root.Stat(".")
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.held = errors.New("actual root identity unavailable")
		t.Fatal(f.held)
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	files, err = artifact.Open(root, identity)
	f.files = files
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		f.held = err
		t.Fatal(err)
	}
	if retained {
		if _, err := store.EnableRetention(state, store.DefaultRetentionPolicy()); err != nil {
			f.held = err
			t.Fatal(err)
		}
	}
	s, err = store.Open(state)
	f.store = s
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	if _, err := auth.Initialize(f.authFile, filepath.Join(dir, "legacy.token"), s.Owner()); err != nil {
		f.held = err
		t.Fatal(err)
	}
	for _, principal := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Operator}, {"reader", auth.Reader}} {
		if err := auth.AddPrincipal(f.authFile, principal.name, principal.role); err != nil {
			f.held = err
			t.Fatal(err)
		}
		path := filepath.Join(dir, principal.name+".token")
		issued, err := auth.Issue(f.authFile, principal.name, path, time.Hour)
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
		f.tokens[principal.name], f.ids[principal.name] = strings.TrimSuffix(string(data), "\n"), issued.ID
	}
	if err := s.ActivateScoped(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.engine, err = auth.NewFile(f.authFile, s.Owner())
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	f.authCancel, f.authDone = stop, make(chan struct{})
	go func() { defer close(f.authDone); f.engine.Run(ctx) }()
	target := backend.Target{Name: "test", Binding: "test-binding", Kind: "local", Backend: &logBackend{}}
	config := control.Config{Artifacts: files}
	if workspace {
		localBackend, err = local.New(filepath.Join(dir, "local"))
		f.localBackend = localBackend
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
		target.Binding, err = backend.LocalBinding(s.Owner())
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
		target.Backend, target.OutputStorage = localBackend, backend.RemoteOutput
		config.LocalBackend = localBackend
		config.LocalWorkspace = &control.LocalWorkspaceConfig{Target: target.Name, Binding: target.Binding, Backend: localBackend, Roots: root}
	}
	f.binding = target.Binding
	f.control, err = control.NewWithConfig(s, []backend.Target{target}, f.engine, config)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	original := rpc.NewHandler(f.control, f.engine)
	f.httpHandler = original
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		original.ServeHTTP(w, r)
		f.returned <- r.URL.Path
	})
	if wrap != nil {
		handler = wrap(handler)
	}
	server = httptest.NewServer(handler)
	f.client = skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	f.store, f.server, f.files, f.rootName = s, server, files, rootName
	return f
}

func artifactTar(t *testing.T) ([]byte, string) {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	payload := bytes.Repeat([]byte("artifact"), 10000)
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := archive.Bytes()
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}

func TestArtifactHTTPRealLifecycle(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, digest := artifactTar(t)
	started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	id := started.Msg.Upload.Id
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		out, err := f.writeUpload(t, ctx, id, int64(offset), data[offset:end])
		if err != nil || out.Msg.Upload.Acknowledged != int64(end) {
			t.Fatalf("write: %v %v", out, err)
		}
		offset = end
	}
	// A wholly acknowledged earlier range is compared through real Files.
	if _, err := f.writeUpload(t, ctx, id, 8, data[8:24]); err != nil {
		t.Fatal(err)
	}
	finalized, err := f.finalizeUpload(t, ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.finalizeUpload(t, ctx, id)
	if err != nil || again.Msg.Reference.Id != finalized.Msg.Reference.Id || again.Msg.Reference.Sha256 != finalized.Msg.Reference.Sha256 || again.Msg.Reference.Size != finalized.Msg.Reference.Size {
		t.Fatalf("finalize replay: %v %v", again, err)
	}
	opened, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: finalized.Msg.Reference}))
	if err != nil {
		t.Fatal(err)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
	var got []byte
	for {
		chunk, err := f.client.ReadArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.ReadArtifactChunkRequest{DownloadId: opened.Msg.Id, Offset: int64(len(got)), Limit: 64 << 10}))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, chunk.Msg.Data...)
		if chunk.Msg.NextOffset != int64(len(got)) {
			t.Fatal("offset changed")
		}
		if chunk.Msg.Complete {
			break
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatal("actual archive bytes changed")
	}
	// Observe the actual original HTTP return for each first-lease read before
	// closing its descriptor; receiving bytes alone is not handler completion.
	firstReads := (len(data) + (64 << 10) - 1) / (64 << 10)
	f.awaitReturned(t, skyv1connect.ControlServiceReadArtifactChunkProcedure, firstReads)
	if _, err := f.client.CloseArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.CloseArtifactDownloadRequest{DownloadId: opened.Msg.Id})); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.RemoveArtifact(ctx, authorized(f.tokens["alice"], &v1.RemoveArtifactRequest{Reference: finalized.Msg.Reference}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("real24h deletion floor: %v", err)
	}
	// Refused removal must preserve actual read authority. No reduced policy,
	// private row, synthetic clock or expiry shortcut is used.
	second, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: finalized.Msg.Reference}))
	if err != nil {
		t.Fatal(err)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
	if second.Msg.Id == opened.Msg.Id {
		t.Fatal("second lease reused original identity")
	}
	var secondData []byte
	secondReads := 0
	for {
		chunk, err := f.client.ReadArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.ReadArtifactChunkRequest{DownloadId: second.Msg.Id, Offset: int64(len(secondData)), Limit: 64 << 10}))
		if err != nil {
			t.Fatal(err)
		}
		secondReads++
		secondData = append(secondData, chunk.Msg.Data...)
		if chunk.Msg.NextOffset != int64(len(secondData)) {
			t.Fatal("second lease offset changed")
		}
		if chunk.Msg.Complete {
			break
		}
	}
	if secondReads != firstReads || !bytes.Equal(secondData, data) {
		t.Fatalf("refused removal changed bytes/read count: %d vs%d", secondReads, firstReads)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceReadArtifactChunkProcedure, secondReads)
	if _, err := f.client.CloseArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.CloseArtifactDownloadRequest{DownloadId: second.Msg.Id})); err != nil {
		t.Fatal(err)
	}
	info, err := f.client.GetServerInfo(ctx, authorized(f.tokens["alice"], &v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	// Installation advertises the actual artifact endpoint; a target's local
	// workspace capability is a separate assertion in workspace_http_test.go.
	if !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) {
		t.Fatal("installed artifact endpoint omitted")
	}

}

func TestArtifactHTTPPrivacyAndBounds(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, digest := artifactTar(t)
	started, err := f.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{started.Msg.Upload.Id, strings.Repeat("f", 32)} {
		_, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["bob"], &v1.GetArtifactUploadRequest{Id: id}))
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatalf("foreign/missing: %v", err)
		}
	}
	_, err = f.client.BeginArtifactUpload(ctx, authorized(f.tokens["reader"], &v1.BeginArtifactUploadRequest{Sha256: digest, Size: int64(len(data))}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("reader mutation: %v", err)
	}
	for _, size := range []int{0, (64 << 10) + 1} {
		_, err := f.client.WriteArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.WriteArtifactChunkRequest{Id: started.Msg.Upload.Id, Data: make([]byte, size)}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("chunk %d: %v", size, err)
		}
	}
	out, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: started.Msg.Upload.Id}))
	if err != nil || out.Msg.Upload.Acknowledged != 0 {
		t.Fatalf("invalid chunk changed ack: %v %v", out, err)
	}
	if _, err := f.client.CancelArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.CancelArtifactUploadRequest{Id: started.Msg.Upload.Id})); err != nil {
		t.Fatal(err)
	}
}

// This deterministic Flush barrier is simulated transport I/O. The artifact
// Store/Files installation is real; these cold requests refuse before Store
// Begin. It tests four original HTTP owners, not kernel TCP deadlines.
type artifactFlushBarrier struct {
	http.ResponseWriter
	entered chan<- struct{}
	release <-chan struct{}
}

func (w *artifactFlushBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *artifactFlushBarrier) FlushError() error {
	w.entered <- struct{}{}
	<-w.release
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func TestArtifactHTTPPermitWaitsForOuterFlush(t *testing.T) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var once sync.Once
	var requests atomic.Int32
	unblock := func() { once.Do(func() { close(release) }) }
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) <= 4 {
				next.ServeHTTP(&artifactFlushBarrier{ResponseWriter: w, entered: entered, release: release}, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	defer unblock()
	_, digest := artifactTar(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactUploadRequest{Sha256: digest, Size: 1024}))
			results <- err
		}()
	}
	for i := 0; i < 4; i++ {
		waitEvent(t, entered)
	}
	if call, err := f.control.TryArtifactCall(); !errors.Is(err, control.ErrArtifactBusy) {
		if call != nil {
			call.Done()
		}
		t.Fatalf("HTTP owner released early: %v", err)
	}
	unblock()
	for i := 0; i < 4; i++ {
		if err := <-results; !artifactBeginNotStarted(err) {
			t.Fatalf("cold guard: %v", err)
		}
	}
	f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 4)
	call, err := f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	call.Done()
	// After the four actual enclosing HTTP owners return, the original worker
	// can use the existing shared four and produce a real fresh observation.
	begun, err := f.beginUpload(t, ctx, digest, 1024)
	if err != nil || begun.Msg.Upload.Id == "" {
		t.Fatalf("worker could not progress after four original returns: %v %v", begun, err)
	}
}

func TestLegacyRPCCannotSilentlyDropWorkspace(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	task := &v1.Task{Argv: []string{"never-run"}, Workdir: &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: strings.Repeat("b", 64), Size: 1024}}
	for _, submit := range []func() error{
		func() error {
			_, err := f.client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "workspace", Task: task}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetryJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "workspace", Task: task}))
			return err
		},
		func() error {
			_, err := f.client.PlanTask(ctx, authorized(f.tokens["alice"], &v1.PlanTaskRequest{Task: task}))
			return err
		},
		func() error {
			_, err := f.client.QuoteTask(ctx, authorized(f.tokens["alice"], &v1.QuoteTaskRequest{Task: task}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], &v1.SubmitRetainedJobRequest{RequestId: "workspace", Task: task}))
			return err
		},
	} {
		if err := submit(); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("legacy workspace: %v", err)
		}
	}
	out, err := f.client.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{}))
	if err != nil || len(out.Msg.Jobs) != 0 {
		t.Fatalf("legacy submit had effects: %v %v", out, err)
	}
}

// awaitReturned consumes observations from the actual enclosing test HTTP
// handler. It is not a client receipt or a synthetic Control Done callback.
func (f *artifactRPCFixture) awaitReturned(t *testing.T, procedure string, count int) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for count > 0 {
		select {
		case got := <-f.returned:
			if got == procedure {
				count--
			}
		case <-timer.C:
			t.Fatal("original HTTP handler did not return")
		}
	}
}

// The sentinel is injected by a replacement body installed inside test
// middleware. It observes that admitted replacement's Close calls, not net/http's
// already-saved transport body. The error projection is simulated; its once count
// proves neither interception of that saved Close nor physical FD closure.
// net/http's later idempotent transport Close cannot clear the retained error.
type artifactHTTPBodyCloseError struct {
	io.ReadCloser
	calls *atomic.Int32
	err   error
}

func (b *artifactHTTPBodyCloseError) Close() error {
	b.calls.Add(1)
	return errors.Join(b.ReadCloser.Close(), b.err)
}

func TestArtifactHTTPOriginalBodyCloseErrorRetainsAdmission(t *testing.T) {
	var closes atomic.Int32
	failure := errors.New("original simulated HTTP body closure uncertainty")
	f := artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skyv1connect.ControlServiceBeginArtifactUploadProcedure {
				r.Body = &artifactHTTPBodyCloseError{ReadCloser: r.Body, calls: &closes, err: failure}
			}
			next.ServeHTTP(w, r)
		})
	})
	// This original request-body Close error belongs to the SAME RPC HTTP
	// response owner, not Control's artifact fileError. Retain its real frame.
	f.held, f.wantHeld = failure, failure
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, digest := artifactTar(t)
	_, err := f.client.BeginArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactUploadRequest{Sha256: digest, Size: 1024}))
	if !artifactBeginNotStarted(err) {
		t.Fatalf("cold Begin: %v", err)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure, 1)
	if closes.Load() != 1 {
		t.Fatalf("admitted synthetic replacement close count=%d", closes.Load())
	}
	// The actual original call remains, so only three additional slots exist.
	var calls []*control.ArtifactCall
	for i := 0; i < 3; i++ {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	if call, err := f.control.TryArtifactCall(); !errors.Is(err, control.ErrArtifactBusy) {
		if call != nil {
			call.Done()
		}
		t.Fatalf("errored HTTP Close falsely joined: %v", err)
	}
	for _, call := range calls {
		call.Done()
	}
	if closes.Load() != 1 {
		t.Fatal("admitted synthetic replacement body retried")
	}
	drainCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := f.control.DrainArtifacts(drainCtx); !errors.Is(err, control.ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Control credited an unresolved original HTTP owner: %v", err)
	}
	if closes.Load() != 1 {
		t.Fatal("admitted original body Close was retried")
	}

	// The original HTTP handler/body/error/frame/path remain retained until
	// process exit. That is not a positive body/FD close or HTTP join receipt.
}
