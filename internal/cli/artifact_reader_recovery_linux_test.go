//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type readerRecoveryProcess struct {
	cmd              *exec.Cmd
	done             chan error
	signaled, waited bool
	out, diag        bytes.Buffer
}

type readerRecoveryNativeFrame struct {
	directory string
	commands  []*exec.Cmd
	processes []*readerRecoveryProcess
	stores    []*store.Store
	owners    []any
	causes    []error
}

var heldReaderRecoveryNativeFrames struct {
	sync.Mutex
	frames []*readerRecoveryNativeFrame
}

func (f *readerRecoveryNativeFrame) hold(owner any, err error) {
	f.owners = append(f.owners, owner)
	f.causes = append(f.causes, err)
}

// TestArtifactReaderRecoveryExec captures this process before exec, when it owns
// no artifact reader. Exec preserves PID/start/namespace; the actual production
// executable then captures the SAME self identity during ordinary startup.
func TestArtifactReaderRecoveryExec(t *testing.T) {
	if os.Getenv("SKYGO_READER_RECOVERY_EXEC") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("SKYGO_READER_RECOVERY_ARGS")), &args); err != nil {
		t.Fatal(err)
	}
	value, err := local.CaptureServerLifetime()
	if err != nil {
		frame := &readerRecoveryNativeFrame{}
		frame.hold(nil, err)
		heldReaderRecoveryNativeFrames.Lock()
		heldReaderRecoveryNativeFrames.frames = append(heldReaderRecoveryNativeFrames.frames, frame)
		heldReaderRecoveryNativeFrames.Unlock()
		t.Fatal(err)
	}
	file, err := os.OpenFile(os.Getenv("SKYGO_READER_RECOVERY_IDENTITY"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := json.NewEncoder(file).Encode(value)
	if closeErr := file.Close(); closeErr != nil {
		frame := &readerRecoveryNativeFrame{}
		frame.hold(file, closeErr)
		frame.hold(nil, writeErr)
		heldReaderRecoveryNativeFrames.Lock()
		heldReaderRecoveryNativeFrames.frames = append(heldReaderRecoveryNativeFrames.frames, frame)
		heldReaderRecoveryNativeFrames.Unlock()
		t.Fatal(closeErr)
	}
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	exe := os.Getenv("SKYGO_READER_RECOVERY_BINARY")
	if err := syscall.Exec(exe, append([]string{exe}, args...), os.Environ()); err != nil {
		t.Fatal(err)
	}
}

// TestArtifactStoppedReaderRecoveryNative qualifies only this isolated
// artifact-only/no-job/no-child/no-reader-FD-transfer fixture. It is not a proof
// for arbitrary historical deployments or mediated kernels. Run only after the
// target kernel's active-namespace ESRCH semantics and visibility are qualified.
func TestArtifactStoppedReaderRecoveryNative(t *testing.T) {
	if os.Getenv("SKYGO_READER_RECOVERY_NATIVE") != "1" {
		t.Skip("set SKYGO_READER_RECOVERY_NATIVE=1 on the separately qualified Linux host")
	}
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-reader-recovery-native")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	frame := &readerRecoveryNativeFrame{directory: directory}
	// Always retain the original frame and evidence; no cleanup deletes this root.
	heldReaderRecoveryNativeFrames.Lock()
	heldReaderRecoveryNativeFrames.frames = append(heldReaderRecoveryNativeFrames.frames, frame)
	heldReaderRecoveryNativeFrames.Unlock()
	t.Logf("retained stopped-reader native evidence: %s", directory)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stop := func(p *readerRecoveryProcess, signal os.Signal) bool {
		t.Helper()
		if p.waited {
			return true
		}
		if p.signaled {
			return false
		}
		p.signaled = true
		if err := p.cmd.Process.Signal(signal); err != nil {
			frame.hold(p, err)
			t.Errorf("original server signal: %v", err)
			return false
		}
		select {
		case err := <-p.done:
			p.waited = true
			if signal == os.Kill {
				var exit *exec.ExitError
				if errors.As(err, &exit) {
					status, ok := exit.Sys().(syscall.WaitStatus)
					if ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
						return true
					}
				}
			} else if err == nil {
				return true
			}
			frame.hold(p, err)
			t.Errorf("original server Wait differs: %v", err)
		case <-time.After(30 * time.Second):
			frame.hold(p, errors.New("original server Wait remains pending"))
			t.Error("original server Wait remains pending; no retry or replacement")
		}
		return false
	}
	t.Cleanup(func() {
		for _, p := range frame.processes {
			if !p.waited && !p.signaled {
				stop(p, os.Interrupt)
			}
		}
	})
	var env []string
	if mode := os.Getenv("SKYGO_ARTIFACT_BINARY_COVERMODE"); mode != "" {
		if mode != testing.CoverMode() || (mode != "set" && mode != "count" && mode != "atomic") {
			t.Fatal("artifact fixture coverage mode differs")
		}
		if dir := coverDir(t); dir != "" {
			env = append(env, "GOCOVERDIR="+dir)
		}
	}
	run := func(endpoint string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, exe, args...)
		cmd.Dir = directory
		cmd.Env = append(append(append([]string(nil), os.Environ()...), env...), "SKYGO_ENDPOINT="+endpoint, "SKYGO_TOKEN=coverage-token")
		cmd.WaitDelay = 2 * time.Second
		var out, diag bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diag
		frame.commands = append(frame.commands, cmd)
		if err := cmd.Run(); err != nil {
			frame.hold(cmd, err)
			t.Fatalf("CLI %q: %v\n%s\n%s", args, err, &out, &diag)
		}
		return out.String()
	}
	data := filepath.Join(directory, "state")
	root := filepath.Join(data, "artifacts")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	run("", "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	start := func(name string, recover bool) (*readerRecoveryProcess, string, local.ServerLifetime) {
		t.Helper()
		address := freeAddress(t)
		args := []string{"server", "--listen", address, "--data-dir", data, "--artifact-dir", root, "--token", "coverage-token"}
		if recover {
			args = append(args, "--recover-stopped-artifact-readers")
		}
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		identity := filepath.Join(directory, name+"-identity.json")
		cmd := exec.Command(helper, "-test.run=^TestArtifactReaderRecoveryExec$")
		cmd.Dir = directory
		cmd.Env = append(append(append([]string(nil), os.Environ()...), env...), "SKYGO_TOKEN=", "SKYGO_READER_RECOVERY_EXEC=1", "SKYGO_READER_RECOVERY_BINARY="+exe, "SKYGO_READER_RECOVERY_ARGS="+string(raw), "SKYGO_READER_RECOVERY_IDENTITY="+identity)
		p := &readerRecoveryProcess{cmd: cmd, done: make(chan error, 1)}
		cmd.Stdout, cmd.Stderr = &p.out, &p.diag
		frame.commands = append(frame.commands, cmd)
		if err := cmd.Start(); err != nil {
			frame.hold(cmd, err)
			t.Fatal(err)
		}
		frame.processes = append(frame.processes, p)
		go func() { p.done <- cmd.Wait() }()
		endpoint := "http://" + address
		client := &http.Client{Timeout: time.Second}
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			response, err := client.Get(endpoint + "/ready")
			if err == nil {
				closeErr := response.Body.Close()
				if closeErr != nil {
					frame.hold(response.Body, closeErr)
					t.Fatal(closeErr)
				}
				if response.StatusCode == http.StatusOK {
					break
				}
			}
			select {
			case err := <-p.done:
				p.waited = true
				frame.hold(p, err)
				t.Fatalf("original server exited during readiness: %v\n%s", err, &p.diag)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-ticker.C:
			}
		}
		raw, err = os.ReadFile(identity)
		if err != nil {
			t.Fatal(err)
		}
		var lifetime local.ServerLifetime
		if err := json.Unmarshal(raw, &lifetime); err != nil {
			t.Fatal(err)
		}
		if lifetime.PID != uint32(cmd.Process.Pid) || lifetime.NamespaceDevice == 0 || lifetime.StartTicks == 0 {
			t.Fatal("actual exec identity not strict")
		}
		return p, endpoint, lifetime
	}
	old, endpoint, original := start("old", false)
	source := filepath.Join(directory, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "seed.txt"), []byte("stopped reader seed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	refPath := filepath.Join(directory, "ref.json")
	refJSON := []byte(run(endpoint, "artifacts", "upload", source))
	if err := os.WriteFile(refPath, refJSON, 0600); err != nil {
		t.Fatal(err)
	}
	ref := new(v1.ArtifactRef)
	if err := protojson.Unmarshal(refJSON, ref); err != nil {
		t.Fatal(err)
	}
	client := skyv1connect.NewControlServiceClient(http.DefaultClient, endpoint)
	begin := connect.NewRequest(&v1.BeginArtifactDownloadRequest{Reference: ref})
	begin.Header().Set("Authorization", "Bearer coverage-token")
	opened, err := client.BeginArtifactDownload(ctx, begin)
	if err != nil {
		t.Fatal(err)
	}
	oldID := opened.Msg.Id
	read := connect.NewRequest(&v1.ReadArtifactChunkRequest{DownloadId: oldID, Limit: 64})
	read.Header().Set("Authorization", "Bearer coverage-token")
	chunk, err := client.ReadArtifactChunk(ctx, read)
	if err != nil || len(chunk.Msg.GetData()) == 0 {
		t.Fatalf("original cached reader: %v", err)
	}
	current, err := local.CaptureServerLifetime()
	if err != nil {
		frame.hold(nil, err)
		t.Fatal(err)
	}
	if stopped, err := local.ObserveServerStopped(current, original); stopped || err != nil {
		frame.hold(nil, err)
		t.Fatalf("live original observer = %v, %v", stopped, err)
	}
	if !stop(old, os.Kill) {
		return
	}
	actor, err := auth.NewLegacy("coverage-token").Authenticate("Bearer coverage-token")
	if err != nil {
		t.Fatal(err)
	}
	inspect := func() (store.ArtifactDownloadClosure, bool, store.ArtifactReservations) {
		t.Helper()
		s, err := store.Open(data)
		if err != nil {
			frame.hold(nil, err)
			t.Fatal(err)
		}
		frame.stores = append(frame.stores, s)
		row, found, queryErr := s.NextArtifactReaderRecovery("")
		facts, factsErr := s.ArtifactReservations()
		if found && queryErr == nil {
			lease, err := s.GetArtifactDownload(actor, row.LeaseID)
			if err != nil {
				queryErr = err
			} else if lease.File != row.File || lease.RootID != row.RootID || lease.Reference != row.Ref || lease.Producer != row.Producer {
				queryErr = errors.New("authorized reader identity differs from cleanup snapshot")
			}
			next, nextFound, err := s.NextArtifactReaderRecovery(row.LeaseID)
			if err != nil || !nextFound || next != row {
				queryErr = errors.New("expected exactly one charged original reader")
			}
		}
		if !found && queryErr == nil {
			if _, err := s.GetArtifactDownload(actor, oldID); !errors.Is(err, store.ErrArtifactExpired) {
				queryErr = errors.New("original row did not retain closed/closing semantics")
			}
		}
		closeErr := s.Close()
		if closeErr != nil {
			frame.hold(s, closeErr)
			t.Fatal(closeErr)
		}
		if queryErr != nil || factsErr != nil {
			t.Fatalf("supported stopped snapshot: %v %v", queryErr, factsErr)
		}
		return row, found, facts
	}
	before, found, facts := inspect()
	if !found || before.LeaseID != oldID || before.Owner != actor.Principal() || before.Ref.ID != ref.Id || before.Ref.SHA256 != ref.Sha256 || before.Ref.Size != uint64(ref.Size) || before.File.Device == 0 || before.File.Inode == 0 || before.RootID != facts.Installation.ID || before.Producer != store.OriginalServerLifetime(original) {
		t.Fatalf("original durable identity differs: %+v", before)
	}
	if !time.Now().Before(opened.Msg.ExpiresAt.AsTime()) {
		t.Fatal("original lease expired before recovery test")
	}
	baseline, _, _ := start("default-off", false)
	if !stop(baseline, os.Interrupt) {
		return
	}
	again, found, baselineFacts := inspect()
	if !found || again != before || baselineFacts.Digest != facts.Digest {
		t.Fatal("default-off startup settled or changed original reader")
	}
	fresh, endpoint, _ := start("recovery", true)
	client = skyv1connect.NewControlServiceClient(http.DefaultClient, endpoint)
	read = connect.NewRequest(&v1.ReadArtifactChunkRequest{DownloadId: oldID, Limit: 64})
	read.Header().Set("Authorization", "Bearer coverage-token")
	if _, err := client.ReadArtifactChunk(ctx, read); err == nil {
		t.Fatal("fresh server adopted old cached handle")
	} else if code := connect.CodeOf(err); code != connect.CodeUnavailable && code != connect.CodeNotFound && code != connect.CodeFailedPrecondition {
		t.Fatalf("old handle refusal was not a semantic response: %v", err)
	}
	closeOld := connect.NewRequest(&v1.CloseArtifactDownloadRequest{DownloadId: oldID})
	closeOld.Header().Set("Authorization", "Bearer coverage-token")
	if _, err := client.CloseArtifactDownload(ctx, closeOld); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("old handle close = %v", err)
	}
	var ids []string
	for len(ids) < 32 {
		begin := connect.NewRequest(&v1.BeginArtifactDownloadRequest{Reference: ref})
		begin.Header().Set("Authorization", "Bearer coverage-token")
		opened, err := client.BeginArtifactDownload(ctx, begin)
		if err != nil {
			if connect.CodeOf(err) != connect.CodeResourceExhausted {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if opened.Msg.Id == oldID || !proto.Equal(opened.Msg.Reference, ref) {
			t.Fatal("fresh authorized lease adopted old identity or changed ref")
		}
		ids = append(ids, opened.Msg.Id)
	}
	// The old unexpired charge would make the 32nd admission fail. No clock or
	// receipt is injected: the production worker must independently settle it.
	if !time.Now().Before(opened.Msg.ExpiresAt.AsTime()) {
		t.Fatal("original expiry confounded charge release")
	}
	read = connect.NewRequest(&v1.ReadArtifactChunkRequest{DownloadId: ids[0], Limit: 64})
	read.Header().Set("Authorization", "Bearer coverage-token")
	freshChunk, err := client.ReadArtifactChunk(ctx, read)
	if err != nil || !bytes.Equal(freshChunk.Msg.GetData(), chunk.Msg.GetData()) {
		t.Fatalf("fresh reader archive bytes changed: %v", err)
	}
	for _, id := range ids {
		r := connect.NewRequest(&v1.CloseArtifactDownloadRequest{DownloadId: id})
		r.Header().Set("Authorization", "Bearer coverage-token")
		if _, err := client.CloseArtifactDownload(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(directory, "fresh-download")
	run(endpoint, "artifacts", "download", "--ref", refPath, destination)
	seed, err := os.ReadFile(filepath.Join(destination, "seed.txt"))
	if err != nil || string(seed) != "stopped reader seed\n" {
		t.Fatalf("fresh extracted content differs: %q, %v", seed, err)
	}
	if !stop(fresh, os.Interrupt) {
		return
	}
	_, found, finalFacts := inspect()
	if found || finalFacts.Digest != facts.Digest {
		t.Fatal("reader charge remained or object/root physical identity changed")
	}
	t.Logf("original exact lease %s owner %s ref %s File %+v root %s explicitly settled; new readers preserved bytes", oldID, before.Owner, before.Ref.ID, before.File, before.RootID)
	// Native target/kernel/deployment qualification remains a separate parent gate.
}
