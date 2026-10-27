//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
)

// This release oracle requires the same source-qualified executable as CLI
// scripts. It skips only when both variables are unset and
// SKYGO_REQUIRE_FIXTURES is not set. Hash identity is checked here; source
// provenance is a separate parent receipt. No executable is built or
// substituted inside the test.
func artifactHTTPCLIExecutable(t *testing.T) string {
	t.Helper()
	path, want := os.Getenv("SKYGO_ARTIFACT_BINARY"), os.Getenv("SKYGO_ARTIFACT_BINARY_SHA256")
	if path == "" && want == "" && os.Getenv("SKYGO_REQUIRE_FIXTURES") == "" {
		t.Skip("requires SKYGO_ARTIFACT_BINARY and SKYGO_ARTIFACT_BINARY_SHA256 (set SKYGO_REQUIRE_FIXTURES=1 to fail instead)")
	}
	if !filepath.IsAbs(path) || want == "" {
		t.Fatal("mandatory real-CLI HTTP oracle requires SKYGO_ARTIFACT_BINARY and SKYGO_ARTIFACT_BINARY_SHA256")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatal("CLI fixture is not an executable regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Fatalf("CLI fixture SHA256=%s want=%s", got, want)
	}
	t.Logf("real CLI fixture SHA256 %s; source provenance requires separate receipt", want)
	return path
}

// UNRUN source oracle: real external CLI -> production HTTP -> actual original
// Control.Run worker, Store and Files. The wrapper only observes completed real
// handlers. It neither manufactures a cold refusal nor grants capacity.
func TestArtifactCLIColdBeginAndFinalizeThroughProductionHTTP(t *testing.T) {
	exe := artifactHTTPCLIExecutable(t)
	var mu sync.Mutex
	counts := make(map[string]int)
	cold := make(chan struct{}, 1)
	var f *artifactRPCFixture
	var firstFinalizeErr error
	var firstFinalizeState string
	f = artifactRPC(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			mu.Lock()
			defer mu.Unlock()
			counts[r.URL.Path]++
			if r.URL.Path == skyv1connect.ControlServiceBeginArtifactUploadProcedure && counts[r.URL.Path] == 1 && len(w.Header().Values(artifactBeginNotStartedHeader)) == 1 && w.Header().Get(artifactBeginNotStartedHeader) == "v1" {
				cold <- struct{}{}
			}
			if r.URL.Path == skyv1connect.ControlServiceFinalizeArtifactUploadProcedure && firstFinalizeState != "finalizing" {
				reservations, err := f.store.ArtifactReservations()
				if err != nil {
					firstFinalizeErr = err
					return
				}
				if len(reservations.Subjects) == 1 && reservations.Subjects[0].Kind == "upload" {
					return
				}
				if len(reservations.Subjects) != 2 || reservations.Subjects[0].Kind != "object" || reservations.Subjects[1].Kind != "upload" || reservations.Subjects[0].ID != reservations.Subjects[1].ID {
					firstFinalizeErr = fmt.Errorf("first Finalize charged subjects=%+v", reservations.Subjects)
					return
				}
				actor, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
				if err != nil {
					firstFinalizeErr = err
					return
				}
				upload, err := f.store.GetArtifactUpload(actor, reservations.Subjects[0].ID)
				firstFinalizeErr, firstFinalizeState = err, upload.State
			}
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := f.directory
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("real production HTTP roundtrip\n"), 5000)
	if err := os.WriteFile(filepath.Join(source, "seed.txt"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(dir, "alice.token")
	if err := os.WriteFile(token, []byte(f.tokens["alice"]+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--endpoint", f.server.URL, "--token-file", token, "--timeout", "15s"}
	command := exec.CommandContext(ctx, exe, append(append([]string{}, base...), "artifacts", "upload", source)...)
	// Explicitly keep subprocess scratch beneath this test's private directory.
	command.Env = append(os.Environ(), "TMPDIR="+dir)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case <-cold:
		f.startSampler(t)
	case err := <-done:
		t.Fatalf("CLI exited before actual cold marker: %v; stderr=%s", err, stderr.String())
	case <-ctx.Done():
		<-done
		t.Fatal("CLI never observed actual cold marker")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CLI upload: %v; stderr=%s", err, stderr.String())
		}
	case <-ctx.Done():
		<-done
		t.Fatal("CLI upload timed out")
	}
	var ref artifact.Ref
	if err := json.Unmarshal(stdout.Bytes(), &ref); err != nil {
		t.Fatalf("CLI Ref JSON: %v; stdout=%s", err, stdout.String())
	}
	if err := ref.Validate(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "upload-id: "+ref.ID) {
		t.Fatalf("CLI omitted exact durable resume ID: %s", stderr.String())
	}
	mu.Lock()
	beginCalls, finalizeCalls := counts[skyv1connect.ControlServiceBeginArtifactUploadProcedure], counts[skyv1connect.ControlServiceFinalizeArtifactUploadProcedure]
	firstErr, firstState := firstFinalizeErr, firstFinalizeState
	mu.Unlock()
	if beginCalls < 2 || beginCalls > 21 || finalizeCalls < 2 || finalizeCalls > 21 {
		t.Fatalf("real retry counts Begin=%d Finalize=%d", beginCalls, finalizeCalls)
	}
	if firstErr != nil || firstState != "finalizing" {
		t.Fatalf("no real Finalize retained durable finalizing: %s %v", firstState, firstErr)
	}
	reservations, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations.Subjects) != 2 || reservations.Subjects[0].Kind != "object" || reservations.Subjects[1].Kind != "upload" || reservations.Subjects[0].ID != ref.ID || reservations.Subjects[1].ID != ref.ID {
		t.Fatalf("CLI allocated additional durable identity: %+v", reservations.Subjects)
	}
	reference := filepath.Join(dir, "ref.json")
	if err := os.WriteFile(reference, stdout.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "downloaded")
	download := exec.CommandContext(ctx, exe, append(append([]string{}, base...), "artifacts", "download", "--ref", reference, destination)...)
	download.Env = append(os.Environ(), "TMPDIR="+dir)
	var downloadOut, downloadErr bytes.Buffer
	download.Stdout, download.Stderr = &downloadOut, &downloadErr
	if err := download.Run(); err != nil {
		t.Fatalf("CLI download: %v; stderr=%s", err, downloadErr.String())
	}
	got, err := os.ReadFile(filepath.Join(destination, "seed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("real extracted CLI payload changed")
	}
}
