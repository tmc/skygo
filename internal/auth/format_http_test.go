package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// admissionBackend validates the finite-job shape without executing workloads.
// The real control plane, store and authorization engine handle every request.
type admissionBackend struct{}

func (admissionBackend) Check(backend.Task) error { return nil }
func (admissionBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected execution")
}
func (admissionBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observation")
}
func (admissionBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancellation")
}
func (admissionBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected log read")
}

func TestMixedCredentialHTTPIdentity(t *testing.T) {
	// safeParent rejects a credential parent with group or other write.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	data, err := os.ReadFile("testdata/prior-auth.json")
	if err != nil {
		t.Fatal(err)
	}
	// Bind the public old-issuer fixture to this real test installation and
	// validity window. Preserve its issued ID, principal, secret and digest.
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg["installation"], err = json.Marshal(s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	var tokens []map[string]json.RawMessage
	if err := json.Unmarshal(cfg["tokens"], &tokens); err != nil || len(tokens) != 1 {
		t.Fatal("invalid prior fixture")
	}
	var oldID string
	if err := json.Unmarshal(tokens[0]["id"], &oldID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tokens[0]["created_at"], _ = json.Marshal(now.Add(-time.Minute))
	tokens[0]["expires_at"], _ = json.Marshal(now.Add(time.Hour))
	cfg["tokens"], err = json.Marshal(tokens)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	oldBytes, err := os.ReadFile("testdata/prior.token")
	if err != nil {
		t.Fatal(err)
	}
	oldToken := strings.TrimSuffix(string(oldBytes), "\n")
	output := filepath.Join(dir, "replacement.token")
	if _, err := auth.Issue(path, "legacy", output, time.Hour); err != nil {
		t.Fatal(err)
	}
	newBytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	newToken := strings.TrimSuffix(string(newBytes), "\n")
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	start := func() (skyv1connect.ControlServiceClient, func()) {
		t.Helper()
		e, err := auth.NewFile(path, s.Owner())
		if err != nil {
			t.Fatal(err)
		}
		c, err := control.New(s, []backend.Target{{Name: "test", Kind: "test", Binding: "test-route", Backend: admissionBackend{}}}, e)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(rpc.NewHandler(c, e))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); e.Run(ctx) }()
		var once sync.Once
		stop := func() { once.Do(func() { server.Close(); cancel(); <-done }) }
		t.Cleanup(stop)
		return skyv1connect.NewControlServiceClient(server.Client(), server.URL), stop
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, stop := start()
	submit := func(token string) string {
		t.Helper()
		r := connect.NewRequest(&v1.SubmitJobRequest{RequestId: "stable", Task: &v1.Task{Argv: []string{"unused"}}})
		r.Header().Set("Authorization", "Bearer "+token)
		response, err := client.SubmitJob(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		return response.Msg.Job.Id
	}
	id := submit(oldToken)
	if submit(newToken) != id {
		t.Fatal("token format changed the principal's request identity")
	}
	get := func(token string) (*connect.Response[v1.GetJobResponse], error) {
		r := connect.NewRequest(&v1.GetJobRequest{Id: id})
		r.Header().Set("Authorization", "Bearer "+token)
		return client.GetJob(ctx, r)
	}
	if err := auth.Revoke(path, oldID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := get(oldToken)
		if connect.CodeOf(err) == connect.CodeUnauthenticated {
			if time.Now().After(deadline) {
				t.Fatal("old credential revocation exceeded two seconds")
			}
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatal("old credential revocation did not reach HTTP within two seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, err := get(newToken)
	if err != nil || current.Msg.Job.CancelRequested || current.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED {
		t.Fatal("revocation changed accepted work or replacement access")
	}
	stop()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	client, _ = start()
	if submit(newToken) != id {
		t.Fatal("restart changed mixed-format retry identity")
	}
	if _, err := get(oldToken); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatal("restart revived revoked prior credential")
	}
}
