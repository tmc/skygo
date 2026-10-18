package server_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

func TestWorkload(t *testing.T) {
	if os.Getenv("SKYGO_SERVER_WORKLOAD") != "heartbeat" {
		return
	}
	fmt.Fprintln(os.Stdout, "started")
	for {
		if err := os.WriteFile(os.Getenv("SKYGO_SERVER_HEARTBEAT"), []byte(time.Now().String()), 0600); err != nil {
			os.Exit(2)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type running struct {
	url    string
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func start(t *testing.T, dir, token string) *running {
	t.Helper()
	// Run does not expose its listener. Reserve and release a loopback port;
	// startup below detects the small race with another process binding that port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{url: "http://" + address, cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = server.Run(ctx, server.Config{Listen: address, DataDir: dir, Token: token})
		close(r.done)
	}()
	t.Cleanup(func() {
		r.cancel()
		select {
		case <-r.done:
			if r.err != nil {
				t.Error(r.err)
			}
		case <-time.After(15 * time.Second):
			t.Error("server did not shut down")
		}
	})
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(8 * time.Second)
	for {
		select {
		case <-r.done:
			t.Fatalf("server exited during startup: %v", r.err)
		default:
		}
		response, err := client.Get(r.url + "/ready")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return r
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func stop(t *testing.T, r *running) {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
		if r.err != nil {
			t.Fatal(r.err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("server shutdown exceeded bound")
	}
}

func TestHealthAndAuthentication(t *testing.T) {
	r := start(t, t.TempDir(), "secret")
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/startup", "/ready", "/live"} {
		response, err := client.Get(r.url + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Errorf("%s returned %d", path, response.StatusCode)
		}
	}
	api := skyv1connect.NewControlServiceClient(client, r.url)
	_, err := api.ListTargets(context.Background(), connect.NewRequest(&v1.ListTargetsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("unauthenticated call = %v", err)
	}
	request := connect.NewRequest(&v1.ListTargetsRequest{})
	request.Header().Set("Authorization", "Bearer secret")
	response, err := api.ListTargets(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Msg.Targets) != 1 || response.Msg.Targets[0].Name != "local" {
		t.Fatalf("targets = %v", response.Msg.Targets)
	}
	stop(t, r)
	if response, err := client.Get(r.url + "/ready"); err == nil {
		response.Body.Close()
		t.Fatal("listener remained available after shutdown")
	}
}

func TestConfigurationErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  server.Config
		want string
	}{
		{"no targets", server.Config{Listen: "127.0.0.1:0", DisableLocal: true}, "store and targets required"},
		{"public without token", server.Config{Listen: "0.0.0.0:0"}, "requires a bearer token"},
		{"hostname without token", server.Config{Listen: "localhost:0"}, "requires a bearer token"},
		{"invalid listen", server.Config{Listen: "invalid"}, "listen address"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.DataDir = t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := server.Run(ctx, tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run error = %v; want %q", err, tt.want)
			}
			// Any startup failure must release the persistent store's singleton lock.
			s, err := store.Open(tt.cfg.DataDir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dir := t.TempDir()
	err = server.Run(context.Background(), server.Config{Listen: listener.Addr().String(), DataDir: dir})
	if err == nil || !strings.Contains(err.Error(), "listen:") {
		t.Fatalf("Run error = %v", err)
	}
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
}

func TestShutdownWithWorkloadAndStream(t *testing.T) {
	dir := t.TempDir()
	r := start(t, dir, "")
	heartbeat := filepath.Join(t.TempDir(), "heartbeat")
	api := skyv1connect.NewControlServiceClient(&http.Client{}, r.url)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	task := &v1.Task{Argv: []string{os.Args[0], "-test.run=^TestWorkload$"}, Env: map[string]string{"SKYGO_SERVER_WORKLOAD": "heartbeat", "SKYGO_SERVER_HEARTBEAT": heartbeat}}
	accepted, err := api.SubmitJob(ctx, connect.NewRequest(&v1.SubmitJobRequest{RequestId: "shutdown", Task: task}))
	if err != nil {
		t.Fatal(err)
	}
	id := accepted.Msg.Job.Id
	stream, err := api.StreamJobLogs(ctx, connect.NewRequest(&v1.StreamJobLogsRequest{Id: id, Follow: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var output []byte
	for stream.Receive() {
		output = append(output, stream.Msg().Data...)
		if strings.Contains(string(output), "started\n") {
			break
		}
	}
	if !strings.Contains(string(output), "started\n") {
		t.Fatalf("workload did not start: %v", stream.Err())
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(heartbeat); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat not created")
		}
		time.Sleep(10 * time.Millisecond)
	}
	streamDone := make(chan error, 1)
	go func() {
		for stream.Receive() {
		}
		streamDone <- stream.Err()
	}()
	stop(t, r)
	select {
	case <-streamDone:
	case <-time.After(time.Second):
		t.Fatal("active log stream survived shutdown")
	}
	before, err := os.ReadFile(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("local workload survived shutdown")
	}
	// The store is closed only after the reconciliation goroutine stops.
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(id); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh server can recover the durable acceptance. It must not respawn it.
	restarted := start(t, dir, "")
	api = skyv1connect.NewControlServiceClient(&http.Client{Timeout: time.Second}, restarted.url)
	deadline = time.Now().Add(3 * time.Second)
	for {
		response, err := api.GetJob(context.Background(), connect.NewRequest(&v1.GetJobRequest{Id: id}))
		if err != nil {
			t.Fatal(err)
		}
		if response.Msg.Job.State == v1.JobState_JOB_STATE_UNKNOWN {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restart did not report unknown execution: %v", response.Msg.Job)
		}
		time.Sleep(10 * time.Millisecond)
	}
	after, err = os.ReadFile(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("restart respawned unknown execution")
	}
}

func TestCanceledStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, server.Config{Listen: "127.0.0.1:0", DataDir: dir}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled startup did not stop")
	}
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
}

func TestTargetInitialization(t *testing.T) {
	dir := t.TempDir()
	var owner string
	sentinel := errors.New("target initialization failed")
	for i := 0; i < 2; i++ {
		calls := 0
		err := server.Run(context.Background(), server.Config{
			DataDir: dir, Listen: "127.0.0.1:0", DisableLocal: true,
			Targets: func(id string) ([]backend.Target, error) {
				calls++
				if id == "" {
					t.Fatal("empty installation identity")
				}
				if owner != "" && id != owner {
					t.Fatalf("installation identity changed: %q != %q", id, owner)
				}
				owner = id
				return nil, sentinel
			},
		})
		if !errors.Is(err, sentinel) || calls != 1 {
			t.Fatalf("Run = %v, calls = %d", err, calls)
		}
	}
	// Both failed starts released the store, preserving its identity.
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Owner() != owner {
		t.Fatal("factory did not receive durable identity")
	}
}
