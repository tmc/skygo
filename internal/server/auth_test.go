package server_test

import (
	"context"
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
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

func scopedState(t *testing.T) (dir, path, token string) {
	t.Helper()
	root := t.TempDir()
	dir = filepath.Join(root, "state")
	path = filepath.Join(root, "auth.json")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "admin.token")
	if _, err := auth.Initialize(path, output, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	return dir, path, strings.TrimSuffix(string(data), "\n")
}

func TestScopedConfigurationRefusesFallback(t *testing.T) {
	for _, mode := range []string{"missing file", "anonymous", "single token", "conflicting token", "unactivated"} {
		t.Run(mode, func(t *testing.T) {
			dir, path, _ := scopedState(t)
			cfg := server.Config{DataDir: dir, Listen: "127.0.0.1:0", AuthFile: path}
			switch mode {
			case "missing file":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "anonymous":
				cfg.AuthFile = ""
			case "single token":
				cfg.AuthFile = ""
				cfg.Token = "legacy-token"
			case "conflicting token":
				cfg.Token = "legacy-token"
			case "unactivated":
				cfg.DataDir = filepath.Join(t.TempDir(), "state")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := server.Run(ctx, cfg); err == nil {
				t.Fatal("invalid scoped configuration started")
			}
			s, err := store.Open(cfg.DataDir)
			if err != nil {
				t.Fatal("startup leaked store lock", err)
			}
			s.Close()
		})
	}
}

func TestScopedAuthReadinessRecovery(t *testing.T) {
	dir, path, token := scopedState(t)
	// Retain a newer complete configuration for supported recovery after an
	// operator's malformed edit. Authentication must stay closed in between.
	if err := auth.AddPrincipal(path, "reader", auth.Reader); err != nil {
		t.Fatal(err)
	}
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, server.Config{DataDir: dir, Listen: address, AuthFile: path}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(12 * time.Second):
			t.Error("scoped server shutdown exceeded bound")
		}
	})
	client := &http.Client{Timeout: 300 * time.Millisecond}
	base := "http://" + address
	status := func(route string) int {
		r, err := client.Get(base + route)
		if err != nil {
			return 0
		}
		r.Body.Close()
		return r.StatusCode
	}
	wait := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if status("/ready") == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("ready did not become %d", want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(http.StatusOK)
	api := skyv1connect.NewControlServiceClient(client, base)
	read := func() error {
		r := connect.NewRequest(&v1.GetServerInfoRequest{})
		r.Header().Set("Authorization", "Bearer "+token)
		_, err := api.GetServerInfo(context.Background(), r)
		return err
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	// An in-place edit is deliberately invalid operator input, not a supported
	// administration operation. The running server must neither use stale auth
	// indefinitely nor fall back to its legacy mode.
	if err := os.WriteFile(path, []byte("{partial-secret-input"), 0600); err != nil {
		t.Fatal(err)
	}
	wait(http.StatusServiceUnavailable)
	if got := status("/live"); got != http.StatusOK {
		t.Fatalf("auth fault changed liveness to %d", got)
	}
	if err := read(); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("invalid auth snapshot = %v", err)
	}
	if err := os.WriteFile(path, valid, 0600); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(path, "recovered", auth.Reader); err != nil {
		t.Fatal(err)
	}
	wait(http.StatusOK)
	if err := read(); err != nil {
		t.Fatal("valid new snapshot did not restore admission", err)
	}
}
