// Package server assembles the singleton control plane and its HTTP lifecycle.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/kube"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// Config selects durable storage, HTTP exposure and an optional Kubernetes target.
type Config struct {
	DisableLocal                                           bool
	DataDir, Listen, Kubeconfig, Context, Namespace, Token string
}

// Run serves until cancellation, rejects new work before draining, and closes
// local processes. Kubernetes workloads remain independent of server lifetime.
func Run(ctx context.Context, cfg Config) (result error) {
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if cfg.Token == "" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("non-loopback listener requires a bearer token; use a trusted network or TLS proxy")
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	var targets []backend.Target
	if !cfg.DisableLocal {
		localBackend, err := local.New(filepath.Join(cfg.DataDir, "logs"))
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, localBackend.Close()) }()
		targets = append(targets, backend.Target{Name: "local", Kind: "local", Backend: localBackend})
	}
	if cfg.Namespace != "" {
		k, err := kube.FromConfig(cfg.Kubeconfig, cfg.Context, cfg.Namespace, s.Owner())
		if err != nil {
			return err
		}
		targets = append(targets, backend.Target{Name: "kubernetes", Kind: "kubernetes", Backend: k})
	} else if cfg.Kubeconfig != "" || cfg.Context != "" {
		return errors.New("namespace is required for a Kubernetes target")
	}
	c, err := control.New(s, targets)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/startup", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		if !c.Healthy() {
			http.Error(w, "durable reconciliation failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() || !c.Healthy() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	api := rpc.NewHandler(c, cfg.Token)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() || !c.Healthy() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		api.ServeHTTP(w, r)
	}))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return runCtx }}
	reconciled := make(chan error, 1)
	runDone := make(chan struct{})
	served := make(chan error, 1)
	go func() { defer close(runDone); reconciled <- c.Run(runCtx) }()
	ready.Store(true)
	go func() { served <- srv.Serve(listener) }()
	select {
	case <-ctx.Done():
	case err := <-reconciled:
		if ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
			result = err
		}
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	ready.Store(false)
	cancel()
	drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := srv.Shutdown(drain); err != nil {
		_ = srv.Close()
		if result == nil {
			result = err
		}
	}
	// Canceling runCtx bounds backend I/O; await the loop before closing its store.
	select {
	case <-runDone:
	case <-time.After(11 * time.Second):
		if result == nil {
			result = errors.New("reconciler shutdown timed out")
		}
	}
	return result
}
