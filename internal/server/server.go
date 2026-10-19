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

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// Config selects durable storage, HTTP exposure, and execution targets.
type Config struct {
	DisableLocal                     bool
	DataDir, Listen, Token, AuthFile string

	// Targets constructs additional targets using the persistent installation ID.
	// It is called once, after the store opens and before the server listens.
	// A nil function adds no targets. Targets must not require a Close call.
	Targets func(owner string) ([]backend.Target, error)
}

// Run serves until cancellation, rejects new work before draining, and closes
// local processes. Kubernetes workloads remain independent of server lifetime.
func Run(ctx context.Context, cfg Config) (result error) {
	if cfg.AuthFile != "" && cfg.Token != "" {
		return errors.New("auth file conflicts with legacy token")
	}
	if cfg.DisableLocal && cfg.Targets == nil {
		return errors.New("no execution targets configured")
	}
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if cfg.AuthFile == "" && cfg.Token == "" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("non-loopback listener requires a bearer token; use a trusted network or TLS proxy")
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	if s.Scoped() != (cfg.AuthFile != "") {
		return errors.New("scoped state requires its auth file; activate legacy state with auth init first")
	}
	engine := auth.NewLegacy(cfg.Token)
	if cfg.AuthFile != "" {
		engine, err = auth.NewFile(cfg.AuthFile, s.Owner())
		if err != nil {
			return fmt.Errorf("load authentication: %w", err)
		}
	}
	var targets []backend.Target
	var localBinding string
	if !cfg.DisableLocal {
		localBackend, err := local.New(filepath.Join(cfg.DataDir, "logs"))
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, localBackend.Close()) }()
		localBinding, err = backend.LocalBinding(s.Owner())
		if err != nil {
			return fmt.Errorf("local binding: %w", err)
		}
		targets = append(targets, backend.Target{Name: "local", Kind: "local", Backend: localBackend, Binding: localBinding})
	}
	if cfg.Targets != nil {
		extra, err := cfg.Targets(s.Owner())
		if err != nil {
			return fmt.Errorf("configure targets: %w", err)
		}
		targets = append(targets, extra...)
	}
	c, err := control.New(s, targets, engine)
	if err != nil {
		return err
	}
	// Only the built-in backend constructed above authorizes legacy local binding.
	if localBinding != "" {
		if err := s.BindLegacyLocal(localBinding); err != nil {
			return fmt.Errorf("bind legacy local jobs: %w", err)
		}
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
		if !c.AuditHealthy() {
			w.Header().Set("Skygo-Audit", "unavailable")
		}
		if !ready.Load() || !c.Healthy() || !engine.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	api := rpc.NewHandler(c, engine)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() || !c.Healthy() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		api.ServeHTTP(w, r)
	}))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	authDone := make(chan struct{})
	if cfg.AuthFile != "" {
		go func() { defer close(authDone); engine.Run(runCtx) }()
	} else {
		close(authDone)
	}
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
	select {
	case <-authDone:
	case <-drain.Done():
		if result == nil {
			result = errors.New("authentication refresh shutdown timed out")
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
