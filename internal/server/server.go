package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// Config selects durable storage, HTTP exposure, and execution targets.
type Config struct {
	DisableLocal bool

	// Artifacts is an optional actual marker-verified service installed while
	// stopped. The caller owns Files and its borrowed root. HTTP completion
	// alone does not prove its descriptors or Control operations have drained.
	// No endpoint or target workspace capability follows from this pointer.
	Artifacts *artifact.Files

	// RecoverStoppedArtifactReaders acknowledges that ALL historical producers
	// had no surviving descriptor copy/use and the same-domain kernel is qualified.
	// It is restricted to isolated artifact-only/no-child/no-transfer deployments.
	RecoverStoppedArtifactReaders bool

	// ArtifactDir opens an existing initialized private root matching Store.
	// It is mutually exclusive with Artifacts and never initializes a root.
	// Only this owned route configures the built-in local workspace target.
	ArtifactDir                                   string
	DataDir, Listen, Token, AuthFile, TargetFacts string

	// Targets constructs additional targets using the persistent installation ID.
	// It is called once, after the store opens and before the server listens.
	// A nil function adds no targets. Targets must not require a Close call.
	Targets func(owner string) ([]backend.Target, error)
}

// Run serves until cancellation, rejects new work before draining, and closes
// local processes. Kubernetes workloads remain independent of server lifetime.
func Run(ctx context.Context, cfg Config) error {
	frame := &serverRun{}
	if err := frame.start(ctx, cfg); err != nil {
		return frame.closeStartup(err)
	}
	var cause error
	select {
	case <-ctx.Done():
	case <-frame.runDone:
		cause = frame.runErr
		if cause == nil {
			cause = errors.New("control returned before shutdown")
		}
	case <-frame.serveDone:
		cause = frame.serveErr
		if errors.Is(cause, http.ErrServerClosed) {
			cause = nil
		}
	}
	return frame.shutdown(cause)
}

// start publishes all original resources into one frame before the next stage.
// No caller cancellation is inherited by Run, authentication or HTTP handlers.
func (f *serverRun) start(ctx context.Context, cfg Config) error {
	if cfg.RecoverStoppedArtifactReaders && cfg.ArtifactDir == "" && cfg.Artifacts == nil {
		return errors.New("stopped artifact reader recovery requires artifacts")
	}
	if cfg.ArtifactDir != "" && cfg.Artifacts != nil {
		return errors.New("artifact directory conflicts with borrowed files")
	}
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
	var facts map[string]*backend.TargetAdvice
	if cfg.TargetFacts != "" {
		facts, err = readTargetFacts(cfg.TargetFacts)
		if err != nil {
			return err
		}
	}
	f.cfg = cfg
	if cfg.ArtifactDir != "" || cfg.Artifacts != nil {
		f.lifetime, f.captureErr = local.CaptureServerLifetime()
		if err := acceptServerLifetime(f.lifetime, f.captureErr); err != nil {
			return err
		}
	}
	if cfg.RecoverStoppedArtifactReaders && (f.captureErr != nil || f.lifetime.NamespaceDevice == 0 || f.lifetime.StartTicks == 0) {
		return errors.New("stopped artifact reader recovery requires a strict known server lifetime")
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		// Store returns no original on failure. Preserve its whole error chain;
		// dependency-hidden cleanup/lock state supplies no retry authority.
		return f.hold("state acquisition failed", err)
	}
	f.store = s
	f.files = cfg.Artifacts
	if cfg.ArtifactDir != "" {
		if err := f.openArtifacts(cfg.ArtifactDir); err != nil {
			return err
		}
	}
	if f.files != nil {
		if err := f.preflightArtifacts(); err != nil {
			return err
		}
	}
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
	f.engine = engine
	var targets []backend.Target
	var localBinding string
	var localOutput control.LocalOutputConfig
	policy, retentionEnabled, err := s.RetentionPolicy()
	if err != nil {
		return fmt.Errorf("read retention policy: %w", err)
	}
	if retentionEnabled && !policy.LocalOutputEnabled && !cfg.DisableLocal {
		return errors.New("local output is disabled by retention policy; use --disable-local with remote targets")
	}
	if !cfg.DisableLocal {
		localBinding, err = backend.LocalBinding(s.Owner())
		if err != nil {
			return fmt.Errorf("local binding: %w", err)
		}
		logs := filepath.Join(cfg.DataDir, "logs")
		var localBackend *local.Backend
		if retentionEnabled {
			outputs, err := retainedOutputs(s)
			if err != nil {
				return fmt.Errorf("load local output reservations: %w", err)
			}
			localBackend, err = local.NewRetained(logs, local.RetentionConfig{
				Owner: s.Owner(), OutputUnit: policy.OutputUnit, Volume: policy.Volume,
				OutputBytes: policy.OutputBytes, OutputDomains: policy.OutputDomains,
				OutputFiles: policy.OutputFiles, Outputs: outputs,
			})
			if err != nil {
				return fmt.Errorf("configure local retention: %w", err)
			}
			localOutput = control.LocalOutputConfig{
				Target: "local", Binding: localBinding, BindOutput: localBackend.BindOutput,
				OutputUsage: func(ctx context.Context) (uint64, uint64, error) {
					usage, err := localBackend.OutputUsage(ctx)
					return usage.AllocatedBytes, usage.Files, err
				},
			}
		} else {
			localBackend, err = local.New(logs)
			if err != nil {
				return err
			}
		}
		f.localBackend = localBackend
		target := backend.Target{Name: "local", Kind: "local", Backend: localBackend, Binding: localBinding, SupportsRetry: localBackend.SupportsRetry()}
		if retentionEnabled {
			target.OutputStorage = backend.LocalOutput
			target.OutputReservation = localBackend.OutputReservation()
			target.Retention = localBackend.Retention()
		}
		targets = append(targets, target)
	}
	if cfg.Targets != nil {
		extra, err := cfg.Targets(s.Owner())
		if err != nil {
			return fmt.Errorf("configure targets: %w", err)
		}
		targets = append(targets, extra...)
	}
	f.targets = targets
	if err := attachTargetFacts(facts, targets); err != nil {
		return err
	}
	config := control.Config{RecoverStoppedArtifactReaders: cfg.RecoverStoppedArtifactReaders, LocalBackend: f.localBackend, ServerLifetime: f.lifetime, LocalOutput: localOutput, Artifacts: f.files}
	if f.root != nil && f.localBackend != nil {
		config.LocalWorkspace = &control.LocalWorkspaceConfig{Target: "local", Binding: localBinding, Backend: f.localBackend, Roots: f.root}
	}
	c, err := control.NewWithConfig(s, targets, engine, config)
	if err != nil {
		return err
	}
	f.control = c
	// Only the built-in backend constructed above authorizes legacy local binding.
	if localBinding != "" {
		if err := s.BindLegacyLocal(localBinding); err != nil {
			return fmt.Errorf("bind legacy local jobs: %w", err)
		}
	}
	if retentionEnabled {
		for _, target := range targets {
			if target.OutputStorage == backend.UnspecifiedOutput {
				continue
			}
			if err := s.BindOutputStorage(target.Name, target.Binding, target.OutputStorage, target.OutputReservation); err != nil {
				return fmt.Errorf("bind output storage for %s: %w", target.Name, err)
			}
		}
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	f.listener = &serverListener{Listener: listener}
	ready := &f.ready
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
		if !ready.Load() || !c.Healthy() || !engine.Ready() || !c.StorageReady() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	rpcHTTP := rpc.NewHandler(c, engine)
	f.rpcHTTP = rpcHTTP
	var api http.Handler = rpcHTTP
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() || !c.Healthy() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		api.ServeHTTP(w, r)
	}))
	f.outer = newArtifactHTTPDrain(mux)
	base := context.WithoutCancel(ctx)
	runCtx, cancelRun := context.WithCancel(base)
	authCtx, cancelAuth := context.WithCancel(base)
	f.cancelRun, f.cancelAuth = cancelRun, cancelAuth
	f.authDone = make(chan struct{})
	if cfg.AuthFile != "" {
		go func() { defer close(f.authDone); engine.Run(authCtx) }()
	} else {
		close(f.authDone)
	}
	f.http = &http.Server{Handler: f.outer, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return base }}
	f.runDone, f.serveDone = make(chan struct{}), make(chan struct{})
	go func() { defer close(f.runDone); f.runErr = c.Run(runCtx) }()
	ready.Store(true)
	go func() { defer close(f.serveDone); f.serveErr = f.http.Serve(f.listener) }()
	return nil
}

func retainedOutputs(s *store.Store) ([]backend.OutputReference, error) {
	var outputs []backend.OutputReference
	var after string
	for {
		page, next, err := s.ListOutputReferences(after, 100)
		if err != nil {
			return nil, err
		}
		if len(outputs)+len(page) > 256 {
			return nil, errors.New("local output inventory exceeds domain limit")
		}
		outputs = append(outputs, page...)
		if next == "" {
			return outputs, nil
		}
		if len(page) == 0 || next <= after || next != page[len(page)-1].AttemptID {
			return nil, errors.New("local output inventory did not advance")
		}
		after = next
	}
}
