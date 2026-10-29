package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// Config selects durable storage, HTTP exposure, and execution targets.
type Config struct {
	DisableLocal bool

	// DurableLocal runs plain local jobs (no workspace, no retry policy)
	// under detached supervisors that outlive the server; a restarted server
	// reattaches to them. LocalSupervisor is the absolute path of a binary
	// whose main dispatches local.SupervisorCommand. Durable execution is
	// not used when output retention is enabled: retained output keeps the
	// server-owned execution path and its existing restart semantics.
	DurableLocal    bool
	LocalSupervisor string

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

	// OIDCFile enables ID tokens with AuthFile. OIDCJWKSURL is the one
	// HTTPS key set URL that file may name; empty permits only a key file.
	OIDCFile, OIDCJWKSURL string

	// ExistingInstallation starts only an existing, current, scoped
	// installation with installed artifacts; it never creates, initializes
	// or migrates state. It requires AuthFile and ArtifactDir and conflicts
	// with Artifacts and RecoverStoppedArtifactReaders. The installation's
	// read-only facts bind the auth file owner and the artifact root before
	// the writer lock is taken, and the writer is accepted only if they
	// still hold. Every other writer must be stopped: raw same-user edits of
	// the auth file, root or database outside bbolt's lock are not detected.
	ExistingInstallation bool

	// LocalTargetName names the built-in local target; empty means "local".
	// It must be a lowercase DNS name of at most 63 bytes. Jobs record the
	// name, so changing it for an existing DataDir leaves earlier jobs naming
	// the previous target. The local binding does not depend on the name.
	LocalTargetName string

	// TLSConfig, if not nil, serves HTTPS on the owned listener with a clone
	// of the configuration, which must supply a certificate. Nil serves HTTP.
	TLSConfig *tls.Config

	// Targets constructs additional targets using the persistent installation ID.
	// It is called once, after the store opens and before the server listens.
	// A nil function adds no targets. Targets must not require a Close call.
	Targets func(owner string) ([]backend.Target, error)

	// Install, if set, constructs the one Integration from the open Store
	// and authentication Engine. It is called once, after authentication
	// loads and before Targets. An Integration returned with an error is
	// still closed before anything it depends on.
	Install func(*store.Store, *auth.Engine) (Integration, error)
}

// Run serves until cancellation and rejects new work before draining. It stops
// server-owned local processes; configured durable local jobs detach instead.
// Kubernetes workloads remain independent of server lifetime.
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
	case <-frame.integrationDone:
		cause = frame.integrationErr
		if cause == nil {
			cause = errors.New("integration returned before shutdown")
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
	if cfg.OIDCFile != "" && cfg.AuthFile == "" || cfg.OIDCJWKSURL != "" && cfg.OIDCFile == "" {
		return errors.New("oidc authentication requires an auth file and an oidc file")
	}
	if cfg.ExistingInstallation && (cfg.AuthFile == "" || cfg.ArtifactDir == "" || cfg.Artifacts != nil || cfg.RecoverStoppedArtifactReaders) {
		return errors.New("existing installation requires an auth file and artifact directory, without borrowed files or reader recovery")
	}
	if cfg.DisableLocal && cfg.Targets == nil && cfg.Install == nil {
		return errors.New("no execution targets configured")
	}
	localName, err := localTargetName(cfg)
	if err != nil {
		return err
	}
	if t := cfg.TLSConfig; t != nil && len(t.Certificates) == 0 && t.GetCertificate == nil && t.GetConfigForClient == nil {
		return errors.New("tls configuration has no certificate")
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
	if cfg.ExistingInstallation {
		err = f.openExisting(cfg)
	} else {
		err = f.openState(cfg)
	}
	if err != nil {
		return err
	}
	s, engine := f.store, f.engine
	if cfg.Install != nil {
		in, err := cfg.Install(s, engine)
		f.integration = in
		if err != nil {
			return fmt.Errorf("install integration: %w", err)
		}
	}
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
				Target: localName, Binding: localBinding, BindOutput: localBackend.BindOutput,
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
			if cfg.DurableLocal {
				err = localBackend.EnableDurable(local.DurableConfig{State: filepath.Join(cfg.DataDir, "local-durable"), Executable: cfg.LocalSupervisor})
				if err != nil {
					return fmt.Errorf("configure durable local execution: %w", err)
				}
			}
		}
		f.localBackend = localBackend
		target := backend.Target{Name: localName, Kind: "local", Backend: localBackend, Binding: localBinding, SupportsRetry: localBackend.SupportsRetry()}
		if names, err := localBackend.AcceleratorNames(ctx); err == nil {
			target.AcceleratorNames = names
		}
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
		if environmentTargets(extra) {
			return errors.New("configure targets: environment support requires an environment integration")
		}
		targets = append(targets, extra...)
	}
	// The integration's targets, read once: Targets is called exactly once
	// per startup, and every later decision uses this slice.
	var installed []backend.Target
	if f.integration != nil {
		extra := f.integration.Targets()
		installed = extra
		if _, ok := f.integration.(EnvironmentProvider); !ok && environmentTargets(extra) {
			return errors.New("install integration: environment support requires an environment integration")
		}
		for _, t := range extra {
			if t.SupportsEnvironmentArtifacts && !t.SupportsPersistentEnvironments {
				return errors.New("install integration: environment artifacts require persistent environments")
			}
		}
		targets = append(targets, extra...)
	}
	f.targets = targets
	if err := attachTargetFacts(facts, targets); err != nil {
		return err
	}
	config := control.Config{RecoverStoppedArtifactReaders: cfg.RecoverStoppedArtifactReaders, LocalBackend: f.localBackend, ServerLifetime: f.lifetime, LocalOutput: localOutput, Artifacts: f.files}
	if f.root != nil && f.localBackend != nil {
		config.LocalWorkspace = &control.LocalWorkspaceConfig{Target: localName, Binding: localBinding, Backend: f.localBackend, Roots: f.root}
	}
	c, err := control.NewWithConfig(s, targets, engine, config)
	if err != nil {
		return err
	}
	f.control = c
	if _, workers := f.integration.(WorkerProvider); workers {
		_, environments := f.integration.(EnvironmentProvider)
		_, services := f.integration.(ServingProvider)
		if environments || services {
			return errors.New("install integration: workers conflict with environments and services")
		}
	}
	if binder, ok := f.integration.(ControlBinder); ok {
		if err := binder.BindControl(c); err != nil {
			return fmt.Errorf("bind integration: %w", err)
		}
	}
	// The integration keeps owning the Controller; the handler only serves it.
	var workers *control.Workers
	if provider, ok := f.integration.(WorkerProvider); ok {
		workers = provider.WorkerController()
		if workers == nil {
			return errors.New("install integration: worker integration has no controller")
		}
	}
	var environments *environmentcontrol.Controller
	var services *servingcontrol.Controller
	if provider, ok := f.integration.(ServingProvider); ok {
		services = provider.ServingController()
		if services == nil {
			if _, ok := f.integration.(EnvironmentProvider); !ok {
				return errors.New("install integration: serving integration has no controller")
			}
		}
	}
	if provider, ok := f.integration.(EnvironmentProvider); ok {
		environments = provider.EnvironmentController()
		if environments == nil && (services == nil || environmentTargets(installed)) {
			return errors.New("install integration: environment integration has no controller")
		}
	}
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
	switch {
	case workers != nil:
		rpcHTTP = rpc.NewWorkerHandler(c, engine, workers)
	case services != nil:
		rpcHTTP = rpc.NewIntegratedHandler(c, engine, environments, services)
	case environments != nil:
		rpcHTTP = rpc.NewEnvironmentHandler(c, engine, environments)
	}
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
	if in := f.integration; in != nil {
		f.integrationDone = make(chan struct{})
		go func() { defer close(f.integrationDone); f.integrationErr = in.Run(runCtx) }()
	}
	ready.Store(true)
	// The TLS listener closes the same original owner, so its Close outcome
	// stays recorded for shutdown.
	var serving net.Listener = f.listener
	if cfg.TLSConfig != nil {
		serving = tls.NewListener(f.listener, cfg.TLSConfig.Clone())
	}
	go func() { defer close(f.serveDone); f.serveErr = f.http.Serve(serving) }()
	return nil
}

// openState opens or initializes state, then artifacts and authentication.
func (f *serverRun) openState(cfg Config) error {
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
	f.engine = auth.NewLegacy(cfg.Token)
	if cfg.OIDCFile != "" {
		f.engine, err = auth.NewFileOIDC(cfg.AuthFile, s.Owner(), cfg.OIDCFile, cfg.OIDCJWKSURL)
	} else if cfg.AuthFile != "" {
		f.engine, err = auth.NewFile(cfg.AuthFile, s.Owner())
	}
	if err != nil {
		return fmt.Errorf("load authentication: %w", err)
	}
	return nil
}

// openExisting acquires an existing current installation without creating,
// initializing or migrating it. Its read-only facts bind the auth file and
// the artifact root first; the writer lock is then accepted only if those
// facts still hold, and the same Engine, root and Files are kept.
func (f *serverRun) openExisting(cfg Config) error {
	installed, err := store.InspectExisting(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("inspect existing installation: %w", err)
	}
	if cfg.OIDCFile != "" {
		f.engine, err = auth.NewFileOIDC(cfg.AuthFile, installed.Owner(), cfg.OIDCFile, cfg.OIDCJWKSURL)
	} else {
		f.engine, err = auth.NewFile(cfg.AuthFile, installed.Owner())
	}
	if err != nil {
		return fmt.Errorf("load authentication: %w", err)
	}
	if err := f.openArtifactRoot(cfg.ArtifactDir, installed.ArtifactInstallation()); err != nil {
		return err
	}
	f.store, err = store.OpenExisting(cfg.DataDir, installed)
	if err != nil {
		return fmt.Errorf("acquire existing installation: %w", err)
	}
	return f.preflightArtifacts()
}

// environmentTargets reports whether any of targets claims environment
// support.
func environmentTargets(targets []backend.Target) bool {
	for _, t := range targets {
		if t.SupportsPersistentEnvironments || t.SupportsEnvironmentArtifacts {
			return true
		}
	}
	return false
}

// localTargetName returns the name of the built-in local target.
func localTargetName(cfg Config) (string, error) {
	name := cfg.LocalTargetName
	if name == "" {
		return "local", nil
	}
	if cfg.DisableLocal {
		return "", errors.New("local target name conflicts with disabled local target")
	}
	if len(name) > 63 {
		return "", errors.New("local target name must be a lowercase DNS name of at most 63 bytes")
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return "", errors.New("local target name must be a lowercase DNS name of at most 63 bytes")
		}
	}
	return name, nil
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
