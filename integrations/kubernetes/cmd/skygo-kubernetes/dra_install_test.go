//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestDRAFlagsScript runs testdata/dra_flags.txt. Its server callback prints
// the route the production command chose and returns without running a
// server or calling Install.
func TestDRAFlagsScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	t.Cleanup(cancel)
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo-kubernetes"] = script.Command(script.CmdUsage{Summary: "run the skygo-kubernetes command in process", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var out, diag bytes.Buffer
		cmd := command(func(_ context.Context, cfg server.Config) error {
			switch {
			case cfg.Install != nil && cfg.Targets == nil:
				fmt.Fprintln(&out, "install route")
			case cfg.Install == nil && cfg.Targets != nil:
				fmt.Fprintln(&out, "targets route")
			default:
				return errors.New("ambiguous server route")
			}
			return nil
		})
		cmd.SetArgs(args)
		cmd.SetIn(strings.NewReader(""))
		cmd.SetOut(&out)
		cmd.SetErr(&diag)
		err := cmd.ExecuteContext(s.Context())
		if err != nil {
			fmt.Fprintln(&diag, err)
		}
		return func(*script.State) (string, string, error) { return out.String(), diag.String(), err }, nil
	})
	scripttest.Test(t, ctx, e, nil, "testdata/dra_flags.txt")
}

// TestDRAOIDCForwarded keeps the shared authentication options on the DRA
// installation route. It does not construct an engine or contact an issuer.
func TestDRAOIDCForwarded(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	l := &draLoopback{class: true}
	file, state, dra := draCommandFixture(t, l)
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.AuthFile != "auth.json" || cfg.OIDCFile != "oidc.json" || cfg.OIDCJWKSURL != "https://issuer.example/keys" {
			t.Fatalf("authentication options: %+v", cfg)
		}
		if cfg.Install == nil || cfg.Targets != nil {
			t.Fatal("DRA did not select the installation route")
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--targets", file, "--execution-state", state, "--dra-state", dra,
		"--auth-file", "auth.json", "--oidc-file", "oidc.json", "--oidc-jwks-url", "https://issuer.example/keys"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server callback not called")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.requests) != 0 {
		t.Fatal("configuration forwarding contacted the API")
	}
}

// draLoopback serves the reads DRA construction makes: discovery of
// resource.k8s.io/v1, namespace skygo-dra and DeviceClass gpu.example.com,
// unless class is false. Any other request fails and is counted as a write
// if it is not a GET.
type draLoopback struct {
	mu       sync.Mutex
	class    bool
	requests []string
}

func (l *draLoopback) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	l.requests = append(l.requests, r.Method+" "+r.URL.Path)
	class := l.class
	l.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	var body any
	switch {
	case r.Method == "GET" && r.URL.Path == "/apis/resource.k8s.io/v1":
		body = map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "resource.k8s.io/v1", "resources": []any{
			map[string]any{"name": "resourceclaims", "singularName": "", "namespaced": true, "kind": "ResourceClaim", "verbs": []string{"create", "delete", "get"}},
			map[string]any{"name": "deviceclasses", "singularName": "", "namespaced": false, "kind": "DeviceClass", "verbs": []string{"get"}},
		}}
	case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/skygo-dra":
		body = map[string]any{"kind": "Namespace", "apiVersion": "v1", "metadata": map[string]any{"name": "skygo-dra", "uid": "ns-uid"}, "status": map[string]any{"phase": "Active"}}
	case r.Method == "GET" && r.URL.Path == "/apis/resource.k8s.io/v1/deviceclasses/gpu.example.com" && class:
		body = map[string]any{"kind": "DeviceClass", "apiVersion": "resource.k8s.io/v1", "metadata": map[string]any{"name": "gpu.example.com", "uid": "class-uid"}, "spec": map[string]any{}}
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404})
		return
	}
	json.NewEncoder(w).Encode(body)
}

// heldTests records each test whose integration or server original was not
// joined: its directories stay in place, never removed under that original.
var heldTests sync.Map // *testing.T to the reason

// holdDirs keeps every directory privateDir made for t, and fails t.
func holdDirs(t *testing.T, reason error) {
	heldTests.Store(t, reason)
	t.Errorf("%v: directories retained", reason)
}

// keepDirs keeps every directory privateDir made for t, as holdDirs does,
// for a HOLD that a control expects: t logs it and does not fail.
func keepDirs(t *testing.T, reason error) {
	heldTests.Store(t, reason)
	t.Logf("MODELED %v: directories retained", reason)
}

// privateDir returns a new private directory without symlinks in its path.
// It is removed when t ends, unless holdDirs or keepDirs was called for t.
// Its cleanup is registered before any owner that uses it, so it runs after
// that owner's cleanup has decided.
func privateDir(t *testing.T) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(base, "skygo-dra-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if reason, held := heldTests.Load(t); held {
			t.Logf("HOLD %v: retained %s", reason, dir)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove %s: %v", dir, err)
		}
	})
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// dirImage returns every entry under dir with its mode and content digest.
func dirImage(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(&b, "%s error %v\n", path, err)
			return nil
		}
		info, _ := d.Info()
		fmt.Fprintf(&b, "%s %v", strings.TrimPrefix(path, dir), info.Mode())
		if info.Mode().IsRegular() {
			data, _ := os.ReadFile(path)
			fmt.Fprintf(&b, " %x", sha256.Sum256(data))
		}
		b.WriteByte('\n')
		return nil
	})
	return b.String()
}

// draCommandFixture writes a kubeconfig for l and a targets file with one
// DRA target, and returns the targets file and two private state roots.
func draCommandFixture(t *testing.T, l *draLoopback) (targets, state, dra string) {
	t.Helper()
	srv := httptest.NewServer(l)
	t.Cleanup(srv.Close)
	dir := privateDir(t)
	kubeconfig := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: %q}}]\ncontexts: [{name: test, context: {cluster: c, user: u}}]\nusers: [{name: u, user: {}}]\ncurrent-context: test\n", srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "kubeconfig"), []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	targets = filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(targets, []byte(draTargetsYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	return targets, privateDir(t), privateDir(t)
}

const draTargetsYAML = `version: 1
targets:
- name: gpu
  kubeconfig: kubeconfig
  context: test
  namespace: skygo-dra
  resources:
    accelerators:
      h100:
        dra:
          device_class: gpu.example.com
`

// errHeld reports an integration original whose join is unknown. Its Store
// and directories are kept, never closed or removed under it.
var errHeld = errors.New("HOLD: integration original not joined; store and state retained")

// TestDRAInstallLifecycle runs the production command with a server callback
// that reenacts the server's integration lifecycle once, in its order: open
// the Store, call Install once, take Targets, run the integration until
// canceled, join it, then Close it, and only after a positive Close close
// the Store. Run's cancellation and join are set up before it starts. A
// startup failure returns no Integration and writes no DRA state. The actual
// server.Run is TestDRAServerRun.
func TestDRAInstallLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		class bool
		err   string
	}{
		{"constructed", true, ""},
		{"startup failure", false, "device class is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &draLoopback{class: tc.class}
			targets, state, dra := draCommandFixture(t, l)
			before := dirImage(t, dra)
			installs := 0
			var names []string
			run := func(ctx context.Context, cfg server.Config) error {
				if cfg.Install == nil || cfg.Targets != nil {
					return errors.New("dra route did not use Install")
				}
				s, err := store.Open(filepath.Join(privateDir(t), "data"))
				if err != nil {
					return err
				}
				installs++
				in, err := cfg.Install(s, auth.NewLegacy(""))
				if err != nil {
					if in != nil {
						// A partial Integration owns an original: it is
						// closed, positively, before the Store.
						closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer closeCancel()
						if cerr := in.Close(closeCtx); cerr != nil {
							holdDirs(t, errHeld)
							return errors.Join(err, cerr, errHeld)
						}
					}
					return errors.Join(err, s.Close())
				}
				for _, target := range in.Targets() {
					names = append(names, target.Name+":"+strings.Join(target.AcceleratorNames, ","))
				}
				// Cancellation and the join are registered before Run starts.
				runCtx, cancel := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- in.Run(runCtx) }()
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						return fmt.Errorf("integration run: %w", err)
					}
				case <-time.After(stateTimeout):
					holdDirs(t, errHeld)
					return fmt.Errorf("integration run did not return: %w", errHeld)
				}
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer closeCancel()
				if err := in.Close(closeCtx); err != nil {
					holdDirs(t, errHeld)
					return errors.Join(err, errHeld)
				}
				return s.Close()
			}
			cmd := command(run)
			cmd.SetArgs([]string{"server", "--targets", targets, "--execution-state", state, "--dra-state", dra, "--disable-local"})
			var out, diag bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&diag)
			ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
			defer cancel()
			err := cmd.ExecuteContext(ctx)
			if installs != 1 {
				t.Fatalf("Install called %d times", installs)
			}
			l.mu.Lock()
			requests := append([]string(nil), l.requests...)
			l.mu.Unlock()
			for _, r := range requests {
				if !strings.HasPrefix(r, "GET ") {
					t.Fatalf("construction sent %s", r)
				}
			}
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				if after := dirImage(t, dra); after != before {
					t.Fatalf("failed startup wrote dra state:\n%s", after)
				}
				return
			}
			if err != nil {
				t.Fatalf("command: %v\n%s", err, diag.String())
			}
			if strings.Join(names, " ") != "gpu:h100" {
				t.Fatalf("targets = %v", names)
			}
			if _, err := os.Stat(filepath.Join(dra, "skygo-dra-state.json")); err != nil {
				t.Fatalf("dra state marker: %v", err)
			}
		})
	}
}

// The old parser's refusals: its strict targets decoding
// (internal/kube/config.go at 4e9431d43dc5) and cobra's unknown flag.
const (
	oldFieldRefusal = "unknown field or invalid value in targets configuration"
	oldFlagRefusal  = "unknown flag: --dra-state"
)

// TestDRAHistoricalBinaryRefusal is the required old-binary gate: a genuine
// executable built from the base commit 4e9431d43dc5 must refuse both the
// DRA targets file and the --dra-state flag with the old parser's own
// diagnostics, before writing either state root or sending a request. The
// coordinator supplies the executable and the SHA-256 recorded by its
// signed-base build receipt; without both the gate is NOT_RUN, never a
// pass. Its oracle is checked against an unrelated failing executable,
// which it must reject, and the binary against a valid legacy file, which
// its parser must accept, and a legacy file with an unknown field, which it
// must refuse with the same diagnostic. Editing current JSON or headers is
// not a substitute for it.
func TestDRAHistoricalBinaryRefusal(t *testing.T) {
	bin := os.Getenv("SKYGO_KUBERNETES_HISTORICAL_BINARY")
	sum := os.Getenv("SKYGO_KUBERNETES_HISTORICAL_BINARY_SHA256")
	if bin == "" || sum == "" {
		t.Skip("REQUIRED GATE NOT_RUN: set SKYGO_KUBERNETES_HISTORICAL_BINARY and its build receipt's SKYGO_KUBERNETES_HISTORICAL_BINARY_SHA256")
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != sum {
		t.Fatalf("historical binary sha256 %s, build receipt %s", got, sum)
	}
	// legacy replaces DRA with the old parser's allowlisted extended-resource
	// policy for the same accelerator. Refusals use the DRA field or flag.
	legacy := strings.Replace(draTargetsYAML, "dra:\n          device_class: gpu.example.com", "resource_name: nvidia.com/gpu\n        required_node_selector:\n          nvidia.com/gpu.product: H100", 1)
	legacy = strings.Replace(legacy, "    accelerators:", "    node_selectors:\n      nvidia.com/gpu.product: [H100]\n    accelerators:", 1)
	t.Run("oracle rejects an unrelated failing executable", func(t *testing.T) {
		f, err := exec.LookPath("false")
		if err != nil {
			t.Fatalf("negative control: %v", err)
		}
		for _, flag := range []bool{false, true} {
			if err := historicalRefusal(t, f, legacy, flag, oldFlagRefusal); err == nil {
				t.Fatal("oracle accepted an unrelated failing executable")
			}
		}
	})
	t.Run("legacy parser positive control", func(t *testing.T) {
		// The old binary must accept the legacy file and keep serving: a
		// process that exits early, for any reason, is not this control.
		out, serving := runHistorical(t, bin, legacy, false, 3*time.Second)
		if !serving || strings.Contains(out, oldFieldRefusal) {
			t.Fatalf("valid legacy targets not served (serving %v):\n%s", serving, out)
		}
		unknown := legacy + "  colour: blue\n"
		if err := historicalRefusal(t, bin, unknown, false, oldFieldRefusal); err != nil {
			t.Fatalf("legacy unknown field: %v", err)
		}
	})
	t.Run("targets file", func(t *testing.T) {
		if err := historicalRefusal(t, bin, draTargetsYAML, false, oldFieldRefusal); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("dra flag", func(t *testing.T) {
		if err := historicalRefusal(t, bin, legacy, true, oldFlagRefusal); err != nil {
			t.Fatal(err)
		}
	})
}

// historicalRefusal runs bin as the server with targets, and the DRA flag
// if flag, and reports why it is not a refusal with diagnostic want, before
// any state write or request.
func historicalRefusal(t *testing.T, bin, targets string, flag bool, want string) error {
	t.Helper()
	l := &draLoopback{class: true}
	file, state, dra := draCommandFixture(t, l)
	if err := os.WriteFile(file, []byte(targets), 0o600); err != nil {
		t.Fatal(err)
	}
	data := privateDir(t)
	before := dirImage(t, state) + dirImage(t, dra) + dirImage(t, data)
	args := []string{"server", "--targets", file, "--execution-state", state, "--data-dir", data, "--listen", "127.0.0.1:0", "--disable-local"}
	if flag {
		args = append(args, "--dra-state", dra)
	}
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput()
	var exit *exec.ExitError
	switch {
	case !errors.As(err, &exit) || ctx.Err() != nil:
		return fmt.Errorf("did not refuse: %v\n%s", err, out)
	case !strings.Contains(string(out), want):
		return fmt.Errorf("refused without %q:\n%s", want, out)
	}
	if after := dirImage(t, state) + dirImage(t, dra) + dirImage(t, data); after != before {
		return fmt.Errorf("wrote state before refusing:\n%s", after)
	}
	l.mu.Lock()
	n := len(l.requests)
	l.mu.Unlock()
	if n != 0 {
		return fmt.Errorf("sent %d requests", n)
	}
	return nil
}

// runHistorical runs bin as the server with targets for d and returns its
// output and whether it was still serving at d; it is then stopped and
// waited for.
func runHistorical(t *testing.T, bin, targets string, flag bool, d time.Duration) (string, bool) {
	t.Helper()
	l := &draLoopback{class: true}
	file, state, dra := draCommandFixture(t, l)
	if err := os.WriteFile(file, []byte(targets), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"server", "--targets", file, "--execution-state", state, "--data-dir", privateDir(t), "--listen", "127.0.0.1:0", "--disable-local"}
	if flag {
		args = append(args, "--dra-state", dra)
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	out, _ := exec.CommandContext(ctx, bin, args...).CombinedOutput()
	return string(out), ctx.Err() != nil
}
