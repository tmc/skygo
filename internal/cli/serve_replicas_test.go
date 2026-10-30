package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// replicaServiceRuntime supplies explicit modeled native observations. The
// script uses the actual Store, file Engine, controller, authenticated RPC
// handler and CLI. It never contacts Kubernetes or executes a workload.
type replicaServiceRuntime struct {
	mu     sync.Mutex
	counts map[string]int32
	gone   map[string]bool
}

func (*replicaServiceRuntime) Check(serving.Spec) error { return nil }
func (r *replicaServiceRuntime) Apply(_ context.Context, ref serving.Ref, rev serving.Revision, _ uint64) (serving.Native, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[ref.Service] = rev.Spec.Scaling.Replicas
	return serving.Native(fmt.Sprintf("modeled-replicas:%d", rev.Spec.Scaling.Replicas)), nil
}
func (r *replicaServiceRuntime) Observe(_ context.Context, ref serving.Ref) (serving.Observation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gone[ref.Service] {
		return serving.Observation{Absent: true}, nil
	}
	n := r.counts[ref.Service]
	if n == 0 {
		return serving.Observation{}, errors.New("no modeled service")
	}
	return serving.Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: n, UpdatedReplicas: n, ReadyReplicas: n, AvailableReplicas: n, ReadyEndpoints: n}, nil
}
func (r *replicaServiceRuntime) Delete(_ context.Context, ref serving.Ref) (serving.Observation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gone[ref.Service] = true
	return serving.Observation{Absent: true}, nil
}
func (r *replicaServiceRuntime) Forward(_ context.Context, ref serving.Ref, w http.ResponseWriter, _ *http.Request) error {
	r.mu.Lock()
	n := r.counts[ref.Service]
	r.mu.Unlock()
	_, err := fmt.Fprintf(w, "replicas=%d\n", n)
	return err
}

func TestServeReplicaScripts(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	config, tokenFile := filepath.Join(dir, "auth.json"), filepath.Join(dir, "alice.token")
	if _, err := auth.Initialize(config, filepath.Join(dir, "admin.token"), db.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(config, "alice", auth.Operator); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Issue(config, "alice", tokenFile, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, db.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	bearer := "Bearer " + string(bytes.TrimSpace(raw))
	actor, err := engine.Authenticate(bearer)
	if err != nil {
		t.Fatal(err)
	}
	rt := &replicaServiceRuntime{counts: map[string]int32{}, gone: map[string]bool{}}
	svc, err := servingcontrol.New(servingcontrol.Config{Store: db, Auth: engine, Target: "kube", Binding: "kube/default", Runtime: rt, Limits: store.ServiceLimits{Replicas: 8, CPUMillis: 2000, MemoryBytes: 2 << 30}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := svc.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctl, err := control.New(db, []backend.Target{{Name: "kube", Kind: "kubernetes", Binding: "kube/default", Backend: noJobs{}}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(rpc.NewIntegratedHandler(ctl, engine, nil, svc))
	t.Cleanup(srv.Close)
	e := scriptEngine(t)
	e.Cmds["service-step"] = script.Command(script.CmdUsage{Summary: "reconcile the modeled service runtime"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return nil, svc.Reconcile(ctx)
	})
	e.Cmds["service-id"] = script.Command(script.CmdUsage{Summary: "bind a variable to an accepted service ID", Args: "variable name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		p, err := svc.List(actor, 0, 200)
		if err != nil {
			return nil, err
		}
		for _, v := range p.Services {
			if v.Name == args[1] {
				return nil, s.Setenv(args[0], v.ID)
			}
		}
		return nil, errors.New("service not found")
	})
	e.Cmds["service-records"] = script.Command(script.CmdUsage{Summary: "check the actual unretired record count", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		p, err := svc.List(actor, 0, 200)
		if err != nil {
			return nil, err
		}
		if len(p.Services) != n {
			return nil, fmt.Errorf("records=%d, want %d", len(p.Services), n)
		}
		return nil, nil
	})
	// This exercises the real authenticated forward route. The separate
	// existing CLI forwarding tests own listener/interrupt behavior.
	e.Cmds["service-forward"] = script.Command(script.CmdUsage{Summary: "read the modeled workload through the real route", Args: "id output"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "GET", srv.URL+"/services/"+args[0]+"/forward/", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", bearer)
		resp, err := srv.Client().Do(req)
		if err != nil {
			return nil, err
		}
		b, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("forward status %d", resp.StatusCode)
		}
		return nil, os.WriteFile(s.Path(args[1]), b, 0600)
	})
	domain := fmt.Sprintf("%s:%d", info.Domain.Lineage, info.Domain.Epoch)
	scripttest.Test(t, context.Background(), e, []string{"SKYGO_ENDPOINT=" + srv.URL, "TOKEN_FILE=" + tokenFile, "DOMAIN=" + domain}, "testdata/serve_replicas.txt")
	p, err := svc.List(actor, 0, 200)
	if err != nil || len(p.Services) != 0 {
		t.Fatalf("final actual service page = %+v, %v", p, err)
	}
}

func TestReadServiceReplicaSpec(t *testing.T) {
	for _, n := range []int32{2, 8} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			spec, err := readServiceSpec(strings.NewReader(strings.Replace(serveSpecDoc, "replicas: 1", fmt.Sprintf("replicas: %d", n), 1)))
			if err != nil || spec.Scaling.Replicas != n {
				t.Fatalf("spec=%+v, error=%v", spec, err)
			}
		})
	}
}

func TestServeReplicaOldEndpoint(t *testing.T) {
	fixture := &servingFixture{}
	srv := serveServices(t, &serveControlInfo{}, fixture)
	file := filepath.Join(t.TempDir(), "two.yaml")
	if err := os.WriteFile(file, []byte(strings.Replace(serveSpecDoc, "replicas: 1", "replicas: 2", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err := execute(context.Background(), "--endpoint", srv.URL, "serve", "up", file, "--name", "web", "--target", "kube")
	if err == nil || !strings.Contains(err.Error(), "does not advertise services-v1") {
		t.Fatalf("old endpoint = %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.creates) != 0 {
		t.Fatalf("old endpoint received %d creates", len(fixture.creates))
	}
}
