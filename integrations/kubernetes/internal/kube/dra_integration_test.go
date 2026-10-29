package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
)

func TestDRAIntegrationConstruction(t *testing.T) {
	f := newDRAFixture(t)
	in, b := f.integration(t)
	targets := in.Targets()
	if len(targets) != 1 || !slices.Equal(targets[0].AcceleratorNames, []string{"h100", "nvidia"}) {
		t.Fatalf("targets = %+v", targets)
	}
	if targets[0].Backend != backend.Backend(b) || b.receipts == nil || b.dra == nil {
		t.Fatal("target is not the integration's receipted DRA backend")
	}
	// Construction sends only reads.
	for _, r := range f.api.log() {
		if !strings.HasPrefix(r, "GET ") {
			t.Fatalf("construction sent %s", r)
		}
	}
	// An ordinary accelerator keeps the receipted ordinary path.
	ordinary := draTask(1)
	ordinary.Accelerator = "nvidia"
	if err := b.Check(ordinary); err != nil {
		t.Fatal(err)
	}
}

func TestDRAIntegrationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(f *draFixture) DRAOptions
		err   string
	}{
		{"no discovery", func(f *draFixture) DRAOptions {
			f.api.set(func() { f.api.discovery = false })
			return DRAOptions{ExecutionState: f.exec, State: f.root}
		}, "resource.k8s.io/v1"},
		{"missing class", func(f *draFixture) DRAOptions {
			f.api.set(func() { delete(f.api.classes, "gpu.example.com") })
			return DRAOptions{ExecutionState: f.exec, State: f.root}
		}, "device class is missing"},
		{"same roots", func(f *draFixture) DRAOptions { return DRAOptions{ExecutionState: f.root, State: f.root} }, "separate"},
		{"relative root", func(f *draFixture) DRAOptions { return DRAOptions{ExecutionState: f.exec, State: "dra"} }, "absolute"},
		{"negative retention", func(f *draFixture) DRAOptions {
			return DRAOptions{ExecutionState: f.exec, State: f.root, LogRetention: -time.Second}
		}, "nonnegative"},
		{"legacy root", func(f *draFixture) DRAOptions {
			os.WriteFile(filepath.Join(f.root, testID+".json"), []byte("{}"), 0o600)
			return DRAOptions{ExecutionState: f.exec, State: f.root}
		}, "unexpected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			opts := tc.setup(f)
			before := treeImage(t, f.root)
			in, err := NewDRAIntegration(context.Background(), f.config, "owner", opts)
			if d, ok := in.(*draIntegration); ok {
				f.own(d) // an unexpected owner keeps the roots until closed
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) || in != nil {
				t.Fatalf("in %v, err = %v, want %q", in, err, tc.err)
			}
			if after := treeImage(t, f.root); after != before {
				t.Fatal("refused construction wrote the dra state")
			}
			for _, r := range f.api.log() {
				if !strings.HasPrefix(r, "GET ") {
					t.Fatalf("refused construction sent %s", r)
				}
			}
		})
	}
	t.Run("no dra accelerator", func(t *testing.T) {
		f := newDRAFixture(t)
		config, err := readConfig([]byte("version: 1\ntargets:\n- name: plain\n  in_cluster: true\n  namespace: workloads\n"), "/", false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewDRAIntegration(context.Background(), config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root}); err == nil {
			t.Fatal("integration without dra accelerators")
		}
	})
}

// TestDRAIntegrationRun checks the integration's lifecycle as the server
// runs it: Run maintains records without any core Observe, returns promptly
// once its context ends, and Close then joins it.
func TestDRAIntegrationRun(t *testing.T) {
	f := newDRAFixture(t)
	in, b := f.integration(t)
	in.interval = 10 * time.Millisecond
	draSucceededAttempt(t, f, b)
	f.now = f.now.Add(2 * time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// Cleanup is registered before the goroutine starts: cancel, then join.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(receiptJoinTimeout):
			t.Error("Run did not return")
		}
	})
	go func() { done <- in.Run(ctx) }()
	deadline := time.Now().Add(receiptJoinTimeout)
	for draRecordOf(t, b, testID).State != draRetired {
		if time.Now().After(deadline) {
			t.Fatal("maintenance did not retire the record")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		done <- err // for Cleanup
	case <-time.After(receiptJoinTimeout):
		t.Fatal("Run did not return")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
	defer closeCancel()
	if err := in.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Start(context.Background(), otherID, draTask(1)); !errors.Is(err, errDRAClosed) {
		t.Fatalf("start after Close: %v", err)
	}
}

func TestDRAIntegrationClose(t *testing.T) {
	t.Run("without run", func(t *testing.T) {
		f := newDRAFixture(t)
		in, _ := f.integration(t)
		if err := in.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := in.Run(context.Background()); err == nil {
			t.Fatal("Run after Close")
		}
	})
	t.Run("held operation", func(t *testing.T) {
		f := newDRAFixture(t)
		in, b := f.integration(t)
		done, err := b.dra.begin("")
		if err != nil {
			t.Fatal(err)
		}
		short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := in.Close(short); err == nil {
			done()
			t.Fatal("Close returned nil while an original operation was held")
		}
		done()
		if err := in.Close(context.Background()); err != nil {
			t.Fatalf("Close after join: %v", err)
		}
	})
	// MODELED: a log body whose Close was uncertain keeps its custody, and
	// the scope's permit, for the process; Close must not report success.
	t.Run("held log body", func(t *testing.T) {
		f := newDRAFixture(t)
		in, b := f.integration(t)
		c := &logCustody{store: b.dra.store.s, id: testID}
		logCustodies.mu.Lock()
		logCustodies.kept = append(logCustodies.kept, c)
		logCustodies.mu.Unlock()
		t.Cleanup(func() {
			logCustodies.mu.Lock()
			logCustodies.kept = slices.DeleteFunc(logCustodies.kept, func(x *logCustody) bool { return x == c })
			logCustodies.mu.Unlock()
		})
		if err := in.Close(context.Background()); !errors.Is(err, errLogBodyHeld) {
			t.Fatalf("Close = %v", err)
		}
	})
}

func ExampleNewDRAIntegration() {
	_, err := NewDRAIntegration(context.Background(), Config{}, "owner", DRAOptions{State: "relative/dra"})
	fmt.Println(err)
	// Output: kubernetes dra state directory must be absolute
}

// exampleDRA serves a MODELED API that answers only the reads of DRA
// construction, for namespace skygo-dra and class gpu.example.com, and
// writes a targets file for it and two private state directories. done
// stops the API and removes the directories. No cluster is contacted.
func exampleDRA() (config Config, opts DRAOptions, done func(), err error) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /apis/resource.k8s.io/v1":
			io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"resource.k8s.io/v1","resources":[`+
				`{"name":"resourceclaims","singularName":"","namespaced":true,"kind":"ResourceClaim","verbs":["create","delete","get"]},`+
				`{"name":"deviceclasses","singularName":"","namespaced":false,"kind":"DeviceClass","verbs":["get"]}]}`)
		case "GET /api/v1/namespaces/skygo-dra":
			io.WriteString(w, `{"kind":"Namespace","apiVersion":"v1","metadata":{"name":"skygo-dra","uid":"ns-uid"},"status":{"phase":"Active"}}`)
		case "GET /apis/resource.k8s.io/v1/deviceclasses/gpu.example.com":
			io.WriteString(w, `{"kind":"DeviceClass","apiVersion":"resource.k8s.io/v1","metadata":{"name":"gpu.example.com","uid":"class-uid"},"spec":{}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		}
	}))
	base, err := os.MkdirTemp("", "dra-example")
	if err == nil {
		base, err = filepath.EvalSymlinks(base) // state paths must not contain symlinks
	}
	done = func() {
		api.Close()
		if base != "" {
			os.RemoveAll(base)
		}
	}
	if err != nil {
		done()
		return Config{}, DRAOptions{}, nil, err
	}
	opts = DRAOptions{ExecutionState: filepath.Join(base, "execution"), State: filepath.Join(base, "dra")}
	kubeconfig := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: %q}}]\ncontexts: [{name: example, context: {cluster: c, user: u}}]\nusers: [{name: u, user: {}}]\ncurrent-context: example\n", api.URL)
	targets := `version: 1
targets:
- name: gpu
  kubeconfig: kubeconfig
  context: example
  namespace: skygo-dra
  resources:
    max_accelerators: 8
    accelerators:
      h100:
        dra:
          device_class: gpu.example.com
`
	for _, err = range []error{
		os.Mkdir(opts.ExecutionState, 0o700),
		os.Mkdir(opts.State, 0o700),
		os.WriteFile(filepath.Join(base, "kubeconfig"), []byte(kubeconfig), 0o600),
		os.WriteFile(filepath.Join(base, "targets.yaml"), []byte(targets), 0o600),
	} {
		if err != nil {
			done()
			return Config{}, DRAOptions{}, nil, err
		}
	}
	config, err = ReadConfig(filepath.Join(base, "targets.yaml"), false)
	if err != nil {
		done()
		return Config{}, DRAOptions{}, nil, err
	}
	return config, opts, done, nil
}

// retainedDRA keeps, for the process, each example integration whose Close
// failed or did not return in time, with the cleanup of the API and state
// it depends on, which is never run.
var retainedDRA []any

// closeDRA closes in, which may be the partial Integration NewDRAIntegration
// returns with an error, within ten seconds. Only after it returned nil may
// the caller run done, which stops the API and removes the state: otherwise
// in and done are retained and done never runs. A nil in closes nothing.
func closeDRA(in server.Integration, done func()) error {
	if in == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := in.Close(ctx)
	if err != nil {
		retainedDRA = append(retainedDRA, in, done)
	}
	return err
}

func ExampleDRAOptions() {
	config, opts, done, err := exampleDRA()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Keep a succeeded Pod, and so its logs, for a day before API cleanup
	// deletes it and then its claim.
	opts.LogRetention = 24 * time.Hour
	in, err := NewDRAIntegration(context.Background(), config, "owner", opts)
	if err != nil {
		// A partial Integration owns an original: only its Close decides
		// whether the API and state may go.
		fmt.Println(err)
		if closeDRA(in, done) == nil {
			done()
		}
		return
	}
	for _, t := range in.Targets() {
		fmt.Println(t.Name, t.AcceleratorNames)
	}
	_, err = os.Stat(filepath.Join(opts.State, "skygo-dra-state.json"))
	fmt.Println("state marker written:", err == nil)
	err = closeDRA(in, done)
	fmt.Println("close:", err)
	if err == nil {
		done()
	}
	// Output:
	// gpu [h100]
	// state marker written: true
	// close: <nil>
}

// ExampleNewDRAIntegration_run shows the lifecycle the server gives an
// integration: construct it, run its maintenance until the run context
// ends, then Close it, which joins Run and every original operation. A
// second construction over the same state, as after a restart, accepts it.
// The API and state go only after every Close returned nil (closeDRA).
func ExampleNewDRAIntegration_run() {
	config, opts, done, err := exampleDRA()
	if err != nil {
		fmt.Println(err)
		return
	}
	for range 2 {
		in, err := NewDRAIntegration(context.Background(), config, "owner", opts)
		if err != nil {
			fmt.Println(err)
			if closeDRA(in, done) == nil {
				done()
			}
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		ran := make(chan error, 1)
		go func() { ran <- in.Run(ctx) }()
		cancel()
		select {
		case err := <-ran:
			fmt.Println("run:", err)
		case <-time.After(10 * time.Second):
			fmt.Println("run: held")
			retainedDRA = append(retainedDRA, in, done)
			return
		}
		err = closeDRA(in, done)
		fmt.Println("close:", err)
		if err != nil {
			return // in, the API and the state are retained
		}
	}
	done()
	// Output:
	// run: context canceled
	// close: <nil>
	// run: context canceled
	// close: <nil>
}

// TestDRAExampleOwnership is the control of closeDRA, the examples' owner
// handling, with the production constructor and Close: a partial Integration
// whose startup original Close failed, and a constructed one whose Close
// fails (a MODELED held log body), are retained with their cleanup, which
// never runs. A partial Integration whose held original was released, and a
// normal one, close and allow it.
func TestDRAExampleOwnership(t *testing.T) {
	check := func(t *testing.T, in server.Integration, wantErr error) {
		t.Helper()
		ran := false
		n := len(retainedDRA)
		err := closeDRA(in, func() { ran = true })
		if err == nil {
			if wantErr != nil {
				t.Fatalf("closeDRA = nil, want %v", wantErr)
			}
			return // the caller may run done
		}
		if !errors.Is(err, wantErr) || ran || len(retainedDRA) != n+2 || retainedDRA[n] != in {
			t.Fatalf("closeDRA = %v, cleanup ran %v, retained %d", err, ran, len(retainedDRA)-n)
		}
	}
	construct := func(ctx context.Context, f *draFixture, fault *draFault) (*draIntegration, error) {
		in, err := newDRAIntegration(ctx, f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root}, f.wire(fault).wrap)
		f.own(in)
		return in, err
	}
	t.Run("partial, failed startup close", func(t *testing.T) {
		f := newDRAFixture(t)
		in, err := construct(context.Background(), f, newDRAFault("GET", "/apis/resource.k8s.io/v1", errModeledClose, false))
		if in != nil {
			f.expect(in, errModeledClose)
		}
		if !errors.Is(err, errModeledClose) || in == nil {
			t.Fatalf("in %v, err = %v", in, err)
		}
		check(t, in, errModeledClose)
	})
	t.Run("partial, released", func(t *testing.T) {
		f := newDRAFixture(t)
		fault := newDRAFault("GET", "/apis/resource.k8s.io/v1", nil, true)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		in, err := construct(ctx, f, fault)
		if !errors.Is(err, errDRAHeld) || in == nil {
			t.Fatalf("in %v, err = %v", in, err)
		}
		fault.release()
		awaitDRA(t, fault.returned, "original discovery Close")
		check(t, in, nil)
	})
	t.Run("failed close", func(t *testing.T) {
		f := newDRAFixture(t)
		in, b := f.integration(t)
		c := &logCustody{store: b.dra.store.s, id: testID}
		logCustodies.mu.Lock()
		logCustodies.kept = append(logCustodies.kept, c)
		logCustodies.mu.Unlock()
		t.Cleanup(func() {
			logCustodies.mu.Lock()
			logCustodies.kept = slices.DeleteFunc(logCustodies.kept, func(x *logCustody) bool { return x == c })
			logCustodies.mu.Unlock()
		})
		check(t, in, errLogBodyHeld)
	})
	t.Run("normal close", func(t *testing.T) {
		f := newDRAFixture(t)
		in, _ := f.integration(t)
		check(t, in, nil)
		check(t, nil, nil) // a nil Integration closes nothing
	})
}

func ExampleDRAPolicy() {
	dir, err := os.MkdirTemp("", "dra-example")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "targets.yaml")
	err = os.WriteFile(path, []byte(`version: 1
targets:
- name: gpu
  in_cluster: true
  namespace: skygo-dra
  resources:
    max_accelerators: 8
    accelerators:
      h100:
        dra:
          device_class: gpu.example.com
          selectors:
          - 'device.attributes["gpu.example.com"].model == "H100"'
`), 0o600)
	if err != nil {
		fmt.Println(err)
		return
	}
	config, err := ReadConfig(path, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	p := config.Targets()[0].Resources.Accelerators["h100"].DRA
	fmt.Println(p.DeviceClass, len(p.Selectors))
	// Output: gpu.example.com 1
}
