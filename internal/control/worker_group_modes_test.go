//go:build darwin || linux

package control

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

func workerModeBudgets() map[string]workeradmission.Budget {
	return map[string]workeradmission.Budget{
		"alpha": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
		"beta":  {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
	}
}

// Native methods are MODELED; Store, scoped file auth, Control and physical
// sampling are real. This CPU fixture installs no artifact service.
func workerModeFixture(t *testing.T) (*submissionDataFixture, *g1Backend, *g1Backend) {
	t.Helper()
	f := newSubmissionDataFixture(t, true, false)
	alpha, beta := newG1Backend(), newG1Backend()
	c, err := New(f.store, []backend.Target{
		{Name: "alpha", Binding: "alpha-binding", Backend: alpha, Kind: "MODELED", OutputStorage: backend.RemoteOutput, SupportsResourceLimits: true},
		{Name: "beta", Binding: "beta-binding", Backend: beta, Kind: "MODELED", OutputStorage: backend.RemoteOutput, SupportsResourceLimits: true},
	}, f.engine)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.control = c
	f.sample(t)
	if c.ArtifactsEnabled() {
		t.Fatal("CPU worker fixture unexpectedly installed artifacts")
	}
	return f, alpha, beta
}

func workerModeController(t *testing.T, f *submissionDataFixture, modes map[string][]backend.WorkerMode) *Workers {
	t.Helper()
	w, err := NewWorkers(f.control, WorkerConfig{Budgets: workerModeBudgets(), Modes: modes})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func workerModeTask(target string) backend.Task {
	return backend.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: target, CPUMillis: 1000, MemoryBytes: 1 << 30}
}

type workerModeStoreImage struct {
	raw      []byte
	sequence uint64
	groups   []store.WorkerGroup
}

func workerModeImage(t *testing.T, f *submissionDataFixture) workerModeStoreImage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	seq, err := f.store.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	groups, err := f.store.WorkerGroups(0, 64)
	if err != nil {
		t.Fatal(err)
	}
	return workerModeStoreImage{raw: raw, sequence: seq, groups: groups}
}

func sameWorkerModeImage(a, b workerModeStoreImage) bool {
	return bytes.Equal(a.raw, b.raw) && a.sequence == b.sequence && reflect.DeepEqual(a.groups, b.groups)
}

func TestWorkerModesConfiguration(t *testing.T) {
	f, alpha, beta := workerModeFixture(t)
	before := workerModeImage(t, f)
	for _, tt := range []struct {
		name string
		in   map[string][]backend.WorkerMode
		want map[string][]backend.WorkerMode
	}{
		{"legacy", nil, nil},
		{"deny all", map[string][]backend.WorkerMode{}, nil},
		{"nil target entry", map[string][]backend.WorkerMode{"alpha": nil}, nil},
		{"empty target entry", map[string][]backend.WorkerMode{"alpha": {}}, nil},
		{"subset", map[string][]backend.WorkerMode{"beta": {backend.WorkerIndependent}}, map[string][]backend.WorkerMode{"beta": {backend.WorkerIndependent}}},
		{"independent", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}}, map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}}},
		{"canonical order", map[string][]backend.WorkerMode{"alpha": {backend.WorkerTCP, backend.WorkerIndependent}}, map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent, backend.WorkerTCP}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := workerModeController(t, f, tt.in)
			if (w.modes == nil) != (tt.in == nil) {
				t.Fatal("explicit mode table collapsed into legacy routing")
			}
			for _, target := range []string{"alpha", "beta", "missing"} {
				if got := w.Modes(target); !reflect.DeepEqual(got, tt.want[target]) {
					t.Fatalf("Modes(%q) = %v, want %v", target, got, tt.want[target])
				}
			}
		})
	}
	for _, tt := range []struct {
		name string
		in   map[string][]backend.WorkerMode
	}{
		{"foreign target", map[string][]backend.WorkerMode{"foreign": {backend.WorkerIndependent}}},
		{"foreign empty target", map[string][]backend.WorkerMode{"foreign": nil}},
		{"tcp alone", map[string][]backend.WorkerMode{"alpha": {backend.WorkerTCP}}},
		{"unknown mode", map[string][]backend.WorkerMode{"alpha": {"other"}}},
		{"duplicate independent", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent, backend.WorkerIndependent}}},
		{"duplicate tcp", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent, backend.WorkerTCP, backend.WorkerTCP}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWorkers(f.control, WorkerConfig{Budgets: workerModeBudgets(), Modes: tt.in})
			if err == nil || w != nil {
				t.Fatal("invalid explicit mode table accepted")
			}
		})
	}
	if !sameWorkerModeImage(before, workerModeImage(t, f)) || alpha.checks.Load() != 0 || beta.checks.Load() != 0 || len(alpha.calls)+len(beta.calls) != 0 {
		t.Fatal("constructor/query performed Store writes or backend calls")
	}
	if w, err := NewWorkers(&Control{}, WorkerConfig{Budgets: workerModeBudgets(), Modes: map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}}}); err == nil || w != nil {
		t.Fatal("positive modes accepted a Control with no actual Store")
	}
	var nilWorkers *Workers
	var zero Workers
	if nilWorkers.Modes("alpha") != nil || zero.Modes("alpha") != nil {
		t.Fatal("nil or zero Workers exposed a mode")
	}
}

func TestWorkerModesDetached(t *testing.T) {
	f, alpha, beta := workerModeFixture(t)
	input := map[string][]backend.WorkerMode{"alpha": {backend.WorkerTCP, backend.WorkerIndependent}}
	w := workerModeController(t, f, input)
	input["alpha"][0] = "foreign"
	delete(input, "alpha")
	input["beta"] = []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP}
	want := []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP}
	before := workerModeImage(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 16; j++ {
				got := w.Modes("alpha")
				if !reflect.DeepEqual(got, want) {
					t.Error("mode snapshot changed with its caller")
					return
				}
				got[0] = "changed"
			}
		}()
	}
	wg.Wait()
	if !reflect.DeepEqual(w.Modes("alpha"), want) || w.Modes("beta") != nil || !sameWorkerModeImage(before, workerModeImage(t, f)) || alpha.checks.Load()+beta.checks.Load() != 0 {
		t.Fatal("input/query mutation escaped the detached memory snapshot")
	}
	j, _, err := w.Submit(f.actor, *f.domain, "detached", workerModeTask("alpha"), backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP})
	if err != nil || j.Target != "alpha" {
		t.Fatalf("detached policy lost its original route: %v", err)
	}
}

func TestWorkerModesPlacement(t *testing.T) {
	for _, tt := range []struct {
		name   string
		modes  map[string][]backend.WorkerMode
		mode   backend.WorkerMode
		target string
		want   string
	}{
		{"legacy tcp", nil, backend.WorkerTCP, "", "alpha"},
		{"legacy independent", nil, backend.WorkerIndependent, "", "alpha"},
		{"deny all independent", map[string][]backend.WorkerMode{}, backend.WorkerIndependent, "", ""},
		{"deny all tcp", map[string][]backend.WorkerMode{}, backend.WorkerTCP, "", ""},
		{"absent first target", map[string][]backend.WorkerMode{"beta": {backend.WorkerIndependent}}, backend.WorkerIndependent, "", "beta"},
		{"absent selected target", map[string][]backend.WorkerMode{"beta": {backend.WorkerIndependent}}, backend.WorkerIndependent, "alpha", ""},
		{"empty selected target", map[string][]backend.WorkerMode{"alpha": {}, "beta": {backend.WorkerIndependent}}, backend.WorkerIndependent, "alpha", ""},
		{"mixed tcp", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}, "beta": {backend.WorkerIndependent, backend.WorkerTCP}}, backend.WorkerTCP, "", "beta"},
		{"mixed independent", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}, "beta": {backend.WorkerIndependent, backend.WorkerTCP}}, backend.WorkerIndependent, "", "alpha"},
		{"unsupported selected tcp", map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}, "beta": {backend.WorkerIndependent, backend.WorkerTCP}}, backend.WorkerTCP, "alpha", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, alpha, beta := workerModeFixture(t)
			w := workerModeController(t, f, tt.modes)
			before := workerModeImage(t, f)
			j, spec, err := w.Submit(f.actor, *f.domain, "placement", workerModeTask(tt.target), backend.WorkerSpec{Count: 2, Mode: tt.mode})
			if tt.want == "" {
				if !errors.Is(err, ErrUnsupported) || j.ID != "" || spec != nil || !sameWorkerModeImage(before, workerModeImage(t, f)) || alpha.checks.Load()+beta.checks.Load() != 0 {
					t.Fatalf("denied route checked/accepted/wrote a group: %v", err)
				}
			} else {
				if err != nil || j.Target != tt.want || spec == nil || spec.Mode != tt.mode {
					t.Fatalf("wrong mode route: %q %+v %v", j.Target, spec, err)
				}
				if tt.want == "beta" && alpha.checks.Load() != 0 {
					t.Fatal("mode-ineligible earlier target was checked")
				}
			}
			if len(alpha.calls)+len(beta.calls) != 0 {
				t.Fatal("submission invoked a native worker effect")
			}
		})
	}
}

func TestWorkerModesReplayBeforeCurrentPolicy(t *testing.T) {
	f, alpha, beta := workerModeFixture(t)
	accepted := workerModeController(t, f, map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent, backend.WorkerTCP}})
	task := workerModeTask("alpha")
	want := backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}
	job, _, err := accepted.Submit(f.actor, *f.domain, "replay", task, want)
	if err != nil {
		t.Fatal(err)
	}
	// Neither controller is run. The replacement shares the actual Control and
	// Store; this is canonical request replay, not two live runners or HA.
	denied := workerModeController(t, f, map[string][]backend.WorkerMode{})
	before := workerModeImage(t, f)
	checks := alpha.checks.Load() + beta.checks.Load()
	replay, spec, err := denied.Submit(f.actor, *f.domain, "replay", task, want)
	if err != nil || !reflect.DeepEqual(replay, job) || spec == nil || *spec != want {
		t.Fatalf("current denial replaced the accepted route: %+v %+v %v", replay, spec, err)
	}
	if !sameWorkerModeImage(before, workerModeImage(t, f)) || alpha.checks.Load()+beta.checks.Load() != checks {
		t.Fatal("healthy replay consulted new placement or wrote rows/sequence")
	}
	if _, _, err := denied.Submit(f.actor, *f.domain, "fresh", task, want); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("new request bypassed explicit denial: %v", err)
	}
	changed := task
	changed.Argv = []string{"different"}
	if _, _, err := denied.Submit(f.actor, *f.domain, "replay", changed, want); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("changed canonical task replayed: %v", err)
	}
	if _, _, err := denied.Submit(f.actor, *f.domain, "replay", task, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("changed canonical mode replayed: %v", err)
	}
	if !sameWorkerModeImage(before, workerModeImage(t, f)) || alpha.checks.Load()+beta.checks.Load() != checks || len(alpha.calls)+len(beta.calls) != 0 {
		t.Fatal("denial/conflict changed canonical rows or ran a backend")
	}
}

func TestWorkerModesAcceptedCleanupUnchanged(t *testing.T) {
	f, alpha, beta := workerModeFixture(t)
	accepted := workerModeController(t, f, map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent, backend.WorkerTCP}})
	want := backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}
	job, _, err := accepted.Submit(f.actor, *f.domain, "accepted", workerModeTask("alpha"), want)
	if err != nil {
		t.Fatal(err)
	}
	// The accepting controller was never run. This one actual runner still
	// owns accepted groups even when its policy denies all new groups.
	denied := workerModeController(t, f, map[string][]backend.WorkerMode{})
	for i := 0; i < 3; i++ {
		g1Step(t, denied)
	}
	if alpha.count("create") != 2 || alpha.mainExec != 2 || len(beta.calls) != 0 {
		t.Fatal("new mode denial stranded or rerouted the already accepted group")
	}
	if _, err := f.control.Cancel(f.actor, job.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		g1Step(t, denied)
	}
	current := g1Read(t, f, job.ID)
	if current.Phase != store.WorkerGroupDone || current.Reserved || current.Group.Spec != want || current.Job.Target != "alpha" {
		t.Fatal("current mode denial changed accepted cancellation/cleanup/refund")
	}
}

func TestWorkerModesSingleIndependentUnchanged(t *testing.T) {
	f, alpha, beta := workerModeFixture(t)
	w := workerModeController(t, f, map[string][]backend.WorkerMode{})
	job, spec, err := w.Submit(f.actor, *f.domain, "single", workerModeTask("alpha"), backend.WorkerSpec{Count: 1, Mode: backend.WorkerIndependent})
	if err != nil || spec != nil || job.Target != "alpha" {
		t.Fatalf("explicit group denial changed ordinary one-worker behavior: %v", err)
	}
	if _, err := f.store.WorkerGroup(job.ID); !errors.Is(err, store.ErrNotFound) || alpha.checks.Load() != 1 || beta.checks.Load() != 0 {
		t.Fatal("ordinary job acquired worker group metadata or another route")
	}
}

// These constructors use real Store retention/scoped transitions. Legacy auth
// suffices for constructor validation; no Actor/delegation is manufactured.
func TestWorkerModesServingPrerequisites(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, scoped := range []bool{false, true} {
			t.Run(fmt.Sprintf("retention_%v/scoped_%v", retained, scoped), func(t *testing.T) {
				home, err := os.UserHomeDir()
				if err != nil {
					t.Fatal(err)
				}
				parent := filepath.Join(home, "tmp", "skypilot", "20261029-worker-mode-prerequisites")
				if err := os.MkdirAll(parent, 0700); err != nil {
					t.Fatal(err)
				}
				dir, err := os.MkdirTemp(parent, "case-")
				if err != nil {
					t.Fatal(err)
				}
				f := &submissionDataFixture{directory: dir}
				t.Cleanup(func() { f.retire(t) })
				state := filepath.Join(dir, "state")
				if retained {
					if _, err := store.EnableRetention(state, store.DefaultRetentionPolicy()); err != nil {
						t.Fatal(err)
					}
				}
				f.store, err = store.Open(state)
				if err != nil {
					f.hold(err)
					t.Fatal(err)
				}
				if scoped {
					if err := f.store.ActivateScoped(); err != nil {
						f.hold(err)
						t.Fatal(err)
					}
				}
				b := newG1Backend()
				f.control, err = New(f.store, []backend.Target{{Name: "alpha", Binding: "modeled", Backend: b, OutputStorage: backend.RemoteOutput}}, auth.NewLegacy(""))
				if err != nil {
					f.hold(err)
					t.Fatal(err)
				}
				budget := map[string]workeradmission.Budget{"alpha": {Ranks: 2, CPUMillis: 2000, MemoryBytes: 2 << 30}}
				before := workerModeImage(t, f)
				for _, modes := range []map[string][]backend.WorkerMode{nil, {}, {"alpha": nil}} {
					if _, err := NewWorkers(f.control, WorkerConfig{Budgets: budget, Modes: modes}); err != nil {
						t.Fatalf("non-advertised legacy/deny config changed prerequisites: %v", err)
					}
				}
				w, err := NewWorkers(f.control, WorkerConfig{Budgets: budget, Modes: map[string][]backend.WorkerMode{"alpha": {backend.WorkerIndependent}}})
				if retained && scoped {
					if err != nil || !reflect.DeepEqual(w.Modes("alpha"), []backend.WorkerMode{backend.WorkerIndependent}) {
						t.Fatalf("actual eligible CPU profile refused: %v", err)
					}
				} else if err == nil || w != nil {
					t.Fatal("positive worker modes bypassed actual serving prerequisites")
				}
				if !sameWorkerModeImage(before, workerModeImage(t, f)) || b.checks.Load() != 0 || len(b.calls) != 0 || f.control.ArtifactsEnabled() {
					t.Fatal("constructor forged readiness, used artifacts or made effects")
				}
			})
		}
	}
}

func ExampleWorkers_Modes() {
	c, _, _, close := exampleWorkerControl()
	defer close()
	config := exampleWorkerConfig()
	config.Modes = map[string][]backend.WorkerMode{"gang": {backend.WorkerIndependent}}
	workers, err := NewWorkers(c, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	modes := workers.Modes("gang")
	modes[0] = backend.WorkerTCP
	fmt.Println(workers.Modes("gang"))
	// Output: [independent]
}
