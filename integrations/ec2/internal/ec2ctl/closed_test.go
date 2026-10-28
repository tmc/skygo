package ec2ctl

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

// TestClosedMachineRefuses closes a Machine wanting stopped and opens a
// replacement that stores absent. The stale Machine must neither send its
// Stop nor overwrite the replacement's state.
func TestClosedMachineRefuses(t *testing.T) {
	e := newEnv(t)
	old := e.create()
	e.drive(old, GoalRunning)
	must(t, old.SetGoal(GoalStopped))
	if st := e.stored(); st.Record.Desired != ec2vm.DesiredStopped || st.Record.Phase != ec2vm.PhaseRunning {
		t.Fatalf("stored before Close: wants %v, is %v", st.Record.Desired, st.Record.Phase)
	}
	must(t, old.Close())

	repl := e.open()
	must(t, repl.SetGoal(GoalAbsent))
	name := filepath.Join(e.dir, stateName)
	before, err := os.ReadFile(name)
	must(t, err)
	total := e.srv.Total()

	if err := old.SetGoal(GoalRunning); !errors.Is(err, os.ErrClosed) {
		t.Errorf("stale SetGoal: %v, want ErrClosed", err)
	}
	if _, err := old.Drive(context.Background(), e.c, fast); !errors.Is(err, os.ErrClosed) {
		t.Errorf("stale Drive: %v, want ErrClosed", err)
	}
	if err := old.Close(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("second Close: %v, want ErrClosed", err)
	}
	if n := e.srv.Total(); n != total {
		t.Errorf("stale Machine sent %d requests", n-total)
	}
	e.count("StopInstances", 0)
	after, err := os.ReadFile(name)
	must(t, err)
	if !bytes.Equal(after, before) {
		t.Fatalf("stale Machine rewrote the state:\n%s\nwant\n%s", after, before)
	}

	if st := e.stored(); st.Record.Desired != ec2vm.DesiredAbsent {
		t.Fatalf("stored wants %v, want absent", st.Record.Desired)
	}
	st, err := repl.Drive(context.Background(), e.c, fast)
	if err != nil || !st.Reached() || st.Record.Phase != ec2vm.PhaseTerminated {
		t.Fatalf("replacement Drive: %v, phase %v", err, st.Record.Phase)
	}
	insts := e.srv.Instances()
	if len(insts) != 1 || insts[0].State != "terminated" {
		t.Fatalf("instances %+v, want one terminated", insts)
	}
}

func TestNilMachineRefuses(t *testing.T) {
	for i, m := range []*Machine{nil, {}} {
		if err := m.SetGoal(GoalRunning); !errors.Is(err, os.ErrClosed) {
			t.Errorf("%d: SetGoal: %v, want ErrClosed", i, err)
		}
		if _, err := m.Drive(context.Background(), nil, fast); !errors.Is(err, os.ErrClosed) {
			t.Errorf("%d: Drive: %v, want ErrClosed", i, err)
		}
		if err := m.Close(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("%d: Close: %v, want ErrClosed", i, err)
		}
	}
}
