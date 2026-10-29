//go:build linux || darwin

package local

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func TestReadDurableRequiresEOF(t *testing.T) {
	for _, suffix := range []string{"}", "]", "{}", "true"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "record.json"), []byte(`{"version":1,"instance":"one"}`+suffix), 0600); err != nil {
				t.Fatal(err)
			}
			var record durableCancel
			if err := readDurable(dir, "record.json", maxDurableRecord, &record); err == nil {
				t.Fatal("accepted trailing JSON")
			}
		})
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "record.json"), []byte("{\"version\":1,\"instance\":\"one\"}\n \t"), 0600); err != nil {
		t.Fatal(err)
	}
	var record durableCancel
	if err := readDurable(dir, "record.json", maxDurableRecord, &record); err != nil {
		t.Fatal(err)
	}
}

// This forces the real original child to the exited-but-unreaped frontier
// before identity capture. Its actual Wait and output closure determine success.
func TestDurableAlreadyExitedChild(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.Argv = []string{"/bin/sh", "-c", "exit 0"}
	f.writeIntent()
	log, err := os.OpenFile(filepath.Join(f.logs, f.id+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader, ready, err := os.Pipe()
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	defer reader.Close()
	defer ready.Close()
	captured := false
	err = superviseWithIdentity(filepath.Join(f.state, f.id), log, ready, func(cmd *exec.Cmd) (processIdentity, error) {
		if err := waitDefaultTerminal(cmd); err != nil {
			return processIdentity{}, err
		}
		if cmd.ProcessState != nil {
			t.Error("original child was reaped before capture")
		}
		captured = true
		return identifyProcess(cmd.Process.Pid)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !captured {
		t.Fatal("child identity frontier was not reached")
	}
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil || line != "ok\n" {
		t.Fatalf("ready = %q, %v", line, err)
	}
	b := f.backend()
	defer b.Close()
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Succeeded || s.ExitCode == nil || *s.ExitCode != 0 {
		t.Fatalf("completed fast child = %+v, %v", s, err)
	}
}

// The process/Wait/output are real; only the final group-absence result and
// signal refusal are MODELED. Neither refusal may be credited as closure.
func TestDurableGroupClosureUncertain(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.Argv = []string{"/bin/sh", "-c", "printf completed"}
	in := f.writeIntent()
	log, err := os.OpenFile(filepath.Join(f.logs, f.id+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	cmd := exec.Command(f.task.Argv[0], f.task.Argv[1:]...)
	cmd.Stdout, cmd.Stderr = writer, writer
	configure(cmd)
	if err := cmd.Start(); err != nil {
		reader.Close()
		writer.Close()
		log.Close()
		t.Fatal(err)
	}
	writer.Close()
	s := &supervisor{dir: filepath.Join(f.state, f.id), intent: in, cmd: cmd,
		out: &output{file: log, marker: filepath.Join(f.logs, f.id+".truncated")},
		killGroup: func(original *exec.Cmd) error {
			if original != cmd || original.ProcessState != nil {
				t.Error("group signal did not use the same unreaped original")
			}
			return errors.New("injected signal refusal")
		},
		groupAbsent: func(original *exec.Cmd) error {
			if original != cmd || original.ProcessState == nil {
				t.Error("group probe preceded original Wait")
			}
			return errors.New("injected absence uncertainty")
		},
	}
	if err := s.wait(reader); err != nil {
		t.Fatal(err)
	}
	var status durableStatus
	if err := readDurable(s.dir, durableStatusFile, maxDurableRecord, &status); err != nil {
		t.Fatal(err)
	}
	if status.State != backend.Unknown || status.GroupClosed || !strings.Contains(status.Reason, "signal refusal") || !strings.Contains(status.Reason, "absence uncertainty") {
		t.Fatalf("unsettled closure = %+v", status)
	}
	b := f.backend()
	defer b.Close()
	b.mu.Lock()
	reservations := b.durableReservationsLocked()
	b.mu.Unlock()
	if len(reservations) != 1 || !reservations[0].Valid {
		t.Fatalf("unknown original lost its reservation: %+v", reservations)
	}
}

func TestDurableReservationValidation(t *testing.T) {
	for _, name := range []string{"foreign terminal", "unreadable intent", "different allocation"} {
		t.Run(name, func(t *testing.T) {
			f := newDurableFixture(t, "0")
			in := f.writeIntent()
			valid := true
			switch name {
			case "foreign terminal":
				zero := int32(0)
				f.write(durableStatusFile, durableStatus{Version: durableVersion, Instance: "local-" + strings.Repeat("b", 32), State: backend.Succeeded, ExitCode: &zero, OutputComplete: true, GroupClosed: true})
			case "unreadable intent":
				valid = false
				if err := os.WriteFile(filepath.Join(f.state, f.id, durableIntentFile), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "different allocation":
				valid = false
				in.Task.CPUMillis = 1000
				f.write(durableIntentFile, in) // missing the original CPU allocation
			}
			b := f.backend()
			defer b.Close()
			b.mu.Lock()
			r := b.durableReservationsLocked()
			b.mu.Unlock()
			if len(r) != 1 || r[0].Valid != valid || valid && !reflect.DeepEqual(r[0].Task, f.task) {
				t.Fatalf("restored reservations = %+v", r)
			}
		})
	}
}

func TestDurableCooperativeDescendant(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.Argv = []string{"/bin/sh", "-c", "sleep 0.05 & wait; echo descendant-finished"}
	b := f.backend()
	defer b.Close()
	if err := b.Start(context.Background(), f.id, f.task); err != nil {
		t.Fatal(err)
	}
	status := f.await(b, f.ref(""))
	if status.State != backend.Succeeded {
		t.Fatalf("cooperative group = %+v", status)
	}
	var persisted durableStatus
	if err := readDurable(filepath.Join(f.state, f.id), durableStatusFile, maxDurableRecord, &persisted); err != nil || !persisted.GroupClosed {
		t.Fatalf("group closure = %+v, %v", persisted, err)
	}
}
