//go:build linux || darwin

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// This table tests the pure warning predicate, not a physical sample or native
// qualification. Production callers always obtain their value from the real probe.
func TestPhysicalWarningPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		size, free            uint64
		complete, known, want bool
	}{
		{"small", 16 << 10, 8 << 30, true, true, false},
		{"admission", 256 << 20, 8 << 30, true, true, true},
		{"maintenance", 320 << 20, 8 << 30, true, true, true},
		{"low-free", 16 << 10, (2 << 30) - 1, true, true, true},
		{"unknown", 0, 0, true, false, false},
		{"incomplete", 320 << 20, 8 << 30, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := store.PhysicalInspection{ProbeComplete: tc.complete, GeometryStatus: "not_requested"}
			if tc.known {
				info.FileBytes = &tc.size
				info.FilesystemFreeBytes = &tc.free
			}
			got := physicalWarning(info)
			if (got != "") != tc.want {
				t.Fatalf("warning=%q", got)
			}
			if strings.Contains(got, "approved") || strings.Contains(got, "safe to") {
				t.Fatal("warning granted authority")
			}
		})
	}
}

func TestPhysicalAdvisoryQuietBeforeServerCallback(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	dir := filepath.Join(t.TempDir(), "state")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	probe, err := store.ProbePhysicalState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if probe.FilesystemFreeBytes == nil || *probe.FilesystemFreeBytes < 2<<30 {
		t.Fatal("quiet-path gate requires at least2GiB free")
	}
	path := filepath.Join(dir, "jobs.db")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(raw)
	calls := 0
	cmd := New(func(context.Context, ServerOptions) error {
		calls++
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if sha256.Sum256(raw) != before {
			return fmt.Errorf("advisory migrated state before callback")
		}
		return nil
	})
	var out, diag bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diag)
	cmd.SetArgs([]string{"server", "--data-dir", dir})
	if err = cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || out.Len() != 0 || diag.Len() != 0 {
		t.Fatalf("calls=%d stdout=%q stderr=%q", calls, out.String(), diag.String())
	}
}

func TestPhysicalInspectionScripts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected HTTP", 500) }))
	defer srv.Close()
	for _, file := range []string{"inspect", "missing"} {
		t.Run(file, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			e := scriptEngine(t)
			snapshots := map[string][32]byte{}
			e.Cmds["physical-init"] = script.Command(script.CmdUsage{Summary: "create real stopped Store fixture", Args: "directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if len(args) != 1 {
					return nil, script.ErrUsage
				}
				db, err := store.Open(s.Path(args[0]))
				if err != nil {
					return nil, err
				}
				return nil, db.Close()
			})
			for _, name := range []string{"physical-snapshot", "physical-unchanged"} {
				name := name
				e.Cmds[name] = script.Command(script.CmdUsage{Summary: "record or compare actual fixture bytes", Args: "directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
					if len(args) != 1 {
						return nil, script.ErrUsage
					}
					path := filepath.Join(s.Path(args[0]), "jobs.db")
					raw, err := os.ReadFile(path)
					if err != nil {
						return nil, err
					}
					sum := sha256.Sum256(raw)
					if name == "physical-snapshot" {
						snapshots[path] = sum
						return nil, nil
					}
					if before, ok := snapshots[path]; !ok || before != sum {
						return nil, fmt.Errorf("inspection changed actual database")
					}
					return nil, nil
				})
			}
			e.Cmds["physical-json"] = script.Command(script.CmdUsage{Summary: "decode actual complete CLI inspection output"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if len(args) != 0 {
					return nil, script.ErrUsage
				}
				var info store.PhysicalInspection
				d := json.NewDecoder(strings.NewReader(s.Stdout()))
				d.DisallowUnknownFields()
				if err := d.Decode(&info); err != nil {
					return nil, err
				}
				var extra any
				if err := d.Decode(&extra); err != io.EOF {
					return nil, fmt.Errorf("unexpected trailing CLI stdout")
				}
				if !info.ProbeComplete || info.GeometryStatus != "complete" || info.Jobs == nil || *info.Jobs != 0 || info.AddedLegacyIDPayloadLowerBound == nil || *info.AddedLegacyIDPayloadLowerBound != 0 {
					return nil, fmt.Errorf("actual inspection incomplete or wrong geometry")
				}
				return nil, nil
			})
			scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=not-read-by-local-command"}, "testdata/physical-inspection/"+file+".txt")
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("local inspection sent%d HTTP calls", calls.Load())
	}
}
