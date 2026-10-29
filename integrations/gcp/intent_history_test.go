//go:build unix

package gce

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// These fixtures were emitted by the frozen version 1 production
// create/save writer, through its real filesystem and local Compute
// model. They are not version 2 records with their header rewritten.
// Keep this provenance distinct from the modeled corruption tests.
func TestLegacyGoalIntents(t *testing.T) {
	for _, tt := range []struct {
		name, sha string
	}{
		{"reached", "1b095ccdfc1c40afee5b747db67e9163f41392a0206ce0b5b12640a8835522ae"},
		{"unreached-after-start", "dc8c75bfd014b544c128008952b7a6a9076209750697a647aeaf9abd52f5e05e"},
		{"lost-start", "61c563e28b8dbd7cb946a4be0d7588928f27b7f89a891b34c3c2caf347d9ccb3"},
		{"prepared-stop", "32b7ac89bf1f4cca2026a3a2b68db6ac290c05339f4fed815378b01e593b662b"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "history-v1", tt.name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			if hex.EncodeToString(sum[:]) != tt.sha {
				t.Fatal("the version 1 writer fixture changed")
			}
			var legacy record
			if err := json.Unmarshal(data, &legacy); err != nil || legacy.Version != 1 || legacy.Intent != 0 {
				t.Fatalf("legacy fixture: %+v, %v", legacy, err)
			}
			f := newFake(t)
			sr := newStateRoot(t)
			f.root = sr
			dir := filepath.Join(sr.dir, "state")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			// A real writer leaves its lock file when Close releases the
			// lock. Reconstruct that required, empty fixture file too.
			if err := os.WriteFile(filepath.Join(dir, lockName), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, stateName)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			m, err := open(dir, testConfig(), f.client())
			sr.own(t, f, m, err)
			if err != nil {
				t.Fatal(err)
			}
			r := m.rec
			if r.Version != stateVersion || r.Intent != 1 || (r.CompletedIntent == 1) != legacy.Reached {
				t.Fatalf("upgrade: version %d, intent %d, completed %d", r.Version, r.Intent, r.CompletedIntent)
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
				t.Fatalf("Open rewrote version 1 state: %v", err)
			}
			if r.DiskCreate == nil || r.InstanceCreate == nil {
				t.Fatal("the create witnesses were lost")
			}
			for _, op := range r.History {
				if op.Intent != 1 {
					t.Fatal("legacy history has another intent")
				}
			}
			if legacy.Op != nil && (r.Op == nil || r.Op.RequestID != legacy.Op.RequestID || r.Op.Intent != 1 || r.Op.State != legacy.Op.State) {
				t.Fatal("the unresolved legacy original changed")
			}
			switch tt.name {
			case "reached":
				if err := m.SetGoal(Running); err != nil {
					t.Fatal(err)
				}
				if r.Intent != 2 || r.CompletedIntent != 1 || r.Reached {
					t.Fatal("a completed legacy goal did not begin a fresh intent")
				}
			case "unreached-after-start":
				if err := m.SetGoal(Running); err != nil {
					t.Fatal(err)
				}
				if r.Intent != 1 || r.CompletedIntent != 0 || !m.lastDone(kindStart) {
					t.Fatal("history alone was used to invent completion")
				}
				// A new, different goal is explicit authority to progress,
				// while repeating the legacy unfinished one stays conservative.
				if err := m.SetGoal(Stopped); err != nil {
					t.Fatal(err)
				}
			case "lost-start":
				id := r.Op.RequestID
				if err := m.SetGoal(Stopped); err != nil {
					t.Fatal(err)
				}
				if st, err := drive(t, m, 10); err != nil || st.Held != heldUnresolved || f.mutations() != 0 || r.Op == nil || r.Op.RequestID != id {
					t.Fatalf("legacy unresolved start = %+v, %v", st, err)
				}
			case "prepared-stop":
				if err := m.SetGoal(Absent); err != nil {
					t.Fatal(err)
				}
			}
			written, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var upgraded record
			if err := json.Unmarshal(written, &upgraded); err != nil || upgraded.Version != stateVersion || upgraded.Intent != 2 {
				t.Fatalf("new writer did not store version 2: %+v, %v", upgraded, err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			m = reopen(t, f, dir)
			if m.rec.Intent != upgraded.Intent || m.rec.CompletedIntent != upgraded.CompletedIntent {
				t.Fatal("intent changed on fresh reopen")
			}
			// The historical-reader gate supplies a task-owned directory;
			// default tests only read their committed fixture corpus.
			if out := os.Getenv("SKYGO_GCE_HISTORY_V2_OUTPUT"); out != "" && tt.name == "reached" {
				if err := os.MkdirAll(out, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, stateName), written, 0o600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
