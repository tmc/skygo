//go:build unix

package gce_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/gcp"
	"golang.org/x/oauth2"
)

const exampleConfig = `{
  "project": "skygo-test",
  "compute_project_id": "123456789012",
  "zone": "us-central1-a",
  "machine_type": "zones/us-central1-a/machineTypes/t2a-standard-1",
  "subnet": "projects/skygo-net/regions/us-central1/subnetworks/default",
  "image": {"project": "debian-cloud", "name": "debian-12-arm64-v20261001", "id": "4242"},
  "disk_type": "zones/us-central1-a/diskTypes/pd-balanced",
  "disk_gib": 20,
  "ssh_user": "skygo",
  "authorized_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
}`

// unsettled keeps, until the process exits, each directory whose owner
// did not positively settle, with that owner: a Machine whose Close
// failed or a constructor error, either of which may keep a file and the
// state lock.
var unsettled []kept

type kept struct {
	dir string
	m   *gce.Machine
	err error
}

// retire removes dir only if err, the result of m's Close or of the
// constructor that made no Machine, is nil. Otherwise it keeps dir, m
// and err.
func retire(dir string, m *gce.Machine, err error) {
	if err != nil {
		unsettled = append(unsettled, kept{dir, m, err})
		return
	}
	os.RemoveAll(dir)
}

// newExample creates a Machine in a new temporary directory. On failure
// it prints the error and keeps any owner.
func newExample() (*gce.Machine, string, bool) {
	dir, err := os.MkdirTemp("", "gce-example")
	if err != nil {
		fmt.Println(err)
		return nil, "", false
	}
	cfg, err := gce.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		retire(dir, nil, nil) // nothing was created in dir
		fmt.Println(err)
		return nil, "", false
	}
	m, err := gce.Create(filepath.Join(dir, "state"), cfg)
	if err != nil {
		retire(dir, nil, err)
		fmt.Println(err)
		return nil, "", false
	}
	return m, dir, true
}

// closeExample closes m and retires dir only after Close succeeds.
func closeExample(m *gce.Machine, dir string) {
	err := m.Close()
	if err != nil {
		fmt.Println(err)
	}
	retire(dir, m, err)
}

func ExampleParseConfig() {
	cfg, err := gce.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(cfg.Zone, cfg.Image.Name, cfg.DiskGiB)
	// Output: us-central1-a debian-12-arm64-v20261001 20
}

func ExampleCreate() {
	m, dir, ok := newExample()
	if !ok {
		return
	}
	defer closeExample(m, dir)
	st, err := gce.ReadStatus(filepath.Join(dir, "state"))
	fmt.Println(st.Desired, st.Reached, st.InstanceID == "", err)
	// Output: running false true <nil>
}

func ExampleOpen() {
	cfg, err := gce.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	m, err := gce.Open("/nonexistent/state", cfg) // locks nothing: no such directory
	fmt.Println(m == nil, err != nil)
	// Output: true true
}

func ExampleReadStatus() {
	_, err := gce.ReadStatus("/nonexistent/state")
	fmt.Println(err != nil)
	// Output: true
}

func ExampleMachine_SetGoal() {
	m, dir, ok := newExample()
	if !ok {
		return
	}
	defer closeExample(m, dir)
	fmt.Println(m.SetGoal(gce.Absent))
	fmt.Println(m.SetGoal(gce.Running))
	// Output:
	// <nil>
	// gce: goal absent is latched
}

// ExampleMachine_Drive shows the call shape only: an expired token is
// refused before any request.
func ExampleMachine_Drive() {
	m, dir, ok := newExample()
	if !ok {
		return
	}
	defer closeExample(m, dir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tok := oauth2.Token{AccessToken: "example", TokenType: "Bearer", Expiry: time.Now().Add(-time.Hour)}
	st, err := m.Drive(ctx, tok, gce.DriveOptions{Steps: 10, Interval: 5 * time.Second})
	fmt.Println(st.Reached, st.Held, err)
	// Output: false token-expiring <nil>
}

// ExampleMachine_Refresh observes a machine without changing it. Nothing
// was created yet, so Refresh reports that without any request, even with
// an expired token.
func ExampleMachine_Refresh() {
	m, dir, ok := newExample()
	if !ok {
		return
	}
	defer closeExample(m, dir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tok := oauth2.Token{AccessToken: "example", TokenType: "Bearer", Expiry: time.Now().Add(-time.Hour)}
	st, err := m.Refresh(ctx, tok)
	fmt.Println(st.Desired, st.Reached, st.Held, err)
	// Output: running false not-created <nil>
}

func ExampleMachine_Close() {
	var m *gce.Machine
	fmt.Println(m.Close())
	// Output: <nil>
}

// TestExampleEarlyReturns shows the examples' failure paths keep what
// they cannot show settled: a constructor error or a failed Close keeps
// its directory and owner past the example, and only a nil result retires
// the directory. NOT_RUN by the author.
func TestExampleEarlyReturns(t *testing.T) {
	errOwner := errors.New("injected unsettled owner")
	for _, tt := range []struct {
		name string
		m    *gce.Machine
		err  error
	}{
		{"constructor error", nil, errOwner},
		{"close error", new(gce.Machine), errOwner},
		{"settled", nil, nil},
	} {
		dir, err := os.MkdirTemp("", "gce-example")
		if err != nil {
			t.Fatal(err)
		}
		n := len(unsettled)
		retire(dir, tt.m, tt.err)
		_, serr := os.Stat(dir)
		switch {
		case tt.err == nil && !os.IsNotExist(serr):
			t.Errorf("%s: directory not retired", tt.name)
		case tt.err != nil && (serr != nil || len(unsettled) != n+1 || unsettled[n].err != tt.err || unsettled[n].m != tt.m):
			t.Errorf("%s: owner or directory not kept", tt.name)
		}
	}
	// A real constructor failure: Create refuses a directory it did not
	// make, and the example path keeps it rather than guess.
	dir, err := os.MkdirTemp("", "gce-example")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := gce.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		t.Fatal(err)
	}
	m, err := gce.Create(dir, cfg)
	n := len(unsettled)
	retire(dir, m, err)
	if err == nil || m != nil || len(unsettled) != n+1 {
		t.Errorf("Create = %v, %v; want a kept constructor error", m, err)
	}
}
