package local

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/backend"
)

// A durable job directory, <state>/<id>, holds these records. The API writes
// the intent before launching a supervisor. The supervisor or a restarted API
// creates the claim exactly once by hard link; only a supervisor claim may
// launch the workload. The supervisor alone writes running and status.
const (
	durableVersion = 1

	durableIntentFile  = "intent.json"
	durableClaimFile   = "claim.json"
	durableRunningFile = "running.json"
	durableCancelFile  = "cancel.json"
	durableStatusFile  = "status.json"

	maxDurableIntent = 4 << 20
	maxDurableRecord = 64 << 10
)

// durableIntent is the API's launch request. Task is the original canonical
// accepted task; it is compared byte-for-byte with references and never
// mutated. Launch holds private placement decisions made for this execution.
type durableIntent struct {
	Version  int           `json:"version"`
	ID       string        `json:"id"`
	Instance string        `json:"instance"`
	Task     backend.Task  `json:"task"`
	Launch   durableLaunch `json:"launch"`
	Logs     string        `json:"logs"`
}

// durableLaunch records resources selected for one execution so a restarted
// server can reconstruct its reservation. Env is added to the workload
// environment after Task.Env; it is never written back to Task.
type durableLaunch struct {
	GPUs        []string          `json:"gpus,omitempty"`
	CPUMillis   int64             `json:"cpu_millis,omitempty"`
	MemoryBytes int64             `json:"memory_bytes,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
}

// durableClaim decides, once, whether a supervisor may launch the workload.
type durableClaim struct {
	Version    int              `json:"version"`
	Instance   string           `json:"instance"`
	Fenced     bool             `json:"fenced,omitempty"`
	Supervisor *processIdentity `json:"supervisor,omitempty"`
}

// durableRunning is written after the workload leader starts. The leader
// remains the supervisor's unreaped child until its group has been killed.
type durableRunning struct {
	Version    int             `json:"version"`
	Instance   string          `json:"instance"`
	Supervisor processIdentity `json:"supervisor"`
	Child      processIdentity `json:"child"`
}

type durableCancel struct {
	Version  int    `json:"version"`
	Instance string `json:"instance"`
}

// durableStatus is the supervisor's authoritative terminal record. It is
// written only after the leader is reaped and the log file is synced and closed.
type durableStatus struct {
	Version        int           `json:"version"`
	Instance       string        `json:"instance"`
	State          backend.State `json:"state"`
	Reason         string        `json:"reason,omitempty"`
	ExitCode       *int32        `json:"exit_code,omitempty"`
	OutputComplete bool          `json:"output_complete"`
	// GroupClosed means the original leader was reaped and its cooperative
	// process group was observed absent, or that no child was ever started.
	GroupClosed bool `json:"group_closed"`
}

func newDurableInstance() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "local-" + hex.EncodeToString(b[:]), nil
}

// checkPrivateDir requires a real directory, not a symlink, owned by this
// user with mode 0700.
func checkPrivateDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || !ownedByUser(info) {
		return fmt.Errorf("%s is not a private directory", dir)
	}
	return nil
}

// readDurable decodes one bounded private regular file. A missing file is
// reported with fs.ErrNotExist; anything else malformed is an error.
func readDurable(dir, name string, max int64, v any) error {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDONLY|noFollow, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !ownedByUser(info) {
		return fmt.Errorf("%s is not a private file", name)
	}
	if info.Size() > max {
		return fmt.Errorf("%s exceeds %d bytes", name, max)
	}
	data, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > max {
		return fmt.Errorf("%s exceeds %d bytes", name, max)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("decode %s: trailing data", name)
	}
	return nil
}

// writeTemp writes v to a new synced private file in dir and returns its name.
func writeTemp(dir string, v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	name := filepath.Join(dir, ".tmp-"+hex.EncodeToString(b[:]))
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|noFollow, 0600)
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// writeDurable atomically replaces dir/name with v.
func writeDurable(dir, name string, v any) error {
	tmp, err := writeTemp(dir, v)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", name, err)
	}
	return syncDir(dir)
}

// createDurable atomically creates dir/name with v, failing with fs.ErrExist
// if it already exists. Readers never see a partial record.
func createDurable(dir, name string, v any) error {
	tmp, err := writeTemp(dir, v)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	err = os.Link(tmp, filepath.Join(dir, name))
	os.Remove(tmp)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fs.ErrExist
		}
		return fmt.Errorf("create %s: %w", name, err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
