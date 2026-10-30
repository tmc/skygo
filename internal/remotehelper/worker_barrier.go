package remotehelper

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// The worker barrier holds a rank's task argv until the control plane
// delivers the rank's start token, the lowercase hex of
// backend.WorkerStartDigest over the rank's own job, attempt, mode, rank,
// world size, Pod UID and task digest. The kubelet projects the Pod's start
// annotation into a file; the helper never contacts the network and holds
// no credential.
//
// Only the exact 64-byte token runs argv. An empty file waits; anything
// else exits without running argv. Before exec the helper writes nothing to
// standard output or error, so the rank's log begins with argv's output.
// Exit codes are diagnostics, never proof that argv did not run.

var (
	errWorkerRefused = errors.New("worker start token refused")
	errWorkerExpired = errors.New("worker start wait expired")
)

// workerWaitLimit bounds one helper's wait for its start token. It exceeds
// Control's barrier timeout, which decides the group: the helper's own limit
// only ends a wait that Control abandoned.
const workerWaitLimit = 30 * time.Minute

// workerExitRefused is the exit code of a refused start token.
const workerExitRefused = 122

// workerPaths are the barrier's fixed files.
type workerPaths struct {
	prepared, start, termination string
}

func defaultWorkerPaths() workerPaths {
	return workerPaths{
		prepared:    "/run/skygo/worker-state/prepared",
		start:       "/run/skygo/worker/start",
		termination: "/dev/termination-log",
	}
}

// workerFile is one original barrier file. Operations are local to one helper;
// tests wrap actual files without replacing another invocation's operations.
type workerFile interface {
	io.Reader
	io.Writer
	io.Closer
	Stat() (os.FileInfo, error)
	Sync() error
}

type workerOps struct {
	open   func(string, int, os.FileMode) (workerFile, error)
	rename func(string, string) error
	run    func(string, []string, []string) error
}

func systemWorkerOps() workerOps {
	return workerOps{
		open: func(name string, flag int, mode os.FileMode) (workerFile, error) {
			return os.OpenFile(name, flag, mode)
		},
		rename: os.Rename,
		run:    execute,
	}
}

// workerIdentity is one rank's identity from its environment.
type workerIdentity struct {
	rank  uint8
	uid   string
	token string
}

// workerEnv returns the rank's identity. Each of SKYGO_JOB_ID,
// SKYGO_ATTEMPT_ID, SKYGO_WORKER_MODE, RANK, WORLD_SIZE, SKYGO_POD_UID and
// SKYGO_TASK_SHA256 is required exactly once.
func workerEnv(env []string) (workerIdentity, error) {
	values := make(map[string]string)
	for _, entry := range env {
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		switch key {
		case "SKYGO_JOB_ID", "SKYGO_ATTEMPT_ID", "SKYGO_WORKER_MODE", "RANK", "WORLD_SIZE", "SKYGO_POD_UID", "SKYGO_TASK_SHA256":
			if _, dup := values[key]; dup {
				return workerIdentity{}, errConfiguration
			}
			values[key] = val
		}
	}
	job, attempt, uid := values["SKYGO_JOB_ID"], values["SKYGO_ATTEMPT_ID"], values["SKYGO_POD_UID"]
	if !lowerHex(job, 32) || !lowerHex(attempt, 32) || !workerUID(uid) || !lowerHex(values["SKYGO_TASK_SHA256"], 64) {
		return workerIdentity{}, errConfiguration
	}
	mode := backend.WorkerMode(values["SKYGO_WORKER_MODE"])
	if mode != backend.WorkerIndependent && mode != backend.WorkerTCP {
		return workerIdentity{}, errConfiguration
	}
	world, ok := canonicalUint(values["WORLD_SIZE"])
	if !ok || world < 2 || world > backend.MaxWorkers {
		return workerIdentity{}, errConfiguration
	}
	rank, ok := canonicalUint(values["RANK"])
	if !ok || rank >= world {
		return workerIdentity{}, errConfiguration
	}
	var task [32]byte
	if _, err := hex.Decode(task[:], []byte(values["SKYGO_TASK_SHA256"])); err != nil {
		return workerIdentity{}, errConfiguration
	}
	t := backend.WorkerStartDigest(job, attempt, mode, uint8(rank), uint8(world), uid, task)
	return workerIdentity{rank: uint8(rank), uid: uid, token: hex.EncodeToString(t[:])}, nil
}

func canonicalUint(s string) (uint64, bool) {
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil && strconv.FormatUint(n, 10) == s
}

// workerUID reports whether uid is 1..128 printable bytes without spaces.
func workerUID(uid string) bool {
	if len(uid) == 0 || len(uid) > 128 {
		return false
	}
	for _, c := range []byte(uid) {
		if c <= ' ' || c > '~' {
			return false
		}
	}
	return true
}

// readSmall reads at most limit bytes of name and refuses a longer file. An
// uncertain Close of the original file is joined to the primary error, and
// no content is returned with it.
func readSmall(name string, limit int64, ops workerOps) (content string, err error) {
	f, err := ops.open(name, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer func() {
		if e := closeOwned(f); e != nil {
			content, err = "", errors.Join(err, e)
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errWorkerRefused
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > limit {
		return "", errWorkerRefused
	}
	return string(data), nil
}

// workerDecision is the barrier's reading of one start file.
type workerDecision uint8

const (
	workerWait workerDecision = iota
	workerRelease
	workerRefuse
)

// decideWorker reads the start file content raw for id: empty waits, the
// exact token releases, and anything else, including a trailing newline or
// upper case, refuses.
func decideWorker(raw string, id workerIdentity) workerDecision {
	switch raw {
	case "":
		return workerWait
	case id.token:
		return workerRelease
	}
	return workerRefuse
}

// workerBarrier publishes the prepared marker, waits for the rank's start
// token and then calls run with argv. It returns errWorkerRefused without
// calling run for any other start content, or run's error. It returns
// without calling run once ctx is done or the wait has expired, and without
// publishing the marker if either already holds, even when the token is
// present. The wait expires at ctx's deadline or after workerWaitLimit,
// whichever is earlier.
//
// The launch decision is the check of ctx and the deadline immediately
// after the token is read and before run. A cancellation or expiry after
// that check is not observed: in production run is syscall.Exec, and
// nothing makes cancellation atomic with it. Control never relies on the
// helper to stop a delivered start.
func workerBarrier(ctx context.Context, argv, env []string, p workerPaths, ops workerOps) error {
	if len(argv) == 0 || argv[0] == "" {
		return errArguments
	}
	id, err := workerEnv(env)
	if err != nil {
		return err
	}
	limit := time.Now().Add(workerWaitLimit)
	if err := workerLive(ctx, limit); err != nil {
		return err
	}
	if err := writeWorkerFile(p.prepared, id.uid, ops); err != nil {
		return err
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		raw, err := readSmall(p.start, 64, ops)
		if err != nil {
			return err
		}
		switch decideWorker(raw, id) {
		case workerRelease:
			if err := workerLive(ctx, limit); err != nil {
				return err
			}
			return ops.run(".", argv, workerRuntimeEnv(env, id.rank))
		case workerRefuse:
			return errWorkerRefused
		}
		if err := workerLive(ctx, limit); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// workerRuntimeEnv sets the task rank from the validated original identity.
// Other variables, including an intentional NUM_NODES override, are unchanged.
func workerRuntimeEnv(env []string, rank uint8) []string {
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key != "SKYPILOT_NODE_RANK" {
			out = append(out, entry)
		}
	}
	return append(out, "SKYPILOT_NODE_RANK="+strconv.Itoa(int(rank)))
}

// workerLive reports why the barrier may no longer launch: ctx is done, or
// the earlier of ctx's deadline and limit has passed. It reads the deadline
// itself, because a context reports its expiry only once its timer fires.
func workerLive(ctx context.Context, limit time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d, ok := ctx.Deadline(); ok && d.Before(limit) {
		limit = d
	}
	if !time.Now().Before(limit) {
		return errWorkerExpired
	}
	return nil
}

// writeWorkerFile publishes content as name through a fresh file in the
// same directory and a rename, so a probe never reads a partial marker.
func writeWorkerFile(name, content string, ops workerOps) error {
	tmp := filepath.Join(filepath.Dir(name), "."+filepath.Base(name)+".tmp")
	f, err := ops.open(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		return err
	}
	n, err := io.WriteString(f, content)
	if err == nil && n != len(content) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, closeOwned(f)); err != nil {
		return err
	}
	return ops.rename(tmp, name)
}

// workerReady is the rank's readiness probe: it succeeds only when the
// prepared marker holds exactly this rank's Pod UID.
func workerReady(env []string, p workerPaths, ops workerOps) error {
	id, err := workerEnv(env)
	if err != nil {
		return err
	}
	got, err := readSmall(p.prepared, 128, ops)
	if err != nil {
		return err
	}
	if got != id.uid {
		return errConfiguration
	}
	return nil
}

// workerInstall copies this helper to the worker helper volume after
// checking its digest, for the rank's task container to run.
func workerInstall(env []string, p paths) error {
	var want string
	seen := false
	for _, entry := range env {
		key, val, ok := strings.Cut(entry, "=")
		if ok && key == "SKYGO_DATA_HELPER_SHA256" {
			if seen {
				return errConfiguration
			}
			want, seen = val, true
		}
	}
	if !lowerHex(want, 64) {
		return errConfiguration
	}
	return copyHelper(p, want)
}

// workerExit is the process exit code of a barrier error.
func workerExit(err error) int {
	if errors.Is(err, errWorkerRefused) {
		return workerExitRefused
	}
	return 1
}

// workerReport writes only fixed diagnostics. Its original write/Close errors
// are returned to the helper's held failure, never printed to stdout or stderr.
func workerReport(p workerPaths, err error, ops workerOps) error {
	msg := "skygo-worker: failed\n"
	switch {
	case errors.Is(err, errWorkerRefused):
		msg = "skygo-worker: start token refused\n"
	case errors.Is(err, errWorkerExpired), errors.Is(err, context.DeadlineExceeded):
		msg = "skygo-worker: start wait expired\n"
	}
	f, err := ops.open(p.termination, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	n, writeErr := io.WriteString(f, msg)
	if writeErr == nil && n != len(msg) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, closeOwned(f))
}
