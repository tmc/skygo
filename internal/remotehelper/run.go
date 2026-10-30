package remotehelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const usage = "usage: skygo-data stage | exec -- argv... | collect -- root... | install | worker -- argv... | worker-ready\n"

var errArguments = errors.New("invalid arguments")
var errConfiguration = errors.New("invalid configuration")

// A helper process executes one operation. An uncertain original Close retains
// its resource/error frame for that process rather than allowing another run.
type helperFailure struct {
	sync.Mutex
	err error
}

var processFailure helperFailure

type closeFailure struct {
	owner io.Closer
	cause error
}

func (e *closeFailure) Error() string { return "original close remains uncertain" }
func (e *closeFailure) Unwrap() error { return e.cause }
func closeOwned(c io.Closer) error {
	if err := c.Close(); err != nil {
		return &closeFailure{c, err}
	}
	return nil
}

type paths struct{ workspace, helper, self, input, output, instance, artifacts string }

var productionPaths = paths{"/workspace", "/opt/skygo", "/skygo-data", "/run/skygo/input", "/run/skygo/output",
	"/run/skygo/instance", "/run/skygo/artifacts"}

// Run executes the helper command and writes only fixed safe diagnostics.
// It is intended as the final command invocation in a helper process. An
// uncertain original resource is retained and subsequent operations refuse.
func Run(ctx context.Context, args, env []string, stdout, stderr io.Writer) int {
	return runHelper(ctx, args, env, stdout, stderr, productionPaths, defaultWorkerPaths(), systemWorkerOps(), &processFailure)
}

// runHelper is the same dispatcher with invocation-local files and operations.
// The public entry point supplies its fixed paths and actual process owners.
func runHelper(ctx context.Context, args, env []string, stdout, stderr io.Writer, p paths, wp workerPaths, ops workerOps, failure *helperFailure) int {
	if len(args) == 1 && args[0] == "--help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if err := grammar(args); err != nil {
		if !workerVerb(args[0]) {
			fmt.Fprintln(stderr, "skygo-data: invalid arguments")
		}
		return 2
	}
	failure.Lock()
	held := failure.err
	failure.Unlock()
	if held != nil {
		if !workerVerb(args[0]) {
			fmt.Fprintln(stderr, "skygo-data: original resource remains held")
		}
		return 1
	}
	err := runWithWorker(ctx, args, env, p, wp, ops)
	if err == nil {
		return 0
	}
	worker := workerVerb(args[0])
	if worker && args[0] != "worker-ready" {
		err = errors.Join(err, workerReport(wp, err, ops))
	}
	// Transport Close errors are sticky inside client; retain the entire returned
	// original frame on any failure, with no formatting of bearer or request data.
	failure.Lock()
	failure.err = errors.Join(failure.err, err)
	failure.Unlock()
	if worker {
		return workerExit(err)
	}
	switch {
	case errors.Is(err, errIdentity):
		fmt.Fprintln(stderr, "skygo-data: instance identity differs")
		return identityExit
	case errors.Is(err, errSealed):
		fmt.Fprintln(stderr, "skygo-data: instance already released")
		return sealedExit
	case errors.Is(err, errConfiguration):
		fmt.Fprintln(stderr, "skygo-data: invalid configuration")
	default:
		fmt.Fprintln(stderr, "skygo-data: operation failed")
	}
	return 1
}

func workerVerb(verb string) bool {
	return verb == "install" || verb == "worker" || verb == "worker-ready"
}

func grammar(args []string) error {
	switch args[0] {
	case "install", "worker-ready":
		if len(args) == 1 {
			return nil
		}
	case "worker":
		if len(args) >= 3 && args[1] == "--" && args[2] != "" {
			return nil
		}
	case "stage":
		if len(args) == 1 {
			return nil
		}
	case "exec":
		if len(args) >= 3 && args[1] == "--" && args[2] != "" {
			return nil
		}
	case "collect":
		if len(args) >= 3 && len(args) <= 18 && args[1] == "--" {
			return nil
		}
	case "env-put":
		if len(args) == 5 {
			return nil
		}
	case "env-wait":
		if len(args) >= 3 && len(args) <= 2+maxEnvArtifacts && args[1] == "--" {
			return nil
		}
	case "env-release":
		if len(args) >= 4 && len(args) <= 3+maxEnvArtifacts && args[2] == "--" {
			return nil
		}
	}
	return errArguments
}
func run(ctx context.Context, args, env []string, p paths) error {
	return runWithWorker(ctx, args, env, p, defaultWorkerPaths(), systemWorkerOps())
}

func runWithWorker(ctx context.Context, args, env []string, p paths, wp workerPaths, ops workerOps) error {
	if err := grammar(args); err != nil {
		return err
	}
	switch args[0] {
	case "install":
		return workerInstall(env, p)
	case "worker":
		return workerBarrier(ctx, args[2:], env, wp, ops)
	case "worker-ready":
		return workerReady(env, wp, ops)
	case "exec":
		return execute(filepath.Join(p.workspace, "work"), args[2:], env)
	case "env-put":
		return putArtifact(args[1:], os.Stdin, p)
	case "env-wait":
		return waitArtifacts(ctx, args[2:], p)
	case "env-release":
		return releaseArtifacts(ctx, args[1], args[3:], p)
	}
	timeout := 600 * time.Second
	if args[0] == "collect" {
		timeout = 606120 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	values := make(map[string]string)
	for _, entry := range env {
		key, val, ok := strings.Cut(entry, "=")
		if ok && strings.HasPrefix(key, "SKYGO_DATA_") {
			if _, exists := values[key]; exists {
				return errConfiguration
			}
			values[key] = val
		}
	}
	tokenDir := p.input
	if args[0] == "collect" {
		tokenDir = p.output
	}
	token, err := readToken(tokenDir)
	if err != nil {
		return errors.Join(errConfiguration, err)
	}
	c, err := newClient(values["SKYGO_DATA_ORIGIN"], values["SKYGO_DATA_CA_PEM"], token)
	if err != nil {
		return errors.Join(errConfiguration, err)
	}
	defer c.Close()
	if args[0] == "stage" {
		return stage(ctx, c, values, p)
	}
	return collect(ctx, c, args[2:], p)
}
func readToken(name string) (token string, err error) {
	root, err := os.OpenRoot(name)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, closeOwned(root)) }()
	f, err := root.Open("token")
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, closeOwned(f)) }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("invalid token file")
	}
	data, err := io.ReadAll(io.LimitReader(f, 115))
	if err != nil {
		return "", err
	}
	if !validToken(string(data)) {
		return "", errConfiguration
	}
	return string(data), nil
}
