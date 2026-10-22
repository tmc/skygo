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

const usage = "usage: skygo-data stage | exec -- argv... | collect -- root...\n"

var errArguments = errors.New("invalid arguments")
var errConfiguration = errors.New("invalid configuration")

// A helper process executes one operation. An uncertain original Close retains
// its resource/error frame for that process rather than allowing another run.
var processFailure struct {
	sync.Mutex
	err error
}

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

type paths struct{ workspace, helper, self, input, output string }

var productionPaths = paths{"/workspace", "/opt/skygo", "/skygo-data", "/run/skygo/input", "/run/skygo/output"}

// Run executes the helper command and writes only fixed safe diagnostics.
// It is intended as the final command invocation in a helper process. An
// uncertain original resource is retained and subsequent operations refuse.
func Run(ctx context.Context, args, env []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "--help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if err := grammar(args); err != nil {
		fmt.Fprintln(stderr, "skygo-data: invalid arguments")
		return 2
	}
	processFailure.Lock()
	held := processFailure.err
	processFailure.Unlock()
	if held != nil {
		fmt.Fprintln(stderr, "skygo-data: original resource remains held")
		return 1
	}
	err := run(ctx, args, env, productionPaths)
	if err == nil {
		return 0
	}
	// Transport Close errors are sticky inside client; retain the entire returned
	// original frame on any failure, with no formatting of bearer or request data.
	processFailure.Lock()
	processFailure.err = errors.Join(processFailure.err, err)
	processFailure.Unlock()
	if errors.Is(err, errConfiguration) {
		fmt.Fprintln(stderr, "skygo-data: invalid configuration")
	} else {
		fmt.Fprintln(stderr, "skygo-data: operation failed")
	}
	return 1
}
func grammar(args []string) error {
	switch args[0] {
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
	}
	return errArguments
}
func run(ctx context.Context, args, env []string, p paths) error {
	if err := grammar(args); err != nil {
		return err
	}
	if args[0] == "exec" {
		return execute(filepath.Join(p.workspace, "work"), args[2:], env)
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
