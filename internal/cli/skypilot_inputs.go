package cli

import (
	"errors"
	"io"
	"strings"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/tasklower"
)

// skypilotInputs contains explicit command inputs, never process environment.
// Workdir references name a previously uploaded snapshot; no upload is implicit.
type skypilotInputs struct {
	workdirRef string
	outputs    []string
	env        []string
}

func (in skypilotInputs) lower(w io.Writer, b []byte, opts tasklower.Options) (*skyv1.Task, error) {
	env := make(map[string]string, len(in.env))
	for _, entry := range in.env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, errors.New("--env requires KEY=VALUE")
		}
		if _, exists := env[name]; exists {
			return nil, errors.New("--env contains a duplicate key")
		}
		env[name] = value
	}
	if in.workdirRef != "" {
		ref, err := readArtifactRef(in.workdirRef)
		if err != nil {
			return nil, errors.New("invalid --workdir-ref")
		}
		opts.Workdir = wireArtifactRef(ref)
	}
	if in.outputs != nil {
		opts.Outputs = in.outputs
	}
	return lowerTaskEnv(w, b, opts, env)
}
