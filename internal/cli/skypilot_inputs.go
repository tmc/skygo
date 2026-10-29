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
	opts, env, err := in.options(opts)
	if err != nil {
		return nil, err
	}
	if in.workdirRef != "" {
		ref, err := readArtifactRef(in.workdirRef)
		if err != nil {
			return nil, errors.New("invalid --workdir-ref")
		}
		opts.Workdir = wireArtifactRef(ref)
	}
	return lowerTaskEnv(w, b, opts, env)
}

// options validates the same explicit inputs before workspace preparation.
func (in skypilotInputs) options(opts tasklower.Options) (tasklower.Options, map[string]string, error) {
	env := make(map[string]string, len(in.env))
	for _, entry := range in.env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			return opts, nil, errors.New("--env requires KEY=VALUE")
		}
		if _, exists := env[name]; exists {
			return opts, nil, errors.New("--env contains a duplicate key")
		}
		env[name] = value
	}
	if in.outputs != nil {
		opts.Outputs = in.outputs
	}
	if opts.Shell == "" {
		opts.Shell = defaultShell
	}
	opts.AllowDrop = metadataFields
	return opts, env, nil
}
