package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"gopkg.in/yaml.v3"
)

// defaultShell runs SkyPilot run text when --shell is not given.
const defaultShell = "/bin/sh"

// metadataFields are the nonexecuting SkyPilot fields a task may carry.
// Lowering drops them by explicit request and says so on stderr.
var metadataFields = []string{"name", "_user_specified_yaml"}

// skypilotTask reports whether b holds SkyPilot task YAML rather than a skygo
// task file: a SkyPilot task has top-level run text, a skygo task has argv.
// It refuses a document with both. Anything it cannot classify is left to
// the skygo decoder, which reports its own error. A task decoder's error never
// triggers a fallback to the other format.
func skypilotTask(b []byte) (bool, error) {
	var n yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(b)).Decode(&n); err != nil || len(n.Content) != 1 || n.Content[0].Kind != yaml.MappingNode {
		return false, nil
	}
	var argv, run bool
	m := n.Content[0].Content
	for i := 0; i < len(m); i += 2 {
		if m[i].Kind != yaml.ScalarNode {
			continue
		}
		switch m[i].Value {
		case "argv":
			argv = true
		case "run":
			run = true
		}
	}
	if argv && run {
		return false, errors.New("task mixes skygo argv and SkyPilot run")
	}
	return run, nil
}

// lowerTask parses SkyPilot task YAML and lowers it to a skygo task.
// Parse and Lower errors are returned unchanged; they are *taskyaml.Error
// values for every refused field. Dropped metadata is reported to w.
func lowerTask(w io.Writer, b []byte, opts tasklower.Options) (*skyv1.Task, error) {
	return lowerTaskEnv(w, b, opts, nil)
}

func lowerTaskEnv(w io.Writer, b []byte, opts tasklower.Options, env map[string]string) (*skyv1.Task, error) {
	d, err := taskyaml.Parse(bytes.NewReader(b), taskyaml.Options{EnvOverrides: env})
	if err != nil {
		return nil, err
	}
	if opts.Shell == "" {
		opts.Shell = defaultShell
	}
	opts.AllowDrop = metadataFields
	task, notes, err := tasklower.Lower(d, opts)
	if err != nil {
		return nil, err
	}
	for _, n := range notes {
		if n.Status == tasklower.Dropped {
			if _, err := fmt.Fprintf(w, "task yaml: dropped %s: %s\n", n.Path, n.Detail); err != nil {
				return nil, fmt.Errorf("write task metadata: %w", err)
			}
		}
	}
	return task, nil
}
