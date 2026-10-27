package tasklower

import (
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
	core "github.com/tmc/skygo/skycompat/tasklower"
)

// Lowering options, notes and status values are shared with the core package.
type (
	Options = core.Options
	Status  = core.Status
	Note    = core.Note
)

// Field treatment is reported with the core status values.
const (
	Lowered = core.Lowered
	Default = core.Default
	Dropped = core.Dropped
)

// Lower delegates to the core lowerer and retains its typed field refusals.
func Lower(d *taskyaml.Document, opts Options) (*skyv1.Task, []Note, error) {
	return core.Lower(d, opts)
}

// Marshal delegates to the core canonical task-file encoder.
func Marshal(t *skyv1.Task) ([]byte, error) { return core.Marshal(t) }
