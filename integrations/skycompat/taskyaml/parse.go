package taskyaml

import (
	"io"

	core "github.com/tmc/skygo/skycompat/taskyaml"
)

// The document and error types are shared with the core parser.
type (
	Options     = core.Options
	Document    = core.Document
	Task        = core.Task
	Resources   = core.Resources
	Accelerator = core.Accelerator
	Autostop    = core.Autostop
	JobRecovery = core.JobRecovery
	Quantity    = core.Quantity
	Position    = core.Position
	Code        = core.Code
	Error       = core.Error
)

// Error categories are shared with the core parser.
const (
	Invalid     = core.Invalid
	Unsupported = core.Unsupported
	Limit       = core.Limit
)

// Parse reads one task using the core parser and its fixed limits.
func Parse(r io.Reader, opts Options) (*Document, error) { return core.Parse(r, opts) }

// ParseDAG reads a name header and one task using the core parser.
func ParseDAG(r io.Reader, opts Options) (*Document, error) { return core.ParseDAG(r, opts) }
