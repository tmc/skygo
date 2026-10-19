package taskyaml

import "fmt"

// Options supplies explicit environment overrides. The zero value selects
// strict fixed limits and never reads the process environment.
type Options struct {
	EnvOverrides map[string]string
}

// Document contains one task, optional DAG name and nonauthoritative original
// YAML provenance. Locations maps recognized field paths to source positions;
// a present null field has a location even when its typed value is nil.
type Document struct {
	DAGName    *string
	Task       Task
	SourceYAML *string
	Locations  map[string]Position
}

// Task preserves selected syntax, not permission or backend support. String
// pointers distinguish absent/null fields from empty strings. NumNodes defaults
// to one. FileMounts and Volumes contain unresolved string references.
type Task struct {
	Name, Run, Setup, Workdir *string
	Envs                      map[string]string
	Resources                 *Resources
	NumNodes                  int
	FileMounts, Volumes       map[string]string
}

// Resources preserves constraints without selecting or validating a provider.
// DiskSize is only accepted for the upstream unspecified default 256; it does
// not request 256 GiB. All fields still require semantic lowering.
type Resources struct {
	CPUs, Memory, DiskSize                     *Quantity
	Accelerators                               map[string]Quantity
	Cloud, Region, Zone, ImageID, InstanceType *string
	UseSpot                                    *bool
}

// Quantity preserves decimal text, source memory unit and an at-least suffix.
// An empty memory Unit means upstream GiB; CPU/accelerator units are empty.
type Quantity struct {
	Value, Unit string
	AtLeast     bool
}

// Position is a one-based YAML line and column. Zero means no source position,
// as for an invalid explicit option or a reader failure.
type Position struct{ Line, Column int }

// Code identifies a stable parse error category.
type Code string

const (
	// Invalid denotes malformed syntax or an invalid selected field value.
	Invalid Code = "invalid"
	// Unsupported denotes syntax outside the selected profile.
	Unsupported Code = "unsupported"
	// Limit denotes an input exceeding a fixed parser bound.
	Limit Code = "limit"
)

// Error identifies a category and source position without exposing input values.
// Path contains only known schema fields; arbitrary keys use their known parent.
type Error struct {
	Code         Code
	Path         string
	Line, Column int
}

// Error formats a safe diagnostic containing no user-supplied keys or values.
func (e *Error) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("task yaml: %s at %d:%d", e.Code, e.Line, e.Column)
	}
	return fmt.Sprintf("task yaml: %s %s at %d:%d", e.Code, e.Path, e.Line, e.Column)
}
