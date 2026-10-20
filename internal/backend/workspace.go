package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// ValidateWorkspace checks only Task's snapshot fields. It does not validate the
// task program, execution policy, artifact authority, or target capabilities.
// Workdir requires an empty Directory. Setup and Outputs require Workdir.
func ValidateWorkspace(t Task) error {
	if t.Workdir == nil {
		if len(t.Setup) != 0 || len(t.Outputs) != 0 {
			return errors.New("setup and outputs require workdir")
		}
		return nil
	}
	if err := t.Workdir.Validate(); err != nil {
		return fmt.Errorf("workdir: %w", err)
	}
	if t.Directory != "" {
		return errors.New("workdir requires an empty directory")
	}
	if len(t.Setup) > 64 {
		return errors.New("setup exceeds 64 arguments")
	}
	if len(t.Setup) > 0 && t.Setup[0] == "" {
		return errors.New("setup requires a program")
	}
	for _, arg := range t.Setup {
		if !utf8.ValidString(arg) || strings.ContainsRune(arg, 0) {
			return errors.New("invalid setup argument")
		}
	}
	if len(t.Setup) > 0 {
		data, err := json.Marshal(t.Setup)
		if err != nil {
			return err
		}
		if len(data) > 16<<10 {
			return errors.New("setup exceeds 16 KiB encoded")
		}
	}
	if len(t.Outputs) > 16 {
		return errors.New("outputs exceeds 16 roots")
	}
	for i, name := range t.Outputs {
		if name == "" || len(name) > 240 || !utf8.ValidString(name) || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\:\x00*?[]") {
			return errors.New("invalid output root")
		}
		parts := strings.Split(name, "/")
		if len(parts) > 16 {
			return errors.New("output root exceeds depth 16")
		}
		for _, part := range parts {
			if part == "" || part == "." || part == ".." {
				return errors.New("invalid output root component")
			}
		}
		for _, earlier := range t.Outputs[:i] {
			if name == earlier || strings.HasPrefix(name, earlier+"/") || strings.HasPrefix(earlier, name+"/") {
				return errors.New("overlapping output roots")
			}
		}
	}
	return nil
}

// CloneTask copies the task's mutable fields without normalization or rewrites.
func CloneTask(t Task) Task {
	t.Argv = slices.Clone(t.Argv)
	t.Env = maps.Clone(t.Env)
	t.TargetLabels = maps.Clone(t.TargetLabels)
	t.NodeSelector = maps.Clone(t.NodeSelector)
	if t.Retry != nil {
		retry := *t.Retry
		retry.ExitCodes = slices.Clone(retry.ExitCodes)
		t.Retry = &retry
	}
	if t.Workdir != nil {
		ref := *t.Workdir
		t.Workdir = &ref
	}
	t.Setup = slices.Clone(t.Setup)
	t.Outputs = slices.Clone(t.Outputs)
	return t
}
