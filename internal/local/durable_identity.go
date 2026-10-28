package local

import (
	"errors"
	"fmt"
)

// processIdentity names one process incarnation: its PID, kernel start time
// and the boot it ran in. A PID alone may be reused and is never an identity.
type processIdentity struct {
	PID   int    `json:"pid"`
	Start uint64 `json:"start"`
	Boot  string `json:"boot"`
}

// errProcessGone reports that a process is absent or already exited.
var errProcessGone = errors.New("process is gone")

// identifyProcess returns the identity of a live process.
func identifyProcess(pid int) (processIdentity, error) {
	if pid <= 0 {
		return processIdentity{}, errors.New("invalid pid")
	}
	boot, err := bootIdentity()
	if err != nil {
		return processIdentity{}, err
	}
	start, err := processStart(pid)
	if err != nil {
		return processIdentity{}, err
	}
	return processIdentity{PID: pid, Start: start, Boot: boot}, nil
}

// alive reports whether exactly this incarnation is still running. It returns
// false with a nil error only on positive evidence that the incarnation has
// exited: another boot, an absent or exited PID, or a different start time.
// Any other failure is uncertainty and is returned as an error.
func (id processIdentity) alive() (bool, error) {
	if id.PID <= 0 || id.Start == 0 || id.Boot == "" {
		return false, errors.New("incomplete process identity")
	}
	boot, err := bootIdentity()
	if err != nil {
		return false, err
	}
	if boot != id.Boot {
		return false, nil
	}
	start, err := processStart(id.PID)
	if errors.Is(err, errProcessGone) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("identify pid %d: %w", id.PID, err)
	}
	return start == id.Start, nil
}
