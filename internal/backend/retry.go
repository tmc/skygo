package backend

import (
	"errors"
	"slices"
)

// RetryPolicy bounds attempts for explicitly selected failed exit codes.
// NormalizeRetry validates and copies input before it becomes accepted task data.
type RetryPolicy struct {
	MaxAttempts  uint32  `json:"max_attempts" yaml:"max_attempts"`
	DelaySeconds uint32  `json:"delay_seconds" yaml:"delay_seconds"`
	ExitCodes    []int32 `json:"exit_codes" yaml:"exit_codes"`
}

// NormalizeRetry returns an independent policy with sorted unique exit codes.
// Nil or a maximum of zero or one with no delay or codes means one attempt and
// returns nil. Enabled retry requires 2..10 attempts and 1..32 input codes, each
// in 1..255. A zero delay becomes one second; delays above 3600 seconds reject.
// It does not change p or prove that a backend supports retry.
func NormalizeRetry(p *RetryPolicy) (*RetryPolicy, error) {
	if p == nil {
		return nil, nil
	}
	if p.MaxAttempts <= 1 {
		if p.DelaySeconds != 0 || len(p.ExitCodes) != 0 {
			return nil, errors.New("retry delay or exit codes require multiple attempts")
		}
		return nil, nil
	}
	if p.MaxAttempts > 10 {
		return nil, errors.New("retry max_attempts must be between 2 and 10")
	}
	if p.DelaySeconds > 3600 {
		return nil, errors.New("retry delay_seconds must not exceed 3600")
	}
	if len(p.ExitCodes) == 0 || len(p.ExitCodes) > 32 {
		return nil, errors.New("retry exit_codes must contain 1..32 entries")
	}
	for _, code := range p.ExitCodes {
		if code < 1 || code > 255 {
			return nil, errors.New("retry exit codes must be between 1 and 255")
		}
	}
	normal := *p
	normal.ExitCodes = slices.Clone(p.ExitCodes)
	slices.Sort(normal.ExitCodes)
	normal.ExitCodes = slices.Compact(normal.ExitCodes)
	if normal.DelaySeconds == 0 {
		normal.DelaySeconds = 1
	}
	return &normal, nil
}

// CurrentAttemptID returns the last explicit attempt's ID. Default and legacy
// jobs have no explicit history and use the logical job's ID for execution.
func (j Job) CurrentAttemptID() string {
	if len(j.Attempts) == 0 {
		return j.ID
	}
	return j.Attempts[len(j.Attempts)-1].ID
}
