// Package artifact defines immutable snapshot references.
package artifact

import "errors"

// Ref identifies one immutable snapshot. It grants no ownership or read authority.
// The zero value is invalid. ID and SHA256 use lowercase hexadecimal encoding.
type Ref struct {
	ID     string `json:"id" yaml:"id"`
	SHA256 string `json:"sha256" yaml:"sha256"`
	Size   int64  `json:"size" yaml:"size"`
}

// Validate checks Ref's canonical shape and encoded size, without filesystem I/O
// or checking the referenced bytes, archive format, ownership, or availability.
func (r Ref) Validate() error {
	if !lowerHex(r.ID, 32) {
		return errors.New("invalid artifact id")
	}
	if !lowerHex(r.SHA256, 64) {
		return errors.New("invalid artifact sha256")
	}
	if r.Size < 1 || r.Size > 32<<20 {
		return errors.New("artifact size outside 1..33554432 bytes")
	}
	return nil
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
