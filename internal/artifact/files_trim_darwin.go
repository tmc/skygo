//go:build darwin

package artifact

import (
	"errors"
	"os"
)

// trimAllocation releases allocation that APFS speculatively retains past the
// logical end of a file grown by delayed appends. On the same open file it
// grows the length by one byte and shrinks it back to size; a same-size
// truncate alone, or a hole punch past the end, was measured not to release it.
// It never rewrites, copies or renames the payload, and it is not proof that
// blocks were released: only a later physical sample can show that.
//
// A failed or interrupted shrink can leave one scratch byte past size. That
// byte is never accepted payload; only openScratchPart, the exact single-link
// part repair used by RecoverPart, sealPart and RemovePart, admits it.
func trimAllocation(file *os.File, size int64, truncate func(*os.File, int64) error) error {
	if size < 0 || size > MaxSize {
		return ErrFileConflict
	}
	if err := truncate(file, size+1); err != nil {
		// A failed grow may still have changed the length; restore it.
		return errors.Join(err, truncate(file, size))
	}
	return truncate(file, size)
}
