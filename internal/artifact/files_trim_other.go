//go:build !darwin

package artifact

import "os"

// trimAllocation is a no-op: the speculative allocation it releases on Darwin
// has not been observed elsewhere, and no other allocation policy changes.
func trimAllocation(*os.File, int64, func(*os.File, int64) error) error { return nil }
