package store

import "time"

// SetClockForTest installs a clock before the Store is shared with concurrent
// requests. The caller owns synchronization of any state captured by now.
func SetClockForTest(s *Store, now func() time.Time) { s.now = now }
