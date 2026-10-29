package servingcontrol

import "github.com/tmc/skygo/internal/store"

// ErrNativeAdvanced is errNativeAdvanced, for tests.
var ErrNativeAdvanced = errNativeAdvanced

// SetCommit replaces c's CommitService so that tests can inject faults.
// It must not race with a pass.
func SetCommit(c *Controller, commit func(string, uint64, store.ServiceCommit) (store.Service, error)) {
	c.gate <- struct{}{}
	c.commit = commit
	<-c.gate
}
