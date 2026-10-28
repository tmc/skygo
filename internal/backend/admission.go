package backend

import "errors"

// ErrResourcesUnavailable means the task fits the target, but other executions
// hold the resources it needs. Start may return this error only before starting
// an execution or creating its output. An ambiguous Start failure must not wrap
// this error: callers may defer this proven unstarted request for later admission.
var ErrResourcesUnavailable = errors.New("execution resources are reserved by other jobs")
