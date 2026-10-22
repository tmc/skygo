package ec2api

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrAccountMismatch reports credentials that belong to an account
	// other than the profile's.
	ErrAccountMismatch = errors.New("credentials belong to a different account")

	// ErrNotOwned reports an observed instance that is not exactly the
	// instance a create request made.
	ErrNotOwned = errors.New("instance is not owned")

	// ErrNotObserved reports a search that found no instance.
	// It is not evidence that no instance exists.
	ErrNotObserved = errors.New("no instance observed")
)

// A CreditMismatchError reports an owned instance whose CPU credit option
// differs from the profile's. The instance is still owned: stopping or
// terminating it remains allowed.
type CreditMismatchError struct {
	InstanceID string
	Want, Got  string
}

func (e *CreditMismatchError) Error() string {
	return fmt.Sprintf("ec2api: instance %s CPU credit option is %s, profile requires %s", e.InstanceID, e.Got, e.Want)
}

// A Class classifies a failed call.
type Class int

const (
	ClassOther Class = iota
	ClassThrottled
	ClassServerError
	ClassIdempotentParameterMismatch
	ClassInstanceNotFound
	ClassUnauthorized
	ClassInsufficientCapacity
	ClassTransport // no complete response was received and released
	ClassMalformed // the response was invalid or exceeded a bound
)

var classNames = [...]string{
	ClassOther:                       "other",
	ClassThrottled:                   "throttled",
	ClassServerError:                 "server error",
	ClassIdempotentParameterMismatch: "idempotent parameter mismatch",
	ClassInstanceNotFound:            "instance not found",
	ClassUnauthorized:                "unauthorized",
	ClassInsufficientCapacity:        "insufficient capacity",
	ClassTransport:                   "transport",
	ClassMalformed:                   "malformed response",
}

func (c Class) String() string {
	if c >= 0 && int(c) < len(classNames) {
		return classNames[c]
	}
	return fmt.Sprintf("Class(%d)", int(c))
}

// An Error reports a failed EC2 or STS call.
//
// The provider's error message is kept out of Error's text because it can
// carry encoded authorization details. It remains available through Unwrap
// for a caller that keeps it private.
type Error struct {
	Action     string // Query action, such as "RunInstances"
	Class      Class
	Code       string // provider error code, if the response carried one
	StatusCode int    // HTTP status, or 0 if no response arrived
	RequestID  string // provider request ID, if the response carried one

	// Sent reports whether the request may have reached the endpoint.
	// A failed mutation with Sent set may have taken effect.
	Sent bool

	provider *providerError // provider code and message, if any
	cause    error          // local, transport or close failure, if any
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ec2api: %s: %s", e.Action, e.Class)
	if e.Code != "" {
		fmt.Fprintf(&b, " (%s)", e.Code)
	}
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, " status %d", e.StatusCode)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&b, " request %s", e.RequestID)
	}
	if e.cause != nil {
		fmt.Fprintf(&b, ": %v", e.cause)
	}
	return b.String()
}

// Unwrap returns the provider error, whose text includes the provider's
// message, and the local cause, if any.
func (e *Error) Unwrap() []error {
	var errs []error
	if e.provider != nil {
		errs = append(errs, e.provider)
	}
	if e.cause != nil {
		errs = append(errs, e.cause)
	}
	return errs
}

// Retryable reports whether the failure is throttling, a server error, or a
// connection failure before any response status arrived that was not
// caused by the caller's context. Only a reissue of the identical request,
// with the same ClientToken and body, is safe; the decision to reissue
// belongs to the caller.
func (e *Error) Retryable() bool {
	switch e.Class {
	case ClassThrottled, ClassServerError:
		return true
	case ClassTransport:
		return e.StatusCode == 0 && !errors.Is(e.cause, context.Canceled) && !errors.Is(e.cause, context.DeadlineExceeded)
	}
	return false
}

// A providerError holds the provider's error code and message.
type providerError struct {
	code, message string
}

func (e *providerError) Error() string { return e.code + ": " + e.message }

// classify maps a provider error code and HTTP status to a Class.
func classify(status int, code string) Class {
	switch code {
	case "RequestLimitExceeded", "Throttling", "ThrottlingException":
		return ClassThrottled
	case "InternalError", "InternalFailure", "ServiceUnavailable", "Unavailable":
		return ClassServerError
	case "IdempotentParameterMismatch":
		return ClassIdempotentParameterMismatch
	case "InvalidInstanceID.NotFound":
		return ClassInstanceNotFound
	case "AuthFailure", "UnauthorizedOperation", "InvalidClientTokenId",
		"SignatureDoesNotMatch", "ExpiredToken", "AccessDenied", "OptInRequired":
		return ClassUnauthorized
	case "InsufficientInstanceCapacity", "InsufficientCapacity":
		return ClassInsufficientCapacity
	}
	if status >= 500 {
		return ClassServerError
	}
	return ClassOther
}
