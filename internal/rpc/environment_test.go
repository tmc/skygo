package rpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

func TestEnvironmentDomain(t *testing.T) {
	lineage := strings.Repeat("a1", 16)
	tests := []struct {
		name string
		in   *v1.RequestDomain
		ok   bool
	}{
		{"missing", nil, false},
		{"zero", &v1.RequestDomain{}, false},
		{"epoch zero", &v1.RequestDomain{Lineage: lineage}, false},
		{"no lineage", &v1.RequestDomain{Epoch: 1}, false},
		{"upper lineage", &v1.RequestDomain{Lineage: strings.ToUpper(lineage), Epoch: 1}, false},
		{"short lineage", &v1.RequestDomain{Lineage: lineage[:31], Epoch: 1}, false},
		{"valid", &v1.RequestDomain{Lineage: lineage, Epoch: 7}, true},
	}
	for _, test := range tests {
		d, err := environmentDomain(test.in)
		if (err == nil) != test.ok {
			t.Errorf("%s: err = %v, want ok %v", test.name, err, test.ok)
			continue
		}
		// Neither refusal is a retention expiry: a missing domain is not
		// RequestDomainRequired and epoch zero is not a legacy replay.
		var expired *store.ExpiryError
		if errors.As(err, &expired) {
			t.Errorf("%s: refusal is an ExpiryError", test.name)
		}
		if test.ok && (d.Lineage != test.in.Lineage || d.Epoch != test.in.Epoch) {
			t.Errorf("%s: domain = %+v", test.name, d)
		}
	}
}

func TestEnvironmentError(t *testing.T) {
	secret := "profile kube-secret-binding"
	tests := []struct {
		name   string
		err    error
		code   connect.Code
		detail bool
	}{
		{"invalid", fmt.Errorf("%w: bad name", environmentcontrol.ErrInvalid), connect.CodeInvalidArgument, false},
		{"unsupported", fmt.Errorf("%w: %s", environmentcontrol.ErrUnsupported, secret), connect.CodeInvalidArgument, false},
		{"not ready", environmentcontrol.ErrNotReady, connect.CodeFailedPrecondition, false},
		{"closed joined", errors.Join(environmentcontrol.ErrClosed, errors.New(secret)), connect.CodeFailedPrecondition, false},
		{"session limit", environmentcontrol.ErrSessionLimit, connect.CodeResourceExhausted, false},
		{"not found", store.ErrNotFound, connect.CodeNotFound, false},
		{"conflict", store.ErrConflict, connect.CodeAlreadyExists, false},
		{"reserved", store.ErrEnvironmentReserved, connect.CodeAlreadyExists, false},
		{"limit", store.ErrEnvironmentLimit, connect.CodeResourceExhausted, false},
		{"denied", auth.ErrPermissionDenied, connect.CodePermissionDenied, false},
		{"epoch expired", &store.ExpiryError{Reason: store.RequestEpochExpired}, connect.CodeFailedPrecondition, true},
		{"lineage changed", fmt.Errorf("accept: %w", &store.ExpiryError{Reason: store.RequestLineageChanged}), connect.CodeFailedPrecondition, true},
		// The Store's plain "unknown request epoch" has no type or
		// sentinel yet; it fails closed without its text.
		{"unknown epoch", errors.New("unknown request epoch"), connect.CodeInternal, false},
		{"runtime", errors.New(secret), connect.CodeInternal, false},
	}
	for _, test := range tests {
		var e *connect.Error
		if !errors.As(environmentError(test.err), &e) {
			t.Fatalf("%s: not a connect error", test.name)
		}
		if e.Code() != test.code {
			t.Errorf("%s: code = %v, want %v", test.name, e.Code(), test.code)
		}
		if got := len(e.Details()) != 0; got != test.detail {
			t.Errorf("%s: details = %v, want %v", test.name, got, test.detail)
		}
		if strings.Contains(e.Message(), secret) || strings.Contains(e.Message(), "unknown request epoch") {
			t.Errorf("%s: message %q carries the underlying text", test.name, e.Message())
		}
	}
}
