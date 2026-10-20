package rpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Real scoped HTTP handlers and Store, with no installed artifact service.
// FailedPrecondition for a valid selector proves routing past the wire adapter;
// it does not claim an artifact result or physical completion was fabricated.
func TestResultOnlyPruneHTTPValidationAndAuthority(t *testing.T) {
	f := scoped(t, &logBackend{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tt := range []struct {
		name, principal string
		mode            v1.PruneMode
		attempt         uint32
		obligation      string
		code            connect.Code
	}{
		{"route", "admin", v1.PruneMode_PRUNE_MODE_RESULT_ONLY, 2, "", connect.CodeFailedPrecondition},
		{"obligation_route", "admin", v1.PruneMode_PRUNE_MODE_RESULT_ONLY, 2, strings.Repeat("a", 32), connect.CodeFailedPrecondition},
		{"zero_refused", "admin", v1.PruneMode_PRUNE_MODE_RESULT_ONLY, 0, "", connect.CodeInvalidArgument},
		{"whole_attempt", "admin", v1.PruneMode_PRUNE_MODE_WHOLE, 2, "", connect.CodeInvalidArgument},
		{"output_attempt", "admin", v1.PruneMode_PRUNE_MODE_OUTPUT_ONLY, 2, "", connect.CodeInvalidArgument},
		{"operator", "alice", v1.PruneMode_PRUNE_MODE_RESULT_ONLY, 2, "", connect.CodePermissionDenied},
		{"reader_before_validation", "reader", v1.PruneMode_PRUNE_MODE_RESULT_ONLY, 0, "", connect.CodePermissionDenied},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &v1.PruneJobsRequest{Mode: tt.mode, Attempt: tt.attempt, Before: timestamppb.New(time.Now().UTC()), ObligationId: tt.obligation}
			if tt.obligation != "" {
				r.Before = nil
			}
			_, err := f.client.PruneJobs(ctx, authorized(f.tokens[tt.principal], r))
			if connect.CodeOf(err) != tt.code {
				t.Fatalf("error=%v; want %s", err, tt.code)
			}
		})
	}
	if sequence, err := f.store.LastSequence(); err != nil || sequence != 0 {
		t.Fatalf("refusal mutated job admission: %d %v", sequence, err)
	}
}
