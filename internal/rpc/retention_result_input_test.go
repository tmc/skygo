package rpc

import (
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResultOnlyPruneSelection(t *testing.T) {
	before := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tt := range []struct {
		name string
		edit func(*v1.PruneJobsRequest)
		bad  bool
	}{
		{"page", func(*v1.PruneJobsRequest) {}, false},
		{"zero", func(r *v1.PruneJobsRequest) { r.Attempt = 0 }, true},
		{"whole_attempt", func(r *v1.PruneJobsRequest) { r.Mode = v1.PruneMode_PRUNE_MODE_WHOLE }, true},
		{"unspecified_attempt", func(r *v1.PruneJobsRequest) { r.Mode = v1.PruneMode_PRUNE_MODE_UNSPECIFIED }, true},
		{"output_attempt", func(r *v1.PruneJobsRequest) { r.Mode = v1.PruneMode_PRUNE_MODE_OUTPUT_ONLY }, true},
		{"metadata_attempt", func(r *v1.PruneJobsRequest) {
			r.Mode = v1.PruneMode_PRUNE_MODE_METADATA_ONLY
			r.AcknowledgeUnverifiedTerminal = true
		}, true},
		{"ack", func(r *v1.PruneJobsRequest) { r.AcknowledgeUnverifiedTerminal = true }, true},
		{"missing_cutoff", func(r *v1.PruneJobsRequest) { r.Before = nil }, true},
		{"oversized_page", func(r *v1.PruneJobsRequest) { r.PageSize = 101 }, true},
		{"obligation", func(r *v1.PruneJobsRequest) {
			r.ObligationId = strings.Repeat("a", 32)
			r.Before = nil
			r.PageSize = 0
			r.PageToken = ""
		}, false},
		{"obligation_with_cutoff", func(r *v1.PruneJobsRequest) { r.ObligationId = strings.Repeat("a", 32) }, true},
		{"obligation_with_token", func(r *v1.PruneJobsRequest) {
			r.ObligationId = strings.Repeat("a", 32)
			r.Before = nil
			r.PageSize = 0
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &v1.PruneJobsRequest{Mode: v1.PruneMode_PRUNE_MODE_RESULT_ONLY, Attempt: 2, Before: timestamppb.New(before), PageSize: 7, PageToken: "opaque"}
			tt.edit(r)
			got, err := pruneSelection(r)
			if tt.bad {
				if !errors.Is(err, control.ErrInvalid) {
					t.Fatalf("selection=%+v error=%v; want invalid", got, err)
				}
				return
			}
			if err != nil || got.Mode != store.ResultOnly || got.Attempt != 2 || got.ObligationID != r.ObligationId || got.Limit != int(r.PageSize) || got.AcknowledgeUnverifiedTerminal {
				t.Fatalf("selection=%+v error=%v", got, err)
			}
			if r.Before == nil && !got.Before.IsZero() || r.Before != nil && !got.Before.Equal(before) {
				t.Fatalf("cutoff changed: %v", got.Before)
			}
		})
	}
}
