package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/local"
)

// Value classification only. Synthetic identity DTOs are not capture receipts,
// death evidence, native owner proof, or a descriptor-Close fault oracle.
func TestServerLifetimeAcceptanceClassification(t *testing.T) {
	known := local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 3, PID: 4}
	partial := known
	partial.PID = 0
	for _, tc := range []struct {
		name  string
		value local.ServerLifetime
		cause error
		want  bool
	}{
		{name: "known", value: known, want: true},
		{name: "positively unavailable", cause: local.ErrServerLifetimeUnavailable, want: true},
		{name: "wrapped unavailable", cause: fmt.Errorf("capture: %w", local.ErrServerLifetimeUnavailable), want: true},
		{name: "zero without unavailable"},
		{name: "ordinary failure", cause: errors.New("original capture failed")},
		{name: "nonzero unavailable", value: known, cause: local.ErrServerLifetimeUnavailable},
		{name: "partial without error", value: partial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := acceptServerLifetime(tc.value, tc.cause)
			if (err == nil) != tc.want {
				t.Fatalf("accept=%v, want %v", err, tc.want)
			}
		})
	}
}
