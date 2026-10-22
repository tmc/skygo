package store

import (
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/environment"
)

func TestEnvironmentBounds(t *testing.T) {
	if maxEnvironments != 32 || maxPrincipalEnvironments != 4 || maxEnvironmentMetadataBytes != 16<<20 || environment.MaxCheckpointBytes != 64<<10 {
		t.Fatal("environment bounds changed")
	}
	// Every accepted environment fits the metadata bound together with its
	// largest record, spec and fixed rows.
	if maxEnvironments*(environmentRecordLimit+environment.MaxSpecBytes+4096) > maxEnvironmentMetadataBytes {
		t.Fatal("admission bounds exceed the metadata bound")
	}
}

func TestEnvironmentErrorsDistinct(t *testing.T) {
	errs := []error{ErrEnvironmentStale, ErrEnvironmentReserved, ErrEnvironmentLimit, ErrConflict, ErrNotFound}
	for i, a := range errs {
		for j, b := range errs {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v is %v", a, b)
			}
		}
	}
}
