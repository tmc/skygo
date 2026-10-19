package store

import (
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
)

func TestValidateAccessUsesOnlyBoundedMetadata(t *testing.T) {
	s := openTest(t)
	if err := s.ValidateAccess(testActor(), "list", "", "allowed"); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateAccess(auth.Actor{}, "authenticate", "", "denied"); err != nil {
		t.Fatal(err)
	}
	secret := "recognizable-private-input"
	for _, input := range []struct{ operation, resource, result string }{
		{secret, "", "allowed"}, {"get", secret, "allowed"}, {"get", "", secret},
	} {
		err := s.ValidateAccess(testActor(), input.operation, input.resource, input.result)
		if err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("invalid metadata diagnostic: %v", err)
		}
	}
	if err := s.ValidateAccess(auth.Actor{}, "list", "", "allowed"); err == nil {
		t.Fatal("zero actor accepted ordinary record")
	}
}
