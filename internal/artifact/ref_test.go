package artifact_test

import (
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

func TestRefValidate(t *testing.T) {
	valid := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1}
	for _, test := range []struct {
		name   string
		change func(*artifact.Ref)
		ok     bool
	}{
		{"minimum", func(*artifact.Ref) {}, true},
		{"maximum", func(r *artifact.Ref) { r.Size = 32 << 20 }, true},
		{"empty id", func(r *artifact.Ref) { r.ID = "" }, false},
		{"short id", func(r *artifact.Ref) { r.ID = strings.Repeat("a", 31) }, false},
		{"long id", func(r *artifact.Ref) { r.ID += "a" }, false},
		{"uppercase id", func(r *artifact.Ref) { r.ID = strings.Repeat("A", 32) }, false},
		{"nonhex id", func(r *artifact.Ref) { r.ID = strings.Repeat("g", 32) }, false},
		{"short hash", func(r *artifact.Ref) { r.SHA256 = strings.Repeat("b", 63) }, false},
		{"uppercase hash", func(r *artifact.Ref) { r.SHA256 = strings.Repeat("B", 64) }, false},
		{"nonhex hash", func(r *artifact.Ref) { r.SHA256 = strings.Repeat("g", 64) }, false},
		{"zero", func(r *artifact.Ref) { r.Size = 0 }, false},
		{"negative", func(r *artifact.Ref) { r.Size = -1 }, false},
		{"too large", func(r *artifact.Ref) { r.Size = 32<<20 + 1 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ref := valid
			test.change(&ref)
			if err := ref.Validate(); (err == nil) != test.ok {
				t.Fatalf("Validate() = %v; valid = %t", err, test.ok)
			}
		})
	}
}
