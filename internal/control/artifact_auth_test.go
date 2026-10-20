package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
)

// Nil Store/Files deliberately make any operation past the auth/service fence
// fail. These tests claim no installed-service, storage, or filesystem behavior.
func TestArtifactEntryAuthBeforeServiceOrStore(t *testing.T) {
	c := &Control{engine: auth.NewLegacy("secret")}
	ctx := context.Background()
	id := strings.Repeat("a", 32)
	ref := artifact.Ref{ID: id, SHA256: strings.Repeat("b", 64), Size: 1}
	cases := []struct {
		name string
		run  func(auth.Actor) error
	}{
		{"begin upload", func(a auth.Actor) error { _, err := c.BeginArtifactUpload(ctx, a, nil, ref.SHA256, 1); return err }},
		{"get upload", func(a auth.Actor) error { _, err := c.GetArtifactUpload(ctx, a, id); return err }},
		{"write upload", func(a auth.Actor) error { _, err := c.WriteArtifactChunk(ctx, a, nil, id, 0, []byte("x")); return err }},
		{"finalize upload", func(a auth.Actor) error { _, err := c.FinalizeArtifactUpload(ctx, a, nil, id); return err }},
		{"cancel upload", func(a auth.Actor) error { return c.CancelArtifactUpload(ctx, a, nil, id) }},
		{"begin download", func(a auth.Actor) error { _, err := c.BeginArtifactDownload(ctx, a, nil, ref); return err }},
		{"read download", func(a auth.Actor) error { _, _, err := c.ReadArtifactChunk(ctx, a, nil, id, 0, 1); return err }},
		{"close download", func(a auth.Actor) error { return c.CloseArtifactDownload(ctx, a, nil, id) }},
		{"remove", func(a auth.Actor) error { return c.RemoveArtifact(ctx, a, ref) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(auth.Actor{}); !errors.Is(err, auth.ErrUnauthenticated) {
				t.Fatalf("unauthenticated: %v", err)
			}
		})
	}
	actor, err := c.engine.Authenticate("Bearer secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.name+" unsupported", func(t *testing.T) {
			if err := test.run(actor); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("uninstalled: %v", err)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = canceled
	for _, test := range cases {
		t.Run(test.name+" canceled", func(t *testing.T) {
			if err := test.run(actor); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled: %v", err)
			}
		})
	}
}
