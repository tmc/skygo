//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/auth"
)

// Both acquisition paths must select the OIDC Engine. The constructor does
// not read the missing mapping file; Run makes OIDC unavailable while the
// same original scoped Engine continues accepting its file credential.
func TestOIDCExistingInstallationEngine(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "ordinary"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			fixture := existingServerFixture(t, true)
			fixture.cfg.ExistingInstallation = existing
			fixture.cfg.OIDCFile = filepath.Join(filepath.Dir(fixture.cfg.AuthFile), "not-yet-written.json")
			frame := &serverRun{}
			fixture.frame = frame
			if err := frame.start(context.Background(), fixture.cfg); err != nil {
				fixture.hold(t, err)
				t.Fatal(err)
			}
			if frame.engine == nil || frame.store == nil || frame.store.Owner() != fixture.owner {
				t.Fatal("original scoped installation lost")
			}
			if _, err := frame.engine.Authenticate("Bearer " + fixture.token); err != nil {
				t.Fatal(err)
			}
			if _, err := frame.engine.Authenticate("Bearer a.b.c"); !errors.Is(err, auth.ErrUnavailable) {
				t.Fatalf("unavailable OIDC = %v", err)
			}
			if err := frame.shutdown(nil); err != nil {
				fixture.hold(t, err)
				t.Fatal(err)
			}
			fixture.frame = nil
		})
	}
}
