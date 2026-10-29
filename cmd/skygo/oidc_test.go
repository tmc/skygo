package main

import (
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

func TestOIDCForwarded(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	stop := errors.New("server callback")
	calls := 0
	cmd := command(func(_ context.Context, got server.Config) error {
		calls++
		if got.AuthFile != "auth.json" || got.OIDCFile != "oidc.json" || got.OIDCJWKSURL != "https://issuer.example/keys" {
			t.Fatalf("config = %+v", got)
		}
		return stop
	})
	cmd.SetArgs([]string{"server", "--auth-file", "auth.json", "--oidc-file", "oidc.json", "--oidc-jwks-url", "https://issuer.example/keys"})
	if err := cmd.Execute(); !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("execute = %v, calls = %d", err, calls)
	}
}
