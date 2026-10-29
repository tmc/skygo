package cli

import (
	"context"
	"errors"
	"testing"
)

func TestOIDCServerOptions(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "https"}[remote], func(t *testing.T) {
			stop := errors.New("startup callback")
			called := 0
			cmd := New(func(_ context.Context, got ServerOptions) error {
				called++
				if got.AuthFile != "auth.json" || got.OIDCFile != "oidc.json" || remote && got.OIDCJWKSURL != "https://issuer.example/keys" || !remote && got.OIDCJWKSURL != "" {
					t.Fatalf("options = %+v", got)
				}
				return stop
			})
			args := []string{"server", "--auth-file", "auth.json", "--oidc-file", "oidc.json"}
			if remote {
				args = append(args, "--oidc-jwks-url", "https://issuer.example/keys")
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); !errors.Is(err, stop) || called != 1 {
				t.Fatalf("execute = %v, calls = %d", err, called)
			}
		})
	}
}
