package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

// UNRUN forwarding oracle; Config.ArtifactDir depends on the separately owned
// server integration slice. --namespace avoids targets-file loading, and the
// callback deliberately never invokes Targets or any Kubernetes API/server.
func TestArtifactDirectoryForwarded(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	for _, tt := range []struct {
		name, path                string
		explicit, scoped, disable bool
	}{
		{name: "omitted"},
		{name: "initialized path", path: "operator artifacts/with spaces", explicit: true, disable: true},
		{name: "scoped auth", path: "operator artifacts/with spaces", explicit: true, scoped: true},
		{name: "explicit empty", explicit: true, scoped: true, disable: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			failure := errors.New("Kubernetes startup callback sentinel")
			type key struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, tt.name))
			cancel()
			calls := 0
			want := server.Config{DataDir: "callback state/with spaces", ArtifactDir: tt.path, Listen: "127.0.0.1:19093", TargetFacts: "advisory facts/with spaces.yaml", DisableLocal: tt.disable}
			if tt.scoped {
				want.AuthFile = "scoped auth/with spaces.json"
			} else {
				want.Token = "legacy-secret"
			}
			cmd := command(func(received context.Context, got server.Config) error {
				calls++
				if received != ctx || received.Value(key{}) != tt.name || !errors.Is(received.Err(), context.Canceled) {
					t.Fatal("original context was replaced")
				}
				if got.ArtifactDir != want.ArtifactDir || got.DataDir != want.DataDir || got.Listen != want.Listen || got.Token != want.Token || got.AuthFile != want.AuthFile || got.TargetFacts != want.TargetFacts || got.DisableLocal != want.DisableLocal || got.Targets == nil {
					t.Fatalf("config=%+v want forwarding fields=%+v and deferred Targets", got, want)
				}
				return failure
			})
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			args := []string{"server", "--namespace", "callback-only", "--data-dir", want.DataDir, "--listen", want.Listen, "--target-facts", want.TargetFacts}
			if tt.explicit {
				args = append(args, "--artifact-dir", tt.path)
			}
			if tt.disable {
				args = append(args, "--disable-local")
			}
			if tt.scoped {
				args = append(args, "--auth-file", want.AuthFile)
			} else {
				args = append(args, "--token", want.Token)
			}
			cmd.SetArgs(args)
			if err := cmd.ExecuteContext(ctx); err != failure {
				t.Fatalf("callback error=%v want exact sentinel", err)
			}
			if calls != 1 {
				t.Fatalf("startup callback calls=%d", calls)
			}
		})
	}
}
