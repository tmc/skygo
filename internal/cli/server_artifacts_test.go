package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// UNRUN callback oracles. A canceled original context prevents the existing
// advisory physical probe from opening state; no server, initializer, artifact
// filesystem or ownership operation is supplied by these callbacks.
func TestServerArtifactDirectoryOptions(t *testing.T) {
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
			failure := errors.New("callback startup sentinel")
			type key struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, tt.name))
			cancel()
			calls := 0
			var got ServerOptions
			cmd := New(func(received context.Context, options ServerOptions) error {
				calls++
				got = options
				if received != ctx || received.Value(key{}) != tt.name || !errors.Is(received.Err(), context.Canceled) {
					t.Fatal("original context was replaced")
				}
				return failure
			})
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			want := ServerOptions{DataDir: "callback state/with spaces", ArtifactDir: tt.path, Listen: "127.0.0.1:19091", TargetFacts: "advisory facts/with spaces.yaml", DisableLocal: tt.disable}
			args := []string{"server", "--data-dir", want.DataDir, "--listen", want.Listen, "--target-facts", want.TargetFacts}
			if tt.explicit {
				args = append(args, "--artifact-dir", tt.path)
			}
			if tt.disable {
				args = append(args, "--disable-local")
			}
			if tt.scoped {
				want.AuthFile = "scoped auth/with spaces.json"
				args = append(args, "--auth-file", want.AuthFile)
			} else {
				want.Token = "legacy-secret"
				args = append(args, "--token", want.Token)
			}
			cmd.SetArgs(args)
			if err := cmd.ExecuteContext(ctx); err != failure {
				t.Fatalf("callback error=%v want exact sentinel", err)
			}
			if calls != 1 || got != want {
				t.Fatalf("callback calls=%d options=%+v want=%+v", calls, got, want)
			}
		})
	}
}

func TestServerArtifactDirectoryFlagDescribesOpenOnly(t *testing.T) {
	calls := 0
	cmd := New(func(context.Context, ServerOptions) error { calls++; return nil })
	server, _, err := cmd.Find([]string{"server"})
	if err != nil {
		t.Fatal(err)
	}
	flag := server.Flags().Lookup("artifact-dir")
	if flag == nil || flag.DefValue != "" || !strings.Contains(flag.Usage, "already initialized") || !strings.Contains(flag.Usage, "empty disables") {
		t.Fatalf("artifact flag does not describe optional initialized open: %+v", flag)
	}
	if calls != 0 {
		t.Fatal("examining command metadata started the callback")
	}
}
