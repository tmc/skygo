package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

// This is an entrypoint forwarding oracle, not a server/native startup test.
// The canceled original context prevents CLI advisory filesystem probing.
func TestStoppedArtifactReaderRecoveryForwarded(t *testing.T) {
	for _, tt := range []struct {
		name  string
		flags []string
		want  bool
	}{
		{"default", nil, false},
		{"explicit false", []string{"--recover-stopped-artifact-readers=false"}, false},
		{"enabled", []string{"--recover-stopped-artifact-readers"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			sentinel := errors.New("original entrypoint callback")
			calls := 0
			cmd := command(func(received context.Context, got server.Config) error {
				calls++
				if received != ctx || !errors.Is(received.Err(), context.Canceled) {
					t.Fatal("original context replaced")
				}
				if got.RecoverStoppedArtifactReaders != tt.want || got.ArtifactDir != "initialized root" {
					t.Fatalf("config=%+v want recovery=%v and original artifact directory", got, tt.want)
				}
				if got.Targets != nil {
					t.Fatal("unexpected target constructor")
				}
				return sentinel
			})
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			args := []string{"server", "--data-dir", "unused recovery entrypoint state", "--artifact-dir", "initialized root"}

			cmd.SetArgs(append(args, tt.flags...))
			if err := cmd.ExecuteContext(ctx); err != sentinel || calls != 1 {
				t.Fatalf("callback calls=%d error=%v output=%s", calls, err, output.String())
			}
		})
	}
}
