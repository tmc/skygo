package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// TestLive requires an explicitly selected disposable context and namespace.
// It creates and deletes Jobs only when SKYGO_KUBE_TEST=1.
func TestLive(t *testing.T) {
	if os.Getenv("SKYGO_KUBE_TEST") != "1" {
		t.Skip("set SKYGO_KUBE_TEST=1 with explicit disposable context, namespace, and image")
	}
	contextName, namespace, image := os.Getenv("SKYGO_KUBE_CONTEXT"), os.Getenv("SKYGO_KUBE_NAMESPACE"), os.Getenv("SKYGO_KUBE_IMAGE")
	if contextName == "" || namespace == "" || image == "" {
		t.Fatal("SKYGO_KUBE_CONTEXT, SKYGO_KUBE_NAMESPACE and SKYGO_KUBE_IMAGE are required")
	}
	b, err := FromConfig(os.Getenv("KUBECONFIG"), contextName, namespace, "skygo-live-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, test := range []struct {
		name, script string
		want         backend.State
	}{{"success", "echo skygo-live", backend.Succeeded}, {"failure", "exit 7", backend.Failed}, {"cancel", "sleep 120", backend.Canceled}} {
		t.Run(test.name, func(t *testing.T) {
			var raw [16]byte
			if _, err := rand.Read(raw[:]); err != nil {
				t.Fatal(err)
			}
			id := hex.EncodeToString(raw[:])
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := b.Cancel(cleanupCtx, id); err != nil {
					t.Errorf("cleanup %s: %v", id, err)
				}
			})
			task := backend.Task{Argv: []string{"/bin/sh", "-c", test.script}, Image: image, TimeoutSeconds: 150}
			if err := b.Start(ctx, id, task); err != nil {
				t.Fatal(err)
			}
			recovered, err := FromConfig(os.Getenv("KUBECONFIG"), contextName, namespace, "skygo-live-test")
			if err != nil {
				t.Fatal(err)
			}
			if err = recovered.Start(ctx, id, task); err != nil {
				t.Fatalf("adopt: %v", err)
			}
			if test.want == backend.Canceled {
				for {
					status, err := recovered.Observe(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if status.State == backend.Running {
						break
					}
					if status.State.Terminal() {
						t.Fatalf("job ended before cancellation: %+v", status)
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(250 * time.Millisecond):
					}
				}
				if err = recovered.Cancel(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
			for {
				status, err := recovered.Observe(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if status.State.Terminal() {
					if status.State != test.want {
						t.Fatalf("state: %+v", status)
					}
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(250 * time.Millisecond):
				}
			}
			if test.want == backend.Succeeded {
				chunk, err := recovered.Logs(ctx, id, 0, 1024)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(chunk.Data), "skygo-live") || !chunk.Complete {
					t.Fatalf("logs: %+v", chunk)
				}
			}
		})
	}
}
