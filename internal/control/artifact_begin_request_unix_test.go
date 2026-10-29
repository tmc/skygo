//go:build darwin || linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/store"
)

// A durable creating row is an unresolved original owner, not permission for
// Control to recreate its part. No physical sample is made in this test.
func TestArtifactBeginRequestCreatingReplay(t *testing.T) {
	f := newUploadGrowthResources(t)
	if err := f.store.Close(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.store = nil
	state := filepath.Join(f.directory, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.store, err = store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	f.control.store = f.store
	f.control.retention.enabled = true
	digest := strings.Repeat("a", 64)
	original, created, err := f.store.BeginArtifactUploadRequest(testActor(), info.Domain, "unresolved", digest, 1024)
	if err != nil || !created || original.State != "creating" {
		t.Fatalf("original admission: %+v %v %v", original, created, err)
	}
	before, err := f.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	names, err := os.ReadDir(f.root.Name())
	if err != nil {
		t.Fatal(err)
	}
	// Replay still needs an original registered call even when its row exists.
	if _, err := f.control.BeginArtifactUploadRequest(context.Background(), testActor(), nil, info.Domain, "unresolved", digest, 1024); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unregistered replay: %v", err)
	}
	call := uploadGrowthCall(t, f.control)
	again, err := f.control.BeginArtifactUploadRequest(context.Background(), testActor(), call, info.Domain, "unresolved", digest, 1024)
	call.Done()
	if err != nil || !reflect.DeepEqual(original, again) {
		t.Fatalf("creating replay changed original: %+v %v", again, err)
	}
	after, err := f.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("creating replay changed reservations: %+v %v", after, err)
	}
	afterNames, err := os.ReadDir(f.root.Name())
	if err != nil || !reflect.DeepEqual(names, afterNames) {
		t.Fatalf("creating replay changed root entries: %v %v", afterNames, err)
	}
}
