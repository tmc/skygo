//go:build linux || darwin

package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/control"
)

// These cases exercise the original Control worker and actual Files methods.
// Store policy time is MODELED through the existing test seam; no test claims
// that an upload hour or a download's five minutes elapsed naturally.
func TestArtifactExpiryControlAbandonedUpload(t *testing.T) {
	for _, state := range []string{"writable", "finalizing-part", "finalizing-published", "finalizing-both"} {
		t.Run(state, func(t *testing.T) {
			f := newDeleteControlFixture(t, time.Now().UTC())
			u := f.upload(t, state)
			f.clock.Store(f.initial.Add(time.Hour + time.Second).UnixNano())
			// There is no authenticated Cancel/Remove mark and no client upload
			// identifier delivered to Run. It must discover the durable upload.
			f.start()
			f.awaitRetired(t, u)
		})
	}
}

func TestArtifactExpiryControlUnknownCreatingStaysCharged(t *testing.T) {
	f := newDeleteControlFixture(t, time.Now().UTC())
	u, err := f.database.BeginArtifactUpload(f.actor, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1024)
	f.check(t, err)
	before := f.projection(t)
	f.clock.Store(f.initial.Add(time.Hour + time.Second).UnixNano())
	f.start()
	until := time.NewTimer(2500 * time.Millisecond)
	defer until.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if !reflect.DeepEqual(before, f.projection(t)) {
			f.fail(t, errors.New("unknown creating upload lost its charged subject"))
		}
		_, found, err := f.database.NextArtifactCleanup("")
		f.check(t, err)
		if found {
			f.fail(t, errors.New("File0 expiry fabricated a deletion intent"))
		}
		for _, suffix := range []string{".part", ".tar"} {
			if _, err := os.Lstat(filepath.Join(f.objects, u.Reference.ID+suffix)); !errors.Is(err, os.ErrNotExist) {
				f.fail(t, fmt.Errorf("unknown creating upload acquired a filesystem name: %v", err))
			}
		}
		select {
		case <-until.C:
			return
		case <-f.done:
			f.fail(t, fmt.Errorf("unknown creating candidate stopped original Run: %w", f.runErr))
		case <-tick.C:
		}
	}
}

func TestArtifactExpiryControlOriginalIdleDownload(t *testing.T) {
	// Backdate BEFORE any metadata/reader construction. The object grant is
	// still live, while this SAME original reader's fixed lease is now old.
	f := newDeleteControlFixture(t, time.Now().UTC().Add(-6*time.Minute))
	u := f.upload(t, "complete")
	call := f.call(t)
	original, err := f.control.BeginArtifactDownload(context.Background(), f.actor, call.call, u.ref)
	f.check(t, err)
	f.download = original
	originalID := original.ID()
	// Keep its actual response owner outstanding across ordinary Run ticks.
	// No delayed I/O is invented: Begin returned, but HTTP Done has not.
	f.clock.Store(time.Now().UTC().UnixNano())
	for i := 0; i < 31; i++ {
		f.openDownload(t, u.ref)
	}
	f.download = original
	f.start()
	assertFull := func() {
		probe := f.call(t)
		unexpected, err := f.control.BeginArtifactDownload(context.Background(), f.actor, probe.call, u.ref)
		probe.finish()
		if !errors.Is(err, control.ErrArtifactBusy) || unexpected != nil {
			f.fail(t, fmt.Errorf("held original response released a cached-reader slot: %v", err))
		}
	}
	assertFull()
	hold := time.NewTimer(2200 * time.Millisecond)
	defer hold.Stop()
	select {
	case <-hold.C:
	case <-f.done:
		f.fail(t, fmt.Errorf("original Run stopped while response held: %w", f.runErr))
	}
	assertFull()
	call.finish()
	deadline := time.NewTimer(12 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		probe := f.call(t)
		replacement, err := f.control.BeginArtifactDownload(context.Background(), f.actor, probe.call, u.ref)
		probe.finish()
		if err == nil {
			if replacement == nil || replacement == original || replacement.ID() == originalID {
				f.fail(t, errors.New("expiry replaced or reused the original handle identity"))
			}
			// The 31 young originals stayed cached; opening the 32nd requires
			// BOTH original-cache retirement and durable lease-slot release.
			// Only shared Close+Finish can retire the original worker subject.
			return
		}
		if !errors.Is(err, control.ErrArtifactBusy) {
			f.fail(t, fmt.Errorf("idle close did not preserve durable slot semantics: %w", err))
		}
		select {
		case <-deadline.C:
			f.fail(t, errors.New("original idle reader did not close and release its exact slot"))
		case <-f.done:
			f.fail(t, fmt.Errorf("original Run stopped before idle close: %w", f.runErr))
		case <-tick.C:
		}
	}
}
