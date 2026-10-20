//go:build linux || darwin

package control

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
)

// These tests use actual small temporary roots and the public Store installer.
// They are source oracles only until executed; they do not qualify native writer
// closure, injected spontaneous kernel close errors, or the sole Run adapter.
func physicalControlFixture(t *testing.T) (*Control, *os.Root) {
	t.Helper()
	f := newUploadGrowthResources(t)
	return f.control, f.root
}

func TestPhysicalActualSmallRootAndIndependentDBPoints(t *testing.T) {
	c, _ := physicalControlFixture(t)
	if c.retention.enabled {
		t.Fatal("fixture unexpectedly enabled request retention")
	}
	result := c.physicalQuantum(context.Background())
	if result.Err != nil || !result.Complete || result.Registered || result.Held {
		t.Fatalf("actual quantum: %+v", result)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	if p.cached == nil || p.cached.sample.Status != artifact.PhysicalIntervalComplete || p.cached.sample.Inodes == nil || *p.cached.sample.Inodes != 2 {
		t.Fatal("missing actual marker/root interval")
	}
	if p.points.databaseBytes == nil || p.points.databaseAt.IsZero() || p.points.databaseFreeAt.IsZero() || p.points.artifactFreeAt.IsZero() {
		t.Fatal("separate actual point unavailable")
	}
	databaseAt := p.points.databaseAt
	// Updating an inventory timestamp cannot renew a stale database observation.
	p.points.databaseAt = databaseAt.Add(-6 * time.Second)
	p.cached.sample.EndedAt = time.Now()
	p.mu.Unlock()
	if !errors.Is(c.cachedLinkGrowthCheck(), artifact.ErrFilesBusy) {
		t.Fatal("inventory refreshed old DB point")
	}
}

func TestPhysicalFourActualOwnerAdmission(t *testing.T) {
	c, _ := physicalControlFixture(t)
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
	}
	result := c.physicalQuantum(context.Background())
	if !errors.Is(result.Err, ErrArtifactBusy) || !result.Registered {
		t.Fatalf("fifth active quantum: %+v", result)
	}
	// This frame owns no FD because common admission refused before Begin. Stopped
	// retirement can remove that empty frame without inventing an I/O completion.
	c.requestPhysicalClose()
	closed := c.physicalQuantum(context.Background())
	if closed.Err != nil || closed.Registered {
		t.Fatalf("unused frame close: %+v", closed)
	}
	for _, call := range calls {
		call.Done()
	}
}

func TestPhysicalUnexplainedActualEntryRefusesPromotion(t *testing.T) {
	c, root := physicalControlFixture(t)
	file, err := root.OpenFile("unexplained", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("unowned observation")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	result := c.physicalQuantum(context.Background())
	if result.Complete || result.Registered || result.Held {
		t.Fatalf("unexplained entry promoted or invented uncertain close: %+v", result)
	}
	if !errors.Is(c.cachedLinkGrowthCheck(), artifact.ErrFilesBusy) {
		t.Fatal("unexplained name admitted growth")
	}
}

func TestPhysicalReturnedArtifactPointFailureAndRecovery(t *testing.T) {
	c, _ := physicalControlFixture(t)
	result := c.physicalQuantum(context.Background())
	if result.Err != nil || !result.Complete {
		t.Fatalf("actual initial sample: %+v", result)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	free := *p.points.artifactFree
	at := p.points.artifactFreeAt
	dbAt := p.points.databaseAt
	root := c.artifacts.root
	p.mu.Unlock()
	badRoot := root
	badRoot.ID = strings.Repeat("e", 32)
	// These explicit returned-component vectors exercise cache integration only;
	// they are not a claimed spontaneous kernel failure or filesystem probe.
	cases := []struct {
		name   string
		sample artifact.PhysicalSample
	}{
		{"free failure", artifact.PhysicalSample{Root: root}},
		{"root mismatch", artifact.PhysicalSample{Root: badRoot, FreeBytes: &free, FreeObservedAt: at}},
		{"stale point", artifact.PhysicalSample{Root: root, FreeBytes: &free, FreeObservedAt: at.Add(-6 * time.Second)}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c.replaceArtifactPoint(artifact.PhysicalSample{Root: root, FreeBytes: &free, FreeObservedAt: time.Now()})
			c.replaceArtifactPoint(tt.sample)
			p.mu.Lock()
			cleared := p.points.artifactFree == nil && p.points.artifactFreeAt.IsZero()
			sameDB := p.points.databaseAt == dbAt && p.points.databaseBytes != nil
			p.mu.Unlock()
			if !cleared || !sameDB {
				t.Fatal("failed new component retained old artifact point or erased independent DB point")
			}
			if !errors.Is(c.cachedLinkGrowthCheck(), artifact.ErrFilesBusy) {
				t.Fatal("failed point admitted growth")
			}
		})
	}
	recovered := c.physicalQuantum(context.Background())
	if recovered.Err != nil || !recovered.Complete {
		t.Fatalf("actual recovery sample: %+v", recovered)
	}
	p.mu.Lock()
	valid := p.points.artifactFree != nil && !p.points.artifactFreeAt.IsZero()
	p.mu.Unlock()
	if !valid {
		t.Fatal("new actual returned point did not recover")
	}
}

func TestPhysicalBeginRootFailureClearsPriorPoint(t *testing.T) {
	c, root := physicalControlFixture(t)
	first := c.physicalQuantum(context.Background())
	if first.Err != nil || !first.Complete {
		t.Fatalf("actual first sample: %+v", first)
	}
	if err := os.Rename(root.Name()+"/.skygo-artifact-root", root.Name()+"/marker-held-for-test"); err != nil {
		t.Fatal(err)
	}
	restore := true
	defer func() {
		if restore {
			if err := os.Rename(root.Name()+"/marker-held-for-test", root.Name()+"/.skygo-artifact-root"); err != nil {
				t.Error(err)
			}
		}
	}()
	failed := c.physicalQuantum(context.Background())
	if failed.Err == nil || failed.Complete || failed.Held {
		t.Fatalf("actual missing-marker Begin: %+v", failed)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	cleared := p.points.artifactFree == nil && p.points.artifactFreeAt.IsZero()
	dbAvailable := p.points.databaseBytes != nil && p.points.databaseFree != nil
	p.mu.Unlock()
	if !cleared || !dbAvailable {
		t.Fatal("failed Begin retained prior artifact point or lost independent successful DB points")
	}
	if err := os.Rename(root.Name()+"/marker-held-for-test", root.Name()+"/.skygo-artifact-root"); err != nil {
		t.Fatal(err)
	}
	restore = false
	recovered := c.physicalQuantum(context.Background())
	if recovered.Err != nil || !recovered.Complete {
		t.Fatalf("actual marker restoration sample: %+v", recovered)
	}
}

type physicalStatFileInfo struct {
	os.FileInfo
	stat *syscall.Stat_t
}

func (i physicalStatFileInfo) Sys() any { return i.stat }

func TestPhysicalDatabaseVolumeRefusesFileMountMismatch(t *testing.T) {
	c, _ := physicalControlFixture(t)
	directory, err := os.Lstat(c.store.Directory())
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Lstat(c.store.Directory() + "/jobs.db")
	if err != nil {
		t.Fatal(err)
	}
	if !physicalDatabaseVolume(directory, file) {
		t.Fatal("actual same-volume regular database refused")
	}
	original, ok := file.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("missing actual database device")
	}
	changed := *original
	changed.Dev++
	// A changed Dev models the file-mount counterexample using otherwise actual
	// observed metadata. No mount, fake kernel receipt or native qualification.
	mismatch := physicalStatFileInfo{FileInfo: file, stat: &changed}
	if physicalDatabaseVolume(directory, mismatch) {
		t.Fatal("DB-file mount borrowed parent-volume free capacity")
	}
	if physicalDatabaseVolume(nil, file) || physicalDatabaseVolume(directory, nil) {
		t.Fatal("missing device observation supplied free-volume authority")
	}
}

func TestPhysicalFailedDatabaseProbeClearsPriorComponents(t *testing.T) {
	c, _ := physicalControlFixture(t)
	c.samplePhysicalDatabase()
	p := &c.artifacts.physical
	p.mu.Lock()
	before := p.points.databaseBytes != nil && p.points.databaseFree != nil
	p.mu.Unlock()
	if !before {
		t.Fatal("actual initial DB probes unavailable")
	}
	name := c.store.Directory() + "/jobs.db"
	if err := os.Rename(name, name+".held-for-test"); err != nil {
		t.Fatal(err)
	}
	restore := true
	defer func() {
		if restore {
			if err := os.Rename(name+".held-for-test", name); err != nil {
				t.Error(err)
			}
		}
	}()
	// The same original Store remains open; this sampler never opens another DB.
	c.samplePhysicalDatabase()
	p.mu.Lock()
	cleared := p.points.databaseBytes == nil && p.points.databaseFree == nil && p.points.databaseAt.IsZero() && p.points.databaseFreeAt.IsZero()
	p.mu.Unlock()
	if !cleared {
		t.Fatal("latest failed DB probe fell back to prior fresh components")
	}
	if err := os.Rename(name+".held-for-test", name); err != nil {
		t.Fatal(err)
	}
	restore = false
	c.samplePhysicalDatabase()
	p.mu.Lock()
	recovered := p.points.databaseBytes != nil && p.points.databaseFree != nil
	p.mu.Unlock()
	if !recovered {
		t.Fatal("actual restored DB point did not recover")
	}
}
