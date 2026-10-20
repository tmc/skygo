package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestObservedStorageGuardBoundaries(t *testing.T) {
	now := time.Now().UTC()
	for _, tt := range []struct {
		name           string
		database, free uint64
		maintenance    bool
		want           string
	}{
		{"admission-below", admissionDatabaseBytes - 1, admissionFreeBytes, false, ""},
		{"admission-at", admissionDatabaseBytes, admissionFreeBytes, false, "admission_hold"},
		{"free-below", 1, admissionFreeBytes - 1, false, "admission_hold"},
		{"maintenance-below", maintenanceDatabaseBytes - 1, emergencyFreeBytes, true, ""},
		{"maintenance-at", maintenanceDatabaseBytes, admissionFreeBytes, true, "maintenance_hold"},
		{"critical", 1, emergencyFreeBytes - 1, true, "storage_critical"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sample := &RetentionAdmin{DatabaseFileBytes: &tt.database, FilesystemFreeBytes: &tt.free, ObservedAt: now}
			if got := storageCondition(sample, tt.maintenance, false, 0, 0, now); got != tt.want {
				t.Fatalf("condition=%q want %q", got, tt.want)
			}
		})
	}
	size, free, output, files := uint64(1), uint64(admissionFreeBytes), uint64(10), uint64(2)
	sample := &RetentionAdmin{DatabaseFileBytes: &size, FilesystemFreeBytes: &free, OutputAllocatedBytes: &output, OutputFileCount: &files, ObservedAt: now}
	if got := storageCondition(sample, false, true, 10, 2, now); got != "" {
		t.Fatal(got)
	}
	if got := storageCondition(sample, false, true, 9, 2, now); got != "output_accounting_hold" {
		t.Fatal(got)
	}
	if got := storageCondition(sample, false, true, 10, 1, now); got != "output_accounting_hold" {
		t.Fatal(got)
	}
	if got := storageCondition(sample, false, false, 0, 0, now.Add(5*time.Second)); got != "" {
		t.Fatal(got)
	}
	if got := storageCondition(sample, false, false, 0, 0, now.Add(5*time.Second+1)); got != "metrics_stale" {
		t.Fatal(got)
	}
	if got := storageCondition(sample, false, false, 0, 0, now.Add(-1)); got != "metrics_stale" {
		t.Fatal(got)
	}
}

func TestStorageHoldPreservesReplayAndEarlyCancellation(t *testing.T) {
	s, info := retainedStore(t)
	c := routeControl(t, s, retainedTarget(&routeBackend{}))
	task := backend.Task{Argv: []string{"work"}}
	job, err := c.SubmitRetained(testActor(), info.Domain, "held", task)
	if err != nil {
		t.Fatal(err)
	}
	c.retention.sample = nil
	if c.StorageReady() {
		t.Fatal("missing sample reported ready")
	}
	replay, err := c.SubmitRetained(testActor(), info.Domain, "held", task)
	if err != nil || replay.ID != job.ID {
		t.Fatal("storage guard blocked replay", err)
	}
	if _, err = c.SubmitRetained(testActor(), info.Domain, "new", task); !errors.Is(err, ErrUnavailable) {
		t.Fatal("missing observation admitted growth", err)
	}
	if !c.retention.wantSample.Load() {
		t.Fatal("missing observation did not request bounded sample")
	}
	if _, err = c.Cancel(testActor(), job.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := c.newLoop(ctx)
	defer stopTestLoop(t, cancel, loop)
	if err = c.schedule(ctx, loop); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(job.ID)
	if err != nil || got.Status.State != backend.Canceled {
		t.Fatal("storage hold prevented early cancel", got, err)
	}
	primeStorage(c)
	if !c.StorageReady() {
		t.Fatal("valid observed admission guard not ready")
	}
	c.retention.sample.ObservedAt = time.Now().Add(-6 * time.Second)
	if c.StorageReady() {
		t.Fatal("stale observation reported ready")
	}
	disabled := routeControl(t, routeStore(t), targetFor("good", &routeBackend{}))
	if !disabled.StorageReady() {
		t.Fatal("disabled retention readiness changed")
	}
}
