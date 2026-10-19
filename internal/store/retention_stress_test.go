//go:build linux || darwin

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This is an opt-in physical qualification gate, not a routine unit test.
// All accepted jobs and transitions use Store APIs. Stopped remote executions
// and successful native cleanup are MODELED; no workload or cloud is executed.
// Bolt file sizes, allocated blocks, reader pins, freelists and free space are
// measured on the actual filesystem. The daemon probes use the actual binary.
func TestRetentionPhysicalChurn(t *testing.T) {
	if os.Getenv("SKYGO_RETENTION_STRESS") != "1" {
		t.Skip("manual physical retention gate; set SKYGO_RETENTION_STRESS=1")
	}
	root := os.Getenv("SKYGO_RETENTION_STRESS_ROOT")
	free, err := filesystemFree(root)
	if err != nil {
		t.Fatal(err)
	}
	if free < 8<<30 {
		t.Fatal("HOLD: natural churn requires at least 8 GiB currently free")
	}
	r := newRetentionStress(t, root, os.Getenv("SKYGO_RETENTION_STRESS_EVIDENCE"))
	r.kind = "natural-churn"
	r.open()
	// Individual maxima cannot coexist: each phase fills the default policy until
	// the first real quota refusal, reports that limiter, and then collects it.
	r.phase("task", stressTask(256<<10, 0, false), false)
	if r.lastCapacity != TaskBytesQuota {
		t.Fatal("HOLD: maximal Task corpus did not reach TaskBytes boundary")
	}
	r.retire()
	r.phase("retry", stressTask(0, 10, false), true)
	if r.lastCapacity != RetryBytesQuota {
		t.Fatal("HOLD: maximal retry corpus did not reach RetryBytes boundary")
	}
	r.retire()
	r.phase("escaped-cleanup-bytes", stressTask(256<<10, 10, true), true)
	if r.lastCapacity != TaskBytesQuota {
		t.Fatal("HOLD: escaped maximal Task corpus did not reach TaskBytes boundary")
	}
	r.retire()
	// Maximal LEGAL 128-byte IDs reach their actual limiter. Escaped new IDs
	// are not accepted by this API and cannot make RequestBytes a required limit.
	for cycle := 0; cycle < 16; cycle++ {
		r.phase(fmt.Sprintf("long-requests-%02d", cycle), stressTask(0, 0, false), false)
		if r.used(RequestRecordsQuota) == DefaultRetentionPolicy().RequestRecords {
			break
		}
	}
	if r.lastCapacity != RequestRecordsQuota || r.used(RequestRecordsQuota) != DefaultRetentionPolicy().RequestRecords {
		t.Fatal("HOLD: maximal legal request corpus did not reach real record quota")
	}
	r.requestGeometry("maximal-legal-ids")
	r.retire()
	r.shortIDs = true
	// Accumulate real request tombstones across metadata-limited fills. The
	// RequestRecords quota must actually be reached; no limit is lowered.
	for cycle := 0; cycle < 16; cycle++ {
		r.phase(fmt.Sprintf("requests-%02d", cycle), stressTask(0, 0, false), false)
		if r.info().Usage != nil && r.used(RequestRecordsQuota) == DefaultRetentionPolicy().RequestRecords {
			break
		}
	}
	if r.used(RequestRecordsQuota) != DefaultRetentionPolicy().RequestRecords {
		t.Fatal("HOLD: bounded request fill did not reach the default request-record ceiling")
	}
	r.requestGeometry("short-legal-ids")
	r.retire()
	// Repeated task/history/shared-header churn with bounded concurrent readers.
	// Each prepared phase coordinates a bounded old reader with actual mark/refill.
	for cycle := 0; cycle < 12; cycle++ {
		r.phase(fmt.Sprintf("churn-%02d", cycle), stressTask(64<<10, 10, true), true)
		r.retire()
	}
	// Manual obligations remain charged and are not automatically acknowledged.
	r.manual()
	r.check(r.s.Close())
	r.s = nil
	r.compact()
	r.passed = true
}

// Free-space watermarks are a separate gate: unavailable disposable storage
// never prevents independently executable natural churn evidence.
func TestRetentionPhysicalWatermarks(t *testing.T) {
	if os.Getenv("SKYGO_RETENTION_WATERMARKS") != "1" {
		t.Skip("manual free-space gate; set SKYGO_RETENTION_WATERMARKS=1")
	}
	root := os.Getenv("SKYGO_RETENTION_WATERMARK_ROOT")
	var fs unix.Statfs_t
	if err := unix.Statfs(root, &fs); err != nil {
		t.Fatal(err)
	}
	total := uint64(fs.Blocks) * uint64(fs.Bsize)
	if os.Getenv("SKYGO_RETENTION_STRESS_DISPOSABLE") != "4GiB" || total < 3<<30 || total > 5<<30 {
		t.Fatal("HOLD: dedicated explicit disposable 3..5 GiB filesystem required")
	}
	r := newRetentionStress(t, root, os.Getenv("SKYGO_RETENTION_WATERMARK_EVIDENCE"))
	r.kind = "free-space-watermarks"
	r.open()
	r.submitCanceled("guard-one")
	r.submitCanceled("guard-two")
	r.check(r.s.Close())
	r.s = nil
	r.watermarks()
	r.passed = true
}
func newRetentionStress(t *testing.T, root, evidence string) *retentionStress {
	t.Helper()
	if root == "" || evidence == "" {
		t.Fatal("HOLD: explicit empty state root and retained evidence directory required")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("HOLD: fixture root must be empty: %v (%d entries)", err, len(entries))
	}
	if err := os.MkdirAll(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(filepath.Join(evidence, "measurements.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	r := &retentionStress{t: t, root: root, evidence: evidence, out: out, start: time.Now(), now: time.Now().UTC(), maxima: make(map[QuotaDomain]uint64), limits: make(map[QuotaDomain]uint64)}
	t.Cleanup(func() {
		if r.s != nil {
			if err := r.s.Close(); err != nil {
				t.Errorf("close final store: %v", err)
			}
		}
		if err := json.NewEncoder(out).Encode(map[string]any{"kind": "final", "gate": r.kind, "reader_overlaps": r.readerOverlaps, "passed": r.passed && !t.Failed(), "mutating_api_calls": r.mutations, "accepted": r.accepted, "max_database_bytes": r.maxFile, "max_allocated_bytes": r.maxAllocated, "max_usage": r.maxima, "limits": r.limits, "max_gc_changed_bytes": r.maxChanged, "elapsed": time.Since(r.start).String(), "database_256_crossed": r.maxFile >= 256<<20, "database_320_crossed": r.maxFile >= 320<<20, "unreached_database_watermarks_are_not_qualified": true, "proof_layer": "MODELED remote stopped receipts and cleanup acknowledgments; actual Store and filesystem"}); err != nil {
			t.Errorf("write final evidence: %v", err)
		}
		if err := out.Sync(); err != nil {
			t.Errorf("sync final evidence: %v", err)
		}
		if err := out.Close(); err != nil {
			t.Errorf("close final evidence: %v", err)
		}
	})

	return r
}

type retentionStress struct {
	t                                 *testing.T
	s                                 *Store
	root, evidence                    string
	out                               *os.File
	start, now                        time.Time
	serial, accepted, mutations       uint64
	maxFile, maxAllocated, maxChanged uint64
	maxima, limits                    map[QuotaDomain]uint64
	passed                            bool
	shortIDs                          bool
	lastCapacity                      QuotaDomain
	readerOverlaps                    int
	kind                              string
}

func (r *retentionStress) check(err error) {
	r.t.Helper()
	if err != nil {
		r.t.Fatal(err)
	}
}
func (r *retentionStress) emit(v any) { r.t.Helper(); r.check(json.NewEncoder(r.out).Encode(v)) }
func (r *retentionStress) open() {
	dir := filepath.Join(r.root, "state")
	s, err := Open(dir)
	r.check(err)
	r.check(s.Close())
	_, err = EnableRetention(dir, DefaultRetentionPolicy())
	r.check(err)
	r.s, err = Open(dir)
	r.check(err)
	r.s.now = func() time.Time { return r.now }
	r.sample("enabled")
}
func (r *retentionStress) info() RetentionInfo {
	i, e := r.s.RetentionInfo(testActor())
	r.check(e)
	return i
}
func (r *retentionStress) used(q QuotaDomain) uint64 {
	for _, u := range r.info().Usage {
		if u.Domain == q {
			return u.Used
		}
	}
	r.t.Fatalf("missing quota %d", q)
	return 0
}
func (r *retentionStress) sample(operation string) {
	r.t.Helper()
	if time.Since(r.start) > 45*time.Minute {
		r.t.Fatal("HOLD: 45 minute gate budget exhausted")
	}
	if r.mutations > 300000 || r.accepted > 45000 {
		r.t.Fatal("HOLD: finite API/write budget exhausted")
	}
	path := filepath.Join(r.root, "state", "jobs.db")
	st, err := os.Stat(path)
	r.check(err)
	allocated := uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
	free, err := filesystemFree(r.root)
	r.check(err)
	if uint64(st.Size()) > r.maxFile {
		r.maxFile = uint64(st.Size())
	}
	if allocated > r.maxAllocated {
		r.maxAllocated = allocated
	}
	var stats bolt.Stats
	var us []Usage
	if r.s != nil {
		stats = r.s.db.Stats()
		us = r.info().Usage
	}
	for _, u := range us {
		if u.Used > r.maxima[u.Domain] {
			r.maxima[u.Domain] = u.Used
		}
		r.limits[u.Domain] = u.Limit
		if u.Used > u.Limit {
			r.t.Fatalf("quota exceeded: %+v", u)
		}
	}
	r.emit(map[string]any{"kind": "physical", "operation": operation, "mutation": r.mutations, "database_bytes": st.Size(), "allocated_bytes": allocated, "free_bytes": free, "stats": stats, "usage": us, "at": time.Now().UTC()})
	if uint64(st.Size()) > 320<<20 {
		r.t.Fatal("HOLD: actual database excursion above 320 MiB; database and observations retained")
	}
	if free < 2<<30 {
		r.t.Fatal("HOLD: unexpected free space below 2 GiB during corpus churn")
	}
}
func (r *retentionStress) mutation(name string, f func() error) error {
	r.t.Helper()
	err := f()
	r.mutations++
	r.sample(name)
	return err
}

// Encoded tasks hit the actual per-task bound, including escaping and retry
// fields. This constructs inputs; it does not reproduce quota accounting.
func stressTask(size int, attempts uint32, escaped bool) backend.Task {
	t := backend.Task{Argv: []string{"modeled-no-execution", ""}}
	if attempts > 0 {
		t.Retry = &backend.RetryPolicy{MaxAttempts: attempts, DelaySeconds: 1, ExitCodes: []int32{1}}
	}
	if size == 0 {
		return t
	}
	b, _ := backend.TaskBytes(t)
	if escaped {
		t.Argv[1] = strings.Repeat("\x01", (size-len(b))/6)
		b, _ = backend.TaskBytes(t)
	}
	t.Argv[1] += strings.Repeat("x", size-len(b))
	return t
}

func (r *retentionStress) submit(task backend.Task) (backend.Job, error) {
	r.t.Helper()
	r.serial++
	id := fmt.Sprintf("%012d", r.serial)
	if !r.shortIDs {
		id += strings.Repeat("x", 116)
	}
	var j backend.Job
	if r.maxFile >= 256<<20 {
		r.t.Fatal("HOLD: actual admission watermark reached; no direct Store admission bypass is permitted")
	}
	err := r.mutation("submit", func() error {
		var e error
		j, e = r.s.SubmitRetained(testActor(), r.info().Domain, id, task, "remote", strings.Repeat("\x01", 256), backend.RemoteOutput, 0)
		return e
	})
	if err == nil {
		r.accepted++
	}
	return j, err
}
func (r *retentionStress) cancel(j backend.Job) {
	r.t.Helper()
	r.check(r.mutation("cancel-request", func() error {
		_, e := r.s.Update(j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
		return e
	}))
	r.check(r.mutation("cancel-before-start", func() error {
		_, start, e := r.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, r.now)
		if start {
			return errors.New("canceled job unexpectedly started")
		}
		return e
	}))
}
func (r *retentionStress) complete(j backend.Job) {
	r.t.Helper()
	for n := 0; n < int(j.Task.Retry.MaxAttempts); n++ {
		if j.NextAttemptAt != nil {
			r.now = *j.NextAttemptAt
		}
		r.check(r.mutation("begin", func() error {
			var start bool
			var e error
			j, start, e = r.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, r.now)
			if e == nil && !start {
				return errors.New("model attempt did not commit Starting")
			}
			return e
		}))
		code := int32(1)
		// Legal maximally escaped diagnostic/instance inputs stress real row growth.
		status := backend.Status{State: backend.Failed, Reason: strings.Repeat("\x01", 1024), ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: strings.Repeat("<", 128)}
		r.check(r.mutation("observe-modeled-stopped", func() error {
			var e error
			j, e = r.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), status, r.now.Add(time.Millisecond))
			return e
		}))
	}
	if j.Status.State != backend.Failed || len(j.Attempts) != int(j.Task.Retry.MaxAttempts) {
		r.t.Fatalf("retry corpus not terminal: %+v", j.Status)
	}
}
func (r *retentionStress) phase(name string, task backend.Task, native bool) {
	r.t.Helper()
	r.emit(map[string]any{"kind": "phase-start", "name": name, "task_encoded_bytes": lenMustTask(task), "native_receipts": "MODELED"})
	var jobs []backend.Job
	for n := 0; n < 10001; n++ {
		j, err := r.submit(task)
		if err != nil {
			var capacity *CapacityError
			if !errors.As(err, &capacity) {
				r.t.Fatalf("HOLD: expected typed capacity at default-policy boundary, got %T: %v", err, err)
			}
			matched := false
			for _, u := range r.info().Usage {
				if u.Domain == capacity.Domain {
					matched = u.Used == capacity.Used && u.Limit == capacity.Limit
				}
			}
			if !matched {
				r.t.Fatalf("capacity fields disagree with actual durable usage: %+v", capacity)
			}
			r.lastCapacity = capacity.Domain
			r.emit(map[string]any{"kind": "capacity", "phase": name, "error": capacity, "usage": r.info().Usage})
			break
		}
		jobs = append(jobs, j)
	}
	if len(jobs) == 10001 {
		r.t.Fatal("unbounded admission: no default quota refusal")
	}
	for _, j := range jobs {
		if native {
			r.complete(j)
		} else {
			r.cancel(j)
		}
	}
	r.dryRun()
	if strings.HasPrefix(name, "churn-") {
		r.readerMarkRefill(task)
	}
	r.collect(name)
	r.check(r.s.db.View(validateSnapshot))
	r.reopen()
	r.emit(map[string]any{"kind": "phase-done", "name": name, "accepted": len(jobs), "usage": r.info().Usage})
}
func lenMustTask(t backend.Task) int {
	b, e := backend.TaskBytes(t)
	if e != nil {
		panic(e)
	}
	return len(b)
}

func (r *retentionStress) collect(phase string) {
	for page := 0; page < 10001; page++ {
		before := r.logical()
		var p PrunePage
		r.check(r.mutation("mark", func() error {
			var e error
			p, e = r.s.Prune(testActor(), PruneSelection{Before: r.now.Add(time.Hour), Mode: Whole, Limit: 100}, true, nil, nil, r.now)
			return e
		}))
		r.changed("mark", before)
		r.emit(map[string]any{"kind": "mark", "phase": phase, "page": p, "usage": r.info().Usage})
		if p.Inspected > 200 {
			r.t.Fatal("mark exceeded inspection bound")
		}
		// Accumulate real cleanup records until a quota or empty live-job set stops
		// marks. Retry each operation once before modeled successful acknowledgement.
		// FailCleanup intentionally stores its bounded diagnostic code, not our text.
		if p.Marked > 0 && r.used(JobsQuota) > 0 {
			continue
		}
		op, hasPending, e := r.s.NextCleanup(0)
		r.check(e)
		if hasPending {
			r.reopen()
			restored, found, e := r.s.NextCleanup(0)
			r.check(e)
			if !found || restored.ID != op.ID || restored.Digest != op.Digest {
				r.t.Fatal("pending cleanup identity changed on reopen")
			}
		}
		drained := 0
		for drained < 1024 {
			op, found, err := r.s.NextCleanup(0)
			r.check(err)
			if !found {
				break
			}
			if op.Operation != NativeCleanup || !op.Stopped || op.Proof.Termination != backend.StoppedWithExitCode || op.Execution.Instance == "" {
				r.t.Fatal("unexpected unproved cleanup dispatch")
			}
			b := r.logical()
			r.check(r.mutation("cleanup-modeled-failure", func() error { return r.s.FailCleanup(op.ID, op.Digest, strings.Repeat("\x01", 1024), r.now) }))
			r.changed("fail-cleanup", b)
			op, found, err = r.s.NextCleanup(0)
			r.check(err)
			if !found {
				r.t.Fatal("failed cleanup disappeared")
			}
			b = r.logical()
			r.check(r.mutation("cleanup-modeled-success", func() error { return r.s.FinishCleanup(op.ID, op.Operation, op.Digest, r.now) }))
			r.changed("finish-cleanup", b)
			drained++
		}
		if r.used(JobsQuota) == 0 {
			return
		}
		if p.Marked == 0 && drained == 0 {
			r.t.Fatalf("collection made no progress: %+v", p)
		}
	}
	r.t.Fatal("bounded collection page budget exhausted")
}
func (r *retentionStress) dryRun() {
	before, err := stressFileHash(r.s.db.Path())
	r.check(err)
	p, err := r.s.Prune(testActor(), PruneSelection{Before: r.now.Add(time.Hour), Mode: Whole, Limit: 100}, false, nil, nil, r.now)
	r.check(err)
	after, err := stressFileHash(r.s.db.Path())
	r.check(err)
	if before != after || p.Marked != 0 {
		r.t.Fatal("dry-run changed actual database bytes")
	}
	r.emit(map[string]any{"kind": "dry-run", "page": p, "database_byte_identical": true})
}
func stressFileHash(path string) ([32]byte, error) {
	var out [32]byte
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return out, err
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}
func (r *retentionStress) manual() {
	for n := 0; n < 10001; n++ {
		j, err := r.submit(stressTask(0, 0, false))
		if err != nil {
			var capacity *CapacityError
			if !errors.As(err, &capacity) {
				r.t.Fatal(err)
			}
			r.emit(map[string]any{"kind": "manual-fill-capacity", "capacity": capacity})
			break
		}
		r.cancel(j)
	}
	for n := 0; n < 100; n++ {
		b := r.logical()
		var p PrunePage
		r.check(r.mutation("manual-mark", func() error {
			var e error
			p, e = r.s.Prune(testActor(), PruneSelection{Before: r.now.Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true, Limit: 100}, true, nil, nil, r.now)
			return e
		}))
		r.changed("manual-mark", b)
		if p.Marked == 0 {
			break
		}
	}
	if r.used(CleanupRecordsQuota) != DefaultRetentionPolicy().CleanupRecords {
		r.t.Fatal("HOLD: manual corpus did not reach real 1024-record quota")
	}
	_, found, err := r.s.NextCleanup(0)
	r.check(err)
	if found {
		r.t.Fatal("manual obligations incorrectly authorized automatic cleanup")
	}
	r.check(r.s.db.View(validateSnapshot))
	r.reopen()
	r.emit(map[string]any{"kind": "manual-full", "usage": r.info().Usage, "automatic_cleanup": false})
}
func (r *retentionStress) compact() {
	source := filepath.Join(r.root, "state", "jobs.db")
	output := filepath.Join(r.root, "compacted.db")
	freeBefore, err := filesystemFree(r.root)
	r.check(err)
	r.check(Compact(filepath.Dir(source), output))
	original, err := os.Stat(source)
	r.check(err)
	result, err := os.Stat(output)
	r.check(err)
	freeAfter, err := filesystemFree(r.root)
	r.check(err)
	if result.Size() > original.Size() {
		r.t.Fatal("compaction grew result beyond source")
	}
	r.emit(map[string]any{"kind": "compact", "source_bytes": original.Size(), "result_bytes": result.Size(), "source_allocated": uint64(original.Sys().(*syscall.Stat_t).Blocks) * 512, "result_allocated": uint64(result.Sys().(*syscall.Stat_t).Blocks) * 512, "free_before": freeBefore, "free_after": freeAfter, "temporary_peak": "not sampled; simultaneous source/result measured"})
}
func (r *retentionStress) reopen() {
	r.check(r.s.Close())
	var e error
	r.s, e = Open(filepath.Join(r.root, "state"))
	r.check(e)
	r.s.now = func() time.Time { return r.now }
	r.sample("reopen")
}
func (r *retentionStress) retire() {
	info := r.info()
	requests, jobs := r.used(RequestRecordsQuota), r.used(JobsQuota)
	if requests > info.Policy.RequestRecords || jobs > info.Policy.Jobs {
		r.t.Fatal("unexpected sealed modern corpus", requests, jobs)
	}
	// Every nonempty call must remove a request; one defensive extra call
	// also permits phase advancement when R is zero. No wall/disk extension.
	callLimit := requests + 1
	r.now = info.RetryUntil.Add(time.Second)
	for n := uint64(0); n < callLimit; n++ {
		old := r.used(RequestRecordsQuota)
		b := r.logical()
		r.check(r.mutation("retire-epoch", func() error { return r.s.MaintainRetention(r.now) }))
		r.changed("retire-epoch", b)
		next := r.used(RequestRecordsQuota)
		if next > old || old-next > 50 || old > 0 && next == old {
			r.t.Fatal("closed modern retirement made invalid progress", old, next)
		}
		if next == 0 {
			return
		}
	}
	r.t.Fatal("bounded epoch retirement did not complete")
}
func (r *retentionStress) submitCanceled(label string) {
	j, e := r.submit(backend.Task{Argv: []string{label}})
	r.check(e)
	r.cancel(j)
}

// The old reader is established AFTER corpus preparation, before GC/refill.
// Only the joined worker invokes plain Store APIs; it never calls testing.Fatal.
func (r *retentionStress) readerMarkRefill(task backend.Task) {
	r.t.Helper()
	before := r.logical()
	actor := testActor()
	tx, err := r.s.db.Begin(false)
	r.check(err)
	oldID := tx.ID()
	readerStarted := time.Now()
	release := make(chan struct{})
	readerDone := make(chan string, 1)
	go func() {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		reason := "released"
		select {
		case <-release:
		case <-timer.C:
			reason = "watchdog"
		}
		err := tx.Rollback()
		if err != nil {
			reason = "rollback-error: " + err.Error()
		}
		readerDone <- reason
	}()
	joined := false
	defer func() {
		if !joined {
			close(release)
			<-readerDone
		}
	}()
	r.serial++
	id := fmt.Sprintf("overlap-%012d", r.serial)
	domain := r.info().Domain
	now := r.now
	type result struct {
		page                               PrunePage
		job                                backend.Job
		middle                             map[string]stressValue
		markBytes, markAllocated, markFree uint64
		markStats                          bolt.Stats
		markUsage                          []Usage
		err                                error
	}
	worker := make(chan result, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		page, e := r.s.Prune(actor, PruneSelection{Before: now.Add(time.Hour), Mode: Whole, Limit: 100}, true, nil, nil, now)
		var j backend.Job
		var middle map[string]stressValue
		var measured result
		if e == nil {
			middle, e = stressLogical(r.s.db)
		}
		if e == nil {
			var st os.FileInfo
			st, e = os.Stat(r.s.db.Path())
			if e == nil {
				measured.markBytes = uint64(st.Size())
				measured.markAllocated = uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
				measured.markFree, e = filesystemFree(r.root)
			}
			if e == nil {
				var info RetentionInfo
				info, e = r.s.RetentionInfo(actor)
				measured.markUsage = info.Usage
				measured.markStats = r.s.db.Stats()
			}
			if e == nil && (measured.markBytes >= 256<<20 || measured.markFree < 2<<30) {
				e = errors.New("HOLD: physical admission watermark after coordinated mark; no refill bypass")
			}
		}
		if e == nil && page.Marked > 0 {
			j, e = r.s.SubmitRetained(actor, domain, id, task, "remote", strings.Repeat("\x01", 256), backend.RemoteOutput, 0)
		}
		measured.page = page
		measured.job = j
		measured.middle = middle
		measured.err = e
		worker <- measured
	}()
	<-started
	completed := <-worker // independent reader watchdog releases mmap-blocked writers
	stats := r.s.db.Stats()
	var currentID int
	r.check(r.s.db.View(func(tx *bolt.Tx) error { currentID = tx.ID(); return nil }))
	close(release)
	reason := <-readerDone
	joined = true
	r.mutations += 2
	if completed.job.ID != "" {
		r.accepted++
	}
	if completed.markBytes > r.maxFile {
		r.maxFile = completed.markBytes
	}
	if completed.markAllocated > r.maxAllocated {
		r.maxAllocated = completed.markAllocated
	}
	r.emit(map[string]any{"kind": "physical", "operation": "reader-mark", "database_bytes": completed.markBytes, "allocated_bytes": completed.markAllocated, "free_bytes": completed.markFree, "stats": completed.markStats, "usage": completed.markUsage})
	r.sample("reader-mark-refill")
	r.emit(map[string]any{"kind": "reader-overlap", "worker_joined": true, "reader_joined": true, "reader_held_ns": time.Since(readerStarted).Nanoseconds(), "watchdog_ns": int64(10 * time.Second), "old_tx_id": oldID, "after_tx_id": currentID, "release": reason, "marked": completed.page.Marked, "refilled": completed.job.ID != "", "pending_pages_before_release": stats.PendingPageN, "open_readers_before_release": stats.OpenTxN})
	r.check(completed.err)
	r.changedBetween("reader-mark", before, completed.middle)
	if reason != "released" || currentID <= oldID || stats.OpenTxN < 1 || stats.PendingPageN == 0 || completed.page.Marked == 0 || completed.job.ID == "" {
		r.t.Fatal("HOLD: required coordinated old-reader GC/refill overlap not established")
	}
	r.readerOverlaps++
	r.cancel(completed.job)
}
func (r *retentionStress) requestGeometry(shape string) {
	var count, bytes, maxValue uint64
	r.check(r.s.db.View(func(tx *bolt.Tx) error {
		return retentionTable(tx, retainedRequestsBucket).ForEach(func(k, v []byte) error {
			count++
			bytes += uint64(len(k) + len(v))
			if uint64(len(v)) > maxValue {
				maxValue = uint64(len(v))
			}
			return nil
		})
	}))
	r.emit(map[string]any{"kind": "request-geometry", "shape": shape, "actual_records": count, "actual_key_value_bytes": bytes, "max_actual_value_bytes": maxValue, "record_limit": r.info().Policy.RequestRecords, "byte_limit": r.info().Policy.RequestBytes, "limiter": r.lastCapacity, "request_bytes_ceiling_reached": r.lastCapacity == RequestBytesQuota})
}

// Hash all actual key/value contents before and after GC. No expected private
// encoded records or mirrored charge formulas are used as the change oracle.
type stressValue struct {
	Hash           [32]byte
	Size, KeyBytes uint64
	Sequence       uint64
	Bucket         bool
}

func (r *retentionStress) logical() map[string]stressValue {
	out, err := stressLogical(r.s.db)
	r.check(err)
	return out
}
func stressLogical(db *bolt.DB) (map[string]stressValue, error) {
	out := make(map[string]stressValue)
	err := db.View(func(tx *bolt.Tx) error {
		var walk func(string, *bolt.Bucket) error
		walk = func(path string, b *bolt.Bucket) error {
			out[path] = stressValue{Sequence: b.Sequence(), Bucket: true}
			return b.ForEach(func(k, v []byte) error {
				p := path + "/" + hex.EncodeToString(k)
				if v == nil {
					return walk(p, b.Bucket(k))
				}
				out[p] = stressValue{Hash: sha256.Sum256(v), Size: uint64(len(v)), KeyBytes: uint64(len(k))}
				return nil
			})
		}
		return tx.ForEach(func(k []byte, b *bolt.Bucket) error { return walk(hex.EncodeToString(k), b) })
	})
	return out, err
}
func (r *retentionStress) changed(operation string, before map[string]stressValue) {
	r.changedBetween(operation, before, r.logical())
}
func (r *retentionStress) changedBetween(operation string, before, after map[string]stressValue) {
	var n uint64
	for k, old := range before {
		next, found := after[k]
		if !found || old != next {
			if old.Bucket {
				n += 16
			} else {
				n += old.KeyBytes + old.Size
				if found {
					n += next.Size
				}
			}
		}
		delete(after, k)
	}
	for _, v := range after {
		if v.Bucket {
			n += 16
		} else {
			n += v.KeyBytes + v.Size
		}
	}
	if n > r.maxChanged {
		r.maxChanged = n
	}
	r.emit(map[string]any{"kind": "logical-diff", "operation": operation, "changed_key_old_new_bytes": n})
	if n > 1<<20 {
		r.t.Fatalf("%s changed %d bytes, above 1 MiB", operation, n)
	}
}

func (r *retentionStress) watermarks() {
	r.t.Helper()
	binary := os.Getenv("SKYGO_RETENTION_STRESS_BINARY")
	want := os.Getenv("SKYGO_RETENTION_STRESS_BINARY_SHA256")
	data, e := os.ReadFile(binary)
	r.check(e)
	got := fmt.Sprintf("%x", sha256.Sum256(data))
	if len(want) != 64 || got != want {
		r.t.Fatal("HOLD: pinned actual integrated daemon binary required")
	}
	request := struct {
		StateDir     string `json:"state_dir"`
		BinarySHA256 string `json:"binary_sha256"`
	}{filepath.Join(r.root, "state"), got}
	encoded, e := json.Marshal(request)
	r.check(e)
	r.check(os.WriteFile(filepath.Join(r.evidence, "daemon-request.pending"), encoded, 0600))
	r.check(os.Rename(filepath.Join(r.evidence, "daemon-request.pending"), filepath.Join(r.evidence, "daemon-request.json")))
	var address string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join(r.evidence, "daemon-started.json"))
		if err == nil {
			var reply struct {
				Address      string `json:"address"`
				BinarySHA256 string `json:"binary_sha256"`
			}
			r.check(json.Unmarshal(b, &reply))
			if reply.BinarySHA256 != want {
				r.t.Fatal("supervisor daemon pin mismatch")
			}
			address = reply.Address
			break
		}
		if !os.IsNotExist(err) {
			r.t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if address == "" {
		r.t.Fatal("HOLD: external owned daemon supervisor did not respond")
	}
	// Original child handles and timeout cleanup are owned OUTSIDE this Go test.
	// A process-wide Go testing timeout therefore cannot bypass daemon ownership.
	r.check(os.Mkdir(filepath.Join(r.evidence, "no-program"), 0500))
	client := skyv1connect.NewControlServiceClient(&http.Client{Timeout: 3 * time.Second}, "http://"+address)
	info := r.awaitSample(client, func(a *skyv1.RetentionAdmin) bool { return *a.FilesystemFreeBytes >= 2<<30 })
	r.emit(map[string]any{"kind": "actual-coordinator-sample", "stage": "baseline", "admin": info.Admin, "binary_sha256": got})
	// These checks are physical only. Database threshold crossings not reached by
	// legitimate bounded churn remain explicitly unqualified in the final receipt.
	if *info.Admin.DatabaseFileBytes >= 256<<20 {
		r.probeRefusal(client, info, "admission_hold", false)
	}
	if *info.Admin.DatabaseFileBytes >= 320<<20 {
		r.probeRefusal(client, info, "maintenance_hold", true)
	}
	filler, e := os.OpenFile(filepath.Join(r.root, "free-space-fixture"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	r.check(e)
	defer func() { _ = filler.Close(); _ = os.Remove(filler.Name()) }()
	r.fillFree(filler, 1536<<20)
	info = r.awaitSample(client, func(a *skyv1.RetentionAdmin) bool {
		return *a.FilesystemFreeBytes < 2<<30 && *a.FilesystemFreeBytes > 1<<30
	})
	r.probeRefusal(client, info, "admission_hold", false)
	call, c := context.WithTimeout(context.Background(), 10*time.Second)
	result, e := client.PruneJobs(call, connect.NewRequest(&skyv1.PruneJobsRequest{Before: timestamppb.New(r.now.Add(time.Hour)), Mode: skyv1.PruneMode_PRUNE_MODE_WHOLE, Execute: true, PageSize: 1}))
	c()
	r.check(e)
	if result.Msg.Marked != 1 {
		r.t.Fatalf("maintenance below 2 GiB did not mark a real job: %+v", result.Msg)
	}
	r.emit(map[string]any{"kind": "guard", "stage": "below-2GiB", "maintenance_marked": result.Msg.Marked, "admin": info.Admin})
	r.fillFree(filler, 768<<20)
	info = r.awaitSample(client, func(a *skyv1.RetentionAdmin) bool { return *a.FilesystemFreeBytes < 1<<30 })
	r.probeRefusal(client, info, "storage_critical", false)
	r.probeRefusal(client, info, "storage_critical", true)
}
func (r *retentionStress) awaitSample(client skyv1connect.ControlServiceClient, match func(*skyv1.RetentionAdmin) bool) *skyv1.GetRetentionResponse {
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		response, err := client.GetRetention(ctx, connect.NewRequest(&skyv1.GetRetentionRequest{}))
		cancel()
		last = err
		if err == nil && response.Msg.Admin != nil {
			a := response.Msg.Admin
			if a.DatabaseFileBytes != nil && a.FilesystemFreeBytes != nil && a.ObservedAt != nil && time.Since(a.ObservedAt.AsTime()) < 5*time.Second && match(a) {
				r.recordGuardSample(a)
				return response.Msg
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.t.Fatalf("HOLD: no matching actual coordinator sample: %v", last)
	return nil
}
func (r *retentionStress) probeRefusal(client skyv1connect.ControlServiceClient, info *skyv1.GetRetentionResponse, condition string, maintenance bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if _, e := os.Lstat(filepath.Join(r.evidence, "no-program", "never-created")); !os.IsNotExist(e) {
		r.t.Fatalf("guard probe executable must remain absent: %v", e)
	}
	if maintenance {
		_, err = client.PruneJobs(ctx, connect.NewRequest(&skyv1.PruneJobsRequest{Before: timestamppb.New(r.now.Add(time.Hour)), Mode: skyv1.PruneMode_PRUNE_MODE_WHOLE, Execute: true, PageSize: 1}))
	} else {
		_, err = client.SubmitRetainedJob(ctx, connect.NewRequest(&skyv1.SubmitRetainedJobRequest{Domain: info.Domain, RequestId: "physical-guard-" + condition, Task: &skyv1.Task{Argv: []string{filepath.Join(r.evidence, "no-program", "never-created")}, Target: "local"}}))
	}
	r.recordGuardSample(info.Admin)
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), condition) {
		r.t.Fatalf("actual guard %q maintenance=%v: %v", condition, maintenance, err)
	}
	r.emit(map[string]any{"kind": "guard", "condition": condition, "maintenance": maintenance, "error": err.Error(), "admin": info.Admin})
}

// Free-space probes intentionally cross 2/1 GiB; they record actual samples
// without invoking the natural-churn preflight that forbids those crossings.
func (r *retentionStress) recordGuardSample(admin *skyv1.RetentionAdmin) {
	st, err := os.Stat(filepath.Join(r.root, "state", "jobs.db"))
	r.check(err)
	allocated := uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
	free, err := filesystemFree(r.root)
	r.check(err)
	if uint64(st.Size()) > r.maxFile {
		r.maxFile = uint64(st.Size())
	}
	if allocated > r.maxAllocated {
		r.maxAllocated = allocated
	}
	r.emit(map[string]any{"kind": "physical", "operation": "actual-coordinator-probe", "database_bytes": st.Size(), "allocated_bytes": allocated, "free_bytes": free, "coordinator_admin": admin})
	if uint64(st.Size()) > 320<<20 {
		r.t.Fatal("HOLD: actual database above 320 MiB")
	}
}

func (r *retentionStress) fillFree(f *os.File, target uint64) {
	block := make([]byte, 4<<20)
	for n := 0; n < 1024; n++ {
		free, e := filesystemFree(r.root)
		r.check(e)
		if free <= target {
			r.check(f.Sync())
			return
		}
		st, e := f.Stat()
		r.check(e)
		if st.Size() >= 3584<<20 {
			r.t.Fatal("HOLD: bounded disposable filler exhausted")
		}
		amount := uint64(len(block))
		if amount > free-target {
			amount = free - target
		}
		written, e := f.Write(block[:int(amount)])
		r.check(e)
		if written != int(amount) {
			r.t.Fatal(io.ErrShortWrite)
		}
	}
	r.t.Fatal("HOLD: bounded actual free-space probe exhausted")
}
