package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func retainedStore(t *testing.T) (*store.Store, store.RetentionInfo) {
	t.Helper()
	return retainedStorePolicy(t, store.DefaultRetentionPolicy())
}
func retainedStorePolicy(t *testing.T, policy store.RetentionPolicy) (*store.Store, store.RetentionInfo) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := store.EnableRetention(dir, policy)
	if err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, info
}
func retainedTarget(b backend.Backend) backend.Target {
	target := targetFor("good", b)
	target.OutputStorage = backend.RemoteOutput
	return target
}
func TestRetainedReplayAndSparseListing(t *testing.T) {
	s, info := retainedStore(t)
	b := &routeBackend{}
	c := routeControl(t, s, retainedTarget(b))
	var jobs []backend.Job
	for i := 0; i < 3; i++ {
		j, err := c.SubmitRetained(testActor(), info.Domain, fmt.Sprint(i), backend.Task{Argv: []string{"work"}})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, j)
	}
	if _, err := s.Update(jobs[1].ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	page, _, err := c.Prune(context.Background(), testActor(), store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.Whole}, true, "")
	if err != nil || page.Marked != 1 {
		t.Fatalf("mark middle: %+v %v", page, err)
	}
	first, token, err := c.List(testActor(), 1, "")
	if err != nil || len(first) != 1 || first[0].ID != jobs[0].ID || token == "" {
		t.Fatal(first, token, err)
	}
	second, next, err := c.List(testActor(), 1, token)
	if err != nil || len(second) != 1 || second[0].ID != jobs[2].ID || next != "" {
		t.Fatal(second, next, err)
	}
	replacement := retainedTarget(&routeBackend{})
	replacement.Name = "replacement"
	replacement.Binding = "changed"
	other := routeControl(t, s, replacement)
	replay, err := other.SubmitRetained(testActor(), info.Domain, "0", jobs[0].Task)
	if err != nil || replay.ID != jobs[0].ID {
		t.Fatal("replay consulted placement", err)
	}
}

func TestRetentionTargetContradictionsAndCopy(t *testing.T) {
	s := routeStore(t)
	b := &routeBackend{}
	noop := func(context.Context, backend.OutputReference) error { return nil }
	for _, target := range []backend.Target{
		{Name: "bad", Binding: "binding", Backend: b, OutputStorage: backend.OutputStorage(99)},
		{Name: "bad", Binding: "binding", Backend: b, OutputStorage: backend.RemoteOutput, OutputReservation: 1},
		{Name: "bad", Binding: "binding", Backend: b, OutputStorage: backend.LocalOutput},
		{Name: "bad", Binding: "binding", Backend: b, Retention: &backend.Retention{RemoveOutput: noop}},
	} {
		if _, err := New(s, []backend.Target{target}, testEngine); err == nil {
			t.Fatal("contradictory output facts accepted")
		}
	}
	target := targetFor("good", b)
	target.OutputStorage = backend.LocalOutput
	target.OutputReservation = 1 << 20
	target.Retention = &backend.Retention{CheckOutputClosed: func(context.Context, backend.OutputReference) (backend.OutputReceipt, error) {
		return backend.OutputReceipt{}, nil
	}, RemoveOutput: noop}
	c := routeControl(t, s, target)
	target.Retention.RemoveOutput = nil
	got, err := c.Targets(testActor())
	if err != nil || got[0].Retention.RemoveOutput == nil {
		t.Fatal("constructor retained caller callback pointer", err)
	}
	got[0].Retention.RemoveOutput = nil
	again, _ := c.Targets(testActor())
	if again[0].Retention.RemoveOutput == nil {
		t.Fatal("discovery shared callback pointer")
	}
}

type retainedLogBackend struct {
	*routeBackend
	entered chan struct{}
	release chan struct{}
}

func (b *retainedLogBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	close(b.entered)
	<-b.release
	return backend.LogChunk{Complete: true}, nil
}
func TestLogLeaseRetainsActualBackendAfterClose(t *testing.T) {
	s := routeStore(t)
	b := &retainedLogBackend{routeBackend: &routeBackend{}, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(b.release) }) }
	defer unblock()
	c := routeControl(t, s, targetFor("good", b))
	j := seedJob(t, s, "log", "good", backend.Running)
	lease, err := c.OpenLogs(testActor(), j.ID, 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := lease.Read(context.Background(), testActor(), 0); done <- err }()
	waitValue(t, b.entered)
	lease.Close()
	c.retention.mu.Lock()
	blocked, err := c.blockedLocked()
	n := len(c.retention.leases)
	c.retention.mu.Unlock()
	if err != nil || n != 1 || len(blocked) != 1 || blocked[0] != j.ID {
		t.Fatal("Close released actual read owner", blocked, n, err)
	}
	unblock()
	if err := waitValue(t, done); err != nil {
		t.Fatal(err)
	}
	c.retention.mu.Lock()
	n = len(c.retention.leases)
	c.retention.mu.Unlock()
	if n != 0 {
		t.Fatal("completed backend retained closed lease")
	}
}

func TestRetainedDryRunHasNoAttributableWrites(t *testing.T) {
	s, info := retainedStore(t)
	b := &routeBackend{}
	c := routeControl(t, s, retainedTarget(b))
	j, err := c.SubmitRetained(testActor(), info.Domain, "dry", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.Directory(), "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	page, _, err := c.Prune(context.Background(), testActor(), store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true}, false, "")
	if err != nil || page.Selected != 1 || page.Marked != 0 {
		t.Fatalf("dry-run: %+v %v", page, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("dry-run wrote database", err)
	}
	if len(c.access) != 0 || b.ioCalls() != 0 {
		t.Fatal("dry-run queued audit or performed backend mutation")
	}
}

func TestPruneQueryBudgetPreservesCompleteSubjectsAndSweepProgress(t *testing.T) {
	s, info := retainedStore(t)
	b := &routeBackend{}
	policy, _, err := s.RetentionPolicy()
	if err != nil {
		t.Fatal(err)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*policy.OutputUnit
	target := targetFor("good", b)
	target.OutputStorage = backend.LocalOutput
	target.OutputReservation = reserve
	target.SupportsRetry = true
	var queries atomic.Int32
	var c *Control
	target.Retention = &backend.Retention{CheckOutputClosed: func(_ context.Context, r backend.OutputReference) (backend.OutputReceipt, error) {
		queries.Add(1)
		// This test isolates logical query/transaction budgets; refresh its synthetic
		// storage observation so slow race instrumentation does not test wall time.
		c.retention.mu.Lock()
		c.retention.sample.ObservedAt = time.Now().UTC()
		c.retention.mu.Unlock()
		return backend.OutputReceipt{ClosureID: "closed-" + r.AttemptID, PayloadReleased: true}, nil
	}, RemoveOutput: func(context.Context, backend.OutputReference) error { t.Error("prune dispatched cleanup"); return nil }}
	c, err = NewWithLocalOutput(s, []backend.Target{target}, testEngine, LocalOutputConfig{Target: target.Name, Binding: target.Binding, BindOutput: func(backend.OutputReference) error { return nil }, OutputUsage: func(context.Context) (uint64, uint64, error) { return 0, 0, nil }})
	if err != nil {
		t.Fatal(err)
	}
	primeStorage(c)
	for i := 0; i < 12; i++ {
		primeStorage(c)
		task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 10, DelaySeconds: 1, ExitCodes: []int32{7}}}
		j, err := c.SubmitRetained(testActor(), info.Domain, fmt.Sprint(i), task)
		if err != nil {
			t.Fatal(err)
		}
		now := j.CreatedAt
		for attempt := 0; attempt < 10; attempt++ {
			var began bool
			j, began, err = s.BeginAttempt(j.ID, j.CurrentAttemptID(), reserve, now)
			if err != nil || !began {
				t.Fatal("begin", err)
			}
			code := int32(7)
			j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: fmt.Sprint("owned-", i, "-", attempt)}, now.Add(time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if j.NextAttemptAt != nil {
				now = *j.NextAttemptAt
			}
		}
	}
	primeStorage(c)
	selection := store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.OutputOnly}
	first, _, err := c.Prune(context.Background(), testActor(), selection, true, "")
	if err != nil || first.Marked != 9 || queries.Load() != 100 {
		t.Fatalf("first bounded sweep %+v queries%d %v", first, queries.Load(), err)
	}
	primeStorage(c)
	// Nine complete jobs consume99 of the Store's100 available header/child
	// IDs. The tenth proof set remains durable but its job is unmarked, so the
	// next bounded sweep re-queries its domains alongside the two unproved jobs.
	second, _, err := c.Prune(context.Background(), testActor(), selection, true, "")
	if err != nil || second.Marked != 3 || queries.Load() != 130 {
		t.Fatalf("second complete sweep %+v queries%d %v", second, queries.Load(), err)
	}
}

func TestRetentionCursorCanonicalBounds(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	cur := c.scopeLogCursor(testActor(), "")
	cur.Job = "11111111111111111111111111111111"
	cur.Attempt = cur.Job
	cur.Generation = 1

	token, err := encodeLogCursor(cur)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeLogCursor(token)
	if err != nil || parsed != cur {
		t.Fatal(parsed, err)
	}
	if _, err = decodeLogCursor(token + "="); !errors.Is(err, ErrInvalid) {
		t.Fatal("padded cursor accepted", err)
	}
	cur.Principal = string(make([]byte, 600))
	if _, err = encodeLogCursor(cur); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized cursor accepted", err)
	}
}

func TestPruneCursorBindsExecutionMode(t *testing.T) {
	s, info := retainedStore(t)
	c := routeControl(t, s, retainedTarget(&routeBackend{}))
	for i := 0; i < 2; i++ {
		j, err := c.SubmitRetained(testActor(), info.Domain, fmt.Sprint(i), backend.Task{Argv: []string{"work"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	selection := store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true, Limit: 1}
	_, token, err := c.Prune(context.Background(), testActor(), selection, false, "")
	if err != nil || token == "" {
		t.Fatal("missing dry-run continuation", err)
	}
	if _, _, err = c.Prune(context.Background(), testActor(), selection, true, token); !errors.Is(err, ErrInvalid) {
		t.Fatal("dry-run cursor accepted for execute", err)
	}
	if _, _, err = c.Prune(context.Background(), testActor(), selection, false, token); err != nil {
		t.Fatal("same-mode continuation rejected", err)
	}
}

func TestLocalOutputConfigurationCannotBypassPersistedPolicy(t *testing.T) {
	policy := store.DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	s, _ := retainedStorePolicy(t, policy)
	remote := retainedTarget(&routeBackend{})
	if _, err := New(s, []backend.Target{remote}, testEngine); err != nil {
		t.Fatal("remote-only configuration requires local output", err)
	}
	local := remote
	local.OutputStorage = backend.LocalOutput
	local.OutputReservation = backend.MaxLogBytes
	if _, err := NewWithLocalOutput(s, []backend.Target{local}, testEngine, LocalOutputConfig{Target: local.Name, Binding: local.Binding, BindOutput: func(backend.OutputReference) error { return nil }, OutputUsage: func(context.Context) (uint64, uint64, error) { return 0, 0, nil }}); err == nil {
		t.Fatal("disabled local output enabled by callbacks")
	}
	enabled, _ := retainedStore(t)
	target := outputTarget(t, enabled, &routeBackend{}, nil, nil)
	if _, err := New(enabled, []backend.Target{target}, testEngine); err == nil {
		t.Fatal("standalone constructor bypassed mandatory local binder")
	}
}

func TestLogLeaseBoundAndCursorConflict(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	job := seedJob(t, s, "log-bound", "good", backend.Running)
	var leases []*LogLease
	defer func() {
		for _, l := range leases {
			l.Close()
		}
	}()
	for i := 0; i < 32; i++ {
		l, err := c.OpenLogs(testActor(), job.ID, 0, "", 0)
		if err != nil {
			t.Fatal(i, err)
		}
		leases = append(leases, l)
	}
	if _, err := c.OpenLogs(testActor(), job.ID, 0, "", 0); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lease bound exceeded", err)
	}
	token, err := leases[0].Cursor(123)
	if err != nil {
		t.Fatal(err)
	}
	leases[0].Close()
	if _, err = c.OpenLogs(testActor(), job.ID, 0, token, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("cursor and raw offset accepted", err)
	}
	l, err := c.OpenLogs(testActor(), job.ID, 1, token, 0)
	if err != nil {
		t.Fatal(err)
	}
	leases = append(leases, l)
	if l.Offset() != 123 || l.AttemptID() != job.ID {
		t.Fatal("resume changed fixed selector")
	}
}

func TestRetentionSampleStaleAndCopied(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	value := uint64(12)
	c.retention.sample = &RetentionAdmin{DatabaseFileBytes: &value, ObservedAt: time.Now().Add(-6 * time.Second)}
	got, err := c.RetentionInfo(testActor(), 0, "")
	if err != nil || got.Admin == nil || got.Admin.DatabaseFileBytes != nil || len(got.Admin.Conditions) != 1 || got.Admin.Conditions[0] != "metrics_stale" {
		t.Fatal("stale value published", got, err)
	}
	if c.retention.sample.DatabaseFileBytes == nil {
		t.Fatal("read mutated observation cache")
	}
	c.retention.sample.ObservedAt = time.Now()
	got, err = c.RetentionInfo(testActor(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	*got.Admin.DatabaseFileBytes = 99
	if *c.retention.sample.DatabaseFileBytes != 12 {
		t.Fatal("response shared mutable cache value")
	}
}

// primeStorage supplies deterministic observed guard data for coordinator tests.
// It is not filesystem or native-backend qualification evidence.
func primeStorage(c *Control) {
	database, free, output, files := uint64(1<<20), uint64(4<<30), uint64(0), uint64(0)
	c.retention.sample = &RetentionAdmin{DatabaseFileBytes: &database, FilesystemFreeBytes: &free, OutputAllocatedBytes: &output, OutputFileCount: &files, ObservedAt: time.Now().UTC()}
}

func TestCursorMaximumScopeAndInvalidUTF8(t *testing.T) {
	c := routeControl(t, routeStore(t), targetFor("good", &routeBackend{}))
	cur := c.scopeLogCursor(testActor(), "11111111111111111111111111111111")
	cur.Principal = string(bytes.Repeat([]byte{'p'}, 64))
	cur.Job = "22222222222222222222222222222222"
	cur.Attempt = "33333333333333333333333333333333"
	cur.Generation = 1
	cur.Offset = backend.MaxLogBytes
	token, err := encodeLogCursor(cur)
	if err != nil || len(token) > 683 {
		t.Fatal("supported maximum scope cannot encode", len(token), err)
	}
	parsed, err := decodeLogCursor(token)
	if err != nil || parsed != cur {
		t.Fatal(parsed, err)
	}
	raw := []byte(`{"v":1,"k":"logs","i":"`)
	raw = append(raw, 0xff)
	raw = append(raw, []byte(`"}`)...)
	invalid := base64.RawURLEncoding.EncodeToString(raw)
	if _, err = decodeLogCursor(invalid); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid UTF8 reached decoder", err)
	}
	cur.Version = 2
	token, err = encodeLogCursor(cur)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeLogCursor(token); !errors.Is(err, ErrInvalid) {
		t.Fatal("future cursor version accepted", err)
	}
}

func TestDisabledListPreservesLegacyCursorShape(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	for _, id := range []string{"first", "second"} {
		if _, err := c.Submit(testActor(), id, backend.Task{Argv: []string{"work"}}); err != nil {
			t.Fatal(err)
		}
	}
	_, token, err := c.List(testActor(), 1, "")
	if err != nil || token == "" {
		t.Fatal(token, err)
	}
	var old legacyCursor
	if err = decodeCursor(token, &old, 256); err != nil || old.After != 1 || old.Max != 2 {
		t.Fatal("disabled listing emitted new cursor shape", old, err)
	}
	if _, err = decodeRetentionCursor(token); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy token accepted as enabled v1", err)
	}
	rows, next, err := c.List(testActor(), 1, token)
	if err != nil || len(rows) != 1 || next != "" {
		t.Fatal(rows, next, err)
	}
}

func TestConfiguredTargetNamesAreBoundedPrintableASCII(t *testing.T) {
	s := routeStore(t)
	for _, name := range []string{"", string(bytes.Repeat([]byte{'x'}, 129)), "line\nfeed", "nul\x00name", "del\x7f", "历史"} {
		target := targetFor(name, &routeBackend{})
		if _, err := New(s, []backend.Target{target}, testEngine); err == nil {
			t.Fatalf("invalid configured name %q accepted", name)
		}
	}
	target := targetFor(string(bytes.Repeat([]byte{'x'}, 128)), &routeBackend{})
	if _, err := New(s, []backend.Target{target}, testEngine); err != nil {
		t.Fatal("maximum printable configured name refused", err)
	}
	// Configuration rules never rewrite or reject a historical accepted job.
	historical, err := s.Submit(testActor(), "historical-target", backend.Task{Argv: []string{"work"}}, "历史", "historical-binding")
	if err != nil {
		t.Fatal(err)
	}
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	got, err := c.Get(testActor(), historical.ID)
	if err != nil || got.Target != "历史" {
		t.Fatal("historical target changed", got, err)
	}
}

func TestFuturePruneAndObligationHighwaterAreInvalidSelectors(t *testing.T) {
	s, info := retainedStore(t)
	c := routeControl(t, s, retainedTarget(&routeBackend{}))
	selection := store.PruneSelection{Before: time.Now(), Mode: store.Whole}
	raw, err := json.Marshal(struct {
		Before  string          `json:"before"`
		Mode    store.PruneMode `json:"mode"`
		Ack     bool            `json:"ack"`
		Execute bool            `json:"execute"`
	}{selection.Before.UTC().Format(time.RFC3339Nano), selection.Mode, false, false})
	if err != nil {
		t.Fatal(err)
	}
	prune := c.scopeCursor(testActor(), "prune", info.Domain.Lineage)
	prune.After, prune.Max = 1, 100
	prune.Selector = fmt.Sprintf("%x", sha256.Sum256(raw))
	token, err := encodeRetentionCursor(prune)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Prune(context.Background(), testActor(), selection, false, token); !errors.Is(err, ErrInvalid) {
		t.Fatal("future prune high-water is not invalid_argument", err)
	}
	obligations := c.scopeCursor(testActor(), "obligations", info.Domain.Lineage)
	obligations.After, obligations.Max = 1, 100
	token, err = encodeRetentionCursor(obligations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.RetentionInfo(testActor(), 0, token); !errors.Is(err, ErrInvalid) {
		t.Fatal("future obligation high-water is not invalid_argument", err)
	}
}
