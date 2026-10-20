package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func retryJob(t *testing.T, s *Store, request string, delay uint32) backend.Job {
	t.Helper()
	j, err := s.Submit(testActor(), request, backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 3, DelaySeconds: delay, ExitCodes: []int32{7}}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func beginTest(t *testing.T, s *Store, j backend.Job, now time.Time) backend.Job {
	t.Helper()
	got, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now)
	if err != nil || !start {
		t.Fatalf("begin: %v %v %+v", start, err, got)
	}
	return got
}
func failedStatus() backend.Status {
	code := int32(7)
	return backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "native-one"}
}
func failTest(t *testing.T, s *Store, j backend.Job, now time.Time) backend.Job {
	t.Helper()
	got, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), failedStatus(), now)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func assertJob(t *testing.T, s *Store, want backend.Job) {
	t.Helper()
	got, err := s.Get(want.ID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("job changed: got %+v want %+v err %v", got, want, err)
	}
}

func TestAttemptLifecycle(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "job", 1)
	now := j.CreatedAt.Add(time.Second)
	original, _ := backend.TaskBytes(j.Task)
	if j.CurrentAttemptID() != j.ID || len(j.Attempts) != 1 || j.Attempts[0].Status.State != backend.Queued {
		t.Fatal(j)
	}
	j = beginTest(t, s, j, now)
	if _, start, err := s.BeginAttempt(j.ID, j.ID, 0, now); err != nil || start {
		t.Fatalf("duplicate begin %v %v", start, err)
	}
	j = failTest(t, s, j, now.Add(time.Second))
	if j.Status.State != backend.Retrying || !j.NextAttemptAt.Equal(now.Add(2*time.Second)) || j.Attempts[0].FinishedAt == nil {
		t.Fatal(j)
	}
	same, err := s.ObserveAttempt(j.ID, j.ID, failedStatus(), now.Add(time.Hour))
	if err != nil || !reflect.DeepEqual(j, same) {
		t.Fatalf("duplicate resets deadline %v", err)
	}
	if _, start, err := s.BeginAttempt(j.ID, j.ID, 0, now.Add(time.Second)); err != nil || start {
		t.Fatalf("early begin %v %v", start, err)
	}
	first := j.Attempts[0]
	j = beginTest(t, s, j, *j.NextAttemptAt)
	if len(j.Attempts) != 2 || j.CurrentAttemptID() == j.ID || j.NextAttemptAt != nil || j.Status.Instance != "" || !reflect.DeepEqual(j.Attempts[0], first) {
		t.Fatal(j)
	}
	stale, err := s.ObserveAttempt(j.ID, j.ID, failedStatus(), now.Add(time.Hour))
	if err != nil || !reflect.DeepEqual(j, stale) {
		t.Fatalf("stale observation %v", err)
	}
	code := int32(0)
	j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, ExitCode: &code, Instance: "native-two"}, now.Add(3*time.Second))
	if err != nil || j.Status.State != backend.Succeeded || j.Attempts[1].FinishedAt == nil {
		t.Fatalf("finish %+v %v", j, err)
	}
	final, _ := backend.TaskBytes(j.Task)
	if !bytes.Equal(original, final) {
		t.Fatal("task changed across attempts")
	}
	if n, err := s.LastSequence(); err != nil || n != 1 {
		t.Fatalf("retry advances logical sequence %d %v", n, err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptCancellationBeforeDue(t *testing.T) {
	for _, queued := range []bool{true, false} {
		t.Run(map[bool]string{true: "queued", false: "retrying"}[queued], func(t *testing.T) {
			s := openTest(t)
			j := retryJob(t, s, "job", 3600)
			now := j.CreatedAt.Add(time.Second)
			if !queued {
				j = failTest(t, s, beginTest(t, s, j, now), now)
			}
			history, _ := json.Marshal(j.Attempts)
			j, err := s.UpdateFor(testActor(), j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
			if err != nil {
				t.Fatal(err)
			}
			stale, start, err := s.BeginAttempt(j.ID, "stale", 0, now)
			if err != nil || start || !reflect.DeepEqual(stale, j) {
				t.Fatalf("stale canceled wrong attempt: %v", err)
			}
			got, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now)
			if err != nil || start || got.Status.State != backend.Canceled || got.NextAttemptAt != nil || len(got.Attempts) != 1 {
				t.Fatalf("cancel before deadline: %+v %v %v", got, start, err)
			}
			if !queued {
				after, _ := json.Marshal(got.Attempts)
				if !bytes.Equal(history, after) {
					t.Fatal("backoff cancellation rewrote failed history")
				}
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAttemptSingleStartConcurrent(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "job", 1)
	now := j.CreatedAt.Add(time.Second)
	var wg sync.WaitGroup
	starts := make(chan bool, 32)
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, start, err := s.BeginAttempt(j.ID, j.ID, 0, now)
			starts <- start
			errs <- err
		}()
	}
	wg.Wait()
	close(starts)
	close(errs)
	n := 0
	for start := range starts {
		if start {
			n++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n != 1 {
		t.Fatalf("starts=%d", n)
	}
}

func TestAttemptNoRetryWithoutSelectedStoppedEvidence(t *testing.T) {
	code := int32(7)
	zero := int32(0)
	other := int32(8)
	for _, test := range []struct {
		name   string
		status backend.Status
		cancel bool
	}{
		{"missing-proof", backend.Status{State: backend.Failed, ExitCode: &code}, false},
		{"missing-code", backend.Status{State: backend.Failed}, false},
		{"zero", backend.Status{State: backend.Failed, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, false},
		{"other", backend.Status{State: backend.Failed, ExitCode: &other, Termination: backend.StoppedWithExitCode}, false},
		{"unknown", backend.Status{State: backend.Unknown}, false},
		{"canceled", failedStatus(), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			j := retryJob(t, s, "job", 1)
			now := j.CreatedAt.Add(time.Second)
			j = beginTest(t, s, j, now)
			if test.cancel {
				var err error
				j, err = s.Update(j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			j, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), test.status, now)
			if err != nil || j.Status.State == backend.Retrying || j.NextAttemptAt != nil {
				t.Fatalf("unsafe retry %+v %v", j, err)
			}
		})
	}
	s := openTest(t)
	j := retryJob(t, s, "exhaust", 1)
	now := j.CreatedAt.Add(time.Second)
	for range 3 {
		j = beginTest(t, s, j, now)
		j = failTest(t, s, j, now)
		now = now.Add(time.Second)
	}
	if j.Status.State != backend.Failed || len(j.Attempts) != 3 || j.NextAttemptAt != nil {
		t.Fatal(j)
	}
}

func TestAttemptStickyInstance(t *testing.T) {
	for _, incoming := range []string{"", "native-one", "replacement"} {
		t.Run(incoming, func(t *testing.T) {
			s := openTest(t)
			j := retryJob(t, s, "job", 1)
			now := j.CreatedAt.Add(time.Second)
			j = beginTest(t, s, j, now)
			var err error
			j, err = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Running, Instance: "native-one"}, now)
			if err != nil {
				t.Fatal(err)
			}
			status := failedStatus()
			status.Instance = incoming
			j, err = s.ObserveAttempt(j.ID, j.ID, status, now)
			if err != nil || j.Status.Instance != "native-one" || j.Attempts[0].Status.Instance != "native-one" {
				t.Fatalf("lost pin %+v %v", j, err)
			}
			if incoming == "replacement" && (j.Status.State != backend.Unknown || j.Status.Termination != 0 || j.NextAttemptAt != nil) {
				t.Fatal("replacement granted evidence")
			}
		})
	}
}

func TestAttemptUpdateCannotRewriteHistory(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*backend.Job)
	}{
		{"history-status", func(j *backend.Job) { j.Attempts[0].Status.State = backend.Failed }},
		{"history-time", func(j *backend.Job) { *j.Attempts[0].StartingAt = j.CreatedAt }},
		{"policy", func(j *backend.Job) { j.Task.Retry.ExitCodes[0] = 8 }},
		{"proof", func(j *backend.Job) { j.Status.Termination = backend.StoppedWithExitCode }},
		{"instance", func(j *backend.Job) { j.Status.Instance = "other" }},
		{"state", func(j *backend.Job) { j.Status.State = backend.Running }},
		{"deadline", func(j *backend.Job) { v := j.CreatedAt; j.NextAttemptAt = &v }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			j := retryJob(t, s, "job", 1)
			j = beginTest(t, s, j, j.CreatedAt.Add(time.Second))
			_, err := s.Update(j.ID, func(j *backend.Job) error { test.change(j); return nil })
			if err == nil {
				t.Fatal("rewrite accepted")
			}
			assertJob(t, s, j)
		})
	}
}

func TestAttemptBoundsAndReservation(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "job", 1)
	now := j.CreatedAt.Add(time.Second)
	j = beginTest(t, s, j, now)
	status := failedStatus()
	status.Reason = strings.Repeat("世", 600)
	j, err := s.ObserveAttempt(j.ID, j.ID, status, now)
	if err != nil || len(j.Status.Reason) > 1024 || len(j.Attempts[0].Status.Reason) > 1024 {
		t.Fatalf("reason bounds %v", err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataBucket).Put(retryBytesKey, sequenceKey(maxRetryBytes))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(testActor(), "full", j.Task, "local", "test-binding"); !errors.Is(err, ErrFull) {
		t.Fatalf("reservation full: %v", err)
	}
	if n, _ := s.LastSequence(); n != 1 {
		t.Fatal("full submission consumed sequence")
	}
	j = beginTest(t, s, j, *j.NextAttemptAt)
	if len(j.Attempts) != 2 {
		t.Fatal("reserved retry failed")
	}
}

func TestAttemptCollisionRollback(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "job", 1)
	now := j.CreatedAt.Add(time.Second)
	j = failTest(t, s, beginTest(t, s, j, now), now)
	for _, candidate := range []string{j.ID, "invalid"} {
		err := s.db.Update(func(tx *bolt.Tx) error {
			_, _, err := beginAttempt(tx, j.ID, j.ID, candidate, "", 0, *j.NextAttemptAt)
			return err
		})
		if err == nil {
			t.Fatal("collision accepted")
		}
		assertJob(t, s, j)
	}
	candidate := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	injected := errors.New("after transition")
	err := s.db.Update(func(tx *bolt.Tx) error {
		_, start, err := beginAttempt(tx, j.ID, j.ID, candidate, "", 0, *j.NextAttemptAt)
		if err != nil {
			return err
		}
		if !start {
			t.Fatal("no start")
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	assertJob(t, s, j)
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(attemptJobsBucket).Get([]byte(candidate)) != nil {
			t.Fatal("index survived rollback")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptNormalizedReplayAndCopies(t *testing.T) {
	s := openTest(t)
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{8, 7, 7}}}
	j, err := s.Submit(testActor(), "same", task, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	task.Retry.ExitCodes[0] = 99
	j.Task.Retry.ExitCodes[0] = 66
	canonical := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 3, DelaySeconds: 1, ExitCodes: []int32{7, 8}}}
	replay, err := s.Submit(testActor(), "same", canonical, "", "")
	if err != nil || replay.Task.Retry.ExitCodes[0] != 7 {
		t.Fatalf("canonical replay %+v %v", replay, err)
	}
	replay = beginTest(t, s, replay, replay.CreatedAt.Add(time.Second))
	saved := *replay.Attempts[0].StartingAt
	*replay.Attempts[0].StartingAt = time.Time{}
	stored, err := s.Get(replay.ID)
	if err != nil || !stored.Attempts[0].StartingAt.Equal(saved) {
		t.Fatal("timestamp alias")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		raw, _ := backend.TaskBytes(canonical)
		if binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey)) != uint64(len(raw)) {
			t.Fatal("replay charged task twice")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	s, err := Open(source)
	if err != nil {
		t.Fatal(err)
	}
	j := retryJob(t, s, "job", 3600)
	now := j.CreatedAt.Add(time.Second)
	j = failTest(t, s, beginTest(t, s, j, now), now)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot.db")
	if err := Backup(source, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(dir, "restored")
	if err := Restore(snapshot, restored); err != nil {
		t.Fatal(err)
	}
	s, err = Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertJob(t, s, j)
	j = beginTest(t, s, j, *j.NextAttemptAt)
	if len(j.Attempts) != 2 {
		t.Fatal(j)
	}
}

func TestAttemptStartingObservation(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "starting", 1)
	now := j.CreatedAt.Add(time.Second)
	j = beginTest(t, s, j, now)
	var err error
	j, err = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Starting, Instance: "created"}, now)
	if err != nil || j.Status.State != backend.Starting || j.Status.Instance != "created" || j.Attempts[0].Status.Instance != "created" {
		t.Fatalf("starting observation: %+v %v", j, err)
	}
	j, err = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Running}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Starting}, now); err == nil {
		t.Fatal("running regressed to starting")
	}
	assertJob(t, s, j)
}
func TestAttemptUpdateReasonBound(t *testing.T) {
	s := openTest(t)
	j := retryJob(t, s, "reason", 1)
	if _, err := s.Update(j.ID, func(j *backend.Job) error { j.Status.Reason = strings.Repeat("x", 1025); return nil }); err == nil {
		t.Fatal("unbounded reason accepted")
	}
	assertJob(t, s, j)
}
