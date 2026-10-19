package store

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func retryCapacityTask(attempts uint32) backend.Task {
	return backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: attempts, ExitCodes: []int32{7}}}
}

func requireRetryCapacity(t *testing.T, err error, used, limit uint64) {
	t.Helper()
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != RetryBytesQuota || capacity.Used != used || capacity.Limit != limit || errors.Is(err, ErrFull) {
		t.Fatalf("capacity error=%v (%+v), want retry bytes used=%d limit=%d", err, capacity, used, limit)
	}
}

func TestRetryCapacityDefaultBoundary(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			var s *Store
			var domain RequestDomain
			if enabled {
				var info RetentionInfo
				s, info = enableTest(t, DefaultRetentionPolicy())
				domain = info.Domain
			} else {
				s = openTest(t)
			}
			submit := func(id string, task backend.Task, target, binding string) (backend.Job, error) {
				if enabled {
					return s.SubmitRetained(testActor(), domain, id, task, target, binding, backend.RemoteOutput, 0)
				}
				return s.Submit(testActor(), id, task, target, binding)
			}
			// Real admitted jobs reserve exactly 16 MiB: 204*10+8 attempts.
			// No counter or persisted admission row is fabricated.
			var first backend.Job
			for i := 0; i < 205; i++ {
				attempts := uint32(10)
				if i == 204 {
					attempts = 8
				}
				j, err := submit(fmt.Sprintf("job-%d", i), retryCapacityTask(attempts), "remote", "test-binding")
				if err != nil {
					t.Fatalf("fill job %d: %v", i, err)
				}
				if i == 0 {
					first = j
				}
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
			before := routingState(t, s.db)
			_, err := submit("refused", retryCapacityTask(2), "remote", "test-binding")
			if enabled {
				requireRetryCapacity(t, err, maxRetryBytes, DefaultRetentionPolicy().RetryBytes)
			} else if !errors.Is(err, ErrFull) {
				t.Fatalf("legacy capacity error=%v, want ErrFull", err)
			}
			if !bytes.Equal(before, routingState(t, s.db)) {
				t.Fatal("failed admission changed rows, counters, indexes or sequences")
			}
			// Replay precedes capacity and route validation, including at full.
			replay, err := submit("job-0", first.Task, "", "")
			if err != nil || !reflect.DeepEqual(replay, first) {
				t.Fatalf("full replay=%+v error=%v", replay, err)
			}
			if !bytes.Equal(before, routingState(t, s.db)) {
				t.Fatal("replay changed retained rows or admission counters")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetryCapacityConfiguredAndGrandfathered(t *testing.T) {
	for _, grandfathered := range []bool{false, true} {
		t.Run(fmt.Sprintf("grandfathered=%t", grandfathered), func(t *testing.T) {
			p := DefaultRetentionPolicy()
			p.RetryBytes = 10 * maxAttemptBytes
			var s *Store
			var info RetentionInfo
			var err error
			used := p.RetryBytes
			if grandfathered {
				dir := t.TempDir()
				s, err = Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if _, err := s.Submit(testActor(), fmt.Sprintf("legacy-%d", i), retryCapacityTask(10), "remote", "test-binding"); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				info, err = EnableRetention(dir, p)
				if err != nil {
					t.Fatal(err)
				}
				s, err = Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				used *= 2
			} else {
				s, info = enableTest(t, p)
				if _, err := s.SubmitRetained(testActor(), info.Domain, "accepted", retryCapacityTask(10), "remote", "test-binding", backend.RemoteOutput, 0); err != nil {
					t.Fatal(err)
				}
			}
			before := routingState(t, s.db)
			_, err = s.SubmitRetained(testActor(), info.Domain, "refused", retryCapacityTask(2), "remote", "test-binding", backend.RemoteOutput, 0)
			requireRetryCapacity(t, err, used, p.RetryBytes)
			if !bytes.Equal(before, routingState(t, s.db)) {
				t.Fatal("lower-policy refusal changed durable state")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetryCapacityMalformedCounterIsNotQuota(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataBucket).Put(retryBytesKey, []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	before := routingState(t, s.db)
	_, err := s.SubmitRetained(testActor(), info.Domain, "refused", retryCapacityTask(2), "remote", "test-binding", backend.RemoteOutput, 0)
	var capacity *CapacityError
	if err == nil || err.Error() != "invalid retry byte counter" || errors.As(err, &capacity) {
		t.Fatalf("corrupt counter error=%v", err)
	}
	if !bytes.Equal(before, routingState(t, s.db)) {
		t.Fatal("corrupt-counter refusal changed durable state")
	}
	// This deliberately corrupt fixture must remain invalid.
	if err := s.db.View(validateSnapshot); err == nil {
		t.Fatal("malformed counter accepted by validator")
	}
}
