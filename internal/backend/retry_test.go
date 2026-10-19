package backend_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestNormalizeRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *backend.RetryPolicy
		want *backend.RetryPolicy
	}{
		{"absent", nil, nil},
		{"zero", &backend.RetryPolicy{}, nil},
		{"single", &backend.RetryPolicy{MaxAttempts: 1}, nil},
		{"single empty", &backend.RetryPolicy{MaxAttempts: 1, ExitCodes: []int32{}}, nil},
		{"default delay", &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}, &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}},
		{"sorted unique", &backend.RetryPolicy{MaxAttempts: 10, DelaySeconds: 3600, ExitCodes: []int32{255, 7, 1, 7}}, &backend.RetryPolicy{MaxAttempts: 10, DelaySeconds: 3600, ExitCodes: []int32{1, 7, 255}}},
		{"minimum", &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}, &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			got, err := backend.NormalizeRetry(tc.in)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("NormalizeRetry = %#v, %v; want %#v", got, err, tc.want)
			}
			again, err := backend.NormalizeRetry(got)
			if err != nil || !reflect.DeepEqual(again, got) {
				t.Fatalf("normalization not idempotent: %#v, %v", again, err)
			}
			after, err := json.Marshal(tc.in)
			if err != nil || string(after) != string(before) {
				t.Fatal("input changed")
			}
		})
	}
}

func TestNormalizeRetryRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   backend.RetryPolicy
	}{
		{"zero delay", backend.RetryPolicy{DelaySeconds: 1}},
		{"zero codes", backend.RetryPolicy{ExitCodes: []int32{7}}},
		{"single delay", backend.RetryPolicy{MaxAttempts: 1, DelaySeconds: 1}},
		{"single codes", backend.RetryPolicy{MaxAttempts: 1, ExitCodes: []int32{7}}},
		{"attempt bound", backend.RetryPolicy{MaxAttempts: 11, ExitCodes: []int32{7}}},
		{"attempt overflow", backend.RetryPolicy{MaxAttempts: ^uint32(0), ExitCodes: []int32{7}}},
		{"delay bound", backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 3601, ExitCodes: []int32{7}}},
		{"missing codes", backend.RetryPolicy{MaxAttempts: 2}},
		{"zero code", backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{0}}},
		{"negative code", backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{-1}}},
		{"large code", backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{256}}},
		{"oversize duplicate input", backend.RetryPolicy{MaxAttempts: 2, ExitCodes: slices.Repeat([]int32{7}, 33)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, _ := json.Marshal(tc.in)
			got, err := backend.NormalizeRetry(&tc.in)
			if err == nil || got != nil {
				t.Fatalf("invalid policy returned %#v, %v", got, err)
			}
			after, _ := json.Marshal(tc.in)
			if string(after) != string(before) {
				t.Fatal("rejected input changed")
			}
		})
	}
	codes := make([]int32, 32)
	for i := range codes {
		codes[i] = int32(32 - i)
	}
	got, err := backend.NormalizeRetry(&backend.RetryPolicy{MaxAttempts: 2, ExitCodes: codes})
	if err != nil || len(got.ExitCodes) != 32 || got.ExitCodes[0] != 1 || got.ExitCodes[31] != 32 {
		t.Fatalf("32-code boundary: %#v, %v", got, err)
	}
}

func TestNormalizeRetryIndependent(t *testing.T) {
	input := &backend.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{9, 7, 9}}
	a, err := backend.NormalizeRetry(input)
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NormalizeRetry(input)
	if err != nil {
		t.Fatal(err)
	}
	a.ExitCodes[0] = 255
	a.MaxAttempts = 10
	input.ExitCodes[0] = 254
	input.DelaySeconds = 999
	if !reflect.DeepEqual(b, &backend.RetryPolicy{MaxAttempts: 3, DelaySeconds: 1, ExitCodes: []int32{7, 9}}) {
		t.Fatal("independent result changed", b)
	}
	if input.MaxAttempts != 3 || input.ExitCodes[1] != 7 || a.DelaySeconds != 1 || a.ExitCodes[1] != 9 {
		t.Fatal("normalization retained caller aliases")
	}
}

func TestRetryTaskBytes(t *testing.T) {
	for _, policy := range []*backend.RetryPolicy{nil, {}, {MaxAttempts: 1}} {
		normal, err := backend.NormalizeRetry(policy)
		if err != nil {
			t.Fatal(err)
		}
		got, err := backend.TaskBytes(backend.Task{Argv: []string{"false"}, Retry: normal})
		if err != nil || string(got) != `{"argv":["false"]}` {
			t.Fatalf("default bytes changed: %s, %v", got, err)
		}
	}
	policy, err := backend.NormalizeRetry(&backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7, 7}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := backend.TaskBytes(backend.Task{Argv: []string{"false"}, Retry: policy})
	want := `{"argv":["false"],"retry":{"max_attempts":2,"delay_seconds":1,"exit_codes":[7]}}`
	if err != nil || string(got) != want {
		t.Fatalf("retry bytes: %s, %v; want %s", got, err, want)
	}
	// Identity serialization must not silently normalize input itself.
	task := backend.Task{Argv: []string{"false"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{9, 7, 9}}}
	got, err = backend.TaskBytes(task)
	plain, plainErr := json.Marshal(task)
	if err != nil || plainErr != nil || string(got) != string(plain) {
		t.Fatal("TaskBytes differs from json.Marshal")
	}
	want = `{"argv":["false"],"retry":{"max_attempts":2,"delay_seconds":0,"exit_codes":[9,7,9]}}`
	if string(got) != want {
		t.Fatalf("TaskBytes normalized input: %s", got)
	}
}

func TestRetryDefaultJobEncoding(t *testing.T) {
	job := backend.Job{ID: "job", Task: backend.Task{Argv: []string{"work"}}, Target: "local", Status: backend.Status{State: backend.Queued}}
	got, err := json.Marshal(job)
	want := `{"id":"job","task":{"argv":["work"]},"target":"local","status":{"state":"queued"},"cancel_requested":false,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","sequence":0}`
	if err != nil || string(got) != want {
		t.Fatalf("default job bytes changed: %s, %v", got, err)
	}
	job.Attempts = []backend.Attempt{}
	got, err = json.Marshal(job)
	if err != nil || string(got) != want {
		t.Fatalf("empty history changed default bytes: %s, %v", got, err)
	}
	if backend.Retrying.Terminal() {
		t.Fatal("backoff is terminal")
	}
}

func TestCurrentAttemptID(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  backend.Job
		want string
	}{
		{"zero", backend.Job{}, ""},
		{"implicit", backend.Job{ID: "logical"}, "logical"},
		{"empty", backend.Job{ID: "logical", Attempts: []backend.Attempt{}}, "logical"},
		{"first", backend.Job{ID: "logical", Attempts: []backend.Attempt{{ID: "logical"}}}, "logical"},
		{"later", backend.Job{ID: "logical", Attempts: []backend.Attempt{{ID: "logical"}, {ID: "second"}}}, "second"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.job.CurrentAttemptID(); got != tc.want {
				t.Fatalf("current=%q; want %q", got, tc.want)
			}
		})
	}
}

func TestAttemptEncoding(t *testing.T) {
	code := int32(7)
	now := time.Date(2026, 10, 18, 1, 2, 3, 0, time.UTC)
	a := backend.Attempt{ID: "attempt", Status: backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "opaque-instance"}, CreatedAt: now, StartingAt: &now, FinishedAt: &now}
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip backend.Attempt
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundtrip, a) {
		t.Fatalf("attempt evidence lost: %#v", roundtrip)
	}
	if backend.UnknownTermination != 0 || backend.StoppedWithExitCode != 1 {
		t.Fatal("termination zero/positive representation changed")
	}
}
