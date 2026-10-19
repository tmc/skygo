package rpc_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"google.golang.org/protobuf/proto"
)

// attemptBackend supplies deterministic retained log chunks, not native stopped
// evidence. Attempt transitions below use the trusted store API without Run or
// external execution, qualifying transport identity/history rather than a backend.
type attemptBackend struct {
	mu      sync.Mutex
	first   string
	refs    []backend.Reference
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*attemptBackend) Check(backend.Task) error { return nil }
func (*attemptBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected start")
}
func (*attemptBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*attemptBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (b *attemptBackend) Logs(ctx context.Context, ref backend.Reference, offset int64, _ int) (backend.LogChunk, error) {
	b.mu.Lock()
	b.refs = append(b.refs, ref)
	first := b.first
	b.mu.Unlock()
	if ref.ID != first {
		return backend.LogChunk{Data: []byte("second"), NextOffset: 6, Complete: true}, nil
	}
	if offset == 0 {
		return backend.LogChunk{Data: []byte("first-a"), NextOffset: 7}, nil
	}
	b.once.Do(func() { close(b.entered) })
	select {
	case <-ctx.Done():
		return backend.LogChunk{}, ctx.Err()
	case <-b.release:
	}
	return backend.LogChunk{Data: []byte("first-b"), NextOffset: 14, Complete: true}, nil
}

func TestRetryRPCReplayAndFixedLogs(t *testing.T) {
	f := scoped(t, &logBackend{})
	b := &attemptBackend{entered: make(chan struct{}), release: make(chan struct{})}
	targets := []backend.Target{{Name: "retry", Kind: "test", Binding: "fixed-route", Backend: b, SupportsRetry: true}}
	c, err := control.New(f.store, targets, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(rpc.NewHandler(c, f.engine))
	defer srv.Close()
	client := skyv1connect.NewControlServiceClient(srv.Client(), srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := &v1.Task{Argv: []string{"unused"}, Retry: &v1.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{9, 7, 9}}}
	request := &v1.SubmitJobRequest{RequestId: "retry", Task: raw}
	_, err = client.SubmitRetryJob(ctx, authorized(f.tokens["reader"], request))
	code(t, err, connect.CodePermissionDenied)
	_, err = client.SubmitRetryJob(ctx, connect.NewRequest(request))
	code(t, err, connect.CodeUnauthenticated)
	accepted, err := client.SubmitRetryJob(ctx, authorized(f.tokens["alice"], request))
	if err != nil {
		t.Fatal(err)
	}
	j := accepted.Msg.Job
	if j.Task.Target != "" || j.Target != "retry" || j.Task.Retry.DelaySeconds != 1 || !reflect.DeepEqual(j.Task.Retry.ExitCodes, []int32{7, 9}) || len(j.Attempts) != 1 || j.Attempts[0].Id != j.Id || j.Attempts[0].Ordinal != 1 {
		t.Fatalf("accepted=%v", j)
	}
	if raw.Retry.DelaySeconds != 0 || !reflect.DeepEqual(raw.Retry.ExitCodes, []int32{9, 7, 9}) {
		t.Fatal("request mutated")
	}
	// Replay against a different route set must hit Lookup before planning.
	removed, err := control.New(f.store, []backend.Target{{Name: "other", Kind: "test", Binding: "other-route", Backend: b}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	replayServer := httptest.NewServer(rpc.NewHandler(removed, f.engine))
	defer replayServer.Close()
	replayClient := skyv1connect.NewControlServiceClient(replayServer.Client(), replayServer.URL)
	replay, err := replayClient.SubmitRetryJob(ctx, authorized(f.tokens["alice"], request))
	if err != nil || replay.Msg.Job.Id != j.Id {
		t.Fatalf("replay=%v,%v", replay, err)
	}
	altered := proto.Clone(request).(*v1.SubmitJobRequest)
	altered.Task.Retry.MaxAttempts = 3
	_, err = replayClient.SubmitRetryJob(ctx, authorized(f.tokens["alice"], altered))
	code(t, err, connect.CodeAlreadyExists)
	info, err := client.GetServerInfo(ctx, authorized(f.tokens["alice"], &v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.Msg.Targets[0].Capabilities, []v1.Capability{1, 4, 5}) {
		t.Fatalf("capabilities=%v", info.Msg)
	}
	now := time.Now().UTC()
	first, started, err := f.store.BeginAttempt(j.Id, j.Id, 0, now)
	if err != nil || !started {
		t.Fatalf("begin=%v,%v", started, err)
	}
	b.mu.Lock()
	b.first = first.CurrentAttemptID()
	b.mu.Unlock()
	if _, err = f.store.ObserveAttempt(j.Id, j.Id, backend.Status{State: backend.Running, Instance: "instance-1"}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	stream, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: j.Id, Follow: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Receive() {
		t.Fatalf("first chunk: %v", stream.Err())
	}
	if got := stream.Msg(); string(got.Data) != "first-a" || got.AttemptId != j.Id || got.NextOffset != 7 {
		t.Fatalf("first=%v", got)
	}
	select {
	case <-b.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	exit := int32(7)
	backoff, err := f.store.ObserveAttempt(j.Id, j.Id, backend.Status{State: backend.Failed, ExitCode: &exit, Termination: backend.StoppedWithExitCode, Instance: "instance-1"}, now.Add(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if backoff.Status.State != backend.Retrying || backoff.NextAttemptAt == nil {
		t.Fatalf("backoff=%+v", backoff)
	}
	got, err := client.GetJob(ctx, authorized(f.tokens["alice"], &v1.GetJobRequest{Id: j.Id}))
	if err != nil {
		t.Fatal(err)
	}
	if got.Msg.Job.State != v1.JobState_JOB_STATE_RETRYING || got.Msg.Job.NextAttemptAt == nil || len(got.Msg.Job.Attempts) != 1 || got.Msg.Job.Attempts[0].State != v1.JobState_JOB_STATE_FAILED || got.Msg.Job.Attempts[0].FinishedAt == nil {
		t.Fatalf("backoff wire=%v", got.Msg)
	}
	second, started, err := f.store.BeginAttempt(j.Id, j.Id, 0, *backoff.NextAttemptAt)
	if err != nil || !started {
		t.Fatalf("second=%v,%v", started, err)
	}
	if _, err = f.store.ObserveAttempt(j.Id, second.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "instance-2"}, backoff.NextAttemptAt.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	close(b.release)
	if !stream.Receive() {
		t.Fatalf("last chunk: %v", stream.Err())
	}
	if got := stream.Msg(); string(got.Data) != "first-b" || got.AttemptId != j.Id || !got.Complete || got.NextOffset != 14 {
		t.Fatalf("last=%v", got)
	}
	if stream.Receive() || stream.Err() != nil {
		t.Fatalf("stream end=%v", stream.Err())
	}
	b.mu.Lock()
	refs := append([]backend.Reference(nil), b.refs...)
	b.mu.Unlock()
	for _, ref := range refs {
		if ref.ID != j.Id || ref.Instance != "instance-1" || ref.Binding != "fixed-route" {
			t.Fatalf("old stream reference=%+v", ref)
		}
	}
	current, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: j.Id}))
	if err != nil {
		t.Fatal(err)
	}
	if !current.Receive() || current.Msg().AttemptId != second.CurrentAttemptID() || string(current.Msg().Data) != "second" {
		t.Fatalf("new stream=%v,%v", current.Msg(), current.Err())
	}
	current.Close()
	selected, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: j.Id, Attempt: 1}))
	if err != nil {
		t.Fatal(err)
	}
	var previous string
	for selected.Receive() {
		if selected.Msg().AttemptId != j.Id {
			t.Fatalf("selected frame=%v", selected.Msg())
		}
		previous += string(selected.Msg().Data)
	}
	if selected.Err() != nil || previous != "first-afirst-b" {
		t.Fatalf("selected logs=%q,%v", previous, selected.Err())
	}
	selected.Close()
	for _, tc := range []struct {
		principal string
		attempt   uint32
		code      connect.Code
	}{
		{"alice", 3, connect.CodeInvalidArgument},
		{"bob", 3, connect.CodeNotFound},
		{"alice", 99, connect.CodeInvalidArgument},
		{"bob", 99, connect.CodeInvalidArgument},
	} {
		invalid, err := client.StreamJobLogs(ctx, authorized(f.tokens[tc.principal], &v1.StreamJobLogsRequest{Id: j.Id, Attempt: tc.attempt}))
		if err == nil {
			invalid.Receive()
			err = invalid.Err()
			invalid.Close()
		}
		code(t, err, tc.code)
	}
	final, err := client.GetJob(ctx, authorized(f.tokens["alice"], &v1.GetJobRequest{Id: j.Id}))
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Msg.Job.Attempts) != 2 || final.Msg.Job.NextAttemptAt != nil || final.Msg.Job.Attempts[1].Id != second.CurrentAttemptID() || final.Msg.Job.Attempts[1].Ordinal != 2 || final.Msg.Job.Attempts[1].StartingAt == nil {
		t.Fatalf("history=%v", final.Msg)
	}
	legacy, err := client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "default", Task: &v1.Task{Argv: []string{"unused"}}}))
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Msg.Job.Task.Retry != nil || len(legacy.Msg.Job.Attempts) != 0 || legacy.Msg.Job.NextAttemptAt != nil {
		t.Fatalf("default extensions=%v", legacy.Msg)
	}
	if _, err := f.store.Update(legacy.Msg.Job.Id, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
		t.Fatal(err)
	}
	implicit, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: legacy.Msg.Job.Id}))
	if err != nil {
		t.Fatal(err)
	}
	if !implicit.Receive() || implicit.Msg().AttemptId != "" {
		t.Fatalf("default log frame=%v,%v", implicit.Msg(), implicit.Err())
	}
	implicit.Close()

}
