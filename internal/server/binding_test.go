package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	bolt "go.etcd.io/bbolt"
)

func TestLegacyWorkload(t *testing.T) {
	if os.Getenv("SKYGO_LEGACY_WORKLOAD") != "success" {
		return
	}
	fmt.Fprintln(os.Stdout, "legacy Go workload")
	os.Exit(0)
}

// priorBucket is the lossless export of a database made by the pinned old Store.
// This reconstruction does not invent a current-schema empty-binding row.
type priorBucket struct {
	Name     []byte                        `json:"name"`
	Sequence uint64                        `json:"sequence"`
	Entries  []struct{ Key, Value []byte } `json:"entries"`
	Buckets  []priorBucket                 `json:"buckets"`
}

func legacyState(t *testing.T) (string, []backend.Job) {
	t.Helper()
	data, err := os.ReadFile("../store/testdata/schema1.json")
	if err != nil {
		t.Fatal(err)
	}
	const want = "2b90b4c29734505f68db6d35f8a5b371641828acfccebe23486a0b2a1aa5b9bf"
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != want {
		t.Fatalf("fixture SHA256 = %s; want %s", got, want)
	}
	var buckets []priorBucket
	if err := json.Unmarshal(data, &buckets); err != nil {
		t.Fatal(err)
	}
	if len(data) > 1<<20 || len(buckets) > 32 {
		t.Fatal("legacy fixture exceeds bounds")
	}
	dir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []backend.Job
	var fill func(*bolt.Bucket, priorBucket, int) error
	fill = func(dst *bolt.Bucket, src priorBucket, depth int) error {
		if depth > 8 || len(src.Entries) > 100 || len(src.Buckets) > 32 {
			return errors.New("legacy fixture exceeds bounds")
		}
		if err := dst.SetSequence(src.Sequence); err != nil {
			return err
		}
		for _, e := range src.Entries {
			if err := dst.Put(e.Key, e.Value); err != nil {
				return err
			}
		}
		for _, child := range src.Buckets {
			b, err := dst.CreateBucket(child.Name)
			if err != nil {
				return err
			}
			if err := fill(b, child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, src := range buckets {
			dst, err := tx.CreateBucket(src.Name)
			if err != nil {
				return err
			}
			if err := fill(dst, src, 0); err != nil {
				return err
			}
			if string(src.Name) == "jobs" {
				for _, e := range src.Entries {
					var job backend.Job
					if err := json.Unmarshal(e.Value, &job); err != nil {
						return err
					}
					jobs = append(jobs, job)
				}
			}
		}
		return nil
	})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("reconstruct fixture: %v, %v", err, closeErr)
	}
	if len(jobs) != 3 {
		t.Fatalf("fixture has %d jobs", len(jobs))
	}
	return dir, jobs
}

type noExecution struct{ calls atomic.Int32 }

func (b *noExecution) Check(backend.Task) error {
	b.calls.Add(1)
	return errors.New("unexpected check")
}
func (b *noExecution) Start(context.Context, string, backend.Task) error {
	b.calls.Add(1)
	return errors.New("unexpected start")
}
func (b *noExecution) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.calls.Add(1)
	return backend.Status{}, errors.New("unexpected observe")
}
func (b *noExecution) Cancel(context.Context, backend.Reference) error {
	b.calls.Add(1)
	return errors.New("unexpected cancel")
}
func (b *noExecution) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	b.calls.Add(1)
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func TestLegacyLocalBinding(t *testing.T) {
	dir, prior := legacyState(t)
	// The old task contains this relative path, so no task/hash rewrite is needed.
	// t.Chdir is scoped to this nonparallel test and restored after server cleanup.
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	working := t.TempDir()
	dest, err := os.OpenFile(filepath.Join(working, "legacy-workload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(dest, source)
	closeErr := dest.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy helper: %v, %v", copyErr, closeErr)
	}
	t.Chdir(working)
	remote := &noExecution{}
	r := startConfig(t, server.Config{DataDir: dir, Targets: func(string) ([]backend.Target, error) {
		return []backend.Target{{Name: "kubernetes", Kind: "kubernetes", Binding: "new-remote", Backend: remote}}, nil
	}})
	api := skyv1connect.NewControlServiceClient(&http.Client{Timeout: time.Second}, r.url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, old := range prior {
		want := v1.JobState_JOB_STATE_UNKNOWN
		if old.Target != "local" {
			want = v1.JobState_JOB_STATE_RUNNING
		} else if old.Status.State == backend.Queued {
			want = v1.JobState_JOB_STATE_SUCCEEDED
		}
		for {
			got, err := api.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: old.ID}))
			if err != nil {
				t.Fatal(err)
			}
			held := old.Target == "local" || strings.Contains(got.Msg.Job.Reason, "binding is unavailable")
			if got.Msg.Job.State == want && held {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("legacy job = %v; want %v", got.Msg.Job, want)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	stop(t, r)
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	for _, old := range prior {
		got, err := s.Get(old.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := ""
		if old.Target == "local" {
			want = binding
		}
		oldTask, _ := backend.TaskBytes(old.Task)
		newTask, _ := backend.TaskBytes(got.Task)
		if got.TargetBinding != want || !bytes.Equal(oldTask, newTask) || got.Sequence != old.Sequence || !got.CreatedAt.Equal(old.CreatedAt) {
			t.Fatalf("legacy identity changed: %+v", got)
		}
	}
	if remote.calls.Load() != 0 {
		t.Fatalf("legacy remote backend called %d times", remote.calls.Load())
	}
}

func TestLegacyLocalBindingRequiresBuiltin(t *testing.T) {
	dir, prior := legacyState(t)
	fake := &noExecution{}
	r := startConfig(t, server.Config{DataDir: dir, DisableLocal: true, Targets: func(string) ([]backend.Target, error) {
		return []backend.Target{{Name: "local", Kind: "local", Binding: "claimed-local", Backend: fake}}, nil
	}})
	api := skyv1connect.NewControlServiceClient(&http.Client{Timeout: time.Second}, r.url)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, old := range prior {
		if old.Target != "local" {
			continue
		}
		for {
			got, err := api.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: old.ID}))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(got.Msg.Job.Reason, "binding is unavailable") {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("legacy route not held: %v", got.Msg.Job)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	stop(t, r)
	assertUnbound(t, dir, prior)
	if fake.calls.Load() != 0 {
		t.Fatalf("claimed local backend called %d times", fake.calls.Load())
	}
}

func TestLegacyBindingAfterConfiguration(t *testing.T) {
	for _, name := range []string{"duplicate target", "auth mismatch"} {
		t.Run(name, func(t *testing.T) {
			dir, prior := legacyState(t)
			cfg := server.Config{DataDir: dir, Listen: "127.0.0.1:0"}
			if name == "auth mismatch" {
				cfg.AuthFile = filepath.Join(t.TempDir(), "missing.json")
			} else {
				cfg.Targets = func(string) ([]backend.Target, error) {
					return []backend.Target{{Name: "local", Kind: "local", Binding: "duplicate", Backend: &noExecution{}}}, nil
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := server.Run(ctx, cfg); err == nil {
				t.Fatal("invalid startup accepted")
			}
			assertUnbound(t, dir, prior)
		})
	}
}

func assertUnbound(t *testing.T, dir string, prior []backend.Job) {
	t.Helper()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, old := range prior {
		got, err := s.Get(old.ID)
		if err != nil {
			t.Fatal(err)
		}
		oldTask, _ := backend.TaskBytes(old.Task)
		newTask, _ := backend.TaskBytes(got.Task)
		if got.TargetBinding != "" || got.Status.State != old.Status.State || !bytes.Equal(oldTask, newTask) {
			t.Fatalf("untrusted startup changed binding/task/state: %+v", got)
		}
	}
}
