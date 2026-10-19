package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestAttemptMigrationPreservesHistoricalRows(t *testing.T) {
	for _, name := range []string{"schema1", "schema2", "schema3-legacy", "schema3-scoped"} {
		t.Run(name, func(t *testing.T) {
			dir := routingFixture(t, name)
			old, err := openSnapshot(filepath.Join(dir, "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			before := priorRoutingState(t, old)
			old.Close()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if !bytes.Equal(before, priorRoutingState(t, s.db)) {
				t.Fatal("migration changed original rows, counters or relations")
			}
			wantScoped := name == "schema2" || name == "schema3-scoped"
			if s.Scoped() != wantScoped {
				t.Fatal("migration changed auth mode")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if !bytes.Equal(tx.Bucket(metadataBucket).Get(schemaKey), sequenceKey(schemaVersion)) || !bytes.Equal(tx.Bucket(metadataBucket).Get(retryBytesKey), sequenceKey(0)) || tx.Bucket(attemptJobsBucket) == nil || tx.Bucket(attemptJobsBucket).Stats().KeyN != 0 {
					t.Fatal("new format metadata missing")
				}
				return tx.Bucket(jobsBucket).ForEach(func(_, raw []byte) error {
					var j backend.Job
					if err := json.Unmarshal(raw, &j); err != nil {
						return err
					}
					if len(j.Attempts) != 0 || j.CurrentAttemptID() != j.ID {
						t.Fatal("legacy execution identity changed")
					}
					return nil
				})
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAttemptMalformedStateRefusedWithoutWrites(t *testing.T) {
	cases := []struct {
		name   string
		change func(*bolt.Tx, backend.Job) error
	}{
		{"missing-index", func(tx *bolt.Tx, _ backend.Job) error { return tx.DeleteBucket(attemptJobsBucket) }},
		{"missing-counter", func(tx *bolt.Tx, _ backend.Job) error { return tx.Bucket(metadataBucket).Delete(retryBytesKey) }},
		{"counter-width", func(tx *bolt.Tx, _ backend.Job) error { return tx.Bucket(metadataBucket).Put(retryBytesKey, []byte{1}) }},
		{"counter-total", func(tx *bolt.Tx, _ backend.Job) error {
			return tx.Bucket(metadataBucket).Put(retryBytesKey, sequenceKey(0))
		}},
		{"missing-owner", func(tx *bolt.Tx, j backend.Job) error { return tx.Bucket(attemptJobsBucket).Delete([]byte(j.ID)) }},
		{"wrong-owner", func(tx *bolt.Tx, j backend.Job) error {
			return tx.Bucket(attemptJobsBucket).Put([]byte(j.ID), []byte("other"))
		}},
		{"orphan-owner", func(tx *bolt.Tx, j backend.Job) error {
			return tx.Bucket(attemptJobsBucket).Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte(j.ID))
		}},
		{"deadline", func(tx *bolt.Tx, j backend.Job) error {
			*j.NextAttemptAt = j.NextAttemptAt.Add(time.Second)
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"proof", func(tx *bolt.Tx, j backend.Job) error {
			j.Attempts[0].Status.Termination = 0
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"first-created", func(tx *bolt.Tx, j backend.Job) error {
			j.Attempts[0].CreatedAt = j.CreatedAt.Add(-time.Second)
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"pin", func(tx *bolt.Tx, j backend.Job) error {
			j.Attempts[0].Status.Instance = "changed"
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"duplicate-attempt", func(tx *bolt.Tx, j backend.Job) error {
			j.Attempts = append(j.Attempts, j.Attempts[0])
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"noncanonical-policy", func(tx *bolt.Tx, j backend.Job) error {
			j.Task.Retry.ExitCodes = append(j.Task.Retry.ExitCodes, 7)
			raw, _ := json.Marshal(j)
			return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
		}},
		{"format3-retry", func(tx *bolt.Tx, _ backend.Job) error {
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(3))
		}},
		{"future", func(tx *bolt.Tx, _ backend.Job) error {
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1))
		}},
	}
	for _, test := range cases {
		for _, unsynced := range []bool{false, true} {
			t.Run(test.name+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
				dir := t.TempDir()
				s, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				j := retryJob(t, s, "job", 1)
				now := j.CreatedAt.Add(time.Second)
				j = failTest(t, s, beginTest(t, s, j, now), now)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				editFixture(t, dir, unsynced, func(tx *bolt.Tx) error { return test.change(tx, j) })
				path := filepath.Join(dir, "jobs.db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				opened, err := Open(dir)
				if err == nil {
					opened.Close()
					t.Fatal("corrupt attempts accepted")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("refusal wrote database", err)
				}
				destination := filepath.Join(t.TempDir(), "restore")
				if err := Restore(path, destination); err == nil {
					t.Fatal("corrupt snapshot restored")
				}
				if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed restore published destination")
				}
			})
		}
	}
}

func TestAttemptPriorS04BinaryRefusesFormat4(t *testing.T) {
	exe := os.Getenv("SKYGO_S04_BINARY")
	if exe == "" {
		t.Skip("requires exact archived signed S04 binary")
	}
	raw, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if os.Getenv("SKYGO_S04_SHA256") != hex.EncodeToString(sum[:]) {
		t.Fatal("prior binary hash mismatch")
	}
	for _, name := range []string{"schema3-legacy", "schema3-scoped"} {
		t.Run(name, func(t *testing.T) {
			dir := routingFixture(t, name)
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "jobs.db")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "server", "--listen", "127.0.0.1:0", "--data-dir", dir)
			cmd.Env = append(os.Environ(), "SKYGO_TOKEN=")
			output, err := cmd.CombinedOutput()
			if err == nil || ctx.Err() != nil || !bytes.Contains(output, []byte("unsupported database schema")) {
				t.Fatal("S04 did not promptly refuse schema4", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("S04 refusal changed schema4", err)
			}
		})
	}
}

func TestAttemptUnstartedExecutionMetadataRefused(t *testing.T) {
	for _, state := range []backend.State{backend.Queued, backend.Canceled} {
		for _, metadata := range []string{"exit", "proof", "instance"} {
			for _, unsynced := range []bool{false, true} {
				t.Run(string(state)+"/"+metadata+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
					dir := t.TempDir()
					s, err := Open(dir)
					if err != nil {
						t.Fatal(err)
					}
					j := retryJob(t, s, "unstarted", 1)
					if state == backend.Canceled {
						j, err = s.Update(j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
						if err != nil {
							t.Fatal(err)
						}
						j, _, err = s.BeginAttempt(j.ID, j.ID, 0, j.CreatedAt.Add(time.Second))
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					editFixture(t, dir, unsynced, func(tx *bolt.Tx) error {
						status := &j.Attempts[0].Status
						switch metadata {
						case "exit":
							code := int32(7)
							status.ExitCode = &code
						case "proof":
							code := int32(7)
							status.ExitCode = &code
							status.Termination = backend.StoppedWithExitCode
						case "instance":
							status.Instance = "never-started"
						}
						j.Status = *status
						raw, err := json.Marshal(j)
						if err != nil {
							return err
						}
						return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw)
					})
					path := filepath.Join(dir, "jobs.db")
					before, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					opened, err := Open(dir)
					if err == nil {
						opened.Close()
						t.Fatal("unstarted execution metadata accepted")
					}
					after, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("refusal wrote state", err)
					}
					destination := filepath.Join(t.TempDir(), "restore")
					if err := Restore(path, destination); err == nil {
						t.Fatal("invalid unstarted snapshot restored")
					}
					if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("failed restore published destination")
					}
				})
			}
		}
	}
}
